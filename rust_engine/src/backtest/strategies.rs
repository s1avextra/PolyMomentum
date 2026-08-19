//! Strategy variants for the backtest harness.
//!
//! Each variant wraps the live `decide_candle_trade` with a different
//! `ZoneConfig`. The harness loops one variant at a time over the same PMXT
//! v2 + BTC tape so per-strategy P&L is comparable.

use std::collections::{BTreeMap, BTreeSet};

use crate::data::models::{DEFAULT_CRYPTO_TAKER_FEE_RATE, DEFAULT_MAKER_FEE_RATE};
use crate::strategy::decision::{DecisionRegime, ZoneConfig};
use crate::strategy::microstructure::MicrostructureConfig;

fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SelectivityFilter {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub require_tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deny_tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub require_tag_values: BTreeMap<String, BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deny_tag_values: BTreeMap<String, BTreeSet<String>>,
}

impl SelectivityFilter {
    pub fn is_disabled(&self) -> bool {
        self.require_tags.is_empty()
            && self.deny_tags.is_empty()
            && self.require_tag_values.is_empty()
            && self.deny_tag_values.is_empty()
    }

    pub fn reject_reason(&self, regime: &DecisionRegime) -> Option<String> {
        if self.is_disabled() {
            return None;
        }
        let tags: BTreeMap<String, String> = regime.causal_tags().into_iter().collect();
        for (dimension, expected) in &self.require_tags {
            match tags.get(dimension) {
                Some(actual) if actual == expected => {}
                Some(actual) => {
                    return Some(format!(
                        "selectivity_require_{}_{}_got_{}",
                        clean_label(dimension),
                        clean_label(expected),
                        clean_label(actual)
                    ));
                }
                None => {
                    return Some(format!(
                        "selectivity_require_{}_{}_missing",
                        clean_label(dimension),
                        clean_label(expected)
                    ));
                }
            }
        }
        for (dimension, allowed) in &self.require_tag_values {
            match tags.get(dimension) {
                Some(actual) if allowed.contains(actual) => {}
                Some(actual) => {
                    return Some(format!(
                        "selectivity_require_{}_{}_got_{}",
                        clean_label(dimension),
                        clean_label(&join_values(allowed)),
                        clean_label(actual)
                    ));
                }
                None => {
                    return Some(format!(
                        "selectivity_require_{}_{}_missing",
                        clean_label(dimension),
                        clean_label(&join_values(allowed))
                    ));
                }
            }
        }
        for (dimension, denied) in &self.deny_tags {
            if tags.get(dimension) == Some(denied) {
                return Some(format!(
                    "selectivity_deny_{}_{}",
                    clean_label(dimension),
                    clean_label(denied)
                ));
            }
        }
        for (dimension, denied_values) in &self.deny_tag_values {
            if let Some(actual) = tags.get(dimension) {
                if denied_values.contains(actual) {
                    return Some(format!(
                        "selectivity_deny_{}_{}",
                        clean_label(dimension),
                        clean_label(actual)
                    ));
                }
            }
        }
        None
    }

    pub fn label(&self) -> String {
        let mut parts = Vec::new();
        for (dimension, value) in &self.require_tags {
            parts.push(format!(
                "req{}-{}",
                clean_label(dimension),
                clean_label(value)
            ));
        }
        for (dimension, value) in &self.deny_tags {
            parts.push(format!(
                "deny{}-{}",
                clean_label(dimension),
                clean_label(value)
            ));
        }
        for (dimension, values) in &self.require_tag_values {
            for value in values {
                parts.push(format!(
                    "req{}-{}",
                    clean_label(dimension),
                    clean_label(value)
                ));
            }
        }
        for (dimension, values) in &self.deny_tag_values {
            for value in values {
                parts.push(format!(
                    "deny{}-{}",
                    clean_label(dimension),
                    clean_label(value)
                ));
            }
        }
        parts.join("_")
    }
}

fn join_values(values: &BTreeSet<String>) -> String {
    values.iter().cloned().collect::<Vec<_>>().join("|")
}

fn clean_label(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

/// Optional research-only risk exits for an already-filled binary position.
/// They remain disabled by default, and release/live loaders reject any
/// enabled exit configuration until the runtime implements exact parity.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExitConfig {
    #[serde(default)]
    pub settlement_basis_enabled: bool,
    /// Buy the missing outcome only when the resulting complete set has a
    /// fee-inclusive guaranteed payout above its total executable cost.
    #[serde(default)]
    pub complete_set_lock_enabled: bool,
    #[serde(default = "default_complete_set_min_profit_usd")]
    pub complete_set_min_profit_usd: f64,
    /// For a trailing complete-set lock, arm after the executable guaranteed
    /// profit reaches this level and lock only after it retreats below it.
    /// Zero preserves the immediate v1 behavior and serialization hash.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub complete_set_arm_profit_usd: f64,
    #[serde(default = "default_exit_min_hold_seconds")]
    pub min_hold_seconds: f64,
    #[serde(default)]
    pub basis_buffer_usd: f64,
    #[serde(default = "default_exit_min_seconds_before_close")]
    pub min_seconds_before_close: f64,
    #[serde(default = "default_exit_retry_cooldown_seconds")]
    pub retry_cooldown_seconds: f64,
}

impl Default for ExitConfig {
    fn default() -> Self {
        Self {
            settlement_basis_enabled: false,
            complete_set_lock_enabled: false,
            complete_set_min_profit_usd: default_complete_set_min_profit_usd(),
            complete_set_arm_profit_usd: 0.0,
            min_hold_seconds: default_exit_min_hold_seconds(),
            basis_buffer_usd: 0.0,
            min_seconds_before_close: default_exit_min_seconds_before_close(),
            retry_cooldown_seconds: default_exit_retry_cooldown_seconds(),
        }
    }
}

impl ExitConfig {
    pub fn is_disabled(&self) -> bool {
        !self.settlement_basis_enabled && !self.complete_set_lock_enabled
    }

    pub fn should_lock_complete_set(
        &self,
        locked_profit_usd: f64,
        entry_ts_s: f64,
        now_ts_s: f64,
        close_ts_s: f64,
    ) -> bool {
        self.complete_set_lock_action(locked_profit_usd, false, entry_ts_s, now_ts_s, close_ts_s)
            == CompleteSetLockAction::Lock
    }

    pub(crate) fn complete_set_lock_action(
        &self,
        locked_profit_usd: f64,
        armed: bool,
        entry_ts_s: f64,
        now_ts_s: f64,
        close_ts_s: f64,
    ) -> CompleteSetLockAction {
        if !self.complete_set_window_open(entry_ts_s, now_ts_s, close_ts_s)
            || !locked_profit_usd.is_finite()
        {
            return CompleteSetLockAction::Wait;
        }
        if self.complete_set_arm_profit_usd == 0.0 {
            return if locked_profit_usd + 1e-9 >= self.complete_set_min_profit_usd {
                CompleteSetLockAction::Lock
            } else {
                CompleteSetLockAction::Wait
            };
        }
        if !armed {
            return if locked_profit_usd + 1e-9 >= self.complete_set_arm_profit_usd {
                CompleteSetLockAction::Arm
            } else {
                CompleteSetLockAction::Wait
            };
        }
        if locked_profit_usd + 1e-9 >= self.complete_set_arm_profit_usd {
            CompleteSetLockAction::Wait
        } else if locked_profit_usd + 1e-9 >= self.complete_set_min_profit_usd {
            CompleteSetLockAction::Lock
        } else {
            CompleteSetLockAction::Wait
        }
    }

    pub fn complete_set_window_open(
        &self,
        entry_ts_s: f64,
        now_ts_s: f64,
        close_ts_s: f64,
    ) -> bool {
        self.complete_set_lock_enabled
            && self.complete_set_min_profit_usd.is_finite()
            && self.complete_set_min_profit_usd >= 0.0
            && self.complete_set_arm_profit_usd.is_finite()
            && self.complete_set_arm_profit_usd >= 0.0
            && (self.complete_set_arm_profit_usd == 0.0
                || self.complete_set_arm_profit_usd + 1e-9 >= self.complete_set_min_profit_usd)
            && self.min_hold_seconds >= 0.0
            && self.min_seconds_before_close >= 0.0
            && entry_ts_s.is_finite()
            && now_ts_s.is_finite()
            && close_ts_s.is_finite()
            && now_ts_s - entry_ts_s >= self.min_hold_seconds
            && close_ts_s - now_ts_s >= self.min_seconds_before_close
    }

    #[allow(clippy::too_many_arguments)]
    pub fn should_exit(
        &self,
        direction: &str,
        open_btc: f64,
        current_btc: f64,
        entry_ts_s: f64,
        now_ts_s: f64,
        close_ts_s: f64,
    ) -> bool {
        if !self.settlement_basis_enabled
            || !open_btc.is_finite()
            || !current_btc.is_finite()
            || !entry_ts_s.is_finite()
            || !now_ts_s.is_finite()
            || !close_ts_s.is_finite()
            || open_btc <= 0.0
            || current_btc <= 0.0
            || self.min_hold_seconds < 0.0
            || self.basis_buffer_usd < 0.0
            || self.min_seconds_before_close < 0.0
            || self.retry_cooldown_seconds < 0.0
            || now_ts_s - entry_ts_s < self.min_hold_seconds
            || close_ts_s - now_ts_s < self.min_seconds_before_close
        {
            return false;
        }

        match direction {
            "up" => current_btc <= open_btc - self.basis_buffer_usd,
            "down" => current_btc >= open_btc + self.basis_buffer_usd,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompleteSetLockAction {
    Wait,
    Arm,
    Lock,
}

fn default_exit_min_hold_seconds() -> f64 {
    15.0
}

fn default_complete_set_min_profit_usd() -> f64 {
    0.10
}

fn default_exit_min_seconds_before_close() -> f64 {
    5.0
}

fn default_exit_retry_cooldown_seconds() -> f64 {
    1.0
}

/// Tunable knobs the harness varies. The variant name is what shows up in
/// the report.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StrategyVariant {
    pub name: String,
    pub zone_config: ZoneConfig,
    pub skip_dead_zone: bool,
    pub min_confidence: f64,
    pub min_edge: f64,
    /// Conservative lower bound for the annualized volatility used to price
    /// the binary payoff. Zero preserves the observed-volatility behavior.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub decision_volatility_floor: f64,
    /// Fraction of bankroll per trade (capped by `max_per_market_usd`).
    pub position_pct: f64,
    /// Hard cap on position size (USD).
    pub max_per_market_usd: f64,
    /// Optional soft cap on projected stressed drawdown. When positive, the
    /// runtime caps or skips a new order if current unresolved exposure plus
    /// the new order would push stressed drawdown above this fraction.
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub max_projected_stressed_drawdown_pct: f64,
    /// Optional feed-forward execution fallback. After this many realized
    /// losses, and once the realized drawdown threshold is met, the runtime can
    /// tighten z-score gates and optionally stop using maker orders.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub degraded_after_losses: u64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub degraded_after_drawdown_pct: f64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub degraded_min_z: f64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub degraded_max_price: f64,
    #[serde(default, skip_serializing_if = "is_false")]
    pub degraded_force_taker: bool,
    /// Use resting maker limit orders instead of one-tick taker.
    pub prefer_maker: bool,
    /// Probability that a resting maker order fills before cancel/market move;
    /// ignored unless `prefer_maker` is true.
    pub maker_fill_prob: f64,
    /// Optional RNG seed for reproducible maker fills. None → entropy.
    pub maker_seed: Option<u64>,
    /// Use the no-slippage Perfect fill model. Sanity baseline only — sets
    /// an upper bound on possible PnL.
    pub use_perfect_fill: bool,
    /// Default fee rate for taker fills.
    pub default_fee_rate: f64,
    /// Maker fee rate. Polymarket pays a rebate (default 0%) but explicit
    /// for clarity.
    pub maker_fee_rate: f64,
    /// Optional order-book confirmation gate for long entries.
    #[serde(default)]
    pub microstructure: MicrostructureConfig,
    /// Optional causal regime filter applied after decision construction and
    /// before any order/exposure side effects.
    #[serde(default, skip_serializing_if = "SelectivityFilter::is_disabled")]
    pub selectivity: SelectivityFilter,
    /// Optional executable sell-side lifecycle. Omitted and disabled for all
    /// legacy variants until replay evidence supports promotion.
    #[serde(default, skip_serializing_if = "ExitConfig::is_disabled")]
    pub exit: ExitConfig,
}

pub(crate) fn decision_volatility_with_floor(
    observed_volatility: f64,
    volatility_floor: f64,
) -> f64 {
    if !observed_volatility.is_finite()
        || observed_volatility < 0.01
        || !volatility_floor.is_finite()
        || !(0.0..=5.0).contains(&volatility_floor)
    {
        return f64::NAN;
    }
    observed_volatility.max(volatility_floor)
}

impl StrategyVariant {
    pub fn risk_profile(&self) -> String {
        format!(
            "position_pct={:.4};max_per_market_usd={:.2};stress_dd_cap={:.4};decision_vol_floor={:.4}{}",
            self.position_pct,
            self.max_per_market_usd,
            self.max_projected_stressed_drawdown_pct,
            self.decision_volatility_floor,
            if self.selectivity.is_disabled() {
                String::new()
            } else {
                format!(";selectivity={}", self.selectivity.label())
            }
        )
    }

    pub fn baseline() -> Self {
        Self {
            name: "baseline".into(),
            zone_config: ZoneConfig::default(),
            skip_dead_zone: true,
            min_confidence: 0.60,
            min_edge: 0.07,
            decision_volatility_floor: 0.0,
            position_pct: 0.10,
            max_per_market_usd: 20.0,
            max_projected_stressed_drawdown_pct: 0.0,
            degraded_after_losses: 0,
            degraded_after_drawdown_pct: 0.0,
            degraded_min_z: 0.0,
            degraded_max_price: 0.0,
            degraded_force_taker: false,
            prefer_maker: false,
            maker_fill_prob: 0.65,
            maker_seed: Some(42),
            use_perfect_fill: false,
            default_fee_rate: DEFAULT_CRYPTO_TAKER_FEE_RATE,
            maker_fee_rate: DEFAULT_MAKER_FEE_RATE,
            microstructure: MicrostructureConfig::disabled(),
            selectivity: SelectivityFilter::default(),
            exit: ExitConfig::default(),
        }
    }

    pub fn decision_volatility(&self, observed_volatility: f64) -> f64 {
        decision_volatility_with_floor(observed_volatility, self.decision_volatility_floor)
    }

    pub fn terminal_only() -> Self {
        let cfg = ZoneConfig {
            early_min_confidence: 1.1,
            early_min_z: 100.0,
            late_min_confidence: 1.1,
            late_min_z: 100.0,
            primary_min_z: 100.0,
            ..ZoneConfig::default()
        };
        Self {
            name: "terminal_only".into(),
            zone_config: cfg,
            ..Self::baseline()
        }
    }

    pub fn aggressive_terminal() -> Self {
        let cfg = ZoneConfig {
            early_min_confidence: 1.1,
            early_min_z: 100.0,
            late_min_confidence: 1.1,
            late_min_z: 100.0,
            primary_min_z: 100.0,
            terminal_min_confidence: 0.50,
            terminal_min_z: 0.20,
            terminal_min_edge: 0.02,
            min_ev_buffer: 0.03,
            ..ZoneConfig::default()
        };
        Self {
            name: "aggressive_terminal".into(),
            zone_config: cfg,
            ..Self::baseline()
        }
    }

    pub fn conservative_terminal() -> Self {
        let cfg = ZoneConfig {
            early_min_confidence: 1.1,
            early_min_z: 100.0,
            late_min_confidence: 1.1,
            late_min_z: 100.0,
            primary_min_z: 100.0,
            terminal_min_confidence: 0.65,
            terminal_min_z: 0.50,
            terminal_min_edge: 0.07,
            min_ev_buffer: 0.07,
            ..ZoneConfig::default()
        };
        Self {
            name: "conservative_terminal".into(),
            zone_config: cfg,
            ..Self::baseline()
        }
    }

    pub fn maker_first() -> Self {
        Self {
            name: "maker_first".into(),
            prefer_maker: true,
            ..Self::baseline()
        }
    }

    /// Very loose confidence/z thresholds — forces trades to fire so we can
    /// verify the harness wiring + resolver. Don't use this for production
    /// numbers; it'll over-fire on noise.
    pub fn loose_smoke() -> Self {
        let cfg = ZoneConfig {
            early_min_confidence: 0.15,
            early_min_z: 0.10,
            early_min_edge: 0.0,
            late_min_confidence: 0.15,
            late_min_z: 0.10,
            late_min_edge: 0.0,
            terminal_min_confidence: 0.15,
            terminal_min_z: 0.10,
            terminal_min_edge: 0.0,
            primary_min_z: 0.10,
            min_ev_buffer: -1.0,
            ..ZoneConfig::default()
        };
        Self {
            name: "loose_smoke".into(),
            zone_config: cfg,
            skip_dead_zone: false,
            min_confidence: 0.15,
            min_edge: 0.0,
            decision_volatility_floor: 0.0,
            position_pct: 0.10,
            max_per_market_usd: 20.0,
            max_projected_stressed_drawdown_pct: 0.0,
            degraded_after_losses: 0,
            degraded_after_drawdown_pct: 0.0,
            degraded_min_z: 0.0,
            degraded_max_price: 0.0,
            degraded_force_taker: false,
            prefer_maker: false,
            maker_fill_prob: 0.65,
            maker_seed: Some(42),
            use_perfect_fill: false,
            default_fee_rate: DEFAULT_CRYPTO_TAKER_FEE_RATE,
            maker_fee_rate: DEFAULT_MAKER_FEE_RATE,
            microstructure: MicrostructureConfig::disabled(),
            selectivity: SelectivityFilter::default(),
            exit: ExitConfig::default(),
        }
    }

    /// Same loose gates as `loose_smoke` but uses resting maker limits. Meant
    /// to test whether maker economics survive realistic non-fill risk.
    pub fn loose_maker() -> Self {
        Self {
            name: "loose_maker".into(),
            prefer_maker: true,
            ..Self::loose_smoke()
        }
    }

    pub fn microstructure_confirmed() -> Self {
        Self {
            name: "microstructure_confirmed".into(),
            microstructure: MicrostructureConfig {
                max_spread: 0.08,
                min_book_depth: 20.0,
                min_book_pressure: 0.10,
                ..MicrostructureConfig::default()
            },
            ..Self::baseline()
        }
    }

    pub fn terminal_microstructure() -> Self {
        Self {
            name: "terminal_microstructure".into(),
            microstructure: MicrostructureConfig {
                max_spread: 0.08,
                min_book_depth: 20.0,
                min_book_pressure: 0.10,
                ..MicrostructureConfig::default()
            },
            ..Self::terminal_only()
        }
    }

    pub fn degraded_execution_active(&self, losses: u64, realized_drawdown_pct: f64) -> bool {
        self.degraded_after_losses > 0
            && losses >= self.degraded_after_losses
            && realized_drawdown_pct.is_finite()
            && realized_drawdown_pct >= self.degraded_after_drawdown_pct.max(0.0)
    }

    pub fn effective_zone_config(&self, losses: u64, realized_drawdown_pct: f64) -> ZoneConfig {
        let mut cfg = self.zone_config;
        if self.degraded_execution_active(losses, realized_drawdown_pct)
            && self.degraded_min_z.is_finite()
            && self.degraded_min_z > 0.0
        {
            cfg.early_min_z = cfg.early_min_z.max(self.degraded_min_z);
            cfg.primary_min_z = cfg.primary_min_z.max(self.degraded_min_z);
            cfg.late_min_z = cfg.late_min_z.max(self.degraded_min_z);
            cfg.terminal_min_z = cfg.terminal_min_z.max(self.degraded_min_z);
        }
        if self.degraded_execution_active(losses, realized_drawdown_pct)
            && self.degraded_max_price.is_finite()
            && self.degraded_max_price > 0.0
        {
            cfg.max_price = cfg.max_price.min(self.degraded_max_price);
            if cfg.min_price > cfg.max_price {
                cfg.min_price = cfg.max_price;
            }
        }
        cfg
    }

    pub fn effective_prefer_maker(&self, losses: u64, realized_drawdown_pct: f64) -> bool {
        if self.degraded_force_taker
            && self.degraded_execution_active(losses, realized_drawdown_pct)
        {
            false
        } else {
            self.prefer_maker
        }
    }
}

/// Default sweep set for the harness.
pub fn default_variants() -> Vec<StrategyVariant> {
    vec![
        StrategyVariant::loose_smoke(),
        StrategyVariant::loose_maker(),
        StrategyVariant::baseline(),
        StrategyVariant::terminal_only(),
        StrategyVariant::aggressive_terminal(),
        StrategyVariant::conservative_terminal(),
        StrategyVariant::maker_first(),
        StrategyVariant::microstructure_confirmed(),
        StrategyVariant::terminal_microstructure(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settlement_basis_exit_is_disabled_and_fail_closed_by_default() {
        let cfg = ExitConfig::default();
        assert!(cfg.is_disabled());
        assert!(!cfg.should_exit("up", 70_000.0, 69_990.0, 0.0, 30.0, 60.0));
    }

    #[test]
    fn settlement_basis_exit_obeys_hold_buffer_and_close_horizon() {
        let cfg = ExitConfig {
            settlement_basis_enabled: true,
            min_hold_seconds: 15.0,
            basis_buffer_usd: 5.0,
            min_seconds_before_close: 5.0,
            retry_cooldown_seconds: 1.0,
            ..ExitConfig::default()
        };
        assert!(!cfg.should_exit("up", 70_000.0, 69_994.0, 0.0, 14.0, 60.0));
        assert!(cfg.should_exit("up", 70_000.0, 69_994.0, 0.0, 15.0, 60.0));
        assert!(cfg.should_exit("down", 70_000.0, 70_006.0, 0.0, 15.0, 60.0));
        assert!(!cfg.should_exit("up", 70_000.0, 69_994.0, 0.0, 56.0, 60.0));
        assert!(!cfg.should_exit("sideways", 70_000.0, 69_000.0, 0.0, 15.0, 60.0));
    }

    #[test]
    fn complete_set_lock_is_disabled_and_requires_guaranteed_profit() {
        let mut cfg = ExitConfig::default();
        assert!(cfg.is_disabled());
        assert!(!cfg.should_lock_complete_set(1.0, 0.0, 15.0, 60.0));

        cfg.complete_set_lock_enabled = true;
        assert!(!cfg.is_disabled());
        assert!(!cfg.should_lock_complete_set(0.099, 0.0, 15.0, 60.0));
        assert!(cfg.should_lock_complete_set(0.10, 0.0, 15.0, 60.0));
        assert!(!cfg.should_lock_complete_set(0.10, 0.0, 14.9, 60.0));
        assert!(!cfg.should_lock_complete_set(0.10, 0.0, 56.0, 60.0));
    }

    #[test]
    fn trailing_complete_set_lock_arms_then_locks_only_inside_profit_band() {
        let mut cfg = ExitConfig {
            complete_set_lock_enabled: true,
            complete_set_arm_profit_usd: 0.50,
            ..ExitConfig::default()
        };

        assert_eq!(
            cfg.complete_set_lock_action(0.49, false, 0.0, 15.0, 60.0),
            CompleteSetLockAction::Wait
        );
        assert_eq!(
            cfg.complete_set_lock_action(0.50, false, 0.0, 15.0, 60.0),
            CompleteSetLockAction::Arm
        );
        assert_eq!(
            cfg.complete_set_lock_action(0.70, true, 0.0, 16.0, 60.0),
            CompleteSetLockAction::Wait
        );
        assert_eq!(
            cfg.complete_set_lock_action(0.30, true, 0.0, 17.0, 60.0),
            CompleteSetLockAction::Lock
        );
        assert_eq!(
            cfg.complete_set_lock_action(0.099, true, 0.0, 18.0, 60.0),
            CompleteSetLockAction::Wait
        );

        cfg.complete_set_arm_profit_usd = 0.05;
        assert_eq!(
            cfg.complete_set_lock_action(0.20, true, 0.0, 19.0, 60.0),
            CompleteSetLockAction::Wait
        );
    }

    #[test]
    fn older_exit_json_defaults_complete_set_lock_to_disabled() {
        let cfg: ExitConfig = serde_json::from_str(
            r#"{
                "settlement_basis_enabled": true,
                "min_hold_seconds": 20.0,
                "basis_buffer_usd": 3.0,
                "min_seconds_before_close": 6.0,
                "retry_cooldown_seconds": 2.0
            }"#,
        )
        .unwrap();

        assert!(cfg.settlement_basis_enabled);
        assert!(!cfg.complete_set_lock_enabled);
        assert_eq!(cfg.complete_set_min_profit_usd, 0.10);
        assert_eq!(cfg.complete_set_arm_profit_usd, 0.0);
    }

    #[test]
    fn decision_volatility_floor_is_optional_and_fail_closed() {
        let mut variant = StrategyVariant::baseline();
        assert_eq!(variant.decision_volatility(0.35), 0.35);
        assert!(serde_json::to_value(&variant)
            .unwrap()
            .get("decision_volatility_floor")
            .is_none());

        variant.decision_volatility_floor = 0.50;
        assert_eq!(variant.decision_volatility(0.35), 0.50);
        assert_eq!(variant.decision_volatility(0.70), 0.70);
        assert!(variant.decision_volatility(f64::NAN).is_nan());

        variant.decision_volatility_floor = -0.01;
        assert!(variant.decision_volatility(0.35).is_nan());
        variant.decision_volatility_floor = 5.01;
        assert!(variant.decision_volatility(0.35).is_nan());
    }

    #[test]
    fn selectivity_filter_requires_and_denies_causal_tags() {
        let mut require_down = SelectivityFilter::default();
        require_down
            .require_tags
            .insert("direction".to_string(), "down".to_string());
        let down = DecisionRegime {
            direction: "down".to_string(),
            zone: "primary".to_string(),
            ..DecisionRegime::default()
        };
        let up = DecisionRegime {
            direction: "up".to_string(),
            zone: "primary".to_string(),
            ..DecisionRegime::default()
        };

        assert!(require_down.reject_reason(&down).is_none());
        assert!(require_down
            .reject_reason(&up)
            .unwrap()
            .starts_with("selectivity_require_direction_down"));

        let mut deny_early = SelectivityFilter::default();
        deny_early
            .deny_tags
            .insert("zone".to_string(), "early".to_string());
        let early = DecisionRegime {
            zone: "early".to_string(),
            price_bucket: "0.50_0.75".to_string(),
            edge_bucket: "0.07_0.15".to_string(),
            z_bucket: "0.7_1.1".to_string(),
            confidence_bucket: "0.50_0.70".to_string(),
            volatility_bucket: "lt_0.40".to_string(),
            reversion_bucket: "0".to_string(),
            minutes_remaining_bucket: "2_4".to_string(),
            ..down.clone()
        };
        assert!(deny_early.reject_reason(&down).is_none());
        assert_eq!(
            deny_early.reject_reason(&early).unwrap(),
            "selectivity_deny_zone_early"
        );

        let mut deny_regime = SelectivityFilter::default();
        deny_regime
            .deny_tags
            .insert("regime".to_string(), early.key());
        assert_eq!(
            deny_regime.reject_reason(&early).unwrap(),
            "selectivity_deny_regime_zone_early_dir_down_price_0.50_0.75_edge_0.07_0.15_z_0.7_1.1_conf_0.50_0.70_vol_lt_0.40_rev_0_min_2_4"
        );
        assert!(deny_regime.reject_reason(&down).is_none());

        let primary_up = DecisionRegime {
            direction: "up".to_string(),
            zone: "primary".to_string(),
            price_bucket: "0.75_0.90".to_string(),
            edge_bucket: "0.07_0.15".to_string(),
            z_bucket: "0.7_1.1".to_string(),
            confidence_bucket: "0.50_0.70".to_string(),
            volatility_bucket: "lt_0.40".to_string(),
            reversion_bucket: "0".to_string(),
            minutes_remaining_bucket: "2_4".to_string(),
            ..DecisionRegime::default()
        };
        let mut deny_many_regimes = SelectivityFilter::default();
        deny_many_regimes.deny_tag_values.insert(
            "regime".to_string(),
            std::collections::BTreeSet::from([early.key(), primary_up.key()]),
        );
        assert_eq!(
            deny_many_regimes.reject_reason(&early).unwrap(),
            "selectivity_deny_regime_zone_early_dir_down_price_0.50_0.75_edge_0.07_0.15_z_0.7_1.1_conf_0.50_0.70_vol_lt_0.40_rev_0_min_2_4"
        );
        assert_eq!(
            deny_many_regimes.reject_reason(&primary_up).unwrap(),
            "selectivity_deny_regime_zone_primary_dir_up_price_0.75_0.90_edge_0.07_0.15_z_0.7_1.1_conf_0.50_0.70_vol_lt_0.40_rev_0_min_2_4"
        );
        assert!(deny_many_regimes.reject_reason(&down).is_none());
    }

    #[test]
    fn degraded_execution_tightens_z_and_forces_taker_only_after_threshold() {
        let mut variant = StrategyVariant::maker_first();
        variant.zone_config.early_min_z = 0.50;
        variant.degraded_after_losses = 2;
        variant.degraded_after_drawdown_pct = 0.05;
        variant.degraded_min_z = 0.90;
        variant.degraded_max_price = 0.75;
        variant.degraded_force_taker = true;

        let healthy = variant.effective_zone_config(1, 0.10);
        assert_eq!(healthy.early_min_z, 0.50);
        assert!(variant.effective_prefer_maker(1, 0.10));

        let degraded = variant.effective_zone_config(2, 0.05);
        assert_eq!(degraded.early_min_z, 0.90);
        assert_eq!(degraded.max_price, 0.75);
        assert!(!variant.effective_prefer_maker(2, 0.05));
    }
}
