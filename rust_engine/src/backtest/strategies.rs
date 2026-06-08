//! Strategy variants for the backtest harness.
//!
//! Each variant wraps the live `decide_candle_trade` with a different
//! `ZoneConfig`. The harness loops one variant at a time over the same PMXT
//! v2 + BTC tape so per-strategy P&L is comparable.

use std::collections::BTreeMap;

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
}

impl SelectivityFilter {
    pub fn is_disabled(&self) -> bool {
        self.require_tags.is_empty() && self.deny_tags.is_empty()
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
        for (dimension, denied) in &self.deny_tags {
            if tags.get(dimension) == Some(denied) {
                return Some(format!(
                    "selectivity_deny_{}_{}",
                    clean_label(dimension),
                    clean_label(denied)
                ));
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
        parts.join("_")
    }
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

/// Tunable knobs the harness varies. The variant name is what shows up in
/// the report.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StrategyVariant {
    pub name: String,
    pub zone_config: ZoneConfig,
    pub skip_dead_zone: bool,
    pub min_confidence: f64,
    pub min_edge: f64,
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
}

impl StrategyVariant {
    pub fn risk_profile(&self) -> String {
        format!(
            "position_pct={:.4};max_per_market_usd={:.2};stress_dd_cap={:.4}{}",
            self.position_pct,
            self.max_per_market_usd,
            self.max_projected_stressed_drawdown_pct,
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
        }
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
            ..down.clone()
        };
        assert!(deny_early.reject_reason(&down).is_none());
        assert_eq!(
            deny_early.reject_reason(&early).unwrap(),
            "selectivity_deny_zone_early"
        );
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
