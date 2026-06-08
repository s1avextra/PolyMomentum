//! Parameter grid for the harness.
//!
//! `SweepGrid` takes a baseline `StrategyVariant` plus per-parameter ranges
//! and generates the cartesian product as a list of variants. The harness
//! then runs each one against the same PMXT v2 hours and ranks them.
//!
//! Heuristic: only sweep dimensions that actually move the needle for the
//! candle strategy — confidence threshold (per zone), z-score threshold
//! (per zone), edge threshold, and the maker/taker selector. Position
//! sizing knobs are independent and can be swept after the gate
//! parameters are tuned.

use crate::backtest::strategies::{SelectivityFilter, StrategyVariant};
use crate::strategy::decision::ZoneConfig;
use crate::strategy::microstructure::MicrostructureConfig;

#[derive(Debug, Clone)]
pub struct SweepGrid {
    /// Baseline variant — every sweep variant inherits everything not
    /// overridden in the grid below.
    pub base: StrategyVariant,
    /// Confidence thresholds applied to ALL zones (early/primary/late/
    /// terminal min_confidence). Keep small (≤8 values) — cartesian product.
    pub conf: Vec<f64>,
    /// Z-score thresholds applied to all zones simultaneously.
    pub z: Vec<f64>,
    /// Edge thresholds applied to all zones simultaneously.
    pub edge: Vec<f64>,
    /// EV-buffer values (negative disables the gate).
    pub ev_buffer: Vec<f64>,
    /// Minimum executable token price.
    pub min_price: Vec<f64>,
    /// Maximum executable token price.
    pub max_price: Vec<f64>,
    /// Hard cutoff before settlement; entries at or before this remaining
    /// time are skipped.
    pub settlement_cutoff_minutes: Vec<f64>,
    /// Settlement-basis floor applied inside the final guard window.
    pub settlement_min_abs_move_usd: Vec<f64>,
    /// Final-window duration where settlement-basis guard is active.
    pub settlement_guard_minutes: Vec<f64>,
    /// Volatility-scaled settlement-basis buffer multiplier.
    pub settlement_sigma_buffer: Vec<f64>,
    /// Maximum feed-forward reversion count allowed; 0 disables the cap.
    pub max_reversion_count: Vec<u64>,
    /// Minimum feed-forward reversion count required; 0 disables the floor.
    pub min_reversion_count: Vec<u64>,
    /// Whether to include a maker variant for each (conf, z, edge) cell.
    pub also_maker: bool,
    /// Restrict the grid to maker variants only.
    pub maker_only: bool,
    /// Restrict the grid to taker variants only.
    pub taker_only: bool,
    /// Maximum executable spread, in binary-option price points.
    pub micro_max_spread: Vec<f64>,
    /// Minimum depth on the thinner side of the order book.
    pub micro_min_depth: Vec<f64>,
    /// Minimum microprice pressure toward the intended token.
    pub micro_min_pressure: Vec<f64>,
    /// Projected stressed-drawdown caps, as fractions of bankroll.
    pub max_projected_stressed_drawdown_pct: Vec<f64>,
    /// Loss-count thresholds that enable degraded execution fallback. 0 disables.
    pub degraded_after_losses: Vec<u64>,
    /// Realized drawdown thresholds for degraded execution fallback.
    pub degraded_after_drawdown_pct: Vec<f64>,
    /// Minimum z-score while degraded execution fallback is active.
    pub degraded_min_z: Vec<f64>,
    /// Maximum executable token price while degraded execution fallback is active. 0 disables.
    pub degraded_max_price: Vec<f64>,
    /// Force taker execution while degraded execution fallback is active.
    pub degraded_force_taker: bool,
    /// Causal selectivity filters to apply after decision construction.
    pub selectivity: Vec<SelectivityFilter>,
    /// Timing zone to keep enabled for each variant.
    pub zone_mode: ZoneMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZoneMode {
    All,
    Early,
    Primary,
    Late,
    Terminal,
}

impl ZoneMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "all" => Some(Self::All),
            "early" => Some(Self::Early),
            "primary" => Some(Self::Primary),
            "late" => Some(Self::Late),
            "terminal" => Some(Self::Terminal),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Early => "early",
            Self::Primary => "primary",
            Self::Late => "late",
            Self::Terminal => "terminal",
        }
    }
}

impl SweepGrid {
    #[cfg(test)]
    pub fn small_default(base: StrategyVariant) -> Self {
        let settlement_min_abs_move_usd = base.zone_config.settlement_min_abs_move_usd;
        let settlement_cutoff_minutes = base.zone_config.settlement_cutoff_minutes;
        let settlement_guard_minutes = base.zone_config.settlement_guard_minutes;
        let settlement_sigma_buffer = base.zone_config.settlement_sigma_buffer;
        let min_price = base.zone_config.min_price;
        let max_price = base.zone_config.max_price;
        let micro_max_spread = base.microstructure.max_spread;
        let micro_min_depth = base.microstructure.min_book_depth;
        let micro_min_pressure = base.microstructure.min_book_pressure;
        let max_projected_stressed_drawdown_pct = base.max_projected_stressed_drawdown_pct;
        let selectivity = base.selectivity.clone();
        Self {
            base,
            conf: vec![0.30, 0.40, 0.50, 0.60],
            z: vec![0.20, 0.50, 1.00],
            edge: vec![0.00, 0.03, 0.07],
            ev_buffer: vec![-1.0, 0.05],
            min_price: vec![min_price],
            max_price: vec![max_price],
            settlement_cutoff_minutes: vec![settlement_cutoff_minutes],
            settlement_min_abs_move_usd: vec![settlement_min_abs_move_usd],
            settlement_guard_minutes: vec![settlement_guard_minutes],
            settlement_sigma_buffer: vec![settlement_sigma_buffer],
            max_reversion_count: vec![0],
            min_reversion_count: vec![0],
            also_maker: true,
            maker_only: false,
            taker_only: false,
            micro_max_spread: vec![micro_max_spread],
            micro_min_depth: vec![micro_min_depth],
            micro_min_pressure: vec![micro_min_pressure],
            max_projected_stressed_drawdown_pct: vec![max_projected_stressed_drawdown_pct],
            degraded_after_losses: vec![0],
            degraded_after_drawdown_pct: vec![0.0],
            degraded_min_z: vec![0.0],
            degraded_max_price: vec![0.0],
            degraded_force_taker: false,
            selectivity: vec![selectivity],
            zone_mode: ZoneMode::All,
        }
    }

    /// Generate every guarded strategy cell.
    /// Cartesian product can balloon — keep the grid small.
    pub fn variants(&self) -> Vec<StrategyVariant> {
        let maker_sides: Vec<bool> = if self.maker_only {
            vec![true]
        } else if self.taker_only {
            vec![false]
        } else if self.also_maker {
            vec![false, true]
        } else {
            vec![false]
        };
        let mut out = Vec::with_capacity(
            self.conf.len()
                * self.z.len()
                * self.edge.len()
                * self.ev_buffer.len()
                * self.min_price.len()
                * self.max_price.len()
                * self.settlement_cutoff_minutes.len()
                * self.settlement_min_abs_move_usd.len()
                * self.settlement_guard_minutes.len()
                * self.settlement_sigma_buffer.len()
                * self.max_reversion_count.len()
                * self.min_reversion_count.len()
                * self.micro_max_spread.len()
                * self.micro_min_depth.len()
                * self.micro_min_pressure.len()
                * self.max_projected_stressed_drawdown_pct.len()
                * self.degraded_after_losses.len()
                * self.degraded_after_drawdown_pct.len()
                * self.degraded_min_z.len()
                * self.degraded_max_price.len()
                * self.selectivity.len().max(1)
                * maker_sides.len(),
        );
        let selectivity_filters = if self.selectivity.is_empty() {
            vec![SelectivityFilter::default()]
        } else {
            self.selectivity.clone()
        };
        for &conf in &self.conf {
            for &z in &self.z {
                for &edge in &self.edge {
                    for &ev in &self.ev_buffer {
                        for &min_price in &self.min_price {
                            for &max_price in &self.max_price {
                                if min_price > max_price {
                                    continue;
                                }
                                for &cutoff in &self.settlement_cutoff_minutes {
                                    for &floor in &self.settlement_min_abs_move_usd {
                                        for &guard in &self.settlement_guard_minutes {
                                            for &sigma in &self.settlement_sigma_buffer {
                                                for &max_reversion_count in
                                                    &self.max_reversion_count
                                                {
                                                    for &min_reversion_count in
                                                        &self.min_reversion_count
                                                    {
                                                        for &micro_spread in &self.micro_max_spread
                                                        {
                                                            for &micro_depth in
                                                                &self.micro_min_depth
                                                            {
                                                                for &micro_pressure in
                                                                    &self.micro_min_pressure
                                                                {
                                                                    for &stress_dd_cap in &self
                                                                .max_projected_stressed_drawdown_pct
                                                            {
                                                                for &degraded_after_losses in
                                                                    &self.degraded_after_losses
                                                                {
                                                                    for &degraded_drawdown in &self
                                                                        .degraded_after_drawdown_pct
                                                                    {
                                                                        for &degraded_min_z in
                                                                            &self.degraded_min_z
                                                                        {
                                                                            for &degraded_max_price in
                                                                            &self.degraded_max_price
                                                                        {
                                                                            for &maker in
                                                                                &maker_sides
                                                                            {
                                                                                for selectivity in
                                                                                    &selectivity_filters
                                                                                {
                                                                                    self.push_variant(
                                                                                        &mut out,
                                                                                        SweepCell {
                                                                                            conf,
                                                                                            z,
                                                                                            edge,
                                                                                            ev,
                                                                                            min_price,
                                                                                            max_price,
                                                                                            cutoff,
                                                                                            floor,
                                                                                            guard,
                                                                                            sigma,
                                                                                            min_reversion_count,
                                                                                            max_reversion_count,
                                                                                            micro_spread,
                                                                                            micro_depth,
                                                                                            micro_pressure,
                                                                                            stress_dd_cap,
                                                                                            degraded_after_losses,
                                                                                            degraded_drawdown,
                                                                                            degraded_min_z,
                                                                                            degraded_max_price,
                                                                                            maker,
                                                                                        },
                                                                                        selectivity.clone(),
                                                                                    );
                                                                                }
                                                                            }
                                                                        }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        out
    }

    fn push_variant(
        &self,
        out: &mut Vec<StrategyVariant>,
        cell: SweepCell,
        selectivity: SelectivityFilter,
    ) {
        let mut cfg = ZoneConfig {
            early_min_confidence: cell.conf,
            late_min_confidence: cell.conf,
            terminal_min_confidence: cell.conf,
            early_min_z: cell.z,
            primary_min_z: cell.z,
            late_min_z: cell.z,
            terminal_min_z: cell.z,
            early_min_edge: cell.edge,
            late_min_edge: cell.edge,
            terminal_min_edge: cell.edge,
            min_price: cell.min_price,
            max_price: cell.max_price,
            min_ev_buffer: cell.ev,
            settlement_cutoff_minutes: cell.cutoff,
            settlement_min_abs_move_usd: cell.floor,
            settlement_guard_minutes: cell.guard,
            settlement_sigma_buffer: cell.sigma,
            min_reversion_count: cell.min_reversion_count,
            max_reversion_count: if cell.max_reversion_count == 0 {
                ZoneConfig::default().max_reversion_count
            } else {
                cell.max_reversion_count
            },
            ..ZoneConfig::default()
        };
        apply_zone_mode(&mut cfg, self.zone_mode);

        let stress_dd_suffix =
            if (cell.stress_dd_cap - self.base.max_projected_stressed_drawdown_pct).abs() > 1e-9 {
                format!("_dd{:.2}", cell.stress_dd_cap)
            } else {
                String::new()
            };
        let degraded_suffix = if cell.degraded_after_losses > 0 {
            let price_suffix = if cell.degraded_max_price > 0.0 {
                format!("p{:.2}", cell.degraded_max_price)
            } else {
                String::new()
            };
            format!(
                "_fbL{}d{:.2}z{:.2}{price_suffix}{}",
                cell.degraded_after_losses,
                cell.degraded_drawdown,
                cell.degraded_min_z,
                if self.degraded_force_taker { "tk" } else { "" }
            )
        } else {
            String::new()
        };

        let cutoff_suffix =
            if (cell.cutoff - ZoneConfig::default().settlement_cutoff_minutes).abs() > 1e-9 {
                format!("_sc{:.1}", cell.cutoff)
            } else {
                String::new()
            };
        let reversion_suffix = if cell.max_reversion_count > 0 {
            format!("_rv{}", cell.max_reversion_count)
        } else {
            String::new()
        };
        let min_reversion_suffix = if cell.min_reversion_count > 0 {
            format!("_minrv{}", cell.min_reversion_count)
        } else {
            String::new()
        };
        let selectivity_suffix = if selectivity.is_disabled() {
            String::new()
        } else {
            format!("_sel{}", selectivity.label())
        };

        let label = format!(
            "{}_c{:.2}_z{:.2}_e{:.2}_ev{:+.2}_p{:.2}-{:.2}{}{}{}_sf{:.0}_sg{:.1}_ss{:.2}_ms{:.2}_md{:.0}_mp{:.2}{}{}{}_{}",
            self.zone_mode.as_str(),
            cell.conf,
            cell.z,
            cell.edge,
            cell.ev,
            cell.min_price,
            cell.max_price,
            cutoff_suffix,
            min_reversion_suffix,
            reversion_suffix,
            cell.floor,
            cell.guard,
            cell.sigma,
            cell.micro_spread,
            cell.micro_depth,
            cell.micro_pressure,
            stress_dd_suffix,
            degraded_suffix,
            selectivity_suffix,
            if cell.maker { "mk" } else { "tk" }
        );
        out.push(StrategyVariant {
            name: label,
            zone_config: cfg,
            skip_dead_zone: self.base.skip_dead_zone,
            min_confidence: cell.conf,
            min_edge: cell.edge,
            position_pct: self.base.position_pct,
            max_per_market_usd: self.base.max_per_market_usd,
            max_projected_stressed_drawdown_pct: cell.stress_dd_cap,
            degraded_after_losses: cell.degraded_after_losses,
            degraded_after_drawdown_pct: cell.degraded_drawdown,
            degraded_min_z: cell.degraded_min_z,
            degraded_max_price: cell.degraded_max_price,
            degraded_force_taker: self.degraded_force_taker,
            prefer_maker: cell.maker,
            maker_fill_prob: self.base.maker_fill_prob,
            maker_seed: self.base.maker_seed,
            use_perfect_fill: false,
            default_fee_rate: self.base.default_fee_rate,
            maker_fee_rate: self.base.maker_fee_rate,
            microstructure: MicrostructureConfig {
                max_spread: cell.micro_spread,
                min_book_depth: cell.micro_depth,
                min_book_pressure: cell.micro_pressure,
            },
            selectivity,
        });
    }
}

#[derive(Debug, Clone, Copy)]
struct SweepCell {
    conf: f64,
    z: f64,
    edge: f64,
    ev: f64,
    min_price: f64,
    max_price: f64,
    cutoff: f64,
    floor: f64,
    guard: f64,
    sigma: f64,
    min_reversion_count: u64,
    max_reversion_count: u64,
    micro_spread: f64,
    micro_depth: f64,
    micro_pressure: f64,
    stress_dd_cap: f64,
    degraded_after_losses: u64,
    degraded_drawdown: f64,
    degraded_min_z: f64,
    degraded_max_price: f64,
    maker: bool,
}

fn apply_zone_mode(cfg: &mut ZoneConfig, mode: ZoneMode) {
    const DISABLED_CONFIDENCE: f64 = 1.1;
    const DISABLED_Z: f64 = 100.0;
    const DISABLED_EDGE: f64 = 1.0;

    match mode {
        ZoneMode::All => {}
        ZoneMode::Early => {
            cfg.primary_min_z = DISABLED_Z;
            cfg.late_min_confidence = DISABLED_CONFIDENCE;
            cfg.late_min_z = DISABLED_Z;
            cfg.late_min_edge = DISABLED_EDGE;
            cfg.terminal_min_confidence = DISABLED_CONFIDENCE;
            cfg.terminal_min_z = DISABLED_Z;
            cfg.terminal_min_edge = DISABLED_EDGE;
        }
        ZoneMode::Primary => {
            cfg.early_min_confidence = DISABLED_CONFIDENCE;
            cfg.early_min_z = DISABLED_Z;
            cfg.early_min_edge = DISABLED_EDGE;
            cfg.late_min_confidence = DISABLED_CONFIDENCE;
            cfg.late_min_z = DISABLED_Z;
            cfg.late_min_edge = DISABLED_EDGE;
            cfg.terminal_min_confidence = DISABLED_CONFIDENCE;
            cfg.terminal_min_z = DISABLED_Z;
            cfg.terminal_min_edge = DISABLED_EDGE;
        }
        ZoneMode::Late => {
            cfg.early_min_confidence = DISABLED_CONFIDENCE;
            cfg.early_min_z = DISABLED_Z;
            cfg.early_min_edge = DISABLED_EDGE;
            cfg.primary_min_z = DISABLED_Z;
            cfg.terminal_min_confidence = DISABLED_CONFIDENCE;
            cfg.terminal_min_z = DISABLED_Z;
            cfg.terminal_min_edge = DISABLED_EDGE;
        }
        ZoneMode::Terminal => {
            cfg.early_min_confidence = DISABLED_CONFIDENCE;
            cfg.early_min_z = DISABLED_Z;
            cfg.early_min_edge = DISABLED_EDGE;
            cfg.primary_min_z = DISABLED_Z;
            cfg.late_min_confidence = DISABLED_CONFIDENCE;
            cfg.late_min_z = DISABLED_Z;
            cfg.late_min_edge = DISABLED_EDGE;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_grid_has_expected_size() {
        let base = StrategyVariant::baseline();
        let grid = SweepGrid::small_default(base);
        let variants = grid.variants();
        // 4 conf × 3 z × 3 edge × 2 ev × 1 floor × 1 guard × 1 sigma × 2 maker = 144
        assert_eq!(variants.len(), 4 * 3 * 3 * 2 * 2);
    }

    #[test]
    fn variants_have_unique_names() {
        let base = StrategyVariant::baseline();
        let grid = SweepGrid::small_default(base);
        let variants = grid.variants();
        let names: std::collections::HashSet<&str> =
            variants.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names.len(), variants.len());
    }

    #[test]
    fn selectivity_filter_is_carried_into_variants() {
        let base = StrategyVariant::baseline();
        let mut grid = SweepGrid::small_default(base);
        let mut filter = SelectivityFilter::default();
        filter
            .require_tags
            .insert("direction".to_string(), "down".to_string());
        grid.selectivity = vec![filter];

        let variants = grid.variants();

        assert!(variants.iter().all(|v| {
            v.selectivity
                .require_tags
                .get("direction")
                .is_some_and(|value| value == "down")
        }));
        assert!(variants
            .iter()
            .all(|v| v.name.contains("_selreqdirection-down_")));
    }

    #[test]
    fn maker_only_emits_no_taker_variants() {
        let base = StrategyVariant::baseline();
        let mut grid = SweepGrid::small_default(base);
        grid.maker_only = true;

        let variants = grid.variants();

        assert_eq!(variants.len(), 4 * 3 * 3 * 2);
        assert!(variants.iter().all(|v| v.prefer_maker));
        assert!(variants.iter().all(|v| v.name.ends_with("_mk")));
    }

    #[test]
    fn taker_only_emits_no_maker_variants() {
        let base = StrategyVariant::baseline();
        let mut grid = SweepGrid::small_default(base);
        grid.taker_only = true;

        let variants = grid.variants();

        assert_eq!(variants.len(), 4 * 3 * 3 * 2);
        assert!(variants.iter().all(|v| !v.prefer_maker));
        assert!(variants.iter().all(|v| v.name.ends_with("_tk")));
    }

    #[test]
    fn microstructure_dimensions_expand_grid() {
        let base = StrategyVariant::baseline();
        let mut grid = SweepGrid::small_default(base);
        grid.micro_max_spread = vec![0.02, 0.03];
        grid.micro_min_pressure = vec![0.0, 0.1];
        let variants = grid.variants();
        assert_eq!(variants.len(), 4 * 3 * 3 * 2 * 2 * 2 * 2);
        assert!(variants.iter().any(|v| v.microstructure.max_spread == 0.02));
        assert!(variants
            .iter()
            .any(|v| v.microstructure.min_book_pressure == 0.1));
    }

    #[test]
    fn stressed_drawdown_cap_expands_grid_and_labels_non_base_caps() {
        let mut base = StrategyVariant::baseline();
        base.max_projected_stressed_drawdown_pct = 0.24;
        let mut grid = SweepGrid::small_default(base);
        grid.max_projected_stressed_drawdown_pct = vec![0.12, 0.24];
        let variants = grid.variants();

        assert_eq!(variants.len(), 4 * 3 * 3 * 2 * 2 * 2);
        assert!(variants
            .iter()
            .any(|v| v.max_projected_stressed_drawdown_pct == 0.12 && v.name.contains("_dd0.12_")));
        assert!(
            variants
                .iter()
                .any(|v| v.max_projected_stressed_drawdown_pct == 0.24
                    && !v.name.contains("_dd0.24_"))
        );
    }

    #[test]
    fn degraded_execution_dimensions_expand_grid() {
        let base = StrategyVariant::baseline();
        let mut grid = SweepGrid::small_default(base);
        grid.degraded_after_losses = vec![1, 2];
        grid.degraded_min_z = vec![0.90];
        grid.degraded_max_price = vec![0.75];
        grid.degraded_force_taker = true;
        let variants = grid.variants();

        assert_eq!(variants.len(), 4 * 3 * 3 * 2 * 2 * 2);
        assert!(variants.iter().any(|v| v.degraded_after_losses == 1
            && v.degraded_force_taker
            && v.degraded_max_price == 0.75
            && v.name.contains("_fbL1d0.00z0.90p0.75tk_")));
    }

    #[test]
    fn price_dimensions_apply_to_zone_config() {
        let base = StrategyVariant::baseline();
        let mut grid = SweepGrid::small_default(base);
        grid.min_price = vec![0.12];
        grid.max_price = vec![0.75];
        let variants = grid.variants();

        assert!(variants.iter().all(|v| v.zone_config.min_price == 0.12));
        assert!(variants.iter().all(|v| v.zone_config.max_price == 0.75));
        assert!(variants.iter().any(|v| v.name.contains("_p0.12-0.75_")));
    }

    #[test]
    fn settlement_cutoff_dimension_applies_to_zone_config() {
        let base = StrategyVariant::baseline();
        let mut grid = SweepGrid::small_default(base);
        grid.settlement_cutoff_minutes = vec![0.30, 2.0];
        let variants = grid.variants();

        assert_eq!(variants.len(), 4 * 3 * 3 * 2 * 2 * 2);
        assert!(variants
            .iter()
            .any(
                |v| (v.zone_config.settlement_cutoff_minutes - 2.0).abs() < 1e-9
                    && v.name.contains("_sc2.0_")
            ));
    }

    #[test]
    fn max_reversion_count_dimension_applies_to_zone_config() {
        let base = StrategyVariant::baseline();
        let mut grid = SweepGrid::small_default(base);
        grid.max_reversion_count = vec![0, 2];
        let variants = grid.variants();

        assert_eq!(variants.len(), 4 * 3 * 3 * 2 * 2 * 2);
        assert!(variants
            .iter()
            .any(|v| v.zone_config.max_reversion_count == 2 && v.name.contains("_rv2_")));
        assert!(variants
            .iter()
            .any(|v| v.zone_config.max_reversion_count == u64::MAX));
    }

    #[test]
    fn min_reversion_count_dimension_applies_to_zone_config() {
        let base = StrategyVariant::baseline();
        let mut grid = SweepGrid::small_default(base);
        grid.min_reversion_count = vec![0, 1];
        let variants = grid.variants();

        assert_eq!(variants.len(), 4 * 3 * 3 * 2 * 2 * 2);
        assert!(variants
            .iter()
            .any(|v| v.zone_config.min_reversion_count == 1 && v.name.contains("_minrv1_")));
        assert!(variants
            .iter()
            .any(|v| v.zone_config.min_reversion_count == 0 && !v.name.contains("_minrv0_")));
    }

    #[test]
    fn zone_mode_disables_unselected_zones() {
        let base = StrategyVariant::baseline();
        let mut grid = SweepGrid::small_default(base);
        grid.zone_mode = ZoneMode::Primary;
        let variants = grid.variants();
        let first = &variants[0];
        assert!(first.name.starts_with("primary_"));
        assert!(first.zone_config.primary_min_z < 100.0);
        assert!(first.zone_config.early_min_confidence > 1.0);
        assert!(first.zone_config.late_min_confidence > 1.0);
        assert!(first.zone_config.terminal_min_confidence > 1.0);
    }
}
