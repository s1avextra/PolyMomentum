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

use crate::backtest::strategies::StrategyVariant;
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
    /// Settlement-basis floor applied inside the final guard window.
    pub settlement_min_abs_move_usd: Vec<f64>,
    /// Final-window duration where settlement-basis guard is active.
    pub settlement_guard_minutes: Vec<f64>,
    /// Volatility-scaled settlement-basis buffer multiplier.
    pub settlement_sigma_buffer: Vec<f64>,
    /// Whether to include a maker variant for each (conf, z, edge) cell.
    pub also_maker: bool,
    /// Maximum executable spread, in binary-option price points.
    pub micro_max_spread: Vec<f64>,
    /// Minimum depth on the thinner side of the order book.
    pub micro_min_depth: Vec<f64>,
    /// Minimum microprice pressure toward the intended token.
    pub micro_min_pressure: Vec<f64>,
}

impl SweepGrid {
    #[cfg(test)]
    pub fn small_default(base: StrategyVariant) -> Self {
        let settlement_min_abs_move_usd = base.zone_config.settlement_min_abs_move_usd;
        let settlement_guard_minutes = base.zone_config.settlement_guard_minutes;
        let settlement_sigma_buffer = base.zone_config.settlement_sigma_buffer;
        let min_price = base.zone_config.min_price;
        let max_price = base.zone_config.max_price;
        let micro_max_spread = base.microstructure.max_spread;
        let micro_min_depth = base.microstructure.min_book_depth;
        let micro_min_pressure = base.microstructure.min_book_pressure;
        Self {
            base,
            conf: vec![0.30, 0.40, 0.50, 0.60],
            z: vec![0.20, 0.50, 1.00],
            edge: vec![0.00, 0.03, 0.07],
            ev_buffer: vec![-1.0, 0.05],
            min_price: vec![min_price],
            max_price: vec![max_price],
            settlement_min_abs_move_usd: vec![settlement_min_abs_move_usd],
            settlement_guard_minutes: vec![settlement_guard_minutes],
            settlement_sigma_buffer: vec![settlement_sigma_buffer],
            also_maker: true,
            micro_max_spread: vec![micro_max_spread],
            micro_min_depth: vec![micro_min_depth],
            micro_min_pressure: vec![micro_min_pressure],
        }
    }

    /// Generate every guarded strategy cell.
    /// Cartesian product can balloon — keep the grid small.
    pub fn variants(&self) -> Vec<StrategyVariant> {
        let maker_sides: Vec<bool> = if self.also_maker {
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
                * self.settlement_min_abs_move_usd.len()
                * self.settlement_guard_minutes.len()
                * self.settlement_sigma_buffer.len()
                * self.micro_max_spread.len()
                * self.micro_min_depth.len()
                * self.micro_min_pressure.len()
                * maker_sides.len(),
        );
        for &conf in &self.conf {
            for &z in &self.z {
                for &edge in &self.edge {
                    for &ev in &self.ev_buffer {
                        for &min_price in &self.min_price {
                            for &max_price in &self.max_price {
                                if min_price > max_price {
                                    continue;
                                }
                                for &floor in &self.settlement_min_abs_move_usd {
                                    for &guard in &self.settlement_guard_minutes {
                                        for &sigma in &self.settlement_sigma_buffer {
                                            for &micro_spread in &self.micro_max_spread {
                                                for &micro_depth in &self.micro_min_depth {
                                                    for &micro_pressure in &self.micro_min_pressure
                                                    {
                                                        for &maker in &maker_sides {
                                                            let cfg = ZoneConfig {
                                                                early_min_confidence: conf,
                                                                late_min_confidence: conf,
                                                                terminal_min_confidence: conf,
                                                                early_min_z: z,
                                                                primary_min_z: z,
                                                                late_min_z: z,
                                                                terminal_min_z: z,
                                                                early_min_edge: edge,
                                                                late_min_edge: edge,
                                                                terminal_min_edge: edge,
                                                                min_price,
                                                                max_price,
                                                                min_ev_buffer: ev,
                                                                settlement_min_abs_move_usd: floor,
                                                                settlement_guard_minutes: guard,
                                                                settlement_sigma_buffer: sigma,
                                                                ..ZoneConfig::default()
                                                            };
                                                            let microstructure =
                                                                MicrostructureConfig {
                                                                    max_spread: micro_spread,
                                                                    min_book_depth: micro_depth,
                                                                    min_book_pressure:
                                                                        micro_pressure,
                                                                };
                                                            let label = format!(
                                                                "c{:.2}_z{:.2}_e{:.2}_ev{:+.2}_p{:.2}-{:.2}_sf{:.0}_sg{:.1}_ss{:.2}_ms{:.2}_md{:.0}_mp{:.2}_{}",
                                                                conf,
                                                                z,
                                                                edge,
                                                                ev,
                                                                min_price,
                                                                max_price,
                                                                floor,
                                                                guard,
                                                                sigma,
                                                                micro_spread,
                                                                micro_depth,
                                                                micro_pressure,
                                                                if maker { "mk" } else { "tk" }
                                                            );
                                                            out.push(StrategyVariant {
                                                                name: label,
                                                                zone_config: cfg,
                                                                skip_dead_zone: self
                                                                    .base
                                                                    .skip_dead_zone,
                                                                min_confidence: conf,
                                                                min_edge: edge,
                                                                position_pct: self
                                                                    .base
                                                                    .position_pct,
                                                                max_per_market_usd: self
                                                                    .base
                                                                    .max_per_market_usd,
                                                                prefer_maker: maker,
                                                                maker_fill_prob: self
                                                                    .base
                                                                    .maker_fill_prob,
                                                                maker_seed: self.base.maker_seed,
                                                                use_perfect_fill: false,
                                                                default_fee_rate: self
                                                                    .base
                                                                    .default_fee_rate,
                                                                maker_fee_rate: self
                                                                    .base
                                                                    .maker_fee_rate,
                                                                microstructure,
                                                            });
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
}
