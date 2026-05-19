//! Order-book microstructure features for short-horizon candle entries.

#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct BookLevelView {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct MicrostructureConfig {
    pub max_spread: f64,
    pub min_book_depth: f64,
    pub min_book_pressure: f64,
}

impl Default for MicrostructureConfig {
    fn default() -> Self {
        Self {
            max_spread: 1.0,
            min_book_depth: 0.0,
            min_book_pressure: -1.0,
        }
    }
}

impl MicrostructureConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn is_active(&self) -> bool {
        self.max_spread < 1.0 || self.min_book_depth > 0.0 || self.min_book_pressure > -1.0
    }

    pub fn apply_safety_floor(
        &mut self,
        max_spread_ceiling: f64,
        min_depth_floor: f64,
        min_pressure_floor: f64,
    ) -> bool {
        let mut changed = false;
        if max_spread_ceiling.is_finite()
            && max_spread_ceiling >= 0.0
            && self.max_spread > max_spread_ceiling
        {
            self.max_spread = max_spread_ceiling;
            changed = true;
        }
        if min_depth_floor.is_finite()
            && min_depth_floor >= 0.0
            && self.min_book_depth < min_depth_floor
        {
            self.min_book_depth = min_depth_floor;
            changed = true;
        }
        if min_pressure_floor.is_finite()
            && (-1.0..=1.0).contains(&min_pressure_floor)
            && self.min_book_pressure < min_pressure_floor
        {
            self.min_book_pressure = min_pressure_floor;
            changed = true;
        }
        changed
    }
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct BookMicrostructure {
    pub best_bid: f64,
    pub best_ask: f64,
    pub spread: f64,
    pub bid_depth: f64,
    pub ask_depth: f64,
    pub imbalance: f64,
    pub microprice: f64,
    pub pressure: f64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MicrostructureSkip {
    pub reason: String,
    pub detail: String,
}

impl BookMicrostructure {
    pub fn from_levels(
        bids: &[BookLevelView],
        asks: &[BookLevelView],
        depth_levels: usize,
    ) -> Self {
        let best_bid = bids.first().map(|l| l.price).unwrap_or(0.0);
        let best_ask = asks.first().map(|l| l.price).unwrap_or(0.0);
        let bid_depth: f64 = bids
            .iter()
            .take(depth_levels)
            .map(|l| l.size.max(0.0))
            .sum();
        let ask_depth: f64 = asks
            .iter()
            .take(depth_levels)
            .map(|l| l.size.max(0.0))
            .sum();
        Self::from_top(best_bid, best_ask, bid_depth, ask_depth)
    }

    pub fn from_levels_with_top(
        best_bid: f64,
        best_ask: f64,
        bids: &[BookLevelView],
        asks: &[BookLevelView],
        depth_levels: usize,
    ) -> Self {
        if best_bid <= 0.0 || best_ask <= 0.0 || best_bid >= best_ask {
            return Self::from_levels(bids, asks, depth_levels);
        }

        let bid_depth: f64 = bids
            .iter()
            .filter(|l| l.price <= best_bid + 1e-9)
            .take(depth_levels)
            .map(|l| l.size.max(0.0))
            .sum();
        let ask_depth: f64 = asks
            .iter()
            .filter(|l| l.price >= best_ask - 1e-9)
            .take(depth_levels)
            .map(|l| l.size.max(0.0))
            .sum();
        Self::from_top(best_bid, best_ask, bid_depth, ask_depth)
    }

    pub fn from_top(best_bid: f64, best_ask: f64, bid_depth: f64, ask_depth: f64) -> Self {
        let spread = if best_bid > 0.0 && best_ask > 0.0 {
            (best_ask - best_bid).max(0.0)
        } else {
            0.0
        };
        let total_depth = bid_depth + ask_depth;
        let imbalance = if total_depth > 0.0 {
            (bid_depth - ask_depth) / total_depth
        } else {
            0.0
        };
        let microprice = if total_depth > 0.0 && best_bid > 0.0 && best_ask > 0.0 {
            (best_ask * bid_depth + best_bid * ask_depth) / total_depth
        } else if best_bid > 0.0 && best_ask > 0.0 {
            (best_bid + best_ask) / 2.0
        } else {
            0.0
        };
        let mid = if best_bid > 0.0 && best_ask > 0.0 {
            (best_bid + best_ask) / 2.0
        } else {
            0.0
        };
        let pressure = if spread > 0.0 {
            ((microprice - mid) / (spread / 2.0)).clamp(-1.0, 1.0)
        } else {
            imbalance
        };
        Self {
            best_bid,
            best_ask,
            spread,
            bid_depth,
            ask_depth,
            imbalance,
            microprice,
            pressure,
        }
    }

    pub fn check_long_entry(&self, cfg: &MicrostructureConfig) -> Result<(), MicrostructureSkip> {
        if !cfg.is_active() {
            return Ok(());
        }
        if self.best_bid <= 0.0 || self.best_ask <= 0.0 || self.best_bid >= self.best_ask {
            return Err(MicrostructureSkip {
                reason: "microstructure_invalid_book".to_string(),
                detail: format!("bid={:.4} ask={:.4}", self.best_bid, self.best_ask),
            });
        }
        if self.spread > cfg.max_spread {
            return Err(MicrostructureSkip {
                reason: "microstructure_wide_spread".to_string(),
                detail: format!("{:.4} > {:.4}", self.spread, cfg.max_spread),
            });
        }
        let min_side_depth = self.bid_depth.min(self.ask_depth);
        if min_side_depth < cfg.min_book_depth {
            return Err(MicrostructureSkip {
                reason: "microstructure_thin_book".to_string(),
                detail: format!("{:.2} < {:.2}", min_side_depth, cfg.min_book_depth),
            });
        }
        if self.pressure < cfg.min_book_pressure {
            return Err(MicrostructureSkip {
                reason: "microstructure_weak_pressure".to_string(),
                detail: format!("{:.3} < {:.3}", self.pressure, cfg.min_book_pressure),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positive_depth_imbalance_pushes_microprice_up() {
        let bids = vec![BookLevelView {
            price: 0.50,
            size: 300.0,
        }];
        let asks = vec![BookLevelView {
            price: 0.52,
            size: 100.0,
        }];
        let f = BookMicrostructure::from_levels(&bids, &asks, 3);
        assert!(f.microprice > 0.51);
        assert!(f.pressure > 0.0);
    }

    #[test]
    fn gate_rejects_weak_pressure() {
        let f = BookMicrostructure::from_top(0.50, 0.52, 100.0, 300.0);
        let cfg = MicrostructureConfig {
            min_book_pressure: 0.1,
            ..MicrostructureConfig::default()
        };
        let err = f.check_long_entry(&cfg).unwrap_err();
        assert_eq!(err.reason, "microstructure_weak_pressure");
    }

    #[test]
    fn authoritative_top_filters_stale_crossed_levels() {
        let bids = vec![
            BookLevelView {
                price: 0.74,
                size: 100.0,
            },
            BookLevelView {
                price: 0.58,
                size: 45.0,
            },
        ];
        let asks = vec![
            BookLevelView {
                price: 0.43,
                size: 100.0,
            },
            BookLevelView {
                price: 0.59,
                size: 55.0,
            },
        ];
        let f = BookMicrostructure::from_levels_with_top(0.58, 0.59, &bids, &asks, 3);

        assert_eq!(f.best_bid, 0.58);
        assert_eq!(f.best_ask, 0.59);
        assert!((f.spread - 0.01).abs() < 1e-9);
        assert_eq!(f.bid_depth, 45.0);
        assert_eq!(f.ask_depth, 55.0);
    }

    #[test]
    fn safety_floor_only_tightens_microstructure() {
        let mut cfg = MicrostructureConfig::disabled();
        assert!(cfg.apply_safety_floor(0.02, 20.0, 0.10));
        assert_eq!(cfg.max_spread, 0.02);
        assert_eq!(cfg.min_book_depth, 20.0);
        assert_eq!(cfg.min_book_pressure, 0.10);

        assert!(!cfg.apply_safety_floor(0.05, 10.0, -0.10));
        assert_eq!(cfg.max_spread, 0.02);
        assert_eq!(cfg.min_book_depth, 20.0);
        assert_eq!(cfg.min_book_pressure, 0.10);
    }
}
