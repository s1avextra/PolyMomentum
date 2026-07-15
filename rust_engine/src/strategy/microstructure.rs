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
    #[serde(
        default = "default_recent_mid_lookback_seconds",
        skip_serializing_if = "is_default_recent_mid_lookback_seconds"
    )]
    pub recent_mid_lookback_seconds: f64,
    #[serde(
        default = "default_max_recent_mid_runup",
        skip_serializing_if = "is_default_max_recent_mid_runup"
    )]
    pub max_recent_mid_runup: f64,
}

fn default_recent_mid_lookback_seconds() -> f64 {
    15.0
}

fn is_default_recent_mid_lookback_seconds(value: &f64) -> bool {
    (*value - default_recent_mid_lookback_seconds()).abs() <= f64::EPSILON
}

fn default_max_recent_mid_runup() -> f64 {
    1.0
}

fn is_default_max_recent_mid_runup(value: &f64) -> bool {
    (*value - default_max_recent_mid_runup()).abs() <= f64::EPSILON
}

impl Default for MicrostructureConfig {
    fn default() -> Self {
        Self {
            max_spread: 1.0,
            min_book_depth: 0.0,
            min_book_pressure: -1.0,
            recent_mid_lookback_seconds: default_recent_mid_lookback_seconds(),
            max_recent_mid_runup: default_max_recent_mid_runup(),
        }
    }
}

impl MicrostructureConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn is_active(&self) -> bool {
        self.max_spread < 1.0
            || self.min_book_depth > 0.0
            || self.min_book_pressure > -1.0
            || self.is_path_active()
    }

    pub fn is_path_active(&self) -> bool {
        self.recent_mid_lookback_seconds.is_finite()
            && self.recent_mid_lookback_seconds > 0.0
            && self.max_recent_mid_runup.is_finite()
            && (0.0..1.0).contains(&self.max_recent_mid_runup)
    }

    pub fn check_recent_mid_path(
        &self,
        recent_mid_runup: Option<f64>,
    ) -> Result<(), MicrostructureSkip> {
        if !self.is_path_active() {
            return Ok(());
        }
        let Some(runup) = recent_mid_runup.filter(|value| value.is_finite() && *value >= 0.0)
        else {
            return Err(MicrostructureSkip {
                reason: "microstructure_path_unavailable".to_string(),
                detail: format!(
                    "need {:.1}s of causal midpoint history",
                    self.recent_mid_lookback_seconds
                ),
            });
        };
        if runup > self.max_recent_mid_runup {
            return Err(MicrostructureSkip {
                reason: "microstructure_recent_runup".to_string(),
                detail: format!("{runup:.4} > {:.4}", self.max_recent_mid_runup),
            });
        }
        Ok(())
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

pub fn recent_mid_runup<'a, H>(history: H, now_ts: f64, lookback_seconds: f64) -> Option<f64>
where
    H: IntoIterator<Item = &'a (f64, f64)>,
    H::IntoIter: DoubleEndedIterator,
{
    if !now_ts.is_finite() || !lookback_seconds.is_finite() || lookback_seconds <= 0.0 {
        return None;
    }
    let cutoff = now_ts - lookback_seconds;
    let mut first_ts: Option<f64> = None;
    let mut latest: Option<(f64, f64)> = None;
    let mut min_mid = f64::INFINITY;
    for &(ts, mid) in history.into_iter().rev() {
        if ts < cutoff {
            break;
        }
        if !ts.is_finite() || !mid.is_finite() || mid <= 0.0 || ts > now_ts {
            continue;
        }
        first_ts = Some(ts);
        if latest.is_none() {
            latest = Some((ts, mid));
        }
        min_mid = min_mid.min(mid);
    }
    let first_ts = first_ts?;
    let (latest_ts, current_mid) = latest?;
    if latest_ts - first_ts < lookback_seconds * 0.80 {
        return None;
    }
    Some((current_mid - min_mid).max(0.0))
}

/// Causal change in binary-outcome log odds over a recent midpoint window.
/// The latest point must be no more than two seconds old and the retained
/// history must cover at least 80% of the requested horizon. Values outside
/// the open probability interval fail closed instead of being clamped.
pub fn recent_mid_logit_change<'a, H>(history: H, now_ts: f64, lookback_seconds: f64) -> Option<f64>
where
    H: IntoIterator<Item = &'a (f64, f64)>,
    H::IntoIter: DoubleEndedIterator,
{
    if !now_ts.is_finite() || !lookback_seconds.is_finite() || lookback_seconds <= 0.0 {
        return None;
    }
    let cutoff = now_ts - lookback_seconds;
    let mut earliest: Option<(f64, f64)> = None;
    let mut latest: Option<(f64, f64)> = None;
    for &(ts, mid) in history.into_iter().rev() {
        if ts < cutoff {
            break;
        }
        if !ts.is_finite() || !mid.is_finite() || !(0.0..1.0).contains(&mid) || ts > now_ts {
            continue;
        }
        if latest.is_none() {
            latest = Some((ts, mid));
        }
        earliest = Some((ts, mid));
    }
    let (earliest_ts, earliest_mid) = earliest?;
    let (latest_ts, latest_mid) = latest?;
    if now_ts - latest_ts > 2.0 || latest_ts - earliest_ts < lookback_seconds * 0.80 {
        return None;
    }
    let logit = |probability: f64| (probability / (1.0 - probability)).ln();
    Some(logit(latest_mid) - logit(earliest_mid))
}

pub fn bookwalk_buy_slippage(asks: &[BookLevelView], size: f64, _tick_size: f64) -> Option<f64> {
    if size <= 0.0 || !size.is_finite() || asks.is_empty() {
        return None;
    }
    let touch = asks.first()?.price;
    if touch <= 0.0 || !touch.is_finite() {
        return None;
    }

    let mut remaining = size;
    let mut total_cost = 0.0;
    for level in asks {
        if remaining <= 0.0 {
            break;
        }
        if level.price <= 0.0
            || level.size <= 0.0
            || !level.price.is_finite()
            || !level.size.is_finite()
        {
            continue;
        }
        let take = remaining.min(level.size);
        total_cost += take * level.price;
        remaining -= take;
    }
    if remaining > 1e-9 {
        return None;
    }

    let vwap = total_cost / size;
    Some((vwap - touch).max(0.0))
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

/// Paired-book consistency for fully collateralized binary outcome tokens.
/// A valid Yes/No pair has complementary probabilities, so both the midpoint
/// and depth-weighted microprice sums should remain close to one.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BinaryComplementMicrostructure {
    pub chosen_mid: f64,
    pub opposite_mid: f64,
    pub chosen_microprice: f64,
    pub opposite_microprice: f64,
    pub mid_sum_residual: f64,
    pub microprice_sum_residual: f64,
}

pub fn binary_complement_microstructure(
    chosen: &BookMicrostructure,
    opposite: &BookMicrostructure,
) -> Option<BinaryComplementMicrostructure> {
    let valid = |book: &BookMicrostructure| {
        book.best_bid.is_finite()
            && book.best_ask.is_finite()
            && book.microprice.is_finite()
            && book.best_bid > 0.0
            && book.best_bid < book.best_ask
            && book.best_ask < 1.0
            && book.bid_depth > 0.0
            && book.ask_depth > 0.0
            && book.microprice >= book.best_bid
            && book.microprice <= book.best_ask
    };
    if !valid(chosen) || !valid(opposite) {
        return None;
    }
    let chosen_mid = (chosen.best_bid + chosen.best_ask) / 2.0;
    let opposite_mid = (opposite.best_bid + opposite.best_ask) / 2.0;
    Some(BinaryComplementMicrostructure {
        chosen_mid,
        opposite_mid,
        chosen_microprice: chosen.microprice,
        opposite_microprice: opposite.microprice,
        mid_sum_residual: chosen_mid + opposite_mid - 1.0,
        microprice_sum_residual: chosen.microprice + opposite.microprice - 1.0,
    })
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

    #[test]
    fn recent_mid_runup_separates_spike_chase_from_pullback() {
        let spike = vec![(100.0, 0.70), (105.0, 0.74), (110.0, 0.82), (115.0, 0.84)];
        let pullback = vec![(100.0, 0.92), (105.0, 0.86), (110.0, 0.82), (115.0, 0.84)];

        assert!((recent_mid_runup(&spike, 115.0, 15.0).unwrap() - 0.14).abs() < 1e-9);
        assert!((recent_mid_runup(&pullback, 115.0, 15.0).unwrap() - 0.02).abs() < 1e-9);

        let cfg = MicrostructureConfig {
            recent_mid_lookback_seconds: 15.0,
            max_recent_mid_runup: 0.08,
            ..MicrostructureConfig::default()
        };
        let err = cfg
            .check_recent_mid_path(recent_mid_runup(&spike, 115.0, 15.0))
            .unwrap_err();
        assert_eq!(err.reason, "microstructure_recent_runup");
        assert!(cfg
            .check_recent_mid_path(recent_mid_runup(&pullback, 115.0, 15.0))
            .is_ok());
    }

    #[test]
    fn recent_mid_logit_change_is_causal_and_requires_coverage() {
        let history = vec![(100.0, 0.40), (104.0, 0.45), (105.0, 0.50), (106.0, 0.90)];
        let change = recent_mid_logit_change(&history, 105.0, 5.0).unwrap();
        let expected = (0.50_f64 / 0.50).ln() - (0.40_f64 / 0.60).ln();
        assert!((change - expected).abs() < 1e-9);

        let sparse = vec![(102.0, 0.40), (105.0, 0.50)];
        assert_eq!(recent_mid_logit_change(&sparse, 105.0, 5.0), None);

        let stale = vec![(100.0, 0.40), (102.0, 0.50)];
        assert_eq!(recent_mid_logit_change(&stale, 105.0, 5.0), None);
    }

    #[test]
    fn binary_complement_microstructure_uses_both_depth_weighted_books() {
        let chosen = BookMicrostructure::from_top(0.58, 0.60, 80.0, 20.0);
        let opposite = BookMicrostructure::from_top(0.40, 0.42, 20.0, 80.0);
        let paired = binary_complement_microstructure(&chosen, &opposite).unwrap();

        assert!((paired.chosen_mid - 0.59).abs() < 1e-12);
        assert!((paired.opposite_mid - 0.41).abs() < 1e-12);
        assert!(paired.mid_sum_residual.abs() < 1e-12);
        assert!(paired.microprice_sum_residual.abs() < 1e-12);

        assert!(
            binary_complement_microstructure(&chosen, &BookMicrostructure::default()).is_none()
        );
    }

    #[test]
    fn active_recent_path_guard_fails_closed_without_history() {
        let cfg = MicrostructureConfig {
            recent_mid_lookback_seconds: 15.0,
            max_recent_mid_runup: 0.08,
            ..MicrostructureConfig::default()
        };
        let err = cfg.check_recent_mid_path(None).unwrap_err();
        assert_eq!(err.reason, "microstructure_path_unavailable");
    }

    #[test]
    fn bookwalk_slippage_uses_visible_ask_depth() {
        let asks = vec![
            BookLevelView {
                price: 0.80,
                size: 5.0,
            },
            BookLevelView {
                price: 0.82,
                size: 10.0,
            },
        ];
        let slippage = bookwalk_buy_slippage(&asks, 10.0, 0.01).unwrap();
        assert!((slippage - 0.01).abs() < 1e-9);
        assert_eq!(bookwalk_buy_slippage(&asks, 20.0, 0.01), None);
    }
}
