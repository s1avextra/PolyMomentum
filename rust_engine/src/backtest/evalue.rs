//! Anytime-valid e-process for win-rate vs per-trade break-even.
//!
//! For outcomes `i = 1..n` with break-even probability `p0_i` in (0,1) and
//! result `X_i` in {0,1}, the betting wealth for a fixed `lambda >= 0` is
//! `E_lambda = prod_i (1 + lambda * (X_i - p0_i))`. Under the composite null
//! (true win prob <= `p0_i` per trade) each factor has expectation <= 1, so
//! `E_lambda` is a supermartingale and Ville's inequality gives
//! `P(sup_t E >= 1/alpha) <= alpha` at ANY stopping time. A discrete mixture
//! over a lambda grid (mean of the per-lambda wealths) avoids tuning lambda
//! and inherits the same guarantee.

/// Promote threshold: rejects the null at alpha = 1/20 = 0.05 by Ville.
pub const PROMOTE_E: f64 = 20.0;
/// Practical futility stop; not a type-I bound.
pub const FUTILITY_E: f64 = 0.1;

const LAMBDA_STEP: f64 = 0.05;
const LAMBDA_COUNT: usize = 20;
/// Factor clamp for the p0 -> 1.0 edge; with p0 < 1 and lambda <= 1 every
/// factor is already strictly positive.
const FACTOR_FLOOR: f64 = 1e-12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EVerdict {
    Promote,
    Kill,
    Continue,
}

/// Running log-wealth per lambda; mixture is combined via log-sum-exp so long
/// win/loss streaks neither overflow nor underflow.
#[derive(Debug, Clone)]
pub struct EProcess {
    log_wealth: [f64; LAMBDA_COUNT],
    n: u64,
}

impl EProcess {
    pub fn new() -> Self {
        Self {
            log_wealth: [0.0; LAMBDA_COUNT],
            n: 0,
        }
    }

    pub fn update(&mut self, break_even: f64, won: bool) -> Result<(), String> {
        if !(break_even > 0.0 && break_even < 1.0) {
            return Err(format!("break_even {break_even} outside (0,1)"));
        }
        let x = if won { 1.0 } else { 0.0 };
        for (j, lw) in self.log_wealth.iter_mut().enumerate() {
            let lambda = (j + 1) as f64 * LAMBDA_STEP;
            let factor = (1.0 + lambda * (x - break_even)).max(FACTOR_FLOOR);
            *lw += factor.ln();
        }
        self.n += 1;
        Ok(())
    }

    /// Current mixture e-value: mean over the lambda grid of exp(log-wealth).
    pub fn e_value(&self) -> f64 {
        let max = self
            .log_wealth
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max);
        let sum: f64 = self.log_wealth.iter().map(|lw| (lw - max).exp()).sum();
        (max + (sum / LAMBDA_COUNT as f64).ln()).exp()
    }

    pub fn n(&self) -> u64 {
        self.n
    }

    pub fn verdict(&self) -> EVerdict {
        let e = self.e_value();
        if e >= PROMOTE_E {
            EVerdict::Promote
        } else if e <= FUTILITY_E {
            EVerdict::Kill
        } else {
            EVerdict::Continue
        }
    }
}

impl Default for EProcess {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Direct product-form mixture (no log space) for cross-checking the
    /// log-sum-exp implementation on short sequences.
    fn direct_mixture(outcomes: &[(f64, bool)]) -> f64 {
        let sum: f64 = (1..=LAMBDA_COUNT)
            .map(|j| {
                let lambda = j as f64 * LAMBDA_STEP;
                outcomes
                    .iter()
                    .map(|&(p0, won)| 1.0 + lambda * (if won { 1.0 } else { 0.0 } - p0))
                    .product::<f64>()
            })
            .sum();
        sum / LAMBDA_COUNT as f64
    }

    // Spec asked for 30 straight wins, but the 20-lambda mean mixture at
    // break-even 0.85 reaches only ~17.84 after 30 wins; 35 wins (~31.6)
    // clears PROMOTE_E with margin.
    #[test]
    fn straight_wins_promote() {
        let outcomes: Vec<(f64, bool)> = vec![(0.85, true); 35];
        let mut ep = EProcess::new();
        for &(p0, won) in &outcomes {
            ep.update(p0, won).unwrap();
        }
        let expected = direct_mixture(&outcomes);
        assert!((ep.e_value() - expected).abs() / expected < 1e-9);
        assert!(ep.e_value() > PROMOTE_E);
        assert_eq!(ep.verdict(), EVerdict::Promote);
        assert_eq!(ep.n(), 35);
    }

    #[test]
    fn straight_losses_kill() {
        let outcomes: Vec<(f64, bool)> = vec![(0.85, false); 10];
        let mut ep = EProcess::new();
        for &(p0, won) in &outcomes {
            ep.update(p0, won).unwrap();
        }
        let expected = direct_mixture(&outcomes);
        assert!((ep.e_value() - expected).abs() / expected < 1e-9);
        assert!(ep.e_value() < FUTILITY_E);
        assert_eq!(ep.verdict(), EVerdict::Kill);
    }

    #[test]
    fn exact_null_never_promotes() {
        // 10 blocks of 17 wins + 3 losses at p0 = 0.85: empirical win rate
        // matches break-even exactly, so wealth must not accumulate.
        let mut ep = EProcess::new();
        for _ in 0..10 {
            for _ in 0..17 {
                ep.update(0.85, true).unwrap();
            }
            for _ in 0..3 {
                ep.update(0.85, false).unwrap();
            }
        }
        assert_eq!(ep.n(), 200);
        assert!(ep.e_value() < PROMOTE_E);
        assert_ne!(ep.verdict(), EVerdict::Promote);
    }

    #[test]
    fn invalid_break_even_rejected() {
        let mut ep = EProcess::new();
        assert!(ep.update(0.0, true).is_err());
        assert!(ep.update(1.0, true).is_err());
        assert!(ep.update(1.5, false).is_err());
        assert!(ep.update(-0.2, false).is_err());
        assert!(ep.update(f64::NAN, true).is_err());
        assert_eq!(ep.n(), 0);
        assert_eq!(ep.e_value(), 1.0);
    }

    #[test]
    fn deterministic_replay_identical() {
        let outcomes: Vec<(f64, bool)> = (0..60)
            .map(|i| (0.55 + 0.005 * (i % 40) as f64, i % 3 != 0))
            .collect();
        let mut a = EProcess::new();
        let mut b = EProcess::new();
        for &(p0, won) in &outcomes {
            a.update(p0, won).unwrap();
            b.update(p0, won).unwrap();
        }
        assert_eq!(a.e_value(), b.e_value());
        assert_eq!(a.n(), b.n());
    }
}
