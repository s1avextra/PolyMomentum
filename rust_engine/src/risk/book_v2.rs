//! RiskBook v2 — event-sourced postings ledger.
//!
//! The v1 book keeps five parallel stored-and-mutated PnL representations
//! that agree only by convention; every live accounting incident traced to
//! one of them. v2 stores exactly one thing — an append-only journal of
//! cash postings — and DERIVES every balance, so the double-count class is
//! impossible by construction. Cash semantics mirror the wallet: a fill
//! posts -(cost+fee), a settlement posts +payout, so book equity tracks the
//! on-chain balance up to auto-redeem lag.
//!
//! Phase discipline (docs/risk_book_v2/): v2 runs in SHADOW next to the
//! driving v1 book until N sessions of byte-level agreement, then cutover
//! via the RISK_BOOK env flag. This module never mutates v1 state.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use tokio::sync::Mutex;

pub const BAND_VENUE_MIN_STAKE: f64 = 5.0;
/// Sub-book that emulates the Kelly-lower sizing policy on the SAME trades
/// with its own compounding equity, so the operator can compare both curves
/// before switching.
pub const KELLY_SIM_STRATEGY: &str = "band_kelly_sim";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostingKind {
    /// One-shot opening balance (import from v1 or a wallet reading).
    Import,
    /// Cash out on an entry fill: -(cost + fee).
    Fill,
    /// Cash in on resolution: +payout (qty for a win, 0 for a loss).
    Settlement,
    /// Manual operator correction, always with a note.
    Adjustment,
}

impl PostingKind {
    fn as_str(self) -> &'static str {
        match self {
            PostingKind::Import => "import",
            PostingKind::Fill => "fill",
            PostingKind::Settlement => "settlement",
            PostingKind::Adjustment => "adjustment",
        }
    }
}

pub struct BookV2 {
    db: Arc<Mutex<Connection>>,
}

impl BookV2 {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path.as_ref()).context("open book_v2 db")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS v2_postings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                kind TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                cid TEXT NOT NULL DEFAULT '',
                amount_usd REAL NOT NULL,
                qty REAL NOT NULL DEFAULT 0,
                price REAL NOT NULL DEFAULT 0,
                note TEXT NOT NULL DEFAULT '',
                idempotency_key TEXT NOT NULL UNIQUE
            );
            CREATE INDEX IF NOT EXISTS v2_postings_cid ON v2_postings(cid);
            CREATE INDEX IF NOT EXISTS v2_postings_strategy
                ON v2_postings(strategy_id);",
        )
        .context("create book_v2 schema")?;
        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
        })
    }

    /// Append a posting. Idempotent: a duplicate key is a silent no-op, so
    /// replayed settlements/fills can never double-post.
    #[allow(clippy::too_many_arguments)]
    pub async fn post(
        &self,
        ts: f64,
        kind: PostingKind,
        strategy_id: &str,
        cid: &str,
        amount_usd: f64,
        qty: f64,
        price: f64,
        note: &str,
        idempotency_key: &str,
    ) -> Result<bool> {
        // Defensive unit normalization: some call sites carry venue
        // millisecond timestamps.
        let ts = if ts > 1.0e11 { ts / 1000.0 } else { ts };
        let db = self.db.lock().await;
        let inserted = db
            .execute(
                "INSERT OR IGNORE INTO v2_postings
                 (ts, kind, strategy_id, cid, amount_usd, qty, price, note, idempotency_key)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    ts,
                    kind.as_str(),
                    strategy_id,
                    cid,
                    amount_usd,
                    qty,
                    price,
                    note,
                    idempotency_key
                ],
            )
            .context("insert posting")?;
        Ok(inserted > 0)
    }

    /// Derived: total cash = sum of every posting. Tracks the wallet up to
    /// auto-redeem lag on settled-but-unredeemed wins.
    pub async fn equity(&self) -> Result<f64> {
        let db = self.db.lock().await;
        let v: f64 = db
            .query_row(
                "SELECT COALESCE(SUM(amount_usd), 0.0) FROM v2_postings",
                [],
                |r| r.get(0),
            )
            .context("sum postings")?;
        Ok(v)
    }

    /// Derived: lifetime realized PnL = everything except the opening
    /// imports and manual adjustments' capital component.
    pub async fn realized_pnl(&self) -> Result<f64> {
        let db = self.db.lock().await;
        let v: f64 = db
            .query_row(
                "SELECT COALESCE(SUM(amount_usd), 0.0) FROM v2_postings
                 WHERE kind IN ('fill', 'settlement')",
                [],
                |r| r.get(0),
            )
            .context("sum trade postings")?;
        Ok(v)
    }

    /// Derived: cost of positions with a fill but no settlement yet.
    pub async fn open_cost(&self) -> Result<f64> {
        let db = self.db.lock().await;
        let v: f64 = db
            .query_row(
                "SELECT COALESCE(-SUM(f.amount_usd), 0.0)
                 FROM v2_postings f
                 WHERE f.kind = 'fill'
                   AND NOT EXISTS (
                     SELECT 1 FROM v2_postings s
                     WHERE s.kind = 'settlement' AND s.cid = f.cid
                   )",
                [],
                |r| r.get(0),
            )
            .context("sum open cost")?;
        Ok(v)
    }

    /// Equity of one strategy's sub-book (postings filtered by strategy).
    pub async fn equity_for(&self, strategy_id: &str) -> Result<f64> {
        let db = self.db.lock().await;
        let v: f64 = db
            .query_row(
                "SELECT COALESCE(SUM(amount_usd), 0.0) FROM v2_postings
                 WHERE strategy_id = ?1",
                params![strategy_id],
                |r| r.get(0),
            )
            .context("sum strategy postings")?;
        Ok(v)
    }

    /// Filled qty for a cid within one strategy's sub-book (None = no fill).
    pub async fn fill_qty(&self, strategy_id: &str, cid: &str) -> Result<Option<f64>> {
        let db = self.db.lock().await;
        let v: Option<f64> = db
            .query_row(
                "SELECT SUM(qty) FROM v2_postings
                 WHERE strategy_id = ?1 AND cid = ?2 AND kind = 'fill'",
                params![strategy_id, cid],
                |r| r.get(0),
            )
            .context("fill qty")?;
        Ok(v.filter(|q| *q > 0.0))
    }

    pub async fn is_empty(&self) -> Result<bool> {
        let db = self.db.lock().await;
        let n: i64 = db
            .query_row("SELECT COUNT(*) FROM v2_postings", [], |r| r.get(0))
            .context("count postings")?;
        Ok(n == 0)
    }
}

/// Kelly sizing guard: until equity reaches `KELLY_EQUITY_CAP_RELEASE_USD`
/// a Kelly stake above this fraction of equity is refused - a `q_lo` good
/// enough to size that large on a small book is a defect until audited.
pub const KELLY_EQUITY_FRACTION_CAP: f64 = 0.10;
pub const KELLY_EQUITY_CAP_RELEASE_USD: f64 = 500.0;

/// Why `kelly_lo_stake` declined to size (the `band_skip_detail` reason).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KellySkip {
    /// No positive Kelly fraction at this price for this `q_lo` (or an
    /// input outside its domain).
    NoEdge,
    /// `f_k x f_lo x equity` is below the venue minimum: skipped, never
    /// clamped up.
    BelowVenueMin,
    /// Above `KELLY_EQUITY_FRACTION_CAP` of an equity still below
    /// `KELLY_EQUITY_CAP_RELEASE_USD`.
    AboveEquityCap,
    /// `f_k` (`KELLY_FRACTION`) outside (0, 1]: a configuration error,
    /// named as such so the skip never reads as a verdict on the price.
    BadFraction,
}

impl KellySkip {
    pub fn as_str(self) -> &'static str {
        match self {
            KellySkip::NoEdge => "kelly_no_edge",
            KellySkip::BelowVenueMin => "kelly_below_venue_min",
            KellySkip::AboveEquityCap => "kelly_above_equity_cap",
            KellySkip::BadFraction => "kelly_bad_fraction",
        }
    }
}

/// `KELLY_FRACTION` is a Kelly multiple in (0, 1] (full Kelly at most;
/// NaN fails). One predicate for the sizer, the preflight and
/// `Pipeline::new`.
pub fn kelly_fraction_valid(f_k: f64) -> bool {
    f_k > 0.0 && f_k <= 1.0
}

/// Kelly on the promoted cell's running ladder Wilson lower bound
/// (docs/profitability_basement_2026-09-18.md, section C "Sizing and
/// accounting"): `stake = f_k x f_lo x equity` with
/// `f_lo = q_lo - (1 - q_lo) / b`, `b` the net odds at the live taker fee.
/// `q_lo` is the artifact's `kelly_q_lo` (callers use the fixed `stake_usd`
/// when it is 0) and `f_k` the operator's `KELLY_FRACTION`, refused
/// outside (0, 1] (`BadFraction`). Never clamps up: a stake below
/// `BAND_VENUE_MIN_STAKE` is a skip; so is one above 10% of equity until
/// equity reaches $500. Capped at `cap` (`stake_usd`).
pub fn kelly_lo_stake(
    price: f64,
    equity: f64,
    q_lo: f64,
    f_k: f64,
    cap: f64,
) -> Result<f64, KellySkip> {
    if !kelly_fraction_valid(f_k) {
        return Err(KellySkip::BadFraction);
    }
    // Positive-form domain test: NaN anywhere fails it.
    let in_domain = price > 0.0 && price < 1.0 && equity > 0.0 && q_lo > 0.0 && q_lo < 1.0;
    if !in_domain {
        return Err(KellySkip::NoEdge);
    }
    // The live taker fee (0.07), one constant with the fill models and the
    // promotion artifact builder.
    let fee_rate = crate::data::models::DEFAULT_CRYPTO_TAKER_FEE_RATE;
    let b = (1.0 - price) / price - fee_rate * (1.0 - price);
    if b <= 0.0 {
        return Err(KellySkip::NoEdge);
    }
    let f_lo = q_lo - (1.0 - q_lo) / b;
    if f_lo <= 0.0 {
        return Err(KellySkip::NoEdge);
    }
    let stake = f_k * f_lo * equity;
    if !stake.is_finite() || stake < BAND_VENUE_MIN_STAKE {
        return Err(KellySkip::BelowVenueMin);
    }
    if equity < KELLY_EQUITY_CAP_RELEASE_USD && stake > KELLY_EQUITY_FRACTION_CAP * equity {
        return Err(KellySkip::AboveEquityCap);
    }
    Ok(stake.min(cap))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn postings_are_idempotent_and_balances_derive() {
        let dir = tempfile::tempdir().unwrap();
        let book = BookV2::open(dir.path().join("book.db")).unwrap();
        assert!(book.is_empty().await.unwrap());
        assert!(book
            .post(1.0, PostingKind::Import, "band", "", 19.0, 0.0, 0.0, "from v1", "import")
            .await
            .unwrap());
        // entry: 7.0 shares at 0.75 = 5.25 + 0.10 fee
        assert!(book
            .post(2.0, PostingKind::Fill, "band", "0xc1", -5.35, 7.0, 0.75, "", "i1::fill")
            .await
            .unwrap());
        // duplicate replay is a no-op
        assert!(!book
            .post(2.0, PostingKind::Fill, "band", "0xc1", -5.35, 7.0, 0.75, "", "i1::fill")
            .await
            .unwrap());
        assert!((book.equity().await.unwrap() - 13.65).abs() < 1e-9);
        assert!((book.open_cost().await.unwrap() - 5.35).abs() < 1e-9);
        // win settles at qty
        book.post(3.0, PostingKind::Settlement, "band", "0xc1", 7.0, 7.0, 0.0, "won", "0xc1::settle")
            .await
            .unwrap();
        assert!((book.equity().await.unwrap() - 20.65).abs() < 1e-9);
        assert!((book.realized_pnl().await.unwrap() - 1.65).abs() < 1e-9);
        assert!(book.open_cost().await.unwrap().abs() < 1e-9);
    }

    /// Net odds at the live taker fee, as `kelly_lo_stake` computes them.
    fn net_odds(p: f64) -> f64 {
        (1.0 - p) / p - crate::data::models::DEFAULT_CRYPTO_TAKER_FEE_RATE * (1.0 - p)
    }

    #[test]
    fn kelly_lo_sizes_on_the_artifact_q_lo_and_never_clamps_up() {
        // p=0.92, q_lo=0.94, f_k=0.25 at $100: f_lo ~ 0.202, stake $5.05.
        let (p, q_lo, f_k) = (0.92, 0.94, 0.25);
        let f_lo = q_lo - (1.0 - q_lo) / net_odds(p);
        let expect = f_k * f_lo * 100.0;
        assert!(expect > BAND_VENUE_MIN_STAKE && expect < 10.0, "{expect}");
        let s = kelly_lo_stake(p, 100.0, q_lo, f_k, 25.0).unwrap();
        assert!((s - expect).abs() < 1e-12, "got {s}, expected {expect}");
        // A hair less equity puts the stake under $5: a skip, never $5.
        assert_eq!(
            kelly_lo_stake(p, 95.0, q_lo, f_k, 25.0),
            Err(KellySkip::BelowVenueMin)
        );
        // Below break-even (q_be = 1/(1+b) ~ 0.9248 at 0.92): no edge.
        assert_eq!(
            kelly_lo_stake(p, 1_000.0, 0.92, f_k, 25.0),
            Err(KellySkip::NoEdge)
        );
        // q_lo 0 (fixed-stake artifact) and other out-of-domain inputs.
        assert_eq!(kelly_lo_stake(p, 100.0, 0.0, f_k, 25.0), Err(KellySkip::NoEdge));
        assert_eq!(kelly_lo_stake(p, 100.0, 1.0, f_k, 25.0), Err(KellySkip::NoEdge));
        assert_eq!(kelly_lo_stake(p, 0.0, q_lo, f_k, 25.0), Err(KellySkip::NoEdge));
        assert_eq!(kelly_lo_stake(1.0, 100.0, q_lo, f_k, 25.0), Err(KellySkip::NoEdge));
        assert_eq!(kelly_lo_stake(0.0, 100.0, q_lo, f_k, 25.0), Err(KellySkip::NoEdge));
        // KELLY_FRACTION outside (0, 1] is a configuration error, named as
        // one: 0 (a disabled Kelly is q_lo 0, not f_k 0), a percent typed
        // as 25, above full Kelly, NaN and infinite - never a price verdict
        // and never a stake bounded only by the cap ($3,037 raw at $600).
        for bad in [0.0, -0.25, 1.5, 25.0, f64::NAN, f64::INFINITY] {
            assert_eq!(
                kelly_lo_stake(p, 600.0, q_lo, bad, 25.0),
                Err(KellySkip::BadFraction),
                "f_k {bad}"
            );
            assert!(!kelly_fraction_valid(bad), "f_k {bad}");
        }
        assert_eq!(KellySkip::BadFraction.as_str(), "kelly_bad_fraction");
        // Full Kelly is the largest multiple accepted: at $600 the cap binds.
        assert!(kelly_fraction_valid(1.0));
        assert_eq!(kelly_lo_stake(p, 600.0, q_lo, 1.0, 25.0), Ok(25.0));
    }

    #[test]
    fn kelly_lo_refuses_above_ten_percent_of_a_small_equity_then_caps() {
        // p=0.92, q_lo=0.97, f_k=0.25: f_lo ~ 0.60, i.e. 15% of equity.
        let (p, q_lo, f_k) = (0.92, 0.97, 0.25);
        let f_lo = q_lo - (1.0 - q_lo) / net_odds(p);
        assert!(f_k * f_lo > KELLY_EQUITY_FRACTION_CAP);
        assert_eq!(
            kelly_lo_stake(p, 100.0, q_lo, f_k, 25.0),
            Err(KellySkip::AboveEquityCap)
        );
        assert_eq!(
            kelly_lo_stake(p, 499.0, q_lo, f_k, 25.0),
            Err(KellySkip::AboveEquityCap)
        );
        // At $500 the guard releases and the cap (stake_usd) binds.
        let s = kelly_lo_stake(p, 500.0, q_lo, f_k, 25.0).unwrap();
        assert!((s - 25.0).abs() < 1e-9, "got {s}");
        // Under the cap the stake is the plain product.
        let s = kelly_lo_stake(p, 500.0, q_lo, f_k, 100.0).unwrap();
        assert!((s - f_k * f_lo * 500.0).abs() < 1e-9, "got {s}");
    }

    #[test]
    fn kelly_lo_uses_the_live_taker_fee() {
        use crate::data::models::DEFAULT_CRYPTO_TAKER_FEE_RATE;
        assert_eq!(DEFAULT_CRYPTO_TAKER_FEE_RATE, 0.07);
        // Closed form at the live fee, unguarded ($600 equity, $100 cap).
        let (p, q_lo, f_k) = (0.85, 0.9413, 0.05);
        let expect = f_k * (q_lo - (1.0 - q_lo) / net_odds(p)) * 600.0;
        assert!(expect > BAND_VENUE_MIN_STAKE && expect < 100.0);
        let s = kelly_lo_stake(p, 600.0, q_lo, f_k, 100.0).unwrap();
        assert!((s - expect).abs() < 1e-12, "got {s}, expected {expect}");
        // The 2026-09-01 study's 0.072 would size differently.
        let b_study = (1.0 - p) / p - 0.072 * (1.0 - p);
        let study = f_k * (q_lo - (1.0 - q_lo) / b_study) * 600.0;
        assert!((s - study).abs() > 1e-6);
    }
}
