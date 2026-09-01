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

/// Sizing from the 2026-09-01 Monte Carlo study
/// (docs/risk_book_v2/sizing_study_2026-09-01.md): half-Kelly on the
/// per-bucket Wilson LOWER bound of the 222-row gate evidence, venue
/// clamped. The <=0.70 bucket's edge CI straddles break-even, so it sizes
/// to zero — no stake on an unproven bucket.
pub fn kelly_lo_stake(price: f64, equity: f64, cap: f64) -> Option<f64> {
    if !(0.0..1.0).contains(&price) || equity <= 0.0 {
        return None;
    }
    let q_lo = if price <= 0.70 {
        return None;
    } else if price <= 0.80 {
        0.9124
    } else {
        0.9413
    };
    let fee_rate = 0.072;
    let b = (1.0 - price) / price - fee_rate * (1.0 - price);
    if b <= 0.0 {
        return None;
    }
    let f_lo = q_lo - (1.0 - q_lo) / b;
    if f_lo <= 0.0 {
        return None;
    }
    Some((0.5 * f_lo * equity).clamp(BAND_VENUE_MIN_STAKE, cap))
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

    #[test]
    fn kelly_lo_schedule_matches_the_study() {
        // p=0.75 at $19: study says $6.03
        let s = kelly_lo_stake(0.75, 19.0, 25.0).unwrap();
        assert!((s - 6.03).abs() < 0.15, "got {s}");
        // p=0.85 at $19: ~$5.58
        let s = kelly_lo_stake(0.85, 19.0, 25.0).unwrap();
        assert!((s - 5.58).abs() < 0.2, "got {s}");
        // p=0.92 clamps up to the venue min
        let s = kelly_lo_stake(0.92, 19.0, 25.0).unwrap();
        assert!((s - 5.0).abs() < 1e-9);
        // the unproven bucket sizes to zero
        assert!(kelly_lo_stake(0.65, 19.0, 25.0).is_none());
        // cap binds as equity grows
        let s = kelly_lo_stake(0.75, 300.0, 25.0).unwrap();
        assert!((s - 25.0).abs() < 1e-9);
    }
}
