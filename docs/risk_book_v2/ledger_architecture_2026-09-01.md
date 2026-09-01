# RiskBook v2 — architecture for a separate, switchable risk/portfolio book

Design only; no implementation code written, no repo files touched. All paths absolute; line numbers from branch `codex/audit1` @ 68c578d.

---

## PART 1 — Current-state map (v1) with file:line

### 1.1 RiskManager (`/Users/ttoomm/Documents/PolyMomentum/rust_engine/src/risk/manager.rs`)

| Concept | Location | Behavior |
|---|---|---|
| `RiskConfig` | manager.rs:47-58 | `initial_bankroll`, `exposure_ratio`, `max_total_exposure_override`, `max_per_market_ratio/override`, `actualize_on_open`, `pin_initial_bankroll` |
| Mutable state `Inner` | manager.rs:80-86 | `positions`, `last_trade_time`, **`total_pnl`, `total_fees_paid` — stored-and-mutated scalars** |
| `open()` | manager.rs:89-113 | load_state, then actualize_on_open, then save |
| **Bankroll actualization** | manager.rs:115-137 | On live restart: `initial_bankroll += total_pnl; total_pnl = 0`. The baseline absorbs session PnL; history is destroyed |
| `initial_bankroll()` | manager.rs:139-141 | Post-actualization baseline (used as "breaker bankroll" everywhere in pipeline) |
| `effective_bankroll()` | manager.rs:147-150 | `(initial_bankroll + total_pnl).max(0)` |
| `max_per_market()` | manager.rs:152-156 | ratio × bankroll, min override |
| `available_capital_for_exposure()` | manager.rs:172-175 | `exposure_cap - caller-supplied exposure` — the book does NOT know its own exposure; the pipeline hands it in |
| `record_pnl()` | manager.rs:183-189 | `total_pnl += amount` then full save_state (mutation, not posting) |
| `record_fees()` | manager.rs:191-194 | in-memory add; durable only at next save_state |
| `record_trade()` | manager.rs:236-256 | append into `trades` table; `pnl` column is always 0 at entry; **the trades log is never read back to derive any balance** — pure telemetry |
| meta store | manager.rs:258-284 | get/set/delete on `meta` key/value |
| `save_state()` | manager.rs:382-431 | rewrites `state` keys (`bankroll_baseline`, `total_pnl`, `total_fees_paid`, `saved_at`); DELETE-all + rewrite of `positions` and `cooldowns` |
| `load_state()` + pin | manager.rs:433-506, pin at 458-468 | persisted `bankroll_baseline` overrides config **unless** `pin_initial_bankroll` (explicit `BANKROLL_USD`, config.rs:256; pin decided at pipeline.rs:944-947) |
| `exposure_cap()` | manager.rs:509-517 | ratio×bankroll min $override |
| **state.db schema (v1)** | manager.rs:519-567 | tables: `state` (k/v scalars), `positions`, `trades` (append log), `cooldowns`, `meta` (k/v), `paper_positions` (JSON payloads), `oracle_pending` (JSON), `live_pending_orders` (JSON journal) |

### 1.2 Pipeline consumption (`/Users/ttoomm/Documents/PolyMomentum/rust_engine/src/live/pipeline.rs`)

**Parallel PnL representations (the core problem — at least five):**
1. `RiskManager.total_pnl` + `bankroll_baseline` (state table) — mutated by `record_pnl` at pipeline.rs:4699 (oracle correction delta) and :4736 (final settlement).
2. `BreakerState.realized_pnl` (session) — breaker.rs:46-80, mutated in the same settlement block (pipeline.rs:4751-4752), persisted as a JSON blob under meta key `candle_breaker_state` (pipeline.rs:5421).
3. Meta key `live_cumulative_realized_pnl` — cross-restart ledger, field `live_loss_ledger_prior` (pipeline.rs:875-879, read at 1186-1190); maintained by a **restart-time fold**: at every actualization the finished session's `breaker_state.realized_pnl` is added into the meta key (pipeline.rs:1080-1095), then breaker meta is deleted (1096-1105).
4. `trades` table (record_trade at pipeline.rs:2429-2443 live fill, 4006-4019 paper) — write-only.
5. Session monitor JSONL + `trade_log` ring (pipeline.rs:885) — telemetry.

They agree only by convention: equity for the operator = `initial_bankroll + breaker.realized_pnl` (pipeline.rs:5102-5103, 5175), cumulative stop measure = `ledger_prior + breaker.realized_pnl` (pipeline.rs:5104), and start-time hard block on the ledger alone (pipeline.rs:1191-1202).

**Exposure is derived from three concurrently-mutated lifecycle maps** — `live_pending_positions`, `paper_positions`, `oracle_pending` (pipeline.rs:871-873); handoffs pending→position→oracle hold awaits between insert and remove:
- `open_position_exposure()` pipeline.rs:4433-4478 — all three maps, dedup by cid (dedup added post-incident, commit 68c578d).
- `breaker_stress_exposure()` pipeline.rs:4390-4424 — open windows + order reservations only; oracle-pending excluded post-incident (comment 4382-4389).
- startup `restored_open_exposure` pipeline.rs:1170-1181 — plain sum of the three maps.

**Sizing chain (band):** `target_stake = clamp(pct×effective_bankroll, $5, stake_usd)` pipeline.rs:497-499; entry path takes `.min(max_per_market).min(available_capital_for_exposure(open_exposure)).min(stress_headroom)` pipeline.rs:3564-3584; venue-minimum sanity handled by the anomaly bound `band_exposure_within_contract` (pipeline.rs:5495-5502).

**Wallet:** never authoritative. Best-effort read every ~5 min (pipeline.rs:4907-4927) into an atomic; used only as an entry-skip guard when stake+$0.60 exceeds the last reading <15 min old (pipeline.rs:3596-3627), plus operator `/balance` display (5175-5192). Initial bankroll may come from a one-shot wallet read (pipeline.rs:920-927, try_wallet_bankroll at 5668) but afterwards the book and the chain never reconcile.

**Settlement idempotency:** `pnl_recorded` flag set + persisted immediately after realization, before trip checks (pipeline.rs:4803-4812) — a point patch for the replay incident, not structural.

**Operator halt state:** in-memory flags + meta keys; sqlite-only edits never affected the live process (comment pipeline.rs:5202-5204, operator_rearm 5205-5225).

### 1.3 Current invariants (all by convention, none enforced by construction)
- equity ≡ baseline + total_pnl ≡ baseline + breaker.realized_pnl (only because actualization zeroes both at the same restart)
- cumulative realized ≡ ledger meta + current session (only because the fold runs exactly once per restart)
- exposure ≡ dedup-summed maps (only because every handoff site remembers to move rows)
- one economic fact (a settlement) must touch: total_pnl, breaker state, fees, trades log, monitor — five writes with awaits between them

### 1.4 Traps we actually hit live (each is a symptom of stored-and-mutated balances)
1. **Double-counted exposure across lifecycle maps** — 2026-08-31 13:09: $7.91 fill visible in two maps for ~3 ms → false `open_exposure_stress` trip mid 6-0 streak, 4 h halt (commit 68c578d; comment pipeline.rs:4426-4432).
2. **Oracle PnL replay on restart** — breaker trip aborted the oracle task between realization and removal-persist; restart replayed the settlement into bankroll, breaker AND ledger (docs/band_canary_audit_2026-08-25.md, critical finding 1).
3. **Actualization folds session PnL into the ledger on restart → manual corrections double-count** — the same dollar exists in `breaker_state.realized_pnl` and (after any restart) in `live_cumulative_realized_pnl`; an operator correcting both places gets it counted twice (memory/project_state: "при ручной коррекции писать ТОЛЬКО в один из них").
4. **Actualization death loop** — 2026-09-01 02:19: bankroll actualized to ~$13.8; venue-min $5 entry > 30% stress rule → trip on EVERY entry; restart → actualization clears trip → re-enter → re-trip (regression test pipeline.rs:6033-6057).
5. **Stress trip on freshly actualized session (peak 0)** — 2026-08-31 17:59: 20-min oracle lag counted as exposure against a just-zeroed session, 8 h halt (comment pipeline.rs:4382-4389).
6. **Pinned bankroll vs wallet drift** — `BANKROLL_USD` pinned (pipeline.rs:944-947, manager.rs:458-468) while the actual wallet drifts (shared-wallet drains, redemption lag, fees); the book finds out only when the venue rejects, or never.
7. **DB-edited halt state inert on the live process** — pipeline.rs:5202-5204.

---

## PART 2 — RiskBook v2 design

### 2.0 Placement and switching
- Module: `/Users/ttoomm/Documents/PolyMomentum/rust_engine/src/risk/book_v2.rs` (+ `risk/mod.rs` exports; trait in `risk/book.rs` or top of book_v2).
- Same sqlite file (`STATE_DB_PATH`, config.rs:374), **own tables prefixed `v2_`, never sharing a row with v1**. v1 tables are never written by v2; v2 tables never read by v1.
- One flag: `RISK_BOOK=v1|v2` (default `v1`), parsed in `Settings::from_env` (config.rs:212) alongside `RISK_BOOK_SHADOW=v2` to arm parallel shadow accounting. Flipping the env is the entire cutover and the entire rollback.
- Scope boundary (keeps the change surgical): `RiskManager` remains the process's *state store* in both modes — live order journal, paper-position persistence, breaker meta keys stay where they are. The flag switches only **who answers the accounting questions** (equity, exposure, sizing budget, money stops) and **who records economic events**.

### 2.1 Core principle: event-sourced postings, derived balances
No stored balance is ever mutated. Every economic fact is one append-only journal row with an idempotency key. Equity, exposure, session PnL, lifetime PnL are queries. "Restart" is not an accounting event — there is nothing to fold, actualize, or zero.

### 2.2 Schema (tables + postings)

```sql
-- The single source of truth. Append-only; no UPDATE, no DELETE.
CREATE TABLE v2_journal (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,             -- unix seconds, event time
    session_id  TEXT NOT NULL,             -- process boot id (view partitioning only)
    strategy_id TEXT NOT NULL,             -- sub-book key ('' for portfolio-level rows)
    kind        TEXT NOT NULL CHECK (kind IN
                  ('allocation',           -- operator/factory grants or moves capital (signed)
                   'fill',                 -- cash OUT: amount = -(shares*price); opens/extends a lot
                   'settlement',           -- cash IN: amount = +payout (shares*1 or 0); closes a lot
                   'settlement_correction',-- oracle disagreement delta vs provisional
                   'fee',                  -- amount = -fee
                   'adjustment',           -- operator manual, signed, note REQUIRED
                   'external_transfer',    -- deposit/withdrawal acknowledgment (wallet expectation only)
                   'import')),             -- one-shot v1 opening balances
    ref_id      TEXT NOT NULL,             -- idempotency key (see below)
    contract_id TEXT,                      -- nullable for non-trade kinds
    token_id    TEXT,
    size        REAL,
    price       REAL,
    amount_usd  REAL NOT NULL,             -- signed cash delta to the strategy's book
    note        TEXT,                      -- required for 'adjustment'
    payload     TEXT,                      -- JSON detail (fill economics, oracle result, ...)
    UNIQUE (kind, ref_id)                  -- replay = INSERT OR IGNORE = AlreadyApplied
);

-- Lot registry: ONE row per contract lifecycle. A projection of the journal —
-- rebuildable from postings; rebuilt+verified on every open (mismatch => halt).
CREATE TABLE v2_lots (
    lot_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT NOT NULL,
    contract_id TEXT NOT NULL,
    token_id    TEXT NOT NULL,
    direction   TEXT NOT NULL,
    size        REAL NOT NULL,
    entry_price REAL NOT NULL,
    cost_usd    REAL NOT NULL,
    opened_seq  INTEGER NOT NULL REFERENCES v2_journal(seq),
    state       TEXT NOT NULL CHECK (state IN
                  ('reserved',            -- order submitted, not filled (exposure reserved)
                   'open',                -- filled, window live
                   'awaiting_resolution', -- window expired, oracle pending (sunk, excluded from stress)
                   'settled_unredeemed',  -- settlement posted, pUSD not yet on chain
                   'settled')),
    end_time    REAL NOT NULL,            -- window expiry
    settled_seq INTEGER REFERENCES v2_journal(seq),
    UNIQUE (strategy_id, contract_id)     -- a contract exists ONCE; two-map double-count impossible
);

-- Per-strategy sub-books (the factory's champion + challengers).
CREATE TABLE v2_strategies (
    strategy_id    TEXT PRIMARY KEY,      -- e.g. 'band_official_v1', 'chal_<hash>'
    display_name   TEXT NOT NULL,
    sizing_policy  TEXT NOT NULL,         -- JSON: {"kind":"pct",...} | {"kind":"flat",...} | {"kind":"kelly_fraction",...}
    floor_usd      REAL NOT NULL,         -- per-strategy stop: derived equity <= floor => entries halted
    stop_loss_usd  REAL NOT NULL,         -- per-strategy cumulative realized floor (money stop)
    max_stake_usd  REAL NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('active','halted','retired')),
    status_reason  TEXT,
    created_at     REAL NOT NULL
);
-- Allocations are NOT a column: allocation(strategy) = SUM(journal 'allocation'+'import' rows).

-- v2-private key/value (portfolio config knobs, session markers). Never shares keys with v1 `meta`.
CREATE TABLE v2_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

-- Wallet observations + reconciliation verdicts.
CREATE TABLE v2_wallet_obs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL, pusd REAL NOT NULL, usdc_e REAL, pol REAL, source TEXT NOT NULL
);
CREATE TABLE v2_recon (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    wallet_usd REAL NOT NULL,             -- observed
    book_cash_usd REAL NOT NULL,          -- derived: SUM(journal.amount_usd) all strategies
    receivable_usd REAL NOT NULL,         -- settled_unredeemed payouts (age-tracked)
    drift_usd REAL NOT NULL,              -- wallet - (book_cash - receivable adjustments)
    verdict TEXT NOT NULL CHECK (verdict IN ('ok','alert','halt')),
    note TEXT
);

-- One-shot import guard + frozen v1 snapshot for audit.
CREATE TABLE v2_imports (
    import_id   TEXT PRIMARY KEY,         -- 'v1_import_1' — uniqueness makes it one-shot
    ts          REAL NOT NULL,
    v1_snapshot TEXT NOT NULL             -- frozen JSON: state rows, breaker meta, ledger meta, open positions
);
```

**Idempotency `ref_id` conventions:** fill → venue trade id (fallback `intent_id:fill_n`); settlement → `contract_id` (one lifecycle, one settlement); settlement_correction → `contract_id:oracle`; fee → `contract_id:fee`; allocation/adjustment → operator-supplied ULID; import rows → fixed strings (`import:opening_allocation:band_official_v1`, `import:prior_cumulative`, `import:lot:<cid>`). The oracle loop replaying after a crash re-posts the same `(kind, ref_id)` and gets `AlreadyApplied` — the `pnl_recorded` flag dance (pipeline.rs:4803-4812) becomes unnecessary.

**Derived quantities (views/queries, never columns):**
- `cash(s)` = Σ journal.amount_usd for strategy s (allocations + imports − fills + settlements − fees ± adjustments)
- `open_cost(s)` = Σ cost_usd over lots in (reserved, open, awaiting_resolution)
- `equity(s)` = cash(s) + open_cost(s)  (mark-at-cost; hold-to-expiry, no MTM needed)
- `exposure(s)` = Σ cost_usd over lots in (reserved, open) — stress measure; awaiting_resolution excluded **by state**, not by heuristic
- `lifetime_realized(s)` = Σ (settlement + settlement_correction + fee + fill) over settled lots, + `import:prior_cumulative` marker — feeds the money stop (≤ −$11.40 rule) with restart-proof continuity and NO fold
- `session view` = same sums filtered `session_id = current` — the breaker's session numbers become a query, not a second ledger

### 2.3 Portfolio-level invariants (checked at every entry decision and on open)
1. Σ allocations (all strategies) ≤ last verified wallet balance − pending-buffer. New challenger allocations are rejected if they would overdraw the wallet.
2. Per-strategy floor/stop: equity(s) ≤ floor_usd or lifetime_realized(s) ≤ stop_loss_usd ⇒ strategy status 'halted' (entry-side only; settlement/reconciliation always run).
3. Global floor: Σ equity ≤ global floor (v2_meta) ⇒ all entries halted.
4. Sizing anomaly bound (per strategy): exposure(s) > 1.5 × policy stake + $1 ⇒ 'bug' halt — the current band contract (pipeline.rs:5495-5502) generalized, evaluated against the same derived equity the sizing used, so the 30%-rule contradiction class cannot recur.
5. Journal integrity on open: rebuild lots from journal, compare to v2_lots; any mismatch ⇒ halt entries + alert (accounting-integrity trip, mirrors current BUGS family).

### 2.4 SizingPolicy trait (pluggable)
```rust
pub struct SizeContext {
    pub strategy_equity: f64,      // derived
    pub open_exposure: f64,        // derived
    pub entry_price: f64,          // budget-aware quote vwap
    pub venue_min_usd: f64,        // $5
    pub q_win: Option<f64>,        // per-price win prob (promotion artifact q-table)
    pub fee_per_dollar: f64,       // taker_fee(p) per $1 staked
}
pub struct StakeDecision { pub stake_usd: f64, pub reason: &'static str }

pub trait SizingPolicy: Send + Sync {
    fn stake(&self, ctx: &SizeContext) -> StakeDecision;
}
```
Built-ins (deserialized from `v2_strategies.sizing_policy` JSON):
- `Flat { usd }`
- `PctEquity { pct, floor_usd, cap_usd }` — exactly today's band clamp (pipeline.rs:497-499): `clamp(pct×equity, floor, cap)`
- `KellyFraction { lambda, cap_pct, q_table: Vec<{p_lo, p_hi, q}> }` — per-price q from the promotion artifact / gate rows (222 resolved rows in logs/strategy-research/20260821_fresh_gate_public_v1.json binned by signal_entry); payoff per $1 at price p: b = (1−p)/p − fee; f* = q − (1−q)/b; stake = clamp(λ·f*·equity, venue_min, cap_pct×equity). Fail-closed: p outside table ⇒ stake 0.
The book applies the policy, then the invariant caps (allocation, floor headroom, anomaly bound) — one gate, one place.

### 2.5 Trait/API surface the pipeline calls
```rust
#[async_trait]
pub trait RiskBook: Send + Sync {
    // Reads (replace effective_bankroll / open_position_exposure / available_capital_for_exposure)
    async fn equity(&self, s: &StrategyId) -> f64;
    async fn open_exposure(&self, s: &StrategyId) -> f64;
    async fn lifetime_realized(&self, s: &StrategyId) -> f64;
    async fn session_realized(&self, s: &StrategyId) -> f64;

    // One entry gate: sizing + all portfolio invariants + halts, atomically
    async fn entry_budget(&self, s: &StrategyId, quote: &EntryQuote) -> EntryVerdict;
    //   EntryVerdict = Approved { stake_usd } | Skip { reason } | Halted { family, reason }
    //   families: Money | Bug | Operator | Drift  (matches the band stopping policy)

    // Postings — every method idempotent via (kind, ref_id); returns Applied(seq) | AlreadyApplied(seq)
    async fn post_reservation(&self, s: &StrategyId, r: ReservationPosting) -> Result<PostingOutcome>;
    async fn post_fill(&self, s: &StrategyId, f: FillPosting) -> Result<PostingOutcome>;
    async fn post_settlement(&self, s: &StrategyId, x: SettlementPosting) -> Result<PostingOutcome>;
    async fn post_settlement_correction(&self, s: &StrategyId, x: CorrectionPosting) -> Result<PostingOutcome>;
    async fn post_fee(&self, s: &StrategyId, x: FeePosting) -> Result<PostingOutcome>;
    async fn post_adjustment(&self, s: &StrategyId, x: AdjustmentPosting) -> Result<PostingOutcome>; // operator only, note required
    async fn release_reservation(&self, s: &StrategyId, ref_id: &str) -> Result<()>; // FOK reject / transient

    // Reconciliation — wallet-anchored truth
    async fn record_wallet_observation(&self, obs: WalletObs) -> ReconVerdict;
    //   ReconVerdict::Ok | Alert{drift} | HaltEntries{drift}  (|drift| > $0.50 => alert + halt entries)

    // Ops / factory
    async fn strategies(&self) -> Vec<StrategySummary>;
    async fn set_strategy_status(&self, s: &StrategyId, status: Status, reason: &str) -> Result<()>;
    async fn snapshot(&self) -> BookSnapshot;   // operator /status, watchdog, parity probe
}
```
Implementations: `V1RiskBook` (thin adapter delegating to today's `RiskManager` + lifecycle-map sums so behavior is bit-identical under `RISK_BOOK=v1`) and `BookV2`. A `ShadowedBook { primary, shadow }` wrapper fans every posting out to both, answers from `primary`, and emits a `risk_book_parity` session record on divergence.

Pipeline call-site mapping: pipeline.rs:3568 `effective_bankroll` → `equity`; :3566/:4433 exposure sums → `open_exposure`; :3570-3584 sizing chain → `entry_budget`; :2429/:4007 `record_trade` → `post_fill`; :4699/:4736/:4750 `record_pnl`/`record_fees` → `post_settlement` / `post_settlement_correction` / `post_fee`; :1080-1095 restart fold → deleted (nothing to fold); :1186-1202 start block → `lifetime_realized` check; :4907-4927 wallet loop → `record_wallet_observation`.

### 2.6 Reconciliation: wallet-anchored truth, no silent actualization
- Expected pUSD = `Σ cash(all strategies)` (fills subtract spend, settlements add payout). Redemption lag handled by lot state: `settled_unredeemed` payouts are subtracted from the expectation while young; a receivable older than a bound (e.g. 30 min) is itself an alert (auto-redeem broken).
- Every ~5-min on-chain reading (existing loop) posts an observation; `|drift| > $0.50` ⇒ telegram alert + **halt NEW entries** (Drift family). Settlement, corrections, reconciliation continue.
- Drift is NEVER folded into balances. The only exits: (a) operator posts a journaled `adjustment` or `external_transfer` with a note; (b) the drift resolves on-chain (late redemption). Actualization does not exist in v2 — `actualize_on_open` (manager.rs:115-137) is v1-only forever.

### 2.7 One-shot import from v1 (initialization)
Guarded by `v2_imports` PK; running twice is a no-op. Postings created:
1. `import` allocation for the champion strategy = v1 `effective_bankroll()` (ref `import:opening_allocation:<sid>`).
2. `import` marker carrying prior cumulative realized = `live_cumulative_realized_pnl` meta + current `breaker_state.realized_pnl` (ref `import:prior_cumulative`) — money-stop continuity vs base $19 without any future fold.
3. One `import` fill posting + lot per open v1 entry across `paper_positions` / `oracle_pending` / `live_pending_orders`, deduped by cid, mapped to lot states open / awaiting_resolution / reserved.
4. Frozen JSON snapshot of all v1 rows into `v2_imports.v1_snapshot`.

---

## PART 3 — Shadow-then-cutover migration plan

**Phase 0 — land behind the flag (no behavior change).** Introduce trait + `V1RiskBook` adapter; refactor pipeline call sites to the trait; `RISK_BOOK=v1` default. Verify: full test suite green and a cached live-replay session produces byte-identical session records vs pre-refactor (backtest-first rule; no paper mode needed).

**Phase 1 — shadow (v1 drives, v2 accounts).** `RISK_BOOK=v1 RISK_BOOK_SHADOW=v2`. First boot runs the one-shot import. `ShadowedBook` fans out postings; per cycle and per settlement a `risk_book_parity` record logs v1 equity/exposure/cumulative vs v2 derived; divergence > $0.01 ⇒ telegram alert (v1 still drives — zero risk delta). Wallet reconciliation runs report-only. Duration: N ≥ 5 live sessions AND ≥ 30 settlements AND the evidence set must contain ≥ 1 restart with an open position, ≥ 1 losing settlement, ≥ 1 oracle correction/disagreement, ≥ 1 FOK reject with reservation release (each is a historical failure trigger; any missing after N sessions gets exercised in a replay harness against cached sessions instead of waiting).

**Phase 2 — cutover.** Flip `RISK_BOOK=v2` (shadow flag now points nowhere or runs v1 as shadow for symmetry). v1 tables frozen, untouched, never shared — **rollback is flipping the env back**; v1 resumes exactly where it stopped because v2 never wrote a v1 row. Any manual db edit or schema change during shadow invalidates the evidence and restarts N.

**Phase 3 — retire v1-only mechanisms.** With v2 driving: `actualize_on_open` inert, restart fold (pipeline.rs:1080-1095) deleted, `live_cumulative_realized_pnl` superseded by derived lifetime realized, breaker session numbers read from `session_realized`. v1 code stays until one full compounding review cycle passes, then is removed in its own change.

---

## PART 4 — Live incidents this design makes structurally impossible

| Incident | v1 root cause | Why impossible in v2 |
|---|---|---|
| 2026-08-31 13:09 — $7.91 exposure double-count, false `open_exposure_stress`, 4 h halt (commit 68c578d) | one trade visible in two of three lifecycle maps during an awaited handoff | `v2_lots` UNIQUE(strategy_id, contract_id): a contract is one row in one state; exposure is a query, not a sum over racing maps |
| 2026-08-25 audit #1 — oracle PnL replay on restart double-counted into bankroll+breaker+ledger | realization not atomic with removal; idempotency flag bolted on later | UNIQUE(kind, ref_id): re-posting a settlement is `AlreadyApplied`; crash-replay is a no-op by construction |
| Restart fold + manual correction double-count (memory: "write only to ONE of them") | same dollar lives in breaker session state AND the cumulative meta key, merged by a restart-time fold | there is one journal; session and lifetime are two filters over the same rows; a correction is one `adjustment` posting; there is no fold |
| 2026-09-01 02:19 — actualization death loop (restart → actualize → $5 entry > 30% of shrunken bankroll → trip → restart) | actualization mutates the baseline the stress rule reads; restart changes accounting state | no actualization exists; restart changes no balance; the anomaly bound is defined relative to the SizingPolicy's own output on the same derived equity — a legal entry can never exceed it |
| 2026-08-31 17:59 — stress trip on freshly actualized session, peak 0, oracle lag counted as exposure, 8 h halt | exposure classes inferred by heuristic; session peak zeroed by actualization | `awaiting_resolution` is a lot **state** excluded from stress by definition; "freshly zeroed session" cannot exist because nothing zeroes |
| Pinned bankroll vs wallet drift (silent; discovered via venue rejects or never) | book never reconciles with chain; pin makes baseline permanently synthetic | wallet-anchored reconciliation every ~5 min; drift > $0.50 ⇒ alert + entry halt; drift can only be cleared by a journaled posting — *silent* divergence is impossible (detection, not prevention, stated honestly) |
| sqlite-edited breaker state inert on live process (pipeline.rs:5202-5204) | halt truth split between in-memory flags and meta rows | halt families are answered by `entry_budget`/`entries_allowed` from book state read per cycle; operator stop/rearm are postings the live process consumes on the next cycle — one source of truth |

**Not fixed by v2 (out of scope, stated explicitly):** venue-book divergence entries (2026-08-26, 0.71/0.40 stale book) are execution-path failures; the four-layer book hardening remains the defense. Oracle *disagreement* itself remains possible — v2 only guarantees the correction is posted exactly once.

Key files for the implementer: `/Users/ttoomm/Documents/PolyMomentum/rust_engine/src/risk/manager.rs` (v1, untouched), `/Users/ttoomm/Documents/PolyMomentum/rust_engine/src/risk/book_v2.rs` (new), `/Users/ttoomm/Documents/PolyMomentum/rust_engine/src/live/pipeline.rs` (call-site refactor to the trait), `/Users/ttoomm/Documents/PolyMomentum/rust_engine/src/config.rs` (RISK_BOOK / RISK_BOOK_SHADOW envs), `/Users/ttoomm/Documents/PolyMomentum/rust_engine/src/execution/sizing.rs` (share-quantum helpers reused by SizingPolicy outputs).