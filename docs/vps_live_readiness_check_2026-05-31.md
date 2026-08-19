# VPS Live Readiness Check - 2026-05-31

Scope: stage the current A+ binary/artifact on the Dublin VPS, restart only
PolyMomentum in paper mode, and verify that real-money live mode remains
fail-closed until funding and live toggles are explicitly enabled.

## Staged Build

- Branch: `codex/audit1`
- Commit: `2ac12dc4279abcce036d0b35cc8f711108a68f5b`
- GitHub release tag: `ci-codex-audit1`
- Release asset SHA-256:
  `fca22ddcecc492c4db582a81be47af56bae99d89cdce7a63e9e78cbc9f1a3ff6`
- VPS binary:
  `/opt/polymomentum/polymomentum-engine`
- VPS binary SHA-256:
  `fca22ddcecc492c4db582a81be47af56bae99d89cdce7a63e9e78cbc9f1a3ff6`
- Promotion artifact:
  `/opt/polymomentum/config/promotion_candidate_a_plus5m_guard_may23_25_20260531.json`

The first deployment attempt was intentionally not used for restart because the
release asset still pointed at `2042ced`. After the CI build completed, the
release target and binary were rechecked and restaged before restart.

## Paper Runtime Result

Paper preflight passed on the VPS:

- `ok=true`
- `mode=paper`
- `venue=paper_only`
- release manifest git SHA:
  `2ac12dc4279abcce036d0b35cc8f711108a68f5b`
- promotion status: `ok`
- promoted strategy hash:
  `3d0fb98e2712141db03166d059e732afdc7576090fac9be065eead926e98ee55`
- promoted sample: 157 trades, win rate `0.8853503184713376`,
  total PnL `79.41254`
- `CANDLE_SETTLEMENT_ALIGNMENT_READY=true`
- `CANDLE_WINDOW_MINUTES=5`
- `BANKROLL_USD=100.00`
- `live_safeguard=ok`: paper mode does not initialize live CLOB order placement

After restart, the running service was:

```text
/opt/polymomentum/polymomentum-engine live --mode paper --promotion-artifact /opt/polymomentum/config/promotion_candidate_a_plus5m_guard_may23_25_20260531.json
```

The daemon started with `git_sha=2ac12dc...` and
`strategy_hash=3d0fb98e...`. A short post-restart diagnostics window showed:

- active/running service
- `NRestarts=0`
- memory about 19 MB after restart
- no warning-or-higher journal entries since restart
- fresh session log:
  `/opt/polymomentum/logs/sessions/session_20260531_121216.jsonl`
- 562 session events after the bounded check
- cycle latencies in the observed window were sub-millisecond to low
  single-digit milliseconds
- one restored paper position resolved with oracle agreement

No live or canary order was placed.

## Read-Only Venue Checks

Read-only CLOB diagnostics succeeded:

- `clob ok`: returned `OK`
- `clob time`: returned `1780229497`
- signing/wallet address:
  `0xe0ab9972e6ac14c29c06699fb0096a83f2a931ba`

## Live Preflight Result

Live preflight correctly failed closed. Passing checks included:

- live mode confirmation flag supplied
- required CLOB credentials present
- alert webhook configured
- promoted taker strategy does not require maker enablement
- promotion artifact valid and hash-matched

Blocking checks:

- `VENUE=paper_only` refuses real-money live mode
- `CLOB_V2_READY=1` is not set
- `POLYMOMENTUM_LIVE_RECONCILIATION_READY=1` is not set
- wallet is not live-ready

Wallet state:

```json
{
  "address": "0xe0ab9972e6ac14c29c06699fb0096a83f2a931ba",
  "live_ready": false,
  "pol": 5.288105663117419,
  "pusd": 0.88363,
  "pusd_allowance_exchange": 0.88363,
  "pusd_allowance_neg_risk_exchange": 0.0,
  "stable_total": 0.88363,
  "usdc_e": 0.0,
  "usdc_native": 0.0
}
```

With the current configured live order budget of `$10.00`, preflight requires
pUSD and both CTF Exchange V2 allowances to be at least `$11.00`.

## Peer And Resource Status

Post-restart VPS status:

- `adgts`: active/running, `NRestarts=0`
- `polyarbitrage`: inactive/dead
- `polyarbitrage-collector`: active/running, `NRestarts=0`
- disk `/` and `/opt/shared`: 72 GB total, 56 GB used, 14 GB available, 81%
- memory: about 4.0 GiB available
- load average after check: `0.32, 0.79, 0.76`

No shared-cache deletion or peer-private directory access was performed.

## Remaining Live Gate

Before any capital-bearing run:

1. Fund/convert enough pUSD for the intended first-run budget.
2. Approve both CTF Exchange V2 pUSD allowances, including NegRisk.
3. Set `VENUE=clob`, `CLOB_V2_READY=1`, and
   `POLYMOMENTUM_LIVE_RECONCILIATION_READY=1` only after the operator accepts
   the venue/reconciliation evidence.
4. Rerun live preflight and require `ok=true`.
5. Get explicit approval for a bounded canary. Start with the smallest allowed
   budget and keep diagnostics/session logging enabled.
