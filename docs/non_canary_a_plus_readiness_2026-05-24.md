# Non-Canary A+ Readiness - 2026-05-24

Scope: completed every production-readiness step that can be proven without a
live canary. No live trading process was started, no service was restarted, and
peer bot private directories were not touched.

## Offline Gate

Final audit evidence:

- `deploy/promotions/evidence/strategy_builder_audit_clean_continuous_grid_early_stresscap024_20260523T03_20260524T02.json`
- Result: `ok=true`, `grade=A+`, `a_plus_ready=true`, `warnings=0`, `failures=0`

The final replay was run from a clean temporary worktree at commit
`9f5862a9ed6571e19e65ce1e53a0c94e97b48d0d`, avoiding the dirty local source
tree.

Key replay metrics:

- PMXT events loaded/processed: `65,528,876`
- BTC five-minute contracts: `288`
- Orders submitted: `126`
- Fills: `70`
- Passive non-fills: `56` (`maker_unfilled=45`, `post_only_cross=11`)
- Critical rejects: `0`
- Resolutions: `70`
- Wins/losses: `57/13`
- PnL: `+51.43`
- Oracle checks: `70`
- Oracle disagreements: `0`
- Circuit breaker: not tripped

Timestamp causality evidence:

- `deploy/promotions/evidence/causality_clean_continuous_grid_early_stresscap024_20260523T03_20260524T02.json`
- Result: `ok=true`, `order_timings=126`, `resolution_timings=70`,
  `violations=[]`, `warnings=[]`

Adaptive breaker probe:

- `deploy/promotions/evidence/harness_sweep_adaptive_probe_continuous_grid_early_stresscap024_current_20260523_24.json`
- Exact candidate cell with `--adaptive-health-rearm-minutes 15`
- Best maker variant unchanged: `70` trades, PnL `+51.43`, breaker not
  tripped, `adaptive_rearms=0`
- Variants with rearm: `0`

## VPS Staging

Staged path:

`/opt/polymomentum/staging/codex-audit1-9f5862a/`

Staged binary:

- Git SHA: `9f5862a9ed6571e19e65ce1e53a0c94e97b48d0d`
- Build timestamp: `2026-05-24T14:36:25Z`
- SHA256: `bb7d4f27ee429c6ff9e3fa638e8b53619e20d9bddb173a4ae7b58e9d1ff72e14`

Staged promotion:

`promotion_candidate_continuous_grid_early_stresscap024_aggregate_20260520_24.json`

Paper preflight evidence:

- `deploy/promotions/evidence/vps_staged_paper_preflight_codex_audit1_9f5862a_20260524.json`
- Result: `ok=true`, `warnings=[]`, `failures=[]`

Live preflight evidence:

- `deploy/promotions/evidence/vps_staged_live_preflight_codex_audit1_9f5862a_20260524.json`
- Result: fail-closed as expected

Remaining live blockers:

- `VENUE=paper_only`
- `CLOB_V2_READY=0`
- `POLYMOMENTUM_LIVE_RECONCILIATION_READY=0`
- `POLYMOMENTUM_VENUE_COMPLIANCE_OK=0`
- Wallet/budget gate: observed `pUSD=$0.88`, `CTF_V2_allow=$0.88`,
  `NegRisk_allow=$0.00`, `POL=5.2881`; configured live order budget requires
  pUSD and both CTF Exchange V2 allowances `>= $11.00`

`LIVE_ALLOW_MAKER_ORDERS=1` is already set on the VPS.

## Resource And Cleanup Notes

VPS checks were read-only. Observed:

- Root disk: `33G` used of `72G` (`48%`)
- PolyMomentum service: active paper mode under `CPUQuotaPerSecUSec=800ms`,
  `MemoryMax=536870912`
- adgts and polyarbitrage-related processes were observed running

Temporary local PMXT/replay cache was deleted after evidence generation:

`/private/tmp/polymomentum_final_gate_20260524T1500Z`

Temporary clean worktree was removed:

`/private/tmp/polymomentum_clean_9f5862a`

## Next Step

Do not start canary until the live blockers above are intentionally cleared.
The next non-code action is to convert/fund pUSD and grant both CTF Exchange V2
allowances for at least the configured order budget plus buffer, then rerun the
same staged live preflight.
