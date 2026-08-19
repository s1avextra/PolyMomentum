# Complete-set lock v1 pre-registration

Status: `PRE_REGISTERED_REPLAY_ONLY_NOT_SCORED`  
Frozen: `2026-07-18T07:10:02Z`  
Accounting correction: `2026-07-18T07:47:52Z` (before any score)  
Collateral correction: `2026-07-18T08:08:09Z` (before any score)  
Strategy: `primary_v6_volfloor_300_complete_set_lock_v1`  
Live ready: no

## Decision

The next exact replay candidate keeps every entry rule in
`primary_v6_volfloor_300` and adds one lifecycle action. After a directional
entry fills, it may buy the same number of shares of the opposite outcome.
The order is allowed only when its submitted price ceiling makes the resulting
complete set worth at least `$0.10` more than both legs and the maximum possible
taker fees under that ceiling.

This is not a stop-loss heuristic. Once the missing leg fills in full, terminal
direction no longer affects payout: each matched Yes/No pair pays `$1`. The
mechanism also avoids the first-leg risk of a two-order arbitrage because the
directional entry already exists and only the missing leg is submitted.

Polymarket documents the crypto-market taker fee formula in
[Fees](https://docs.polymarket.com/trading/fees) and the conversion of equal
complementary positions into collateral in
[Merge Positions](https://docs.polymarket.com/trading/ctf/merge). Its batch
order endpoint says orders are processed in parallel, but does not document an
atomic all-or-none pair; therefore avoiding a new two-leg batch is a deliberate
execution constraint, not a claim that batch placement is atomic
([Post Multiple Orders](https://docs.polymarket.com/api-reference/trade/post-multiple-orders)).

## Frozen execution rule

For an entry fill with quantity `s`, price `p_entry`, and fee `f_entry`, walk
the opposite visible asks for exactly `s` shares. Let `p_limit` be the worst
visible ask rounded up to the venue tick. The order is eligible only if:

```text
locked_profit_floor = s
                    - s * p_entry
                    - f_entry
                    - s * p_limit
                    - fee(s, min(p_limit, 0.50), 0.07)

locked_profit_floor >= $0.10
```

Using the FOK ceiling instead of signal-time VWAP is intentional. During the
registered `202 ms` latency interval, the order may fill every share at that
ceiling. The binary fee curve peaks at `0.50`, so a ceiling above `0.50`
reserves the fee at `0.50` rather than the smaller fee at the ceiling. This
strict accounting correction was made before any score and does not change the
candidate parameters or hash. If full visible depth is unavailable, the book
is stale, the FOK does not fill, or the reported fill quantity does not exactly
reconcile, the lifecycle fails closed. It waits at least one second before
retrying an ordinary failed FOK; a fill-reconciliation mismatch trips the
backtest breaker. The first attempt is permitted after 15 seconds of holding
and no attempt is made in the final five seconds.

The maximum hedge spend plus its fee must fit cash that is not already
committed to open, submitted, or locked positions. That maximum amount is
reserved during the 202 ms in-flight interval. After a successful lock, the
replay keeps both legs' full fee-inclusive capital committed until the market
closes and only then realizes the guaranteed PnL. This pre-score correction
makes the implementation match the explicit no-immediate-merge assumption; it
does not change the candidate parameters or hash.

The frozen variant is
[`20260718_complete_set_lock_v1_variant.json`](../deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_variant.json).
The machine-readable evaluation contract is
[`20260718_complete_set_lock_v1_preregistration.json`](../deploy/promotions/evidence/strategy_registry/20260718_complete_set_lock_v1_preregistration.json).

## No-peeking evaluation contract

The previously examined 42-fold history may be used only as a diagnostic. It
earns no promotion credit. The current bounded July 18 capture can diagnose the
execution path, but no forward score is revealed below 100 admissible
exact-replay trades whose windows begin after the final pre-score correction. The
replay wrapper first requires 100 terminal conditions, then keeps candidate
reports and command output sealed until the candidate itself reaches 100
trades; an undersized run deletes those sealed files without exposing metrics.

The candidate must then pass two chronological, disjoint blocks of at least
100 trades each with unchanged code and parameters. In each block:

- every successful lock must realize at least `$0.10` after both fees;
- failed or partial hedges must leave terminal resolution unchanged;
- fee-inclusive total PnL must exceed the identical-entry baseline;
- both chronological halves must remain profitable;
- Wilson, profitable-report, loss-burst, tail, payoff, and reconciliation
  gates must all pass.

Failure rejects `complete_set_lock_v1`; the scored blocks will not be used to
retune its floor or timing.

## Promotion boundary and caveats

Even two forward passes do not authorize live trading. The live runtime does
not yet implement this lifecycle and release preflight rejects variants that
enable it. Live eligibility additionally requires exact FOK fill
reconciliation, current market-specific fees, and a verified redemption or
merge operating path.

Immediate on-chain merge is not assumed. Holding both legs locks collateral
until settlement. Gas and operational costs are excluded consistently with the
current terminal baseline and must be budgeted before promotion. A successful
lock also caps later directional upside. Finally, recent large-scale research
finds that single-market prediction-market arbitrage can be brief and
depth-constrained; that supports conservative execution but is not evidence
that this candidate will be profitable
([Saguillo et al., 2026](https://arxiv.org/abs/2605.00864)).

The verdict remains `KEEP_REPLAY_RESEARCH`, grade A-, and
`LIVE_TRADING_OFF` until the full contract passes.
