# PolyMomentum

Single-strategy bot trading **"Up or Down" 5/15-min crypto candle markets on Polymarket**, written in Rust. Multi-exchange momentum signal → BS-binary fair-value mispricing → CLOB execution.

> **Status (2026-07-14):** Rust-only engine. Offline execution, inventory, preflight, diagnostics, and strategy-builder gates are strong, but no strategy registry entry is live-ready. Live trading remains fail-closed behind explicit preflight, CLOB v2, reconciliation, wallet, and operator confirmation gates.

## What it does

For each active candle window:

1. **Subscribe** to Binance, Bybit, OKX (BTC) plus Binance/Bybit (ETH+SOL) spot WS feeds; pull Deribit IV every 60 s.
2. **Subscribe** to Polymarket's `/ws/market` book channel for every active candle's YES/NO tokens; resubscribe automatically when the contract scanner refreshes.
3. **Detect momentum** via `MomentumDetector`: z-score of the move from window-open against EWMA fast/slow realized vol, weighted with consistency + reversion penalty.
4. **Compute BS fair value** of the binary "above strike" using observed Deribit IV + window time-to-expiry.
5. **Decide** through `decide_candle_trade`: 4-zone gates (early / primary / late / terminal) with independent confidence/z/edge thresholds, dead-zone filter, entry-price EV gate, and an `edge_cap` brake against stale-data signals (relaxed in terminal zone).
6. **Hold to resolution**, mark won/lost vs our BTC tape, and cross-check against Polymarket's on-chain CTF resolution.

Zone gates split the window into bands with independent thresholds. Current registry evidence is fail-closed: tail-guard and primary-only challengers are useful research hypotheses, but none has passed full chronological, tail-risk, and freshest-window promotion gates.

## Layout

```
PolyMomentum/
├── rust_engine/             # the entire bot
│   ├── src/
│   │   ├── main.rs          # CLI parsing and command orchestration
│   │   ├── lib.rs           # module re-exports
│   │   ├── strategy_builder.rs # replay-first strategy research and promotion gates
│   │   ├── config.rs        # env-driven Settings
│   │   ├── data/            # gamma client, scanner, ctf reader, wallet, models
│   │   ├── strategy/        # momentum detector, decide_candle_trade
│   │   ├── fair_value.rs    # Black-Scholes binary pricer
│   │   ├── execution/fees.rs
│   │   ├── risk/manager.rs  # SQLite RiskManager (state.db)
│   │   ├── monitoring/      # JSONL session writer + Slack alerter
│   │   ├── live/            # cycle loop, paper resolver, oracle verifier, breaker
│   │   ├── polymarket_ws.rs # full L2 book WS feed
│   │   ├── exchange.rs      # multi-exchange spot WS aggregator
│   │   ├── price_state.rs
│   │   ├── clob.rs          # CLOB direct order placement (live mode)
│   │   └── signing.rs       # EIP-712 order signing
│   └── Cargo.toml
├── deploy/                  # Rust-only deploy + systemd
├── docs/                    # docs (RUST_PORT_PLAN.md, peer-bot notes)
└── data/                    # local cache (gitignored)
```

## Subcommands

Run `polymomentum-engine --help` or `polymomentum-engine <command> --help` for the
complete, authoritative command surface. Primary operational commands include:

```bash
polymomentum-engine live --mode paper           # main runtime (default)
polymomentum-engine preflight --mode paper      # startup/deploy checks
polymomentum-engine release-manifest --mode paper
polymomentum-engine live --mode live --i-understand-live   # real money; also requires VENUE/compliance env
polymomentum-engine scan                        # Gamma + scanner smoke test
polymomentum-engine wallet                      # pUSD/USDC diagnostics + POL balance
polymomentum-engine ctf <condition_id>          # on-chain CTF resolution read
polymomentum-engine validate-replay <session.jsonl>   # parity check
```

## Operational commands

```bash
# Health
ssh vps 'systemctl is-active polymomentum-engine adgts polyarbitrage'

# Live cycle log
ssh vps 'journalctl -u polymomentum-engine -f -n 5 | grep candle.cycle'

# Trade tape
ssh vps 'journalctl -u polymomentum-engine | grep -E "candle.trade|candle.resolved"'

# Kill switch (halts trading within ~100ms)
ssh vps 'sudo touch /opt/polymomentum/KILL'
# resume:
ssh vps 'sudo rm /opt/polymomentum/KILL && \
         sqlite3 /opt/polymomentum/logs/candle/state.db \
                 "DELETE FROM meta WHERE key=\"candle_breaker_tripped\"" && \
         systemctl restart polymomentum-engine'

# Deploy (build, ship, restart) — paper mode default
bash deploy/deploy.sh vps --enable-service --mode paper

# Live deployment is intentionally fail-closed. Do not sed the unit by hand:
# configure VENUE/OPERATOR_COUNTRY/compliance env first, then redeploy.
bash deploy/deploy.sh vps --enable-service --mode live --i-understand-live
```

## Replay-grade data collection

Paper-mode design goal: paper run logs replayable through the same decision function, with **identical PnL**, AND paper fills representative of live fills.

- **Per-evaluation JSONL** (`/opt/polymomentum/logs/sessions/session_*.jsonl`) — `cat=signal type=evaluation` events fire on every contract evaluation (trade or skip), with full state: open price, current price, z-score, confidence, EWMA fast/slow vol, cross-asset boost, top of book, decision zone, fair value, edge, traded flag, skip reason + detail.
- **CTF oracle cross-check** — `cat=oracle type=resolution` events compare our BTC-tape resolution to Polymarket's CCIX-driven settlement, read directly from the on-chain `ConditionalTokens` contract via `eth_call`.
- **Persistent state** — SQLite at `/opt/polymomentum/logs/candle/state.db` survives deploys; `paper_positions`, `oracle_pending`, `meta`, `trades`, `state` tables.

Validate a paper session against the decision function:

```bash
polymomentum-engine validate-replay /opt/polymomentum/logs/sessions/session_<ts>.jsonl
```

Exit 0 = clean, 1 = decision drift.

## Tests

```bash
cd rust_engine
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked       # full library and CLI suite
cargo build --release --locked --bin polymomentum-engine
```

## Multibot etiquette

PolyMomentum shares the VPS (`193.24.234.202`, alias `vps`) with **adgts** (XRP/USDT futures grid, port 9092) and **polyarbitrage** (port 127.0.0.1:9090). Don't touch their `/opt/<name>`, `/etc/<name>`, or systemd units. Coexistence caps applied via `polymomentum-engine.service`: `Nice=5`, `CPUQuota=80%`, `MemoryMax=512M`, `TasksMax=256`, and read-only `/opt/shared` access for the live service. `deploy/deploy.sh --enable-service` checks peer service state before restarting PolyMomentum and refuses to restart while a peer unit is deactivating. Use `nice -n 10 cargo build --release` for builds on the VPS. See [docs/cross_bot_note_mexc_hardening.md](docs/cross_bot_note_mexc_hardening.md) for the cross-Claude coordination protocol.

## Strategy reality check

Current strategy-builder evidence is stronger than the original April paper loop, but still not sufficient for capital. The latest registry marks the live candidates as `rejected`, `questionable`, or `dead_end`; the next productive work is strict backtest/live-replay research on fresh fully resolved windows, not paper validation unless the question is live venue plumbing.

---

*Repository renamed from PolyCrossArb → PolyMomentum on 2026-04-13 when the cross-arb and weather strategies were deleted. Python implementation removed on 2026-04-26 after the Rust port reached production-ready paper mode.*
