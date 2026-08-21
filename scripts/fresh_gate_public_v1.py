#!/usr/bin/env python3
"""One-shot fresh gate for signal_favorite_band_official_v1 on the public
discovery instrument, per docs/signal_favorite_band_official_v1_gate_amendment_2026-08-21.md.

Phase 1 (--count-support): caches market identity (token ids + outcome
NAMES only — outcomePrices is never parsed) and entry prints for every
post-freeze window, reports the selected band-entry count. Carries zero
outcome information.

Phase 2 (--consume): refuses if the consumed marker exists, requires
support >= 110 from the cache, writes the marker BEFORE the first
outcome fetch, then performs the single official-label read and emits
the verdict artifact.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from adaptation_persistence_study import http_json, load_opens, taker_fee, wilson_lo  # noqa: E402

GAMMA = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
DECISION_OFFSET_S = 240
ENTRY_WINDOW_S = 30
WINDOW_S = 300
BAND_LO, BAND_HI = 0.55, 0.92
FRESH_START_TS = 1_787_130_000  # 2026-08-19T09:00:00Z, per the amendment
SETTLE_BUFFER_S = 7200
SUPPORT_MIN = 110
WILSON_MARGIN = 0.02

CACHE_DIR = Path("logs/strategy-research/fresh-gate-public/cache")
OUTCOME_DIR = Path("logs/strategy-research/fresh-gate-public/outcomes")
MARKER = Path("logs/strategy-research/fresh_gate_public_v1.CONSUMED")
ARTIFACT = Path("logs/strategy-research/20260821_fresh_gate_public_v1.json")
ZIP_DIR = Path("data/binance_1s_twap_era")


def fetch_opens_rest(start_ts: int, end_ts: int, pause_s: float) -> dict[int, float]:
    """Binance 1s opens via klines REST for the tail day (same venue/series)."""
    opens: dict[int, float] = {}
    ts = start_ts
    while ts < end_ts:
        params = urllib.parse.urlencode(
            {"symbol": "BTCUSDT", "interval": "1s", "startTime": ts * 1000, "limit": 1000}
        )
        rows = None
        for host in ("https://data-api.binance.vision", "https://api.binance.com"):
            try:
                rows = http_json(f"{host}/api/v3/klines?{params}")
                break
            except Exception:
                continue
        if not rows:
            break
        for r in rows:
            opens[int(r[0]) // 1000] = float(r[1])
        ts = int(rows[-1][0]) // 1000 + 1
        time.sleep(pause_s)
    return opens


def load_all_opens(end_ts: int, pause_s: float) -> dict[int, float]:
    opens: dict[int, float] = {}
    day = dt.datetime.fromtimestamp(FRESH_START_TS, dt.timezone.utc).date()
    last_day = dt.datetime.fromtimestamp(end_ts + DECISION_OFFSET_S, dt.timezone.utc).date()
    rest_from = None
    while day <= last_day:
        if (ZIP_DIR / f"BTCUSDT-1s-{day.isoformat()}.zip").exists():
            opens.update(load_opens(ZIP_DIR, [day.isoformat()]))
        else:
            rest_from = rest_from or day
        day += dt.timedelta(days=1)
    if rest_from is not None:
        start = int(dt.datetime.combine(rest_from, dt.time(), dt.timezone.utc).timestamp())
        opens.update(fetch_opens_rest(start, end_ts + DECISION_OFFSET_S + 1, pause_s))
    return opens


def parse_identity(market: dict) -> dict | None:
    """Token ids + outcome NAMES only. outcomePrices is deliberately never read."""
    condition_id = market.get("conditionId")
    outcomes = market.get("outcomes")
    tokens = market.get("clobTokenIds")
    outcomes = json.loads(outcomes) if isinstance(outcomes, str) else outcomes
    tokens = json.loads(tokens) if isinstance(tokens, str) else tokens
    if not condition_id or not outcomes or not tokens or len(tokens) != 2:
        return None
    return {
        "condition_id": condition_id,
        "token_by_name": {str(o).lower(): t for o, t in zip(outcomes, tokens)},
    }


def cache_window(ws: int, opens: dict[int, float], pause_s: float) -> dict:
    path = CACHE_DIR / f"{ws}.json"
    if path.exists():
        return json.loads(path.read_text())
    p_open, p_dec = opens.get(ws), opens.get(ws + DECISION_OFFSET_S)
    if p_open is None or p_dec is None:
        row = {"window_start": ws, "status": "no_btc_opens"}
    elif p_dec == p_open:
        row = {"window_start": ws, "status": "no_signal"}
    else:
        signal = "up" if p_dec > p_open else "down"
        markets = http_json(f"{GAMMA}/markets?slug=btc-updown-5m-{ws}&closed=true")
        time.sleep(pause_s)
        ident = parse_identity(markets[0]) if markets else None
        if ident is None:
            row = {"window_start": ws, "status": "market_not_found" if not markets else "identity_incomplete"}
        else:
            params = urllib.parse.urlencode({"market": ident["condition_id"], "limit": 500})
            trades = http_json(f"{DATA_API}/trades?{params}")
            time.sleep(pause_s)
            decision_ts = ws + DECISION_OFFSET_S
            signal_token = ident["token_by_name"].get(signal)
            entry = None
            for t in sorted(trades or [], key=lambda x: x.get("timestamp", 0)):
                if t.get("side") != "BUY" or t.get("asset") != signal_token:
                    continue
                ts = int(t.get("timestamp", 0))
                if decision_ts <= ts <= decision_ts + ENTRY_WINDOW_S:
                    entry = float(t["price"])
                    break
            row = {
                "window_start": ws,
                "status": "ok",
                "signal": signal,
                "condition_id": ident["condition_id"],
                "token_by_name": ident["token_by_name"],
                "signal_entry": entry,
            }
    tmp = path.with_suffix(f".tmp.{ws}")
    tmp.write_text(json.dumps(row) + "\n")
    tmp.rename(path)
    return row


def selected(row: dict) -> bool:
    e = row.get("signal_entry")
    return row.get("status") == "ok" and e is not None and BAND_LO < e <= BAND_HI


def end_boundary(now_ts: int) -> int:
    last = (now_ts - SETTLE_BUFFER_S - WINDOW_S) // WINDOW_S * WINDOW_S
    return last


def count_support(pause_s: float) -> tuple[int, list[dict], dict[str, int]]:
    now_ts = int(time.time())
    end_ws = end_boundary(now_ts)
    opens = load_all_opens(end_ws, pause_s)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    rows, skipped = [], {}
    for ws in range(FRESH_START_TS, end_ws + 1, WINDOW_S):
        row = cache_window(ws, opens, pause_s)
        if row["status"] == "ok":
            rows.append(row)
        else:
            skipped[row["status"]] = skipped.get(row["status"], 0) + 1
    n_sel = sum(1 for r in rows if selected(r))
    return n_sel, rows, skipped


def consume(pause_s: float) -> None:
    """Single logical outcome read. The marker freezes the sample; a rerun is
    allowed ONLY to complete an interrupted read (artifact absent or
    READ_INCOMPLETE) over that same frozen sample — never a second look."""
    if MARKER.exists():
        if ARTIFACT.exists():
            prior = json.loads(ARTIFACT.read_text())
            if prior.get("verdict") != "READ_INCOMPLETE":
                sys.exit("REFUSED: fresh gate already consumed with a final verdict.")
        sel_ws = json.loads(MARKER.read_text())["selected_windows"]
        sel = [json.loads((CACHE_DIR / f"{ws}.json").read_text()) for ws in sel_ws]
    else:
        cached = [json.loads(p.read_text()) for p in sorted(CACHE_DIR.glob("*.json"))]
        sel = [r for r in cached if selected(r)]
        if len(sel) < SUPPORT_MIN:
            sys.exit(f"REFUSED: support {len(sel)} < {SUPPORT_MIN}. Run --count-support later.")
        with open(MARKER, "x") as f:
            json.dump({
                "consumed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "fresh_start_ts": FRESH_START_TS,
                "last_window_ts": max(r["window_start"] for r in sel),
                "support": len(sel),
                "selected_windows": sorted(r["window_start"] for r in sel),
                "amendment": "docs/signal_favorite_band_official_v1_gate_amendment_2026-08-21.md",
            }, f, indent=1)
            f.write("\n")

    OUTCOME_DIR.mkdir(parents=True, exist_ok=True)
    outcomes_read, unresolved = [], 0
    for r in sel:
        opath = OUTCOME_DIR / f"{r['window_start']}.json"
        if opath.exists():
            outcomes_read.append({**r, **json.loads(opath.read_text())})
            continue
        try:
            markets = http_json(f"{GAMMA}/markets?slug=btc-updown-5m-{r['window_start']}&closed=true")
        except Exception:
            unresolved += 1
            continue
        time.sleep(pause_s)
        m = markets[0] if markets else {}
        if m.get("umaResolutionStatus") != "resolved":
            unresolved += 1
            continue
        prices = m.get("outcomePrices")
        prices = json.loads(prices) if isinstance(prices, str) else prices
        names = m.get("outcomes")
        names = json.loads(names) if isinstance(names, str) else names
        winners = [str(o).lower() for o, p in zip(names, prices) if float(p) > 0.5]
        if len(winners) != 1:
            unresolved += 1
            continue
        rec = {"official": winners[0], "won": winners[0] == r["signal"]}
        tmp = opath.with_suffix(f".tmp.{r['window_start']}")
        tmp.write_text(json.dumps(rec) + "\n")
        tmp.rename(opath)
        outcomes_read.append({**r, **rec})

    n = len(outcomes_read)
    w = sum(r["won"] for r in outcomes_read)
    be = sum(r["signal_entry"] + taker_fee(r["signal_entry"]) for r in outcomes_read) / n if n else 0.0
    wl = wilson_lo(w, n)
    wilson_edge = wl - be
    point_edge = w / n - be if n else 0.0
    if unresolved > 0:
        verdict_str = "READ_INCOMPLETE"  # infrastructure gap, not a result: rerun --consume
    elif n >= SUPPORT_MIN and wilson_edge > WILSON_MARGIN and point_edge > 0:
        verdict_str = "PASS"
    else:
        verdict_str = "FAIL_TOMBSTONE"
    verdict = {
        "schema_version": 1,
        "registration": "fresh_gate_public_v1_20260821",
        "candidate": "signal_favorite_band_official_v1",
        "amendment": "docs/signal_favorite_band_official_v1_gate_amendment_2026-08-21.md",
        "fresh_range": [FRESH_START_TS, max(r["window_start"] for r in sel)],
        "support": n,
        "unresolved_excluded": unresolved,
        "wins": w,
        "win_rate": round(w / n, 4) if n else None,
        "avg_break_even": round(be, 4),
        "point_edge": round(point_edge, 4),
        "wilson_lo": round(wl, 4),
        "wilson_edge": round(wilson_edge, 4),
        "gate": f"wilson_edge > +{WILSON_MARGIN} AND point_edge > 0 AND n >= {SUPPORT_MIN}",
        "verdict": verdict_str,
        "rows": outcomes_read,
    }
    tmp = ARTIFACT.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(verdict, f, indent=1)
        f.write("\n")
    tmp.rename(ARTIFACT)
    print(json.dumps({k: verdict[k] for k in
                      ["support", "unresolved_excluded", "wins", "win_rate",
                       "avg_break_even", "point_edge", "wilson_edge", "verdict"]}, indent=1))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count-support", action="store_true")
    ap.add_argument("--consume", action="store_true")
    ap.add_argument("--pause-s", type=float, default=0.12)
    args = ap.parse_args()
    if args.count_support:
        n_sel, rows, skipped = count_support(args.pause_s)
        print(json.dumps({"selected_band_entries": n_sel, "usable_windows": len(rows),
                          "skipped": skipped, "support_min": SUPPORT_MIN,
                          "ready": n_sel >= SUPPORT_MIN}, indent=1))
    elif args.consume:
        consume(args.pause_s)
    else:
        ap.error("pick --count-support or --consume")


if __name__ == "__main__":
    main()
