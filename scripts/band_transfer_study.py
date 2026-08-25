#!/usr/bin/env python3
"""Transfer test of the FROZEN band mechanism onto sibling 5m candle markets.

Hypothesis (fixed verbatim from signal_favorite_band_official_v1, zero
per-asset tuning): at 240s into a <asset>-updown-5m window, the momentum-side
token bought at an executed price in (0.55, 0.92] carries positive
fee-adjusted edge under official resolutions.

Because nothing is tuned, every historic window is fair evidence. Reporting
still splits by hour-comb and day so instability shows up. Instrument is the
discovery instrument: Binance 1s opens (checksum-verified dailies), Gamma
official resolutions, data-api executed prints.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from adaptation_persistence_study import http_json, taker_fee, wilson_lo  # noqa: E402

GAMMA = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
DECISION_OFFSET_S = 240
ENTRY_WINDOW_S = 30
WINDOW_S = 300
BAND_LO, BAND_HI = 0.55, 0.92

ASSETS = {
    "eth": ("ETHUSDT", "eth-updown-5m"),
    "sol": ("SOLUSDT", "sol-updown-5m"),
    "xrp": ("XRPUSDT", "xrp-updown-5m"),
}


def load_opens(zip_dir: Path, symbol: str, days: list[str]) -> dict[int, float]:
    import csv
    import io
    import zipfile

    opens: dict[int, float] = {}
    for day in days:
        zp = zip_dir / f"{symbol}-1s-{day}.zip"
        if not zp.exists():
            print(f"WARN missing {zp}", file=sys.stderr)
            continue
        with zipfile.ZipFile(zp) as z:
            with z.open(z.namelist()[0]) as f:
                for row in csv.reader(io.TextIOWrapper(f, "utf-8")):
                    if not row or not row[0].isdigit():
                        continue
                    ts = int(row[0])
                    while ts > 10**11:
                        ts //= 1000
                    opens[ts] = float(row[1])
    return opens


def study_window(slug_prefix: str, ws: int, opens: dict[int, float], pause_s: float, cache_dir: Path) -> dict | None:
    cache = cache_dir / f"{ws}.json"
    if cache.exists():
        return json.loads(cache.read_text())
    p_open, p_dec = opens.get(ws), opens.get(ws + DECISION_OFFSET_S)
    if p_open is None or p_dec is None:
        return None
    row: dict
    if p_dec == p_open:
        row = {"window_start": ws, "status": "no_signal"}
    else:
        signal = "up" if p_dec > p_open else "down"
        markets = http_json(f"{GAMMA}/markets?slug={slug_prefix}-{ws}&closed=true")
        time.sleep(pause_s)
        if not markets:
            row = {"window_start": ws, "status": "market_not_found"}
        else:
            m = markets[0]
            if m.get("umaResolutionStatus") != "resolved":
                row = {"window_start": ws, "status": "unresolved"}
            else:
                outcomes = json.loads(m["outcomes"]) if isinstance(m.get("outcomes"), str) else m["outcomes"]
                prices = json.loads(m["outcomePrices"]) if isinstance(m.get("outcomePrices"), str) else m["outcomePrices"]
                tokens = json.loads(m["clobTokenIds"]) if isinstance(m.get("clobTokenIds"), str) else m["clobTokenIds"]
                cid = m.get("conditionId")
                by_name = {str(o).lower(): (t, float(p)) for o, t, p in zip(outcomes, tokens, prices)}
                winners = [n for n, (_, p) in by_name.items() if p > 0.5]
                if len(winners) != 1 or not cid:
                    row = {"window_start": ws, "status": "no_unique_winner"}
                else:
                    params = urllib.parse.urlencode({"market": cid, "limit": 500})
                    trades = http_json(f"{DATA_API}/trades?{params}")
                    time.sleep(pause_s)
                    decision_ts = ws + DECISION_OFFSET_S
                    signal_token = by_name.get(signal, (None, 0.0))[0]
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
                        "official": winners[0],
                        "signal_entry": entry,
                        "won": winners[0] == signal,
                    }
    tmp = cache.with_suffix(f".tmp.{ws}")
    tmp.write_text(json.dumps(row) + "\n")
    tmp.rename(cache)
    return row


def summarize(rows: list[dict]) -> dict:
    sel = [r for r in rows if r.get("status") == "ok" and r.get("signal_entry") is not None
           and BAND_LO < r["signal_entry"] <= BAND_HI]
    n = len(sel)
    w = sum(r["won"] for r in sel)
    be = sum(r["signal_entry"] + taker_fee(r["signal_entry"]) for r in sel) / n if n else None
    wl = wilson_lo(w, n)
    return {
        "band_n": n,
        "wins": w,
        "win_rate": round(w / n, 4) if n else None,
        "avg_break_even": round(be, 4) if be is not None else None,
        "point_edge": round(w / n - be, 4) if n and be is not None else None,
        "wilson_edge": round(wl - be, 4) if n and be is not None else None,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asset", choices=sorted(ASSETS), required=True)
    ap.add_argument("--zip-dir", default="data/binance_1s_alts")
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--pause-s", type=float, default=0.12)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    symbol, slug_prefix = ASSETS[args.asset]
    cache_dir = Path(f"logs/strategy-research/band-transfer/{args.asset}")
    cache_dir.mkdir(parents=True, exist_ok=True)
    opens = load_opens(Path(args.zip_dir), symbol, args.days)

    rows, skipped = [], {}
    for day in args.days:
        base = dt.datetime.fromisoformat(day + "T00:00:00+00:00")
        for ws in range(int(base.timestamp()), int(base.timestamp()) + 86400, WINDOW_S):
            res = study_window(slug_prefix, ws, opens, args.pause_s, cache_dir)
            if res is None:
                continue
            if res["status"] != "ok":
                skipped[res["status"]] = skipped.get(res["status"], 0) + 1
                continue
            res["day"] = day
            res["hour"] = dt.datetime.utcfromtimestamp(res["window_start"]).hour
            rows.append(res)
        print(f"{args.asset} {day}: usable={sum(1 for r in rows if r.get('day') == day)}", flush=True)

    primary = [r for r in rows if r["hour"] % 4 == 1]   # 01/05/09/13/17/21
    disjoint = [r for r in rows if r["hour"] % 4 == 3]  # 03/07/11/15/19/23
    by_day = {}
    for r in rows:
        e = r.get("signal_entry")
        if r["status"] == "ok" and e is not None and BAND_LO < e <= BAND_HI:
            d = by_day.setdefault(r["day"], [0, 0])
            d[0] += 1
            d[1] += r["won"]

    result = {
        "schema_version": 1,
        "registration": f"band_transfer_{args.asset}_20260825",
        "hypothesis": "FROZEN signal_favorite_band_official_v1 mechanism, zero per-asset tuning",
        "days": args.days,
        "usable_rows": len(rows),
        "skipped": skipped,
        "all": summarize(rows),
        "primary_comb": summarize(primary),
        "disjoint_comb": summarize(disjoint),
        "per_day_band": {d: {"n": v[0], "wins": v[1]} for d, v in sorted(by_day.items())},
        "rows": rows,
    }
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(result, f, indent=1)
        f.write("\n")
    tmp.rename(out)
    print(json.dumps({k: result[k] for k in ("usable_rows", "skipped", "all", "primary_comb", "disjoint_comb")}, indent=1))


if __name__ == "__main__":
    main()
