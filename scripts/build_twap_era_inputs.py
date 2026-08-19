#!/usr/bin/env python3
"""Build partial_twap_lock_v1 inputs from checksum-verified Binance 1s dailies.

Produces, for the amended TWAP-era data plan
(docs/partial_twap_lock_v1_preregistration_2026-08-17.md, amendment of
2026-08-17: PMXT archive coverage ends at 2026-08-10T00):

1. causal windows JSONL (--windows-out): one row per 5-minute window over
   [--span-start, --span-end), fields {chronological_window, p0, p60, p120,
   p180, p240, utc_day, utc_hour, window_start}. Prices are 1s-kline OPENS
   at the exact offset second (causal: the price in force at that instant).
   Era tags by fixed calendar boundaries declared before any label exists:
       window_start <  2026-08-09T00:00Z  -> older
       window_start <  2026-08-09T17:00Z  -> recent_discovery
       otherwise                          -> fresh_holdout
2. settlement tape CSV (--tape-out): timestamp_ms,price at 1s resolution
   (kline open at open_time), for TWAP labels via
   `opportunity-labels --resolution-rule twap_vs_open`.

Outcome safety: this script reads only public BTC price data; it never
touches Polymarket books, Gamma prices, or resolutions.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import zipfile
from pathlib import Path

DEFAULT_OLDER_BOUNDARY = "2026-08-09T00:00:00Z"
DEFAULT_DISCOVERY_BOUNDARY = "2026-08-09T17:00:00Z"


def parse_rfc(ts: str) -> int:
    return int(dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())


def era_tag(window_start: int, older_before: int, discovery_before: int) -> str:
    if window_start < older_before:
        return "older"
    if window_start < discovery_before:
        return "recent_discovery"
    return "fresh_holdout"


def load_1s_opens(zip_paths: list[Path]) -> dict[int, float]:
    """second-epoch -> kline open price."""
    opens: dict[int, float] = {}
    for zp in zip_paths:
        with zipfile.ZipFile(zp) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                reader = csv.reader(io.TextIOWrapper(f, "utf-8"))
                for row in reader:
                    if not row or not row[0].isdigit():
                        continue
                    open_time = int(row[0])
                    # Binance archives use ms or µs depending on range;
                    # reduce to epoch seconds regardless of source unit.
                    while open_time > 10**11:
                        open_time //= 1000
                    opens[open_time] = float(row[1])
    return opens


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--zips", nargs="+", required=True)
    ap.add_argument("--span-start", required=True, help="RFC3339, inclusive")
    ap.add_argument("--span-end", required=True, help="RFC3339, exclusive")
    ap.add_argument("--windows-out", required=True)
    ap.add_argument("--tape-out", required=True)
    ap.add_argument("--older-before", default=DEFAULT_OLDER_BOUNDARY,
                    help="windows before this are tagged older")
    ap.add_argument("--discovery-before", default=DEFAULT_DISCOVERY_BOUNDARY,
                    help="windows before this (and >= older-before) are recent_discovery; later ones fresh_holdout")
    args = ap.parse_args()
    older_before = parse_rfc(args.older_before)
    discovery_before = parse_rfc(args.discovery_before)

    span_start = int(dt.datetime.fromisoformat(args.span_start.replace("Z", "+00:00")).timestamp())
    span_end = int(dt.datetime.fromisoformat(args.span_end.replace("Z", "+00:00")).timestamp())
    assert span_start % 300 == 0 and span_end % 300 == 0, "span must align to 5m"

    opens = load_1s_opens([Path(p) for p in args.zips])

    # Fail-closed continuity: every second in the span must be present.
    missing = [s for s in range(span_start, span_end) if s not in opens]
    if missing:
        raise SystemExit(
            f"settlement tape has {len(missing)} missing seconds in span "
            f"(first: {missing[0]}); refusing to emit a gappy TWAP source"
        )

    windows = []
    for ws in range(span_start, span_end, 300):
        start = dt.datetime.fromtimestamp(ws, dt.timezone.utc)
        row = {
            "chronological_window": era_tag(ws, older_before, discovery_before),
            "p0": opens[ws],
            "p60": opens[ws + 60],
            "p120": opens[ws + 120],
            "p180": opens[ws + 180],
            "p240": opens[ws + 240],
            "utc_day": start.date().isoformat(),
            "utc_hour": start.hour,
            "window_start": ws,
        }
        windows.append(row)

    wout = Path(args.windows_out)
    wout.parent.mkdir(parents=True, exist_ok=True)
    tmp = wout.with_suffix(wout.suffix + ".tmp")
    with open(tmp, "w") as f:
        for row in windows:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
    tmp.rename(wout)

    tout = Path(args.tape_out)
    tout.parent.mkdir(parents=True, exist_ok=True)
    tmp = tout.with_suffix(tout.suffix + ".tmp")
    with open(tmp, "w") as f:
        f.write("timestamp_ms,price\n")
        for s in range(span_start, span_end):
            f.write(f"{s * 1000},{opens[s]}\n")
    tmp.rename(tout)

    tags = {}
    for row in windows:
        tags[row["chronological_window"]] = tags.get(row["chronological_window"], 0) + 1
    print(json.dumps({
        "windows": len(windows),
        "tape_rows": span_end - span_start,
        "era_counts": tags,
        "windows_out": str(wout),
        "tape_out": str(tout),
    }, indent=1))


if __name__ == "__main__":
    main()
