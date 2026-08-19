#!/usr/bin/env python3
"""Build a checksum-verified causal Binance spot/perpetual one-second tape."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import tempfile
import urllib.request
import zipfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


BASE_URL = "https://data.binance.vision/data"
SCHEMA_VERSION = "binance_cross_venue_tape_v1"
DAY_MS = 86_400_000


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        dir=path.parent, prefix=f"{path.name}.tmp."
    )
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as output:
            json.dump(value, output, indent=2, sort_keys=True)
            output.write("\n")
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def timestamp_ms(raw: str) -> int:
    value = int(raw)
    return value // 1_000 if value >= 10**15 else value


def dates_from_seal(path: Path) -> tuple[list[str], str]:
    raw = path.read_bytes()
    seal = json.loads(raw)
    if seal.get("schema_version") != "opportunity_dataset_v1":
        raise ValueError("unsupported opportunity dataset seal")
    if seal.get("outcome_columns_present") is not False:
        raise ValueError("dataset seal is not outcome-free")
    dates = sorted({entry["hour"][:10] for entry in seal["entries"]})
    if not dates:
        raise ValueError("dataset seal contains no hours")
    return dates, hashlib.sha256(raw).hexdigest()


def source_specs(date: str) -> dict[str, tuple[str, str, str]]:
    return {
        "spot": (
            f"{BASE_URL}/spot/daily/klines/BTCUSDT/1s/"
            f"BTCUSDT-1s-{date}.zip",
            f"BTCUSDT-spot-1s-{date}.zip",
            f"BTCUSDT-1s-{date}.zip",
        ),
        "perpetual": (
            f"{BASE_URL}/futures/um/daily/aggTrades/BTCUSDT/"
            f"BTCUSDT-aggTrades-{date}.zip",
            f"BTCUSDT-perp-aggTrades-{date}.zip",
            f"BTCUSDT-aggTrades-{date}.zip",
        ),
    }


def download_atomic(url: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        dir=output_path.parent, prefix=f"{output_path.name}.tmp."
    )
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "PolyMomentum/1"})
        with urllib.request.urlopen(request) as response, os.fdopen(
            handle, "wb"
        ) as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
        os.replace(temporary_name, output_path)
    except BaseException:
        try:
            os.close(handle)
        except OSError:
            pass
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def ensure_source(
    raw_dir: Path, url: str, local_name: str, download: bool
) -> tuple[Path, bool]:
    path = raw_dir / local_name
    checksum_path = path.with_suffix(path.suffix + ".CHECKSUM")
    if path.exists() and checksum_path.exists():
        return path, False
    if path.exists() or checksum_path.exists():
        raise ValueError(f"incomplete pre-existing archive pair: {path}")
    if not download:
        raise FileNotFoundError(f"missing source archive: {path}")
    download_atomic(url, path)
    try:
        download_atomic(f"{url}.CHECKSUM", checksum_path)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return path, True


def verified_member(archive_path: Path, expected_source_name: str) -> tuple[zipfile.ZipFile, str]:
    checksum_path = archive_path.with_suffix(archive_path.suffix + ".CHECKSUM")
    parts = checksum_path.read_text(encoding="utf-8").strip().split()
    if len(parts) != 2:
        raise ValueError(f"invalid checksum file: {checksum_path}")
    expected_hash, expected_name = parts
    if expected_name.lstrip("*") != expected_source_name:
        raise ValueError(f"checksum filename mismatch: {archive_path}")
    if sha256(archive_path) != expected_hash:
        raise ValueError(f"checksum mismatch: {archive_path}")
    archive = zipfile.ZipFile(archive_path)
    members = archive.namelist()
    if len(members) != 1 or not members[0].endswith(".csv"):
        archive.close()
        raise ValueError(f"expected exactly one CSV member: {archive_path}")
    return archive, members[0]


@dataclass(frozen=True)
class SpotSecond:
    close: str
    quote_volume: str
    taker_buy_quote_volume: str
    trade_count: int


@dataclass
class PerpetualSecond:
    close: str
    last_trade_ms: int
    quote_volume: Decimal
    taker_buy_quote_volume: Decimal
    trade_count: int
    aggregate_count: int


def load_spot_seconds(archive_path: Path, expected_name: str) -> dict[int, SpotSecond]:
    archive, member = verified_member(archive_path, expected_name)
    seconds: dict[int, SpotSecond] = {}
    try:
        with archive.open(member) as compressed:
            rows = csv.reader(io.TextIOWrapper(compressed, encoding="utf-8", newline=""))
            for row in rows:
                if not row or not row[0].isdigit():
                    continue
                if len(row) < 11:
                    raise ValueError(f"short spot kline row in {archive_path}")
                second_start_ms = timestamp_ms(row[0])
                if second_start_ms in seconds:
                    raise ValueError(f"duplicate spot second in {archive_path}")
                seconds[second_start_ms] = SpotSecond(
                    close=row[4],
                    quote_volume=row[7],
                    trade_count=int(row[8]),
                    taker_buy_quote_volume=row[10],
                )
    finally:
        archive.close()
    return seconds


def load_perpetual_seconds(
    archive_path: Path, expected_name: str
) -> dict[int, PerpetualSecond]:
    archive, member = verified_member(archive_path, expected_name)
    seconds: dict[int, PerpetualSecond] = {}
    last_timestamp = -1
    try:
        with archive.open(member) as compressed:
            rows = csv.reader(io.TextIOWrapper(compressed, encoding="utf-8", newline=""))
            for row in rows:
                if not row or not row[0].isdigit():
                    continue
                if len(row) < 7:
                    raise ValueError(f"short perpetual aggTrades row in {archive_path}")
                trade_ms = timestamp_ms(row[5])
                if trade_ms < last_timestamp:
                    raise ValueError(f"out-of-order perpetual trade in {archive_path}")
                last_timestamp = trade_ms
                second_start_ms = trade_ms - trade_ms % 1_000
                price = Decimal(row[1])
                quantity = Decimal(row[2])
                quote_notional = price * quantity
                trade_count = int(row[4]) - int(row[3]) + 1
                current = seconds.get(second_start_ms)
                if current is None:
                    current = PerpetualSecond(
                        close=row[1],
                        last_trade_ms=trade_ms,
                        quote_volume=Decimal(0),
                        taker_buy_quote_volume=Decimal(0),
                        trade_count=0,
                        aggregate_count=0,
                    )
                    seconds[second_start_ms] = current
                current.close = row[1]
                current.last_trade_ms = trade_ms
                current.quote_volume += quote_notional
                if row[6].lower() == "false":
                    current.taker_buy_quote_volume += quote_notional
                current.trade_count += trade_count
                current.aggregate_count += 1
    finally:
        archive.close()
    return seconds


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def maximum_gap_seconds(seconds: list[int]) -> int:
    if len(seconds) < 2:
        return 0
    return max((right - left) // 1_000 - 1 for left, right in zip(seconds, seconds[1:]))


def write_partition(
    date: str,
    spot: dict[int, SpotSecond],
    perpetual: dict[int, PerpetualSecond],
    output_path: Path,
) -> dict[str, object]:
    spot_times = sorted(spot)
    perpetual_times = sorted(perpetual)
    if not spot_times or not perpetual_times:
        raise ValueError(f"empty Binance source partition for {date}")
    day_start_ms = min(spot_times[0], perpetual_times[0])
    day_start_ms -= day_start_ms % DAY_MS
    expected = [day_start_ms + offset * 1_000 for offset in range(86_400)]
    spot_missing = sum(timestamp not in spot for timestamp in expected)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        dir=output_path.parent, prefix=f"{output_path.name}.tmp."
    )
    aligned_rows = 0
    last_perpetual_close: str | None = None
    last_perpetual_trade_ms: int | None = None
    try:
        with os.fdopen(handle, "wb") as raw_output:
            with gzip.GzipFile(fileobj=raw_output, mode="wb", filename="", mtime=0) as zipped:
                with io.TextIOWrapper(zipped, encoding="utf-8", newline="") as text_output:
                    writer = csv.writer(text_output, lineterminator="\n")
                    writer.writerow(
                        [
                            "second_start_ms",
                            "available_at_ms",
                            "spot_close",
                            "spot_quote_volume",
                            "spot_taker_buy_quote_volume",
                            "spot_trade_count",
                            "perpetual_close",
                            "perpetual_last_trade_ms",
                            "perpetual_quote_volume",
                            "perpetual_taker_buy_quote_volume",
                            "perpetual_trade_count",
                            "perpetual_aggregate_count",
                        ]
                    )
                    for second_start_ms in expected:
                        spot_second = spot.get(second_start_ms)
                        perpetual_second = perpetual.get(second_start_ms)
                        if perpetual_second is not None:
                            last_perpetual_close = perpetual_second.close
                            last_perpetual_trade_ms = perpetual_second.last_trade_ms
                        if spot_second is None:
                            continue
                        if last_perpetual_close is not None:
                            aligned_rows += 1
                        writer.writerow(
                            [
                                second_start_ms,
                                second_start_ms + 1_000,
                                spot_second.close,
                                spot_second.quote_volume,
                                spot_second.taker_buy_quote_volume,
                                spot_second.trade_count,
                                last_perpetual_close or "",
                                last_perpetual_trade_ms or "",
                                decimal_text(perpetual_second.quote_volume)
                                if perpetual_second
                                else "0",
                                decimal_text(perpetual_second.taker_buy_quote_volume)
                                if perpetual_second
                                else "0",
                                perpetual_second.trade_count if perpetual_second else 0,
                                perpetual_second.aggregate_count if perpetual_second else 0,
                            ]
                        )
        os.replace(temporary_name, output_path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise

    return {
        "date": date,
        "expected_seconds": 86_400,
        "spot_seconds": len(spot),
        "spot_missing_seconds": spot_missing,
        "spot_maximum_gap_seconds": maximum_gap_seconds(spot_times),
        "perpetual_seconds_with_trades": len(perpetual),
        "perpetual_seconds_without_trades": 86_400 - len(perpetual),
        "perpetual_maximum_trade_gap_seconds": maximum_gap_seconds(perpetual_times),
        "aligned_output_rows": aligned_rows,
        "output": {"path": str(output_path), "sha256": sha256(output_path)},
    }


def run(args: argparse.Namespace) -> dict[str, object]:
    dates, seal_sha256 = dates_from_seal(args.dataset_seal)
    downloaded_paths: set[Path] = set()
    completed = False
    partitions = []
    sources = []
    try:
        for date in dates:
            archives: dict[str, tuple[Path, str]] = {}
            source_record: dict[str, object] = {"date": date}
            for source, (url, local_name, official_name) in source_specs(date).items():
                archive_path, downloaded = ensure_source(
                    args.raw_dir, url, local_name, args.download
                )
                if downloaded:
                    downloaded_paths.add(archive_path)
                    downloaded_paths.add(
                        archive_path.with_suffix(archive_path.suffix + ".CHECKSUM")
                    )
                verified_archive, _ = verified_member(archive_path, official_name)
                verified_archive.close()
                archives[source] = (archive_path, official_name)
                source_record[source] = {
                    "url": url,
                    "archive": {
                        "path": str(archive_path),
                        "sha256": sha256(archive_path),
                    },
                    "checksum": {
                        "path": str(
                            archive_path.with_suffix(archive_path.suffix + ".CHECKSUM")
                        ),
                        "sha256": sha256(
                            archive_path.with_suffix(archive_path.suffix + ".CHECKSUM")
                        ),
                    },
                }
            spot = load_spot_seconds(*archives["spot"])
            perpetual = load_perpetual_seconds(*archives["perpetual"])
            output_path = args.output_dir / f"BTCUSDT-cross-venue-1s-{date}.csv.gz"
            partitions.append(write_partition(date, spot, perpetual, output_path))
            sources.append(source_record)

        ready = all(
            partition["spot_missing_seconds"] == 0
            and partition["aligned_output_rows"] == 86_400
            for partition in partitions
        )
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "dataset_seal": {
                "path": str(args.dataset_seal),
                "sha256": seal_sha256,
            },
            "source_dates": dates,
            "source_date_count": len(dates),
            "sources": sources,
            "partitions": partitions,
            "causal_contract": {
                "bucket_ms": 1_000,
                "available_at_semantics": (
                    "A source second becomes observable only at second_start_ms + 1000; "
                    "no partially formed spot kline or perpetual trade bucket is visible."
                ),
                "spot_source": "BTCUSDT spot 1s klines",
                "perpetual_source": "BTCUSDT USD-M perpetual aggTrades grouped by transact_time second",
                "perpetual_taker_buy_semantics": "is_buyer_maker=false",
                "perpetual_empty_second_semantics": (
                    "carry the last trade price; volumes and trade counts are zero"
                ),
                "timestamp_normalization": (
                    "spot microseconds and perpetual milliseconds normalized to Unix milliseconds"
                ),
            },
            "quality": {
                "complete_spot_days": sum(
                    partition["spot_missing_seconds"] == 0 for partition in partitions
                ),
                "fully_aligned_days": sum(
                    partition["aligned_output_rows"] == 86_400 for partition in partitions
                ),
                "ready_for_feature_join": ready,
            },
            "label_access_audit": {
                "label_artifacts_read": 0,
                "outcomes_read": 0,
                "scores_read": 0,
                "pnl_read": 0,
            },
            "status": "ready" if ready else "blocked_source_quality",
        }
        atomic_json(args.manifest, manifest)
        completed = True
        return manifest
    finally:
        if args.delete_after_process and completed:
            for path in downloaded_paths:
                path.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-seal", type=Path, required=True)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--download", action="store_true")
    parser.add_argument(
        "--delete-after-process",
        action="store_true",
        help="delete only archives downloaded by this invocation after successful processing",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = run(args)
    print(
        f"status={manifest['status']} dates={manifest['source_date_count']} "
        f"manifest={args.manifest}"
    )


if __name__ == "__main__":
    main()
