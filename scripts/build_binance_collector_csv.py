#!/usr/bin/env python3
"""Convert checksum-verified official Binance one-second archives to BTC tape CSV."""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import tempfile
import zipfile
from pathlib import Path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verified_member(archive_path: Path) -> tuple[zipfile.ZipFile, str]:
    checksum_path = archive_path.with_suffix(archive_path.suffix + ".CHECKSUM")
    expected_hash, expected_name = checksum_path.read_text().strip().split()
    if expected_name.lstrip("*") != archive_path.name or sha256(archive_path) != expected_hash:
        raise ValueError(f"checksum mismatch: {archive_path}")
    archive = zipfile.ZipFile(archive_path)
    members = archive.namelist()
    if len(members) != 1:
        archive.close()
        raise ValueError(f"expected exactly one CSV member: {archive_path}")
    return archive, members[0]


def timestamp_ms(raw: str) -> int:
    value = int(raw)
    return value // 1_000 if value >= 10**15 else value


def run(archives: list[Path], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with tempfile.NamedTemporaryFile(
        "w", newline="", dir=output_path.parent, prefix=f"{output_path.name}.tmp.", delete=False
    ) as raw_output:
        writer = csv.writer(raw_output)
        writer.writerow(["timestamp_ms", "source", "price"])
        for archive_path in archives:
            archive, member = verified_member(archive_path)
            try:
                with archive.open(member) as compressed:
                    rows = csv.reader(line.decode("utf-8") for line in compressed)
                    for row in rows:
                        if len(row) < 7:
                            continue
                        writer.writerow(
                            [timestamp_ms(row[6]), "binance_btcusdt", float(row[4])]
                        )
                        count += 1
            finally:
                archive.close()
        temporary_path = Path(raw_output.name)
    os.replace(temporary_path, output_path)
    return count


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows = run(args.archive, args.output)
    print(f"rows={rows} output={args.output}")


if __name__ == "__main__":
    main()
