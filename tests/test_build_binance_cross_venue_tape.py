import csv
import hashlib
import importlib.util
import json
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "build_binance_cross_venue_tape",
    ROOT / "scripts" / "build_binance_cross_venue_tape.py",
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def archive_with_checksum(path: Path, source_name: str, rows: list[list[object]]) -> None:
    csv_name = source_name.removesuffix(".zip") + ".csv"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        contents = "\n".join(",".join(str(value) for value in row) for row in rows) + "\n"
        archive.writestr(csv_name, contents)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_suffix(path.suffix + ".CHECKSUM").write_text(
        f"{digest}  {source_name}\n", encoding="utf-8"
    )


class CrossVenueTapeTests(unittest.TestCase):
    def test_timestamp_normalizes_spot_microseconds(self):
        self.assertEqual(MODULE.timestamp_ms("1786147200000000"), 1786147200000)
        self.assertEqual(MODULE.timestamp_ms("1786147200002"), 1786147200002)

    def test_checksum_rejects_wrong_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "local.zip"
            archive_with_checksum(path, "official.zip", [["1"]])
            path.write_bytes(path.read_bytes() + b"corrupt")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                MODULE.verified_member(path, "official.zip")

    def test_perpetual_side_and_causal_availability(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spot_path = root / "spot.zip"
            perp_path = root / "perp.zip"
            start_us = 1_786_147_200_000_000
            spot_rows = [
                [start_us, "100", "101", "99", "100", "2", start_us + 999_999,
                 "200", 2, "1", "100", 0],
                [start_us + 1_000_000, "100", "102", "100", "102", "1",
                 start_us + 1_999_999, "102", 1, "1", "102", 0],
            ]
            perp_rows = [
                ["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
                 "transact_time", "is_buyer_maker"],
                [1, "100", "2", 10, 11, 1_786_147_200_002, "false"],
                [2, "101", "1", 12, 12, 1_786_147_200_999, "true"],
            ]
            archive_with_checksum(spot_path, "spot-source.zip", spot_rows)
            archive_with_checksum(perp_path, "perp-source.zip", perp_rows)

            spot = MODULE.load_spot_seconds(spot_path, "spot-source.zip")
            perp = MODULE.load_perpetual_seconds(perp_path, "perp-source.zip")
            bucket = perp[1_786_147_200_000]
            self.assertEqual(bucket.close, "101")
            self.assertEqual(str(bucket.quote_volume), "301")
            self.assertEqual(str(bucket.taker_buy_quote_volume), "200")
            self.assertEqual(bucket.trade_count, 3)

            output = root / "partition.csv.gz"
            quality = MODULE.write_partition("2026-08-08", spot, perp, output)
            self.assertEqual(quality["aligned_output_rows"], 2)
            import gzip

            with gzip.open(output, "rt", newline="") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(rows[0]["available_at_ms"], "1786147201000")
            self.assertEqual(rows[0]["perpetual_taker_buy_quote_volume"], "200")
            self.assertEqual(rows[1]["perpetual_quote_volume"], "0")
            self.assertEqual(rows[1]["perpetual_close"], "101")

    def test_seal_date_read_is_outcome_free(self):
        with tempfile.TemporaryDirectory() as directory:
            seal_path = Path(directory) / "seal.json"
            seal_path.write_text(
                json.dumps(
                    {
                        "schema_version": "opportunity_dataset_v1",
                        "outcome_columns_present": False,
                        "entries": [
                            {"hour": "2026-08-08T03:00:00Z"},
                            {"hour": "2026-08-08T22:00:00Z"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            dates, digest = MODULE.dates_from_seal(seal_path)
            self.assertEqual(dates, ["2026-08-08"])
            self.assertEqual(digest, hashlib.sha256(seal_path.read_bytes()).hexdigest())


if __name__ == "__main__":
    unittest.main()
