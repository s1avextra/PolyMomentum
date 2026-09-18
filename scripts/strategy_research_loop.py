#!/usr/bin/env python3
"""Bounded, research-only coordinator of the band factory (Mac only).

One cycle is the resource gate followed by the band lane
(scripts/band_lane.py): the public cache refresh, evidence accrual, the
rescreen of support-only rejections and one grammar C proposal screened
feed-forward.  The coordinator cannot trade, deploy or promote; the ladder
evaluator (scripts/executable_truth.py --tick, run by
deploy/factory-runner.sh every 15 min) owns registration and the promotion
gate (docs/profitability_basement_2026-09-18.md section C).

Phase 3 of that document deleted the LLM factory from this module: the
late-window and baseline lanes, LmStudioClient, the exact-L2 / PMXT /
fresh-holdout / fixed-forward / economic-screen job chain, the registry
audit and the public snapshot refresh.  load_config is fail-closed against
their config blocks, so a stale overlay is refused rather than half read.
"""

from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import importlib.util as _importlib_util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
import uuid


def _load(name: str, filename: str) -> Any:
    spec = _importlib_util.spec_from_file_location(name, Path(__file__).resolve().parent / filename)
    assert spec and spec.loader
    module = _importlib_util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


factory_generator = _load("factory_generator", "factory_generator.py")
evidence_accrual = _load("evidence_accrual", "evidence_accrual.py")
band_lane = _load("band_lane", "band_lane.py")

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "deploy/strategy-research-loop.json"
# The whole config contract.  Anything else (an LLM block, a deleted lane, a
# stale overlay key) is refused so a cycle never runs on a config it only
# half reads.
CONFIG_BLOCKS = ("schema_version", "mode", "state_dir", "resource_policy", "generator", "lanes")
RESOURCE_POLICY_KEYS = ("minimum_cpu_count", "minimum_free_disk_gib", "maximum_load_per_cpu")
LANE_REQUIRED_KEYS = ("enabled", "minimum_interval_seconds", "start_ts", "maximum_new_windows_per_cycle", "gates")
LANE_OPTIONAL_KEYS = ("campaign_n", "audit_cleared", "entry_semantics")
GATE_KEYS = ("minimum_signals", "minimum_recent_signals", "minimum_entries")


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("%s.tmp.%s" % (path.name, os.getpid()))
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(str(temporary), str(path))


def resolve_repo_path(raw: str) -> Path:
    path = (ROOT / raw).resolve() if not os.path.isabs(raw) else Path(raw).resolve()
    if path == ROOT or ROOT in path.parents:
        return path
    raise ValueError("path escapes repository: %s" % raw)


def _positive_int(block: Mapping[str, Any], key: str, where: str) -> int:
    value = block.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("%s.%s must be a positive integer" % (where, key))
    return value


def load_config(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError("strategy research config must be a JSON object")
    unknown = sorted(set(payload) - set(CONFIG_BLOCKS))
    if unknown:
        raise ValueError(
            "unknown config blocks (the LLM factory was deleted in Phase 3): %s" % ", ".join(unknown)
        )
    missing = [key for key in CONFIG_BLOCKS if key not in payload]
    if missing:
        raise ValueError("missing config blocks: %s" % ", ".join(missing))
    if payload["schema_version"] != 1:
        raise ValueError("strategy research config schema_version must be 1")
    if payload["mode"] != "research_only":
        raise ValueError("strategy research loop must be research_only")
    if not isinstance(payload["state_dir"], str) or not payload["state_dir"]:
        raise ValueError("state_dir must be a non-empty path")
    policy = payload["resource_policy"]
    if not isinstance(policy, dict) or set(policy) != set(RESOURCE_POLICY_KEYS):
        raise ValueError("resource_policy must hold exactly %s" % ", ".join(RESOURCE_POLICY_KEYS))
    for key in RESOURCE_POLICY_KEYS:
        value = policy[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ValueError("resource_policy.%s must be a positive number" % key)
    generator = payload["generator"]
    if not isinstance(generator, dict) or set(generator) - set(factory_generator.DEFAULTS):
        raise ValueError("generator may hold only %s" % ", ".join(sorted(factory_generator.DEFAULTS)))
    if not isinstance(generator.get("trial_ledger_enabled", False), bool):
        raise ValueError("generator.trial_ledger_enabled must be a boolean")
    lanes = payload["lanes"]
    if not isinstance(lanes, dict) or set(lanes) != {band_lane.LANE}:
        raise ValueError("lanes must hold exactly %s (the other lanes were deleted)" % band_lane.LANE)
    lane = lanes[band_lane.LANE]
    where = "lanes.%s" % band_lane.LANE
    if not isinstance(lane, dict):
        raise ValueError("%s must be an object" % where)
    unknown = sorted(set(lane) - set(LANE_REQUIRED_KEYS) - set(LANE_OPTIONAL_KEYS))
    missing = [key for key in LANE_REQUIRED_KEYS if key not in lane]
    if unknown or missing:
        raise ValueError("%s: unknown keys %s, missing keys %s" % (where, unknown, missing))
    if not isinstance(lane["enabled"], bool):
        raise ValueError("%s.enabled must be a boolean" % where)
    for key in ("minimum_interval_seconds", "start_ts", "maximum_new_windows_per_cycle"):
        _positive_int(lane, key, where)
    gates = lane["gates"]
    if not isinstance(gates, dict) or set(gates) != set(GATE_KEYS):
        raise ValueError("%s.gates must hold exactly %s" % (where, ", ".join(GATE_KEYS)))
    for key in GATE_KEYS:
        _positive_int(gates, key, where + ".gates")
    if "campaign_n" in lane:
        _positive_int(lane, "campaign_n", where)
    cleared = lane.get("audit_cleared", [])
    if not isinstance(cleared, list) or not all(isinstance(item, str) for item in cleared):
        raise ValueError("%s.audit_cleared must be a list of fingerprints" % where)
    if lane.get("entry_semantics", "one_look") not in band_lane.ENTRY_SEMANTICS:
        raise ValueError("%s.entry_semantics must be one of %s" % (where, ", ".join(band_lane.ENTRY_SEMANTICS)))
    return payload


class CycleLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle: Optional[Any] = None

    def __enter__(self) -> "CycleLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self.handle.close()
            self.handle = None
            raise RuntimeError("another strategy research cycle is active") from error
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write("%s\n" % os.getpid())
        self.handle.flush()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if self.handle is not None:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()


class Ledger:
    """SQLite research ledger: hypotheses (one row per fingerprint), their
    e-process accrual, cycles and meta.  The LLM-era jobs table is neither
    created nor read; an older database keeps it untouched."""

    def __init__(self, path: Path) -> None:
        self.state_dir = path.parent
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(path), timeout=5)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cycles (
                cycle_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                details_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS hypotheses (
                fingerprint TEXT PRIMARY KEY,
                lane TEXT NOT NULL,
                created_at TEXT NOT NULL,
                proposal_json TEXT NOT NULL,
                review_json TEXT,
                status TEXT NOT NULL,
                evidence_path TEXT
            );
            CREATE TABLE IF NOT EXISTS evidence_accrual (
                fingerprint TEXT PRIMARY KEY,
                lane TEXT NOT NULL,
                n INTEGER NOT NULL,
                wins INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                last_window_start INTEGER NOT NULL,
                e_value REAL NOT NULL,
                verdict TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        columns = {
            str(row["name"]) for row in self.connection.execute("PRAGMA table_info(hypotheses)")
        }
        if "source" not in columns:
            self.connection.execute("ALTER TABLE hypotheses ADD COLUMN source TEXT")
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def meta(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self.connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return str(row["value"]) if row else default

    def set_meta(self, key: str, value: str) -> None:
        self.connection.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        self.connection.commit()

    def begin_cycle(self, cycle_id: str, details: Mapping[str, Any]) -> None:
        self.connection.execute(
            "INSERT INTO cycles VALUES(?, ?, NULL, 'running', ?)",
            (cycle_id, utc_now(), canonical_json(details)),
        )
        self.connection.commit()

    def finish_cycle(self, cycle_id: str, status: str, details: Mapping[str, Any]) -> None:
        self.connection.execute(
            "UPDATE cycles SET finished_at = ?, status = ?, details_json = ? WHERE cycle_id = ?",
            (utc_now(), status, canonical_json(details), cycle_id),
        )
        self.connection.commit()

    def has_hypothesis(self, fingerprint: str) -> bool:
        return self.connection.execute(
            "SELECT 1 FROM hypotheses WHERE fingerprint = ?", (fingerprint,)
        ).fetchone() is not None

    def hypothesis(self, fingerprint: str) -> Optional[Dict[str, Any]]:
        row = self.connection.execute(
            "SELECT * FROM hypotheses WHERE fingerprint = ?", (fingerprint,)
        ).fetchone()
        return dict(row) if row else None

    def lane_hypotheses(self, lane: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for row in self.connection.execute(
            "SELECT fingerprint, status, created_at, proposal_json, source FROM hypotheses "
            "WHERE lane = ? ORDER BY created_at, rowid",
            (lane,),
        ):
            try:
                proposal = json.loads(row["proposal_json"])
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "fingerprint": str(row["fingerprint"]),
                    "status": str(row["status"]),
                    "created_at": str(row["created_at"]),
                    "proposal": proposal,
                    "source": row["source"],
                }
            )
        return rows

    def add_hypothesis(
        self,
        fingerprint: str,
        lane: str,
        proposal: Mapping[str, Any],
        review: Optional[Mapping[str, Any]],
        status: str,
        evidence_path: Optional[Path],
        source: Optional[str] = None,
    ) -> None:
        self.connection.execute(
            "INSERT INTO hypotheses(fingerprint, lane, created_at, proposal_json, "
            "review_json, status, evidence_path, source) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
            (
                fingerprint,
                lane,
                utc_now(),
                canonical_json(proposal),
                canonical_json(review) if review is not None else None,
                status,
                str(evidence_path) if evidence_path else None,
                source,
            ),
        )
        self.connection.commit()

    def update_hypothesis_status(self, fingerprint: str, status: str) -> None:
        self.connection.execute(
            "UPDATE hypotheses SET status = ? WHERE fingerprint = ?",
            (status, fingerprint),
        )
        self.connection.commit()

    def accrual(self, fingerprint: str) -> Optional[Dict[str, Any]]:
        row = self.connection.execute(
            "SELECT * FROM evidence_accrual WHERE fingerprint = ?", (fingerprint,)
        ).fetchone()
        return dict(row) if row else None

    def delete_accrual(self, fingerprint: str) -> int:
        cursor = self.connection.execute(
            "DELETE FROM evidence_accrual WHERE fingerprint = ?", (fingerprint,)
        )
        self.connection.commit()
        return int(cursor.rowcount)

    def accrue(
        self,
        fingerprint: str,
        lane: str,
        outcomes: Sequence[Tuple[int, float, bool]],
        seed_last_window_start: int = -1,
    ) -> Dict[str, Any]:
        """Fold (window_start, break_even, won) outcomes into the e-process.

        Only outcomes with window_start > last_window_start are applied, in
        ascending order, so replaying the same outcomes is a no-op.  A first
        accrual starts its cut at seed_last_window_start.
        """
        row = self.accrual(fingerprint)
        if row is None:
            process = evidence_accrual.EProcess()
            wins = 0
            last_window_start = int(seed_last_window_start)
        else:
            process = evidence_accrual.EProcess.from_json(str(row["state_json"]))
            wins = int(row["wins"])
            last_window_start = int(row["last_window_start"])
        applied = 0
        for window_start, break_even, won in sorted(outcomes, key=lambda item: int(item[0])):
            if int(window_start) <= last_window_start:
                continue
            process.update(float(break_even), bool(won))
            wins += int(bool(won))
            last_window_start = int(window_start)
            applied += 1
        result = {
            "n": process.n,
            "wins": wins,
            "e_value": process.e_value(),
            "verdict": process.verdict(),
            "applied": applied,
        }
        self.connection.execute(
            "INSERT INTO evidence_accrual VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(fingerprint) DO UPDATE SET n = excluded.n, wins = excluded.wins, "
            "state_json = excluded.state_json, last_window_start = excluded.last_window_start, "
            "e_value = excluded.e_value, verdict = excluded.verdict, "
            "updated_at = excluded.updated_at",
            (
                fingerprint,
                lane,
                process.n,
                wins,
                process.to_json(),
                last_window_start,
                result["e_value"],
                result["verdict"],
                utc_now(),
            ),
        )
        self.connection.commit()
        return result

    def summary(self) -> Dict[str, Any]:
        hypotheses = [
            dict(row)
            for row in self.connection.execute(
                "SELECT lane, status, COUNT(*) AS count FROM hypotheses GROUP BY lane, status"
            )
        ]
        last_cycle = self.connection.execute(
            "SELECT cycle_id, started_at, finished_at, status, details_json "
            "FROM cycles ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        return {
            "hypotheses": hypotheses,
            "last_cycle": dict(last_cycle) if last_cycle else None,
        }


def resource_status(config: Mapping[str, Any], state_dir: Path) -> Dict[str, Any]:
    policy = config["resource_policy"]
    disk = shutil.disk_usage(str(state_dir))
    free_gib = disk.free / float(1024 ** 3)
    cpus = max(1, os.cpu_count() or 1)
    try:
        load_1m = os.getloadavg()[0]
    except (AttributeError, OSError):
        load_1m = 0.0
    load_per_cpu = load_1m / cpus
    checks = {
        "free_disk": free_gib >= float(policy["minimum_free_disk_gib"]),
        "load": load_per_cpu <= float(policy["maximum_load_per_cpu"]),
        "dev_box": cpus >= int(policy["minimum_cpu_count"]),
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "free_disk_gib": round(free_gib, 3),
        "cpu_count": cpus,
        "load_1m": round(load_1m, 3),
        "load_per_cpu": round(load_per_cpu, 3),
    }


def run_band_lane(
    config: Mapping[str, Any], ledger: Ledger, state_dir: Path, dry_run: bool
) -> Dict[str, Any]:
    return band_lane.run_band_lane(config, ledger, state_dir, dry_run)


def run_cycle(config: Mapping[str, Any], dry_run: bool, selected_lane: Optional[str]) -> Dict[str, Any]:
    if selected_lane not in (None, band_lane.LANE):
        raise ValueError("unknown lane: %s" % selected_lane)
    state_dir = resolve_repo_path(config["state_dir"])
    state_dir.mkdir(parents=True, exist_ok=True)
    with CycleLock(state_dir / "locks/cycle.lock"):
        ledger = Ledger(state_dir / "research.sqlite3")
        cycle_id = str(uuid.uuid4())
        result: Dict[str, Any] = {
            "cycle_id": cycle_id,
            "started_at": utc_now(),
            "dry_run": dry_run,
            "lane": band_lane.LANE,
        }
        ledger.begin_cycle(cycle_id, {"dry_run": dry_run, "selected_lane": selected_lane})
        try:
            resources = resource_status(config, state_dir)
            result["resources"] = resources
            if resources["passed"]:
                result["lane_result"] = run_band_lane(config, ledger, state_dir, dry_run)
            else:
                result["lane_result"] = {
                    "status": "deferred_resource_gate",
                    "failed_checks": [key for key, value in resources["checks"].items() if not value],
                }
            result["finished_at"] = utc_now()
            ledger.finish_cycle(cycle_id, "completed", result)
            result["ledger"] = ledger.summary()
            atomic_json(state_dir / "status.json", result)
            return result
        except Exception as error:
            result["finished_at"] = utc_now()
            result["error"] = "%s: %s" % (type(error).__name__, error)
            ledger.finish_cycle(cycle_id, "failed", result)
            atomic_json(state_dir / "status.json", result)
            raise
        finally:
            ledger.close()


def status(config: Mapping[str, Any]) -> Dict[str, Any]:
    state_dir = resolve_repo_path(config["state_dir"])
    database = state_dir / "research.sqlite3"
    if not database.is_file():
        return {"status": "not_initialized", "state_dir": str(state_dir)}
    ledger = Ledger(database)
    try:
        return {
            "status": "initialized",
            "state_dir": str(state_dir),
            "resources": resource_status(config, state_dir),
            "ledger": ledger.summary(),
        }
    finally:
        ledger.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--once", action="store_true", help="run exactly one bounded cycle")
    parser.add_argument("--dry-run", action="store_true", help="propose only: no network, no cache, no hypotheses")
    parser.add_argument("--lane", choices=(band_lane.LANE,), help="the band lane (the only lane)")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    config = load_config(args.config.resolve())
    if args.status:
        print(json.dumps(status(config), indent=2, sort_keys=True))
        return 0
    if not args.once:
        parser.error("--once is required; scheduling belongs to deploy/factory-runner.sh")
    try:
        result = run_cycle(config, args.dry_run, args.lane)
    except RuntimeError as error:
        print(json.dumps({"status": "skipped", "reason": str(error)}, indent=2))
        return 0
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
