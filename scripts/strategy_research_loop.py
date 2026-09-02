#!/usr/bin/env python3
"""Bounded, research-only coordinator for continuous strategy discovery.

The coordinator deliberately cannot trade, deploy, promote, or execute commands
provided by an LLM. During the v3 migration it runs the immutable opportunity
dataset policy-search lane; the two legacy proposal lanes remain fail-closed:

* baseline_evolution: the existing deterministic Rust evolve-search;
* late_window_mechanisms: public-data hypothesis proposal and cheap screening.

Expensive exact-L2 work is bounded to preregistered discovery and outcome-blind
fresh-holdout windows.  The coordinator can classify a candidate as research
eligible, but it cannot promote or trade it.  This keeps the 750-condition
forward floor at the final confirmation stage instead of using it as an
exploration gate.
"""

from __future__ import annotations

import argparse
import copy
import contextlib
import csv
import datetime as dt
import fcntl
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile

import importlib.util as _importlib_util

_FACTORY_SPEC = _importlib_util.spec_from_file_location(
    "factory_generator", Path(__file__).resolve().parent / "factory_generator.py"
)
assert _FACTORY_SPEC and _FACTORY_SPEC.loader
factory_generator = _importlib_util.module_from_spec(_FACTORY_SPEC)
_FACTORY_SPEC.loader.exec_module(factory_generator)

_ACCRUAL_SPEC = _importlib_util.spec_from_file_location(
    "evidence_accrual", Path(__file__).resolve().parent / "evidence_accrual.py"
)
assert _ACCRUAL_SPEC and _ACCRUAL_SPEC.loader
evidence_accrual = _importlib_util.module_from_spec(_ACCRUAL_SPEC)
_ACCRUAL_SPEC.loader.exec_module(evidence_accrual)

_BAND_SPEC = _importlib_util.spec_from_file_location(
    "band_lane", Path(__file__).resolve().parent / "band_lane.py"
)
assert _BAND_SPEC and _BAND_SPEC.loader
band_lane = _importlib_util.module_from_spec(_BAND_SPEC)
_BAND_SPEC.loader.exec_module(band_lane)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "deploy/strategy-research-loop.json"
SAFE_STAGES = {
    "mechanism_definition",
    "public_directional_screen",
    "historic_feed_forward_screen",
    "economic_opportunity_screen",
    "exact_l2_replay",
    "fresh_resolved_holdout",
    "fixed_forward_confirmation",
    "bounded_vps_shadow",
}
LATE_OPERATORS = {"path_only", "move_only", "path_and_move", "path_or_move"}
LATE_PATH_MINUTES = {0, 2, 3, 4}
LATE_MOVE_THRESHOLDS = {0, 100, 200}
LATE_MAXIMUM_ENTRY_PRICES = {0.75, 0.85, 0.90, 0.95, 0.97, 1.0}
LATE_DECISION_BUFFERS_USD = {0, 100, 125, 200}
LATE_SETTLEMENT_SIGMA_BUFFERS = {0.0, 0.1, 0.2}
LATE_MINIMUM_BOOK_PRESSURES = {-1.0, -0.15, 0.15}
LATE_DIRECTIONS = {"both", "up", "down"}
LATE_EVALUATOR_VERSION = "late_window_public_v16"
EXACT_ELIGIBILITY_POLICY_VERSION = "staged_stress_v6"
PUBLIC_SNAPSHOT_VERSION = "binance_spot_1m_v1"
LATE_REPLAY_BUCKET_HOURS = 1
FRESH_SELECTION_GRANULARITY_VERSION = "signal_hour_v1"
FRESH_GLOBAL_RESERVE_VERSION = "other_jobs_measured_starts_v1"
# The late lane walks fallback_late_proposals() in order until the ledger
# holds this many late hypotheses; from then on a ready LLM alternates with
# the uniform control and the grid is only the not-ready fallback.
DIAGNOSTIC_PROPOSAL_PREFIX = 6
LATE_PROPOSAL_SOURCES = ("diagnostic", "llm", "uniform_control", "fallback_grid", "burst_queue")
# Burst-queue replays are LLM samples too: parity balances the whole LLM arm
# against the uniform control so one burst cannot starve the control arm.
LATE_LLM_ARM_SOURCES = ("llm", "burst_queue")
UNIFORM_LATE_ATTEMPTS = 64


def payoff_derived_entry_cap(
    maximum_loss_recovery_wins: int = 50,
    taker_fee_rate: float = 0.07,
    tick_size: float = 0.01,
) -> float:
    """Largest tick whose fee-aware winning return repays a full loss on target."""
    target_cost = maximum_loss_recovery_wins / float(maximum_loss_recovery_wins + 1)
    ticks = int(round(1.0 / tick_size))
    allowed = [
        index * tick_size
        for index in range(1, ticks)
        if index * tick_size
        + taker_fee_rate * (index * tick_size) * (1.0 - index * tick_size)
        <= target_cost + 1e-12
    ]
    if not allowed:
        raise ValueError("no tick satisfies the payoff target")
    return round(max(allowed), 8)


LATE_PAYOFF_DERIVED_ENTRY_CAP = payoff_derived_entry_cap()


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def load_config(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text())
    if payload.get("schema_version") != 1:
        raise ValueError("strategy research config schema_version must be 1")
    if payload.get("mode") != "research_only":
        raise ValueError("strategy research loop must be research_only")
    if payload.get("maximum_exact_l2_shortlist") != 2:
        raise ValueError("maximum_exact_l2_shortlist must remain 2")
    if int(payload.get("fresh_holdout", {}).get("maximum_windows_per_hypothesis", 0)) < 1:
        raise ValueError("fresh_holdout maximum_windows_per_hypothesis must be positive")
    forward = payload.get("fixed_forward_confirmation", {})
    if forward.get("enabled", False):
        if int(forward.get("minimum_checkpoint_fills", 0)) < 1:
            raise ValueError("fixed forward minimum_checkpoint_fills must be positive")
        if int(forward.get("maximum_target_fills", 0)) < int(
            forward["minimum_checkpoint_fills"]
        ):
            raise ValueError("fixed forward maximum_target_fills must cover checkpoint")
    migration_search = payload.get("architecture_migration", {}).get(
        "opportunity_policy_search", {}
    )
    if migration_search.get("enabled", False):
        if not migration_search.get("dataset_seal") or not migration_search.get(
            "labels_manifest"
        ):
            raise ValueError("opportunity policy search requires sealed dataset and labels")
        if not migration_search.get("pmxt_cache_dir"):
            raise ValueError("opportunity exact replay requires a PMXT cache directory")
        for field in ("minimum_calibration_support", "minimum_policy_support"):
            if int(migration_search.get(field, 0)) < 1:
                raise ValueError("opportunity policy-search support floors must be positive")
        margin = float(migration_search.get("safety_margin", -1.0))
        if not 0.0 <= margin < 1.0:
            raise ValueError("opportunity policy-search safety_margin must be in [0, 1)")
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
    def __init__(
        self, path: Path, generator_config: Optional[Mapping[str, Any]] = None
    ) -> None:
        self.generator_config = generator_config
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
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                lane TEXT NOT NULL,
                hypothesis_fingerprint TEXT NOT NULL,
                stage TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                reason TEXT,
                UNIQUE(hypothesis_fingerprint, stage)
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

    def has_late_rule(self, rule: Mapping[str, Any]) -> bool:
        target = canonical_json(normalized_late_rule(rule))
        for row in self.connection.execute(
            "SELECT proposal_json FROM hypotheses WHERE lane = 'late_window_mechanisms'"
        ):
            try:
                proposal = json.loads(row["proposal_json"])
                existing = canonical_json(normalized_late_rule(proposal["rule"]))
            except (KeyError, TypeError, ValueError):
                continue
            if existing == target:
                return True
        return False

    def late_hypotheses(self) -> List[Dict[str, Any]]:
        return self.lane_hypotheses("late_window_mechanisms")

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
        factory_generator.record_kill_feedback(
            self.generator_config, self.state_dir, status, canonical_json(proposal)
        )

    def enqueue(
        self,
        lane: str,
        fingerprint: str,
        stage: str,
        payload: Mapping[str, Any],
        reason: str,
        status: str = "blocked",
    ) -> bool:
        if stage not in SAFE_STAGES:
            raise ValueError("unsafe queue stage: %s" % stage)
        if status not in ("blocked", "queued"):
            raise ValueError("unsafe queue status: %s" % status)
        now = utc_now()
        cursor = self.connection.execute(
            "INSERT OR IGNORE INTO jobs VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                str(uuid.uuid4()),
                lane,
                fingerprint,
                stage,
                status,
                now,
                now,
                canonical_json(payload),
                reason,
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def next_job(self, stage: str) -> Optional[Dict[str, Any]]:
        row = self.connection.execute(
            "SELECT * FROM jobs WHERE stage = ? AND status = 'queued' "
            "ORDER BY created_at, rowid LIMIT 1",
            (stage,),
        ).fetchone()
        return dict(row) if row else None

    def update_job(
        self, job_id: str, status: str, payload: Mapping[str, Any], reason: str
    ) -> None:
        if status not in ("queued", "completed", "blocked", "failed"):
            raise ValueError("unsafe job status: %s" % status)
        self.connection.execute(
            "UPDATE jobs SET status = ?, updated_at = ?, payload_json = ?, reason = ? "
            "WHERE job_id = ?",
            (status, utc_now(), canonical_json(payload), reason, job_id),
        )
        self.connection.commit()

    def update_hypothesis_status(self, fingerprint: str, status: str) -> None:
        previous = self.connection.execute(
            "SELECT status, proposal_json FROM hypotheses WHERE fingerprint = ?",
            (fingerprint,),
        ).fetchone()
        self.connection.execute(
            "UPDATE hypotheses SET status = ? WHERE fingerprint = ?",
            (status, fingerprint),
        )
        self.connection.commit()
        if previous is not None and str(previous["status"]) != status:
            factory_generator.record_kill_feedback(
                self.generator_config,
                self.state_dir,
                status,
                previous["proposal_json"],
            )

    def jobs(self, stage: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        if status is None:
            rows = self.connection.execute(
                "SELECT * FROM jobs WHERE stage = ? ORDER BY created_at, rowid", (stage,)
            )
        else:
            rows = self.connection.execute(
                "SELECT * FROM jobs WHERE stage = ? AND status = ? ORDER BY created_at, rowid",
                (stage, status),
            )
        return [dict(row) for row in rows]

    def measured_fresh_window_starts(self, exclude_job_id: Optional[str] = None) -> set:
        starts = set()
        query = (
            "SELECT job_id, payload_json FROM jobs WHERE stage IN "
            "('fresh_resolved_holdout', 'fixed_forward_confirmation')"
        )
        parameters: tuple = ()
        if exclude_job_id is not None:
            query += " AND job_id != ?"
            parameters = (exclude_job_id,)
        for row in self.connection.execute(query, parameters):
            try:
                payload = json.loads(row["payload_json"])
            except (TypeError, ValueError):
                continue
            for field in ("completed_windows", "support_only_windows"):
                for window in payload.get(field, []):
                    start = window.get("start")
                    if start:
                        starts.add(str(start))
            for superseded in payload.get("superseded_fresh_holdout_windows", []):
                for window in superseded.get("windows", []):
                    start = window.get("start")
                    if start:
                        starts.add(str(start))
        return starts

    def accrual(self, fingerprint: str) -> Optional[Dict[str, Any]]:
        row = self.connection.execute(
            "SELECT * FROM evidence_accrual WHERE fingerprint = ?", (fingerprint,)
        ).fetchone()
        return dict(row) if row else None

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
        jobs = [
            dict(row)
            for row in self.connection.execute(
                "SELECT lane, stage, status, COUNT(*) AS count FROM jobs GROUP BY lane, stage, status"
            )
        ]
        last_cycle = self.connection.execute(
            "SELECT cycle_id, started_at, finished_at, status, details_json "
            "FROM cycles ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        return {
            "hypotheses": hypotheses,
            "jobs": jobs,
            "last_cycle": dict(last_cycle) if last_cycle else None,
        }


def seconds_since(raw: Optional[str]) -> float:
    if not raw:
        return math.inf
    try:
        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return (dt.datetime.now(dt.timezone.utc) - parsed).total_seconds()
    except ValueError:
        return math.inf


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


def run_command(command: Sequence[str], timeout: int, dry_run: bool) -> Dict[str, Any]:
    signature = tuple(command[1:3])
    if signature not in {
        ("strategy-builder", "registry-audit"),
        ("strategy-builder", "evolve-search"),
        ("strategy-builder", "rolling-history"),
        ("strategy-builder", "opportunity-policy-search"),
        ("strategy-builder", "opportunity-probability-search"),
        ("strategy-builder", "opportunity-probability-decision"),
        ("strategy-builder", "opportunity-pair-features"),
        ("strategy-builder", "opportunity-flow-features"),
        ("strategy-builder", "opportunity-flow-search"),
        ("strategy-builder", "opportunity-flow-decision"),
        ("strategy-builder", "opportunity-liquidity-search"),
        ("strategy-builder", "opportunity-liquidity-decision"),
        ("strategy-builder", "opportunity-exact-replay"),
    }:
        raise ValueError("command rejected by research-only allowlist: %r" % (signature,))
    if dry_run:
        return {"status": "dry_run", "command": list(command)}

    def lower_priority() -> None:
        with contextlib.suppress(OSError):
            os.nice(10)

    started = time.monotonic()
    completed = subprocess.run(
        list(command),
        cwd=str(ROOT),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
        preexec_fn=lower_priority,
    )
    return {
        "status": "completed" if completed.returncode == 0 else "failed",
        "returncode": completed.returncode,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-4000:],
    }


class LmStudioClient:
    def __init__(self, config: Mapping[str, Any], state_dir: Path) -> None:
        self.config = config
        self.state_dir = state_dir
        self.base_url = os.environ.get(
            str(config["base_url_env"]), str(config["default_base_url"])
        ).rstrip("/")
        self.model = os.environ.get(str(config["model_env"]), str(config["default_model"]))

    def readiness(self) -> Dict[str, Any]:
        model_url = self.base_url.rsplit("/v1", 1)[0] + "/api/v0/models/" + urllib.parse.quote(
            self.model, safe=""
        )
        request = urllib.request.Request(model_url, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=float(self.config["connect_timeout_seconds"])) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (OSError, ValueError, urllib.error.URLError) as error:
            return {"ready": False, "reason": type(error).__name__, "model": self.model}
        state = str(payload.get("state", "")).lower()
        loaded = state == "loaded"
        return {"ready": loaded, "state": state or "unknown", "model": self.model}

    def complete(
        self,
        system: str,
        user: str,
        schema_name: str,
        schema: Mapping[str, Any],
        temperature: float,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        if any(token in (system + " " + user).lower() for token in ("pnl", "wallet", "secret", "private_key")):
            raise ValueError("private or outcome-bearing LLM prompt rejected")
        model = model or self.model
        if self.config.get("disable_reasoning"):
            # A sampler must not think: reasoning tokens burn the output
            # budget before the JSON starts. reasoning_effort covers
            # gpt-oss-style models; the /no_think prefix covers the Qwen
            # family; models without a reasoning mode ignore both.
            system = "/no_think " + system
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "max_tokens": int(self.config["maximum_output_tokens"]),
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": schema_name, "strict": True, "schema": schema},
            },
        }
        if self.config.get("disable_reasoning"):
            payload["reasoning_effort"] = "low"
        if isinstance(self.config.get("extra_body"), dict):
            payload.update(self.config["extra_body"])
        body = canonical_json(payload).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + "/chat/completions",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        lock_path = self.state_dir / "locks/llm.lock"
        try:
            with CycleLock(lock_path):
                with urllib.request.urlopen(
                    request, timeout=float(self.config["request_timeout_seconds"])
                ) as response:
                    envelope = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            try:
                error_body = error.read(1000).decode("utf-8", errors="replace")
            except OSError:
                error_body = ""
            return {
                "ok": False,
                "reason": "HTTPError",
                "http_status": error.code,
                "error_body": error_body,
            }
        except (RuntimeError, OSError, ValueError, urllib.error.URLError) as error:
            return {"ok": False, "reason": type(error).__name__}
        try:
            content = envelope["choices"][0]["message"]["content"]
            parsed = json.loads(content)
        except (KeyError, IndexError, TypeError, ValueError) as error:
            return {"ok": False, "reason": "invalid_response_%s" % type(error).__name__}
        return {
            "ok": True,
            "value": parsed,
            "model": model,
            "prompt_sha256": hashlib.sha256((system + "\n" + user).encode()).hexdigest(),
            "response_sha256": stable_hash(parsed),
        }


LATE_PROPOSAL_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["title", "rationale", "expected_failure_mode", "rule"],
    "properties": {
        "title": {"type": "string"},
        "rationale": {"type": "string"},
        "expected_failure_mode": {"type": "string"},
        "rule": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "operator",
                "path_minutes",
                "minimum_two_minute_move_usd",
                "maximum_entry_price",
                "minimum_decision_buffer_usd",
                "settlement_sigma_buffer",
                "minimum_book_pressure",
                "direction",
            ],
            "properties": {
                "operator": {"type": "string", "enum": sorted(LATE_OPERATORS)},
                "path_minutes": {"type": "integer", "enum": sorted(LATE_PATH_MINUTES)},
                "minimum_two_minute_move_usd": {
                    "type": "integer",
                    "enum": sorted(LATE_MOVE_THRESHOLDS),
                },
                "maximum_entry_price": {
                    "type": "number",
                    "enum": sorted(LATE_MAXIMUM_ENTRY_PRICES),
                },
                "minimum_decision_buffer_usd": {
                    "type": "integer",
                    "enum": sorted(LATE_DECISION_BUFFERS_USD),
                },
                "settlement_sigma_buffer": {
                    "type": "number",
                    "enum": sorted(LATE_SETTLEMENT_SIGMA_BUFFERS),
                },
                "minimum_book_pressure": {
                    "type": "number",
                    "enum": sorted(LATE_MINIMUM_BOOK_PRESSURES),
                },
                "direction": {"type": "string", "enum": sorted(LATE_DIRECTIONS)},
            },
        },
    },
}

REVIEW_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "proposal_sha256",
        "verdict",
        "reason",
        "causality_risk",
        "duplication_risk",
        "execution_risk",
    ],
    "properties": {
        "proposal_sha256": {"type": "string"},
        "verdict": {"type": "string", "enum": ["accept", "reject"]},
        "reason": {"type": "string"},
        "causality_risk": {"type": "string"},
        "duplication_risk": {"type": "string"},
        "execution_risk": {"type": "string"},
    },
}


def normalized_late_rule(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """Add v6 execution defaults when reading an already-frozen v5 hypothesis."""
    rule = dict(raw)
    rule.setdefault("maximum_entry_price", 1.0)
    rule.setdefault("minimum_decision_buffer_usd", 0)
    rule.setdefault("settlement_sigma_buffer", 0.0)
    rule.setdefault("minimum_book_pressure", -1.0)
    rule.setdefault("direction", "both")
    return rule


def validate_late_proposal(proposal: Mapping[str, Any]) -> Dict[str, Any]:
    if set(proposal) != {"title", "rationale", "expected_failure_mode", "rule"}:
        raise ValueError("unexpected late proposal fields")
    raw_rule = proposal.get("rule")
    if not isinstance(raw_rule, dict):
        raise ValueError("invalid late rule fields")
    rule = normalized_late_rule(raw_rule)
    if set(rule) != {
        "operator",
        "path_minutes",
        "minimum_two_minute_move_usd",
        "maximum_entry_price",
        "minimum_decision_buffer_usd",
        "settlement_sigma_buffer",
        "minimum_book_pressure",
        "direction",
    }:
        raise ValueError("invalid late rule fields")
    operator = rule["operator"]
    path_minutes = rule["path_minutes"]
    threshold = rule["minimum_two_minute_move_usd"]
    maximum_entry_price = float(rule["maximum_entry_price"])
    minimum_decision_buffer = rule["minimum_decision_buffer_usd"]
    sigma_buffer = float(rule["settlement_sigma_buffer"])
    minimum_book_pressure = float(rule["minimum_book_pressure"])
    direction = str(rule["direction"])
    if operator not in LATE_OPERATORS:
        raise ValueError("operator is not allowlisted")
    if path_minutes not in LATE_PATH_MINUTES or threshold not in LATE_MOVE_THRESHOLDS:
        raise ValueError("rule value is outside the frozen grid")
    if operator == "path_only" and (path_minutes not in (3, 4) or threshold != 0):
        raise ValueError("path_only requires path 3/4 and zero move threshold")
    if operator == "move_only" and (path_minutes != 0 or threshold < 100):
        raise ValueError("move_only requires no path and threshold >= 100")
    if operator in ("path_and_move", "path_or_move") and (
        path_minutes not in (2, 3, 4) or threshold < 100
    ):
        raise ValueError("combined rule requires path 2/3/4 and threshold >= 100")
    if operator == "path_or_move" and (path_minutes != 4 or threshold != 200):
        raise ValueError("path_or_move is executable only for the frozen 4m-or-$200 tag")
    if maximum_entry_price not in LATE_MAXIMUM_ENTRY_PRICES:
        raise ValueError("maximum_entry_price is outside the frozen grid")
    if minimum_decision_buffer not in LATE_DECISION_BUFFERS_USD:
        raise ValueError("minimum_decision_buffer_usd is outside the frozen grid")
    if sigma_buffer not in LATE_SETTLEMENT_SIGMA_BUFFERS:
        raise ValueError("settlement_sigma_buffer is outside the frozen grid")
    if minimum_book_pressure not in LATE_MINIMUM_BOOK_PRESSURES:
        raise ValueError("minimum_book_pressure is outside the frozen grid")
    if direction not in LATE_DIRECTIONS:
        raise ValueError("direction is outside the frozen grid")
    for field in ("title", "rationale", "expected_failure_mode"):
        if not isinstance(proposal[field], str) or not proposal[field].strip():
            raise ValueError("%s must be a non-empty string" % field)
    validated = dict(proposal)
    validated["rule"] = rule
    return validated


def uniform_late_proposal(rng: random.Random) -> Dict[str, Any]:
    """One rule drawn uniformly from the executable late grid.

    Rejection sampling over the product of the schema enums stays uniform
    over the operator co-constraints that validate_late_proposal enforces."""
    grid = {
        field: spec["enum"]
        for field, spec in LATE_PROPOSAL_SCHEMA["properties"]["rule"]["properties"].items()
    }
    while True:
        rule = {field: rng.choice(values) for field, values in grid.items()}
        proposal = {
            "title": "Uniform control: %s" % factory_generator.compact_rule(rule),
            "rationale": (
                "Seeded uniform draw from the executable late grid; "
                "the control arm for the sampler."
            ),
            "expected_failure_mode": (
                "An unconditioned grid point carries no mechanism and is expected "
                "to fail the public directional screen."
            ),
            "rule": rule,
        }
        try:
            return validate_late_proposal(proposal)
        except ValueError:
            continue


def fallback_late_proposals() -> Iterable[Dict[str, Any]]:
    payoff_rules = [
        ("path_only", 3, 0, "both"),
        ("path_and_move", 3, 100, "both"),
        ("path_only", 3, 0, "down"),
        ("path_only", 3, 0, "up"),
        ("path_and_move", 3, 100, "down"),
        ("path_and_move", 3, 100, "up"),
        ("path_and_move", 2, 100, "down"),
        ("path_and_move", 2, 100, "up"),
    ]
    for operator, path_minutes, threshold, direction in payoff_rules:
        yield {
            "title": "%sm %s continuation with payoff-derived %.2f cap"
            % (path_minutes, direction, LATE_PAYOFF_DERIVED_ENTRY_CAP),
            "rationale": (
                "The fee-aware price ceiling is the largest $0.01 tick designed "
                "to recover one full stake loss within 50 equal winning fills; "
                "directional variants are frozen independently."
            ),
            "expected_failure_mode": (
                "The payoff ceiling or earlier decision may reduce executable support, "
                "and the cached family economics may reject the mechanism before exact replay."
            ),
            "rule": {
                "operator": operator,
                "path_minutes": path_minutes,
                "minimum_two_minute_move_usd": threshold,
                "maximum_entry_price": LATE_PAYOFF_DERIVED_ENTRY_CAP,
                "minimum_decision_buffer_usd": 0,
                "settlement_sigma_buffer": 0.0,
                "minimum_book_pressure": -1.0,
                "direction": direction,
            },
        }
    yield {
        "title": "Three-minute $200 path with a 0.90 price ceiling",
        "rationale": "Preserve the strong early-displacement mechanism while refusing thin near-one payoffs.",
        "expected_failure_mode": "The stricter price ceiling may leave too few fresh fills.",
        "rule": {
            "operator": "path_and_move",
            "path_minutes": 3,
            "minimum_two_minute_move_usd": 200,
            "maximum_entry_price": 0.90,
            "minimum_decision_buffer_usd": 200,
            "settlement_sigma_buffer": 0.0,
            "minimum_book_pressure": -1.0,
            "direction": "both",
        },
    }
    yield {
        "title": "Two-minute down continuation with non-negative book support",
        "rationale": "Retain the executable 0.95 payoff cap while excluding negative chosen-side book pressure diagnosed in the completed fresh block.",
        "expected_failure_mode": "The book-pressure guard may remove too many exact fills or fail on independent fresh windows.",
        "rule": {
            "operator": "path_and_move",
            "path_minutes": 2,
            "minimum_two_minute_move_usd": 100,
            "maximum_entry_price": 0.95,
            "minimum_decision_buffer_usd": 100,
            "settlement_sigma_buffer": 0.0,
            "minimum_book_pressure": -0.15,
            "direction": "down",
        },
    }
    yield {
        "title": "Two-minute up continuation with positive book support",
        "rationale": "Apply the symmetric continuation mechanism only when chosen-side book pressure is strictly positive.",
        "expected_failure_mode": "The positive-pressure gate may not preserve five exact fills or positive independent economics.",
        "rule": {
            "operator": "path_and_move",
            "path_minutes": 2,
            "minimum_two_minute_move_usd": 100,
            "maximum_entry_price": 0.95,
            "minimum_decision_buffer_usd": 100,
            "settlement_sigma_buffer": 0.0,
            "minimum_book_pressure": 0.15,
            "direction": "up",
        },
    }
    yield {
        "title": "Two-minute up continuation with non-negative book support",
        "rationale": "Relax the positive-pressure guard by one frozen bucket after independent economics passed but executable density failed.",
        "expected_failure_mode": "Neutral book states may reintroduce tail losses or still fail to produce five fresh fills.",
        "rule": {
            "operator": "path_and_move",
            "path_minutes": 2,
            "minimum_two_minute_move_usd": 100,
            "maximum_entry_price": 0.95,
            "minimum_decision_buffer_usd": 100,
            "settlement_sigma_buffer": 0.0,
            "minimum_book_pressure": -0.15,
            "direction": "up",
        },
    }
    yield {
        "title": "Four-minute path or $200 move with a 0.90 price ceiling",
        "rationale": "Raise only the payoff ceiling by one frozen step after the 0.85 variant proved historically unexecutable.",
        "expected_failure_mode": "The 0.90 ceiling may still produce too few fills or admit poor-payoff tail losses.",
        "rule": {
            "operator": "path_or_move",
            "path_minutes": 4,
            "minimum_two_minute_move_usd": 200,
            "maximum_entry_price": 0.90,
            "minimum_decision_buffer_usd": 200,
            "settlement_sigma_buffer": 0.1,
            "minimum_book_pressure": -1.0,
            "direction": "both",
        },
    }
    yield {
        "title": "Four-minute path or $200 move with a 0.95 price ceiling",
        "rationale": "Use the final frozen price step after both 0.85 and 0.90 variants proved historically unexecutable.",
        "expected_failure_mode": "The 0.95 ceiling may admit tail losses whose cost overwhelms the thin winning payoff.",
        "rule": {
            "operator": "path_or_move",
            "path_minutes": 4,
            "minimum_two_minute_move_usd": 200,
            "maximum_entry_price": 0.95,
            "minimum_decision_buffer_usd": 200,
            "settlement_sigma_buffer": 0.1,
            "minimum_book_pressure": -1.0,
            "direction": "both",
        },
    }
    yield {
        "title": "Four-minute path or $200 move with executable market pricing",
        "rationale": "Remove only the payoff ceiling after every preregistered ceiling through 0.95 produced zero historical fills; exact replay still measures real asks, fees, and net economics.",
        "expected_failure_mode": "Near-one fills may have too little payoff to absorb fees or a single terminal reversal.",
        "rule": {
            "operator": "path_or_move",
            "path_minutes": 4,
            "minimum_two_minute_move_usd": 200,
            "maximum_entry_price": 1.0,
            "minimum_decision_buffer_usd": 200,
            "settlement_sigma_buffer": 0.1,
            "minimum_book_pressure": -1.0,
            "direction": "both",
        },
    }
    yield {
        "title": "Four-minute path or $200 move with a $125 decision buffer",
        "rationale": "Relax only the decision margin by one frozen step after the $200 variant passed historical exact economics but its first independent signal produced no executable attempt.",
        "expected_failure_mode": "The additional lower-displacement entries may add terminal reversals or still fail to produce five independent fills.",
        "rule": {
            "operator": "path_or_move",
            "path_minutes": 4,
            "minimum_two_minute_move_usd": 200,
            "maximum_entry_price": 1.0,
            "minimum_decision_buffer_usd": 125,
            "settlement_sigma_buffer": 0.1,
            "minimum_book_pressure": -1.0,
            "direction": "both",
        },
    }
    yield {
        "title": "Four-minute path or $200 move with a $100 decision buffer",
        "rationale": "Use the final frozen decision-margin step after the $125 variant preserved independent positive economics but could produce at most four fresh fills.",
        "expected_failure_mode": "The denser lower-displacement sample may admit a terminal reversal whose asymmetric loss overwhelms the thin late-entry wins.",
        "rule": {
            "operator": "path_or_move",
            "path_minutes": 4,
            "minimum_two_minute_move_usd": 200,
            "maximum_entry_price": 1.0,
            "minimum_decision_buffer_usd": 100,
            "settlement_sigma_buffer": 0.1,
            "minimum_book_pressure": -1.0,
            "direction": "both",
        },
    }
    priority = [
        ("path_and_move", 3, 100, 0.85, 100, 0.1, "both"),
        ("path_and_move", 3, 100, 0.90, 100, 0.1, "both"),
        ("path_and_move", 3, 100, 0.90, 200, 0.0, "both"),
        ("path_and_move", 3, 100, 0.95, 100, 0.1, "both"),
        ("path_and_move", 3, 100, 0.90, 100, 0.1, "down"),
        ("path_and_move", 3, 100, 0.85, 100, 0.1, "down"),
        ("path_and_move", 3, 100, 0.95, 100, 0.1, "down"),
        ("path_and_move", 3, 100, 0.95, 0, 0.0, "down"),
        ("path_and_move", 3, 100, 0.90, 0, 0.0, "down"),
        ("path_only", 4, 0, 0.95, 0, 0.0, "down"),
        ("move_only", 0, 100, 0.85, 0, 0.0, "down"),
        ("path_only", 3, 0, 0.85, 0, 0.0, "down"),
        ("path_and_move", 2, 100, 0.85, 0, 0.0, "down"),
        ("path_and_move", 2, 100, 0.85, 0, 0.0, "both"),
        ("path_and_move", 2, 100, 0.95, 100, 0.0, "down"),
        ("path_and_move", 2, 100, 0.90, 125, 0.0, "down"),
        ("path_and_move", 4, 100, 0.90, 200, 0.0, "both"),
        ("move_only", 0, 200, 0.90, 200, 0.1, "both"),
        ("path_only", 3, 0, 0.85, 200, 0.2, "both"),
        ("path_or_move", 4, 200, 0.85, 200, 0.1, "both"),
    ]
    for operator, path_minutes, threshold, price, decision_buffer, sigma, direction in priority:
        yield {
            "title": "%s path=%sm move=$%s cap=%.2f buffer=$%s sigma=%.1f direction=%s"
            % (operator, path_minutes, threshold, price, decision_buffer, sigma, direction),
            "rationale": "Executable causal signal combined with payoff and settlement-margin controls.",
            "expected_failure_mode": "Fresh support or executable opportunity count may be insufficient.",
            "rule": {
                "operator": operator,
                "path_minutes": path_minutes,
                "minimum_two_minute_move_usd": threshold,
                "maximum_entry_price": price,
                "minimum_decision_buffer_usd": decision_buffer,
                "settlement_sigma_buffer": sigma,
                "minimum_book_pressure": -1.0,
                "direction": direction,
            },
        }
    base_rules = [
        ("path_and_move", 3, 100),
        ("path_and_move", 3, 200),
        ("path_and_move", 4, 100),
        ("path_and_move", 4, 200),
        ("path_or_move", 4, 200),
        ("move_only", 0, 100),
        ("move_only", 0, 200),
        ("path_only", 3, 0),
        ("path_only", 4, 0),
    ]
    for operator, path_minutes, threshold in base_rules:
        for price in (0.75, 0.85, 0.90, 0.95):
            for decision_buffer in (0, 100, 200):
                for sigma in (0.0, 0.1, 0.2):
                    if decision_buffer == 0 and sigma == 0.0 and price == 0.95:
                        continue
                    yield {
                        "title": "%s path=%sm move=$%s cap=%.2f buffer=$%s sigma=%.1f"
                        % (operator, path_minutes, threshold, price, decision_buffer, sigma),
                        "rationale": "Finite causal grid over signal, payoff ceiling, and settlement margin.",
                        "expected_failure_mode": "Selectivity may erase support or the edge may not replicate.",
                        "rule": {
                            "operator": operator,
                            "path_minutes": path_minutes,
                            "minimum_two_minute_move_usd": threshold,
                            "maximum_entry_price": price,
                            "minimum_decision_buffer_usd": decision_buffer,
                            "settlement_sigma_buffer": sigma,
                            "minimum_book_pressure": -1.0,
                            "direction": "both",
                        },
                    }


def sign(value: float) -> int:
    return 1 if value > 0 else (-1 if value < 0 else 0)


def load_public_windows(path: Path) -> List[Dict[str, Any]]:
    windows: Dict[int, Dict[str, Any]] = {}
    with gzip.open(str(path), "rt", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            key = int(row["window_start"])
            normalized = {
                field: row[field]
                for field in (
                    "window_start",
                    "utc_day",
                    "utc_hour",
                    "chronological_window",
                    "p0",
                    "p60",
                    "p120",
                    "p180",
                    "p240",
                    "terminal",
                )
            }
            existing = windows.get(key)
            if existing is not None and canonical_json(existing) != canonical_json(normalized):
                raise ValueError("inconsistent duplicate public window %s" % key)
            windows[key] = normalized
    return [windows[key] for key in sorted(windows)]


def wilson_lower(wins: int, total: int, z: float = 1.959963984540054) -> Optional[float]:
    if total == 0:
        return None
    probability = wins / float(total)
    denominator = 1.0 + z * z / total
    centre = probability + z * z / (2.0 * total)
    spread = z * math.sqrt(probability * (1.0 - probability) / total + z * z / (4.0 * total * total))
    return (centre - spread) / denominator


def score_records(records: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    total = len(records)
    wins = sum(1 for row in records if row["won"])
    return {
        "signals": total,
        "wins": wins,
        "losses": total - wins,
        "accuracy": wins / float(total) if total else None,
        "wilson_95_lower": wilson_lower(wins, total),
    }


def group_scores(records: Sequence[Mapping[str, Any]], field: str) -> Dict[str, Any]:
    groups: Dict[str, List[Mapping[str, Any]]] = {}
    for row in records:
        groups.setdefault(str(row[field]), []).append(row)
    return {key: score_records(value) for key, value in sorted(groups.items())}


def rank_replay_buckets(
    buckets: Mapping[Tuple[str, int], Mapping[str, int]]
) -> List[Tuple[Tuple[str, int], Mapping[str, int]]]:
    """Put one causally up-rich and one down-rich window before total-support order."""
    by_total = sorted(
        buckets.items(),
        key=lambda item: (-int(item[1]["public_signals"]), item[0]),
    )
    ranked: List[Tuple[Tuple[str, int], Mapping[str, int]]] = []
    selected = set()
    for direction in ("up", "down"):
        key = "public_%s_signals" % direction
        choices = [item for item in by_total if item[0] not in selected and int(item[1][key]) > 0]
        if choices:
            choice = min(
                choices,
                key=lambda item: (
                    -int(item[1][key]),
                    -int(item[1]["public_signals"]),
                    item[0],
                ),
            )
            ranked.append(choice)
            selected.add(choice[0])
    ranked.extend(item for item in by_total if item[0] not in selected)
    return ranked


def causal_late_signal(
    row: Mapping[str, Any], proposal: Mapping[str, Any]
) -> Optional[Dict[str, Any]]:
    """Evaluate only checkpoints available at the frozen decision time."""
    rule = normalized_late_rule(proposal["rule"])
    path_minutes = int(rule["path_minutes"])
    threshold = float(rule["minimum_two_minute_move_usd"])
    decision_offset = max(120 if threshold else 0, path_minutes * 60)
    p0 = float(row["p0"])
    decision_price = float(row["p%s" % decision_offset])
    direction = sign(decision_price - p0)
    if direction == 0:
        return None
    direction_name = "up" if direction > 0 else "down"
    selected_direction = str(rule["direction"])
    if selected_direction != "both" and direction_name != selected_direction:
        return None
    prior = p0
    path_aligned = path_minutes > 0
    for name in ["p60", "p120", "p180", "p240"][:path_minutes]:
        current = float(row[name])
        path_aligned = path_aligned and sign(current - prior) == direction
        prior = current
    move = float(row["p120"]) - p0
    move_aligned = threshold > 0 and abs(move) >= threshold and sign(move) == direction
    eligible = {
        "path_only": path_aligned,
        "move_only": move_aligned,
        "path_and_move": path_aligned and move_aligned,
        "path_or_move": path_aligned or move_aligned,
    }[str(rule["operator"])]
    decision_buffer = abs(decision_price - p0)
    if not eligible or decision_buffer < float(rule["minimum_decision_buffer_usd"]):
        return None
    return {
        "direction": direction,
        "direction_name": direction_name,
        "decision_offset_seconds": decision_offset,
        "decision_price": decision_price,
        "decision_buffer_usd": decision_buffer,
    }


def chronological_forward_windows(
    windows: Sequence[Mapping[str, Any]],
    proposal: Mapping[str, Any],
    sealed_at: str,
    excluded_starts: Sequence[str],
) -> List[Dict[str, Any]]:
    """Return post-seal signal-hour windows without reading terminal outcomes."""
    seal_s = dt.datetime.fromisoformat(sealed_at.replace("Z", "+00:00")).timestamp()
    excluded = set(excluded_starts)
    buckets: Dict[Tuple[str, int], Dict[str, int]] = {}
    for row in windows:
        if int(row["window_start"]) <= seal_s:
            continue
        signal = causal_late_signal(row, proposal)
        if signal is None:
            continue
        bucket = (str(row["utc_day"]), int(row["utc_hour"]))
        start = "%sT%02d:00:00Z" % bucket
        if start in excluded:
            continue
        stats = buckets.setdefault(
            bucket,
            {"public_signals": 0, "public_up_signals": 0, "public_down_signals": 0},
        )
        stats["public_signals"] += 1
        stats[
            "public_up_signals"
            if signal["direction_name"] == "up"
            else "public_down_signals"
        ] += 1
    return [
        {
            "start": "%sT%02d:00:00Z" % (day, hour),
            "end": "%sT%02d:00:00Z" % (day, hour),
            **stats,
            "selection_basis": "chronological_post_seal_causal_features_only",
        }
        for (day, hour), stats in sorted(buckets.items())
    ]


def evaluate_late_rule(
    windows: Sequence[Mapping[str, Any]],
    proposal: Mapping[str, Any],
    gates: Mapping[str, Any],
    excluded_fresh_window_starts: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    rule = normalized_late_rule(proposal["rule"])
    operator = rule["operator"]
    path_minutes = int(rule["path_minutes"])
    threshold = float(rule["minimum_two_minute_move_usd"])
    minimum_decision_buffer = float(rule["minimum_decision_buffer_usd"])
    selected_direction = str(rule["direction"])
    decision_offset = max(120 if threshold else 0, path_minutes * 60)
    checkpoint_names = ["p60", "p120", "p180", "p240"][:path_minutes]
    records: List[Dict[str, Any]] = []
    fresh_replay_buckets: Dict[Tuple[str, int], Dict[str, int]] = {}
    excluded_starts = set(excluded_fresh_window_starts or [])
    excluded_fresh_bucket_starts = set()
    for row in windows:
        p0 = float(row["p0"])
        decision_price = float(row["p%s" % decision_offset])
        direction = sign(decision_price - p0)
        if direction == 0:
            continue
        direction_name = "up" if direction > 0 else "down"
        if selected_direction != "both" and direction_name != selected_direction:
            continue
        prior = p0
        path_aligned = bool(checkpoint_names)
        for name in checkpoint_names:
            current = float(row[name])
            path_aligned = path_aligned and sign(current - prior) == direction
            prior = current
        move = float(row["p120"]) - p0
        move_aligned = threshold > 0 and abs(move) >= threshold and sign(move) == direction
        eligible = {
            "path_only": path_aligned,
            "move_only": move_aligned,
            "path_and_move": path_aligned and move_aligned,
            "path_or_move": path_aligned or move_aligned,
        }[operator]
        decision_buffer = abs(decision_price - p0)
        if not eligible or decision_buffer < minimum_decision_buffer:
            continue
        if str(row["chronological_window"]) == "fresh_holdout":
            bucket = (
                str(row["utc_day"]),
                (int(row["utc_hour"]) // LATE_REPLAY_BUCKET_HOURS)
                * LATE_REPLAY_BUCKET_HOURS,
            )
            bucket_start = "%sT%02d:00:00Z" % bucket
            if bucket_start in excluded_starts:
                excluded_fresh_bucket_starts.add(bucket_start)
                continue
            stats = fresh_replay_buckets.setdefault(
                bucket,
                {"public_signals": 0, "public_up_signals": 0, "public_down_signals": 0},
            )
            stats["public_signals"] += 1
            stats["public_up_signals" if direction > 0 else "public_down_signals"] += 1
            continue
        terminal_direction = sign(float(row["terminal"]) - p0)
        if terminal_direction == 0:
            continue
        records.append(
            {
                "window_start": row["window_start"],
                "utc_day": row["utc_day"],
                "utc_hour": int(row["utc_hour"]),
                "chronological_window": row["chronological_window"],
                "direction": direction_name,
                "won": direction == terminal_direction,
                "decision_buffer_usd": decision_buffer,
                "post_decision_continued": sign(float(row["terminal"]) - decision_price) == direction,
            }
        )
    overall = score_records(records)
    by_direction = group_scores(records, "direction")
    by_chronology = group_scores(records, "chronological_window")
    support_ok = overall["signals"] >= int(gates["minimum_signals"])
    wilson_ok = bool(
        overall["wilson_95_lower"] is not None
        and overall["wilson_95_lower"] >= float(gates["minimum_wilson_lower"])
    )
    expected_directions = {"down", "up"} if selected_direction == "both" else {selected_direction}
    direction_ok = set(by_direction) == expected_directions and all(
        value["accuracy"] >= float(gates["minimum_group_accuracy"])
        for value in by_direction.values()
    )
    chronology_ok = len(by_chronology) >= 2 and all(
        value["accuracy"] >= float(gates["minimum_group_accuracy"])
        for value in by_chronology.values()
    )
    buffers = sorted(float(row["decision_buffer_usd"]) for row in records)
    replay_buckets: Dict[Tuple[str, int], Dict[str, int]] = {}
    for row in records:
        bucket = (
            str(row["utc_day"]),
            (int(row["utc_hour"]) // LATE_REPLAY_BUCKET_HOURS)
            * LATE_REPLAY_BUCKET_HOURS,
        )
        stats = replay_buckets.setdefault(
            bucket,
            {"public_signals": 0, "public_up_signals": 0, "public_down_signals": 0},
        )
        stats["public_signals"] += 1
        stats["public_%s_signals" % row["direction"]] += 1
    candidate_replay_windows = [
        {
            "start": "%sT%02d:00:00Z" % (day, start_hour),
            "end": "%sT%02d:00:00Z"
            % (day, start_hour + LATE_REPLAY_BUCKET_HOURS - 1),
            **stats,
        }
        for (day, start_hour), stats in rank_replay_buckets(replay_buckets)
    ]
    fresh_candidate_windows = [
        {
            "start": "%sT%02d:00:00Z" % (day, start_hour),
            "end": "%sT%02d:00:00Z"
            % (day, start_hour + LATE_REPLAY_BUCKET_HOURS - 1),
            **stats,
            "selection_basis": "causal_features_only_terminal_outcome_unread",
        }
        for (day, start_hour), stats in rank_replay_buckets(fresh_replay_buckets)
    ]
    fresh_capacity_signals = sum(
        int(window["public_signals"])
        for window in fresh_candidate_windows[: int(gates.get("maximum_fresh_windows", len(fresh_candidate_windows)))]
    )
    fresh_capacity_ok = fresh_capacity_signals >= int(gates.get("minimum_fresh_signals", 0))
    return {
        "schema_version": 1,
        "evaluator_version": LATE_EVALUATOR_VERSION,
        "generated_at": utc_now(),
        "stage": "public_directional_screen",
        "research_only": True,
        "proposal": proposal,
        "decision_offset_seconds": decision_offset,
        "source_window_count": len(windows),
        "discovery_window_count": sum(
            1 for row in windows if str(row["chronological_window"]) != "fresh_holdout"
        ),
        "fresh_holdout_window_count": sum(
            1 for row in windows if str(row["chronological_window"]) == "fresh_holdout"
        ),
        "overall": overall,
        "directions": by_direction,
        "chronological_windows": by_chronology,
        "decision_buffer_usd": {
            "median": buffers[len(buffers) // 2] if buffers else None,
            "minimum": buffers[0] if buffers else None,
            "maximum": buffers[-1] if buffers else None,
        },
        "post_decision_continuation_rate": (
            sum(1 for row in records if row["post_decision_continued"]) / float(len(records))
            if records
            else None
        ),
        "candidate_replay_windows": candidate_replay_windows,
        "fresh_candidate_windows": fresh_candidate_windows,
        "fresh_candidate_selection_is_outcome_blind": True,
        "fresh_candidate_windows_are_globally_unmeasured": True,
        "fresh_previously_measured_exclusion": {
            "policy": "global_measured_window_reserve_v1",
            "input_window_count": len(excluded_starts),
            "matching_candidate_window_count": len(excluded_fresh_bucket_starts),
            "input_starts_sha256": stable_hash(sorted(excluded_starts)),
        },
        "fresh_capacity_signals": fresh_capacity_signals,
        "gates": {
            "support": support_ok,
            "wilson_lower": wilson_ok,
            "direction_stability": direction_ok,
            "chronological_stability": chronology_ok,
            "fresh_support_capacity": fresh_capacity_ok,
        },
        "warnings": {
            "groups_below_nominal_support": {
                "directions": [
                    key
                    for key, value in by_direction.items()
                    if value["signals"] < int(gates["minimum_group_signals"])
                ],
                "chronological_windows": [
                    key
                    for key, value in by_chronology.items()
                    if value["signals"] < int(gates["minimum_group_signals"])
                ],
            }
        },
        "stage_1_survivor": (
            support_ok and wilson_ok and direction_ok and chronology_ok and fresh_capacity_ok
        ),
        "does_not_establish": ["executable edge", "fills", "net economics", "promotion eligibility"],
    }


def propose_late_rule(
    client: LmStudioClient,
    ledger: Ledger,
    snapshot_hash: str,
    config: Optional[Mapping[str, Any]] = None,
    state_dir: Optional[Path] = None,
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], Dict[str, Any]]:
    gen_cfg = factory_generator.generator_config(config)
    readiness = client.readiness()
    proposal_result: Dict[str, Any] = {"readiness": readiness}
    proposal: Optional[Dict[str, Any]] = None
    review: Optional[Dict[str, Any]] = None
    late_rows = ledger.late_hypotheses()
    if len(late_rows) < DIAGNOSTIC_PROPOSAL_PREFIX:
        for fallback in fallback_late_proposals():
            validated = validate_late_proposal(fallback)
            if not ledger.has_late_rule(validated["rule"]):
                proposal_result["fallback"] = "diagnostic_priority"
                proposal_result["proposal_source"] = "diagnostic"
                return validated, None, proposal_result
    killed_items: List[Dict[str, Any]] = []
    if readiness["ready"]:
        # Strict alternation: the arm with fewer ledger entries goes next, LLM on ties.
        llm_arm = sum(1 for row in late_rows if row["source"] in LATE_LLM_ARM_SOURCES)
        control_arm = sum(1 for row in late_rows if row["source"] == "uniform_control")
        proposal_result["turn"] = "llm" if llm_arm <= control_arm else "uniform_control"
        if gen_cfg["negative_prompt_enabled"] or gen_cfg["novelty_gate_enabled"]:
            registry_raw = (config or {}).get("registry_path")
            killed_items = factory_generator.killed_negative_items(
                resolve_repo_path(str(registry_raw)) if registry_raw else None, late_rows
            )

    def novelty_rejected(candidate: Mapping[str, Any]) -> bool:
        # One gate for both arms: the sampler A/B must not filter the LLM arm only.
        if not gen_cfg["novelty_gate_enabled"] or state_dir is None:
            return False
        novelty = factory_generator.novelty_check(
            candidate,
            gen_cfg,
            client.base_url,
            float(client.config["request_timeout_seconds"]),
            state_dir,
            killed_items,
        )
        return novelty.get("status") == "rejected"

    if proposal_result.get("turn") == "llm":
        system = (
            "You propose one bounded causal rule from a public Bitcoin five-minute continuation study. "
            "Return only the strict JSON object. Do not request code, files, commands, private data, outcomes, scores, or economics."
        )
        user = (
            "Choose one not-obviously-redundant rule. Available checkpoints are 60, 120, 180 and 240 seconds; "
            "settlement is after the decision. path_minutes must be 0, 2, 3 or 4. The two-minute move threshold must "
            "be one of 0,100,200 USD because those are the executable runtime buckets. path_only uses path 3/4 and threshold 0; "
            "move_only uses path 0 and threshold >=100; AND rules use path 2/3/4 and threshold >=100; "
            "OR is executable only with path 4 and threshold 200. maximum_entry_price must be one of "
            "0.75,0.85,0.90,0.95,1.0. minimum_decision_buffer_usd must be 0,100,125,200. "
            "settlement_sigma_buffer must be 0.0,0.1,0.2. direction must be both,up,down. "
            "The rule predicts the terminal side visible "
            "at the causal decision checkpoint; price and settlement buffers protect the asymmetric payoff."
        )
        if state_dir is not None:
            queued = factory_generator.queue_pop(state_dir)
            if queued and isinstance(queued.get("proposal"), dict):
                try:
                    replay = validate_late_proposal(queued["proposal"])
                    if not ledger.has_late_rule(replay["rule"]):
                        proposal_result["from_burst_queue"] = True
                        proposal_result["proposal_source"] = "burst_queue"
                        proposal_result["eoh_operator"] = queued.get("operator")
                        if queued.get("sampler_model"):
                            proposal_result["sampler_model"] = queued["sampler_model"]
                        return replay, None, proposal_result
                except ValueError:
                    pass
        extra_sections: List[str] = []
        if gen_cfg["eoh_operators_enabled"]:
            parents = factory_generator.eoh_parents(late_rows)
            operator = factory_generator.select_eoh_operator(len(late_rows), bool(parents))
            section, parent_fingerprints = factory_generator.eoh_prompt_section(
                operator, parents
            )
            extra_sections.append(section)
            proposal_result["eoh_operator"] = operator
            if parent_fingerprints:
                proposal_result["eoh_parent_fingerprints"] = parent_fingerprints
        if gen_cfg["negative_prompt_enabled"]:
            negatives = factory_generator.negative_prompt_text(killed_items)
            if negatives:
                extra_sections.append(negatives)
                proposal_result["negative_prompt_chars"] = len(negatives)
        if gen_cfg["kill_feedback_enabled"] and state_dir is not None:
            feedback = factory_generator.kill_feedback_prompt_text(state_dir)
            if feedback:
                extra_sections.append(feedback)
        if extra_sections:
            user = user + "\n" + "\n".join(extra_sections)
        temperature = factory_generator.operator_temperature(
            proposal_result.get("eoh_operator"), gen_cfg
        )
        burst_n = max(1, int(gen_cfg["samples_per_burst"]))
        llm_cfg = (config or {}).get("llm") or {}
        sampler_model = factory_generator.next_sampler_model(
            llm_cfg, ledger, "late_window_mechanisms"
        )
        if sampler_model:
            proposal_result["sampler_model"] = sampler_model
        survivors: List[Dict[str, Any]] = []
        burst_stats = {"generated": 0, "invalid": 0, "duplicate": 0, "novelty_rejected": 0}
        generated: Dict[str, Any] = {"ok": False, "reason": "no_samples"}
        use_constrained = bool(gen_cfg.get("constrained_schema"))
        for sample_index in range(burst_n):
            schema = (
                factory_generator.constrained_proposal_schema(LATE_PROPOSAL_SCHEMA)
                if use_constrained
                else LATE_PROPOSAL_SCHEMA
            )
            schema_name = (
                "late_window_proposal_v2c" if use_constrained else "late_window_proposal_v1"
            )
            try:
                generated = client.complete(
                    system, user, schema_name, schema, temperature, model=sampler_model
                )
            except ValueError as error:
                generated = {"ok": False, "reason": "prompt_guard_%s" % error}
                break
            if (
                use_constrained
                and sample_index == 0
                and generated.get("reason") == "HTTPError"
                and 400 <= int(generated.get("http_status") or 0) < 500
                and any(
                    token in str(generated.get("error_body", "")).lower()
                    for token in ("schema", "grammar")
                )
            ):
                # A backend that cannot compile anyOf branches rejects the
                # request outright; fall back to the legacy schema for the
                # rest of the burst rather than losing it.  A timeout, a
                # connection error or a model-load failure (a cold sampler)
                # is not a schema problem and must not degrade the burst.
                use_constrained = False
                proposal_result["constrained_schema_fallback"] = generated.get("reason")
                continue
            burst_stats["generated"] += 1
            if not generated.get("ok"):
                continue
            try:
                candidate = validate_late_proposal(generated["value"])
            except ValueError:
                burst_stats["invalid"] += 1
                continue
            if ledger.has_late_rule(candidate["rule"]) or any(
                normalized_late_rule(candidate["rule"]) == normalized_late_rule(seen["rule"])
                for seen in survivors
            ):
                burst_stats["duplicate"] += 1
                continue
            if novelty_rejected(candidate):
                burst_stats["novelty_rejected"] += 1
                continue
            survivors.append(candidate)
        proposal_result["burst"] = {
            **burst_stats,
            "temperature": temperature,
            "survivors": len(survivors),
            "constrained_schema": use_constrained,
        }
        proposal_result["generator"] = {key: value for key, value in generated.items() if key != "value"}
        if survivors:
            proposal = survivors[0]
            proposal_result["proposal_source"] = "llm"
            if len(survivors) > 1 and state_dir is not None:
                factory_generator.queue_push(
                    state_dir,
                    [
                        {
                            "proposal": extra,
                            "operator": proposal_result.get("eoh_operator"),
                            "sampler_model": sampler_model,
                        }
                        for extra in survivors[1:]
                    ],
                )
        if proposal is not None:
            # Advisory: the verdict is persisted in review_json and counted by
            # factory_kpi, never swapped for a grid rule - a reviewer gate on
            # this arm alone would bias the LLM-versus-uniform verdict.
            proposal_sha = stable_hash(proposal)
            review_system = (
                "Review a public causal rule for leakage, duplication, and likely execution-price failure. "
                "Return strict JSON. Do not use or request outcomes, scores, economics, code, commands, or private data."
            )
            review_user = "proposal_sha256=%s\nproposal=%s" % (proposal_sha, canonical_json(proposal))
            reviewed = client.complete(
                review_system,
                review_user,
                "late_window_review_v1",
                REVIEW_SCHEMA,
                0.0,
                model=llm_cfg.get("reviewer_model"),
            )
            proposal_result["reviewer"] = {key: value for key, value in reviewed.items() if key != "value"}
            if reviewed.get("ok") and isinstance(reviewed.get("value"), dict):
                candidate_review = reviewed["value"]
                if candidate_review.get("proposal_sha256") == proposal_sha:
                    review = candidate_review
                else:
                    proposal_result["review_hash_mismatch"] = True

    if proposal is None and readiness["ready"]:
        # The control turn, or an LLM turn whose burst left no survivor: the
        # draw keeps the control arm advancing (a failing model cannot hold
        # the turn) and the grid stays the not-ready fallback.
        rng = random.Random(len(late_rows))
        control_stats = {"draws": 0, "duplicate": 0, "novelty_rejected": 0}
        proposal_result["control"] = control_stats
        for _ in range(UNIFORM_LATE_ATTEMPTS):
            control = uniform_late_proposal(rng)
            control_stats["draws"] += 1
            if ledger.has_late_rule(control["rule"]):
                control_stats["duplicate"] += 1
                continue
            if novelty_rejected(control):
                control_stats["novelty_rejected"] += 1
                continue
            proposal_result["proposal_source"] = "uniform_control"
            return control, None, proposal_result
    if proposal is None:
        for fallback in fallback_late_proposals():
            validated = validate_late_proposal(fallback)
            if not ledger.has_late_rule(validated["rule"]):
                proposal = validated
                proposal_result["fallback"] = "deterministic_finite_grid"
                proposal_result["proposal_source"] = "fallback_grid"
                break
    if proposal is None:
        raise RuntimeError("late-window finite grid exhausted for this snapshot")
    return proposal, review, proposal_result


def compile_late_variant(
    proposal: Mapping[str, Any], base_variant_path: Path, output: Path, fingerprint: str
) -> Dict[str, Any]:
    """Compile the finite public DSL into existing default-off causal tags."""
    validated = validate_late_proposal(proposal)
    payload = json.loads(base_variant_path.read_text())
    variants = payload if isinstance(payload, list) else [payload]
    if len(variants) != 1 or not isinstance(variants[0], dict):
        raise ValueError("late-window base variant must contain exactly one variant")
    variant = copy.deepcopy(variants[0])
    rule = normalized_late_rule(validated["rule"])
    operator = rule["operator"]
    path_minutes = int(rule["path_minutes"])
    threshold = int(rule["minimum_two_minute_move_usd"])
    direction = str(rule["direction"])
    require_tags: Dict[str, str] = {}
    require_values: Dict[str, List[str]] = {}
    if operator == "path_or_move":
        require_tags["article_path_4m_or_move_2m_200"] = "aligned"
    else:
        if operator in ("path_only", "path_and_move"):
            require_tags["article_path_%sm" % path_minutes] = "aligned"
        if operator in ("move_only", "path_and_move"):
            require_values["article_move_2m"] = (
                ["aligned_ge_200"]
                if threshold == 200
                else ["aligned_100_200", "aligned_ge_200"]
            )
    if direction != "both":
        require_tags["direction"] = direction
    selectivity: Dict[str, Any] = {}
    if require_tags:
        selectivity["require_tags"] = require_tags
    if require_values:
        selectivity["require_tag_values"] = require_values
    variant["name"] = "research_late_%s_%s" % (operator, fingerprint[:12])
    variant["selectivity"] = selectivity
    zone_config = variant.setdefault("zone_config", {})
    zone_config["max_price"] = float(rule["maximum_entry_price"])
    decision_buffer = float(rule["minimum_decision_buffer_usd"])
    sigma_buffer = float(rule["settlement_sigma_buffer"])
    minimum_book_pressure = float(rule["minimum_book_pressure"])
    if decision_buffer > 0.0 or sigma_buffer > 0.0:
        zone_config["settlement_guard_minutes"] = 5.0
        zone_config["settlement_min_abs_move_usd"] = decision_buffer
        zone_config["settlement_sigma_buffer"] = sigma_buffer
    else:
        zone_config["settlement_guard_minutes"] = 0.0
        zone_config["settlement_min_abs_move_usd"] = 0.0
        zone_config["settlement_sigma_buffer"] = 0.0
    microstructure = variant.setdefault("microstructure", {})
    microstructure["min_book_pressure"] = minimum_book_pressure
    atomic_json(output, [variant])
    return {
        "path": str(output),
        "sha256": sha256_file(output),
        "base_path": str(base_variant_path),
        "base_sha256": sha256_file(base_variant_path),
        "strategy_name": variant["name"],
        "selectivity": selectivity,
        "execution_filters": {
            "maximum_entry_price": zone_config["max_price"],
            "settlement_guard_minutes": zone_config["settlement_guard_minutes"],
            "minimum_decision_buffer_usd": zone_config["settlement_min_abs_move_usd"],
            "settlement_sigma_buffer": zone_config["settlement_sigma_buffer"],
            "minimum_book_pressure": microstructure["min_book_pressure"],
        },
    }


def download_atomic(url: str, destination: Path, timeout: int) -> None:
    if destination.is_file():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name("%s.tmp.%s" % (destination.name, os.getpid()))
    request = urllib.request.Request(url, headers={"User-Agent": "PolyMomentumResearch/1"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output)
        os.replace(str(temporary), str(destination))
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
        raise


def build_verified_btc_tape(day: str, data_dir: Path, base_url: str) -> Dict[str, Any]:
    archive_name = "BTCUSDT-1s-%s.zip" % day
    archive = data_dir / archive_name
    checksum = data_dir / (archive_name + ".CHECKSUM")
    url = "%s/%s" % (base_url.rstrip("/"), archive_name)
    download_atomic(url, archive, 120)
    download_atomic(url + ".CHECKSUM", checksum, 30)
    expected_hash, expected_name = checksum.read_text().strip().split()
    if expected_name.lstrip("*") != archive.name or sha256_file(archive) != expected_hash:
        raise ValueError("BTC archive checksum mismatch for %s" % day)
    tape = data_dir / ("btc_%s.csv" % day.replace("-", ""))
    if not tape.is_file():
        tape.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(str(archive)) as bundle:
            members = bundle.namelist()
            if len(members) != 1:
                raise ValueError("BTC archive must contain exactly one CSV")
            with tempfile.NamedTemporaryFile(
                "w", newline="", dir=str(tape.parent), prefix=tape.name + ".tmp.", delete=False
            ) as raw_output:
                writer = csv.writer(raw_output)
                writer.writerow(["timestamp_ms", "source", "price"])
                with bundle.open(members[0]) as compressed:
                    rows = csv.reader(line.decode("utf-8") for line in compressed)
                    for row in rows:
                        if len(row) < 7:
                            continue
                        timestamp = int(row[6])
                        if timestamp >= 10 ** 15:
                            timestamp //= 1000
                        writer.writerow([timestamp, "binance_btcusdt", float(row[4])])
                temporary = Path(raw_output.name)
        os.replace(str(temporary), str(tape))
    return {
        "archive": str(archive),
        "archive_sha256": sha256_file(archive),
        "tape": str(tape),
        "tape_sha256": sha256_file(tape),
    }


def build_verified_btc_tape_for_window(
    start: str, end: str, data_dir: Path, base_url: str
) -> Dict[str, Any]:
    """Build a tape with the one-hour warmup/cooldown required by rolling-history."""
    start_dt = dt.datetime.fromisoformat(start.replace("Z", "+00:00")) - dt.timedelta(hours=1)
    end_dt = dt.datetime.fromisoformat(end.replace("Z", "+00:00")) + dt.timedelta(hours=1)
    days: List[str] = []
    day = start_dt.date()
    while day <= end_dt.date():
        days.append(day.isoformat())
        day += dt.timedelta(days=1)
    sources = [build_verified_btc_tape(day_text, data_dir, base_url) for day_text in days]
    if len(sources) == 1:
        return sources[0]
    key = "%s_%s" % (
        start_dt.strftime("%Y%m%dT%H"),
        end_dt.strftime("%Y%m%dT%H"),
    )
    tape = data_dir / ("btc_window_%s.csv" % key)
    if not tape.is_file():
        with tempfile.NamedTemporaryFile(
            "w", newline="", dir=str(data_dir), prefix=tape.name + ".tmp.", delete=False
        ) as output:
            writer = csv.writer(output)
            writer.writerow(["timestamp_ms", "source", "price"])
            for source in sources:
                with Path(source["tape"]).open(newline="") as handle:
                    rows = csv.reader(handle)
                    next(rows, None)
                    writer.writerows(rows)
            temporary = Path(output.name)
        os.replace(str(temporary), str(tape))
    return {
        "archives": [source["archive"] for source in sources],
        "archive_sha256": [source["archive_sha256"] for source in sources],
        "tape": str(tape),
        "tape_sha256": sha256_file(tape),
        "coverage_start": start_dt.isoformat(),
        "coverage_end": end_dt.isoformat(),
    }


def build_public_windows_from_1m_archive(archive: Path) -> List[Dict[str, Any]]:
    """Build causal five-minute checkpoints from a checksum-verified Binance archive."""
    minutes: Dict[int, Tuple[float, float]] = {}
    with zipfile.ZipFile(str(archive)) as bundle:
        members = bundle.namelist()
        if len(members) != 1:
            raise ValueError("Binance 1m archive must contain exactly one CSV")
        with bundle.open(members[0]) as compressed:
            rows = csv.reader(line.decode("utf-8") for line in compressed)
            for row in rows:
                if len(row) < 7:
                    continue
                timestamp_ms = int(row[0])
                if timestamp_ms >= 10 ** 15:
                    timestamp_ms //= 1000
                minute_s = timestamp_ms // 1000
                minutes[minute_s] = (float(row[1]), float(row[4]))
    windows: List[Dict[str, Any]] = []
    for start_s in sorted(timestamp for timestamp in minutes if timestamp % 300 == 0):
        points = [minutes.get(start_s + offset) for offset in (0, 60, 120, 180, 240)]
        if any(point is None for point in points):
            continue
        assert all(point is not None for point in points)
        observed = [point for point in points if point is not None]
        timestamp = dt.datetime.fromtimestamp(start_s, tz=dt.timezone.utc)
        windows.append(
            {
                "window_start": start_s,
                "utc_day": timestamp.date().isoformat(),
                "utc_hour": timestamp.hour,
                "p0": observed[0][0],
                "p60": observed[1][0],
                "p120": observed[2][0],
                "p180": observed[3][0],
                "p240": observed[4][0],
                "terminal": observed[4][1],
            }
        )
    return windows


CAUSAL_PUBLIC_WINDOW_FIELDS = (
    "window_start",
    "utc_day",
    "utc_hour",
    "chronological_window",
    "p0",
    "p60",
    "p120",
    "p180",
    "p240",
)
PUBLIC_WINDOW_LABEL_FIELDS = ("window_start", "terminal")


def atomic_gzip_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    """Write deterministic gzip JSONL with an atomic rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("%s.tmp.%s" % (path.name, os.getpid()))
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
                with io.TextIOWrapper(compressed, encoding="utf-8") as text:
                    for row in rows:
                        text.write(canonical_json(row) + "\n")
        os.replace(str(temporary), str(path))
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


def split_public_snapshot_views(
    source: Path, causal_output: Path, label_output: Path
) -> Dict[str, Any]:
    """Physically isolate causal checkpoints from terminal labels."""
    causal_rows: List[Dict[str, Any]] = []
    label_rows: List[Dict[str, Any]] = []
    seen = set()
    with gzip.open(str(source), "rt", encoding="utf-8") as handle:
        for index, line in enumerate(handle, start=1):
            row = json.loads(line)
            missing = [
                field
                for field in (*CAUSAL_PUBLIC_WINDOW_FIELDS, *PUBLIC_WINDOW_LABEL_FIELDS)
                if field not in row
            ]
            if missing:
                raise ValueError(
                    "public snapshot line %s missing fields: %s"
                    % (index, ",".join(missing))
                )
            window_start = int(row["window_start"])
            if window_start in seen:
                raise ValueError("duplicate public snapshot window_start %s" % window_start)
            seen.add(window_start)
            causal_rows.append({field: row[field] for field in CAUSAL_PUBLIC_WINDOW_FIELDS})
            label_rows.append({field: row[field] for field in PUBLIC_WINDOW_LABEL_FIELDS})
    if not causal_rows:
        raise ValueError("public snapshot contains no windows")
    atomic_gzip_jsonl(causal_output, causal_rows)
    atomic_gzip_jsonl(label_output, label_rows)
    return {
        "causal_path": str(causal_output),
        "causal_sha256": sha256_file(causal_output),
        "label_path": str(label_output),
        "label_sha256": sha256_file(label_output),
        "window_count": len(causal_rows),
        "causal_fields": list(CAUSAL_PUBLIC_WINDOW_FIELDS),
        "label_fields": list(PUBLIC_WINDOW_LABEL_FIELDS),
    }


def refresh_public_snapshot(
    config: Mapping[str, Any], ledger: Ledger, state_dir: Path, dry_run: bool
) -> Dict[str, Any]:
    refresh = config.get("public_refresh", {})
    seed = resolve_repo_path(config["lanes"]["late_window_mechanisms"]["public_snapshot"])
    if not refresh.get("enabled", False):
        return {"status": "disabled", "path": str(seed), "sha256": sha256_file(seed)}
    latest_day = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(
        days=int(refresh["resolution_lag_days"])
    )
    start_day = dt.date.fromisoformat(str(refresh["start_date"]))
    if latest_day < start_day:
        raise ValueError("public refresh latest day precedes start date")
    output = state_dir / "public_snapshots/binance_spot_1m_current.jsonl.gz"
    causal_output = state_dir / "public_snapshots/binance_spot_1m_causal_current.jsonl.gz"
    label_output = state_dir / "public_snapshots/binance_spot_1m_labels_current.jsonl.gz"
    manifest_path = state_dir / "public_snapshots/binance_spot_1m_current.manifest.json"
    if (
        output.is_file()
        and manifest_path.is_file()
        and ledger.meta("public_snapshot.latest_day") == latest_day.isoformat()
    ):
        if not causal_output.is_file() or not label_output.is_file():
            views = split_public_snapshot_views(output, causal_output, label_output)
            manifest = json.loads(manifest_path.read_text())
            manifest["physical_views"] = views
            atomic_json(manifest_path, manifest)
        else:
            views = {
                "causal_path": str(causal_output),
                "causal_sha256": sha256_file(causal_output),
                "label_path": str(label_output),
                "label_sha256": sha256_file(label_output),
            }
        return {
            "status": "current",
            "path": str(output),
            "sha256": sha256_file(output),
            "manifest": str(manifest_path),
            "latest_day": latest_day.isoformat(),
            **views,
        }
    if dry_run:
        return {
            "status": "dry_run",
            "path": str(seed),
            "sha256": sha256_file(seed),
            "latest_day": latest_day.isoformat(),
        }
    data_dir = state_dir / "data/binance-public-1m"
    base_url = str(refresh["binance_archive_base_url"]).rstrip("/")
    source_rows: List[Dict[str, Any]] = []
    all_windows: List[Dict[str, Any]] = []
    day = start_day
    while day <= latest_day:
        day_text = day.isoformat()
        archive_name = "BTCUSDT-1m-%s.zip" % day_text
        archive = data_dir / archive_name
        checksum = data_dir / (archive_name + ".CHECKSUM")
        url = "%s/%s" % (base_url, archive_name)
        download_atomic(url, archive, int(refresh["download_timeout_seconds"]))
        download_atomic(url + ".CHECKSUM", checksum, 30)
        expected_hash, expected_name = checksum.read_text().strip().split()
        actual_hash = sha256_file(archive)
        if expected_name.lstrip("*") != archive.name or actual_hash != expected_hash:
            raise ValueError("public BTC archive checksum mismatch for %s" % day_text)
        windows = build_public_windows_from_1m_archive(archive)
        all_windows.extend(windows)
        source_rows.append(
            {"day": day_text, "archive_sha256": actual_hash, "windows": len(windows)}
        )
        day += dt.timedelta(days=1)
    holdout_days = int(refresh["fresh_holdout_days"])
    recent_days = int(refresh["recent_discovery_days"])
    holdout_start = latest_day - dt.timedelta(days=holdout_days - 1)
    recent_start = holdout_start - dt.timedelta(days=recent_days)
    temporary = output.with_name("%s.tmp.%s" % (output.name, os.getpid()))
    output.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(str(temporary), "wt", encoding="utf-8") as handle:
        for row in sorted(all_windows, key=lambda item: int(item["window_start"])):
            row_day = dt.date.fromisoformat(str(row["utc_day"]))
            enriched = dict(row)
            if row_day >= holdout_start:
                enriched["chronological_window"] = "fresh_holdout"
            elif row_day >= recent_start:
                enriched["chronological_window"] = "recent_discovery"
            else:
                enriched["chronological_window"] = "older"
            handle.write(canonical_json(enriched) + "\n")
    os.replace(str(temporary), str(output))
    views = split_public_snapshot_views(output, causal_output, label_output)
    manifest = {
        "schema_version": 1,
        "source_schema": PUBLIC_SNAPSHOT_VERSION,
        "generated_at": utc_now(),
        "start_day": start_day.isoformat(),
        "latest_fully_resolved_day": latest_day.isoformat(),
        "recent_discovery_start": recent_start.isoformat(),
        "fresh_holdout_start": holdout_start.isoformat(),
        "terminal_outcome_policy": "fresh_holdout rows may be selected only from causal checkpoints",
        "window_count": len(all_windows),
        "sources": source_rows,
        "sources_sha256": stable_hash(source_rows),
        "physical_views": views,
    }
    atomic_json(manifest_path, manifest)
    ledger.set_meta("public_snapshot.latest_day", latest_day.isoformat())
    return {
        "status": "refreshed",
        "path": str(output),
        "sha256": sha256_file(output),
        "manifest": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "latest_day": latest_day.isoformat(),
        "window_count": len(all_windows),
        "fresh_holdout_start": holdout_start.isoformat(),
        **views,
    }


def summarize_exact_report(report_payload: Mapping[str, Any]) -> Dict[str, Any]:
    variant = report_payload["variants"][0]
    by_direction: Dict[str, Dict[str, Any]] = {}
    regimes = variant.get("diagnostics", {}).get("by_regime", {})
    if isinstance(regimes, dict):
        for regime, stats in regimes.items():
            direction = next(
                (part.split("=", 1)[1] for part in str(regime).split("|") if part.startswith("dir=")),
                None,
            )
            if direction not in ("up", "down") or not isinstance(stats, dict):
                continue
            aggregate = by_direction.setdefault(
                direction, {"trades": 0, "wins": 0, "losses": 0, "total_pnl": 0.0}
            )
            aggregate["trades"] += int(stats.get("trades") or 0)
            aggregate["wins"] += int(stats.get("wins") or 0)
            aggregate["losses"] += int(stats.get("losses") or 0)
            aggregate["total_pnl"] += float(stats.get("total_pnl") or 0.0)
    summary = {
        key: variant.get(key)
        for key in (
            "trades",
            "wins",
            "losses",
            "win_rate",
            "total_pnl",
            "avg_pnl",
            "total_fees",
            "execution_attempts",
            "fills_success",
            "fills_failed",
            "fill_rate",
            "unresolved_fills",
            "breaker_tripped",
        )
    }
    summary["by_direction"] = by_direction
    return summary


def exact_report_data_support(report_payload: Mapping[str, Any]) -> Dict[str, Any]:
    manifest = report_payload.get("data_manifest", {})
    sources = manifest.get("sources", []) if isinstance(manifest, dict) else []
    pmxt = next(
        (
            source
            for source in sources
            if isinstance(source, dict) and source.get("name") == "pmxt_v2_archive"
        ),
        None,
    )
    if pmxt is None:
        return {
            "observable": False,
            "pmxt_complete": False,
            "pmxt_row_count": 0,
            "reason": "pmxt_manifest_missing",
        }
    row_count = int(pmxt.get("row_count") or 0)
    complete = bool(pmxt.get("complete"))
    observable = complete and row_count > 0
    return {
        "observable": observable,
        "pmxt_complete": complete,
        "pmxt_row_count": row_count,
        "reason": None if observable else "pmxt_target_events_unavailable",
    }


def exact_report_official_resolution_support(
    report_payload: Mapping[str, Any], required_source_kind: str
) -> Dict[str, Any]:
    manifest = report_payload.get("data_manifest", {})
    sources = manifest.get("sources", []) if isinstance(manifest, dict) else []
    settlement = next(
        (
            source
            for source in sources
            if isinstance(source, dict)
            and source.get("name") == "btc_settlement_price_tape"
        ),
        None,
    )
    if settlement is None:
        return {"ready": False, "reason": "settlement_manifest_missing"}
    actual = str(settlement.get("metadata", {}).get("source_kind") or "unknown")
    complete = bool(settlement.get("complete"))
    rows = int(settlement.get("row_count") or 0)
    checksum = settlement.get("checksum_sha256")
    ready = complete and rows > 0 and actual == required_source_kind and bool(checksum)
    return {
        "ready": ready,
        "reason": None if ready else "official_settlement_source_not_hash_pinned",
        "required_source_kind": required_source_kind,
        "actual_source_kind": actual,
        "complete": complete,
        "row_count": rows,
        "checksum_sha256": checksum,
    }


def forward_official_resolution_support(
    completed_windows: Sequence[Mapping[str, Any]], config: Mapping[str, Any]
) -> Dict[str, Any]:
    required = str(config["official_resolution"]["required_source_kind"])
    reports = []
    for window in completed_windows:
        report_path = Path(str(window.get("report", "")))
        if not report_path.is_file():
            reports.append(
                {"report": str(report_path), "ready": False, "reason": "report_missing"}
            )
            continue
        support = exact_report_official_resolution_support(
            json.loads(report_path.read_text()), required
        )
        reports.append(
            {"report": str(report_path), "report_sha256": sha256_file(report_path), **support}
        )
    return {
        "ready": bool(reports) and all(item["ready"] for item in reports),
        "required_source_kind": required,
        "reports": reports,
    }


def bounded_shadow_verdict(
    session: Mapping[str, Any], config: Mapping[str, Any]
) -> Dict[str, Any]:
    policy = config["bounded_vps_shadow"]
    checks = {
        "paper_only_venue": session.get("venue") == "paper_only",
        "zero_live_order_submissions": int(session.get("live_order_submissions") or 0)
        == 0,
        "bounded_duration": 0 < float(session.get("duration_seconds") or 0.0)
        <= float(policy["maximum_duration_seconds"]),
        "minimum_shadow_resolutions": int(session.get("shadow_resolutions") or 0)
        >= int(policy["minimum_shadow_resolutions"]),
        "official_resolution_parity": bool(
            session.get("official_resolution_parity_ready", False)
        ),
        "zero_unresolved_positions": int(session.get("unresolved_positions") or 0) == 0,
        "zero_breaker_trips": int(session.get("breaker_trips") or 0) == 0,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "paper_or_live_authorized": False,
        "purpose": "production wiring and official-resolution parity only",
    }


def replay_window_data_support(window: Mapping[str, Any]) -> Dict[str, Any]:
    existing = window.get("data_support")
    if isinstance(existing, dict) and "observable" in existing:
        return dict(existing)
    report_path = Path(str(window.get("report", "")))
    if report_path.is_file():
        return exact_report_data_support(json.loads(report_path.read_text()))
    # Old synthetic/unit records did not carry a manifest. Keep them in the
    # measured sample rather than silently manufacturing a data exclusion.
    return {
        "observable": True,
        "pmxt_complete": None,
        "pmxt_row_count": None,
        "reason": "legacy_support_metadata_unavailable",
    }


def exact_eligibility(
    completed_windows: Sequence[Mapping[str, Any]],
    variant_path: Path,
    gates: Mapping[str, Any],
) -> Dict[str, Any]:
    summaries: List[Mapping[str, Any]] = []
    for window in completed_windows:
        report_path = Path(str(window.get("report", "")))
        if report_path.is_file():
            summaries.append(summarize_exact_report(json.loads(report_path.read_text())))
        elif isinstance(window.get("summary"), dict):
            summaries.append(window["summary"])
    variants = json.loads(variant_path.read_text())
    variant = variants[0] if isinstance(variants, list) else variants
    bankroll = float(gates.get("reference_bankroll_usd", 100.0))
    stake_caps = [
        float(value)
        for value in (
            variant.get("max_per_market_usd"),
            float(variant.get("position_pct", 0.0)) * bankroll,
        )
        if value is not None and float(value) > 0.0
    ]
    maximum_stake = min(stake_caps) if stake_caps else 0.0
    trades = sum(int(summary.get("trades") or 0) for summary in summaries)
    attempts = sum(int(summary.get("execution_attempts") or 0) for summary in summaries)
    fills = sum(int(summary.get("fills_success") or 0) for summary in summaries)
    failed_fills = sum(int(summary.get("fills_failed") or 0) for summary in summaries)
    unresolved = sum(int(summary.get("unresolved_fills") or 0) for summary in summaries)
    pnl = sum(float(summary.get("total_pnl") or 0.0) for summary in summaries)
    fees = sum(float(summary.get("total_fees") or 0.0) for summary in summaries)
    public_signals = sum(
        int(window.get("public_signals", summary.get("execution_attempts") or 0) or 0)
        for window, summary in zip(completed_windows, summaries)
    )
    active = [summary for summary in summaries if int(summary.get("trades") or 0) > 0]
    positive = [summary for summary in active if float(summary.get("total_pnl") or 0.0) > 0.0]
    by_direction: Dict[str, Dict[str, Any]] = {}
    for summary in summaries:
        for direction, stats in (summary.get("by_direction") or {}).items():
            aggregate = by_direction.setdefault(
                direction, {"trades": 0, "wins": 0, "losses": 0, "total_pnl": 0.0}
            )
            for key in ("trades", "wins", "losses"):
                aggregate[key] += int(stats.get(key) or 0)
            aggregate["total_pnl"] += float(stats.get("total_pnl") or 0.0)
    direction_minimum = int(gates["minimum_direction_fills"])
    required_directions = ("up", "down") if bool(gates.get("require_both_directions", False)) else tuple(by_direction)
    direction_support_ok = all(
        int(by_direction.get(direction, {}).get("trades", 0)) >= direction_minimum
        for direction in required_directions
    )
    direction_pnl_ok = all(
        float(by_direction.get(direction, {}).get("total_pnl", 0.0)) > 0.0
        for direction in required_directions
    )
    require_stake_loss_robustness = bool(
        gates.get("require_one_maximum_stake_loss_robustness", True)
    )
    raw_stake_loss_robustness = pnl - maximum_stake > 0.0
    checks = {
        "minimum_fills": fills >= int(gates["minimum_fills"]),
        "minimum_fill_rate": (fills / float(attempts) if attempts else 0.0)
        >= float(gates["minimum_fill_rate"]),
        "minimum_signal_to_attempt_rate": (
            attempts / float(public_signals) if public_signals else 0.0
        )
        >= float(gates.get("minimum_signal_to_attempt_rate", 0.0)),
        "minimum_active_window_fraction": (
            len(active) / float(len(summaries)) if summaries else 0.0
        )
        >= float(gates.get("minimum_active_window_fraction", 0.0)),
        "positive_total_net_pnl": pnl > 0.0,
        "positive_mean_net_pnl": (pnl / trades if trades else 0.0) > 0.0,
        "minimum_positive_active_window_fraction": (
            len(positive) / float(len(active)) if active else 0.0
        )
        >= float(gates["minimum_positive_active_window_fraction"]),
        "direction_robustness": direction_support_ok and direction_pnl_ok,
        "maximum_unresolved_fills": unresolved <= int(gates["maximum_unresolved_fills"]),
        "breaker_not_tripped": not any(bool(summary.get("breaker_tripped")) for summary in summaries),
        "required_one_maximum_stake_loss_robustness": (
            raw_stake_loss_robustness or not require_stake_loss_robustness
        ),
    }
    enforce_signal_to_attempt_rate = bool(
        gates.get("enforce_minimum_signal_to_attempt_rate", True)
    )
    required_checks = dict(checks)
    if not enforce_signal_to_attempt_rate:
        required_checks.pop("minimum_signal_to_attempt_rate")
    eligible = all(required_checks.values())
    if eligible:
        classification = "research_eligible"
    elif not checks["minimum_fills"]:
        classification = "insufficient_support"
    else:
        classification = "rejected"
    return {
        "schema_version": 1,
        "evaluated_at": utc_now(),
        "classification": classification,
        "eligible": eligible,
        "gates": checks,
        "policy": {
            "require_one_maximum_stake_loss_robustness": require_stake_loss_robustness,
            "enforce_minimum_signal_to_attempt_rate": enforce_signal_to_attempt_rate,
            "required_gates": sorted(required_checks),
        },
        "observations": {
            "raw_one_maximum_stake_loss_robustness": raw_stake_loss_robustness,
        },
        "aggregate": {
            "windows": len(summaries),
            "active_windows": len(active),
            "active_window_fraction": len(active) / float(len(summaries)) if summaries else 0.0,
            "public_signals": public_signals,
            "trades": trades,
            "execution_attempts": attempts,
            "signal_to_attempt_rate": attempts / float(public_signals) if public_signals else 0.0,
            "fills_success": fills,
            "fills_failed": failed_fills,
            "fill_rate": fills / float(attempts) if attempts else 0.0,
            "total_pnl": pnl,
            "average_pnl": pnl / trades if trades else 0.0,
            "total_fees": fees,
            "unresolved_fills": unresolved,
            "positive_active_window_fraction": len(positive) / float(len(active)) if active else 0.0,
            "by_direction": by_direction,
            "maximum_stake_usd": maximum_stake,
            "pnl_after_one_maximum_stake_loss": pnl - maximum_stake,
        },
    }


def eligibility_gates_for_proposal(
    gates: Mapping[str, Any], proposal: Mapping[str, Any]
) -> Dict[str, Any]:
    adjusted = dict(gates)
    if normalized_late_rule(proposal["rule"])["direction"] != "both":
        adjusted["require_both_directions"] = False
    return adjusted


def cached_family_rule_id(proposal: Mapping[str, Any]) -> Optional[str]:
    rule = normalized_late_rule(proposal["rule"])
    operator = str(rule["operator"])
    path = int(rule["path_minutes"])
    move = int(rule["minimum_two_minute_move_usd"])
    if operator == "path_only" and path in (3, 4):
        return "path_%sm" % path
    if operator == "move_only" and move in (100, 200):
        return "move_2m_%s" % move
    if operator == "path_or_move" and path == 4 and move == 200:
        return "path_4m_or_move_2m_200_aligned"
    if operator == "path_and_move":
        if path in (3, 4) and move >= 100:
            return "path_%sm_and_move_2m_100" % path
        if move in (100, 200):
            return "move_2m_%s" % move
    return None


def cached_family_economic_verdict(
    config: Mapping[str, Any], proposal: Mapping[str, Any]
) -> Dict[str, Any]:
    screen_path = resolve_repo_path(config["economic_screen"]["cached_family_screen"])
    screen = json.loads(screen_path.read_text())
    rule_id = cached_family_rule_id(proposal)
    result = screen.get("results", {}).get(rule_id) if rule_id else None
    if not isinstance(result, dict):
        return {
            "passed": False,
            "classification": "unavailable",
            "reason": "no compatible cached fee-aware family screen",
            "rule_id": rule_id,
            "source": {"path": str(screen_path), "sha256": sha256_file(screen_path)},
        }
    direction = normalized_late_rule(proposal["rule"])["direction"]
    metrics = result.get("overall", {}) if direction == "both" else result.get(
        "by_direction", {}
    ).get(direction, {})
    conditions = int(metrics.get("conditions") or 0)
    wins = int(metrics.get("wins") or 0)
    losses = int(metrics.get("losses") or 0)
    positive = float(metrics.get("positive_unit_payoff") or 0.0)
    negative = float(metrics.get("negative_unit_payoff") or 0.0)
    mean_win = positive / wins if wins else 0.0
    mean_loss = negative / losses if losses else 0.0
    recovery = math.ceil(mean_loss / mean_win) if mean_loss > 0.0 and mean_win > 0.0 else 0
    profit_factor = metrics.get("unit_profit_factor")
    profit_factor_ok = (
        float(profit_factor) >= float(config["economic_screen"]["minimum_unit_profit_factor"])
        if profit_factor is not None
        else positive > 0.0 and negative == 0.0
    )
    break_even = metrics.get("mean_fee_aware_cost_per_share")
    lower = wilson_lower(wins, conditions)
    checks = {
        "minimum_conditions": conditions
        >= int(config["economic_screen"]["minimum_conditions"]),
        "positive_mean_fee_aware_payoff": float(
            metrics.get("mean_one_share_payoff") or 0.0
        )
        > float(config["economic_screen"]["minimum_mean_one_share_payoff"]),
        "minimum_unit_profit_factor": profit_factor_ok,
        "maximum_loss_recovery_wins": recovery
        <= int(config["economic_screen"]["maximum_loss_recovery_wins"]),
    }
    return {
        "passed": all(checks.values()),
        "classification": "passed" if all(checks.values()) else "rejected",
        "rule_id": rule_id,
        "direction": direction,
        "metrics": {
            **metrics,
            "mean_winning_unit_payoff": mean_win,
            "mean_losing_unit_payoff": mean_loss,
            "loss_recovery_wins": recovery,
            "wilson_95_lower": lower,
            "break_even_accuracy": break_even,
            "wilson_margin_over_break_even": lower - float(break_even)
            if lower is not None and break_even is not None
            else None,
        },
        "checks": checks,
        "source": {"path": str(screen_path), "sha256": sha256_file(screen_path)},
        "limitations": [
            "Cached top-of-book rows are a broad family proxy; candidate price, buffer, and sigma filters are applied only in exact replay.",
            "This cheap screen can authorize bounded exact replay but cannot establish executable profitability.",
        ],
    }


def exact_replay_economic_verdict(
    config: Mapping[str, Any],
    ledger: Ledger,
    fingerprint: str,
    verdict: Mapping[str, Any],
    policy_version: Any,
) -> Dict[str, Any]:
    aggregate = verdict["aggregate"]
    wins = sum(
        int(stats.get("wins") or 0)
        for stats in aggregate.get("by_direction", {}).values()
    )
    fills = int(aggregate.get("fills_success") or 0)
    losses = fills - wins
    pnl = float(aggregate.get("total_pnl") or 0.0)
    maximum_stake = float(aggregate.get("maximum_stake_usd") or 0.0)
    mean_win = pnl / wins if wins else 0.0
    recovery = math.ceil(maximum_stake / mean_win) if mean_win > 0.0 else None
    break_even = (
        maximum_stake / (maximum_stake + mean_win)
        if maximum_stake > 0.0 and mean_win > 0.0
        else None
    )
    hypothesis = ledger.hypothesis(fingerprint)
    public_path = Path(str(hypothesis.get("evidence_path"))) if hypothesis else Path()
    public = json.loads(public_path.read_text()) if public_path.is_file() else {}
    public_overall = public.get("overall", {})
    lower = public_overall.get("wilson_95_lower")
    checks = {
        "positive_net_pnl": pnl > 0.0,
        "minimum_mean_net_win": mean_win
        >= float(config["economic_screen"]["exact_minimum_mean_net_win_usd"]),
        "maximum_loss_recovery_wins": recovery is not None
        and recovery <= int(config["economic_screen"]["maximum_loss_recovery_wins"]),
        "public_wilson_above_break_even": bool(
            lower is not None and break_even is not None and lower > break_even
        ),
    }
    required = dict(checks)
    if not config["economic_screen"].get(
        "require_exact_public_wilson_above_break_even", True
    ):
        required.pop("public_wilson_above_break_even")
    return {
        "passed": all(required.values()),
        "classification": "passed" if all(required.values()) else "rejected",
        "metrics": {
            "fills": fills,
            "wins": wins,
            "losses": losses,
            "total_net_pnl_usd": pnl,
            "mean_net_win_usd": mean_win,
            "maximum_stake_usd": maximum_stake,
            "loss_recovery_wins": recovery,
            "break_even_accuracy": break_even,
            "public_accuracy": public_overall.get("accuracy"),
            "public_wilson_95_lower": lower,
            "wilson_margin_over_break_even": lower - break_even
            if lower is not None and break_even is not None
            else None,
        },
        "checks": checks,
        "required_checks": sorted(required),
        "source": {
            "eligibility_policy_version": policy_version,
            "public_screen": str(public_path) if public_path.is_file() else None,
            "public_screen_sha256": sha256_file(public_path)
            if public_path.is_file()
            else None,
        },
    }


def exact_fresh_economic_verdict(
    config: Mapping[str, Any], ledger: Ledger, fingerprint: str, payload: Mapping[str, Any]
) -> Dict[str, Any]:
    return exact_replay_economic_verdict(
        config,
        ledger,
        fingerprint,
        payload["fresh_holdout_verdict"],
        payload.get("fresh_holdout_eligibility_policy_version"),
    )


def enqueue_economic_screen(
    ledger: Ledger,
    job: Mapping[str, Any],
    payload: Mapping[str, Any],
    source_kind: str,
) -> bool:
    economic_payload = {
        "source_kind": source_kind,
        "proposal": payload["proposal"],
        "variant": payload["variant"],
    }
    for key in (
        "public_screen",
        "candidate_replay_windows",
        "fresh_candidate_windows",
        "fresh_previously_measured_exclusion",
        "fresh_holdout_verdict",
        "fresh_holdout_eligibility_policy_version",
    ):
        if key in payload:
            economic_payload[key] = payload[key]
    return ledger.enqueue(
        str(job["lane"]),
        str(job["hypothesis_fingerprint"]),
        "economic_opportunity_screen",
        economic_payload,
        "queued for cached fee-aware economic fail-fast",
        status="queued",
    )


def economic_rejection_status(payload: Mapping[str, Any]) -> str:
    return (
        "rejected_exact_economics"
        if str(payload.get("source_kind")) == "fresh_exact_replay"
        else "rejected_economic_screen"
    )


def run_queued_economic_screen(
    config: Mapping[str, Any], ledger: Ledger, state_dir: Path, dry_run: bool
) -> Dict[str, Any]:
    if not config.get("economic_screen", {}).get("enabled", False):
        return {"status": "disabled"}
    job = ledger.next_job("economic_opportunity_screen")
    if job is None:
        return {"status": "empty"}
    payload = json.loads(job["payload_json"])
    if dry_run:
        return {"status": "dry_run", "job_id": job["job_id"]}
    source_kind = str(payload.get("source_kind", "cached_family_top_of_book"))
    if source_kind == "fresh_exact_replay":
        verdict = exact_fresh_economic_verdict(
            config, ledger, str(job["hypothesis_fingerprint"]), payload
        )
    else:
        verdict = cached_family_economic_verdict(config, payload["proposal"])
        if verdict["passed"] and len(
            ledger.jobs("exact_l2_replay", "queued")
        ) >= int(config["maximum_exact_l2_shortlist"]):
            # A passing candidate waits for the shortlist before anything is
            # logged or written (the cached verdict is cheap to recompute), so
            # a saturated cycle leaves no evidence behind while a rejected
            # head still drains the queue.
            return {"status": "shortlist_saturated", "job_id": job["job_id"]}
    payload["economic_verdict"] = verdict
    verdict_metrics = verdict.get("metrics") or {}
    factory_generator.append_trial_entry(
        config,
        str(job["hypothesis_fingerprint"]),
        "economic_opportunity_screen",
        str(verdict.get("classification")),
        n=verdict_metrics.get("fills", verdict_metrics.get("conditions")),
        wins=verdict_metrics.get("wins"),
    )
    evidence_path = state_dir / (
        "evidence/economic/%s.json" % job["hypothesis_fingerprint"]
    )
    atomic_json(
        evidence_path,
        {
            "schema_version": 1,
            "generated_at": utc_now(),
            "status": verdict["classification"],
            "research_only": True,
            "live_ready": False,
            "hypothesis_fingerprint": job["hypothesis_fingerprint"],
            "proposal": payload["proposal"],
            "variant": payload["variant"],
            "verdict": verdict,
        },
    )
    payload["economic_evidence"] = str(evidence_path)
    payload["economic_evidence_sha256"] = sha256_file(evidence_path)
    if verdict["classification"] == "unavailable":
        ledger.update_job(
            job["job_id"], "blocked", payload, str(verdict["reason"])
        )
        ledger.update_hypothesis_status(
            str(job["hypothesis_fingerprint"]), "economic_screen_unavailable"
        )
        return {"status": "blocked", "job_id": job["job_id"], "verdict": verdict}
    if not verdict["passed"]:
        rejection_status = economic_rejection_status(payload)
        ledger.update_job(job["job_id"], "completed", payload, "economic screen rejected")
        ledger.update_hypothesis_status(
            str(job["hypothesis_fingerprint"]), rejection_status
        )
        return {
            "status": rejection_status,
            "job_id": job["job_id"],
            "artifact": str(evidence_path),
            "verdict": verdict,
        }
    if source_kind != "fresh_exact_replay":
        enqueued = ledger.enqueue(
            str(job["lane"]),
            str(job["hypothesis_fingerprint"]),
            "exact_l2_replay",
            {
                "proposal": payload["proposal"],
                "public_screen": payload.get("public_screen"),
                "economic_screen": str(evidence_path),
                "variant": payload["variant"],
                "candidate_replay_windows": payload["candidate_replay_windows"],
                "fresh_candidate_windows": payload["fresh_candidate_windows"],
                "fresh_previously_measured_exclusion": payload[
                    "fresh_previously_measured_exclusion"
                ],
                "completed_windows": [],
                "maximum_exact_l2_shortlist": int(config["maximum_exact_l2_shortlist"]),
            },
            "cached fee-aware screen passed; bounded exact L2 queued",
            status="queued",
        )
        payload["exact_l2_enqueued"] = enqueued
    ledger.update_job(job["job_id"], "completed", payload, "economic screen passed")
    if source_kind != "fresh_exact_replay":
        ledger.update_hypothesis_status(
            str(job["hypothesis_fingerprint"]), "eligible_for_exact_l2"
        )
    return {
        "status": "economic_screen_passed",
        "job_id": job["job_id"],
        "artifact": str(evidence_path),
        "verdict": verdict,
    }


def fixed_forward_design(
    verdict: Mapping[str, Any], config: Mapping[str, Any]
) -> Dict[str, Any]:
    policy = config["fixed_forward_confirmation"]
    aggregate = verdict["aggregate"]
    wins = sum(
        int(stats.get("wins") or 0)
        for stats in aggregate.get("by_direction", {}).values()
    )
    total_pnl = float(aggregate.get("total_pnl") or 0.0)
    mean_net_win = total_pnl / wins if wins else 0.0
    maximum_stake = float(aggregate.get("maximum_stake_usd") or 0.0)
    break_even = (
        maximum_stake / (maximum_stake + mean_net_win)
        if maximum_stake > 0.0 and mean_net_win > 0.0
        else None
    )
    alpha = float(policy["confidence_alpha"])
    calculated_target = (
        math.ceil(math.log(alpha) / math.log(break_even))
        if break_even is not None and 0.0 < break_even < 1.0
        else int(policy["maximum_target_fills"]) + 1
    )
    checkpoint = int(policy["minimum_checkpoint_fills"])
    maximum_target = int(policy["maximum_target_fills"])
    return {
        "design_version": "fixed_forward_chronological_v1",
        "minimum_checkpoint_fills": checkpoint,
        "payoff_derived_target_fills": max(checkpoint, calculated_target),
        "maximum_target_fills": maximum_target,
        "target_is_feasible": calculated_target <= maximum_target,
        "confidence_alpha": alpha,
        "prior_maximum_stake_usd": maximum_stake,
        "prior_mean_net_win_usd": mean_net_win,
        "prior_implied_break_even_accuracy": break_even,
        "selection_policy": (
            "all causal signal-hour windows strictly after sealed_at, in chronological "
            "order, with no terminal-outcome scoring or signal-density ranking"
        ),
        "paper_or_live_authorized": False,
    }


def enqueue_fixed_forward_confirmation(
    config: Mapping[str, Any],
    ledger: Ledger,
    job: Mapping[str, Any],
    payload: Mapping[str, Any],
    verdict: Mapping[str, Any],
    evidence_path: Path,
) -> bool:
    if not config.get("fixed_forward_confirmation", {}).get("enabled", False):
        return False
    design = fixed_forward_design(verdict, config)
    if not design["target_is_feasible"]:
        return ledger.enqueue(
            str(job["lane"]),
            str(job["hypothesis_fingerprint"]),
            "fixed_forward_confirmation",
            {
                "proposal": payload["proposal"],
                "variant": payload["variant"],
                "sealed_at": utc_now(),
                "design": design,
                "fresh_holdout_evidence": str(evidence_path),
                "completed_windows": [],
                "support_only_windows": [],
                "candidate_replay_windows": [],
            },
            "payoff-derived forward target exceeds the configured bounded maximum",
            status="blocked",
        )
    return ledger.enqueue(
        str(job["lane"]),
        str(job["hypothesis_fingerprint"]),
        "fixed_forward_confirmation",
        {
            "proposal": payload["proposal"],
            "variant": payload["variant"],
            "sealed_at": utc_now(),
            "design": design,
            "fresh_holdout_evidence": str(evidence_path),
            "fresh_holdout_evidence_sha256": sha256_file(evidence_path),
            "completed_windows": [],
            "support_only_windows": [],
            "candidate_replay_windows": [],
            "deferred_window_starts": [],
        },
        "sealed future-only confirmation awaiting economic clearance and new resolved windows",
        status="queued",
    )


def forward_economic_clearance(
    config: Mapping[str, Any], ledger: Ledger, fingerprint: str
) -> Optional[bool]:
    if not config["fixed_forward_confirmation"].get(
        "requires_economic_screen", True
    ):
        return True
    for job in ledger.jobs("economic_opportunity_screen"):
        if str(job["hypothesis_fingerprint"]) != fingerprint:
            continue
        payload = json.loads(job["payload_json"])
        verdict = payload.get("economic_verdict", {})
        if job["status"] == "completed" and isinstance(verdict, dict):
            return bool(verdict.get("passed", False))
    return None


def write_forward_evidence(
    state_dir: Path,
    job: Mapping[str, Any],
    payload: Mapping[str, Any],
    status: str,
) -> Path:
    path = state_dir / (
        "evidence/fixed-forward/%s.json" % job["hypothesis_fingerprint"]
    )
    atomic_json(
        path,
        {
            "schema_version": 1,
            "generated_at": utc_now(),
            "status": status,
            "research_only": True,
            "live_ready": False,
            "hypothesis_fingerprint": job["hypothesis_fingerprint"],
            **dict(payload),
        },
    )
    return path


def run_queued_fixed_forward_job(
    config: Mapping[str, Any],
    ledger: Ledger,
    state_dir: Path,
    snapshot: Path,
    dry_run: bool,
) -> Dict[str, Any]:
    policy = config.get("fixed_forward_confirmation", {})
    if not policy.get("enabled", False):
        return {"status": "disabled"}
    job = ledger.next_job("fixed_forward_confirmation")
    if job is None:
        return {"status": "empty"}
    payload = json.loads(job["payload_json"])
    fingerprint = str(job["hypothesis_fingerprint"])
    if sha256_file(Path(payload["variant"]["path"])) != payload["variant"]["sha256"]:
        ledger.update_job(job["job_id"], "blocked", payload, "frozen variant hash mismatch")
        return {"status": "blocked", "job_id": job["job_id"], "reason": "variant_hash_mismatch"}

    completed = payload.setdefault("completed_windows", [])
    support_only = payload.setdefault("support_only_windows", [])
    existing_candidates = payload.setdefault("candidate_replay_windows", [])
    known_starts = {str(item["start"]) for item in existing_candidates}
    excluded = ledger.measured_fresh_window_starts(exclude_job_id=str(job["job_id"]))
    excluded.update(str(item["start"]) for item in [*completed, *support_only])
    discovered = chronological_forward_windows(
        load_public_windows(snapshot), payload["proposal"], payload["sealed_at"], sorted(excluded)
    )
    appended = [item for item in discovered if str(item["start"]) not in known_starts]
    if appended:
        existing_candidates.extend(appended)
        existing_candidates.sort(key=lambda item: str(item["start"]))
        payload["candidate_replay_windows_sha256"] = stable_hash(existing_candidates)

    clearance = forward_economic_clearance(config, ledger, fingerprint)
    payload["economic_screen_clearance"] = clearance
    if clearance is False:
        ledger.update_job(
            job["job_id"], "blocked", payload, "economic opportunity screen failed"
        )
        hypothesis = ledger.hypothesis(fingerprint)
        rejection_status = (
            str(hypothesis["status"])
            if hypothesis
            and hypothesis.get("status")
            in ("rejected_economic_screen", "rejected_exact_economics")
            else "rejected_economic_screen"
        )
        ledger.update_hypothesis_status(fingerprint, rejection_status)
        evidence = write_forward_evidence(
            state_dir, job, payload, "blocked_failed_economic_screen"
        )
        return {"status": "blocked", "job_id": job["job_id"], "artifact": str(evidence)}
    if clearance is None:
        ledger.update_job(
            job["job_id"], "queued", payload, "awaiting economic opportunity screen"
        )
        return {
            "status": "awaiting_economic_screen",
            "job_id": job["job_id"],
            "new_candidate_windows": len(appended),
        }

    measured = [*completed, *support_only]
    remaining = [
        item
        for item in existing_candidates
        if not replay_window_start_is_covered(item, measured)
    ]
    if not remaining:
        ledger.update_job(
            job["job_id"], "queued", payload, "awaiting next fully resolved post-seal signal window"
        )
        return {"status": "awaiting_new_data", "job_id": job["job_id"]}
    window = remaining[0]
    if dry_run:
        return {"status": "dry_run", "job_id": job["job_id"], "window": window}
    result = execute_replay_window(
        config, job, payload, state_dir, window, "fixed_forward"
    )
    if result["status"] == "deferred":
        ledger.update_job(job["job_id"], "queued", payload, result["reason"])
        return result
    if result["status"] != "completed_window":
        retries = int(payload.get("retry_count", 0)) + 1
        payload["retry_count"] = retries
        limit = int(policy["maximum_retries_per_job"])
        ledger.update_job(
            job["job_id"],
            "queued" if retries <= limit else "failed",
            payload,
            "fixed forward retry %s/%s after no compact report" % (retries, limit),
        )
        return result
    measured_window = {
        key: value for key, value in result.items() if key not in ("status", "job_id")
    }
    report_path = Path(str(measured_window.get("report", "")))
    official_window = (
        exact_report_official_resolution_support(
            json.loads(report_path.read_text()),
            str(config["official_resolution"]["required_source_kind"]),
        )
        if report_path.is_file()
        else {"ready": False, "reason": "report_missing"}
    )
    measured_window["official_resolution_support"] = official_window
    if not official_window["ready"]:
        support_only.append(measured_window)
        payload["official_resolution_support"] = forward_official_resolution_support(
            completed, config
        )
        ledger.update_job(
            job["job_id"],
            "blocked",
            payload,
            "forward report lacks hash-pinned official Chainlink settlement parity",
        )
        evidence = write_forward_evidence(
            state_dir, job, payload, "blocked_official_resolution_parity"
        )
        return {
            "status": "blocked",
            "job_id": job["job_id"],
            "reason": "official_resolution_parity_failed",
            "artifact": str(evidence),
        }
    if measured_window.get("data_support", {}).get("observable", True):
        completed.append(measured_window)
    else:
        support_only.append(measured_window)
        result["status"] = "support_only_window"

    target = int(payload["design"]["payoff_derived_target_fills"])
    gates = dict(policy["gates"])
    gates["minimum_fills"] = target
    verdict = exact_eligibility(
        completed,
        Path(payload["variant"]["path"]),
        eligibility_gates_for_proposal(gates, payload["proposal"]),
    )
    wins = sum(
        int(stats.get("wins") or 0)
        for stats in verdict["aggregate"].get("by_direction", {}).values()
    )
    fills = int(verdict["aggregate"]["fills_success"])
    lower = wilson_lower(wins, fills)
    break_even = payload["design"]["prior_implied_break_even_accuracy"]
    verdict["forward"] = {
        "wins": wins,
        "losses": fills - wins,
        "wilson_95_lower": lower,
        "frozen_break_even_accuracy": break_even,
        "wilson_above_break_even": bool(
            lower is not None and break_even is not None and lower > break_even
        ),
    }
    verdict["official_resolution"] = forward_official_resolution_support(
        completed, config
    )
    payload["forward_verdict"] = verdict
    checkpoint = int(payload["design"]["minimum_checkpoint_fills"])
    operational_keys = (
        "minimum_fill_rate",
        "minimum_signal_to_attempt_rate",
        "minimum_active_window_fraction",
        "maximum_unresolved_fills",
        "breaker_not_tripped",
    )
    checkpoint_failed = fills >= checkpoint and not all(
        bool(verdict["gates"][key]) for key in operational_keys
    )
    final = fills >= target
    final_passed = bool(
        final
        and verdict["eligible"]
        and verdict["forward"]["wilson_above_break_even"]
        and verdict["observations"]["raw_one_maximum_stake_loss_robustness"]
        and verdict["official_resolution"]["ready"]
    )
    shadow_enqueued = False
    if checkpoint_failed or final:
        status = (
            "forward_confirmed_research_only"
            if final_passed
            else "rejected_fixed_forward"
        )
        factory_generator.append_trial_entry(
            config,
            fingerprint,
            "fixed_forward_confirmation",
            status,
            n=fills,
            wins=wins,
        )
        ledger.update_job(job["job_id"], "completed", payload, status)
        ledger.update_hypothesis_status(fingerprint, status)
        if final_passed and config.get("bounded_vps_shadow", {}).get("enabled", False):
            shadow_enqueued = ledger.enqueue(
                str(job["lane"]),
                fingerprint,
                "bounded_vps_shadow",
                {
                    "proposal": payload["proposal"],
                    "variant": payload["variant"],
                    "forward_job_id": job["job_id"],
                    "forward_verdict": verdict,
                    "policy": config["bounded_vps_shadow"],
                    "session_artifact": None,
                },
                "requires an explicit bounded paper-only VPS session artifact",
                status="blocked",
            )
    else:
        status = "fixed_forward_collecting"
        ledger.update_job(
            job["job_id"], "queued", payload, "awaiting additional post-seal signal windows"
        )
    evidence = write_forward_evidence(state_dir, job, payload, status)
    return {
        **result,
        "forward_status": status,
        "artifact": str(evidence),
        "artifact_sha256": sha256_file(evidence),
        "fills": fills,
        "target_fills": target,
        "bounded_shadow_enqueued": shadow_enqueued,
    }


def execute_replay_window(
    config: Mapping[str, Any],
    job: Mapping[str, Any],
    payload: Mapping[str, Any],
    state_dir: Path,
    window: Mapping[str, Any],
    stage: str,
) -> Dict[str, Any]:
    exact = config["exact_replay"]
    day = str(window["start"])[:10]
    window_start = dt.datetime.fromisoformat(str(window["start"]).replace("Z", "+00:00"))
    window_end = dt.datetime.fromisoformat(str(window["end"]).replace("Z", "+00:00"))
    span_seconds = (window_end - window_start).total_seconds()
    if span_seconds < 0 or span_seconds % 3600 != 0:
        raise ValueError("replay window must use inclusive whole-hour boundaries")
    fold_hours = int(span_seconds // 3600) + 1
    try:
        tape = build_verified_btc_tape_for_window(
            str(window["start"]),
            str(window["end"]),
            state_dir / "data/binance",
            str(exact["binance_archive_base_url"]),
        )
    except Exception as error:
        return {"status": "deferred", "reason": "btc_tape_unavailable", "error": str(error)}
    settlement_tape: Optional[Path] = None
    if stage == "fixed_forward":
        pattern = str(config["official_resolution"]["settlement_tape_pattern"])
        settlement_tape = state_dir / pattern.format(date=day)
        if not settlement_tape.is_file():
            return {
                "status": "deferred",
                "reason": "official_settlement_tape_unavailable",
                "required_path": str(settlement_tape),
            }
    fingerprint = str(job["hypothesis_fingerprint"])
    run_key = "%s_%s" % (day.replace("-", ""), str(window["start"])[11:13])
    out_dir = state_dir / ("runs/%s/%s/%s" % (stage, fingerprint, run_key))
    # PMXT event sidecars depend on the replay window and selected condition IDs,
    # not on strategy parameters. Sharing the window root avoids downloading and
    # distilling identical hourly parquet files for every candidate.
    cache_root = state_dir / ("cache/pmxt/windows/%s" % run_key)
    variant = Path(payload["variant"]["path"])
    command = [
        str(resolve_repo_path(config["engine_path"])),
        "strategy-builder",
        "rolling-history",
        "--start",
        str(window["start"]),
        "--end",
        str(window["end"]),
        "--out-dir",
        str(out_dir),
        "--cache-root",
        str(cache_root),
        "--btc-csv",
        tape["tape"],
        "--bankroll",
        str(exact["bankroll_usd"]),
        "--latency-ms",
        str(exact["latency_ms"]),
        "--threads",
        str(exact["threads"]),
        "--window-minutes",
        "5",
        "--fold-hours",
        str(fold_hours),
        "--max-folds",
        "1",
        "--profile",
        "a_plus5m",
        "--variant-json",
        str(variant),
        "--zone-mode",
        "all",
        "--require-full-folds",
        "--preflight-pmxt-hours",
        "--stop-at-first-missing-hour",
        "--atomic-parquet",
        "--execute",
    ]
    if settlement_tape is not None:
        command.extend(["--settlement-btc-csv", str(settlement_tape)])
    command_result = run_command(command, int(exact["timeout_seconds"]), False)
    reports = sorted((out_dir / "reports").glob("*_sweep.json"))
    window_result: Dict[str, Any] = {
        "start": window["start"],
        "end": window["end"],
        "public_signals": window["public_signals"],
        "btc_tape": tape,
        "settlement_tape": {
            "path": str(settlement_tape),
            "sha256": sha256_file(settlement_tape),
        }
        if settlement_tape is not None
        else None,
        "command_status": command_result["status"],
        "returncode": command_result.get("returncode"),
    }
    if reports:
        report = reports[0]
        report_payload = json.loads(report.read_text())
        window_result["report"] = str(report)
        window_result["report_sha256"] = sha256_file(report)
        window_result["data_support"] = exact_report_data_support(report_payload)
        window_result["summary"] = summarize_exact_report(report_payload)
        return {"status": "completed_window", "job_id": job["job_id"], **window_result}
    stderr_tail = str(command_result.get("stderr_tail") or "")
    pmxt_network_failure = (
        any(
            marker in stderr_tail
            for marker in (
                "download PMXT v2 hour",
                "preflight PMXT hour",
                "r2v2.pmxt.dev",
            )
        )
        and any(
            marker in stderr_tail.lower()
            for marker in (
                "connection reset",
                "connection refused",
                "dns",
                "error sending request",
                "timed out",
                "timeout",
                "after 4 attempts",
            )
        )
    )
    if pmxt_network_failure:
        return {
            "status": "deferred",
            "job_id": job["job_id"],
            "reason": "pmxt_archive_unavailable",
            "command": command_result,
        }
    return {
        "status": "failed",
        "job_id": job["job_id"],
        "command": command_result,
    }


def finalize_historical_exact_job(
    config: Mapping[str, Any], ledger: Ledger, job: Mapping[str, Any], payload: Dict[str, Any]
) -> Dict[str, Any]:
    verdict = exact_eligibility(
        payload.get("completed_windows", []),
        Path(payload["variant"]["path"]),
        eligibility_gates_for_proposal(
            config["exact_replay"]["historical_gates"], payload["proposal"]
        ),
    )
    payload["historical_verdict"] = verdict
    payload["historical_eligibility_policy_version"] = EXACT_ELIGIBILITY_POLICY_VERSION
    payload["historical_maximum_windows"] = int(
        config["exact_replay"]["maximum_windows_per_hypothesis"]
    )
    fingerprint = str(job["hypothesis_fingerprint"])
    factory_generator.append_trial_entry(
        config,
        fingerprint,
        "exact_l2_replay",
        str(verdict["classification"]),
        n=verdict["aggregate"].get("fills_success"),
        wins=factory_generator.direction_wins(verdict["aggregate"]),
    )
    if verdict["eligible"]:
        economic_verdict = exact_replay_economic_verdict(
            config,
            ledger,
            fingerprint,
            verdict,
            EXACT_ELIGIBILITY_POLICY_VERSION,
        )
        payload["historical_economic_verdict"] = economic_verdict
        economic_metrics = economic_verdict.get("metrics") or {}
        factory_generator.append_trial_entry(
            config,
            fingerprint,
            "economic_opportunity_screen",
            str(economic_verdict.get("classification")),
            n=economic_metrics.get("fills"),
            wins=economic_metrics.get("wins"),
        )
        if not economic_verdict["passed"]:
            ledger.update_hypothesis_status(fingerprint, "rejected_exact_economics")
        elif payload.get("fresh_candidate_windows"):
            fresh_windows = payload["fresh_candidate_windows"]
            maximum_fresh_windows = int(
                config["fresh_holdout"]["maximum_windows_per_hypothesis"]
            )
            ledger.enqueue(
                str(job["lane"]),
                fingerprint,
                "fresh_resolved_holdout",
                {
                    "proposal": payload["proposal"],
                    "variant": payload["variant"],
                    "candidate_replay_windows": fresh_windows,
                    "completed_windows": [],
                    "maximum_windows": maximum_fresh_windows,
                    "selection_policy": "first observable windows, up to the fixed maximum, from the frozen causal-signal-ranked newest fully resolved pool; terminal outcomes unread",
                    "selection_granularity_version": FRESH_SELECTION_GRANULARITY_VERSION,
                    "global_reserve_version": FRESH_GLOBAL_RESERVE_VERSION,
                    "global_reserve": payload.get("fresh_previously_measured_exclusion"),
                    "global_reserve_candidate_starts_sha256": stable_hash(
                        sorted(str(item["start"]) for item in fresh_windows)
                    ),
                    "historical_verdict": verdict,
                    "parent_exact_job_id": job["job_id"],
                },
                "historical exact gates passed; frozen fresh holdout queued",
                status="queued",
            )
            ledger.update_hypothesis_status(fingerprint, "eligible_for_fresh_holdout")
        else:
            ledger.update_hypothesis_status(fingerprint, "historical_eligible_awaiting_fresh_data")
    else:
        ledger.update_hypothesis_status(
            fingerprint,
            "historical_insufficient_support"
            if verdict["classification"] == "insufficient_support"
            else "rejected_historical_exact",
        )
    ledger.update_job(job["job_id"], "completed", payload, "bounded exact replay complete")
    return verdict


def historical_exact_priority(candidate: Mapping[str, Any]) -> tuple:
    """Resume the closest support-only shortfall before untouched exact jobs."""
    payload = json.loads(candidate["payload_json"])
    aggregate = payload.get("historical_verdict", {}).get("aggregate", {})
    fills = int(aggregate.get("fills_success") or 0)
    stake = float(aggregate.get("maximum_stake_usd") or 0.0)
    pnl = float(aggregate.get("total_pnl") or 0.0)
    coverage = pnl / stake if stake > 0.0 else float("-inf")
    return (fills, coverage)


def run_queued_exact_job(
    config: Mapping[str, Any], ledger: Ledger, state_dir: Path, dry_run: bool
) -> Dict[str, Any]:
    migration = config.get("architecture_migration", {})
    if not migration.get("legacy_exact_replay_enabled", True):
        queued_jobs = ledger.jobs("exact_l2_replay", "queued")
        if dry_run or not queued_jobs:
            return {
                "status": "paused_architecture_migration",
                "reason": migration.get("reason", "legacy_exact_replay_disabled"),
                "queued_jobs": len(queued_jobs),
            }
        superseded = migration.get("superseded_exact_fingerprints", {})
        blocked = []
        for job in queued_jobs:
            fingerprint = str(job["hypothesis_fingerprint"])
            payload = json.loads(job["payload_json"])
            evidence = superseded.get(fingerprint)
            disposition = (
                "superseded_execution_equivalent"
                if evidence is not None
                else "blocked_architecture_migration"
            )
            payload["architecture_migration"] = {
                "status": disposition,
                "reason": migration.get("reason", "legacy_exact_replay_disabled"),
                "evidence": evidence,
                "preserved_completed_windows": len(payload.get("completed_windows", [])),
                "preserved_support_only_windows": len(
                    payload.get("support_only_windows", [])
                ),
                "recorded_at": utc_now(),
            }
            reason = (
                str(evidence["reason"])
                if evidence is not None
                else "legacy exact replay paused for opportunity-table migration"
            )
            ledger.update_job(job["job_id"], "blocked", payload, reason)
            ledger.update_hypothesis_status(fingerprint, disposition)
            blocked.append(
                {
                    "job_id": job["job_id"],
                    "hypothesis_fingerprint": fingerprint,
                    "status": disposition,
                }
            )
        return {
            "status": "paused_architecture_migration",
            "reason": migration.get("reason", "legacy_exact_replay_disabled"),
            "blocked_jobs": blocked,
        }
    exact = config["exact_replay"]
    if not exact.get("enabled", False):
        return {"status": "disabled"}
    queued_jobs = ledger.jobs("exact_l2_replay", "queued")
    if not queued_jobs:
        return {"status": "empty"}
    job = max(queued_jobs, key=historical_exact_priority)
    payload = json.loads(job["payload_json"])
    completed = payload.setdefault("completed_windows", [])
    support_only = payload.setdefault("support_only_windows", [])
    observable_completed = []
    support_only_starts = {item["start"] for item in support_only}
    for item in completed:
        support = item.get("data_support", {})
        if support.get("observable", True):
            observable_completed.append(item)
        elif item["start"] not in support_only_starts:
            support_only.append(item)
            support_only_starts.add(item["start"])
    if len(observable_completed) != len(completed):
        payload["completed_windows"] = observable_completed
        completed = observable_completed
    frozen_windows = payload.setdefault(
        "frozen_candidate_replay_windows", payload["candidate_replay_windows"]
    )
    attempted = {item["start"] for item in [*completed, *support_only]}
    windows = [
        window
        for window in frozen_windows
        if window["start"] not in attempted
    ]
    maximum = int(exact["maximum_windows_per_hypothesis"])
    gates = exact["historical_gates"]
    current_fills = sum(
        int(item.get("summary", {}).get("fills_success") or 0) for item in completed
    )
    remaining_budget = max(0, maximum - len(completed))
    maximum_possible_fills = current_fills + sum(
        sorted((int(window["public_signals"]) for window in windows), reverse=True)[
            :remaining_budget
        ]
    )
    support_impossible = maximum_possible_fills < int(gates["minimum_fills"])
    if support_impossible:
        payload["maximum_possible_fills"] = maximum_possible_fills
        payload["historical_early_stop_reason"] = "minimum_fills_unreachable"
        verdict = finalize_historical_exact_job(config, ledger, job, payload)
        return {
            "status": "completed",
            "job_id": job["job_id"],
            "windows": len(completed),
            "maximum_possible_fills": maximum_possible_fills,
            "historical_verdict": verdict,
        }
    if not windows or len(completed) >= maximum:
        verdict = finalize_historical_exact_job(config, ledger, job, payload)
        return {
            "status": "completed",
            "job_id": job["job_id"],
            "windows": len(completed),
            "historical_verdict": verdict,
        }
    window = windows[0]
    if dry_run:
        return {"status": "dry_run", "job_id": job["job_id"], "window": window}
    known_unobservable = known_unobservable_pmxt_window(ledger, window)
    if known_unobservable is not None:
        result = {
            "status": "completed_window",
            "job_id": job["job_id"],
            "start": window["start"],
            "end": window["end"],
            "public_signals": window["public_signals"],
            "data_support": known_unobservable,
            "summary": {},
        }
    else:
        result = execute_replay_window(config, job, payload, state_dir, window, "exact_l2")
    if result["status"] == "deferred":
        ledger.update_job(job["job_id"], "queued", payload, result["reason"])
        return result
    if result["status"] != "completed_window":
        ledger.update_job(job["job_id"], "failed", payload, "exact replay produced no compact report")
        return result
    measured_window = {
        key: value for key, value in result.items() if key not in ("status", "job_id")
    }
    if measured_window.get("data_support", {}).get("observable", True):
        completed.append(measured_window)
    else:
        support_only.append(measured_window)
        result["status"] = "support_only_window"
    final = len(completed) >= maximum or len(windows) == 1
    if final:
        result["historical_verdict"] = finalize_historical_exact_job(config, ledger, job, payload)
    else:
        ledger.update_job(
            job["job_id"], "queued", payload, "more preregistered windows remain"
        )
    return result


def fresh_holdout_priority(candidate: Mapping[str, Any]) -> tuple:
    candidate_payload = json.loads(candidate["payload_json"])
    aggregate = candidate_payload.get("historical_verdict", {}).get("aggregate", {})
    stake = float(aggregate.get("maximum_stake_usd") or 0.0)
    pnl = float(aggregate.get("total_pnl") or 0.0)
    coverage = pnl / stake if stake > 0.0 else float("-inf")
    return (coverage, int(aggregate.get("fills_success") or 0))


def replay_window_start_is_covered(
    candidate: Mapping[str, Any], measured_windows: Sequence[Mapping[str, Any]]
) -> bool:
    candidate_start = dt.datetime.fromisoformat(
        str(candidate["start"]).replace("Z", "+00:00")
    )
    for measured in measured_windows:
        measured_start = dt.datetime.fromisoformat(
            str(measured["start"]).replace("Z", "+00:00")
        )
        measured_end = dt.datetime.fromisoformat(
            str(measured["end"]).replace("Z", "+00:00")
        )
        if measured_start <= candidate_start <= measured_end:
            return True
    return False


def known_unobservable_pmxt_window(
    ledger: Ledger, candidate: Mapping[str, Any]
) -> Optional[Dict[str, Any]]:
    for stage in ("fresh_resolved_holdout", "fixed_forward_confirmation"):
        for source_job in ledger.jobs(stage):
            payload = json.loads(source_job["payload_json"])
            for measured in payload.get("support_only_windows", []):
                support = replay_window_data_support(measured)
                if (
                    support.get("reason") == "pmxt_target_events_unavailable"
                    and replay_window_start_is_covered(candidate, [measured])
                ):
                    return {
                        **support,
                        "reused": True,
                        "source_job_id": source_job["job_id"],
                        "source_window_start": measured["start"],
                        "source_window_end": measured["end"],
                        "source_report": measured.get("report"),
                        "source_report_sha256": measured.get("report_sha256"),
                    }
    return None


def run_queued_fresh_holdout_job(
    config: Mapping[str, Any], ledger: Ledger, state_dir: Path, dry_run: bool
) -> Dict[str, Any]:
    if not config["fresh_holdout"].get("enabled", False):
        return {"status": "disabled"}
    queued_jobs = ledger.jobs("fresh_resolved_holdout", "queued")
    if not queued_jobs:
        return {"status": "empty"}

    # The lower-ranked frozen jobs stay queued, so a failed leader cannot erase
    # preregistered alternatives or create optional stopping.
    job = max(queued_jobs, key=fresh_holdout_priority)
    payload = json.loads(job["payload_json"])
    if "variant" not in payload or "candidate_replay_windows" not in payload:
        ledger.update_job(
            job["job_id"],
            "blocked",
            payload,
            "fresh holdout requires an executable frozen variant and preregistered windows",
        )
        return {"status": "blocked", "job_id": job["job_id"], "reason": "legacy_unexecutable_job"}
    completed = payload.setdefault("completed_windows", [])
    support_only = payload.setdefault("support_only_windows", [])
    support_only_starts = {item["start"] for item in support_only}
    observable_completed = []
    for item in completed:
        support = replay_window_data_support(item)
        item.setdefault("data_support", support)
        if support["observable"]:
            observable_completed.append(item)
        elif item["start"] not in support_only_starts:
            support_only.append(item)
            support_only_starts.add(item["start"])
    if len(observable_completed) != len(completed):
        payload["completed_windows"] = observable_completed
        completed = observable_completed
    maximum = int(payload.get("maximum_windows") or config["fresh_holdout"]["maximum_windows_per_hypothesis"])
    frozen_windows = payload.setdefault(
        "frozen_candidate_replay_windows",
        payload["candidate_replay_windows"],
    )
    candidate_windows = payload["candidate_replay_windows"]
    if (
        len(frozen_windows) < len(candidate_windows)
        and frozen_windows == candidate_windows[: len(frozen_windows)]
    ):
        payload["original_frozen_window_count"] = len(frozen_windows)
        payload["support_reserve_policy"] = (
            "append only the already-preregistered causal-ranked reserve; exclude a "
            "window only when its PMXT manifest has no observable target events"
        )
        frozen_windows.extend(candidate_windows[len(frozen_windows) :])
    measured_windows = [*completed, *support_only]
    windows = [
        window
        for window in frozen_windows
        if not replay_window_start_is_covered(window, measured_windows)
    ]
    deferred_starts = set(payload.get("deferred_window_starts", []))
    runnable_windows = [
        window for window in windows if window["start"] not in deferred_starts
    ]
    current_verdict = exact_eligibility(
        completed,
        Path(payload["variant"]["path"]),
        eligibility_gates_for_proposal(
            config["fresh_holdout"]["gates"], payload["proposal"]
        ),
    )
    remaining_budget = max(0, maximum - len(completed))
    maximum_possible_fills = int(current_verdict["aggregate"]["fills_success"]) + sum(
        sorted((int(window["public_signals"]) for window in windows), reverse=True)[
            :remaining_budget
        ]
    )
    support_impossible = maximum_possible_fills < int(
        config["fresh_holdout"]["gates"]["minimum_fills"]
    )
    if support_impossible:
        payload["maximum_possible_fills"] = maximum_possible_fills
    if not support_impossible and windows and len(completed) < maximum:
        if runnable_windows:
            window = runnable_windows[0]
        else:
            payload["deferred_window_starts"] = []
            window = windows[0]
        if dry_run:
            return {"status": "dry_run", "job_id": job["job_id"], "window": window}
        known_unobservable = known_unobservable_pmxt_window(ledger, window)
        if known_unobservable is not None:
            result = {
                "status": "completed_window",
                "job_id": job["job_id"],
                "start": window["start"],
                "end": window["end"],
                "public_signals": window["public_signals"],
                "data_support": known_unobservable,
                "summary": {},
            }
        else:
            result = execute_replay_window(
                config, job, payload, state_dir, window, "fresh_holdout"
            )
        if result["status"] == "deferred":
            deferred = payload.setdefault("deferred_window_starts", [])
            if window["start"] not in deferred:
                deferred.append(window["start"])
            ledger.update_job(job["job_id"], "queued", payload, result["reason"])
            return {**result, "deferred_window": window["start"], "frozen_set_preserved": True}
        if result["status"] != "completed_window":
            retries = int(payload.get("retry_count", 0)) + 1
            payload["retry_count"] = retries
            retry_limit = int(config["fresh_holdout"]["maximum_retries_per_job"])
            ledger.update_job(
                job["job_id"],
                "queued" if retries <= retry_limit else "failed",
                payload,
                "fresh holdout retry %s/%s after no compact report" % (retries, retry_limit),
            )
            return result
        measured_window = {
            key: value for key, value in result.items() if key not in ("status", "job_id")
        }
        if measured_window.get("data_support", {}).get("observable", True):
            completed.append(measured_window)
        else:
            support_only.append(measured_window)
            result["status"] = "support_only_window"
        if len(completed) < maximum and len(windows) > 1:
            ledger.update_job(job["job_id"], "queued", payload, "fresh holdout budget remains")
            return result
    verdict = exact_eligibility(
        completed,
        Path(payload["variant"]["path"]),
        eligibility_gates_for_proposal(
            config["fresh_holdout"]["gates"], payload["proposal"]
        ),
    )
    payload["fresh_holdout_verdict"] = verdict
    payload["fresh_holdout_eligibility_policy_version"] = EXACT_ELIGIBILITY_POLICY_VERSION
    fingerprint = str(job["hypothesis_fingerprint"])
    status = "research_eligible" if verdict["eligible"] else (
        "holdout_insufficient_support"
        if verdict["classification"] == "insufficient_support"
        else "rejected_fresh_holdout"
    )
    factory_generator.append_trial_entry(
        config,
        fingerprint,
        "fresh_resolved_holdout",
        status,
        n=verdict["aggregate"].get("fills_success"),
        wins=factory_generator.direction_wins(verdict["aggregate"]),
    )
    ledger.update_hypothesis_status(fingerprint, status)
    ledger.update_job(job["job_id"], "completed", payload, "frozen fresh holdout complete")
    evidence = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "status": status,
        "research_only": True,
        "live_ready": False,
        "hypothesis_fingerprint": fingerprint,
        "proposal": payload["proposal"],
        "variant": payload["variant"],
        "selection_policy": payload.get("selection_policy"),
        "support_reserve_policy": payload.get("support_reserve_policy"),
        "global_reserve_version": payload.get("global_reserve_version"),
        "global_reserve": payload.get("global_reserve"),
        "global_reserve_candidate_starts_sha256": payload.get(
            "global_reserve_candidate_starts_sha256"
        ),
        "completed_windows": completed,
        "support_only_windows": support_only,
        "verdict": verdict,
        "next_stage": "fixed_forward_confirmation_design" if verdict["eligible"] else None,
    }
    evidence_path = state_dir / ("evidence/eligible/%s.json" % fingerprint)
    atomic_json(evidence_path, evidence)
    economic_enqueued = False
    forward_enqueued = False
    if verdict["eligible"]:
        economic_enqueued = enqueue_economic_screen(
            ledger, job, payload, "fresh_exact_replay"
        )
        forward_enqueued = enqueue_fixed_forward_confirmation(
            config, ledger, job, payload, verdict, evidence_path
        )
    return {
        "status": "research_eligible" if verdict["eligible"] else verdict["classification"],
        "job_id": job["job_id"],
        "artifact": str(evidence_path),
        "artifact_sha256": sha256_file(evidence_path),
        "verdict": verdict,
        "economic_screen_enqueued": economic_enqueued,
        "fixed_forward_enqueued": forward_enqueued,
    }


def recover_retryable_holdout_jobs(config: Mapping[str, Any], ledger: Ledger) -> List[str]:
    recovered: List[str] = []
    retry_limit = int(config["fresh_holdout"]["maximum_retries_per_job"])
    for job in ledger.jobs("fresh_resolved_holdout", "failed"):
        payload = json.loads(job["payload_json"])
        if "variant" not in payload or int(payload.get("retry_count", 0)) >= retry_limit:
            continue
        ledger.update_job(
            job["job_id"],
            "queued",
            payload,
            "retrying after coordinator coverage fix",
        )
        recovered.append(str(job["job_id"]))
    return recovered


def reconcile_completed_fresh_holdout_jobs(
    config: Mapping[str, Any], ledger: Ledger, state_dir: Path
) -> List[Dict[str, Any]]:
    """Recompute policy-only verdict changes without consuming new outcomes."""
    reconciled: List[Dict[str, Any]] = []
    reconciliation_fingerprints = set(
        config["exact_replay"].get("policy_reconciliation_fingerprints", [])
    )
    for job in ledger.jobs("fresh_resolved_holdout", "completed"):
        payload = json.loads(job["payload_json"])
        if (
            "variant" not in payload
            or "candidate_replay_windows" not in payload
        ):
            continue
        policy_changed = (
            payload.get("fresh_holdout_eligibility_policy_version")
            != EXACT_ELIGIBILITY_POLICY_VERSION
        )
        if (
            policy_changed
            and reconciliation_fingerprints
            and str(job["hypothesis_fingerprint"]) not in reconciliation_fingerprints
        ):
            continue
        previous_verdict = payload.get("fresh_holdout_verdict")
        previous_policy = payload.get("fresh_holdout_eligibility_policy_version")
        if policy_changed and (previous_verdict is not None or previous_policy is not None):
            payload.setdefault("superseded_fresh_holdout_verdicts", []).append(
                {
                    "policy_version": previous_policy,
                    "verdict": previous_verdict,
                    "superseded_at": utc_now(),
                }
            )
        if policy_changed:
            verdict = exact_eligibility(
                payload.get("completed_windows", []),
                Path(payload["variant"]["path"]),
                eligibility_gates_for_proposal(
                    config["fresh_holdout"]["gates"], payload["proposal"]
                ),
            )
            payload["fresh_holdout_verdict"] = verdict
            payload["fresh_holdout_eligibility_policy_version"] = (
                EXACT_ELIGIBILITY_POLICY_VERSION
            )
        else:
            verdict = payload.get("fresh_holdout_verdict", {})
        if not isinstance(verdict, dict) or "eligible" not in verdict:
            continue
        fingerprint = str(job["hypothesis_fingerprint"])
        has_economic_job = any(
            str(item["hypothesis_fingerprint"]) == fingerprint
            for item in ledger.jobs("economic_opportunity_screen")
        )
        has_forward_job = any(
            str(item["hypothesis_fingerprint"]) == fingerprint
            for item in ledger.jobs("fixed_forward_confirmation")
        )
        if not policy_changed and (
            not verdict["eligible"] or (has_economic_job and has_forward_job)
        ):
            continue
        status = "research_eligible" if verdict["eligible"] else (
            "holdout_insufficient_support"
            if verdict.get("classification") == "insufficient_support"
            else "rejected_fresh_holdout"
        )
        ledger.update_hypothesis_status(fingerprint, status)
        ledger.update_job(
            job["job_id"], "completed", payload, "fresh holdout verdict reconciled in place"
        )
        evidence_path = state_dir / ("evidence/eligible/%s.json" % fingerprint)
        evidence = {
            "schema_version": 1,
            "generated_at": utc_now(),
            "status": status,
            "research_only": True,
            "live_ready": False,
            "hypothesis_fingerprint": fingerprint,
            "proposal": payload["proposal"],
            "variant": payload["variant"],
            "selection_policy": payload.get("selection_policy"),
            "support_reserve_policy": payload.get("support_reserve_policy"),
            "global_reserve_version": payload.get("global_reserve_version"),
            "global_reserve": payload.get("global_reserve"),
            "global_reserve_candidate_starts_sha256": payload.get(
                "global_reserve_candidate_starts_sha256"
            ),
            "completed_windows": payload.get("completed_windows", []),
            "support_only_windows": payload.get("support_only_windows", []),
            "verdict": verdict,
            "next_stage": "fixed_forward_confirmation" if verdict["eligible"] else None,
        }
        atomic_json(evidence_path, evidence)
        forward_enqueued = False
        economic_enqueued = False
        if verdict["eligible"]:
            economic_enqueued = enqueue_economic_screen(
                ledger, job, payload, "fresh_exact_replay"
            )
            forward_enqueued = enqueue_fixed_forward_confirmation(
                config, ledger, job, payload, verdict, evidence_path
            )
        if policy_changed or economic_enqueued or forward_enqueued:
            reconciled.append(
                {
                    "job_id": job["job_id"],
                    "hypothesis_fingerprint": fingerprint,
                    "classification": verdict["classification"],
                    "economic_screen_enqueued": economic_enqueued,
                    "fixed_forward_enqueued": forward_enqueued,
                }
            )
    return reconciled


def reconcile_economic_screen_statuses(ledger: Ledger) -> List[str]:
    """Keep terminal downstream states from being masked by earlier gates."""
    reconciled: List[str] = []
    for job in ledger.jobs("economic_opportunity_screen", "completed"):
        payload = json.loads(job["payload_json"])
        verdict = payload.get("economic_verdict")
        if not isinstance(verdict, dict) or verdict.get("passed") is not False:
            continue
        fingerprint = str(job["hypothesis_fingerprint"])
        hypothesis = ledger.hypothesis(fingerprint)
        rejection_status = economic_rejection_status(payload)
        if hypothesis and hypothesis.get("status") != rejection_status:
            ledger.update_hypothesis_status(fingerprint, rejection_status)
            reconciled.append(fingerprint)
    for job in ledger.jobs("economic_opportunity_screen", "blocked"):
        fingerprint = str(job["hypothesis_fingerprint"])
        hypothesis = ledger.hypothesis(fingerprint)
        if hypothesis and hypothesis.get("status") == "stage_1_survivor":
            ledger.update_hypothesis_status(fingerprint, "economic_screen_unavailable")
            reconciled.append(fingerprint)
    return reconciled


def reconcile_fresh_holdout_global_reserve(
    config: Mapping[str, Any], ledger: Ledger, snapshot: Path
) -> List[Dict[str, Any]]:
    """Repair legacy queued holdouts whose exact queue overwrote the clean reserve."""
    windows: Optional[List[Dict[str, Any]]] = None
    reconciled: List[Dict[str, Any]] = []
    for job in ledger.jobs("fresh_resolved_holdout", "queued"):
        payload = json.loads(job["payload_json"])
        if payload.get("global_reserve_version") == FRESH_GLOBAL_RESERVE_VERSION:
            continue
        proposal = payload.get("proposal")
        if not isinstance(proposal, dict) or "rule" not in proposal:
            continue
        if windows is None:
            windows = load_public_windows(snapshot)
        excluded = ledger.measured_fresh_window_starts(
            exclude_job_id=str(job["job_id"])
        )
        screen = evaluate_late_rule(
            windows,
            proposal,
            config["lanes"]["late_window_mechanisms"]["stage_1_gates"],
            excluded_fresh_window_starts=excluded,
        )
        clean_windows = screen["fresh_candidate_windows"]
        clean_starts = {str(item["start"]) for item in clean_windows}
        previous_completed = payload.get("completed_windows", [])
        previous_support_only = payload.get("support_only_windows", [])
        retained_completed = [
            item for item in previous_completed if str(item.get("start")) in clean_starts
        ]
        retained_support_only = [
            item for item in previous_support_only if str(item.get("start")) in clean_starts
        ]
        removed = [
            item
            for item in [*previous_completed, *previous_support_only]
            if str(item.get("start")) not in clean_starts
        ]
        if removed:
            payload.setdefault("superseded_fresh_holdout_windows", []).append(
                {
                    "reason": "legacy_exact_queue_overwrote_global_reserve",
                    "superseded_at": utc_now(),
                    "windows": removed,
                }
            )
        payload["candidate_replay_windows"] = clean_windows
        payload["frozen_candidate_replay_windows"] = clean_windows
        payload["completed_windows"] = retained_completed
        payload["support_only_windows"] = retained_support_only
        payload["deferred_window_starts"] = [
            start
            for start in payload.get("deferred_window_starts", [])
            if str(start) in clean_starts
        ]
        payload["global_reserve_version"] = FRESH_GLOBAL_RESERVE_VERSION
        payload["global_reserve"] = screen["fresh_previously_measured_exclusion"]
        payload["global_reserve_candidate_starts_sha256"] = stable_hash(
            sorted(clean_starts)
        )
        ledger.update_job(
            job["job_id"],
            "queued",
            payload,
            "repaired frozen fresh pool under independent global reserve",
        )
        reconciled.append(
            {
                "job_id": job["job_id"],
                "hypothesis_fingerprint": job["hypothesis_fingerprint"],
                "clean_windows": len(clean_windows),
                "retained_observable_windows": len(retained_completed),
                "removed_legacy_windows": len(removed),
            }
        )
    return reconciled


def reconcile_fresh_holdout_window_granularity(
    config: Mapping[str, Any], ledger: Ledger, snapshot: Path
) -> List[Dict[str, Any]]:
    """Replace legacy 8h pools with outcome-blind signal-hour pools."""
    windows: Optional[List[Dict[str, Any]]] = None
    reconciled: List[Dict[str, Any]] = []
    for job in ledger.jobs("fresh_resolved_holdout", "queued"):
        payload = json.loads(job["payload_json"])
        if (
            payload.get("selection_granularity_version")
            == FRESH_SELECTION_GRANULARITY_VERSION
        ):
            continue
        proposal = payload.get("proposal")
        if not isinstance(proposal, dict) or "rule" not in proposal or "variant" not in payload:
            continue
        if windows is None:
            windows = load_public_windows(snapshot)
        screen = evaluate_late_rule(
            windows,
            proposal,
            config["lanes"]["late_window_mechanisms"]["stage_1_gates"],
        )
        signal_hour_pool = screen["fresh_candidate_windows"]
        payload.setdefault("superseded_candidate_replay_pools", []).append(
            {
                "selection_granularity_version": payload.get(
                    "selection_granularity_version", "legacy_8h"
                ),
                "candidate_replay_windows": payload.get("candidate_replay_windows", []),
                "frozen_candidate_replay_windows": payload.get(
                    "frozen_candidate_replay_windows", []
                ),
                "superseded_at": utc_now(),
            }
        )
        payload["candidate_replay_windows"] = signal_hour_pool
        payload["frozen_candidate_replay_windows"] = list(signal_hour_pool)
        payload["selection_granularity_version"] = FRESH_SELECTION_GRANULARITY_VERSION
        payload["selection_policy"] = (
            "first observable signal-hour windows, up to the fixed maximum, from "
            "the frozen causal-signal-ranked newest fully resolved pool; terminal "
            "outcomes unread"
        )
        payload["selection_snapshot"] = {
            "path": str(snapshot),
            "sha256": sha256_file(snapshot),
        }
        payload["deferred_window_starts"] = []
        ledger.update_job(
            job["job_id"],
            "queued",
            payload,
            "migrated outcome-blind replay selection to signal-hour windows",
        )
        reconciled.append(
            {
                "job_id": job["job_id"],
                "hypothesis_fingerprint": job["hypothesis_fingerprint"],
                "signal_hour_windows": len(signal_hour_pool),
            }
        )
    return reconciled


def reconcile_completed_exact_jobs(
    config: Mapping[str, Any], ledger: Ledger, snapshot: Path
) -> List[Dict[str, Any]]:
    """Migrate policy changes and extend clean support-only shortfalls."""
    windows: Optional[List[Dict[str, Any]]] = None
    reconciled: List[Dict[str, Any]] = []
    maximum = int(config["exact_replay"]["maximum_windows_per_hypothesis"])
    reconciliation_fingerprints = set(
        config["exact_replay"].get("policy_reconciliation_fingerprints", [])
    )
    excluded_fresh_window_starts = ledger.measured_fresh_window_starts()
    for job in ledger.jobs("exact_l2_replay", "completed"):
        payload = json.loads(job["payload_json"])
        verdict = payload.get("historical_verdict", {})
        gates = verdict.get("gates", {})
        attempted_starts = {
            str(item.get("start"))
            for field in ("completed_windows", "support_only_windows")
            for item in payload.get(field, [])
            if item.get("start")
        }
        frozen_windows = payload.get(
            "frozen_candidate_replay_windows",
            payload.get("candidate_replay_windows", []),
        )
        has_remaining_window = any(
            str(item.get("start")) not in attempted_starts for item in frozen_windows
        )
        failed_only_minimum_fills = (
            verdict.get("classification") == "insufficient_support"
            and gates.get("minimum_fills") is False
            and all(
                bool(value)
                for key, value in gates.items()
                if key != "minimum_fills"
            )
        )
        completed_count = len(payload.get("completed_windows", []))
        previous_maximum = int(
            payload.get("historical_maximum_windows") or completed_count
        )
        if (
            failed_only_minimum_fills
            and maximum > previous_maximum
            and completed_count < maximum
            and has_remaining_window
            and isinstance(payload.get("proposal"), dict)
        ):
            if windows is None:
                windows = load_public_windows(snapshot)
            screen = evaluate_late_rule(
                windows,
                payload["proposal"],
                config["lanes"]["late_window_mechanisms"]["stage_1_gates"],
                excluded_fresh_window_starts=excluded_fresh_window_starts,
            )
            if screen["gates"]["fresh_support_capacity"]:
                payload.setdefault("superseded_historical_verdicts", []).append(
                    {
                        "maximum_windows": previous_maximum,
                        "verdict": verdict,
                        "superseded_at": utc_now(),
                        "reason": "support_only_window_cap_expansion",
                    }
                )
                payload["fresh_candidate_windows"] = screen["fresh_candidate_windows"]
                payload["fresh_capacity_signals"] = screen["fresh_capacity_signals"]
                payload["fresh_previously_measured_exclusion"] = screen[
                    "fresh_previously_measured_exclusion"
                ]
                payload["historical_maximum_windows"] = maximum
                ledger.update_job(
                    job["job_id"],
                    "queued",
                    payload,
                    "reopened clean minimum-fills shortfall under %d-window historical cap"
                    % maximum,
                )
                ledger.update_hypothesis_status(
                    str(job["hypothesis_fingerprint"]), "stage_1_survivor"
                )
                reconciled.append(
                    {
                        "job_id": job["job_id"],
                        "hypothesis_fingerprint": job["hypothesis_fingerprint"],
                        "reconciliation": "historical_window_cap_expansion",
                        "previous_maximum_windows": previous_maximum,
                        "maximum_windows": maximum,
                        "fresh_candidate_windows": len(
                            screen["fresh_candidate_windows"]
                        ),
                    }
                )
            continue
        if (
            payload.get("historical_eligibility_policy_version")
            == EXACT_ELIGIBILITY_POLICY_VERSION
            or "variant" not in payload
        ):
            continue
        if (
            reconciliation_fingerprints
            and str(job["hypothesis_fingerprint"]) not in reconciliation_fingerprints
        ):
            continue
        proposal = payload.get("proposal")
        if not isinstance(proposal, dict) or "rule" not in proposal:
            continue
        if windows is None:
            windows = load_public_windows(snapshot)
        screen = evaluate_late_rule(
            windows,
            proposal,
            config["lanes"]["late_window_mechanisms"]["stage_1_gates"],
            excluded_fresh_window_starts=excluded_fresh_window_starts,
        )
        payload["fresh_candidate_windows"] = screen["fresh_candidate_windows"]
        payload["fresh_previously_measured_exclusion"] = screen[
            "fresh_previously_measured_exclusion"
        ]
        verdict = finalize_historical_exact_job(config, ledger, job, payload)
        reconciled.append(
            {
                "job_id": job["job_id"],
                "hypothesis_fingerprint": job["hypothesis_fingerprint"],
                "historical_verdict": verdict,
                "fresh_candidate_windows": len(screen["fresh_candidate_windows"]),
            }
        )
    return reconciled


def prune_queued_exact_jobs_without_fresh_capacity(
    config: Mapping[str, Any], ledger: Ledger, snapshot: Path
) -> List[Dict[str, Any]]:
    windows: Optional[List[Dict[str, Any]]] = None
    pruned: List[Dict[str, Any]] = []
    excluded_fresh_window_starts = ledger.measured_fresh_window_starts()
    for job in ledger.jobs("exact_l2_replay", "queued"):
        payload = json.loads(job["payload_json"])
        proposal = payload.get("proposal")
        if not isinstance(proposal, dict) or "rule" not in proposal:
            continue
        if windows is None:
            windows = load_public_windows(snapshot)
        screen = evaluate_late_rule(
            windows,
            proposal,
            config["lanes"]["late_window_mechanisms"]["stage_1_gates"],
            excluded_fresh_window_starts=excluded_fresh_window_starts,
        )
        if screen["gates"]["fresh_support_capacity"]:
            payload["fresh_candidate_windows"] = screen["fresh_candidate_windows"]
            payload["fresh_capacity_signals"] = screen["fresh_capacity_signals"]
            payload["fresh_previously_measured_exclusion"] = screen[
                "fresh_previously_measured_exclusion"
            ]
            ledger.update_job(job["job_id"], "queued", payload, str(job["reason"] or "queued"))
            continue
        payload["fresh_candidate_windows"] = screen["fresh_candidate_windows"]
        payload["fresh_capacity_signals"] = screen["fresh_capacity_signals"]
        payload["fresh_previously_measured_exclusion"] = screen[
            "fresh_previously_measured_exclusion"
        ]
        ledger.update_job(
            job["job_id"],
            "blocked",
            payload,
            "fresh support capacity cannot satisfy the fixed holdout fill floor",
        )
        ledger.update_hypothesis_status(
            str(job["hypothesis_fingerprint"]), "rejected_stage_1_fresh_capacity"
        )
        pruned.append(
            {
                "job_id": job["job_id"],
                "hypothesis_fingerprint": job["hypothesis_fingerprint"],
                "fresh_capacity_signals": screen["fresh_capacity_signals"],
            }
        )
    return pruned


def run_registry_audit(config: Mapping[str, Any], state_dir: Path, dry_run: bool) -> Dict[str, Any]:
    engine = resolve_repo_path(config["engine_path"])
    registry = resolve_repo_path(config["registry_path"])
    destination = state_dir / "evidence/registry-audit-latest.json"
    temporary = destination.with_name("%s.tmp.%s" % (destination.name, os.getpid()))
    destination.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(engine),
        "strategy-builder",
        "registry-audit",
        "--registry",
        str(registry),
        "--output",
        str(temporary),
    ]
    result = run_command(command, int(config["resource_policy"]["command_timeout_seconds"]), dry_run)
    if not dry_run and result["status"] == "completed" and temporary.is_file():
        os.replace(str(temporary), str(destination))
        result["artifact"] = str(destination)
        result["sha256"] = sha256_file(destination)
    else:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
    return result


def run_baseline_evolution(
    config: Mapping[str, Any], ledger: Ledger, state_dir: Path, dry_run: bool
) -> Dict[str, Any]:
    migration = config.get("architecture_migration", {})
    if not migration.get("legacy_candidate_generation_enabled", True):
        return {
            "status": "paused_architecture_migration",
            "reason": migration.get("reason", "legacy_candidate_generation_disabled"),
        }
    lane = config["lanes"]["baseline_evolution"]
    if seconds_since(ledger.meta("baseline_evolution.last_at")) < int(lane["minimum_interval_seconds"]):
        return {"status": "not_due"}
    engine = resolve_repo_path(config["engine_path"])
    reports = [resolve_repo_path(raw) for raw in lane["reports"]]
    missing = [str(path) for path in reports if not path.is_file()]
    if missing:
        if not dry_run:
            ledger.set_meta("baseline_evolution.last_at", utc_now())
        return {"status": "blocked", "reason": "missing_reports", "paths": missing}
    report_contracts = []
    for report in reports:
        payload = json.loads(report.read_text())
        manifest = payload.get("data_manifest", {})
        sources = manifest.get("sources", []) if isinstance(manifest, dict) else manifest
        versions = sorted(
            {
                str(source.get("metadata", {}).get("replay_semantics_version"))
                for source in sources
                if isinstance(source, dict)
                if source.get("metadata", {}).get("replay_semantics_version") is not None
            }
        )
        report_contracts.append(
            {
                "path": str(report),
                "start": payload.get("start"),
                "end": payload.get("end"),
                "replay_semantics_versions": versions,
            }
        )
    required_semantics = str(lane["required_replay_semantics_version"])
    valid_contracts = [
        contract
        for contract in report_contracts
        if contract["replay_semantics_versions"] == [required_semantics]
    ]
    distinct_windows = {
        (contract["start"], contract["end"])
        for contract in valid_contracts
        if contract["start"] and contract["end"]
    }
    if len(valid_contracts) < int(lane["minimum_reports"]) or len(distinct_windows) < int(
        lane["minimum_distinct_windows"]
    ):
        if not dry_run:
            ledger.set_meta("baseline_evolution.last_at", utc_now())
        return {
            "status": "blocked",
            "reason": "awaiting_current_semantics_chronological_reports",
            "required_replay_semantics_version": required_semantics,
            "required_reports": int(lane["minimum_reports"]),
            "required_distinct_windows": int(lane["minimum_distinct_windows"]),
            "valid_reports": len(valid_contracts),
            "distinct_windows": len(distinct_windows),
            "report_contracts": report_contracts,
        }
    input_hash = stable_hash(
        {
            "reports": [
                {"path": str(report), "sha256": sha256_file(report)}
                for report in reports
            ],
            "required_replay_semantics_version": required_semantics,
            "seed": int(lane["seed"]),
            "population": int(lane["population"]),
            "generations": int(lane["generations"]),
            "elite_count": int(lane["elite_count"]),
            "top": int(lane["top"]),
        }
    )
    if ledger.meta("baseline_evolution.input_hash") == input_hash:
        if not dry_run:
            ledger.set_meta("baseline_evolution.last_at", utc_now())
        return {"status": "unchanged_inputs", "input_hash": input_hash}
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = state_dir / "runs/baseline_evolution" / run_id
    command: List[str] = [
        str(engine),
        "strategy-builder",
        "evolve-search",
    ]
    for report in reports:
        command.extend(["--report", str(report)])
    command.extend(
        [
            "--out-dir",
            str(out_dir),
            "--seed",
            str(lane["seed"]),
            "--population",
            str(lane["population"]),
            "--generations",
            str(lane["generations"]),
            "--elite-count",
            str(lane["elite_count"]),
            "--top",
            str(lane["top"]),
        ]
    )
    result = run_command(command, int(config["resource_policy"]["command_timeout_seconds"]), dry_run)
    if not dry_run:
        ledger.set_meta("baseline_evolution.last_at", utc_now())
    summary = out_dir / "evolution_summary.json"
    if result["status"] == "completed" and summary.is_file():
        digest = sha256_file(summary)
        fingerprint = stable_hash({"lane": "baseline_evolution", "summary_sha256": digest})
        proposal = {
            "kind": "deterministic_evolution",
            "summary_path": str(summary),
            "summary_sha256": digest,
            "historical_only": True,
        }
        if not ledger.has_hypothesis(fingerprint):
            ledger.add_hypothesis(
                fingerprint,
                "baseline_evolution",
                proposal,
                None,
                "historic_screen_complete",
                summary,
            )
        result["artifact"] = str(summary)
        result["sha256"] = digest
        result["input_hash"] = input_hash
        ledger.set_meta("baseline_evolution.input_hash", input_hash)
    return result


def run_late_window_lane(
    config: Mapping[str, Any], ledger: Ledger, state_dir: Path, dry_run: bool
) -> Dict[str, Any]:
    migration = config.get("architecture_migration", {})
    if not migration.get("legacy_candidate_generation_enabled", True):
        return {
            "status": "paused_architecture_migration",
            "reason": migration.get("reason", "legacy_candidate_generation_disabled"),
        }
    lane = config["lanes"]["late_window_mechanisms"]
    if seconds_since(ledger.meta("late_window_mechanisms.last_at")) < int(
        lane["minimum_interval_seconds"]
    ):
        return {"status": "not_due"}
    snapshot = resolve_repo_path(lane["public_snapshot"])
    if not snapshot.is_file():
        return {"status": "blocked", "reason": "missing_public_snapshot"}
    snapshot_hash = sha256_file(snapshot)
    client = LmStudioClient(config["llm"], state_dir)
    proposal, review, provenance = propose_late_rule(
        client, ledger, snapshot_hash, config, state_dir
    )
    fingerprint = stable_hash(
        {
            "lane": "late_window_mechanisms",
            "rule": normalized_late_rule(proposal["rule"]),
            "evaluator_version": LATE_EVALUATOR_VERSION,
        }
    )
    if ledger.has_hypothesis(fingerprint):
        return {"status": "duplicate", "fingerprint": fingerprint}
    if dry_run:
        return {
            "status": "dry_run",
            "fingerprint": fingerprint,
            "proposal": proposal,
            "review": review,
            "llm": provenance,
        }
    windows = load_public_windows(snapshot)
    evidence = evaluate_late_rule(
        windows,
        proposal,
        lane["stage_1_gates"],
        sorted(ledger.measured_fresh_window_starts()),
    )
    variant_artifact = None
    if evidence["stage_1_survivor"]:
        variant_artifact = compile_late_variant(
            proposal,
            resolve_repo_path(lane["base_variant"]),
            state_dir / ("candidates/late_window_mechanisms/%s/variant.json" % fingerprint),
            fingerprint,
        )
    evidence.update(
        {
            "fingerprint": fingerprint,
            "source": {"path": str(snapshot), "sha256": snapshot_hash},
            "llm": provenance,
            "review": review,
            "compiled_variant": variant_artifact,
        }
    )
    evidence_path = state_dir / ("evidence/late_window_mechanisms/%s.json" % fingerprint)
    atomic_json(evidence_path, evidence)
    status = "stage_1_survivor" if evidence["stage_1_survivor"] else "rejected_stage_1"
    factory_generator.append_trial_entry(
        config,
        fingerprint,
        "public_directional_screen",
        status,
        n=evidence["overall"]["signals"],
        wins=evidence["overall"]["wins"],
    )
    ledger.add_hypothesis(
        fingerprint,
        "late_window_mechanisms",
        proposal,
        review,
        status,
        evidence_path,
        source=provenance["proposal_source"],
    )
    if evidence["stage_1_survivor"]:
        ledger.enqueue(
            "late_window_mechanisms",
            fingerprint,
            "economic_opportunity_screen",
            {
                "source_kind": "cached_family_top_of_book",
                "proposal": proposal,
                "public_screen": str(evidence_path),
                "variant": variant_artifact,
                "candidate_replay_windows": evidence["candidate_replay_windows"],
                "fresh_candidate_windows": evidence["fresh_candidate_windows"],
                "fresh_previously_measured_exclusion": evidence[
                    "fresh_previously_measured_exclusion"
                ],
                "completed_windows": [],
                "maximum_exact_l2_shortlist": int(config["maximum_exact_l2_shortlist"]),
            },
            "queued for cached fee-aware economic fail-fast before exact L2",
            status="queued",
        )
    ledger.set_meta("late_window_mechanisms.last_at", utc_now())
    return {
        "status": status,
        "fingerprint": fingerprint,
        "artifact": str(evidence_path),
        "overall": evidence["overall"],
        "gates": evidence["gates"],
        "llm_ready": provenance["readiness"]["ready"],
        "proposal_source": provenance["proposal_source"],
        "burst": provenance.get("burst"),
    }


# e-process verdict -> summary bucket.  The null is the worst-case break-even
# (entry cap + taker fee) over every public signal, not the executable
# strategy's per-fill null, so only a futility kill changes the hypothesis
# status; a promote verdict stays informational (evidence_accrual.verdict and
# the trial ledger) and never overwrites the pipeline stage.
FRESH_PUBLIC_ACCRUAL_BUCKETS = {"continue": "accruing", "promote": "promoted", "kill": "killed"}


def taker_fee(price: float) -> float:
    # Copied from scripts/adaptation_persistence_study.py so the loop does not
    # import a study script for one line.
    return 0.072 * price * (1.0 - price)


def stage_1_screen_cut(evidence: Mapping[str, Any]) -> Optional[int]:
    """Last second of the newest signal hour the public screen enumerated.

    Scored replay buckets and the outcome-blind fresh buckets both count as
    used at proposal time, so accrual starts strictly after them.  Legacy
    artifacts without bucket lists have no safe cut and return None.
    """
    ends = [
        str(window["end"])
        for key in ("candidate_replay_windows", "fresh_candidate_windows")
        for window in evidence.get(key, [])
    ]
    if not ends:
        return None
    newest = dt.datetime.fromisoformat(max(ends).replace("Z", "+00:00"))
    return int(newest.timestamp()) + 3600 * LATE_REPLAY_BUCKET_HOURS - 1


def run_fresh_public_accrual(
    config: Mapping[str, Any], ledger: Ledger, snapshot_path: Path
) -> Dict[str, Any]:
    """Accrue e-value evidence for live late-lane rules on fresh public windows.

    Only windows strictly newer than both the stage-1 screen's newest hour and
    the accrual's own cut are scored, so no window is ever counted twice.  The
    pipeline stage in hypotheses.status is left alone unless the e-process
    reaches futility.
    """
    summary = {"evaluated": 0, "promoted": 0, "killed": 0, "accruing": 0, "skipped": 0}
    windows: Optional[List[Dict[str, Any]]] = None
    for hypothesis in ledger.late_hypotheses():
        if hypothesis["status"] in factory_generator.KILL_STATUS_STAGES:
            continue
        fingerprint = hypothesis["fingerprint"]
        proposal = hypothesis["proposal"]
        record = ledger.hypothesis(fingerprint) or {}
        try:
            cut = stage_1_screen_cut(json.loads(Path(record["evidence_path"]).read_text()))
        except (KeyError, TypeError, OSError, ValueError):
            cut = None
        rule = normalized_late_rule(proposal["rule"])
        price = float(rule["maximum_entry_price"])
        break_even = price + taker_fee(price)
        # The L2-only filters cannot be evaluated from public windows, so those
        # rules are not scored: the accrual would count signals they never trade.
        if (
            cut is None
            or not 0.0 < break_even < 1.0
            or float(rule["settlement_sigma_buffer"]) > 0.0
            or float(rule["minimum_book_pressure"]) > -1.0
        ):
            summary["skipped"] += 1
            continue
        accrual = ledger.accrual(fingerprint)
        floor = max(cut, int(accrual["last_window_start"])) if accrual else cut
        if windows is None:
            windows = load_public_windows(snapshot_path)
        outcomes: List[Tuple[int, float, bool]] = []
        for row in windows:
            if int(row["window_start"]) <= floor:
                continue
            signal = causal_late_signal(row, proposal)
            if signal is None:
                continue
            terminal_direction = sign(float(row["terminal"]) - float(row["p0"]))
            if terminal_direction == 0:
                continue
            outcomes.append(
                (int(row["window_start"]), break_even, signal["direction"] == terminal_direction)
            )
        result = ledger.accrue(fingerprint, "late_window_mechanisms", outcomes, cut)
        if result["applied"]:
            factory_generator.append_trial_entry(
                config,
                fingerprint,
                "fresh_public_accrual",
                result["verdict"],
                n=result["n"],
                wins=result["wins"],
            )
        if result["verdict"] == "kill":
            ledger.update_hypothesis_status(fingerprint, "killed_futility")
        summary["evaluated"] += 1
        summary[FRESH_PUBLIC_ACCRUAL_BUCKETS[result["verdict"]]] += 1
    return summary


def run_band_lane(
    config: Mapping[str, Any], ledger: Ledger, state_dir: Path, dry_run: bool
) -> Dict[str, Any]:
    return band_lane.run_band_lane(
        config, ledger, state_dir, dry_run, LmStudioClient(config["llm"], state_dir)
    )


def run_opportunity_policy_search(
    config: Mapping[str, Any], ledger: Ledger, state_dir: Path, dry_run: bool
) -> Dict[str, Any]:
    search = config["architecture_migration"]["opportunity_policy_search"]
    if not search.get("enabled", False):
        return {"status": "disabled"}
    dataset_seal = resolve_repo_path(str(search["dataset_seal"]))
    labels_manifest = resolve_repo_path(str(search["labels_manifest"]))
    missing = [str(path) for path in (dataset_seal, labels_manifest) if not path.is_file()]
    if missing:
        return {"status": "awaiting_sealed_inputs", "missing": missing}
    settings = {
        "minimum_calibration_support": int(search["minimum_calibration_support"]),
        "minimum_policy_support": int(search["minimum_policy_support"]),
        "safety_margin": float(search["safety_margin"]),
        "latency_ms": int(search["latency_ms"]),
        "maximum_exact_replays": int(config["maximum_exact_l2_shortlist"]),
    }
    input_hash = stable_hash(
        {
            "dataset_seal_sha256": sha256_file(dataset_seal),
            "labels_manifest_sha256": sha256_file(labels_manifest),
            "settings": settings,
        }
    )
    output = state_dir / "runs/opportunity_policy_search" / (input_hash + ".json")
    if ledger.meta("opportunity_policy_search.input_hash") == input_hash and output.is_file():
        report = json.loads(output.read_text())
        result = {
            "status": "unchanged_inputs",
            "input_hash": input_hash,
            "artifact": str(output),
            "verdict": report.get("verdict"),
            "eligible_policy_count": report.get("eligible_policy_count"),
            "unique_replay_count": report.get("exact_replay_plan", {}).get(
                "unique_replay_count"
            ),
        }
        result["exact_replay"] = run_opportunity_exact_replay(
            config, ledger, state_dir, output, dry_run
        )
        return result
    command = [
        str(resolve_repo_path(config["engine_path"])),
        "strategy-builder",
        "opportunity-policy-search",
        "--dataset-seal",
        str(dataset_seal),
        "--labels-manifest",
        str(labels_manifest),
        "--output",
        str(output),
        "--minimum-calibration-support",
        str(settings["minimum_calibration_support"]),
        "--minimum-policy-support",
        str(settings["minimum_policy_support"]),
        "--safety-margin",
        str(settings["safety_margin"]),
        "--latency-ms",
        str(settings["latency_ms"]),
        "--maximum-exact-replays",
        str(settings["maximum_exact_replays"]),
    ]
    execution = run_command(
        command, int(config["resource_policy"]["command_timeout_seconds"]), dry_run
    )
    if dry_run:
        return {"status": "dry_run", "input_hash": input_hash, "execution": execution}
    if execution["status"] != "completed" or not output.is_file():
        return {"status": "failed", "input_hash": input_hash, "execution": execution}
    report = json.loads(output.read_text())
    ledger.set_meta("opportunity_policy_search.input_hash", input_hash)
    ledger.set_meta("opportunity_policy_search.last_at", utc_now())
    result = {
        "status": "completed",
        "input_hash": input_hash,
        "artifact": str(output),
        "verdict": report.get("verdict"),
        "policies_evaluated": report.get("policies_evaluated"),
        "eligible_policy_count": report.get("eligible_policy_count"),
        "unique_replay_count": report.get("exact_replay_plan", {}).get(
            "unique_replay_count"
        ),
        "fresh_holdout_outcomes_accessed": report.get(
            "fresh_holdout_outcomes_accessed"
        ),
        "execution": execution,
    }
    result["exact_replay"] = run_opportunity_exact_replay(
        config, ledger, state_dir, output, dry_run
    )
    return result


def run_opportunity_exact_replay(
    config: Mapping[str, Any],
    ledger: Ledger,
    state_dir: Path,
    policy_search_report: Path,
    dry_run: bool,
) -> Dict[str, Any]:
    search = config["architecture_migration"]["opportunity_policy_search"]
    report = json.loads(policy_search_report.read_text())
    replay_count = int(
        report.get("exact_replay_plan", {}).get("unique_replay_count", 0)
    )
    if replay_count == 0:
        return {"status": "no_replay_plan", "unique_replay_count": 0}
    if replay_count > int(config["maximum_exact_l2_shortlist"]):
        return {
            "status": "blocked_unbounded_replay_plan",
            "unique_replay_count": replay_count,
        }

    dataset_seal = resolve_repo_path(str(search["dataset_seal"]))
    labels_manifest = resolve_repo_path(str(search["labels_manifest"]))
    cache_dir = resolve_repo_path(str(search["pmxt_cache_dir"]))
    missing = [
        str(path)
        for path in (dataset_seal, labels_manifest, policy_search_report, cache_dir)
        if not path.exists()
    ]
    if missing:
        return {"status": "awaiting_exact_replay_inputs", "missing": missing}

    input_hash = stable_hash(
        {
            "dataset_seal_sha256": sha256_file(dataset_seal),
            "labels_manifest_sha256": sha256_file(labels_manifest),
            "policy_search_report_sha256": sha256_file(policy_search_report),
            "pmxt_cache_dir": str(cache_dir),
        }
    )
    output = state_dir / "runs/opportunity_exact_replay" / (input_hash + ".json")
    if ledger.meta("opportunity_exact_replay.input_hash") == input_hash and output.is_file():
        replay = json.loads(output.read_text())
        return {
            "status": "unchanged_inputs",
            "input_hash": input_hash,
            "artifact": str(output),
            "verdict": replay.get("verdict"),
            "source_pmxt_scans": replay.get("source_pmxt_scans"),
            "fresh_holdout_outcomes_accessed": replay.get(
                "fresh_holdout_outcomes_accessed"
            ),
        }

    command = [
        str(resolve_repo_path(config["engine_path"])),
        "strategy-builder",
        "opportunity-exact-replay",
        "--dataset-seal",
        str(dataset_seal),
        "--labels-manifest",
        str(labels_manifest),
        "--policy-search-report",
        str(policy_search_report),
        "--cache-dir",
        str(cache_dir),
        "--output",
        str(output),
    ]
    execution = run_command(
        command, int(config["resource_policy"]["command_timeout_seconds"]), dry_run
    )
    if dry_run:
        return {"status": "dry_run", "input_hash": input_hash, "execution": execution}
    if execution["status"] != "completed" or not output.is_file():
        return {"status": "failed", "input_hash": input_hash, "execution": execution}

    replay = json.loads(output.read_text())
    if replay.get("fresh_holdout_outcomes_accessed") is not False:
        return {
            "status": "failed_outcome_boundary",
            "input_hash": input_hash,
            "artifact": str(output),
        }
    ledger.set_meta("opportunity_exact_replay.input_hash", input_hash)
    ledger.set_meta("opportunity_exact_replay.last_at", utc_now())
    return {
        "status": "completed",
        "input_hash": input_hash,
        "artifact": str(output),
        "verdict": replay.get("verdict"),
        "source_pmxt_scans": replay.get("source_pmxt_scans"),
        "duplicate_hour_scans_avoided": replay.get("duplicate_hour_scans_avoided"),
        "fresh_holdout_outcomes_accessed": False,
        "execution": execution,
    }


def run_cycle(config: Mapping[str, Any], dry_run: bool, selected_lane: Optional[str]) -> Dict[str, Any]:
    state_dir = resolve_repo_path(config["state_dir"])
    state_dir.mkdir(parents=True, exist_ok=True)
    with CycleLock(state_dir / "locks/cycle.lock"):
        ledger = Ledger(state_dir / "research.sqlite3", config.get("generator"))
        cycle_id = str(uuid.uuid4())
        result: Dict[str, Any] = {"cycle_id": cycle_id, "started_at": utc_now(), "dry_run": dry_run}
        ledger.begin_cycle(cycle_id, {"dry_run": dry_run, "selected_lane": selected_lane})
        try:
            resources = resource_status(config, state_dir)
            result["resources"] = resources
            result["registry_audit"] = run_registry_audit(config, state_dir, dry_run)
            active_config = copy.deepcopy(config)
            opportunity_mode = selected_lane in (None, "opportunity_policy_search") and bool(
                active_config.get("architecture_migration", {})
                .get("opportunity_policy_search", {})
                .get("enabled", False)
            )
            band_mode = selected_lane == "band_mechanisms"
            if resources["passed"] and not opportunity_mode and not band_mode:
                try:
                    public_snapshot = refresh_public_snapshot(
                        active_config, ledger, state_dir, dry_run
                    )
                except Exception as error:
                    seed = resolve_repo_path(
                        active_config["lanes"]["late_window_mechanisms"]["public_snapshot"]
                    )
                    public_snapshot = {
                        "status": "fallback_seed",
                        "path": str(seed),
                        "sha256": sha256_file(seed),
                        "error": "%s: %s" % (type(error).__name__, error),
                    }
                result["public_snapshot"] = public_snapshot
                active_config["lanes"]["late_window_mechanisms"]["public_snapshot"] = str(
                    public_snapshot["path"]
                )
                if not dry_run and public_snapshot["status"] != "fallback_seed":
                    result["reconciled_exact_jobs"] = reconcile_completed_exact_jobs(
                        active_config, ledger, Path(public_snapshot["path"])
                    )
                    result["pruned_exact_jobs"] = prune_queued_exact_jobs_without_fresh_capacity(
                        active_config, ledger, Path(public_snapshot["path"])
                    )
                    result["reconciled_fresh_holdout_jobs"] = (
                        reconcile_completed_fresh_holdout_jobs(
                            active_config, ledger, state_dir
                        )
                    )
                    result["reconciled_economic_statuses"] = (
                        reconcile_economic_screen_statuses(ledger)
                    )
                    result["reconciled_fresh_global_reserve"] = (
                        reconcile_fresh_holdout_global_reserve(
                            active_config, ledger, Path(public_snapshot["path"])
                        )
                    )
                    result["reconciled_fresh_window_granularity"] = (
                        reconcile_fresh_holdout_window_granularity(
                            active_config, ledger, Path(public_snapshot["path"])
                        )
                    )
                    result["recovered_holdout_jobs"] = recover_retryable_holdout_jobs(
                        active_config, ledger
                    )
                    result["fresh_public_accrual"] = run_fresh_public_accrual(
                        active_config, ledger, Path(public_snapshot["path"])
                    )
            if selected_lane:
                lane = selected_lane
            elif opportunity_mode:
                lane = "opportunity_policy_search"
            else:
                lane = ledger.meta("next_lane", "baseline_evolution") or "baseline_evolution"
            if lane not in (
                "baseline_evolution",
                "late_window_mechanisms",
                "opportunity_policy_search",
                "band_mechanisms",
            ):
                raise ValueError("unknown lane: %s" % lane)
            result["lane"] = lane
            if resources["passed"]:
                queued_replay_priority = not opportunity_mode and bool(
                    ledger.jobs("economic_opportunity_screen", "queued")
                    or ledger.jobs("fixed_forward_confirmation", "queued")
                    or ledger.jobs("fresh_resolved_holdout", "queued")
                    or ledger.jobs("exact_l2_replay", "queued")
                )
                if lane == "opportunity_policy_search":
                    result["lane_result"] = run_opportunity_policy_search(
                        active_config, ledger, state_dir, dry_run
                    )
                elif lane == "band_mechanisms":
                    result["lane_result"] = run_band_lane(
                        active_config, ledger, state_dir, dry_run
                    )
                elif queued_replay_priority:
                    result["lane_result"] = {
                        "status": "queued_replay_priority",
                        "reason": "finish preregistered replay work before proposing another hypothesis",
                    }
                elif lane == "baseline_evolution":
                    result["lane_result"] = run_baseline_evolution(
                        active_config, ledger, state_dir, dry_run
                    )
                    if not dry_run:
                        ledger.set_meta("next_lane", "late_window_mechanisms")
                else:
                    result["lane_result"] = run_late_window_lane(
                        active_config, ledger, state_dir, dry_run
                    )
                    if not dry_run:
                        ledger.set_meta("next_lane", "baseline_evolution")
                if not band_mode and result["lane_result"].get("status") in (
                    "not_due",
                    "blocked",
                    "duplicate",
                    "rejected_stage_1",
                    # A fresh survivor's economic screen is cheap and deterministic;
                    # running it now keeps the next lane turn free to propose instead
                    # of spending it on queued_replay_priority.
                    "stage_1_survivor",
                    "queued_replay_priority",
                    "paused_architecture_migration",
                ):
                    result["economic_screen_job"] = run_queued_economic_screen(
                        active_config, ledger, state_dir, dry_run
                    )
                    if result["economic_screen_job"].get("status") in (
                        "empty",
                        "disabled",
                        "blocked",
                        "rejected_economic_screen",
                        "rejected_exact_economics",
                        "economic_screen_passed",
                        "shortlist_saturated",
                    ):
                        result["fixed_forward_job"] = run_queued_fixed_forward_job(
                            active_config,
                            ledger,
                            state_dir,
                            Path(
                                active_config["lanes"]["late_window_mechanisms"][
                                    "public_snapshot"
                                ]
                            ),
                            dry_run,
                        )
                    if result.get("fixed_forward_job", {}).get("status") in (
                        "empty",
                        "disabled",
                        "blocked",
                        "awaiting_economic_screen",
                        "awaiting_new_data",
                    ):
                        result["fresh_holdout_job"] = run_queued_fresh_holdout_job(
                            active_config, ledger, state_dir, dry_run
                        )
                        if result["fresh_holdout_job"].get("status") in (
                            "empty",
                            "disabled",
                        ):
                            result["exact_job"] = run_queued_exact_job(
                                active_config, ledger, state_dir, dry_run
                            )
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
    parser.add_argument("--dry-run", action="store_true", help="do not run engine commands or write hypotheses")
    parser.add_argument(
        "--lane",
        choices=(
            "baseline_evolution",
            "late_window_mechanisms",
            "opportunity_policy_search",
            "band_mechanisms",
        ),
    )
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    config = load_config(args.config.resolve())
    if args.status:
        print(json.dumps(status(config), indent=2, sort_keys=True))
        return 0
    if not args.once:
        parser.error("--once is required; scheduling belongs to launchd/systemd")
    try:
        result = run_cycle(config, args.dry_run, args.lane)
    except RuntimeError as error:
        print(json.dumps({"status": "skipped", "reason": str(error)}, indent=2))
        return 0
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
