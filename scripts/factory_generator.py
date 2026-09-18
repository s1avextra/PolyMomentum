#!/usr/bin/env python3
"""Shared helpers of the band factory: the generator config block, the
trial-ledger appender (one row per (candidate, stage, look_id)) and the
structural (field-Hamming) novelty check against signal-level kills.

The LLM-era generator upgrades (EoH operators, killed-registry negative
prompting, embedding novelty, kill feedback, the sampler roster and the
burst queue) were deleted in Phase 3 of
docs/profitability_basement_2026-09-18.md.  Every entry point fails safe.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]

# The whole "generator" config block; strategy_research_loop.load_config
# refuses any other key.
DEFAULTS: Dict[str, Any] = {"trial_ledger_enabled": False}

# Enum-grid rules within this many field changes of a signal-level kill are
# not novel (measured 2026-09-02: cosine embeddings could not separate them,
# so the field Hamming distance is the gate).
NOVELTY_MIN_HAMMING = 2
# Only signal-level kills mark a neighbourhood dead.  Support and economics
# rejections say "not enough data" or "not executable at this price"; a
# one-field neighbour of those is a legitimate refine (2026-09-03).
NOVELTY_BLOCKING_PREFIXES = ("rejected_signal", "rejected_stage_1", "killed")


def _merged(block: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    merged = dict(DEFAULTS)
    if isinstance(block, Mapping):
        merged.update(block)
    return merged


def generator_config(loop_config: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    return _merged((loop_config or {}).get("generator"))


# --- structural novelty ------------------------------------------------------


def novelty_blocking_status(status: str) -> bool:
    return status.startswith(NOVELTY_BLOCKING_PREFIXES)


def structural_novelty(
    rule: Mapping[str, Any],
    killed_items: Sequence[Mapping[str, Any]],
    min_hamming: int = NOVELTY_MIN_HAMMING,
) -> Dict[str, Any]:
    """Reject a rule within `min_hamming` field changes of any killed rule.
    `killed_items` carry the killed rule's fields (`rule_fields`), its
    `status` and a display `rule`."""
    best: Optional[Mapping[str, Any]] = None
    best_distance: Optional[int] = None
    for item in killed_items:
        fields = item.get("rule_fields")
        if not isinstance(fields, Mapping):
            continue
        if not novelty_blocking_status(str(item.get("status", ""))):
            continue
        keys = set(rule) | set(fields)
        distance = sum(1 for key in keys if rule.get(key) != fields.get(key))
        if best_distance is None or distance < best_distance:
            best_distance, best = distance, item
    if best is not None and best_distance is not None and best_distance < int(min_hamming):
        return {
            "status": "rejected",
            "gate": "structural",
            "min_hamming": best_distance,
            "against": {"rule": best.get("rule"), "status": best.get("status")},
        }
    return {"status": "accepted", "gate": "structural", "min_hamming": best_distance}


# --- shared trial ledger -----------------------------------------------------


def look_id(candidate: str, stage: str, fresh_range: Sequence[int]) -> str:
    """One look = one (candidate, stage, window range) read of the labels."""
    payload = "%s|%s|%s" % (candidate, stage, json.dumps([int(value) for value in fresh_range]))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def append_trial_entry(
    loop_config: Mapping[str, Any],
    candidate: str,
    stage: str,
    verdict: str,
    n: Optional[int] = None,
    wins: Optional[int] = None,
    fresh_range: Optional[Sequence[int]] = None,
) -> bool:
    """One line per screen-stage verdict, shape-compatible with
    scripts/fresh_gate_public_v1.py's trial ledger records.  With a
    fresh_range the row carries look_id = sha256(candidate|stage|range) and
    a row with that look_id already in the ledger is not written again (one
    ledger row per (fingerprint, look_id)); returns False for the skip."""
    try:
        if not generator_config(loop_config)["trial_ledger_enabled"]:
            return False
        state_dir = Path(str(loop_config["state_dir"]))
        if not state_dir.is_absolute():
            state_dir = ROOT / state_dir
        record: Dict[str, Any] = {
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "source": "research_loop",
            "candidate": str(candidate),
            "stage": str(stage),
        }
        if n is not None:
            record["n"] = int(n)
        if wins is not None:
            record["wins"] = int(wins)
        record["verdict"] = str(verdict)
        path = state_dir / "trial_ledger.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        if fresh_range is not None:
            record["fresh_range"] = [int(value) for value in fresh_range]
            record["look_id"] = look_id(candidate, stage, fresh_range)
            marker = '"look_id": "%s"' % record["look_id"]
            if path.is_file():
                with path.open(encoding="utf-8") as handle:
                    if any(marker in line for line in handle):
                        return False
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
        return True
    except Exception:
        return False
