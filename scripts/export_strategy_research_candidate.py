#!/usr/bin/env python3
"""Freeze one research-loop candidate into durable repository evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f"{destination.name}.tmp.{os.getpid()}")
    shutil.copyfile(source, temporary)
    os.replace(temporary, destination)


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def repository_path(path: Path) -> str:
    return str(path.resolve().relative_to(ROOT))


def export_candidate(
    fingerprint: str, state_dir: Path, output_dir: Path, date_tag: str
) -> dict[str, Any]:
    candidate_dir = state_dir / "candidates/late_window_mechanisms" / fingerprint
    source_variant = candidate_dir / "variant.json"
    source_holdout = state_dir / "evidence/eligible" / f"{fingerprint}.json"
    source_public = (
        state_dir / "evidence/late_window_mechanisms" / f"{fingerprint}.json"
    )
    source_economic = state_dir / "evidence/economic" / f"{fingerprint}.json"
    source_forward = state_dir / "evidence/fixed-forward" / f"{fingerprint}.json"
    for source in (
        source_variant,
        source_holdout,
        source_public,
        source_economic,
        source_forward,
    ):
        if not source.is_file():
            raise FileNotFoundError(source)

    holdout = json.loads(source_holdout.read_text())
    public = json.loads(source_public.read_text())
    economic = json.loads(source_economic.read_text())
    forward = json.loads(source_forward.read_text())
    if holdout.get("hypothesis_fingerprint") != fingerprint:
        raise ValueError("holdout evidence fingerprint mismatch")
    if economic.get("hypothesis_fingerprint") != fingerprint:
        raise ValueError("economic evidence fingerprint mismatch")
    if forward.get("hypothesis_fingerprint") != fingerprint:
        raise ValueError("fixed-forward evidence fingerprint mismatch")

    stem = f"{date_tag}_late_window_path_or_move_{fingerprint[:12]}"
    destinations = {
        "variant": output_dir / f"{stem}_variant.json",
        "holdout": output_dir / f"{stem}_fresh_holdout.json",
        "public": output_dir / f"{stem}_public_screen.json",
        "economic": output_dir / f"{stem}_economic_screen.json",
        "forward": output_dir / f"{stem}_fixed_forward_gate.json",
        "manifest": output_dir / f"{stem}_research_manifest.json",
    }
    atomic_copy(source_variant, destinations["variant"])
    atomic_copy(source_holdout, destinations["holdout"])
    atomic_copy(source_public, destinations["public"])
    atomic_copy(source_economic, destinations["economic"])
    atomic_copy(source_forward, destinations["forward"])

    completed = holdout.get("completed_windows", [])
    public_signals = sum(int(item.get("public_signals") or 0) for item in completed)
    summaries = [item.get("summary") or {} for item in completed]
    attempts = sum(int(item.get("execution_attempts") or 0) for item in summaries)
    fills = sum(int(item.get("fills_success") or 0) for item in summaries)
    active_windows = sum(int(item.get("trades") or 0) > 0 for item in summaries)
    total_pnl = sum(float(item.get("total_pnl") or 0.0) for item in summaries)
    maximum_stake = float(
        holdout.get("verdict", {}).get("aggregate", {}).get("maximum_stake_usd")
        or 0.0
    )
    mean_win = total_pnl / fills if fills else 0.0
    break_even_accuracy = (
        maximum_stake / (maximum_stake + mean_win)
        if maximum_stake > 0.0 and mean_win > 0.0
        else None
    )
    public_overall = public.get("overall", {})
    wilson_lower = public_overall.get("wilson_95_lower")
    zero_loss_fills = (
        math.ceil(math.log(0.05) / math.log(break_even_accuracy))
        if break_even_accuracy is not None and 0.0 < break_even_accuracy < 1.0
        else None
    )
    manifest = {
        "schema_version": 1,
        "generated_at": f"{date_tag[:4]}-{date_tag[4:6]}-{date_tag[6:]}T00:00:00Z",
        "status": "rejected",
        "research_only": True,
        "live_ready": False,
        "hypothesis_fingerprint": fingerprint,
        "frozen_rule": holdout["proposal"]["rule"],
        "artifacts": {
            key: {
                "path": repository_path(path),
                "sha256": sha256_file(path),
            }
            for key, path in destinations.items()
            if key != "manifest"
        },
        "coverage": {
            "windows": len(summaries),
            "active_windows": active_windows,
            "active_window_fraction": active_windows / len(summaries)
            if summaries
            else 0.0,
            "public_signals": public_signals,
            "execution_attempts": attempts,
            "signal_to_attempt_rate": attempts / public_signals
            if public_signals
            else 0.0,
            "fills": fills,
        },
        "asymmetric_payoff_risk": {
            "maximum_stake_usd": maximum_stake,
            "observed_total_net_pnl_usd": total_pnl,
            "observed_mean_net_win_usd": mean_win,
            "raw_one_maximum_stake_loss_robustness": total_pnl - maximum_stake
            > 0.0,
            "wins_to_recover_one_maximum_stake_loss": math.ceil(
                maximum_stake / mean_win
            )
            if mean_win > 0.0
            else None,
            "implied_break_even_accuracy": break_even_accuracy,
            "public_accuracy": public_overall.get("accuracy"),
            "public_wilson_95_lower": wilson_lower,
            "wilson_margin_over_break_even": wilson_lower - break_even_accuracy
            if wilson_lower is not None and break_even_accuracy is not None
            else None,
            "minimum_all_win_forward_fills_for_one_sided_95pct_lower_bound": zero_loss_fills,
        },
        "decision": {
            "research_eligibility_only": True,
            "economic_screen_passed": bool(
                economic.get("verdict", {}).get("passed", False)
            ),
            "economic_screen_checks": economic.get("verdict", {}).get("checks", {}),
            "fixed_forward_confirmation_required": True,
            "fixed_forward_status": forward.get("status"),
            "official_resolution_parity_required": True,
            "paper_or_live_authorized": False,
            "reason": "Rejected by the fee-aware economic gate: the mean net win is too small, one maximum-stake loss requires 255 average wins to recover, and the public Wilson lower bound is below break-even.",
        },
    }
    atomic_json(destinations["manifest"], manifest)
    manifest["manifest"] = {
        "path": repository_path(destinations["manifest"]),
        "sha256": sha256_file(destinations["manifest"]),
    }
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fingerprint", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "deploy/promotions/evidence/strategy_registry",
    )
    parser.add_argument("--date-tag", default="20260803")
    args = parser.parse_args()
    result = export_candidate(
        args.fingerprint, args.state_dir, args.output_dir, args.date_tag
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
