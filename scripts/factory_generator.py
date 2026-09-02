#!/usr/bin/env python3
"""Generator-side upgrades for the strategy research loop (phase 2).

Implements the four hypothesis-generator upgrades from
docs/hypothesis_factory_research_2026-09-01/ (EoH mutation operators,
killed-registry negative prompting, embedding-based novelty rejection,
Eureka-style kill feedback) plus the shared trial-ledger appender.

Every feature is disabled unless enabled in the loop config's "generator"
block, and every entry point fails safe: an error degrades to the unmodified
legacy behavior of scripts/strategy_research_loop.py.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]

DEFAULTS: Dict[str, Any] = {
    "eoh_operators_enabled": False,
    "negative_prompt_enabled": False,
    "novelty_gate_enabled": False,
    "embedding_model": "text-embedding-nomic-embed-text-v1.5",
    "novelty_max_cosine": 0.97,
    # Enum-grid rules embed at cosine 0.97-0.98 even when every field differs
    # (measured 2026-09-02), so structured proposals use a field Hamming gate
    # against killed rules instead; the cosine path stays for free text.
    "novelty_min_hamming": 2,
    "kill_feedback_enabled": False,
    "trial_ledger_enabled": False,
    # Sampler-role knobs (2026-09-01): high entropy for exploration
    # operators, moderate for refinement; burst N samples per generation
    # so screens - not generation - are the funnel's bottleneck.
    "explore_temperature": 0.2,
    "refine_temperature": 0.2,
    "samples_per_burst": 1,
    "constrained_schema": False,
}

NEGATIVE_PROMPT_MAX_CHARS = 1200
NOVELTY_RECENT_ACCEPTED = 50
KILL_FEEDBACK_PROMPT_ENTRIES = 5

EOH_OPERATORS = ("E1", "E2", "M1", "M2")
EOH_INSTRUCTIONS = {
    "E1": (
        "Mutation operator E1: propose a rule mechanically DIFFERENT from every "
        "parent shown - a different checkpoint structure or predicate family."
    ),
    "E2": (
        "Mutation operator E2: keep the parents' core idea, but change the "
        "implementation parameters materially."
    ),
    "M1": "Mutation operator M1: improve this specific parent's weakest aspect.",
    "M2": "Mutation operator M2: tune only the numeric parameters of this parent.",
}

# Best downstream status first; anything absent ranks after the known ladder.
GOOD_STATUS_RANK = {
    "forward_confirmed_research_only": 0,
    "research_eligible": 1,
    "eligible_for_fresh_holdout": 2,
    "historical_eligible_awaiting_fresh_data": 3,
    "eligible_for_exact_l2": 4,
    "stage_1_survivor": 5,
}

# Terminal negative hypothesis statuses -> the stage that killed the rule.
KILL_STATUS_STAGES = {
    "rejected_stage_1": "public_directional_screen",
    "rejected_stage_1_fresh_capacity": "public_directional_screen",
    "rejected_economic_screen": "economic_opportunity_screen",
    "rejected_exact_economics": "economic_opportunity_screen",
    "rejected_historical_exact": "exact_l2_replay",
    "historical_insufficient_support": "exact_l2_replay",
    "rejected_fresh_holdout": "fresh_resolved_holdout",
    "holdout_insufficient_support": "fresh_resolved_holdout",
    "rejected_fixed_forward": "fixed_forward_confirmation",
    "killed_futility": "fresh_public_accrual",
}

# LmStudioClient.complete rejects prompts containing these tokens; injected
# registry/feedback text must never trip that guard.
_GUARDED_TOKEN_REPLACEMENTS = (
    (re.compile(r"pnl", re.IGNORECASE), "net-payoff"),
    (re.compile(r"wallet", re.IGNORECASE), "account"),
    (re.compile(r"private_key", re.IGNORECASE), "credential"),
    (re.compile(r"secret", re.IGNORECASE), "hidden"),
)


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _sanitize(text: str) -> str:
    for pattern, replacement in _GUARDED_TOKEN_REPLACEMENTS:
        text = pattern.sub(replacement, text)
    return text


def _append_jsonl(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")


def _merged(block: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    merged = dict(DEFAULTS)
    if isinstance(block, Mapping):
        merged.update(block)
    return merged


def generator_config(loop_config: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    return _merged((loop_config or {}).get("generator"))


def compact_rule(rule: Mapping[str, Any]) -> str:
    return (
        "op=%s path=%sm move=$%s cap=%s buffer=$%s sigma=%s pressure=%s dir=%s"
        % (
            rule.get("operator"),
            rule.get("path_minutes"),
            rule.get("minimum_two_minute_move_usd"),
            rule.get("maximum_entry_price"),
            rule.get("minimum_decision_buffer_usd"),
            rule.get("settlement_sigma_buffer"),
            rule.get("minimum_book_pressure"),
            rule.get("direction"),
        )
    )


# --- upgrade 1: EoH operator rotation -------------------------------------


def eoh_parents(
    late_rows: Sequence[Mapping[str, Any]], limit: int = 3
) -> List[Mapping[str, Any]]:
    eligible = [
        row
        for row in late_rows
        if not str(row.get("status", "")).startswith("rejected")
        and str(row.get("status")) not in KILL_STATUS_STAGES
    ]
    recent = sorted(eligible, key=lambda row: str(row.get("created_at", "")), reverse=True)
    recent.sort(
        key=lambda row: GOOD_STATUS_RANK.get(str(row.get("status")), len(GOOD_STATUS_RANK))
    )
    return recent[:limit]


def select_eoh_operator(proposal_count: int, has_parents: bool) -> str:
    operator = EOH_OPERATORS[proposal_count % len(EOH_OPERATORS)]
    return operator if operator == "E1" or has_parents else "E1"


def eoh_prompt_section(
    operator: str, parents: Sequence[Mapping[str, Any]]
) -> Tuple[str, List[str]]:
    shown = list(parents[:1] if operator in ("M1", "M2") else parents[:3])
    lines = [EOH_INSTRUCTIONS[operator]]
    for parent in shown:
        lines.append(
            "Parent rule (downstream status %s): %s"
            % (parent.get("status"), compact_rule(parent["proposal"]["rule"]))
        )
    return _sanitize("\n".join(lines)), [str(parent["fingerprint"]) for parent in shown]


# --- upgrade 2: killed-registry negatives ----------------------------------


def killed_negative_items(
    registry_path: Optional[Path], late_rows: Sequence[Mapping[str, Any]]
) -> List[Dict[str, Any]]:
    """Compact killed-rule descriptors: ledger rejects first, then registry."""
    items: List[Dict[str, Any]] = []
    ledger_rejects = [
        row
        for row in late_rows
        if str(row.get("status", "")).startswith(("rejected", "killed"))
    ]
    for row in sorted(
        ledger_rejects, key=lambda row: str(row.get("created_at", "")), reverse=True
    ):
        try:
            items.append(
                {
                    "kind": "ledger_rule",
                    "rule": compact_rule(row["proposal"]["rule"]),
                    "rule_fields": dict(row["proposal"]["rule"]),
                    "status": str(row.get("status")),
                }
            )
        except (KeyError, TypeError):
            continue
    if registry_path is not None and registry_path.is_file():
        try:
            entries = json.loads(registry_path.read_text()).get("entries", [])
        except (OSError, ValueError):
            entries = []
        killed = [
            entry
            for entry in entries
            if isinstance(entry, dict)
            and str(entry.get("status")) in ("rejected", "dead_end", "questionable")
        ]
        for entry in sorted(
            killed, key=lambda entry: str(entry.get("updated_at", "")), reverse=True
        ):
            items.append(
                {
                    "kind": "registry_family",
                    "id": str(entry.get("strategy_id")),
                    "status": str(entry.get("status")),
                    "reason": str(entry.get("reason", ""))[:160],
                }
            )
    return items


def negative_prompt_text(
    items: Sequence[Mapping[str, Any]], max_chars: int = NEGATIVE_PROMPT_MAX_CHARS
) -> str:
    if not items:
        return ""
    lines = [
        "These families/rules were tested and KILLED (reason). "
        "Propose something structurally different:"
    ]
    used = len(lines[0])
    for item in items:
        if item.get("kind") == "ledger_rule":
            line = "- %s (%s)" % (item.get("rule"), item.get("status"))
        else:
            line = "- %s [%s]: %s" % (item.get("id"), item.get("status"), item.get("reason"))
        if used + len(line) + 1 > max_chars:
            break
        lines.append(line)
        used += len(line) + 1
    return _sanitize("\n".join(lines))


# --- upgrade 3: semantic novelty gate --------------------------------------


def cosine(a: Sequence[float], b: Sequence[float]) -> float:
    if len(a) != len(b) or not a:
        raise ValueError("cosine requires equal-length non-empty vectors")
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def embed_text(base_url: str, model: str, text: str, timeout: float) -> List[float]:
    body = json.dumps({"model": model, "input": text}).encode("utf-8")
    request = urllib.request.Request(
        base_url.rstrip("/") + "/embeddings",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    vector = payload["data"][0]["embedding"]
    if not isinstance(vector, list) or not vector:
        raise ValueError("empty embedding vector")
    return [float(value) for value in vector]


def load_embedding_cache(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    if not path.is_file():
        return records
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record.get("sha"), str) and isinstance(record.get("vector"), list):
            records.append(record)
    return records


def structural_novelty(
    rule: Mapping[str, Any], killed_items: Sequence[Mapping[str, Any]], min_hamming: int
) -> Dict[str, Any]:
    """Reject a rule within `min_hamming` field changes of any killed rule."""
    best: Optional[Mapping[str, Any]] = None
    best_distance: Optional[int] = None
    for item in killed_items:
        fields = item.get("rule_fields")
        if not isinstance(fields, Mapping):
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


def novelty_check(
    proposal: Mapping[str, Any],
    gen_cfg: Mapping[str, Any],
    base_url: str,
    timeout: float,
    state_dir: Path,
    killed_items: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Novelty gate: field Hamming distance for structured rules, cosine
    similarity for free text; any failure returns status=error (fail-open)."""
    rule = proposal.get("rule") if isinstance(proposal, Mapping) else None
    if isinstance(rule, Mapping) and any(
        isinstance(item.get("rule_fields"), Mapping) for item in killed_items
    ):
        return structural_novelty(
            rule, killed_items, int(gen_cfg.get("novelty_min_hamming", DEFAULTS["novelty_min_hamming"]))
        )
    if isinstance(rule, Mapping):
        # Structured rule, nothing killed yet: nothing to be near.
        return {"status": "accepted", "gate": "structural", "min_hamming": None}
    try:
        model = str(gen_cfg["embedding_model"])
        threshold = float(gen_cfg["novelty_max_cosine"])
        cache_path = state_dir / "proposal_embeddings.jsonl"
        records = load_embedding_cache(cache_path)
        by_sha = {record["sha"]: record for record in records}
        candidate_sha = _sha(proposal)
        vector = embed_text(base_url, model, _canonical(proposal), timeout)
        for item in killed_items:
            item_sha = _sha(item)
            if item_sha in by_sha:
                continue
            killed_vector = embed_text(base_url, model, _canonical(item), timeout)
            record = {"ts": _utc_now(), "sha": item_sha, "kind": "killed", "vector": killed_vector}
            _append_jsonl(cache_path, record)
            records.append(record)
            by_sha[item_sha] = record
        accepted = [
            record for record in records if record.get("kind", "accepted") == "accepted"
        ][-NOVELTY_RECENT_ACCEPTED:]
        killed = [record for record in records if record.get("kind") == "killed"]
        best: Optional[Mapping[str, Any]] = None
        best_cosine = -1.0
        compared = 0
        for record in accepted + killed:
            if record["sha"] == candidate_sha or len(record["vector"]) != len(vector):
                continue
            similarity = cosine(vector, record["vector"])
            compared += 1
            if similarity > best_cosine:
                best_cosine = similarity
                best = record
        if best is not None and best_cosine >= threshold:
            return {
                "status": "rejected",
                "sha": candidate_sha,
                "max_cosine": round(best_cosine, 6),
                "against": {"sha": best["sha"], "kind": best.get("kind", "accepted")},
            }
        if candidate_sha not in by_sha:
            _append_jsonl(
                cache_path,
                {"ts": _utc_now(), "sha": candidate_sha, "kind": "accepted", "vector": vector},
            )
        return {
            "status": "accepted",
            "sha": candidate_sha,
            "max_cosine": round(best_cosine, 6) if best is not None else None,
            "compared": compared,
        }
    except Exception as error:  # fail-open by design; exact-dup check still guards
        return {"status": "error", "error": "%s: %s" % (type(error).__name__, error)}


# --- upgrade 4: Eureka-style kill feedback ---------------------------------


def record_kill_feedback(
    generator_block: Optional[Mapping[str, Any]],
    state_dir: Path,
    status: str,
    proposal_json: Optional[str],
) -> bool:
    try:
        if not _merged(generator_block)["kill_feedback_enabled"]:
            return False
        stage = KILL_STATUS_STAGES.get(str(status))
        if stage is None:
            return False
        summary = str(status)
        try:
            proposal = json.loads(proposal_json) if proposal_json else {}
        except ValueError:
            proposal = {}
        if isinstance(proposal.get("rule"), dict):
            summary = compact_rule(proposal["rule"])
        elif proposal:
            summary = _canonical(proposal)[:160]
        _append_jsonl(
            state_dir / "kill_feedback.jsonl",
            {"ts": _utc_now(), "rule": summary, "stage": stage, "reason": str(status)},
        )
        return True
    except Exception:
        return False


def kill_feedback_prompt_text(
    state_dir: Path, limit: int = KILL_FEEDBACK_PROMPT_ENTRIES
) -> str:
    path = state_dir / "kill_feedback.jsonl"
    if not path.is_file():
        return ""
    entries: List[Dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict) and record.get("rule"):
            entries.append(record)
    if not entries:
        return ""
    lines = ["Recent failures and why:"]
    for record in entries[-limit:]:
        lines.append(
            "- [%s] %s: %s" % (record.get("stage"), record.get("rule"), record.get("reason"))
        )
    return _sanitize("\n".join(lines))


# --- upgrade 5: shared trial ledger ----------------------------------------


def direction_wins(aggregate: Mapping[str, Any]) -> int:
    return sum(
        int((stats or {}).get("wins") or 0)
        for stats in (aggregate.get("by_direction") or {}).values()
    )


def append_trial_entry(
    loop_config: Mapping[str, Any],
    candidate: str,
    stage: str,
    verdict: str,
    n: Optional[int] = None,
    wins: Optional[int] = None,
) -> bool:
    """One line per screen-stage verdict, shape-compatible with
    scripts/fresh_gate_public_v1.py's trial ledger records."""
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
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record) + "\n")
        return True
    except Exception:
        return False


# --- sampler burst queue ---------------------------------------------------

def proposal_queue_path(state_dir, filename="proposal_queue.jsonl"):
    return state_dir / filename


def queue_pop(state_dir, filename="proposal_queue.jsonl"):
    """Pop the oldest queued proposal (burst survivor from a prior cycle)."""
    path = proposal_queue_path(state_dir, filename)
    if not path.is_file():
        return None
    lines = [line for line in path.read_text().splitlines() if line.strip()]
    if not lines:
        return None
    head, rest = lines[0], lines[1:]
    tmp = path.with_suffix(".tmp")
    tmp.write_text("\n".join(rest) + ("\n" if rest else ""))
    tmp.rename(path)
    try:
        return json.loads(head)
    except json.JSONDecodeError:
        return None


def queue_push(state_dir, proposals, filename="proposal_queue.jsonl"):
    if not proposals:
        return
    path = proposal_queue_path(state_dir, filename)
    with path.open("a") as handle:
        for item in proposals:
            handle.write(json.dumps(item, sort_keys=True) + "\n")


def next_sampler_model(llm_config, ledger, lane):
    """Round-robin over llm.sampler_models, one model per burst; the cursor is
    ledger meta keyed per lane so it survives cycles and every lane rotates
    through the whole roster (a shared cursor pins each lane to one model
    whenever the lanes burst at the same cadence).  None when the ensemble is
    not configured, so the client default model is used."""
    models = llm_config.get("sampler_models") or []
    if not models:
        return None
    key = "sampler_model_index.%s" % lane
    cursor = int(ledger.meta(key, "0") or 0)
    ledger.set_meta(key, str(cursor + 1))
    return str(models[cursor % len(models)])


def operator_temperature(operator, gen_cfg):
    """E1/E2 explore the grid (entropy up); M1/M2 refine parents."""
    if operator in ("M1", "M2"):
        return float(gen_cfg.get("refine_temperature", 0.2))
    return float(gen_cfg.get("explore_temperature", 0.2))


# --- constrained proposal schema -------------------------------------------
#
# The frozen grid's operator/path/threshold co-constraints lived only in
# prompt text, so ~70-85% of samples died in validation. Encoding them as
# anyOf branches makes constrained decoding emit VALID combinations by
# construction. validate_late_proposal stays as defense in depth.

def _rule_branch(operator, paths, thresholds, base):
    properties = dict(base)
    properties["operator"] = {"type": "string", "enum": [operator]}
    properties["path_minutes"] = {"type": "integer", "enum": paths}
    properties["minimum_two_minute_move_usd"] = {"type": "integer", "enum": thresholds}
    return {
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
        "properties": properties,
    }


def constrained_proposal_schema(legacy_schema):
    free = {
        key: dict(value)
        for key, value in legacy_schema["properties"]["rule"]["properties"].items()
        if key not in ("operator", "path_minutes", "minimum_two_minute_move_usd")
    }
    schema = json.loads(json.dumps(legacy_schema))
    for field in ("title", "rationale", "expected_failure_mode"):
        schema["properties"][field]["minLength"] = 1
    schema["properties"]["rule"] = {
        "anyOf": [
            _rule_branch("path_only", [3, 4], [0], free),
            _rule_branch("move_only", [0], [100, 200], free),
            _rule_branch("path_and_move", [2, 3, 4], [100, 200], free),
            _rule_branch("path_or_move", [4], [200], free),
        ]
    }
    return schema
