#!/usr/bin/env python3
"""Sweep models x temperatures in the SAMPLER role the factory actually needs.

Uses the research loop's real proposal schema, validator, and rule
canonicalizer, so results transfer directly. Metrics per cell:
  valid_rate        - schema-valid AND mechanically coherent
  unique_valid      - distinct canonical rules among valid samples
  mean_pair_cosine  - embedding similarity among valid samples (lower = more diverse)
  sec_per_sample    - wall clock
  uvpm              - unique valid proposals per minute (the sampler KPI)

Usage: uv run python scripts/sampler_sweep.py [--samples 6]
"""

import argparse
import importlib.util
import itertools
import json
import math
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODELS = [
    "openai/gpt-oss-20b",
    "qwen3.5-9b-claude-4.6-opus-reasoning-distilled-v2",
    "google/gemma-4-12b-qat",
    "google/gemma-4-26b-a4b",
]
TEMPS = [0.4, 0.8, 1.2]
EMBED_MODEL = "text-embedding-nomic-embed-text-v1.5"


def load_loop():
    spec = importlib.util.spec_from_file_location(
        "strategy_research_loop", ROOT / "scripts/strategy_research_loop.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def embed(base_url, text):
    payload = json.dumps({"model": EMBED_MODEL, "input": text}).encode()
    req = urllib.request.Request(
        f"{base_url}/embeddings", data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())["data"][0]["embedding"]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    return dot / (na * nb) if na and nb else 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", type=int, default=6)
    ap.add_argument("--state-dir", default="/tmp/sampler_sweep_state")
    args = ap.parse_args()

    loop = load_loop()
    config = json.loads((ROOT / "deploy/strategy-research-loop.json").read_text())
    base_url = config["llm"].get("default_base_url", "http://127.0.0.1:1234/v1")
    state = Path(args.state_dir)
    state.mkdir(parents=True, exist_ok=True)

    system = (
        "You propose one bounded causal rule from a public Bitcoin five-minute continuation study. "
        "Return only the strict JSON object. Do not request code, files, commands, private data, outcomes, scores, or economics."
    )
    user = (
        "Sample ONE rule as a diverse draw from the whole executable grid - prefer regions a uniform sampler "
        "would rarely hit. Available checkpoints are 60, 120, 180 and 240 seconds; settlement is after the "
        "decision. path_minutes must be 0, 2, 3 or 4. The two-minute move threshold must be one of 0,100,200 USD. "
        "path_only uses path 3/4 and threshold 0; move_only uses path 0 and threshold >=100; AND rules use path "
        "2/3/4 and threshold >=100; OR is executable only with path 4 and threshold 200. maximum_entry_price must "
        "be one of 0.75,0.85,0.90,0.95,1.0. minimum_decision_buffer_usd must be 0,100,125,200. "
        "settlement_sigma_buffer must be 0.0,0.1,0.2. direction must be both,up,down."
    )

    results = []
    for model in MODELS:
        llm_cfg = dict(config["llm"])
        llm_cfg["default_model"] = model
        client = loop.LmStudioClient(llm_cfg, state)
        # Warm up (JIT load) before timing anything.
        ready = False
        for _ in range(6):
            probe = client.complete(system, "Return any single valid rule.", "late_window_proposal_v1", loop.LATE_PROPOSAL_SCHEMA, 0.0)
            if probe.get("ok"):
                ready = True
                break
            time.sleep(20)
        if not ready:
            results.append({"model": model, "error": "warmup failed"})
            continue
        for temp in TEMPS:
            valid = []
            t0 = time.monotonic()
            for _ in range(args.samples):
                out = client.complete(system, user, "late_window_proposal_v1", loop.LATE_PROPOSAL_SCHEMA, temp)
                if not out.get("ok"):
                    continue
                try:
                    proposal = loop.validate_late_proposal(out["value"])
                except (ValueError, KeyError, TypeError):
                    continue
                valid.append(loop.normalized_late_rule(proposal["rule"]))
            wall = time.monotonic() - t0
            unique = {json.dumps(r, sort_keys=True) for r in valid}
            cos = None
            if len(unique) >= 2:
                try:
                    vecs = [embed(base_url, u) for u in itertools.islice(unique, 8)]
                    pairs = [
                        cosine(vecs[i], vecs[j])
                        for i in range(len(vecs))
                        for j in range(i + 1, len(vecs))
                    ]
                    cos = sum(pairs) / len(pairs)
                except Exception:
                    cos = None
            cell = {
                "model": model,
                "temp": temp,
                "samples": args.samples,
                "valid": len(valid),
                "unique_valid": len(unique),
                "mean_pair_cosine": round(cos, 4) if cos is not None else None,
                "sec_per_sample": round(wall / args.samples, 1),
                "uvpm": round(len(unique) / (wall / 60.0), 2) if wall > 0 else 0.0,
            }
            results.append(cell)
            print(json.dumps(cell), flush=True)

    print("\n=== SAMPLER SWEEP SUMMARY (sorted by uvpm) ===")
    for cell in sorted(
        (c for c in results if "error" not in c), key=lambda c: -c["uvpm"]
    ):
        print(
            f"{cell['model'][:44]:44} T={cell['temp']:.1f}  valid {cell['valid']}/{cell['samples']}"
            f"  unique {cell['unique_valid']}  cos {cell['mean_pair_cosine']}  {cell['sec_per_sample']}s/spl  uvpm={cell['uvpm']}"
        )
    for cell in results:
        if "error" in cell:
            print(f"{cell['model']}: {cell['error']}")


if __name__ == "__main__":
    main()
