#!/usr/bin/env python3
"""Mini-benchmark of LM Studio models for the hypothesis factory.

Measures, per model: decode speed (tok/s), time-to-first-token, and a
factory-relevant quality suite scored mechanically (no LLM judges):

  A. strict-JSON hypothesis fill — schema validity + parameter sanity
  B. constraint following — numeric bounds actually respected
  C. quant sanity — questions with known answers (break-even, EV, Wilson)
  D. novelty — avoids mechanisms explicitly listed as dead

Works against a local OR LM-Link-routed endpoint (remote models appear on
localhost when both machines share an LM Link network).

Usage:
  uv run python scripts/llm_bench.py                       # all loaded models
  uv run python scripts/llm_bench.py --models a,b --runs 2
  uv run python scripts/llm_bench.py --base-url http://127.0.0.1:1234
"""

import argparse
import json
import time
import urllib.request

DEFAULT_BASE = "http://127.0.0.1:1234"

HYPOTHESIS_SCHEMA = {
    "name": "hypothesis",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["family", "ask_floor", "ask_cap", "decision_seconds", "rationale"],
        "properties": {
            "family": {"type": "string", "enum": ["favorite_band", "underdog_band", "late_momentum"]},
            "ask_floor": {"type": "number"},
            "ask_cap": {"type": "number"},
            "decision_seconds": {"type": "number"},
            "rationale": {"type": "string"},
        },
    },
}

# (prompt, mechanical check) pairs. Checks return True on pass.
def _check_fill(obj):
    return (
        0.0 < obj["ask_floor"] < obj["ask_cap"] <= 1.0
        and 0 < obj["decision_seconds"] <= 300
        and len(obj["rationale"]) > 20
    )

def _check_bounds(obj):
    return 0.60 <= obj["ask_floor"] <= 0.70 and 200 <= obj["decision_seconds"] <= 250

def _check_novelty(obj):
    return obj["family"] != "favorite_band"

QUALITY_TASKS = [
    (
        "fill",
        "Propose one candidate strategy for Polymarket btc-updown-5m candle markets. "
        "A 5-minute window resolves up/down vs its open; entries are taker FOK buys. "
        "Fill the schema with internally consistent parameters.",
        _check_fill,
    ),
    (
        "bounds",
        "Propose a candidate with ask_floor between 0.60 and 0.70 inclusive and "
        "decision_seconds between 200 and 250 inclusive. Respect these bounds exactly.",
        _check_bounds,
    ),
    (
        "novelty",
        "These families were already tested and KILLED: favorite_band (all variants). "
        "Propose a candidate from a family that is NOT killed.",
        _check_novelty,
    ),
]

QUANT_TASKS = [
    # (question, exact expected answer as string)
    ("A share bought at 0.80 pays 1.00 if it wins, 0 otherwise, no fees. "
     "What win probability makes this break even? Answer with the number only.", "0.8"),
    ("You buy 10 shares at 0.60. The position wins. "
     "What is the profit in dollars? Answer with the number only.", "4"),
    ("Strategy A wins 90 of 100. Strategy B wins 45 of 50. "
     "Which has the smaller Wilson 95% lower bound distance to its point estimate, A or B? "
     "Answer with the single letter only.", "a"),
]


def api(base, path, payload=None, timeout=180):
    url = f"{base}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def chat(base, model, prompt, schema=None, max_tokens=700, retries=3):
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": max_tokens,
        "temperature": 0.7,
    }
    if schema:
        payload["response_format"] = {"type": "json_schema", "json_schema": schema}
    last = None
    for attempt in range(retries):
        t0 = time.monotonic()
        try:
            resp = api(base, "/v1/chat/completions", payload)
            break
        except Exception as e:  # JIT model switching yields transient 400s
            last = e
            time.sleep(25)
    else:
        raise last
    dt = time.monotonic() - t0
    text = resp["choices"][0]["message"]["content"]
    usage = resp.get("usage", {})
    stats = resp.get("stats", {})  # LM Studio extension when available
    return {
        "text": text,
        "wall_s": dt,
        "completion_tokens": usage.get("completion_tokens", 0),
        "tps": stats.get("tokens_per_second")
        or (usage.get("completion_tokens", 0) / dt if dt > 0 else 0.0),
        "ttft": stats.get("time_to_first_token"),
    }


def bench_model(base, model, runs):
    out = {"model": model, "speed": {}, "quality": {}, "errors": []}

    # Warm up: force the model to load before anything is scored.
    try:
        chat(base, model, "Say OK.", max_tokens=8, retries=5)
    except Exception as e:
        out["errors"].append(f"warmup: {e}")
        return out

    # Speed: one long deterministic-ish generation
    try:
        r = chat(
            base,
            model,
            "Write a detailed 400-word factual explanation of how order books match trades.",
            max_tokens=600,
        )
        out["speed"] = {
            "decode_tps": round(r["tps"], 1),
            "wall_s": round(r["wall_s"], 1),
            "ttft_s": round(r["ttft"], 2) if r["ttft"] else None,
        }
    except Exception as e:
        out["errors"].append(f"speed: {e}")

    # Quality A/B/D: schema tasks, `runs` attempts each, scored mechanically
    for name, prompt, check in QUALITY_TASKS:
        passed = 0
        for _ in range(runs):
            try:
                r = chat(base, model, prompt, schema=HYPOTHESIS_SCHEMA)
                obj = json.loads(r["text"])
                if check(obj):
                    passed += 1
            except Exception:
                pass
        out["quality"][name] = f"{passed}/{runs}"

    # Quality C: quant sanity, exact-answer match
    passed = 0
    for q, expected in QUANT_TASKS:
        try:
            r = chat(base, model, q, max_tokens=200)
            answer = r["text"].strip().lower().rstrip(".")
            norm = answer.replace("$", "").replace(",", "").split()[-1] if answer else ""
            ok = norm.startswith(expected) or expected in norm
            if expected in ("a", "b"):
                ok = norm.strip("()") == expected
            if ok:
                passed += 1
        except Exception:
            pass
    out["quality"]["quant"] = f"{passed}/{len(QUANT_TASKS)}"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=DEFAULT_BASE)
    ap.add_argument("--models", default="", help="comma list; default: all loaded")
    ap.add_argument("--runs", type=int, default=3, help="attempts per schema task")
    args = ap.parse_args()

    listed = api(args.base_url, "/v1/models")
    available = [m["id"] for m in listed.get("data", [])]
    models = [m for m in args.models.split(",") if m] or available
    print(f"endpoint: {args.base_url}\navailable: {available}\nbenching: {models}\n")

    results = []
    for m in models:
        print(f"--- {m}")
        r = bench_model(args.base_url, m, args.runs)
        results.append(r)
        print(json.dumps(r, indent=1))

    print("\n=== SUMMARY (quality first, then speed) ===")
    for r in results:
        q = r["quality"]
        sp = r["speed"].get("decode_tps", 0)
        print(f"{r['model']:44} tps={sp:>6} fill={q.get('fill')} bounds={q.get('bounds')} "
              f"novelty={q.get('novelty')} quant={q.get('quant')}")


if __name__ == "__main__":
    main()
