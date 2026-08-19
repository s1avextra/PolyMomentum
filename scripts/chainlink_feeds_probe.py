#!/usr/bin/env python3
"""List Data Streams feeds available to our subscription (HMAC-authed).

Auth scheme mirrors rust_engine/src/data/chainlink.rs:
string_to_sign = "METHOD full_path sha256(body) api_key timestamp_ms".
"""

import hashlib
import hmac
import json
import time
import urllib.request
from pathlib import Path


def env_value(key: str) -> str:
    for line in Path(".env").read_text().splitlines():
        if line.startswith(key + "="):
            return line.partition("=")[2].strip()
    return ""


def authed_get(base: str, full_path: str, api_key: str, secret: str):
    ts = str(int(time.time() * 1000))
    body_hash = hashlib.sha256(b"").hexdigest()
    sts = f"GET {full_path} {body_hash} {api_key} {ts}"
    sig = hmac.new(secret.encode(), sts.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(
        base + full_path,
        headers={
            "Authorization": api_key,
            "X-Authorization-Timestamp": ts,
            "X-Authorization-Signature-SHA256": sig,
            "User-Agent": "polymomentum-engine/0.2",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.status, json.load(resp)


def main() -> None:
    base = env_value("CHAINLINK_DATA_STREAMS_REST_URL") or "https://api.dataengine.chain.link"
    api_key = env_value("CHAINLINK_DATA_STREAMS_API_KEY")
    secret = env_value("CHAINLINK_DATA_STREAMS_HMAC_SECRET")
    assert api_key and secret, "chainlink credentials missing from .env"
    status, feeds = authed_get(base, "/api/v1/feeds", api_key, secret)
    print("HTTP", status, "| feeds:", len(feeds.get("feeds", [])))
    for f in feeds.get("feeds", []):
        print(json.dumps(f))


if __name__ == "__main__":
    main()
