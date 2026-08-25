#!/usr/bin/env python3
"""One-time deposit-wallet setup for the band canary's separated EOA.

Run ON THE VPS (the key never leaves it):

  uv run --with py-clob-client-v2 --with py-builder-relayer-client \
      python scripts/setup_deposit_wallet.py /etc/polymomentum/band-canary-secrets.env.newwallet

Steps (all idempotent, gasless via the official Relayer):
  1. create/derive Builder API creds from our existing CLOB L2 creds;
  2. derive the deterministic deposit-wallet address; deploy if needed;
  3. execute the 4 trading approvals AS the deposit wallet
     (pUSD -> both exchanges, CTF setApprovalForAll -> both exchanges).

Prints only addresses and tx states - never key material.
"""

import sys
import time

CLOB = "https://clob.polymarket.com"
RELAYER = "https://relayer-v2.polymarket.com"
CHAIN_ID = 137
PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
EXCHANGE = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_EXCHANGE = "0xe2222d279d744050d28e00520010520000310F59"
MAX_UINT = (1 << 256) - 1


def load_env(path: str) -> dict:
    env = {}
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


def main() -> None:
    env = load_env(sys.argv[1])
    pk = env["PRIVATE_KEY"]

    from eth_abi import encode
    from eth_utils import keccak, to_checksum_address
    from py_clob_client_v2 import ApiCreds, ClobClient
    from py_builder_relayer_client.client import RelayClient
    from py_builder_relayer_client.models import DepositWalletCall
    from py_builder_signing_sdk.config import BuilderConfig, BuilderApiKeyCreds

    creds = ApiCreds(
        api_key=env["POLY_API_KEY"],
        api_secret=env["POLY_API_SECRET"],
        api_passphrase=env["POLY_API_PASSPHRASE"],
    )
    clob = ClobClient(host=CLOB, chain_id=CHAIN_ID, key=pk, creds=creds)

    print("step 1: builder api creds")
    try:
        builder = clob.create_builder_api_key()
    except Exception as exc:
        print("  create failed (may already exist):", str(exc)[:120])
        builder = clob.derive_builder_api_key()
    bkey = builder["apiKey"] if isinstance(builder, dict) else builder.api_key
    bsec = builder["secret"] if isinstance(builder, dict) else builder.api_secret
    bpass = builder["passphrase"] if isinstance(builder, dict) else builder.api_passphrase
    print("  builder key:", bkey[:8], "…")

    relay = RelayClient(
        RELAYER,
        CHAIN_ID,
        pk,
        BuilderConfig(
            local_builder_creds=BuilderApiKeyCreds(key=bkey, secret=bsec, passphrase=bpass)
        ),
    )

    wallet = relay.get_expected_deposit_wallet()
    print("step 2: deposit wallet:", wallet)
    try:
        resp = relay.deploy_deposit_wallet()
        print("  deploy submitted:", getattr(resp, "transaction_id", resp))
        print("  deploy state:", resp.wait())
    except Exception as exc:
        print("  deploy skipped/failed (may already exist):", str(exc)[:140])

    def sel(sig: str) -> bytes:
        return keccak(text=sig)[:4]

    def call(target: str, data: bytes) -> "DepositWalletCall":
        return DepositWalletCall(target=to_checksum_address(target), value="0", data="0x" + data.hex())

    approvals = [
        call(PUSD, sel("approve(address,uint256)") + encode(["address", "uint256"], [EXCHANGE, MAX_UINT])),
        call(PUSD, sel("approve(address,uint256)") + encode(["address", "uint256"], [NEG_RISK_EXCHANGE, MAX_UINT])),
        call(CTF, sel("setApprovalForAll(address,bool)") + encode(["address", "bool"], [EXCHANGE, True])),
        call(CTF, sel("setApprovalForAll(address,bool)") + encode(["address", "bool"], [NEG_RISK_EXCHANGE, True])),
    ]
    print("step 3: trading approvals batch")
    resp = relay.execute_deposit_wallet_batch(
        calls=approvals,
        wallet_address=wallet,
        nonce=str(relay.get_deposit_wallet_nonce(wallet))
        if hasattr(relay, "get_deposit_wallet_nonce")
        else "0",
        deadline=str(int(time.time()) + 300),
    )
    print("  batch submitted:", getattr(resp, "transaction_id", resp))
    print("  batch state:", resp.wait())
    print("DONE. Fund this address with pUSD:", wallet)


if __name__ == "__main__":
    main()
