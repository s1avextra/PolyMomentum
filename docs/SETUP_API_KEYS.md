# Getting Your Polymarket API Keys

This is a **one-time** setup needed only for **live mode**. Paper mode and the `scan`/`wallet`/`ctf`/`validate-replay` subcommands work without any of these.

## What you need

| Credential | What it is | Used for |
|-----------|------------|----------|
| `PRIVATE_KEY` | Your Polygon wallet's private key | Signing orders + (optionally) auto-detecting bankroll |
| `POLY_API_KEY` | UUID identifying your API access | Authenticating CLOB POSTs |
| `POLY_API_SECRET` | Base64 HMAC signing key | Signing CLOB request headers |
| `POLY_API_PASSPHRASE` | Random string | Additional auth factor |
| `CLOB_V2_READY` | Explicit live-order guard | Must stay `0` until the CLOB V2 order path is verified |

The API key, secret, and passphrase are **derived from your private key**. Polymarket's CLOB exposes a `POST /auth/api-key` endpoint that returns deterministic creds tied to a wallet — running the derivation twice yields the same values.

## Step 1 — Create a dedicated trading wallet

**Do NOT use your main wallet.** Spin up a fresh one (MetaMask → "Create Account", or any wallet generator). Export the private key — it should start with `0x` and be 32 bytes / 64 hex chars.

## Step 2 — Fund with pUSD + a little POL

Polymarket CLOB V2 settles in **pUSD** on Polygon. API-only traders must wrap
USDC.e into pUSD through the Collateral Onramp before live trading. Keep a small
amount of POL for gas and keep USDC.e/native USDC readings as diagnostics only.

Verify with the bot:

```bash
PRIVATE_KEY=0x...your_key... \
POLYGON_RPC_URL=https://polygon-bor-rpc.publicnode.com \
./target/release/polymomentum-engine wallet
```

## Step 3 — Derive API credentials

Polymarket's CLOB derivation is documented in their official SDKs. CLOB V2 keeps
L1/L2 API auth compatible, so existing API key derivation still applies. Run
`create_or_derive_api_creds` once with your private key. You'll get back the
three values to paste into `.env`:

```
POLY_API_KEY=<uuid>
POLY_API_SECRET=<base64>
POLY_API_PASSPHRASE=<string>
```

Easiest path: clone the official Polymarket TypeScript example, plug in your `PRIVATE_KEY`, run, copy the printed creds. The bot itself doesn't need to derive — it just uses what you paste in.

## Step 4 — Set token allowances

Before your first live trade you must approve the CLOB V2 contracts to spend
the correct collateral and outcome tokens. Polymarket's UI handles wrapping for
site users, but API-only wallets must explicitly prepare pUSD and allowances.

- pUSD: `0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB`
- CTF Exchange V2: `0xE111180000d2663C0091e4f400237545B87B996B`
- Neg Risk CTF Exchange V2: `0xe2222d279d744050d28e00520010520000310F59`
- Collateral Onramp: `0x93070a847efEf7F70739046A929D47a521F5B8ee`
- CtfCollateralAdapter: `0xAdA100Db00Ca00073811820692005400218FcE1f`
- NegRiskCtfCollateralAdapter: `0xadA2005600Dec949baf300f4C6120000bDB6eAab`

with `cast send` (Foundry) or any Polygon explorer "Write Contract" UI.

## Step 5 — Drop everything into `.env`

Paste the values into `/etc/polymomentum/env` (or a local `.env` for dev runs):

```
PRIVATE_KEY=0x...
POLY_API_KEY=...
POLY_API_SECRET=...
POLY_API_PASSPHRASE=...
POLYGON_RPC_URL=https://polygon-bor-rpc.publicnode.com
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
ALERT_REQUIRED=1
VENUE=paper_only
OPERATOR_COUNTRY=
POLYMOMENTUM_VENUE_COMPLIANCE_OK=0
POLYMARKET_US_API_ENABLED=0
CLOB_V2_READY=0
```

Verify the bot can read your wallet and the on-chain view of an old market:

```bash
./target/release/polymomentum-engine wallet
./target/release/polymomentum-engine ctf 0x<some_resolved_condition_id>
```

## Step 6 — First live trade

When you're ready (paper validated, venue/account compliance cleared, CLOB V2
order signing verified, pUSD/allowances ready, $1-sized stake), do not edit the
unit with `sed`. For the Dublin VPS and international CLOB, configure the venue
env first:

```bash
VENUE=polymarket_international
OPERATOR_COUNTRY=IE
POLYMOMENTUM_VENUE_COMPLIANCE_OK=1
POLYMARKET_US_API_ENABLED=0
CLOB_V2_READY=1
```

Do not set `CLOB_V2_READY=1` until the code path has migrated away from V1 raw
order signing and live order reconciliation has been verified with a
minimum-size canary.

Then deploy through the guarded path:

```bash
bash deploy/deploy.sh vps --enable-service --mode live --i-understand-live
```

Watch the next trade tape:

```bash
ssh vps 'journalctl -u polymomentum-engine -f -n 0 | grep candle.trade.live'
```

Roll back by setting `VENUE=paper_only` and redeploying paper mode:

```bash
bash deploy/deploy.sh vps --enable-service --mode paper
```
