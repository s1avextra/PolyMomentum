#!/bin/bash
# PolyMomentum band-canary redeem sweeper.
# Claims resolved CTF positions for the band wallet and wraps the USDC.e
# payout back into pUSD so winnings return to spendable balance.
# Recipe verified against on-chain redemptions of our own positions
# (tx 0x479f4817..., 2026-08-25): CTF.redeemPositions(USDC.e, 0x0, cid, [1,2])
# then Onramp 0x62355638(USDC.e, self, amount).
set -uo pipefail

ENV_FILE="${BAND_ENV:-/etc/polymomentum/band-canary.env}"
set -a; . /etc/polymomentum/env; . "$ENV_FILE"; set +a
CAST=/root/.foundry/bin/cast
RPC="${POLYGON_RPC_URL:?}"
CTF=0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
USDCE=0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
ONRAMP=0x93070a847efEf7F70739046A929D47a521F5B8ee
ZERO32=0x0000000000000000000000000000000000000000000000000000000000000000

ADDR=$($CAST wallet address --private-key "$PRIVATE_KEY")

CIDS=$(curl -s "https://data-api.polymarket.com/trades?user=$ADDR&limit=300" | python3 -c '
import sys, json
seen = []
try:
    rows = json.load(sys.stdin)
except Exception:
    rows = []
for t in rows:
    c = t.get("conditionId") or t.get("market")
    if c and c.startswith("0x") and c not in seen:
        seen.append(c)
print("\n".join(seen))')

num() { echo "$1" | awk "{print \$1}"; }

REDEEMED=0
for CID in $CIDS; do
    DEN=$(num "$($CAST call $CTF "payoutDenominator(bytes32)(uint256)" "$CID" --rpc-url "$RPC" 2>/dev/null || echo 0)")
    [ "$DEN" = "0" ] && continue
    HOLD=0
    for IDX in 1 2; do
        COLL=$($CAST call $CTF "getCollectionId(bytes32,bytes32,uint256)(bytes32)" $ZERO32 "$CID" $IDX --rpc-url "$RPC" 2>/dev/null || echo "")
        [ -z "$COLL" ] && continue
        POS=$(num "$($CAST call $CTF "getPositionId(address,bytes32)(uint256)" $USDCE "$COLL" --rpc-url "$RPC" 2>/dev/null || echo 0)")
        BAL=$(num "$($CAST call $CTF "balanceOf(address,uint256)(uint256)" "$ADDR" "$POS" --rpc-url "$RPC" 2>/dev/null || echo 0)")
        [ "$BAL" != "0" ] && HOLD=1
    done
    if [ "$HOLD" = "1" ]; then
        echo "$(date -u +%FT%TZ) redeem $CID"
        $CAST send $CTF "redeemPositions(address,bytes32,bytes32,uint256[])" \
            $USDCE $ZERO32 "$CID" "[1,2]" \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null \
            && REDEEMED=$((REDEEMED+1)) || echo "$(date -u +%FT%TZ) redeem FAILED $CID"
    fi
done

# Wrap any meaningful USDC.e payout back to pUSD.
UB=$(num "$($CAST call $USDCE "balanceOf(address)(uint256)" "$ADDR" --rpc-url "$RPC" 2>/dev/null || echo 0)")
if [ -n "$UB" ] && [ "$UB" -ge 1000000 ] 2>/dev/null; then
    AL=$(num "$($CAST call $USDCE "allowance(address,address)(uint256)" "$ADDR" $ONRAMP --rpc-url "$RPC" 2>/dev/null || echo 0)")
    if [ "$AL" -lt "$UB" ] 2>/dev/null; then
        # Single-shot MAX approve: no zero-window (per the peer's shared-wallet
        # incident note), never needs refreshing again.
        $CAST send $USDCE "approve(address,uint256)" $ONRAMP \
            0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff \
            --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null
    fi
    ADDR_HEX=$(echo "$ADDR" | tr "[:upper:]" "[:lower:]" | sed "s/^0x//")
    DATA=0x623556380000000000000000000000002791bca1f2de4661ed88a30c99a7a9449aa84174000000000000000000000000${ADDR_HEX}$(printf "%064x" "$UB")
    echo "$(date -u +%FT%TZ) wrap ${UB} micro USDC.e -> pUSD"
    $CAST send $ONRAMP "$DATA" --rpc-url "$RPC" --private-key "$PRIVATE_KEY" >/dev/null \
        || echo "$(date -u +%FT%TZ) wrap FAILED"
fi
echo "$(date -u +%FT%TZ) sweep done: redeemed=$REDEEMED usdce_micro=$UB"
