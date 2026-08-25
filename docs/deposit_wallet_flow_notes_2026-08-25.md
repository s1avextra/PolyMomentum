# Deposit-wallet flow: implementation notes (Track A, 2026-08-25)

Venue now rejects fresh EOA makers ("maker address not allowed, please
use the deposit wallet flow"); grandfathered EOAs (our 0xe0ab) still
trade. Everything below is sourced from docs.polymarket.com/trading/
deposit-wallets and the official clients (py-clob-client-v2,
rs-clob-client-v2 aka polymarket_client_sdk_v2 0.7.0).

## Address derivation (deterministic, CREATE2)

- Factory `0x00000000000Fb5C9ADea0298D729A0CB3823Cc07`
- Beacon  `0x7A18EDfe055488A3128f01F563e5B479D92ffc3a`
- walletId = signer EOA left-padded to 32 bytes; salt =
  keccak(encode(factory, walletId)); init code hash = beacon proxy;
  CREATE2. (Exact init code hash not in the SDKs we inspected —
  practical alternative: Relayer WALLET-CREATE returns `proxyAddress`.)

## Deployment / funding / approvals

- Relayer `https://relayer-v2.polymarket.com`: WALLET-CREATE → poll to
  STATE_CONFIRMED (gasless, returns proxyAddress).
- Fund: plain pUSD ERC-20 transfer to the deposit wallet address.
- 4 approvals executed AS the wallet via a gasless Relayer batch signed
  by the EOA (Batch struct: wallet, nonce, deadline, calls): pUSD →
  both exchanges; CTF setApprovalForAll → both exchanges.

## POLY_1271 order signing (full algorithm, from
py_clob_client_v2/order_utils/exchange_order_builder_v2.py)

maker = deposit wallet, signer = EOA, signatureType = 3.

1. contents_hash = keccak(abi(ORDER_TYPE_HASH, salt, maker, signer,
   tokenId, makerAmount, takerAmount, side u8, sigType u8, timestamp,
   metadata b32, builder b32))
2. tds_hash = keccak(abi(SOLADY_TYPE_HASH, contents_hash,
   keccak("DepositWallet"), keccak("1"), chainId, SIGNER, bytes32(0)))
   where SOLADY_TYPE_STRING = "TypedDataSign(Order contents,string
   name,string version,uint256 chainId,address verifyingContract,
   bytes32 salt)" + ORDER_TYPE_STRING
3. digest = keccak(0x1901 ‖ ctf_exchange_v2_domain_sep ‖ tds_hash);
   EOA signs the raw digest.
4. wire signature = 0x ‖ innerSig(65B) ‖ app_domain_separator ‖
   contents_hash ‖ hex(ORDER_TYPE_STRING) ‖ uint16 len(ORDER_TYPE_STRING)

L1/L2 auth stays EOA-bound (per upstream issues #64/#70) - our derived
creds for 0x235b remain valid.

## Remaining work

1. Relayer client: WALLET-CREATE + approvals batch (schema from the
   official relayer clients or venue docs; auth format TBD).
2. Port step 1-4 into signing.rs (mirror; ~80 lines) + maker/funder
   plumbing (env BAND_DEPOSIT_WALLET) + signatureType=3 wire.
3. Fund derived wallet with pUSD from 0x235b (operator tx), then
   switch the band unit back to the separated setup (secrets file
   .newwallet is preserved).

## Status

Track B live in the meantime: band canary compounds on the
grandfathered shared wallet (peer notified via cross-bot note).
