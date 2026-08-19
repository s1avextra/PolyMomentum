# Canary orderflow handoff - 2026-05-17

## Purpose

This is a handoff for another Codex instance to reconstruct the failed live
canary orderflow without running its own canary.

The canary is a failure-case dataset. Do not treat it as live-promotion
evidence. Use it to reproduce execution semantics, duplicate-slot behavior,
venue rejects, latency tails, and the one-sided fill.

## Source artifact

Primary local export:

```text
/private/tmp/polyarbitrage_actions_latest.jsonl
```

Canary window inside that file:

```text
line range: 4743..6752
start_ms:   1778981934929
end_ms:     1778982651540
start_utc:  2026-05-17T01:38:54.929Z
end_utc:    2026-05-17T01:50:51.540Z
```

Filter rule:

```python
ts = row.get("ts_decision_ms") or row.get("ts_ms") or row.get("ts_ack_ms")
keep if 1778981934929 <= ts <= 1778982651540
```

The source file also contains older paper rows and later post-fix paper rows.
Do not use the whole file as the canary.

## Exact extraction script

Run from the repo root:

```bash
python3 - <<'PY'
import csv
import json
from pathlib import Path

src = Path("/private/tmp/polyarbitrage_actions_latest.jsonl")
out_jsonl = Path("/private/tmp/polyarbitrage_canary_orderflow_20260517.jsonl")
out_csv = Path("/private/tmp/polyarbitrage_canary_orders_20260517.csv")

start_ms = 1778981934929
end_ms = 1778982651540

rows = []
with src.open("r", encoding="utf-8") as f:
    for line_no, line in enumerate(f, 1):
        try:
            row = json.loads(line)
        except Exception:
            continue
        ts = row.get("ts_decision_ms") or row.get("ts_ms") or row.get("ts_ack_ms")
        if ts is None or ts < start_ms or ts > end_ms:
            continue
        row = dict(row)
        row["_source_line"] = line_no
        rows.append(row)

with out_jsonl.open("w", encoding="utf-8") as f:
    for row in rows:
        f.write(json.dumps(row, sort_keys=True) + "\n")

order_fields = [
    "_source_line",
    "ts_decision_ms",
    "ts_ack_ms",
    "type",
    "result",
    "origin",
    "asset",
    "condition_id",
    "outcome",
    "side",
    "order_type",
    "post_only",
    "price",
    "size",
    "order_id",
    "error_msg",
]
with out_csv.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=order_fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        if row.get("type") == "order_placed":
            writer.writerow(row)

print(f"wrote {len(rows)} canary rows to {out_jsonl}")
print(f"wrote order CSV to {out_csv}")
PY
```

Expected extracted row counts:

```text
all rows:          2010
order_placed:     1986
order_cancelled:  23
fill:             1
```

Expected deterministic extraction artifacts:

| artifact | bytes | sha256 |
|---|---:|---|
| `/private/tmp/polyarbitrage_canary_orderflow_20260517.jsonl` | 997315 | `11392bf8631e09a5222d1e49a3e8e9dbb06c01d5715ba53a3514327a26912598` |
| `/private/tmp/polyarbitrage_canary_orders_20260517.csv` | 578541 | `fd9be86f0acb608d57bf4cfd287dad1aa046a1a13556fffce894247b4ca6d1f8` |

## High-level verdict

The canary failed because live execution did not match the paper model.

Observed:

- `1986` live order attempts in roughly 12 minutes.
- `1960` permanent CLOB errors.
- `26` placed GTC orders.
- `23` cancel acknowledgements.
- `1` user-channel fill.
- `49` duplicate order slots if counted as slots with more than one attempt.
- Worst duplicate slot: `760` repeats.
- Available CLOB balance was previously observed to fall from `6.03363` to
  `0.88363` pUSD during this canary.

All order attempts in the canary were:

```text
origin:      quote
order_type:  Gtc
post_only:   true
side:        Buy
asset:       BTC
size:        5
```

There were no pair-arb FAK orders in the failed canary window.

## Error classes

Permanent CLOB error classification:

| error class | count | meaning |
|---|---:|---|
| `balance_allowance` | 1199 | CLOB rejected because available balance/allowance was insufficient after committed orders. |
| `marketable_buy_min_size` | 520 | Venue treated the 1-cent BUY as marketable/min-size invalid: `invalid amount for a marketable BUY order ($0.05), min size: $1`. |
| `post_only_cross` | 241 | Post-only GTC would cross the spread and was rejected. |

Representative first permanent error:

```json
{"asset":"BTC","condition_id":"0x326c0bda74321a615627e3c78b7dc6d8ee0a149294736145b031aaedb913719f","error_msg":"400 Bad Request: {\"error\":\"invalid post-only order: order crosses book\"}","order_type":"Gtc","origin":"quote","outcome":"Up","post_only":true,"price":"0.46","result":"permanent_error","side":"Buy","size":5,"ts_ack_ms":1778981934967,"ts_decision_ms":1778981934929,"type":"order_placed"}
```

Representative last permanent error:

```json
{"asset":"BTC","condition_id":"0x7d4d82a2095f4cc755fd290e5bbc4a6632e9e6383088a33b4ffbb3ca873ed094","error_msg":"400 Bad Request: {\"error\":\"not enough balance / allowance: the balance is not enough -> balance: 1933630, sum of matched orders: 1050000, order amount (inc. fees): 1056000\"}","order_type":"Gtc","origin":"quote","outcome":"Down","post_only":true,"price":"0.20","result":"permanent_error","side":"Buy","size":5,"ts_ack_ms":1778982651540,"ts_decision_ms":1778982651431,"type":"order_placed"}
```

## Latency profile

Decision-to-ack latency over all `1986` order attempts:

| metric | ms |
|---|---:|
| min | 22 |
| p50 | 109 |
| p90 | 852.4 |
| p95 | 3448.7 |
| p99 | 6932.96 |
| max | 7745 |

This latency profile is contaminated by the duplicate/reject backlog. Do not
use it as a production latency budget until a clean canary has zero duplicate
slots and zero permanent CLOB errors.

## Minute-by-minute order pressure

| UTC minute | order attempts |
|---|---:|
| 2026-05-17T01:38:00.000Z | 40 |
| 2026-05-17T01:39:00.000Z | 112 |
| 2026-05-17T01:40:00.000Z | 10 |
| 2026-05-17T01:41:00.000Z | 170 |
| 2026-05-17T01:42:00.000Z | 360 |
| 2026-05-17T01:43:00.000Z | 572 |
| 2026-05-17T01:44:00.000Z | 290 |
| 2026-05-17T01:45:00.000Z | 124 |
| 2026-05-17T01:46:00.000Z | 88 |
| 2026-05-17T01:47:00.000Z | 50 |
| 2026-05-17T01:50:00.000Z | 170 |

The storm peaked at `572` attempts in the 01:43 UTC minute.

## Market-level sequence

| cid | first UTC | last UTC | orders | placed | permanent_error | cancels | fills |
|---|---|---|---:|---:|---:|---:|---:|
| `0x326c0bda74321a615627e3c78b7dc6d8ee0a149294736145b031aaedb913719f` | 2026-05-17T01:38:54.929Z | 2026-05-17T01:39:30.236Z | 152 | 2 | 150 | 0 | 0 |
| `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | 2026-05-17T01:40:01.006Z | 2026-05-17T01:44:30.407Z | 1402 | 16 | 1386 | 0 | 0 |
| `0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4` | 2026-05-17T01:45:04.938Z | 2026-05-17T01:47:19.932Z | 262 | 7 | 255 | 0 | 1 |
| `0x7d4d82a2095f4cc755fd290e5bbc4a6632e9e6383088a33b4ffbb3ca873ed094` | 2026-05-17T01:50:01.840Z | 2026-05-17T01:50:51.431Z | 170 | 1 | 169 | 0 | 0 |
| `<cancel rows without condition_id>` | 2026-05-17T01:45:13.449Z | 2026-05-17T01:46:28.108Z | 0 | 0 | 0 | 23 | 0 |

The cancel rows log `market` as the order id, not condition id. They are
mostly cancels for placed orders on the `0xc9fd...` market.

## One-sided fill

Only one fill row exists in the canary window:

```json
{"asset":"BTC","condition_id":"0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4","order_id":"767da622-2c7f-4ff8-9746-0e749ea6e575","outcome":"Up","price":"0.77","side":"Buy","size":58,"ts_ms":1778982439932,"type":"fill"}
```

Notes:

- This is the one-sided fill that made the canary unusable for strategy
  expectancy.
- The pre-hardening action schema did not include `fee_usdc`, `origin`,
  `order_type`, or `liquidity` on fill rows.
- The `order_id` here appears as a trade/user-channel id in the old log path,
  not one of the tracked placed order ids. Later hardening added explicit
  maker/taker order-id attribution.

## Duplicate-slot ledger

These are the largest repeated slots. Slot key is:

```text
condition_id, outcome, side, order_type, post_only, price, size, origin, result
```

| count | lines | UTC span | cid | outcome | price | result | error class |
|---:|---|---|---|---|---:|---|---|
| 760 | 5015-6296 | 2026-05-17T01:41:14.209Z -> 2026-05-17T01:44:30.407Z | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Down | 0.96 | permanent_error | balance_allowance |
| 520 | 5263-6295 | 2026-05-17T01:42:42.106Z -> 2026-05-17T01:44:30.407Z | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | permanent_error | marketable_buy_min_size |
| 147 | 4743-4894 | 2026-05-17T01:38:54.929Z -> 2026-05-17T01:39:30.236Z | `0x326c0bda74321a615627e3c78b7dc6d8ee0a149294736145b031aaedb913719f` | Up | 0.46 | permanent_error | post_only_cross |
| 48 | 4935-4982 | 2026-05-17T01:41:05.808Z -> 2026-05-17T01:41:10.157Z | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Down | 0.94 | permanent_error | balance_allowance |
| 32 | 4983-5014 | 2026-05-17T01:41:10.373Z -> 2026-05-17T01:41:13.431Z | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Down | 0.95 | permanent_error | balance_allowance |
| 31 | 6583-6643 | 2026-05-17T01:50:01.840Z -> 2026-05-17T01:50:15.510Z | `0x7d4d82a2095f4cc755fd290e5bbc4a6632e9e6383088a33b4ffbb3ca873ed094` | Up | 0.46 | permanent_error | balance_allowance |
| 31 | 6584-6644 | 2026-05-17T01:50:01.840Z -> 2026-05-17T01:50:15.511Z | `0x7d4d82a2095f4cc755fd290e5bbc4a6632e9e6383088a33b4ffbb3ca873ed094` | Down | 0.46 | permanent_error | balance_allowance |
| 30 | 6436-6530 | 2026-05-17T01:46:03.992Z -> 2026-05-17T01:46:52.286Z | `0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4` | Up | 0.59 | permanent_error | balance_allowance |
| 20 | 6532-6570 | 2026-05-17T01:47:03.224Z -> 2026-05-17T01:47:10.707Z | `0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4` | Up | 0.69 | permanent_error | balance_allowance |
| 20 | 6689-6727 | 2026-05-17T01:50:34.139Z -> 2026-05-17T01:50:37.583Z | `0x7d4d82a2095f4cc755fd290e5bbc4a6632e9e6383088a33b4ffbb3ca873ed094` | Up | 0.61 | permanent_error | balance_allowance |
| 20 | 6690-6728 | 2026-05-17T01:50:34.139Z -> 2026-05-17T01:50:37.583Z | `0x7d4d82a2095f4cc755fd290e5bbc4a6632e9e6383088a33b4ffbb3ca873ed094` | Down | 0.32 | permanent_error | post_only_cross |

## Placed orders

There were `26` placed GTC orders. These are the rows another instance should
use for exact placed-order latency and cancel linkage.

| line | decision UTC | ack ms | cid | outcome | price | size | order_id |
|---:|---|---:|---|---|---:|---:|---|
| 4744 | 2026-05-17T01:38:54.929Z | 84 | `0x326c0bda74321a615627e3c78b7dc6d8ee0a149294736145b031aaedb913719f` | Down | 0.46 | 5 | `0x2c518a0ad55338636464039c6f642ec3bd7560d7a7811c5b5830c08cca899254` |
| 4746 | 2026-05-17T01:38:54.933Z | 192 | `0x326c0bda74321a615627e3c78b7dc6d8ee0a149294736145b031aaedb913719f` | Down | 0.46 | 5 | `0x6552acb3cd54cc89654d1ceb5e94918fef6daaa6180db590fbd8ad2cbc66c5f3` |
| 4895 | 2026-05-17T01:40:01.006Z | 1198 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.46 | 5 | `0x3fb91ddf3b7f96f9f9f19eea7979a588607b6fe8776b3dbf017841e4f003ebaa` |
| 4896 | 2026-05-17T01:40:01.006Z | 6043 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Down | 0.46 | 5 | `0x4760c2dcca51e15badf304252c6c7a190e75448e02b88202e619fad827d2f224` |
| 4902 | 2026-05-17T01:40:01.031Z | 7013 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Down | 0.46 | 5 | `0xe1f31fcc41d8fe64aa3588b5c8936c2af5a0714739f736850df815e9ccf064c9` |
| 4905 | 2026-05-17T01:41:02.797Z | 2135 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0x5fb86ec72ba26ed7e4d8a78a311d81e618f0052250fb695b10e3c87740199132` |
| 4907 | 2026-05-17T01:41:02.799Z | 4184 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0xac0cf44f800199d2bea104ac24fa3a9582a8e04af0582502115b67df6860751f` |
| 4909 | 2026-05-17T01:41:02.800Z | 5194 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0x6c18ebe4a0009eb51e114196d1f524cebb4cec7d634957451666dd5956f9619f` |
| 4911 | 2026-05-17T01:41:02.806Z | 5373 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0x143e5df0f49836f6611e7a9368e759133416c11e880d5c1e9f0af9ec788a6110` |
| 4913 | 2026-05-17T01:41:02.834Z | 5407 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0x828b4efe4def82d963c78da4dff6968195b9969e359e305cbb388dd6fcbb0e43` |
| 4915 | 2026-05-17T01:41:03.806Z | 4488 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0xe4ea8d4b455cf42065442394d13b5d88cf1a50cf559c79e846ce1ebf1cfe5865` |
| 4917 | 2026-05-17T01:41:03.807Z | 4538 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0x97c52a89cabfdfb4a581f8b017ecaee60eac146d9ff9c0d842305ef95e05fb90` |
| 4919 | 2026-05-17T01:41:03.837Z | 4566 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0x14929877dd8433690af8a25d5aeab4da83a08965874e63e6a63bf7a55c1fbb48` |
| 4921 | 2026-05-17T01:41:03.838Z | 4625 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0xc465577e52e78eecaca83560dd6f950b1f6840e179b2bcf5bf92f85bffcd02a2` |
| 4923 | 2026-05-17T01:41:04.181Z | 4340 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0x0e4b2aaac8036cd28c19683ff5c056f85da87dfab03185960d697a4385de41fe` |
| 4925 | 2026-05-17T01:41:04.807Z | 3766 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0xac7e35a2c639ddb57733f7a0294bcae8866055194dab21f4d2d3da1d4098dd56` |
| 5238 | 2026-05-17T01:42:39.233Z | 29 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0x1c05293ef718cb13ed40834b52d5952f54e0513a89dd497182d4d0cff3c423c6` |
| 5239 | 2026-05-17T01:42:39.234Z | 61 | `0x5f2772c6b66851c6192bc6b16d143a8e76b4809be70d58dc3013235e046687a0` | Up | 0.01 | 5 | `0x3426f2da02fe2f71c1930fb859aa28c030fb29685ede3f1c9bf4f3c99704e633` |
| 6297 | 2026-05-17T01:45:04.938Z | 5040 | `0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4` | Up | 0.28 | 5 | `0xc258554ee9d9d277998c51fcbc02dc79f234f218dedb1f14c1d8807e530a470f` |
| 6299 | 2026-05-17T01:45:04.956Z | 6890 | `0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4` | Up | 0.28 | 5 | `0xb2ca8f38094ec6cfb6d475ed9973e219da4432faeb136480d176b010d7e15ab9` |
| 6338 | 2026-05-17T01:45:14.055Z | 6619 | `0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4` | Up | 0.26 | 5 | `0x38b9871ffad17303730029329f55d3265b3b1460424276a8dbe7e3c48dd692c9` |
| 6361 | 2026-05-17T01:45:22.132Z | 283 | `0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4` | Up | 0.28 | 5 | `0xe9491b4d80fe45516ab272d68deb5476c1e74ca92666d5f41089c48929babab2` |
| 6457 | 2026-05-17T01:46:20.896Z | 57 | `0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4` | Down | 0.24 | 5 | `0x8d8b2f6a00b9aa090195fe15ba9cf0862b3744d69b8cff2c7c71f0c447ba31e5` |
| 6468 | 2026-05-17T01:46:22.029Z | 359 | `0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4` | Down | 0.26 | 5 | `0xcc7576ecc7c0f7994e4bcd98cf219066bba8b73985af8a3f18b5f096e7ef00f9` |
| 6579 | 2026-05-17T01:47:15.206Z | 217 | `0xc9fdc3787b38bb80ff5b4c44e4195490b29563fc6a1aa40e9beba4fbaf2446a4` | Down | 0.23 | 5 | `0x6bb89468fa27a8538f0734a0c9a3960c3b8f6700f53145ef6d97d228c5b37825` |
| 6732 | 2026-05-17T01:50:49.107Z | 807 | `0x7d4d82a2095f4cc755fd290e5bbc4a6632e9e6383088a33b4ffbb3ca873ed094` | Down | 0.21 | 5 | `0xe4b4032b2465bb50c335604dd73b4a713433c4020a9e1e03b6ae979399bb401c` |

## Root-cause interpretation

The engine violated paper/live parity in three ways:

1. It kept submitting maker quotes that the venue rejected as post-only crosses.
2. It did not halt immediately on balance/allowance exhaustion, so repeated
   slots continued after the CLOB already said funds were reserved/insufficient.
3. It treated outstanding live order state too optimistically before user WS
   confirmation/cancel reconciliation, causing duplicate slot retries.

This is why later fixes added:

- local post-only preflight;
- desired quote clamping one tick inside visible book;
- insufficient-balance risk halt;
- live REST ack application to local order state;
- canary parity blocker in the paper readiness loop;
- fee/liquidity/order-type attribution on fill rows.

## Replay guidance

For backtest/paper parity, replay this canary as an execution-layer failure,
not as strategy alpha evidence.

Required model behavior to match this canary:

- A post-only GTC BUY that crosses the visible ask must reject locally before
  REST placement.
- Balance/allowance CLOB reject must halt placement, not keep retrying.
- Duplicate slot count must be zero after a reject or live order ack.
- User-channel fills must be the only live fill source.
- Fill rows must be attributed to maker/taker order ids before PnL accounting.

Promotion gate derived from this canary:

```text
permanent CLOB errors = 0
duplicate slots with count > 1 = 0
one-sided fill cids = 0
unknown-liquidity fills = 0
balance/allowance rejects = 0
post-only-cross REST rejects = 0
```
