#!/usr/bin/env python3
"""Settlement-basis fair value for btc-updown-5m windows: the frozen spec of
docs/adaptive_family_research_2026-10-01.md section 2 as pure functions over
a 1 s close series.

The market settles Up iff the 60 s TWAP at the window end is >= the 60 s
TWAP before the open (tie to Up).  P[i] is the 1 s close of the second
starting at epoch i, W the window start, t the elapsed second:

    K      = mean(P[W-60 .. W-1])                                  strike
    t<=240: m_t = P[W+t-1] - K                 V_t = (240 - t) + 20.5
    t> 240: r = 300 - t
            m_t = (sum(P[W+240 .. W+t-1]) + r*P[W+t-1])/60 - K     V_t = r(r+1)(2r+1)/21600
    d_i    = (P[i] - P[i-30])^2 / 30
    sig2_i = EWMA of d, half-life 300 s
    s_t    = sqrt(c * sig2_(W+t-1) * V_t + s_b^2)
    z_t    = |m_t| / s_t;  side = up if m_t >= 0 else down

Every quantity at elapsed second t reads closes of seconds before W + t only
(the newest is P[W+t-1]): no lookahead.

The EWMA is the normalised finite-window form over the last `warmup_s`
values, sig2_i = sum_k lam^k d_(i-k) / sum_k lam^k, k < warmup_s, lam =
2^(-1/half_life): the recursion sig2_i = lam*sig2_(i-1) + (1-lam)*d_i with
its start forgotten (at 24 half-lives the two differ by 6e-8), and a pure
function of the closes whatever the series start.

A spec is data: the rule's shape (`params`), the fitted constants c and s_b,
the calibration table P(win | z bucket) with Wilson bounds and the schedule
cap per bucket, all from labelled windows that ended at or before `cut_ts`,
bound by a sha256 (`spec_sha256`).  The scoring path (`state`, `path`,
`schedule_cap`, `bucket`) reads the spec it is handed and nothing else;
DEFAULT_PARAMS only seeds a NEW spec in `build_spec`.
"""

from __future__ import annotations

import hashlib
import json
import math
from array import array
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

SPEC_VERSION = "settlement_z_v1"
# The shape of a new spec (build_spec).  window_s/twap_s are the venue's
# settlement rule (Gamma btc-5m-twap-60); v_locked is V at the lock start
# (60*61*121/21600 = 20.503, frozen as 20.5); looks_s are the 13 seconds per
# window the calibration table pools; cap_ticks the prices a schedule cap may
# take; s_b_grid = (low, high, step) of the basis-noise fit.
DEFAULT_PARAMS: Dict[str, Any] = {
    "window_s": 300,
    "twap_s": 60,
    "v_locked": 20.5,
    "subsample_s": 30,
    "half_life_s": 300,
    "warmup_s": 7200,
    "max_fill_s": 5,
    "looks_s": list(range(150, 271, 10)),
    "z_edges": [0.0, 1.0, 1.5, 1.75, 2.0, 2.25, 2.5, 2.75, 3.0, 3.5, 4.0, 5.0],
    "cap_buffer": 0.005,
    "cap_ticks": [round(0.80 + 0.01 * index, 2) for index in range(20)],
    "s_b_grid": [1.0, 8.0, 0.1],
}
WILSON_Z = 1.959964


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def wilson_lower(wins: int, total: int) -> Optional[float]:
    if not total:
        return None
    p = wins / total
    z = WILSON_Z
    centre = p + z * z / (2 * total)
    spread = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total))
    return (centre - spread) / (1 + z * z / total)


def wilson_upper(wins: int, total: int) -> Optional[float]:
    lower = wilson_lower(total - wins, total)
    return None if lower is None else 1.0 - lower


def break_even(price: float, fee_rate: float) -> float:
    return float(price) + float(fee_rate) * float(price) * (1.0 - float(price))


# --- the close series --------------------------------------------------------


class CloseSeries:
    """1 s closes over [start, end): the close of the second starting at
    epoch i, forward-filled over a gap of at most `max_fill_s` seconds; a
    second further than that from its last close is unusable, and so is
    every quantity that reads it."""

    def __init__(self, closes: Mapping[int, float], start: int, end: int, max_fill_s: int = DEFAULT_PARAMS["max_fill_s"]) -> None:
        self.start = int(start)
        self.end = max(int(end), self.start)
        self.px = array("d")
        self.total = array("d", [0.0])  # total[j] = sum of px over [start, start + j)
        self.bad = array("l", [0])  # bad[j] = unusable seconds in [start, start + j)
        last: Optional[float] = None
        age = 0
        for ts in range(self.start, self.end):
            value = closes.get(ts)
            if value is not None:
                last, age = float(value), 0
            else:
                age += 1
            usable = last is not None and age <= int(max_fill_s)
            price = last if usable else 0.0
            self.px.append(price)
            self.total.append(self.total[-1] + price)
            self.bad.append(self.bad[-1] + (0 if usable else 1))
        self._sigma2: Dict[Tuple[int, int, int], array] = {}

    def covers(self, a: int, b: int) -> bool:
        """Every second of [a, b) is inside the series and usable."""
        return self.start <= a <= b <= self.end and self.bad[b - self.start] == self.bad[a - self.start]

    def close(self, ts: int) -> Optional[float]:
        return self.px[ts - self.start] if self.covers(ts, ts + 1) else None

    def sum(self, a: int, b: int) -> Optional[float]:
        return (self.total[b - self.start] - self.total[a - self.start]) if self.covers(a, b) else None

    def sigma2_series(self, params: Mapping[str, Any]) -> array:
        """sig2 per second of the series (NaN where the warm-up is not
        covered), computed once per (subsample, half-life, warm-up)."""
        k, half_life, warmup = int(params["subsample_s"]), int(params["half_life_s"]), int(params["warmup_s"])
        key = (k, half_life, warmup)
        if key in self._sigma2:
            return self._sigma2[key]
        count = self.end - self.start
        lam = 2.0 ** (-1.0 / half_life)
        tail = lam**warmup
        norm = (1.0 - lam) / (1.0 - tail)
        out = array("d", [math.nan]) * count
        squares = array("d", [0.0]) * count
        px, bad = self.px, self.bad
        weighted = 0.0
        run = 0
        for j in range(count):
            if j >= k and bad[j + 1] == bad[j - k]:
                move = px[j] - px[j - k]
                square = move * move / k
                squares[j] = square
                weighted = lam * weighted + square
                run += 1
                if run > warmup:
                    weighted = max(weighted - tail * squares[j - warmup], 0.0)
                    run = warmup
                if run == warmup:
                    out[j] = weighted * norm
            else:
                weighted, run = 0.0, 0
        self._sigma2[key] = out
        return out

    def sigma2(self, ts: int, params: Mapping[str, Any]) -> Optional[float]:
        if not self.start <= ts < self.end:
            return None
        value = self.sigma2_series(params)[ts - self.start]
        return None if math.isnan(value) else value


# --- the model ---------------------------------------------------------------


def strike(series: CloseSeries, window_start: int, params: Mapping[str, Any]) -> Optional[float]:
    twap = int(params["twap_s"])
    total = series.sum(window_start - twap, window_start)
    return None if total is None else total / twap


def variance_weight(t: int, params: Mapping[str, Any]) -> float:
    """V_t: the seconds of 1 s variance the settlement margin still carries
    at elapsed second t.  Before the closing TWAP starts every remaining
    second counts in full, plus the TWAP's own share; inside it the r
    remaining seconds weigh (j/twap)^2."""
    window, twap = int(params["window_s"]), int(params["twap_s"])
    lock = window - twap
    if t <= lock:
        return float(lock - t) + float(params["v_locked"])
    r = window - t
    return r * (r + 1) * (2 * r + 1) / (6.0 * twap * twap)


def settlement_margin(
    series: CloseSeries, window_start: int, t: int, params: Mapping[str, Any], strike_price: Optional[float] = None
) -> Optional[float]:
    """m_t: the settlement margin as it stands at elapsed second t, the
    seconds of the closing TWAP still to come carried at the latest close."""
    window, twap = int(params["window_s"]), int(params["twap_s"])
    lock = window - twap
    if not 1 <= t <= window:
        return None
    k = strike(series, window_start, params) if strike_price is None else strike_price
    latest = series.close(window_start + t - 1)
    if k is None or latest is None:
        return None
    if t <= lock:
        return latest - k
    locked = series.sum(window_start + lock, window_start + t)
    if locked is None:
        return None
    return (locked + (window - t) * latest) / twap - k


def state(series: CloseSeries, window_start: int, t: int, spec: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """The model at elapsed second t of a window, from closes of seconds
    before window_start + t only; None when a close it needs is missing."""
    params = spec["params"]
    k = strike(series, window_start, params)
    margin = settlement_margin(series, window_start, t, params, k)
    sigma2 = series.sigma2(window_start + t - 1, params)
    if margin is None or sigma2 is None:
        return None
    v = variance_weight(t, params)
    s = math.sqrt(float(spec["c"]) * sigma2 * v + float(spec["s_b"]) ** 2)
    return {
        "t": int(t),
        "strike": k,
        "margin": margin,
        "v": v,
        "sigma2": sigma2,
        "s": s,
        "z": abs(margin) / s,
        "side": "up" if margin >= 0 else "down",
    }


def path(
    series: CloseSeries, window_start: int, t_low: int, t_high: int, spec: Mapping[str, Any], sigma2_const: Optional[float] = None
) -> Dict[int, Tuple[float, float]]:
    """{t: (m_t, z_t)} for t_low <= t <= t_high, the same numbers as
    `state` (side = up iff m_t >= 0); a second whose inputs are missing is
    absent.  With `sigma2_const` the volatility is that constant instead
    of the EWMA (the constant-sigma control floor)."""
    params = spec["params"]
    k = strike(series, window_start, params)
    if k is None:
        return {}
    c, basis2 = float(spec["c"]), float(spec["s_b"]) ** 2
    out: Dict[int, Tuple[float, float]] = {}
    for t in range(int(t_low), int(t_high) + 1):
        margin = settlement_margin(series, window_start, t, params, k)
        sigma2 = series.sigma2(window_start + t - 1, params) if sigma2_const is None else float(sigma2_const)
        if margin is None or sigma2 is None:
            continue
        out[t] = (margin, abs(margin) / math.sqrt(c * sigma2 * variance_weight(t, params) + basis2))
    return out


def final_margin(series: CloseSeries, window_start: int, params: Mapping[str, Any]) -> Optional[float]:
    """The settlement margin at the window end on the Binance proxy: the
    closing TWAP minus the strike (>= 0 settles Up)."""
    return settlement_margin(series, window_start, int(params["window_s"]), params)


# --- calibration -------------------------------------------------------------


def bucket_index(z: float, edges: Sequence[float]) -> Optional[int]:
    """The bucket [edges[i], edges[i+1]) holding z; the last is open-ended."""
    if z < edges[0]:
        return None
    index = len(edges) - 1
    while z < edges[index]:
        index -= 1
    return index


def calibration_looks(
    series: CloseSeries, windows: Iterable[Tuple[int, str]], spec: Mapping[str, Any]
) -> List[Tuple[float, bool]]:
    """(z, won) at every calibration look of every (window_start, official)
    window: won iff the settlement side at that second is the official
    outcome."""
    looks: List[Tuple[float, bool]] = []
    for window_start, official in windows:
        for t in spec["params"]["looks_s"]:
            current = state(series, int(window_start), int(t), spec)
            if current is not None:
                looks.append((current["z"], current["side"] == official))
    return looks


def schedule_tick(lower: Optional[float], fee_rate: float, params: Mapping[str, Any]) -> Optional[float]:
    """The largest tick whose break-even sits cap_buffer inside a bucket's
    Wilson lower bound; None when no tick does."""
    if lower is None:
        return None
    allowed = [float(tick) for tick in params["cap_ticks"] if break_even(tick, fee_rate) <= lower - float(params["cap_buffer"])]
    return max(allowed) if allowed else None


def calibration_table(looks: Iterable[Tuple[float, bool]], fee_rate: float, params: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """P(win | z bucket) with Wilson bounds and the schedule cap per bucket."""
    edges = [float(edge) for edge in params["z_edges"]]
    counts = [[0, 0] for _ in edges]
    for z, won in looks:
        index = bucket_index(z, edges)
        if index is not None:
            counts[index][0] += int(bool(won))
            counts[index][1] += 1
    table = []
    for index, (wins, total) in enumerate(counts):
        lower = wilson_lower(wins, total)
        table.append(
            {
                "z_low": edges[index],
                "z_high": edges[index + 1] if index + 1 < len(edges) else None,
                "n": total,
                "wins": wins,
                "rate": (wins / total) if total else None,
                "wilson_lower": lower,
                "wilson_upper": wilson_upper(wins, total),
                "cap": schedule_tick(lower, fee_rate, params),
            }
        )
    return table


def bucket(z: float, spec: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    """The frozen calibration row of z."""
    index = bucket_index(float(z), [row["z_low"] for row in spec["table"]])
    return None if index is None else spec["table"][index]


def schedule_cap(z: float, spec: Mapping[str, Any]) -> Optional[float]:
    """k98: the frozen schedule cap at z (None: no tick clears, no trade)."""
    row = bucket(z, spec)
    return None if row is None else row["cap"]


def fit_c(series: CloseSeries, window_starts: Iterable[int], params: Mapping[str, Any]) -> Dict[str, Any]:
    """c, label-free: the scale that makes the remaining move of the
    settlement margin, m_final - m_t over the calibration looks, unit
    variance against sig2 * V_t (no basis noise: both ends are Binance)."""
    total = 0.0
    count = windows = 0
    for window_start in window_starts:
        k = strike(series, int(window_start), params)
        final = None if k is None else settlement_margin(series, int(window_start), int(params["window_s"]), params, k)
        if final is None:
            continue
        used = False
        for t in params["looks_s"]:
            margin = settlement_margin(series, int(window_start), int(t), params, k)
            sigma2 = series.sigma2(int(window_start) + int(t) - 1, params)
            if margin is None or not sigma2:
                continue
            total += (final - margin) ** 2 / (sigma2 * variance_weight(int(t), params))
            count += 1
            used = True
        windows += int(used)
    return {"c": (total / count) if count else None, "looks": count, "windows": windows}


def fit_s_b(series: CloseSeries, windows: Iterable[Tuple[int, str]], params: Mapping[str, Any]) -> Dict[str, Any]:
    """s_b: the basis noise of the Binance proxy against the oracle, the
    grid value maximising the likelihood of P(up) = Phi(m_final / s_b) on
    the official labels."""
    finals = []
    for window_start, official in windows:
        final = final_margin(series, int(window_start), params)
        if final is not None and official in ("up", "down"):
            finals.append((final, official == "up"))
    low, high, step = (float(value) for value in params["s_b_grid"])
    best: Optional[Tuple[float, float]] = None
    for index in range(int(round((high - low) / step)) + 1):
        candidate = round(low + index * step, 6)
        loss = 0.0
        for final, up in finals:
            p_up = min(max(0.5 * (1.0 + math.erf(final / candidate / math.sqrt(2.0))), 1e-12), 1.0 - 1e-12)
            loss -= math.log(p_up if up else 1.0 - p_up)
        if finals and (best is None or loss < best[0]):
            best = (loss, candidate)
    return {"s_b": best[1] if best else None, "windows": len(finals), "log_loss": (best[0] / len(finals)) if best else None}


def first_crossing(
    series: CloseSeries, windows: Iterable[Tuple[int, str]], spec: Mapping[str, Any], z_min: float, t_low: int, t_high: int
) -> Dict[str, Any]:
    """Accuracy of the stopped rule: over the windows whose z reaches z_min
    in [t_low, t_high], the share whose side at the FIRST such second is
    the official outcome."""
    wins = total = 0
    for window_start, official in windows:
        for t, (margin, z) in sorted(path(series, int(window_start), t_low, t_high, spec).items()):
            if z >= z_min:
                total += 1
                wins += int(("up" if margin >= 0 else "down") == official)
                break
    return {
        "z_min": float(z_min),
        "t_low": int(t_low),
        "t_high": int(t_high),
        "n": total,
        "wins": wins,
        "rate": (wins / total) if total else None,
        "wilson_lower": wilson_lower(wins, total),
        "wilson_upper": wilson_upper(wins, total),
    }


# --- the frozen spec ---------------------------------------------------------


def spec_sha256(spec: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical({key: value for key, value in spec.items() if key != "sha256"}).encode("utf-8")).hexdigest()


def verify_spec(spec: Mapping[str, Any]) -> None:
    """Raise unless the spec is this module's version and its hash binds
    its content: a frozen spec edited after the fact scores nothing."""
    if not isinstance(spec, Mapping) or spec.get("version") != SPEC_VERSION:
        raise ValueError("settlement spec version is %r, not %s" % ((spec or {}).get("version") if isinstance(spec, Mapping) else None, SPEC_VERSION))
    if spec.get("sha256") != spec_sha256(spec):
        raise ValueError("settlement spec sha256 does not match its content")


def build_spec(
    series: CloseSeries,
    windows: Sequence[Tuple[int, str]],
    cut_ts: int,
    fee_rate: float,
    c: Optional[float] = None,
    s_b: Optional[float] = None,
    crossings: Sequence[Tuple[float, int, int]] = (),
    params: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Fit and freeze a spec on the labelled (window_start, official)
    windows that ended at or before `cut_ts`; later windows are dropped
    here, so nothing after the cut can reach a constant or the table.  c
    and s_b are fitted unless given (a given value is recorded as such).
    `crossings` lists the (z_min, t_low, t_high) stopped rules whose
    accuracy on the same windows is frozen beside the table."""
    params = json.loads(_canonical(dict(DEFAULT_PARAMS if params is None else params)))
    before = sorted(
        (int(window_start), str(official))
        for window_start, official in windows
        if official in ("up", "down") and int(window_start) + int(params["window_s"]) <= int(cut_ts)
    )
    fitted_c = fit_c(series, [window_start for window_start, _ in before], params)
    fitted_s_b = fit_s_b(series, before, params) if s_b is None else None
    if (c is None and fitted_c["c"] is None) or (s_b is None and fitted_s_b["s_b"] is None):
        raise ValueError("no labelled window before the cut %d has the closes to fit the settlement spec" % int(cut_ts))
    spec: Dict[str, Any] = {
        "version": SPEC_VERSION,
        "params": params,
        "c": float(fitted_c["c"] if c is None else c),
        "s_b": float(fitted_s_b["s_b"] if s_b is None else s_b),
        "fee_rate": float(fee_rate),
        "cut_ts": int(cut_ts),
    }
    looks = calibration_looks(series, before, spec)
    sigmas = sorted(
        value
        for window_start, _ in before
        for value in (series.sigma2(window_start + int(t) - 1, params) for t in params["looks_s"])
        if value is not None
    )
    spec["fit"] = {
        "windows": len(before),
        "looks": len(looks),
        "c": {**fitted_c, "source": "fit" if c is None else "given"},
        "s_b": {**(fitted_s_b or {}), "source": "fit" if s_b is None else "given"},
        "first_window_start": before[0][0] if before else None,
        "last_window_start": before[-1][0] if before else None,
    }
    # The constant-sigma control floor's volatility: the median sig2 of the looks.
    spec["sigma2_median"] = sigmas[len(sigmas) // 2] if sigmas else None
    spec["table"] = calibration_table(looks, fee_rate, params)
    spec["first_crossing"] = [first_crossing(series, before, spec, z_min, t_low, t_high) for z_min, t_low, t_high in crossings]
    spec["sha256"] = spec_sha256(spec)
    return spec
