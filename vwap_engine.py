# ============================================================
# VWAP_ENGINE.PY — v3 Multi-Signal VWAP Tracker
# ============================================================
#
# SIGNAL PRIORITY ORDER (highest to lowest):
#
#  1. VWAP_TREND_LONG / VWAP_TREND_SHORT  ← PRIMARY (new in v3)
#     Price stays on ONE SIDE of a RISING/FALLING VWAP for 20+ bars
#     → Wait for price to pull back and TOUCH VWAP → Enter in trend direction
#     This is the INDIANB trade, RAILTEL, GALLANTT, DEEPAKFERT, AFCONS pattern
#     Long:  Price above rising VWAP 20+ bars, pulls back to touch VWAP → LONG
#     Short: Price below falling VWAP 20+ bars, bounces up to touch VWAP → SHORT
#
#  2. GAP_REVERSAL  ← SECONDARY (stricter entry in v3)
#     Gap Up stock: price crosses BELOW VWAP and STAYS below for 3 bars → SHORT
#     Gap Down stock: price crosses ABOVE VWAP and STAYS above for 3 bars → LONG
#     v2 entered on the FIRST cross → 89% SL rate
#     v3 requires 3 bar confirmation → much higher quality
#
#  3. VWAP_BREAKOUT  ← TERTIARY
#     VWAP flat for 90+ mins → sudden slope + price break + vol spike
#     Can fire later in the day (e.g. GALLANTT at 11:30, NETWEB at 9:30)
#
# VWAP DATA SOURCES (in priority):
#   1. Exchange 'ap' field on WS tick — the exchange's own VWAP calculation
#   2. Self-computed (H+L+C)/3 × volume — only when ap is missing
#
# KEY IMPROVEMENT vs v2:
#   - 3-bar confirmation for GAP_REVERSAL (was: 1 tick)
#   - VWAP slope validation for TREND signals (must be directional)
#   - Intraday volume check at signal time (min 100k shares)
#   - Trade budget awareness (trend vs reversal slot counts)
# ============================================================

import logging
import datetime
import collections
from typing import Optional, Tuple, Dict, List

import config

logger = logging.getLogger(__name__)


def now_ist() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


# ── Constants (from config with fallback defaults) ─────────
MIN_TICKS_FOR_SIGNAL  = getattr(config, "VWAP_MIN_TICKS",          10)
CROSS_BUFFER_PCT      = getattr(config, "CROSS_BUFFER_PCT",         0.05)
CROSS_CONFIRM_BARS    = getattr(config, "CROSS_CONFIRM_BARS",        3)    # NEW
TREND_CONFIRM_BARS    = getattr(config, "TREND_CONFIRM_BARS",        15)
TREND_PULLBACK_PCT    = getattr(config, "TREND_PULLBACK_PCT",        0.4)
TREND_VWAP_SLOPE_MIN  = getattr(config, "TREND_VWAP_SLOPE_MIN",     0.003)
TREND_MIN_BARS_ONSIDE = getattr(config, "TREND_MIN_CANDLES_ONSIDE", 20)
FLAT_SLOPE_THRESHOLD  = 0.008
FLAT_MIN_MINUTES      = getattr(config, "FLAT_MIN_MINUTES",          90)
BREAKOUT_DIST_PCT     = getattr(config, "BREAKOUT_DIST_PCT",         0.5)
BREAKOUT_VOL_MULT     = getattr(config, "BREAKOUT_VOL_MULT",         1.8)
MIN_INTRADAY_VOL      = getattr(config, "MIN_INTRADAY_VOLUME",       100_000)


class MinuteBar:
    __slots__ = ("minute", "vwap", "ltp", "volume", "above_vwap",
                 "from_ws", "cum_vol")

    def __init__(self, minute, vwap, ltp, volume, above_vwap,
                 from_ws=True, cum_vol=0.0):
        self.minute     = minute
        self.vwap       = vwap
        self.ltp        = ltp
        self.volume     = volume
        self.above_vwap = above_vwap
        self.from_ws    = from_ws
        self.cum_vol    = cum_vol   # cumulative intraday volume at this bar


class VWAPTracker:
    # Signal type constants
    SIG_VWAP_TREND_LONG    = "VWAP_TREND_LONG"    # PRIMARY: catch momentum rides
    SIG_VWAP_TREND_SHORT   = "VWAP_TREND_SHORT"   # PRIMARY: catch downtrend rides
    SIG_GAP_REVERSAL       = "GAP_REVERSAL"        # SECONDARY
    SIG_VWAP_BREAKOUT      = "VWAP_BREAKOUT"       # TERTIARY
    # Aliases for backward compat
    SIG_TREND_RIDE         = "VWAP_TREND_LONG"     # maps to new name

    def __init__(self, symbol: str, gap_direction: str = "NONE"):
        self.symbol        = symbol
        self.gap_direction = gap_direction

        # VWAP accumulators (for fallback self-computation)
        self._cum_tp_vol = 0.0
        self._cum_vol    = 0.0
        self._tick_count = 0

        # Live state
        self.ltp           = 0.0
        self.vwap          = 0.0
        self._last_ap      = 0.0  # last known exchange VWAP — reused when ap missing
        self.volume_total  = 0.0
        self.was_above     = None
        self.last_tick_ts  = None

        # Per-minute history (rolling 400 bars = ~6.5 hrs)
        self._bars: collections.deque = collections.deque(maxlen=400)
        self._last_bar_minute = -1
        self._bar_tick_vol    = 0.0

        # Signal fired flags
        self._signals_fired = {
            self.SIG_VWAP_TREND_LONG:  False,
            self.SIG_VWAP_TREND_SHORT: False,
            self.SIG_GAP_REVERSAL:     False,
            self.SIG_VWAP_BREAKOUT:    False,
        }

        # ── GAP_REVERSAL v3 state (3-bar confirmation) ────────
        self._cross_side       = None   # 'above' or 'below' after confirmed cross
        self._cross_bar_count  = 0      # consecutive bars on new side after cross

        # ── VWAP_TREND state ─────────────────────────────────
        self._trend_direction = None    # 'LONG' or 'SHORT' once confirmed
        self._trend_confirmed = False
        self._bars_on_trend_side = 0    # consecutive bars on trend side

        # VWAP Breakout state
        self._flat_start_minute = None
        self._flat_avg_vol      = 0.0

    # ─────────────────────────────────────────────────────────
    #  Tick handler
    # ─────────────────────────────────────────────────────────

    def on_tick(self, tick: dict, from_ws: bool = True):
        ltp_raw = tick.get("ltp") or tick.get("ltP") or tick.get("lp") or 0
        if isinstance(ltp_raw, dict):
            ltp_raw = 0
        ltp  = float(ltp_raw)
        vol  = float(tick.get("v") or tick.get("vol") or tick.get("volume") or
                     tick.get("ttv") or tick.get("trdVol") or 0)
        high = float(tick.get("h") or tick.get("high") or ltp)
        low  = float(tick.get("l") or tick.get("low")  or ltp)

        if ltp <= 0:
            return

        self.ltp          = ltp
        self.last_tick_ts = now_ist()
        self._tick_count += 1
        self._last_from_ws = from_ws

        # ── VWAP: use exchange 'ap' field (Kotak Neo WS) ────────────
        # Kotak only sends 'ap' when it changes — if missing, reuse last known value.
        # Never fall back to self-computed VWAP once we have a real exchange value.
        atp_raw = (tick.get("ap")  or tick.get("atp") or
                   tick.get("aP")  or tick.get("avg_price") or 0)
        try:
            ap_val = float(atp_raw)
        except (TypeError, ValueError):
            ap_val = 0.0

        tick_vol           = vol if vol > 0 else 1.0
        self._bar_tick_vol += tick_vol

        if ap_val > 0:
            # Fresh exchange VWAP received — update and cache it
            self._last_ap     = ap_val
            self.vwap         = ap_val
            self.volume_total = float(tick.get("v") or tick.get("ttv") or
                                      tick.get("trdVol") or tick.get("vol") or
                                      self.volume_total)
        elif self._last_ap > 0:
            # ap missing this tick (unchanged) — reuse last known exchange VWAP
            self.vwap = self._last_ap
            vol_raw = float(tick.get("v") or tick.get("ttv") or
                            tick.get("trdVol") or tick.get("vol") or 0)
            if vol_raw > 0:
                self.volume_total = vol_raw
        else:
            # No exchange VWAP yet at all — self-compute as seed only
            self._cum_tp_vol += ((high + low + ltp) / 3.0) * tick_vol
            self._cum_vol    += tick_vol
            self.volume_total = self._cum_vol
            if self._cum_vol > 0:
                self.vwap = self._cum_tp_vol / self._cum_vol

        self._update_minute_bar()

    def _update_minute_bar(self):
        t = self.last_tick_ts
        if not t or self.vwap <= 0:
            return
        market_open = t.replace(hour=9, minute=15, second=0, microsecond=0)
        mins = int((t - market_open).total_seconds() / 60)
        if mins < 0:
            return
        if mins != self._last_bar_minute:
            self._bars.append(MinuteBar(
                minute     = mins,
                vwap       = self.vwap,
                ltp        = self.ltp,
                volume     = self._bar_tick_vol,
                above_vwap = (self.ltp > self.vwap),
                from_ws    = getattr(self, "_last_from_ws", True),
                cum_vol    = self.volume_total,
            ))
            self._last_bar_minute = mins
            self._bar_tick_vol    = 0.0

    # ─────────────────────────────────────────────────────────
    #  Master signal checker
    # ─────────────────────────────────────────────────────────

    def check_signal(self) -> Tuple[bool, str, str]:
        """Returns (fired, signal_type, direction='LONG'|'SHORT')"""
        if self._tick_count < MIN_TICKS_FOR_SIGNAL:
            return False, "", ""
        if self.vwap <= 0 or self.ltp <= 0:
            return False, "", ""

        # Volume gate: don't enter on thin stocks
        if self.volume_total < MIN_INTRADAY_VOL:
            return False, "", ""

        is_gap_stock = self.gap_direction in ("GAP_UP", "GAP_DOWN")

        # ── Signal 1: VWAP_TREND (primary — both gap and non-gap stocks) ──
        if not self._signals_fired[self.SIG_VWAP_TREND_LONG]:
            f, d = self._check_vwap_trend()
            if f:
                sig = self.SIG_VWAP_TREND_LONG if d == "LONG" else self.SIG_VWAP_TREND_SHORT
                self._signals_fired[sig] = True
                return True, sig, d

        # ── Signal 2: GAP_REVERSAL (gap stocks only, 3-bar confirmed) ──────
        if is_gap_stock and not self._signals_fired[self.SIG_GAP_REVERSAL]:
            f, d = self._check_gap_reversal()
            if f:
                self._signals_fired[self.SIG_GAP_REVERSAL] = True
                return True, self.SIG_GAP_REVERSAL, d

        # ── Signal 3: VWAP_BREAKOUT ─────────────────────────────────────────
        if not self._signals_fired[self.SIG_VWAP_BREAKOUT]:
            f, d = self._check_vwap_breakout()
            if f:
                self._signals_fired[self.SIG_VWAP_BREAKOUT] = True
                return True, self.SIG_VWAP_BREAKOUT, d

        return False, "", ""

    # ─────────────────────────────────────────────────────────
    #  Signal 1: VWAP_TREND — THE PRIMARY SIGNAL
    # ─────────────────────────────────────────────────────────
    #
    # WHAT WE'RE CATCHING:
    #   Stocks like RAILTEL (gap up +10%, stays above rising VWAP all day)
    #   GALLANTT (flat morning, then explosive VWAP + price rise from 11:30)
    #   DEEPAKFERT (gap, consolidation, then VWAP starts climbing)
    #   INDIANB from yesterday (below falling VWAP for hours → SHORT)
    #   JUSTDIAL (gap down, price stays below falling VWAP → SHORT)
    #
    # CONDITIONS FOR LONG:
    #   1. Last 20 bars: price ABOVE VWAP (on the right side)
    #   2. VWAP slope is POSITIVE (rising) — min 0.003%/min
    #   3. Price pulls back to within 0.4% of VWAP (not extended)
    #   4. VWAP is still rising (slope still positive at signal time)
    #
    # CONDITIONS FOR SHORT:
    #   All inverse of above (price below falling VWAP, pulls back up)
    #
    # SL PLACEMENT: Just below VWAP (for LONG) or just above VWAP (SHORT)
    # This is tighter than a fixed % SL because if VWAP breaks, thesis is wrong.

    def _check_vwap_trend(self) -> Tuple[bool, str]:
        bars = list(self._bars)
        if len(bars) < TREND_MIN_BARS_ONSIDE:
            return False, ""

        # Need at least these many bars ALL on the same side
        recent = bars[-TREND_MIN_BARS_ONSIDE:]

        all_above = all(b.above_vwap for b in recent)
        all_below = all(not b.above_vwap for b in recent)

        if not (all_above or all_below):
            return False, ""

        # Validate VWAP is actually sloping (not flat)
        slope = self._slope_pct_per_min(recent)

        if all_above:
            # For a LONG entry, VWAP must be rising
            if slope < TREND_VWAP_SLOPE_MIN:
                return False, ""
            direction = "LONG"
        else:
            # For a SHORT entry, VWAP must be falling
            if slope > -TREND_VWAP_SLOPE_MIN:
                return False, ""
            direction = "SHORT"

        # Price must be near VWAP (pullback touch)
        dist_pct = abs(self.ltp - self.vwap) / self.vwap * 100
        if dist_pct > TREND_PULLBACK_PCT:
            return False, ""

        # Final check: price is still on the right side (not blown through)
        if direction == "LONG" and self.ltp >= self.vwap * 0.996:
            logger.info(f"[VWAPTrend] {self.symbol}: LONG signal "
                        f"({TREND_MIN_BARS_ONSIDE}bars above rising VWAP, "
                        f"slope={slope:+.4f}%/min, dist={dist_pct:.2f}%)")
            return True, "LONG"
        if direction == "SHORT" and self.ltp <= self.vwap * 1.004:
            logger.info(f"[VWAPTrend] {self.symbol}: SHORT signal "
                        f"({TREND_MIN_BARS_ONSIDE}bars below falling VWAP, "
                        f"slope={slope:+.4f}%/min, dist={dist_pct:.2f}%)")
            return True, "SHORT"

        return False, ""

    # ─────────────────────────────────────────────────────────
    #  Signal 2: GAP_REVERSAL (v3 — 3-bar confirmation)
    # ─────────────────────────────────────────────────────────
    #
    # v2 PROBLEM: entered on the FIRST tick below VWAP → 89% SL rate
    # v3 FIX: require 3 consecutive bars on the new side before entering
    #
    # How it works:
    #   GAP_UP stock: track when price crosses below VWAP
    #   Count consecutive bars where close < VWAP
    #   Once count ≥ CROSS_CONFIRM_BARS → fire SHORT signal
    #
    # This filters out:
    #   - Single candle wicks that touch VWAP and snap back
    #   - Choppy VWAP crossings (price going back and forth)
    #   - Stocks that are still in momentum mode (strong gap)

    def _check_gap_reversal(self) -> Tuple[bool, str]:
        bars = list(self._bars)
        if len(bars) < CROSS_CONFIRM_BARS + 2:
            return False, ""

        # Count recent bars on the opposite side of gap direction
        if self.gap_direction == "GAP_UP":
            # For reversal: need bars BELOW VWAP
            recent = bars[-CROSS_CONFIRM_BARS:]
            if all(not b.above_vwap for b in recent):
                # Additional filter: the bars just before should have been ABOVE
                # (confirms actual cross, not just stock that never went above)
                prior_bars = bars[-(CROSS_CONFIRM_BARS + 3):-CROSS_CONFIRM_BARS]
                if prior_bars and any(b.above_vwap for b in prior_bars):
                    buf = self.vwap * (CROSS_BUFFER_PCT / 100)
                    if self.ltp < self.vwap - buf:
                        return True, "SHORT"

        elif self.gap_direction == "GAP_DOWN":
            # For reversal: need bars ABOVE VWAP
            recent = bars[-CROSS_CONFIRM_BARS:]
            if all(b.above_vwap for b in recent):
                prior_bars = bars[-(CROSS_CONFIRM_BARS + 3):-CROSS_CONFIRM_BARS]
                if prior_bars and any(not b.above_vwap for b in prior_bars):
                    buf = self.vwap * (CROSS_BUFFER_PCT / 100)
                    if self.ltp > self.vwap + buf:
                        return True, "LONG"

        return False, ""

    # ─────────────────────────────────────────────────────────
    #  Signal 3: VWAP_BREAKOUT
    # ─────────────────────────────────────────────────────────

    def _check_vwap_breakout(self) -> Tuple[bool, str]:
        bars = list(self._bars)
        if len(bars) < 20:
            return False, ""

        ws_bars = [b for b in bars if b.from_ws]
        if len(ws_bars) < 5:
            return False, ""

        recent_5  = ws_bars[-5:]
        slope_now = self._slope_pct_per_min(recent_5)

        breaking_up   = slope_now >  FLAT_SLOPE_THRESHOLD * 3
        breaking_down = slope_now < -FLAT_SLOPE_THRESHOLD * 3

        if not (breaking_up or breaking_down):
            if abs(slope_now) <= FLAT_SLOPE_THRESHOLD:
                if self._flat_start_minute is None:
                    self._flat_start_minute = bars[-1].minute
                    self._flat_avg_vol      = self._avg_volume(bars[-10:])
            else:
                self._flat_start_minute = None
            return False, ""

        if self._flat_start_minute is None:
            return False, ""

        flat_dur = bars[-1].minute - self._flat_start_minute
        if flat_dur < FLAT_MIN_MINUTES:
            return False, ""

        dist_pct  = (self.ltp - self.vwap) / self.vwap * 100
        cur_vol   = bars[-1].volume if bars[-1].volume > 0 else 1
        vol_ratio = cur_vol / self._flat_avg_vol if self._flat_avg_vol > 0 else 0

        if breaking_up and dist_pct >= BREAKOUT_DIST_PCT and vol_ratio >= BREAKOUT_VOL_MULT:
            logger.info(f"[VWAPBreakout] {self.symbol} LONG  "
                        f"flat={flat_dur}min dist={dist_pct:.2f}% vol={vol_ratio:.1f}x")
            self._flat_start_minute = None
            return True, "LONG"

        if breaking_down and dist_pct <= -BREAKOUT_DIST_PCT and vol_ratio >= BREAKOUT_VOL_MULT:
            logger.info(f"[VWAPBreakout] {self.symbol} SHORT "
                        f"flat={flat_dur}min dist={dist_pct:.2f}% vol={vol_ratio:.1f}x")
            self._flat_start_minute = None
            return True, "SHORT"

        return False, ""

    # ─────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────

    def _slope_pct_per_min(self, bars: List[MinuteBar]) -> float:
        if len(bars) < 2:
            return 0.0
        s = bars[0].vwap
        e = bars[-1].vwap
        if s <= 0:
            return 0.0
        mins = max(bars[-1].minute - bars[0].minute, 1)
        return (e - s) / s * 100 / mins

    def _avg_volume(self, bars: List[MinuteBar]) -> float:
        vols = [b.volume for b in bars if b.volume > 0]
        return sum(vols) / len(vols) if vols else 1.0

    def get_vwap_slope(self) -> float:
        bars = list(self._bars)
        return self._slope_pct_per_min(bars[-10:]) if len(bars) >= 10 else 0.0

    def get_vwap_slope_recent(self, n: int = 5) -> float:
        bars = list(self._bars)
        return self._slope_pct_per_min(bars[-n:]) if len(bars) >= n else 0.0

    def get_flat_duration(self) -> int:
        if self._flat_start_minute is None:
            return 0
        bars = list(self._bars)
        return (bars[-1].minute - self._flat_start_minute) if bars else 0

    def get_trend_duration(self) -> int:
        """Consecutive bars all on same side of VWAP."""
        bars = list(self._bars)
        if not bars:
            return 0
        count = 0
        ref   = bars[-1].above_vwap
        for b in reversed(bars):
            if b.above_vwap == ref:
                count += 1
            else:
                break
        return count

    def get_bars_above_below(self) -> Tuple[int, int]:
        """Returns (bars_above_vwap, bars_below_vwap) in recent history."""
        bars = list(self._bars)
        above = sum(1 for b in bars if b.above_vwap)
        below = len(bars) - above
        return above, below

    def mark_signal_used(self, signal_type: str = None):
        if signal_type:
            self._signals_fired[signal_type] = True
            # Mark both trend variants if one is used
            if signal_type in (self.SIG_VWAP_TREND_LONG, self.SIG_VWAP_TREND_SHORT):
                self._signals_fired[self.SIG_VWAP_TREND_LONG] = True
                self._signals_fired[self.SIG_VWAP_TREND_SHORT] = True
        else:
            for k in self._signals_fired:
                self._signals_fired[k] = True

    def reset_signal(self, signal_type: str = None):
        if signal_type:
            self._signals_fired[signal_type] = False
        else:
            for k in self._signals_fired:
                self._signals_fired[k] = False
        self._trend_confirmed = False
        self._trend_direction = None
        self._cross_side      = None
        self._cross_bar_count = 0

    def get_state(self) -> dict:
        slope_20 = self._slope_pct_per_min(list(self._bars)[-20:]) if len(self._bars) >= 20 else 0.0
        a, b = self.get_bars_above_below()
        return {
            "symbol"         : self.symbol,
            "ltp"            : self.ltp,
            "vwap"           : self.vwap,
            "was_above"      : self.was_above,
            "tick_count"     : self._tick_count,
            "volume_total"   : self.volume_total,
            "gap_direction"  : self.gap_direction,
            "vwap_slope_5"   : round(self.get_vwap_slope_recent(5), 5),
            "vwap_slope_20"  : round(slope_20, 5),
            "flat_duration"  : self.get_flat_duration(),
            "trend_duration" : self.get_trend_duration(),
            "bars_above"     : a,
            "bars_below"     : b,
            "signals_fired"  : dict(self._signals_fired),
        }


# ──────────────────────────────────────────────────────────
#  VWAPManager
# ──────────────────────────────────────────────────────────

class VWAPManager:
    def __init__(self):
        self._token_to_tracker: Dict[str, VWAPTracker] = {}
        self._symbol_to_token:  Dict[str, str]         = {}

    def add_stock(self, symbol: str, token: str, gap_direction: str = "NONE"):
        if token not in self._token_to_tracker:
            tracker = VWAPTracker(symbol, gap_direction)
            self._token_to_tracker[token] = tracker
            self._symbol_to_token[symbol] = token

    def remove_stock(self, symbol: str):
        token = self._symbol_to_token.pop(symbol, None)
        if token:
            self._token_to_tracker.pop(token, None)

    def on_tick(self, token: str, tick: dict, from_ws: bool = True):
        tracker = self._token_to_tracker.get(token)
        if tracker:
            tracker.on_tick(tick, from_ws=from_ws)

    def get_tracker(self, symbol: str) -> Optional[VWAPTracker]:
        token = self._symbol_to_token.get(symbol)
        return self._token_to_tracker.get(token) if token else None

    def get_tracker_by_token(self, token: str) -> Optional[VWAPTracker]:
        return self._token_to_tracker.get(token)

    def check_all_signals(self) -> list:
        signals = []
        for token, tracker in list(self._token_to_tracker.items()):
            f, sig_type, direction = tracker.check_signal()
            if f:
                signals.append({
                    "symbol"    : tracker.symbol,
                    "token"     : token,
                    "sig_type"  : sig_type,
                    "direction" : direction,
                    "ltp"       : tracker.ltp,
                    "vwap"      : tracker.vwap,
                    "gap_dir"   : tracker.gap_direction,
                    "slope"     : tracker.get_vwap_slope(),
                    "flat_mins" : tracker.get_flat_duration(),
                    "trend_mins": tracker.get_trend_duration(),
                    "volume"    : tracker.volume_total,
                })
        return signals

    @property
    def active_tokens(self) -> list:
        return list(self._token_to_tracker.keys())

    @property
    def active_count(self) -> int:
        return len(self._token_to_tracker)
