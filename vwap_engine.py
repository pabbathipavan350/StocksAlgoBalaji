# ============================================================
# VWAP_ENGINE.PY — Multi-Signal VWAP Tracker
# ============================================================
#
# SIGNAL TYPES:
#
#  1. GAP_REVERSAL  (existing)
#     Gap Up  stock → price crosses BELOW VWAP → SHORT
#     Gap Down stock → price crosses ABOVE VWAP → LONG
#
#  2. TREND_RIDE  (new)
#     Price stays on one side of VWAP for 15+ consecutive minutes
#     from market open → wait for pullback touch → enter in trend dir
#     Long:  15min above VWAP, pulls back near VWAP → LONG
#     Short: 15min below VWAP, bounces near VWAP  → SHORT
#
#  3. VWAP_BREAKOUT  (new)
#     VWAP flat for FLAT_MIN_MINUTES then suddenly slopes +/-
#     AND price moves >0.5% from VWAP with volume spike → LONG/SHORT
#
#  4. TREND_CONTINUATION  (new — for non-gap stocks in watchlist)
#     Price above/below VWAP for 20+ bars with sloping VWAP
#     Enter on pullback to VWAP
#
# ============================================================

import logging
import datetime
import collections
from typing import Optional, Tuple, Dict, List

import config

logger = logging.getLogger(__name__)


def now_ist() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


# ── Tuneable constants (override in config.py if needed) ──
MIN_TICKS_FOR_SIGNAL  = 10
TREND_RIDE_MINUTES    = 15       # minutes price must stay on one VWAP side
TREND_PULLBACK_PCT    = 0.3      # how close to VWAP = "pullback touch" (%)
FLAT_SLOPE_THRESHOLD  = 0.008    # % per minute considered flat VWAP
FLAT_MIN_MINUTES      = getattr(config, "FLAT_MIN_MINUTES",  90)
BREAKOUT_DIST_PCT     = getattr(config, "BREAKOUT_DIST_PCT", 0.5)
BREAKOUT_VOL_MULT     = getattr(config, "BREAKOUT_VOL_MULT", 1.8)
CROSS_BUFFER_PCT      = 0.05


class MinuteBar:
    __slots__ = ("minute", "vwap", "ltp", "volume", "above_vwap")
    def __init__(self, minute, vwap, ltp, volume, above_vwap):
        self.minute     = minute
        self.vwap       = vwap
        self.ltp        = ltp
        self.volume     = volume
        self.above_vwap = above_vwap


class VWAPTracker:
    SIG_GAP_REVERSAL       = "GAP_REVERSAL"
    SIG_TREND_RIDE         = "TREND_RIDE"
    SIG_VWAP_BREAKOUT      = "VWAP_BREAKOUT"
    SIG_TREND_CONTINUATION = "TREND_CONTINUATION"

    def __init__(self, symbol: str, gap_direction: str = "NONE"):
        self.symbol        = symbol
        self.gap_direction = gap_direction

        # VWAP accumulators
        self._cum_tp_vol = 0.0
        self._cum_vol    = 0.0
        self._tick_count = 0

        # Live state
        self.ltp           = 0.0
        self.vwap          = 0.0
        self.volume_total  = 0.0
        self.was_above     = None
        self.last_tick_ts  = None

        # Per-minute history
        self._bars: collections.deque = collections.deque(maxlen=400)
        self._last_bar_minute = -1
        self._bar_tick_vol    = 0.0

        # Signal fired flags
        self._signals_fired = {
            self.SIG_GAP_REVERSAL      : False,
            self.SIG_TREND_RIDE        : False,
            self.SIG_VWAP_BREAKOUT     : False,
            self.SIG_TREND_CONTINUATION: False,
        }

        # Trend Ride state
        self._trend_direction = None
        self._trend_confirmed = False

        # VWAP Breakout state
        self._flat_start_minute = None
        self._flat_avg_vol      = 0.0

    # ── Tick ─────────────────────────────────────────────

    def on_tick(self, tick: dict):
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

        tick_vol          = vol if vol > 0 else 1.0
        self._cum_tp_vol += ((high + low + ltp) / 3.0) * tick_vol
        self._cum_vol    += tick_vol
        self._bar_tick_vol += tick_vol
        self.volume_total  = self._cum_vol

        if self._cum_vol > 0:
            self.vwap = self._cum_tp_vol / self._cum_vol

        self._update_minute_bar()

        if self.vwap > 0:
            buf = self.vwap * (CROSS_BUFFER_PCT / 100.0)
            if ltp > self.vwap + buf:
                self.was_above = True
            elif ltp < self.vwap - buf:
                self.was_above = False

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
            ))
            self._last_bar_minute = mins
            self._bar_tick_vol    = 0.0

    # ── Master signal check ───────────────────────────────

    def check_signal(self) -> Tuple[bool, str, str]:
        """Returns (fired, signal_type, direction='LONG'|'SHORT')"""
        if self._tick_count < MIN_TICKS_FOR_SIGNAL:
            return False, "", ""
        if self.vwap <= 0 or self.ltp <= 0:
            return False, "", ""

        # Priority order: reversal first, then trend, then breakout
        if (self.gap_direction in ("GAP_UP", "GAP_DOWN")
                and not self._signals_fired[self.SIG_GAP_REVERSAL]):
            f, d = self._check_gap_reversal()
            if f:
                self._signals_fired[self.SIG_GAP_REVERSAL] = True
                return True, self.SIG_GAP_REVERSAL, d

        if not self._signals_fired[self.SIG_TREND_RIDE]:
            f, d = self._check_trend_ride()
            if f:
                self._signals_fired[self.SIG_TREND_RIDE] = True
                return True, self.SIG_TREND_RIDE, d

        if not self._signals_fired[self.SIG_VWAP_BREAKOUT]:
            f, d = self._check_vwap_breakout()
            if f:
                self._signals_fired[self.SIG_VWAP_BREAKOUT] = True
                return True, self.SIG_VWAP_BREAKOUT, d

        if (self.gap_direction == "NONE"
                and not self._signals_fired[self.SIG_TREND_CONTINUATION]):
            f, d = self._check_trend_continuation()
            if f:
                self._signals_fired[self.SIG_TREND_CONTINUATION] = True
                return True, self.SIG_TREND_CONTINUATION, d

        return False, "", ""

    # ── Signal 1: Gap Reversal ────────────────────────────

    def _check_gap_reversal(self) -> Tuple[bool, str]:
        buf   = self.vwap * (CROSS_BUFFER_PCT / 100.0)
        above = self.ltp > self.vwap + buf
        below = self.ltp < self.vwap - buf

        if self.was_above is None:
            self.was_above = above
            return False, ""

        prev = self.was_above
        if above:
            self.was_above = True
        elif below:
            self.was_above = False

        if self.gap_direction == "GAP_UP"   and prev and below:
            return True, "SHORT"
        if self.gap_direction == "GAP_DOWN" and not prev and above:
            return True, "LONG"
        return False, ""

    # ── Signal 2: Trend Ride ──────────────────────────────

    def _check_trend_ride(self) -> Tuple[bool, str]:
        bars = list(self._bars)
        if len(bars) < TREND_RIDE_MINUTES:
            return False, ""

        if not self._trend_confirmed:
            recent = bars[-TREND_RIDE_MINUTES:]
            if all(b.above_vwap for b in recent):
                self._trend_direction = "LONG"
                self._trend_confirmed = True
                logger.info(f"[TrendRide] {self.symbol}: LONG confirmed "
                            f"({TREND_RIDE_MINUTES}min above VWAP)")
            elif all(not b.above_vwap for b in recent):
                self._trend_direction = "SHORT"
                self._trend_confirmed = True
                logger.info(f"[TrendRide] {self.symbol}: SHORT confirmed "
                            f"({TREND_RIDE_MINUTES}min below VWAP)")

        if self._trend_confirmed and self._trend_direction:
            dist_pct = abs(self.ltp - self.vwap) / self.vwap * 100
            if dist_pct <= TREND_PULLBACK_PCT:
                if self._trend_direction == "LONG"  and self.ltp >= self.vwap * 0.997:
                    return True, "LONG"
                if self._trend_direction == "SHORT" and self.ltp <= self.vwap * 1.003:
                    return True, "SHORT"

        return False, ""

    # ── Signal 3: VWAP Breakout ───────────────────────────

    def _check_vwap_breakout(self) -> Tuple[bool, str]:
        bars = list(self._bars)
        if len(bars) < 20:
            return False, ""

        recent_5  = bars[-5:]
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

        dist_pct    = (self.ltp - self.vwap) / self.vwap * 100
        cur_vol     = bars[-1].volume if bars[-1].volume > 0 else 1
        vol_ratio   = cur_vol / self._flat_avg_vol if self._flat_avg_vol > 0 else 0

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

    # ── Signal 4: Trend Continuation ─────────────────────

    def _check_trend_continuation(self) -> Tuple[bool, str]:
        bars = list(self._bars)
        if len(bars) < 25:
            return False, ""

        recent_20 = bars[-20:]
        all_above = all(b.above_vwap for b in recent_20)
        all_below = all(not b.above_vwap for b in recent_20)

        if not (all_above or all_below):
            return False, ""

        slope_20 = self._slope_pct_per_min(recent_20)
        dist_pct = abs(self.ltp - self.vwap) / self.vwap * 100

        if all_above and slope_20 > FLAT_SLOPE_THRESHOLD and dist_pct <= TREND_PULLBACK_PCT:
            return True, "LONG"
        if all_below and slope_20 < -FLAT_SLOPE_THRESHOLD and dist_pct <= TREND_PULLBACK_PCT:
            return True, "SHORT"

        return False, ""

    # ── Helpers ───────────────────────────────────────────

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
        return self._slope_pct_per_min(bars[-5:]) if len(bars) >= 5 else 0.0

    def get_flat_duration(self) -> int:
        if self._flat_start_minute is None:
            return 0
        bars = list(self._bars)
        return (bars[-1].minute - self._flat_start_minute) if bars else 0

    def get_trend_duration(self) -> int:
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

    def mark_signal_used(self, signal_type: str = None):
        if signal_type:
            self._signals_fired[signal_type] = True
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

    def get_state(self) -> dict:
        return {
            "symbol"         : self.symbol,
            "ltp"            : self.ltp,
            "vwap"           : self.vwap,
            "was_above"      : self.was_above,
            "tick_count"     : self._tick_count,
            "gap_direction"  : self.gap_direction,
            "vwap_slope"     : round(self.get_vwap_slope(), 5),
            "flat_duration"  : self.get_flat_duration(),
            "trend_duration" : self.get_trend_duration(),
            "trend_confirmed": self._trend_confirmed,
            "trend_direction": self._trend_direction,
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

    def on_tick(self, token: str, tick: dict):
        tracker = self._token_to_tracker.get(token)
        if tracker:
            tracker.on_tick(tick)

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
                })
        return signals

    @property
    def active_tokens(self) -> list:
        return list(self._token_to_tracker.keys())

    @property
    def active_count(self) -> int:
        return len(self._token_to_tracker)
