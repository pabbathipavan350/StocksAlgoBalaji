# ============================================================
# VWAP_ENGINE.PY — v4 (VWAP field fix)
# ============================================================
#
# ROOT CAUSE FIX — VWAP WRONG VALUES:
#
# Kotak Neo WebSocket sends average traded price (= session VWAP) as:
#   field name: "atp"   ← this is the correct field
#
# Previous code checked: "ap", "aP", "avg_price" — NONE matched Kotak Neo.
# So ap_val was ALWAYS 0.0, always falling to self-compute fallback.
# Self-compute from scratch (e.g. from 10:31) gives wrong VWAP because
# it misses all session data from 09:15 to when algo started.
#
# Example: AEGISLOG real VWAP = 715.48 (TradingView, session from 9:15)
#          Algo computed VWAP = 701.02 (self-compute from 10:31, 7 mins only)
#          Difference = 14 rupees → WRONG SL, wrong entries
#
# FIX:
#   on_tick() now checks "atp" FIRST before any other field.
#   Priority order: atp → ap → aP → avg_price → self-compute fallback
#
# REST poll sends synthetic tick {"ltp": x, "ap": y}. 
# The "ap" field in synthetic ticks is filled from REST /depth response if
# available, otherwise 0 (falls to self-compute which is fine for REST path
# since REST ticks accumulate correctly across the full poll window).
#
# LATE START VWAP SEEDING (new in v4):
#   VWAPTracker.seed_vwap(vwap, volume) — call this at startup for any stock
#   that was already trading. Fetched via ohlc open+high+low → approximate seed.
#   See gap_scanner.py seed_gap_vwaps() for the call site.
# ============================================================

import logging
import datetime
import collections
from typing import Optional, Tuple, Dict, List

import config

logger = logging.getLogger(__name__)


def now_ist() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


MIN_TICKS_FOR_SIGNAL  = getattr(config, "VWAP_MIN_TICKS",           10)
CROSS_BUFFER_PCT      = getattr(config, "CROSS_BUFFER_PCT",          0.05)
CROSS_CONFIRM_BARS    = getattr(config, "CROSS_CONFIRM_BARS",         3)
TREND_CONFIRM_BARS    = getattr(config, "TREND_CONFIRM_BARS",         15)
TREND_PULLBACK_PCT    = getattr(config, "TREND_PULLBACK_PCT",         0.4)
TREND_VWAP_SLOPE_MIN  = getattr(config, "TREND_VWAP_SLOPE_MIN",      0.003)
TREND_MIN_BARS_ONSIDE = getattr(config, "TREND_MIN_CANDLES_ONSIDE",  20)
FLAT_SLOPE_THRESHOLD  = 0.008
FLAT_MIN_MINUTES      = getattr(config, "FLAT_MIN_MINUTES",           90)
BREAKOUT_DIST_PCT     = getattr(config, "BREAKOUT_DIST_PCT",          0.5)
BREAKOUT_VOL_MULT     = getattr(config, "BREAKOUT_VOL_MULT",          1.8)
MIN_INTRADAY_VOL      = getattr(config, "MIN_INTRADAY_VOLUME",        100_000)


class MinuteBar:
    __slots__ = ("minute", "vwap", "ltp", "volume", "above_vwap", "from_ws", "cum_vol")

    def __init__(self, minute, vwap, ltp, volume, above_vwap, from_ws=True, cum_vol=0.0):
        self.minute     = minute
        self.vwap       = vwap
        self.ltp        = ltp
        self.volume     = volume
        self.above_vwap = above_vwap
        self.from_ws    = from_ws
        self.cum_vol    = cum_vol


class VWAPTracker:
    SIG_VWAP_TREND_LONG  = "VWAP_TREND_LONG"
    SIG_VWAP_TREND_SHORT = "VWAP_TREND_SHORT"
    SIG_GAP_REVERSAL     = "GAP_REVERSAL"
    SIG_VWAP_BREAKOUT    = "VWAP_BREAKOUT"

    def __init__(self, symbol: str, gap_direction: str = "NONE"):
        self.symbol        = symbol
        self.gap_direction = gap_direction

        # Self-compute accumulators (fallback when atp not in tick)
        self._cum_tp_vol = 0.0
        self._cum_vol    = 0.0
        self._tick_count = 0
        self._seeded     = False   # True after seed_vwap() called

        self.ltp          = 0.0
        self.vwap         = 0.0
        self.volume_total = 0.0
        self.was_above    = None
        self.last_tick_ts = None

        self._bars: collections.deque = collections.deque(maxlen=400)
        self._last_bar_minute = -1
        self._bar_tick_vol    = 0.0
        self._last_from_ws    = True

        self._signals_fired = {
            self.SIG_VWAP_TREND_LONG:  False,
            self.SIG_VWAP_TREND_SHORT: False,
            self.SIG_GAP_REVERSAL:     False,
            self.SIG_VWAP_BREAKOUT:    False,
        }

        self._cross_side      = None
        self._cross_bar_count = 0
        self._flat_start_minute = None
        self._flat_avg_vol      = 0.0

    # ─────────────────────────────────────────────────────
    #  seed_vwap — call at startup when algo starts late
    # ─────────────────────────────────────────────────────

    def seed_vwap(self, vwap: float, volume: float = 0.0):
        """
        Seeds VWAP from an external source (e.g. ohlc fetch at startup).
        Called when algo starts after 9:15 and we have real session VWAP data.

        The seed becomes the starting point for self-compute accumulation.
        Any subsequent 'atp' ticks from WS will override it directly.

        Example: AEGISLOG starts at 10:31.
          seed_vwap(715.48, 87610)
          → self.vwap = 715.48 immediately
          → self-compute seeded so future ticks accumulate correctly
        """
        if vwap <= 0:
            return
        self.vwap         = vwap
        self.volume_total = volume
        self._seeded      = True

        # Seed self-compute accumulators so they're consistent
        # We set cum_tp_vol = vwap * volume so future ticks blend in correctly
        if volume > 0:
            self._cum_tp_vol = vwap * volume
            self._cum_vol    = volume
        logger.info(f"[VWAPSeed] {self.symbol}: seeded VWAP={vwap:.2f}  vol={volume:,.0f}")

    # ─────────────────────────────────────────────────────
    #  on_tick — primary data ingestion
    # ─────────────────────────────────────────────────────

    def on_tick(self, tick: dict, from_ws: bool = True):
        # ── LTP ───────────────────────────────────────────
        ltp_raw = tick.get("ltp") or tick.get("ltP") or tick.get("lp") or 0
        if isinstance(ltp_raw, dict):
            ltp_raw = 0
        ltp = float(ltp_raw)

        # ── Volume ────────────────────────────────────────
        vol = float(tick.get("v") or tick.get("vol") or tick.get("volume") or
                    tick.get("ttv") or tick.get("trdVol") or 0)

        # ── High / Low (for self-compute fallback) ────────
        high = float(tick.get("h") or tick.get("high") or ltp)
        low  = float(tick.get("l") or tick.get("low")  or ltp)

        if ltp <= 0:
            return

        self.ltp           = ltp
        self.last_tick_ts  = now_ist()
        self._tick_count  += 1
        self._last_from_ws = from_ws

        tick_vol            = vol if vol > 0 else 1.0
        self._bar_tick_vol += tick_vol

        # ── VWAP: field priority ───────────────────────────
        #
        # Kotak Neo WebSocket sends average traded price as "atp"
        # This IS the exchange-computed session VWAP (cumulative from 9:15).
        #
        # Priority:
        #   1. "atp" — Kotak Neo WS field (CORRECT, use this)
        #   2. "ap"  — other brokers / synthetic REST ticks
        #   3. "aP"  — alternate casing
        #   4. "avg_price" — generic alias
        #   5. self-compute — fallback when none of above present
        #
        atp_raw = (tick.get("atp") or tick.get("aP") or
                   tick.get("ap")  or tick.get("avg_price") or 0)
        try:
            atp_val = float(atp_raw)
        except (TypeError, ValueError):
            atp_val = 0.0

        if atp_val > 0:
            # Exchange-provided VWAP — most accurate, use directly
            self.vwap         = atp_val
            self.volume_total = float(tick.get("v") or tick.get("ttv") or
                                      tick.get("trdVol") or tick.get("vol") or
                                      self.volume_total)
            # Keep self-compute in sync so if atp disappears we continue smoothly
            if self.volume_total > 0:
                self._cum_tp_vol = atp_val * self.volume_total
                self._cum_vol    = self.volume_total

            if not self._seeded:
                self._seeded = True
                logger.info(f"[VWAP-atp] {self.symbol}: first atp tick "
                            f"atp={atp_val:.2f}  ltp={ltp:.2f}  "
                            f"vol={self.volume_total:,.0f}  from_ws={from_ws}")

        else:
            # Fallback: self-compute VWAP from (H+L+C)/3 × volume
            # Only accurate if we have data since 9:15 OR were seeded
            tp = (high + low + ltp) / 3.0
            self._cum_tp_vol += tp * tick_vol
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
                from_ws    = self._last_from_ws,
                cum_vol    = self.volume_total,
            ))
            self._last_bar_minute = mins
            self._bar_tick_vol    = 0.0

    # ─────────────────────────────────────────────────────
    #  Signal checking
    # ─────────────────────────────────────────────────────

    def check_signal(self) -> Tuple[bool, str, str]:
        if self._tick_count < MIN_TICKS_FOR_SIGNAL:
            return False, "", ""
        if self.vwap <= 0 or self.ltp <= 0:
            return False, "", ""
        if self.volume_total < MIN_INTRADAY_VOL:
            return False, "", ""

        is_gap = self.gap_direction in ("GAP_UP", "GAP_DOWN")

        # B-08 FIX: for gap stocks, evaluate GAP_REVERSAL first.
        # Previous order (VWAP_TREND first) meant a brief upward VWAP trend
        # on a GAP_UP stock could fire VWAP_TREND_LONG — direction lock blocks
        # the entry but wastes the signal slot and creates a TOCTOU window.
        # Now: gap stocks → GAP_REVERSAL first, then VWAP_TREND.
        #      non-gap stocks → VWAP_TREND first (unchanged).

        if is_gap and not self._signals_fired[self.SIG_GAP_REVERSAL]:
            f, d = self._check_gap_reversal()
            if f:
                self._signals_fired[self.SIG_GAP_REVERSAL] = True
                return True, self.SIG_GAP_REVERSAL, d

        if not (self._signals_fired[self.SIG_VWAP_TREND_LONG] and
                self._signals_fired[self.SIG_VWAP_TREND_SHORT]):
            f, d = self._check_vwap_trend()
            if f:
                sig = self.SIG_VWAP_TREND_LONG if d == "LONG" else self.SIG_VWAP_TREND_SHORT
                self._signals_fired[sig] = True
                return True, sig, d

        if not self._signals_fired[self.SIG_VWAP_BREAKOUT]:
            f, d = self._check_vwap_breakout()
            if f:
                self._signals_fired[self.SIG_VWAP_BREAKOUT] = True
                return True, self.SIG_VWAP_BREAKOUT, d

        return False, "", ""

    def _check_vwap_trend(self) -> Tuple[bool, str]:
        bars = list(self._bars)
        if len(bars) < TREND_MIN_BARS_ONSIDE:
            return False, ""

        recent     = bars[-TREND_MIN_BARS_ONSIDE:]
        all_above  = all(b.above_vwap for b in recent)
        all_below  = all(not b.above_vwap for b in recent)
        if not (all_above or all_below):
            return False, ""

        slope = self._slope_pct_per_min(recent)

        if all_above:
            if slope < TREND_VWAP_SLOPE_MIN:
                return False, ""
            dist = abs(self.ltp - self.vwap) / self.vwap * 100
            if dist > TREND_PULLBACK_PCT:
                return False, ""
            if self.ltp >= self.vwap * 0.996:
                return True, "LONG"

        if all_below:
            if slope > -TREND_VWAP_SLOPE_MIN:
                return False, ""
            dist = abs(self.ltp - self.vwap) / self.vwap * 100
            if dist > TREND_PULLBACK_PCT:
                return False, ""
            if self.ltp <= self.vwap * 1.004:
                return True, "SHORT"

        return False, ""

    def _check_gap_reversal(self) -> Tuple[bool, str]:
        bars = list(self._bars)
        if len(bars) < CROSS_CONFIRM_BARS + 2:
            return False, ""

        if self.gap_direction == "GAP_UP":
            recent = bars[-CROSS_CONFIRM_BARS:]
            if all(not b.above_vwap for b in recent):
                prior = bars[-(CROSS_CONFIRM_BARS + 3):-CROSS_CONFIRM_BARS]
                if prior and any(b.above_vwap for b in prior):
                    buf = self.vwap * (CROSS_BUFFER_PCT / 100)
                    if self.ltp < self.vwap - buf:
                        return True, "SHORT"

        elif self.gap_direction == "GAP_DOWN":
            recent = bars[-CROSS_CONFIRM_BARS:]
            if all(b.above_vwap for b in recent):
                prior = bars[-(CROSS_CONFIRM_BARS + 3):-CROSS_CONFIRM_BARS]
                if prior and any(not b.above_vwap for b in prior):
                    buf = self.vwap * (CROSS_BUFFER_PCT / 100)
                    if self.ltp > self.vwap + buf:
                        return True, "LONG"

        return False, ""

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
            self._flat_start_minute = None
            return True, "LONG"
        if breaking_down and dist_pct <= -BREAKOUT_DIST_PCT and vol_ratio >= BREAKOUT_VOL_MULT:
            self._flat_start_minute = None
            return True, "SHORT"

        return False, ""

    # ─────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────

    def _slope_pct_per_min(self, bars: List[MinuteBar]) -> float:
        if len(bars) < 2:
            return 0.0
        s    = bars[0].vwap
        e    = bars[-1].vwap
        mins = max(bars[-1].minute - bars[0].minute, 1)
        return (e - s) / s * 100 / mins if s > 0 else 0.0

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
        bars  = list(self._bars)
        above = sum(1 for b in bars if b.above_vwap)
        return above, len(bars) - above

    def mark_signal_used(self, signal_type: str = None):
        if signal_type:
            self._signals_fired[signal_type] = True
            if signal_type in (self.SIG_VWAP_TREND_LONG, self.SIG_VWAP_TREND_SHORT):
                self._signals_fired[self.SIG_VWAP_TREND_LONG]  = True
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

    def get_state(self) -> dict:
        bars     = list(self._bars)
        slope_20 = self._slope_pct_per_min(bars[-20:]) if len(bars) >= 20 else 0.0
        a, b     = self.get_bars_above_below()
        return {
            "symbol"        : self.symbol,
            "ltp"           : self.ltp,
            "vwap"          : self.vwap,
            "seeded"        : self._seeded,
            "tick_count"    : self._tick_count,
            "volume_total"  : self.volume_total,
            "gap_direction" : self.gap_direction,
            "vwap_slope_5"  : round(self.get_vwap_slope_recent(5), 5),
            "vwap_slope_20" : round(slope_20, 5),
            "flat_duration" : self.get_flat_duration(),
            "trend_duration": self.get_trend_duration(),
            "bars_above"    : a,
            "bars_below"    : b,
            "signals_fired" : dict(self._signals_fired),
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

    def seed_stock_vwap(self, symbol: str, vwap: float, volume: float = 0.0):
        """Seed VWAP for a stock by symbol name."""
        tracker = self.get_tracker(symbol)
        if tracker:
            tracker.seed_vwap(vwap, volume)

    def seed_stock_vwap_by_token(self, token: str, vwap: float, volume: float = 0.0):
        """Seed VWAP for a stock by token (used at startup seeding)."""
        tracker = self._token_to_tracker.get(token)
        if tracker:
            tracker.seed_vwap(vwap, volume)

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
