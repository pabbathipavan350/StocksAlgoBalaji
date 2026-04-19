# ============================================================
# TRADE_MANAGER.PY — v4
# ============================================================
#
# CHANGES FROM v3:
#   1. MAX_TRADES_PER_DAY check removed entirely.
#   2. Trade slot budget (TREND/OTHER split) removed entirely.
#      can_enter() only checks: day_stopped, symbol in open,
#      open count >= MAX_SIMULTANEOUS, session_blocked, direction lock.
#   3. _session_blocked: Set[str] — once a stock hits SL (hard or trail),
#      it is added here and can never be entered again today.
#      Fixes the HDBFS double-loss pattern.
#   4. GAP_DIRECTION_LOCK — for gap stocks, only entries in the
#      gap direction are allowed. GAP_UP → SHORT only. GAP_DOWN → LONG only.
#      Fixes FSL being shorted then longed on the same day.
#   5. print_status() updated — no slot counts, shows blocked count.
# ============================================================

import logging
import datetime
import threading
from typing import Dict, Optional, Set

import config
from order_manager import OrderManager

logger = logging.getLogger(__name__)


def now_ist() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


def calc_qty(entry_price: float) -> int:
    if entry_price <= 0:
        return 1
    exposure = config.CAPITAL_PER_TRADE * config.LEVERAGE
    qty      = int(exposure / entry_price)
    return max(1, qty)


def calc_trade_cost(entry: float, exit_p: float, qty: int, side: str) -> float:
    """
    Full intraday trade cost breakdown for Kotak Neo:
      - Brokerage: ₹20 flat per order × 2 orders (entry + exit) = ₹40/trade
      - STT: 0.025% of sell-side turnover
      - Exchange txn: 0.00297% of total turnover
      - SEBI: 0.0001% of total turnover
      - Stamp duty: 0.003% of buy-side value
      - GST: 18% on (exchange txn + SEBI)
    """
    turnover = (entry + exit_p) * qty
    buy_val  = entry  * qty
    sell_val = exit_p * qty
    brokerage = 2 * getattr(config, "BROKERAGE_FLAT_RS", 20)   # ₹20 × 2 orders
    stt       = sell_val * config.STT_INTRADAY_PCT
    txn       = turnover * config.EXCHANGE_TXN_PCT
    sebi      = turnover * config.SEBI_PCT
    stamp     = buy_val  * config.STAMP_DUTY_PCT
    gst       = (txn + sebi) * config.GST_PCT
    return round(brokerage + stt + txn + sebi + stamp + gst, 2)


# ──────────────────────────────────────────────────────────
#  Single Trade State
# ──────────────────────────────────────────────────────────

class Trade:
    _id_counter = 0

    def __init__(self, symbol: str, token: str, direction: str,
                 entry_price: float, entry_time: datetime.datetime,
                 entry_vwap: float, gap_pct: float, gap_direction: str,
                 signal_type: str = "GAP_REVERSAL",
                 override_qty: int = None):
        Trade._id_counter += 1
        self.trade_id      = Trade._id_counter
        self.symbol        = symbol
        self.token         = token
        self.direction     = direction
        self.gap_direction = gap_direction
        self.gap_pct       = gap_pct
        self.signal_type   = signal_type
        self.entry_price   = entry_price
        self.entry_time    = entry_time
        self.entry_vwap    = entry_vwap
        # override_qty: set when live order partially filled
        # None = use standard calc_qty (paper mode or full fill)
        self.qty      = override_qty if override_qty else calc_qty(entry_price)
        self.exposure = round(entry_price * self.qty, 2)

        # SL calculation
        is_trend    = signal_type in ("VWAP_TREND_LONG", "VWAP_TREND_SHORT")
        is_reversal = signal_type == "GAP_REVERSAL"
        vwap_buf    = entry_vwap * (getattr(config, "GAP_REVERSAL_SL_BUFFER", 0.2) / 100)

        if is_trend or is_reversal:
            # VWAP-anchored SL
            if direction == "LONG":
                self.sl_price = round(entry_vwap - vwap_buf, 2)
            else:
                self.sl_price = round(entry_vwap + vwap_buf, 2)
        else:
            # Fixed % SL for VWAP_BREAKOUT
            sl_off = entry_price * (config.SL_PCT / 100.0)
            if direction == "LONG":
                self.sl_price = round(entry_price - sl_off, 2)
            else:
                self.sl_price = round(entry_price + sl_off, 2)

        # Ensure SL is at least 0.3% from entry
        min_dist = entry_price * 0.003
        if direction == "LONG":
            self.sl_price = min(self.sl_price, entry_price - min_dist)
        else:
            self.sl_price = max(self.sl_price, entry_price + min_dist)

        tgt_off = entry_price * (config.TARGET_PCT / 100.0)
        if direction == "LONG":
            self.target_price = round(entry_price + tgt_off, 2)
        else:
            self.target_price = round(entry_price - tgt_off, 2)

        self.peak_price   = entry_price
        self.trail_active = False
        self.exit_price   = 0.0
        self.exit_time    = None
        self.exit_reason  = ""
        self.is_open      = True
        self.ltp          = entry_price

    def update_ltp(self, ltp: float):
        if not self.is_open or ltp <= 0:
            return None

        self.ltp = ltp

        if self.direction == "LONG":
            if ltp > self.peak_price:
                self.peak_price = ltp
        else:
            if ltp < self.peak_price:
                self.peak_price = ltp

        trail_trigger = self.entry_price * (config.TRAIL_TRIGGER_PCT / 100.0)
        trail_buf     = self.peak_price  * (config.TRAIL_BUFFER_PCT  / 100.0)

        if self.direction == "LONG":
            if (ltp - self.entry_price) >= trail_trigger:
                new_sl = round(self.peak_price - trail_buf, 2)
                if new_sl > self.sl_price:
                    self.sl_price     = new_sl
                    self.trail_active = True
        else:
            if (self.entry_price - ltp) >= trail_trigger:
                new_sl = round(self.peak_price + trail_buf, 2)
                if new_sl < self.sl_price:
                    self.sl_price     = new_sl
                    self.trail_active = True

        if self.direction == "LONG"  and ltp >= self.target_price:
            return "Target"
        if self.direction == "SHORT" and ltp <= self.target_price:
            return "Target"
        if self.direction == "LONG"  and ltp <= self.sl_price:
            return "Trail SL" if self.trail_active else "SL"
        if self.direction == "SHORT" and ltp >= self.sl_price:
            return "Trail SL" if self.trail_active else "SL"

        return None

    def close(self, exit_price: float, reason: str):
        self.is_open     = False
        self.exit_price  = exit_price
        self.exit_time   = now_ist()
        self.exit_reason = reason

    @property
    def unrealised_pnl(self) -> float:
        if self.direction == "LONG":
            return round((self.ltp - self.entry_price) * self.qty, 2)
        return round((self.entry_price - self.ltp) * self.qty, 2)

    @property
    def realised_pnl(self) -> float:
        if not self.exit_price:
            return 0.0
        if self.direction == "LONG":
            return round((self.exit_price - self.entry_price) * self.qty, 2)
        return round((self.entry_price - self.exit_price) * self.qty, 2)

    @property
    def net_pnl(self) -> float:
        cost = calc_trade_cost(self.entry_price, self.exit_price or self.ltp,
                               self.qty, self.direction)
        return round(self.realised_pnl - cost, 2)

    @property
    def duration_mins(self) -> float:
        end = self.exit_time or now_ist()
        return round((end - self.entry_time).total_seconds() / 60, 1)


# ──────────────────────────────────────────────────────────
#  Trade Manager
# ──────────────────────────────────────────────────────────

class TradeManager:

    def __init__(self, client, report_manager):
        self.client     = client
        self.report_mgr = report_manager

        self._open:    Dict[str, Trade] = {}
        self._closed:  list             = []

        # ── v4: session-level block list ──────────────────
        self._session_blocked: Set[str] = set()

        self.order_mgr   = OrderManager(client)
        self.day_pnl_rs  = 0.0
        self.trade_count = 0
        self.consec_sl   = 0
        self.day_stopped = False

        # ── B-02 FIX: thread lock ─────────────────────────
        # Three threads call can_enter() concurrently:
        #   - WS tick handler (_on_message)
        #   - REST gap poller (_rest_poll_loop)
        #   - REST trend scanner (_rest_trend_scan_loop)
        #
        # Without a lock, the TOCTOU race happens:
        #   Thread A: can_enter("FSL") → True  (7 open < 8 max)
        #   Thread B: can_enter("FSL") → True  (still 7 open, A hasn't written yet)
        #   Thread A: enter("FSL") → opens position
        #   Thread B: enter("FSL") → opens SECOND position on same symbol!
        #
        # Fix: use RLock (re-entrant so the same thread can acquire twice safely).
        # The lock is held for the ENTIRE can_enter → enter sequence atomically.
        # on_tick (SL/target checks) and exit also hold the lock to prevent
        # reading stale _open state while an entry is being written.
        self._lock = threading.RLock()

    # ──────────────────────────────────────────────────────
    #  can_enter — check only (no lock here, lock is in try_enter)
    # ──────────────────────────────────────────────────────

    def can_enter(self, symbol: str, signal_type: str = "GAP_REVERSAL",
                  gap_direction: str = "NONE", entry_direction: str = "") -> bool:
        """
        Pure check — returns True/False. Does NOT acquire the lock.
        Call try_enter() for the atomic check+enter sequence.
        This method is kept public so the REST trend scanner can do a
        cheap pre-screen before acquiring the lock.
        """
        if self.day_stopped:
            return False
        if symbol in self._open:
            return False
        if symbol in self._session_blocked:
            return False
        if len(self._open) >= config.MAX_SIMULTANEOUS:
            return False
        if config.GAP_DIRECTION_LOCK and entry_direction and gap_direction != "NONE":
            if gap_direction == "GAP_UP" and entry_direction != "SHORT":
                logger.debug(f"[DirectionLock] {symbol}: GAP_UP stock, rejecting {entry_direction}")
                return False
            if gap_direction == "GAP_DOWN" and entry_direction != "LONG":
                logger.debug(f"[DirectionLock] {symbol}: GAP_DOWN stock, rejecting {entry_direction}")
                return False
        if not config.PAPER_TRADE:
            if self.day_pnl_rs <= config.MAX_DAILY_LOSS_RS:
                return False
        return True

    # ──────────────────────────────────────────────────────
    #  try_enter — ATOMIC can_enter + enter under lock (B-02 FIX)
    # ──────────────────────────────────────────────────────

    def try_enter(self, symbol: str, token: str, direction: str,
                  ltp: float, vwap: float, gap_pct: float,
                  gap_direction: str,
                  signal_type: str = "GAP_REVERSAL") -> Optional["Trade"]:
        """
        B-02 FIX: Atomic check-then-enter under RLock.

        The TOCTOU race (time-of-check / time-of-use):
          Without lock:
            Thread A: can_enter("FSL") → True  (7 open positions)
            Thread B: can_enter("FSL") → True  (still sees 7, A not done yet)
            Thread A: _open["FSL"] = trade     (now 8 open)
            Thread B: _open["FSL"] = trade     (DUPLICATE! overwrites A's trade)

          With RLock:
            Thread A acquires lock → can_enter → enter → _open["FSL"] = trade
                                                                    → releases lock
            Thread B acquires lock → can_enter → False (FSL now in _open)
                                                                    → returns None

        All three calling threads (WS, REST gap, REST trend) must call
        try_enter() — not enter() directly — for the lock to be effective.
        """
        with self._lock:
            # Re-check inside the lock — this is the atomic half
            if not self.can_enter(symbol, signal_type, gap_direction, direction):
                return None
            return self.enter(symbol, token, direction, ltp, vwap,
                              gap_pct, gap_direction, signal_type)

    # ──────────────────────────────────────────────────────
    #  enter
    # ──────────────────────────────────────────────────────

    def enter(self, symbol: str, token: str, direction: str,
              ltp: float, vwap: float, gap_pct: float,
              gap_direction: str,
              signal_type: str = "GAP_REVERSAL") -> Optional[Trade]:

        if not self.can_enter(symbol, signal_type, gap_direction, direction):
            return None

        entry_price = ltp
        entry_qty   = None   # None means use calc_qty — set if partial fill
        if not config.PAPER_TRADE:
            actual_price, actual_qty = self._place_entry_order(symbol, token, direction, ltp)
            if not actual_price:
                return None
            entry_price = actual_price   # real fill price from exchange
            if actual_qty and actual_qty != calc_qty(ltp):
                entry_qty = actual_qty   # partial fill — override qty

        trade = Trade(
            symbol        = symbol,
            token         = token,
            direction     = direction,
            entry_price   = entry_price,
            entry_time    = now_ist(),
            entry_vwap    = vwap,
            gap_pct       = gap_pct,
            gap_direction = gap_direction,
            signal_type   = signal_type,
            override_qty  = entry_qty,   # actual filled qty (None = use calc_qty)
        )
        self._open[symbol] = trade
        self.trade_count  += 1

        mode_tag = "[PAPER]" if config.PAPER_TRADE else "[LIVE]"
        emoji    = "📈" if direction == "LONG" else "📉"

        print(f"\n{'='*55}")
        print(f"{emoji} {mode_tag} ENTRY #{self.trade_count} — {direction} {symbol}  [{signal_type}]")
        print(f"   Entry  : ₹{entry_price:.2f}  Qty: {trade.qty}  Exposure: ₹{trade.exposure:,.0f}")
        print(f"   VWAP   : ₹{vwap:.2f}")
        print(f"   SL     : ₹{trade.sl_price:.2f}  "
              f"(dist: {abs(entry_price - trade.sl_price)/entry_price*100:.2f}%)")
        print(f"   Target : ₹{trade.target_price:.2f}  ({config.TARGET_PCT}%)")
        print(f"   Trail  : activates at +{config.TRAIL_TRIGGER_PCT}%")
        print(f"   Open   : {len(self._open)}/{config.MAX_SIMULTANEOUS}  "
              f"Blocked: {len(self._session_blocked)}")
        return trade

    # ──────────────────────────────────────────────────────
    #  on_tick — lock protects _open reads during SL checks
    # ──────────────────────────────────────────────────────

    def on_tick(self, token: str, ltp: float) -> Optional[str]:
        with self._lock:
            for symbol, trade in list(self._open.items()):
                if trade.token != token:
                    continue
                reason = trade.update_ltp(ltp)
                if reason:
                    self.exit(symbol, ltp, reason)
                    return symbol
        return None

    # ──────────────────────────────────────────────────────
    #  exit — lock protects _open write and _session_blocked write
    # ──────────────────────────────────────────────────────

    def exit(self, symbol: str, ltp: float, reason: str) -> Optional["Trade"]:
        # Note: exit is called from on_tick (already holding lock) and from
        # square_off_all (main thread). RLock is re-entrant so nested acquire is safe.
        with self._lock:
            trade = self._open.pop(symbol, None)
            if not trade:
                return None

            exit_price = ltp
            if not config.PAPER_TRADE:
                exit_price = self._place_exit_order(trade, ltp, reason) or ltp

            trade.close(exit_price, reason)
            self._closed.append(trade)

            net    = trade.net_pnl
            is_sl  = reason in ("SL", "Trail SL")
            self.day_pnl_rs += net

            if is_sl:
                self.consec_sl += 1
                self._session_blocked.add(symbol)
                logger.info(f"[SessionBlock] {symbol} blocked after SL — "
                            f"total blocked: {len(self._session_blocked)}")
            else:
                self.consec_sl = 0

            if not config.PAPER_TRADE and self.day_pnl_rs <= config.MAX_DAILY_LOSS_RS:
                print(f"\n[Guard] Daily loss limit ₹{config.MAX_DAILY_LOSS_RS:,.0f} hit — stopping")
                self.day_stopped = True

            emoji     = "✅" if net >= 0 else "❌"
            pct       = round((exit_price - trade.entry_price) / trade.entry_price * 100, 2)
            mode_tag  = "[PAPER]" if config.PAPER_TRADE else "[LIVE]"
            trail_tag = " 🔒trail" if trade.trail_active else ""
            block_tag = " 🚫blocked" if is_sl else ""

            print(f"\n{emoji} {mode_tag} EXIT #{trade.trade_id} — {reason}{trail_tag}{block_tag}")
            print(f"   {trade.direction} {symbol}  "
                  f"{trade.entry_time.strftime('%H:%M')}→{trade.exit_time.strftime('%H:%M')} "
                  f"({trade.duration_mins}m)")
            print(f"   Entry ₹{trade.entry_price:.2f}  Exit ₹{exit_price:.2f}  "
                  f"Move {pct:+.2f}%  Peak ₹{trade.peak_price:.2f}")
            print(f"   Gross ₹{trade.realised_pnl:+.0f}  "
                  f"Cost ₹{calc_trade_cost(trade.entry_price, exit_price, trade.qty, trade.direction):.0f}  "
                  f"Net ₹{net:+.0f}")
            print(f"   Day P&L: ₹{self.day_pnl_rs:+,.0f}  "
                  f"Consec SL: {self.consec_sl}  Open: {len(self._open)}  "
                  f"Blocked: {len(self._session_blocked)}")

            self.report_mgr.log_trade(trade)
            return trade

    # ──────────────────────────────────────────────────────
    #  square_off_all
    # ──────────────────────────────────────────────────────

    def square_off_all(self):
        for symbol in list(self._open.keys()):
            trade = self._open[symbol]
            print(f"\n[SquareOff] Closing {trade.direction} {symbol} at ₹{trade.ltp:.2f}")
            self.exit(symbol, trade.ltp, "Square-off 15:10")

    # ──────────────────────────────────────────────────────
    #  Orders (live mode)
    # ──────────────────────────────────────────────────────

    def _place_entry_order(self, symbol, token, direction, ltp):
        """
        Live entry via OrderManager — market order with fill confirmation.
        Returns (actual_avg_price, actual_filled_qty) or (None, 0) on failure.
        """
        qty    = calc_qty(ltp)
        result = self.order_mgr.place_entry(symbol, token, direction, qty, ltp)
        if result and result.filled and result.filled_qty > 0:
            return result.avg_price, result.filled_qty
        return None, 0

    def _place_exit_order(self, trade, ltp, reason):
        """
        Live exit via OrderManager — market order with fill confirmation + retry.
        Returns actual avg exit price, or ltp as fallback if confirmation fails.
        """
        result = self.order_mgr.place_exit(
            symbol    = trade.symbol,
            token     = trade.token,
            direction = trade.direction,
            qty       = trade.qty,
            ltp       = ltp,
            reason    = reason,
        )
        if result and result.avg_price > 0:
            return result.avg_price
        return ltp   # fallback — should not normally happen

    # ──────────────────────────────────────────────────────
    #  Status print
    # ──────────────────────────────────────────────────────

    def print_status(self):
        t = now_ist()
        print(f"\n[{t.strftime('%H:%M:%S')}] "
              f"Open: {len(self._open)}/{config.MAX_SIMULTANEOUS}  "
              f"Trades today: {self.trade_count}  "
              f"Blocked: {len(self._session_blocked)}  "
              f"Day P&L: ₹{self.day_pnl_rs:+,.0f}  "
              f"Consec SL: {self.consec_sl}")
        for sym, trade in self._open.items():
            trail_tag = "🔒" if trade.trail_active else ""
            print(f"  {trade.direction:5s} {sym:15s}  [{trade.signal_type[:14]}]  "
                  f"E=₹{trade.entry_price:.2f}  L=₹{trade.ltp:.2f}  "
                  f"SL=₹{trade.sl_price:.2f}  TGT=₹{trade.target_price:.2f}  "
                  f"Unreal=₹{trade.unrealised_pnl:+.0f}{trail_tag}")
