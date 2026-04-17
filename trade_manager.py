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
from typing import Dict, Optional, Set

import config

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
    turnover = (entry + exit_p) * qty
    buy_val  = entry  * qty
    sell_val = exit_p * qty
    stt      = sell_val * config.STT_INTRADAY_PCT
    txn      = turnover * config.EXCHANGE_TXN_PCT
    sebi     = turnover * config.SEBI_PCT
    stamp    = buy_val  * config.STAMP_DUTY_PCT
    gst      = (txn + sebi) * config.GST_PCT
    return round(stt + txn + sebi + stamp + gst, 2)


# ──────────────────────────────────────────────────────────
#  Single Trade State
# ──────────────────────────────────────────────────────────

class Trade:
    _id_counter = 0

    def __init__(self, symbol: str, token: str, direction: str,
                 entry_price: float, entry_time: datetime.datetime,
                 entry_vwap: float, gap_pct: float, gap_direction: str,
                 signal_type: str = "GAP_REVERSAL"):
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
        self.qty           = calc_qty(entry_price)
        self.exposure      = round(entry_price * self.qty, 2)

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
        # Symbols added here after any SL exit.
        # They cannot be entered again for the rest of the session.
        self._session_blocked: Set[str] = set()

        self.day_pnl_rs  = 0.0
        self.trade_count = 0
        self.consec_sl   = 0
        self.day_stopped = False

    # ──────────────────────────────────────────────────────
    #  can_enter — simplified in v4
    # ──────────────────────────────────────────────────────

    def can_enter(self, symbol: str, signal_type: str = "GAP_REVERSAL",
                  gap_direction: str = "NONE", entry_direction: str = "") -> bool:
        """
        Returns True only if ALL of the following are satisfied:
          1. Day not stopped (daily loss limit not hit)
          2. Symbol not already in an open trade
          3. Symbol not in session_blocked (previous SL today)
          4. Open trade count < MAX_SIMULTANEOUS (8)
          5. Direction lock: if gap stock, entry must match gap direction
        No MAX_TRADES_PER_DAY check. No slot-type budget.
        """
        if self.day_stopped:
            return False

        if symbol in self._open:
            return False

        # Bug fix 1: session block — never re-enter a stock that SL'd today
        if symbol in self._session_blocked:
            return False

        if len(self._open) >= config.MAX_SIMULTANEOUS:
            return False

        # Bug fix 2: direction lock for gap stocks
        if config.GAP_DIRECTION_LOCK and entry_direction and gap_direction != "NONE":
            if gap_direction == "GAP_UP" and entry_direction != "SHORT":
                logger.debug(f"[DirectionLock] {symbol}: GAP_UP stock, rejecting {entry_direction}")
                return False
            if gap_direction == "GAP_DOWN" and entry_direction != "LONG":
                logger.debug(f"[DirectionLock] {symbol}: GAP_DOWN stock, rejecting {entry_direction}")
                return False

        # Live mode only: daily loss guard
        if not config.PAPER_TRADE:
            if self.day_pnl_rs <= config.MAX_DAILY_LOSS_RS:
                return False

        return True

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
        if not config.PAPER_TRADE:
            entry_price = self._place_limit_order(symbol, token, direction, ltp)
            if not entry_price:
                return None

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
    #  on_tick
    # ──────────────────────────────────────────────────────

    def on_tick(self, token: str, ltp: float) -> Optional[str]:
        for symbol, trade in list(self._open.items()):
            if trade.token != token:
                continue
            reason = trade.update_ltp(ltp)
            if reason:
                self.exit(symbol, ltp, reason)
                return symbol
        return None

    # ──────────────────────────────────────────────────────
    #  exit
    # ──────────────────────────────────────────────────────

    def exit(self, symbol: str, ltp: float, reason: str) -> Optional[Trade]:
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
            # Bug fix 1: block this symbol for the rest of the session
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

    def _place_limit_order(self, symbol, token, direction, ltp):
        txn_type = "B" if direction == "LONG" else "S"
        limit_px = round(ltp * 1.002, 2) if direction == "LONG" else round(ltp * 0.998, 2)
        qty      = calc_qty(ltp)
        try:
            resp = self.client.place_order(
                exchange_segment = config.CM_SEGMENT,
                product          = "MIS",
                price            = str(limit_px),
                order_type       = "L",
                quantity         = qty,
                trading_symbol   = symbol,
                transaction_type = txn_type,
                instrument_token = token,
            )
            logger.info(f"[Order] Entry placed: {resp}")
            return limit_px
        except Exception as e:
            logger.error(f"[Order] Entry failed {symbol}: {e}")
            return None

    def _place_exit_order(self, trade, ltp, reason):
        txn_type = "S" if trade.direction == "LONG" else "B"
        try:
            resp = self.client.place_order(
                exchange_segment = config.CM_SEGMENT,
                product          = "MIS",
                price            = "0",
                order_type       = "MKT",
                quantity         = trade.qty,
                trading_symbol   = trade.symbol,
                transaction_type = txn_type,
                instrument_token = trade.token,
            )
            logger.info(f"[Order] Exit placed: {resp}")
            return ltp
        except Exception as e:
            logger.error(f"[Order] Exit failed {trade.symbol}: {e}")
            return None

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
