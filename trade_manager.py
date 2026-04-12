# ============================================================
# TRADE_MANAGER.PY — Position & Order Management
# ============================================================
# Manages up to MAX_SIMULTANEOUS open equity intraday trades.
#
# Per trade:
#   - Capital allocation: Rs 25,000 per trade
#   - Intraday leverage: 4x (so buying power = Rs 1,00,000)
#   - SL: 0.5% from entry
#   - Trail: activates at +1%, SL ratchets to peak - 0.5%
#   - Hard target: 2.5%
#   - Paper mode: simulated fills, full P&L log
# ============================================================

import logging
import datetime
from typing import Dict, Optional

import config

logger = logging.getLogger(__name__)


def now_ist() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


def calc_qty(entry_price: float) -> int:
    """
    Calculate share quantity for Rs 25,000 allocated capital.
    With 4x leverage, buying power = Rs 1,00,000.
    We deploy Rs 25,000 of capital per trade (= 4× = Rs 1,00,000 exposure).
    """
    if entry_price <= 0:
        return 1
    exposure = config.CAPITAL_PER_TRADE * config.LEVERAGE   # Rs 1,00,000
    qty      = int(exposure / entry_price)
    return max(1, qty)


def calc_trade_cost(entry: float, exit_p: float, qty: int, side: str) -> float:
    """
    Calculate approximate transaction costs for equity intraday.
    side: 'LONG' or 'SHORT'
    """
    turnover  = (entry + exit_p) * qty
    buy_val   = entry  * qty
    sell_val  = exit_p * qty

    stt       = sell_val * config.STT_INTRADAY_PCT   # STT on sell side
    txn       = turnover * config.EXCHANGE_TXN_PCT
    sebi      = turnover * config.SEBI_PCT
    stamp     = buy_val  * config.STAMP_DUTY_PCT
    gst       = (txn + sebi) * config.GST_PCT
    return round(stt + txn + sebi + stamp + gst, 2)


# ──────────────────────────────────────────────────────────
#  Single Trade State
# ──────────────────────────────────────────────────────────

class Trade:
    _id_counter = 0

    def __init__(self, symbol: str, token: str, direction: str,
                 entry_price: float, entry_time: datetime.datetime,
                 entry_vwap: float, gap_pct: float, gap_direction: str, signal_type: str = "GAP_REVERSAL"):
        Trade._id_counter += 1
        self.trade_id      = Trade._id_counter
        self.symbol        = symbol
        self.token         = token
        self.direction     = direction          # 'LONG' or 'SHORT'
        self.gap_direction = gap_direction      # 'GAP_UP' or 'GAP_DOWN'
        self.gap_pct       = gap_pct
        self.signal_type   = signal_type
        self.entry_price   = entry_price
        self.entry_time    = entry_time
        self.entry_vwap    = entry_vwap
        self.qty           = calc_qty(entry_price)
        self.exposure      = round(entry_price * self.qty, 2)

        # SL & Target
        sl_offset          = entry_price * (config.SL_PCT / 100.0)
        tgt_offset         = entry_price * (config.TARGET_PCT / 100.0)

        if direction == "LONG":
            self.sl_price     = round(entry_price - sl_offset,  2)
            self.target_price = round(entry_price + tgt_offset, 2)
        else:  # SHORT
            self.sl_price     = round(entry_price + sl_offset,  2)
            self.target_price = round(entry_price - tgt_offset, 2)

        # Trail state
        self.peak_price    = entry_price
        self.trail_active  = False

        # Exit state
        self.exit_price    = 0.0
        self.exit_time     = None
        self.exit_reason   = ""
        self.is_open       = True

        # Live LTP
        self.ltp           = entry_price

    def update_ltp(self, ltp: float):
        """Call on every tick for this position. Returns exit_reason if SL/Target hit."""
        if not self.is_open or ltp <= 0:
            return None

        self.ltp = ltp

        # Update peak
        if self.direction == "LONG":
            if ltp > self.peak_price:
                self.peak_price = ltp
        else:  # SHORT
            if ltp < self.peak_price:
                self.peak_price = ltp

        # ── Trailing SL update ────────────────────────────
        trail_trigger = self.entry_price * (config.TRAIL_TRIGGER_PCT / 100.0)
        trail_buffer  = self.peak_price  * (config.TRAIL_BUFFER_PCT  / 100.0)

        if self.direction == "LONG":
            profit = ltp - self.entry_price
            if profit >= trail_trigger:
                new_sl = round(self.peak_price - trail_buffer, 2)
                if new_sl > self.sl_price:
                    self.sl_price    = new_sl
                    self.trail_active = True
        else:  # SHORT
            profit = self.entry_price - ltp
            if profit >= trail_trigger:
                new_sl = round(self.peak_price + trail_buffer, 2)
                if new_sl < self.sl_price:
                    self.sl_price    = new_sl
                    self.trail_active = True

        # ── Target check ──────────────────────────────────
        if self.direction == "LONG" and ltp >= self.target_price:
            return "Target"
        if self.direction == "SHORT" and ltp <= self.target_price:
            return "Target"

        # ── SL check ─────────────────────────────────────
        if self.direction == "LONG" and ltp <= self.sl_price:
            return "Trail SL" if self.trail_active else "SL"
        if self.direction == "SHORT" and ltp >= self.sl_price:
            return "Trail SL" if self.trail_active else "SL"

        return None

    def close(self, exit_price: float, reason: str):
        self.is_open    = False
        self.exit_price = exit_price
        self.exit_time  = now_ist()
        self.exit_reason = reason

    @property
    def unrealised_pnl(self) -> float:
        if self.direction == "LONG":
            return round((self.ltp - self.entry_price) * self.qty, 2)
        else:
            return round((self.entry_price - self.ltp) * self.qty, 2)

    @property
    def realised_pnl(self) -> float:
        if not self.exit_price:
            return 0.0
        if self.direction == "LONG":
            return round((self.exit_price - self.entry_price) * self.qty, 2)
        else:
            return round((self.entry_price - self.exit_price) * self.qty, 2)

    @property
    def net_pnl(self) -> float:
        cost = calc_trade_cost(self.entry_price, self.exit_price or self.ltp,
                               self.qty, self.direction)
        return round(self.realised_pnl - cost, 2)

    @property
    def duration_mins(self) -> float:
        if not self.exit_time:
            return round((now_ist() - self.entry_time).total_seconds() / 60, 1)
        return round((self.exit_time - self.entry_time).total_seconds() / 60, 1)


# ──────────────────────────────────────────────────────────
#  Trade Manager
# ──────────────────────────────────────────────────────────

class TradeManager:
    """
    Manages all open trades.
    - Enforces MAX_SIMULTANEOUS limit
    - Handles paper mode order simulation
    - Delegates SL/trail/target monitoring to Trade objects
    """

    def __init__(self, client, report_manager):
        self.client      = client
        self.report_mgr  = report_manager
        self._open: Dict[str, Trade]   = {}   # symbol → Trade
        self._closed: list             = []
        self.day_pnl_rs  = 0.0
        self.trade_count = 0
        self.consec_sl   = 0
        self.day_stopped = False

    # ── Entry ─────────────────────────────────────────────

    def can_enter(self, symbol: str) -> bool:
        if self.day_stopped:
            return False
        if symbol in self._open:
            return False   # already in trade for this symbol
        # In paper mode — only block if already in this symbol or day stopped
        # No limit on simultaneous trades or daily trade count
        if config.PAPER_TRADE:
            return True
        # Live mode — enforce all limits
        if len(self._open) >= config.MAX_SIMULTANEOUS:
            return False
        if self.trade_count >= config.MAX_TRADES_PER_DAY:
            return False
        if self.day_pnl_rs <= config.MAX_DAILY_LOSS_RS:
            return False
        return True

    def enter(self, symbol: str, token: str, direction: str,
              ltp: float, vwap: float, gap_pct: float,
              gap_direction: str, signal_type: str = "GAP_REVERSAL") -> Optional[Trade]:
        """Open a new trade. Paper: fill at LTP. Live: limit order."""
        if not self.can_enter(symbol):
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
        print(f"   Signal    : {signal_type}")
        print(f"   Entry     : ₹{entry_price:.2f}  Qty: {trade.qty}  "
              f"Exposure: ₹{trade.exposure:,.0f}")
        print(f"   VWAP      : ₹{vwap:.2f}")
        print(f"   SL        : ₹{trade.sl_price:.2f}  ({config.SL_PCT}%)")
        print(f"   Target    : ₹{trade.target_price:.2f}  ({config.TARGET_PCT}%)")
        print(f"   Trail     : activates at +{config.TRAIL_TRIGGER_PCT}%")
        limit_str = "unlimited" if config.PAPER_TRADE else str(config.MAX_SIMULTANEOUS)
        print(f"   Open      : {len(self._open)}/{limit_str}")
        return trade

    # ── Tick Update ───────────────────────────────────────

    def on_tick(self, token: str, ltp: float) -> Optional[str]:
        """
        Update all positions with this token's LTP.
        Returns symbol if exit triggered, else None.
        """
        for symbol, trade in list(self._open.items()):
            if trade.token != token:
                continue
            reason = trade.update_ltp(ltp)
            if reason:
                self.exit(symbol, ltp, reason)
                return symbol
        return None

    # ── Exit ──────────────────────────────────────────────

    def exit(self, symbol: str, ltp: float, reason: str) -> Optional[Trade]:
        trade = self._open.pop(symbol, None)
        if not trade:
            return None

        exit_price = ltp
        if not config.PAPER_TRADE:
            exit_price = self._place_exit_order(trade, ltp, reason) or ltp

        trade.close(exit_price, reason)
        self._closed.append(trade)

        # Update day counters
        net = trade.net_pnl
        self.day_pnl_rs += net
        is_sl = reason in ("SL", "Trail SL")
        self.consec_sl  = (self.consec_sl + 1) if is_sl else 0

        # Daily loss guard — only enforce in live mode
        if not config.PAPER_TRADE and self.day_pnl_rs <= config.MAX_DAILY_LOSS_RS:
            print(f"\n[Guard] Daily loss limit ₹{config.MAX_DAILY_LOSS_RS:,.0f} hit — stopping")
            self.day_stopped = True

        emoji = "✅" if net >= 0 else "❌"
        pct   = round((exit_price - trade.entry_price) / trade.entry_price * 100, 2)
        mode_tag = "[PAPER]" if config.PAPER_TRADE else "[LIVE]"
        trail_tag = " 🔒trail" if trade.trail_active else ""
        print(f"\n{emoji} {mode_tag} EXIT #{trade.trade_id} — {reason}{trail_tag}")
        print(f"   {trade.direction} {symbol}  "
              f"{trade.entry_time.strftime('%H:%M')}→{trade.exit_time.strftime('%H:%M')} "
              f"({trade.duration_mins}m)")
        print(f"   Entry ₹{trade.entry_price:.2f}  Exit ₹{exit_price:.2f}  "
              f"Move {pct:+.2f}%  Peak ₹{trade.peak_price:.2f}")
        print(f"   Gross ₹{trade.realised_pnl:+.0f}  "
              f"Cost ₹{calc_trade_cost(trade.entry_price, exit_price, trade.qty, trade.direction):.0f}  "
              f"Net ₹{net:+.0f}")
        print(f"   Day P&L: ₹{self.day_pnl_rs:+,.0f}  "
              f"Consec SL: {self.consec_sl}  Open: {len(self._open)}")

        self.report_mgr.log_trade(trade)
        return trade

    def square_off_all(self):
        """Square off all open positions at EOD."""
        for symbol in list(self._open.keys()):
            trade = self._open[symbol]
            print(f"\n[SquareOff] Closing {trade.direction} {symbol} at ₹{trade.ltp:.2f}")
            self.exit(symbol, trade.ltp, "Square-off 15:15")

    # ── Live order helpers ────────────────────────────────

    def _place_limit_order(self, symbol: str, token: str,
                           direction: str, ltp: float) -> Optional[float]:
        """Place a limit entry order. Returns fill price or None."""
        txn_type = "B" if direction == "LONG" else "S"
        # Limit with small buffer for fill assurance
        limit_px = round(ltp * 1.002, 2) if direction == "LONG" else round(ltp * 0.998, 2)
        qty      = calc_qty(ltp)
        try:
            resp = self.client.place_order(
                exchange_segment = config.CM_SEGMENT,
                product          = "MIS",        # Intraday
                price            = str(limit_px),
                order_type       = "L",           # Limit
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

    def _place_exit_order(self, trade: Trade, ltp: float, reason: str) -> Optional[float]:
        """Place a market exit order."""
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

    # ── Status ────────────────────────────────────────────

    def print_status(self):
        t = now_ist()
        limit_str = "unlimited" if config.PAPER_TRADE else str(config.MAX_SIMULTANEOUS)
        print(f"\n[{t.strftime('%H:%M:%S')}] Open: {len(self._open)}/{limit_str}  "
              f"Trades today: {self.trade_count}  Day P&L: ₹{self.day_pnl_rs:+,.0f}  "
              f"Consec SL: {self.consec_sl}")
        for sym, trade in self._open.items():
            unreal = trade.unrealised_pnl
            trail_tag = "🔒" if trade.trail_active else ""
            print(f"  {trade.direction:5s} {sym:15s}  "
                  f"E=₹{trade.entry_price:.2f}  L=₹{trade.ltp:.2f}  "
                  f"SL=₹{trade.sl_price:.2f}  TGT=₹{trade.target_price:.2f}  "
                  f"Unreal=₹{unreal:+.0f}{trail_tag}")
