# ============================================================
# TRADE_MANAGER.PY — v4
# ============================================================
#
# CHANGES FROM v3:
#
#  BUG FIXES:
#   B-02  threading.RLock on can_enter+enter — prevents race condition
#         where 3 concurrent threads (WS, REST gap, REST trend) all
#         pass can_enter() simultaneously and open duplicate positions.
#
#   B-03  _session_blocked set — once a stock hits SL (hard or trail),
#         it is blocked for the rest of the session. Fixes the pattern
#         of losing on the same stock twice in one day (HDBFS/FSL).
#
#   B-04  GAP_DIRECTION_LOCK — GAP_UP stocks: SHORT entries only.
#         GAP_DOWN stocks: LONG entries only. Prevents going LONG on
#         a stock that just gapped up (directly against the thesis).
#
#  PAPER TRADE REALISM:
#   P-01  Depth simulator — on every paper entry, fetches real live
#         order book from Kotak Neo and walks ask/bid levels to compute
#         what a real market order would actually fill at.
#         - NORMAL: book had depth, fill is clean
#         - THIN_BOOK: had to walk deep, avg fill is honest but flag it
#         - FALLBACK_SLIPPAGE: depth fetch failed, fixed % used
#
#   P-02  Same depth walk for paper exit simulation.
#
#  LIVE MODE SL ORDER:
#   L-01  Immediately after entry confirmed, places SL-M on exchange.
#   L-02  When trail SL moves, modify_sl_order() updates trigger price.
#   L-03  Before profit exit, cancel_sl_order() first, then place_exit().
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
    turnover  = (entry + exit_p) * qty
    buy_val   = entry  * qty
    sell_val  = exit_p * qty
    stt       = sell_val * config.STT_INTRADAY_PCT
    txn       = turnover * config.EXCHANGE_TXN_PCT
    sebi      = turnover * config.SEBI_PCT
    stamp     = buy_val  * config.STAMP_DUTY_PCT
    gst       = (txn + sebi) * config.GST_PCT
    return round(stt + txn + sebi + stamp + gst, 2)


# ──────────────────────────────────────────────────────────
#  Depth Simulator — paper mode fill price engine
# ──────────────────────────────────────────────────────────

class DepthSimulator:
    """
    Fetches live order book and walks it to simulate real market order fills.
    Used ONLY in PAPER_TRADE mode.

    Fill quality tags:
      NORMAL            - clean fill within normal spread
      THIN_BOOK         - walked deep into book, price is honest but review it
      FALLBACK_SLIPPAGE - depth fetch failed, fixed slippage % used instead
    """

    def __init__(self, client):
        self.client = client

    def simulate_entry(self, symbol: str, token: str,
                       direction: str, qty: int, ltp: float) -> tuple:
        """Returns (fill_price, fill_quality_tag). Always returns a price."""
        depth = self._fetch_depth(token, symbol)
        if not depth:
            fill = self._fixed_slippage(ltp, direction, "entry")
            logger.info(f"[DepthSim] {symbol} ENTRY fallback  ltp={ltp:.2f}  fill={fill:.2f}")
            return fill, "FALLBACK_SLIPPAGE"
        # LONG = buy = walk ASK side.  SHORT = sell = walk BID side.
        levels = depth["asks"] if direction == "LONG" else depth["bids"]
        return self._walk_book(levels, qty, ltp, symbol, "ENTRY")

    def simulate_exit(self, symbol: str, token: str,
                      direction: str, qty: int, ltp: float) -> tuple:
        """Returns (fill_price, fill_quality_tag). Always returns a price."""
        depth = self._fetch_depth(token, symbol)
        if not depth:
            fill = self._fixed_slippage(ltp, direction, "exit")
            logger.info(f"[DepthSim] {symbol} EXIT fallback  ltp={ltp:.2f}  fill={fill:.2f}")
            return fill, "FALLBACK_SLIPPAGE"
        # LONG exit = sell = walk BID side.  SHORT exit = buy = walk ASK side.
        levels = depth["bids"] if direction == "LONG" else depth["asks"]
        return self._walk_book(levels, qty, ltp, symbol, "EXIT")

    def _walk_book(self, levels: list, required_qty: int,
                   ltp: float, symbol: str, side: str) -> tuple:
        max_walk   = getattr(config, "MAX_BOOK_WALK_PCT", 0.5) / 100.0
        total_cost = 0.0
        total_done = 0
        worst_px   = ltp
        thin       = False

        for price, avail_qty in levels:
            if total_done >= required_qty:
                break
            if abs(price - ltp) / ltp > max_walk:
                thin = True
                break
            take        = min(avail_qty, required_qty - total_done)
            total_cost += price * take
            total_done += take
            worst_px    = price

        # Remaining qty after walking — fill at worst level reached
        if total_done < required_qty:
            rem         = required_qty - total_done
            total_cost += worst_px * rem
            total_done += rem
            thin        = True

        avg   = round(total_cost / total_done, 2)
        tag   = "THIN_BOOK" if thin else "NORMAL"
        slip  = abs(avg - ltp) / ltp * 100
        logger.info(f"[DepthSim] {symbol} {side}  qty={required_qty}  "
                    f"ltp={ltp:.2f}  fill={avg:.2f}  slip={slip:.3f}%  quality={tag}")
        return avg, tag

    def _fetch_depth(self, token: str, symbol: str) -> Optional[dict]:
        try:
            resp = self.client.quotes(
                instrument_tokens=[{
                    "instrument_token": token,
                    "exchange_segment": config.CM_SEGMENT,
                }],
                quote_type="depth",
            )
            items = []
            if isinstance(resp, dict):
                items = resp.get("data", resp.get("success", []))
            elif isinstance(resp, list):
                items = resp
            if not items:
                return None
            item = items[0] if isinstance(items, list) else items
            if not isinstance(item, dict):
                return None

            depth_raw = (item.get("depth") or item.get("marketDepth") or
                         item.get("d") or {})
            buy_raw  = depth_raw.get("buy")  or depth_raw.get("bids") or []
            sell_raw = depth_raw.get("sell") or depth_raw.get("asks") or []

            # ── Flat format fallback (Kotak Neo quotes API) ──────────────
            # Official docs: bid keys = bp/bp1-4 + bq/bq1-4
            #                ask keys = sp/sp1-4 + bs/bs1-4  (NOT ap/aq)
            # Levels 1-5: bp1..bp5 / bq1..bq5 / sp1..sp5 / bs1..bs5
            if not buy_raw and not sell_raw:
                for i in range(1, 6):
                    bp = float(item.get(f"bp{i}") or 0)
                    bq = int(float(item.get(f"bq{i}") or 0))
                    sp = float(item.get(f"sp{i}") or 0)
                    bs = int(float(item.get(f"bs{i}") or 0))
                    if bp > 0:
                        buy_raw.append({"price": bp, "qty": bq})
                    if sp > 0:
                        sell_raw.append({"price": sp, "qty": bs})
                # Also try level-0 keys (bp/bq/sp/bs without index suffix)
                if not buy_raw and not sell_raw:
                    bp0 = float(item.get("bp") or 0)
                    bq0 = int(float(item.get("bq") or 0))
                    sp0 = float(item.get("sp") or 0)
                    bs0 = int(float(item.get("bs") or 0))
                    if bp0 > 0:
                        buy_raw.append({"price": bp0, "qty": bq0})
                    if sp0 > 0:
                        sell_raw.append({"price": sp0, "qty": bs0})
            # ─────────────────────────────────────────────────────────────

            def parse(raw, desc: bool) -> list:
                out = []
                for lvl in raw:
                    try:
                        p = float(lvl.get("price") or lvl.get("p") or
                                  lvl.get("prc")   or 0)
                        q = int(float(lvl.get("quantity") or lvl.get("qty") or
                                      lvl.get("q") or lvl.get("vol") or 0))
                        if p > 0 and q > 0:
                            out.append((p, q))
                    except (TypeError, ValueError):
                        continue
                out.sort(key=lambda x: x[0], reverse=desc)
                return out

            bids = parse(buy_raw,  desc=True)
            asks = parse(sell_raw, desc=False)
            if not bids and not asks:
                return None
            return {"bids": bids, "asks": asks}

        except Exception as e:
            logger.warning(f"[DepthSim] fetch_depth failed {symbol}: {e}")
            return None

    def _fixed_slippage(self, ltp: float, direction: str, side: str) -> float:
        if ltp >= 1000: pct = 0.08
        elif ltp >= 500: pct = 0.10
        elif ltp >= 300: pct = 0.13
        else:            pct = getattr(config, "PAPER_SLIPPAGE_PCT", 0.15)
        slip = ltp * (pct / 100)
        if side == "entry":
            return round(ltp + slip if direction == "LONG" else ltp - slip, 2)
        else:
            return round(ltp - slip if direction == "LONG" else ltp + slip, 2)


# ──────────────────────────────────────────────────────────
#  Trade
# ──────────────────────────────────────────────────────────

class Trade:
    _id_counter = 0

    def __init__(self, symbol: str, token: str, direction: str,
                 entry_price: float, entry_time: datetime.datetime,
                 entry_vwap: float, gap_pct: float, gap_direction: str,
                 signal_type: str = "GAP_REVERSAL",
                 fill_quality: str = "NORMAL",
                 override_qty: int = None):
        Trade._id_counter += 1
        self.trade_id     = Trade._id_counter
        self.symbol       = symbol
        self.token        = token
        self.direction    = direction
        self.gap_direction= gap_direction
        self.gap_pct      = gap_pct
        self.signal_type  = signal_type
        self.entry_price  = entry_price
        self.entry_time   = entry_time
        self.entry_vwap   = entry_vwap
        self.fill_quality = fill_quality   # NORMAL / THIN_BOOK / FALLBACK_SLIPPAGE / LIVE
        self.qty          = override_qty if override_qty else calc_qty(entry_price)
        self.exposure     = round(entry_price * self.qty, 2)

        # SL calculation
        is_trend       = signal_type in ("VWAP_TREND_LONG", "VWAP_TREND_SHORT")
        is_reversal    = signal_type == "GAP_REVERSAL"
        is_early_trend = signal_type == "EARLY_TREND"
        vwap_buf       = entry_vwap * (getattr(config, "GAP_REVERSAL_SL_BUFFER", 0.2) / 100)

        if is_early_trend:
            # Fixed % SL and target — no VWAP anchor, no trailing
            sl_off  = entry_price * (getattr(config, "EARLY_TREND_SL_PCT",     0.7) / 100.0)
            tgt_off = entry_price * (getattr(config, "EARLY_TREND_TARGET_PCT", 1.5) / 100.0)
            if direction == "LONG":
                self.sl_price     = round(entry_price - sl_off,  2)
                self.target_price = round(entry_price + tgt_off, 2)
            else:
                self.sl_price     = round(entry_price + sl_off,  2)
                self.target_price = round(entry_price - tgt_off, 2)
        elif is_trend or is_reversal:
            if direction == "LONG":
                self.sl_price = round(entry_vwap - vwap_buf, 2)
            else:
                self.sl_price = round(entry_vwap + vwap_buf, 2)
        else:
            sl_off = entry_price * (config.SL_PCT / 100.0)
            if direction == "LONG":
                self.sl_price = round(entry_price - sl_off, 2)
            else:
                self.sl_price = round(entry_price + sl_off, 2)

        # Ensure SL is never too close to entry (skip for EARLY_TREND — 0.7% is intentional)
        if not is_early_trend:
            min_dist = entry_price * 0.003
            if direction == "LONG":
                self.sl_price = min(self.sl_price, entry_price - min_dist)
            else:
                self.sl_price = max(self.sl_price, entry_price + min_dist)

        # EARLY_TREND already set its own target above — skip default
        if not is_early_trend:
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
        self.concurrent_at_entry = 0   # total open trades at moment this was entered (set by TradeManager)
        self.ltp          = entry_price
        self.sl_order_id  = ""   # exchange SL-M order id (live mode)

    def update_ltp(self, ltp: float):
        """
        Returns:
          None                   — no event
          "__SL_MOVED__<price>"  — trail SL just updated (not an exit)
          "Target" / "SL" / "Trail SL"  — exit event
        """
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
        trail_buffer  = self.peak_price  * (config.TRAIL_BUFFER_PCT  / 100.0)

        sl_moved = False
        # EARLY_TREND uses fixed SL only — no trailing stop
        if self.signal_type != "EARLY_TREND":
            if self.direction == "LONG":
                if (ltp - self.entry_price) >= trail_trigger:
                    new_sl = round(self.peak_price - trail_buffer, 2)
                    if new_sl > self.sl_price:
                        self.sl_price     = new_sl
                        self.trail_active = True
                        sl_moved          = True
            else:
                if (self.entry_price - ltp) >= trail_trigger:
                    new_sl = round(self.peak_price + trail_buffer, 2)
                    if new_sl < self.sl_price:
                        self.sl_price     = new_sl
                        self.trail_active = True
                        sl_moved          = True

        # Target
        if self.direction == "LONG"  and ltp >= self.target_price: return "Target"
        if self.direction == "SHORT" and ltp <= self.target_price: return "Target"

        # SL
        if self.direction == "LONG"  and ltp <= self.sl_price:
            return "Trail SL" if self.trail_active else "SL"
        if self.direction == "SHORT" and ltp >= self.sl_price:
            return "Trail SL" if self.trail_active else "SL"

        # Trail moved but no exit yet — signal to modify exchange SL order
        if sl_moved:
            return f"__SL_MOVED__{self.sl_price}"

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
    TREND_SIGNALS      = {"VWAP_TREND_LONG", "VWAP_TREND_SHORT"}
    OTHER_SIGNALS      = {"GAP_REVERSAL", "VWAP_BREAKOUT"}
    EARLY_TREND_SIGNAL = "EARLY_TREND"

    def __init__(self, client, report_manager):
        self.client      = client
        self.report_mgr  = report_manager
        self._open:   Dict[str, Trade] = {}
        self._closed: list             = []

        # v4 B-03: symbols blocked after SL for rest of session
        self._session_blocked: Set[str] = set()

        # v4 B-02: entry lock — makes can_enter+enter atomic
        # RLock so same thread can re-enter (e.g. can_enter called inside enter)
        self._entry_lock = threading.RLock()

        self.order_mgr  = OrderManager(client)
        self.depth_sim  = DepthSimulator(client)

        self.day_pnl_rs  = 0.0
        self.trade_count = 0
        self.consec_sl   = 0
        self.day_stopped = False

        self._max_trend_slots       = getattr(config, "MAX_TREND_SLOTS", 5)
        self._max_other_slots       = getattr(config, "MAX_OTHER_SLOTS", 3)
        self._max_early_trend_slots = getattr(config, "EARLY_TREND_MAX_SLOTS", 8)

    def _count_open_by_type(self) -> tuple:
        trend = sum(1 for t in self._open.values()
                    if t.signal_type in self.TREND_SIGNALS)
        return trend, len(self._open) - trend

    # ──────────────────────────────────────────────────────
    #  can_enter
    # ──────────────────────────────────────────────────────

    def can_enter(self, symbol: str, signal_type: str = "GAP_REVERSAL",
                  gap_direction: str = "NONE",
                  entry_direction: str = "") -> bool:
        if self.day_stopped:
            return False
        if symbol in self._open:
            return False
        if symbol in self._session_blocked:
            logger.debug(f"[SessionBlock] {symbol} blocked for session")
            return False

        # v4 B-04: direction lock
        # Main strategy: GAP_UP → SHORT only, GAP_DOWN → LONG only (gap fill thesis)
        # EARLY_TREND: GAP_UP → LONG (trend continuation), GAP_DOWN → SHORT
        # Direction lock does NOT apply to EARLY_TREND — skip it entirely.
        if (getattr(config, "GAP_DIRECTION_LOCK", True)
                and entry_direction
                and signal_type != self.EARLY_TREND_SIGNAL):
            if gap_direction == "GAP_UP" and entry_direction != "SHORT":
                logger.debug(f"[DirLock] {symbol}: GAP_UP, reject {entry_direction}")
                return False
            if gap_direction == "GAP_DOWN" and entry_direction != "LONG":
                logger.debug(f"[DirLock] {symbol}: GAP_DOWN, reject {entry_direction}")
                return False

        # Per-type slot check — no combined MAX_SIMULTANEOUS cap
        trend_c, other_c = self._count_open_by_type()
        if signal_type in self.TREND_SIGNALS and trend_c >= self._max_trend_slots:
            return False
        if signal_type in self.OTHER_SIGNALS and other_c >= self._max_other_slots:
            return False

        # EARLY_TREND has its own independent 8-slot limit
        if signal_type == self.EARLY_TREND_SIGNAL:
            early_c = sum(1 for t in self._open.values()
                          if t.signal_type == self.EARLY_TREND_SIGNAL)
            if early_c >= self._max_early_trend_slots:
                return False

        if not config.PAPER_TRADE:
            if self.day_pnl_rs <= config.MAX_DAILY_LOSS_RS:
                return False

        return True

    # ──────────────────────────────────────────────────────
    #  enter — atomic under _entry_lock
    # ──────────────────────────────────────────────────────

    def enter(self, symbol: str, token: str, direction: str,
              ltp: float, vwap: float, gap_pct: float,
              gap_direction: str,
              signal_type: str = "GAP_REVERSAL") -> Optional[Trade]:

        with self._entry_lock:
            # Re-check inside lock — state may have changed since outer check
            if not self.can_enter(symbol, signal_type, gap_direction, direction):
                return None

            fill_quality = "LIVE"
            entry_price  = ltp
            entry_qty    = None

            if config.PAPER_TRADE:
                # Simulate real fill from live order book
                entry_price, fill_quality = self.depth_sim.simulate_entry(
                    symbol, token, direction, calc_qty(ltp), ltp
                )
            else:
                actual = self.order_mgr.place_entry(
                    symbol, token, direction, calc_qty(ltp), ltp
                )
                if not actual or not actual.filled:
                    return None
                entry_price = actual.avg_price
                if actual.filled_qty != calc_qty(ltp):
                    entry_qty = actual.filled_qty

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
                fill_quality  = fill_quality,
                override_qty  = entry_qty,
            )
            self._open[symbol] = trade
            self.trade_count  += 1
            # Record how many trades were running at the moment this one opened
            # len(_open) already includes this trade, so subtract 1 for "others open"
            trade.concurrent_at_entry = len(self._open) - 1

        # Place SL-M on exchange after entry (live only, outside lock)
        if not config.PAPER_TRADE:
            sl_id = self.order_mgr.place_sl_order(
                symbol, token, direction, trade.qty, trade.sl_price
            )
            if sl_id:
                trade.sl_order_id = sl_id
            else:
                logger.critical(
                    f"[SLOrder] CRITICAL: SL-M failed for {direction} {symbol}  "
                    f"sl=Rs{trade.sl_price:.2f}  qty={trade.qty}. "
                    f"Set SL MANUALLY on broker app!"
                )
                print(f"\n CRITICAL: SL ORDER FAILED {symbol} "
                      f"SL=Rs{trade.sl_price:.2f} — SET MANUALLY ON BROKER APP!")

        trend_c, other_c = self._count_open_by_type()
        mode_tag = "[PAPER]" if config.PAPER_TRADE else "[LIVE]"
        emoji    = "📈" if direction == "LONG" else "📉"
        fq_tag   = (f"  [{fill_quality}]"
                    if fill_quality not in ("NORMAL", "LIVE") else "")

        print(f"\n{'='*55}")
        print(f"{emoji} {mode_tag} ENTRY #{self.trade_count} — "
              f"{direction} {symbol}  [{signal_type}]{fq_tag}")
        print(f"   Entry     : Rs{entry_price:.2f}  "
              f"(signal LTP Rs{ltp:.2f}  slip Rs{abs(entry_price-ltp):.2f})")
        print(f"   Qty       : {trade.qty}  Exposure: Rs{trade.exposure:,.0f}")
        print(f"   VWAP      : Rs{vwap:.2f}")
        print(f"   SL        : Rs{trade.sl_price:.2f}  "
              f"({abs(entry_price-trade.sl_price)/entry_price*100:.2f}%)")
        print(f"   Target    : Rs{trade.target_price:.2f}  ({config.TARGET_PCT}%)")
        print(f"   Trail     : activates at +{config.TRAIL_TRIGGER_PCT}%")
        print(f"   Slots     : Trend {trend_c}/{self._max_trend_slots}  "
              f"Other {other_c}/{self._max_other_slots}  "
              f"Blocked: {len(self._session_blocked)}  "
              f"Concurrent: {trade.concurrent_at_entry} already open")
        if not config.PAPER_TRADE and trade.sl_order_id:
            print(f"   SL-M      : Rs{trade.sl_price:.2f} placed [{trade.sl_order_id}]")
        return trade

    # ──────────────────────────────────────────────────────
    #  on_tick
    # ──────────────────────────────────────────────────────

    def on_tick(self, token: str, ltp: float) -> Optional[str]:
        for symbol, trade in list(self._open.items()):
            if trade.token != token:
                continue

            result = trade.update_ltp(ltp)

            if result is None:
                return None

            # Trail SL moved — modify exchange SL order (live only)
            if isinstance(result, str) and result.startswith("__SL_MOVED__"):
                new_sl = float(result.split("__SL_MOVED__")[1])
                if not config.PAPER_TRADE and trade.sl_order_id:
                    new_id = self.order_mgr.modify_sl_order(
                        sl_order_id  = trade.sl_order_id,
                        symbol       = symbol,
                        token        = token,
                        direction    = trade.direction,
                        qty          = trade.qty,
                        new_sl_price = new_sl,
                    )
                    if new_id:
                        trade.sl_order_id = new_id
                    logger.info(f"[Trail] {symbol} SL -> Rs{new_sl:.2f}  "
                                f"exchange order updated")
                return None  # not an exit

            # Real exit
            self.exit(symbol, ltp, result)
            return symbol

        return None

    # ──────────────────────────────────────────────────────
    #  exit
    # ──────────────────────────────────────────────────────

    def exit(self, symbol: str, ltp: float, reason: str) -> Optional[Trade]:
        trade = self._open.pop(symbol, None)
        if not trade:
            return None

        exit_price   = ltp
        fill_quality = trade.fill_quality

        if config.PAPER_TRADE:
            sim_exit, exit_fq = self.depth_sim.simulate_exit(
                symbol, trade.token, trade.direction, trade.qty, ltp
            )
            exit_price = sim_exit
            # Carry worst fill quality of entry/exit into the record
            rank = {"NORMAL": 0, "FALLBACK_SLIPPAGE": 1, "THIN_BOOK": 2, "LIVE": -1}
            if rank.get(exit_fq, 0) > rank.get(fill_quality, 0):
                fill_quality = exit_fq

        else:
            # Live: cancel SL order first, then market exit
            if trade.sl_order_id:
                self.order_mgr.cancel_sl_order(trade.sl_order_id, symbol)

            actual = self.order_mgr.place_exit(
                symbol    = symbol,
                token     = trade.token,
                direction = trade.direction,
                qty       = trade.qty,
                ltp       = ltp,
                reason    = reason,
            )
            if actual and actual.avg_price > 0:
                exit_price = actual.avg_price

        trade.fill_quality = fill_quality
        trade.close(exit_price, reason)
        self._closed.append(trade)

        net   = trade.net_pnl
        is_sl = reason in ("SL", "Trail SL")
        self.day_pnl_rs += net

        if is_sl:
            self.consec_sl += 1
            self._session_blocked.add(symbol)
            logger.info(f"[SessionBlock] {symbol} blocked after SL  "
                        f"total: {len(self._session_blocked)}")
        else:
            self.consec_sl = 0

        if not config.PAPER_TRADE and self.day_pnl_rs <= config.MAX_DAILY_LOSS_RS:
            print(f"\n[Guard] Daily loss limit Rs{config.MAX_DAILY_LOSS_RS:,.0f} hit")
            self.day_stopped = True

        emoji     = "✅" if net >= 0 else "❌"
        pct       = round((exit_price - trade.entry_price) /
                          trade.entry_price * 100, 2)
        mode_tag  = "[PAPER]" if config.PAPER_TRADE else "[LIVE]"
        trail_tag = " trail" if trade.trail_active else ""
        block_tag = " BLOCKED" if is_sl else ""
        fq_tag    = (f"  [{fill_quality}]"
                     if fill_quality not in ("NORMAL", "LIVE") else "")

        print(f"\n{emoji} {mode_tag} EXIT #{trade.trade_id} — "
              f"{reason}{trail_tag}{block_tag}{fq_tag}")
        print(f"   {trade.direction} {symbol}  "
              f"{trade.entry_time.strftime('%H:%M')}->"
              f"{trade.exit_time.strftime('%H:%M')} "
              f"({trade.duration_mins}m)")
        print(f"   Entry Rs{trade.entry_price:.2f}  Exit Rs{exit_price:.2f}  "
              f"Move {pct:+.2f}%  Peak Rs{trade.peak_price:.2f}")
        print(f"   Gross Rs{trade.realised_pnl:+.0f}  "
              f"Cost Rs{calc_trade_cost(trade.entry_price, exit_price, trade.qty, trade.direction):.0f}  "
              f"Net Rs{net:+.0f}")
        print(f"   Day P&L: Rs{self.day_pnl_rs:+,.0f}  "
              f"Consec SL: {self.consec_sl}  "
              f"Open: {len(self._open)}  "
              f"Blocked: {len(self._session_blocked)}")

        self.report_mgr.log_trade(trade)
        return trade

    # ──────────────────────────────────────────────────────
    #  square_off_all
    # ──────────────────────────────────────────────────────

    def square_off_all(self):
        for symbol in list(self._open.keys()):
            trade = self._open[symbol]
            print(f"\n[SquareOff] Closing {trade.direction} "
                  f"{symbol} at Rs{trade.ltp:.2f}")
            self.exit(symbol, trade.ltp, "Square-off 15:18")

    # ──────────────────────────────────────────────────────
    #  print_status
    # ──────────────────────────────────────────────────────

    def print_status(self):
        t = now_ist()
        trend_c, other_c = self._count_open_by_type()
        print(f"\n[{t.strftime('%H:%M:%S')}] "
              f"Open: {len(self._open)} (Trend:{trend_c}/{self._max_trend_slots} "
              f"Other:{other_c}/{self._max_other_slots})  "
              f"Blocked:{len(self._session_blocked)}  "
              f"Trades: {self.trade_count}  "
              f"Day P&L: Rs{self.day_pnl_rs:+,.0f}  "
              f"Consec SL: {self.consec_sl}")
        for sym, trade in self._open.items():
            fq_tag    = (f" [{trade.fill_quality}]"
                         if trade.fill_quality not in ("NORMAL", "LIVE") else "")
            trail_tag = "trail" if trade.trail_active else ""
            print(f"  {trade.direction:5s} {sym:15s}  "
                  f"[{trade.signal_type[:14]}]  "
                  f"E=Rs{trade.entry_price:.2f}  L=Rs{trade.ltp:.2f}  "
                  f"SL=Rs{trade.sl_price:.2f}  TGT=Rs{trade.target_price:.2f}  "
                  f"Unreal=Rs{trade.unrealised_pnl:+.0f}"
                  f"{trail_tag}{fq_tag}")
