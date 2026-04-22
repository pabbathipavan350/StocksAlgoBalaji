# ============================================================
# ORDER_MANAGER.PY — Live Order Execution & Confirmation
# ============================================================
#
# WHY THIS FILE EXISTS:
#
# The previous _place_limit_order() and _place_exit_order() in
# trade_manager.py had critical gaps for live trading:
#
#  ENTRY (was LIMIT order):
#   - Placed limit at LTP+0.2%, then IMMEDIATELY returned limit_px
#   - Never waited to see if order actually filled
#   - Never checked how many qty filled (partial fill ignored)
#   - entry_price = limit_px, not actual avgPrc from exchange
#   - SL and Target then calculated on wrong price
#
#  EXIT (was MARKET order):
#   - Placed market order, then immediately returned ltp
#   - Never confirmed fill — if order failed, position "open" on
#     exchange but algo thinks it's closed → unhedged live exposure
#   - Actual fill price = market price at execution (slippage)
#     but we recorded ltp at the moment we DECIDED to exit → wrong P&L
#
# THIS FILE FIXES ALL OF THAT:
#
#  ENTRY flow:
#   1. Place MARKET order (not limit) for guaranteed fill
#      Why market? Intraday momentum stocks — you want to be IN.
#      Missing entry by Rs 1-2 is worse than paying 0.1% slippage.
#   2. Poll order_report for up to MAX_CONFIRM_WAIT_SECS seconds
#   3. If status = CMP → return actual avgPrc and actual flQty
#   4. If status = RJT → log rejection reason, return None (no trade)
#   5. If partial fill → cancel remaining, accept partial qty
#   6. If timeout → cancel order, return None
#
#  EXIT flow:
#   1. Place MARKET order (same reasoning — must exit clean)
#   2. Poll order_report until CMP or timeout
#   3. Return actual avgPrc (the real exit price for P&L)
#   4. If partial exit → retry remaining qty with fresh market order
#   5. If complete failure → alert and keep monitoring (do not lose track)
#
# KOTAK NEO order_report fields used:
#   nOrdNo  : order number
#   ordSt   : OPN / CMP / RJT / CAN
#   flQty   : filled quantity
#   qty     : ordered quantity
#   avgPrc  : average traded price (THE real fill price)
#   rjRsn   : rejection reason (if RJT)
# ============================================================

import time
import logging
from typing import Optional, Tuple

import config

logger = logging.getLogger(__name__)

MAX_CONFIRM_WAIT_SECS = 10    # Max seconds to wait for fill confirmation
POLL_INTERVAL_SECS    = 0.5   # Poll order_report every 0.5 seconds
MAX_PARTIAL_RETRIES   = 2     # Retry partial fill this many times


class OrderResult:
    """Returned by place_entry / place_exit."""
    def __init__(self, filled: bool, avg_price: float, filled_qty: int,
                 order_id: str, reason: str = ""):
        self.filled      = filled       # True = fully or partially filled
        self.avg_price   = avg_price    # actual traded price from exchange
        self.filled_qty  = filled_qty   # actual filled quantity
        self.order_id    = order_id
        self.reason      = reason       # rejection reason if not filled

    def __repr__(self):
        return (f"OrderResult(filled={self.filled}, avg={self.avg_price:.2f}, "
                f"qty={self.filled_qty}, id={self.order_id}, reason='{self.reason}')")


class OrderManager:

    def __init__(self, client):
        self.client = client

    # ─────────────────────────────────────────────────────
    #  ENTRY — Market order with fill confirmation
    # ─────────────────────────────────────────────────────

    def place_entry(self, symbol: str, token: str, direction: str,
                    qty: int, ltp: float) -> Optional[OrderResult]:
        """
        Places a MARKET entry order and waits for fill confirmation.

        Returns OrderResult with actual avgPrc and filled_qty.
        Returns None if order rejected, failed, or timed out.

        WHY MARKET not LIMIT:
        For momentum intraday trades, missing the entry is the bigger risk.
        A LIMIT order at LTP+0.2% may not fill if price jumps 0.5% in 1 second.
        Market order guarantees entry — slippage on liquid NSE stocks is 0.05-0.1%.
        """
        txn_type = "B" if direction == "LONG" else "S"

        logger.info(f"[Order] Placing MARKET ENTRY {txn_type} {symbol}  "
                    f"qty={qty}  ltp_ref={ltp:.2f}")

        try:
            resp = self.client.place_order(
                exchange_segment = config.CM_SEGMENT,
                product          = "MIS",
                price            = "0",          # 0 = market order
                order_type       = "MKT",
                quantity         = qty,
                trading_symbol   = symbol,
                transaction_type = txn_type,
                instrument_token = token,
            )
        except Exception as e:
            logger.error(f"[Order] Entry place_order exception {symbol}: {e}")
            return None

        order_id = self._extract_order_id(resp)
        if not order_id:
            logger.error(f"[Order] Entry: no order_id in response {symbol}: {resp}")
            return None

        logger.info(f"[Order] Entry order placed — id={order_id}  "
                    f"{direction} {symbol}  qty={qty}")

        # Wait for fill confirmation
        result = self._wait_for_fill(order_id, symbol, qty, "ENTRY")

        if result and result.filled:
            logger.info(f"[Order] Entry CONFIRMED — {symbol}  "
                        f"avgPrc={result.avg_price:.2f}  filledQty={result.filled_qty}  "
                        f"(requested {qty})  slippage={result.avg_price-ltp:+.2f}")
            # If partial fill, cancel remaining
            if result.filled_qty < qty:
                self._cancel_order(order_id, symbol, "partial fill")
                logger.warning(f"[Order] Partial entry fill: {result.filled_qty}/{qty}  "
                               f"Accepting partial position")
        else:
            reason = result.reason if result else "timeout"
            logger.error(f"[Order] Entry FAILED — {symbol}: {reason}")
            return None

        return result

    # ─────────────────────────────────────────────────────
    #  EXIT — Market order with fill confirmation + retry
    # ─────────────────────────────────────────────────────

    def place_exit(self, symbol: str, token: str, direction: str,
                   qty: int, ltp: float, reason: str) -> Optional[OrderResult]:
        """
        Places a MARKET exit order and confirms fill.

        For SL exits: speed is critical, market is non-negotiable.
        For target exits: market order gets slightly less than target
        but guarantees exit. Better to book profit than chase exact price.

        If partial fill: retries remaining qty up to MAX_PARTIAL_RETRIES times.
        If complete failure: logs CRITICAL alert — operator must manually close.
        """
        txn_type = "S" if direction == "LONG" else "B"
        remaining = qty
        total_filled = 0
        total_value  = 0.0
        attempt      = 0

        logger.info(f"[Order] Placing MARKET EXIT {txn_type} {symbol}  "
                    f"qty={qty}  reason={reason}  ltp_ref={ltp:.2f}")

        while remaining > 0 and attempt <= MAX_PARTIAL_RETRIES:
            attempt += 1
            if attempt > 1:
                logger.warning(f"[Order] Exit retry {attempt} for {symbol}  "
                               f"remaining={remaining}")
                time.sleep(1.0)   # brief pause before retry

            try:
                resp = self.client.place_order(
                    exchange_segment = config.CM_SEGMENT,
                    product          = "MIS",
                    price            = "0",
                    order_type       = "MKT",
                    quantity         = remaining,
                    trading_symbol   = symbol,
                    transaction_type = txn_type,
                    instrument_token = token,
                )
            except Exception as e:
                logger.error(f"[Order] Exit place_order exception {symbol} "
                             f"attempt {attempt}: {e}")
                continue

            order_id = self._extract_order_id(resp)
            if not order_id:
                logger.error(f"[Order] Exit: no order_id in response: {resp}")
                continue

            result = self._wait_for_fill(order_id, symbol, remaining, "EXIT")

            if result and result.filled_qty > 0:
                total_filled += result.filled_qty
                total_value  += result.avg_price * result.filled_qty
                remaining    -= result.filled_qty
                logger.info(f"[Order] Exit partial fill — {result.filled_qty} @ "
                            f"₹{result.avg_price:.2f}  remaining={remaining}")
            else:
                logger.error(f"[Order] Exit attempt {attempt} failed for {symbol}")

        if total_filled == 0:
            # CRITICAL: could not exit at all
            logger.critical(
                f"[Order] ❌ CRITICAL: Could not exit {direction} {symbol}  "
                f"qty={qty}  reason={reason}. "
                f"POSITION IS STILL OPEN ON EXCHANGE. Manual intervention required!"
            )
            print(f"\n🚨 CRITICAL: EXIT FAILED for {symbol}  qty={qty}  "
                  f"MANUAL CLOSE REQUIRED on Kotak Neo app!")
            return None

        avg_exit_price = round(total_value / total_filled, 2)

        if total_filled < qty:
            logger.warning(f"[Order] Partial exit: filled {total_filled}/{qty}  "
                           f"avg={avg_exit_price:.2f}  "
                           f"UNFILLED qty={qty-total_filled} — check exchange!")
            print(f"\n⚠️  PARTIAL EXIT {symbol}: {total_filled}/{qty} filled. "
                  f"Unfilled {qty-total_filled} qty — check Kotak Neo!")

        logger.info(f"[Order] Exit CONFIRMED — {symbol}  "
                    f"avgPrc={avg_exit_price:.2f}  filledQty={total_filled}/{qty}  "
                    f"slippage={avg_exit_price-ltp:+.2f}")

        return OrderResult(
            filled     = True,
            avg_price  = avg_exit_price,
            filled_qty = total_filled,
            order_id   = "multi",
        )

    # ─────────────────────────────────────────────────────
    #  Poll order_report until filled / rejected / timeout
    # ─────────────────────────────────────────────────────

    def _wait_for_fill(self, order_id: str, symbol: str, qty: int,
                       side: str) -> Optional[OrderResult]:
        """
        Polls order_report every 0.5s until:
          - ordSt = CMP (complete) → return filled result
          - ordSt = RJT (rejected) → return failure with reason
          - ordSt = CAN (cancelled) → return failure
          - Timeout after MAX_CONFIRM_WAIT_SECS → return None

        Kotak Neo order_report response structure:
          {"data": [{"nOrdNo": "...", "ordSt": "CMP", "flQty": "130",
                     "qty": "130", "avgPrc": "717.50", "rjRsn": ""}]}
        """
        deadline = time.time() + MAX_CONFIRM_WAIT_SECS

        while time.time() < deadline:
            try:
                report = self.client.order_report()
                order  = self._find_order(report, order_id)

                if not order:
                    time.sleep(POLL_INTERVAL_SECS)
                    continue

                status = str(order.get("ordSt") or order.get("order_status") or
                             order.get("status") or "").upper()
                fl_qty = self._safe_int(order.get("flQty") or
                                        order.get("filled_qty") or
                                        order.get("filledQty") or 0)
                avg_prc = self._safe_float(order.get("avgPrc") or
                                           order.get("avg_price") or
                                           order.get("averagePrice") or 0)
                rj_rsn  = str(order.get("rjRsn") or order.get("reject_reason") or "")

                logger.debug(f"[Order] {side} poll {order_id}  "
                             f"status={status}  filled={fl_qty}/{qty}  avg={avg_prc:.2f}")

                if status in ("CMP", "COMPLETE", "COMPLETED", "TRADED"):
                    # Fully filled — use avgPrc from exchange
                    return OrderResult(
                        filled     = True,
                        avg_price  = avg_prc if avg_prc > 0 else self._safe_float(
                                        order.get("prc") or 0),
                        filled_qty = fl_qty if fl_qty > 0 else qty,
                        order_id   = order_id,
                    )

                elif status in ("RJT", "REJECTED"):
                    logger.error(f"[Order] {side} REJECTED {symbol}: {rj_rsn}")
                    return OrderResult(
                        filled     = False,
                        avg_price  = 0.0,
                        filled_qty = 0,
                        order_id   = order_id,
                        reason     = f"REJECTED: {rj_rsn}",
                    )

                elif status in ("CAN", "CANCELLED"):
                    return OrderResult(
                        filled     = fl_qty > 0,
                        avg_price  = avg_prc,
                        filled_qty = fl_qty,
                        order_id   = order_id,
                        reason     = "CANCELLED",
                    )

                # Still OPN (open/pending) — keep polling
                time.sleep(POLL_INTERVAL_SECS)

            except Exception as e:
                logger.warning(f"[Order] order_report poll error: {e}")
                time.sleep(POLL_INTERVAL_SECS)

        # Timed out
        logger.error(f"[Order] {side} TIMEOUT after {MAX_CONFIRM_WAIT_SECS}s  "
                     f"order_id={order_id}  {symbol}")
        # Try to cancel the timed-out order to avoid stray open orders
        self._cancel_order(order_id, symbol, "timeout")
        return None

    # ─────────────────────────────────────────────────────
    #  Cancel an order (used for timeout / partial cleanup)
    # ─────────────────────────────────────────────────────

    def _cancel_order(self, order_id: str, symbol: str, reason: str):
        try:
            self.client.cancel_order(order_id=order_id)
            logger.info(f"[Order] Cancelled {order_id} ({symbol}) — reason: {reason}")
        except Exception as e:
            logger.warning(f"[Order] Cancel failed {order_id}: {e}")

    # ─────────────────────────────────────────────────────
    #  SL-M Order — place, modify, cancel  (LIVE MODE ONLY)
    # ─────────────────────────────────────────────────────
    #
    #  WHY SL-M not SL-L:
    #  Gap stocks move fast. A SL-L (stop limit) may not fill if
    #  price gaps through the limit level — leaving you unprotected
    #  with a growing loss. SL-M guarantees exit the moment trigger
    #  is hit, at whatever market price is available.
    #
    #  FLOW:
    #  1. After entry fill confirmed → place_sl_order()
    #     Places SL-M at trade.sl_price as trigger.
    #     Returns sl_order_id stored on Trade object.
    #
    #  2. When trail SL moves up → modify_sl_order()
    #     Modifies the existing SL order trigger to new sl_price.
    #     Kotak Neo: modify_order() with new trigger_price.
    #     If modify fails → cancel + replace (fallback).
    #
    #  3. Before profit exit → cancel_sl_order()
    #     Must cancel BEFORE placing market exit order.
    #     If both fire simultaneously, broker rejects one as
    #     "position already closed" — creates reconciliation mess.
    # ─────────────────────────────────────────────────────

    def place_sl_order(self, symbol: str, token: str, direction: str,
                       qty: int, sl_price: float) -> str:
        """
        Places a SL-M order on the exchange immediately after entry.
        Returns the sl_order_id string, or "" on failure.

        direction = direction of the TRADE (not the SL order)
        LONG trade  → SL fires when price FALLS → txn_type = SELL
        SHORT trade → SL fires when price RISES → txn_type = BUY
        """
        txn_type = "S" if direction == "LONG" else "B"
        trigger  = str(round(sl_price, 2))

        logger.info(f"[SLOrder] Placing SL-M {txn_type} {symbol}  "
                    f"qty={qty}  trigger={trigger}")
        try:
            resp = self.client.place_order(
                exchange_segment  = config.CM_SEGMENT,
                product           = "MIS",
                price             = "0",          # SL-M — no limit price
                order_type        = "SL-M",
                quantity          = qty,
                trading_symbol    = symbol,
                transaction_type  = txn_type,
                instrument_token  = token,
                trigger_price     = trigger,
            )
            order_id = self._extract_order_id(resp)
            if order_id:
                logger.info(f"[SLOrder] ✅ SL-M placed — id={order_id}  "
                            f"{symbol}  trigger=₹{trigger}")
                return order_id
            else:
                logger.error(f"[SLOrder] No order_id in response: {resp}")
                return ""
        except Exception as e:
            logger.error(f"[SLOrder] place_sl_order failed {symbol}: {e}")
            return ""

    def modify_sl_order(self, sl_order_id: str, symbol: str, token: str,
                        direction: str, qty: int,
                        new_sl_price: float) -> str:
        """
        Modifies the trigger price of an existing SL-M order.
        Called every time trail SL moves up.

        Returns new sl_order_id (may change after modify on some brokers).
        Returns "" if modify failed — caller should treat existing id as valid
        and log a warning (old SL still protects, just not at latest trail level).

        Strategy:
          1. Try modify_order() first (cheaper, single API call)
          2. If modify fails → cancel + replace (guaranteed update)
        """
        txn_type    = "S" if direction == "LONG" else "B"
        new_trigger = str(round(new_sl_price, 2))

        logger.info(f"[SLOrder] Modifying SL-M {symbol}  "
                    f"id={sl_order_id}  new_trigger=₹{new_trigger}")
        try:
            resp = self.client.modify_order(
                order_id      = sl_order_id,
                price         = "0",
                order_type    = "SL-M",
                quantity      = qty,
                trigger_price = new_trigger,
            )
            new_id = self._extract_order_id(resp) or sl_order_id
            logger.info(f"[SLOrder] ✅ SL-M modified — id={new_id}  "
                        f"{symbol}  trigger=₹{new_trigger}")
            return new_id

        except Exception as e:
            logger.warning(f"[SLOrder] modify_order failed {symbol}, "
                           f"trying cancel+replace: {e}")
            # Fallback: cancel existing, place fresh SL-M
            self._cancel_order(sl_order_id, symbol, "modify_fallback")
            new_id = self.place_sl_order(symbol, token, direction,
                                         qty, new_sl_price)
            if new_id:
                logger.info(f"[SLOrder] ✅ SL-M cancel+replace done  "
                            f"id={new_id}  {symbol}  trigger=₹{new_trigger}")
            else:
                logger.error(f"[SLOrder] ❌ cancel+replace ALSO failed {symbol}  "
                             f"— SL protection may be lost! Check broker app.")
            return new_id

    def cancel_sl_order(self, sl_order_id: str, symbol: str) -> bool:
        """
        Cancels the standing SL-M order before placing a profit exit.
        MUST be called before place_exit() to avoid dual-exit conflict.

        Returns True if cancelled successfully (or already gone).
        Returns False if cancel failed — caller should still proceed with
        exit but log a warning so operator can check broker app.
        """
        if not sl_order_id:
            return True   # nothing to cancel
        try:
            self.client.cancel_order(order_id=sl_order_id)
            logger.info(f"[SLOrder] ✅ SL-M cancelled before profit exit  "
                        f"id={sl_order_id}  {symbol}")
            return True
        except Exception as e:
            # Common case: SL already triggered by exchange (race condition)
            # In that case position is already flat — exit will fail too,
            # which is fine. Log it and let trade_manager handle.
            err = str(e).lower()
            if any(x in err for x in ("not found", "invalid", "already",
                                       "cancelled", "completed")):
                logger.info(f"[SLOrder] SL order already gone (triggered/cancelled) "
                            f"id={sl_order_id}  {symbol}: {e}")
                return True   # treat as success — position is flat
            logger.error(f"[SLOrder] cancel_sl_order failed {symbol}  "
                         f"id={sl_order_id}: {e}")
            return False

    # ─────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────

    def _extract_order_id(self, resp) -> str:
        """Extract nOrdNo from Kotak Neo place_order response."""
        if not resp:
            return ""
        if isinstance(resp, dict):
            # {"data": {"nOrdNo": "..."}}  or  {"nOrdNo": "..."}
            data = resp.get("data") or resp
            if isinstance(data, dict):
                for f in ("nOrdNo", "order_id", "orderId", "id"):
                    v = data.get(f)
                    if v:
                        return str(v)
            # Some SDKs return {"data": [{"nOrdNo": "..."}]}
            if isinstance(data, list) and data:
                for f in ("nOrdNo", "order_id", "orderId", "id"):
                    v = data[0].get(f)
                    if v:
                        return str(v)
        return ""

    def _find_order(self, report, order_id: str) -> Optional[dict]:
        """Find a specific order by nOrdNo in order_report response."""
        if not report:
            return None
        orders = []
        if isinstance(report, dict):
            orders = report.get("data", report.get("orders", []))
        elif isinstance(report, list):
            orders = report
        for o in orders:
            if not isinstance(o, dict):
                continue
            oid = str(o.get("nOrdNo") or o.get("order_id") or
                      o.get("orderId") or "")
            if oid == order_id:
                return o
        return None

    def _safe_float(self, val) -> float:
        try:
            return float(val) if val else 0.0
        except (TypeError, ValueError):
            return 0.0

    def _safe_int(self, val) -> int:
        try:
            return int(float(val)) if val else 0
        except (TypeError, ValueError):
            return 0
