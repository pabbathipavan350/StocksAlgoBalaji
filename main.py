# ============================================================
# MAIN.PY — NSE Gap Up/Down VWAP Reversal Algo
# ============================================================
#
# STRATEGY SUMMARY:
#   1. 9:15 AM  → Scan Nifty 500 for stocks with gap >= 4%
#   2. 9:15 AM  → Subscribe to gap stocks via WebSocket
#   3. 9:15 AM  → Start VWAP calculation from tick data
#   4. 9:30 AM  → Begin accepting entries:
#                  Gap Up  stock → wait for price < VWAP → SHORT
#                  Gap Down stock → wait for price > VWAP → LONG
#   5. SL: 0.5%  | Trail: activates at +1% | Target: 2.5%
#   6. Max 8 simultaneous trades, Rs 25,000 per trade
#   7. Square off all at 3:15 PM
#
# PAPER MODE:
#   - All orders simulated (no real orders placed)
#   - Full CSV trade log + daily report generated
#   - Set PAPER_TRADE = False in config.py when ready for live
#
# RUN:
#   pip install neo_api_client pyotp
#   python main.py
# ============================================================

import threading
import signal
import logging
import logging.handlers
import os
import datetime
import time
from typing import Dict, Set

import config
from auth               import get_kotak_session
from gap_scanner        import ScripMaster, PrevCloseFetcher, GapScanner
from vwap_engine        import VWAPManager
from trade_manager      import TradeManager
from report_manager     import ReportManager
from telegram_notifier  import TelegramNotifier
from session_manager    import SessionManager


def now_ist() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


def setup_logging():
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/gap_algo_{now_ist().strftime('%Y%m%d')}.log"
    fmt      = "%(asctime)s %(levelname)-8s %(name)s | %(message)s"
    root     = logging.getLogger()
    root.setLevel(logging.DEBUG)
    fh = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=20*1024*1024, backupCount=5, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(fmt))
    root.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(fmt))
    root.addHandler(ch)
    return logging.getLogger("main")


# ──────────────────────────────────────────────────────────
#  Main Algo Class
# ──────────────────────────────────────────────────────────

class GapVWAPAlgo:

    def __init__(self):
        self.logger    = setup_logging()
        self.client    = None
        self.telegram  = TelegramNotifier()

        # Core components
        self.scrip_master   = None
        self.gap_scanner    = None
        self.vwap_mgr       = VWAPManager()
        self.report_mgr     = ReportManager()
        self.trade_mgr      = None
        self.session_mgr    = None

        # Gap stock lists
        self._gap_up:   list = []   # [{symbol, token, prev_close, ltp, gap_pct, ...}]
        self._gap_down: list = []
        self._watchlist: Dict[str, dict] = {}   # token → gap stock info

        # Subscribed tokens (avoid duplicate subscribes)
        self._subscribed_tokens: Set[str] = set()

        # State
        self._running        = True
        self._entries_open   = False   # True from 9:30 AM
        self._sq_done        = False

        # Timing
        self._entry_open_t   = datetime.time(
            *map(int, config.ENTRY_START.split(":")))
        self._sq_off_t       = datetime.time(
            *map(int, config.SQUARE_OFF_TIME.split(":")))
        self._no_new_entry_t = datetime.time(
            *map(int, config.NO_NEW_ENTRY_TIME.split(":")))

        # Auto-shutdown
        self._shutdown_time  = now_ist() + datetime.timedelta(hours=5, minutes=50)

        # No-tick watchdog
        self._last_tick_time  = now_ist()
        self._circuit_alerted = False

        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT,  self._handle_sigterm)

    # ── Initialization ────────────────────────────────────

    def initialize(self):
        print("\n" + "=" * 60)
        print("  NSE GAP UP / GAP DOWN VWAP REVERSAL ALGO")
        print(f"  Mode     : {'*** PAPER TRADE ***' if config.PAPER_TRADE else '*** LIVE ***'}")
        print(f"  Capital  : ₹{config.TOTAL_CAPITAL:,.0f}  "
              f"Leverage {config.LEVERAGE}x  "
              f"₹{config.CAPITAL_PER_TRADE:,.0f}/trade")
        print(f"  Max Sim  : {config.MAX_SIMULTANEOUS} trades  "
              f"(buying power ₹{config.CAPITAL_PER_TRADE * config.LEVERAGE:,.0f}/trade)")
        print(f"  Gap ≥    : {config.MIN_GAP_PCT}%")
        print(f"  SL       : {config.SL_PCT}%  Target: {config.TARGET_PCT}%  "
              f"Trail: activates at +{config.TRAIL_TRIGGER_PCT}%")
        print(f"  Entry    : after {config.ENTRY_START}  "
              f"Square-off: {config.SQUARE_OFF_TIME}")
        print("=" * 60)

        self._shutdown_time = now_ist() + datetime.timedelta(hours=5, minutes=50)
        print(f"  Started  : {now_ist().strftime('%H:%M:%S')} IST")
        print(f"  Auto-stop: {self._shutdown_time.strftime('%H:%M:%S')} IST  (5h 50m from start)")
        print(f"  Square-off at {config.SQUARE_OFF_TIME} IST → algo exits immediately")
        print("=" * 60)

        # Auth
        self.client = get_kotak_session()
        self.session_mgr = SessionManager(self.client, get_kotak_session)
        self.session_mgr.on_reconnect = self._on_reconnect
        self.session_mgr.start()

        # Trade manager
        self.trade_mgr = TradeManager(self.client, self.report_mgr)

        # Load scrip master (token list only — no API quotes yet)
        self.scrip_master = ScripMaster(self.client)
        scrips = self.scrip_master.load(mode=config.WATCHLIST_MODE)

        # Build gap scanner (prev close will be fetched inside run_gap_scan at 9:15)
        self.gap_scanner = GapScanner(self.client, scrips)

        # Setup WebSocket callbacks
        self._setup_websocket()

        t = now_ist()
        print("\n[Init] Initialisation complete.")
        if t.time() >= datetime.time(*map(int, config.MARKET_OPEN.split(":"))):
            print(f"[Init] Market already open ({t.strftime('%H:%M')}) — gap scan will run immediately")
        else:
            print(f"[Init] Waiting for market open at {config.MARKET_OPEN} IST...")
        print(f"[Init] Entries unlock at {config.ENTRY_START} IST\n")

    def _setup_websocket(self):
        self.client.on_message = self._on_message
        self.client.on_error   = self._on_ws_error
        self.client.on_close   = self._on_ws_close
        self.client.on_open    = self._on_ws_open

    def _on_ws_open(self, *args):
        print("[WS] Connected")

    def _on_ws_error(self, error):
        self.logger.error(f"[WS] Error: {error}")

    def _on_ws_close(self, *args):
        self.logger.warning("[WS] Closed")

    # ── Gap Scan at 9:15 ─────────────────────────────────

    def run_gap_scan(self):
        """Run gap scan. Called at 9:15 and then every 5 minutes."""
        is_first_scan = len(self._watchlist) == 0
        scan_label    = "INITIAL SCAN" if is_first_scan else "RESCAN"
        print(f"\n[GapScan] {scan_label} at {now_ist().strftime('%H:%M:%S')}...")

        # ── Step 1: Fetch prev close (only on first scan) ─
        if is_first_scan:
            print("[GapScan] Fetching previous close prices...")
            prev_fetcher = PrevCloseFetcher(self.client)
            prev_close   = prev_fetcher.fetch(self.gap_scanner.scrips)
            self.gap_scanner.set_prev_close(prev_close)
            if len(prev_close) == 0:
                print("[GapScan] ⚠️  WARNING: 0 prev close values. "
                      "Check quotes API permissions.")

        # ── Step 2: Scan for gaps ─────────────────────────
        gap_up, gap_down = self.gap_scanner.scan()
        self._gap_up     = gap_up
        self._gap_down   = gap_down

        # ── Step 3: Find newly discovered stocks ──────────
        all_gap_stocks = gap_up + gap_down
        new_stocks     = [s for s in all_gap_stocks
                          if s["token"] not in self._watchlist]
        already_watching = len(self._watchlist)

        print(f"\n{'─'*55}")
        print(f"  GAP SCAN RESULTS — {now_ist().strftime('%H:%M')}  [{scan_label}]")
        print(f"{'─'*55}")
        print(f"  🟢 GAP UP  ({len(gap_up)} stocks — SHORT on VWAP break):")
        for s in gap_up[:15]:
            tag = " ← NEW" if s["token"] not in self._watchlist else ""
            print(f"     {s['symbol']:<12} +{s['gap_pct']:.2f}%  "
                  f"Prev ₹{s['prev_close']:.2f} → ₹{s['ltp']:.2f}{tag}")
        if len(gap_up) > 15:
            print(f"     ... +{len(gap_up)-15} more")

        print(f"\n  🔴 GAP DOWN ({len(gap_down)} stocks — LONG on VWAP cross):")
        for s in gap_down[:15]:
            tag = " ← NEW" if s["token"] not in self._watchlist else ""
            print(f"     {s['symbol']:<12} {s['gap_pct']:.2f}%  "
                  f"Prev ₹{s['prev_close']:.2f} → ₹{s['ltp']:.2f}{tag}")
        if len(gap_down) > 15:
            print(f"     ... +{len(gap_down)-15} more")

        print(f"\n  Already watching : {already_watching} stocks")
        print(f"  Newly added      : {len(new_stocks)} stocks")
        print(f"{'─'*55}\n")

        if is_first_scan:
            self.gap_scanner.save_gap_list(gap_up, gap_down)

        # ── Step 4: Subscribe & add VWAP trackers for NEW stocks only ─
        if new_stocks:
            self._subscribe_new_stocks(new_stocks)

        # ── Step 5: On first scan, also subscribe ALL watchlist stocks ─
        # for TREND_RIDE and VWAP_BREAKOUT signals (not just gap stocks)
        if is_first_scan:
            self._subscribe_full_watchlist()

        # Telegram alert only on first scan
        if is_first_scan:
            self.telegram.alert_gap_list(gap_up, gap_down)
            self.telegram.alert_startup(
                gap_up_count   = len(gap_up),
                gap_down_count = len(gap_down),
                mode           = "PAPER" if config.PAPER_TRADE else "LIVE",
            )
        elif new_stocks:
            new_up   = [s for s in new_stocks if s["direction"] == "GAP_UP"]
            new_down = [s for s in new_stocks if s["direction"] == "GAP_DOWN"]
            self.telegram.alert_gap_list(new_up, new_down)

    def _subscribe_new_stocks(self, new_stocks: list):
        """Subscribe only newly found gap stocks to WS and register VWAP trackers."""
        tokens_to_sub = []
        for info in new_stocks:
            token = info["token"]
            if token not in self._subscribed_tokens:
                self._watchlist[token] = info
                tokens_to_sub.append({
                    "instrument_token": token,
                    "exchange_segment": config.CM_SEGMENT,
                })
                self.vwap_mgr.add_stock(
                    symbol        = info["symbol"],
                    token         = token,
                    gap_direction = info["direction"],
                )
                self._subscribed_tokens.add(token)
                print(f"[WS] Subscribing {info['direction']} {info['symbol']} "
                      f"({info['gap_pct']:+.2f}%)")

        if not tokens_to_sub:
            return

        # Subscribe in batches of 50
        for i in range(0, len(tokens_to_sub), 50):
            batch = tokens_to_sub[i:i+50]
            try:
                self.client.subscribe(
                    instrument_tokens=batch,
                    isIndex=False,
                    isDepth=False,
                )
            except Exception as e:
                self.logger.error(f"[WS] Subscribe error: {e}")
            time.sleep(0.1)

        print(f"[WS] ✅ Subscribed {len(tokens_to_sub)} new stocks  "
              f"(total watching: {self.vwap_mgr.active_count})")

    # ── WebSocket Tick Handler ────────────────────────────

    def _on_message(self, message):
        try:
            if not isinstance(message, dict):
                return
            msg_type = message.get("type", "")
            if msg_type not in ("stock_feed", "sf", "index_feed", "if"):
                return
            ticks = message.get("data", [])
            if not ticks:
                return

            for tick in ticks:
                token = str(tick.get("tk") or tick.get("token") or
                            tick.get("instrument_token") or "")
                ltp   = float(tick.get("ltp") or tick.get("ltP") or
                               tick.get("lp")  or 0)
                if not token or ltp <= 0:
                    continue

                self._last_tick_time  = now_ist()
                self._circuit_alerted = False

                # Update VWAP tracker
                self.vwap_mgr.on_tick(token, tick)

                # Update any open trade with this token
                self.trade_mgr.on_tick(token, ltp)

                # Check for entry signals (only after 9:30 and before 2:30)
                if self._entries_open and not self.trade_mgr.day_stopped:
                    self._check_entry_signal(token, ltp)

        except Exception as e:
            self.logger.error(f"_on_message: {e}", exc_info=True)

    def _check_entry_signal(self, token: str, ltp: float):
        """
        Check VWAP tracker for any of the 4 signal types.
        Each signal type has its own entry narrative printed.
        """
        info = self._watchlist.get(token)
        if not info:
            return

        symbol  = info["symbol"]
        tracker = self.vwap_mgr.get_tracker(symbol)
        if not tracker:
            return

        fired, sig_type, direction = tracker.check_signal()
        if not fired:
            return

        if not self.trade_mgr.can_enter(symbol):
            tracker.mark_signal_used(sig_type)
            return

        vwap      = tracker.vwap
        slope     = tracker.get_vwap_slope()
        flat_mins = tracker.get_flat_duration()
        trend_mins= tracker.get_trend_duration()

        # ── Signal context print ──────────────────────────
        from vwap_engine import VWAPTracker
        if sig_type == VWAPTracker.SIG_GAP_REVERSAL:
            context = (f"Gap {info.get('gap_pct',0):+.2f}% reversal — "
                       f"price crossed {'below' if direction=='SHORT' else 'above'} VWAP")
        elif sig_type == VWAPTracker.SIG_TREND_RIDE:
            context = (f"Trend ride — {trend_mins}min on {'above' if direction=='LONG' else 'below'} "
                       f"VWAP, pullback touch entry (slope {slope:+.4f}%/min)")
        elif sig_type == VWAPTracker.SIG_VWAP_BREAKOUT:
            context = (f"VWAP Breakout — {flat_mins}min flat VWAP suddenly "
                       f"{'rising' if direction=='LONG' else 'falling'} "
                       f"(slope {slope:+.4f}%/min) with vol spike")
        elif sig_type == VWAPTracker.SIG_TREND_CONTINUATION:
            context = (f"Trend continuation — 20min consistently "
                       f"{'above' if direction=='LONG' else 'below'} VWAP "
                       f"(slope {slope:+.4f}%/min), pullback entry")
        else:
            context = sig_type

        self.logger.info(f"[Signal] {symbol} {direction} [{sig_type}]  "
                         f"LTP={ltp:.2f} VWAP={vwap:.2f}  {context}")
        print(f"\n🔔 SIGNAL: {direction} {symbol}  [{sig_type}]")
        print(f"   {context}")
        print(f"   LTP ₹{ltp:.2f}  VWAP ₹{vwap:.2f}  "
              f"Dist {abs(ltp-vwap)/vwap*100:.2f}%")

        trade = self.trade_mgr.enter(
            symbol        = symbol,
            token         = token,
            direction     = direction,
            ltp           = ltp,
            vwap          = vwap,
            gap_pct       = info.get("gap_pct", 0.0),
            gap_direction = info.get("direction", "NONE"),
            signal_type   = sig_type,
        )

        if trade:
            tracker.mark_signal_used(sig_type)
            self.telegram.alert_entry(
                symbol    = symbol,
                direction = direction,
                gap_dir   = info.get("direction", "NONE"),
                entry     = trade.entry_price,
                vwap      = vwap,
                sl        = trade.sl_price,
                target    = trade.target_price,
                qty       = trade.qty,
                gap_pct   = info.get("gap_pct", 0.0),
            )

    # ── Reconnect ─────────────────────────────────────────

    def _subscribe_full_watchlist(self):
        """
        Subscribe ALL watchlist stocks (not just gap stocks) at market open.
        This enables TREND_RIDE and VWAP_BREAKOUT signals on any liquid stock.
        Runs once at 9:15 — after gap stocks are already subscribed.
        """
        scrips        = self.gap_scanner.scrips
        tokens_to_sub = []
        added         = 0

        for symbol, info in scrips.items():
            token = info["token"]
            if token in self._subscribed_tokens:
                continue   # already subscribed as gap stock
            tokens_to_sub.append({
                "instrument_token": token,
                "exchange_segment": config.CM_SEGMENT,
            })
            # gap_direction = "NONE" → enables TREND_RIDE + VWAP_BREAKOUT + TREND_CONTINUATION
            self.vwap_mgr.add_stock(symbol=symbol, token=token, gap_direction="NONE")
            # Also add to watchlist for entry routing
            self._watchlist[token] = {
                "symbol"   : symbol,
                "token"    : token,
                "direction": "NONE",
                "gap_pct"  : 0.0,
                "name"     : info.get("name", symbol),
                "ltp"      : 0.0,
                "prev_close": info.get("prev_close", 0.0),
            }
            self._subscribed_tokens.add(token)
            added += 1

        if not tokens_to_sub:
            print("[WS] All watchlist stocks already subscribed")
            return

        print(f"[WS] Subscribing {added} watchlist stocks for trend/breakout signals...")
        for i in range(0, len(tokens_to_sub), 50):
            batch = tokens_to_sub[i:i+50]
            try:
                self.client.subscribe(
                    instrument_tokens=batch,
                    isIndex=False,
                    isDepth=False,
                )
            except Exception as e:
                self.logger.error(f"[WS] Watchlist subscribe error: {e}")
            time.sleep(0.15)

        print(f"[WS] ✅ Total watching: {self.vwap_mgr.active_count} stocks  "
              f"(gap: {len(self._gap_up)+len(self._gap_down)}  "
              f"trend/breakout: {added})")
        self.client            = new_client
        self.trade_mgr.client  = new_client
        self._setup_websocket()
        time.sleep(2)
        # Re-subscribe all watched stocks
        self._subscribed_tokens.clear()
        all_stocks = list(self._watchlist.values())
        if all_stocks:
            self._subscribe_new_stocks(all_stocks)
        self.logger.info("[Reconnect] Re-subscribed all gap stocks after session refresh")

    # ── Helpers ───────────────────────────────────────────

    def _on_reconnect(self, new_client):
        """Called by SessionManager after a successful re-login."""
        self.client           = new_client
        self.trade_mgr.client = new_client
        self.gap_scanner.client = new_client
        self._setup_websocket()
        time.sleep(2)
        # Re-subscribe everything that was subscribed before disconnect
        self._subscribed_tokens.clear()
        all_stocks = list(self._watchlist.values())
        if all_stocks:
            self._subscribe_new_stocks(all_stocks)
        self.logger.info(f"[Reconnect] Re-subscribed {len(all_stocks)} stocks after session refresh")
        print(f"[Reconnect] ✅ Session restored — re-subscribed {len(all_stocks)} stocks")

    def _is_market_hours(self, t: datetime.datetime) -> bool:
        open_t = datetime.time(*map(int, config.MARKET_OPEN.split(":")))
        return open_t <= t.time() <= self._sq_off_t

    def _check_no_tick(self):
        t       = now_ist()
        elapsed = (t - self._last_tick_time).total_seconds()
        if elapsed > 300 and self._is_market_hours(t) and not self._circuit_alerted:
            msg = f"No tick for {elapsed/60:.0f} mins — circuit halt or WS issue?"
            self.logger.warning(f"[Circuit] {msg}")
            self.telegram.alert_risk(msg)
            self._circuit_alerted = True

    def _print_status(self):
        t = now_ist()
        entry_tag = "ENTRIES OPEN" if self._entries_open else f"entries open at {config.ENTRY_START}"
        watching  = self.vwap_mgr.active_count
        print(f"\n[{t.strftime('%H:%M:%S')}] "
              f"Watching: {watching}  "
              f"Open trades: {len(self.trade_mgr._open)}/{config.MAX_SIMULTANEOUS}  "
              f"Day P&L: ₹{self.trade_mgr.day_pnl_rs:+,.0f}  "
              f"[{entry_tag}]")
        if self.trade_mgr._open:
            self.trade_mgr.print_status()

    def _handle_sigterm(self, signum, frame):
        print(f"\n[Shutdown] Signal {signum} received — stopping...")
        self._running = False

    def _graceful_shutdown(self):
        print("\n[Shutdown] Saving state and exiting...")
        try:
            self.trade_mgr.square_off_all()
        except Exception as e:
            self.logger.error(f"[Shutdown] square_off error: {e}")
        try:
            report = self.report_mgr.generate_daily_report()
            print(report)
        except Exception as e:
            self.logger.error(f"[Shutdown] report error: {e}")
        try:
            self.report_mgr.close()
        except Exception:
            pass
        try:
            if self.session_mgr:
                self.session_mgr.stop()
        except Exception:
            pass
        self.telegram.alert_shutdown(
            trades  = self.trade_mgr.trade_count,
            net_pnl = self.trade_mgr.day_pnl_rs,
        )
        print("[Shutdown] Done.")
        os._exit(0)

    # ── Main Loop ─────────────────────────────────────────

    def run(self):
        self.initialize()

        gap_scanned      = False
        last_status_min  = -1
        sq_done          = False
        consec_sl_paused = False
        consec_sl_resume = None
        last_rescan_time = None   # track when we last rescanned

        print(f"[Main] Running — waiting for {config.MARKET_OPEN} IST...\n")

        try:
            while self._running:
                t = now_ist()

                # ── Auto-shutdown ─────────────────────────
                if t >= self._shutdown_time:
                    print(f"\n[Main] Auto-shutdown time reached — stopping")
                    break

                # ── Run gap scan at 9:15, then every 5 min ───
                market_open_t = datetime.time(*map(int, config.MARKET_OPEN.split(":")))
                should_scan   = False
                if not gap_scanned and t.time() >= market_open_t:
                    should_scan = True   # first scan
                elif (gap_scanned
                      and last_rescan_time is not None
                      and not sq_done
                      and t.time() < self._no_new_entry_t
                      and (t - last_rescan_time).total_seconds() >= config.SCAN_INTERVAL_SECS):
                    should_scan = True   # periodic rescan

                if should_scan:
                    self.run_gap_scan()
                    gap_scanned      = True
                    last_rescan_time = t

                # ── Open entries at 9:30 ──────────────────
                if gap_scanned and not self._entries_open:
                    if t.time() >= self._entry_open_t:
                        self._entries_open = True
                        print(f"\n[Main] ✅ ENTRIES NOW OPEN — {t.strftime('%H:%M:%S')} IST")
                        print(f"[Main] Watching {self.vwap_mgr.active_count} gap stocks for VWAP cross\n")

                # ── Resume after consec SL pause ─────────
                if consec_sl_paused and consec_sl_resume:
                    if t >= consec_sl_resume:
                        consec_sl_paused           = False
                        consec_sl_resume           = None
                        self.trade_mgr.consec_sl   = 0
                        print(f"\n[Guard] Consec SL pause lifted — entries re-enabled")

                # ── Pause on consec SL ─────────────────────
                if (self.trade_mgr.consec_sl >= config.MAX_CONSEC_SL
                        and not consec_sl_paused
                        and not self.trade_mgr.day_stopped):
                    consec_sl_paused = True
                    consec_sl_resume = t + datetime.timedelta(minutes=30)
                    self._entries_open = False
                    print(f"\n[Guard] {config.MAX_CONSEC_SL} consecutive SLs — "
                          f"pausing entries for 30 min (resume {consec_sl_resume.strftime('%H:%M')})")
                    # Re-open entries after pause if still within entry window
                    def _reopen_entries(resume_t):
                        time.sleep(30 * 60)
                        if now_ist().time() < self._no_new_entry_t:
                            self._entries_open = True
                    threading.Thread(target=_reopen_entries,
                                     args=(consec_sl_resume,), daemon=True).start()

                # ── No new entries after 2:30 PM ──────────
                if self._entries_open and t.time() >= self._no_new_entry_t:
                    if not sq_done:
                        self._entries_open = False
                        print(f"\n[Main] No new entries after {config.NO_NEW_ENTRY_TIME} — "
                              f"managing open positions only")

                # ── Square off at 3:15 PM ─────────────────
                if t.time() >= self._sq_off_t and not sq_done:
                    print(f"\n[Main] {config.SQUARE_OFF_TIME} — squaring off all positions & stopping algo")
                    self.trade_mgr.square_off_all()
                    self._entries_open = False
                    sq_done            = True
                    self._running      = False   # exit main loop immediately

                # ── Status print every minute ─────────────
                if gap_scanned and t.minute != last_status_min:
                    self._print_status()
                    self._check_no_tick()
                    last_status_min = t.minute

                time.sleep(1)

        except KeyboardInterrupt:
            print("\n[Main] Keyboard interrupt — shutting down")
        finally:
            self._graceful_shutdown()


# ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    algo = GapVWAPAlgo()
    algo.run()
