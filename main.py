# ============================================================
# MAIN.PY — NSE Gap VWAP Trend Algo v4
# ============================================================
#
# FIXES vs v3:
#   1. MAX_TRADES_PER_DAY removed — algo runs all day, only limit
#      is 8 simultaneous open positions.
#   2. RESTTrendScanner crash fixed — _all_scrips guaranteed to be
#      list of dicts. ScripMaster.load() rows are normalised here
#      into {"token":..., "symbol":..., "isin":...} before use.
#   3. Session block for re-entry — can_enter() passes gap_direction
#      and entry_direction; TradeManager blocks SL'd symbols.
#   4. Direction lock — GAP_UP stocks: SHORT only. GAP_DOWN: LONG only.
#   5. Bug 4 fixed — _entries_open flag is now respected inside
#      can_enter() by passing it explicitly. The main loop pause sets
#      self._entries_open = False and the TradeManager checks it via
#      self._entries_allowed which is kept in sync.
#      Simplest safe fix: wrap _check_entry_signal() with the flag check
#      so no tick ever reaches can_enter when entries are paused.
#   6. Banner updated to v4. Removed slot references from print.
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


def _normalise_scrip(row) -> dict:
    """
    FIX (Bug 3 — RESTTrendScanner crash):
    ScripMaster.load() may return plain strings or dicts depending on
    the watchlist file format. This function guarantees every item
    in _all_scrips is a dict with at least 'token' and 'symbol' keys.
    """
    if isinstance(row, dict):
        # Already a dict — normalise key names
        token  = str(row.get("TOKEN") or row.get("token") or
                     row.get("instrument_token") or "")
        symbol = str(row.get("SYMBOL") or row.get("symbol") or
                     row.get("trading_symbol") or row.get("pSymbol") or "")
        isin   = str(row.get("ISIN") or row.get("isin") or
                     row.get("pISIN") or "")
        name   = str(row.get("NAME") or row.get("name") or
                     row.get("pSymbolName") or symbol)
        return {"token": token, "symbol": symbol, "isin": isin, "name": name}
    elif isinstance(row, str):
        # Plain string — treat as symbol, token unknown
        return {"token": "", "symbol": row.strip(), "isin": "", "name": row.strip()}
    else:
        return {"token": "", "symbol": str(row), "isin": "", "name": ""}


class GapVWAPAlgo:

    def __init__(self):
        self.logger   = setup_logging()
        self.client   = None
        self.telegram = TelegramNotifier()

        self.scrip_master = None
        self.gap_scanner  = None
        self.vwap_mgr     = VWAPManager()
        self.report_mgr   = ReportManager()
        self.trade_mgr    = None
        self.session_mgr  = None

        self._gap_up:   list = []
        self._gap_down: list = []\

        # token → gap stock info dict
        self._watchlist: Dict[str, dict] = {}

        # FIX: all scrips normalised to dicts via _normalise_scrip()
        # Keys guaranteed: token, symbol, isin, name
        self._all_scrips: list = []

        # token → symbol reverse lookup (built from _all_scrips for O(1) access)
        self._token_to_symbol: Dict[str, str] = {}

        self._subscribed_tokens: Set[str] = set()

        self._running         = True
        self._entries_open    = False
        self._no_entry_forced = False   # one-way latch — once True, never reverts
        self._sq_done         = False

        self._entry_open_t   = datetime.time(*map(int, config.ENTRY_START.split(":")))
        self._sq_off_t       = datetime.time(*map(int, config.SQUARE_OFF_TIME.split(":")))
        self._no_new_entry_t = datetime.time(*map(int, config.NO_NEW_ENTRY_TIME.split(":")))

        self._shutdown_time   = now_ist() + datetime.timedelta(hours=5, minutes=50)
        self._last_tick_time  = now_ist()
        self._circuit_alerted = False
        self._ws_connected    = False

        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT,  self._handle_sigterm)

    # ─────────────────────────────────────────────────────
    #  Initialization
    # ─────────────────────────────────────────────────────

    def initialize(self):
        print("\n" + "=" * 60)
        print("  NSE GAP VWAP TREND ALGO  v4")
        print(f"  Mode     : {'*** PAPER TRADE ***' if config.PAPER_TRADE else '*** LIVE ***'}")
        print(f"  Capital  : ₹{config.TOTAL_CAPITAL:,.0f}  "
              f"Leverage {config.LEVERAGE}x  ₹{config.CAPITAL_PER_TRADE:,.0f}/trade")
        print(f"  Gap ≥    : {config.MIN_GAP_PCT}%")
        print(f"  SL       : {config.SL_PCT}%  Target: {config.TARGET_PCT}%  "
              f"Trail: activates at +{config.TRAIL_TRIGGER_PCT}%")
        print(f"  Slots    : {config.MAX_SIMULTANEOUS} max simultaneous  (no daily cap)")
        print(f"  Confirm  : {config.CROSS_CONFIRM_BARS} bars reversal | "
              f"{config.TREND_MIN_CANDLES_ONSIDE} bars trend")
        print(f"  Guards   : SessionBlock ON  DirectionLock ON")
        print("=" * 60)

        self._shutdown_time = now_ist() + datetime.timedelta(hours=5, minutes=50)
        print(f"  Started  : {now_ist().strftime('%H:%M:%S')} IST")
        print(f"  Auto-stop: {self._shutdown_time.strftime('%H:%M:%S')} IST")
        print("=" * 60)

        self.client      = get_kotak_session()
        self.session_mgr = SessionManager(self.client, get_kotak_session)
        self.session_mgr.on_reconnect = self._on_reconnect
        self.session_mgr.start()

        self.trade_mgr = TradeManager(self.client, self.report_mgr)

        self.scrip_master = ScripMaster(self.client)
        raw_scrips = self.scrip_master.load(mode=config.WATCHLIST_MODE)

        # FIX: normalise every scrip to a dict before storing
        self._all_scrips = [_normalise_scrip(r) for r in raw_scrips]

        # Build O(1) token → symbol lookup
        self._token_to_symbol = {
            s["token"]: s["symbol"]
            for s in self._all_scrips
            if s["token"]
        }

        print(f"[Init] Watchlist loaded: {len(self._all_scrips)} stocks  "
              f"({len(self._token_to_symbol)} with tokens)")

        self.gap_scanner = GapScanner(self.client, raw_scrips)
        self._setup_websocket()

        t = now_ist()
        print("\n[Init] Initialisation complete.")
        if t.time() >= datetime.time(*map(int, config.MARKET_OPEN.split(":"))):
            print(f"[Init] Market already open ({t.strftime('%H:%M')}) — gap scan runs immediately")
        else:
            print(f"[Init] Waiting for market open at {config.MARKET_OPEN} IST...")
        print(f"[Init] Entries unlock at {config.ENTRY_START} IST\n")

    # ─────────────────────────────────────────────────────
    #  WebSocket setup & handlers
    # ─────────────────────────────────────────────────────

    def _setup_websocket(self):
        self.client.on_message = self._on_message
        self.client.on_error   = self._on_ws_error
        self.client.on_close   = self._on_ws_close
        self.client.on_open    = self._on_ws_open

    def _on_ws_open(self, *args):
        print("[WS] Connected")
        self._ws_connected = True

    def _on_ws_error(self, error):
        err_str = str(error)
        if "already closed" in err_str or "NoneType" in err_str:
            self.logger.debug(f"[WS] Suppressed: {err_str}")
            return
        self.logger.error(f"[WS] Error: {error}")

    def _on_ws_close(self, *args):
        self._ws_connected = False
        self.logger.warning("[WS] Closed")
        if self._running:
            threading.Thread(
                target=self._ws_reconnect_loop,
                daemon=True,
                name="WSReconnect"
            ).start()

    def _ws_reconnect_loop(self):
        delays = [5, 10, 20, 30]
        for attempt, delay in enumerate(delays, 1):
            if not self._running:
                return
            print(f"\n[WS] Reconnect attempt {attempt}/{len(delays)} in {delay}s...")
            time.sleep(delay)
            if not self._running:
                return
            try:
                self._setup_websocket()
                tokens_list = [
                    {"instrument_token": tok, "exchange_segment": config.CM_SEGMENT}
                    for tok in self._subscribed_tokens
                ]
                for i in range(0, len(tokens_list), 50):
                    self.client.subscribe(
                        instrument_tokens=tokens_list[i:i+50],
                        isIndex=False,
                        isDepth=False,
                    )
                    time.sleep(0.5)
                print(f"[WS] ✅ Reconnected — re-subscribed {len(tokens_list)} tokens")
                return
            except Exception as e:
                self.logger.error(f"[WS] Reconnect attempt {attempt} failed: {e}")
        print("[WS] ❌ All reconnect attempts failed — session manager will handle")

    # ─────────────────────────────────────────────────────
    #  Gap Scan
    # ─────────────────────────────────────────────────────

    def run_gap_scan(self):
        is_first_scan = len(self._watchlist) == 0
        scan_label    = "INITIAL SCAN" if is_first_scan else "RESCAN"
        print(f"\n[GapScan] {scan_label} at {now_ist().strftime('%H:%M:%S')}...")

        if is_first_scan:
            print("[GapScan] Fetching previous close prices...")
            prev_fetcher = PrevCloseFetcher(self.client)
            prev_close   = prev_fetcher.fetch(self.gap_scanner.scrips)
            self.gap_scanner.set_prev_close(prev_close)
            if not prev_close:
                print("[GapScan] ⚠️  WARNING: 0 prev close values fetched")

        gap_up, gap_down = self.gap_scanner.scan()
        self._gap_up     = gap_up
        self._gap_down   = gap_down

        all_gap_stocks = gap_up + gap_down
        new_stocks     = [s for s in all_gap_stocks
                          if s["token"] not in self._watchlist]

        print(f"\n{'─'*55}")
        print(f"  GAP SCAN — {now_ist().strftime('%H:%M')}  [{scan_label}]")
        print(f"{'─'*55}")
        print(f"  🟢 GAP UP  ({len(gap_up)} stocks):")
        for s in gap_up[:15]:
            tag = " ← NEW" if s["token"] not in self._watchlist else ""
            print(f"     {s['symbol']:<14} +{s['gap_pct']:.2f}%{tag}")
        if len(gap_up) > 15:
            print(f"     ... +{len(gap_up)-15} more")
        print(f"\n  🔴 GAP DOWN ({len(gap_down)} stocks):")
        for s in gap_down[:15]:
            tag = " ← NEW" if s["token"] not in self._watchlist else ""
            print(f"     {s['symbol']:<14} {s['gap_pct']:.2f}%{tag}")
        print(f"\n  Already watching : {len(self._watchlist)}")
        print(f"  Newly added      : {len(new_stocks)}")
        print(f"{'─'*55}\n")

        if is_first_scan:
            self.gap_scanner.save_gap_list(gap_up, gap_down)

        if new_stocks:
            self._subscribe_new_stocks(new_stocks)

        if is_first_scan:
            threading.Thread(
                target=self._rest_poll_loop,
                daemon=True,
                name="RESTGapPoller"
            ).start()
            threading.Thread(
                target=self._rest_trend_scan_loop,
                daemon=True,
                name="RESTTrendScanner"
            ).start()
            self.telegram.alert_gap_list(gap_up, gap_down)
            self.telegram.alert_startup(
                gap_up_count   = len(gap_up),
                gap_down_count = len(gap_down),
                mode           = "PAPER" if config.PAPER_TRADE else "LIVE",
            )

    def _subscribe_new_stocks(self, new_stocks: list):
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
                print(f"[WS] Subscribing {info['direction']} "
                      f"{info['symbol']} ({info['gap_pct']:+.2f}%)")

        if not tokens_to_sub:
            return

        def _do_subscribe():
            for i in range(0, len(tokens_to_sub), 50):
                try:
                    self.client.subscribe(
                        instrument_tokens=tokens_to_sub[i:i+50],
                        isIndex=False,
                        isDepth=False,
                    )
                    time.sleep(1.0)
                except Exception as e:
                    self.logger.error(f"[WS] Subscribe error: {e}")
                    time.sleep(2.0)
            print(f"[WS] ✅ Subscribed {len(tokens_to_sub)} new stocks  "
                  f"(total watching: {self.vwap_mgr.active_count})")

        threading.Thread(target=_do_subscribe, daemon=True, name="GapSubscribe").start()

    # ─────────────────────────────────────────────────────
    #  WebSocket tick handler
    # ─────────────────────────────────────────────────────

    def _on_message(self, message):
        try:
            if not isinstance(message, dict):
                return
            if message.get("type", "") not in ("stock_feed", "sf", "index_feed", "if"):
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

                self.vwap_mgr.on_tick(token, tick)
                self.trade_mgr.on_tick(token, ltp)

                # FIX (Bug 4): _entries_open gate is here — if entries are
                # paused (consec SL guard), ticks never reach signal checking.
                # This is the only reliable place to gate it — can_enter() alone
                # is not enough because it runs in multiple threads.
                if self._entries_open and not self.trade_mgr.day_stopped:
                    self._check_entry_signal(token, ltp)

        except Exception as e:
            self.logger.error(f"_on_message: {e}", exc_info=True)

    def _check_entry_signal(self, token: str, ltp: float):
        # Resolve symbol and gap info
        info   = self._watchlist.get(token)
        symbol = info["symbol"] if info else self._token_to_symbol.get(token)

        if not symbol:
            tracker = self.vwap_mgr.get_tracker_by_token(token)
            if not tracker:
                return
            symbol = tracker.symbol
            info   = {"gap_pct": 0.0, "direction": "NONE"}

        tracker = self.vwap_mgr.get_tracker_by_token(token)
        if not tracker:
            return

        fired, sig_type, direction = tracker.check_signal()
        if not fired:
            return

        gap_dir = info.get("direction", "NONE") if info else "NONE"
        gap_pct = info.get("gap_pct",   0.0)    if info else 0.0

        # can_enter now receives gap_direction and entry_direction
        # for the session block and direction lock checks
        if not self.trade_mgr.can_enter(symbol, sig_type,
                                        gap_direction=gap_dir,
                                        entry_direction=direction):
            tracker.mark_signal_used(sig_type)
            return

        vwap  = tracker.vwap
        slope = tracker.get_vwap_slope()

        self.logger.info(f"[Signal] {symbol} {direction} [{sig_type}]  "
                         f"LTP={ltp:.2f} VWAP={vwap:.2f}  slope={slope:+.4f}%/min")
        print(f"\n🔔 SIGNAL: {direction} {symbol}  [{sig_type}]")
        print(f"   LTP ₹{ltp:.2f}  VWAP ₹{vwap:.2f}  "
              f"Dist {abs(ltp-vwap)/vwap*100:.2f}%  slope {slope:+.4f}%/min")

        trade = self.trade_mgr.enter(
            symbol        = symbol,
            token         = token,
            direction     = direction,
            ltp           = ltp,
            vwap          = vwap,
            gap_pct       = gap_pct,
            gap_direction = gap_dir,
            signal_type   = sig_type,
        )

        if trade:
            tracker.mark_signal_used(sig_type)
            self.telegram.alert_entry(
                symbol    = symbol,
                direction = direction,
                gap_dir   = gap_dir,
                entry     = trade.entry_price,
                vwap      = vwap,
                sl        = trade.sl_price,
                target    = trade.target_price,
                qty       = trade.qty,
                gap_pct   = gap_pct,
            )

    # ─────────────────────────────────────────────────────
    #  REST poll loop — gap stocks, every 15s
    # ─────────────────────────────────────────────────────

    def _rest_poll_loop(self):
        print("[REST] Gap poll loop started — every 15s")
        while self._running:
            time.sleep(getattr(config, "REST_POLL_INTERVAL", 15))
            if not self._running:
                break

            gap_tokens  = list(self._subscribed_tokens)
            if not gap_tokens:
                continue

            instruments = [
                {"instrument_token": tok, "exchange_segment": config.CM_SEGMENT}
                for tok in gap_tokens
            ]

            try:
                for i in range(0, len(instruments), 50):
                    resp  = self.client.quotes(
                        instrument_tokens=instruments[i:i+50],
                        quote_type="ltp",
                    )
                    items = (resp.get("data", resp.get("success", []))
                             if isinstance(resp, dict) else resp
                             if isinstance(resp, list) else [])

                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        tok     = str(item.get("instrument_token") or
                                      item.get("tk") or item.get("token") or "")
                        ltp_val = self._extract_ltp(item)
                        ap_val  = self._extract_ap(item)
                        if not tok or ltp_val <= 0:
                            continue

                        self.vwap_mgr.on_tick(tok, {"ltp": ltp_val, "ap": ap_val},
                                              from_ws=False)
                        self.trade_mgr.on_tick(tok, ltp_val)

                        if self._entries_open and not self.trade_mgr.day_stopped:
                            self._check_entry_signal(tok, ltp_val)
                    time.sleep(0.3)
            except Exception as e:
                self.logger.warning(f"[REST] Gap poll error: {e}")

    # ─────────────────────────────────────────────────────
    #  REST trend scan — full watchlist, every 60s
    # ─────────────────────────────────────────────────────

    def _rest_trend_scan_loop(self):
        """
        Scans the full watchlist every 60s to detect VWAP_TREND setups
        on stocks that are NOT in the morning gap list.

        This is how we catch mid-day breakouts like GALLANTT, NETWEB,
        DEEPAKFERT that develop strong trends during the session.

        FIX vs v3: _all_scrips are now normalised dicts (via _normalise_scrip),
        so scrip.get("token") and scrip.get("symbol") are always safe to call.
        The v3 crash was: AttributeError: 'str' object has no attribute 'get'
        because ScripMaster returned some rows as plain strings.
        """
        time.sleep(90)   # wait for market open + first gap scan
        print("[REST] Trend scan loop started — every 60s for full watchlist")

        while self._running:
            time.sleep(getattr(config, "REST_TREND_INTERVAL", 60))
            if not self._running or not self._entries_open:
                continue

            # Only scan non-gap stocks (gap stocks are covered by WS + rest_poll)
            instruments = []
            for scrip in self._all_scrips:
                tok = scrip["token"]   # always safe — dict guaranteed by _normalise_scrip
                if tok and tok not in self._subscribed_tokens:
                    instruments.append({
                        "instrument_token": tok,
                        "exchange_segment": config.CM_SEGMENT
                    })

            if not instruments:
                continue

            scanned = 0
            try:
                for i in range(0, len(instruments), 50):
                    if not self._running:
                        break
                    batch = instruments[i:i+50]
                    resp  = self.client.quotes(
                        instrument_tokens=batch,
                        quote_type="ltp",
                    )
                    items = (resp.get("data", resp.get("success", []))
                             if isinstance(resp, dict) else resp
                             if isinstance(resp, list) else [])

                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        tok = str(item.get("instrument_token") or
                                  item.get("tk") or item.get("token") or "")
                        if not tok:
                            continue
                        ltp_val = self._extract_ltp(item)
                        ap_val  = self._extract_ap(item)
                        if ltp_val <= 0:
                            continue

                        # O(1) token → symbol lookup using pre-built dict
                        symbol = self._token_to_symbol.get(tok)
                        if not symbol:
                            continue

                        # Add tracker if first time seeing this stock
                        if not self.vwap_mgr.get_tracker(symbol):
                            self.vwap_mgr.add_stock(
                                symbol        = symbol,
                                token         = tok,
                                gap_direction = "NONE",
                            )

                        self.vwap_mgr.on_tick(tok, {"ltp": ltp_val, "ap": ap_val},
                                              from_ws=False)

                        # Only check signal if a slot is available
                        if (self._entries_open
                                and not self.trade_mgr.day_stopped
                                and self.trade_mgr.can_enter(symbol, "VWAP_TREND_LONG",
                                                             gap_direction="NONE")):
                            self._check_entry_signal(tok, ltp_val)

                        scanned += 1

                    time.sleep(0.1)

                self.logger.debug(f"[TrendScan] Scanned {scanned} non-gap stocks")

            except Exception as e:
                self.logger.warning(f"[TrendScan] Error: {e}", exc_info=True)

    # ─────────────────────────────────────────────────────
    #  LTP / AP extraction helpers
    # ─────────────────────────────────────────────────────

    def _extract_ltp(self, item: dict) -> float:
        ltp_data = item.get("ltp")
        if isinstance(ltp_data, dict):
            for f in ("ltp", "ltP", "lp"):
                v = ltp_data.get(f)
                if v:
                    try:
                        val = float(v)
                        if val > 0:
                            return val
                    except (TypeError, ValueError):
                        pass
        for f in ("ltp", "ltP", "lp", "last_price"):
            v = item.get(f)
            if v and not isinstance(v, dict):
                try:
                    val = float(v)
                    if val > 0:
                        return val
                except (TypeError, ValueError):
                    pass
        return 0.0

    def _extract_ap(self, item: dict) -> float:
        for f in ("ap", "aP", "avg_price"):
            v = item.get(f)
            if v and not isinstance(v, dict):
                try:
                    val = float(v)
                    if val > 0:
                        return val
                except (TypeError, ValueError):
                    pass
        return 0.0

    # ─────────────────────────────────────────────────────
    #  Reconnect
    # ─────────────────────────────────────────────────────

    def _on_reconnect(self, new_client):
        self.client             = new_client
        self.trade_mgr.client   = new_client
        self.gap_scanner.client = new_client
        self._ws_connected      = False
        self._setup_websocket()
        time.sleep(2)
        all_tokens = [
            {"instrument_token": tok, "exchange_segment": config.CM_SEGMENT}
            for tok in self._subscribed_tokens
        ]
        print(f"[Reconnect] Re-subscribing {len(all_tokens)} tokens...")
        for i in range(0, len(all_tokens), 50):
            try:
                self.client.subscribe(
                    instrument_tokens=all_tokens[i:i+50],
                    isIndex=False,
                    isDepth=False,
                )
            except Exception as e:
                self.logger.error(f"[Reconnect] Subscribe batch error: {e}")
            time.sleep(1.0)
        print("[Reconnect] ✅ Session restored")

    # ─────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────

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
        if self._entries_open:
            entry_tag = "ENTRIES OPEN"
        elif self._no_entry_forced:
            entry_tag = f"no new entries after {config.NO_NEW_ENTRY_TIME}"
        else:
            entry_tag = f"entries open at {config.ENTRY_START}"

        print(f"\n[{t.strftime('%H:%M:%S')}] "
              f"Watching: {self.vwap_mgr.active_count}  "
              f"Open: {len(self.trade_mgr._open)}/{config.MAX_SIMULTANEOUS}  "
              f"Blocked: {len(self.trade_mgr._session_blocked)}  "
              f"Day P&L: ₹{self.trade_mgr.day_pnl_rs:+,.0f}  "
              f"[{entry_tag}]")
        if self.trade_mgr._open:
            self.trade_mgr.print_status()

    def _handle_sigterm(self, signum, frame):
        print(f"\n[Shutdown] Signal {signum} — stopping...")
        self._running = False

    def _graceful_shutdown(self):
        print("\n[Shutdown] Squaring off all positions...")
        try:
            self.trade_mgr.square_off_all()
        except Exception as e:
            self.logger.error(f"[Shutdown] square_off: {e}")
        try:
            report = self.report_mgr.generate_daily_report()
            print(report)
        except Exception as e:
            self.logger.error(f"[Shutdown] report: {e}")
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

    # ─────────────────────────────────────────────────────
    #  Main Loop
    # ─────────────────────────────────────────────────────

    def run(self):
        self.initialize()

        gap_scanned      = False
        last_status_min  = -1
        sq_done          = False
        consec_sl_paused = False
        consec_sl_resume = None
        last_rescan_time = None

        print(f"[Main] Running — waiting for {config.MARKET_OPEN} IST...\n")

        try:
            while self._running:
                t = now_ist()

                if t >= self._shutdown_time:
                    print("\n[Main] Auto-shutdown time reached")
                    break

                # ── Gap scan ──────────────────────────────
                market_open_t = datetime.time(*map(int, config.MARKET_OPEN.split(":")))
                should_scan   = (
                    (not gap_scanned and t.time() >= market_open_t) or
                    (gap_scanned
                     and last_rescan_time is not None
                     and not sq_done
                     and not self._no_entry_forced
                     and (t - last_rescan_time).total_seconds() >= config.SCAN_INTERVAL_SECS)
                )

                if should_scan:
                    self.run_gap_scan()
                    gap_scanned      = True
                    last_rescan_time = t
                    if t.time() >= self._entry_open_t and not self._entries_open:
                        self._entries_open = True
                        print(f"\n[Main] ✅ ENTRIES NOW OPEN — {t.strftime('%H:%M:%S')} IST")

                # ── Open entries at 9:30 ──────────────────
                if (gap_scanned
                        and not self._entries_open
                        and not self._no_entry_forced
                        and not consec_sl_paused
                        and t.time() >= self._entry_open_t):
                    self._entries_open = True
                    print(f"\n[Main] ✅ ENTRIES NOW OPEN — {t.strftime('%H:%M:%S')} IST  "
                          f"({self.vwap_mgr.active_count} stocks watching)")

                # ── Resume after consec SL pause ──────────
                if consec_sl_paused and consec_sl_resume and t >= consec_sl_resume:
                    consec_sl_paused         = False
                    consec_sl_resume         = None
                    self.trade_mgr.consec_sl = 0
                    if not self._no_entry_forced:
                        self._entries_open = True
                        print(f"\n[Guard] Consec SL pause lifted — entries re-enabled "
                              f"({t.strftime('%H:%M')})")

                # ── Pause on 5 consecutive SLs ────────────
                # FIX (Bug 4): _entries_open = False here is what gates
                # _check_entry_signal in _on_message. All three threads
                # (WS, RESTGapPoller, RESTTrendScanner) check _entries_open
                # before calling _check_entry_signal, so setting it False here
                # immediately stops all new entries across all threads.
                if (self.trade_mgr.consec_sl >= config.MAX_CONSEC_SL
                        and not consec_sl_paused
                        and not self.trade_mgr.day_stopped):
                    consec_sl_paused   = True
                    consec_sl_resume   = t + datetime.timedelta(minutes=30)
                    self._entries_open = False
                    print(f"\n[Guard] {config.MAX_CONSEC_SL} consecutive SLs — "
                          f"entries PAUSED for 30 min  "
                          f"(resume {consec_sl_resume.strftime('%H:%M')})")

                # ── No new entries after 15:00 ────────────
                if gap_scanned and not self._no_entry_forced:
                    if t.time() >= self._no_new_entry_t:
                        self._no_entry_forced = True   # one-way latch
                        self._entries_open    = False
                        print(f"\n[Main] ⏹  No new entries after "
                              f"{config.NO_NEW_ENTRY_TIME} — managing open positions only")

                # ── Square off at 15:10 ───────────────────
                if t.time() >= self._sq_off_t and not sq_done:
                    print(f"\n[Main] 🔔 {config.SQUARE_OFF_TIME} — squaring off all positions")
                    self.trade_mgr.square_off_all()
                    self._entries_open    = False
                    self._no_entry_forced = True
                    sq_done               = True
                    self._running         = False

                # ── Status print every minute ─────────────
                if gap_scanned and t.minute != last_status_min:
                    self._print_status()
                    self._check_no_tick()
                    last_status_min = t.minute

                time.sleep(1)

        except KeyboardInterrupt:
            print("\n[Main] Keyboard interrupt")
        finally:
            self._graceful_shutdown()


# ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    algo = GapVWAPAlgo()
    algo.run()
