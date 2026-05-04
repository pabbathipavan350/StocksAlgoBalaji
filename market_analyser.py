# ============================================================
# MARKET_ANALYSER.PY — Market Context + Algo Behaviour Analyser
# ============================================================
#
# Runs COMPLETELY SEPARATELY from the main algo.
# Never imported by main.py — zero risk of breaking the algo.
#
# What it does:
#   1. Snapshots all major NSE indices every 5 minutes during market hours
#   2. Reads today's trade CSVs at EOD
#   3. Correlates algo P&L with market regime (trending/choppy/falling)
#   4. Saves a daily market_context_YYYYMMDD.json + human-readable .txt report
#   5. Builds a 30-day pattern file showing which market conditions the algo wins in
#
# HOW TO RUN:
#   python3 market_analyser.py
#   → Run this in a separate terminal alongside the main algo
#   → Or schedule it with cron: it auto-detects market hours
#
# SAFETY:
#   Every single network call is wrapped in try/except.
#   If any index fails to fetch, it logs a warning and continues.
#   The main algo is completely unaffected regardless.
# ============================================================

import os
import csv
import json
import time
import datetime
import threading
import traceback
import logging

# ── logging ──────────────────────────────────────────────────
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-7s  %(name)s | %(message)s",
    datefmt = "%H:%M:%S",
)
logger = logging.getLogger("market_analyser")

# Suppress urllib3 connection pool debug spam
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


# ── IST helpers ──────────────────────────────────────────────
def now_ist() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)

def today_str() -> str:
    return now_ist().strftime("%Y%m%d")

def market_open() -> bool:
    t = now_ist().time()
    return datetime.time(9, 15) <= t <= datetime.time(15, 35)


# ── Index definitions ────────────────────────────────────────
# Kotak Neo uses instrument_token for indices on nse_cm segment.
# These are standard NSE index tokens — stable across sessions.
# Each entry: (display_name, instrument_token, exchange_segment)
#
# If a token changes or is unavailable, that index is skipped
# gracefully — it never stops the snapshot loop.

INDICES = [
    # Major broad market
    ("NIFTY 50",          "26000", "nse_cm"),
    ("NIFTY BANK",        "26009", "nse_cm"),
    ("NIFTY MIDCAP 100",  "26012", "nse_cm"),
    ("NIFTY SMALLCAP 100","26034", "nse_cm"),
    ("NIFTY 500",         "26004", "nse_cm"),
    ("NIFTY 200",         "26013", "nse_cm"),

    # Sectoral — useful for understanding gap behaviour
    ("NIFTY IT",          "26017", "nse_cm"),
    ("NIFTY FMCG",        "26035", "nse_cm"),
    ("NIFTY AUTO",        "26008", "nse_cm"),
    ("NIFTY PHARMA",      "26016", "nse_cm"),
    ("NIFTY REALTY",      "26018", "nse_cm"),
    ("NIFTY METAL",       "26015", "nse_cm"),
    ("NIFTY ENERGY",      "26019", "nse_cm"),
    ("NIFTY INFRA",       "26020", "nse_cm"),
    ("NIFTY PSU BANK",    "26014", "nse_cm"),
    ("NIFTY MIDCAP 50",   "26011", "nse_cm"),
    ("NIFTY NEXT 50",     "26001", "nse_cm"),
    ("NIFTY MICROCAP 250","26083", "nse_cm"),
]

SNAPSHOT_INTERVAL_MINS = 5   # snapshot every 5 minutes
REPORTS_DIR            = "reports"


# ════════════════════════════════════════════════════════════
#  IndexFetcher — safe wrapper around Kotak quotes API
# ════════════════════════════════════════════════════════════
class IndexFetcher:
    """
    Fetches LTP for all defined indices using the Kotak Neo client.
    Every call is isolated in try/except — one bad token never
    prevents fetching the rest.
    """

    def __init__(self, client):
        self.client = client
        self._open_prices: dict = {}   # name → first LTP of the day

    def fetch_all(self) -> dict:
        """
        Returns dict: {index_name: {"ltp": float, "open": float,
                                     "change_pct": float, "token": str}}
        Missing indices are absent from the dict — never raises.
        """
        results = {}
        for name, token, segment in INDICES:
            try:
                ltp = self._fetch_ltp(token, segment)
                if ltp is None or ltp <= 0:
                    continue

                # Track open price (first snapshot of the day)
                if name not in self._open_prices:
                    self._open_prices[name] = ltp

                open_px     = self._open_prices[name]
                change_pct  = round((ltp - open_px) / open_px * 100, 3) if open_px else 0.0

                results[name] = {
                    "ltp"        : round(ltp, 2),
                    "open"       : round(open_px, 2),
                    "change_pct" : change_pct,
                    "token"      : token,
                }
            except Exception:
                logger.debug(f"[IndexFetch] {name} skipped: {traceback.format_exc(limit=1)}")
                continue

        return results

    def _fetch_ltp(self, token: str, segment: str):
        """Single index LTP fetch — mirrors gap_scanner.py's proven parser."""
        try:
            resp = self.client.quotes(
                instrument_tokens=[{
                    "instrument_token" : token,
                    "exchange_segment" : segment,
                }],
                quote_type="ltp",
            )
        except Exception as e:
            logger.debug(f"[IndexFetch] quotes() failed for {token}: {e}")
            return None

        # Unwrap — same logic as gap_scanner.py
        if isinstance(resp, list):
            data = resp
        elif isinstance(resp, dict):
            data = (resp.get("data") or resp.get("200") or
                    resp.get("message") or resp.get("result") or [])
        else:
            data = []

        if not isinstance(data, list):
            data = [data] if isinstance(data, dict) else []

        # Log raw response once on first call to help debug format issues
        if not getattr(self, "_logged_sample", False):
            logger.info(f"[IndexFetch] Sample raw resp for token {token}: "
                        f"type={type(resp).__name__} "
                        f"data={str(resp)[:300]}")
            self._logged_sample = True

        for item in data:
            if not isinstance(item, dict):
                continue

            # LTP can be flat or nested dict — handle both
            ltp_raw = item.get("ltp") or item.get("ltP") or item.get("last_price")

            if isinstance(ltp_raw, dict):
                # Nested: {'ltp': {'ltp': '23500.00'}}
                for f in ("ltp", "ltP", "lp", "last_price"):
                    v = ltp_raw.get(f)
                    if v is not None and v != "":
                        ltp_raw = v
                        break

            if ltp_raw is None:
                # Some Kotak responses use 'lp' directly on item
                ltp_raw = item.get("lp") or item.get("last_traded_price")

            try:
                val = float(str(ltp_raw).replace(",", ""))
                if val > 0:
                    return val
            except (TypeError, ValueError):
                pass

        # Still nothing — log the full item so we can fix it next time
        logger.debug(f"[IndexFetch] No LTP found for token {token}. "
                     f"data_len={len(data)} "
                     f"first_item={str(data[0])[:200] if data else 'empty'}")
        return None



# ════════════════════════════════════════════════════════════
#  RegimeClassifier — classify market into a regime label
# ════════════════════════════════════════════════════════════
class RegimeClassifier:
    """
    Uses intraday NIFTY 50 snapshots to classify market regime:
      STRONG_UP   → Nifty up >0.8% from open
      MILD_UP     → Nifty up 0.3–0.8%
      FLAT        → Nifty within ±0.3%
      MILD_DOWN   → Nifty down 0.3–0.8%
      STRONG_DOWN → Nifty down >0.8%
      CHOPPY      → large intraday range but small net change
    """

    STRONG_THRESH = 0.8
    MILD_THRESH   = 0.3
    CHOP_RANGE    = 1.2   # if high-low range > 1.2% but net < 0.3% → choppy

    def classify(self, snapshots: list) -> str:
        """snapshots = list of dicts from fetch_all(), ordered by time"""
        nifty_vals = []
        for snap in snapshots:
            n = snap.get("NIFTY 50")
            if n:
                nifty_vals.append(n["ltp"])

        if len(nifty_vals) < 2:
            return "UNKNOWN"

        first  = nifty_vals[0]
        last   = nifty_vals[-1]
        high   = max(nifty_vals)
        low    = min(nifty_vals)
        net    = (last - first) / first * 100
        rng    = (high - low)  / first * 100

        if rng >= self.CHOP_RANGE and abs(net) < self.MILD_THRESH:
            return "CHOPPY"
        if net >=  self.STRONG_THRESH: return "STRONG_UP"
        if net >=  self.MILD_THRESH:   return "MILD_UP"
        if net <= -self.STRONG_THRESH: return "STRONG_DOWN"
        if net <= -self.MILD_THRESH:   return "MILD_DOWN"
        return "FLAT"


# ════════════════════════════════════════════════════════════
#  TradeReader — reads EOD CSVs from the algo
# ════════════════════════════════════════════════════════════
class TradeReader:
    """
    Reads trade CSVs written by report_manager.py.
    Filters to today's trades only.
    Never raises — returns empty list on any failure.
    """

    CSV_FILES = [
        "reports/trade_log_GAP_REVERSAL.csv",
        "reports/trade_log_TREND.csv",
        "reports/trade_log_BREAKOUT.csv",
        "reports/trade_log_EARLY_TREND.csv",
    ]

    def read_today(self) -> list:
        today = now_ist().strftime("%Y-%m-%d")
        trades = []
        for path in self.CSV_FILES:
            try:
                if not os.path.exists(path):
                    continue
                with open(path, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get("Date", "").strip() == today:
                            trades.append(row)
            except Exception as e:
                logger.warning(f"[TradeReader] Failed to read {path}: {e}")
        return trades

    def analyse(self, trades: list) -> dict:
        if not trades:
            return {"total": 0}

        total   = len(trades)
        winners = [t for t in trades if _safe_float(t.get("Net PnL", "0")) > 0]
        losers  = [t for t in trades if _safe_float(t.get("Net PnL", "0")) <= 0]
        net_pnl = sum(_safe_float(t.get("Net PnL", "0")) for t in trades)
        avg_win  = (sum(_safe_float(t.get("Net PnL","0")) for t in winners)
                    / len(winners)) if winners else 0
        avg_loss = (sum(_safe_float(t.get("Net PnL","0")) for t in losers)
                    / len(losers))  if losers  else 0

        # By fill quality
        thin    = [t for t in trades if "THIN" in t.get("Fill Quality","")]
        normal  = [t for t in trades if "THIN" not in t.get("Fill Quality","")]
        thin_pnl   = sum(_safe_float(t.get("Net PnL","0")) for t in thin)
        normal_pnl = sum(_safe_float(t.get("Net PnL","0")) for t in normal)

        # By signal type
        by_signal = {}
        for t in trades:
            sig = t.get("Signal Type", "UNKNOWN")
            if sig not in by_signal:
                by_signal[sig] = {"count": 0, "pnl": 0.0, "wins": 0}
            by_signal[sig]["count"] += 1
            by_signal[sig]["pnl"]   += _safe_float(t.get("Net PnL","0"))
            if _safe_float(t.get("Net PnL","0")) > 0:
                by_signal[sig]["wins"] += 1

        # By direction
        longs  = [t for t in trades if t.get("Direction","") == "LONG"]
        shorts = [t for t in trades if t.get("Direction","") == "SHORT"]

        # Avg duration
        durations = [_safe_float(t.get("Duration (mins)","0")) for t in trades]
        avg_dur   = sum(durations) / len(durations) if durations else 0

        # Exit reason breakdown
        exit_reasons = {}
        for t in trades:
            reason = t.get("Exit Reason","Other").split()[0]
            exit_reasons[reason] = exit_reasons.get(reason, 0) + 1

        # Hourly P&L buckets (which hour of day is algo best/worst)
        hourly = {}
        for t in trades:
            try:
                hr = t.get("Entry Time","00:00:00")[:2]
                hr_key = f"{hr}:00"
                if hr_key not in hourly:
                    hourly[hr_key] = {"pnl": 0.0, "count": 0}
                hourly[hr_key]["pnl"]   += _safe_float(t.get("Net PnL","0"))
                hourly[hr_key]["count"] += 1
            except Exception:
                pass

        return {
            "total"          : total,
            "winners"        : len(winners),
            "losers"         : len(losers),
            "win_rate"       : round(len(winners)/total*100, 1),
            "net_pnl"        : round(net_pnl, 2),
            "avg_win"        : round(avg_win, 2),
            "avg_loss"       : round(avg_loss, 2),
            "rr_ratio"       : round(abs(avg_win/avg_loss), 2) if avg_loss else 0,
            "thin_book_count": len(thin),
            "thin_book_pnl"  : round(thin_pnl, 2),
            "normal_pnl"     : round(normal_pnl, 2),
            "by_signal"      : by_signal,
            "long_count"     : len(longs),
            "short_count"    : len(shorts),
            "long_pnl"       : round(sum(_safe_float(t.get("Net PnL","0")) for t in longs), 2),
            "short_pnl"      : round(sum(_safe_float(t.get("Net PnL","0")) for t in shorts), 2),
            "avg_duration"   : round(avg_dur, 1),
            "exit_reasons"   : exit_reasons,
            "hourly"         : hourly,
        }


# ════════════════════════════════════════════════════════════
#  PatternTracker — 30-day rolling pattern file
# ════════════════════════════════════════════════════════════
class PatternTracker:
    """
    Maintains reports/pattern_30d.json — rolling 30-day record of:
      date, regime, net_pnl, win_rate, thin_book_pnl, normal_pnl,
      nifty_change_pct, banknifty_change_pct, midcap_change_pct
    This is the file you bring back on May 31st for deep analysis.
    """

    FILE = "reports/pattern_30d.json"

    def load(self) -> list:
        try:
            if os.path.exists(self.FILE):
                with open(self.FILE) as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"[PatternTracker] load failed: {e}")
        return []

    def append(self, entry: dict):
        try:
            history = self.load()
            # Remove existing entry for today if re-running
            today = entry.get("date","")
            history = [h for h in history if h.get("date") != today]
            history.append(entry)
            # Keep last 60 days (buffer)
            history = history[-60:]
            with open(self.FILE, "w") as f:
                json.dump(history, f, indent=2)
        except Exception as e:
            logger.warning(f"[PatternTracker] save failed: {e}")

    def summary(self, history: list) -> str:
        """Human readable 30-day pattern summary."""
        if not history:
            return "  No history yet.\n"

        lines = []
        lines.append(f"  {'Date':<12} {'Regime':<14} {'Net PnL':>9} "
                     f"{'WR%':>6} {'Nifty%':>8} {'ThinBk':>9} {'Normal':>9}")
        lines.append(f"  {'─'*75}")

        for h in history[-30:]:
            lines.append(
                f"  {h.get('date',''):<12} "
                f"{h.get('regime','?'):<14} "
                f"Rs{h.get('net_pnl',0):>+8,.0f} "
                f"{h.get('win_rate',0):>5.1f}% "
                f"{h.get('nifty_change_pct',0):>+7.2f}% "
                f"Rs{h.get('thin_book_pnl',0):>+7,.0f} "
                f"Rs{h.get('normal_pnl',0):>+7,.0f}"
            )

        # Regime-wise average P&L
        regimes = {}
        for h in history:
            r = h.get("regime","UNKNOWN")
            if r not in regimes:
                regimes[r] = []
            regimes[r].append(h.get("net_pnl", 0))

        lines.append(f"\n  REGIME PERFORMANCE (avg daily P&L):")
        for regime, pnls in sorted(regimes.items()):
            avg = sum(pnls) / len(pnls)
            lines.append(f"    {regime:<16}: Rs{avg:>+8,.0f}  ({len(pnls)} days)")

        return "\n".join(lines) + "\n"


# ════════════════════════════════════════════════════════════
#  MarketAnalyser — main orchestrator
# ════════════════════════════════════════════════════════════
class MarketAnalyser:
    """
    Main class. Call start() to begin background snapshotting.
    Call generate_eod_report() at end of day to write the full report.
    Can also be run standalone (see __main__ block below).
    """

    def __init__(self, client=None):
        self.client    = client
        self.fetcher   = IndexFetcher(client) if client else None
        self.classifier= RegimeClassifier()
        self.reader    = TradeReader()
        self.tracker   = PatternTracker()

        self._snapshots: list  = []   # list of {time, index_name: {ltp, change_pct}}
        self._running          = False
        self._thread           = None

        os.makedirs(REPORTS_DIR, exist_ok=True)

    # ── Public API ───────────────────────────────────────────

    def start(self):
        """Start background snapshot thread. Non-blocking."""
        if not self.fetcher:
            logger.warning("[MarketAnalyser] No client — index fetching disabled. "
                           "EOD report will use trade data only.")
            return
        self._running = True
        self._thread  = threading.Thread(
            target = self._snapshot_loop,
            name   = "MarketAnalyser",
            daemon = True,
        )
        self._thread.start()
        logger.info("[MarketAnalyser] Background snapshot thread started "
                    f"(every {SNAPSHOT_INTERVAL_MINS} min)")

    def stop(self):
        self._running = False

    def generate_eod_report(self) -> str:
        """
        Call this at end of day (after square-off).
        Writes:
          reports/market_context_YYYYMMDD.json
          reports/market_context_YYYYMMDD.txt
        Returns the text report as a string.
        """
        try:
            return self._build_report()
        except Exception as e:
            msg = f"[MarketAnalyser] EOD report failed: {e}\n{traceback.format_exc()}"
            logger.error(msg)
            return msg

    # ── Snapshot loop ────────────────────────────────────────

    def _snapshot_loop(self):
        logger.info("[MarketAnalyser] Snapshot loop running")
        while self._running:
            try:
                if market_open():
                    self._take_snapshot()
                else:
                    logger.debug("[MarketAnalyser] Market closed — skipping snapshot")
            except Exception as e:
                logger.warning(f"[MarketAnalyser] Snapshot error (non-fatal): {e}")

            # Sleep in small chunks so stop() is responsive
            for _ in range(SNAPSHOT_INTERVAL_MINS * 60 // 5):
                if not self._running:
                    break
                time.sleep(5)

        logger.info("[MarketAnalyser] Snapshot loop stopped")

    def _take_snapshot(self):
        if not self.fetcher:
            return
        try:
            data = self.fetcher.fetch_all()
            if not data:
                logger.debug("[MarketAnalyser] Empty snapshot — all indices failed")
                return
            snap = {"time": now_ist().strftime("%H:%M")}
            snap.update(data)
            self._snapshots.append(snap)
            # Log a one-liner so you can see it in terminal
            nifty = data.get("NIFTY 50", {})
            bank  = data.get("NIFTY BANK", {})
            mid   = data.get("NIFTY MIDCAP 100", {})
            logger.info(
                f"[Snapshot] Nifty {nifty.get('ltp','-'):>8}  "
                f"({nifty.get('change_pct',0):>+.2f}%)  "
                f"Bank {bank.get('ltp','-'):>8}  "
                f"({bank.get('change_pct',0):>+.2f}%)  "
                f"MidCap {mid.get('ltp','-'):>8}  "
                f"({mid.get('change_pct',0):>+.2f}%)"
            )
        except Exception as e:
            logger.warning(f"[MarketAnalyser] _take_snapshot inner error: {e}")

    # ── Report builder ───────────────────────────────────────

    def _build_report(self) -> str:
        today      = now_ist().strftime("%Y-%m-%d")
        date_tag   = today_str()
        regime     = self.classifier.classify(self._snapshots)
        trades     = self.reader.read_today()
        trade_stats= self.reader.analyse(trades)

        # Final index snapshot (closing values)
        final_indices = {}
        if self._snapshots:
            final_indices = {k: v for k, v in self._snapshots[-1].items()
                             if k != "time"}
        elif self.fetcher:
            try:
                final_indices = self.fetcher.fetch_all()
            except Exception:
                pass

        # Build pattern entry
        nifty_chg = final_indices.get("NIFTY 50", {}).get("change_pct", 0)
        bank_chg  = final_indices.get("NIFTY BANK", {}).get("change_pct", 0)
        mid_chg   = final_indices.get("NIFTY MIDCAP 100", {}).get("change_pct", 0)
        small_chg = final_indices.get("NIFTY SMALLCAP 100", {}).get("change_pct", 0)

        pattern_entry = {
            "date"               : today,
            "regime"             : regime,
            "net_pnl"            : trade_stats.get("net_pnl", 0),
            "win_rate"           : trade_stats.get("win_rate", 0),
            "total_trades"       : trade_stats.get("total", 0),
            "thin_book_pnl"      : trade_stats.get("thin_book_pnl", 0),
            "normal_pnl"         : trade_stats.get("normal_pnl", 0),
            "nifty_change_pct"   : nifty_chg,
            "banknifty_change_pct": bank_chg,
            "midcap_change_pct"  : mid_chg,
            "smallcap_change_pct": small_chg,
            "snapshots_taken"    : len(self._snapshots),
        }
        self.tracker.append(pattern_entry)
        history = self.tracker.load()

        # Save JSON
        json_path = f"{REPORTS_DIR}/market_context_{date_tag}.json"
        ctx_data  = {
            "date"          : today,
            "regime"        : regime,
            "indices"       : final_indices,
            "snapshots"     : self._snapshots,
            "trade_stats"   : trade_stats,
            "pattern_entry" : pattern_entry,
        }
        try:
            with open(json_path, "w") as f:
                json.dump(ctx_data, f, indent=2)
        except Exception as e:
            logger.warning(f"[MarketAnalyser] JSON save failed: {e}")

        # ── Build text report ────────────────────────────────
        sep  = "=" * 65
        sep2 = "─" * 61

        r  = f"\n{sep}\n"
        r += f"  MARKET CONTEXT REPORT — {today}\n"
        r += f"{sep}\n\n"

        # Market regime
        regime_emoji = {
            "STRONG_UP"  : "🟢 STRONG UP",
            "MILD_UP"    : "🟡 MILD UP",
            "FLAT"       : "⬜ FLAT",
            "MILD_DOWN"  : "🟠 MILD DOWN",
            "STRONG_DOWN": "🔴 STRONG DOWN",
            "CHOPPY"     : "🌀 CHOPPY",
            "UNKNOWN"    : "❓ UNKNOWN",
        }.get(regime, regime)

        r += f"  MARKET REGIME   : {regime_emoji}\n"
        r += f"  Snapshots taken : {len(self._snapshots)}\n\n"

        # Index table
        r += f"  MAJOR INDICES\n  {sep2}\n"
        r += f"  {'Index':<24} {'LTP':>10}  {'Open':>10}  {'Change%':>9}\n"
        r += f"  {'─'*57}\n"
        index_order = [
            "NIFTY 50", "NIFTY BANK", "NIFTY MIDCAP 100",
            "NIFTY SMALLCAP 100", "NIFTY 500", "NIFTY 200",
            "NIFTY NEXT 50", "NIFTY MIDCAP 50", "NIFTY MICROCAP 250",
            "NIFTY IT", "NIFTY FMCG", "NIFTY AUTO", "NIFTY PHARMA",
            "NIFTY REALTY", "NIFTY METAL", "NIFTY ENERGY",
            "NIFTY INFRA", "NIFTY PSU BANK",
        ]
        found_any = False
        for name in index_order:
            d = final_indices.get(name)
            if not d:
                continue
            found_any = True
            arrow = "▲" if d["change_pct"] >= 0 else "▼"
            r += (f"  {name:<24} {d['ltp']:>10,.2f}  "
                  f"{d['open']:>10,.2f}  "
                  f"{arrow}{abs(d['change_pct']):>7.2f}%\n")
        if not found_any:
            r += "  (No index data — API unavailable today)\n"
        r += "\n"

        # Intraday Nifty movement
        if self._snapshots:
            r += f"  NIFTY 50 INTRADAY MOVEMENT\n  {sep2}\n"
            prev_val = None
            for snap in self._snapshots:
                n = snap.get("NIFTY 50")
                if not n:
                    continue
                trend = ""
                if prev_val:
                    diff = n["ltp"] - prev_val
                    trend = f"  {'▲' if diff>=0 else '▼'} {abs(diff):.1f}"
                r += f"  {snap['time']}  {n['ltp']:>9,.2f}  ({n['change_pct']:>+.2f}%){trend}\n"
                prev_val = n["ltp"]
            r += "\n"

        # Algo performance
        r += f"  ALGO PERFORMANCE\n  {sep2}\n"
        if trade_stats.get("total", 0) == 0:
            r += "  No trades found for today.\n"
        else:
            ts = trade_stats
            r += f"  Total Trades     : {ts['total']}\n"
            r += f"  Win Rate         : {ts['win_rate']}%\n"
            r += f"  Net P&L          : Rs{ts['net_pnl']:>+,.0f}\n"
            r += f"  Avg Win          : Rs{ts['avg_win']:>+,.0f}\n"
            r += f"  Avg Loss         : Rs{ts['avg_loss']:>+,.0f}\n"
            r += f"  R:R Ratio        : {ts['rr_ratio']:.2f}\n"
            r += f"  Avg Duration     : {ts['avg_duration']} mins\n\n"

            r += f"  DIRECTION SPLIT\n"
            r += f"  LONG  : {ts['long_count']} trades   Rs{ts['long_pnl']:>+,.0f}\n"
            r += f"  SHORT : {ts['short_count']} trades   Rs{ts['short_pnl']:>+,.0f}\n\n"

            r += f"  FILL QUALITY SPLIT\n"
            r += f"  Normal fills : {ts['total'] - ts['thin_book_count']} trades  Rs{ts['normal_pnl']:>+,.0f}\n"
            r += f"  Thin book    : {ts['thin_book_count']} trades  Rs{ts['thin_book_pnl']:>+,.0f}\n\n"

            r += f"  BY SIGNAL TYPE\n"
            for sig, sd in ts.get("by_signal", {}).items():
                wr = sd["wins"]/sd["count"]*100 if sd["count"] else 0
                r += (f"  {sig:<22}: {sd['count']:>3} trades  "
                      f"WR {wr:>5.1f}%  Rs{sd['pnl']:>+8,.0f}\n")
            r += "\n"

            r += f"  HOURLY P&L BREAKDOWN\n"
            for hr, hd in sorted(ts.get("hourly", {}).items()):
                bar_len = int(abs(hd["pnl"]) / 200)
                bar = ("█" * min(bar_len, 20))
                sign = "+" if hd["pnl"] >= 0 else "-"
                r += (f"  {hr}  Rs{hd['pnl']:>+7,.0f}  "
                      f"[{sign}{bar:<20}]  {hd['count']} trades\n")
            r += "\n"

            r += f"  EXIT REASON BREAKDOWN\n"
            for reason, count in sorted(ts.get("exit_reasons", {}).items()):
                r += f"  {reason:<22}: {count}\n"
            r += "\n"

        # Correlation insight
        r += f"  MARKET vs ALGO INSIGHT\n  {sep2}\n"
        net = trade_stats.get("net_pnl", 0)
        r += f"  Market Regime : {regime}\n"
        r += f"  Algo Result   : Rs{net:>+,.0f}\n"
        if regime in ("STRONG_UP", "MILD_UP") and net > 0:
            r += "  ✅ Algo profitable on UP day — good alignment\n"
        elif regime in ("STRONG_DOWN", "MILD_DOWN") and net > 0:
            r += "  ✅ Algo profitable on DOWN day — short bias working\n"
        elif regime == "CHOPPY" and net < 0:
            r += "  ⚠️  Choppy market + loss — typical. Consider reducing trades on choppy days.\n"
        elif regime in ("STRONG_UP", "MILD_UP") and net < 0:
            r += "  ⚠️  Loss on UP day — gap reversal shorts may be fighting the trend.\n"
        elif regime in ("STRONG_DOWN", "MILD_DOWN") and net < 0:
            r += "  ⚠️  Loss on DOWN day — gap reversal longs may be fighting the trend.\n"
        elif regime == "FLAT" and net < 0:
            r += "  ⚠️  Flat market + loss — signal may be too noisy with no clear direction.\n"

        thin_pnl = trade_stats.get("thin_book_pnl", 0)
        if thin_pnl < -500:
            r += f"  🚨 Thin book trades cost Rs{abs(thin_pnl):,.0f} today — consider SKIP filter.\n"
        r += "\n"

        # 30-day pattern
        r += f"  30-DAY PATTERN\n  {sep2}\n"
        r += self.tracker.summary(history)
        r += "\n"

        r += f"{sep}\n"

        # Save text report
        txt_path = f"{REPORTS_DIR}/market_context_{date_tag}.txt"
        try:
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(r)
            logger.info(f"[MarketAnalyser] Report saved: {txt_path}")
            logger.info(f"[MarketAnalyser] JSON saved:   {json_path}")
        except Exception as e:
            logger.warning(f"[MarketAnalyser] Text save failed: {e}")

        print(r)
        return r


# ════════════════════════════════════════════════════════════
#  Helpers
# ════════════════════════════════════════════════════════════
def _safe_float(val, default=0.0) -> float:
    try:
        return float(str(val).replace(",","").replace("+",""))
    except (TypeError, ValueError):
        return default


# ════════════════════════════════════════════════════════════
#  Standalone runner — python3 market_analyser.py
# ════════════════════════════════════════════════════════════
def _run_standalone():
    """
    Run market_analyser standalone without the main algo.
    Imports Kotak client the same way main.py does.
    Snapshots every 5 min during market hours, generates EOD report at 15:35.
    """
    print("\n" + "="*65)
    print("  MARKET ANALYSER — Standalone Mode")
    print("  Snapshots every 5 min  |  EOD report at 15:35 IST")
    print("  Ctrl+C to stop")
    print("="*65 + "\n")

    # Import auth — same pattern as main.py
    try:
        from auth import get_client
        client = get_client()
        logger.info("[Standalone] Kotak client connected")
    except Exception as e:
        logger.warning(f"[Standalone] Could not connect Kotak client: {e}")
        logger.warning("[Standalone] Running in trade-data-only mode (no live index fetch)")
        client = None

    analyser = MarketAnalyser(client)
    analyser.start()

    eod_generated = False

    try:
        while True:
            now = now_ist()
            t   = now.time()

            # Generate EOD report at 15:35 once
            if t >= datetime.time(15, 35) and not eod_generated:
                logger.info("[Standalone] Market closed — generating EOD report")
                analyser.generate_eod_report()
                eod_generated = True

            # Reset for next day at midnight
            if t < datetime.time(9, 0):
                eod_generated = False

            time.sleep(30)

    except KeyboardInterrupt:
        logger.info("\n[Standalone] Stopped by user")
        if not eod_generated:
            logger.info("[Standalone] Generating partial report...")
            analyser.generate_eod_report()
        analyser.stop()


# ════════════════════════════════════════════════════════════
#  Integration helper — call from main.py
# ════════════════════════════════════════════════════════════
def create_analyser(client) -> "MarketAnalyser":
    """
    Called from main.py to create and start the analyser.
    Safe to call even if client is None — will degrade gracefully.

    Usage in main.py (optional, after trade_mgr init):
        from market_analyser import create_analyser
        self.market_analyser = create_analyser(self.client)
        # At EOD, after square_off_all():
        self.market_analyser.generate_eod_report()
    """
    try:
        a = MarketAnalyser(client)
        a.start()
        return a
    except Exception as e:
        logger.warning(f"[MarketAnalyser] create_analyser failed: {e} — disabled")
        return MarketAnalyser(None)   # degraded instance, never raises


if __name__ == "__main__":
    _run_standalone()
