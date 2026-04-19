# ============================================================
# CONFIG.PY — NSE Gap VWAP Trend Algo v4
# ============================================================
#
# WHAT CHANGED FROM v3:
#   1. MAX_TRADES_PER_DAY removed completely.
#      Only limit is MAX_SIMULTANEOUS = 8 open at once.
#      When one closes, next signal takes that slot. All day.
#   2. Trade slot budget (TREND 5 / OTHER 3 split) removed.
#      All 8 slots are equal. Any signal type fills any slot.
#   3. Watchlist expanded to 2133 real NSE EQ stocks.
#   4. SESSION_BLOCKED: stocks that hit SL blocked for rest of day.
#   5. GAP_DIRECTION_LOCK: gap stocks only trade in gap direction.
#   6. RESTTrendScanner scrip list now returns proper dicts.
# ============================================================

import os

def _load_env(path=".env"):
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val

_load_env()

PAPER_TRADE         = True

KOTAK_CONSUMER_KEY    = os.getenv("KOTAK_CONSUMER_KEY",    "")
KOTAK_MOBILE_NUMBER   = os.getenv("KOTAK_MOBILE_NUMBER",   "")
KOTAK_UCC             = os.getenv("KOTAK_UCC",             "")
KOTAK_MPIN            = os.getenv("KOTAK_MPIN",            "")
KOTAK_ENVIRONMENT     = os.getenv("KOTAK_ENVIRONMENT",     "prod")

# ── Capital & Position Sizing ─────────────────────────────
TOTAL_CAPITAL        = 200_000
LEVERAGE             = 4
CAPITAL_PER_TRADE    = 25_000
MAX_SIMULTANEOUS     = 8        # Only slot limit. No daily cap. No type split.

# ── Gap Scanner ───────────────────────────────────────────
MIN_GAP_PCT          = 5.0
MAX_GAP_PCT          = 25.0
MIN_PREV_VOLUME      = 500_000
MIN_PRICE            = 50.0
MIN_INTRADAY_VOLUME  = 100_000
SCAN_BATCH_SIZE      = 50
SCAN_INTERVAL_SECS   = 300

# ── VWAP Signal Parameters ─────────────────────────────────
VWAP_MIN_TICKS           = 10
CROSS_BUFFER_PCT         = 0.05
CROSS_CONFIRM_BARS       = 3
TREND_CONFIRM_BARS       = 15
TREND_PULLBACK_PCT       = 0.4
TREND_VWAP_SLOPE_MIN     = 0.003
TREND_MIN_CANDLES_ONSIDE = 20
FLAT_MIN_MINUTES         = 90
BREAKOUT_DIST_PCT        = 0.5
BREAKOUT_VOL_MULT        = 1.8

# ── SL & Target ───────────────────────────────────────────
SL_PCT                 = 0.8
TRAIL_TRIGGER_PCT      = 1.0
TRAIL_BUFFER_PCT       = 0.5
TARGET_PCT             = 3.0
GAP_REVERSAL_SL_BUFFER = 0.2

# ── v4 Guards ─────────────────────────────────────────────
# After SL exit, symbol is added to _session_blocked set in TradeManager.
# It cannot be entered again for the rest of the day.
# This prevents the HDBFS pattern: same stock, 2 SL losses in one day.

# For gap stocks, only entries matching the gap direction are allowed:
#   GAP_UP  stock → only SHORT entries allowed
#   GAP_DOWN stock → only LONG entries allowed
# Prevents FSL being shorted AND longed on the same day.
GAP_DIRECTION_LOCK = True

# ── Timing ────────────────────────────────────────────────
MARKET_OPEN       = "09:15"
ENTRY_START       = "09:30"
SQUARE_OFF_TIME   = "15:10"
NO_NEW_ENTRY_TIME = "15:00"

# ── VWAP Data ─────────────────────────────────────────────
REST_POLL_INTERVAL  = 15
REST_TREND_INTERVAL = 60

# ── Risk Guards ───────────────────────────────────────────
# MAX_TRADES_PER_DAY: REMOVED. No daily trade limit.
MAX_DAILY_LOSS_RS = -10_000
MAX_CONSEC_SL     = 5

# ── Watchlist ─────────────────────────────────────────────
WATCHLIST_MODE = "file"
WATCHLIST_FILE = "watchlist.csv"

# ── Transaction Costs ─────────────────────────────────────
# B-09 FIX: Kotak Neo charges flat ₹20 per order (not percentage).
# Each trade = 2 orders (entry + exit) = ₹40 total brokerage per trade.
# Previously BROKERAGE_PCT = 0.0 was correct but the ₹40 flat fee was
# missing entirely, understating costs by ~₹40/trade (₹640 on 16 trades).
BROKERAGE_FLAT_RS    = 20          # ₹20 per order × 2 orders = ₹40/trade
BROKERAGE_PCT        = 0.0         # No percentage brokerage (flat fee model)
STT_INTRADAY_PCT     = 0.00025
EXCHANGE_TXN_PCT     = 0.0000297
SEBI_PCT             = 0.000001
GST_PCT              = 0.18
STAMP_DUTY_PCT       = 0.00003

CM_SEGMENT     = "nse_cm"
TRADE_LOG_FILE = "reports/trade_log.csv"
GAP_LIST_FILE  = "reports/gap_list.csv"
CAPITAL_FILE   = "capital.json"
