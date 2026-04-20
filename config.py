# ============================================================
# CONFIG.PY — NSE Gap VWAP Trend Algo v3
# ============================================================
#
# STRATEGY PHILOSOPHY (v3 — based on live chart analysis):
#
#  The core edge is catching stocks where price and VWAP are
#  moving TOGETHER in the same direction — not against each other.
#  This means:
#
#  MOMENTUM CONTINUATION (primary — new):
#    A stock gaps up AND stays above rising VWAP all day →
#    catch on any pullback to VWAP → LONG (ride the trend)
#
#    A stock gaps down AND stays below falling VWAP all day →
#    catch on any bounce to VWAP → SHORT (ride the downtrend)
#
#    Examples: RAILTEL, GALLANTT, DEEPAKFERT, NETWEB, AFCONS
#    all showed this pattern on 15-Apr. INDIANB was the perfect
#    example of a short — it stayed below falling VWAP for 82 mins.
#
#  GAP REVERSAL (secondary — retained with stricter filters):
#    A gap stock that crosses VWAP with CONFIRMATION (3+ bars
#    on the right side, not just a single wick cross)
#
#  VWAP BREAKOUT (tertiary — breakout from flat period):
#    Flat VWAP → sudden directional move with volume spike
#
# KEY CHANGES FROM v2:
#   1. MIN_GAP_PCT = 3% — stocks with 3%+ gaps qualify
#   2. VWAP cross requires 3 bar confirmation (no more wick traps)
#   3. SL widened from 0.5% to 0.8% — survive normal noise
#   4. GAP_REVERSAL SL tightened to VWAP level (not fixed %)
#   5. VWAP data source: exchange 'ap' field (always preferred) +
#      fallback to REST poll every 15s (not 30s) for fresher data
#   6. Watchlist expanded from 340 to ~2100 real EQ stocks
#   7. Minimum intraday volume check at ENTRY TIME (not just scan)
#   8. Trade budget split: 5 slots for TREND, 3 for REVERSAL/BREAKOUT
#   9. Consecutive SL pause threshold raised from 4 to 5
#  10. VWAP_TREND_LONG / VWAP_TREND_SHORT added as new signal types
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

# ── Mode ──────────────────────────────────────────────────
PAPER_TRADE         = True     # Set False only when ready for live

# ── Kotak Neo Credentials (from .env) ─────────────────────
KOTAK_CONSUMER_KEY    = os.getenv("KOTAK_CONSUMER_KEY",    "")
KOTAK_MOBILE_NUMBER   = os.getenv("KOTAK_MOBILE_NUMBER",   "")
KOTAK_UCC             = os.getenv("KOTAK_UCC",             "")
KOTAK_MPIN            = os.getenv("KOTAK_MPIN",            "")
KOTAK_ENVIRONMENT     = os.getenv("KOTAK_ENVIRONMENT",     "prod")

# ── Capital & Position Sizing ─────────────────────────────
TOTAL_CAPITAL        = 200_000   # Rs 2,00,000 real capital
LEVERAGE             = 4         # 4x intraday leverage
CAPITAL_PER_TRADE    = 25_000    # Rs 25,000 per trade (buying power)
MAX_SIMULTANEOUS     = 8         # Max 8 trades open at once

# Trade budget split (must sum to MAX_SIMULTANEOUS):
#   TREND signals get 5 slots — the primary edge
#   REVERSAL + BREAKOUT share 3 slots
MAX_TREND_SLOTS      = 5         # For VWAP_TREND_LONG / VWAP_TREND_SHORT
MAX_OTHER_SLOTS      = 3         # For GAP_REVERSAL + VWAP_BREAKOUT

# ── Gap Scanner Settings ───────────────────────────────────
MIN_GAP_PCT          = 3.0       # Stocks with 3%+ gaps qualify
MAX_GAP_PCT          = 25.0      # Skip extreme circuit moves
MIN_PREV_VOLUME      = 500_000   # Min previous day volume (liquidity filter)
MIN_PRICE            = 50.0      # Skip penny stocks
MIN_INTRADAY_VOLUME  = 100_000   # NEW: min shares traded since 9:15 before entry
                                  # Filters thin stocks where VWAP is unreliable
SCAN_BATCH_SIZE      = 50
SCAN_INTERVAL_SECS   = 300       # Rescan every 5 min for new gaps

# ── VWAP Signal Parameters ─────────────────────────────────
VWAP_MIN_TICKS           = 10    # Minimum ticks before any signal fires

# GAP_REVERSAL — the "price crossed VWAP" signal (STRICTER in v3)
CROSS_BUFFER_PCT         = 0.3   # Entry must be X% from VWAP for gap reversal (allowed range: 0.05 – 0.3)
CROSS_BUFFER_PCT         = max(0.05, min(0.3, CROSS_BUFFER_PCT))   # Clamp: never below 0.05 or above 0.3
CROSS_CONFIRM_BARS       = 3     # NEW: require 3 consecutive bars on new side
                                  # This prevents wick trap entries
                                  # v2 entered on the FIRST cross — caused 89% SL rate

# VWAP_TREND_LONG / VWAP_TREND_SHORT — the primary new signal
TREND_CONFIRM_BARS       = 15    # Bars above/below VWAP to confirm trend direction
TREND_PULLBACK_PCT       = 0.4   # Max distance from VWAP to consider "at VWAP"
TREND_VWAP_SLOPE_MIN     = 0.003 # Min VWAP slope %/min to confirm directional trend
                                  # A flat VWAP = no trend = no entry
                                  # RAILTEL, GALLANTT, DEEPAKFERT all had rising VWAP
TREND_MIN_CANDLES_ONSIDE = 20    # Price must have been on trend side for 20+ bars

# VWAP_BREAKOUT
FLAT_MIN_MINUTES     = 90
BREAKOUT_DIST_PCT    = 0.5
BREAKOUT_VOL_MULT    = 1.8

# ── SL & Target ───────────────────────────────────────────
SL_PCT               = 0.8       # WIDENED from 0.5% — survive normal noise
                                  # 0.5% was being stopped by 1-2 candle wicks
                                  # on stocks that eventually moved correctly

TRAIL_TRIGGER_PCT    = 1.0       # Trail activates at +1% profit
TRAIL_BUFFER_PCT     = 0.5       # Trail SL = peak - 0.5%
TARGET_PCT           = 3.0       # Hard target 3% (raised from 2.5%)

# For GAP_REVERSAL specifically — SL is placed just above VWAP at entry
# (tighter, because VWAP is the thesis — if price goes back above VWAP
#  after a SHORT reversal, the trade is wrong)
GAP_REVERSAL_SL_BUFFER = 0.5    # Place SL 0.5% beyond VWAP level at entry

# ── Timing Guards ─────────────────────────────────────────
MARKET_OPEN          = "09:15"
ENTRY_START          = "09:30"
SQUARE_OFF_TIME      = "15:18"

# ── VWAP Data Strategy ────────────────────────────────────
# PRIMARY:   Exchange 'ap' field on WS ticks (most accurate)
# SECONDARY: REST poll every 15s (increased from 30s for fresher data)
# TERTIARY:  Self-compute from (H+L+C)/3 × volume (fallback only)
#
# WHY NOT WebSocket for ALL 2000+ stocks:
#   Subscribing 2000+ stocks floods the single WS connection.
#   Strategy: subscribe ONLY gap stocks (30–100 stocks) via WS.
#   For non-gap stocks in the trend watchlist, use REST batch poll.
#   This gives real VWAP data without overloading the connection.
REST_POLL_INTERVAL   = 15        # Poll gap stocks every 15s (was 30s)
REST_TREND_INTERVAL  = 60        # Poll full watchlist for trend scan every 60s
                                  # Only need to detect trend — not tick precision

# ── Daily Risk Guards ─────────────────────────────────────
MAX_DAILY_LOSS_RS    = -10_000
MAX_CONSEC_SL        = 5         # RAISED from 4 — avoid hair-trigger pauses

# ── Watchlist Mode ────────────────────────────────────────
# 'file' → Read from watchlist.csv
# v3 watchlist has ~2100 EQ stocks from NSE scrip master
# Filtered: ISIN starts INE (real equity, not ETF/index fund)
WATCHLIST_MODE       = "file"
WATCHLIST_FILE       = "watchlist.csv"

# ── Costs (equity intraday) ───────────────────────────────
BROKERAGE_PCT        = 0.0
STT_INTRADAY_PCT     = 0.00025
EXCHANGE_TXN_PCT     = 0.0000297
SEBI_PCT             = 0.000001
GST_PCT              = 0.18
STAMP_DUTY_PCT       = 0.00003

# ── Segments ──────────────────────────────────────────────
CM_SEGMENT           = "nse_cm"

# ── Files ─────────────────────────────────────────────────
TRADE_LOG_FILE       = "reports/trade_log.csv"
GAP_LIST_FILE        = "reports/gap_list.csv"
CAPITAL_FILE         = "capital.json"
