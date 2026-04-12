# ============================================================
# CONFIG.PY — NSE Gap Up/Down VWAP Reversal Algo
# ============================================================
# Strategy:
#   1. Scan Nifty 500 stocks at 9:15 AM for gaps >= 4%
#   2. Watch gap stocks continuously for VWAP cross
#   3. Gap Up  → price crosses BELOW VWAP → SHORT  (reversal)
#   4. Gap Down → price crosses ABOVE VWAP → LONG   (reversal)
#   5. SL: 0.5% from entry | Target: 2–3% trail
#   6. Entries only after 9:30 AM
#   7. Paper mode with full trade log + daily report
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
LEVERAGE             = 4         # 4x intraday leverage = Rs 8,00,000 buying power
CAPITAL_PER_TRADE    = 25_000    # Rs 25,000 per trade (buying power)
MAX_SIMULTANEOUS     = 8         # Max 8 trades open at once

# ── Gap Scanner Settings ───────────────────────────────────
MIN_GAP_PCT          = 3.0       # Minimum gap % to qualify (3%)
MAX_GAP_PCT          = 20.0      # Skip runaway gaps > 20%
MIN_PREV_VOLUME      = 500_000   # Min previous day volume (raised — avoids illiquid stocks)
MIN_PRICE            = 50.0      # Skip penny stocks below ₹50
SCAN_BATCH_SIZE      = 50        # Quotes API max per call
SCAN_INTERVAL_SECS   = 300       # Re-scan every 5 minutes for new gap stocks

# ── VWAP Strategy ─────────────────────────────────────────
VWAP_MIN_TICKS       = 5
VWAP_CROSS_BUFFER    = 0.05

# ── SL & Target ───────────────────────────────────────────
SL_PCT               = 0.5
TRAIL_TRIGGER_PCT    = 1.0
TRAIL_BUFFER_PCT     = 0.5
TARGET_PCT           = 2.5

# ── New Signal Parameters ─────────────────────────────────
FLAT_MIN_MINUTES     = 90        # Min flat VWAP minutes before breakout signal
BREAKOUT_DIST_PCT    = 0.5       # Price must be >0.5% from VWAP to confirm breakout
BREAKOUT_VOL_MULT    = 1.8       # Volume must be 1.8x flat-period average

# ── Timing Guards ─────────────────────────────────────────
MARKET_OPEN          = "09:15"   # Market open — start scanning
ENTRY_START          = "09:30"   # No entries before 9:30 AM
SQUARE_OFF_TIME      = "15:10"   # Square off all positions & stop algo
NO_NEW_ENTRY_TIME    = "15:00"   # No new entries after 3:00 PM

# ── Daily Risk Guards ─────────────────────────────────────
MAX_TRADES_PER_DAY   = 16        # Max 8 simultaneous × 2 rounds max
MAX_DAILY_LOSS_RS    = -10_000   # Stop all if day loss > Rs 10,000
MAX_CONSEC_SL        = 4         # Pause after 4 consecutive SL hits

# ── Watchlist Mode ────────────────────────────────────────
# 'file'      → Read from watchlist.csv (recommended — curated high-liquidity list)
# 'nifty500'  → Auto-filter from NSE scrip master
# 'nifty200'  → Smaller, faster scan
# 'custom'    → Use CUSTOM_SYMBOLS list below
WATCHLIST_MODE       = "file"
WATCHLIST_FILE       = "watchlist.csv"   # one symbol per line, header = SYMBOL

CUSTOM_SYMBOLS = [
    # Add custom symbols here if WATCHLIST_MODE = 'custom'
    # e.g. "RELIANCE", "TCS", "HDFCBANK"
]

# ── Costs (equity intraday) ───────────────────────────────
BROKERAGE_PCT        = 0.0       # Zero brokerage via Kotak Neo API
STT_INTRADAY_PCT     = 0.00025   # STT on sell side only
EXCHANGE_TXN_PCT     = 0.0000297 # NSE transaction charge
SEBI_PCT             = 0.000001
GST_PCT              = 0.18      # On brokerage + transaction charges
STAMP_DUTY_PCT       = 0.00003   # On buy side

# ── Segments ──────────────────────────────────────────────
CM_SEGMENT           = "nse_cm"

# ── Files ─────────────────────────────────────────────────
TRADE_LOG_FILE       = "reports/trade_log.csv"
GAP_LIST_FILE        = "reports/gap_list.csv"
CAPITAL_FILE         = "capital.json"
