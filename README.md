# NSE Gap Up / Gap Down VWAP Reversal Algo

## Strategy

1. **9:15 AM** — Scans Nifty 500 stocks for gaps ≥ 4%
2. **9:15 AM** — Subscribes to all gap stocks via WebSocket; VWAP calculation begins immediately
3. **9:30 AM** — Entries unlock:
   - **Gap Up stock** → wait for price to cross **BELOW VWAP** → enter **SHORT**
   - **Gap Down stock** → wait for price to cross **ABOVE VWAP** → enter **LONG**
4. **SL**: 0.5% from entry (hard stop)
5. **Trail**: activates at +1% profit; SL ratchets to `peak - 0.5%`
6. **Target**: 2.5% (trail may catch more)
7. **3:15 PM** — All positions squared off

## Capital

| Parameter | Value |
|---|---|
| Total Capital | ₹2,00,000 |
| Leverage | 4x intraday |
| Per Trade | ₹25,000 capital (= ₹1,00,000 exposure) |
| Max Simultaneous | 8 trades |
| Max Buying Power | ₹8,00,000 |

## Setup

```bash
# 1. Install dependencies
pip install neo_api_client pyotp

# 2. Fill in your credentials
cp .env.template .env
# Edit .env with your Kotak Neo credentials

# 3. (Optional) Add Nifty 500 symbol list for accurate filtering
# Download from NSE India and save as nifty500_symbols.csv
# One symbol per line: RELIANCE, TCS, HDFCBANK, ...

# 4. Run in paper mode (PAPER_TRADE = True in config.py)
python main.py
```

## Files

| File | Purpose |
|---|---|
| `main.py` | Main orchestrator — run this |
| `config.py` | All settings — edit gap%, SL, capital here |
| `gap_scanner.py` | Loads scrip master, fetches prev close, finds gaps |
| `vwap_engine.py` | Per-stock VWAP tracker + signal generator |
| `trade_manager.py` | Order management, SL/trail/target monitoring |
| `report_manager.py` | CSV trade log + daily report |
| `auth.py` | Kotak Neo login (TOTP + MPIN) |
| `session_manager.py` | Session keepalive (pings every 25 min) |
| `telegram_notifier.py` | Optional Telegram alerts |

## Outputs (generated daily)

| File | Contents |
|---|---|
| `reports/gap_list_YYYYMMDD.csv` | All stocks that gapped, with signal direction |
| `reports/trade_log.csv` | Every trade — entry, exit, P&L, cost |
| `reports/report_YYYYMMDD.txt` | Daily summary — win rate, P&L, breakdown |
| `reports/daily_history.json` | Historical day-by-day summary |
| `logs/gap_algo_YYYYMMDD.log` | Full debug log |

## Switching to Live

1. Review paper trade logs for a few days
2. In `config.py` set `PAPER_TRADE = False`
3. Run `python main.py`

## Key Config Parameters (config.py)

```python
PAPER_TRADE        = True    # False for live
MIN_GAP_PCT        = 4.0     # Min gap % to qualify
CAPITAL_PER_TRADE  = 25_000  # Rs per trade
MAX_SIMULTANEOUS   = 8       # Max open trades
SL_PCT             = 0.5     # Stop loss %
TARGET_PCT         = 2.5     # Hard target %
TRAIL_TRIGGER_PCT  = 1.0     # Trail activates after this % profit
TRAIL_BUFFER_PCT   = 0.5     # Trail SL = peak - this %
ENTRY_START        = "09:30" # No entries before this
SQUARE_OFF_TIME    = "15:15" # EOD square off
```
