# ============================================================
# TELEGRAM_NOTIFIER.PY — Trade Alerts (reused from algo_v3)
# ============================================================

import os
import threading
import logging

logger = logging.getLogger(__name__)

try:
    import urllib.request
    import urllib.parse
    _URLLIB_OK = True
except ImportError:
    _URLLIB_OK = False


class TelegramNotifier:

    def __init__(self):
        self.token   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        self.enabled = bool(self.token and self.chat_id and _URLLIB_OK)

        if self.enabled:
            print(f"  [Telegram] ✅ Alerts enabled (chat_id={self.chat_id})")
        else:
            print(f"  [Telegram] ⚠️  Alerts disabled "
                  f"(set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID in .env to enable)")

    def send(self, message: str):
        if not self.enabled:
            return
        threading.Thread(target=self._send_sync, args=(message,),
                         daemon=True, name="TelegramSend").start()

    def _send_sync(self, message: str):
        try:
            url  = f"https://api.telegram.org/bot{self.token}/sendMessage"
            data = urllib.parse.urlencode({
                "chat_id"   : self.chat_id,
                "text"      : message,
                "parse_mode": "HTML",
            }).encode("utf-8")
            req = urllib.request.Request(url, data=data)
            with urllib.request.urlopen(req, timeout=8) as resp:
                if resp.status != 200:
                    logger.debug(f"Telegram HTTP {resp.status}")
        except Exception as e:
            logger.debug(f"Telegram send failed: {e}")

    def alert_gap_list(self, gap_up: list, gap_down: list):
        lines = [f"📊 <b>GAP SCAN COMPLETE</b>"]
        lines.append(f"🟢 Gap Up  ({len(gap_up)} stocks):")
        for s in gap_up[:5]:
            lines.append(f"  {s['symbol']} +{s['gap_pct']:.1f}%")
        if len(gap_up) > 5:
            lines.append(f"  ...and {len(gap_up)-5} more")
        lines.append(f"🔴 Gap Down ({len(gap_down)} stocks):")
        for s in gap_down[:5]:
            lines.append(f"  {s['symbol']} {s['gap_pct']:.1f}%")
        if len(gap_down) > 5:
            lines.append(f"  ...and {len(gap_down)-5} more")
        self.send("\n".join(lines))

    def alert_entry(self, symbol: str, direction: str, gap_dir: str,
                    entry: float, vwap: float, sl: float, target: float,
                    qty: int, gap_pct: float):
        emoji = "📈" if direction == "LONG" else "📉"
        self.send(
            f"{emoji} <b>ENTRY — {direction} {symbol}</b>\n"
            f"Gap     : {gap_dir} {gap_pct:+.2f}%\n"
            f"Price   : ₹{entry:.2f}  VWAP: ₹{vwap:.2f}\n"
            f"SL      : ₹{sl:.2f}\n"
            f"Target  : ₹{target:.2f}\n"
            f"Qty     : {qty}"
        )

    def alert_exit(self, symbol: str, direction: str, entry: float,
                   exit_p: float, net_rs: float, reason: str):
        emoji = "✅" if net_rs >= 0 else "❌"
        self.send(
            f"{emoji} <b>EXIT — {symbol}</b>\n"
            f"Entry   : ₹{entry:.2f}  Exit: ₹{exit_p:.2f}\n"
            f"Net P&amp;L : ₹{net_rs:+.0f}\n"
            f"Reason  : {reason}"
        )

    def alert_risk(self, message: str):
        self.send(f"⚠️ <b>RISK ALERT</b>\n{message}")

    def alert_startup(self, gap_up_count: int, gap_down_count: int, mode: str):
        self.send(
            f"🚀 <b>GAP ALGO STARTED</b>\n"
            f"Mode    : {mode}\n"
            f"Gap Up  : {gap_up_count} stocks\n"
            f"Gap Down: {gap_down_count} stocks\n"
            f"Entry after: 09:30 IST"
        )

    def alert_shutdown(self, trades: int, net_pnl: float):
        emoji = "✅" if net_pnl >= 0 else "❌"
        self.send(
            f"{emoji} <b>GAP ALGO STOPPED</b>\n"
            f"Trades  : {trades}\n"
            f"Net P&amp;L : ₹{net_pnl:+.0f}"
        )
