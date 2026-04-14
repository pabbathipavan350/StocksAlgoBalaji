# ============================================================
# REPORT_MANAGER.PY — Trade Log + Daily Report
# ============================================================
# Logs every completed trade to CSV.
# Generates end-of-day summary report as TXT + JSON.
# ============================================================

import csv
import os
import json
import datetime
import config


def now_ist() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


class ReportManager:

    def __init__(self):
        self.today  = now_ist().date()
        self.trades = []
        os.makedirs("reports", exist_ok=True)
        self._init_csv()
        self._init_daily_history()

    # ── CSV Trade Log ─────────────────────────────────────

    def _init_csv(self):
        exists       = os.path.exists(config.TRADE_LOG_FILE)
        self._log    = open(config.TRADE_LOG_FILE, "a", newline="", encoding="utf-8")
        self._writer = csv.writer(self._log)
        if not exists:
            self._writer.writerow([
                "Date", "Trade#", "Mode",
                "Entry Time", "Exit Time", "Duration (mins)",
                "Symbol", "Name", "Signal Type", "Direction",
                "Gap Direction", "Gap %",
                "Qty", "Exposure (Rs)",
                "Entry Price", "Exit Price", "Peak Price",
                "VWAP at Entry",
                "Move %", "Move from VWAP %",
                "Gross PnL", "Cost", "Net PnL",
                "SL Price", "Target Price",
                "Exit Reason", "Trail Active",
                "Day PnL Running",
            ])

    def log_trade(self, trade):
        """Log one completed trade to CSV."""
        self.trades.append(trade)
        from trade_manager import calc_trade_cost
        gross   = trade.realised_pnl
        cost    = calc_trade_cost(trade.entry_price, trade.exit_price,
                                  trade.qty, trade.direction)
        net     = trade.net_pnl
        move_pct = round((trade.exit_price - trade.entry_price) /
                          trade.entry_price * 100, 3)
        vwap_move = round((trade.entry_price - trade.entry_vwap) /
                           trade.entry_vwap * 100, 3) if trade.entry_vwap else 0

        # Running day P&L is set by TradeManager; we just log what we have
        running_pnl = sum(t.net_pnl for t in self.trades)

        mode = "PAPER" if config.PAPER_TRADE else "LIVE"

        self._writer.writerow([
            str(self.today),
            trade.trade_id,
            mode,
            trade.entry_time.strftime("%H:%M:%S"),
            trade.exit_time.strftime("%H:%M:%S") if trade.exit_time else "",
            trade.duration_mins,
            trade.symbol,
            "",              # name — not stored in Trade; add if needed
            getattr(trade, 'signal_type', 'GAP_REVERSAL'),
            trade.direction,
            trade.gap_direction,
            f"{trade.gap_pct:+.2f}%",
            trade.qty,
            f"{trade.exposure:,.0f}",
            f"{trade.entry_price:.2f}",
            f"{trade.exit_price:.2f}",
            f"{trade.peak_price:.2f}",
            f"{trade.entry_vwap:.2f}",
            f"{move_pct:+.3f}%",
            f"{vwap_move:+.3f}%",
            f"{gross:+.2f}",
            f"{cost:.2f}",
            f"{net:+.2f}",
            f"{trade.sl_price:.2f}",
            f"{trade.target_price:.2f}",
            trade.exit_reason,
            "Y" if trade.trail_active else "N",
            f"{running_pnl:+.2f}",
        ])
        self._log.flush()

    # ── Daily History JSON ────────────────────────────────

    def _init_daily_history(self):
        self._history_file = "reports/daily_history.json"
        self._history      = []
        if os.path.exists(self._history_file):
            try:
                with open(self._history_file, "r") as f:
                    self._history = json.load(f)
            except Exception:
                self._history = []

    # ── End of Day Report ─────────────────────────────────

    def generate_daily_report(self) -> str:
        today_str = self.today.strftime("%Y%m%d")
        fname     = f"reports/report_{today_str}.txt"

        total    = len(self.trades)
        if total == 0:
            report = f"\n{'='*55}\n  GAP ALGO DAILY REPORT — {self.today}\n  No trades today.\n{'='*55}\n"
            with open(fname, "w") as f:
                f.write(report)
            return report

        winners  = [t for t in self.trades if t.net_pnl > 0]
        losers   = [t for t in self.trades if t.net_pnl <= 0]
        win_rate = len(winners) / total * 100

        gross_pnl = sum(t.realised_pnl for t in self.trades)
        net_pnl   = sum(t.net_pnl      for t in self.trades)

        avg_win  = sum(t.net_pnl for t in winners) / len(winners) if winners else 0
        avg_loss = sum(t.net_pnl for t in losers)  / len(losers)  if losers  else 0
        rr_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

        best  = max(self.trades, key=lambda t: t.net_pnl)
        worst = min(self.trades, key=lambda t: t.net_pnl)

        # Gap direction breakdown
        gap_up_trades   = [t for t in self.trades if t.gap_direction == "GAP_UP"]
        gap_down_trades = [t for t in self.trades if t.gap_direction == "GAP_DOWN"]

        # Exit reason breakdown
        exits = {}
        for t in self.trades:
            key = t.exit_reason.split()[0] if t.exit_reason else "Other"
            exits[key] = exits.get(key, 0) + 1

        mode = "*** PAPER ***" if config.PAPER_TRADE else "LIVE"

        report  = f"\n{'='*60}\n"
        report += f"  GAP UP/DOWN VWAP ALGO — DAILY REPORT\n"
        report += f"  Date  : {self.today}    Mode: {mode}\n"
        report += f"{'='*60}\n\n"

        report += f"  OVERVIEW\n"
        report += f"  {'─'*50}\n"
        report += f"  Total Trades      : {total}\n"
        report += f"  Winners / Losers  : {len(winners)} / {len(losers)}\n"
        report += f"  Win Rate          : {win_rate:.1f}%\n"
        report += f"  Avg Win           : ₹{avg_win:+.0f}\n"
        report += f"  Avg Loss          : ₹{avg_loss:+.0f}\n"
        report += f"  Reward:Risk       : {rr_ratio:.2f}\n\n"

        report += f"  P&L SUMMARY\n"
        report += f"  {'─'*50}\n"
        report += f"  Gross P&L         : ₹{gross_pnl:+,.0f}\n"
        report += f"  Net P&L           : ₹{net_pnl:+,.0f}\n"
        report += f"  Capital Deployed  : ₹{config.TOTAL_CAPITAL:,.0f}\n"
        report += f"  Return on Capital : {net_pnl/config.TOTAL_CAPITAL*100:+.2f}%\n\n"

        report += f"  GAP DIRECTION BREAKDOWN\n"
        report += f"  {'─'*50}\n"
        report += f"  Gap Up  trades (SHORT): {len(gap_up_trades)}  "
        if gap_up_trades:
            report += f"Net ₹{sum(t.net_pnl for t in gap_up_trades):+,.0f}\n"
        else:
            report += "\n"
        report += f"  Gap Down trades (LONG) : {len(gap_down_trades)}  "
        if gap_down_trades:
            report += f"Net ₹{sum(t.net_pnl for t in gap_down_trades):+,.0f}\n"
        else:
            report += "\n"

        report += f"\n  EXIT REASON BREAKDOWN\n"
        report += f"  {'─'*50}\n"
        for reason, count in sorted(exits.items()):
            report += f"  {reason:<20}: {count}\n"

        report += f"\n  BEST TRADE   : {best.symbol} {best.direction} ₹{best.net_pnl:+.0f}\n"
        report += f"  WORST TRADE  : {worst.symbol} {worst.direction} ₹{worst.net_pnl:+.0f}\n"

        report += f"\n  TRADE DETAIL\n"
        report += f"  {'─'*50}\n"
        header = (f"  {'#':>3} {'Sym':<12} {'Dir':<6} {'Gap%':>6} "
                  f"{'Entry':>8} {'Exit':>8} {'Move%':>7} {'Net PnL':>9} {'Reason':<15}\n")
        report += header
        report += f"  {'─'*95}\n"
        for t in self.trades:
            move = round((t.exit_price - t.entry_price) / t.entry_price * 100, 2)
            report += (f"  {t.trade_id:>3} {t.symbol:<12} {t.direction:<6} "
                       f"{t.gap_pct:>+6.2f}% "
                       f"₹{t.entry_price:>8.2f} ₹{t.exit_price:>8.2f} "
                       f"{move:>+6.2f}% "
                       f"₹{t.net_pnl:>+9.0f} {t.exit_reason:<15}\n")

        report += f"\n{'='*60}\n"

        with open(fname, "w", encoding="utf-8") as f:
            f.write(report)

        # Save to daily history JSON
        self._history.append({
            "date"      : str(self.today),
            "mode"      : "PAPER" if config.PAPER_TRADE else "LIVE",
            "trades"    : total,
            "winners"   : len(winners),
            "win_rate"  : round(win_rate, 1),
            "net_pnl"   : round(net_pnl, 2),
            "gross_pnl" : round(gross_pnl, 2),
        })
        with open(self._history_file, "w") as f:
            json.dump(self._history, f, indent=2)

        print(f"\n[Report] Daily report saved: {fname}")
        return report

    def close(self):
        try:
            self._log.close()
        except Exception:
            pass
