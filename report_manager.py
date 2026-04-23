# ============================================================
# REPORT_MANAGER.PY — Trade Log + Daily Report
# ============================================================
#
# CHANGES:
#   - Separate CSV per signal type:
#       reports/trade_log_TREND.csv       (VWAP_TREND_LONG / SHORT)
#       reports/trade_log_GAP_REVERSAL.csv
#       reports/trade_log_BREAKOUT.csv
#   - Each CSV has "Concurrent_At_Entry" column showing how
#     many other trades were already open when this one started
#   - Daily report breaks down P&L per signal type
# ============================================================

import csv
import os
import json
import datetime
import config


def now_ist() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


def _log_file_for(signal_type: str) -> str:
    if signal_type in ("VWAP_TREND_LONG", "VWAP_TREND_SHORT"):
        return "reports/trade_log_TREND.csv"
    elif signal_type == "GAP_REVERSAL":
        return "reports/trade_log_GAP_REVERSAL.csv"
    elif signal_type == "EARLY_TREND":
        return "reports/trade_log_EARLY_TREND.csv"
    else:
        return "reports/trade_log_BREAKOUT.csv"


CSV_HEADERS = [
    "Date", "Trade#", "Mode",
    "Entry Time", "Exit Time", "Duration (mins)",
    "Symbol", "Signal Type", "Direction",
    "Gap Direction", "Gap %",
    "Qty", "Exposure (Rs)",
    "Entry Price", "Exit Price", "Peak Price",
    "VWAP at Entry",
    "Move %", "Move from VWAP %",
    "Gross PnL", "Cost", "Net PnL",
    "SL Price", "Target Price",
    "Exit Reason", "Trail Active",
    "Fill Quality",
    "Concurrent_At_Entry",
    "Day PnL Running",
]


class ReportManager:

    def __init__(self):
        self.today  = now_ist().date()
        self.trades = []
        os.makedirs("reports", exist_ok=True)
        self._writers = {}
        for path in (
            "reports/trade_log_TREND.csv",
            "reports/trade_log_GAP_REVERSAL.csv",
            "reports/trade_log_BREAKOUT.csv",
            "reports/trade_log_EARLY_TREND.csv",
        ):
            self._init_csv(path)
        self._init_daily_history()

    def _init_csv(self, path: str):
        exists = os.path.exists(path)
        fh     = open(path, "a", newline="", encoding="utf-8")
        writer = csv.writer(fh)
        if not exists:
            writer.writerow(CSV_HEADERS)
            fh.flush()
        self._writers[path] = (fh, writer)

    def _writer_for(self, signal_type: str):
        path = _log_file_for(signal_type)
        return self._writers[path][1]

    def log_trade(self, trade):
        self.trades.append(trade)
        from trade_manager import calc_trade_cost

        gross     = trade.realised_pnl
        cost      = calc_trade_cost(trade.entry_price, trade.exit_price,
                                    trade.qty, trade.direction)
        net       = trade.net_pnl
        move_pct  = round((trade.exit_price - trade.entry_price) /
                           trade.entry_price * 100, 3)
        vwap_move = round((trade.entry_price - trade.entry_vwap) /
                           trade.entry_vwap * 100, 3) if trade.entry_vwap else 0
        running_pnl = sum(t.net_pnl for t in self.trades)
        mode        = "PAPER" if config.PAPER_TRADE else "LIVE"
        signal_type = getattr(trade, "signal_type", "GAP_REVERSAL")
        concurrent  = getattr(trade, "concurrent_at_entry", 0)

        row = [
            str(self.today),
            trade.trade_id,
            mode,
            trade.entry_time.strftime("%H:%M:%S"),
            trade.exit_time.strftime("%H:%M:%S") if trade.exit_time else "",
            trade.duration_mins,
            trade.symbol,
            signal_type,
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
            getattr(trade, "fill_quality", "NORMAL"),
            concurrent,
            f"{running_pnl:+.2f}",
        ]

        writer = self._writer_for(signal_type)
        writer.writerow(row)
        path = _log_file_for(signal_type)
        self._writers[path][0].flush()

    def _init_daily_history(self):
        self._history_file = "reports/daily_history.json"
        self._history      = []
        if os.path.exists(self._history_file):
            try:
                with open(self._history_file, "r") as f:
                    self._history = json.load(f)
            except Exception:
                self._history = []

    def generate_daily_report(self) -> str:
        today_str = self.today.strftime("%Y%m%d")
        fname     = f"reports/report_{today_str}.txt"

        total = len(self.trades)
        if total == 0:
            report = (f"\n{'='*55}\n  GAP ALGO DAILY REPORT — {self.today}\n"
                      f"  No trades today.\n{'='*55}\n")
            with open(fname, "w") as f:
                f.write(report)
            return report

        winners  = [t for t in self.trades if t.net_pnl > 0]
        losers   = [t for t in self.trades if t.net_pnl <= 0]
        win_rate = len(winners) / total * 100
        gross_pnl = sum(t.realised_pnl for t in self.trades)
        net_pnl   = sum(t.net_pnl      for t in self.trades)
        avg_win   = sum(t.net_pnl for t in winners) / len(winners) if winners else 0
        avg_loss  = sum(t.net_pnl for t in losers)  / len(losers)  if losers  else 0
        rr_ratio  = abs(avg_win / avg_loss) if avg_loss != 0 else 0
        best      = max(self.trades, key=lambda t: t.net_pnl)
        worst     = min(self.trades, key=lambda t: t.net_pnl)
        peak_concurrent = max(
            (getattr(t, "concurrent_at_entry", 0) + 1) for t in self.trades
        )

        def type_stats(label, signal_types):
            ts = [t for t in self.trades
                  if getattr(t, "signal_type", "") in signal_types]
            if not ts:
                return f"  {label:<24}: 0 trades\n"
            w     = sum(1 for t in ts if t.net_pnl > 0)
            pnl   = sum(t.net_pnl for t in ts)
            wr    = w / len(ts) * 100
            max_c = max(getattr(t, "concurrent_at_entry", 0) for t in ts)
            return (f"  {label:<24}: {len(ts):>3} trades  "
                    f"WR {wr:>5.1f}%  Net Rs{pnl:>+8,.0f}  "
                    f"MaxConcurrent {max_c}\n")

        exits = {}
        for t in self.trades:
            key = t.exit_reason.split()[0] if t.exit_reason else "Other"
            exits[key] = exits.get(key, 0) + 1

        mode   = "*** PAPER ***" if config.PAPER_TRADE else "LIVE"
        report  = f"\n{'='*62}\n"
        report += f"  GAP UP/DOWN VWAP ALGO — DAILY REPORT\n"
        report += f"  Date  : {self.today}    Mode: {mode}\n"
        report += f"{'='*62}\n\n"
        report += f"  OVERVIEW\n  {'─'*56}\n"
        report += f"  Total Trades        : {total}\n"
        report += f"  Winners / Losers    : {len(winners)} / {len(losers)}\n"
        report += f"  Win Rate            : {win_rate:.1f}%\n"
        report += f"  Avg Win             : Rs{avg_win:+.0f}\n"
        report += f"  Avg Loss            : Rs{avg_loss:+.0f}\n"
        report += f"  Reward:Risk         : {rr_ratio:.2f}\n"
        report += f"  Peak Concurrent     : {peak_concurrent} trades open at once\n\n"
        report += f"  P&L SUMMARY\n  {'─'*56}\n"
        report += f"  Gross P&L           : Rs{gross_pnl:+,.0f}\n"
        report += f"  Net P&L             : Rs{net_pnl:+,.0f}\n"
        report += f"  Capital Deployed    : Rs{config.TOTAL_CAPITAL:,.0f}\n"
        report += f"  Return on Capital   : {net_pnl/config.TOTAL_CAPITAL*100:+.2f}%\n\n"
        report += f"  BY SIGNAL TYPE  (separate CSVs in reports/)\n  {'─'*56}\n"
        report += type_stats("VWAP_TREND",    ("VWAP_TREND_LONG", "VWAP_TREND_SHORT"))
        report += type_stats("GAP_REVERSAL",  ("GAP_REVERSAL",))
        report += type_stats("VWAP_BREAKOUT", ("VWAP_BREAKOUT",))
        report += type_stats("EARLY_TREND",   ("EARLY_TREND",))
        report += f"\n  EXIT REASON BREAKDOWN\n  {'─'*56}\n"
        for reason, count in sorted(exits.items()):
            report += f"  {reason:<22}: {count}\n"
        report += f"\n  BEST TRADE    : {best.symbol} {best.direction} Rs{best.net_pnl:+.0f}\n"
        report += f"  WORST TRADE   : {worst.symbol} {worst.direction} Rs{worst.net_pnl:+.0f}\n"

        for group_label, signal_types in (
            ("VWAP TREND",    ("VWAP_TREND_LONG", "VWAP_TREND_SHORT")),
            ("GAP REVERSAL",  ("GAP_REVERSAL",)),
            ("VWAP BREAKOUT", ("VWAP_BREAKOUT",)),
            ("EARLY TREND",   ("EARLY_TREND",)),
        ):
            group = [t for t in self.trades
                     if getattr(t, "signal_type", "") in signal_types]
            if not group:
                continue
            report += f"\n  TRADE DETAIL — {group_label}\n  {'─'*56}\n"
            report += (f"  {'#':>3} {'Sym':<12} {'Dir':<6} {'Gap%':>6} "
                       f"{'Entry':>8} {'Exit':>8} {'Move%':>7} "
                       f"{'Net PnL':>9} {'Con':>4} {'Reason':<14}\n")
            report += f"  {'─'*100}\n"
            for t in group:
                move = round((t.exit_price - t.entry_price) /
                              t.entry_price * 100, 2)
                con  = getattr(t, "concurrent_at_entry", 0)
                report += (f"  {t.trade_id:>3} {t.symbol:<12} {t.direction:<6} "
                           f"{t.gap_pct:>+6.2f}% "
                           f"Rs{t.entry_price:>8.2f} Rs{t.exit_price:>8.2f} "
                           f"{move:>+6.2f}% "
                           f"Rs{t.net_pnl:>+9.0f} {con:>4} {t.exit_reason:<14}\n")

        report += f"\n{'='*62}\n"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(report)

        self._history.append({
            "date"           : str(self.today),
            "mode"           : "PAPER" if config.PAPER_TRADE else "LIVE",
            "trades"         : total,
            "winners"        : len(winners),
            "win_rate"       : round(win_rate, 1),
            "net_pnl"        : round(net_pnl, 2),
            "gross_pnl"      : round(gross_pnl, 2),
            "peak_concurrent": peak_concurrent,
        })
        with open(self._history_file, "w") as f:
            json.dump(self._history, f, indent=2)

        print(f"\n[Report] Daily report saved: {fname}")
        print(f"[Report] Logs: trade_log_TREND.csv  trade_log_GAP_REVERSAL.csv  "
              f"trade_log_BREAKOUT.csv  trade_log_EARLY_TREND.csv")
        return report

    def close(self):
        for fh, _ in self._writers.values():
            try:
                fh.close()
            except Exception:
                pass
