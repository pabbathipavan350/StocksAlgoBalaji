# ============================================================
# GAP_SCANNER.PY — NSE Gap Up / Gap Down Stock Finder
# ============================================================
# Workflow:
#   1. Load Nifty 500 symbols from NSE scrip master CSV
#   2. Batch-fetch previous close for all symbols
#   3. At 9:15 AM, batch-fetch current LTP
#   4. Calculate gap % = (LTP - prev_close) / prev_close * 100
#   5. Return gap_up list (gap >= +4%) and gap_down list (gap <= -4%)
#   6. Re-scan continuously; add new qualifying stocks to watchlist
# ============================================================

import logging
import time
import io
import csv
import datetime
from typing import Dict, List, Tuple, Optional

import config

logger = logging.getLogger(__name__)


def now_ist() -> datetime.datetime:
    return datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)


# ──────────────────────────────────────────────────────────
#  Scrip Master Loader
# ──────────────────────────────────────────────────────────

class ScripMaster:
    """
    Loads the NSE CM scrip master CSV from Kotak Neo.
    Extracts instrument_token and symbol for EQ series stocks.
    Optionally filters by index (Nifty 500 / 200).
    """

    def __init__(self, client):
        self.client   = client
        self._df: Dict[str, dict] = {}   # symbol → {token, symbol, series, name}

    def load(self, mode: str = "nifty500") -> Dict[str, dict]:
        """
        Returns dict: symbol → {token, symbol, series, name, prev_close}
        mode: 'nifty500', 'nifty200', 'custom'
        """
        print(f"\n[ScripMaster] Loading NSE CM scrip master...")
        rows = self._fetch_rows()
        if not rows:
            raise RuntimeError("ScripMaster: could not load any rows — check API response")

        # Debug: show first row keys so we can see column names
        print(f"[ScripMaster] Sample columns: {list(rows[0].keys())[:10]}")
        print(f"[ScripMaster] Total rows fetched: {len(rows)}")

        # Build symbol map — EQ series only
        all_eq: Dict[str, dict] = {}
        for row in rows:
            # ── Series / Group ────────────────────────────
            # Kotak transformed CSV uses: pGroup = "EQ"
            # Older/raw CSVs use: Series = "EQ"
            series = (row.get("pGroup")  or row.get("Series") or
                      row.get("series")  or row.get("SERIES") or
                      row.get("group")   or "").strip().upper()
            if series != "EQ":
                continue

            # ── Symbol (trading symbol) ───────────────────
            # pSymbolName = base name e.g. "RELIANCE"
            # pTrdSymbol  = trading symbol e.g. "RELIANCE-EQ"
            symbol = (row.get("pSymbolName")   or row.get("pTrdSymbol")      or
                      row.get("Symbol")         or row.get("symbol")          or
                      row.get("TradingSymbol")  or row.get("trading_symbol")  or
                      row.get("SYMBOL")         or "").strip().upper()
            # Strip exchange suffix like "-EQ" if present
            if symbol.endswith("-EQ"):
                symbol = symbol[:-3]

            # ── Token ─────────────────────────────────────
            # pSymbol = numeric instrument token in Kotak's format
            token = str(row.get("pSymbol")         or row.get("Token")          or
                        row.get("token")            or row.get("InstrumentToken") or
                        row.get("instrument_token") or "").strip()

            # ── Name ──────────────────────────────────────
            name = (row.get("pDesc")             or row.get("InstrumentName")  or
                    row.get("instrument_name")   or row.get("CompanyName")     or
                    row.get("company_name")      or row.get("Name")            or
                    symbol).strip()

            # Capture prev close from scrip master if available (field: dPrvCls)
            prev_close_val = 0.0
            for pc_field in ("dPrvCls", "prevClose", "prev_close", "PrevClose", "dClose"):
                val = row.get(pc_field)
                if val:
                    try:
                        prev_close_val = float(val)
                        if prev_close_val > 0:
                            break
                    except (ValueError, TypeError):
                        pass

            if not symbol or not token:
                continue

            all_eq[symbol] = {
                "token"      : token,
                "symbol"     : symbol,
                "series"     : "EQ",
                "name"       : name,
                "prev_close" : prev_close_val,   # may be > 0 from CSV
                "dPrvCls"    : prev_close_val,
            }

        print(f"[ScripMaster] Total EQ symbols loaded: {len(all_eq)}")

        # Filter by watchlist mode
        if mode == "custom":
            custom_upper = [s.upper() for s in config.CUSTOM_SYMBOLS]
            self._df = {s: v for s, v in all_eq.items() if s in custom_upper}
        elif mode == "file":
            self._df = self._load_from_file(all_eq, config.WATCHLIST_FILE)
        elif mode == "nifty200":
            self._df = self._filter_by_nifty_index(all_eq, 200)
        else:  # nifty500 (default)
            self._df = self._filter_by_nifty_index(all_eq, 500)

        print(f"[ScripMaster] Watchlist symbols ({mode}): {len(self._df)}")
        return self._df

    def _load_from_file(self, all_eq: Dict, filepath: str) -> Dict:
        """Load watchlist from a CSV/text file with SYMBOL column or one per line."""
        import os
        if not os.path.exists(filepath):
            print(f"[ScripMaster] ⚠️  Watchlist file '{filepath}' not found — "
                  f"falling back to nifty500 token filter")
            return self._filter_by_nifty_index(all_eq, 500)

        symbols_in_file = set()
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Handle CSV with header "SYMBOL"
                sym = line.split(",")[0].strip().upper()
                if sym and sym != "SYMBOL":
                    symbols_in_file.add(sym)

        filtered = {s: v for s, v in all_eq.items() if s in symbols_in_file}
        not_found = symbols_in_file - set(all_eq.keys())
        if not_found:
            print(f"[ScripMaster] {len(not_found)} symbols in watchlist.csv "
                  f"not found in scrip master (may be delisted/renamed): "
                  f"{', '.join(sorted(not_found)[:10])}{'...' if len(not_found)>10 else ''}")
        print(f"[ScripMaster] Loaded {len(filtered)} symbols from {filepath}")
        return filtered

    def _fetch_rows(self) -> list:
        """
        Kotak Neo scrip_master() returns a PLAIN URL STRING like:
          'https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/2026-04-10/transformed-v1/nse_cm-v1.csv'

        We detect this, download the CSV, and return rows as list of dicts.
        Also handles: list of dicts, bytes, plain CSV string, dict with URL key.
        """
        import urllib.request
        import zipfile

        try:
            raw = self.client.scrip_master(exchange_segment=config.CM_SEGMENT)
        except Exception as e:
            logger.error(f"scrip_master() call failed: {e}")
            raise

        print(f"[ScripMaster] Response type: {type(raw).__name__}")
        raw_preview = str(raw)[:200].replace("\n", " ")
        print(f"[ScripMaster] Response preview: {raw_preview}")

        # ── CASE A: plain string that IS a URL (most common in prod) ──
        if isinstance(raw, str):
            stripped = raw.strip()
            if stripped.startswith("http"):
                # It's a URL — download the CSV from it
                return self._download_csv_from_url(stripped)
            elif stripped:
                # It's an inline CSV string
                rows = list(csv.DictReader(io.StringIO(stripped)))
                if rows:
                    return rows

        # ── CASE B: already a list of dicts ───────────────────────────
        if isinstance(raw, list):
            if raw and isinstance(raw[0], dict):
                return raw
            joined = "\n".join(str(r) for r in raw)
            return list(csv.DictReader(io.StringIO(joined)))

        # ── CASE C: dict response (URL or data inside) ────────────────
        if isinstance(raw, dict):
            print(f"[ScripMaster] Dict keys: {list(raw.keys())[:10]}")
            # Look for a URL in any common key
            for key in ("filePath", "file_path", "fileUrl", "file_url", "url",
                        "data", "message", "result"):
                val = raw.get(key, "")
                if isinstance(val, str) and val.strip().startswith("http"):
                    return self._download_csv_from_url(val.strip())
                if isinstance(val, list) and val:
                    return val
                if isinstance(val, str) and val.strip():
                    rows = list(csv.DictReader(io.StringIO(val)))
                    if rows:
                        return rows

        # ── CASE D: bytes ─────────────────────────────────────────────
        if isinstance(raw, bytes):
            decoded = raw.decode("utf-8", errors="replace").strip()
            if decoded.startswith("http"):
                return self._download_csv_from_url(decoded)
            return list(csv.DictReader(io.StringIO(decoded)))

        # ── FALLBACK: build dated URL directly (Kotak's known pattern) ─
        print("[ScripMaster] Building dated CDN URL as fallback...")
        import datetime
        today = datetime.date.today().strftime("%Y-%m-%d")
        fallback_urls = [
            f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{today}/transformed-v1/nse_cm-v1.csv",
            f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{today}/transformed/nse_cm.csv",
            f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/nse_cm/ScripMaster.csv",
        ]
        for url in fallback_urls:
            rows = self._download_csv_from_url(url)
            if rows:
                return rows

        print("[ScripMaster] ❌ All attempts failed. Cannot load scrip master.")
        return []

    def _download_csv_from_url(self, url: str) -> list:
        """Download a CSV (or zip containing CSV) from a URL and return list of dicts."""
        import urllib.request
        import zipfile
        try:
            print(f"[ScripMaster] Downloading: {url[:90]}...")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=60) as resp:
                content = resp.read()
            # Handle zip
            if content[:2] == b"PK":
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    csv_name = next(
                        (n for n in zf.namelist() if n.endswith(".csv")), None)
                    content = zf.read(csv_name) if csv_name else content
            text = content.decode("utf-8", errors="replace")
            rows = list(csv.DictReader(io.StringIO(text)))
            if rows:
                print(f"[ScripMaster] Downloaded {len(rows):,} rows ✓")
                return rows
            print(f"[ScripMaster] URL returned 0 rows: {url[:60]}")
        except Exception as e:
            logger.warning(f"[ScripMaster] Download failed ({url[:60]}): {e}")
        return []

    def _filter_by_nifty_index(self, all_eq: Dict, count: int) -> Dict:
        """
        Attempts to match against hardcoded Nifty index symbol names.
        Falls back gracefully — uses all_eq if index file unavailable.
        The Kotak scrip master doesn't ship an index filter, so we
        use a practical proxy: symbols with highest market activity
        (all tokens from NSE EQ series, capped at the first `count`
        alphabetically after sorting by token number which roughly
        correlates to listing seniority / market cap for big indices).

        In production, you can replace this with a local CSV of
        Nifty 500 constituents downloaded from nseindia.com.
        """
        nifty_file = "nifty500_symbols.csv"
        import os
        if os.path.exists(nifty_file):
            with open(nifty_file, "r") as f:
                symbols_in_index = {row.strip().upper() for row in f if row.strip()}
            filtered = {s: v for s, v in all_eq.items() if s in symbols_in_index}
            if filtered:
                print(f"[ScripMaster] Filtered to {len(filtered)} symbols from {nifty_file}")
                return filtered

        # Fallback: sort by token (numeric) — lower tokens = older/larger listings
        # This gives a reasonable Nifty 500-like universe
        sorted_by_token = sorted(all_eq.items(), key=lambda x: int(x[1]["token"]) if x[1]["token"].isdigit() else 999999)
        top_n = dict(sorted_by_token[:count])
        print(f"[ScripMaster] Fallback: using top {len(top_n)} symbols by token order")
        print(f"[ScripMaster] TIP: place 'nifty500_symbols.csv' (one symbol per line) for accurate filtering")
        return top_n

    @property
    def symbols(self) -> Dict[str, dict]:
        return self._df


# ──────────────────────────────────────────────────────────
#  Previous Close Fetcher
# ──────────────────────────────────────────────────────────

class PrevCloseFetcher:
    """
    Fetches previous day close for all watchlist symbols.
    Uses batch quotes with 'ohlc' type to get prev close (close field).
    """

    def __init__(self, client):
        self.client = client

    def fetch(self, scrips: Dict[str, dict]) -> Dict[str, float]:
        """Returns symbol → prev_close dict.
        
        Actual Kotak quotes response format (from debug):
        [{'exchange_token': '4', 'display_symbol': '21STCENMGM-EQ', 'exchange': 'nse_cm',
          'ohlc': {'open': '30.3400', 'high': '30.3400', 'low': '29.7500', 'close': '29.7500'}},
         ...]
        Token key   : 'exchange_token'
        Close field : item['ohlc']['close']
        """
        symbols  = list(scrips.keys())
        prev_cls = {}

        # Build reverse maps: exchange_token → symbol AND display_symbol → symbol
        tok_to_sym: Dict[str, str] = {scrips[s]["token"]: s for s in symbols}
        # Also map display_symbol like "RELIANCE-EQ" → "RELIANCE"
        disp_to_sym: Dict[str, str] = {}
        for s in symbols:
            disp_to_sym[f"{s}-EQ"] = s
            disp_to_sym[s]         = s

        total   = len(symbols)
        batch_n = config.SCAN_BATCH_SIZE
        batches = [symbols[i:i+batch_n] for i in range(0, total, batch_n)]

        print(f"[PrevClose] Fetching prev close for {total} symbols in {len(batches)} batches...")
        failed        = 0
        debug_printed = False

        for idx, batch_syms in enumerate(batches):
            batch_tokens = [{"instrument_token": scrips[s]["token"],
                             "exchange_segment": config.CM_SEGMENT}
                            for s in batch_syms]
            try:
                resp = self.client.quotes(
                    instrument_tokens=batch_tokens,
                    quote_type="ohlc"
                )

                if not debug_printed:
                    debug_printed = True
                    print(f"[PrevClose] Response sample: {str(resp)[:250]}")

                # Unwrap response
                if isinstance(resp, list):
                    data = resp
                elif isinstance(resp, dict):
                    data = (resp.get("data") or resp.get("200") or
                            resp.get("message") or resp.get("result") or [])
                else:
                    data = []
                if not isinstance(data, list):
                    data = []

                for item in data:
                    if not isinstance(item, dict):
                        continue

                    # ── Match token → symbol ──────────────
                    # Kotak response uses 'exchange_token' (not 'tk')
                    tok = str(item.get("exchange_token") or
                              item.get("tk")             or
                              item.get("token")          or "").strip()
                    sym = tok_to_sym.get(tok)

                    # Fallback: match via display_symbol e.g. "RELIANCE-EQ"
                    if not sym:
                        disp = str(item.get("display_symbol") or
                                   item.get("symbol") or "").strip().upper()
                        sym = disp_to_sym.get(disp)

                    if not sym:
                        continue

                    # ── Extract close from nested ohlc dict ──
                    pc = 0.0
                    ohlc = item.get("ohlc")
                    if isinstance(ohlc, dict):
                        for f in ("close", "c", "prev_close"):
                            val = ohlc.get(f)
                            if val is not None and val != "":
                                try:
                                    pc = float(val)
                                    if pc > 0:
                                        break
                                except (ValueError, TypeError):
                                    pass

                    # Flat field fallback (older API versions)
                    if pc <= 0:
                        for f in ("close", "c", "prev_close", "ltp", "ltP"):
                            val = item.get(f)
                            if val is not None and val != "":
                                try:
                                    pc = float(val)
                                    if pc > 0:
                                        break
                                except (ValueError, TypeError):
                                    pass

                    if pc > 0:
                        prev_cls[sym] = pc

            except Exception as e:
                logger.warning(f"PrevClose batch {idx+1} failed: {e}")
                failed += 1

            if idx < len(batches) - 1:
                time.sleep(0.25)

        print(f"[PrevClose] Got prev close for {len(prev_cls)}/{total} symbols "
              f"({failed} batch failures)")

        # Fallback: read dPrvCls from scrip master CSV if quotes API failed
        if len(prev_cls) == 0:
            print("[PrevClose] ⚠️  Quotes returned 0 — using scrip master dPrvCls fallback...")
            for sym, info in scrips.items():
                pc = float(info.get("dPrvCls") or info.get("prev_close") or 0)
                if pc > 0:
                    prev_cls[sym] = pc
            print(f"[PrevClose] Scrip master fallback: {len(prev_cls)} symbols")

        return prev_cls


# ──────────────────────────────────────────────────────────
#  Gap Scanner
# ──────────────────────────────────────────────────────────

class GapScanner:
    """
    Scans all watchlist symbols for gap up / gap down at market open.
    Returns two lists:
        gap_up   → [(symbol, token, prev_close, ltp, gap_pct), ...]
        gap_down → [(symbol, token, prev_close, ltp, gap_pct), ...]
    """

    def __init__(self, client, scrips: Dict[str, dict]):
        self.client  = client
        self.scrips  = scrips   # symbol → {token, symbol, ...}
        self._prev_close: Dict[str, float] = {}

    def set_prev_close(self, prev_close: Dict[str, float]):
        self._prev_close = prev_close
        # Inject into scrips dict for convenience
        for sym, pc in prev_close.items():
            if sym in self.scrips:
                self.scrips[sym]["prev_close"] = pc

    @property
    def scrip_count(self) -> int:
        return len(self.scrips)

    def scan(self) -> Tuple[List[dict], List[dict]]:
        """
        Fetches current LTP for all watchlist symbols and identifies gaps.
        Returns (gap_up_list, gap_down_list).
        Each item: {symbol, token, prev_close, ltp, gap_pct, direction}
        """
        symbols  = [s for s in self.scrips if self._prev_close.get(s, 0) > 0]
        tokens   = [{"instrument_token": self.scrips[s]["token"],
                     "exchange_segment": config.CM_SEGMENT} for s in symbols]

        batch_n  = config.SCAN_BATCH_SIZE
        batches  = [tokens[i:i+batch_n] for i in range(0, len(tokens), batch_n)]
        ltp_map: Dict[str, float] = {}

        # Build reverse maps
        tok_to_sym:  Dict[str, str] = {self.scrips[s]["token"]: s for s in symbols}
        disp_to_sym: Dict[str, str] = {}
        for s in symbols:
            disp_to_sym[f"{s}-EQ"] = s
            disp_to_sym[s]         = s

        for idx, batch in enumerate(batches):
            batch_syms = symbols[idx * batch_n: (idx+1) * batch_n]
            try:
                resp = self.client.quotes(
                    instrument_tokens=batch,
                    quote_type="ltp"
                )

                if isinstance(resp, list):
                    data = resp
                elif isinstance(resp, dict):
                    data = (resp.get("data") or resp.get("200") or
                            resp.get("message") or resp.get("result") or [])
                else:
                    data = []
                if not isinstance(data, list):
                    data = []

                for item in data:
                    if not isinstance(item, dict):
                        continue

                    # Match via exchange_token first, then display_symbol
                    tok = str(item.get("exchange_token") or
                              item.get("tk") or item.get("token") or "").strip()
                    sym = tok_to_sym.get(tok)
                    if not sym:
                        disp = str(item.get("display_symbol") or
                                   item.get("symbol") or "").strip().upper()
                        sym = disp_to_sym.get(disp)
                    if not sym:
                        continue

                    # LTP may be flat or nested under 'ltp' dict or direct field
                    ltp = 0.0
                    ltp_data = item.get("ltp")
                    if isinstance(ltp_data, dict):
                        # nested: {'ltp': {'ltp': '123.45'}}
                        for f in ("ltp", "ltP", "lp", "last_price"):
                            val = ltp_data.get(f)
                            if val is not None and val != "":
                                try:
                                    ltp = float(val)
                                    if ltp > 0:
                                        break
                                except (ValueError, TypeError):
                                    pass
                    if ltp <= 0:
                        for f in ("ltp", "ltP", "lp", "last_price", "close", "c"):
                            val = item.get(f)
                            if val is not None and val != "" and not isinstance(val, dict):
                                try:
                                    ltp = float(val)
                                    if ltp > 0:
                                        break
                                except (ValueError, TypeError):
                                    pass
                    if ltp > 0:
                        ltp_map[sym] = ltp

            except Exception as e:
                logger.warning(f"Gap scan batch {idx+1} error: {e}")

            if idx < len(batches) - 1:
                time.sleep(0.25)

        gap_up, gap_down = [], []

        for sym, ltp in ltp_map.items():
            pc = self._prev_close.get(sym, 0)
            if pc <= 0:
                continue

            # B-01 FIX: price filter — skip penny stocks below MIN_PRICE (₹50)
            # ALLCARGO (₹9.38) passed previously because this check was missing here.
            # config.MIN_PRICE is checked in the watchlist load but NOT in the
            # live LTP scan, so stocks whose price dropped below ₹50 after listing
            # could still appear. This adds the filter at the correct point.
            if ltp < config.MIN_PRICE:
                continue

            # Also skip stocks where prev_close itself was below MIN_PRICE
            # (prevents ratio distortion from very cheap stocks)
            if pc < config.MIN_PRICE:
                continue

            gap_pct = (ltp - pc) / pc * 100

            if gap_pct >= config.MIN_GAP_PCT and gap_pct <= config.MAX_GAP_PCT:
                entry = {
                    "symbol"     : sym,
                    "token"      : self.scrips[sym]["token"],
                    "name"       : self.scrips[sym].get("name", sym),
                    "prev_close" : pc,
                    "ltp"        : ltp,
                    "gap_pct"    : round(gap_pct, 2),
                    "direction"  : "GAP_UP",
                }
                gap_up.append(entry)

            elif gap_pct <= -config.MIN_GAP_PCT and gap_pct >= -config.MAX_GAP_PCT:
                entry = {
                    "symbol"     : sym,
                    "token"      : self.scrips[sym]["token"],
                    "name"       : self.scrips[sym].get("name", sym),
                    "prev_close" : pc,
                    "ltp"        : ltp,
                    "gap_pct"    : round(gap_pct, 2),
                    "direction"  : "GAP_DOWN",
                }
                gap_down.append(entry)

        # Sort by absolute gap size (biggest first)
        gap_up.sort(key=lambda x: x["gap_pct"], reverse=True)
        gap_down.sort(key=lambda x: x["gap_pct"])

        return gap_up, gap_down

    def save_gap_list(self, gap_up: List[dict], gap_down: List[dict]):
        """Save today's gap list to CSV for review."""
        import os
        os.makedirs("reports", exist_ok=True)
        date_str = now_ist().strftime("%Y%m%d")
        fname    = f"reports/gap_list_{date_str}.csv"
        with open(fname, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Direction", "Symbol", "Name", "Prev Close",
                             "Open LTP", "Gap %", "Trade Signal"])
            for s in gap_up:
                writer.writerow(["GAP UP", s["symbol"], s["name"],
                                 f"{s['prev_close']:.2f}", f"{s['ltp']:.2f}",
                                 f"+{s['gap_pct']:.2f}%", "SHORT when below VWAP"])
            for s in gap_down:
                writer.writerow(["GAP DOWN", s["symbol"], s["name"],
                                 f"{s['prev_close']:.2f}", f"{s['ltp']:.2f}",
                                 f"{s['gap_pct']:.2f}%", "LONG when above VWAP"])
        print(f"[GapScanner] Gap list saved: {fname}")
        return fname
