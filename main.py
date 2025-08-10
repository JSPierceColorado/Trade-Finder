import os
import json
import time
import math
import requests
import gspread
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone

# =========================
# Config (env or defaults)
# =========================
SHEET_NAME   = os.getenv("SHEET_NAME", "Trading Log")
TICKERS_TAB  = os.getenv("TICKERS_TAB", "tickers")
SCREENER_TAB = os.getenv("SCREENER_TAB", "screener")

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY") or os.getenv("API_KEY")

# Universe filters (analysis stage)
MIN_DAILY_VOL = int(os.getenv("MIN_DAILY_VOL", "300000"))   # grouped daily volume filter
MIN_PRICE     = float(os.getenv("MIN_PRICE", "2.0"))
ALLOWED_EXCHANGES = set(os.getenv("ALLOWED_EXCHANGES", "XNYS,XNAS,ARCX,BATS,XASE").split(","))
INCLUDE_TYPES = set(os.getenv("INCLUDE_TYPES", "CS,ADRC,ADRP,ADRR").split(","))   # common + ADRs
EXCLUDE_TYPES = set(os.getenv("EXCLUDE_TYPES", "ETF,ETN,FUND,SP,PFD,WRT,RIGHT,UNIT,REIT").split(","))

# Safety knobs
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "800"))        # how many to analyze per run
DAYS_LOOKBACK = int(os.getenv("DAYS_LOOKBACK", "200"))    # bars per ticker for indicators
SLEEP_MS_BETWEEN_CALLS = int(os.getenv("SLEEP_MS_BETWEEN_CALLS", "50"))  # pause between Polygon calls

# =========================
# Google Sheets
# =========================
def get_google_client():
    creds = json.loads(os.getenv("GOOGLE_CREDS_JSON"))
    return gspread.service_account_from_dict(creds)

def write_tickers_sheet(gc, tickers):
    ws = gc.open(SHEET_NAME).worksheet(TICKERS_TAB)
    ws.clear()
    ws.append_row(["Ticker"])
    rows = [[t] for t in tickers]
    batch = 1000
    for i in range(0, len(rows), batch):
        ws.append_rows(rows[i:i+batch], value_input_option="USER_ENTERED")
    print(f"✅ Wrote {len(tickers)} tickers to '{TICKERS_TAB}'")

def write_screener_sheet(gc, rows):
    ws = gc.open(SHEET_NAME).worksheet(SCREENER_TAB)
    ws.clear()
    headers = ["Ticker","Price","EMA_20","RSI_14","MACD","Signal","Bullish Signal","Buy Reason","Timestamp","RankScore","TopPick"]
    ws.append_row(headers)
    batch = 100
    for i in range(0, len(rows), batch):
        ws.append_rows(rows[i:i+batch], value_input_option="USER_ENTERED")
    print(f"✅ Wrote {len(rows)} rows to '{SCREENER_TAB}'")

# =========================
# Polygon helpers
# =========================
BASE = "https://api.polygon.io"

def polite_sleep():
    if SLEEP_MS_BETWEEN_CALLS > 0:
        time.sleep(SLEEP_MS_BETWEEN_CALLS / 1000.0)

def fetch_all_polygon_meta():
    """
    Pull all active U.S. equities with metadata (type, primary_exchange).
    Returns list of dicts like {"ticker": "AAPL", "type": "CS", "primary_exchange": "XNAS"}
    """
    url = f"{BASE}/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": 1000,
        "apiKey": POLYGON_API_KEY,
    }
    out = []
    next_url = url
    next_params = params
    page = 0
    while next_url:
        resp = requests.get(next_url, params=next_params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        for r in results:
            out.append({
                "ticker": r.get("ticker"),
                "type": (r.get("type") or "").upper(),                   # CS, ETF, ADRC, etc.
                "primary_exchange": (r.get("primary_exchange") or "").upper()
            })
        page += 1
        print(f"   • Page {page}: {len(results)} tickers")
        next_url = data.get("next_url")
        next_params = {"apiKey": POLYGON_API_KEY} if next_url else None
        polite_sleep()
    # keep unique
    seen = set()
    meta = []
    for r in out:
        t = r["ticker"]
        if t and t not in seen:
            seen.add(t)
            meta.append(r)
    print(f"📦 Polygon active equities (meta): {len(meta)}")
    return meta

def last_trading_date_utc():
    # Try yesterday, then back up to 5 days to avoid weekends/holidays
    d = datetime.now(timezone.utc).date() - timedelta(days=1)
    for _ in range(5):
        yield d
        d -= timedelta(days=1)

def fetch_grouped_map():
    """
    One call to get ALL tickers' daily volume/close for a date.
    Returns dict: { 'AAPL': {'v': volume, 'c': close}, ... }
    """
    for d in last_trading_date_utc():
        url = f"{BASE}/v2/aggs/grouped/locale/us/market/stocks/{d.isoformat()}"
        params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}
        try:
            resp = requests.get(url, params=params, timeout=45)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            if results:
                m = {}
                for r in results:
                    t = r.get("T")
                    if not t:
                        continue
                    m[t] = {"v": r.get("v", 0), "c": r.get("c", 0.0)}
                print(f"🗂️ Grouped bars date: {d.isoformat()} (tickers: {len(m)})")
                return m
        except Exception:
            pass
        polite_sleep()
    print("⚠️ Could not fetch grouped aggregates; proceeding without prefilter (may be slow).")
    return {}

def fetch_daily_bars_df(ticker, days=DAYS_LOOKBACK):
    """
    One request per ticker: daily aggregates over last N calendar days (adjusted).
    """
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days*2)
    url = f"{BASE}/v2/aggs/ticker/{ticker}/range/1/day/{start.isoformat()}/{end.isoformat()}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLYGON_API_KEY}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            return None
        df = pd.DataFrame(results)
        df.rename(columns={"c":"close","o":"open","h":"high","l":"low","v":"volume","t":"timestamp"}, inplace=True)
        df = df.tail(days).reset_index(drop=True)
        return df
    except Exception:
        return None
    finally:
        polite_sleep()

# =========================
# Indicator calculations
# =========================
def ema(series, window):
    return series.ewm(span=window, adjust=False).mean()

def rsi(series, window=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/window, adjust=False).mean()
    roll_down = down.ewm(alpha=1/window, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    rsi_vals = 100 - (100 / (1 + rs))
    return rsi_vals

def macd(series, fast=12, slow=26, signal=9):
    ema_fast = ema(series, fast)
    ema_slow = ema(series, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    return macd_line, signal_line

# =========================
# Analysis
# =========================
def analyze_one(ticker):
    df = fetch_daily_bars_df(ticker, DAYS_LOOKBACK)
    if df is None or df.shape[0] < 50:
        return None

    # Final guard: latest bar must have volume
    if "volume" not in df.columns or float(df["volume"].iloc[-1]) <= 0:
        return None

    close = df["close"]
    price = float(close.iloc[-1])

    ema20 = float(ema(close, 20).iloc[-1])
    rsi14 = float(rsi(close, 14).iloc[-1])

    macd_line, signal_line = macd(close, 12, 26, 9)
    macd_v = float(macd_line.iloc[-1])
    signal_v = float(signal_line.iloc[-1])

    is_bullish = (
        (25 < rsi14 < 65) and
        (macd_v > signal_v) and
        ((price > ema20) or (rsi14 < 45))
    )

    rank_score = 0.0
    if price > ema20:
        rank_score += 10
    rank_score += max(0.0, min(rsi14 - 25.0, 40.0))
    if macd_v > signal_v:
        rank_score += 5.0

    buy_reason = "RSI 25-65, MACD crossover, Price>EMA20 or RSI<45" if is_bullish else "Not all slightly-looser criteria met"

    row = [
        ticker,
        round(price, 2),
        round(ema20, 2),
        round(rsi14, 2),
        round(macd_v, 4),
        round(signal_v, 4),
        "✅" if is_bullish else "",
        buy_reason,
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        round(rank_score, 3),
        ""
    ]

    if any(v in (None, "", 0, 0.0) for v in row[1:6]):
        return None
    return row

# =========================
# Orchestration
# =========================
def main():
    if not POLYGON_API_KEY:
        raise RuntimeError("Missing POLYGON_API_KEY env var.")

    print("🚀 Ticker collector + screener starting")

    gc = get_google_client()

    # 1) Universe: all active Polygon equities (with metadata)
    print("📥 Fetching all active equities (meta) from Polygon…")
    meta = fetch_all_polygon_meta()
    all_tickers = sorted({m["ticker"] for m in meta if m["ticker"]})
    write_tickers_sheet(gc, all_tickers)  # keep the full list in 'tickers'

    # 2) Prefilter with single grouped request (volume/price), plus type/exchange
    grouped = fetch_grouped_map()
    filtered = []
    for m in meta:
        t = m["ticker"]; typ = m["type"]; ex = m["primary_exchange"]
        if not t or not typ or not ex:
            continue
        if typ in EXCLUDE_TYPES:
            continue
        if typ not in INCLUDE_TYPES:
            continue
        if ex not in ALLOWED_EXCHANGES:
            continue
        g = grouped.get(t)
        if not g:
            continue
        if g["v"] is None or g["v"] < MIN_DAILY_VOL:
            continue
        if g["c"] is None or g["c"] < MIN_PRICE:
            continue
        filtered.append(t)

    filtered = sorted(set(filtered))
    print(f"🎯 Analysis universe after filters: {len(filtered)} tickers")

    # 3) Analyze a capped subset (respect rate limits)
    subset = filtered[:MAX_TICKERS]
    print(f"🧪 Analyzing {len(subset)} tickers (MAX_TICKERS={MAX_TICKERS})…")

    rows = []
    for i, t in enumerate(subset, 1):
        r = analyze_one(t)
        if r:
            rows.append(r)
        if i % 50 == 0:
            print(f"   • analyzed {i}/{len(subset)}")

    # 4) Rank + Top 5
    rows_sorted = sorted(rows, key=lambda r: r[9], reverse=True)
    for i, row in enumerate(rows_sorted):
        if i < 5:
            row[10] = "TOP 5"

    write_screener_sheet(gc, rows_sorted)
    print("✅ Screener update complete")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("❌ Fatal error:", e)
        traceback.print_exc()
