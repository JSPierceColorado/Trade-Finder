import os
import json
import time
import requests
import gspread
from datetime import datetime, timezone
import pandas as pd
import numpy as np

# ======================
# CONFIG
# ======================
SHEET_NAME = "Trading Log"
TICKERS_TAB = "tickers"
SCREENER_TAB = "screener"

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

MAX_TICKERS = 20000       # hard cap for sanity
DAYS_LOOKBACK = 50        # for indicators
REQUEST_SLEEP = 0.25      # rate limit spacing in seconds

# ======================
# GOOGLE SHEETS
# ======================
def get_google_client():
    creds = json.loads(GOOGLE_CREDS_JSON)
    return gspread.service_account_from_dict(creds)

def write_to_sheet(ws, rows):
    ws.clear()
    ws.append_row([
        "Ticker", "Price", "EMA_20", "RSI_14",
        "MACD", "Signal", "Bullish Signal", "Buy Reason",
        "Timestamp", "RankScore"
    ])
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")

# ======================
# TECHNICAL INDICATORS
# ======================
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def macd(series, fast=12, slow=26, signal=9):
    macd_line = ema(series, fast) - ema(series, slow)
    signal_line = ema(macd_line, signal)
    return macd_line, signal_line

# ======================
# POLYGON API
# ======================
def fetch_all_tickers():
    print("📥 Fetching all active equities from Polygon…")
    tickers = []
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": 1000,
        "apiKey": POLYGON_API_KEY
    }
    next_url = url
    page = 1
    while next_url:
        resp = requests.get(next_url, params=params if page == 1 else None, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # FIX: don't filter by primary_exchange human names; take all here
        batch = [t["ticker"] for t in data.get("results", []) if "ticker" in t]
        tickers.extend(batch)
        print(f"   • Page {page}: {len(batch)} tickers")
        next_url = data.get("next_url")
        if next_url:
            # include apiKey on subsequent pages
            joiner = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{joiner}apiKey={POLYGON_API_KEY}"
        page += 1
        time.sleep(REQUEST_SLEEP)
    print(f"📦 Polygon active equities: {len(tickers)}")
    return tickers

def fetch_daily_bars_df(ticker, days):
    start = (datetime.now(timezone.utc) - pd.Timedelta(days=days*2)).strftime("%Y-%m-%d")
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
    params = {"adjusted": "true", "sort": "asc", "limit": 5000, "apiKey": POLYGON_API_KEY}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if not results:
            return None
        df = pd.DataFrame(results)
        df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        df.rename(columns={"c": "close", "v": "volume"}, inplace=True)
        return df
    except Exception:
        return None

# ======================
# ANALYSIS
# ======================
def analyze_one(ticker):
    df = fetch_daily_bars_df(ticker, DAYS_LOOKBACK)
    if df is None or df.shape[0] < 50:
        return None

    # skip symbols with no recent volume
    if "volume" not in df.columns or float(df["volume"].iloc[-1]) <= 0:
        return None

    close = df["close"]
    price = float(close.iloc[-1])
    ema20 = float(ema(close, 20).iloc[-1])
    rsi14 = float(rsi(close, 14).iloc[-1])
    macd_line, signal_line = macd(close, 12, 26, 9)
    macd_v = float(macd_line.iloc[-1])
    signal_v = float(signal_line.iloc[-1])

    # MORE EXCLUSIVE CRITERIA
    is_bullish = (
        (40 < rsi14 < 60) and          # tighter RSI range
        (macd_v > signal_v * 1.01) and # MACD clearly above signal
        (price > ema20 * 1.01)         # price 1% above EMA20
    )

    rank_score = 0.0
    if price > ema20:
        rank_score += 10
    rank_score += max(0.0, min(rsi14 - 25.0, 40.0))
    if macd_v > signal_v:
        rank_score += 5.0

    buy_reason = "Meets strict RSI, MACD, EMA20 criteria" if is_bullish else "Criteria not met"

    return [
        ticker,
        round(price, 2),
        round(ema20, 2),
        round(rsi14, 2),
        round(macd_v, 4),
        round(signal_v, 4),
        "✅" if is_bullish else "",
        buy_reason,
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        round(rank_score, 3)
    ]

# ======================
# MAIN
# ======================
def main():
    print("🚀 Ticker collector + screener starting")
    gc = get_google_client()

    # Fetch tickers
    tickers = fetch_all_tickers()
    ws_tickers = gc.open(SHEET_NAME).worksheet(TICKERS_TAB)
    ws_tickers.clear()
    ws_tickers.append_row(["Ticker"])
    if tickers:
        ws_tickers.append_rows([[t] for t in tickers], value_input_option="USER_ENTERED")
    print(f"✅ Wrote {len(tickers)} tickers to '{TICKERS_TAB}'")

    # Analyze
    print(f"🧪 Analyzing {min(len(tickers), MAX_TICKERS)} tickers (MAX_TICKERS={MAX_TICKERS})…")
    rows = []
    for i, t in enumerate(tickers[:MAX_TICKERS], 1):
        row = analyze_one(t)
        if row:
            rows.append(row)
        if i % 50 == 0:
            print(f"   • {i} analyzed")
        time.sleep(REQUEST_SLEEP)

    # Write screener results
    ws_screener = gc.open(SHEET_NAME).worksheet(SCREENER_TAB)
    write_to_sheet(ws_screener, rows)
    print(f"✅ Wrote {len(rows)} analyzed rows to '{SCREENER_TAB}'")

if __name__ == "__main__":
    main()
