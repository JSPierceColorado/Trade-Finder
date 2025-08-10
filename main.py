import os
import json
import requests
import gspread
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from ta.trend import EMAIndicator, SMAIndicator, MACD
from ta.momentum import RSIIndicator

# ===== CONFIG =====
SHEET_NAME = "Trading Log"
TICKERS_TAB = "tickers"
SCREENER_TAB = "screener"

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

MAX_TICKERS = int(os.getenv("MAX_TICKERS", 20000))
MIN_PRICE = float(os.getenv("MIN_PRICE", 5))
MIN_DAILY_VOL = int(os.getenv("MIN_DAILY_VOL", 500000))
RSI_MIN = float(os.getenv("RSI_MIN", 45))
RSI_MAX = float(os.getenv("RSI_MAX", 70))
MAX_EXT_ABOVE_EMA20_PCT = float(os.getenv("MAX_EXT_ABOVE_EMA20_PCT", 0.05))
REQUIRE_20D_HIGH = os.getenv("REQUIRE_20D_HIGH", "true").lower() == "true"

# ===== HELPERS =====
def get_google_client():
    creds = json.loads(GOOGLE_CREDS_JSON)
    return gspread.service_account_from_dict(creds)

def fetch_all_polygon_tickers():
    print("📥 Fetching all active equities from Polygon…")
    url = "https://api.polygon.io/v3/reference/tickers"
    all_tickers = []
    params = {
        "market": "stocks",
        "active": "true",
        "limit": 1000,
        "apiKey": POLYGON_API_KEY
    }
    while True:
        r = requests.get(url, params=params)
        data = r.json()
        results = data.get("results", [])
        if not results:
            break
        batch = [t for t in results if t.get("type") == "CS" and t.get("primary_exchange") in ["NYSE", "NASDAQ", "AMEX"]]
        all_tickers.extend(batch)
        print(f"   • Page {len(all_tickers)//1000 + 1}: {len(batch)} tickers")
        if "next_url" in data:
            url = data["next_url"]
            params = {"apiKey": POLYGON_API_KEY}
        else:
            break
    return all_tickers

def write_tickers_sheet(ws, tickers):
    rows = [[t["ticker"]] for t in tickers]
    ws.clear()
    ws.update([["Ticker"]] + rows)
    print(f"✅ Wrote {len(tickers)} tickers to '{TICKERS_TAB}'")

def get_hist_data(ticker, days=250):
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days*2)
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date:%Y-%m-%d}/{end_date:%Y-%m-%d}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLYGON_API_KEY}
    r = requests.get(url, params=params)
    if r.status_code != 200:
        return None
    data = r.json().get("results", [])
    if not data:
        return None
    df = pd.DataFrame(data)
    df["t"] = pd.to_datetime(df["t"], unit="ms")
    df.rename(columns={"o": "Open", "h": "High", "l": "Low", "c": "Close", "v": "Volume"}, inplace=True)
    return df

def analyze_ticker(ticker):
    df = get_hist_data(ticker)
    if df is None or len(df) < 50:
        return None

    # Basic filters
    price = df["Close"].iloc[-1]
    if price < MIN_PRICE:
        return None
    avg_vol20 = df["Volume"].tail(20).mean()
    if avg_vol20 < MIN_DAILY_VOL:
        return None

    # Indicators
    ema20 = EMAIndicator(df["Close"], window=20).ema_indicator().iloc[-1]
    sma50 = SMAIndicator(df["Close"], window=50).sma_indicator().iloc[-1]
    sma200 = SMAIndicator(df["Close"], window=200).sma_indicator().iloc[-1] if len(df) >= 200 else np.nan
    rsi14 = RSIIndicator(df["Close"], window=14).rsi().iloc[-1]
    macd_obj = MACD(df["Close"], window_slow=26, window_fast=12, window_sign=9)
    macd_line = macd_obj.macd().iloc[-1]
    macd_signal = macd_obj.macd_signal().iloc[-1]
    macd_hist = macd_obj.macd_diff().iloc[-1]

    # Breakout check
    high20 = df["High"].tail(20).max()
    is_breakout = price >= high20

    # ===== BUY SIGNALS =====
    if not (price > ema20 > sma50 > sma200):
        return None
    if (price / ema20 - 1.0) > MAX_EXT_ABOVE_EMA20_PCT:
        return None
    if not (RSI_MIN < rsi14 < RSI_MAX):
        return None
    if not (macd_line > macd_signal and macd_hist > 0):
        return None
    if REQUIRE_20D_HIGH and not is_breakout:
        return None

    return {
        "Ticker": ticker,
        "Price": round(price, 2),
        "EMA_20": round(ema20, 2),
        "SMA_50": round(sma50, 2),
        "SMA_200": round(sma200, 2) if not np.isnan(sma200) else "",
        "RSI_14": round(rsi14, 2),
        "MACD": round(macd_line, 4),
        "Signal": round(macd_signal, 4),
        "Hist": round(macd_hist, 4),
        "Bullish Signal": "✅",
        "Buy Reason": "Stricter trend + MACD crossover + breakout",
        "Timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }

def write_screener_sheet(ws, rows):
    headers = ["Ticker", "Price", "EMA_20", "SMA_50", "SMA_200", "RSI_14", "MACD", "Signal", "Hist",
               "Bullish Signal", "Buy Reason", "Timestamp"]
    ws.clear()
    ws.update([headers] + [list(r.values()) for r in rows])
    print(f"✅ Wrote {len(rows)} analyzed rows to '{SCREENER_TAB}'")

def main():
    print("🚀 Ticker collector + screener starting")
    gc = get_google_client()
    tickers_ws = gc.open(SHEET_NAME).worksheet(TICKERS_TAB)
    screener_ws = gc.open(SHEET_NAME).worksheet(SCREENER_TAB)

    tickers = fetch_all_polygon_tickers()
    write_tickers_sheet(tickers_ws, tickers)

    print(f"🧪 Analyzing {len(tickers)} tickers (MAX_TICKERS={MAX_TICKERS})…")
    analyzed = []
    for t in tickers[:MAX_TICKERS]:
        res = analyze_ticker(t["ticker"])
        if res:
            analyzed.append(res)
    write_screener_sheet(screener_ws, analyzed)

if __name__ == "__main__":
    main()
