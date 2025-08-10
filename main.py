import os
import json
import time
import requests
import gspread
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from ta.trend import EMAIndicator, SMAIndicator, MACD
from ta.momentum import RSIIndicator

# ===== CONFIG =====
SHEET_NAME   = os.getenv("SHEET_NAME", "Trading Log")
TICKERS_TAB  = os.getenv("TICKERS_TAB", "tickers")
SCREENER_TAB = os.getenv("SCREENER_TAB", "screener")

POLYGON_API_KEY   = os.getenv("POLYGON_API_KEY")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")
if not POLYGON_API_KEY or not GOOGLE_CREDS_JSON:
    raise RuntimeError("Missing POLYGON_API_KEY or GOOGLE_CREDS_JSON")

# Universe filters
ALLOWED_EXCHANGES = {"XNYS", "XNAS", "XASE", "ARCX", "BATS"}  # NYSE, NASDAQ, AMEX, ARCA, Cboe
ALLOWED_TYPES     = {"CS", "ADRC", "ADRP", "ADRR"}            # Common + ADR variants

# Screener knobs
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "20000"))  # cap for analysis
MIN_PRICE   = float(os.getenv("MIN_PRICE", "5"))      # basic price floor
MIN_DAILY_VOL = int(os.getenv("MIN_DAILY_VOL", "500000"))  # avg shares (20d) in analysis stage

# Buy rules (rolled-back MACD; still stricter trend)
RSI_MIN = float(os.getenv("RSI_MIN", "52"))
RSI_MAX = float(os.getenv("RSI_MAX", "60"))
MAX_EXT_ABOVE_EMA20_PCT = float(os.getenv("MAX_EXT_ABOVE_EMA20_PCT", "0.05"))  # 5%
REQUIRE_20D_HIGH = os.getenv("REQUIRE_20D_HIGH", "true").lower() in ("1","true","yes")

REQUEST_SLEEP = float(os.getenv("REQUEST_SLEEP", "0.05"))  # seconds between HTTP calls

# ===== HELPERS =====
def get_google_client():
    creds = json.loads(GOOGLE_CREDS_JSON)
    return gspread.service_account_from_dict(creds)

def polite_sleep():
    if REQUEST_SLEEP > 0:
        time.sleep(REQUEST_SLEEP)

# -------- Polygon v3 tickers (correct pagination + filters) --------
def fetch_all_polygon_tickers_meta():
    """
    Return list of dicts with at least: ticker, type, primary_exchange (code).
    """
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": 1000,
        "apiKey": POLYGON_API_KEY
    }
    out = []
    page = 0
    while True:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        page += 1
        print(f"   • Page {page}: {len(results)} tickers")
        for rec in results:
            t = rec.get("ticker")
            typ = (rec.get("type") or "").upper()
            ex  = (rec.get("primary_exchange") or "").upper()
            if not t:
                continue
            out.append({"ticker": t, "type": typ, "primary_exchange": ex})
        next_url = data.get("next_url")
        if not next_url:
            break
        # next_url already includes query params; we must append apiKey if not present
        if "apiKey=" not in next_url:
            sep = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{sep}apiKey={POLYGON_API_KEY}"
        url = next_url
        params = None  # subsequent pages use absolute next_url
        polite_sleep()
    # Filter by allowed exchanges/types here
    filtered = [m for m in out if m["type"] in ALLOWED_TYPES and m["primary_exchange"] in ALLOWED_EXCHANGES]
    # Deduplicate
    seen = set()
    uniq = []
    for m in filtered:
        if m["ticker"] not in seen:
            seen.add(m["ticker"])
            uniq.append(m)
    print(f"📦 Polygon active equities (post-filter): {len(uniq)}")
    return uniq

def write_tickers_sheet(ws, metas):
    rows = [[m["ticker"]] for m in metas]
    ws.clear()
    ws.update([["Ticker"]] + rows)
    print(f"✅ Wrote {len(rows)} tickers to '{TICKERS_TAB}'")

# -------- Historical data (daily) --------
def get_hist_data(ticker, days=250):
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days*2)  # buffer for weekends/holidays
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_dt:%Y-%m-%d}/{end_dt:%Y-%m-%d}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLYGON_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        results = r.json().get("results", [])
        if not results:
            return None
        df = pd.DataFrame(results)
        df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        df.rename(columns={"o":"Open","h":"High","l":"Low","c":"Close","v":"Volume"}, inplace=True)
        return df.tail(days).reset_index(drop=True)
    except Exception:
        return None
    finally:
        polite_sleep()

# -------- Analysis --------
def analyze_ticker(ticker):
    df = get_hist_data(ticker, days=250)
    if df is None or len(df) < 200:
        return None

    # Basic liquidity & price checks
    price = float(df["Close"].iloc[-1])
    if price < MIN_PRICE:
        return None
    avg_vol20 = float(df["Volume"].tail(20).mean())
    if avg_vol20 < MIN_DAILY_VOL:
        return None
    if float(df["Volume"].iloc[-1]) <= 0:
        return None

    # Indicators (ta)
    ema20_series = EMAIndicator(df["Close"], window=20).ema_indicator()
    sma50_series = SMAIndicator(df["Close"], window=50).sma_indicator()
    sma200_series= SMAIndicator(df["Close"], window=200).sma_indicator()
    rsi14 = float(RSIIndicator(df["Close"], window=14).rsi().iloc[-1])
    macd_obj = MACD(df["Close"], window_slow=26, window_fast=12, window_sign=9)
    macd_line = float(macd_obj.macd().iloc[-1])
    macd_signal = float(macd_obj.macd_signal().iloc[-1])
    macd_hist = float(macd_obj.macd_diff().iloc[-1])

    ema20 = float(ema20_series.iloc[-1])
    sma50 = float(sma50_series.iloc[-1])
    sma200 = float(sma200_series.iloc[-1])

    # Breakout (20D high)
    high20 = float(df["High"].tail(20).max())
    is_breakout = price >= high20 - 1e-9

    # ===== BUY SIGNALS (rolled back MACD: crossover + hist > 0) =====
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

    return [
        ticker,
        round(price, 2),
        round(ema20, 2),
        round(sma50, 2),
        round(sma200, 2),
        round(rsi14, 2),
        round(macd_line, 4),
        round(macd_signal, 4),
        round(macd_hist, 4),
        int(avg_vol20),
        round(high20, 2),
        "✅" if is_breakout else "",
        "✅",
        "Stricter trend + MACD crossover" + (" + 20D breakout" if REQUIRE_20D_HIGH else ""),
        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    ]

def write_screener_sheet(ws, rows):
    headers = [
        "Ticker","Price","EMA_20","SMA_50","SMA_200","RSI_14",
        "MACD","Signal","Hist","AvgVol20","20D_High","Breakout",
        "Bullish Signal","Buy Reason","Timestamp"
    ]
    ws.clear()
    if rows:
        ws.append_row(headers)
        # write in batches
        for i in range(0, len(rows), 200):
            ws.append_rows(rows[i:i+200], value_input_option="USER_ENTERED")
    else:
        ws.append_row(headers)
    print(f"✅ Wrote {len(rows)} analyzed rows to '{SCREENER_TAB}'")

def main():
    print("🚀 Ticker collector + screener starting")
    gc = get_google_client()
    tickers_ws  = gc.open(SHEET_NAME).worksheet(TICKERS_TAB)
    screener_ws = gc.open(SHEET_NAME).worksheet(SCREENER_TAB)

    metas = fetch_all_polygon_tickers_meta()  # uses correct exchange/type codes
    write_tickers_sheet(tickers_ws, metas)

    tickers = [m["ticker"] for m in metas][:MAX_TICKERS]
    print(f"🧪 Analyzing {len(tickers)} tickers (MAX_TICKERS={MAX_TICKERS})…")
    rows = []
    for i, t in enumerate(tickers, 1):
        res = analyze_ticker(t)
        if res:
            rows.append(res)
        if i % 50 == 0:
            print(f"   • analyzed {i}/{len(tickers)}")

    write_screener_sheet(screener_ws, rows)

if __name__ == "__main__":
    main()
