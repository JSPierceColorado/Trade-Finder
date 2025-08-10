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

# Universe prefilter (from grouped bars + metadata)
MIN_DAILY_VOL = int(os.getenv("MIN_DAILY_VOL", "300000"))
MIN_PRICE     = float(os.getenv("MIN_PRICE", "2.0"))
ALLOWED_EXCHANGES = set(os.getenv("ALLOWED_EXCHANGES", "XNYS,XNAS,ARCX,BATS,XASE").split(","))
INCLUDE_TYPES = set(os.getenv("INCLUDE_TYPES", "CS,ADRC,ADRP,ADRR").split(","))
EXCLUDE_TYPES = set(os.getenv("EXCLUDE_TYPES", "ETF,ETN,FUND,SP,PFD,WRT,RIGHT,UNIT,REIT").split(","))

# Analysis caps & pacing
MAX_TICKERS = int(os.getenv("MAX_TICKERS", "800"))
DAYS_LOOKBACK = int(os.getenv("DAYS_LOOKBACK", "200"))
SLEEP_MS_BETWEEN_CALLS = int(os.getenv("SLEEP_MS_BETWEEN_CALLS", "50"))

# Stricter BUY criteria (tune these to widen/narrow the funnel)
RSI_MIN = float(os.getenv("RSI_MIN", "50"))
RSI_MAX = float(os.getenv("RSI_MAX", "62"))
MAX_EXT_ABOVE_EMA20_PCT = float(os.getenv("MAX_EXT_ABOVE_EMA20_PCT", "0.08"))  # 8% cap
MIN_AVG_DOLLAR_VOL_20 = float(os.getenv("MIN_AVG_DOLLAR_VOL_20", "10000000"))  # $10M
REQUIRE_20D_HIGH = os.getenv("REQUIRE_20D_HIGH", "false").lower() in ("1","true","yes")

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
    for i in range(0, len(rows), 1000):
        ws.append_rows(rows[i:i+1000], value_input_option="USER_ENTERED")
    print(f"✅ Wrote {len(tickers)} tickers to '{TICKERS_TAB}'")

def write_screener_sheet(gc, rows):
    ws = gc.open(SHEET_NAME).worksheet(SCREENER_TAB)
    ws.clear()
    headers = [
        "Ticker","Price","EMA_20","SMA_50","RSI_14","MACD","Signal","MACD_Hist","MACD_Hist_Δ",
        "AvgVol20","Avg$Vol20","20D_High","Breakout","Bullish Signal","Buy Reason","Timestamp"
    ]
    ws.append_row(headers)
    for i in range(0, len(rows), 100):
        ws.append_rows(rows[i:i+100], value_input_option="USER_ENTERED")
    print(f"✅ Wrote {len(rows)} rows to '{SCREENER_TAB}'")

# =========================
# Polygon helpers
# =========================
BASE = "https://api.polygon.io"

def polite_sleep():
    if SLEEP_MS_BETWEEN_CALLS > 0:
        time.sleep(SLEEP_MS_BETWEEN_CALLS / 1000.0)

def fetch_all_polygon_meta():
    url = f"{BASE}/v3/reference/tickers"
    params = {"market":"stocks","active":"true","limit":1000,"apiKey":POLYGON_API_KEY}
    out, next_url, next_params, page = [], url, params, 0
    while next_url:
        r = requests.get(next_url, params=next_params, timeout=30); r.raise_for_status()
        data = r.json(); results = data.get("results", [])
        for rec in results:
            out.append({
                "ticker": rec.get("ticker"),
                "type": (rec.get("type") or "").upper(),
                "primary_exchange": (rec.get("primary_exchange") or "").upper()
            })
        page += 1; print(f"   • Page {page}: {len(results)} tickers")
        next_url = data.get("next_url"); next_params = {"apiKey": POLYGON_API_KEY} if next_url else None
        polite_sleep()
    seen, meta = set(), []
    for m in out:
        t = m["ticker"]
        if t and t not in seen:
            seen.add(t); meta.append(m)
    print(f"📦 Polygon active equities (meta): {len(meta)}")
    return meta

def last_trading_date_utc():
    d = datetime.now(timezone.utc).date() - timedelta(days=1)
    for _ in range(5):
        yield d; d -= timedelta(days=1)

def fetch_grouped_map():
    for d in last_trading_date_utc():
        url = f"{BASE}/v2/aggs/grouped/locale/us/market/stocks/{d.isoformat()}"
        params = {"adjusted":"true","apiKey":POLYGON_API_KEY}
        try:
            r = requests.get(url, params=params, timeout=45); r.raise_for_status()
            res = r.json().get("results", [])
            if res:
                m = { rec["T"]: {"v": rec.get("v",0), "c": rec.get("c",0.0)} for rec in res if rec.get("T") }
                print(f"🗂️ Grouped bars date: {d.isoformat()} (tickers: {len(m)})")
                return m
        except Exception:
            pass
        polite_sleep()
    print("⚠️ Could not fetch grouped aggregates; proceeding without prefilter (may be slow).")
    return {}

def fetch_daily_bars_df(ticker, days=DAYS_LOOKBACK):
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days*2)
    url = f"{BASE}/v2/aggs/ticker/{ticker}/range/1/day/{start.isoformat()}/{end.isoformat()}"
    params = {"adjusted":"true","sort":"asc","limit":50000,"apiKey":POLYGON_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=30); r.raise_for_status()
        results = r.json().get("results", [])
        if not results: return None
        df = pd.DataFrame(results)
        df.rename(columns={"c":"close","o":"open","h":"high","l":"low","v":"volume","t":"timestamp"}, inplace=True)
        return df.tail(days).reset_index(drop=True)
    except Exception:
        return None
    finally:
        polite_sleep()

# =========================
# Indicators
# =========================
def ema(series, window): return series.ewm(span=window, adjust=False).mean()
def sma(series, window): return series.rolling(window).mean()

def rsi(series, window=14):
    delta = series.diff()
    up = delta.clip(lower=0); down = -delta.clip(upper=0)
    ru = up.ewm(alpha=1/window, adjust=False).mean()
    rd = down.ewm(alpha=1/window, adjust=False).mean().replace(0, np.nan)
    rs = ru / rd
    return 100 - (100 / (1 + rs))

def macd(series, fast=12, slow=26, signal=9):
    ef = ema(series, fast); es = ema(series, slow)
    line = ef - es; sig = ema(line, signal)
    return line, sig, line - sig

# =========================
# Analysis
# =========================
def analyze_one(ticker):
    df = fetch_daily_bars_df(ticker, DAYS_LOOKBACK)
    if df is None or df.shape[0] < 60:  # need enough for SMA50 etc.
        return None

    # Guard: latest bar must have volume
    if "volume" not in df.columns or float(df["volume"].iloc[-1]) <= 0:
        return None

    close = df["close"].astype(float)
    vol   = df["volume"].astype(float)

    price = float(close.iloc[-1])
    ema20 = float(ema(close, 20).iloc[-1])
    sma50 = float(sma(close, 50).iloc[-1])
    rsi14 = float(rsi(close, 14).iloc[-1])

    macd_line, macd_sig, macd_hist = macd(close, 12, 26, 9)
    macd_v   = float(macd_line.iloc[-1])
    signal_v = float(macd_sig.iloc[-1])
    hist_v   = float(macd_hist.iloc[-1])
    hist_prev= float(macd_hist.iloc[-2]) if macd_hist.shape[0] >= 2 else np.nan
    hist_delta = hist_v - hist_prev if not np.isnan(hist_prev) else np.nan

    avg_vol20 = float(vol.tail(20).mean())
    avg_dollar_vol20 = float((vol.tail(20) * close.tail(20)).mean())

    high_20 = float(close.tail(20).max())
    is_breakout = price >= high_20 - 1e-9  # allow float fuzz

    # ===== Stricter BUY signal =====
    # Liquidity
    if avg_dollar_vol20 < MIN_AVG_DOLLAR_VOL_20:
        return None
    # Trend
    if not (price > ema20 > sma50):
        return None
    # RSI band
    if not (RSI_MIN < rsi14 < RSI_MAX):
        return None
    # MACD health
    if not (macd_v > signal_v and hist_v > 0 and (not np.isnan(hist_delta) and hist_delta > 0)):
        return None
    # Not too extended above EMA20
    if (price / ema20 - 1.0) > MAX_EXT_ABOVE_EMA20_PCT:
        return None
    # Optional breakout
    if REQUIRE_20D_HIGH and not is_breakout:
        return None

    buy_reason = (
        f"Uptrend (P>EMA20>SMA50), RSI {RSI_MIN}-{RSI_MAX}, MACD>Signal & Hist↑, "
        f"≤{int(MAX_EXT_ABOVE_EMA20_PCT*100)}% above EMA20"
        + (" + 20D breakout" if REQUIRE_20D_HIGH else "")
    )

    row = [
        ticker,
        round(price, 2),
        round(ema20, 2),
        round(sma50, 2),
        round(rsi14, 2),
        round(macd_v, 4),
        round(signal_v, 4),
        round(hist_v, 4),
        round(hist_delta, 4) if not np.isnan(hist_delta) else "",
        int(avg_vol20),
        int(avg_dollar_vol20),
        round(high_20, 2),
        "✅" if is_breakout else "",
        "✅",
        buy_reason,
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    ]
    return row

# =========================
# Orchestration
# =========================
def main():
    if not POLYGON_API_KEY:
        raise RuntimeError("Missing POLYGON_API_KEY env var.")

    print("🚀 Ticker collector + screener starting")
    gc = get_google_client()

    # 1) Universe with metadata (write full set to tickers tab)
    print("📥 Fetching all active equities (meta) from Polygon…")
    meta = fetch_all_polygon_meta()
    all_tickers = sorted({m["ticker"] for m in meta if m["ticker"]})
    write_tickers_sheet(gc, all_tickers)

    # 2) Prefilter with grouped aggregates + metadata (cheap)
    grouped = fetch_grouped_map()
    filtered = []
    for m in meta:
        t = m["ticker"]; typ = m["type"]; ex = m["primary_exchange"]
        if not t or not typ or not ex: continue
        if typ in EXCLUDE_TYPES or typ not in INCLUDE_TYPES: continue
        if ex not in ALLOWED_EXCHANGES: continue
        g = grouped.get(t); if not g: continue
        if (g["v"] or 0) < MIN_DAILY_VOL: continue
        if (g["c"] or 0.0) < MIN_PRICE: continue
        filtered.append(t)
    filtered = sorted(set(filtered))
    print(f"🎯 Prefiltered universe: {len(filtered)} tickers")

    # 3) Analyze a capped subset
    subset = filtered[:MAX_TICKERS]
    print(f"🧪 Analyzing {len(subset)} tickers (MAX_TICKERS={MAX_TICKERS})…")
    rows = []
    for i, t in enumerate(subset, 1):
        r = analyze_one(t)
        if r: rows.append(r)
        if i % 50 == 0: print(f"   • analyzed {i}/{len(subset)}")

    # 4) No ranking. Write all qualifiers.
    write_screener_sheet(gc, rows)
    print("✅ Screener update complete")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("❌ Fatal error:", e)
        traceback.print_exc()
