from __future__ import annotations
from polygon import RESTClient
import backoff, requests, pandas as pd
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import os
from dotenv import load_dotenv

# ==========================
# CONFIG
# ==========================
load_dotenv()

API_KEY = os.getenv("POLYGON_CURRENCIES_API_KEY")
OUT_DIR = Path("ohlc_data") 
YEARS_BACK = 10                   

PAIRS = [
    "EURUSD","USDJPY","GBPUSD","AUDUSD","USDCHF","USDCAD","NZDUSD",
    "EURGBP","EURJPY","EURCHF","EURAUD","EURNZD","EURCAD",
    "GBPJPY","GBPCHF","GBPAUD","GBPCAD","GBPNZD",
    "AUDJPY","AUDNZD","AUDCAD","AUDCHF",
    "NZDJPY","NZDCAD","NZDCHF",
    "CADJPY","CADCHF","CHFJPY",
    "USDNOK","USDSEK","USDDKK","USDZAR","USDTRY","USDMXN","USDPLN","USDHUF","USDILS",
    "USDCNH","USDHKD","USDSGD","USDKRW","USDINR",
]

# ==========================
# HELPERS
# ==========================
def agg_ticker_fx(p: str) -> str:
    return f"C:{p}"

def month_chunks(start_yyyymmdd: str, end_yyyymmdd: str):
    s = datetime.fromisoformat(start_yyyymmdd)
    e = datetime.fromisoformat(end_yyyymmdd)
    cur = datetime(s.year, s.month, 1)
    while cur <= e:
        nxt = (cur + relativedelta(months=1)) - relativedelta(days=1)
        start = max(cur, s).date().isoformat()
        end   = min(nxt, e).date().isoformat()
        yield start, end
        cur = cur + relativedelta(months=1)

@backoff.on_exception(backoff.expo, (requests.HTTPError, requests.ConnectionError), max_time=300)
def fetch_aggs_month(client: RESTClient, pair: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Pull one calendar month (or partial) of 1-minute aggregates for a pair.
    Uses the official SDK's generator which handles pagination internally.
    """
    rows = []
    for r in client.list_aggs(
        ticker=agg_ticker_fx(pair),
        multiplier=1,
        timespan="minute",
        from_=start_date,
        to=end_date,
        adjusted=True,
        sort="asc",
        limit=50000,
    ):
        rows.append({
            "pair": pair,
            "ts_ms": r.timestamp,         # ms since epoch
            "open": r.open, "high": r.high, "low": r.low, "close": r.close,
            "volume": r.volume, "vwap": getattr(r, "vwap", None),
            "trades": getattr(r, "transactions", None),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        ts = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        # Pandas warns 'T' will be removed; use 'min'
        df["interval_start_utc"] = ts.dt.floor("min")
        df["interval_end_utc"]   = df["interval_start_utc"] + pd.Timedelta(minutes=1)
        df = df[[
            "pair","ts_ms","interval_start_utc","interval_end_utc",
            "open","high","low","close","volume","vwap","trades"
        ]]
    return df

def save_month_parquet(df: pd.DataFrame, pair: str, y: int, m: int):
    if df.empty:
        return
    outdir = OUT_DIR / "aggs_1min" / pair
    outdir.mkdir(parents=True, exist_ok=True)
    fp = outdir / f"{y:04d}-{m:02d}.parquet"
    df.to_parquet(fp, index=False)

# ==========================
# MAIN
# ==========================
if __name__ == "__main__":
    pd.set_option("display.width", 160)
    today = date.today()
    start_date = (today - relativedelta(years=YEARS_BACK)).isoformat()
    end_date   = today.isoformat()

    print(f"Pulling 1-minute OHLC for {len(PAIRS)} pairs from {start_date} to {end_date}")
    client = RESTClient(API_KEY)

    for pair in PAIRS:
        print(f"\n[AGGS 1m] {pair}")
        total_rows = 0
        for s, e in month_chunks(start_date, end_date):
            y, m = s.split("-")[0], s.split("-")[1]
            df = fetch_aggs_month(client, pair, s, e)
            save_month_parquet(df, pair, int(y), int(m))
            cnt = 0 if df.empty else len(df)
            total_rows += cnt
            print(f"  {pair} {s}→{e}: {cnt:>7,d} rows")

        print(f"  ⇒ {pair} total: {total_rows:,} rows")

    print(f"\n✅ Done. Parquet files under: {OUT_DIR.resolve()}")
