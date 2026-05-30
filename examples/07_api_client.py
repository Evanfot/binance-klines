"""
07 — HTTP API client: calling the Docker service from another container
     (or from the host during development).

The binance-data container exposes its full storage layer over HTTP on
port 8000.  This script demonstrates every endpoint.

Prerequisites:
  pip install requests

Usage:
  # Against local Docker container:
  python examples/07_api_client.py

  # Against a specific host:
  BASE_URL=http://binance-data:8000 python examples/07_api_client.py
"""

import os
import sys
import json

try:
    import requests
except ImportError:
    print("Install requests:  pip install requests")
    sys.exit(1)

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000").rstrip("/")

session = requests.Session()
session.headers["Accept"] = "application/json"


def get(path: str, **params) -> dict:
    url = f"{BASE_URL}{path}"
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def post(path: str, body: dict) -> dict:
    url = f"{BASE_URL}{path}"
    r = session.post(url, json=body, timeout=60)
    r.raise_for_status()
    return r.json()


# ── Health check ───────────────────────────────────────────────────────────────
print(f"\nConnecting to {BASE_URL} …")
health = get("/health")
print(f"Health : {health}")

# ── Store status ───────────────────────────────────────────────────────────────
status = get("/status")
print(f"\nStatus :")
print(f"  Date          : {status['date']}")
print(f"  Mode          : {status['mode']}")
print(f"  Symbols       : {status['symbols']}")
print(f"  Total files   : {status['total_files']:,}")
print(f"  Missing yest. : {status['missing_yesterday']}")

# ── Symbol list ────────────────────────────────────────────────────────────────
symbols_resp = get("/symbols")
symbols = symbols_resp["symbols"]
print(f"\nSymbols in store: {len(symbols)}")
print(f"  First 10: {symbols[:10]}")

if not symbols:
    print("\nNo symbols found — run 'docker compose up -d' with BINANCE_MODE=init first.")
    sys.exit(0)

sym = symbols[0]

# ── Date range ─────────────────────────────────────────────────────────────────
rng = get(f"/symbols/{sym}/range")
print(f"\n{sym} range: {rng['start']} → {rng['end']}")

# ── Load symbol (last 5 days) ──────────────────────────────────────────────────
data = get(f"/symbols/{sym}", lookback_days=5, include_live=True)
print(f"\n{sym} — last 5 days: {data['rows']} bars")
if data["data"]:
    last = data["data"][-1]
    print(f"  Last bar: open_time={last['open_time']}  close={last['close']}")

# ── Live current bar ───────────────────────────────────────────────────────────
try:
    current = get(f"/live/{sym}/current")
    print(f"\n{sym} current bar: close={current['bar']['close']}")
except requests.HTTPError as e:
    if e.response.status_code == 404:
        print(f"\n{sym}: no current bar (stream may not be running)")
    else:
        raise

# ── Live intraday ──────────────────────────────────────────────────────────────
intraday = get(f"/live/{sym}")
print(f"\n{sym} intraday bars: {intraday['rows']}")

# ── Cross-section ──────────────────────────────────────────────────────────────
top3 = ",".join(symbols[:3])
cs = get("/cross-section", symbols=top3, lookback_days=3)
print(f"\nCross-section ({top3}): {cs['rows']} bars")

# ── Universe ───────────────────────────────────────────────────────────────────
try:
    univ = get("/universe", size=10)
    print(f"\nUniverse (top 10): {univ['symbols']}")
except requests.HTTPError:
    print("\nUniverse endpoint failed (store may not have enough history)")

# ── SQL query (only works if QUERY_ENABLED=true) ───────────────────────────────
from config import HISTORICAL_DIR, INTERVAL
btc_glob = str(HISTORICAL_DIR / sym / INTERVAL / "*.parquet")

try:
    result = post("/query", {
        "sql": f"""
            SELECT DATE_TRUNC('day', open_time) AS day,
                   SUM(quote_volume)            AS dollar_vol
            FROM read_parquet('{btc_glob}')
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 5
        """
    })
    print(f"\nSQL query result ({result['rows']} rows):")
    for row in result["data"]:
        print(f"  {row}")
except requests.HTTPError as e:
    if e.response.status_code == 403:
        print("\nSQL endpoint disabled (set QUERY_ENABLED=true to enable)")
    else:
        print(f"\nSQL query error: {e.response.text}")
