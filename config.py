"""
Central configuration for the Binance data system.
Edit this file to change behaviour across the entire system.
"""

import os
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
# BINANCE_DATA_ROOT overrides the default source-relative root (set in Docker).
ROOT = Path(os.environ["BINANCE_DATA_ROOT"]) if os.environ.get("BINANCE_DATA_ROOT") else Path(__file__).parent
HISTORICAL_DIR = ROOT / "klines" / "historical"
LIVE_DIR       = ROOT / "klines" / "live"
LOG_DIR        = ROOT / "logs"

for _d in (HISTORICAL_DIR, LIVE_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── Binance endpoints ─────────────────────────────────────────────────────────
BINANCE_BASE_URL    = "https://data.binance.vision"
BINANCE_REST_URL    = "https://api.binance.com"
BINANCE_WS_URL      = "wss://stream.binance.com:9443"
BINANCE_DATA_PREFIX = "data/spot/daily/klines"
INTERVAL            = "1m"

# ── Universe ──────────────────────────────────────────────────────────────────
# Set to None to download ALL spot symbols (useful for first run universe discovery).
# Set to a list of symbols to restrict (e.g. for targeted backfills).
SYMBOL_OVERRIDE: list[str] | None = None

# Dollar-volume universe filter (used by strategy layer, not the downloader).
UNIVERSE_SIZE          = 50
UNIVERSE_LOOKBACK_DAYS = 20
UNIVERSE_MIN_DAYS      = 5   # symbol must appear in top-N for this many consecutive days before inclusion
UNIVERSE_MIN_VOL       = 0.20  # minimum annualized volatility; filters stablecoins and FX pairs

# ── Downloader behaviour ──────────────────────────────────────────────────────
MAX_CONCURRENT_DOWNLOADS = 8    # parallel symbol downloads
RETRY_ATTEMPTS           = 3
RETRY_BACKOFF_S          = 2.0  # seconds, doubles on each retry
VERIFY_CHECKSUMS         = True
# Daily update re-checks this trailing window (not just yesterday) so that a day's
# 1m archives, which Binance publishes per-symbol over the following 1-2 days, keep
# filling as they land instead of freezing at whatever coverage existed the morning
# after. Matches the closes re-aggregation window (closes.BACKFILL_DAYS).
DAILY_UPDATE_BACKFILL_DAYS = 14

# ── Live stream ───────────────────────────────────────────────────────────────
# How many completed 1m bars to keep in the live intraday parquet per symbol.
# Older bars are already in historical; this is just a rolling buffer.
LIVE_INTRADAY_BUFFER_BARS = 1440   # 24h of 1m bars

# Reconnect behaviour
WS_MAX_RECONNECT_ATTEMPTS = 10
WS_RECONNECT_BACKOFF_S    = 1.0   # doubles each attempt, capped at 60s

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"   # DEBUG | INFO | WARNING | ERROR
