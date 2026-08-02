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
# This system tracks Binance USDT-M PERPETUAL FUTURES ("um"), not spot. The volume
# alphas (a006/a014) exploit an open↔volume structure that only lives in perp volume
# (leverage/liquidation/funding); on spot volume the sleeve is dead. See the trend_trader
# decision log 202607-perp-volume-signals.md. All endpoints below are the fapi/fstream/
# futures-archive equivalents of the old spot ones.
BINANCE_BASE_URL    = "https://data.binance.vision"
BINANCE_REST_URL    = "https://fapi.binance.com"        # spot was api.binance.com
BINANCE_WS_URL      = "wss://fstream.binance.com"       # spot was stream.binance.com:9443
BINANCE_DATA_PREFIX = "data/futures/um/daily/klines"    # spot was data/spot/daily/klines
INTERVAL            = "1m"

# ── Universe ──────────────────────────────────────────────────────────────────
# Set to None to download ALL USDT-M perpetual symbols (useful for first run universe
# discovery). Set to a list of symbols to restrict (e.g. for targeted backfills).
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

# Seconds before the UTC-midnight buffer clear to snapshot the provisional daily
# close. The snapshot captures the current day's near-final close from the live 1m
# buffer; the smaller this lead, the closer the provisional is to the true 23:59:59
# close. Kept at 120s (snapshot ~23:58) so it lands well within the ~2 min before the
# 00:00 clear (the aggregation+write takes ~2-3s) and is comfortably present when the
# downstream trader caches at 00:01. Was 1200s (23:40) back when the trader rebuilt at
# 23:45 and needed a wider margin; the trader now runs at 00:01, so the lead can shrink.
PROVISIONAL_LEAD_S = 120

# Reconnect behaviour
WS_MAX_RECONNECT_ATTEMPTS = 10
WS_RECONNECT_BACKOFF_S    = 1.0   # doubles each attempt, capped at 60s

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"   # DEBUG | INFO | WARNING | ERROR
