# Binance 1m Kline Data System

Institutional-grade historical download + live streaming for Binance spot klines, packaged as a self-contained Docker service with an HTTP API.

---

## Contents

- [Architecture](#architecture)
- [Quickstart — Local Python](#quickstart--local-python)
- [Docker Setup](#docker-setup)
  - [Modes](#modes)
  - [First Run](#first-run)
  - [Connecting Other Containers](#connecting-other-containers)
- [HTTP API Reference](#http-api-reference)
- [Reading Data — Python](#reading-data--python)
  - [DataStore (recommended)](#datastore-recommended)
  - [HistoricalStore](#historicalstore)
  - [LiveStore](#livestore)
  - [DuckDB queries](#duckdb-queries)
  - [Universe construction](#universe-construction)
- [Data Schema](#data-schema)
- [Configuration](#configuration)
- [Cron / Scheduling](#cron--scheduling)
- [Security](#security)
- [Examples](#examples)

---

## Architecture

```
binance_data/
├── config.py        — all tunable parameters
├── logger.py        — rotating file + console logging
├── storage.py       — all disk I/O (parquet, DuckDB)
├── downloader.py    — async batch historical downloader
├── stream.py        — WebSocket live stream with reconnect
├── universe.py      — top-N dollar-volume universe
├── main.py          — CLI entry point
├── api.py           — HTTP API server (FastAPI)
└── klines/
    ├── historical/  — immutable daily parquet files
    │   └── BTCUSDT/1m/
    │       ├── 2024-01-15.parquet
    │       └── 2024-01-16.parquet
    └── live/        — stream-owned intraday files
        └── BTCUSDT/
            ├── intraday.parquet    — completed bars since UTC midnight
            └── current_bar.parquet — in-progress bar (overwritten each tick)
```

### The boundary rule

| Store | Owns | Writer |
|---|---|---|
| `historical/` | dates strictly < today UTC | Downloader only |
| `live/` | today's data only | Stream only |

The downloader runs after UTC midnight to collect yesterday into `historical/`.
The stream clears its intraday buffer at the same moment.
The two writers can never conflict.

---

## Quickstart — Local Python

```bash
pip install -r requirements.txt

# Full historical download then start streaming (first run)
python main.py init

# Download from a specific date (much faster for testing)
python main.py init --start 2024-01-01

# Stream only (historical already downloaded)
python main.py stream

# Download yesterday's files and exit (for cron)
python main.py update

# Store health summary
python main.py status
```

---

## Docker Setup

### Build

```bash
docker build -t binance-data:latest .

# Or with Compose (also handles volumes and networking):
docker compose build
```


`python main.py` is for local development only.
In Docker everything is driven by the `BINANCE_MODE` environment variable — the
container manages downloading, streaming, and the UTC midnight rollover by itself.

| Local command | Docker equivalent |
|---|---|
| `python main.py init` | `BINANCE_MODE=init docker compose up -d` |
| `python main.py stream` | `docker compose up -d` (stream is the default) |
| `python main.py status` | `curl http://localhost:8000/status` |
| `python main.py update` | Not needed — the running stream handles midnight rollover automatically |

Each container runs three things in one process:
1. The HTTP API on port 8000 — available immediately on startup
2. The historical download in the background (only when `BINANCE_MODE=init`)
3. Continuous streaming — including the UTC midnight rollover (clears intraday buffer, moves yesterday into historical)

Data is stored on a named Docker volume (`binance-klines`) and survives container
restarts, so history is never re-downloaded.

### Modes

Set `BINANCE_MODE` to control what the container does on startup:

| `BINANCE_MODE` | Behaviour |
|---|---|
| `init` | Download full history, then stream + serve API |
| `stream` *(default)* | Stream + serve API (history must already be on the volume) |
| `api` | Serve API only — no streaming, read-only replica |

The HTTP API is always served on port 8000 regardless of mode.

### First Run

```bash
# Download all history from Binance launch (~2017) then stream.
# This will take several hours on the first run.
BINANCE_MODE=init docker compose up -d

# Faster: start from a specific date
BINANCE_MODE=init BINANCE_START_DATE=2024-01-01 docker compose up -d

# Watch progress
docker compose logs -f
```

### Subsequent Runs

```bash
# History is on the volume — just stream
docker compose up -d

# Check what's in the store
curl http://localhost:8000/status
```

### Connecting Other Containers

There are two ways for another container to access data.

#### Option A — HTTP API (recommended, language-agnostic)

Join the `binance-net` Docker network and call the API at `http://binance-data:8000`.

```yaml
# your-strategy/docker-compose.yml
services:
  strategy:
    image: your-strategy:latest
    networks:
      - binance-net      # join the shared network

networks:
  binance-net:
    external: true       # created by binance-data's compose
```

```python
# Inside your strategy container
import requests
resp = requests.get("http://binance-data:8000/symbols/BTCUSDT?lookback_days=30")
data = resp.json()["data"]  # list of OHLCV dicts
```

#### Option B — Shared Volume (Python containers only)

Mount the `binance-klines` volume and use the storage classes directly.
The parquet files are readable by any process that can import pyarrow/pandas.

```yaml
# your-strategy/docker-compose.yml
services:
  strategy:
    image: your-strategy:latest
    volumes:
      - binance-klines:/app/klines   # same path the binance container uses
    networks:
      - binance-net

volumes:
  binance-klines:
    external: true

networks:
  binance-net:
    external: true
```

```python
# Copy storage.py and config.py into your image, or install binance-data as a package.
# Then use the storage classes exactly as in local development.
import sys
sys.path.insert(0, "/path/to/binance_data")
from storage import DataStore
df = DataStore().load("BTCUSDT", lookback_days=30)
```

---

## HTTP API Reference

Interactive docs (Swagger UI): `http://localhost:8000/docs`

### Meta

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness check |
| GET | `/status` | Store health: symbol count, file count, gaps |

### Historical

| Method | Path | Key query params | Description |
|---|---|---|---|
| GET | `/symbols` | — | List all symbols in the store |
| GET | `/symbols/{symbol}` | `lookback_days`, `start`, `end`, `include_live` | OHLCV bars for one symbol |
| GET | `/symbols/{symbol}/range` | — | First and last available date |
| GET | `/cross-section` | `symbols` (comma-sep), `lookback_days`, `include_live` | Multi-symbol panel |

### Live

| Method | Path | Description |
|---|---|---|
| GET | `/live/{symbol}` | Rolling intraday buffer (completed bars since UTC midnight) |
| GET | `/live/{symbol}/current` | The single in-progress bar (updated every second) |

### Universe

| Method | Path | Key query params | Description |
|---|---|---|---|
| GET | `/universe` | `size`, `lookback_days` | Top-N symbols by dollar volume |

### SQL (disabled by default)

| Method | Path | Description |
|---|---|---|
| POST | `/query` | Raw DuckDB SELECT — requires `QUERY_ENABLED=true` |

**Request body:**
```json
{ "sql": "SELECT open_time, close FROM read_parquet('...') LIMIT 5" }
```

See [Security](#security) before enabling.

### curl examples

```bash
# Health
curl http://localhost:8000/health

# Store status
curl http://localhost:8000/status | python -m json.tool

# List symbols
curl http://localhost:8000/symbols

# Last 5 days of BTCUSDT including live
curl "http://localhost:8000/symbols/BTCUSDT?lookback_days=5&include_live=true"

# Date range for a symbol
curl http://localhost:8000/symbols/ETHUSDT/range

# Cross-section: 3 symbols, last 10 days
curl "http://localhost:8000/cross-section?symbols=BTCUSDT,ETHUSDT,BNBUSDT&lookback_days=10"

# Live intraday bars
curl http://localhost:8000/live/BTCUSDT

# Current in-progress bar
curl http://localhost:8000/live/BTCUSDT/current

# Universe (top 20)
curl "http://localhost:8000/universe?size=20"

# SQL query (requires QUERY_ENABLED=true)
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM read_parquet(\"/app/klines/historical/BTCUSDT/1m/*.parquet\")"}'
```

---

## Reading Data — Python

### DataStore (recommended)

`DataStore` is the unified interface that seamlessly stitches historical and live data.

```python
from storage import DataStore

ds = DataStore()

# Single symbol — last 30 days, live data appended
df = ds.load("BTCUSDT", lookback_days=30, include_live=True)

# Single symbol — specific date range
df = ds.load("BTCUSDT")  # full history

# Multi-symbol panel — last 20 days
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
panel = ds.load_cross_section(symbols, lookback_days=20, include_live=True)
# panel has an extra "symbol" column
```

### HistoricalStore

Direct access to the immutable daily files.

```python
from storage import HistoricalStore
from datetime import date

store = HistoricalStore()

# All symbols with data
symbols = store.available_symbols()

# Date range for a symbol
start, end = store.date_range("BTCUSDT")

# Single symbol, date range
df = store.load_symbol("BTCUSDT",
    start=date(2024, 1, 1),
    end=date(2024, 2, 1),   # exclusive
)

# Multi-symbol cross-section
df = store.load_cross_section(
    ["BTCUSDT", "ETHUSDT"],
    start=date(2024, 1, 1),
    end=date(2024, 2, 1),
)

# Missing date check
missing = store.missing_dates("BTCUSDT", date(2024, 1, 1), date.today())
```

### LiveStore

Read-only access to in-progress intraday data (stream must be running).

```python
from storage import LiveStore

live = LiveStore()

# Rolling completed bars since UTC midnight
df = live.read_intraday("BTCUSDT")

# The single in-progress bar (overwritten every second)
bar = live.read_current_bar("BTCUSDT")  # pd.Series or None

# Both combined
df = live.read_all("BTCUSDT")
```

### DuckDB queries

DuckDB reads parquet files directly — efficient for cross-sectional SQL analytics
over the full universe without loading everything into memory.

```python
from storage import HistoricalStore, all_symbols_glob
from config import HISTORICAL_DIR, INTERVAL

store = HistoricalStore()

# Glob covering all symbols
glob = all_symbols_glob()  # /app/klines/historical/*/1m/*.parquet

# Example: top 10 symbols by dollar volume yesterday
df = store.query(f"""
    SELECT
        split_part(filename, '/', -3)  AS symbol,
        SUM(quote_volume)              AS dollar_vol,
        SUM(num_trades)                AS trades
    FROM read_parquet('{glob}', filename=true)
    WHERE open_time >= current_date - INTERVAL '1 day'
      AND open_time <  current_date
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
""")

# Example: hourly VWAP for BTCUSDT
btc_glob = str(HISTORICAL_DIR / "BTCUSDT" / INTERVAL / "*.parquet")

df = store.query(f"""
    SELECT
        time_bucket(INTERVAL '1 hour', open_time) AS hour,
        SUM(quote_volume) / NULLIF(SUM(volume), 0) AS vwap
    FROM read_parquet('{btc_glob}')
    WHERE open_time >= current_date - INTERVAL '7 days'
    GROUP BY 1
    ORDER BY 1
""")

# Example: rolling realised volatility
df = store.query(f"""
    WITH bars AS (
        SELECT open_time,
               LN(close / LAG(close) OVER (ORDER BY open_time)) AS log_ret
        FROM read_parquet('{btc_glob}')
    )
    SELECT open_time,
           STDDEV(log_ret) OVER (
               ORDER BY open_time
               ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
           ) * SQRT(525600) AS ann_vol
    FROM bars
    ORDER BY open_time DESC
    LIMIT 20
""")
```

### Universe construction

```python
from universe import build_universe, universe_dollar_volumes
from datetime import date

# Default: top 50 by 20-day dollar volume, with 5-day entry buffer
universe = build_universe()

# Custom parameters
universe = build_universe(
    size=20,
    lookback_days=10,
    min_consecutive_days=3,
)

# Historical universe (as of a past date)
past_universe = build_universe(as_of=date(2024, 6, 1), size=50)

# Dollar-volume detail for position sizing
dv_df = universe_dollar_volumes(universe)
# columns: symbol, dollar_volume, vwap, avg_daily_dv
```

### Derived metrics

```python
# All derivable from the raw columns:
df["vwap"]            = df["quote_volume"] / df["volume"]
df["dollar_volume"]   = df["quote_volume"]          # quote_volume IS dollar volume
df["taker_buy_ratio"] = df["taker_buy_quote_volume"] / df["quote_volume"]
df["returns"]         = df["close"].pct_change()
```

---

## Data Schema

Each parquet file contains completed 1-minute bars with these columns:

| Column | Type | Notes |
|---|---|---|
| `open_time` | `timestamp[ms, UTC]` | Bar open — use for indexing |
| `open` | `float64` | |
| `high` | `float64` | |
| `low` | `float64` | |
| `close` | `float64` | |
| `volume` | `float64` | Base asset volume |
| `close_time` | `timestamp[ms, UTC]` | Bar close (= open_time + 59 999 ms) |
| `quote_volume` | `float64` | **Dollar volume** = Σ(price × qty) |
| `num_trades` | `int64` | Trade count — volatility proxy |
| `taker_buy_base_volume` | `float64` | |
| `taker_buy_quote_volume` | `float64` | Taker buy dollar volume |

**Key derived metrics:**

| Metric | Formula |
|---|---|
| VWAP | `quote_volume / volume` |
| Taker buy ratio (order flow) | `taker_buy_quote_volume / quote_volume` |
| Dollar volume | `quote_volume` (already in quote currency) |
| Realised vol (annualised, 1m bars) | `std(log_returns) × √525600` |

---

## Configuration

Edit `config.py` — changes take effect on the next container start.

```python
# ── Paths (relative to project root inside container) ─────────────────────────
HISTORICAL_DIR = ROOT / "klines" / "historical"
LIVE_DIR       = ROOT / "klines" / "live"

# ── Universe ──────────────────────────────────────────────────────────────────
UNIVERSE_SIZE          = 50     # number of symbols
UNIVERSE_LOOKBACK_DAYS = 20     # rolling window for dollar volume
UNIVERSE_MIN_DAYS      = 5      # consecutive days in top pool before inclusion

# ── Downloader ────────────────────────────────────────────────────────────────
MAX_CONCURRENT_DOWNLOADS = 8    # parallel symbol downloads
RETRY_ATTEMPTS           = 3
RETRY_BACKOFF_S          = 2.0  # seconds, doubles on each retry
VERIFY_CHECKSUMS         = True # sha256 verify every downloaded file

# ── Live stream ───────────────────────────────────────────────────────────────
LIVE_INTRADAY_BUFFER_BARS = 1440   # rolling intraday bar count (24h default)
WS_MAX_RECONNECT_ATTEMPTS = 10
WS_RECONNECT_BACKOFF_S    = 1.0    # doubles each attempt, capped at 60s

# ── Restrict to specific symbols (None = all spot symbols) ────────────────────
SYMBOL_OVERRIDE: list[str] | None = None
```

---

## Cron / Scheduling

Run `main.py update` after UTC midnight each day to collect yesterday's files.
The stream handles the UTC midnight rollover automatically when running continuously.

**Local cron:**
```cron
# 00:05 UTC daily — collect yesterday, exit cleanly
5 0 * * * cd /path/to/binance_data && python main.py update >> logs/cron.log 2>&1
```

**Docker cron (separate container, same volume):**
```yaml
# Add to docker-compose.yml
  binance-update:
    image: binance-data:latest
    command: python main.py update
    volumes:
      - binance-klines:/app/klines
      - binance-logs:/app/logs
    networks:
      - binance-net
    restart: "no"      # exit after update; use host cron or a scheduler to trigger
```

---

## Security

### Network isolation

The HTTP API is designed for **trusted internal Docker networks only**.
Do not expose port 8000 to the public internet.
In production, place an authenticating reverse proxy (nginx, Traefik) in front.

### SQL endpoint (`/query`)

The raw SQL endpoint is **disabled by default** (`QUERY_ENABLED=false`).

**Why it's dangerous:** DuckDB can read arbitrary files on the container
filesystem via functions like `read_csv()`, `read_json()`, and `glob()`.
An attacker with access to the endpoint could exfiltrate environment variables,
credentials mounted as files, or any readable path.

**Mitigations applied when enabled:**

| Mitigation | Details |
|---|---|
| SELECT-only | First keyword must be `SELECT` or `WITH` (CTEs) |
| Keyword denylist | Blocks `COPY`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `DROP`, `ALTER`, `TRUNCATE`, `ATTACH`, `LOAD`, `INSTALL`, `read_csv`, `read_json`, `read_text`, `read_blob`, `glob`, `scandir`, `eval` |
| Comment stripping | SQL comments stripped before validation to prevent bypass |

**Residual risk:** `read_parquet()` with an arbitrary path is not blocked,
because it is the primary use case.  The practical risk is low (non-parquet files
will error; typical container filesystems have no sensitive parquet files outside
`/app/klines/`), but it is not zero.

**Recommendation:** Keep `QUERY_ENABLED=false` (the default).
Use the structured endpoints (`/symbols/{symbol}`, `/cross-section`, etc.) instead.
If you need SQL power, run DuckDB queries in the same process that reads the volume
(Option B above), where there is no network-accessible attack surface.

To enable (trusted environments only):
```bash
QUERY_ENABLED=true docker compose up -d
```

---

## Examples

The `examples/` directory contains runnable scripts demonstrating every major feature.
Run them from the project root after populating the store (`python main.py init`).

| Script | What it shows |
|---|---|
| [`01_basic_load.py`](examples/01_basic_load.py) | Load a single symbol, compute VWAP, daily summaries |
| [`02_live_data.py`](examples/02_live_data.py) | Read live intraday buffer and current in-progress bar |
| [`03_cross_section.py`](examples/03_cross_section.py) | Multi-symbol panel, correlations, volatility |
| [`04_universe.py`](examples/04_universe.py) | Build the dollar-volume universe, inspect turnover |
| [`05_duckdb_queries.py`](examples/05_duckdb_queries.py) | SQL analytics: VWAP, order flow, realised vol |
| [`06_momentum_signal.py`](examples/06_momentum_signal.py) | Cross-sectional momentum signal with position weights |
| [`07_api_client.py`](examples/07_api_client.py) | Call every HTTP API endpoint from Python |

```bash
python examples/01_basic_load.py
python examples/07_api_client.py           # against running Docker container
BASE_URL=http://binance-data:8000 python examples/07_api_client.py  # from another container
```
