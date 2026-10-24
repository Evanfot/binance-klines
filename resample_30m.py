"""
30-minute OHLCV bars resampled from 1m historical parquet files, written to a DuckDB file.

Companion to closes.py/taker.py: daily_closes.parquet only has daily granularity, so
30m-bar research (trend_trader) needs this separate build. Aggregation mirrors closes.py's
_AGG_SQL (arg_min/arg_max open/close, max/min high/low, summed volumes incl. taker buy)
but buckets with DuckDB's time_bucket() instead of truncating to a calendar day. Only
klines/historical/ is read (completed days only), so there's no partial-bar risk from the
live buffer.

Self-contained (no `from config import ...`) so it can be `docker cp`'d into the running
container and executed standalone — code is baked into the image at build time, not
bind-mounted, so this file wouldn't otherwise be visible until the next image
rebuild/redeploy. See trend_trader/scripts/sync_30m.sh.

Schema: open_time, symbol, open, high, low, close, volume, quote_volume, num_trades,
        taker_buy_base_volume, taker_buy_quote_volume, vwap
Output: klines/processed/ohlcv_30m.duckdb (table: binance_30m)

Not part of the daily cron — this is a research artifact, rerun manually after a
bulk download/backfill:
    docker exec binance-data python3 /tmp/resample_30m.py
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

ROOT = Path(os.environ["BINANCE_DATA_ROOT"]) if os.environ.get("BINANCE_DATA_ROOT") else Path(__file__).parent
HISTORICAL_DIR = ROOT / "klines" / "historical"
OUT_PATH = ROOT / "klines" / "processed" / "ohlcv_30m.duckdb"
INTERVAL = "1m"
BUCKET_MINUTES = 30

_AGG_SQL = """
    CREATE OR REPLACE TABLE binance_30m AS
    SELECT
        time_bucket(INTERVAL '{bucket} minutes', open_time) AS open_time,
        regexp_extract(filename, '/([^/]+)/1m/', 1)         AS symbol,
        arg_min(open,  open_time)                           AS open,
        max(high)                                           AS high,
        min(low)                                            AS low,
        arg_max(close, open_time)                           AS close,
        sum(volume)                                         AS volume,
        sum(quote_volume)                                   AS quote_volume,
        sum(num_trades)::BIGINT                             AS num_trades,
        sum(taker_buy_base_volume)                          AS taker_buy_base_volume,
        sum(taker_buy_quote_volume)                         AS taker_buy_quote_volume,
        CASE WHEN sum(volume) > 0
             THEN sum(quote_volume) / sum(volume)
             ELSE NULL END                                  AS vwap
    FROM read_parquet('{glob}', filename=true)
    WHERE year(CAST(open_time AS DATE)) BETWEEN 1900 AND 9999
    GROUP BY 1, 2
    ORDER BY 2, 1
"""


def build(output_path: Path = OUT_PATH, bucket_minutes: int = BUCKET_MINUTES) -> None:
    """Rebuild ohlcv_30m.duckdb from all 1m historical files. Overwrites any existing file."""
    glob = str(HISTORICAL_DIR / "*" / INTERVAL / "*.parquet")
    log.info("building %dm bars from %s", bucket_minutes, glob)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(output_path))
    con.execute("SET TimeZone='UTC'")
    try:
        con.execute(_AGG_SQL.format(bucket=bucket_minutes, glob=glob))
    except duckdb.IOException:
        con.close()
        log.warning("no historical 1m files found at %s — ohlcv_30m not written", glob)
        return

    n_rows, n_symbols = con.execute(
        "SELECT count(*), count(DISTINCT symbol) FROM binance_30m"
    ).fetchone()
    lo, hi = con.execute("SELECT min(open_time), max(open_time) FROM binance_30m").fetchone()
    con.close()

    log.info(
        "30m build complete: %d rows, %d symbols, range %s -> %s",
        n_rows, n_symbols, lo, hi,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    build()
