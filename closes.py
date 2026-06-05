"""
Daily OHLCV closes derived from 1m historical parquet files.

Schema: date, symbol, open, high, low, close, volume, quote_volume, num_trades, vwap
Output: klines/daily_closes.parquet  (single file, sorted by date then symbol)

Entry points
------------
build()        — full rebuild from scratch (slow; run once)
update()       — append / refresh one day (fast; run daily after the 1m download)
load_closes()  — load the wide pivot matrix (dates × symbols)
"""

from __future__ import annotations

import logging
import tempfile
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import HISTORICAL_DIR, INTERVAL, ROOT

log = logging.getLogger(__name__)

CLOSES_PATH = ROOT / "klines" / "daily_closes.parquet"

CLOSES_SCHEMA = pa.schema([
    pa.field("date",         pa.date32()),
    pa.field("symbol",       pa.string()),
    pa.field("open",         pa.float64()),
    pa.field("high",         pa.float64()),
    pa.field("low",          pa.float64()),
    pa.field("close",        pa.float64()),
    pa.field("volume",       pa.float64()),
    pa.field("quote_volume", pa.float64()),
    pa.field("num_trades",   pa.int64()),
    pa.field("vwap",         pa.float64()),
])


# ── Public API ─────────────────────────────────────────────────────────────────

def build(output_path: Path = CLOSES_PATH) -> None:
    """
    Rebuild daily_closes.parquet from all 1m historical files.
    Overwrites any existing file.
    """
    glob = str(HISTORICAL_DIR / "*" / INTERVAL / "*.parquet")
    log.info("building daily closes from %s", glob)

    df = _aggregate_glob(glob)
    if df.empty:
        log.warning("no historical 1m files found — closes file not written")
        return

    _write(df, output_path)
    log.info(
        "closes build complete: %d rows, %d symbols, %d days",
        len(df), df["symbol"].nunique(), df["date"].nunique(),
    )


def update(yesterday: date | None = None, output_path: Path = CLOSES_PATH) -> None:
    """
    Append one day's closes to daily_closes.parquet.

    Falls back to a full build if the file does not exist yet.
    Idempotent — safe to run multiple times for the same day.
    """
    if yesterday is None:
        yesterday = date.today() - timedelta(days=1)

    if not output_path.exists():
        log.info("closes file missing — running full build")
        build(output_path)
        return

    glob = str(HISTORICAL_DIR / "*" / INTERVAL / f"{yesterday.isoformat()}.parquet")
    log.info("updating daily closes for %s", yesterday)

    new_df = _aggregate_glob(glob)
    if new_df.empty:
        log.warning("no 1m files found for %s — closes not updated", yesterday)
        return

    existing = pq.read_table(output_path).to_pandas()
    existing["date"] = pd.to_datetime(existing["date"]).dt.date

    # Drop any existing rows for this date (makes the operation idempotent)
    existing = existing[existing["date"] != yesterday]

    combined = (
        pd.concat([existing, new_df], ignore_index=True)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )
    _write(combined, output_path)
    log.info("closes updated: +%d rows for %s", len(new_df), yesterday)


def load_closes(
    column: str = "close",
    start: date | None = None,
    end: date | None = None,
    output_path: Path = CLOSES_PATH,
) -> pd.DataFrame:
    """
    Return a wide pivot matrix: index=date, columns=symbol, values=`column`.

    Parameters
    ----------
    column : one of open, high, low, close, volume, quote_volume, num_trades, vwap
    start  : first date to include (inclusive)
    end    : last date to include (inclusive)
    """
    df = pq.read_table(output_path).to_pandas()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    if start:
        df = df[df["date"] >= start]
    if end:
        df = df[df["date"] <= end]

    return df.pivot(index="date", columns="symbol", values=column)


# ── Internal helpers ───────────────────────────────────────────────────────────

_AGG_SQL = """
    SELECT
        CAST(open_time AT TIME ZONE 'UTC' AS DATE)       AS date,
        regexp_extract(filename, '/([^/]+)/1m/', 1)      AS symbol,
        arg_min(open,  open_time)                        AS open,
        max(high)                                        AS high,
        min(low)                                         AS low,
        arg_max(close, open_time)                        AS close,
        sum(volume)                                      AS volume,
        sum(quote_volume)                                AS quote_volume,
        sum(num_trades)::BIGINT                          AS num_trades,
        CASE WHEN sum(volume) > 0
             THEN sum(quote_volume) / sum(volume)
             ELSE NULL END                               AS vwap
    FROM read_parquet('{glob}', filename=true)
    GROUP BY date, symbol
    ORDER BY date, symbol
"""


def _aggregate_glob(glob: str) -> pd.DataFrame:
    """Run the aggregation SQL over a parquet glob. Returns empty DF if no files match."""
    try:
        con = duckdb.connect()
        df = con.execute(_AGG_SQL.format(glob=glob)).df()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df
    except duckdb.IOException:
        return pd.DataFrame(columns=[f.name for f in CLOSES_SCHEMA])


def _write(df: pd.DataFrame, path: Path) -> None:
    """Atomic write: temp file then rename."""
    table = pa.Table.from_pandas(df, schema=CLOSES_SCHEMA, safe=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as tf:
        tmp = Path(tf.name)
    try:
        pq.write_table(table, tmp, compression="zstd")
        tmp.rename(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
