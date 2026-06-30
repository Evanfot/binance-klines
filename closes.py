"""
Daily OHLCV closes derived from 1m historical parquet files.

Schema: date, symbol, open, high, low, close, volume, quote_volume, num_trades, vwap
Output: klines/daily_closes.parquet  (single file, sorted by date then symbol)

Entry points
------------
build()              — full rebuild from scratch (slow; run once)
update()             — append / refresh one day from historical (official; provisional=False)
update_provisional() — upsert today's close from the live 1m buffer before the official
                       daily file is published (provisional=True; later overwritten by update())
load_closes()        — load the wide pivot matrix (dates × symbols)
"""

from __future__ import annotations

import logging
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import HISTORICAL_DIR, INTERVAL, LIVE_DIR, ROOT

log = logging.getLogger(__name__)

CLOSES_PATH = ROOT / "klines" / "processed" / "daily_closes.parquet"

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
    # True for a same-day close synthesised from the live 1m buffer (update_provisional);
    # False once the official daily file from Binance lands (build / update).
    pa.field("provisional",  pa.bool_()),
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


# Re-check this many trailing days on every update. Binance publishes daily 1m
# archives with a 1-2 day lag, so a day's file often isn't present when update()
# first runs for it. Re-checking a window means a lagged day gets filled once it
# lands, instead of being tried once and skipped forever (which silently froze
# daily_closes for ~3 weeks).
BACKFILL_DAYS = 14


def update(
    yesterday: date | None = None,
    output_path: Path = CLOSES_PATH,
    backfill_days: int = BACKFILL_DAYS,
) -> None:
    """
    Refresh daily closes for the trailing ``backfill_days`` window up to ``yesterday``.

    Aggregates every day in the window that has 1m files and isn't already final in
    the file (plus ``yesterday`` itself), then upserts. Falls back to a full build if
    the file doesn't exist. Idempotent. Days without 1m files yet are logged and
    retried on the next run rather than skipped permanently.
    """
    if yesterday is None:
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)

    if not output_path.exists():
        log.info("closes file missing — running full build")
        build(output_path)
        return

    existing = pq.read_table(output_path).to_pandas()
    existing["date"] = pd.to_datetime(existing["date"]).dt.date
    if "provisional" not in existing.columns:
        existing["provisional"] = False
    existing["provisional"] = existing["provisional"].fillna(False).astype(bool)

    # Days we already have a FINAL (non-provisional) close for can be left alone;
    # provisional days are eligible for upgrade to the official bar.
    have_final = set(existing.loc[~existing["provisional"], "date"])
    window = {yesterday - timedelta(days=i) for i in range(max(1, backfill_days))}
    targets = sorted((window - have_final) | {yesterday})

    frames = []
    for d in targets:
        glob = str(HISTORICAL_DIR / "*" / INTERVAL / f"{d.isoformat()}.parquet")
        df_d = _aggregate_glob(glob)
        if df_d.empty:
            log.info("closes: no 1m files for %s yet — will retry next run", d)
        else:
            frames.append(df_d)

    if not frames:
        log.warning("closes: no 1m files for any of %d target day(s) up to %s",
                    len(targets), yesterday)
        return

    new_df = pd.concat(frames, ignore_index=True)
    new_dates = set(new_df["date"])
    existing = existing[~existing["date"].isin(new_dates)]  # official replaces provisional
    combined = (
        pd.concat([existing, new_df], ignore_index=True)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )
    _write(combined, output_path)
    log.info("closes updated: +%d rows across %d day(s), latest %s",
             len(new_df), len(new_dates), max(new_dates))


def update_provisional(
    day: date | None = None,
    output_path: Path = CLOSES_PATH,
    max_staleness_min: float = 10.0,
) -> bool:
    """Upsert a *provisional* daily close for `day` from the live 1m buffer.

    The daily close is ``arg_max(close, open_time)`` over the day's 1m bars, which the
    live buffer (``klines/live/<symbol>/{intraday,current_bar}.parquet``) holds until
    the UTC-midnight clear. Run shortly before midnight to get a near-final close
    without waiting for Binance to publish the official daily file. The row is flagged
    ``provisional=True`` and is later overwritten by :func:`update` (``provisional=False``).

    Skips writing (returns ``False``) if there is no live data, the freshest bar is
    older than ``max_staleness_min`` (a dead/stalled stream), or an official close for
    ``day`` already exists — so consumers block rather than trade on stale prices.
    Returns ``True`` on a successful write.
    """
    if day is None:
        day = datetime.now(timezone.utc).date()

    glob = str(LIVE_DIR / "*" / "*.parquet")
    con = None
    try:
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'")
        df = con.execute(_AGG_SQL_LIVE.format(glob=glob, day=day.isoformat())).df()
    except duckdb.IOException:
        log.warning("provisional %s: no live buffer files at %s — not written", day, glob)
        return False
    finally:
        if con is not None:
            con.close()

    df = df[df["symbol"].astype(bool)]  # drop rows whose symbol failed to parse
    if df.empty:
        log.warning("provisional %s: no live 1m bars for the day — not written", day)
        return False

    last_bar = pd.to_datetime(df["last_bar"], utc=True).max()
    age_min = (datetime.now(timezone.utc) - last_bar.to_pydatetime()).total_seconds() / 60
    if age_min > max_staleness_min:
        log.warning(
            "provisional %s: freshest live bar is %.1f min old (> %.0f) — stream stale, not written",
            day, age_min, max_staleness_min,
        )
        return False

    df = df.drop(columns=["last_bar"])
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["provisional"] = True

    if not output_path.exists():
        log.warning("provisional %s: %s missing — run build() first; skipping", day, output_path)
        return False

    existing = pq.read_table(output_path).to_pandas()
    if "provisional" not in existing.columns:
        existing["provisional"] = False
    existing["provisional"] = existing["provisional"].fillna(False).astype(bool)
    existing["date"] = pd.to_datetime(existing["date"]).dt.date

    # Never clobber an already-final (official) row for this day.
    if not existing[(existing["date"] == day) & (~existing["provisional"])].empty:
        log.info("provisional %s: official close already present — skipping", day)
        return False

    existing = existing[existing["date"] != day]
    combined = (
        pd.concat([existing, df], ignore_index=True)
        .sort_values(["date", "symbol"])
        .reset_index(drop=True)
    )
    _write(combined, output_path)
    log.info(
        "provisional closes written: %d symbols for %s (freshest bar %.1f min old)",
        len(df), day, age_min,
    )
    return True


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
        CAST(open_time AS DATE)                          AS date,
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
    WHERE year(CAST(open_time AS DATE)) BETWEEN 1900 AND 9999
    GROUP BY date, symbol
    ORDER BY date, symbol
"""

# Same aggregation, but over the live buffer (klines/live/<symbol>/{intraday,current_bar}.parquet).
# Symbol comes from the path segment after 'live/'; restricted to one day; `last_bar` lets the
# caller reject a stalled stream. union_by_name tolerates the two live files having identical cols.
_AGG_SQL_LIVE = """
    SELECT
        CAST(open_time AS DATE)                          AS date,
        regexp_extract(filename, 'live/([^/]+)/', 1)     AS symbol,
        arg_min(open,  open_time)                        AS open,
        max(high)                                        AS high,
        min(low)                                         AS low,
        arg_max(close, open_time)                        AS close,
        sum(volume)                                      AS volume,
        sum(quote_volume)                                AS quote_volume,
        sum(num_trades)::BIGINT                          AS num_trades,
        CASE WHEN sum(volume) > 0
             THEN sum(quote_volume) / sum(volume)
             ELSE NULL END                               AS vwap,
        max(open_time)                                   AS last_bar
    FROM read_parquet('{glob}', filename=true, union_by_name=true)
    WHERE CAST(open_time AS DATE) = DATE '{day}'
    GROUP BY date, symbol
    ORDER BY date, symbol
"""


def _aggregate_glob(glob: str) -> pd.DataFrame:
    """Run the aggregation SQL over a parquet glob. Returns empty DF if no files match."""
    try:
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'")
        df = con.execute(_AGG_SQL.format(glob=glob)).df()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df
    except duckdb.IOException:
        return pd.DataFrame(columns=[f.name for f in CLOSES_SCHEMA])


def _write(df: pd.DataFrame, path: Path) -> None:
    """Atomic write: temp file then rename."""
    df = df.copy()
    if "provisional" not in df.columns:
        df["provisional"] = False
    # Official rows (build/update) and rows from pre-flag files have no/NaN flag → final.
    df["provisional"] = df["provisional"].fillna(False).astype(bool)
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
