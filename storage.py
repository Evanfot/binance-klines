"""
Storage layer — the ONLY place that reads/writes parquet files.

Rules enforced here:
  - historical/ is immutable once written (atomic write + checksum verified)
  - live/ is owned exclusively by the stream; downloader never touches it
  - All writes are atomic (temp file → rename)
  - DuckDB is used for cross-sectional queries across symbols
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import HISTORICAL_DIR, LIVE_DIR, INTERVAL, LIVE_INTRADAY_BUFFER_BARS

log = logging.getLogger(__name__)

# ── Schema ─────────────────────────────────────────────────────────────────────
# Matches Binance 1m kline columns exactly.
KLINE_SCHEMA = pa.schema([
    pa.field("open_time",               pa.timestamp("ms", tz="UTC")),
    pa.field("open",                    pa.float64()),
    pa.field("high",                    pa.float64()),
    pa.field("low",                     pa.float64()),
    pa.field("close",                   pa.float64()),
    pa.field("volume",                  pa.float64()),
    pa.field("close_time",              pa.timestamp("ms", tz="UTC")),
    pa.field("quote_volume",            pa.float64()),   # = sum(price * qty) → use for VWAP
    pa.field("num_trades",              pa.int64()),
    pa.field("taker_buy_base_volume",   pa.float64()),
    pa.field("taker_buy_quote_volume",  pa.float64()),
])

KLINE_COLUMNS = [f.name for f in KLINE_SCHEMA]


# ── Path helpers ───────────────────────────────────────────────────────────────

def hist_path(symbol: str, dt: date) -> Path:
    """Path for a completed daily historical file."""
    return HISTORICAL_DIR / symbol / INTERVAL / f"{dt.isoformat()}.parquet"


def live_intraday_path(symbol: str) -> Path:
    """Path for a symbol's rolling intraday buffer (stream-owned)."""
    return LIVE_DIR / symbol / "intraday.parquet"


def live_current_bar_path(symbol: str) -> Path:
    """Path for the single in-progress (incomplete) bar."""
    return LIVE_DIR / symbol / "current_bar.parquet"


# ── Atomic write helper ────────────────────────────────────────────────────────

def _atomic_write(table: pa.Table, dest: Path) -> None:
    """Write parquet to a temp file then rename — prevents partial files."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=dest.parent, suffix=".tmp", delete=False
    ) as tf:
        tmp = Path(tf.name)
    try:
        pq.write_table(table, tmp, compression="zstd")
        tmp.rename(dest)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


# ── Historical store ───────────────────────────────────────────────────────────

class HistoricalStore:
    """
    Manages completed daily parquet files under klines/historical/.

    The downloader is the only writer.  The strategy layer reads via DuckDB.
    Files are immutable once written and checksum-verified.
    """

    def exists(self, symbol: str, dt: date) -> bool:
        return hist_path(symbol, dt).exists()

    def write_day(
        self,
        symbol: str,
        dt: date,
        df: pd.DataFrame,
        expected_checksum: str | None = None,
    ) -> None:
        """
        Write one day's klines atomically.

        Parameters
        ----------
        expected_checksum:
            sha256 hex digest of the *source* zip bytes (from Binance .CHECKSUM).
            Stored as parquet metadata for later verification.
        """
        if dt >= date.today():
            raise ValueError(
                f"Refusing to write {dt} to historical store — "
                "today's data belongs to the live stream."
            )

        table = _df_to_table(df)

        # Embed checksum in metadata if provided
        if expected_checksum:
            existing_meta = table.schema.metadata or {}
            existing_meta[b"source_checksum"] = expected_checksum.encode()
            table = table.replace_schema_metadata(existing_meta)

        dest = hist_path(symbol, dt)
        _atomic_write(table, dest)
        log.debug("historical write  %s  %s", symbol, dt)

    def missing_dates(self, symbol: str, start: date, end: date) -> list[date]:
        """Return dates in [start, end) that are missing from the store."""
        missing = []
        d = start
        while d < end:
            if not self.exists(symbol, d):
                missing.append(d)
            d = _next_day(d)
        return missing

    def available_symbols(self) -> list[str]:
        """All symbols that have at least one file in the historical store."""
        return sorted(p.name for p in HISTORICAL_DIR.iterdir() if p.is_dir())

    def date_range(self, symbol: str) -> tuple[date, date] | None:
        """First and last available date for a symbol, or None."""
        sym_dir = HISTORICAL_DIR / symbol / INTERVAL
        if not sym_dir.exists():
            return None
        dates = sorted(
            date.fromisoformat(p.stem)
            for p in sym_dir.glob("*.parquet")
        )
        if not dates:
            return None
        return dates[0], dates[-1]

    # ── DuckDB query interface ─────────────────────────────────────────────────

    def query(self, sql: str) -> pd.DataFrame:
        """
        Run arbitrary SQL over the historical store.

        Use the `read_parquet()` glob patterns in your SQL.  The helper
        `hist_glob(symbol)` and `all_symbols_glob()` below generate them.

        Example
        -------
        df = store.query(f'''
            SELECT symbol, open_time, close, quote_volume / volume AS vwap
            FROM read_parquet('{all_symbols_glob()}', hive_partitioning=false)
            WHERE open_time >= '2024-01-01'
        ''')
        """
        con = duckdb.connect()
        return con.execute(sql).df()

    def load_symbol(
        self,
        symbol: str,
        start: date | None = None,
        end: date | None = None,
    ) -> pd.DataFrame:
        """Load one symbol's history into a DataFrame."""
        sym_dir = HISTORICAL_DIR / symbol / INTERVAL
        if not sym_dir.exists():
            return pd.DataFrame(columns=KLINE_COLUMNS)

        files = sorted(sym_dir.glob("*.parquet"))
        if start:
            files = [f for f in files if date.fromisoformat(f.stem) >= start]
        if end:
            files = [f for f in files if date.fromisoformat(f.stem) < end]
        if not files:
            return pd.DataFrame(columns=KLINE_COLUMNS)

        return pd.concat(
            [pq.read_table(f).to_pandas() for f in files],
            ignore_index=True,
        )

    def load_cross_section(
        self,
        symbols: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """
        Load multiple symbols over a date range.
        Returns a DataFrame with an added 'symbol' column.
        """
        frames = []
        for sym in symbols:
            df = self.load_symbol(sym, start=start, end=end)
            if not df.empty:
                df["symbol"] = sym
                frames.append(df)
        if not frames:
            return pd.DataFrame(columns=[*KLINE_COLUMNS, "symbol"])
        return (
            pd.concat(frames, ignore_index=True)
            .sort_values("open_time")
            .reset_index(drop=True)
        )


# ── Live store ─────────────────────────────────────────────────────────────────

class LiveStore:
    """
    Manages in-progress data under klines/live/.

    Two files per symbol:
      intraday.parquet  — rolling buffer of completed bars since UTC midnight
      current_bar.parquet — the single incomplete bar being built

    Only the stream writer should call write_* methods.
    Readers (strategy) use read_* methods.
    """

    def write_completed_bar(self, symbol: str, bar: dict) -> None:
        """Append a completed 1m bar to the intraday buffer."""
        new_row = pd.DataFrame([bar])
        path = live_intraday_path(symbol)
        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists():
            existing = pq.read_table(path).to_pandas()
            combined = pd.concat([existing, new_row], ignore_index=True)
            # Keep rolling buffer
            if len(combined) > LIVE_INTRADAY_BUFFER_BARS:
                combined = combined.iloc[-LIVE_INTRADAY_BUFFER_BARS:]
        else:
            combined = new_row

        _atomic_write(_df_to_table(combined), path)

    def write_current_bar(self, symbol: str, bar: dict) -> None:
        """Overwrite the current incomplete bar."""
        path = live_current_bar_path(symbol)
        path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write(_df_to_table(pd.DataFrame([bar])), path)

    def clear_intraday(self, symbol: str) -> None:
        """Called at UTC midnight when historical takes over yesterday."""
        path = live_intraday_path(symbol)
        if path.exists():
            path.unlink()
            log.debug("cleared intraday buffer  %s", symbol)

    def read_intraday(self, symbol: str) -> pd.DataFrame:
        path = live_intraday_path(symbol)
        if not path.exists():
            return pd.DataFrame(columns=KLINE_COLUMNS)
        return pq.read_table(path).to_pandas()

    def read_current_bar(self, symbol: str) -> pd.Series | None:
        path = live_current_bar_path(symbol)
        if not path.exists():
            return None
        df = pq.read_table(path).to_pandas()
        return df.iloc[0] if len(df) else None

    def read_all(self, symbol: str) -> pd.DataFrame:
        """Intraday completed bars + current bar combined."""
        intraday = self.read_intraday(symbol)
        current = self.read_current_bar(symbol)
        if current is not None:
            intraday = pd.concat(
                [intraday, pd.DataFrame([current])], ignore_index=True
            )
        return intraday


# ── Unified reader (strategy-facing) ──────────────────────────────────────────

class DataStore:
    """
    The interface the strategy layer should use.

    Seamlessly stitches historical + live data with no overlap,
    and enforces the boundary rule: historical ends at yesterday's close,
    live starts from today's UTC midnight.
    """

    def __init__(self):
        self.historical = HistoricalStore()
        self.live = LiveStore()

    def load(
        self,
        symbol: str,
        lookback_days: int | None = None,
        include_live: bool = True,
    ) -> pd.DataFrame:
        """
        Load a symbol's full history (optionally limited to last N days)
        with live data appended if requested.
        """
        today = date.today()

        if lookback_days:
            start = _offset_date(today, -lookback_days)
        else:
            r = self.historical.date_range(symbol)
            start = r[0] if r else today

        hist = self.historical.load_symbol(symbol, start=start, end=today)

        if include_live:
            live = self.live.read_all(symbol)
            if not live.empty:
                hist = pd.concat([hist, live], ignore_index=True)
                hist["open_time"] = pd.to_datetime(hist["open_time"], utc=True)
                hist = hist.drop_duplicates(subset=["open_time"]).sort_values("open_time")

        return hist

    def load_cross_section(
        self,
        symbols: list[str],
        lookback_days: int,
        include_live: bool = True,
    ) -> pd.DataFrame:
        today = date.today()
        start = _offset_date(today, -lookback_days)
        hist = self.historical.load_cross_section(symbols, start, today)

        if include_live:
            live_frames = []
            for sym in symbols:
                lf = self.live.read_all(sym)
                if not lf.empty:
                    lf["symbol"] = sym
                    live_frames.append(lf)
            if live_frames:
                live_all = pd.concat(live_frames, ignore_index=True)
                hist = pd.concat([hist, live_all], ignore_index=True)
                hist["open_time"] = pd.to_datetime(hist["open_time"], utc=True)
                hist = (
                    hist.drop_duplicates(subset=["symbol", "open_time"])
                    .sort_values(["symbol", "open_time"])
                )

        return hist


# ── Helpers ────────────────────────────────────────────────────────────────────

def _df_to_table(df: pd.DataFrame) -> pa.Table:
    """Convert a DataFrame to a PyArrow table, coercing types to schema."""
    for col in KLINE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[KLINE_COLUMNS]
    return pa.Table.from_pandas(df, schema=KLINE_SCHEMA, safe=False)


def _next_day(d: date) -> date:
    from datetime import timedelta
    return d + timedelta(days=1)


def _offset_date(d: date, days: int) -> date:
    from datetime import timedelta
    return d + timedelta(days=days)



def all_symbols_glob() -> str:
    return str(HISTORICAL_DIR / "*" / INTERVAL / "*.parquet")


def hist_glob(symbol: str) -> str:
    return str(HISTORICAL_DIR / symbol / INTERVAL / "*.parquet")
