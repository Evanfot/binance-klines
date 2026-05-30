"""
HTTP API server — wraps the storage layer for inter-container access.

Environment variables:
  BINANCE_MODE        init | stream | api  (default: stream)
  BINANCE_START_DATE  YYYY-MM-DD           (only used with MODE=init)
  API_PORT            integer              (default: 8000)
  QUERY_ENABLED       true | false         (default: false)
                      The /query endpoint executes raw DuckDB SQL and is
                      disabled by default.  Set to "true" only in trusted
                      internal networks — see Security section in README.

Modes:
  init   — download full history, then stream, while serving the API
  stream — stream only (history must already be populated), serve API
  api    — serve API only, no streaming (useful for read-only replicas)
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date, timedelta

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from logger import setup_logging
from storage import DataStore, HistoricalStore, LiveStore, all_symbols_glob

log = setup_logging("api")

BINANCE_MODE = os.getenv("BINANCE_MODE", "stream")
_start_env = os.getenv("BINANCE_START_DATE", "").strip()
BINANCE_START_DATE: date | None = date.fromisoformat(_start_env) if _start_env else None
API_PORT = int(os.getenv("API_PORT", "8000"))
QUERY_ENABLED = os.getenv("QUERY_ENABLED", "false").lower() == "true"


# ── SQL safety ─────────────────────────────────────────────────────────────────
# DuckDB can read arbitrary files (read_csv, read_json, glob, …), load
# extensions, and write via COPY.  We restrict /query to SELECT-only and
# block every known escape hatch.  The endpoint also requires explicit
# opt-in via QUERY_ENABLED=true.

_COMMENT_RE = re.compile(r"--[^\n]*|/\*.*?\*/", re.DOTALL)

# Any of these words appearing in the stripped query is rejected.
_BLOCKED_RE = re.compile(
    r"\b(?:"
    r"copy|insert|update|delete|create|drop|alter|truncate|rename|"
    r"attach|detach|load|install|"
    r"pragma|"
    r"read_csv|read_csv_auto|read_json|read_json_auto|read_text|read_blob|"
    r"glob|scandir|"
    r"execute|eval|call"
    r")\b",
    re.IGNORECASE,
)


def _validate_sql(sql: str) -> None:
    """
    Raise ValueError if *sql* contains operations beyond read-only SELECT.

    Checks performed (in order):
      1. Strip SQL comments so they cannot be used to smuggle keywords.
      2. First keyword must be SELECT or WITH (for CTEs).
      3. No word in the denylist may appear anywhere in the query.
    """
    clean = _COMMENT_RE.sub(" ", sql).strip()
    first = clean.split()[0].upper() if clean.split() else ""
    if first not in ("SELECT", "WITH"):
        raise ValueError(
            f"Only SELECT (or WITH … SELECT) statements are permitted; "
            f"got {first!r}"
        )
    m = _BLOCKED_RE.search(clean)
    if m:
        raise ValueError(f"Disallowed keyword in query: {m.group()!r}")


# ── Serialisation ──────────────────────────────────────────────────────────────

def _df_to_records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df.to_dict(orient="records")


def _series_to_dict(s: pd.Series) -> dict:
    out = s.to_dict()
    for k, v in out.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif hasattr(v, "item"):
            out[k] = v.item()
    return out


# ── Background data task ───────────────────────────────────────────────────────

async def _data_mode_task() -> None:
    if BINANCE_MODE == "init":
        from downloader import run_initial_download
        log.info("=== INITIAL DOWNLOAD (start=%s) ===", BINANCE_START_DATE)
        await run_initial_download(start=BINANCE_START_DATE)

    if BINANCE_MODE in ("init", "stream"):
        import aiohttp
        from stream import ChunkedStreamManager
        from downloader import fetch_all_symbols

        store = HistoricalStore()
        symbols = store.available_symbols()
        if not symbols:
            log.info("historical store empty — fetching symbol list from Binance")
            async with aiohttp.ClientSession() as session:
                symbols = await fetch_all_symbols(session)

        log.info("=== STREAMING %d symbols ===", len(symbols))
        manager = ChunkedStreamManager(symbols)
        await manager.run()


_stream_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _stream_task
    if BINANCE_MODE != "api":
        _stream_task = asyncio.create_task(_data_mode_task())
        log.info("background data task started (mode=%s)", BINANCE_MODE)
    yield
    if _stream_task and not _stream_task.done():
        _stream_task.cancel()
        try:
            await _stream_task
        except asyncio.CancelledError:
            pass


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Binance Data API",
    description="HTTP interface to the Binance historical + live kline store.",
    version="0.1.0",
    lifespan=lifespan,
)


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok", "mode": BINANCE_MODE}


# ── Store status ───────────────────────────────────────────────────────────────

@app.get("/status", tags=["meta"])
def status():
    store = HistoricalStore()
    symbols = store.available_symbols()
    today = date.today()
    yesterday = today - timedelta(days=1)

    behind: list[dict] = []
    total_files = 0
    for sym in symbols:
        r = store.date_range(sym)
        if r:
            start, end = r
            total_files += (end - start).days + 1
            if end < yesterday:
                behind.append({"symbol": sym, "last_date": end.isoformat()})

    return {
        "date": today.isoformat(),
        "mode": BINANCE_MODE,
        "symbols": len(symbols),
        "total_files": total_files,
        "missing_yesterday": len(behind),
        "behind": behind[:20],
    }


# ── Symbols ────────────────────────────────────────────────────────────────────

@app.get("/symbols", tags=["historical"])
def list_symbols():
    return {"symbols": HistoricalStore().available_symbols()}


@app.get("/symbols/{symbol}/range", tags=["historical"])
def symbol_range(symbol: str):
    r = HistoricalStore().date_range(symbol)
    if r is None:
        raise HTTPException(404, f"symbol {symbol!r} not found in historical store")
    return {"symbol": symbol, "start": r[0].isoformat(), "end": r[1].isoformat()}


@app.get("/symbols/{symbol}", tags=["historical"])
def load_symbol(
    symbol: str,
    lookback_days: int | None = Query(None, description="Load last N days"),
    start: str | None = Query(None, description="Start date YYYY-MM-DD (inclusive)"),
    end: str | None = Query(None, description="End date YYYY-MM-DD (exclusive)"),
    include_live: bool = Query(True, description="Append today's live data"),
):
    ds = DataStore()
    if lookback_days is not None:
        df = ds.load(symbol, lookback_days=lookback_days, include_live=include_live)
    else:
        _start = date.fromisoformat(start) if start else None
        _end = date.fromisoformat(end) if end else None
        df = ds.historical.load_symbol(symbol, start=_start, end=_end)
        if include_live:
            live = ds.live.read_all(symbol)
            if not live.empty:
                df = (
                    pd.concat([df, live], ignore_index=True)
                    .drop_duplicates(subset=["open_time"])
                    .sort_values("open_time")
                    .reset_index(drop=True)
                )
    return {"symbol": symbol, "rows": len(df), "data": _df_to_records(df)}


# ── Cross-section ──────────────────────────────────────────────────────────────

@app.get("/cross-section", tags=["historical"])
def load_cross_section(
    symbols: str = Query(..., description="Comma-separated list of symbols"),
    lookback_days: int = Query(20),
    include_live: bool = Query(True),
):
    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    df = DataStore().load_cross_section(sym_list, lookback_days=lookback_days, include_live=include_live)
    return {"symbols": sym_list, "rows": len(df), "data": _df_to_records(df)}


# ── Live data ──────────────────────────────────────────────────────────────────

@app.get("/live/{symbol}", tags=["live"])
def live_symbol(symbol: str):
    df = LiveStore().read_all(symbol)
    return {"symbol": symbol, "rows": len(df), "data": _df_to_records(df)}


@app.get("/live/{symbol}/current", tags=["live"])
def live_current_bar(symbol: str):
    bar = LiveStore().read_current_bar(symbol)
    if bar is None:
        raise HTTPException(404, f"no current bar for {symbol!r}")
    return {"symbol": symbol, "bar": _series_to_dict(bar)}


# ── Universe ───────────────────────────────────────────────────────────────────

@app.get("/universe", tags=["universe"])
def get_universe(
    size: int = Query(50, description="Number of symbols"),
    lookback_days: int = Query(20),
):
    from universe import build_universe
    syms = build_universe(size=size, lookback_days=lookback_days)
    return {"symbols": syms, "count": len(syms)}


# ── DuckDB passthrough ─────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    sql: str


@app.post("/query", tags=["query"])
def run_query(req: QueryRequest):
    """
    Run a read-only DuckDB SELECT over the historical parquet store.

    **Disabled by default** — requires `QUERY_ENABLED=true` in environment.

    Restrictions enforced server-side:
    - Statement must be SELECT or WITH … SELECT (CTEs allowed)
    - Blocked: COPY, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, ATTACH,
      LOAD, INSTALL, read_csv, read_json, read_text, glob, scandir, eval

    Example SQL referencing all symbols:
    ```sql
    SELECT open_time, close, quote_volume
    FROM read_parquet('/app/klines/historical/BTCUSDT/1m/*.parquet')
    WHERE open_time >= '2024-01-01'
    ORDER BY open_time
    ```
    """
    if not QUERY_ENABLED:
        raise HTTPException(
            403,
            "SQL query endpoint is disabled. Set QUERY_ENABLED=true to enable it "
            "(only do so on trusted internal networks — see README Security section).",
        )
    try:
        _validate_sql(req.sql)
    except ValueError as exc:
        raise HTTPException(400, f"Query rejected: {exc}")
    try:
        df = HistoricalStore().query(req.sql)
        return {"rows": len(df), "data": _df_to_records(df)}
    except Exception as exc:
        raise HTTPException(400, str(exc))


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=API_PORT, log_level="info")
