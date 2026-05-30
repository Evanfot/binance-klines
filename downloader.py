"""
Historical batch downloader.

Responsibilities:
  - Discover all tradeable spot symbols from Binance exchange info
  - Download missing daily 1m kline files from data.binance.vision
  - Verify checksums
  - Write atomically to the historical store
  - Never touch today's date (live stream owns it)
  - Safe to re-run at any time (idempotent)
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import zipfile
from datetime import date, timedelta
from typing import AsyncIterator

import aiohttp
import pandas as pd

from config import (
    BINANCE_BASE_URL,
    BINANCE_DATA_PREFIX,
    BINANCE_REST_URL,
    INTERVAL,
    MAX_CONCURRENT_DOWNLOADS,
    RETRY_ATTEMPTS,
    RETRY_BACKOFF_S,
    SYMBOL_OVERRIDE,
    VERIFY_CHECKSUMS,
)
from storage import KLINE_COLUMNS, HistoricalStore

log = logging.getLogger(__name__)


# ── Symbol discovery ───────────────────────────────────────────────────────────

async def fetch_all_symbols(session: aiohttp.ClientSession) -> list[str]:
    """
    Return all active USDT spot symbols from Binance exchange info.
    Filters to USDT quote asset and TRADING status only.
    """
    if SYMBOL_OVERRIDE:
        log.info("symbol override active: %d symbols", len(SYMBOL_OVERRIDE))
        return SYMBOL_OVERRIDE

    url = f"{BINANCE_REST_URL}/api/v3/exchangeInfo"
    async with session.get(url) as resp:
        resp.raise_for_status()
        data = await resp.json()

    symbols = [
        s["symbol"]
        for s in data["symbols"]
        if s["quoteAsset"] == "USDT"
        and s["status"] == "TRADING"
        and s["isSpotTradingAllowed"]
    ]
    log.info("discovered %d USDT spot symbols", len(symbols))
    return sorted(symbols)


# ── Date range helpers ─────────────────────────────────────────────────────────

def _binance_launch_date() -> date:
    """Earliest date Binance has kline data."""
    return date(2017, 8, 17)


def _yesterday() -> date:
    return date.today() - timedelta(days=1)


def _date_range(start: date, end: date) -> list[date]:
    """Inclusive start, exclusive end."""
    out = []
    d = start
    while d < end:
        out.append(d)
        d += timedelta(days=1)
    return out


# ── Download helpers ───────────────────────────────────────────────────────────

def _zip_url(symbol: str, dt: date) -> str:
    fname = f"{symbol}-{INTERVAL}-{dt.isoformat()}.zip"
    return f"{BINANCE_BASE_URL}/{BINANCE_DATA_PREFIX}/{symbol}/{INTERVAL}/{fname}"


def _checksum_url(symbol: str, dt: date) -> str:
    return _zip_url(symbol, dt) + ".CHECKSUM"


async def _fetch_bytes(
    session: aiohttp.ClientSession, url: str
) -> bytes | None:
    """Fetch URL with retries. Returns None if 404."""
    backoff = RETRY_BACKOFF_S
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 404:
                    return None
                resp.raise_for_status()
                return await resp.read()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == RETRY_ATTEMPTS:
                raise
            log.warning("attempt %d failed for %s: %s — retrying in %.1fs",
                        attempt, url, exc, backoff)
            await asyncio.sleep(backoff)
            backoff *= 2
    return None  # unreachable


def _verify_checksum(data: bytes, checksum_text: str) -> bool:
    """Verify sha256 of zip bytes against Binance .CHECKSUM file content."""
    expected = checksum_text.strip().split()[0].lower()
    actual = hashlib.sha256(data).hexdigest().lower()
    return actual == expected


def _parse_csv(zip_bytes: bytes) -> pd.DataFrame:
    """Extract and parse the CSV from a Binance kline zip."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            df = pd.read_csv(f, header=None)

    # Binance kline CSVs have 12 columns; drop the 12th (always 0)
    df = df.iloc[:, :11]
    df.columns = KLINE_COLUMNS  # type: ignore[assignment]

    # Convert timestamps to datetime
    df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms", utc=True).dt.as_unit("ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.as_unit("ms")

    # Numeric coercion
    float_cols = ["open", "high", "low", "close", "volume", "quote_volume",
                  "taker_buy_base_volume", "taker_buy_quote_volume"]
    df[float_cols] = df[float_cols].apply(pd.to_numeric, errors="coerce")
    df["num_trades"] = pd.to_numeric(df["num_trades"], errors="coerce").astype("Int64")

    return df


# ── Per-symbol downloader ──────────────────────────────────────────────────────

async def download_symbol(
    session: aiohttp.ClientSession,
    store: HistoricalStore,
    symbol: str,
    start: date | None = None,
) -> dict:
    """
    Download all missing days for one symbol.
    Returns a summary dict with counts of downloaded / skipped / failed dates.
    """
    start = start or _binance_launch_date()
    end   = date.today()   # exclusive — never download today

    missing = store.missing_dates(symbol, start, end)

    if not missing:
        log.debug("%s  up to date", symbol)
        return {"symbol": symbol, "downloaded": 0, "skipped": 0, "failed": 0}

    log.info("%s  %d missing dates", symbol, len(missing))

    downloaded = skipped = failed = 0

    for dt in missing:
        zip_url  = _zip_url(symbol, dt)
        chk_url  = _checksum_url(symbol, dt)

        try:
            zip_bytes = await _fetch_bytes(session, zip_url)
            if zip_bytes is None:
                # File doesn't exist on Binance (symbol wasn't listed yet)
                skipped += 1
                continue

            checksum_text = None
            if VERIFY_CHECKSUMS:
                chk_bytes = await _fetch_bytes(session, chk_url)
                if chk_bytes:
                    checksum_text = chk_bytes.decode()
                    if not _verify_checksum(zip_bytes, checksum_text):
                        log.error("checksum mismatch  %s  %s — skipping", symbol, dt)
                        failed += 1
                        continue

            df = _parse_csv(zip_bytes)
            expected_sha = checksum_text.strip().split()[0] if checksum_text else None
            store.write_day(symbol, dt, df, expected_checksum=expected_sha)
            downloaded += 1

        except Exception as exc:
            log.error("failed  %s  %s: %s", symbol, dt, exc)
            failed += 1

    return {"symbol": symbol, "downloaded": downloaded,
            "skipped": skipped, "failed": failed}


# ── Orchestrator ───────────────────────────────────────────────────────────────

async def run_initial_download(start: date | None = None) -> None:
    """
    Download the entire historical universe.

    Safe to run repeatedly — already-downloaded files are skipped.
    Set start to limit how far back to go (default: Binance launch).
    """
    store = HistoricalStore()
    sem   = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    async def bounded(session, symbol):
        async with sem:
            return await download_symbol(session, store, symbol, start=start)

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_DOWNLOADS * 2)
    async with aiohttp.ClientSession(connector=connector) as session:
        symbols = await fetch_all_symbols(session)
        log.info("starting download for %d symbols", len(symbols))

        tasks = [bounded(session, sym) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Summary
    total_dl = total_skip = total_fail = 0
    for r in results:
        if isinstance(r, Exception):
            log.error("task exception: %s", r)
            continue
        total_dl   += r["downloaded"]
        total_skip += r["skipped"]
        total_fail += r["failed"]

    log.info(
        "download complete — downloaded: %d  skipped: %d  failed: %d",
        total_dl, total_skip, total_fail,
    )


async def run_daily_update() -> None:
    """
    Download yesterday's files for all symbols already in the store.
    Run this once after UTC midnight each day.
    """
    store   = HistoricalStore()
    symbols = store.available_symbols()
    yesterday = _yesterday()

    if not symbols:
        log.warning("no symbols in historical store — run initial download first")
        return

    log.info("daily update: %d symbols for %s", len(symbols), yesterday)

    sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    async def bounded(session, symbol):
        async with sem:
            return await download_symbol(session, store, symbol, start=yesterday)

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_DOWNLOADS * 2)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [bounded(session, sym) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    downloaded = sum(r.get("downloaded", 0) for r in results if isinstance(r, dict))
    failed     = sum(r.get("failed", 0)     for r in results if isinstance(r, dict))
    log.info("daily update complete — downloaded: %d  failed: %d", downloaded, failed)


# ── Gap checker ────────────────────────────────────────────────────────────────

def check_gaps(symbol: str, start: date | None = None) -> list[date]:
    """Return list of missing dates for a symbol. Useful for diagnostics."""
    store = HistoricalStore()
    start = start or _binance_launch_date()
    return store.missing_dates(symbol, start, date.today())


# ── Entry points ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from logger import setup_logging

    setup_logging("downloader")

    parser = argparse.ArgumentParser(description="Binance historical kline downloader")
    parser.add_argument("mode", choices=["initial", "update", "gaps"],
                        help="initial=full history, update=yesterday only, gaps=report")
    parser.add_argument("--start", type=date.fromisoformat, default=None,
                        help="start date for initial download (YYYY-MM-DD)")
    parser.add_argument("--symbol", default=None,
                        help="check gaps for a specific symbol")
    args = parser.parse_args()

    if args.mode == "initial":
        asyncio.run(run_initial_download(start=args.start))
    elif args.mode == "update":
        asyncio.run(run_daily_update())
    elif args.mode == "gaps":
        sym = args.symbol or "BTCUSDT"
        gaps = check_gaps(sym, start=args.start)
        if gaps:
            print(f"{sym}: {len(gaps)} missing dates")
            for g in gaps[:20]:
                print(f"  {g}")
            if len(gaps) > 20:
                print(f"  ... and {len(gaps) - 20} more")
        else:
            print(f"{sym}: no gaps found")
