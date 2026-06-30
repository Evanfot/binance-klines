"""
Live 1m kline stream via Binance WebSocket.

Design:
  - Subscribes to kline streams for all tracked symbols
  - Completed bars → live intraday buffer (parquet)
  - In-progress bar → current_bar (parquet, overwritten each update)
  - On reconnect: fetches any gap from REST API before resuming WS
  - At UTC midnight: signals downloader to collect yesterday, clears intraday buffer
  - Never writes to historical store
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Callable

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from config import (
    BINANCE_REST_URL,
    BINANCE_WS_URL,
    INTERVAL,
    WS_MAX_RECONNECT_ATTEMPTS,
    WS_RECONNECT_BACKOFF_S,
)
from storage import LiveStore, KLINE_COLUMNS

log = logging.getLogger(__name__)

UTC = timezone.utc


# ── Bar parsing ────────────────────────────────────────────────────────────────

def _epoch_to_dt(epoch: float) -> datetime:
    """Binance epoch (ms or µs) → UTC datetime.

    Binance moved some kline feeds to microsecond epochs in 2025; detect the scale
    by magnitude (a real ms date is < ~5e12, µs is ~1e15) so a format change can't
    silently corrupt timestamps (parsing µs as ms inflated dates to year ~58461).
    """
    divisor = 1_000_000 if epoch > 5_000_000_000_000 else 1_000
    return datetime.fromtimestamp(epoch / divisor, tz=UTC)


def _parse_kline_event(msg: dict) -> dict:
    """Parse a Binance kline websocket message into a bar dict."""
    k = msg["k"]
    return {
        "open_time":               _epoch_to_dt(k["t"]),
        "open":                    float(k["o"]),
        "high":                    float(k["h"]),
        "low":                     float(k["l"]),
        "close":                   float(k["c"]),
        "volume":                  float(k["v"]),
        "close_time":              _epoch_to_dt(k["T"]),
        "quote_volume":            float(k["q"]),
        "num_trades":              int(k["n"]),
        "taker_buy_base_volume":   float(k["V"]),
        "taker_buy_quote_volume":  float(k["Q"]),
        "is_closed":               k["x"],   # True = bar completed
    }


def _parse_rest_kline(row: list) -> dict:
    """Parse a REST API klines row into a bar dict."""
    return {
        "open_time":               _epoch_to_dt(row[0]),
        "open":                    float(row[1]),
        "high":                    float(row[2]),
        "low":                     float(row[3]),
        "close":                   float(row[4]),
        "volume":                  float(row[5]),
        "close_time":              _epoch_to_dt(row[6]),
        "quote_volume":            float(row[7]),
        "num_trades":              int(row[8]),
        "taker_buy_base_volume":   float(row[9]),
        "taker_buy_quote_volume":  float(row[10]),
        "is_closed":               True,
    }


# ── REST gap fill ──────────────────────────────────────────────────────────────

async def fetch_rest_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict]:
    """
    Fetch completed 1m bars from REST API to fill a gap.
    Returns bars in ascending order, excluding the final (possibly open) bar.
    """
    url    = f"{BINANCE_REST_URL}/api/v3/klines"
    bars   = []
    cursor = start_ms

    while cursor < end_ms:
        params = {
            "symbol":    symbol,
            "interval":  INTERVAL,
            "startTime": cursor,
            "endTime":   end_ms,
            "limit":     1000,
        }
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            rows = await resp.json()

        if not rows:
            break

        for row in rows:
            bar = _parse_rest_kline(row)
            if bar["close_time"].timestamp() * 1000 < end_ms:
                bars.append(bar)
        
        last_close_ms = rows[-1][6]
        if last_close_ms >= end_ms:
            break
        cursor = last_close_ms + 1

    log.debug("REST gap fill  %s  %d bars", symbol, len(bars))
    return bars


# ── Midnight rollover ──────────────────────────────────────────────────────────

def _next_midnight_utc() -> float:
    """Return the timestamp of the next UTC midnight."""
    now = datetime.now(UTC)
    tomorrow = (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return tomorrow.timestamp()


# ── Stream manager ─────────────────────────────────────────────────────────────

class KlineStreamManager:
    """
    Manages live kline streams for a set of symbols.

    Usage
    -----
    manager = KlineStreamManager(symbols, on_bar=my_callback)
    await manager.run()

    Callbacks
    ---------
    on_bar(symbol, bar):   called for every completed 1m bar
    on_tick(symbol, bar):  called for every WS update (in-progress bars too)
    """

    def __init__(
        self,
        symbols: list[str],
        on_bar:  Callable[[str, dict], None] | None = None,
        on_tick: Callable[[str, dict], None] | None = None,
    ):
        self.symbols  = symbols
        self.on_bar   = on_bar
        self.on_tick  = on_tick
        self.store    = LiveStore()
        self._running = False

        # Track last seen close_time per symbol to detect gaps on reconnect
        self._last_close: dict[str, datetime] = {}

    # ── Public API ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Run the stream indefinitely, reconnecting on failure."""
        self._running = True
        backoff = WS_RECONNECT_BACKOFF_S
        attempts = 0

        while self._running:
            try:
                await self._connect_and_stream()
                backoff = WS_RECONNECT_BACKOFF_S  # reset on clean exit
                attempts = 0
            except Exception as exc:
                attempts += 1
                if attempts > WS_MAX_RECONNECT_ATTEMPTS:
                    log.error("max reconnect attempts reached — giving up")
                    raise
                log.warning(
                    "stream error (attempt %d/%d): %s — reconnecting in %.1fs",
                    attempts, WS_MAX_RECONNECT_ATTEMPTS, exc, backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    def stop(self) -> None:
        self._running = False

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _connect_and_stream(self) -> None:
        """Open one WebSocket connection and process messages."""
        # Binance combined stream: up to 1024 streams per connection
        # For >1024 symbols, chunk into multiple connections
        streams = "/".join(
            f"{sym.lower()}@kline_{INTERVAL}" for sym in self.symbols
        )
        url = f"{BINANCE_WS_URL}/stream?streams={streams}"

        log.info("connecting to Binance WS (%d symbols)", len(self.symbols))

        async with aiohttp.ClientSession() as session:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                log.info("WS connected — filling any gaps from REST")
                await self._fill_gaps_on_reconnect(session)

                async for raw in ws:
                    if not self._running:
                        break
                    await self._handle_message(session, raw)

    async def _handle_message(
        self, session: aiohttp.ClientSession, raw: str
    ) -> None:
        try:
            msg    = json.loads(raw)
            data   = msg.get("data", msg)
            symbol = data["s"]
            bar    = _parse_kline_event(data)
        except (KeyError, json.JSONDecodeError) as exc:
            log.debug("unparseable message: %s", exc)
            return

        # Always write the current (possibly incomplete) bar
        self.store.write_current_bar(symbol, {k: v for k, v in bar.items() if k != "is_closed"})

        if self.on_tick:
            self.on_tick(symbol, bar)

        if bar["is_closed"]:
            clean = {k: v for k, v in bar.items() if k != "is_closed"}
            self.store.write_completed_bar(symbol, clean)
            self._last_close[symbol] = bar["close_time"]

            if self.on_bar:
                self.on_bar(symbol, clean)

            log.debug("bar closed  %s  %s  close=%.4f",
                      symbol, bar["open_time"], bar["close"])

    async def _fill_gaps_on_reconnect(
        self, session: aiohttp.ClientSession
    ) -> None:
        """
        For each symbol, if we were previously tracking it, fetch any bars
        we missed while disconnected.
        """
        now_ms = int(datetime.now(UTC).timestamp() * 1000)

        for symbol, last_close in self._last_close.items():
            gap_start_ms = int(last_close.timestamp() * 1000) + 1
            if gap_start_ms >= now_ms:
                continue

            log.info("filling gap  %s  from %s", symbol, last_close.isoformat())
            try:
                bars = await fetch_rest_klines(
                    session, symbol, gap_start_ms, now_ms
                )
                for bar in bars:
                    self.store.write_completed_bar(symbol, bar)
                    if self.on_bar:
                        self.on_bar(symbol, bar)
                if bars:
                    self._last_close[symbol] = bars[-1]["close_time"]
            except Exception as exc:
                log.error("gap fill failed  %s: %s", symbol, exc)

    def do_rollover(self) -> None:
        """Clear this chunk's intraday buffers. Called by ChunkedStreamManager."""
        for symbol in self.symbols:
            try:
                self.store.clear_intraday(symbol)
            except Exception as exc:
                log.error("rollover error  %s: %s", symbol, exc)


# ── Chunked manager for large universes ────────────────────────────────────────

class ChunkedStreamManager:
    """
    Wraps multiple KlineStreamManagers for universes > 1024 symbols.
    Binance WS allows max 1024 streams per connection.
    """
    MAX_PER_CONNECTION = 200  # conservative — large symbol names can hit URL limits

    def __init__(
        self,
        symbols: list[str],
        on_bar:  Callable[[str, dict], None] | None = None,
        on_tick: Callable[[str, dict], None] | None = None,
    ):
        chunks = [
            symbols[i : i + self.MAX_PER_CONNECTION]
            for i in range(0, len(symbols), self.MAX_PER_CONNECTION)
        ]
        self.managers = [
            KlineStreamManager(chunk, on_bar=on_bar, on_tick=on_tick)
            for chunk in chunks
        ]
        log.info(
            "chunked stream: %d symbols across %d connections",
            len(symbols), len(self.managers),
        )

    async def run(self) -> None:
        await asyncio.gather(
            self._midnight_rollover_loop(),
            *[m.run() for m in self.managers],
        )

    def stop(self) -> None:
        for m in self.managers:
            m.stop()

    async def _midnight_rollover_loop(self) -> None:
        """Single rollover loop for all chunks — fires once at UTC midnight.

        Just before the clear, snapshot a *provisional* daily close from the live
        buffer so consumers get the day's near-final close without waiting for the
        official Binance daily file (published 3-8h later). update() overwrites it
        once that file lands.
        """
        # Snapshot 20 min before the buffer clears, leaving margin for the downstream
        # trader's nightly cache rebuild (23:45 UTC) to pick it up before it decides.
        PROVISIONAL_LEAD_S = 1200

        while True:
            # 1. Wake shortly before midnight and snapshot the provisional close
            #    (the live buffer still holds the full day at this point).
            lead_sleep = _next_midnight_utc() - time.time() - PROVISIONAL_LEAD_S
            if lead_sleep > 0:
                log.info("provisional close snapshot in %.0f seconds", lead_sleep)
                await asyncio.sleep(lead_sleep)
            try:
                from closes import update_provisional
                update_provisional()
            except Exception as exc:
                log.error("provisional closes update failed: %s", exc)

            # 2. Wait for the actual UTC-midnight rollover.
            sleep_s = _next_midnight_utc() - time.time() + 5  # +5s buffer
            log.info("midnight rollover in %.0f seconds", sleep_s)
            await asyncio.sleep(max(0, sleep_s))

            log.info("UTC midnight rollover — clearing intraday buffers")
            for manager in self.managers:
                manager.do_rollover()
            log.info("rollover complete")

            # Download yesterday's completed daily files into the historical store.
            # Binance publishes daily files at an unpredictable time (typically 3-8h
            # after UTC midnight). Retry hourly until something downloads.
            from downloader import run_daily_update
            downloaded = False
            for attempt in range(1, 13):
                await asyncio.sleep(300)
                log.info("running daily historical update (attempt %d/12)", attempt)
                try:
                    result = await run_daily_update()
                except Exception as exc:
                    log.error("daily update failed: %s", exc)
                    break
                if result.get("downloaded", 0) > 0 or result.get("failed", 0) > 0:
                    downloaded = True
                    break
                if attempt < 12:
                    log.info("files not yet available — retrying in 1 hour")
                    await asyncio.sleep(3600 - 300)

            if downloaded:
                try:
                    from closes import update as update_closes
                    update_closes()
                    log.info("daily closes updated")
                except Exception as exc:
                    log.error("closes update failed: %s", exc)


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from logger import setup_logging

    setup_logging("stream")

    parser = argparse.ArgumentParser(description="Binance live kline stream")
    parser.add_argument("--symbols", nargs="+", default=["BTCUSDT", "ETHUSDT"],
                        help="symbols to stream")
    args = parser.parse_args()

    def on_bar(symbol: str, bar: dict) -> None:
        log.info("BAR  %-12s  %s  close=%.4f  vol=%.2f",
                 symbol, bar["open_time"], bar["close"], bar["volume"])

    manager = ChunkedStreamManager(args.symbols, on_bar=on_bar)

    try:
        asyncio.run(manager.run())
    except KeyboardInterrupt:
        log.info("stream stopped by user")
