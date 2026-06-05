"""
Main orchestrator — single entry point for the data system.

Modes:
  init      — full historical download then start streaming
  stream    — stream only (assumes historical is populated)
  update    — download yesterday's files then exit (for cron)
  status    — print store health summary

The system is designed so historical and live data never collide:
  - Downloader only writes dates < today
  - Stream only writes to live/
  - At UTC midnight, stream clears intraday and downloader collects yesterday
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from datetime import date

from logger import setup_logging

log = setup_logging("main")


async def _run_stream(symbols: list[str]) -> None:
    from stream import ChunkedStreamManager

    def on_bar(symbol: str, bar: dict) -> None:
        log.debug("bar  %s  %s  close=%.6f", symbol, bar["open_time"], bar["close"])

    manager = ChunkedStreamManager(symbols, on_bar=on_bar)

    loop = asyncio.get_event_loop()
    task = asyncio.current_task()

    def _shutdown() -> None:
        log.info("shutdown signal received — stopping stream")
        manager.stop()
        if task:
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    try:
        await manager.run()
    except asyncio.CancelledError:
        log.info("stream stopped")


async def cmd_init(args) -> None:
    """Download full history then start streaming."""
    from downloader import run_initial_download, fetch_all_symbols
    import aiohttp

    start = args.start if hasattr(args, "start") else None
    log.info("=== INITIAL DOWNLOAD ===")
    await run_initial_download(start=start)

    log.info("=== STARTING STREAM ===")
    async with aiohttp.ClientSession() as session:
        symbols = await fetch_all_symbols(session)

    await _run_stream(symbols)


async def cmd_stream(args) -> None:
    """Stream only — historical must already be populated."""
    from storage import HistoricalStore
    from downloader import fetch_all_symbols
    import aiohttp

    store = HistoricalStore()
    symbols = store.available_symbols()

    if not symbols:
        log.warning("historical store is empty — consider running 'init' first")
        async with aiohttp.ClientSession() as session:
            symbols = await fetch_all_symbols(session)

    log.info("streaming %d symbols", len(symbols))
    await _run_stream(symbols)


async def cmd_update(args) -> None:
    """Download yesterday's files and exit. Run via cron after UTC midnight."""
    from downloader import run_daily_update
    from closes import update as update_closes
    log.info("=== DAILY UPDATE ===")
    await run_daily_update()
    log.info("=== UPDATING DAILY CLOSES ===")
    update_closes()


async def cmd_closes_build(args) -> None:
    """Build daily_closes.parquet from all historical 1m files."""
    from closes import build
    log.info("=== BUILDING DAILY CLOSES ===")
    build()


def cmd_closes_update(args) -> None:
    """Append yesterday's closes to daily_closes.parquet."""
    from closes import update
    from datetime import date as _date
    yesterday = args.date or (_date.today() - __import__("datetime").timedelta(days=1))
    log.info("=== CLOSES UPDATE ===")
    update(yesterday=yesterday)


def cmd_status(args) -> None:
    """Print a health summary of the historical store."""
    from storage import HistoricalStore
    from datetime import timedelta

    store   = HistoricalStore()
    symbols = store.available_symbols()
    today   = date.today()
    yesterday = today - timedelta(days=1)

    print(f"\n{'='*55}")
    print(f"  Binance Data Store Status — {today.isoformat()}")
    print(f"{'='*55}")
    print(f"  Symbols in store : {len(symbols)}")

    if not symbols:
        print("  Store is empty.")
        return

    # Check coverage
    missing_yesterday = []
    total_files = 0
    for sym in symbols:
        r = store.date_range(sym)
        if r:
            start, end = r
            days = (end - start).days + 1
            total_files += days
            if end < yesterday:
                missing_yesterday.append((sym, end))

    print(f"  Total daily files: {total_files:,}")
    print(f"  Missing yesterday: {len(missing_yesterday)}")

    if missing_yesterday[:10]:
        print("\n  Symbols behind (first 10):")
        for sym, last in missing_yesterday[:10]:
            print(f"    {sym:<20} last={last.isoformat()}")

    # Sample date range
    sample = symbols[:3]
    print("\n  Sample date ranges:")
    for sym in sample:
        r = store.date_range(sym)
        if r:
            print(f"    {sym:<20} {r[0].isoformat()} → {r[1].isoformat()}")

    print(f"{'='*55}\n")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Binance 1m kline data system",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full initial download then stream
  python main.py init

  # Initial download from a specific date
  python main.py init --start 2023-01-01

  # Stream only (historical already downloaded)
  python main.py stream

  # Daily cron update (run after UTC midnight) — also updates closes
  python main.py update

  # Check store health
  python main.py status

  # Build daily_closes.parquet from scratch (run once after initial download)
  python main.py closes-build

  # Manually append one day's closes (normally handled by 'update')
  python main.py closes-update
  python main.py closes-update --date 2025-06-01
        """,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Full history download + start stream")
    p_init.add_argument("--start", type=date.fromisoformat, default=None,
                        help="Earliest date to download (default: Binance launch)")

    sub.add_parser("stream", help="Stream only")
    sub.add_parser("update", help="Download yesterday and exit (also updates closes)")
    sub.add_parser("status", help="Print store health summary")
    sub.add_parser("closes-build", help="Build daily_closes.parquet from all 1m history")
    p_closes_update = sub.add_parser("closes-update", help="Append yesterday's closes (standalone)")
    p_closes_update.add_argument("--date", type=date.fromisoformat, default=None,
                                 help="Date to update (default: yesterday)")

    args = parser.parse_args()

    dispatch = {
        "init":          cmd_init,
        "stream":        cmd_stream,
        "update":        cmd_update,
        "status":        cmd_status,
        "closes-build":  cmd_closes_build,
        "closes-update": cmd_closes_update,
    }

    fn = dispatch[args.cmd]

    if asyncio.iscoroutinefunction(fn):
        asyncio.run(fn(args))
    else:
        fn(args)


if __name__ == "__main__":
    main()
