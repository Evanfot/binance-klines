"""
Universe construction — top N symbols by 20-day dollar volume.

Reads from the historical store only.
Applies entry/exit buffers to prevent thrashing.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

from config import (
    UNIVERSE_LOOKBACK_DAYS,
    UNIVERSE_MIN_DAYS,
    UNIVERSE_MIN_VOL,
    UNIVERSE_SIZE,
)
from storage import HistoricalStore

log = logging.getLogger(__name__)


def build_universe(
    as_of: date | None = None,
    lookback_days: int = UNIVERSE_LOOKBACK_DAYS,
    size: int = UNIVERSE_SIZE,
    min_consecutive_days: int = UNIVERSE_MIN_DAYS,
    min_annualized_vol: float = UNIVERSE_MIN_VOL,
) -> list[str]:
    """
    Return the top `size` symbols by dollar volume over the last `lookback_days`.

    Parameters
    ----------
    as_of:
        The date to compute the universe as of (default: yesterday).
    lookback_days:
        Rolling window for dollar volume calculation.
    size:
        Number of symbols to include.
    min_consecutive_days:
        Symbol must appear in top (size * 1.5) for this many consecutive days
        before inclusion — prevents one-day volume anomalies.

    Returns
    -------
    Sorted list of symbol strings.
    """
    as_of = as_of or (date.today() - timedelta(days=1))
    start = as_of - timedelta(days=lookback_days + min_consecutive_days + 5)

    store   = HistoricalStore()
    symbols = store.available_symbols()

    if not symbols:
        log.warning("no symbols in historical store")
        return []

    log.info("building universe as of %s from %d available symbols", as_of, len(symbols))

    # Load close + quote_volume for all symbols over the window
    # quote_volume = sum(price * qty) per bar — this IS dollar volume
    frames = []
    for sym in symbols:
        df = store.load_symbol(sym, start=start, end=as_of + timedelta(days=1))
        if df.empty:
            continue
        df["symbol"] = sym
        df["date"]   = df["open_time"].dt.date
        daily = (
            df.groupby("date")
            .agg(dollar_volume=("quote_volume", "sum"), close=("close", "last"))
            .reset_index()
        )
        daily["symbol"] = sym
        frames.append(daily)

    if not frames:
        return []

    all_daily = pd.concat(frames, ignore_index=True)

    # Filter to lookback window
    cutoff = as_of - timedelta(days=lookback_days)
    window = all_daily[all_daily["date"] >= cutoff]

    # Annualized volatility filter — drops stablecoins and FX pairs
    if min_annualized_vol > 0:
        closes = window.pivot(index="date", columns="symbol", values="close")
        ann_vol = np.log(closes / closes.shift(1)).std() * np.sqrt(365)
        volatile = ann_vol[ann_vol >= min_annualized_vol].index
        window = window[window["symbol"].isin(volatile)]
        log.info("vol filter (>= %.0f%% ann.): %d/%d symbols pass",
                 min_annualized_vol * 100, len(volatile), len(ann_vol))

    # Total dollar volume per symbol over the window
    totals = (
        window.groupby("symbol")["dollar_volume"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )

    # Candidate pool: top (size * 1.5) to allow buffer filtering
    candidate_size = int(size * 1.5)
    candidates = set(totals.head(candidate_size)["symbol"])

    if min_consecutive_days <= 1:
        # No buffer — just take top N
        universe = list(totals.head(size)["symbol"])
        log.info("universe: %d symbols (no buffer)", len(universe))
        return sorted(universe)

    # Apply consecutive-days filter
    # Symbol must appear in top candidate_size for min_consecutive_days straight
    recent_days = sorted(all_daily["date"].unique())[-min_consecutive_days:]

    eligible = set()
    for sym in candidates:
        sym_data = all_daily[all_daily["symbol"] == sym]
        daily_rank = {}
        for d in recent_days:
            day_totals = all_daily[all_daily["date"] == d].sort_values(
                "dollar_volume", ascending=False
            )
            rank = day_totals["symbol"].tolist().index(sym) + 1 if sym in day_totals["symbol"].values else 9999
            daily_rank[d] = rank

        if all(r <= candidate_size for r in daily_rank.values()):
            eligible.add(sym)

    # From eligible, take top N by total dollar volume
    universe = list(
        totals[totals["symbol"].isin(eligible)].head(size)["symbol"]
    )
    log.info("universe: %d symbols after %d-day buffer filter", len(universe), min_consecutive_days)
    return sorted(universe)


def universe_dollar_volumes(
    symbols: list[str],
    as_of: date | None = None,
    lookback_days: int = UNIVERSE_LOOKBACK_DAYS,
) -> pd.DataFrame:
    """
    Return a DataFrame with dollar_volume and vwap for each symbol
    over the lookback window. Useful for position sizing.
    """
    as_of = as_of or (date.today() - timedelta(days=1))
    start = as_of - timedelta(days=lookback_days)

    store  = HistoricalStore()
    frames = []

    for sym in symbols:
        df = store.load_symbol(sym, start=start, end=as_of + timedelta(days=1))
        if df.empty:
            continue
        frames.append({
            "symbol":        sym,
            "dollar_volume": df["quote_volume"].sum(),
            "vwap":          df["quote_volume"].sum() / df["volume"].sum()
                             if df["volume"].sum() > 0 else float("nan"),
            "avg_daily_dv":  df["quote_volume"].sum() / lookback_days,
        })

    return pd.DataFrame(frames).sort_values("dollar_volume", ascending=False)


if __name__ == "__main__":
    from logger import setup_logging
    setup_logging("universe")

    universe = build_universe()
    print(f"\nTop {UNIVERSE_SIZE} symbols by {UNIVERSE_LOOKBACK_DAYS}-day dollar volume:\n")
    for i, sym in enumerate(universe, 1):
        print(f"  {i:3d}.  {sym}")
