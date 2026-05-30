"""
03 — Cross-sectional panel: load multiple symbols, compute returns and
     pairwise correlations.

Run from the project root:
  python examples/03_cross_section.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from storage import DataStore
from universe import build_universe

ds = DataStore()

# Use the top 10 universe symbols (or fall back to whatever is available)
universe = build_universe(size=10)
if not universe:
    from storage import HistoricalStore
    universe = HistoricalStore().available_symbols()[:10]

if not universe:
    print("Store is empty. Run 'python main.py init' first.")
    sys.exit(1)

print(f"Loading cross-section for {len(universe)} symbols over 20 days…")

panel = ds.load_cross_section(universe, lookback_days=20, include_live=True)
panel["vwap"]    = panel["quote_volume"] / panel["volume"]
panel["returns"] = panel.groupby("symbol")["close"].pct_change()

# ── Daily close pivot ─────────────────────────────────────────────────────────
panel["date"] = panel["open_time"].dt.date
daily_close = (
    panel.groupby(["date", "symbol"])["close"]
    .last()
    .unstack("symbol")
)

# ── Daily returns ─────────────────────────────────────────────────────────────
daily_ret = daily_close.pct_change().dropna(how="all")

print("\n=== Last 5 daily closes ===")
print(daily_close.tail(5).to_string())

print("\n=== Annualised volatility (20-day) ===")
ann_vol = daily_ret.std() * (252 ** 0.5)
print(ann_vol.sort_values(ascending=False).to_string())

print("\n=== Correlation matrix (daily returns) ===")
corr = daily_ret.corr().round(2)
print(corr.to_string())

# ── Dollar-volume ranking ──────────────────────────────────────────────────────
daily_dv = (
    panel.groupby(["date", "symbol"])["quote_volume"]
    .sum()
    .unstack("symbol")
)
avg_dv = daily_dv.mean().sort_values(ascending=False)

print("\n=== Average daily dollar volume (20-day) ===")
for sym, dv in avg_dv.items():
    print(f"  {sym:<15}  ${dv:>15,.0f}")
