"""
06 — Cross-sectional momentum signal.

Ranks universe symbols by 20-day price momentum, computes a simple long/
short signal, and prints position weights.  Intended as a skeleton to plug
into a live strategy loop.

Run from the project root:
  python examples/06_momentum_signal.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from storage import DataStore
from universe import build_universe, universe_dollar_volumes

ds       = DataStore()
universe = build_universe(size=20)

if not universe:
    print("Store is empty or universe is too small. Run 'python main.py init' first.")
    sys.exit(1)

# ── Load 25 days so we have enough history for a 20-day return ────────────────
panel = ds.load_cross_section(universe, lookback_days=25, include_live=False)

if panel.empty:
    print("No data in the cross-section.")
    sys.exit(1)

panel["date"] = panel["open_time"].dt.date

# Daily close per symbol
daily_close = (
    panel.groupby(["date", "symbol"])["close"]
    .last()
    .unstack("symbol")
    .sort_index()
)

# ── 20-day momentum: return from 20 days ago to yesterday ─────────────────────
if len(daily_close) < 21:
    print(f"Need at least 21 days; only have {len(daily_close)}.")
    sys.exit(1)

ret_20d = (daily_close.iloc[-1] / daily_close.iloc[-21] - 1).rename("momentum_20d")

# ── Dollar-volume weight (for position sizing) ─────────────────────────────────
dv = universe_dollar_volumes(universe, lookback_days=20)
dv = dv.set_index("symbol")["avg_daily_dv"]

# ── Signal construction ────────────────────────────────────────────────────────
signal = pd.DataFrame({"momentum_20d": ret_20d, "avg_daily_dv": dv}).dropna()
signal["rank"]    = signal["momentum_20d"].rank(ascending=True)  # 1 = worst
signal["z_score"] = (
    (signal["momentum_20d"] - signal["momentum_20d"].mean())
    / signal["momentum_20d"].std()
)

n = len(signal)
long_cut  = int(n * 0.3)   # top 30%
short_cut = int(n * 0.3)   # bottom 30%

signal["position"] = 0.0
signal.loc[signal["rank"] > (n - long_cut),  "position"] =  1.0
signal.loc[signal["rank"] <= short_cut,       "position"] = -1.0

# Dollar-volume-weighted position sizes (normalised to ±1)
long_mask  = signal["position"] >  0
short_mask = signal["position"] <  0

if long_mask.any():
    lw = signal.loc[long_mask, "avg_daily_dv"]
    signal.loc[long_mask, "weight"] = lw / lw.sum()

if short_mask.any():
    sw = signal.loc[short_mask, "avg_daily_dv"]
    signal.loc[short_mask, "weight"] = -(sw / sw.sum())

signal["weight"] = signal["weight"].fillna(0.0)

# ── Output ────────────────────────────────────────────────────────────────────
print(f"\n=== Cross-sectional momentum signal — {daily_close.index[-1]} ===")
print(f"Universe size: {n} symbols\n")

print("LONG (top 30% momentum):")
longs = signal[signal["position"] > 0].sort_values("momentum_20d", ascending=False)
for sym, row in longs.iterrows():
    print(f"  {sym:<15}  mom={row['momentum_20d']:+.2%}  weight={row['weight']:+.3f}")

print("\nSHORT (bottom 30% momentum):")
shorts = signal[signal["position"] < 0].sort_values("momentum_20d")
for sym, row in shorts.iterrows():
    print(f"  {sym:<15}  mom={row['momentum_20d']:+.2%}  weight={row['weight']:+.3f}")

print(f"\nGross exposure: {signal['weight'].abs().sum():.3f}")
print(f"Net exposure  : {signal['weight'].sum():.3f}")
