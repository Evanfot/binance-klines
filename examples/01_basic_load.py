"""
01 — Load a single symbol and compute common derived metrics.

Run from the project root:
  python examples/01_basic_load.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from storage import DataStore

ds = DataStore()

# ── Load 30 days of BTCUSDT including today's live bars ───────────────────────
df = ds.load("BTCUSDT", lookback_days=30, include_live=True)

if df.empty:
    print("No data found. Run 'python main.py init' first.")
    sys.exit(1)

# ── Derived metrics ───────────────────────────────────────────────────────────
df["vwap"]              = df["quote_volume"] / df["volume"]
df["dollar_volume"]     = df["quote_volume"]          # quote_volume IS dollar volume
df["taker_buy_ratio"]   = df["taker_buy_quote_volume"] / df["quote_volume"]
df["returns"]           = df["close"].pct_change()

# ── Daily summary ─────────────────────────────────────────────────────────────
df["date"] = df["open_time"].dt.date
daily = df.groupby("date").agg(
    open       = ("open",          "first"),
    high       = ("high",          "max"),
    low        = ("low",           "min"),
    close      = ("close",         "last"),
    dollar_vol = ("dollar_volume", "sum"),
    num_trades = ("num_trades",    "sum"),
)

print("\n=== BTCUSDT — last 30 days ===")
print(f"Bars loaded      : {len(df):,}")
print(f"Date range       : {df['open_time'].min()} → {df['open_time'].max()}")
print(f"Last close       : ${df['close'].iloc[-1]:,.2f}")
print(f"Avg daily DV     : ${daily['dollar_vol'].mean():,.0f}")
print(f"Avg taker ratio  : {df['taker_buy_ratio'].mean():.3f}  (>0.5 = aggressive buying)")
print()
print(daily.tail(5).to_string())
