"""
02 — Access live streaming data (intraday buffer + current bar).

The stream must be running (`python main.py stream` or Docker) for live
files to exist.  This script reads them safely without interfering with
the writer.

Run from the project root:
  python examples/02_live_data.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from storage import DataStore, HistoricalStore, LiveStore

live  = LiveStore()
hist  = HistoricalStore()
ds    = DataStore()

symbols = hist.available_symbols()
if not symbols:
    print("Historical store is empty. Run 'python main.py init' first.")
    sys.exit(1)

# ── Current in-progress bar ───────────────────────────────────────────────────
print("\n=== Current bar (in-progress, updated every second) ===")
for sym in symbols[:5]:
    bar = live.read_current_bar(sym)
    if bar is not None:
        print(
            f"  {sym:<15}  open={bar['open']:.4f}  "
            f"close={bar['close']:.4f}  "
            f"trades={int(bar['num_trades'])}"
        )
    else:
        print(f"  {sym:<15}  no current bar (stream may not be running)")

# ── Intraday completed bars ───────────────────────────────────────────────────
print("\n=== Intraday completed bars (since UTC midnight) ===")
for sym in symbols[:3]:
    df = live.read_intraday(sym)
    if df.empty:
        print(f"  {sym}: no intraday data yet")
        continue
    df["vwap"] = df["quote_volume"] / df["volume"]
    print(
        f"  {sym}: {len(df)} bars  "
        f"last close={df['close'].iloc[-1]:.4f}  "
        f"vwap={df['vwap'].iloc[-1]:.4f}"
    )

# ── Seamless historical + live stitch ─────────────────────────────────────────
print("\n=== Historical + live stitched (DataStore) ===")
sym = symbols[0]
df = ds.load(sym, lookback_days=2, include_live=True)
print(f"  {sym}: {len(df)} total bars")
print(f"  First bar: {df['open_time'].iloc[0]}")
print(f"  Last bar:  {df['open_time'].iloc[-1]}")
print(f"  Gap check: {df['open_time'].diff().dropna().max()}  (should be 1 min if no gaps)")
