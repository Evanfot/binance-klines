"""
04 — Universe construction: build the top-N dollar-volume universe and
     inspect each symbol's coverage and liquidity.

Run from the project root:
  python examples/04_universe.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import date, timedelta
from universe import build_universe, universe_dollar_volumes
from storage import HistoricalStore

store = HistoricalStore()

if not store.available_symbols():
    print("Store is empty. Run 'python main.py init' first.")
    sys.exit(1)

# ── Default universe (top 50 by 20-day dollar volume) ─────────────────────────
universe = build_universe()
print(f"\n=== Universe: top {len(universe)} symbols ===")
for i, sym in enumerate(universe, 1):
    r = store.date_range(sym)
    date_str = f"{r[0].isoformat()} → {r[1].isoformat()}" if r else "no data"
    print(f"  {i:3d}.  {sym:<15}  {date_str}")

# ── Dollar-volume detail ───────────────────────────────────────────────────────
print("\n=== Dollar-volume stats (20-day lookback) ===")
dv = universe_dollar_volumes(universe)
print(
    dv.assign(
        dollar_volume = dv["dollar_volume"].map("${:,.0f}".format),
        avg_daily_dv  = dv["avg_daily_dv"].map("${:,.0f}".format),
        vwap          = dv["vwap"].map("{:.4f}".format),
    ).to_string(index=False)
)

# ── Historical universe (as of 30 days ago) ───────────────────────────────────
past_date = date.today() - timedelta(days=30)
past_universe = build_universe(as_of=past_date, size=10)
print(f"\n=== Universe as of {past_date.isoformat()} (top 10) ===")
for i, sym in enumerate(past_universe, 1):
    print(f"  {i:2d}.  {sym}")

# ── Universe turnover ──────────────────────────────────────────────────────────
current_set = set(universe)
past_set    = set(past_universe)
added       = current_set - past_set
removed     = past_set    - current_set

print(f"\n=== Turnover vs. 30 days ago ===")
print(f"  Added  : {sorted(added)  or 'none'}")
print(f"  Removed: {sorted(removed) or 'none'}")
