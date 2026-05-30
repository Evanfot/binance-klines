# %% Imports
import sys
from pathlib import Path
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from storage import DataStore, HistoricalStore

store = DataStore()
hist  = HistoricalStore()

# %% Available symbols
symbols = hist.available_symbols()
print(f"{len(symbols)} symbols in store")
print(symbols[:10])

# %% Load a single symbol
SYMBOL = symbols[0] if symbols else "BTCUSDT"
SYMBOL = 'BTCUSDT'
df = store.load(SYMBOL, lookback_days=30)
df = df.set_index("open_time")
df.index = df.index.tz_convert(None)  # matplotlib can't handle tz-aware index
print(df.shape)
df.head()

# %% Plot close price
import matplotlib.pyplot as plt

fig, (ax_price, ax_vol) = plt.subplots(
    2, 1, figsize=(14, 6), sharex=True,
    gridspec_kw={"height_ratios": [3, 1]},
)

df["close"].plot(ax=ax_price, linewidth=0.8)
ax_price.set_title(f"{SYMBOL} close")
ax_price.set_ylabel("price")

ax_vol.bar(df.index, df["volume"], width=0.0006, color="steelblue", alpha=0.6)
ax_vol.set_ylabel("volume")
ax_vol.set_xlabel("time")

plt.tight_layout()
plt.show()

# %% Basic stats
df[["open", "high", "low", "close", "volume"]].describe()

# %% Load a cross-section of symbols
top = symbols[:5]
end   = date.today()
start = end - timedelta(days=7)

cross = hist.load_cross_section(top, start=start, end=end)
cross = cross.set_index(["symbol", "open_time"]).sort_index()
print(cross.shape)
cross.head(10)

# %% Inspect live data
from storage import LiveStore

live_store = LiveStore()
live_df = live_store.read_all(SYMBOL)
print(f"live bars: {len(live_df)}")
live_df.tail()

# %%
