"""
05 — DuckDB queries: SQL analytics across the full symbol universe.

DuckDB reads the parquet files directly without loading everything into
memory, making it suitable for cross-sectional queries over long histories.

Run from the project root:
  python examples/05_duckdb_queries.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from storage import HistoricalStore, all_symbols_glob

store = HistoricalStore()
glob  = all_symbols_glob()

if not store.available_symbols():
    print("Store is empty. Run 'python main.py init' first.")
    sys.exit(1)


# ── Helper: print query result neatly ─────────────────────────────────────────
def run(label: str, sql: str) -> None:
    print(f"\n=== {label} ===")
    df = store.query(sql)
    print(df.to_string(index=False))


# ── 1. VWAP for BTCUSDT over the last 7 days ─────────────────────────────────
from config import HISTORICAL_DIR, INTERVAL

btc_glob = str(HISTORICAL_DIR / "BTCUSDT" / INTERVAL / "*.parquet")

run(
    "BTCUSDT — hourly VWAP (last 7 days)",
    f"""
    SELECT
        time_bucket(INTERVAL '1 hour', open_time) AS hour,
        SUM(quote_volume) / NULLIF(SUM(volume), 0) AS vwap,
        SUM(quote_volume)                           AS dollar_vol,
        SUM(num_trades)                             AS trades
    FROM read_parquet('{btc_glob}')
    WHERE open_time >= current_date - INTERVAL '7 days'
    GROUP BY 1
    ORDER BY 1 DESC
    LIMIT 10
    """,
)


# ── 2. Top 10 symbols by dollar volume yesterday ──────────────────────────────
run(
    "Top 10 symbols by dollar volume yesterday",
    f"""
    SELECT
        split_part(filename, '/', -3) AS symbol,
        SUM(quote_volume)             AS dollar_vol,
        SUM(num_trades)               AS trades,
        LAST(close ORDER BY open_time) AS last_close
    FROM read_parquet('{glob}', filename=true)
    WHERE open_time >= current_date - INTERVAL '1 day'
      AND open_time <  current_date
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
    """,
)


# ── 3. Taker-buy ratio (order flow imbalance) across universe, last 24h ───────
run(
    "Taker-buy ratio — last 24 hours (aggressive buying > 0.5)",
    f"""
    SELECT
        split_part(filename, '/', -3)                           AS symbol,
        SUM(taker_buy_quote_volume) / NULLIF(SUM(quote_volume), 0) AS taker_buy_ratio,
        SUM(quote_volume)                                       AS dollar_vol
    FROM read_parquet('{glob}', filename=true)
    WHERE open_time >= current_date - INTERVAL '1 day'
    GROUP BY 1
    HAVING dollar_vol > 1e6
    ORDER BY taker_buy_ratio DESC
    LIMIT 10
    """,
)


# ── 4. Rolling 20-bar realised volatility (annualised), last bar per symbol ───
run(
    "Realised volatility — 20-bar rolling, latest value",
    f"""
    WITH bars AS (
        SELECT
            split_part(filename, '/', -3) AS symbol,
            open_time,
            LN(close / LAG(close) OVER (
                PARTITION BY split_part(filename, '/', -3)
                ORDER BY open_time
            )) AS log_ret
        FROM read_parquet('{glob}', filename=true)
        WHERE open_time >= current_date - INTERVAL '2 days'
    ),
    vol AS (
        SELECT
            symbol,
            open_time,
            STDDEV(log_ret) OVER (
                PARTITION BY symbol
                ORDER BY open_time
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) * SQRT(525600) AS ann_vol_1m
        FROM bars
    )
    SELECT symbol, ROUND(ann_vol_1m * 100, 2) AS ann_vol_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY open_time DESC) AS rn
        FROM vol
    )
    WHERE rn = 1
      AND ann_vol_pct IS NOT NULL
    ORDER BY ann_vol_pct DESC
    LIMIT 15
    """,
)
