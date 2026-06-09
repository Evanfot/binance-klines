"""
Minimal closes smoke-test — runs in seconds.

Copy to the server and run:
  docker exec -it binance-data python /app/test_closes.py
"""

import os, sys, tempfile
from pathlib import Path

ROOT = Path(os.environ["BINANCE_DATA_ROOT"]) if os.environ.get("BINANCE_DATA_ROOT") else Path(__file__).parent
HIST = ROOT / "klines" / "historical"

print(f"ROOT : {ROOT}")
print(f"HIST : {HIST}  (exists={HIST.exists()})")

candidates = sorted(d.name for d in HIST.iterdir() if (d / "1m").is_dir()) if HIST.exists() else []
if not candidates:
    print("FAIL: no symbol directories found"); sys.exit(1)

sym = "BTCUSDT" if "BTCUSDT" in candidates else candidates[0]
files = sorted((HIST / sym / "1m").glob("*.parquet"))
if not files:
    print(f"FAIL: no parquet files in {HIST / sym / '1m'}"); sys.exit(1)

test_date = files[-1].stem
test_file = str(files[-1])
print(f"sym  : {sym}  ({len(files)} files, testing {test_date})")

import duckdb, pandas as pd
print(f"duckdb  : {duckdb.__version__}")
print(f"pandas  : {pd.__version__}")
try:
    import pytz; print(f"pytz    : {pytz.__version__}")
except ImportError:
    print("pytz    : NOT INSTALLED")

# ── Step 1: open_time type in BTCUSDT ─────────────────────────────────────────
print("\n--- step 1: open_time type (BTCUSDT)")
try:
    con = duckdb.connect()
    r = con.execute(f"SELECT count(*) FROM read_parquet('{test_file}')").fetchone()
    t = con.execute(f"SELECT typeof(open_time), open_time FROM read_parquet('{test_file}') LIMIT 1").fetchone()
    print(f"  rows={r[0]}  type={t[0]}  raw_value={t[1]}")
except Exception as e:
    print(f"  FAIL: {e}"); sys.exit(1)

# ── Step 2: scan open_time types across ALL symbols for this date ─────────────
print("\n--- step 2: open_time types across all symbols")
glob = str(HIST / "*" / "1m" / f"{test_date}.parquet")
try:
    con = duckdb.connect()
    types = con.execute(f"""
        SELECT DISTINCT typeof(open_time) AS t, count(*) AS n
        FROM read_parquet('{glob}', filename=true)
        GROUP BY t
    """).fetchall()
    print(f"  distinct open_time types: {types}")
    if len(types) > 1:
        print("  WARNING: mixed open_time types across symbol files — this causes year 58403 error")
        # Show which files have unexpected types
        dominant = types[0][0]
        others = con.execute(f"""
            SELECT DISTINCT filename, typeof(open_time) AS t
            FROM read_parquet('{glob}', filename=true)
            WHERE typeof(open_time) != '{dominant}'
            LIMIT 5
        """).fetchall()
        print(f"  files with non-{dominant} type (first 5): {others}")
except Exception as e:
    print(f"  FAIL: {e}"); import traceback; traceback.print_exc()

# ── Step 3: AT TIME ZONE cast (requires pytz) ─────────────────────────────────
print("\n--- step 3: AT TIME ZONE 'UTC' cast on single file")
try:
    con = duckdb.connect()
    r = con.execute(f"SELECT CAST(open_time AT TIME ZONE 'UTC' AS DATE) FROM read_parquet('{test_file}') LIMIT 1").fetchone()
    print(f"  OK  date={r[0]}")
except Exception as e:
    print(f"  FAIL (pytz missing?): {e}")
    try:
        con = duckdb.connect()
        con.execute("SET TimeZone='UTC'")
        r = con.execute(f"SELECT CAST(open_time AS DATE) FROM read_parquet('{test_file}') LIMIT 1").fetchone()
        print(f"  fallback (SET TimeZone) OK  date={r[0]}")
        print("  FIX: add pytz to requirements.txt, OR change SQL to SET TimeZone='UTC' + plain CAST")
    except Exception as e2:
        print(f"  fallback also failed: {e2}"); sys.exit(1)

# ── Step 4: aggregation SQL — raw date values before pandas conversion ─────────
print("\n--- step 4: aggregation SQL, raw date column (before pandas)")
AGG_SQL = """
    SELECT
        CAST(open_time AT TIME ZONE 'UTC' AS DATE)  AS date,
        regexp_extract(filename, '/([^/]+)/1m/', 1) AS symbol,
        arg_min(open,  open_time)  AS open,
        max(high)                  AS high,
        min(low)                   AS low,
        arg_max(close, open_time)  AS close,
        sum(volume)                AS volume,
        sum(quote_volume)          AS quote_volume,
        sum(num_trades)::BIGINT    AS num_trades,
        CASE WHEN sum(volume) > 0 THEN sum(quote_volume)/sum(volume) ELSE NULL END AS vwap
    FROM read_parquet('{glob}', filename=true)
    GROUP BY date, symbol
    ORDER BY date, symbol
"""
try:
    con = duckdb.connect()
    df = con.execute(AGG_SQL.format(glob=glob)).df()
    print(f"  raw df shape={df.shape}  date dtype={df['date'].dtype}")
    print(f"  date min={df['date'].min()}  max={df['date'].max()}")
    print(f"  date sample (raw): {df['date'].head(3).tolist()}")
except Exception as e:
    print(f"  FAIL (query itself): {e}"); import traceback; traceback.print_exc(); sys.exit(1)

# Find which symbols have out-of-range dates (year > 9999)
print("\n--- step 4b: identify symbols with bad dates")
try:
    bad = con.execute(f"""
        SELECT
            regexp_extract(filename, '/([^/]+)/1m/', 1) AS symbol,
            CAST(open_time AT TIME ZONE 'UTC' AS DATE)  AS date,
            typeof(open_time)                           AS ot_type,
            min(open_time)                              AS min_ot,
            max(open_time)                              AS max_ot
        FROM read_parquet('{glob}', filename=true)
        GROUP BY symbol, date, ot_type
        HAVING year(date) > 9999 OR year(date) < 1900
        LIMIT 10
    """).fetchall()
    if bad:
        print(f"  BAD SYMBOLS (year out of range): {bad}")
        print("  These files have corrupt or mis-encoded open_time values")
        # Show the raw open_time from one bad symbol
        bad_sym = bad[0][0]
        raw = con.execute(f"""
            SELECT open_time, typeof(open_time)
            FROM read_parquet('{str(HIST / bad_sym / "1m" / (test_date + ".parquet"))}')
            LIMIT 3
        """).fetchall()
        print(f"  raw open_time from {bad_sym}: {raw}")
    else:
        print("  no obviously bad dates found — issue may be in pandas conversion step")
        # Try the conversion with explicit error handling
        dates_ts = pd.to_datetime(df["date"])
        print(f"  pd.to_datetime dtype={dates_ts.dtype}  min={dates_ts.min()}  max={dates_ts.max()}")
        df["date"] = dates_ts.dt.date
        print(f"  OK  conversion succeeded: {df['date'].head(3).tolist()}")
except Exception as e:
    print(f"  FAIL: {e}"); import traceback; traceback.print_exc(); sys.exit(1)

# ── Step 5: pyarrow schema cast + temp write ──────────────────────────────────
print("\n--- step 5: pyarrow schema cast + write")
import pyarrow as pa, pyarrow.parquet as pq

SCHEMA = pa.schema([
    pa.field("date",         pa.date32()),
    pa.field("symbol",       pa.string()),
    pa.field("open",         pa.float64()),
    pa.field("high",         pa.float64()),
    pa.field("low",          pa.float64()),
    pa.field("close",        pa.float64()),
    pa.field("volume",       pa.float64()),
    pa.field("quote_volume", pa.float64()),
    pa.field("num_trades",   pa.int64()),
    pa.field("vwap",         pa.float64()),
])
try:
    table = pa.Table.from_pandas(df, schema=SCHEMA, safe=False)
    out_dir = ROOT / "klines"
    out_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=out_dir, suffix=".tmp", delete=False) as tf:
        tmp = Path(tf.name)
    pq.write_table(table, tmp, compression="zstd")
    tmp.unlink()
    print(f"  OK  {len(table)} rows written (temp file cleaned up)")
except Exception as e:
    print(f"  FAIL: {e}"); import traceback; traceback.print_exc(); sys.exit(1)

print("\nALL STEPS PASSED")
