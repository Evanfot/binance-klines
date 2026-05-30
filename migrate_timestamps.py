#!/usr/bin/env python
"""
One-time migration: fix parquet files where timestamps were stored as
microseconds but labeled as milliseconds (pandas 2.x + PyArrow safe=False).

Safe to re-run — already-correct files are detected and skipped.
"""
from __future__ import annotations

import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from config import HISTORICAL_DIR

# A valid ms timestamp for any real Binance date (2017–2040) is < 2.2e12.
# Corrupted values (μs stored as ms) are ~1e15.
_CORRUPT_THRESHOLD_MS = 5_000_000_000_000  # year ~2128 in ms


def _fix_file(path_str: str) -> tuple[str, str]:
    """
    Read one parquet file, correct timestamps if corrupted, rewrite atomically.
    Returns (path, status) where status is 'fixed', 'ok', or 'error: <msg>'.
    """
    path = Path(path_str)
    try:
        table = pq.read_table(path)

        raw = table.column("open_time").cast(pa.int64())[0].as_py()
        if raw is None or raw <= _CORRUPT_THRESHOLD_MS:
            return path_str, "ok"

        for col in ("open_time", "close_time"):
            idx = table.schema.get_field_index(col)
            corrected = (
                pc.divide(table.column(col).cast(pa.int64()), 1000)
                .cast(pa.timestamp("ms", tz="UTC"))
            )
            table = table.set_column(idx, col, corrected)

        with tempfile.NamedTemporaryFile(
            dir=path.parent, suffix=".tmp", delete=False
        ) as tf:
            tmp = Path(tf.name)
        try:
            pq.write_table(table, tmp, compression="zstd")
            tmp.rename(path)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise

        return path_str, "fixed"

    except Exception as exc:
        return path_str, f"error: {exc}"


def main() -> None:
    files = sorted(HISTORICAL_DIR.rglob("*.parquet"))
    total = len(files)
    if not total:
        print("No parquet files found.")
        return

    print(f"Found {total:,} parquet files — starting migration...")
    t0 = time.monotonic()

    fixed = skipped = errors = 0

    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(_fix_file, str(f)): f for f in files}
        for done, future in enumerate(as_completed(futures), 1):
            _, status = future.result()
            if status == "fixed":
                fixed += 1
            elif status == "ok":
                skipped += 1
            else:
                errors += 1
                path = futures[future]
                print(f"  ERROR {path}: {status}")

            if done % 5000 == 0 or done == total:
                elapsed = time.monotonic() - t0
                rate = done / elapsed
                eta = (total - done) / rate if rate else 0
                print(
                    f"  {done:>7,}/{total:,}"
                    f"  fixed={fixed:,}"
                    f"  skipped={skipped:,}"
                    f"  errors={errors}"
                    f"  {rate:.0f} files/s"
                    f"  ETA {eta:.0f}s"
                )

    elapsed = time.monotonic() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Fixed  : {fixed:,}")
    print(f"  Already correct: {skipped:,}")
    print(f"  Errors : {errors}")


if __name__ == "__main__":
    main()
