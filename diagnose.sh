#!/usr/bin/env bash
# ── binance-data health check ────────────────────────────────────────────────
# One question, answered end to end: is the system *finalising* daily closes, or
# is it silently frozen at the stream's provisional snapshot?
#
# The stream (container) keeps the live buffer fresh and writes a provisional
# close near midnight — but it does NOT download Binance's official daily archives
# or roll them into daily_closes.parquet. That is `main.py update`, which must run
# on a cron (see deploy/binance-update.cron). If that cron is missing, everything
# below looks "up" while daily_closes.parquet quietly rots at provisional and
# downstream signals go NaN.
#
# Usage:   ./diagnose.sh              (run on the Docker host)
#          BINANCE_CONTAINER=foo ./diagnose.sh
#
# Read-only: runs `docker ps/inspect/exec` and reads files; changes nothing.
set -uo pipefail

CONTAINER="${BINANCE_CONTAINER:-binance-data}"
CLOSES="/data/klines/processed/daily_closes.parquet"

bold() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }
note() { printf '   \033[2m%s\033[0m\n' "$1"; }

exec_in() { docker exec "$CONTAINER" "$@"; }

if ! docker inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "container '$CONTAINER' not found — set BINANCE_CONTAINER or start the stack." >&2
  exit 1
fi

# ── A. Container up & healthy (the stream) ────────────────────────────────────
bold "A. Container up & healthy (the stream)"
docker ps --filter "name=^/${CONTAINER}$" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
docker inspect -f 'health={{if .State.Health}}{{.State.Health.Status}}{{else}}n/a{{end}}  restarts={{.RestartCount}}  started={{.State.StartedAt}}' "$CONTAINER"
note "want: Up (healthy), low restarts. Restarting/high restarts = stream crash-looping."

# ── B. Stream ingesting (live buffer freshness) ───────────────────────────────
bold "B. Stream ingesting (live buffer freshness)"
exec_in sh -c "find /data/klines/live -name '*.parquet' -printf '%T@ %TY-%Tm-%Td %TH:%TM  %p\n' 2>/dev/null | sort -n | tail -3" || true
note "the newest live file should be < ~2 min old; if it is minutes/hours stale the stream is stalled."

# ── C. Daily closes — finalized or frozen at provisional? (the crux) ──────────
bold "C. Daily closes — finalized or frozen at provisional?"
exec_in sh -c "ls -l --time-style=long-iso $CLOSES 2>/dev/null" || note "daily_closes.parquet missing!"
exec_in python - "$CLOSES" <<'PY' 2>/dev/null || note "(could not read daily_closes.parquet)"
import sys, pandas as pd
df = pd.read_parquet(sys.argv[1])
df["date"] = pd.to_datetime(df["date"]).dt.date
print("   date          rows  provisional")
for d in sorted(df["date"].unique())[-6:]:
    s = df[df["date"] == d]
    print(f"   {d}   {len(s):5d}   {int(s['provisional'].sum()):5d}")
prov_days = sorted(df.loc[df["provisional"], "date"].unique())
if prov_days:
    print(f"   ! {len(prov_days)} day(s) still provisional (newest {prov_days[-1]}) "
          "— these finalize only when `main.py update` runs")
PY
note "trailing days should reach provisional=0 within ~1-2 days. A persistently high"
note "provisional count = the finalize job (main.py update) is not running."

# ── D. Is the finalize job actually scheduled? ────────────────────────────────
bold "D. Finalize job (main.py update) scheduled?"
found=""
for who in "" "sudo"; do
  out=$($who crontab -l 2>/dev/null | grep -E "main.py update" || true)
  [ -n "$out" ] && { echo "   [${who:-$USER} crontab] $out"; found=1; }
done
[ -z "$found" ] && echo "   !! NO cron entry runs 'main.py update' — closes will freeze at provisional."
[ -z "$found" ] && note "install it:  ( sudo crontab -l 2>/dev/null; cat deploy/binance-update.cron ) | sudo crontab -"
if command -v systemctl >/dev/null 2>&1; then
  systemctl list-timers --all 2>/dev/null | grep -iE 'binance|klines|update' && found=1 || true
fi
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$REPO/logs/update.log" ] && { echo "   last update.log lines:"; tail -3 "$REPO/logs/update.log" | sed 's/^/     /'; }

# ── E. Store status summary ───────────────────────────────────────────────────
bold "E. Store status summary"
exec_in python main.py status || true
note "'Missing yesterday' near the full symbol count is normal on the current UTC day"
note "(Binance publishes each day's archive 1-2 days late); it should shrink day over day."

echo
