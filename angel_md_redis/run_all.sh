#!/usr/bin/env bash
set -euo pipefail

BASE="/home/jagruti/Documents/smartapi/angel_md_redis"
cd "$BASE"

# venv
source "$BASE/venv/bin/activate"

# timezone for dt=YYYY-MM-DD folders (optional)
export ARCHIVE_TZ="Asia/Kolkata"

# better logs
export PYTHONUNBUFFERED=1

# start redis
docker compose up -d

DAY="$(date +%F)"
LOGDIR="$BASE/logs/$DAY"
PIDDIR="$LOGDIR/pids"
mkdir -p "$PIDDIR"

start() {
  local name="$1"; shift
  echo "Starting $name ..."
  nohup "$@" >> "$LOGDIR/$name.log" 2>&1 &
  echo $! > "$PIDDIR/$name.pid"
}

# 1) Producer: WS -> Redis (eq + opt ticks)
start "producer" python3 run_producer.py

# 2) Greeks: REST -> Redis (needs md:active_expiry from producer)
start "greeks" python3 run_greeks_only.py

# 3) Joiner: opt ticks + latest greeks -> features stream
start "joiner" python3 run_joiner.py

# 4) Archivers: Redis streams -> data_lake/stream=.../dt=YYYY-MM-DD/...
start "arch_eq"       python3 run_archiver_all.py eq
start "arch_opt"      python3 run_archiver_all.py opt
start "arch_greeks"   python3 run_archiver_all.py greeks
start "arch_features" python3 run_archiver_all.py features

echo
echo "All started."
echo "Logs: $LOGDIR"
echo "PIDs: $PIDDIR"
echo "To stop everything:"
echo "  kill \$(cat $PIDDIR/*.pid)"