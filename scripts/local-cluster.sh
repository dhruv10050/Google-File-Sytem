#!/usr/bin/env bash
# Runs a GFS cluster as separate OS processes on this machine:
# one master, one shadow master and N chunkservers.
#
#   scripts/local-cluster.sh start [N]   build, then start (default N=4)
#   scripts/local-cluster.sh stop        kill everything (SIGKILL: a crash)
#   scripts/local-cluster.sh kill-cs I   kill -9 chunkserver I
#   scripts/local-cluster.sh start-cs I  restart chunkserver I
#   scripts/local-cluster.sh kill-master / start-master
#
# State lives in ./tmp/cluster (data dirs, logs, pids).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=tmp/cluster
BIN=bin
MASTER=127.0.0.1:7000
SHADOW=127.0.0.1:7100
HB=${HB:-500ms}
CHUNK_MB=${CHUNK_MB:-64}

mkdir -p "$ROOT/logs" "$ROOT/pids"

start_master() {
  "$BIN/gfs-master" --listen "$MASTER" --dir "$ROOT/master" --heartbeat "$HB" --chunk-mb "$CHUNK_MB" \
    >>"$ROOT/logs/master.log" 2>&1 &
  echo $! >"$ROOT/pids/master"
}
start_shadow() {
  "$BIN/gfs-master" --listen "$SHADOW" --dir "$ROOT/shadow" --heartbeat "$HB" --chunk-mb "$CHUNK_MB" \
    --shadow --primary "$MASTER" >>"$ROOT/logs/shadow.log" 2>&1 &
  echo $! >"$ROOT/pids/shadow"
}
start_cs() {
  local i=$1
  "$BIN/gfs-chunkserver" --listen "127.0.0.1:$((7001 + i))" --dir "$ROOT/cs$i" \
    --masters "$MASTER,$SHADOW" >>"$ROOT/logs/cs$i.log" 2>&1 &
  echo $! >"$ROOT/pids/cs$i"
}
kill_pid() {
  local f="$ROOT/pids/$1"
  if [[ -f $f ]]; then kill -9 "$(cat "$f")" 2>/dev/null || true; fi
  rm -f "$f"
}

case "${1:-}" in
  start)
    n=${2:-4}
    go build -o "$BIN/" ./cmd/...
    start_master
    start_shadow
    for ((i = 0; i < n; i++)); do start_cs "$i"; done
    sleep 1
    echo "cluster up: master $MASTER, shadow $SHADOW, $n chunkservers (logs in $ROOT/logs)"
    echo "try: GFS_MASTERS=$MASTER,$SHADOW $BIN/gfs status"
    ;;
  stop)
    for f in "$ROOT"/pids/*; do
      if [[ -e $f ]]; then kill_pid "$(basename "$f")"; fi
    done
    ;;
  kill-cs) kill_pid "cs$2" ;;
  start-cs) start_cs "$2" ;;
  kill-master) kill_pid master ;;
  start-master) start_master ;;
  clean)
    "$0" stop
    rm -rf "$ROOT"
    ;;
  *)
    sed -n '2,12p' "$0"
    exit 2
    ;;
esac
