#!/usr/bin/env bash
# End-to-end check with real processes: put/get, kill -9 a chunkserver and
# wait for re-replication, crash and restart the master, and read through
# the shadow while the primary is down.
set -euo pipefail
cd "$(dirname "$0")/.."
export CHUNK_MB=4 HB=200ms
export GFS_MASTERS=127.0.0.1:7000,127.0.0.1:7100
C=scripts/local-cluster.sh
T=tmp/smoke
gfs() { bin/gfs "$@"; }

wait_for() { # wait_for <seconds> <description> <command...>
  local secs=$1 what=$2; shift 2
  for ((i = 0; i < secs * 5; i++)); do "$@" >/dev/null 2>&1 && return 0; sleep 0.2; done
  echo "FAIL: timed out waiting for $what"; "$@" || true; gfs status || true; exit 1
}
# Match on captured output: with pipefail, `cmd | grep -q` can fail when
# grep exits early and cmd gets SIGPIPE.
fully_replicated() { [[ $(gfs status) == *"under-replicated: 0"* ]]; }

$C clean >/dev/null; mkdir -p $T
trap '$C stop' EXIT
$C start 5
wait_for 10 "5 chunkservers" sh -c "bin/gfs status | grep -c '127.0.0.1:70' | grep -qx 5"

head -c 50000000 /dev/urandom >$T/in
gfs mkdir /smoke
gfs put $T/in /smoke/f
gfs get /smoke/f $T/out && cmp $T/in $T/out && echo "ok: put/get 50 MB"
gfs snapshot /smoke /snap
echo "changed" | gfs write /snap/f 0
gfs get /smoke/f $T/out && cmp $T/in $T/out && [[ $(gfs read /snap/f 0 7) == changed ]] && echo "ok: snapshot is copy-on-write"

$C kill-cs 0
wait_for 30 "re-replication after kill -9" sh -c "bin/gfs status | grep -c '127.0.0.1:70' | grep -qx 4 && bin/gfs status | grep -q 'under-replicated: 0'"
gfs get /smoke/f $T/out && cmp $T/in $T/out && echo "ok: re-replicated after kill -9, data intact"

$C kill-master
[[ $(gfs ls /smoke) == *f* ]] && echo "ok: shadow serves ls with primary down"
gfs get /smoke/f $T/out && cmp $T/in $T/out && echo "ok: shadow serves reads with primary down"
if gfs mkdir /nope 2>/dev/null; then echo "FAIL: write accepted without primary"; exit 1; fi

$C start-master
wait_for 15 "master back" fully_replicated
gfs get /smoke/f $T/out && cmp $T/in $T/out && echo "ok: master recovered from crash"
echo "hello" | gfs write /smoke/f 0
[[ $(gfs read /smoke/f 0 6) == hello ]] && echo "ok: writes after master restart"
gfs status
echo "smoke test passed"
