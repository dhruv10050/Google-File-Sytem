#!/bin/bash
#SBATCH --job-name=gfs-snapshot
#SBATCH --partition=debug
#SBATCH --qos=normal
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --time=45:00
#SBATCH --output=gfs-snapshot-%j.out
#
# Snapshots (paper §3.4) vs. copying the data through a client, the cost of
# copy-on-write on the first write to a shared chunk, and snapshots of a
# directory of many small files. Runs `gfs-bench snapshot` on the master's
# node (no chunkserver there, so the copy crosses the network), then checks
# a snapshot of the whole dataset byte for byte against what was written.

source "$(dirname "$0")/lib.sh" 2>/dev/null || source "$HOME/gfs/slurm/lib.sh"

DS_FILES=${DS_FILES:-8}
DS_BYTES=${DS_BYTES:-2G}
RUNS=${RUNS:-3}
COW_FILES=${COW_FILES:-3}
MANY=${MANY:-1000}

resolve_ips
record_env
echo "DS_FILES=$DS_FILES DS_BYTES=$DS_BYTES RUNS=$RUNS COW_FILES=$COW_FILES MANY=$MANY" >>"$OUT/env.txt"

start_cluster --rebalance-interval 0

log "writing dataset: $DS_FILES x $DS_BYTES"
clients dataset "$DS_FILES" write -masters "$MASTER" -path "/bench/ds/f{id}" -bytes "$DS_BYTES" -op 8M
log "dataset: $(agg dataset)"

log "snapshot benchmark"
"$BIN/gfs-bench" snapshot -masters "$MASTER" -ds /bench/ds -files "$DS_FILES" -runs "$RUNS" -cow-files "$COW_FILES" -many "$MANY" \
	>"$OUT/raw/snapshot.csv" 2>"$OUT/logs/snapshot.err" || log "WARNING: snapshot benchmark failed (see logs/snapshot.err)"
awk -F, '{split($7, a, ";"); k = $1 " " a[1]; n[k]++; t[k] += ($6 - $5) / 1e6}
	END {for (k in n) printf "  %-28s n=%-3d mean %.2f ms\n", k, n[k], t[k] / n[k]}' "$OUT/raw/snapshot.csv" | sort
grep '^check' "$OUT/raw/snapshot.csv" || true

log "checking snapshot ds-r1 against the original dataset (every byte)"
clients verify-snap 6 verify -masters "$MASTER" -prefix /bench/snap/ds-r1/f -content-of /bench/ds/f -files "$DS_FILES" -of 6 -expect-bytes "$DS_BYTES" || true
cat "$OUT/raw/verify-snap.csv"
"$BIN/gfs" -masters "$MASTER" status >"$OUT/status-final.txt" 2>&1 || true
log done
