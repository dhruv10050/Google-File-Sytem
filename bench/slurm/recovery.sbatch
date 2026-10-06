#!/bin/bash
#SBATCH --job-name=gfs-recovery
#SBATCH --partition=debug
#SBATCH --qos=normal
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --time=45:00
#SBATCH --output=gfs-recovery-%j.out
#
# Recovery time (paper §6.2.2): fill the cluster with DS_FILES x DS_BYTES,
# kill -9 KILL of the chunkservers at once, and time how long the master
# takes to restore every chunk to 3 replicas. Afterwards every replica of
# every chunk is read back and compared byte for byte with what was written.

source "$(dirname "$0")/lib.sh" 2>/dev/null || source "$HOME/gfs/slurm/lib.sh"

DS_FILES=${DS_FILES:-10}
DS_BYTES=${DS_BYTES:-2G}
KILL=${KILL:-2}
WRITERS=${WRITERS:-$DS_FILES}

resolve_ips
record_env
echo "DS_FILES=$DS_FILES DS_BYTES=$DS_BYTES KILL=$KILL" >>"$OUT/env.txt"

# Defaults: 1 s heartbeats, dead after 5 missed, 4 clones per server.
start_cluster --rebalance-interval 0

log "writing dataset: $DS_FILES x $DS_BYTES"
clients dataset "$WRITERS" write -masters "$MASTER" -path "/bench/ds/f{id}" -bytes "$DS_BYTES" -op 8M
log "dataset: $(agg dataset)"
sleep 3 # let every chunkserver report its chunks in a heartbeat
"$BIN/gfs" -masters "$MASTER" status >"$OUT/status-before.txt" 2>&1

mapfile -t VICTIMS < <(printf '%s\n' "${CSNODES[@]}" | shuf -n "$KILL")
LIVE=$((${#CSNODES[@]} - KILL))
echo "victims=${VICTIMS[*]}" >>"$OUT/env.txt"

"$BIN/gfs-bench" watch -masters "$MASTER" -expect-servers "$LIVE" -interval 100ms -timeout 30m >"$OUT/raw/watch.csv" &
WATCH=$!
sleep 1

log "kill -9 chunkservers on ${VICTIMS[*]}"
date +%s%N >"$OUT/raw/kill-issued.txt"
kp=()
for v in "${VICTIMS[@]}"; do
	kill_cs "$v" | sed "s/^/$v,/" >"$OUT/raw/kill.$v.csv" &
	kp+=($!)
done
wait "${kp[@]}"
cat "$OUT"/raw/kill.*.csv >"$OUT/raw/kill.csv" && rm -f "$OUT"/raw/kill.*.csv

wait $WATCH
"$BIN/gfs" -masters "$MASTER" status >"$OUT/status-after.txt" 2>&1

awk -F, -v live="$LIVE" '
	FNR==NR { if (k==""||$2<k) k=$2; next }
	$1=="recovered" { r=$2; next }
	d=="" && $2==live { d=$1 }
	END { printf "kill->detected %.1f s, detected->recovered %.1f s, kill->recovered %.1f s\n", (d-k)/1e9, (r-d)/1e9, (r-k)/1e9 }
' "$OUT/raw/kill.csv" "$OUT/raw/watch.csv" | tee "$OUT/recovery.txt"
tail -2 "$OUT/raw/watch.csv"

log "verifying every replica of every chunk"
clients verify 6 verify -masters "$MASTER" -files "$DS_FILES" -of 6 -all-replicas -replicas 3 -expect-bytes "$DS_BYTES" || true
cat "$OUT/raw/verify.csv"
awk -F, '{b+=$4; split($7,a,";"); for(i in a){split(a[i],kv,"="); s[kv[1]]+=kv[2]}} END{printf "verified %.2f GB in %d replica reads of %d chunks, %d problems\n", b/1e9, s["replica_reads"], s["chunks"], s["problems"]}' "$OUT/raw/verify.csv" | tee -a "$OUT/recovery.txt"
log done
