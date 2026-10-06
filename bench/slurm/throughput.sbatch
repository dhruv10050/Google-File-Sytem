#!/bin/bash
#SBATCH --job-name=gfs-throughput
#SBATCH --partition=debug
#SBATCH --qos=normal
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --time=45:00
#SBATCH --output=gfs-throughput-%j.out
#
# Read, write and record-append throughput vs. number of clients (paper
# §6.1.1–6.1.3), plus raw network and disk baselines for the same nodes.
#
#   reads:  N clients each read READ_BYTES as random 4 MB regions of a
#           DS_FILES x DS_BYTES dataset
#   writes: N clients each write WRITE_BYTES to their own new file in
#           OP_W-sized writes
#   append: N clients each append APPEND_BYTES as APPEND_RECORD-sized records
#           to one shared file; the file is then scanned to check that every
#           acknowledged record is present
#
# Both run in two client modes: "avoid" (no replica on the client's own node
# is read from or pushed to first, so every byte crosses the network) and
# "local" (normal nearest-replica behaviour).

source "$(dirname "$0")/lib.sh" 2>/dev/null || source "$HOME/gfs/slurm/lib.sh"

NS=${NS:-"1 2 4 8"}
RUNS=${RUNS:-3}
MODES=${MODES:-"avoid local"}
KINDS=${KINDS:-"read write"}
DS_FILES=${DS_FILES:-8}
DS_BYTES=${DS_BYTES:-2G}
READ_BYTES=${READ_BYTES:-2G}
WRITE_BYTES=${WRITE_BYTES:-1G}
OP_W=${OP_W:-1M}
APPEND_BYTES=${APPEND_BYTES:-512M}
APPEND_RECORD=${APPEND_RECORD:-1M}
NET_SECS=${NET_SECS:-10}

flag() { [ "$1" = avoid ] && echo -avoid-local || true; }

resolve_ips
record_env
{
	echo "NS=$NS RUNS=$RUNS MODES=$MODES KINDS=$KINDS DS_FILES=$DS_FILES DS_BYTES=$DS_BYTES"
	echo "READ_BYTES=$READ_BYTES WRITE_BYTES=$WRITE_BYTES OP_W=$OP_W NET_SECS=$NET_SECS"
	echo "APPEND_BYTES=$APPEND_BYTES APPEND_RECORD=$APPEND_RECORD"
} >>"$OUT/env.txt"

# ── Baselines ────────────────────────────────────────────────────────────
log "network baseline"
for n in "${NODES[@]}"; do
	step "$n" "$BIN/gfs-bench" net-serve -listen "${IP[$n]}:9100" >/dev/null 2>&1 &
	STEPS+=($!)
done
sleep 2
# one sender, one receiver
step "${NODES[0]}" "$BIN/gfs-bench" net-send -to "${IP[${NODES[1]}]}:9100" -seconds "$NET_SECS" >"$OUT/raw/net-pair.csv"
# every node sends to the next one at once (each NIC sends and receives)
t=$(($(date +%s%N) + 3000000000)) ring=()
for ((i = 0; i < ${#NODES[@]}; i++)); do
	to=${NODES[$(((i + 1) % ${#NODES[@]}))]}
	step "${NODES[$i]}" "$BIN/gfs-bench" net-send -to "${IP[$to]}:9100" -seconds "$NET_SECS" -id "$i" -start-at "$t" >"$OUT/raw/net-ring.$i.csv" &
	ring+=($!)
done
wait "${ring[@]}" || true
cat "$OUT"/raw/net-ring.*.csv >"$OUT/raw/net-ring.csv" && rm -f "$OUT"/raw/net-ring.*.csv
for p in "${STEPS[@]}"; do kill "$p" 2>/dev/null || true; done
wait 2>/dev/null || true
STEPS=()
log "net pair: $(agg net-pair)"
log "net ring: $(agg net-ring)"

log "disk baseline (2 GiB, fdatasync)"
for n in "${CSNODES[@]}"; do
	step "$n" bash -c "mkdir -p $DATA && dd if=/dev/zero of=$DATA/dd bs=8M count=256 conv=fdatasync 2>&1 | tail -1; rm -f $DATA/dd" |
		sed "s/^/$n,/" >>"$OUT/raw/disk.txt"
done
cat "$OUT/raw/disk.txt"

# ── GFS ──────────────────────────────────────────────────────────────────
start_cluster

log "writing read dataset: $DS_FILES x $DS_BYTES"
[[ " $KINDS " == *" read "* ]] || DS_BYTES=64M # only used for the final check
clients dataset "$DS_FILES" write -masters "$MASTER" -path "/bench/ds/f{id}" -bytes "$DS_BYTES" -op 8M
log "dataset: $(agg dataset)"

for run in $(seq 1 "$RUNS"); do
	for N in $NS; do
		for mode in $MODES; do
			if [[ " $KINDS " == *" read "* ]]; then
				tag=read-$mode-n$N-r$run
				clients "$tag" "$N" read -masters "$MASTER" -files "$DS_FILES" -bytes "$READ_BYTES" -op 4M $(flag "$mode") || true
				log "$tag: $(agg "$tag")"
			fi
			if [[ " $KINDS " == *" append "* ]]; then
				tag=append-$mode-n$N-r$run
				clients "$tag" "$N" append -masters "$MASTER" -path "/bench/a/$tag" -bytes "$APPEND_BYTES" -record "$APPEND_RECORD" $(flag "$mode") || true
				log "$tag: $(agg "$tag")"
				"$BIN/gfs-bench" scan -masters "$MASTER" -path "/bench/a/$tag" >"$OUT/raw/$tag.scan" 2>&1 || true
				# every acknowledged record must be found: writer i acked records=k -> seq 0..k-1 present
				awk -F, 'FNR==NR { split($7, a, "="); acked[$2] = a[2]; next }
					$1 == "writer" { found[$2] = $3; ok[$2] = $4 }
					$1 == "scan" { summary = $7 }
					END { bad = 0; for (w in acked) if (found[w] < acked[w] || ok[w] != "true") bad++
					      printf "scan: %s; writers missing records: %d\n", summary, bad }' \
					"$OUT/raw/$tag.csv" "$OUT/raw/$tag.scan" | tee -a "$OUT/raw/$tag.scan"
				"$BIN/gfs" -masters "$MASTER" rm "/bench/a/$tag" >/dev/null 2>&1 || true
			fi
			[[ " $KINDS " == *" write "* ]] || continue
			tag=write-$mode-n$N-r$run
			clients "$tag" "$N" write -masters "$MASTER" -path "/bench/w/$tag/f{id}" -bytes "$WRITE_BYTES" -op "$OP_W" $(flag "$mode") || true
			log "$tag: $(agg "$tag")"
			for ((i = 0; i < N; i++)); do "$BIN/gfs" -masters "$MASTER" rm "/bench/w/$tag/f$i" >/dev/null 2>&1 || true; done
		done
	done
done

log "final read check of the dataset (every byte, one replica)"
clients verify-final 6 verify -masters "$MASTER" -files "$DS_FILES" -of 6 -expect-bytes "$DS_BYTES" || true
cat "$OUT/raw/verify-final.csv"
"$BIN/gfs" -masters "$MASTER" status >"$OUT/status-final.txt" 2>&1 || true
log done
