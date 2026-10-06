#!/bin/bash
#SBATCH --job-name=gfs-meta
#SBATCH --partition=debug
#SBATCH --qos=normal
#SBATCH --nodes=6
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --time=45:00
#SBATCH --output=gfs-meta-%j.out
#
# Master metadata throughput: N concurrent clients create, then stat, then
# delete files. Creates and deletes are logged (one group-committed fsync
# per batch of records); stats are served from memory. Clients run on the
# chunkserver nodes only, so every request crosses the network: N clients
# are min(N, 5) processes, one per node, with N/processes client threads
# each. Every phase runs twice: each client in its own directory
# ("private"), and all clients in one directory ("shared"), which namespace
# locking (§4.1) should not serialize.

source "$(dirname "$0")/lib.sh" 2>/dev/null || source "$HOME/gfs/slurm/lib.sh"

NS=${NS:-"1 5 10 20 40 80"}
RUNS=${RUNS:-3}
OPS=${OPS:-20000}       # operations per phase, split over the clients...
MIN_OPS=${MIN_OPS:-1000} # ...but at least this many per client

CLIENT_NODES=("${CSNODES[@]}")
resolve_ips
record_env
echo "NS=$NS RUNS=$RUNS OPS=$OPS MIN_OPS=$MIN_OPS" >>"$OUT/env.txt"

log "master disk: synchronous 4 KiB writes (op log fsync baseline)"
step "$MNODE" bash -c "mkdir -p $DATA && dd if=/dev/zero of=$DATA/sync bs=4k count=2000 oflag=dsync 2>&1 | tail -1; rm -f $DATA/sync" |
	tee "$OUT/raw/disk-sync.txt"

start_cluster --trash-retention 0 --rebalance-interval 0

for run in $(seq 1 "$RUNS"); do
	for N in $NS; do
		procs=$((N < ${#CLIENT_NODES[@]} ? N : ${#CLIENT_NODES[@]}))
		threads=$((N / procs))
		count=$((OPS / N > MIN_OPS ? OPS / N : MIN_OPS))
		for layout in private shared; do
			sh=""
			[ "$layout" = shared ] && sh=-shared
			for op in create stat delete; do
				tag=meta-$op-$layout-n$N-r$run
				clients "$tag" "$procs" meta -masters "$MASTER" -op "$op" -dir "/bench/meta/r$run-n$N-$layout" $sh -threads "$threads" -count "$count" || true
				log "$tag: $(awk -F, '{n+=$4; if(s==""||$5<s)s=$5; if($6>e)e=$6} END{printf "%.0f ops/s (%d ops, %.2f s)", n/((e-s)/1e9), n, (e-s)/1e9}' "$OUT/raw/$tag.csv")"
			done
		done
	done
done
"$BIN/gfs" -masters "$MASTER" status >"$OUT/status-final.txt" 2>&1 || true
log done
