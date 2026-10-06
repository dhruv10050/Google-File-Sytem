# Sourced by the bench/slurm/*.sbatch scripts. Runs inside a SLURM
# allocation: the node running the batch script hosts the master, every
# other allocated node hosts one chunkserver, and benchmark clients are
# placed round-robin over all nodes starting with the master's.
#
# Every long-running process is its own `srun --overlap` step: background
# processes of a finished step are killed by proctrack/cgroup.

set -euo pipefail

BIN=${BIN:-$HOME/gfs/bin}
OUT=${OUT:-$HOME/gfs/results/$SLURM_JOB_ID}
# Node-local disk. /scratch is not writable on every node (group-restricted
# on two, nearly full on one); /tmp is local on all of them.
DATA=${DATA_ROOT:-/tmp}/$USER-gfs-$SLURM_JOB_ID
PORT_M=7000
PORT_CS=7001

mkdir -p "$OUT/logs" "$OUT/raw"

mapfile -t ALL < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
MNODE=$SLURMD_NODENAME
NODES=("$MNODE")
for n in "${ALL[@]}"; do [ "$n" = "$MNODE" ] || NODES+=("$n"); done
CSNODES=("${NODES[@]:1}")

declare -A IP
STEPS=()

log() { echo "[$(date +%H:%M:%S)] $*"; }

# step NODE CMD... runs CMD as a one-task job step on NODE.
step() {
	local node=$1
	shift
	srun --overlap -N1 -n1 -w "$node" "$@"
}

# Each node is addressed by its 1 GbE address on 172.16.0.0/24 (the
# interface name differs between nodes); InfiniBand is down.
resolve_ips() {
	for n in "${NODES[@]}"; do
		IP[$n]=$(step "$n" bash -c "ip -4 -o addr | awk '\$4 ~ /^172\\.16\\.0\\./ { split(\$4, a, \"/\"); print a[1] }'")
		[ -n "${IP[$n]}" ] || { echo "no 172.16.0.x address on $n" >&2; exit 1; }
	done
	MASTER=${IP[$MNODE]}:$PORT_M
	for n in "${NODES[@]}"; do echo "$n,${IP[$n]}"; done >"$OUT/nodes.csv"
}

record_env() {
	{
		echo "job=$SLURM_JOB_ID"
		echo "nodes=${NODES[*]}"
		echo "master=$MNODE"
		echo "chunkservers=${CSNODES[*]}"
		echo "started=$(date -Is)"
	} >"$OUT/env.txt"
	for n in "${NODES[@]}"; do
		step "$n" bash -c 'echo "== $(hostname -s)"; nproc; free -g | sed -n 2p; df -h /tmp | tail -1; for i in /sys/class/net/eno*; do echo "${i##*/} $(cat $i/speed 2>/dev/null)"; done; chronyc tracking 2>/dev/null | grep -E "System time|Last offset" || true' >>"$OUT/env.txt" 2>&1 || true
	done
}

# start_cluster [master flags...] starts the master and one chunkserver per
# chunkserver node, then waits until all of them have registered.
start_cluster() {
	for n in "${NODES[@]}"; do step "$n" bash -c "rm -rf $DATA && mkdir -p $DATA"; done
	step "$MNODE" "$BIN/gfs-master" --listen "${IP[$MNODE]}:$PORT_M" --dir "$DATA/master" "$@" \
		>"$OUT/logs/master.log" 2>&1 &
	STEPS+=($!)
	for n in "${CSNODES[@]}"; do
		local a=${IP[$n]}:$PORT_CS
		# exec keeps the PID, so kill_cs can kill -9 exactly this process.
		step "$n" bash -c "echo \$\$ >$DATA/cs.pid; exec $BIN/gfs-chunkserver --listen $a --advertise $a --dir $DATA/cs --masters $MASTER" \
			>"$OUT/logs/cs-$n.log" 2>&1 &
		STEPS+=($!)
	done
	"$BIN/gfs-bench" watch -masters "$MASTER" -expect-servers "${#CSNODES[@]}" -interval 250ms -timeout 60s >/dev/null
	log "cluster up: master $MNODE, chunkservers ${CSNODES[*]}"
}

# kill_cs NODE kill -9s the chunkserver on NODE and prints the kill time
# (unix ns, NODE's clock).
kill_cs() {
	step "$1" bash -c "date +%s%N; kill -9 \$(cat $DATA/cs.pid)"
}

stop_cluster() {
	for p in "${STEPS[@]}"; do kill "$p" 2>/dev/null || true; done
	wait "${STEPS[@]}" 2>/dev/null || true
	STEPS=()
	for n in "${NODES[@]}"; do step "$n" rm -rf "$DATA" || true; done
}

trap stop_cluster EXIT

# clients TAG N CMD... runs N copies of `gfs-bench CMD... -id i -start-at T`
# on nodes chosen round-robin, all starting at the same wall-clock time T.
# Each client's CSV line goes to $OUT/raw/TAG.csv; returns non-zero if any
# client failed.
clients() {
	local tag=$1 n=$2
	shift 2
	local t=$(($(date +%s%N) + 4000000000)) pids=() rc=0
	for ((i = 0; i < n; i++)); do
		local node=${NODES[$((i % ${#NODES[@]}))]}
		step "$node" "$BIN/gfs-bench" "$@" -id "$i" -start-at "$t" \
			>"$OUT/raw/$tag.$i.csv" 2>"$OUT/logs/$tag.$i.err" &
		pids+=($!)
	done
	for p in "${pids[@]}"; do wait "$p" || rc=1; done
	cat "$OUT/raw/$tag".*.csv >"$OUT/raw/$tag.csv"
	rm -f "$OUT/raw/$tag".*.csv
	[ $rc = 0 ] || log "WARNING: a client of $tag failed (see logs/$tag.*.err)"
	return $rc
}

# agg TAG prints aggregate MB/s for a finished clients() run:
# total bytes / (last end - first start).
agg() {
	awk -F, '{b+=$4; if(s==""||$5<s)s=$5; if($6>e)e=$6} END{printf "%.1f MB/s (%d clients, %.1f s)\n", b/1e6/((e-s)/1e9), NR, (e-s)/1e9}' "$OUT/raw/$1.csv"
}
