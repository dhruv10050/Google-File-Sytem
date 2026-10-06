# Google File System in Go

A from-scratch implementation of the [Google File System](https://research.google/pubs/the-google-file-system/) (Ghemawat, Gobioff, Leung — SOSP 2003) in Go, using gRPC and Protocol Buffers. The paper is the spec: section numbers like (§4.5) in the code and below point to the part of the paper each mechanism implements.

> **Status:** the system described in the paper is implemented and tested: the master with its operation log, checkpoints and shadow; chunkservers; 3-way replication; leases; heartbeats; re-replication; checksums; stale-replica detection; record append; copy-on-write snapshots; lazy garbage collection; rebalancing; namespace locking. It has been benchmarked on a 6-node SLURM cluster; see [Benchmarks](#benchmarks).

## Architecture

```
                 metadata only                         ┌──────────────────────┐
   ┌────────┐  ───────────────────────────────────────▶│ master               │
   │ client │  ◀── chunk handle, version, replicas ────│  namespace + chunks  │
   │ (lib)  │                                          │  op log + checkpoint │──▶ shadow master
   └────────┘                                          └──────────▲───────────┘    (tails the log,
     │   ▲      data: pushed along a chain, then                  │ heartbeats       serves reads)
     │   │      committed by the primary                          │ reports, leases
     ▼   │                                                        │
   ┌────────────┐ push ┌────────────┐ push ┌────────────┐         │
   │chunkserver │─────▶│chunkserver │─────▶│chunkserver │─────────┘
   │  primary   │      │ secondary  │      │ secondary  │
   └────────────┘      └────────────┘      └────────────┘
       64 MB chunks on local disk, CRC32C per 64 KB block
```

**Master** (`internal/master`) holds all metadata in memory: the namespace, the file→chunk mapping, and each chunk's version and reference count.
- The master is a deterministic state machine. Every mutation is a log record applied by one function, `State.Apply`. Normal operation, crash recovery and the shadow master all run that same code.
- Records are group-committed to an fsynced, CRC-framed operation log. No RPC replies until everything it observed is durable (§2.6.3).
- Checkpoints are protobuf snapshots written atomically. Older log segments are removed once a checkpoint covers them.
- Replica locations are never persisted. They are rebuilt from chunkserver reports (§2.6.2).

**Chunkservers** (`internal/chunkserver`) store each chunk as a plain file plus a `.meta` sidecar holding its version, length and a CRC32C per 64 KB block.
- Every read verifies the checksums of the blocks it touches.
- A partial overwrite verifies the edge blocks first (§5.2).
- A corrupted replica returns an error, is reported to the master and gets replaced.

**Client library** (`pkg/client`) caches chunk locations and lease holders, and moves all data directly to and from chunkservers.

### The write path (§3.1–3.2)

1. The client asks the master which replica holds the lease for the chunk. If nobody holds one, the master bumps the chunk version on every live replica, logs the new version, and grants a 60 s lease to one of them, the *primary*.
2. The client pushes the data along a chain of replicas, nearest first. Each chunkserver forwards frames to the next hop as they arrive, so the transfer is pipelined.
3. The client sends the write request to the primary. The primary assigns it a serial number, applies it, and forwards it to the secondaries in that order. Any failure is returned to the client, which retries.

### Failure handling

| Failure | Mechanism |
|---|---|
| Chunkserver dies | Heartbeats stop, and the master declares it dead after 5 missed intervals. Chunks below their replication goal are cloned, those missing the most replicas first, with a per-server clone limit (§4.3). |
| Replica misses mutations while down | Its version falls behind. The master treats it as stale: it never hands it to clients, and it is garbage-collected and re-replicated (§4.5). Clients also pass the version they expect, so a stale replica refuses to serve. |
| Disk corruption | Detected by checksum on read. The replica stops serving, the master clones a good copy and then deletes the bad one (§5.2). |
| Master crash | Restart loads the latest checkpoint, replays the log after it (truncating a torn final record) and checks invariants. Chunkservers re-register, and replicas found to be ahead of the log are adopted (§4.5). |
| Master down | The client falls back to the shadow master for metadata reads. Writes wait for the primary (§5.1.3). |

### Record append, snapshots, deletion

| Feature | How it works |
|---|---|
| Record append (§3.3) | The client pushes the record and the primary picks the offset: the current end of its replica. Every secondary writes the record at that same offset. If the record doesn't fit, all replicas pad the chunk and the client retries on the next chunk. Records are limited to a quarter chunk. A failed attempt is retried, so each record is written atomically at least once and may be duplicated. |
| Snapshots (§3.4) | The master revokes the leases on the source's chunks, then logs a record that copies the metadata and raises each chunk's reference count. The first write to a shared chunk makes every replica copy it *locally* under a new handle before a lease is granted, so snapshot data never crosses the network. |
| Deletion and GC (§4.4) | `rm` renames the file to a hidden `/.deleted/<time>-<lsn>-<name>`. It stays readable there and can be restored with `undelete`. A periodic scan removes hidden files older than `--trash-retention` (default 3 days). Their chunks become orphans, which chunkservers delete once a heartbeat reports them. |
| Rebalancing (§4.3) | When nothing is under-replicated, the master trims extra replicas (e.g. from a chunkserver that came back after its chunks were re-replicated) and moves replicas from the fullest to the emptiest chunkserver, a few at a time, so new servers fill up gradually. |
| Namespace locking (§4.1) | Per-path read/write locks, created on demand. An operation read-locks the ancestors of every path it touches and write-locks the paths themselves. A snapshot therefore blocks creations under its source, while creates in one directory run concurrently. Locks are taken in one global order, so they cannot deadlock. |

### Where this differs from the paper

- **Lease fencing by version.** The paper waits for an old lease to expire before granting a new one. Here, granting a lease always bumps the chunk version on the replicas first. A replica that sees a new version drops any lease it holds, and secondaries reject mutations carrying an old version. So a new primary can be chosen immediately, without waiting up to 60 s.
- **A lease is void once any replica it covered is lost.** If a secondary dies mid-lease, retried writes can succeed on the remaining replicas without changing the version. The dead replica would then come back looking current. Voiding the lease forces the next write to bump the version, which marks the returning replica stale. There is a regression test for this: `TestStaleReplicaAfterRestart`.
- **One in-flight mutation per chunk at the primary.** This keeps the ordering of mutations at the secondaries simple. Data pushes from many clients still overlap.
- **Rebalancing counts replicas, not bytes.** The paper balances disk space. With mostly full 64 MB chunks, the two agree, and replica counts change the moment the master moves one rather than a heartbeat later.

## Layout

```
proto/gfs.proto          gRPC services + persistent formats (log records, checkpoints, chunk metadata)
api/gfspb/               generated code (committed)
internal/master/         state machine, op log, checkpoints, leases, heartbeats, re-replication, snapshots, GC, rebalancing, namespace locks, shadow
internal/chunkserver/    chunk store with checksums, data buffer, mutation pipeline, cloning
internal/common/         shared constants, path helpers, gRPC dialing
pkg/client/              client library
cmd/gfs-master           master / shadow master binary
cmd/gfs-chunkserver      chunkserver binary
cmd/gfs                  CLI
cmd/gfs-bench            benchmark client (read/write/append workloads, verifiers, network test)
internal/testcluster/    in-process cluster harness + integration and failure tests
scripts/                 local multi-process cluster and smoke test
bench/                   SLURM benchmark jobs, analysis script, results
```

## Build

You need Go 1.25 or newer. `protoc` is only needed if you change the `.proto` file.

```sh
go build -o bin/ ./cmd/...

# static Linux binaries for a cluster
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o dist/linux-amd64/ ./cmd/...

# regenerate gRPC code after editing proto/gfs.proto
protoc -I proto --go_out=api/gfspb --go_opt=paths=source_relative \
  --go-grpc_out=api/gfspb --go-grpc_opt=paths=source_relative proto/gfs.proto
```

## Run locally

```sh
scripts/local-cluster.sh start 4          # master :7000, shadow :7100, 4 chunkservers
export GFS_MASTERS=127.0.0.1:7000,127.0.0.1:7100

bin/gfs mkdir -p /data/logs
bin/gfs put ./bigfile /data/bigfile       # copy a local file in
bin/gfs ls /data
bin/gfs stat /data/bigfile
bin/gfs get /data/bigfile ./copy          # copy it back out
echo hello | bin/gfs write /data/bigfile 0
bin/gfs read /data/bigfile 0 5
echo record | bin/gfs append /data/log    # prints the offset the primary chose
bin/gfs snapshot /data /backup            # instant copy-on-write copy
bin/gfs rm /data/bigfile                  # hidden under /.deleted until GC
bin/gfs ls /.deleted
bin/gfs undelete /.deleted/<name>         # restore it
bin/gfs status                            # servers, replica histogram, clones, moves

scripts/local-cluster.sh kill-cs 0        # kill -9 a chunkserver; watch `gfs status`
scripts/local-cluster.sh stop
```

Or run the binaries yourself:

```sh
gfs-master      --listen :7000 --dir /scratch/gfs/master    # see --help: --trash-retention, --rebalance-interval, ...
gfs-master      --listen :7100 --dir /scratch/gfs/shadow --shadow --primary host1:7000
gfs-chunkserver --listen :7001 --dir /scratch/gfs/cs --masters host1:7000,host2:7100
```

## Tests

```sh
go test -race ./...          # unit + integration + failure injection
scripts/smoke-test.sh        # the same scenarios with real OS processes and kill -9
```

- **Unit tests:**
  - Log replay across segments, torn-tail truncation, and rejection of corruption mid-log.
  - Checkpoint round trip and corruption detection.
  - State-machine validation.
  - Chunk store reads and writes across 64 KB blocks, sparse writes, checksum detection, and reload after a restart.
- **Integration tests** (`internal/testcluster` runs a whole cluster in one process on loopback ports):
  - Replicated reads and writes across chunk boundaries, sparse files, and concurrent writers.
  - Failure scenarios: chunkserver death → re-replication; on-disk corruption → detection and replacement; secondary killed mid-lease → stale on return; master crash → recovery from checkpoint + log; primary down → reads served by the shadow.
  - Record append: 8 concurrent appenders across padded chunks, the size limit, and appends that continue through a chunkserver death. Every acknowledged record is checked on every replica.
  - Snapshots: copy-on-write isolation, directory snapshots surviving deletion and a master restart, snapshots taken during concurrent writes, and races between snapshots and creates.
  - Garbage collection: lazy delete, undelete and restart; expiry; chunks still shared with a snapshot are kept.
  - Rebalancing: three empty chunkservers added to a full cluster fill up to an even spread; excess replicas from a returning server are trimmed.
  - Namespace locks: which operations conflict, and 16 goroutines locking overlapping path sets without deadlock.

## Benchmarks

Measured on 6 nodes of a SLURM cluster: 1 master and 5 chunkservers on 1 Gb/s Ethernet, with 64 MiB chunks and 3 replicas. Aggregate throughput is the median of 3 runs. In network-only mode, clients never use a replica on their own node. Full tables, method and caveats: [bench/RESULTS.md](bench/RESULTS.md), [bench/METHODOLOGY.md](bench/METHODOLOGY.md).

| Workload | 1 client | 8 clients | 1 GbE limit at 8 |
|---|---:|---:|---:|
| Read, 4 MiB random regions (network-only) | 110.7 MB/s | **358.2 MB/s** | 582.5 MB/s |
| Write, 3 replicas, 8 MiB ops (network-only) | 82.1 MB/s | **157.8 MB/s** | 194.2 MB/s |
| Write, 3 replicas, 1 MiB ops (network-only) | 50.3 MB/s | 136.2 MB/s | 194.2 MB/s |
| Record append to one shared file, 1 MiB records | 53.4 MB/s | 84.7 MB/s | 116.5 MB/s |

**Recovery:** with 20 GiB of data (320 chunks), killing 2 of the 5 chunkservers restored full 3-way replication in a median of **150.8 s** (range 150.4–152.6 s over 3 runs). Detection took 4.7 s, and re-replication of about 383 chunks ran at 176 MB/s. Afterwards, all 960 replicas were byte-for-byte identical in every run. Appends: 32,256 records were checked, with 0 missing and 0 duplicated.

![throughput](bench/results/throughput.png)

Reads are served from the page cache (the nodes have far more RAM than the dataset), so they measure the network and GFS rather than the disks.

**Snapshots vs. copying:**
- A snapshot of a 17.2 GB directory (8 files, 256 chunks) took **0.5 ms**, or 172 ms when it first had to revoke leases from writes that had just finished. Copying the same directory through a client took **167 s**.
- The first write to a chunk shared with a snapshot took a median of 357 ms, while each of its 3 replicas copied the chunk locally (copy-on-write). The first write to an unshared chunk took 4.8 ms.
- All snapshots kept their original contents: 0 problems in a 17.2 GB byte-for-byte check.

**Master metadata operations:** each create and delete is fsynced to the op log before it is acknowledged.

| Operation | 1 client | 80 clients |
|---|---:|---:|
| Create (logged) | 2,281 ops/s | **68,802 ops/s** |
| Stat | 4,443 ops/s | **135,403 ops/s** |
| Delete (logged) | 2,286 ops/s | **66,324 ops/s** |

Clients all working in one shared directory were as fast as clients in private directories (within −4 % to +7 %), so the per-path namespace locks are not a bottleneck.

## Roadmap

- [x] M1–M4 core: master metadata + op log + checkpoints + shadow, chunkservers, replication, leases, heartbeats, re-replication, checksums, stale-replica detection
- [x] Record append (§3.3) with padding and at-least-once semantics
- [x] Copy-on-write snapshots (§3.4) with lease revocation and crash-safe refcounts
- [x] Lazy garbage collection with hidden deleted files (§4.4), rebalancing (§4.3), namespace read/write locks (§4.1)
- [x] Benchmarks on a SLURM cluster: read/write/append throughput vs. number of clients, recovery time
- [x] Benchmarks: snapshot vs. full copy, metadata operations per second
