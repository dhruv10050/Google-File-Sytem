# Google File System in Go

A from-scratch implementation of the [Google File System](https://research.google/pubs/the-google-file-system/) (Ghemawat, Gobioff, Leung — SOSP 2003) in Go, using gRPC and Protocol Buffers. The paper is the spec: section numbers like (§4.5) in the code and below point to the part of the paper each mechanism implements.

> **Status:** the core system is implemented and tested: the master with its operation log, checkpoints and shadow; chunkservers; 3-way replication; leases; heartbeats; re-replication; checksums; stale-replica detection. Record append, snapshots, garbage collection, rebalancing and namespace locking are next, followed by benchmarks on a SLURM cluster. See [Roadmap](#roadmap).

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

### Where this differs from the paper

- **Lease fencing by version.** The paper waits for an old lease to expire before granting a new one. Here, granting a lease always bumps the chunk version on the replicas first. A replica that sees a new version drops any lease it holds, and secondaries reject mutations carrying an old version. So a new primary can be chosen immediately, without waiting up to 60 s.
- **A lease is void once any replica it covered is lost.** If a secondary dies mid-lease, retried writes can succeed on the remaining replicas without changing the version. The dead replica would then come back looking current. Voiding the lease forces the next write to bump the version, which marks the returning replica stale. There is a regression test for this: `TestStaleReplicaAfterRestart`.
- **One in-flight mutation per chunk at the primary.** This keeps the ordering of mutations at the secondaries simple. Data pushes from many clients still overlap.

## Layout

```
proto/gfs.proto          gRPC services + persistent formats (log records, checkpoints, chunk metadata)
api/gfspb/               generated code (committed)
internal/master/         state machine, op log, checkpoints, leases, heartbeats, re-replication, shadow
internal/chunkserver/    chunk store with checksums, data buffer, mutation pipeline, cloning
internal/common/         shared constants, path helpers, gRPC dialing
pkg/client/              client library
cmd/gfs-master           master / shadow master binary
cmd/gfs-chunkserver      chunkserver binary
cmd/gfs                  CLI
internal/testcluster/    in-process cluster harness + integration and failure tests
scripts/                 local multi-process cluster and smoke test
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
bin/gfs status                            # servers, replica histogram, clones

scripts/local-cluster.sh kill-cs 0        # kill -9 a chunkserver; watch `gfs status`
scripts/local-cluster.sh stop
```

Or run the binaries yourself:

```sh
gfs-master      --listen :7000 --dir /scratch/gfs/master
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

## Roadmap

- [x] M1–M4 core: master metadata + op log + checkpoints + shadow, chunkservers, replication, leases, heartbeats, re-replication, checksums, stale-replica detection
- [ ] Record append (§3.3) with padding and at-least-once semantics
- [ ] Copy-on-write snapshots (§3.4) with lease revocation and crash-safe refcounts
- [ ] Lazy garbage collection with hidden deleted files (§4.4), rebalancing (§4.3), namespace read/write locks (§4.1)
- [ ] Benchmarks on a SLURM cluster: read/write/append throughput vs. number of clients, recovery time, snapshot vs. copy, metadata ops/s
