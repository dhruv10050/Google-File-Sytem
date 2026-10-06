# Benchmark methodology

How every number in [RESULTS.md](RESULTS.md) was produced. Only measured values are reported: nothing estimated, nothing extrapolated.

## Cluster and layout

- **Cluster:** a university SLURM cluster, jobs ≤ 45 min.
- **Allocation:** `--nodes=6 --ntasks-per-node=1 --cpus-per-task=8 --mem=48G`. In practice we got node01–node06. node07 is draining and node08 is down, so 6 is the maximum.
- **Roles:**
  - The node running the batch script runs the **master**.
  - The other **5 nodes each run one chunkserver**.
  - Benchmark **clients** are placed round-robin over all 6 nodes, starting with the master's node. Client *i* runs on node *i mod 6*, so N=8 puts 2 clients on each of the first two nodes.
- **Network:** 1 Gb/s Ethernet (172.16.0.0/24). InfiniBand is down. Every node has exactly one NIC in use.
- **Chunk storage:** node-local disk, `/tmp/$USER-gfs-<job>`, on the root filesystem. The shared `/scratch` was not writable on every node.
- **GFS configuration (defaults):**
  - 64 MiB chunks, 3 replicas.
  - 60 s leases, 1 s heartbeats; a chunkserver is declared dead after 5 s of silence.
  - At most 4 concurrent clones per chunkserver, counted at both source and destination.
  - Chunkservers do not fsync each write (`-fsync=false`), as in the paper's write benchmarks. Clones do fsync each installed chunk.
- **Binaries:** static Linux/amd64 builds, cross-compiled from this repo. Scripts: `bench/slurm/{lib.sh,throughput.sbatch,recovery.sbatch}`. Analysis: `bench/analyze.py`.

## Measuring a run

Each client is a `gfs-bench` process, launched as its own `srun --overlap` step on its node.
- All clients of a run are given the same wall-clock start time (launch + 4 s). Setup, such as creating the file or warming the location cache, happens before that time.
- Each client prints `kind,id,host,bytes,start_ns,end_ns`.
- **Aggregate throughput** = total bytes / (latest end − earliest start), so a straggler lowers the number instead of being averaged away.
- **MB = 10^6 bytes.**
- Every configuration is run **3 times**. We report the **median**, plus the min–max.
- Runs are interleaved (run 1 of every N and mode, then run 2, …) so that drift in background load doesn't bias one configuration.

### Client modes

- **`avoid`** (*network-only*): `client.Options.AvoidLocal`. A client never reads from a replica on its own node, and never pushes to its own node first. Every byte crosses the network at least once per replica, so the 1 GbE limits below apply exactly. **This is the mode the headline numbers use.**
- **`local`:** the normal GFS client policy (nearest replica first, which may be on the same machine). Reported separately: same-node transfers use loopback, not the NIC, so these numbers are not bounded by the network limit in the same way.

## Workloads (paper §6.1, §3.4, §6.2.4)

| Benchmark | What each client does | Paper equivalent |
|---|---|---|
| Read | Reads `READ_BYTES` (2 GiB) as random, aligned 4 MiB regions from an 8 × 2 GiB dataset (16 GiB, written once before the read runs) | §6.1.1: N clients read 4 MB regions from a 320 GB set |
| Write | Writes `WRITE_BYTES` (1 GiB) to its own new file in `OP_W` writes. The file is deleted after the run, and its chunks are reclaimed by heartbeat GC | §6.1.2: N clients write 1 GB each to distinct files in 1 MB writes |
| Record append | N clients append 1 MiB records to one shared file, 512 MiB each. Afterwards the whole file is scanned: every record carries (writer, sequence, checksum), and each writer's records must all be present exactly once | §6.1.3: N clients append to a single file |
| Recovery | 10 × 2 GiB files (20 GiB of data, 60 GiB of replicas). Then `kill -9` the chunkserver process on 2 of the 5 nodes, chosen at random, and time until every chunk has 3 live replicas again | §6.2.2: kill chunkservers, measure re-replication time |
| Snapshot vs. copy | One client on the master's node, 8 × 2 GiB dataset (256 chunks). Per run, 3 runs: snapshot one file, snapshot the directory, copy the file through the client, copy the directory with 8 parallel file copies. Copies pipeline 8 MiB reads and writes | §3.4: snapshots are near-instant; data is copied only on the first write to a shared chunk |
| Copy-on-write | 4 KiB write at the start of every chunk of 3 snapshotted files (96 chunks), each followed by a rewrite of the same bytes. The same for the first file's 3 client copies, which are not shared ("unshared first write") | §3.4: the first write to a shared chunk makes each replica copy the chunk locally |
| Metadata | N clients (1–80) create, then stat, then delete empty files. One gRPC client per thread, min(N, 5) processes on the chunkserver nodes only. Each N runs once with a private directory per client and once with one shared directory. 20,000 ops per phase, at least 1,000 per client | §6.2.4: production masters saw 200–500 ops/s, and the master "can now easily support many thousands of file accesses per second" (no benchmark in §6.1) |

- **File contents** are a pure function of (path, offset), using splitmix64. Any process can regenerate the expected bytes of any region.
- **After each throughput job,** every byte of the dataset is read back and checked.
- **After each recovery run,** every replica of every chunk is fetched directly from its chunkserver and compared byte for byte (`gfs-bench verify -all-replicas -replicas 3`).

### Recovery timeline

These are measured from the `watch` CSV, polled every 100 ms from the master node, and the victims' kill timestamps:
- **kill → detected:** from the `kill -9` timestamp (on the victim's clock) to the first master poll that shows only 3 live chunkservers. Bounded below by the 5 s dead-after timeout.
- **detected → 3×:** until `under_replicated = 0`. This is the re-replication itself.
- **kill → 3×:** the sum of the two. **This is the headline recovery number.**
- Node clocks are NTP-synchronised. `chronyc` offsets are recorded in each job's `env.txt`.

### Snapshot and metadata details

- **Lease state.** Run 1's snapshots start right after the dataset is written, while leases from those writes are still live, so the master must revoke them first. Runs 2 and 3 come minutes later, after every 60 s lease has expired. The number of leases revoked is not recorded. The two cases are reported separately. A 1,000-file directory (one 4 KiB chunk each, all freshly leased) is snapshotted the same way: once with revocation, twice without.
- **Content check after the snapshot runs.** Every byte of the patched source files must equal the original except the 4 KiB patch at each chunk start. Every snapshot must still hold the original bytes at those offsets. Afterwards, every byte of one directory snapshot (17.2 GB) is read back by 6 verifiers and compared with the original dataset.
- **Metadata throughput** = total successful ops / (last client end − first client start), median of 3 runs. Latency percentiles are per process. A row reports the median process p50 and the worst process p99 (per-process percentiles cannot be merged exactly), and then the median of that over runs.
- **Op log.** Every create and delete is logged and fsynced before it is acknowledged; concurrent mutations share an fsync (group commit). Stat is a read and is not logged. The master disk's synchronous 4 KiB write rate (`dd oflag=dsync`) is recorded in each metadata job.
- With `--trash-retention 0`, a delete removes the file at once, rather than renaming it into `/.deleted`. Created files are empty (no chunks), so these phases involve the master only.

## Baselines measured in the same job

- **Network:**
  - `gfs-bench net-send` / `net-serve`: one TCP stream between two nodes for 10 s.
  - A ring where all 6 nodes send to their neighbour at once, so every NIC sends and receives simultaneously. The per-NIC rate from the ring is used as the network limit.
- **Disk:** `dd bs=8M count=256 conv=fdatasync` (2 GiB) into the chunk directory on each chunkserver node.

## Network limits (idealised, `avoid` mode)

The per-NIC rate *B* is measured by the ring test. Clients occupy min(N, 6) nodes, and chunkservers occupy 5.
- **Read:** each byte leaves one chunkserver NIC and enters one client NIC, so the limit is B × min(N, 5).
- **Write:** each byte enters 3 chunkserver NICs (client → r1 → r2 → r3), so the limit is min(B × min(N, 6), 5B / 3).
- **Append:** all appends to a chunk go through its primary, and the file is filled one chunk at a time, so the limit is min(B × N, B).
- **Recovery:** with 2 of 5 chunkservers dead, all clones flow into the 3 survivors, so the limit is 3B of ingress.

## Caveats (state these whenever quoting numbers)

1. **Page cache:** nodes have 186–502 GB of RAM, and the dataset is 16 GiB × 3 replicas spread over 5 nodes. Reads are served from the page cache, not the disk, so read throughput measures the network and GFS, not the disks. The paper's numbers were disk-backed.
2. **Co-located clients:** with N > 1, clients share nodes with chunkservers. In `avoid` mode that only shares a NIC's opposite direction and CPU, but the `local` mode numbers benefit from loopback.
3. **Shared cluster:** node04 and node06 were running other users' jobs during our runs (`mix` state). The min–max ranges reflect that.
4. **Scale:** 5 chunkservers and 6 nodes, against the paper's 16 chunkservers and 16 clients. Absolute numbers are bounded by 1 GbE; compare them with the network limit, not with the paper's figures.
