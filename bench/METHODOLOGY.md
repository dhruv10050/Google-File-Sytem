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

## Workloads (paper §6.1)

| Benchmark | What each client does | Paper equivalent |
|---|---|---|
| Read | Reads `READ_BYTES` (2 GiB) as random, aligned 4 MiB regions from an 8 × 2 GiB dataset (16 GiB, written once before the read runs) | §6.1.1: N clients read 4 MB regions from a 320 GB set |
| Write | Writes `WRITE_BYTES` (1 GiB) to its own new file in `OP_W` writes. The file is deleted after the run, and its chunks are reclaimed by heartbeat GC | §6.1.2: N clients write 1 GB each to distinct files in 1 MB writes |
| Record append | N clients append 1 MiB records to one shared file, 512 MiB each. Afterwards the whole file is scanned: every record carries (writer, sequence, checksum), and each writer's records must all be present exactly once | §6.1.3: N clients append to a single file |
| Recovery | 10 × 2 GiB files (20 GiB of data, 60 GiB of replicas). Then `kill -9` the chunkserver process on 2 of the 5 nodes, chosen at random, and time until every chunk has 3 live replicas again | §6.2.2: kill chunkservers, measure re-replication time |

- **File contents** are a pure function of (path, offset), using splitmix64. Any process can regenerate the expected bytes of any region.
- **After each throughput job,** every byte of the dataset is read back and checked.
- **After each recovery run,** every replica of every chunk is fetched directly from its chunkserver and compared byte for byte (`gfs-bench verify -all-replicas -replicas 3`).

### Recovery timeline

These are measured from the `watch` CSV, polled every 100 ms from the master node, and the victims' kill timestamps:
- **kill → detected:** from the `kill -9` timestamp (on the victim's clock) to the first master poll that shows only 3 live chunkservers. Bounded below by the 5 s dead-after timeout.
- **detected → 3×:** until `under_replicated = 0`. This is the re-replication itself.
- **kill → 3×:** the sum of the two. **This is the headline recovery number.**
- Node clocks are NTP-synchronised. `chronyc` offsets are recorded in each job's `env.txt`.

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
