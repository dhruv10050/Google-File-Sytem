# Benchmark results

Measured on a 6-node SLURM cluster (1 GbE), October 2026. Per-client raw CSVs, summary CSVs and the plot are in [results/](results/), all produced by [`analyze.py`](analyze.py) from the job outputs. Nothing here is estimated or extrapolated.

![throughput](results/throughput.png)

## Setup

6 nodes of a SLURM cluster: 1 master node and 5 chunkserver nodes, all on 1 Gb/s Ethernet. Clients are placed round-robin over all 6 nodes. 64 MiB chunks, 3 replicas, node-local disk. The full method, including how the network limits are derived, is in [METHODOLOGY.md](METHODOLOGY.md).

- **Network baseline (measured in each job):** one TCP stream 117.8 MB/s. Per NIC, with all 6 nodes sending and receiving at once, **116.5 MB/s** (*B*). The limits in the tables are derived from *B*.
- **Disk baseline:** `dd` 2 GiB with fdatasync, 227–442 MB/s per chunkserver node.
- **Aggregate throughput** = total bytes / (last client end − first client start). MB = 10^6 bytes. Each configuration was run 3 times, and the table reports the median and the min–max.
- **Client modes:**
  - `avoid` (network-only): a client never reads from, or pushes first to, a replica on its own node, so every byte crosses the network.
  - `local`: the normal nearest-replica policy, which may use loopback.

## Read throughput (§6.1.1)

Each client reads 2 GiB as random 4 MiB regions of a 16 GiB dataset.

| op | client mode | op size | clients | runs | median MB/s | min–max | network limit | % of limit |
| append | avoid | 1M | 1 | 3 | 53.4 | 52.3–54.8 | 116.5 | 46% |
| read | avoid | 4M | 1 | 3 | 110.7 | 110.0–111.0 | 116.5 | 95% |
| read | avoid | 4M | 2 | 3 | 185.2 | 175.0–186.2 | 233.0 | 80% |
| read | avoid | 4M | 4 | 3 | 268.6 | 265.3–270.1 | 466.0 | 58% |
| read | avoid | 4M | 6 | 3 | 323.4 | 314.8–335.1 | 582.5 | 56% |
| read | avoid | 4M | 8 | 3 | 358.2 | 347.1–363.3 | 582.5 | 61% |
| read | local | 4M | 1 | 3 | 110.3 | 110.0–110.4 | – |  |
| read | local | 4M | 2 | 3 | 205.3 | 204.5–207.9 | – |  |
| read | local | 4M | 4 | 3 | 366.8 | 359.0–369.9 | – |  |
| read | local | 4M | 6 | 3 | 485.3 | 480.8–495.9 | – |  |
| read | local | 4M | 8 | 3 | 417.3 | 416.9–419.1 | – |  |

## Write throughput (§6.1.2)

Each client writes 1 GiB to its own new file. Every byte is written to 3 replicas through a pipelined chain.

| op | client mode | op size | clients | runs | median MB/s | min–max | network limit | % of limit |
| append | avoid | 1M | 1 | 3 | 53.4 | 52.3–54.8 | 116.5 | 46% |
| write | avoid | 8M | 1 | 3 | 82.1 | 81.9–83.7 | 116.5 | 70% |
| write | avoid | 8M | 2 | 3 | 104.3 | 96.1–106.2 | 194.2 | 54% |
| write | avoid | 8M | 4 | 3 | 128.8 | 125.8–129.9 | 194.2 | 66% |
| write | avoid | 8M | 6 | 3 | 146.9 | 139.8–149.4 | 194.2 | 76% |
| write | avoid | 8M | 8 | 3 | 157.8 | 156.4–158.7 | 194.2 | 81% |
| write | avoid | 1M | 1 | 3 | 50.3 | 41.2–50.5 | 116.5 | 43% |
| write | avoid | 1M | 2 | 3 | 75.8 | 73.7–79.6 | 194.2 | 39% |
| write | avoid | 1M | 4 | 3 | 104.7 | 101.9–108.6 | 194.2 | 54% |
| write | avoid | 1M | 6 | 3 | 122.8 | 122.3–125.3 | 194.2 | 63% |
| write | avoid | 1M | 8 | 3 | 136.2 | 132.3–136.9 | 194.2 | 70% |
| write | local | 8M | 1 | 3 | 80.9 | 80.2–85.6 | – |  |
| write | local | 8M | 2 | 3 | 108.6 | 108.2–112.4 | – |  |
| write | local | 8M | 4 | 3 | 140.2 | 139.4–142.1 | – |  |
| write | local | 8M | 6 | 3 | 161.2 | 160.0–167.4 | – |  |
| write | local | 8M | 8 | 3 | 174.7 | 173.5–181.3 | – |  |

## Record append throughput (§6.1.3)

N clients append 1 MiB records to **one shared file**: 512 MiB per client. All appends to a chunk go through its single primary, so the limit is one NIC (*B*) whatever N is.

| op | client mode | op size | clients | runs | median MB/s | min–max | network limit | % of limit |
| append | avoid | 1M | 1 | 3 | 53.4 | 52.3–54.8 | 116.5 | 46% |
| append | avoid | 1M | 2 | 3 | 68.2 | 67.0–69.2 | 116.5 | 59% |
| append | avoid | 1M | 4 | 3 | 80.7 | 78.2–81.8 | 116.5 | 69% |
| append | avoid | 1M | 6 | 3 | 85.3 | 84.8–85.6 | 116.5 | 73% |
| append | avoid | 1M | 8 | 3 | 84.7 | 83.2–85.4 | 116.5 | 73% |

After every run, the whole file was scanned and every record was checked: 15 runs, 63 writers, **32,256 records**, **0 missing, 0 duplicated, 0 corrupt**.

## Recovery (§6.2.2)

20 GiB of data (10 files × 2 GiB = 21.5 GB; 320 chunks, 64.4 GB with replicas). `kill -9` the chunkservers on 2 of the 5 nodes, chosen at random. The time runs until every chunk has 3 live replicas again. Afterwards, every replica of every chunk was fetched directly from its chunkserver and compared byte for byte.

| job | killed nodes | chunks | chunks under-replicated | clones | kill → detected | detected → 3× | **kill → 3×** | clone rate | verified | problems |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 103666 | node05, node04 | 320 | 258 | 382 | 4.7 s | 145.7 s | **150.4 s** | 176.0 MB/s | 64.4 GB, 960 replicas | 0 |
| 103667 | node02, node06 | 320 | 265 | 385 | 4.5 s | 148.1 s | **152.6 s** | 174.5 MB/s | 64.4 GB, 960 replicas | 0 |
| 103668 | node03, node04 | 320 | 303 | 383 | 4.9 s | 145.9 s | **150.8 s** | 176.2 MB/s | 64.4 GB, 960 replicas | 0 |
| **median** | | | | | **4.7 s** | **145.9 s** | **150.8 s** | **176.0 MB/s** | | **0** |

With 2 of 5 servers dead, every clone lands on the 3 survivors, so the network limit is 3*B* = 349.5 MB/s of ingress. Cloning ran at 176 MB/s, about 50 % of that. Each clone is also fsynced on install, and the per-server clone limit is 4.

## Data integrity

- At the end of each throughput job, the dataset written at its start was read back and verified: 17.2 GB in each read job, 0.54 GB in the write and append jobs. 0 problems in all 4 jobs.
- After each recovery: 960/960 replicas were identical, with 0 problems, in all 3 runs.
- Appends: 32,256/32,256 records were present exactly once.

## Caveats

1. **Reads come from the page cache, not disk.** The nodes have 186–502 GB of RAM and the dataset is 16 GiB, so read numbers measure the network and GFS, not the disks. The paper's numbers were disk-backed.
2. **Scale.** 5 chunkservers on 1 GbE, against the paper's 16 chunkservers and 16 clients. Compare the numbers with the network-limit column, not with the paper's figures.
3. **Shared cluster.** Other users' jobs ran on some nodes during these runs. The min–max ranges include that.
4. **Clients share nodes with chunkservers.** In `local` mode, reads served by a co-located chunkserver use loopback, so those numbers can exceed the network limit. With N = 8, two nodes host two clients each. Local-mode reads fell from N = 6 to N = 8; we did not investigate why.
5. **Software version.** The binaries are from commit 49596ce: core GFS plus record append. Snapshots, lazy GC, namespace locks and rebalancing were implemented afterwards and are not part of these runs.
