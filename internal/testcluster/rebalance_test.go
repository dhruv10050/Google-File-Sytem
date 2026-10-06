package testcluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ───────────────────────── Rebalancing (§4.3) ─────────────────────────

// chunkCounts returns, per chunkserver index, how many of path's chunks it
// holds according to the master, and whether every chunk has want replicas.
func chunkCounts(c *Cluster, path string, want int) ([]int, bool) {
	counts := make([]int, len(c.CS))
	ok := true
	for _, l := range c.Locations(path) {
		if len(l.Replicas) != want {
			ok = false
		}
		for _, a := range l.Replicas {
			counts[c.Index(a)]++
		}
	}
	return counts, ok
}

// replicaFilesOn maps each chunkserver index to the chunk files of path it
// should hold according to the master.
func replicaFilesOn(c *Cluster, path string) map[int]map[string]bool {
	out := map[int]map[string]bool{}
	for _, l := range c.Locations(path) {
		for _, a := range l.Replicas {
			i := c.Index(a)
			if out[i] == nil {
				out[i] = map[string]bool{}
			}
			out[i][c.ChunkFile(i, l.Handle)] = true
		}
	}
	return out
}

// Chunkservers added to a full cluster are filled gradually: replicas move
// from the busiest servers until counts are even, every chunk keeps exactly
// three replicas, and the moved-away copies are deleted from disk.
func TestRebalanceOntoNewChunkservers(t *testing.T) {
	c := Start(t, Options{RebalanceInterval: 100 * time.Millisecond, MaxRebalanceMoves: 2})
	cl := c.Client()
	cl.Create("/big", 0)
	data := randBytes(12*MB+100, 50) // 13 chunks, 39 replicas on 3 servers
	mustWrite(t, cl, "/big", 0, data)
	before := replicaFilesOn(c, "/big")

	for range 3 {
		c.AddChunkServer()
	}
	c.WaitServers(6)
	c.WaitFor(30*time.Second, "replica counts balanced across 6 servers", func() bool {
		counts, ok := chunkCounts(c, "/big", 3)
		if !ok {
			return false
		}
		lo, hi := counts[0], counts[0]
		for _, n := range counts {
			lo, hi = min(lo, n), max(hi, n)
		}
		return hi-lo <= 2
	})
	counts, _ := chunkCounts(c, "/big", 3)
	t.Logf("replicas per server after rebalancing: %v", counts)
	for i := 3; i < 6; i++ {
		if counts[i] == 0 {
			t.Fatalf("new server %d received nothing: %v", i, counts)
		}
	}
	st, err := cl.ClusterStatus()
	if err != nil || st.MovesCompleted == 0 || st.UnderReplicated != 0 {
		t.Fatalf("status: moves=%d under=%d err=%v", st.GetMovesCompleted(), st.GetUnderReplicated(), err)
	}

	// Replicas moved off the original servers are deleted there.
	after := replicaFilesOn(c, "/big")
	var moved []string
	for i := 0; i < 3; i++ {
		for f := range before[i] {
			if !after[i][f] {
				moved = append(moved, f)
			}
		}
	}
	if len(moved) == 0 {
		t.Fatal("no replica left an original server")
	}
	c.WaitFor(10*time.Second, "moved-away replicas deleted", func() bool { return allGone(moved) })
	for _, files := range after {
		for f := range files {
			if _, err := os.Stat(f); err != nil {
				t.Fatalf("replica the master lists is missing: %v", err)
			}
		}
	}
	checkContent(t, c.Client(), "/big", data)

	// Balanced: the scan stops moving replicas, and writes still work.
	done := st.MovesCompleted
	time.Sleep(500 * time.Millisecond)
	if st, _ := cl.ClusterStatus(); st.MovesCompleted != done || st.MovesInFlight != 0 {
		t.Fatalf("moves continue on a balanced cluster: %d -> %d (%d in flight)", done, st.MovesCompleted, st.MovesInFlight)
	}
	mustWrite(t, cl, "/big", 5*MB, []byte("after rebalancing"))
	copy(data[5*MB:], "after rebalancing")
	checkContent(t, cl, "/big", data)
}

// A chunkserver that returns after its chunks were re-replicated brings
// back current replicas, leaving chunks with four. The excess copy is
// removed from the most-loaded server, which is never the returning one
// (it holds the fewest), and deleted from that server's disk.
func TestExcessReplicasTrimmed(t *testing.T) {
	// Moves disabled (-1) so only trimming changes placement.
	c := Start(t, Options{ChunkServers: 4, Lease: 300 * time.Millisecond, RebalanceInterval: 100 * time.Millisecond, MaxRebalanceMoves: -1})
	cl := c.Client()
	cl.Create("/t", 0)
	data := randBytes(3*MB+10, 51)
	mustWrite(t, cl, "/t", 0, data)

	victim := c.Index(c.Locations("/t")[0].Replicas[0])
	held, _ := chunkCounts(c, "/t", 3)
	// Let the write leases lapse: otherwise re-replication fences the
	// chunks with a version bump and the returning replicas are stale.
	time.Sleep(500 * time.Millisecond)
	c.StopChunkServer(victim)
	c.WaitFor(20*time.Second, "re-replication without the stopped server", func() bool {
		counts, ok := chunkCounts(c, "/t", 3)
		return ok && counts[victim] == 0
	})
	nchunks := len(c.Locations("/t"))

	c.StartChunkServer(victim)
	c.WaitFor(20*time.Second, "returning replicas kept, excess trimmed and deleted", func() bool {
		counts, ok := chunkCounts(c, "/t", 3)
		return ok && counts[victim] == held[victim] && diskReplicas(c) == 3*nchunks
	})
	checkContent(t, c.Client(), "/t", data)
	mustWrite(t, cl, "/t", MB, []byte("trimmed"))
	copy(data[MB:], "trimmed")
	checkContent(t, cl, "/t", data)
}

// diskReplicas counts chunk files on every chunkserver's disk.
func diskReplicas(c *Cluster) int {
	n := 0
	for i := range c.CS {
		ents, _ := os.ReadDir(filepath.Join(c.CSDir(i), "chunks"))
		for _, e := range ents {
			if strings.HasSuffix(e.Name(), ".chunk") {
				n++
			}
		}
	}
	return n
}
