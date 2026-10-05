package testcluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
	"github.com/dhruv10050/Google-File-Sytem/pkg/client"
)

const MB = 1 << 20

func randBytes(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	b := make([]byte, n)
	for i := 0; i+8 <= n; i += 8 {
		v := r.Uint64()
		for j := 0; j < 8; j++ {
			b[i+j] = byte(v >> (8 * j))
		}
	}
	for i := n &^ 7; i < n; i++ {
		b[i] = byte(r.Uint32())
	}
	return b
}

func mustWrite(t *testing.T, cl *client.Client, path string, off int64, data []byte) {
	t.Helper()
	if err := cl.Write(path, off, data); err != nil {
		t.Fatalf("write %s@%d: %v", path, off, err)
	}
}

func readAll(t *testing.T, cl *client.Client, path string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := cl.ReadTo(path, &buf, 0); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return buf.Bytes()
}

func checkContent(t *testing.T, cl *client.Client, path string, want []byte) {
	t.Helper()
	got := readAll(t, cl, path)
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: read %d bytes, want %d; contents differ", path, len(got), len(want))
	}
}

// ───────────────────────── Basic operation ─────────────────────────

func TestNamespace(t *testing.T) {
	c := Start(t, Options{})
	cl := c.Client()
	if err := cl.Mkdir("/a/b", false); err == nil {
		t.Fatal("mkdir without parent succeeded")
	}
	if err := cl.Mkdir("/a/b", true); err != nil {
		t.Fatal(err)
	}
	if err := cl.Create("/a/b/f", 2); err != nil {
		t.Fatal(err)
	}
	if err := cl.Create("/a/b/f", 2); err == nil {
		t.Fatal("duplicate create succeeded")
	}
	ents, err := cl.List("/a/b")
	if err != nil || len(ents) != 1 || ents[0].Name != "f" || ents[0].IsDir {
		t.Fatalf("ls = %v, %v", ents, err)
	}
	st, err := cl.Stat("/a/b/f")
	if err != nil || st.IsDir || st.Replication != 2 {
		t.Fatalf("stat = %v, %v", st, err)
	}
	if err := cl.Delete("/a/b"); err == nil {
		t.Fatal("deleted non-empty directory")
	}
	if err := cl.Delete("/a/b/f"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Stat("/a/b/f"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("stat deleted file: %v", err)
	}
	if err := cl.Delete("/a/b"); err != nil {
		t.Fatal(err)
	}
}

// A write spanning several chunks reads back exactly; partial reads at
// arbitrary offsets, reads past EOF and Size behave like a regular file.
func TestWriteReadAcrossChunks(t *testing.T) {
	c := Start(t, Options{})
	cl := c.Client()
	cl.Create("/f", 0)
	data := randBytes(3*MB+MB/2, 1)
	mustWrite(t, cl, "/f", 0, data)
	checkContent(t, cl, "/f", data)

	buf := make([]byte, 300000)
	off := int64(MB - 1000) // straddles the chunk 0/1 boundary
	if n, err := cl.ReadAt("/f", buf, off); err != nil || n != len(buf) || !bytes.Equal(buf, data[off:off+int64(n)]) {
		t.Fatalf("ReadAt across boundary: n=%d err=%v", n, err)
	}
	n, err := cl.ReadAt("/f", buf, int64(len(data))-10)
	if err != io.EOF || n != 10 {
		t.Fatalf("ReadAt near end = %d, %v; want 10, EOF", n, err)
	}
	if size, err := cl.Size("/f"); err != nil || size != int64(len(data)) {
		t.Fatalf("Size = %d, %v", size, err)
	}
	locs := c.Locations("/f")
	if len(locs) != 4 {
		t.Fatalf("%d chunks, want 4", len(locs))
	}
	for _, l := range locs {
		if len(l.Replicas) != 3 {
			t.Fatalf("chunk %d has %d replicas", l.Index, len(l.Replicas))
		}
	}

	// Overwrite in the middle.
	patch := randBytes(5000, 2)
	mustWrite(t, cl, "/f", 2*MB-100, patch)
	copy(data[2*MB-100:], patch)
	checkContent(t, cl, "/f", data)
}

// Writing past the end leaves a hole that reads as zeros.
func TestSparseFile(t *testing.T) {
	c := Start(t, Options{})
	cl := c.Client()
	cl.Create("/sparse", 0)
	tail := randBytes(1000, 3)
	off := int64(2*MB + 500)
	mustWrite(t, cl, "/sparse", off, tail)
	want := append(make([]byte, off), tail...)
	checkContent(t, cl, "/sparse", want)
}

// Concurrent clients writing disjoint regions of one file, including
// regions inside the same chunk, all land correctly.
func TestConcurrentWriters(t *testing.T) {
	c := Start(t, Options{ChunkServers: 4})
	setup := c.Client()
	setup.Create("/shared", 0)
	const writers, region = 8, 400000 // several regions share each chunk
	want := make([]byte, writers*region)
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		data := randBytes(region, uint64(100+w))
		copy(want[w*region:], data)
		wg.Add(1)
		go func(w int, data []byte) {
			defer wg.Done()
			cl := c.Client()
			errs <- cl.Write("/shared", int64(w*region), data)
		}(w, data)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	checkContent(t, c.Client(), "/shared", want)
}

// ───────────────────────── Failure handling ─────────────────────────

// Killing a chunkserver drops chunks below their goal; the master clones
// them back to 3 live replicas and the data stays readable even after a
// second failure (§4.3).
func TestRereplicationAfterChunkserverDeath(t *testing.T) {
	c := Start(t, Options{ChunkServers: 5})
	cl := c.Client()
	cl.Create("/r", 0)
	data := randBytes(6*MB, 4)
	mustWrite(t, cl, "/r", 0, data)

	victim := c.Index(c.Locations("/r")[0].Replicas[0])
	c.StopChunkServer(victim)
	c.WaitFor(20*time.Second, "re-replication to 3 live replicas", func() bool {
		for _, l := range c.Locations("/r") {
			if len(l.Replicas) != 3 {
				return false
			}
			for _, a := range l.Replicas {
				if a == c.CSAddr[victim] {
					return false
				}
			}
		}
		return true
	})
	st, err := cl.ClusterStatus()
	if err != nil || st.UnderReplicated != 0 || st.ClonesCompleted == 0 {
		t.Fatalf("status: under=%d clones=%d err=%v", st.GetUnderReplicated(), st.GetClonesCompleted(), err)
	}
	checkContent(t, c.Client(), "/r", data)

	second := c.Index(c.Locations("/r")[1].Replicas[0])
	c.StopChunkServer(second)
	checkContent(t, c.Client(), "/r", data)
}

// A replica damaged on disk is detected by checksums when read, never
// returned to the client, reported to the master and replaced (§5.2).
func TestCorruptReplicaIsReplaced(t *testing.T) {
	c := Start(t, Options{ChunkServers: 4})
	cl := c.Client()
	cl.Create("/c", 0)
	data := randBytes(MB, 5)
	mustWrite(t, cl, "/c", 0, data)
	loc := c.Locations("/c")[0]
	bad := c.Index(loc.Replicas[0])

	f, err := os.OpenFile(c.ChunkFile(bad, loc.Handle), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte("garbage!"), 300000)
	f.Close()

	// Every read succeeds with correct data, whichever replica is tried.
	for i := 0; i < 10; i++ {
		checkContent(t, c.Client(), "/c", data)
	}
	// Force the bad replica to notice even if no read happened to pick it.
	if err := readDirect(t, c, bad, loc.Handle); err == nil {
		t.Fatal("corrupted replica served data without an error")
	}
	c.WaitFor(20*time.Second, "corrupt replica replaced", func() bool {
		l := c.Locations("/c")[0]
		if len(l.Replicas) != 3 {
			return false
		}
		for _, a := range l.Replicas {
			if c.Index(a) == bad {
				return false
			}
		}
		return true
	})
	c.WaitFor(10*time.Second, "corrupt replica deleted", func() bool {
		_, err := c.ChunkInfo(bad, loc.Handle)
		return err != nil
	})
	checkContent(t, c.Client(), "/c", data)
}

// A chunkserver that misses mutations while down comes back with an old
// version; the master treats it as stale, clients never read from it, and
// it is garbage-collected and re-replicated (§4.5).
func TestStaleReplicaAfterRestart(t *testing.T) {
	c := Start(t, Options{ChunkServers: 3})
	cl := c.Client()
	cl.Create("/s", 0)
	v1 := randBytes(MB/2, 6)
	mustWrite(t, cl, "/s", 0, v1)
	loc := c.Locations("/s")[0]
	// Kill a secondary of the current lease: the lease itself stays valid,
	// so only the master's lease-replica check forces the version bump that
	// makes this replica detectably stale.
	lease := c.LeaseHolder("/s", 0)
	victim := c.Index(lease.Secondaries[0])

	c.StopChunkServer(victim)
	c.WaitFor(10*time.Second, "victim declared dead", func() bool {
		return len(c.Locations("/s")[0].Replicas) == 2
	})
	v2 := randBytes(MB/2, 7)
	mustWrite(t, cl, "/s", 0, v2)
	newVersion := c.Locations("/s")[0].Version
	if newVersion <= loc.Version {
		t.Fatalf("version did not advance: %d -> %d", loc.Version, newVersion)
	}

	c.StartChunkServer(victim)
	// Reads right after the restart must never return v1.
	for i := 0; i < 20; i++ {
		checkContent(t, c.Client(), "/s", v2)
	}
	c.WaitFor(20*time.Second, "stale replica replaced by a current one", func() bool {
		info, err := c.ChunkInfo(victim, loc.Handle)
		return err == nil && info.Version == newVersion && len(c.Locations("/s")[0].Replicas) == 3
	})
	checkContent(t, c.Client(), "/s", v2)
}

// The master recovers its namespace and chunk table from checkpoint + log
// after a crash, and chunkservers re-register so data is readable and
// writable again (§2.6.3, §5.1.1).
func TestMasterRestart(t *testing.T) {
	c := Start(t, Options{CheckpointEveryRecords: 7})
	cl := c.Client()
	cl.Mkdir("/d", false)
	files := map[string][]byte{}
	for i := 0; i < 6; i++ {
		p := fmt.Sprintf("/d/f%d", i)
		cl.Create(p, 0)
		files[p] = randBytes(MB+i*1000, uint64(10+i))
		mustWrite(t, cl, p, 0, files[p])
	}
	cl.Delete("/d/f0")
	delete(files, "/d/f0")

	c.StopMaster()
	c.StartMaster()
	c.WaitServers(3)

	cl2 := c.Client()
	ents, err := cl2.List("/d")
	if err != nil || len(ents) != len(files) {
		t.Fatalf("after restart: %d entries, err %v", len(ents), err)
	}
	for p, want := range files {
		checkContent(t, cl2, p, want)
	}
	// Writes work again once the master has heard from the chunkservers.
	extra := randBytes(1000, 99)
	c.WaitFor(15*time.Second, "writes accepted after restart", func() bool {
		return cl2.Write("/d/f1", 0, extra) == nil
	})
	copy(files["/d/f1"], extra)
	checkContent(t, cl2, "/d/f1", files["/d/f1"])
}

// A shadow master follows the log and keeps serving metadata reads while
// the primary is down; writes are refused (§5.1.3).
func TestShadowServesReads(t *testing.T) {
	c := Start(t, Options{Shadow: true, CheckpointEveryRecords: 5, Verbose: os.Getenv("GFS_VERBOSE") != ""})
	cl := c.Client()
	cl.Mkdir("/x", false)
	data := randBytes(2*MB+77, 8)
	cl.Create("/x/f", 0)
	mustWrite(t, cl, "/x/f", 0, data)
	for i := 0; i < 8; i++ {
		cl.Create(fmt.Sprintf("/x/g%d", i), 0)
	}

	primary, err := cl.ClusterStatus()
	if err != nil {
		t.Fatal(err)
	}
	shadowCl, err := client.New(client.Options{Masters: []string{c.ShadowAddr}})
	if err != nil {
		t.Fatal(err)
	}
	defer shadowCl.Close()
	c.WaitFor(10*time.Second, "shadow caught up", func() bool {
		st, err := shadowCl.ClusterStatus()
		return err == nil && st.IsShadow && st.AppliedLsn >= primary.AppliedLsn
	})

	c.WaitFor(10*time.Second, "shadow knows replica locations", func() bool {
		locs, err := shadowLocations(c, "/x/f")
		if err != nil {
			return false
		}
		for _, l := range locs {
			if len(l.Replicas) != 3 {
				return false
			}
		}
		return len(locs) == 3
	})

	c.StopMaster()
	cl2 := c.Client() // primary first, falls back to the shadow
	ents, err := cl2.List("/x")
	if err != nil || len(ents) != 9 {
		t.Fatalf("list via shadow: %d entries, %v", len(ents), err)
	}
	checkContent(t, cl2, "/x/f", data)
	if err := cl2.Create("/x/new", 0); err == nil {
		t.Fatal("create succeeded with the primary down")
	}
}

// readDirect reads a whole chunk from chunkserver i, bypassing the client,
// and returns the stream error (nil if the replica served it cleanly).
func readDirect(t *testing.T, c *Cluster, i int, h uint64) error {
	t.Helper()
	conn, err := common.Dial(c.CSAddr[i])
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := pb.NewChunkServerClient(conn).Read(ctx, &pb.ReadRequest{Handle: h, Length: MB})
	if err != nil {
		return err
	}
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func shadowLocations(c *Cluster, path string) ([]*pb.ChunkLocation, error) {
	conn, err := common.Dial(c.ShadowAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := pb.NewMasterClient(conn).GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: path, Count: 100})
	if err != nil {
		return nil, err
	}
	return resp.Chunks, nil
}
