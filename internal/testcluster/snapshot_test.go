package testcluster

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dhruv10050/Google-File-Sytem/pkg/client"
)

// ───────────────────────── Snapshots (§3.4) ─────────────────────────

func handles(c *Cluster, path string) []uint64 {
	var out []uint64
	for _, l := range c.Locations(path) {
		out = append(out, l.Handle)
	}
	return out
}

// A snapshot shares every chunk; the first write to a shared chunk copies
// only that chunk, on the servers that already hold it, and the snapshot
// keeps the old contents. The writer's cached lease is revoked by the
// snapshot, so it cannot slip a write into the shared chunk.
func TestSnapshotCopyOnWrite(t *testing.T) {
	c := Start(t, Options{ChunkServers: 5})
	cl := c.Client()
	cl.Create("/f", 0)
	orig := randBytes(3*MB+1234, 20)
	mustWrite(t, cl, "/f", 0, orig)
	before := handles(c, "/f")

	if err := cl.Snapshot("/f", "/snap"); err != nil {
		t.Fatal(err)
	}
	if got := handles(c, "/snap"); !slices.Equal(got, before) {
		t.Fatalf("snapshot chunks %v, want shared %v", got, before)
	}

	patch := randBytes(5000, 21)
	mustWrite(t, cl, "/f", MB+100, patch) // same client: its lease on chunk 1 is cached
	want := slices.Clone(orig)
	copy(want[MB+100:], patch)
	checkContent(t, c.Client(), "/f", want)
	checkContent(t, c.Client(), "/snap", orig)
	checkContent(t, cl, "/snap", orig)

	after := handles(c, "/f")
	if after[0] != before[0] || after[2] != before[2] || after[3] != before[3] {
		t.Fatalf("untouched chunks were copied: %v -> %v", before, after)
	}
	if after[1] == before[1] {
		t.Fatal("written chunk was not copied")
	}
	// The copy was made locally: same servers as the original chunk.
	snapLocs, fLocs := c.Locations("/snap")[1].Replicas, c.Locations("/f")[1].Replicas
	slices.Sort(snapLocs)
	slices.Sort(fLocs)
	if !slices.Equal(snapLocs, fLocs) {
		t.Fatalf("copy on %v, original on %v", fLocs, snapLocs)
	}

	// Record append to the shared last chunk copies it too.
	off, err := cl.RecordAppend("/snap", []byte("appended"))
	if err != nil {
		t.Fatal(err)
	}
	if off != int64(len(orig)) {
		t.Fatalf("append at %d, want %d", off, len(orig))
	}
	checkContent(t, c.Client(), "/f", want)
	checkContent(t, c.Client(), "/snap", append(slices.Clone(orig), "appended"...))
}

// Snapshotting a directory copies the tree; deleting the originals leaves
// the snapshot intact, and both survive a master restart from checkpoint +
// log with consistent reference counts.
func TestSnapshotDirectoryDeleteAndRestart(t *testing.T) {
	c := Start(t, Options{CheckpointEveryRecords: 9})
	cl := c.Client()
	cl.Mkdir("/d/sub", true)
	files := map[string][]byte{}
	for i, p := range []string{"/d/a", "/d/b", "/d/sub/c"} {
		cl.Create(p, 0)
		files[p] = randBytes(MB+i*5000, uint64(30+i))
		mustWrite(t, cl, p, 0, files[p])
	}
	if err := cl.Snapshot("/d", "/backup"); err != nil {
		t.Fatal(err)
	}
	if ents, err := cl.List("/backup/sub"); err != nil || len(ents) != 1 || ents[0].Name != "c" {
		t.Fatalf("list /backup/sub: %v, %v", ents, err)
	}
	for _, p := range []string{"/d/a", "/d/b", "/d/sub/c", "/d/sub", "/d"} {
		if err := cl.Delete(p); err != nil {
			t.Fatalf("delete %s: %v", p, err)
		}
	}
	patch := randBytes(100, 39)
	mustWrite(t, cl, "/backup/a", 10, patch)
	copy(files["/d/a"][10:], patch)

	c.StopMaster()
	c.StartMaster() // recovery checks that reference counts match the files
	c.WaitServers(3)
	cl2 := c.Client()
	for p, want := range files {
		checkContent(t, cl2, "/backup"+p[2:], want)
	}
	if _, err := cl2.Stat("/d"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("stat /d after delete: %v", err)
	}
	c.WaitFor(15*time.Second, "writes accepted after restart", func() bool {
		return cl2.Write("/backup/b", 0, patch) == nil
	})
	copy(files["/d/b"], patch)
	checkContent(t, cl2, "/backup/b", files["/d/b"])
}

func TestSnapshotRejects(t *testing.T) {
	c := Start(t, Options{})
	cl := c.Client()
	cl.Mkdir("/d", false)
	cl.Create("/d/f", 0)
	cl.Create("/g", 0)
	for _, tc := range []struct{ src, dst string }{
		{"/missing", "/x"}, // no source
		{"/d/f", "/g"},     // target exists
		{"/d/f", "/no/x"},  // target parent missing
		{"/d", "/d/inner"}, // into itself
		{"/", "/root"},
	} {
		if err := cl.Snapshot(tc.src, tc.dst); err == nil {
			t.Errorf("snapshot %s -> %s succeeded", tc.src, tc.dst)
		}
	}
}

// Writers keep overwriting chunk 0 with whole-chunk generations while a
// snapshot is taken. The snapshot must hold exactly one generation, and
// must not change afterwards even though the writers continue.
func TestSnapshotDuringWrites(t *testing.T) {
	c := Start(t, Options{ChunkServers: 4})
	gen := func(g int) []byte { return bytes.Repeat([]byte{byte(g)}, MB) }
	setup := c.Client()
	setup.Create("/hot", 0)
	mustWrite(t, setup, "/hot", 0, gen(1))

	var (
		stop atomic.Bool
		next atomic.Int32
		wg   sync.WaitGroup
	)
	next.Store(1)
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := c.Client()
			for !stop.Load() {
				if err := cl.Write("/hot", 0, gen(int(next.Add(1))%250+1)); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	for i := 0; i < 3; i++ {
		dst := fmt.Sprintf("/snap%d", i)
		if err := setup.Snapshot("/hot", dst); err != nil {
			t.Fatal(err)
		}
		first := readAll(t, c.Client(), dst)
		if len(first) != MB || !bytes.Equal(first, bytes.Repeat(first[:1], MB)) {
			t.Fatalf("%s is not one whole generation", dst)
		}
		time.Sleep(150 * time.Millisecond)
		if again := readAll(t, c.Client(), dst); !bytes.Equal(again, first) {
			l := c.Locations(dst)[0]
			for _, a := range l.Replicas {
				b := replicaBytes(t, c, a, l.Handle, MB)
				t.Logf("replica %s handle %d: len %d gen %d uniform %v", a, l.Handle, len(b), b[0], bytes.Equal(b, bytes.Repeat(b[:1], len(b))))
			}
			t.Logf("/hot handles %v", handles(c, "/hot"))
			t.Fatalf("%s changed after the snapshot: generation %d -> %d", dst, first[0], again[0])
		}
	}
	stop.Store(true)
	wg.Wait()
	if n := next.Load(); n < 10 {
		t.Fatalf("only %d writes during the test", n)
	}
}

// Snapshots of a directory race with writers appending blocks to files in
// it (chunk allocation and copy-on-write), creates and deletes. Namespace
// locks keep each snapshot a point-in-time copy: every file in it is a
// whole-block prefix of what its writer wrote. Nothing deadlocks, and the
// master recovers a consistent state afterwards.
func TestSnapshotNamespaceRace(t *testing.T) {
	c := Start(t, Options{ChunkServers: 4})
	setup := c.Client()
	setup.Mkdir("/d", false)
	const block = 256 << 10
	blockData := func(w, i int) []byte { return bytes.Repeat([]byte{byte(w*50 + i)}, block) }
	var (
		stop atomic.Bool
		wg   sync.WaitGroup
	)
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := c.Client()
			p := fmt.Sprintf("/d/w%d", w)
			if err := cl.Create(p, 0); err != nil {
				t.Error(err)
				return
			}
			for i := 0; i < 40 && !stop.Load(); i++ {
				if err := cl.Write(p, int64(i*block), blockData(w, i)); err != nil {
					t.Errorf("write %s block %d: %v", p, i, err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		cl := c.Client()
		for i := 0; !stop.Load(); i++ {
			p := fmt.Sprintf("/d/c%d", i)
			if err := cl.Create(p, 0); err != nil {
				t.Errorf("create %s: %v", p, err)
				return
			}
			if i%2 == 0 {
				if err := cl.Delete(p); err != nil {
					t.Errorf("delete %s: %v", p, err)
					return
				}
			}
		}
	}()
	for k := 0; k < 6; k++ {
		time.Sleep(80 * time.Millisecond)
		if err := setup.Snapshot("/d", fmt.Sprintf("/s%d", k)); err != nil {
			t.Fatalf("snapshot %d: %v", k, err)
		}
	}
	stop.Store(true)
	wg.Wait()

	c.StopMaster()
	c.StartMaster() // recovery verifies reference counts
	c.WaitServers(3)
	cl := c.Client()
	for k := 0; k < 6; k++ {
		for w := 0; w < 3; w++ {
			p := fmt.Sprintf("/s%d/w%d", k, w)
			if _, err := cl.Stat(p); errors.Is(err, client.ErrNotFound) {
				continue // the snapshot predates the file
			}
			got := readAll(t, cl, p)
			if len(got)%block != 0 {
				t.Fatalf("%s: %d bytes, not whole blocks", p, len(got))
			}
			for i := 0; i < len(got)/block; i++ {
				if !bytes.Equal(got[i*block:(i+1)*block], blockData(w, i)) {
					t.Fatalf("%s block %d is wrong", p, i)
				}
			}
		}
	}
}
