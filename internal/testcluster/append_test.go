package testcluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
	"github.com/dhruv10050/Google-File-Sytem/pkg/client"
)

// ───────────────────────── Record append (§3.3) ─────────────────────────

type appended struct {
	off  int64
	data []byte
}

// appendRecords runs writers clients concurrently, each appending n records
// of 1 KiB–200 KiB with unique contents, and returns every acknowledged
// record. With the tests' 1 MiB chunks, records regularly fail to fit and
// chunks get padded.
func appendRecords(t *testing.T, c *Cluster, path string, writers, n int, during func(i int)) []appended {
	t.Helper()
	var (
		mu   sync.Mutex
		out  []appended
		errs []error
		wg   sync.WaitGroup
	)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := c.Client()
			for i := 0; i < n; i++ {
				seed := uint64(w*100000 + i)
				data := randBytes(1024+int(seed*7919%(199*1024)), seed)
				off, err := cl.RecordAppend(path, data)
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				} else {
					out = append(out, appended{off, data})
				}
				mu.Unlock()
				if w == 0 && during != nil {
					during(i)
				}
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("%d appends failed, first: %v", len(errs), errs[0])
	}
	return out
}

// checkRecords verifies the record-append guarantees for acknowledged
// records: they never overlap or span a chunk boundary, they read back
// intact through the client, and every live replica holds each record at
// the same offset. Chunks before the last are padded to full size on every
// replica.
func checkRecords(t *testing.T, c *Cluster, path string, recs []appended) {
	t.Helper()
	cs := c.opt.ChunkSize
	sort.Slice(recs, func(i, j int) bool { return recs[i].off < recs[j].off })
	for i, r := range recs {
		if r.off/cs != (r.off+int64(len(r.data))-1)/cs {
			t.Fatalf("record at %d (%d bytes) spans a chunk boundary", r.off, len(r.data))
		}
		if i > 0 && recs[i-1].off+int64(len(recs[i-1].data)) > r.off {
			t.Fatalf("records at %d and %d overlap", recs[i-1].off, r.off)
		}
	}
	cl := c.Client()
	for _, r := range recs {
		buf := make([]byte, len(r.data))
		if _, err := cl.ReadAt(path, buf, r.off); err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("read record at %d: %v", r.off, err)
		}
		if !bytes.Equal(buf, r.data) {
			t.Fatalf("record at %d reads back wrong", r.off)
		}
	}
	locs := c.Locations(path)
	for _, l := range locs {
		for _, addr := range l.Replicas {
			got := replicaBytes(t, c, addr, l.Handle, cs)
			if l.Index < int64(len(locs))-1 && int64(len(got)) != cs {
				t.Fatalf("chunk %d on %s: length %d, want padded to %d", l.Index, addr, len(got), cs)
			}
			for _, r := range recs {
				if r.off/cs != l.Index {
					continue
				}
				o := r.off - l.Index*cs
				if o+int64(len(r.data)) > int64(len(got)) || !bytes.Equal(got[o:o+int64(len(r.data))], r.data) {
					t.Fatalf("chunk %d on %s: record at chunk offset %d differs", l.Index, addr, o)
				}
			}
		}
	}
}

// replicaBytes reads a whole replica straight from a chunkserver.
func replicaBytes(t *testing.T, c *Cluster, addr string, h uint64, n int64) []byte {
	t.Helper()
	conn, err := common.Dial(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := pb.NewChunkServerClient(conn).Read(ctx, &pb.ReadRequest{Handle: h, Length: n})
	if err != nil {
		t.Fatalf("read %016x on %s: %v", h, addr, err)
	}
	var out []byte
	for {
		f, err := stream.Recv()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("read %016x on %s: %v", h, addr, err)
		}
		out = append(out, f.Data...)
	}
}

func TestRecordAppendConcurrent(t *testing.T) {
	c := Start(t, Options{ChunkServers: 4})
	cl := c.Client()
	if err := cl.Create("/log", 0); err != nil {
		t.Fatal(err)
	}
	recs := appendRecords(t, c, "/log", 8, 40, nil)
	if len(recs) != 8*40 {
		t.Fatalf("%d records acknowledged, want %d", len(recs), 8*40)
	}
	if n := len(c.Locations("/log")); n < 3 {
		t.Fatalf("only %d chunks: the test should have filled several", n)
	}
	checkRecords(t, c, "/log", recs)
}

func TestRecordAppendSizeLimit(t *testing.T) {
	c := Start(t, Options{})
	cl := c.Client()
	cl.Create("/l", 0)
	limit := int(c.opt.ChunkSize / 4)
	if _, err := cl.RecordAppend("/l", make([]byte, limit+1)); !errors.Is(err, client.ErrRecordTooLarge) {
		t.Fatalf("oversized record: err = %v, want ErrRecordTooLarge", err)
	}
	// Four maximum-size records fill chunk 0 exactly; the fifth goes to chunk 1.
	for i := 0; i < 5; i++ {
		off, err := cl.RecordAppend("/l", randBytes(limit, uint64(i)))
		if err != nil {
			t.Fatal(err)
		}
		if want := int64(i * limit); off != want {
			t.Fatalf("record %d at %d, want %d", i, off, want)
		}
	}
	if _, err := cl.RecordAppend("/x", []byte("a")); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("append to missing file: err = %v, want ErrNotFound", err)
	}
}

// Appends keep succeeding while a chunkserver holding the file's current
// chunk dies, and no acknowledged record is lost or damaged (§3.3: at least
// once, at the same offset on every replica).
func TestRecordAppendSurvivesChunkserverDeath(t *testing.T) {
	c := Start(t, Options{ChunkServers: 5})
	cl := c.Client()
	cl.Create("/log", 0)
	cl.RecordAppend("/log", []byte("first"))
	killed := -1
	recs := appendRecords(t, c, "/log", 4, 30, func(i int) {
		if i == 10 {
			locs := c.Locations("/log")
			killed = c.Index(locs[len(locs)-1].Replicas[0])
			c.StopChunkServer(killed)
		}
	})
	if killed < 0 {
		t.Fatal("no chunkserver was stopped")
	}
	c.WaitFor(20*time.Second, "every chunk back to 3 replicas", func() bool {
		for _, l := range c.Locations("/log") {
			if len(l.Replicas) != 3 {
				return false
			}
		}
		return true
	})
	checkRecords(t, c, "/log", recs)
}
