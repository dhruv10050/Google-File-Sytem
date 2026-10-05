package chunkserver

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

const testChunk = 1 << 20

func randBytes(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func newChunk(t *testing.T, dir string, h common.ChunkHandle) (*Store, *chunk) {
	t.Helper()
	s, err := OpenStore(dir, testChunk, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(h, 1); err != nil {
		t.Fatal(err)
	}
	c, err := s.Get(h)
	if err != nil {
		t.Fatal(err)
	}
	return s, c
}

// Writes at arbitrary offsets, including ones that straddle 64 KB blocks,
// overwrite part of a block, or leave a gap, must read back exactly, with
// gaps reading as zeros.
func TestStoreWriteRead(t *testing.T) {
	s, c := newChunk(t, t.TempDir(), 1)
	model := make([]byte, 0, testChunk)
	writes := []struct{ off, n int }{
		{0, 100}, {50, 70000}, {200000, 5}, {131071, 2}, {0, 300000}, {65536, 65536}, {500000, 1000},
	}
	for i, w := range writes {
		data := randBytes(w.n, uint64(i))
		if err := s.Write(c, int64(w.off), data); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if end := w.off + w.n; end > len(model) {
			model = append(model, make([]byte, end-len(model))...)
		}
		copy(model[w.off:], data)
	}
	got, err := s.Read(c, 0, testChunk)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, model) {
		t.Fatalf("read back %d bytes, model has %d; contents differ", len(got), len(model))
	}
	part, err := s.Read(c, 65530, 20)
	if err != nil || !bytes.Equal(part, model[65530:65550]) {
		t.Fatalf("unaligned read wrong: %v", err)
	}
	if b, err := s.Read(c, int64(len(model)), 10); err != nil || len(b) != 0 {
		t.Fatalf("read at end = %d bytes, %v", len(b), err)
	}
	if err := s.Write(c, testChunk-1, []byte{1, 2}); !errors.Is(err, ErrBounds) {
		t.Fatalf("write past chunk end: %v", err)
	}
}

// Flipping a byte on disk must be caught by the block checksum on read and
// on a partial overwrite of that block (§5.2).
func TestStoreDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	s, c := newChunk(t, dir, 2)
	if err := s.Write(c, 0, randBytes(200000, 7)); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(s.chunkPath(2), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte{0xAA}, 70000) // inside block 1
	f.Close()

	if _, err := s.Read(c, 0, 1000); err != nil {
		t.Fatalf("block 0 is intact but read failed: %v", err)
	}
	if _, err := s.Read(c, 69000, 10); !errors.Is(err, ErrCorrupted) {
		t.Fatalf("read of corrupted block: err = %v", err)
	}

	s2, c2 := newChunk(t, t.TempDir(), 3)
	s2.Write(c2, 0, randBytes(100000, 8))
	f, _ = os.OpenFile(s2.chunkPath(3), os.O_RDWR, 0)
	f.WriteAt([]byte{0x55}, 66000)
	f.Close()
	if err := s2.Write(c2, 65600, []byte("x")); !errors.Is(err, ErrCorrupted) {
		t.Fatalf("partial overwrite of corrupted block: err = %v", err)
	}
	if !c2.IsCorrupted() {
		t.Fatal("chunk not marked corrupted")
	}
}

func TestStoreReload(t *testing.T) {
	dir := t.TempDir()
	s, c := newChunk(t, dir, 4)
	data := randBytes(150000, 9)
	s.Write(c, 0, data)
	if err := s.SetVersion(c, 1, 2); err != nil {
		t.Fatal(err)
	}
	// Leftovers of interrupted operations are cleaned up on load.
	os.WriteFile(s.chunkPath(5), []byte("orphan"), 0o644)
	os.WriteFile(s.chunkPath(6)+".tmp", []byte("partial clone"), 0o644)

	s2, err := OpenStore(dir, testChunk, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(s2.All()) != 1 {
		t.Fatalf("reloaded %d chunks, want 1", len(s2.All()))
	}
	c2, _ := s2.Get(4)
	if c2.Version() != 2 || c2.IsCorrupted() {
		t.Fatalf("version %d corrupted %v", c2.Version(), c2.IsCorrupted())
	}
	got, err := s2.Read(c2, 0, testChunk)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("data after reload differs (%v)", err)
	}
	for _, p := range []string{s.chunkPath(5), s.chunkPath(6) + ".tmp"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s not cleaned up", p)
		}
	}

	// A data file whose size disagrees with its metadata is corrupted.
	os.Truncate(s2.chunkPath(4), 1000)
	s3, _ := OpenStore(dir, testChunk, false)
	if c3, _ := s3.Get(4); !c3.IsCorrupted() {
		t.Fatal("truncated chunk not detected on load")
	}
}

func TestStoreVersions(t *testing.T) {
	s, c := newChunk(t, t.TempDir(), 7)
	if err := s.SetVersion(c, 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.SetVersion(c, 1, 2); err != nil {
		t.Fatalf("idempotent retry failed: %v", err)
	}
	if err := s.SetVersion(c, 5, 6); err == nil {
		t.Fatal("stale replica accepted a version bump")
	}
	if err := s.Create(7, 1); !errors.Is(err, ErrExists) {
		t.Fatalf("re-create: %v", err)
	}
}

func TestStoreInstall(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testChunk, false)
	if err != nil {
		t.Fatal(err)
	}
	data := randBytes(300000, 10)
	if err := s.Install(8, 3, data); err != nil {
		t.Fatal(err)
	}
	c, _ := s.Get(8)
	got, err := s.Read(c, 0, testChunk)
	if err != nil || !bytes.Equal(got, data) || c.Version() != 3 {
		t.Fatalf("installed chunk wrong: %v", err)
	}
}

func TestDataBuffer(t *testing.T) {
	b := newDataBuffer(100, time.Hour)
	b.Put("a", make([]byte, 60))
	b.Put("b", make([]byte, 60)) // evicts a
	_, okA := b.Get("a")
	_, okB := b.Get("b")
	if okA || !okB {
		t.Fatal("LRU eviction by size failed")
	}
}
