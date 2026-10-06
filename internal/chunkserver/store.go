package chunkserver

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

var (
	ErrNoChunk   = errors.New("chunk not found")
	ErrCorrupted = errors.New("chunk checksum mismatch")
	ErrExists    = errors.New("chunk already exists")
	ErrBounds    = errors.New("write exceeds chunk size")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func checksum(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// chunk is one replica held by this server.
//
// Locking: mutMu serializes the mutation pipeline for the chunk (primary
// assigns serial numbers and forwards under it; version changes and lease
// revocation wait for in-flight mutations). mu guards the on-disk bytes and
// metadata so reads never observe a half-applied write. leaseMu guards the
// lease fields.
type chunk struct {
	handle common.ChunkHandle

	mutMu sync.Mutex
	mu    sync.RWMutex

	version   uint64
	length    int64
	crcs      []uint32
	corrupted bool

	serial uint64 // primary: last assigned; secondary: last applied (per version)

	leaseMu      sync.Mutex
	leaseExpiry  time.Time
	lastMutation time.Time
}

// Store keeps chunks as plain files plus a checksum/version sidecar (§2.5, §5.2):
//
//	<dir>/chunks/<handle>.chunk   raw data, file length == chunk length
//	<dir>/chunks/<handle>.meta    ChunkMeta{version, length, block CRCs}
type Store struct {
	dir       string
	chunkSize int64
	fsyncData bool

	mu     sync.RWMutex
	chunks map[common.ChunkHandle]*chunk
}

func OpenStore(dir string, chunkSize int64, fsyncData bool) (*Store, error) {
	s := &Store{
		dir:       filepath.Join(dir, "chunks"),
		chunkSize: chunkSize,
		fsyncData: fsyncData,
		chunks:    map[common.ChunkHandle]*chunk{},
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return nil, err
	}
	return s, s.load()
}

func (s *Store) chunkPath(h common.ChunkHandle) string {
	return filepath.Join(s.dir, h.String()+".chunk")
}
func (s *Store) metaPath(h common.ChunkHandle) string {
	return filepath.Join(s.dir, h.String()+".meta")
}

// load scans the chunk directory at startup. Chunks whose data file does not
// match their sidecar are kept but marked corrupted, so they are reported to
// the master and replaced rather than served.
func (s *Store) load() error {
	ents, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	metas := map[common.ChunkHandle]bool{}
	for _, e := range ents {
		name := e.Name()
		if strings.HasSuffix(name, ".tmp") {
			os.Remove(filepath.Join(s.dir, name))
			continue
		}
		if !strings.HasSuffix(name, ".meta") {
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSuffix(name, ".meta"), 16, 64)
		if err != nil {
			continue
		}
		h := common.ChunkHandle(v)
		metas[h] = true
		c := &chunk{handle: h}
		raw, err := os.ReadFile(s.metaPath(h))
		var m pb.ChunkMeta
		if err == nil {
			err = proto.Unmarshal(raw, &m)
		}
		if err != nil {
			c.corrupted = true
		} else {
			c.version, c.length, c.crcs = m.Version, m.Length, m.BlockCrc
			fi, err := os.Stat(s.chunkPath(h))
			if err != nil || fi.Size() != c.length || int64(len(c.crcs)) != blocksFor(c.length) {
				c.corrupted = true
			}
		}
		s.chunks[h] = c
	}
	// A data file without a sidecar is an interrupted creation: discard it.
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".chunk") {
			continue
		}
		v, err := strconv.ParseUint(strings.TrimSuffix(name, ".chunk"), 16, 64)
		if err == nil && !metas[common.ChunkHandle(v)] {
			os.Remove(filepath.Join(s.dir, name))
		}
	}
	return nil
}

func blocksFor(length int64) int64 { return (length + common.BlockSize - 1) / common.BlockSize }

func (s *Store) SetChunkSize(n int64) {
	s.mu.Lock()
	s.chunkSize = n
	s.mu.Unlock()
}

func (s *Store) ChunkSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.chunkSize
}

func (s *Store) Get(h common.ChunkHandle) (*chunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.chunks[h]
	if !ok {
		return nil, ErrNoChunk
	}
	return c, nil
}

// All returns every chunk sorted by handle.
func (s *Store) All() []*chunk {
	s.mu.RLock()
	out := make([]*chunk, 0, len(s.chunks))
	for _, c := range s.chunks {
		out = append(out, c)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].handle < out[j].handle })
	return out
}

// Report returns version/length for a healthy chunk; ok=false if corrupted.
func (c *chunk) Report() (r *pb.ChunkReport, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return &pb.ChunkReport{Handle: uint64(c.handle), Version: c.version, Length: c.length}, !c.corrupted
}

func (c *chunk) Version() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

func (c *chunk) Length() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.length
}

func (c *chunk) MarkCorrupted() {
	c.mu.Lock()
	c.corrupted = true
	c.mu.Unlock()
}

func (c *chunk) IsCorrupted() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.corrupted
}

// DiskUsed is the total logical size of all chunks.
func (s *Store) DiskUsed() int64 {
	var n int64
	for _, c := range s.All() {
		c.mu.RLock()
		n += c.length
		c.mu.RUnlock()
	}
	return n
}

// Create makes a new empty chunk (§2.5 lazy allocation: the file grows as
// data is written).
func (s *Store) Create(h common.ChunkHandle, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.chunks[h]; ok {
		if c.version == version && c.length == 0 && !c.corrupted {
			return nil // idempotent retry
		}
		return ErrExists
	}
	f, err := os.OpenFile(s.chunkPath(h), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	f.Close()
	c := &chunk{handle: h, version: version}
	if err := s.persistMeta(c, true); err != nil {
		os.Remove(s.chunkPath(h))
		return err
	}
	s.chunks[h] = c
	return nil
}

// Install atomically creates a chunk from complete contents (used by
// re-replication clones). Any existing replica with the same handle is
// replaced.
func (s *Store) Install(h common.ChunkHandle, version uint64, data []byte) error {
	if int64(len(data)) > s.chunkSize {
		return ErrBounds
	}
	tmp := s.chunkPath(h) + ".tmp"
	if err := writeFileSync(tmp, data, true); err != nil {
		return err
	}
	c := &chunk{handle: h, version: version, length: int64(len(data))}
	for off := int64(0); off < c.length; off += common.BlockSize {
		c.crcs = append(c.crcs, checksum(data[off:min(off+common.BlockSize, c.length)]))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Rename(tmp, s.chunkPath(h)); err != nil {
		return err
	}
	if err := s.persistMeta(c, true); err != nil {
		return err
	}
	s.chunks[h] = c
	return nil
}

func (s *Store) Delete(h common.ChunkHandle) {
	s.mu.Lock()
	delete(s.chunks, h)
	s.mu.Unlock()
	os.Remove(s.metaPath(h))
	os.Remove(s.chunkPath(h))
}

// SetVersion moves a replica from oldV to newV and persists it durably
// before returning (§4.5). A replica not at oldV (and not already at newV)
// is stale.
func (s *Store) SetVersion(c *chunk, oldV, newV uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.corrupted {
		return ErrCorrupted
	}
	if c.version == newV {
		return nil
	}
	if c.version != oldV {
		return fmt.Errorf("stale replica: have version %d, expected %d", c.version, oldV)
	}
	c.version = newV
	c.serial = 0
	return s.persistMeta(c, true)
}

// Write applies data at off. It verifies existing blocks that are only
// partially overwritten before touching them (§5.2), then recomputes the
// checksums of every affected block.
func (s *Store) Write(c *chunk, off int64, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.corrupted {
		return ErrCorrupted
	}
	end := off + int64(len(data))
	if off < 0 || end > s.chunkSize {
		return ErrBounds
	}
	if len(data) == 0 {
		return nil
	}
	f, err := os.OpenFile(s.chunkPath(c.handle), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	B := common.BlockSize
	oldLen := c.length
	newLen := max(oldLen, end)
	// Blocks from the first one this write touches (or that a gap before it
	// extends) to the last one it touches. Blocks after it are unchanged and
	// keep their checksums: recomputing them would re-read the rest of the
	// chunk and would hide any corruption in it.
	startB := min(off, oldLen) / B
	lastB := (end - 1) / B

	// Verify blocks holding existing data that this write will not fully
	// replace but whose checksum will be recomputed.
	for _, b := range []int64{startB, (end - 1) / B} {
		bs, existingEnd := b*B, min(b*B+B, oldLen)
		if bs >= oldLen || (off <= bs && end >= existingEnd) {
			continue
		}
		blk := make([]byte, existingEnd-bs)
		if _, err := f.ReadAt(blk, bs); err != nil {
			return err
		}
		if checksum(blk) != c.crcs[b] {
			c.corrupted = true
			return ErrCorrupted
		}
	}

	if _, err := f.WriteAt(data, off); err != nil {
		return err
	}
	for int64(len(c.crcs)) <= lastB {
		c.crcs = append(c.crcs, 0)
	}
	for b := startB; b <= lastB; b++ {
		bs, be := b*B, min(b*B+B, newLen)
		if bs >= off && be <= end {
			c.crcs[b] = checksum(data[bs-off : be-off])
			continue
		}
		blk := make([]byte, be-bs)
		if _, err := f.ReadAt(blk, bs); err != nil && err != io.EOF {
			return err
		}
		c.crcs[b] = checksum(blk)
	}
	c.length = newLen
	if s.fsyncData {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return s.persistMeta(c, s.fsyncData)
}

// Pad extends the chunk with zeros to n bytes; a chunk already that long is
// left alone. Record append pads a chunk that cannot fit the next record
// (§3.3). Records are at most a quarter chunk, so the padding is too.
func (s *Store) Pad(c *chunk, n int64) error {
	l := c.Length()
	if l >= n {
		return nil
	}
	return s.Write(c, l, make([]byte, n-l))
}

// Read returns up to n bytes at off, verifying the checksum of every
// overlapped block. It returns fewer bytes at the end of the chunk.
func (s *Store) Read(c *chunk, off, n int64) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.corrupted {
		return nil, ErrCorrupted
	}
	if off < 0 || n < 0 {
		return nil, fmt.Errorf("invalid range")
	}
	if off >= c.length || n == 0 {
		return nil, nil
	}
	n = min(n, c.length-off)
	B := common.BlockSize
	bs := off / B * B
	be := min((off+n+B-1)/B*B, c.length)
	buf := make([]byte, be-bs)
	f, err := os.Open(s.chunkPath(c.handle))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := io.ReadFull(io.NewSectionReader(f, bs, be-bs), buf); err != nil {
		return nil, ErrCorrupted // file shorter than metadata says
	}
	for b := bs; b < be; b += B {
		if checksum(buf[b-bs:min(b+B, be)-bs]) != c.crcs[b/B] {
			return nil, ErrCorrupted
		}
	}
	return buf[off-bs : off-bs+n], nil
}

func (s *Store) persistMeta(c *chunk, sync bool) error {
	raw, err := proto.Marshal(&pb.ChunkMeta{
		Handle: uint64(c.handle), Version: c.version, Length: c.length, BlockCrc: c.crcs,
	})
	if err != nil {
		return err
	}
	tmp := s.metaPath(c.handle) + ".tmp"
	if err := writeFileSync(tmp, raw, sync); err != nil {
		return err
	}
	return os.Rename(tmp, s.metaPath(c.handle))
}

func writeFileSync(path string, data []byte, sync bool) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}
