package master

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

var (
	ErrNotFound = errors.New("no such file or directory")
	ErrExists   = errors.New("already exists")
	ErrNotDir   = errors.New("parent is not a directory")
	ErrNotEmpty = errors.New("directory not empty")
	ErrInvalid  = errors.New("invalid operation")
)

// FileMeta is the persistent per-file metadata (§2.6).
type FileMeta struct {
	Replication int32
	Ctime       int64
	Chunks      []common.ChunkHandle

	// Set while the file sits under TrashDir awaiting garbage collection.
	DeletedFrom string
	Deleted     int64 // unix nanos
}

// TrashDir holds deleted files under hidden names until garbage collection
// removes them (§4.4.1). Clients cannot create anything inside it.
const TrashDir = "/.deleted"

func InTrash(p string) bool { return p == TrashDir || strings.HasPrefix(p, TrashDir+"/") }

// TrashName is the hidden name a file deleted by rec gets. It includes the
// deletion time, as in the paper, and the LSN, which makes it unique.
func TrashName(p string, rec *pb.LogRecord) string {
	return fmt.Sprintf("%s/%d-%d-%s", TrashDir, rec.UnixNanos, rec.Lsn, path.Base(p))
}

// ChunkMeta is the persistent per-chunk metadata. Replica locations are
// deliberately absent: they are soft state rebuilt from chunkservers (§2.6.2).
type ChunkMeta struct {
	Version  uint64
	Refcount int32
}

// State is everything the master persists. It changes only through Apply,
// which the primary (on commit), recovery (on replay) and the shadow master
// (on log tailing) all share, so the three can never diverge.
type State struct {
	Dirs       map[string]bool
	Files      map[string]*FileMeta
	Chunks     map[common.ChunkHandle]*ChunkMeta
	NextHandle uint64
	LSN        uint64 // last applied log sequence number
}

func NewState() *State {
	return &State{
		Dirs:       map[string]bool{"/": true},
		Files:      map[string]*FileMeta{},
		Chunks:     map[common.ChunkHandle]*ChunkMeta{},
		NextHandle: 1,
	}
}

func (s *State) exists(p string) bool {
	_, f := s.Files[p]
	return f || s.Dirs[p]
}

func (s *State) hasChildren(dir string) bool {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	for d := range s.Dirs {
		if d != dir && strings.HasPrefix(d, prefix) {
			return true
		}
	}
	for f := range s.Files {
		if strings.HasPrefix(f, prefix) {
			return true
		}
	}
	return false
}

// Validate checks that rec can be applied to the current state without
// changing anything.
func (s *State) Validate(rec *pb.LogRecord) error {
	switch op := rec.Op.(type) {
	case *pb.LogRecord_Mkdir:
		p := op.Mkdir.Path
		if InTrash(p) {
			return fmt.Errorf("%s is reserved: %w", p, ErrInvalid)
		}
		if s.exists(p) {
			return fmt.Errorf("%s: %w", p, ErrExists)
		}
		if !s.Dirs[common.Parent(p)] {
			return fmt.Errorf("%s: %w", common.Parent(p), ErrNotDir)
		}
	case *pb.LogRecord_CreateFile:
		p := op.CreateFile.Path
		if InTrash(p) {
			return fmt.Errorf("%s is reserved: %w", p, ErrInvalid)
		}
		if s.exists(p) {
			return fmt.Errorf("%s: %w", p, ErrExists)
		}
		if !s.Dirs[common.Parent(p)] {
			return fmt.Errorf("%s: %w", common.Parent(p), ErrNotDir)
		}
		if op.CreateFile.Replication < 1 {
			return fmt.Errorf("replication %d: %w", op.CreateFile.Replication, ErrInvalid)
		}
	case *pb.LogRecord_DeleteFile:
		p := op.DeleteFile.Path
		if p == "/" {
			return fmt.Errorf("cannot delete root: %w", ErrInvalid)
		}
		if s.Dirs[p] {
			if s.hasChildren(p) {
				return fmt.Errorf("%s: %w", p, ErrNotEmpty)
			}
		} else if _, ok := s.Files[p]; !ok {
			return fmt.Errorf("%s: %w", p, ErrNotFound)
		}
	case *pb.LogRecord_AddChunk:
		a := op.AddChunk
		f, ok := s.Files[a.Path]
		if !ok {
			return fmt.Errorf("%s: %w", a.Path, ErrNotFound)
		}
		if a.Index != int64(len(f.Chunks)) {
			return fmt.Errorf("add chunk index %d to file with %d chunks: %w", a.Index, len(f.Chunks), ErrInvalid)
		}
		if _, dup := s.Chunks[common.ChunkHandle(a.Handle)]; dup {
			return fmt.Errorf("chunk handle %d reused: %w", a.Handle, ErrInvalid)
		}
	case *pb.LogRecord_SetVersion:
		c, ok := s.Chunks[common.ChunkHandle(op.SetVersion.Handle)]
		if !ok {
			return fmt.Errorf("chunk %d: %w", op.SetVersion.Handle, ErrNotFound)
		}
		if op.SetVersion.Version <= c.Version {
			return fmt.Errorf("version %d not above %d: %w", op.SetVersion.Version, c.Version, ErrInvalid)
		}
	case *pb.LogRecord_Snapshot:
		src, dst := op.Snapshot.Source, op.Snapshot.Target
		if !s.exists(src) {
			return fmt.Errorf("%s: %w", src, ErrNotFound)
		}
		if InTrash(dst) {
			return fmt.Errorf("%s is reserved: %w", dst, ErrInvalid)
		}
		if s.exists(dst) {
			return fmt.Errorf("%s: %w", dst, ErrExists)
		}
		if !s.Dirs[common.Parent(dst)] {
			return fmt.Errorf("%s: %w", common.Parent(dst), ErrNotDir)
		}
		if s.Dirs[src] && (src == "/" || strings.HasPrefix(dst, src+"/")) {
			return fmt.Errorf("snapshot of %s into itself: %w", src, ErrInvalid)
		}
	case *pb.LogRecord_ReplaceChunk:
		r := op.ReplaceChunk
		f, ok := s.Files[r.Path]
		if !ok {
			return fmt.Errorf("%s: %w", r.Path, ErrNotFound)
		}
		if r.Index < 0 || r.Index >= int64(len(f.Chunks)) || f.Chunks[r.Index] != common.ChunkHandle(r.OldHandle) {
			return fmt.Errorf("%s chunk %d is not %d: %w", r.Path, r.Index, r.OldHandle, ErrInvalid)
		}
		if _, dup := s.Chunks[common.ChunkHandle(r.NewHandle)]; dup {
			return fmt.Errorf("chunk handle %d reused: %w", r.NewHandle, ErrInvalid)
		}
	case *pb.LogRecord_TrashFile:
		p := op.TrashFile.Path
		if _, ok := s.Files[p]; !ok {
			return fmt.Errorf("%s: %w", p, ErrNotFound)
		}
		if InTrash(p) {
			return fmt.Errorf("%s is already deleted: %w", p, ErrInvalid)
		}
	case *pb.LogRecord_RestoreFile:
		h := op.RestoreFile.Hidden
		f, ok := s.Files[h]
		if !ok {
			return fmt.Errorf("%s: %w", h, ErrNotFound)
		}
		if !InTrash(h) || f.DeletedFrom == "" {
			return fmt.Errorf("%s is not a deleted file: %w", h, ErrInvalid)
		}
		if s.exists(f.DeletedFrom) {
			return fmt.Errorf("%s: %w", f.DeletedFrom, ErrExists)
		}
		if !s.Dirs[common.Parent(f.DeletedFrom)] {
			return fmt.Errorf("%s: %w", common.Parent(f.DeletedFrom), ErrNotDir)
		}
	default:
		return fmt.Errorf("unknown log op %T: %w", rec.Op, ErrInvalid)
	}
	return nil
}

// Apply validates and applies one record. Records must arrive in LSN order.
// It returns the chunks that lost their last reference (now orphaned).
func (s *State) Apply(rec *pb.LogRecord) ([]common.ChunkHandle, error) {
	if rec.Lsn != s.LSN+1 {
		return nil, fmt.Errorf("log gap: applying lsn %d after %d", rec.Lsn, s.LSN)
	}
	if err := s.Validate(rec); err != nil {
		return nil, err
	}
	var orphaned []common.ChunkHandle
	switch op := rec.Op.(type) {
	case *pb.LogRecord_Mkdir:
		s.Dirs[op.Mkdir.Path] = true
	case *pb.LogRecord_CreateFile:
		s.Files[op.CreateFile.Path] = &FileMeta{Replication: op.CreateFile.Replication, Ctime: rec.UnixNanos}
	case *pb.LogRecord_DeleteFile:
		p := op.DeleteFile.Path
		if s.Dirs[p] {
			delete(s.Dirs, p)
			break
		}
		for _, h := range s.Files[p].Chunks {
			c := s.Chunks[h]
			c.Refcount--
			if c.Refcount <= 0 {
				delete(s.Chunks, h)
				orphaned = append(orphaned, h)
			}
		}
		delete(s.Files, p)
	case *pb.LogRecord_AddChunk:
		a := op.AddChunk
		h := common.ChunkHandle(a.Handle)
		f := s.Files[a.Path]
		f.Chunks = append(f.Chunks, h)
		s.Chunks[h] = &ChunkMeta{Version: a.Version, Refcount: 1}
		s.NextHandle = max(s.NextHandle, a.Handle+1)
	case *pb.LogRecord_SetVersion:
		s.Chunks[common.ChunkHandle(op.SetVersion.Handle)].Version = op.SetVersion.Version
	case *pb.LogRecord_Snapshot:
		s.applySnapshot(op.Snapshot.Source, op.Snapshot.Target, rec.UnixNanos)
	case *pb.LogRecord_TrashFile:
		p := op.TrashFile.Path
		f := s.Files[p]
		f.DeletedFrom, f.Deleted = p, rec.UnixNanos
		s.Dirs[TrashDir] = true
		s.Files[TrashName(p, rec)] = f
		delete(s.Files, p)
	case *pb.LogRecord_RestoreFile:
		h := op.RestoreFile.Hidden
		f := s.Files[h]
		s.Files[f.DeletedFrom] = f
		f.DeletedFrom, f.Deleted = "", 0
		delete(s.Files, h)
	case *pb.LogRecord_ReplaceChunk:
		r := op.ReplaceChunk
		old, h := common.ChunkHandle(r.OldHandle), common.ChunkHandle(r.NewHandle)
		s.Files[r.Path].Chunks[r.Index] = h
		s.Chunks[h] = &ChunkMeta{Version: r.Version, Refcount: 1}
		s.NextHandle = max(s.NextHandle, r.NewHandle+1)
		c := s.Chunks[old]
		c.Refcount--
		if c.Refcount <= 0 {
			delete(s.Chunks, old)
			orphaned = append(orphaned, old)
		}
	}
	s.LSN = rec.Lsn
	return orphaned, nil
}

// applySnapshot copies the metadata of src (a file, or a directory and
// everything under it) to dst. Copies share chunks with the originals.
func (s *State) applySnapshot(src, dst string, now int64) {
	copyFile := func(from, to string) {
		f := s.Files[from]
		s.Files[to] = &FileMeta{Replication: f.Replication, Ctime: now, Chunks: append([]common.ChunkHandle(nil), f.Chunks...)}
		for _, h := range f.Chunks {
			s.Chunks[h].Refcount++
		}
	}
	if !s.Dirs[src] {
		copyFile(src, dst)
		return
	}
	// Collect first: the copies are inserted into the maps being scanned.
	prefix := src + "/"
	var dirs, files []string
	for d := range s.Dirs {
		if strings.HasPrefix(d, prefix) {
			dirs = append(dirs, d)
		}
	}
	for p := range s.Files {
		if strings.HasPrefix(p, prefix) {
			files = append(files, p)
		}
	}
	s.Dirs[dst] = true
	for _, d := range dirs {
		s.Dirs[dst+"/"+d[len(prefix):]] = true
	}
	for _, p := range files {
		copyFile(p, dst+"/"+p[len(prefix):])
	}
}

// SnapshotChunks returns the chunks a snapshot of src would share.
func (s *State) SnapshotChunks(src string) map[common.ChunkHandle]bool {
	out := map[common.ChunkHandle]bool{}
	for p, f := range s.Files {
		if p == src || (s.Dirs[src] && strings.HasPrefix(p, strings.TrimSuffix(src, "/")+"/")) {
			for _, h := range f.Chunks {
				out[h] = true
			}
		}
	}
	return out
}

// ToCheckpoint serializes the full state.
func (s *State) ToCheckpoint() *pb.Checkpoint {
	cp := &pb.Checkpoint{Lsn: s.LSN, NextHandle: s.NextHandle}
	for d := range s.Dirs {
		cp.Dirs = append(cp.Dirs, d)
	}
	sort.Strings(cp.Dirs)
	paths := make([]string, 0, len(s.Files))
	for p := range s.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		f := s.Files[p]
		fs := &pb.FileState{Path: p, Replication: f.Replication, CtimeUnixNanos: f.Ctime, DeletedFrom: f.DeletedFrom, DeletedUnixNanos: f.Deleted}
		for _, h := range f.Chunks {
			fs.Chunks = append(fs.Chunks, uint64(h))
		}
		cp.Files = append(cp.Files, fs)
	}
	hs := make([]common.ChunkHandle, 0, len(s.Chunks))
	for h := range s.Chunks {
		hs = append(hs, h)
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i] < hs[j] })
	for _, h := range hs {
		c := s.Chunks[h]
		cp.Chunks = append(cp.Chunks, &pb.ChunkState{Handle: uint64(h), Version: c.Version, Refcount: c.Refcount})
	}
	return cp
}

func StateFromCheckpoint(cp *pb.Checkpoint) (*State, error) {
	s := NewState()
	s.LSN, s.NextHandle = cp.Lsn, cp.NextHandle
	for _, d := range cp.Dirs {
		s.Dirs[d] = true
	}
	for _, f := range cp.Files {
		fm := &FileMeta{Replication: f.Replication, Ctime: f.CtimeUnixNanos, DeletedFrom: f.DeletedFrom, Deleted: f.DeletedUnixNanos}
		for _, h := range f.Chunks {
			fm.Chunks = append(fm.Chunks, common.ChunkHandle(h))
		}
		s.Files[f.Path] = fm
	}
	for _, c := range cp.Chunks {
		s.Chunks[common.ChunkHandle(c.Handle)] = &ChunkMeta{Version: c.Version, Refcount: c.Refcount}
	}
	return s, s.CheckInvariants()
}

// CheckInvariants recomputes reference counts from the file table and
// compares them with the stored ones; run after recovery.
func (s *State) CheckInvariants() error {
	refs := map[common.ChunkHandle]int32{}
	for p, f := range s.Files {
		for _, h := range f.Chunks {
			if _, ok := s.Chunks[h]; !ok {
				return fmt.Errorf("file %s references unknown chunk %s", p, h)
			}
			refs[h]++
			if uint64(h) >= s.NextHandle {
				return fmt.Errorf("chunk %s >= next handle %d", h, s.NextHandle)
			}
		}
	}
	for h, c := range s.Chunks {
		if refs[h] != c.Refcount {
			return fmt.Errorf("chunk %s refcount %d, but %d references", h, c.Refcount, refs[h])
		}
	}
	return nil
}

// Children lists direct children of dir. The trash directory is hidden
// from the root listing but can be listed itself.
func (s *State) Children(dir string) []*pb.ListEntry {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	var out []*pb.ListEntry
	for d := range s.Dirs {
		if d != "/" && d != TrashDir && common.Parent(d) == dir {
			out = append(out, &pb.ListEntry{Name: strings.TrimPrefix(d, prefix), IsDir: true})
		}
	}
	for p, f := range s.Files {
		if common.Parent(p) == dir {
			out = append(out, &pb.ListEntry{Name: strings.TrimPrefix(p, prefix), NumChunks: int64(len(f.Chunks)),
				DeletedFrom: f.DeletedFrom, DeletedUnixNanos: f.Deleted})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
