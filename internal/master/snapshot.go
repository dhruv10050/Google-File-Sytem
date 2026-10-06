package master

import (
	"context"
	"errors"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

// Snapshots (§3.4) copy a file or directory tree almost instantly: only
// metadata is duplicated, and the copies share chunks with the originals.
// A shared chunk (refcount > 1) is copied the first time someone wants to
// mutate it, by each replica locally, so the copy never crosses the network.

var errSnapshotRaced = errors.New("source chunks changed during snapshot")

// Snapshot copies source to target. It first revokes every outstanding
// lease on the source's chunks, so no client can mutate them without asking
// the master again; the master then sees the raised reference count and
// copies the chunk before granting a new lease.
func (m *Master) Snapshot(ctx context.Context, req *pb.SnapshotRequest) (*pb.Empty, error) {
	if m.cfg.Shadow {
		return nil, status.Error(codes.FailedPrecondition, "shadow master is read-only")
	}
	src, err := cleanPath(req.Source)
	if err != nil {
		return nil, err
	}
	dst, err := cleanPath(req.Target)
	if err != nil {
		return nil, err
	}
	// Write locks on both paths keep creations, deletions, allocations and
	// copy-on-writes under them out until the snapshot is logged (§4.1).
	defer m.ns.lockSet(nil, []string{src, dst})()
	rec := &pb.LogRecord{Op: &pb.LogRecord_Snapshot{Snapshot: &pb.SnapshotOp{Source: src, Target: dst}}}
	for attempt := 0; ; attempt++ {
		err := m.snapshotOnce(src, rec)
		if !errors.Is(err, errSnapshotRaced) {
			if err != nil {
				return nil, err
			}
			m.log.Info("snapshot", "source", src, "target", dst)
			return &pb.Empty{}, nil
		}
		if attempt == 20 {
			return nil, status.Errorf(codes.Aborted, "snapshot of %s: %v", src, err)
		}
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
}

// snapshotOnce holds every source chunk's lock (in handle order, the order
// copy-on-write also uses) from lease revocation until the snapshot is
// logged, so no lease can be granted in between. If the source gained or
// lost chunks meanwhile, it gives up and the caller retries.
func (m *Master) snapshotOnce(src string, rec *pb.LogRecord) error {
	m.mu.RLock()
	if !m.st.exists(src) {
		m.mu.RUnlock()
		return status.Errorf(codes.NotFound, "%s: %v", src, ErrNotFound)
	}
	set := m.st.SnapshotChunks(src)
	hs := make([]common.ChunkHandle, 0, len(set))
	for h := range set {
		hs = append(hs, h)
	}
	slices.Sort(hs)
	softs := make([]*chunkSoft, len(hs))
	for i, h := range hs {
		softs[i] = m.soft[h]
	}
	m.mu.RUnlock()

	for i, cs := range softs {
		if cs == nil {
			return errSnapshotRaced
		}
		cs.mu.Lock()
		defer softs[i].mu.Unlock()
	}
	for i, h := range hs {
		if err := m.revokeLease(h, softs[i]); err != nil {
			return err
		}
	}
	return m.commitIf(rec, func() error {
		now := m.st.SnapshotChunks(src)
		if len(now) != len(set) {
			return errSnapshotRaced
		}
		for h := range now {
			if !set[h] {
				return errSnapshotRaced
			}
		}
		return nil
	})
}

// copyOnWrite gives chunk idx of path its own copy of the shared chunk h
// (cs.mu held for h). Every live replica duplicates h locally under a new
// handle; the replacement is logged only after that, so a crash leaves at
// worst orphan copies, reclaimed by garbage collection.
func (m *Master) copyOnWrite(path string, idx int64, h common.ChunkHandle) error {
	m.mu.Lock()
	meta := m.st.Chunks[h]
	if meta == nil {
		m.mu.Unlock()
		return status.Error(codes.NotFound, "chunk deleted concurrently")
	}
	version := meta.Version
	live := m.liveLocsLocked(h)
	addrs := m.addrsLocked(live)
	nh := common.ChunkHandle(m.nextHandle)
	m.nextHandle++
	m.pending[nh] = true
	m.mu.Unlock()

	errs := make([]error, len(live))
	done := make(chan int, len(live))
	for i := range live {
		go func() {
			cli, err := m.csClient(addrs[i])
			if err == nil {
				// A local copy of a full chunk includes a disk read and an fsync.
				ctx, cancel := context.WithTimeout(m.ctx, 3*rpcTimeout)
				_, err = cli.CopyChunk(ctx, &pb.CopyChunkRequest{Handle: uint64(h), Version: version, NewHandle: uint64(nh), NewVersion: 1})
				cancel()
			}
			errs[i] = err
			done <- i
		}()
	}
	for range live {
		<-done
	}
	var copied []string
	for i, id := range live {
		if errs[i] == nil {
			copied = append(copied, id)
		} else {
			m.log.Warn("copy-on-write failed on replica", "chunk", h, "server", short(id), "err", errs[i])
		}
	}
	if len(copied) == 0 {
		m.mu.Lock()
		delete(m.pending, nh)
		m.mu.Unlock()
		return status.Errorf(codes.Unavailable, "no replica of chunk %s could copy it", h)
	}
	err := m.commit(&pb.LogRecord{Op: &pb.LogRecord_ReplaceChunk{ReplaceChunk: &pb.ReplaceChunkOp{
		Path: path, Index: idx, OldHandle: uint64(h), NewHandle: uint64(nh), Version: 1,
	}}})
	m.mu.Lock()
	delete(m.pending, nh)
	if err == nil {
		cs := m.soft[nh]
		for _, id := range copied {
			if s := m.servers[id]; s != nil {
				cs.locs[id] = true
				s.chunks[nh] = true
			}
		}
	}
	m.mu.Unlock()
	if err == nil {
		m.log.Debug("copy-on-write", "path", path, "index", idx, "from", h, "to", nh, "replicas", len(copied))
	}
	return err
}
