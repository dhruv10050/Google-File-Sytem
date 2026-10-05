package master

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

const rpcTimeout = 10 * time.Second

func (m *Master) csClient(addr string) (pb.ChunkServerClient, error) {
	conn, err := m.pool.Get(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewChunkServerClient(conn), nil
}

// GetChunkLocations answers the read path (§2.4): handle, version and live
// replicas for the requested chunk and a few that follow it.
func (m *Master) GetChunkLocations(ctx context.Context, req *pb.GetChunkLocationsRequest) (*pb.GetChunkLocationsResponse, error) {
	p, err := cleanPath(req.Path)
	if err != nil {
		return nil, err
	}
	count := int64(max(req.Count, 1))
	m.mu.RLock()
	f, ok := m.st.Files[p]
	if !ok {
		m.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "%s: %v", p, ErrNotFound)
	}
	resp := &pb.GetChunkLocationsResponse{NumChunks: int64(len(f.Chunks))}
	for i := req.StartIndex; i < int64(len(f.Chunks)) && i < req.StartIndex+count; i++ {
		h := f.Chunks[i]
		resp.Chunks = append(resp.Chunks, &pb.ChunkLocation{
			Index: i, Handle: uint64(h), Version: m.st.Chunks[h].Version,
			Replicas: m.addrsLocked(m.liveLocsLocked(h)),
		})
	}
	lsn := m.st.LSN
	m.mu.RUnlock()
	return resp, m.readDone(lsn)
}

// GetLeaseHolder answers the write path (§3.1 steps 1-2): which replica
// holds the lease, granting one if nobody does. With allocate set it first
// creates any missing chunks up to the requested index.
func (m *Master) GetLeaseHolder(ctx context.Context, req *pb.GetLeaseHolderRequest) (*pb.GetLeaseHolderResponse, error) {
	if m.cfg.Shadow {
		return nil, status.Error(codes.FailedPrecondition, "shadow master is read-only")
	}
	p, err := cleanPath(req.Path)
	if err != nil {
		return nil, err
	}
	if req.Index < 0 {
		return nil, status.Error(codes.InvalidArgument, "negative chunk index")
	}
	m.mu.RLock()
	f, ok := m.st.Files[p]
	n := 0
	if ok {
		n = len(f.Chunks)
	}
	m.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%s: %v", p, ErrNotFound)
	}
	if req.Index >= int64(n) {
		if !req.Allocate {
			return nil, status.Errorf(codes.OutOfRange, "chunk index %d beyond end of file (%d chunks)", req.Index, n)
		}
		if err := m.allocateChunks(p, req.Index); err != nil {
			return nil, err
		}
	}

	m.mu.RLock()
	f, ok = m.st.Files[p]
	if !ok || req.Index >= int64(len(f.Chunks)) {
		m.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "%s: deleted concurrently", p)
	}
	h := f.Chunks[req.Index]
	cs := m.soft[h]
	m.mu.RUnlock()

	cs.mu.Lock()
	defer cs.mu.Unlock()

	m.mu.RLock()
	meta := m.st.Chunks[h]
	if meta == nil {
		m.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "chunk deleted concurrently")
	}
	primary, expiry := cs.primary, cs.leaseExpiry
	live := m.liveLocsLocked(h)
	valid := primary != "" && time.Now().Before(expiry) && m.leaseReplicasIntactLocked(h, cs)
	var resp *pb.GetLeaseHolderResponse
	if valid {
		resp = m.leaseResponseLocked(h, req.Index, meta.Version, primary, live, expiry)
	}
	m.mu.RUnlock()
	if resp != nil {
		return resp, nil
	}
	return m.grantLease(h, req.Index, p)
}

// leaseReplicasIntactLocked reports whether every replica that was live
// when the lease was granted still is. If one was lost, mutations under this
// lease may have succeeded without it, yet it would still report the
// current version on return. Voiding the lease forces the next grant to bump
// the version, which marks such a replica stale (§4.5).
func (m *Master) leaseReplicasIntactLocked(h common.ChunkHandle, cs *chunkSoft) bool {
	for _, id := range cs.leaseLocs {
		if m.servers[id] == nil || !cs.locs[id] {
			return false
		}
	}
	return len(cs.leaseLocs) > 0
}

func (m *Master) leaseResponseLocked(h common.ChunkHandle, idx int64, version uint64, primary string, live []string, expiry time.Time) *pb.GetLeaseHolderResponse {
	var secs []string
	for _, id := range live {
		if id != primary {
			secs = append(secs, id)
		}
	}
	return &pb.GetLeaseHolderResponse{
		Chunk: &pb.ChunkLocation{
			Index: idx, Handle: uint64(h), Version: version, Replicas: m.addrsLocked(live),
		},
		Primary:              m.addrsLocked([]string{primary})[0],
		Secondaries:          m.addrsLocked(secs),
		LeaseExpiryUnixNanos: expiry.UnixNano(),
	}
}

// grantLease picks a primary among the up-to-date replicas (cs.mu held).
// It first bumps the chunk version on every live replica and logs it
// (§4.5), which also fences any previous primary: a replica that sees a new
// version drops its lease, and secondaries reject mutations carrying an old
// version. This lets the master grant a new lease immediately when the old
// primary has died, instead of waiting for the old lease to expire.
func (m *Master) grantLease(h common.ChunkHandle, idx int64, path string) (*pb.GetLeaseHolderResponse, error) {
	m.mu.RLock()
	goal := int32(0)
	if f := m.st.Files[path]; f != nil {
		goal = f.Replication
	}
	nLive := len(m.liveLocsLocked(h))
	nServers := len(m.servers)
	m.mu.RUnlock()
	// Right after a restart the master has not heard from every chunkserver
	// yet; bumping versions now would needlessly strand replicas as stale.
	if time.Since(m.startedAt) < m.cfg.RereplicationDelay && nLive < min(int(goal), nServers) {
		return nil, status.Error(codes.Unavailable, "master recovering: waiting for chunkserver reports")
	}

	ok, version, err := m.bumpVersion(h)
	if err != nil {
		return nil, err
	}
	rand.Shuffle(len(ok), func(i, j int) { ok[i], ok[j] = ok[j], ok[i] })
	for _, id := range ok {
		m.mu.RLock()
		s := m.servers[id]
		m.mu.RUnlock()
		if s == nil {
			continue
		}
		cli, err := m.csClient(s.addr)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(m.ctx, rpcTimeout)
		_, err = cli.GrantLease(ctx, &pb.GrantLeaseRequest{Handle: uint64(h), Version: version, DurationMs: m.cfg.LeaseDuration.Milliseconds()})
		cancel()
		if err != nil {
			m.log.Warn("grant lease failed", "chunk", h, "server", short(id), "err", err)
			continue
		}
		m.mu.Lock()
		cs := m.soft[h]
		if cs == nil {
			m.mu.Unlock()
			return nil, status.Error(codes.NotFound, "chunk deleted concurrently")
		}
		// Our clock starts after the RPC returned, so the master's expiry is
		// always later than the primary's own.
		live := m.liveLocsLocked(h)
		cs.primary, cs.leaseExpiry, cs.leaseLocs = id, time.Now().Add(m.cfg.LeaseDuration), live
		resp := m.leaseResponseLocked(h, idx, version, id, live, cs.leaseExpiry)
		m.mu.Unlock()
		return resp, nil
	}
	return nil, status.Errorf(codes.Unavailable, "no replica of chunk %s accepted the lease", h)
}

// bumpVersion increments the chunk version on every live replica, then logs
// it (cs.mu held). Replicas are updated before the log: if the master
// crashes in between, it recovers the old version, sees replicas reporting
// the newer one, and adopts it (§4.5). Logging first could instead make
// every replica look stale. Replicas that fail the bump are dropped from
// the location set; they will be reported stale and garbage-collected.
func (m *Master) bumpVersion(h common.ChunkHandle) (ok []string, newV uint64, err error) {
	m.mu.Lock()
	meta, cs := m.st.Chunks[h], m.soft[h]
	if meta == nil || cs == nil {
		m.mu.Unlock()
		return nil, 0, status.Error(codes.NotFound, "chunk deleted concurrently")
	}
	oldV := meta.Version
	live := m.liveLocsLocked(h)
	addrs := m.addrsLocked(live)
	cs.primary, cs.leaseExpiry = "", time.Time{}
	m.mu.Unlock()
	if len(live) == 0 {
		return nil, 0, status.Errorf(codes.Unavailable, "chunk %s has no live replicas", h)
	}

	newV = oldV + 1
	errs := make([]error, len(live))
	var wg sync.WaitGroup
	for i := range live {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli, err := m.csClient(addrs[i])
			if err == nil {
				ctx, cancel := context.WithTimeout(m.ctx, rpcTimeout)
				_, err = cli.SetVersion(ctx, &pb.SetVersionRequest{Handle: uint64(h), OldVersion: oldV, NewVersion: newV})
				cancel()
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	m.mu.Lock()
	for i, id := range live {
		if errs[i] == nil {
			ok = append(ok, id)
			continue
		}
		m.log.Warn("version bump failed; dropping replica", "chunk", h, "server", short(id), "err", errs[i])
		delete(cs.locs, id)
		if s := m.servers[id]; s != nil {
			delete(s.chunks, h)
		}
	}
	m.mu.Unlock()
	if len(ok) == 0 {
		return nil, 0, status.Errorf(codes.Unavailable, "no replica of chunk %s accepted version %d", h, newV)
	}
	if err := m.commit(&pb.LogRecord{Op: &pb.LogRecord_SetVersion{SetVersion: &pb.SetVersionOp{Handle: uint64(h), Version: newV}}}); err != nil {
		return nil, 0, err
	}
	return ok, newV, nil
}

// allocateChunks creates every missing chunk of path up to index upTo.
// The chunk is created on chunkservers before AddChunk is logged, so a crash
// leaves at worst an orphan replica (reclaimed by GC), never metadata that
// points at a chunk that does not exist.
func (m *Master) allocateChunks(path string, upTo int64) error {
	m.mu.Lock()
	am := m.fileAlloc[path]
	if am == nil {
		am = &sync.Mutex{}
		m.fileAlloc[path] = am
	}
	m.mu.Unlock()
	am.Lock()
	defer am.Unlock()

	for {
		m.mu.Lock()
		f, ok := m.st.Files[path]
		if !ok {
			m.mu.Unlock()
			return status.Errorf(codes.NotFound, "%s: %v", path, ErrNotFound)
		}
		idx := int64(len(f.Chunks))
		if idx > upTo {
			m.mu.Unlock()
			return nil
		}
		h := common.ChunkHandle(m.nextHandle)
		m.nextHandle++
		m.pending[h] = true
		targets := m.placeLocked(int(f.Replication), nil)
		addrs := m.addrsLocked(targets)
		for _, id := range targets {
			m.servers[id].creates = append(m.servers[id].creates, time.Now())
		}
		m.mu.Unlock()

		created := m.createOn(h, targets, addrs)
		if len(created) == 0 {
			m.mu.Lock()
			delete(m.pending, h)
			m.mu.Unlock()
			return status.Errorf(codes.Unavailable, "could not create chunk on any chunkserver (%d candidates)", len(targets))
		}
		err := m.commit(&pb.LogRecord{Op: &pb.LogRecord_AddChunk{AddChunk: &pb.AddChunkOp{Path: path, Index: idx, Handle: uint64(h), Version: 1}}})
		m.mu.Lock()
		delete(m.pending, h)
		if err == nil {
			cs := m.soft[h]
			for _, id := range created {
				if s := m.servers[id]; s != nil {
					cs.locs[id] = true
					s.chunks[h] = true
				}
			}
		}
		m.mu.Unlock()
		if err != nil {
			return err
		}
	}
}

func (m *Master) createOn(h common.ChunkHandle, ids, addrs []string) []string {
	errs := make([]error, len(ids))
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cli, err := m.csClient(addrs[i])
			if err == nil {
				ctx, cancel := context.WithTimeout(m.ctx, rpcTimeout)
				_, err = cli.CreateChunk(ctx, &pb.CreateChunkRequest{Handle: uint64(h), Version: 1})
				cancel()
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	var ok []string
	for i, id := range ids {
		if errs[i] == nil {
			ok = append(ok, id)
		} else {
			m.log.Warn("create chunk failed", "chunk", h, "server", short(id), "err", errs[i])
		}
	}
	return ok
}
