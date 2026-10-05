package master

import (
	"context"
	"time"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

// Chunk locations are never persisted: the master learns them from
// chunkserver reports at registration and in heartbeats (§2.6.2).

func (m *Master) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	m.mu.Lock()
	if old := m.servers[req.ServerId]; old != nil {
		m.removeServerLocked(old) // restarted: forget everything it told us before
	}
	s := &serverInfo{
		id: req.ServerId, addr: req.Address, lastHB: time.Now(),
		diskUsed: req.DiskUsed, diskFree: req.DiskFree,
		chunks: map[common.ChunkHandle]bool{},
	}
	m.servers[s.id] = s
	garbage, adopt := m.processReportsLocked(s, req.Chunks)
	m.mu.Unlock()

	m.adoptVersions(s, adopt)
	m.log.Info("chunkserver registered", "id", short(s.id), "addr", s.addr, "chunks", len(req.Chunks), "garbage", len(garbage))
	return &pb.RegisterResponse{
		ChunkSize:           m.cfg.ChunkSize,
		HeartbeatIntervalMs: m.cfg.HeartbeatInterval.Milliseconds(),
		Garbage:             garbage,
	}, nil
}

func (m *Master) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	now := time.Now()
	m.mu.Lock()
	s := m.servers[req.ServerId]
	if s == nil || s.addr != req.Address {
		m.mu.Unlock()
		return &pb.HeartbeatResponse{Reregister: true}, nil
	}
	s.lastHB, s.diskUsed, s.diskFree = now, req.DiskUsed, req.DiskFree
	garbage, adopt := m.processReportsLocked(s, req.Chunks)

	// Corrupted replicas (§5.2): stop using them at once; delete them only
	// once enough healthy replicas exist again.
	goals := map[common.ChunkHandle]int32{}
	if len(req.Corrupted) > 0 {
		goals = m.replicationGoalsLocked()
	}
	for _, raw := range req.Corrupted {
		h := common.ChunkHandle(raw)
		cs := m.soft[h]
		if cs == nil {
			garbage = append(garbage, raw)
			continue
		}
		if cs.locs[s.id] {
			m.log.Warn("replica reported corrupted", "chunk", h, "server", short(s.id))
		}
		delete(cs.locs, s.id)
		delete(s.chunks, h)
		cs.corruptOn[s.id] = true
		if len(m.liveLocsLocked(h)) >= int(goals[h]) {
			garbage = append(garbage, raw)
			delete(cs.corruptOn, s.id)
		}
	}

	// Lease extensions are piggybacked on heartbeats (§3.1).
	var ext []*pb.LeaseExtension
	for _, raw := range req.LeaseExtensions {
		cs := m.soft[common.ChunkHandle(raw)]
		h := common.ChunkHandle(raw)
		if cs != nil && cs.primary == s.id && now.Before(cs.leaseExpiry) && m.leaseReplicasIntactLocked(h, cs) {
			cs.leaseExpiry = now.Add(m.cfg.LeaseDuration)
			ext = append(ext, &pb.LeaseExtension{Handle: raw, DurationMs: m.cfg.LeaseDuration.Milliseconds()})
		}
	}
	m.mu.Unlock()

	m.adoptVersions(s, adopt)
	if m.cfg.Shadow {
		return &pb.HeartbeatResponse{}, nil
	}
	return &pb.HeartbeatResponse{Garbage: garbage, Extended: ext}, nil
}

// processReportsLocked reconciles a chunkserver's report with metadata:
// unknown chunks are orphans, older versions are stale (§4.5), and newer
// versions mean the master crashed mid-lease-grant and must adopt them.
func (m *Master) processReportsLocked(s *serverInfo, reps []*pb.ChunkReport) (garbage []uint64, adopt []*pb.ChunkReport) {
	for _, r := range reps {
		h := common.ChunkHandle(r.Handle)
		meta := m.st.Chunks[h]
		cs := m.soft[h]
		switch {
		case meta == nil || cs == nil:
			if !m.pending[h] && !m.cfg.Shadow {
				garbage = append(garbage, r.Handle)
			}
		case r.Version < meta.Version:
			if m.cfg.Shadow {
				continue // the shadow may simply lag the primary's version bump
			}
			delete(cs.locs, s.id)
			delete(s.chunks, h)
			garbage = append(garbage, r.Handle)
		case r.Version > meta.Version && !m.cfg.Shadow:
			adopt = append(adopt, r)
		case cs.corruptOn[s.id]:
			// known-bad copy; ignore until it is deleted or replaced
		default:
			cs.locs[s.id] = true
			s.chunks[h] = true
		}
	}
	return garbage, adopt
}

// adoptVersions handles replicas ahead of the master: the master assumes it
// failed while granting a lease and takes the higher version (§4.5).
func (m *Master) adoptVersions(s *serverInfo, reps []*pb.ChunkReport) {
	for _, r := range reps {
		h := common.ChunkHandle(r.Handle)
		m.mu.RLock()
		cs := m.soft[h]
		m.mu.RUnlock()
		if cs == nil {
			continue
		}
		cs.mu.Lock()
		m.mu.RLock()
		meta := m.st.Chunks[h]
		need := meta != nil && r.Version > meta.Version
		m.mu.RUnlock()
		if need {
			m.log.Warn("adopting higher chunk version from replica", "chunk", h, "version", r.Version, "server", short(s.id))
			m.mu.Lock()
			// Replicas we counted at the old version are now stale.
			for id := range cs.locs {
				delete(cs.locs, id)
				if srv := m.servers[id]; srv != nil {
					delete(srv.chunks, h)
				}
			}
			m.mu.Unlock()
			if err := m.commit(&pb.LogRecord{Op: &pb.LogRecord_SetVersion{SetVersion: &pb.SetVersionOp{Handle: r.Handle, Version: r.Version}}}); err != nil {
				m.log.Error("adopt version", "chunk", h, "err", err)
				cs.mu.Unlock()
				continue
			}
		}
		m.mu.Lock()
		if meta := m.st.Chunks[h]; meta != nil && meta.Version == r.Version && m.servers[s.id] == s {
			cs.locs[s.id] = true
			s.chunks[h] = true
		}
		m.mu.Unlock()
		cs.mu.Unlock()
	}
}

func (m *Master) removeServerLocked(s *serverInfo) {
	for h := range s.chunks {
		if cs := m.soft[h]; cs != nil {
			delete(cs.locs, s.id)
		}
	}
	delete(m.servers, s.id)
}

// livenessLoop declares chunkservers dead after DeadAfter without a
// heartbeat; their replicas stop counting immediately, which is what
// triggers re-replication (§4.3).
func (m *Master) livenessLoop() {
	t := time.NewTicker(m.cfg.HeartbeatInterval / 2)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		m.mu.Lock()
		for _, s := range m.servers {
			if now.Sub(s.lastHB) > m.cfg.DeadAfter {
				m.log.Warn("chunkserver declared dead", "id", short(s.id), "addr", s.addr, "chunks", len(s.chunks))
				m.removeServerLocked(s)
			}
		}
		m.mu.Unlock()
	}
}

// liveLocsLocked returns the IDs of live servers holding an up-to-date replica.
func (m *Master) liveLocsLocked(h common.ChunkHandle) []string {
	cs := m.soft[h]
	if cs == nil {
		return nil
	}
	out := make([]string, 0, len(cs.locs))
	for id := range cs.locs {
		if m.servers[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

func (m *Master) addrsLocked(ids []string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if s := m.servers[id]; s != nil {
			out = append(out, s.addr)
		}
	}
	return out
}

// replicationGoalsLocked maps each chunk to the highest replication factor
// among the files referencing it.
func (m *Master) replicationGoalsLocked() map[common.ChunkHandle]int32 {
	g := make(map[common.ChunkHandle]int32, len(m.st.Chunks))
	for _, f := range m.st.Files {
		for _, h := range f.Chunks {
			g[h] = max(g[h], f.Replication)
		}
	}
	return g
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
