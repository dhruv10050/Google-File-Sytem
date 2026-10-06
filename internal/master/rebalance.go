package master

import (
	"context"
	"math/rand/v2"
	"time"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

// Rebalancing (§4.3). Placement of new chunks already favours emptier
// servers (§4.2); this scan also evens out existing replicas. It runs only
// when no chunk is under-replicated, since restoring redundancy matters
// more, and it moves at most MaxRebalanceMoves replicas at a time, so a
// newly added chunkserver fills gradually instead of being swamped.
//
// Load is the number of replicas a server holds, counting moves in flight.
// The paper balances disk space utilization; with mostly full 64 MB chunks
// the two agree, and counts change exactly when the master moves a replica
// rather than one heartbeat later.

// dropQuarantine bounds how long a heartbeat built before a deletion can
// take to arrive: the chunkserver's heartbeat RPC times out after 5 s.
const dropQuarantine = 10 * time.Second

// droppedLocked reports whether h was removed from s recently, so a report
// of it must be treated as garbage rather than as a live replica.
func (m *Master) droppedLocked(s *serverInfo, h common.ChunkHandle) bool {
	t, ok := s.drop[h]
	if !ok {
		return false
	}
	if !t.IsZero() && time.Since(t) > dropQuarantine {
		delete(s.drop, h)
		return false
	}
	return true
}

// dropReplicaLocked removes s's replica of h (m.mu and cs.mu held) and tells
// s to delete it. If that fails, s learns from the garbage list in the
// next heartbeat that reports the chunk.
func (m *Master) dropReplicaLocked(h common.ChunkHandle, cs *chunkSoft, s *serverInfo) {
	delete(cs.locs, s.id)
	delete(s.chunks, h)
	s.drop[h] = time.Time{}
	go func() {
		if cli, err := m.csClient(s.addr); err == nil {
			ctx, cancel := context.WithTimeout(m.ctx, rpcTimeout)
			cli.DeleteChunks(ctx, &pb.DeleteChunksRequest{Handles: []uint64{uint64(h)}})
			cancel()
		}
		m.mu.Lock()
		if _, ok := s.drop[h]; ok {
			s.drop[h] = time.Now()
		}
		m.mu.Unlock()
	}()
}

func (m *Master) loadLocked(s *serverInfo) int { return len(s.chunks) + s.movesIn - s.movesOut }

func (m *Master) rebalanceScan() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.servers) < 2 {
		return
	}
	goals := m.replicationGoalsLocked()

	// Excess replicas, e.g. from a chunkserver that came back after its
	// chunks were re-replicated: remove the copy on the most-loaded server.
	for h, goal := range goals {
		cs := m.soft[h]
		if cs == nil || cs.cloning {
			continue
		}
		live := m.liveLocsLocked(h)
		if len(live) <= int(goal) {
			continue
		}
		victim := m.servers[live[0]]
		for _, id := range live[1:] {
			if s := m.servers[id]; m.loadLocked(s) > m.loadLocked(victim) {
				victim = s
			}
		}
		cs.cloning = true
		go m.trim(h, cs, victim.id)
	}

	limit := m.cfg.MaxClonesPerServer
	for m.movesBusy.Load() < int64(m.cfg.MaxRebalanceMoves) {
		var src, dst *serverInfo
		total := 0
		for _, s := range m.servers {
			total += m.loadLocked(s)
			if src == nil || m.loadLocked(s) > m.loadLocked(src) {
				src = s
			}
			if dst == nil || m.loadLocked(s) < m.loadLocked(dst) {
				dst = s
			}
		}
		avg := float64(total) / float64(len(m.servers))
		if gap := m.loadLocked(src) - m.loadLocked(dst); float64(gap) <= max(2, 0.1*avg) {
			return
		}
		if src.clonesBusy >= limit || dst.clonesBusy >= limit {
			return
		}
		h, ok := m.movableLocked(src, dst, goals)
		if !ok {
			return
		}
		m.soft[h].cloning = true
		src.clonesBusy++
		dst.clonesBusy++
		src.movesOut++
		dst.movesIn++
		m.clonesBusy.Add(1)
		m.movesBusy.Add(1)
		go m.clone(h, src.id, dst.id, true)
	}
}

// movableLocked picks a chunk on src to move to dst: fully replicated, not
// on dst, not busy, and preferably without a lease (an idle chunk, so no
// writer is disturbed by the revocation).
func (m *Master) movableLocked(src, dst *serverInfo, goals map[common.ChunkHandle]int32) (common.ChunkHandle, bool) {
	var leased []common.ChunkHandle
	now := time.Now()
	for h := range src.chunks { // random order
		cs := m.soft[h]
		if cs == nil || cs.cloning || cs.locs[dst.id] || cs.corruptOn[dst.id] || m.droppedLocked(dst, h) ||
			len(m.liveLocsLocked(h)) != int(goals[h]) {
			continue
		}
		if cs.primary != "" && now.Before(cs.leaseExpiry) {
			leased = append(leased, h)
			continue
		}
		return h, true
	}
	if len(leased) > 0 {
		return leased[rand.IntN(len(leased))], true
	}
	return 0, false
}

// trim removes the replica of h on id if the chunk still has more than its
// goal. It revokes any lease first so no mutation is in flight.
func (m *Master) trim(h common.ChunkHandle, cs *chunkSoft, id string) {
	defer func() {
		m.mu.Lock()
		cs.cloning = false
		m.mu.Unlock()
	}()
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if err := m.revokeLease(h, cs); err != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	goal := m.replicationGoalsLocked()[h]
	if s := m.servers[id]; s != nil && cs.locs[id] && len(m.liveLocsLocked(h)) > int(goal) {
		m.dropReplicaLocked(h, cs, s)
		m.log.Debug("trimmed excess replica", "chunk", h, "server", short(id))
	}
}
