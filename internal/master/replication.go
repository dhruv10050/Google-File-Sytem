package master

import (
	"context"
	"math/rand/v2"
	"sort"
	"time"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

const (
	recentCreateWindow = time.Minute
	cloneTimeout       = 5 * time.Minute
)

// placeLocked chooses up to n live servers for new replicas (§4.2, §4.3):
// prefer below-average disk utilization, few recent creations (creation
// predicts imminent write traffic), and few active clones. Replicas always
// land on distinct servers. Random shuffling breaks ties.
func (m *Master) placeLocked(n int, exclude map[string]bool) []string {
	now := time.Now()
	type cand struct {
		id    string
		score float64
	}
	var cands []cand
	for id, s := range m.servers {
		if exclude[id] {
			continue
		}
		keep := s.creates[:0]
		for _, t := range s.creates {
			if now.Sub(t) < recentCreateWindow {
				keep = append(keep, t)
			}
		}
		s.creates = keep
		util := 0.0
		if total := s.diskUsed + s.diskFree; total > 0 {
			util = float64(s.diskUsed) / float64(total)
		}
		cands = append(cands, cand{id, util + 0.02*float64(len(s.creates)) + 0.01*float64(s.clonesBusy)})
	}
	rand.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].score < cands[j].score })
	out := make([]string, 0, n)
	for _, c := range cands[:min(n, len(cands))] {
		out = append(out, c.id)
	}
	return out
}

// replicationLoop periodically finds chunks below their replication goal
// and clones them, most-endangered first (§4.3).
func (m *Master) replicationLoop() {
	t := time.NewTicker(m.cfg.ReplicationScanInterval)
	defer t.Stop()
	var lastRebalance time.Time
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}
		if time.Since(m.startedAt) < m.cfg.RereplicationDelay {
			continue // chunkservers are still reporting in after a restart
		}
		if m.replicationScan() == 0 && m.cfg.RebalanceInterval > 0 && time.Since(lastRebalance) >= m.cfg.RebalanceInterval {
			lastRebalance = time.Now()
			m.rebalanceScan()
		}
	}
}

type cloneTask struct {
	h       common.ChunkHandle
	missing int
	live    []string
}

// replicationScan starts clones for under-replicated chunks and returns
// how many chunks are below their goal.
func (m *Master) replicationScan() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	var tasks []cloneTask
	for h, goal := range m.replicationGoalsLocked() {
		cs := m.soft[h]
		if cs == nil || cs.cloning {
			continue
		}
		live := m.liveLocsLocked(h)
		if len(live) == 0 {
			continue // lost unless a replica comes back; nothing to copy from
		}
		if miss := int(goal) - len(live); miss > 0 {
			tasks = append(tasks, cloneTask{h, miss, live})
		}
	}
	// Priority: chunks missing more replicas first (§4.3).
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].missing != tasks[j].missing {
			return tasks[i].missing > tasks[j].missing
		}
		return tasks[i].h < tasks[j].h
	})
	limit := m.cfg.MaxClonesPerServer
	for _, t := range tasks {
		if m.cfg.MaxClonesCluster > 0 && m.clonesBusy.Load() >= int64(m.cfg.MaxClonesCluster) {
			break
		}
		var src *serverInfo
		for _, id := range t.live {
			if s := m.servers[id]; s.clonesBusy < limit && (src == nil || s.clonesBusy < src.clonesBusy) {
				src = s
			}
		}
		if src == nil {
			continue
		}
		cs := m.soft[t.h]
		exclude := map[string]bool{}
		for id := range cs.locs {
			exclude[id] = true
		}
		for id := range cs.corruptOn {
			exclude[id] = true
		}
		for id, s := range m.servers {
			if s.clonesBusy >= limit || m.droppedLocked(s, t.h) {
				exclude[id] = true
			}
		}
		dst := m.placeLocked(1, exclude)
		if len(dst) == 0 {
			continue
		}
		d := m.servers[dst[0]]
		cs.cloning = true
		src.clonesBusy++
		d.clonesBusy++
		m.clonesBusy.Add(1)
		go m.clone(t.h, src.id, d.id, false)
	}
	return len(tasks)
}

// clone copies chunk h from src to dst; with move set it then removes the
// replica on src (rebalancing). It holds the chunk's lock for the
// whole copy so no new lease can be granted, and first revokes any current
// lease, so the copy can never miss a mutation. If the primary cannot be
// reached to revoke, the version is bumped instead, which fences it.
func (m *Master) clone(h common.ChunkHandle, srcID, dstID string, move bool) {
	defer func() {
		m.mu.Lock()
		if move {
			if s := m.servers[srcID]; s != nil {
				s.movesOut--
			}
			if s := m.servers[dstID]; s != nil {
				s.movesIn--
			}
			m.movesBusy.Add(-1)
		}
		if cs := m.soft[h]; cs != nil {
			cs.cloning = false
		}
		for _, id := range []string{srcID, dstID} {
			if s := m.servers[id]; s != nil && s.clonesBusy > 0 {
				s.clonesBusy--
			}
		}
		m.mu.Unlock()
		m.clonesBusy.Add(-1)
	}()

	m.mu.RLock()
	cs := m.soft[h]
	m.mu.RUnlock()
	if cs == nil {
		return
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if err := m.revokeLease(h, cs); err != nil {
		m.log.Warn("clone: could not fence chunk", "chunk", h, "err", err)
		return
	}

	m.mu.RLock()
	meta := m.st.Chunks[h]
	src, dst := m.servers[srcID], m.servers[dstID]
	srcOK := src != nil && cs.locs[srcID]
	if !srcOK {
		// The chosen source may have been fenced out; use any live replica.
		for _, id := range m.liveLocsLocked(h) {
			src, srcOK = m.servers[id], true
			break
		}
	}
	m.mu.RUnlock()
	if meta == nil || dst == nil || !srcOK {
		return
	}
	version := meta.Version

	start := time.Now()
	cli, err := m.csClient(dst.addr)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(m.ctx, cloneTimeout)
	rep, err := cli.CloneChunk(ctx, &pb.CloneChunkRequest{Handle: uint64(h), Version: version, Source: src.addr})
	cancel()
	if err != nil {
		m.log.Warn("clone failed", "chunk", h, "src", short(src.id), "dst", short(dstID), "err", err)
		return
	}
	m.mu.Lock()
	added := false
	if meta := m.st.Chunks[h]; meta != nil && meta.Version == version && m.servers[dstID] == dst {
		cs.locs[dstID] = true
		delete(cs.corruptOn, dstID)
		dst.chunks[h] = true
		added = true
	}
	if move && added {
		// Still under cs.mu: no lease can include the replica being removed.
		if s := m.servers[srcID]; s != nil && cs.locs[srcID] {
			m.dropReplicaLocked(h, cs, s)
			m.movesDone.Add(1)
		}
	}
	m.mu.Unlock()
	m.clonesDone.Add(1)
	m.log.Debug("chunk re-replicated", "chunk", h, "src", short(src.id), "dst", short(dstID), "bytes", rep.Length, "took", time.Since(start))
}
