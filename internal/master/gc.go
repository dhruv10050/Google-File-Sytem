package master

import (
	"time"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
)

// Garbage collection (§4.4). Deleting a file only renames it into TrashDir.
// gcLoop removes hidden files once they are older than TrashRetention; their
// chunks then lose a reference, and chunks with none left are orphans. The
// master forgets orphans at once, and chunkservers delete their replicas
// when a heartbeat reports a chunk the master no longer knows (see
// processReportsLocked). Replicas left behind by failed creations or
// stale versions are reclaimed the same way.
func (m *Master) gcLoop() {
	t := time.NewTicker(m.cfg.GCInterval)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}
		m.collectTrash(time.Now())
	}
}

// collectTrash removes hidden files deleted before now - TrashRetention and
// returns how many it removed.
func (m *Master) collectTrash(now time.Time) int {
	cutoff := now.Add(-m.cfg.TrashRetention).UnixNano()
	m.mu.RLock()
	var expired []string
	for p, f := range m.st.Files {
		if f.Deleted != 0 && f.Deleted <= cutoff && InTrash(p) {
			expired = append(expired, p)
		}
	}
	m.mu.RUnlock()
	n := 0
	for _, p := range expired {
		// Fails harmlessly if the file was undeleted or removed meanwhile.
		if err := m.commit(&pb.LogRecord{Op: &pb.LogRecord_DeleteFile{DeleteFile: &pb.DeleteFileOp{Path: p}}}); err == nil {
			n++
		}
	}
	if n > 0 {
		m.log.Info("garbage collected deleted files", "files", n)
	}
	return n
}
