package master

import (
	"slices"
	"strings"
	"sync"

	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

// Namespace locking (§4.1). Each operation read-locks every ancestor of
// the paths it touches and read- or write-locks the paths themselves, so
// operations on different parts of the namespace run concurrently while
// conflicting ones are serialized. Examples:
//
//   - create /d/f: read /, /d; write /d/f. Concurrent creates in /d proceed
//     together; they serialize only on the same name.
//   - snapshot /d -> /s: read / ; write /d, /s. A create in /d needs a read
//     lock on /d, so it cannot run while /d is being snapshotted.
//   - chunk allocation or copy-on-write for /d/f: read /, /d, /d/f, so a
//     snapshot or deletion of /d/f waits until it finishes.
//
// Locks are created on demand and freed when unused, so the table stays
// proportional to the operations in flight, not the namespace size. They
// are acquired in one global order (by depth, then lexicographically),
// which rules out deadlock among namespace operations. They are always
// taken before any chunk lock or Master.mu.
type nsLocks struct {
	mu    sync.Mutex
	table map[string]*nsLock
}

type nsLock struct {
	sync.RWMutex
	refs int
}

type nsHeld struct {
	path  string
	write bool
	l     *nsLock
}

// lockSet locks every path in reads and writes, plus all their ancestors
// (read). A path in both sets is write-locked. It returns the unlock func.
func (n *nsLocks) lockSet(reads, writes []string) func() {
	mode := map[string]bool{} // path -> write
	for _, p := range append(slices.Clone(reads), writes...) {
		for _, a := range append(common.Ancestors(p), p) {
			if _, ok := mode[a]; !ok {
				mode[a] = false
			}
		}
	}
	for _, p := range writes {
		mode[p] = true
	}
	order := make([]string, 0, len(mode))
	for p := range mode {
		order = append(order, p)
	}
	slices.SortFunc(order, func(a, b string) int {
		if da, db := depth(a), depth(b); da != db {
			return da - db
		}
		return strings.Compare(a, b)
	})

	held := make([]nsHeld, len(order))
	n.mu.Lock()
	if n.table == nil {
		n.table = map[string]*nsLock{}
	}
	for i, p := range order {
		l := n.table[p]
		if l == nil {
			l = &nsLock{}
			n.table[p] = l
		}
		l.refs++
		held[i] = nsHeld{p, mode[p], l}
	}
	n.mu.Unlock()

	for _, h := range held {
		if h.write {
			h.l.Lock()
		} else {
			h.l.RLock()
		}
	}
	return func() {
		for i := len(held) - 1; i >= 0; i-- {
			if h := held[i]; h.write {
				h.l.Unlock()
			} else {
				h.l.RUnlock()
			}
		}
		n.mu.Lock()
		for _, h := range held {
			if h.l.refs--; h.l.refs == 0 {
				delete(n.table, h.path)
			}
		}
		n.mu.Unlock()
	}
}

func (n *nsLocks) size() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.table)
}

func depth(p string) int {
	if p == "/" {
		return 0
	}
	return strings.Count(p, "/")
}
