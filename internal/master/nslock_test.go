package master

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blocks reports whether acquiring (reads, writes) waits while held is held.
func blocks(n *nsLocks, reads, writes []string) bool {
	done := make(chan func())
	go func() { done <- n.lockSet(reads, writes) }()
	select {
	case unlock := <-done:
		unlock()
		return false
	case <-time.After(50 * time.Millisecond):
		go func() { (<-done)() }() // release once the holder lets go
		return true
	}
}

func TestNamespaceLockConflicts(t *testing.T) {
	var n nsLocks
	unlock := n.lockSet(nil, []string{"/home/user"}) // snapshot source
	for _, tc := range []struct {
		reads, writes []string
		want          bool
	}{
		{nil, []string{"/home/user/f"}, true}, // create under the snapshotted directory
		{[]string{"/home/user/f"}, nil, true}, // chunk allocation for a file under it
		{nil, []string{"/home/other"}, false}, // create elsewhere in /home
		{[]string{"/home"}, nil, false},       // read lock on the parent
		{nil, []string{"/home"}, true},        // delete or snapshot of the parent
		{nil, []string{"/save/user"}, false},  // unrelated path
		{[]string{"/home/user"}, nil, true},   // reading the locked path itself
	} {
		if got := blocks(&n, tc.reads, tc.writes); got != tc.want {
			t.Errorf("reads %v writes %v: blocked = %v, want %v", tc.reads, tc.writes, got, tc.want)
		}
	}
	unlock()
	time.Sleep(60 * time.Millisecond) // let blocked attempts finish
	if s := n.size(); s != 0 {
		t.Fatalf("%d locks left in the table after all were released", s)
	}
}

// Many goroutines lock overlapping sets in arbitrary argument order; the
// global ordering must prevent deadlock, and writers must be exclusive.
func TestNamespaceLockNoDeadlock(t *testing.T) {
	var n nsLocks
	paths := []string{"/a", "/a/b", "/a/b/c", "/a/d", "/e", "/e/f"}
	var inside [6]atomic.Int32
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				x, y := paths[(g+i)%6], paths[(g*7+i*3)%6]
				unlock := n.lockSet([]string{y}, []string{x})
				xi := (g + i) % 6
				if inside[xi].Add(1) != 1 {
					t.Errorf("two writers inside %s", x)
				}
				inside[xi].Add(-1)
				unlock()
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock")
	}
	if s := n.size(); s != 0 {
		t.Fatalf("%d locks left in the table", s)
	}
}
