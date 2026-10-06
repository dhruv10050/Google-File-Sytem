package testcluster

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dhruv10050/Google-File-Sytem/internal/master"
	"github.com/dhruv10050/Google-File-Sytem/pkg/client"
)

// ───────────────────────── Garbage collection (§4.4) ─────────────────────────

func trashEntries(t *testing.T, cl *client.Client) map[string]string {
	t.Helper()
	ents, err := cl.List(master.TrashDir)
	if err != nil {
		t.Fatalf("list %s: %v", master.TrashDir, err)
	}
	out := map[string]string{} // hidden path -> original path
	for _, e := range ents {
		out[master.TrashDir+"/"+e.Name] = e.DeletedFrom
	}
	return out
}

// replicaFiles lists the chunk files of every replica of path's chunks.
func replicaFiles(c *Cluster, path string) []string {
	var out []string
	for _, l := range c.Locations(path) {
		for _, a := range l.Replicas {
			out = append(out, c.ChunkFile(c.Index(a), l.Handle))
		}
	}
	return out
}

func allGone(files []string) bool {
	for _, f := range files {
		if _, err := os.Stat(f); !errors.Is(err, os.ErrNotExist) {
			return false
		}
	}
	return true
}

// Deleting a file hides it under /.deleted, where it stays readable, keeps
// its storage, survives a master restart, and can be restored (§4.4.1).
func TestDeleteIsLazyAndUndeletable(t *testing.T) {
	c := Start(t, Options{CheckpointEveryRecords: 4})
	cl := c.Client()
	cl.Mkdir("/d", false)
	cl.Create("/d/f", 0)
	data := randBytes(2*MB+300, 40)
	mustWrite(t, cl, "/d/f", 0, data)
	files := replicaFiles(c, "/d/f")

	if err := cl.Delete("/d/f"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Stat("/d/f"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("stat after delete: %v", err)
	}
	if ents, _ := cl.List("/"); len(ents) != 1 || ents[0].Name != "d" {
		t.Fatalf("root lists %v; the trash should be hidden", ents)
	}
	trash := trashEntries(t, cl)
	if len(trash) != 1 {
		t.Fatalf("trash holds %v", trash)
	}
	var hidden string
	for h, orig := range trash {
		hidden = h
		if orig != "/d/f" {
			t.Fatalf("%s deleted from %q", h, orig)
		}
	}
	checkContent(t, cl, hidden, data)
	if allGone(files) || len(files) != 9 {
		t.Fatalf("replicas of a deleted file were removed before GC (%d files)", len(files))
	}
	if err := cl.Write(hidden, 0, []byte("x")); err == nil {
		t.Fatal("write to a deleted file succeeded")
	}
	if err := cl.Create(master.TrashDir+"/mine", 0); err == nil {
		t.Fatal("created a file inside the trash")
	}

	c.StopMaster()
	c.StartMaster()
	c.WaitServers(3)
	cl = c.Client()
	if got := trashEntries(t, cl); len(got) != 1 || got[hidden] != "/d/f" {
		t.Fatalf("after restart, trash holds %v", got)
	}

	// The parent was removed after the delete: undelete needs it back.
	if err := cl.Delete("/d"); err != nil {
		t.Fatalf("delete emptied directory: %v", err)
	}
	if _, err := cl.Undelete(hidden); err == nil {
		t.Fatal("undelete into a missing directory succeeded")
	}
	cl.Mkdir("/d", false)
	orig, err := cl.Undelete(hidden)
	if err != nil || orig != "/d/f" {
		t.Fatalf("undelete: %q, %v", orig, err)
	}
	checkContent(t, cl, "/d/f", data)
	if got := trashEntries(t, cl); len(got) != 0 {
		t.Fatalf("trash after undelete: %v", got)
	}
	c.WaitFor(15*time.Second, "writes accepted after restart", func() bool {
		return cl.Write("/d/f", 0, []byte("restored")) == nil
	})
}

// Hidden files older than the retention period are removed; their chunks
// become orphans and chunkservers delete the replicas after a heartbeat.
// Chunks still shared with a snapshot are kept. Deleting a hidden file
// reclaims it at once.
func TestGarbageCollection(t *testing.T) {
	c := Start(t, Options{TrashRetention: 300 * time.Millisecond, GCInterval: 100 * time.Millisecond})
	cl := c.Client()
	for _, p := range []string{"/a", "/b", "/shared"} {
		cl.Create(p, 0)
		mustWrite(t, cl, p, 0, randBytes(MB+500, 41))
	}
	aFiles, bFiles, sharedFiles := replicaFiles(c, "/a"), replicaFiles(c, "/b"), replicaFiles(c, "/shared")
	if err := cl.Snapshot("/shared", "/snap"); err != nil {
		t.Fatal(err)
	}
	want := readAll(t, cl, "/snap")

	for _, p := range []string{"/a", "/shared"} {
		if err := cl.Delete(p); err != nil {
			t.Fatal(err)
		}
	}
	c.WaitFor(10*time.Second, "trash emptied by GC", func() bool {
		st, err := cl.ClusterStatus()
		return err == nil && st.DeletedFiles == 0
	})
	c.WaitFor(15*time.Second, "replicas of /a deleted on chunkservers", func() bool { return allGone(aFiles) })
	for _, f := range sharedFiles {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("replica still used by /snap was deleted: %v", err)
		}
	}
	checkContent(t, cl, "/snap", want)

	// Long retention: only an explicit second delete reclaims /b quickly.
	c.StopMaster()
	c.opt.TrashRetention = time.Hour
	c.StartMaster()
	c.WaitServers(3)
	cl = c.Client()
	if err := cl.Delete("/b"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	trash := trashEntries(t, cl)
	if len(trash) != 1 || allGone(bFiles) {
		t.Fatalf("with 1 h retention: trash %v, replicas gone %v", trash, allGone(bFiles))
	}
	for hidden := range trash {
		if !strings.HasPrefix(hidden, master.TrashDir+"/") {
			t.Fatalf("hidden name %q", hidden)
		}
		if err := cl.Delete(hidden); err != nil {
			t.Fatalf("delete %s: %v", hidden, err)
		}
	}
	c.WaitFor(15*time.Second, "replicas of /b deleted after expedited delete", func() bool { return allGone(bFiles) })
	st, err := cl.ClusterStatus()
	if err != nil || st.NumChunks != int64(len(c.Locations("/snap"))) {
		t.Fatalf("master still tracks %d chunks, want only /snap's", st.NumChunks)
	}
}
