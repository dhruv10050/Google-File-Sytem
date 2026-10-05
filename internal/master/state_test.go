package master

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// history is a small sequence of namespace and chunk operations.
func history() []*pb.LogRecord {
	ops := []*pb.LogRecord{
		{Op: &pb.LogRecord_Mkdir{Mkdir: &pb.MkdirOp{Path: "/a"}}},
		{Op: &pb.LogRecord_CreateFile{CreateFile: &pb.CreateFileOp{Path: "/a/f", Replication: 3}}},
		{Op: &pb.LogRecord_AddChunk{AddChunk: &pb.AddChunkOp{Path: "/a/f", Index: 0, Handle: 1, Version: 1}}},
		{Op: &pb.LogRecord_AddChunk{AddChunk: &pb.AddChunkOp{Path: "/a/f", Index: 1, Handle: 2, Version: 1}}},
		{Op: &pb.LogRecord_SetVersion{SetVersion: &pb.SetVersionOp{Handle: 1, Version: 2}}},
		{Op: &pb.LogRecord_CreateFile{CreateFile: &pb.CreateFileOp{Path: "/g", Replication: 2}}},
		{Op: &pb.LogRecord_AddChunk{AddChunk: &pb.AddChunkOp{Path: "/g", Index: 0, Handle: 3, Version: 1}}},
		{Op: &pb.LogRecord_DeleteFile{DeleteFile: &pb.DeleteFileOp{Path: "/g"}}},
	}
	for i, r := range ops {
		r.Lsn = uint64(i + 1)
		r.UnixNanos = int64(1000 + i)
	}
	return ops
}

func applyAll(t *testing.T, s *State, recs []*pb.LogRecord) {
	t.Helper()
	for _, r := range recs {
		if _, err := s.Apply(r); err != nil {
			t.Fatalf("apply lsn %d: %v", r.Lsn, err)
		}
	}
}

func TestStateApply(t *testing.T) {
	s := NewState()
	applyAll(t, s, history())
	if !s.Dirs["/a"] || s.Files["/a/f"] == nil || s.Files["/g"] != nil {
		t.Fatalf("namespace wrong: dirs=%v files=%v", s.Dirs, s.Files)
	}
	if got := s.Files["/a/f"].Chunks; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("chunks = %v", got)
	}
	if s.Chunks[1].Version != 2 || s.Chunks[3] != nil {
		t.Fatalf("chunk table wrong: %+v", s.Chunks)
	}
	if s.NextHandle != 4 || s.LSN != 8 {
		t.Fatalf("NextHandle=%d LSN=%d", s.NextHandle, s.LSN)
	}
	if err := s.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestStateRejects(t *testing.T) {
	s := NewState()
	applyAll(t, s, history())
	bad := []*pb.LogRecord{
		{Op: &pb.LogRecord_Mkdir{Mkdir: &pb.MkdirOp{Path: "/a"}}},                                             // exists
		{Op: &pb.LogRecord_CreateFile{CreateFile: &pb.CreateFileOp{Path: "/nope/x", Replication: 1}}},         // no parent
		{Op: &pb.LogRecord_DeleteFile{DeleteFile: &pb.DeleteFileOp{Path: "/a"}}},                              // not empty
		{Op: &pb.LogRecord_AddChunk{AddChunk: &pb.AddChunkOp{Path: "/a/f", Index: 5, Handle: 9, Version: 1}}}, // index gap
		{Op: &pb.LogRecord_AddChunk{AddChunk: &pb.AddChunkOp{Path: "/a/f", Index: 2, Handle: 1, Version: 1}}}, // handle reuse
		{Op: &pb.LogRecord_SetVersion{SetVersion: &pb.SetVersionOp{Handle: 1, Version: 2}}},                   // not increasing
	}
	for i, r := range bad {
		r.Lsn = s.LSN + 1
		if _, err := s.Apply(r); err == nil {
			t.Errorf("bad record %d accepted", i)
		}
	}
	if _, err := s.Apply(&pb.LogRecord{Lsn: s.LSN + 2, Op: history()[0].Op}); err == nil {
		t.Error("LSN gap accepted")
	}
	if s.LSN != 8 {
		t.Errorf("rejected records advanced LSN to %d", s.LSN)
	}
}

func TestCheckpointRoundTrip(t *testing.T) {
	s := NewState()
	applyAll(t, s, history())
	dir := t.TempDir()
	if err := WriteCheckpoint(dir, s.ToCheckpoint()); err != nil {
		t.Fatal(err)
	}
	cp, err := LoadLatestCheckpoint(dir)
	if err != nil || cp == nil {
		t.Fatalf("load: %v", err)
	}
	s2, err := StateFromCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(s.ToCheckpoint(), s2.ToCheckpoint()) {
		t.Fatal("checkpoint round trip changed the state")
	}
}

func TestCheckpointCorruptionDetected(t *testing.T) {
	s := NewState()
	applyAll(t, s, history())
	dir := t.TempDir()
	if err := WriteCheckpoint(dir, s.ToCheckpoint()); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, ckptName(s.LSN))
	b, _ := os.ReadFile(p)
	b[len(b)/2] ^= 0xff
	os.WriteFile(p, b, 0o644)
	if _, err := readCheckpoint(p); err == nil {
		t.Fatal("corrupted checkpoint was accepted")
	}
}

func TestPruneCheckpoints(t *testing.T) {
	dir := t.TempDir()
	for _, lsn := range []uint64{10, 20, 30} {
		if err := WriteCheckpoint(dir, &pb.Checkpoint{Lsn: lsn, NextHandle: 1, Dirs: []string{"/"}}); err != nil {
			t.Fatal(err)
		}
	}
	oldest, err := PruneCheckpoints(dir, 2)
	if err != nil || oldest != 20 {
		t.Fatalf("oldest kept = %d, err %v", oldest, err)
	}
	lsns, _ := listCheckpoints(dir)
	if len(lsns) != 2 {
		t.Fatalf("kept %v", lsns)
	}
}

func writeLog(t *testing.T, dir string, recs []*pb.LogRecord) {
	t.Helper()
	l, err := OpenLog(dir, recs[0].Lsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		l.Append(r)
	}
	if err := l.WaitDurable(recs[len(recs)-1].Lsn); err != nil {
		t.Fatal(err)
	}
	l.Close()
}

func replay(t *testing.T, dir string) *State {
	t.Helper()
	s := NewState()
	err := ReplayLog(dir, quiet, func(r *pb.LogRecord) error {
		_, err := s.Apply(r)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOpLogReplay(t *testing.T) {
	dir := t.TempDir()
	h := history()
	writeLog(t, dir, h[:4])
	writeLog(t, dir, h[4:]) // a reopened log starts a new segment
	segs, _ := listSegments(dir)
	if len(segs) != 2 {
		t.Fatalf("segments = %d, want 2", len(segs))
	}
	want := NewState()
	applyAll(t, want, h)
	if got := replay(t, dir); !proto.Equal(got.ToCheckpoint(), want.ToCheckpoint()) {
		t.Fatal("replayed state differs")
	}
}

// A crash in the middle of a write leaves a partial frame; recovery drops
// it and keeps every complete record before it.
func TestOpLogTornTail(t *testing.T) {
	dir := t.TempDir()
	h := history()
	writeLog(t, dir, h)
	segs, _ := listSegments(dir)
	fi, _ := os.Stat(segs[0].path)
	if err := os.Truncate(segs[0].path, fi.Size()-3); err != nil {
		t.Fatal(err)
	}
	s := replay(t, dir)
	if s.LSN != uint64(len(h)-1) {
		t.Fatalf("LSN after torn tail = %d, want %d", s.LSN, len(h)-1)
	}
	// The truncation is persistent, and a new log continues cleanly.
	writeLog(t, dir, []*pb.LogRecord{{Lsn: s.LSN + 1, Op: h[len(h)-1].Op}})
	if s2 := replay(t, dir); s2.LSN != uint64(len(h)) {
		t.Fatalf("LSN after continuing = %d", s2.LSN)
	}
}

func TestOpLogCorruptMiddleIsFatal(t *testing.T) {
	dir := t.TempDir()
	h := history()
	writeLog(t, dir, h[:4])
	writeLog(t, dir, h[4:])
	segs, _ := listSegments(dir)
	b, _ := os.ReadFile(segs[0].path)
	b[len(b)-2] ^= 0xff
	os.WriteFile(segs[0].path, b, 0o644)
	err := ReplayLog(dir, quiet, func(*pb.LogRecord) error { return nil })
	if err == nil {
		t.Fatal("corruption in a non-final segment was ignored")
	}
}

func TestOpLogTailFrom(t *testing.T) {
	dir := t.TempDir()
	h := history()
	l, err := OpenLog(dir, 1, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for _, r := range h {
		l.Append(r)
	}
	l.WaitDurable(uint64(len(h)))
	recs, err := l.TailFrom(t.Context(), 3, 100)
	if err != nil || len(recs) != len(h)-2 || recs[0].Lsn != 3 {
		t.Fatalf("TailFrom(3) = %d recs, err %v", len(recs), err)
	}
	l.TrimTail(5)
	if _, err := l.TailFrom(t.Context(), 3, 100); !errors.Is(err, ErrTooOld) {
		t.Fatalf("TailFrom below trimmed tail: err = %v", err)
	}
}
