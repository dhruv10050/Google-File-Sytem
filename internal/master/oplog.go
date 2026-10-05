package master

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
)

// Operation log (§2.6.3).
//
// On disk: <dir>/<firstLSN:020d>.log segments, each a sequence of frames
//
//	| magic u32 | payload_len u32 | crc32c(payload) u32 | payload (LogRecord) |
//
// Appends are group-committed: a single writer goroutine takes every queued
// record, writes them with one write + one fsync, and only then advances the
// durable LSN that callers wait on. Durable records are also kept in memory
// (since the last checkpoint) so shadow masters can tail them.

const (
	frameMagic  uint32 = 0x47465331 // "GFS1"
	frameHeader        = 12
)

var (
	crcTable  = crc32.MakeTable(crc32.Castagnoli)
	ErrTooOld = errors.New("requested log position is older than the retained log tail")
)

type logItem struct {
	rec    *pb.LogRecord
	rotate bool
}

type OpLog struct {
	dir string
	log *slog.Logger

	mu        sync.Mutex
	queue     []logItem
	durable   uint64
	notify    chan struct{} // closed whenever durable advances
	tail      []*pb.LogRecord
	tailStart uint64 // LSN of tail[0] (or the next LSN when tail is empty)
	err       error
	closed    bool

	wake chan struct{}
	done chan struct{}
	f    *os.File // owned by the writer goroutine
}

// OpenLog starts a log whose next record will have LSN nextLSN. A fresh
// segment is created on the first append, so a torn tail left in an older
// segment by a crash is never appended to.
func OpenLog(dir string, nextLSN uint64, logger *slog.Logger) (*OpLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &OpLog{
		dir: dir, log: logger,
		durable: nextLSN - 1, tailStart: nextLSN,
		notify: make(chan struct{}),
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
	go l.writer()
	return l, nil
}

// Append queues a record. The caller assigns LSNs and must call Append in
// LSN order (the master does so while holding its state lock).
func (l *OpLog) Append(rec *pb.LogRecord) {
	l.mu.Lock()
	l.queue = append(l.queue, logItem{rec: rec})
	l.mu.Unlock()
	l.signal()
}

// Rotate makes the next record start a new segment (used at checkpoints).
func (l *OpLog) Rotate() {
	l.mu.Lock()
	l.queue = append(l.queue, logItem{rotate: true})
	l.mu.Unlock()
	l.signal()
}

func (l *OpLog) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// WaitDurable blocks until every record up to lsn is on stable storage.
func (l *OpLog) WaitDurable(lsn uint64) error {
	for {
		l.mu.Lock()
		if l.err != nil {
			err := l.err
			l.mu.Unlock()
			return err
		}
		if l.durable >= lsn {
			l.mu.Unlock()
			return nil
		}
		ch := l.notify
		l.mu.Unlock()
		<-ch
	}
}

func (l *OpLog) Durable() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.durable
}

func (l *OpLog) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	l.mu.Unlock()
	l.signal()
	<-l.done
}

func (l *OpLog) writer() {
	defer close(l.done)
	defer func() {
		if l.f != nil {
			l.f.Close()
		}
	}()
	for {
		<-l.wake
		l.mu.Lock()
		items, closed := l.queue, l.closed
		l.queue = nil
		l.mu.Unlock()
		if len(items) > 0 {
			if err := l.writeBatch(items); err != nil {
				l.log.Error("operation log write failed", "err", err)
				l.mu.Lock()
				l.err = fmt.Errorf("oplog: %w", err)
				close(l.notify)
				l.notify = make(chan struct{})
				l.mu.Unlock()
				return
			}
		}
		if closed {
			l.mu.Lock()
			pending := len(l.queue)
			l.mu.Unlock()
			if pending == 0 {
				return
			}
			l.signal()
		}
	}
}

func (l *OpLog) writeBatch(items []logItem) error {
	var buf bytes.Buffer
	var recs []*pb.LogRecord
	flush := func() error {
		if buf.Len() == 0 {
			return nil
		}
		if _, err := l.f.Write(buf.Bytes()); err != nil {
			return err
		}
		buf.Reset()
		return l.f.Sync()
	}
	for _, it := range items {
		if it.rotate {
			if err := flush(); err != nil {
				return err
			}
			if l.f != nil {
				l.f.Close()
				l.f = nil
			}
			continue
		}
		if l.f == nil {
			f, err := os.OpenFile(filepath.Join(l.dir, fmt.Sprintf("%020d.log", it.rec.Lsn)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				return err
			}
			l.f = f
			syncDir(l.dir)
		}
		if err := encodeFrame(&buf, it.rec); err != nil {
			return err
		}
		recs = append(recs, it.rec)
	}
	if err := flush(); err != nil {
		return err
	}
	if len(recs) == 0 {
		return nil
	}
	l.mu.Lock()
	l.durable = recs[len(recs)-1].Lsn
	if len(l.tail) == 0 {
		l.tailStart = recs[0].Lsn
	}
	l.tail = append(l.tail, recs...)
	close(l.notify)
	l.notify = make(chan struct{})
	l.mu.Unlock()
	return nil
}

// TrimTail drops in-memory records with LSN <= lsn.
func (l *OpLog) TrimTail(lsn uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	i := sort.Search(len(l.tail), func(i int) bool { return l.tail[i].Lsn > lsn })
	l.tail = append([]*pb.LogRecord(nil), l.tail[i:]...)
	if len(l.tail) > 0 {
		l.tailStart = l.tail[0].Lsn
	} else {
		l.tailStart = max(l.tailStart, lsn+1)
	}
}

// TailFrom returns durable records starting at LSN from, blocking until at
// least one is available. ErrTooOld means the caller must fetch a checkpoint.
func (l *OpLog) TailFrom(ctx context.Context, from uint64, limit int) ([]*pb.LogRecord, error) {
	for {
		l.mu.Lock()
		if l.err != nil {
			err := l.err
			l.mu.Unlock()
			return nil, err
		}
		if from < l.tailStart {
			l.mu.Unlock()
			return nil, ErrTooOld
		}
		if from <= l.durable {
			i := int(from - l.tailStart)
			j := min(len(l.tail), i+limit)
			out := append([]*pb.LogRecord(nil), l.tail[i:j]...)
			l.mu.Unlock()
			return out, nil
		}
		ch := l.notify
		l.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func encodeFrame(buf *bytes.Buffer, rec *pb.LogRecord) error {
	payload, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	var hdr [frameHeader]byte
	binary.LittleEndian.PutUint32(hdr[0:], frameMagic)
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[8:], crc32.Checksum(payload, crcTable))
	buf.Write(hdr[:])
	buf.Write(payload)
	return nil
}

// decodeFrames parses frames from b. It returns the records and the length
// of the valid prefix; validLen < len(b) means the rest is torn or corrupt.
func decodeFrames(b []byte) (recs []*pb.LogRecord, validLen int) {
	off := 0
	for off+frameHeader <= len(b) {
		if binary.LittleEndian.Uint32(b[off:]) != frameMagic {
			break
		}
		n := int(binary.LittleEndian.Uint32(b[off+4:]))
		sum := binary.LittleEndian.Uint32(b[off+8:])
		if off+frameHeader+n > len(b) {
			break
		}
		payload := b[off+frameHeader : off+frameHeader+n]
		if crc32.Checksum(payload, crcTable) != sum {
			break
		}
		rec := &pb.LogRecord{}
		if err := proto.Unmarshal(payload, rec); err != nil {
			break
		}
		recs = append(recs, rec)
		off += frameHeader + n
	}
	return recs, off
}

type segment struct {
	first uint64
	path  string
}

func listSegments(dir string) ([]segment, error) {
	ents, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var segs []segment
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue
		}
		segs = append(segs, segment{first: n, path: filepath.Join(dir, name)})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].first < segs[j].first })
	return segs, nil
}

// ReplayLog calls fn for every record in every segment, in order. A torn
// final record (a crash mid-write) in the last segment is truncated away;
// damage anywhere else is a hard error.
func ReplayLog(dir string, logger *slog.Logger, fn func(*pb.LogRecord) error) error {
	segs, err := listSegments(dir)
	if err != nil {
		return err
	}
	for i, s := range segs {
		b, err := os.ReadFile(s.path)
		if err != nil {
			return err
		}
		recs, valid := decodeFrames(b)
		if valid < len(b) {
			if i != len(segs)-1 {
				return fmt.Errorf("oplog segment %s corrupt at byte %d", s.path, valid)
			}
			logger.Warn("truncating torn operation log tail", "segment", filepath.Base(s.path), "valid_bytes", valid, "dropped_bytes", len(b)-valid)
			if err := os.Truncate(s.path, int64(valid)); err != nil {
				return err
			}
		}
		for _, r := range recs {
			if err := fn(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// RemoveSegmentsCoveredBy deletes segments whose every record has
// LSN <= lsn. The newest segment is never removed.
func RemoveSegmentsCoveredBy(dir string, lsn uint64) error {
	segs, err := listSegments(dir)
	if err != nil {
		return err
	}
	for i := 0; i+1 < len(segs); i++ {
		if segs[i+1].first-1 <= lsn {
			if err := os.Remove(segs[i].path); err != nil {
				return err
			}
		}
	}
	return nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
