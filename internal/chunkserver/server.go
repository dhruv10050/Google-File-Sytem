// Package chunkserver implements a GFS chunkserver: chunk storage with
// per-block checksums, the pipelined data push, primary/secondary mutation
// ordering, and heartbeats to the master(s).
package chunkserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

type Config struct {
	Dir        string
	ListenAddr string   // e.g. ":7001"
	Advertise  string   // address other machines use to reach us; defaults to ListenAddr
	Masters    []string // primary master first, then shadow masters
	FsyncData  bool
	BufferMB   int64
	Logger     *slog.Logger
}

type Server struct {
	pb.UnimplementedChunkServerServer

	cfg   Config
	id    string
	addr  string
	store *Store
	buf   *dataBuffer
	pool  *common.ConnPool
	log   *slog.Logger

	mu          sync.Mutex
	hbInterval  time.Duration
	pendingCorr map[common.ChunkHandle]bool // corrupted replicas not yet acknowledged

	grpc   *grpc.Server
	lis    net.Listener
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.BufferMB == 0 {
		cfg.BufferMB = 1024
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	id, err := loadOrCreateID(cfg.Dir)
	if err != nil {
		return nil, err
	}
	// The real chunk size arrives with the master's Register response.
	store, err := OpenStore(cfg.Dir, common.DefaultChunkSize, cfg.FsyncData)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg:         cfg,
		id:          id,
		store:       store,
		buf:         newDataBuffer(cfg.BufferMB<<20, 2*time.Minute),
		pool:        common.NewConnPool(),
		log:         cfg.Logger.With("cs", id[:8]),
		hbInterval:  time.Second,
		pendingCorr: map[common.ChunkHandle]bool{},
		ctx:         ctx,
		cancel:      cancel,
	}
	for _, c := range store.All() {
		if c.IsCorrupted() {
			s.pendingCorr[c.handle] = true
		}
	}
	return s, nil
}

func loadOrCreateID(dir string) (string, error) {
	p := filepath.Join(dir, "server-id")
	if b, err := os.ReadFile(p); err == nil {
		return strings.TrimSpace(string(b)), nil
	}
	var r [16]byte
	if _, err := rand.Read(r[:]); err != nil {
		return "", err
	}
	id := hex.EncodeToString(r[:])
	return id, writeFileSync(p, []byte(id+"\n"), true)
}

func (s *Server) ID() string   { return s.id }
func (s *Server) Addr() string { return s.addr }

// Start listens, serves, and starts registration/heartbeat loops.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return err
	}
	s.lis = lis
	s.addr = s.cfg.Advertise
	if s.addr == "" {
		s.addr = advertiseAddr(lis.Addr().(*net.TCPAddr))
	}
	s.grpc = grpc.NewServer(common.ServerOptions()...)
	pb.RegisterChunkServerServer(s.grpc, s)
	go s.grpc.Serve(lis)

	for i, m := range s.cfg.Masters {
		s.wg.Add(1)
		go s.heartbeatLoop(m, i == 0)
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-t.C:
				s.buf.Expire()
			}
		}
	}()
	s.log.Info("chunkserver started", "addr", s.addr, "chunks", len(s.store.All()))
	return nil
}

// Stop shuts the server down immediately (used by tests to simulate a crash
// when combined with not flushing anything further).
func (s *Server) Stop() {
	s.cancel()
	if s.grpc != nil {
		s.grpc.Stop()
	}
	s.wg.Wait()
	s.pool.Close()
}

// ───────────────────────── Data flow ─────────────────────────

// PushData receives mutation data and forwards each frame to the next hop
// of the chain as soon as it arrives (§3.2), then buffers it locally.
func (s *Server) PushData(stream pb.ChunkServer_PushDataServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.DataId == "" {
		return status.Error(codes.InvalidArgument, "first frame must carry data_id")
	}
	var (
		fwd     chan []byte
		fwdDone chan error
	)
	if len(first.Chain) > 0 {
		next := first.Chain[0]
		conn, err := s.pool.Get(next)
		if err != nil {
			return status.Errorf(codes.Unavailable, "dial %s: %v", next, err)
		}
		down, err := pb.NewChunkServerClient(conn).PushData(stream.Context())
		if err != nil {
			return status.Errorf(codes.Unavailable, "push to %s: %v", next, err)
		}
		if err := down.Send(&pb.PushDataFrame{DataId: first.DataId, Chain: first.Chain[1:]}); err != nil {
			return status.Errorf(codes.Unavailable, "push to %s: %v", next, err)
		}
		fwd = make(chan []byte, 16)
		fwdDone = make(chan error, 1)
		go func() {
			var sendErr error
			for p := range fwd {
				if sendErr == nil {
					sendErr = down.Send(&pb.PushDataFrame{Payload: p})
				}
			}
			_, err := down.CloseAndRecv()
			if err == nil {
				err = sendErr
			}
			if err != nil {
				err = fmt.Errorf("downstream %s: %w", next, err)
			}
			fwdDone <- err
		}()
	}

	data := append([]byte(nil), first.Payload...)
	if fwd != nil && len(first.Payload) > 0 {
		fwd <- first.Payload
	}
	var recvErr error
	for {
		f, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			recvErr = err
			break
		}
		data = append(data, f.Payload...)
		if fwd != nil {
			fwd <- f.Payload
		}
	}
	if fwd != nil {
		close(fwd)
		if err := <-fwdDone; err != nil && recvErr == nil {
			recvErr = err
		}
	}
	if recvErr != nil {
		return status.Errorf(codes.Unavailable, "push data: %v", recvErr)
	}
	s.buf.Put(first.DataId, data)
	return stream.SendAndClose(&pb.PushDataResponse{Bytes: int64(len(data))})
}

// ─────────────────────── Control flow ───────────────────────

func (s *Server) getChunk(h uint64) (*chunk, error) {
	c, err := s.store.Get(common.ChunkHandle(h))
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "chunk %016x not on this server", h)
	}
	return c, nil
}

func (s *Server) leaseValid(c *chunk) bool {
	c.leaseMu.Lock()
	defer c.leaseMu.Unlock()
	return time.Now().Before(c.leaseExpiry)
}

func (s *Server) clearLease(c *chunk) {
	c.leaseMu.Lock()
	c.leaseExpiry = time.Time{}
	c.leaseMu.Unlock()
}

// Write is sent by the client to the primary once all replicas hold the
// data (§3.1 step 4). The primary assigns the serial number, applies the
// mutation locally, and forwards it to every secondary (step 5).
//
// One mutation per chunk is in flight at the primary at a time (mutMu),
// which keeps secondaries trivially in serial order. Data push, the
// expensive part, is not serialized.
func (s *Server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.Empty, error) {
	c, err := s.getChunk(req.Handle)
	if err != nil {
		return nil, err
	}
	c.mutMu.Lock()
	defer c.mutMu.Unlock()
	data, err := s.primaryCheck(c, req.Version, req.DataId)
	if err != nil {
		return nil, err
	}
	c.serial++
	if err := s.applyLocal(c, req.Offset, data); err != nil {
		return nil, err
	}
	s.buf.Delete(req.DataId)
	m := &pb.ApplyMutationRequest{Handle: req.Handle, Version: req.Version, Serial: c.serial, Offset: req.Offset, DataId: req.DataId}
	return &pb.Empty{}, s.forward(ctx, c, req.Secondaries, m)
}

// RecordAppend appends the pushed data at an offset the primary chooses: the
// current end of its replica (§3.3). Every secondary writes it at that same
// offset. If the record does not fit, all replicas pad the chunk to its full
// size and the client retries on the next chunk.
//
// A failed append can leave the record on some replicas only. The client
// retries, so a record may be present more than once, and replicas may hold
// different bytes in the failed region: GFS guarantees the record is written
// atomically at least once, at an offset that is the same on every replica.
func (s *Server) RecordAppend(ctx context.Context, req *pb.RecordAppendRequest) (*pb.RecordAppendResponse, error) {
	c, err := s.getChunk(req.Handle)
	if err != nil {
		return nil, err
	}
	c.mutMu.Lock()
	defer c.mutMu.Unlock()
	data, err := s.primaryCheck(c, req.Version, req.DataId)
	if err != nil {
		return nil, err
	}
	size := s.store.ChunkSize()
	if int64(len(data)) > size/4 {
		return nil, status.Errorf(codes.InvalidArgument, "record of %d bytes exceeds the limit of %d (chunk size / 4)", len(data), size/4)
	}
	c.serial++
	m := &pb.ApplyMutationRequest{Handle: req.Handle, Version: req.Version, Serial: c.serial}
	resp := &pb.RecordAppendResponse{}
	if off := c.Length(); off+int64(len(data)) > size {
		m.PadTo, resp.ChunkFull = size, true
		err = s.padLocal(c, size)
	} else {
		m.Offset, m.DataId, resp.Offset = off, req.DataId, off
		err = s.applyLocal(c, off, data)
	}
	if err != nil {
		return nil, err
	}
	s.buf.Delete(req.DataId)
	if err := s.forward(ctx, c, req.Secondaries, m); err != nil {
		return nil, err
	}
	return resp, nil
}

// primaryCheck verifies, under mutMu, that this replica is the current
// primary for version and returns the pushed data.
func (s *Server) primaryCheck(c *chunk, version uint64, dataID string) ([]byte, error) {
	if v := c.Version(); v != version {
		return nil, status.Errorf(codes.FailedPrecondition, "version mismatch: have %d, request %d", v, version)
	}
	if !s.leaseValid(c) {
		return nil, status.Error(codes.FailedPrecondition, "not primary (no valid lease)")
	}
	data, ok := s.buf.Get(dataID)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "pushed data not found (expired?)")
	}
	return data, nil
}

// forward sends an applied mutation to every secondary in parallel.
func (s *Server) forward(ctx context.Context, c *chunk, secondaries []string, m *pb.ApplyMutationRequest) error {
	errs := make([]error, len(secondaries))
	var wg sync.WaitGroup
	for i, sec := range secondaries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := s.pool.Get(sec)
			if err == nil {
				_, err = pb.NewChunkServerClient(conn).ApplyMutation(ctx, m)
			}
			if err != nil {
				errs[i] = fmt.Errorf("secondary %s: %v", sec, err)
			}
		}()
	}
	wg.Wait()
	c.leaseMu.Lock()
	c.lastMutation = time.Now()
	c.leaseMu.Unlock()
	if err := errors.Join(errs...); err != nil {
		// The region is now inconsistent across replicas; the client retries (§3.1 step 7).
		return status.Errorf(codes.Aborted, "mutation failed at secondaries: %v", err)
	}
	return nil
}

// ApplyMutation is the primary's forwarded write (or record-append padding)
// to a secondary.
func (s *Server) ApplyMutation(ctx context.Context, req *pb.ApplyMutationRequest) (*pb.Empty, error) {
	c, err := s.getChunk(req.Handle)
	if err != nil {
		return nil, err
	}
	c.mutMu.Lock()
	defer c.mutMu.Unlock()
	if v := c.Version(); v != req.Version {
		return nil, status.Errorf(codes.FailedPrecondition, "version mismatch: have %d, request %d", v, req.Version)
	}
	if req.Serial <= c.serial {
		return nil, status.Errorf(codes.FailedPrecondition, "out-of-order serial %d (last %d)", req.Serial, c.serial)
	}
	if req.PadTo > 0 {
		if err := s.padLocal(c, req.PadTo); err != nil {
			return nil, err
		}
		c.serial = req.Serial
		return &pb.Empty{}, nil
	}
	data, ok := s.buf.Get(req.DataId)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "pushed data not found (expired?)")
	}
	if err := s.applyLocal(c, req.Offset, data); err != nil {
		return nil, err
	}
	c.serial = req.Serial
	s.buf.Delete(req.DataId)
	return &pb.Empty{}, nil
}

func (s *Server) padLocal(c *chunk, n int64) error {
	return s.storeErr(c, s.store.Pad(c, n))
}

func (s *Server) applyLocal(c *chunk, off int64, data []byte) error {
	return s.storeErr(c, s.store.Write(c, off, data))
}

// storeErr maps a store error to a gRPC status, noting corruption.
func (s *Server) storeErr(c *chunk, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrCorrupted):
		s.noteCorrupted(c)
		return status.Errorf(codes.DataLoss, "chunk %s corrupted", c.handle)
	case errors.Is(err, ErrBounds):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "write: %v", err)
	}
}

func (s *Server) noteCorrupted(c *chunk) {
	c.MarkCorrupted()
	s.mu.Lock()
	s.pendingCorr[c.handle] = true
	s.mu.Unlock()
	s.log.Warn("checksum mismatch; replica marked corrupted", "chunk", c.handle)
}

// ─────────────────────────── Reads ───────────────────────────

const readWindow = 1 << 20

func (s *Server) Read(req *pb.ReadRequest, stream pb.ChunkServer_ReadServer) error {
	c, err := s.getChunk(req.Handle)
	if err != nil {
		return err
	}
	if c.IsCorrupted() {
		return status.Errorf(codes.DataLoss, "chunk %016x corrupted", req.Handle)
	}
	if v := c.Version(); v < req.MinVersion {
		return status.Errorf(codes.FailedPrecondition, "stale replica: version %d < %d", v, req.MinVersion)
	}
	off, end := req.Offset, req.Offset+req.Length
	for off < end {
		b, err := s.store.Read(c, off, min(readWindow, end-off))
		if errors.Is(err, ErrCorrupted) {
			s.noteCorrupted(c)
			return status.Errorf(codes.DataLoss, "chunk %016x corrupted", req.Handle)
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read: %v", err)
		}
		if len(b) == 0 {
			break // end of chunk
		}
		if err := stream.Send(&pb.DataFrame{Data: b}); err != nil {
			return err
		}
		off += int64(len(b))
	}
	return nil
}

func (s *Server) ChunkInfo(ctx context.Context, req *pb.ChunkInfoRequest) (*pb.ChunkReport, error) {
	c, err := s.getChunk(req.Handle)
	if err != nil {
		return nil, err
	}
	r, ok := c.Report()
	if !ok {
		return nil, status.Errorf(codes.DataLoss, "chunk %016x corrupted", req.Handle)
	}
	return r, nil
}

// ───────────────────── Master commands ─────────────────────

func (s *Server) CreateChunk(ctx context.Context, req *pb.CreateChunkRequest) (*pb.Empty, error) {
	if err := s.store.Create(common.ChunkHandle(req.Handle), req.Version); err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "create %016x: %v", req.Handle, err)
	}
	return &pb.Empty{}, nil
}

// SetVersion persists a new chunk version. It waits for any in-flight
// mutation and drops this server's lease on the chunk: a version change
// starts a new lease epoch, so an old primary can never complete a
// mutation under the old version afterwards (secondaries reject it).
func (s *Server) SetVersion(ctx context.Context, req *pb.SetVersionRequest) (*pb.Empty, error) {
	c, err := s.getChunk(req.Handle)
	if err != nil {
		return nil, err
	}
	c.mutMu.Lock()
	defer c.mutMu.Unlock()
	if err := s.store.SetVersion(c, req.OldVersion, req.NewVersion); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	s.clearLease(c)
	return &pb.Empty{}, nil
}

func (s *Server) GrantLease(ctx context.Context, req *pb.GrantLeaseRequest) (*pb.Empty, error) {
	c, err := s.getChunk(req.Handle)
	if err != nil {
		return nil, err
	}
	if v := c.Version(); v != req.Version {
		return nil, status.Errorf(codes.FailedPrecondition, "lease for version %d but have %d", req.Version, v)
	}
	d := time.Duration(req.DurationMs) * time.Millisecond
	c.leaseMu.Lock()
	// The master starts its own lease clock after this RPC returns, so its
	// view of the expiry is always later than ours.
	c.leaseExpiry = time.Now().Add(d)
	c.lastMutation = time.Now()
	c.leaseMu.Unlock()
	return &pb.Empty{}, nil
}

// RevokeLease stops accepting new mutations and returns once any in-flight
// mutation has finished.
func (s *Server) RevokeLease(ctx context.Context, req *pb.RevokeLeaseRequest) (*pb.Empty, error) {
	c, err := s.getChunk(req.Handle)
	if err != nil {
		return &pb.Empty{}, nil
	}
	c.mutMu.Lock()
	s.clearLease(c)
	c.mutMu.Unlock()
	return &pb.Empty{}, nil
}

// CloneChunk copies a replica from another chunkserver (§4.3). The source
// verifies its checksums while serving the read, so corruption is never
// propagated (§5.2).
func (s *Server) CloneChunk(ctx context.Context, req *pb.CloneChunkRequest) (*pb.ChunkReport, error) {
	conn, err := s.pool.Get(req.Source)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial source: %v", err)
	}
	cli := pb.NewChunkServerClient(conn)
	info, err := cli.ChunkInfo(ctx, &pb.ChunkInfoRequest{Handle: req.Handle})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "source info: %v", err)
	}
	if info.Version != req.Version {
		return nil, status.Errorf(codes.FailedPrecondition, "source has version %d, want %d", info.Version, req.Version)
	}
	rs, err := cli.Read(ctx, &pb.ReadRequest{Handle: req.Handle, MinVersion: req.Version, Offset: 0, Length: info.Length})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "source read: %v", err)
	}
	data := make([]byte, 0, info.Length)
	for {
		f, err := rs.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "source read: %v", err)
		}
		data = append(data, f.Data...)
	}
	if int64(len(data)) != info.Length {
		return nil, status.Errorf(codes.Unavailable, "short clone: %d of %d bytes", len(data), info.Length)
	}
	if err := s.store.Install(common.ChunkHandle(req.Handle), req.Version, data); err != nil {
		return nil, status.Errorf(codes.Internal, "install: %v", err)
	}
	s.mu.Lock()
	delete(s.pendingCorr, common.ChunkHandle(req.Handle))
	s.mu.Unlock()
	return &pb.ChunkReport{Handle: req.Handle, Version: req.Version, Length: info.Length}, nil
}

func (s *Server) DeleteChunks(ctx context.Context, req *pb.DeleteChunksRequest) (*pb.Empty, error) {
	s.deleteChunks(req.Handles)
	return &pb.Empty{}, nil
}

func (s *Server) deleteChunks(hs []uint64) {
	for _, h := range hs {
		s.store.Delete(common.ChunkHandle(h))
		s.mu.Lock()
		delete(s.pendingCorr, common.ChunkHandle(h))
		s.mu.Unlock()
	}
	if len(hs) > 0 {
		s.log.Debug("deleted chunks", "n", len(hs))
	}
}

// VerifyAll reads and checksums every chunk (a full scrub).
func (s *Server) VerifyAll(ctx context.Context, _ *pb.Empty) (*pb.VerifyAllResponse, error) {
	resp := &pb.VerifyAllResponse{}
	for _, c := range s.store.All() {
		resp.Chunks++
		if c.IsCorrupted() {
			resp.Corrupted = append(resp.Corrupted, uint64(c.handle))
			continue
		}
		r, _ := c.Report()
		for off := int64(0); off < r.Length; off += readWindow {
			b, err := s.store.Read(c, off, readWindow)
			if err != nil {
				s.noteCorrupted(c)
				resp.Corrupted = append(resp.Corrupted, uint64(c.handle))
				break
			}
			resp.Bytes += int64(len(b))
		}
	}
	return resp, nil
}

// ───────────────────────── Heartbeats ─────────────────────────

const reportBatch = 1000

func (s *Server) diskFree() int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(s.cfg.Dir, &st); err != nil {
		return 0
	}
	return int64(st.Bavail) * int64(st.Bsize)
}

func (s *Server) fullReport() []*pb.ChunkReport {
	var out []*pb.ChunkReport
	for _, c := range s.store.All() {
		if r, ok := c.Report(); ok {
			out = append(out, r)
		}
	}
	return out
}

func (s *Server) register(ctx context.Context, cli pb.MasterClient, primary bool) error {
	resp, err := cli.Register(ctx, &pb.RegisterRequest{
		ServerId: s.id, Address: s.addr, Chunks: s.fullReport(),
		DiskUsed: s.store.DiskUsed(), DiskFree: s.diskFree(),
	})
	if err != nil {
		return err
	}
	if primary {
		if resp.ChunkSize > 0 {
			s.store.SetChunkSize(resp.ChunkSize)
		}
		if resp.HeartbeatIntervalMs > 0 {
			s.mu.Lock()
			s.hbInterval = time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond
			s.mu.Unlock()
		}
		s.deleteChunks(resp.Garbage)
	}
	return nil
}

// heartbeatLoop registers with one master and then heartbeats it. Only the
// primary master's instructions (garbage, lease extensions) are obeyed;
// shadows only use the reports to track replica locations.
func (s *Server) heartbeatLoop(masterAddr string, primary bool) {
	defer s.wg.Done()
	conn, err := common.Dial(masterAddr)
	if err != nil {
		s.log.Error("dial master", "addr", masterAddr, "err", err)
		return
	}
	defer conn.Close()
	cli := pb.NewMasterClient(conn)
	registered := false
	cursor := 0
	for {
		s.mu.Lock()
		iv := s.hbInterval
		s.mu.Unlock()
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(iv):
		}
		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		if !registered {
			if err := s.register(ctx, cli, primary); err != nil {
				cancel()
				continue
			}
			registered = true
		}
		req, sent := s.buildHeartbeat(&cursor)
		resp, err := cli.Heartbeat(ctx, req)
		cancel()
		if err != nil {
			continue
		}
		if resp.Reregister {
			registered = false
			continue
		}
		if !primary {
			continue
		}
		s.deleteChunks(resp.Garbage)
		for _, e := range resp.Extended {
			c, err := s.store.Get(common.ChunkHandle(e.Handle))
			if err != nil {
				continue
			}
			c.leaseMu.Lock()
			// Extend from when the request was sent: never later than the master's view.
			if exp := sent.Add(time.Duration(e.DurationMs) * time.Millisecond); exp.After(c.leaseExpiry) && !c.leaseExpiry.IsZero() {
				c.leaseExpiry = exp
			}
			c.leaseMu.Unlock()
		}
	}
}

func (s *Server) buildHeartbeat(cursor *int) (*pb.HeartbeatRequest, time.Time) {
	now := time.Now()
	all := s.store.All()
	req := &pb.HeartbeatRequest{
		ServerId: s.id, Address: s.addr, DiskUsed: s.store.DiskUsed(), DiskFree: s.diskFree(),
		SentUnixNanos: now.UnixNano(),
	}
	if *cursor >= len(all) {
		*cursor = 0
	}
	for i := *cursor; i < len(all) && len(req.Chunks) < reportBatch; i++ {
		if r, ok := all[i].Report(); ok {
			req.Chunks = append(req.Chunks, r)
		}
		*cursor = i + 1
	}
	for _, c := range all {
		c.leaseMu.Lock()
		// Ask for an extension while the chunk is being actively mutated (§3.1).
		if now.Before(c.leaseExpiry) && now.Sub(c.lastMutation) < 30*time.Second {
			req.LeaseExtensions = append(req.LeaseExtensions, uint64(c.handle))
		}
		c.leaseMu.Unlock()
	}
	s.mu.Lock()
	for h := range s.pendingCorr {
		req.Corrupted = append(req.Corrupted, uint64(h))
	}
	s.mu.Unlock()
	return req, now
}

// advertiseAddr turns a listener address into one peers can dial: a
// wildcard bind ("[::]:7001") becomes "<hostname>:7001".
func advertiseAddr(a *net.TCPAddr) string {
	if a.IP != nil && !a.IP.IsUnspecified() {
		return a.String()
	}
	host, err := os.Hostname()
	if err != nil {
		host = "localhost"
	}
	return net.JoinHostPort(host, strconv.Itoa(a.Port))
}
