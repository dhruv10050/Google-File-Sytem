// Package master implements the GFS master: in-memory metadata made durable
// by an operation log and checkpoints, chunk leases, replica placement,
// re-replication, heartbeats, and a read-only shadow mode.
package master

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

type Config struct {
	ListenAddr string
	Dir        string
	Logger     *slog.Logger

	ChunkSize          int64
	DefaultReplication int32

	LeaseDuration     time.Duration // §3.1: 60 s
	HeartbeatInterval time.Duration
	DeadAfter         time.Duration // missed-heartbeat window before a chunkserver is declared dead

	// Re-replication (§4.3).
	ReplicationScanInterval time.Duration
	RereplicationDelay      time.Duration // after startup, wait for chunkservers to report first
	MaxClonesPerServer      int
	MaxClonesCluster        int

	CheckpointEveryRecords int

	// Garbage collection (§4.4): deleted files stay under a hidden name for
	// TrashRetention (default 3 days, as in the paper; negative deletes at
	// once), checked every GCInterval.
	TrashRetention time.Duration
	GCInterval     time.Duration

	// Shadow mode (§5.1.3): follow PrimaryAddr's operation log, serve reads only.
	Shadow      bool
	PrimaryAddr string
}

func (c *Config) setDefaults() {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.ChunkSize == 0 {
		c.ChunkSize = common.DefaultChunkSize
	}
	if c.DefaultReplication == 0 {
		c.DefaultReplication = common.DefaultReplication
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = 60 * time.Second
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = time.Second
	}
	if c.DeadAfter == 0 {
		c.DeadAfter = 5 * c.HeartbeatInterval
	}
	if c.ReplicationScanInterval == 0 {
		c.ReplicationScanInterval = time.Second
	}
	if c.RereplicationDelay == 0 {
		c.RereplicationDelay = c.DeadAfter + 2*c.HeartbeatInterval
	}
	if c.MaxClonesPerServer == 0 {
		c.MaxClonesPerServer = 4
	}
	if c.TrashRetention == 0 {
		c.TrashRetention = 72 * time.Hour
	}
	if c.GCInterval == 0 {
		c.GCInterval = time.Minute
	}
	if c.CheckpointEveryRecords == 0 {
		c.CheckpointEveryRecords = 50000
	}
}

// chunkSoft is the master's non-persistent view of one chunk.
type chunkSoft struct {
	// mu serializes lease grants, version bumps and clones of this chunk.
	// It is never held while acquiring Master.mu for long.
	mu sync.Mutex

	// Guarded by Master.mu:
	locs        map[string]bool // server IDs holding an up-to-date replica
	corruptOn   map[string]bool // server IDs that reported a corrupted replica
	primary     string          // server ID of the lease holder
	leaseExpiry time.Time
	leaseLocs   []string // replicas at grant time; the lease is void once one is lost
	cloning     bool
}

type serverInfo struct {
	id, addr   string
	lastHB     time.Time
	diskUsed   int64
	diskFree   int64
	chunks     map[common.ChunkHandle]bool
	creates    []time.Time // recent chunk creations, for placement (§4.2)
	clonesBusy int
}

type Master struct {
	pb.UnimplementedMasterServer

	cfg Config
	log *slog.Logger

	// mu guards st and all soft state below.
	mu         sync.RWMutex
	st         *State
	soft       map[common.ChunkHandle]*chunkSoft
	servers    map[string]*serverInfo // by server ID
	pending    map[common.ChunkHandle]bool
	nextHandle uint64
	fileAlloc  map[string]*sync.Mutex

	oplog      *OpLog
	sinceCkpt  atomic.Int64
	ckptActive atomic.Bool
	ckptMu     sync.Mutex

	pool       *common.ConnPool
	startedAt  time.Time
	clonesBusy atomic.Int64
	clonesDone atomic.Int64

	grpc   *grpc.Server
	lis    net.Listener
	addr   string
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(cfg Config) (*Master, error) {
	cfg.setDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	m := &Master{
		cfg:       cfg,
		log:       cfg.Logger,
		soft:      map[common.ChunkHandle]*chunkSoft{},
		servers:   map[string]*serverInfo{},
		pending:   map[common.ChunkHandle]bool{},
		fileAlloc: map[string]*sync.Mutex{},
		pool:      common.NewConnPool(),
		ctx:       ctx,
		cancel:    cancel,
	}
	if cfg.Shadow {
		m.log = m.log.With("role", "shadow")
	}
	if err := m.recover(); err != nil {
		cancel()
		return nil, err
	}
	return m, nil
}

func (m *Master) ckptDir() string { return filepath.Join(m.cfg.Dir, "checkpoints") }
func (m *Master) logDir() string  { return filepath.Join(m.cfg.Dir, "oplog") }

// recover rebuilds metadata from the newest checkpoint plus the log (§2.6.3).
func (m *Master) recover() error {
	start := time.Now()
	cp, err := LoadLatestCheckpoint(m.ckptDir())
	if err != nil {
		return err
	}
	st := NewState()
	if cp != nil {
		if st, err = StateFromCheckpoint(cp); err != nil {
			return fmt.Errorf("checkpoint invalid: %w", err)
		}
	}
	replayed := 0
	err = ReplayLog(m.logDir(), m.log, func(r *pb.LogRecord) error {
		if r.Lsn <= st.LSN {
			return nil
		}
		if _, err := st.Apply(r); err != nil {
			return fmt.Errorf("replay lsn %d: %w", r.Lsn, err)
		}
		replayed++
		return nil
	})
	if err != nil {
		return err
	}
	if err := st.CheckInvariants(); err != nil {
		return fmt.Errorf("recovered state inconsistent: %w", err)
	}
	m.st = st
	m.nextHandle = st.NextHandle
	for h := range st.Chunks {
		m.soft[h] = newChunkSoft()
	}
	m.oplog, err = OpenLog(m.logDir(), st.LSN+1, m.log)
	if err != nil {
		return err
	}
	m.sinceCkpt.Store(int64(replayed))
	m.log.Info("metadata recovered", "checkpoint_lsn", cpLSN(cp), "replayed", replayed, "lsn", st.LSN,
		"files", len(st.Files), "chunks", len(st.Chunks), "took", time.Since(start))
	return nil
}

func cpLSN(cp *pb.Checkpoint) uint64 {
	if cp == nil {
		return 0
	}
	return cp.Lsn
}

func newChunkSoft() *chunkSoft {
	return &chunkSoft{locs: map[string]bool{}, corruptOn: map[string]bool{}}
}

// Start serves RPCs and starts background loops.
func (m *Master) Start() error {
	lis, err := net.Listen("tcp", m.cfg.ListenAddr)
	if err != nil {
		return err
	}
	m.lis = lis
	m.addr = lis.Addr().String()
	m.startedAt = time.Now()
	m.grpc = grpc.NewServer(common.ServerOptions()...)
	pb.RegisterMasterServer(m.grpc, m)
	go m.grpc.Serve(lis)

	m.goLoop(m.livenessLoop)
	if m.cfg.Shadow {
		m.goLoop(m.followPrimary)
	} else {
		m.goLoop(m.replicationLoop)
		m.goLoop(m.gcLoop)
	}
	m.log.Info("master started", "addr", m.addr, "shadow", m.cfg.Shadow)
	return nil
}

func (m *Master) goLoop(f func()) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		f()
	}()
}

func (m *Master) Addr() string { return m.addr }

// Stop shuts down without a final checkpoint: recovery must never depend on
// a clean shutdown (§5.1.1).
func (m *Master) Stop() {
	m.cancel()
	if m.grpc != nil {
		m.grpc.Stop()
	}
	m.wg.Wait()
	m.ckptMu.Lock() // wait for an in-flight checkpoint
	m.ckptMu.Unlock()
	m.oplog.Close()
	m.pool.Close()
}

// ─────────────────────── Commit pipeline ───────────────────────

// commit validates, applies and logs one metadata mutation, returning once
// it is durable. Records are applied in LSN order under m.mu; durability is
// group-committed by the log writer. The change is visible to other RPCs
// before it is durable, but every RPC waits for durability of everything
// it observed before replying (see readDone), so no client ever acts on
// state that could be lost in a crash (§2.6.3).
func (m *Master) commit(rec *pb.LogRecord) error {
	return m.commitIf(rec, nil)
}

// commitIf is commit with a precondition on soft state, checked under m.mu
// atomically with applying rec. A failed precondition's error is returned
// unchanged.
func (m *Master) commitIf(rec *pb.LogRecord, pre func() error) error {
	if m.cfg.Shadow {
		return status.Error(codes.FailedPrecondition, "shadow master is read-only")
	}
	m.mu.Lock()
	if pre != nil {
		if err := pre(); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	rec.Lsn = m.st.LSN + 1
	rec.UnixNanos = time.Now().UnixNano()
	orphaned, err := m.st.Apply(rec)
	if err != nil {
		m.mu.Unlock()
		return toStatus(err)
	}
	m.oplog.Append(rec)
	m.afterApplyLocked(rec, orphaned)
	lsn := rec.Lsn
	m.mu.Unlock()

	if err := m.oplog.WaitDurable(lsn); err != nil {
		return status.Errorf(codes.Internal, "log: %v", err)
	}
	if m.sinceCkpt.Add(1) >= int64(m.cfg.CheckpointEveryRecords) {
		m.maybeCheckpoint()
	}
	return nil
}

// afterApplyLocked keeps soft state in sync with an applied record.
func (m *Master) afterApplyLocked(rec *pb.LogRecord, orphaned []common.ChunkHandle) {
	var added uint64
	if a := rec.GetAddChunk(); a != nil {
		added = a.Handle
	} else if r := rec.GetReplaceChunk(); r != nil {
		added = r.NewHandle
	}
	if added != 0 {
		if h := common.ChunkHandle(added); m.soft[h] == nil {
			m.soft[h] = newChunkSoft()
		}
		m.nextHandle = max(m.nextHandle, added+1)
	}
	// Orphaned chunks are forgotten; their replicas are reclaimed lazily
	// when chunkservers report them in heartbeats (§4.4).
	for _, h := range orphaned {
		if cs := m.soft[h]; cs != nil {
			for id := range cs.locs {
				if s := m.servers[id]; s != nil {
					delete(s.chunks, h)
				}
			}
		}
		delete(m.soft, h)
	}
}

// readDone waits until everything a read observed is durable.
func (m *Master) readDone(lsn uint64) error {
	if m.cfg.Shadow {
		return nil
	}
	return m.oplog.WaitDurable(lsn)
}

func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrNotDir), errors.Is(err, ErrNotEmpty):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// ─────────────────────── Checkpointing ───────────────────────

func (m *Master) maybeCheckpoint() {
	if !m.ckptActive.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer m.ckptActive.Store(false)
		if err := m.Checkpoint(); err != nil {
			m.log.Error("checkpoint failed", "err", err)
		}
	}()
}

// Checkpoint snapshots the state and starts a new log segment (§2.6.3).
// Mutations are blocked only while the state is copied, not while the
// checkpoint is written.
func (m *Master) Checkpoint() error {
	m.ckptMu.Lock()
	defer m.ckptMu.Unlock()
	start := time.Now()
	m.mu.Lock() // excludes commits, so Rotate lands exactly after cp.Lsn
	cp := m.st.ToCheckpoint()
	m.oplog.Rotate()
	m.sinceCkpt.Store(0)
	m.mu.Unlock()

	// A shadow's state may come from an installed checkpoint that its own
	// log never contained, so only the primary waits on its log here.
	if !m.cfg.Shadow {
		if err := m.oplog.WaitDurable(cp.Lsn); err != nil {
			return err
		}
	}
	if err := WriteCheckpoint(m.ckptDir(), cp); err != nil {
		return err
	}
	oldest, err := PruneCheckpoints(m.ckptDir(), 2)
	if err != nil {
		return err
	}
	if err := RemoveSegmentsCoveredBy(m.logDir(), oldest); err != nil {
		return err
	}
	m.oplog.TrimTail(oldest)
	m.log.Info("checkpoint written", "lsn", cp.Lsn, "files", len(cp.Files), "chunks", len(cp.Chunks), "took", time.Since(start))
	return nil
}

// ─────────────────────── Namespace RPCs ───────────────────────

func (m *Master) GetConfig(ctx context.Context, _ *pb.Empty) (*pb.ConfigResponse, error) {
	return &pb.ConfigResponse{
		ChunkSize: m.cfg.ChunkSize, BlockSize: common.BlockSize,
		DefaultReplication: m.cfg.DefaultReplication, IsShadow: m.cfg.Shadow,
	}, nil
}

func cleanPath(p string) (string, error) {
	c, err := common.CleanPath(p)
	if err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	return c, nil
}

func (m *Master) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.Empty, error) {
	p, err := cleanPath(req.Path)
	if err != nil {
		return nil, err
	}
	dirs := []string{p}
	if req.Parents {
		dirs = append(common.Ancestors(p), p)
	}
	for _, d := range dirs {
		if req.Parents {
			m.mu.RLock()
			exists := m.st.Dirs[d]
			m.mu.RUnlock()
			if exists {
				continue
			}
		}
		err := m.commit(&pb.LogRecord{Op: &pb.LogRecord_Mkdir{Mkdir: &pb.MkdirOp{Path: d}}})
		if err != nil && !(req.Parents && status.Code(err) == codes.AlreadyExists) {
			return nil, err
		}
	}
	return &pb.Empty{}, nil
}

func (m *Master) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.Empty, error) {
	p, err := cleanPath(req.Path)
	if err != nil {
		return nil, err
	}
	r := req.Replication
	if r == 0 {
		r = m.cfg.DefaultReplication
	}
	return &pb.Empty{}, m.commit(&pb.LogRecord{Op: &pb.LogRecord_CreateFile{CreateFile: &pb.CreateFileOp{Path: p, Replication: r}}})
}

// DeleteFile removes an empty directory at once, but only renames a file to
// a hidden name (§4.4.1); gcLoop reclaims it later. Deleting a hidden file
// removes it immediately.
func (m *Master) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.Empty, error) {
	p, err := cleanPath(req.Path)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	_, isFile := m.st.Files[p]
	m.mu.RUnlock()
	if isFile && !InTrash(p) && m.cfg.TrashRetention > 0 {
		return &pb.Empty{}, m.commit(&pb.LogRecord{Op: &pb.LogRecord_TrashFile{TrashFile: &pb.TrashFileOp{Path: p}}})
	}
	return &pb.Empty{}, m.commit(&pb.LogRecord{Op: &pb.LogRecord_DeleteFile{DeleteFile: &pb.DeleteFileOp{Path: p}}})
}

// Undelete moves a hidden file back to where it was deleted from.
func (m *Master) Undelete(ctx context.Context, req *pb.UndeleteRequest) (*pb.UndeleteResponse, error) {
	p, err := cleanPath(req.Path)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	var orig string
	if f := m.st.Files[p]; f != nil {
		orig = f.DeletedFrom
	}
	m.mu.RUnlock()
	if err := m.commit(&pb.LogRecord{Op: &pb.LogRecord_RestoreFile{RestoreFile: &pb.RestoreFileOp{Hidden: p}}}); err != nil {
		return nil, err
	}
	return &pb.UndeleteResponse{Path: orig}, nil
}

func (m *Master) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	p, err := cleanPath(req.Path)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	if !m.st.Dirs[p] {
		m.mu.RUnlock()
		return nil, status.Errorf(codes.NotFound, "%s: not a directory", p)
	}
	resp := &pb.ListResponse{Entries: m.st.Children(p)}
	lsn := m.st.LSN
	m.mu.RUnlock()
	return resp, m.readDone(lsn)
}

func (m *Master) Stat(ctx context.Context, req *pb.StatRequest) (*pb.StatResponse, error) {
	p, err := cleanPath(req.Path)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	var resp *pb.StatResponse
	if m.st.Dirs[p] {
		resp = &pb.StatResponse{IsDir: true}
	} else if f, ok := m.st.Files[p]; ok {
		resp = &pb.StatResponse{NumChunks: int64(len(f.Chunks)), Replication: f.Replication, CtimeUnixNanos: f.Ctime}
	}
	lsn := m.st.LSN
	m.mu.RUnlock()
	if resp == nil {
		return nil, status.Errorf(codes.NotFound, "%s: %v", p, ErrNotFound)
	}
	return resp, m.readDone(lsn)
}

// ClusterStatus reports replica health; benchmarks poll it during recovery.
func (m *Master) ClusterStatus(ctx context.Context, _ *pb.Empty) (*pb.ClusterStatusResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now()
	resp := &pb.ClusterStatusResponse{
		NumFiles: int64(len(m.st.Files)), NumChunks: int64(len(m.st.Chunks)),
		AppliedLsn: m.st.LSN, IsShadow: m.cfg.Shadow,
		ClonesInFlight: m.clonesBusy.Load(), ClonesCompleted: m.clonesDone.Load(),
	}
	for p := range m.st.Files {
		if InTrash(p) {
			resp.DeletedFiles++
		}
	}
	for _, s := range m.servers {
		resp.Servers = append(resp.Servers, &pb.ServerStatus{
			ServerId: s.id, Address: s.addr, NumChunks: int64(len(s.chunks)),
			DiskUsed: s.diskUsed, DiskFree: s.diskFree, LastHeartbeatMsAgo: now.Sub(s.lastHB).Milliseconds(),
		})
	}
	goals := m.replicationGoalsLocked()
	for h := range m.st.Chunks {
		n := len(m.liveLocsLocked(h))
		for len(resp.ReplicaHistogram) <= n {
			resp.ReplicaHistogram = append(resp.ReplicaHistogram, 0)
		}
		resp.ReplicaHistogram[n]++
		if n < int(goals[h]) {
			resp.UnderReplicated++
		}
	}
	return resp, nil
}
