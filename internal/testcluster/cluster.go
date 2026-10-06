// Package testcluster runs a whole GFS cluster inside one test process: a
// master, an optional shadow and N chunkservers on loopback ports, each
// with its own directory, with fast timers so failure handling is quick.
// Servers can be stopped and restarted on the same address and directory
// to simulate crashes.
package testcluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/chunkserver"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
	"github.com/dhruv10050/Google-File-Sytem/internal/master"
	"github.com/dhruv10050/Google-File-Sytem/pkg/client"
)

type Options struct {
	ChunkServers           int
	ChunkSize              int64 // default 1 MiB so tests cross chunk boundaries cheaply
	Replication            int32
	Shadow                 bool
	Lease                  time.Duration
	Heartbeat              time.Duration
	CheckpointEveryRecords int
	TrashRetention         time.Duration // 0 = master default (3 days)
	GCInterval             time.Duration
	Verbose                bool // log to stderr instead of discarding
}

type Cluster struct {
	t   testing.TB
	opt Options
	dir string
	log *slog.Logger

	Master     *master.Master
	MasterAddr string
	Shadow     *master.Master
	ShadowAddr string
	CS         []*chunkserver.Server // nil while stopped
	CSAddr     []string
}

func Start(t testing.TB, opt Options) *Cluster {
	t.Helper()
	if opt.ChunkServers == 0 {
		opt.ChunkServers = 3
	}
	if opt.ChunkSize == 0 {
		opt.ChunkSize = 1 << 20
	}
	if opt.Replication == 0 {
		opt.Replication = 3
	}
	if opt.Lease == 0 {
		opt.Lease = 5 * time.Second
	}
	if opt.Heartbeat == 0 {
		opt.Heartbeat = 100 * time.Millisecond
	}
	c := &Cluster{t: t, opt: opt, dir: t.TempDir()}
	if opt.Verbose {
		c.log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	} else {
		c.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	c.MasterAddr = freeAddr(t)
	c.StartMaster()
	if opt.Shadow {
		c.ShadowAddr = freeAddr(t)
		c.StartShadow()
	}
	c.CS = make([]*chunkserver.Server, opt.ChunkServers)
	c.CSAddr = make([]string, opt.ChunkServers)
	for i := range c.CS {
		c.CSAddr[i] = freeAddr(t)
		c.StartChunkServer(i)
	}
	c.WaitServers(opt.ChunkServers)
	t.Cleanup(c.Stop)
	return c
}

func freeAddr(t testing.TB) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func (c *Cluster) masterConfig(shadow bool) master.Config {
	name, addr := "master", c.MasterAddr
	if shadow {
		name, addr = "shadow", c.ShadowAddr
	}
	return master.Config{
		ListenAddr: addr, Dir: filepath.Join(c.dir, name), Logger: c.log.With("c", name),
		ChunkSize: c.opt.ChunkSize, DefaultReplication: c.opt.Replication,
		LeaseDuration: c.opt.Lease, HeartbeatInterval: c.opt.Heartbeat,
		CheckpointEveryRecords: c.opt.CheckpointEveryRecords,
		TrashRetention:         c.opt.TrashRetention, GCInterval: c.opt.GCInterval,
		Shadow: shadow, PrimaryAddr: c.MasterAddr,
	}
}

func (c *Cluster) startMaster(shadow bool) *master.Master {
	c.t.Helper()
	var m *master.Master
	var err error
	// The port may linger briefly after a restart; retry the bind.
	for i := 0; i < 50; i++ {
		if m, err = master.New(c.masterConfig(shadow)); err != nil {
			c.t.Fatalf("master.New: %v", err)
		}
		if err = m.Start(); err == nil {
			return m
		}
		m.Stop()
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("master start: %v", err)
	return nil
}

func (c *Cluster) StartMaster() { c.Master = c.startMaster(false) }
func (c *Cluster) StartShadow() { c.Shadow = c.startMaster(true) }

// StopMaster simulates a master crash (no final checkpoint).
func (c *Cluster) StopMaster() {
	if c.Master != nil {
		c.Master.Stop()
		c.Master = nil
	}
}

func (c *Cluster) masters() []string {
	if c.opt.Shadow {
		return []string{c.MasterAddr, c.ShadowAddr}
	}
	return []string{c.MasterAddr}
}

func (c *Cluster) CSDir(i int) string { return filepath.Join(c.dir, fmt.Sprintf("cs%d", i)) }

func (c *Cluster) StartChunkServer(i int) {
	c.t.Helper()
	var err error
	for try := 0; try < 50; try++ {
		var s *chunkserver.Server
		s, err = chunkserver.New(chunkserver.Config{
			Dir: c.CSDir(i), ListenAddr: c.CSAddr[i], Masters: c.masters(),
			BufferMB: 256, Logger: c.log.With("c", fmt.Sprintf("cs%d", i)),
		})
		if err != nil {
			c.t.Fatalf("chunkserver.New: %v", err)
		}
		if err = s.Start(); err == nil {
			c.CS[i] = s
			return
		}
		s.Stop()
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("chunkserver %d start: %v", i, err)
}

// StopChunkServer simulates a chunkserver crash.
func (c *Cluster) StopChunkServer(i int) {
	if c.CS[i] != nil {
		c.CS[i].Stop()
		c.CS[i] = nil
	}
}

func (c *Cluster) Stop() {
	for i := range c.CS {
		c.StopChunkServer(i)
	}
	if c.Shadow != nil {
		c.Shadow.Stop()
		c.Shadow = nil
	}
	c.StopMaster()
}

// Client returns a client for this cluster, closed at test end.
func (c *Cluster) Client() *client.Client {
	c.t.Helper()
	cl, err := client.New(client.Options{
		Masters: c.masters(), LocationTTL: 2 * time.Second, Retries: 10, RPCTimeout: 10 * time.Second,
	})
	if err != nil {
		c.t.Fatalf("client: %v", err)
	}
	c.t.Cleanup(cl.Close)
	return cl
}

// WaitServers waits until the master sees n live chunkservers.
func (c *Cluster) WaitServers(n int) {
	c.t.Helper()
	cl, err := client.New(client.Options{Masters: []string{c.MasterAddr}})
	if err != nil {
		c.t.Fatalf("client: %v", err)
	}
	defer cl.Close()
	c.WaitFor(10*time.Second, fmt.Sprintf("%d chunkservers registered", n), func() bool {
		st, err := cl.ClusterStatus()
		return err == nil && len(st.Servers) >= n
	})
}

// WaitFor polls cond until it holds or the timeout expires.
func (c *Cluster) WaitFor(timeout time.Duration, what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Locations asks the master for every chunk of path.
func (c *Cluster) Locations(path string) []*pb.ChunkLocation {
	c.t.Helper()
	conn, err := common.Dial(c.MasterAddr)
	if err != nil {
		c.t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := pb.NewMasterClient(conn).GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: path, Count: 1 << 20})
	if err != nil {
		c.t.Fatalf("locations of %s: %v", path, err)
	}
	return resp.Chunks
}

// ChunkInfo asks chunkserver i about handle h.
func (c *Cluster) ChunkInfo(i int, h uint64) (*pb.ChunkReport, error) {
	conn, err := common.Dial(c.CSAddr[i])
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return pb.NewChunkServerClient(conn).ChunkInfo(ctx, &pb.ChunkInfoRequest{Handle: h})
}

// ChunkFile is the on-disk data file of handle h on chunkserver i.
func (c *Cluster) ChunkFile(i int, h uint64) string {
	return filepath.Join(c.CSDir(i), "chunks", common.ChunkHandle(h).String()+".chunk")
}

// Index returns the chunkserver index serving addr, or -1.
func (c *Cluster) Index(addr string) int {
	for i, a := range c.CSAddr {
		if a == addr {
			return i
		}
	}
	return -1
}

// LeaseHolder asks the master who holds the lease on chunk idx of path.
func (c *Cluster) LeaseHolder(path string, idx int64) *pb.GetLeaseHolderResponse {
	c.t.Helper()
	conn, err := common.Dial(c.MasterAddr)
	if err != nil {
		c.t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := pb.NewMasterClient(conn).GetLeaseHolder(ctx, &pb.GetLeaseHolderRequest{Path: path, Index: idx})
	if err != nil {
		c.t.Fatalf("lease holder of %s[%d]: %v", path, idx, err)
	}
	return resp
}
