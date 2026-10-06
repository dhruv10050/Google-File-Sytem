// Package client is the GFS client library (§2.3). It talks to the master
// only for metadata, caches chunk locations and lease holders, and moves
// all file data directly to and from chunkservers.
package client

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

var (
	ErrNotFound       = errors.New("gfs: file not found")
	ErrRecordTooLarge = errors.New("gfs: record larger than a quarter chunk")
)

type Options struct {
	// Masters lists the primary master first, then shadow masters. Reads
	// fall back to shadows when the primary is unreachable (§5.1.3).
	Masters     []string
	LocationTTL time.Duration // chunk-location cache lifetime; default 60 s
	Retries     int           // attempts per chunk mutation; default 6
	RPCTimeout  time.Duration
	// AvoidLocal makes every byte cross the network (benchmarks): reads
	// prefer replicas on other hosts, and a replica on this host is placed
	// last in the push chain so it receives data from a remote hop.
	AvoidLocal bool
}

type Client struct {
	opt       Options
	masters   []pb.MasterClient
	pool      *common.ConnPool
	chunkSize int64
	local     map[string]bool // this host's IPs, for "closest replica"

	mu     sync.Mutex
	files  map[string]*fileEntry
	leases map[leaseKey]*pb.GetLeaseHolderResponse
	tail   map[string]int64 // chunk index of the last successful record append per file
}

type fileEntry struct {
	fetched   time.Time
	numChunks int64
	chunks    map[int64]*pb.ChunkLocation
}

type leaseKey struct {
	path string
	idx  int64
}

func New(opt Options) (*Client, error) {
	if len(opt.Masters) == 0 {
		return nil, errors.New("no master address")
	}
	if opt.LocationTTL == 0 {
		opt.LocationTTL = time.Minute
	}
	if opt.Retries == 0 {
		opt.Retries = 6
	}
	if opt.RPCTimeout == 0 {
		opt.RPCTimeout = 30 * time.Second
	}
	c := &Client{
		opt: opt, pool: common.NewConnPool(), local: localIPs(),
		files: map[string]*fileEntry{}, leases: map[leaseKey]*pb.GetLeaseHolderResponse{}, tail: map[string]int64{},
	}
	for _, a := range opt.Masters {
		conn, err := c.pool.Get(a)
		if err != nil {
			return nil, err
		}
		c.masters = append(c.masters, pb.NewMasterClient(conn))
	}
	var cfg *pb.ConfigResponse
	err := c.readMaster(func(ctx context.Context, m pb.MasterClient) (err error) {
		cfg, err = m.GetConfig(ctx, &pb.Empty{})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("contact master: %w", err)
	}
	c.chunkSize = cfg.ChunkSize
	return c, nil
}

func (c *Client) Close()           { c.pool.Close() }
func (c *Client) ChunkSize() int64 { return c.chunkSize }

func (c *Client) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), c.opt.RPCTimeout)
}

// writeMaster runs a mutation against the primary master only.
func (c *Client) writeMaster(f func(context.Context, pb.MasterClient) error) error {
	ctx, cancel := c.ctx()
	defer cancel()
	return mapErr(f(ctx, c.masters[0]))
}

// readMaster tries the primary, then shadows if the primary is unreachable.
func (c *Client) readMaster(f func(context.Context, pb.MasterClient) error) error {
	var err error
	for i, m := range c.masters {
		ctx, cancel := context.WithTimeout(context.Background(), c.opt.RPCTimeout/time.Duration(len(c.masters)))
		err = f(ctx, m)
		cancel()
		if code := status.Code(err); code != codes.Unavailable && code != codes.DeadlineExceeded || i == len(c.masters)-1 {
			break
		}
	}
	return mapErr(err)
}

func mapErr(err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %s", ErrNotFound, status.Convert(err).Message())
	}
	return err
}

// ───────────────────────── Namespace ─────────────────────────

func (c *Client) Mkdir(path string, parents bool) error {
	return c.writeMaster(func(ctx context.Context, m pb.MasterClient) error {
		_, err := m.Mkdir(ctx, &pb.MkdirRequest{Path: path, Parents: parents})
		return err
	})
}

// Create makes an empty file; replication 0 means the cluster default.
func (c *Client) Create(path string, replication int) error {
	return c.writeMaster(func(ctx context.Context, m pb.MasterClient) error {
		_, err := m.CreateFile(ctx, &pb.CreateFileRequest{Path: path, Replication: int32(replication)})
		return err
	})
}

func (c *Client) Delete(path string) error {
	c.invalidate(path)
	return c.writeMaster(func(ctx context.Context, m pb.MasterClient) error {
		_, err := m.DeleteFile(ctx, &pb.DeleteFileRequest{Path: path})
		return err
	})
}

func (c *Client) List(path string) ([]*pb.ListEntry, error) {
	var resp *pb.ListResponse
	err := c.readMaster(func(ctx context.Context, m pb.MasterClient) (err error) {
		resp, err = m.List(ctx, &pb.ListRequest{Path: path})
		return err
	})
	if err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

func (c *Client) Stat(path string) (*pb.StatResponse, error) {
	var resp *pb.StatResponse
	err := c.readMaster(func(ctx context.Context, m pb.MasterClient) (err error) {
		resp, err = m.Stat(ctx, &pb.StatRequest{Path: path})
		return err
	})
	return resp, err
}

func (c *Client) ClusterStatus() (*pb.ClusterStatusResponse, error) {
	var resp *pb.ClusterStatusResponse
	err := c.readMaster(func(ctx context.Context, m pb.MasterClient) (err error) {
		resp, err = m.ClusterStatus(ctx, &pb.Empty{})
		return err
	})
	return resp, err
}

// Size returns the file length: full chunks plus the last chunk's length.
func (c *Client) Size(path string) (int64, error) {
	fe, err := c.fileInfo(path, 0, true)
	if err != nil {
		return 0, err
	}
	if fe.numChunks == 0 {
		return 0, nil
	}
	last, err := c.location(path, fe.numChunks-1)
	if err != nil {
		return 0, err
	}
	var lastErr error
	for _, addr := range c.orderReplicas(last.Replicas) {
		cli, err := c.cs(addr)
		if err != nil {
			lastErr = err
			continue
		}
		ctx, cancel := c.ctx()
		info, err := cli.ChunkInfo(ctx, &pb.ChunkInfoRequest{Handle: last.Handle})
		cancel()
		if err == nil && info.Version >= last.Version {
			return (fe.numChunks-1)*c.chunkSize + info.Length, nil
		}
		lastErr = err
	}
	return 0, fmt.Errorf("size of %s: no replica answered: %v", path, lastErr)
}

// ───────────────────────── Metadata cache ─────────────────────────

const prefetch = 16

// fileInfo returns cached chunk locations for path starting at idx,
// refreshing from the master when stale or missing.
func (c *Client) fileInfo(path string, idx int64, refresh bool) (*fileEntry, error) {
	c.mu.Lock()
	fe := c.files[path]
	fresh := fe != nil && time.Since(fe.fetched) < c.opt.LocationTTL
	if fresh && !refresh && (idx >= fe.numChunks || fe.chunks[idx] != nil) {
		c.mu.Unlock()
		return fe, nil
	}
	c.mu.Unlock()

	var resp *pb.GetChunkLocationsResponse
	err := c.readMaster(func(ctx context.Context, m pb.MasterClient) (err error) {
		resp, err = m.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: path, StartIndex: idx, Count: prefetch})
		return err
	})
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	fe = c.files[path]
	if fe == nil || time.Since(fe.fetched) >= c.opt.LocationTTL || fe.numChunks != resp.NumChunks {
		fe = &fileEntry{chunks: map[int64]*pb.ChunkLocation{}}
		c.files[path] = fe
	}
	fe.fetched, fe.numChunks = time.Now(), resp.NumChunks
	for _, l := range resp.Chunks {
		fe.chunks[l.Index] = l
	}
	return fe, nil
}

func (c *Client) location(path string, idx int64) (*pb.ChunkLocation, error) {
	fe, err := c.fileInfo(path, idx, false)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	l := fe.chunks[idx]
	c.mu.Unlock()
	if l == nil {
		return nil, io.EOF
	}
	return l, nil
}

func (c *Client) invalidate(path string) {
	c.mu.Lock()
	delete(c.files, path)
	for k := range c.leases {
		if k.path == path {
			delete(c.leases, k)
		}
	}
	c.mu.Unlock()
}

func (c *Client) cs(addr string) (pb.ChunkServerClient, error) {
	conn, err := c.pool.Get(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewChunkServerClient(conn), nil
}

// ───────────────────────── Locality ─────────────────────────

func localIPs() map[string]bool {
	out := map[string]bool{}
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok {
			out[ipn.IP.String()] = true
		}
	}
	if h, err := os.Hostname(); err == nil {
		out[h] = true
		out[strings.Split(h, ".")[0]] = true
	}
	return out
}

func hostIP(addr string) net.IP {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return nil
	}
	return ips[0]
}

func (c *Client) isLocal(addr string) bool {
	host, _, _ := net.SplitHostPort(addr)
	if c.local[host] || c.local[strings.Split(host, ".")[0]] {
		return true
	}
	ip := hostIP(addr)
	return ip != nil && c.local[ip.String()]
}

// distance approximates network distance from IP addresses (§3.2): same
// host is 0, otherwise fewer shared leading bits means farther.
func distance(a, b net.IP) int {
	if a == nil || b == nil {
		return 128
	}
	a16, b16 := a.To16(), b.To16()
	n := 0
	for i := range a16 {
		x := a16[i] ^ b16[i]
		if x == 0 {
			n += 8
			continue
		}
		for x&0x80 == 0 {
			n++
			x <<= 1
		}
		break
	}
	return 128 - n
}

// orderReplicas sorts read candidates: a same-host replica first (last
// with AvoidLocal), the rest in random order to spread load.
func (c *Client) orderReplicas(addrs []string) []string {
	out := append([]string(nil), addrs...)
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	sort.SliceStable(out, func(i, j int) bool {
		li, lj := c.isLocal(out[i]), c.isLocal(out[j])
		if c.opt.AvoidLocal {
			return !li && lj
		}
		return li && !lj
	})
	return out
}

// chainOrder builds the data-push chain (§3.2): each hop forwards to the
// closest replica that has not received the data yet, starting from the
// client itself (with AvoidLocal, a same-host replica goes last instead).
func (c *Client) chainOrder(addrs []string) []string {
	var rest, local []string
	for _, a := range addrs {
		if c.isLocal(a) {
			local = append(local, a)
		} else {
			rest = append(rest, a)
		}
	}
	var out, tail []string
	var cur net.IP
	if c.opt.AvoidLocal {
		tail = local
	} else if len(local) > 0 {
		out = append(out, local[0])
		cur = hostIP(local[0])
		rest = append(rest, local[1:]...)
	}
	for len(rest) > 0 {
		best := 0
		if cur != nil {
			for i := range rest {
				if distance(cur, hostIP(rest[i])) < distance(cur, hostIP(rest[best])) {
					best = i
				}
			}
		}
		out = append(out, rest[best])
		cur = hostIP(rest[best])
		rest = append(rest[:best], rest[best+1:]...)
	}
	return append(out, tail...)
}

func newDataID() string {
	var b [16]byte
	crand.Read(b[:])
	return hex.EncodeToString(b[:])
}
