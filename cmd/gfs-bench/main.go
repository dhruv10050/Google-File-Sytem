// Command gfs-bench runs the benchmark workloads. Each invocation is one
// client process; SLURM scripts in bench/slurm start several at once and
// collect the CSV line each prints.
//
//	gfs-bench net-serve  -listen :9100
//	gfs-bench net-send   -to host:9100 -seconds 10
//	gfs-bench write      -masters M -path /bench/w0 -bytes 1G -op 8M [-avoid-local] [-start-at NS]
//	gfs-bench read       -masters M -prefix /bench/ds/f -files 8 -bytes 2G -op 4M [-avoid-local] [-start-at NS]
//	gfs-bench append     -masters M -path /bench/a/log -bytes 1G -record 1M [-avoid-local] [-start-at NS]
//	gfs-bench scan       -masters M -path /bench/a/log
//	gfs-bench verify     -masters M -prefix /bench/ds/f -files 8 [-content-of /bench/ds/f] [-all-replicas -replicas 3] [-id i -of n]
//	gfs-bench watch      -masters M -expect-servers 3 [-interval 200ms] [-timeout 30m]
//	gfs-bench meta       -masters M -op create|stat|delete -dir /bench/meta/r1 [-shared] -threads 8 -count 500 [-start-at NS]
//	gfs-bench snapshot   -masters M -ds /bench/ds -files 8 -runs 3 -cow-files 3 -many 1000
//
// File contents are a pure function of (path, offset), so any process can
// regenerate the expected bytes of any region to verify it.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
	"github.com/dhruv10050/Google-File-Sytem/pkg/client"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: gfs-bench <net-serve|net-send|write|read|append|scan|verify|watch|meta|snapshot> [flags]")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "net-serve":
		err = netServe(args)
	case "net-send":
		err = netSend(args)
	case "write":
		err = write(args)
	case "read":
		err = read(args)
	case "append":
		err = appendBench(args)
	case "scan":
		err = scan(args)
	case "verify":
		err = verify(args)
	case "watch":
		err = watch(args)
	case "meta":
		err = meta(args)
	case "snapshot":
		err = snapshotBench(args)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "gfs-bench %s: %v\n", cmd, err)
		os.Exit(1)
	}
}

// ───────────────────────── Helpers ─────────────────────────

type sizeFlag int64

func (s *sizeFlag) String() string { return strconv.FormatInt(int64(*s), 10) }
func (s *sizeFlag) Set(v string) error {
	mult := int64(1)
	switch {
	case strings.HasSuffix(v, "G"):
		mult, v = 1<<30, strings.TrimSuffix(v, "G")
	case strings.HasSuffix(v, "M"):
		mult, v = 1<<20, strings.TrimSuffix(v, "M")
	case strings.HasSuffix(v, "K"):
		mult, v = 1<<10, strings.TrimSuffix(v, "K")
	}
	n, err := strconv.ParseInt(v, 10, 64)
	*s = sizeFlag(n * mult)
	return err
}

func host() string {
	h, _ := os.Hostname()
	return strings.Split(h, ".")[0]
}

func seedOf(path string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(path))
	return h.Sum64()
}

func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// fill writes the expected content of [off, off+len(buf)) of a file with
// the given seed. off and len(buf) must be multiples of 8.
func fill(buf []byte, seed uint64, off int64) {
	w := uint64(off / 8)
	for i := 0; i+8 <= len(buf); i += 8 {
		binary.LittleEndian.PutUint64(buf[i:], splitmix(seed^(w+uint64(i/8))))
	}
}

// waitUntil sleeps until the shared start time so all clients begin
// together; a start time of 0 means start now.
func waitUntil(ns int64) {
	if d := time.Until(time.Unix(0, ns)); ns > 0 && d > 0 {
		time.Sleep(d)
	}
}

func newClient(masters string, avoidLocal bool) (*client.Client, error) {
	return client.New(client.Options{Masters: strings.Split(masters, ","), AvoidLocal: avoidLocal, RPCTimeout: 2 * time.Minute})
}

type common_ struct {
	masters    *string
	avoidLocal *bool
	startAt    *int64
	id         *int
}

func commonFlags(fs *flag.FlagSet) common_ {
	return common_{
		masters:    fs.String("masters", "localhost:7000", "master addresses, primary first"),
		avoidLocal: fs.Bool("avoid-local", false, "never use a replica on this host as a read source or first push hop"),
		startAt:    fs.Int64("start-at", 0, "unix nanoseconds at which to start the timed phase"),
		id:         fs.Int("id", envInt("SLURM_PROCID", 0), "client id (default $SLURM_PROCID)"),
	}
}

func envInt(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

// result prints the one CSV line a timed client reports:
// kind,id,host,bytes,start_ns,end_ns,extra
func result(kind string, id int, n int64, start, end time.Time, extra string) {
	fmt.Printf("%s,%d,%s,%d,%d,%d,%s\n", kind, id, host(), n, start.UnixNano(), end.UnixNano(), extra)
}

// ───────────────────────── Network baseline ─────────────────────────

func netServe(args []string) error {
	fs := flag.NewFlagSet("net-serve", flag.ExitOnError)
	listen := fs.String("listen", ":9100", "listen address")
	fs.Parse(args)
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go func() {
			io.Copy(io.Discard, c)
			c.Close()
		}()
	}
}

func netSend(args []string) error {
	fs := flag.NewFlagSet("net-send", flag.ExitOnError)
	to := fs.String("to", "", "address of a net-serve process")
	secs := fs.Float64("seconds", 10, "duration")
	startAt := fs.Int64("start-at", 0, "unix nanoseconds to start")
	id := fs.Int("id", envInt("SLURM_PROCID", 0), "id")
	fs.Parse(args)
	var c net.Conn
	var err error
	for i := 0; i < 50; i++ { // the server may still be starting
		if c, err = net.Dial("tcp", *to); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	defer c.Close()
	buf := make([]byte, 1<<20)
	waitUntil(*startAt)
	start := time.Now()
	deadline := start.Add(time.Duration(*secs * float64(time.Second)))
	var n int64
	for time.Now().Before(deadline) {
		m, err := c.Write(buf)
		n += int64(m)
		if err != nil {
			return err
		}
	}
	result("net", *id, n, start, time.Now(), *to)
	return nil
}

// ───────────────────────── Write ─────────────────────────

func write(args []string) error {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	cf := commonFlags(fs)
	path := fs.String("path", "", "file to write (created if missing); {id} is replaced by the client id")
	var size, op sizeFlag = 1 << 30, 8 << 20
	fs.Var(&size, "bytes", "bytes to write")
	fs.Var(&op, "op", "bytes per write call")
	fs.Parse(args)
	p := strings.ReplaceAll(*path, "{id}", strconv.Itoa(*cf.id))
	if size%8 != 0 || op%8 != 0 {
		return errors.New("-bytes and -op must be multiples of 8")
	}
	c, err := newClient(*cf.masters, *cf.avoidLocal)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Mkdir(common.Parent(p), true); err != nil && !strings.Contains(err.Error(), "exists") {
		return err
	}
	if err := c.Create(p, 0); err != nil && !strings.Contains(err.Error(), "exists") {
		return err
	}

	// Generate the next buffer while the current one is being written.
	seed := seedOf(p)
	type piece struct {
		off int64
		buf []byte
	}
	ch := make(chan piece, 2)
	go func() {
		for off := int64(0); off < int64(size); off += int64(op) {
			b := make([]byte, min(int64(op), int64(size)-off))
			fill(b, seed, off)
			ch <- piece{off, b}
		}
		close(ch)
	}()
	waitUntil(*cf.startAt)
	start := time.Now()
	var n int64
	for pc := range ch {
		if err := c.Write(p, pc.off, pc.buf); err != nil {
			return err
		}
		n += int64(len(pc.buf))
	}
	result("write", *cf.id, n, start, time.Now(), p)
	return nil
}

// ───────────────────────── Read ─────────────────────────

func read(args []string) error {
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	cf := commonFlags(fs)
	prefix := fs.String("prefix", "/bench/ds/f", "dataset files are <prefix>0 .. <prefix>N-1")
	files := fs.Int("files", 8, "number of dataset files")
	check := fs.Bool("check", false, "verify every byte read (costs CPU)")
	var size, op sizeFlag = 2 << 30, 4 << 20
	fs.Var(&size, "bytes", "bytes to read")
	fs.Var(&op, "op", "bytes per read: a random op-aligned region (§6.1.1 uses 4 MB)")
	fs.Parse(args)
	c, err := newClient(*cf.masters, *cf.avoidLocal)
	if err != nil {
		return err
	}
	defer c.Close()
	type file struct {
		path    string
		regions int64
	}
	var ds []file
	for i := 0; i < *files; i++ {
		p := *prefix + strconv.Itoa(i)
		sz, err := c.Size(p)
		if err != nil {
			return err
		}
		if sz >= int64(op) {
			ds = append(ds, file{p, sz / int64(op)})
		}
	}
	if len(ds) == 0 {
		return errors.New("dataset is empty")
	}
	r := rand.New(rand.NewPCG(uint64(*cf.id)+1, uint64(time.Now().UnixNano())))
	buf, want := make([]byte, op), make([]byte, op)
	// Warm the location cache so the timed phase measures data transfer.
	for _, f := range ds {
		c.ReadAt(f.path, buf[:8], 0)
	}
	waitUntil(*cf.startAt)
	start := time.Now()
	var n int64
	for n < int64(size) {
		f := ds[r.IntN(len(ds))]
		off := r.Int64N(f.regions) * int64(op)
		m, err := c.ReadAt(f.path, buf, off)
		if err != nil {
			return fmt.Errorf("read %s@%d: %w", f.path, off, err)
		}
		if *check {
			fill(want, seedOf(f.path), off)
			if !bytes.Equal(buf[:m], want[:m]) {
				return fmt.Errorf("content mismatch in %s@%d", f.path, off)
			}
		}
		n += int64(m)
	}
	result("read", *cf.id, n, start, time.Now(), "")
	return nil
}

// ───────────────────────── Record append ─────────────────────────

// Records are self-describing so a reader can find them among padding,
// duplicates and fragments of failed appends (§3.3):
//
//	magic[8] writer u32 seq u32 length u32 crc u32 reserved[8] payload
//
// length covers the whole record and is a multiple of 8, so records start at
// 8-byte aligned offsets; crc covers the payload.
const (
	recMagic  = "GFSREC01"
	recHeader = 32
)

func makeRecord(buf []byte, writer, seq uint32) {
	p := buf[recHeader:]
	fill(p, splitmix(uint64(writer)<<32|uint64(seq)), 0)
	copy(buf, recMagic)
	binary.LittleEndian.PutUint32(buf[8:], writer)
	binary.LittleEndian.PutUint32(buf[12:], seq)
	binary.LittleEndian.PutUint32(buf[16:], uint32(len(buf)))
	binary.LittleEndian.PutUint32(buf[20:], crc32.Checksum(p, crc32.MakeTable(crc32.Castagnoli)))
}

func appendBench(args []string) error {
	fs := flag.NewFlagSet("append", flag.ExitOnError)
	cf := commonFlags(fs)
	path := fs.String("path", "/bench/a/log", "file all clients append to (created if missing)")
	var size, rec sizeFlag = 1 << 30, 1 << 20
	fs.Var(&size, "bytes", "bytes to append")
	fs.Var(&rec, "record", "record size (multiple of 8, at most chunk size / 4)")
	fs.Parse(args)
	if rec%8 != 0 || rec < recHeader+8 {
		return errors.New("-record must be a multiple of 8 and at least 40")
	}
	c, err := newClient(*cf.masters, *cf.avoidLocal)
	if err != nil {
		return err
	}
	defer c.Close()
	c.Mkdir(common.Parent(*path), true)
	if err := c.Create(*path, 0); err != nil && !strings.Contains(err.Error(), "exists") {
		return err
	}
	n := int64(size) / int64(rec)
	ch := make(chan []byte, 2)
	go func() {
		for i := int64(0); i < n; i++ {
			b := make([]byte, rec)
			makeRecord(b, uint32(*cf.id), uint32(i))
			ch <- b
		}
		close(ch)
	}()
	waitUntil(*cf.startAt)
	start := time.Now()
	var done, bytes int64
	for b := range ch {
		if _, err := c.RecordAppend(*path, b); err != nil {
			return fmt.Errorf("record %d: %w", done, err)
		}
		done++
		bytes += int64(len(b))
	}
	result("append", *cf.id, bytes, start, time.Now(), fmt.Sprintf("records=%d", done))
	return nil
}

// scan reads a record-append file sequentially and reports, per writer, how
// many distinct records it found and whether they are exactly seq 0..n-1.
// Duplicates (from retried appends) are counted but are not errors.
func scan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	masters := fs.String("masters", "localhost:7000", "master addresses")
	path := fs.String("path", "/bench/a/log", "file to scan")
	fs.Parse(args)
	c, err := newClient(*masters, false)
	if err != nil {
		return err
	}
	defer c.Close()
	start := time.Now()
	tab := crc32.MakeTable(crc32.Castagnoli)
	seen := map[uint32]map[uint32]int{}
	var total, valid, skipped int64
	cs := c.ChunkSize()
	buf := make([]byte, cs)
	for idx := int64(0); ; idx++ {
		n, err := c.ReadAt(*path, buf, idx*cs)
		if err != nil && err != io.EOF {
			return err
		}
		total += int64(n)
		b := buf[:n]
		for p := 0; p+recHeader <= len(b); {
			l := int(binary.LittleEndian.Uint32(b[p+16:]))
			if string(b[p:p+8]) == recMagic && l > recHeader && l%8 == 0 && p+l <= len(b) &&
				crc32.Checksum(b[p+recHeader:p+l], tab) == binary.LittleEndian.Uint32(b[p+20:]) {
				w, s := binary.LittleEndian.Uint32(b[p+8:]), binary.LittleEndian.Uint32(b[p+12:])
				if seen[w] == nil {
					seen[w] = map[uint32]int{}
				}
				seen[w][s]++
				valid += int64(l)
				p += l
				continue
			}
			skipped += 8
			p += 8
		}
		if err == io.EOF || n < len(buf) {
			break
		}
	}
	var writers []uint32
	for w := range seen {
		writers = append(writers, w)
	}
	slices.Sort(writers)
	var records, dups int
	for _, w := range writers {
		d, contiguous := 0, true
		for s, k := range seen[w] {
			if int(s) >= len(seen[w]) {
				contiguous = false
			}
			d++
			dups += k - 1
		}
		records += d
		fmt.Printf("writer,%d,%d,%t\n", w, d, contiguous)
	}
	result("scan", 0, total, start, time.Now(), fmt.Sprintf("writers=%d;records=%d;duplicates=%d;record_bytes=%d;other_bytes=%d", len(writers), records, dups, valid, skipped))
	return nil
}

// ───────────────────────── Verify ─────────────────────────

// verify checks file contents against the generator. With -all-replicas it
// reads every replica of every chunk directly from its chunkserver, so a
// pass means each copy is intact, not just one.
func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	cf := commonFlags(fs)
	prefix := fs.String("prefix", "/bench/ds/f", "files are <prefix>0 .. <prefix>N-1")
	files := fs.Int("files", 8, "number of files")
	contentOf := fs.String("content-of", "", "expected content is that written to <content-of>i, e.g. for snapshots (default: -prefix)")
	of := fs.Int("of", envInt("SLURM_NTASKS", 1), "number of verifier processes; this one takes files with i % of == id")
	allReplicas := fs.Bool("all-replicas", false, "read and check every replica of every chunk")
	wantReplicas := fs.Int("replicas", 3, "with -all-replicas: required replica count per chunk")
	var expect sizeFlag
	fs.Var(&expect, "expect-bytes", "required size of each file (0 = don't check)")
	fs.Parse(args)

	c, err := newClient(*cf.masters, false)
	if err != nil {
		return err
	}
	defer c.Close()
	conn, err := common.Dial(strings.Split(*cf.masters, ",")[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	mc := pb.NewMasterClient(conn)
	pool := common.NewConnPool()
	defer pool.Close()

	start := time.Now()
	var checked, chunks, replicaReads int64
	var problems []string
	for i := *cf.id; i < *files; i += *of {
		p := *prefix + strconv.Itoa(i)
		sz, err := c.Size(p)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: size: %v", p, err))
			continue
		}
		if expect > 0 && sz != int64(expect) {
			problems = append(problems, fmt.Sprintf("%s: size %d, want %d", p, sz, int64(expect)))
		}
		seed := seedOf(p)
		if *contentOf != "" {
			seed = seedOf(*contentOf + strconv.Itoa(i))
		}
		if !*allReplicas {
			buf, want := make([]byte, 8<<20), make([]byte, 8<<20)
			for off := int64(0); off < sz; off += int64(len(buf)) {
				m, err := c.ReadAt(p, buf, off)
				if err != nil && err != io.EOF {
					problems = append(problems, fmt.Sprintf("%s@%d: %v", p, off, err))
					break
				}
				fill(want[:m], seed, off)
				if !bytes.Equal(buf[:m], want[:m]) {
					problems = append(problems, fmt.Sprintf("%s@%d: content mismatch", p, off))
				}
				checked += int64(m)
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		locs, err := mc.GetChunkLocations(ctx, &pb.GetChunkLocationsRequest{Path: p, Count: 1 << 30})
		cancel()
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s: locations: %v", p, err))
			continue
		}
		cs := c.ChunkSize()
		for _, l := range locs.Chunks {
			chunks++
			if len(l.Replicas) < *wantReplicas {
				problems = append(problems, fmt.Sprintf("%s chunk %d: %d replicas, want %d", p, l.Index, len(l.Replicas), *wantReplicas))
			}
			chunkLen := min(cs, sz-l.Index*cs)
			want := make([]byte, chunkLen)
			fill(want, seed, l.Index*cs)
			for _, addr := range l.Replicas {
				got, err := readReplica(pool, addr, l, chunkLen)
				replicaReads++
				if err != nil {
					problems = append(problems, fmt.Sprintf("%s chunk %d on %s: %v", p, l.Index, addr, err))
					continue
				}
				if !bytes.Equal(got, want) {
					problems = append(problems, fmt.Sprintf("%s chunk %d on %s: content mismatch", p, l.Index, addr))
				}
				checked += int64(len(got))
			}
		}
	}
	for _, pr := range problems {
		fmt.Fprintln(os.Stderr, "PROBLEM:", pr)
	}
	result("verify", *cf.id, checked, start, time.Now(), fmt.Sprintf("chunks=%d;replica_reads=%d;problems=%d", chunks, replicaReads, len(problems)))
	if len(problems) > 0 {
		return fmt.Errorf("%d problems", len(problems))
	}
	return nil
}

func readReplica(pool *common.ConnPool, addr string, l *pb.ChunkLocation, n int64) ([]byte, error) {
	conn, err := pool.Get(addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stream, err := pb.NewChunkServerClient(conn).Read(ctx, &pb.ReadRequest{Handle: l.Handle, MinVersion: l.Version, Length: n})
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, n)
	for {
		f, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, f.Data...)
	}
	if int64(len(out)) != n {
		return nil, fmt.Errorf("replica returned %d bytes, want %d", len(out), n)
	}
	return out, nil
}

// ───────────────────────── Recovery watcher ─────────────────────────

// watch polls the master and prints one CSV row per poll:
// t_ns,servers,chunks,under_replicated,clones_in_flight,clones_completed,histogram
// It exits once the master sees exactly -expect-servers live chunkservers
// and no under-replicated chunks, printing "recovered,<t_ns>".
func watch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	masters := fs.String("masters", "localhost:7000", "master address")
	expect := fs.Int("expect-servers", 3, "live chunkservers after the failure")
	interval := fs.Duration("interval", 200*time.Millisecond, "poll interval")
	timeout := fs.Duration("timeout", 30*time.Minute, "give up after")
	fs.Parse(args)
	conn, err := common.Dial(strings.Split(*masters, ",")[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	mc := pb.NewMasterClient(conn)
	deadline := time.Now().Add(*timeout)
	var mu sync.Mutex
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		st, err := mc.ClusterStatus(ctx, &pb.Empty{})
		cancel()
		now := time.Now().UnixNano()
		if err == nil {
			mu.Lock()
			h := strings.Trim(strings.ReplaceAll(fmt.Sprint(st.ReplicaHistogram), " ", ";"), "[]")
			fmt.Printf("%d,%d,%d,%d,%d,%d,%s\n", now, len(st.Servers), st.NumChunks, st.UnderReplicated, st.ClonesInFlight, st.ClonesCompleted, h)
			mu.Unlock()
			if len(st.Servers) == *expect && st.UnderReplicated == 0 {
				fmt.Printf("recovered,%d\n", now)
				return nil
			}
		}
		time.Sleep(*interval)
	}
	return errors.New("timed out")
}
