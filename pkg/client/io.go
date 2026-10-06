package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

// ───────────────────────── Leases ─────────────────────────

// leaseHolder returns the primary and secondaries for chunk idx of path,
// asking the master to allocate the chunk if it does not exist yet. The
// answer is cached until shortly before the lease expires (§3.1).
func (c *Client) leaseHolder(path string, idx int64) (*pb.GetLeaseHolderResponse, error) {
	k := leaseKey{path, idx}
	c.mu.Lock()
	lh := c.leases[k]
	c.mu.Unlock()
	if lh != nil && time.Until(time.Unix(0, lh.LeaseExpiryUnixNanos)) > 2*time.Second {
		return lh, nil
	}
	var resp *pb.GetLeaseHolderResponse
	err := c.writeMaster(func(ctx context.Context, m pb.MasterClient) (err error) {
		resp, err = m.GetLeaseHolder(ctx, &pb.GetLeaseHolderRequest{Path: path, Index: idx, Allocate: true})
		return err
	})
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.leases[k] = resp
	if fe := c.files[path]; fe != nil {
		fe.chunks[idx] = resp.Chunk
		fe.numChunks = max(fe.numChunks, idx+1)
	}
	c.mu.Unlock()
	return resp, nil
}

func (c *Client) dropLease(path string, idx int64) {
	c.mu.Lock()
	delete(c.leases, leaseKey{path, idx})
	if fe := c.files[path]; fe != nil {
		delete(fe.chunks, idx)
	}
	c.mu.Unlock()
}

// ───────────────────────── Write ─────────────────────────

// Write stores data at offset in path. A write that spans chunks is split
// into one mutation per chunk (§3.3), so a concurrent overlapping writer may
// see the pieces interleave: the region is consistent but undefined.
func (c *Client) Write(path string, offset int64, data []byte) error {
	if offset < 0 {
		return errors.New("negative offset")
	}
	for _, p := range common.SplitRange(offset, int64(len(data)), c.chunkSize) {
		if err := c.writeChunk(path, p.ChunkIndex, p.ChunkOffset, data[p.BufOffset:p.BufOffset+p.Length]); err != nil {
			return fmt.Errorf("write %s chunk %d: %w", path, p.ChunkIndex, err)
		}
	}
	return nil
}

func (c *Client) writeChunk(path string, idx, off int64, data []byte) error {
	var err error
	backoff := 50 * time.Millisecond
	for attempt := 0; attempt < c.opt.Retries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff = min(2*backoff, 2*time.Second)
		}
		var lh *pb.GetLeaseHolderResponse
		lh, err = c.leaseHolder(path, idx)
		if err != nil {
			if errors.Is(err, ErrNotFound) || status.Code(err) == codes.InvalidArgument || status.Code(err) == codes.PermissionDenied {
				return err
			}
			continue
		}
		err = c.mutate(lh, off, data)
		if err == nil {
			return nil
		}
		// Any failure may mean the lease moved or a replica died; ask the
		// master again. Replicas that applied the mutation before the
		// failure hold the same bytes after the retry, so retrying is safe
		// for writes at a fixed offset.
		c.dropLease(path, idx)
	}
	return err
}

// mutate pushes data along the replica chain (§3.2), then asks the primary
// to commit it. The primary picks the order and forwards it to the
// secondaries (§3.1).
func (c *Client) mutate(lh *pb.GetLeaseHolderResponse, off int64, data []byte) error {
	all := append([]string{lh.Primary}, lh.Secondaries...)
	chain := c.chainOrder(all)
	id := newDataID()
	if err := c.push(chain, id, data); err != nil {
		return err
	}
	cli, err := c.cs(lh.Primary)
	if err != nil {
		return err
	}
	ctx, cancel := c.ctx()
	defer cancel()
	_, err = cli.Write(ctx, &pb.WriteRequest{
		Handle: lh.Chunk.Handle, Version: lh.Chunk.Version, Offset: off,
		DataId: id, Secondaries: lh.Secondaries,
	})
	return err
}

// ───────────────────────── Record append ─────────────────────────

// RecordAppend appends data to path as a single record at an offset chosen
// by GFS (§3.3) and returns that offset. Many clients may append to the
// same file concurrently without any locking of their own.
//
// The record is written atomically, at least once: if an attempt fails part
// way, it is retried and the file may then contain the record more than
// once, or a fragment of it on some replicas. Readers must therefore be able
// to recognise records, e.g. by a checksum and a unique id per record. A
// record may be at most a quarter of the chunk size.
func (c *Client) RecordAppend(path string, data []byte) (int64, error) {
	if int64(len(data)) > c.chunkSize/4 {
		return 0, ErrRecordTooLarge
	}
	c.mu.Lock()
	idx, ok := c.tail[path]
	c.mu.Unlock()
	if !ok {
		fe, err := c.fileInfo(path, 0, true)
		if err != nil {
			return 0, err
		}
		idx = max(fe.numChunks-1, 0)
	}
	var err error
	backoff := 50 * time.Millisecond
	for attempt := 0; attempt < c.opt.Retries; {
		var lh *pb.GetLeaseHolderResponse
		if lh, err = c.leaseHolder(path, idx); err == nil {
			var resp *pb.RecordAppendResponse
			if resp, err = c.appendOnce(lh, data); err == nil {
				if !resp.ChunkFull {
					c.mu.Lock()
					c.tail[path] = idx
					c.mu.Unlock()
					return idx*c.chunkSize + resp.Offset, nil
				}
				// The chunk is full and has been padded. Move on to the next
				// chunk, or further if other appenders already have.
				next := idx + 1
				if fe, ferr := c.fileInfo(path, next, true); ferr == nil {
					next = max(next, fe.numChunks-1)
				}
				idx = next
				continue
			}
			c.dropLease(path, idx)
		}
		if errors.Is(err, ErrNotFound) || status.Code(err) == codes.InvalidArgument || status.Code(err) == codes.PermissionDenied || status.Code(err) == codes.NotFound {
			return 0, err
		}
		attempt++
		time.Sleep(backoff)
		backoff = min(2*backoff, 2*time.Second)
	}
	return 0, fmt.Errorf("record append to %s: %w", path, err)
}

func (c *Client) appendOnce(lh *pb.GetLeaseHolderResponse, data []byte) (*pb.RecordAppendResponse, error) {
	id := newDataID()
	if err := c.push(c.chainOrder(append([]string{lh.Primary}, lh.Secondaries...)), id, data); err != nil {
		return nil, err
	}
	cli, err := c.cs(lh.Primary)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.ctx()
	defer cancel()
	return cli.RecordAppend(ctx, &pb.RecordAppendRequest{
		Handle: lh.Chunk.Handle, Version: lh.Chunk.Version, DataId: id, Secondaries: lh.Secondaries,
	})
}

// push streams data to chain[0], which forwards it to the rest of the chain
// while still receiving, so the transfer is pipelined.
func (c *Client) push(chain []string, id string, data []byte) error {
	cli, err := c.cs(chain[0])
	if err != nil {
		return err
	}
	ctx, cancel := c.ctx()
	defer cancel()
	stream, err := cli.PushData(ctx)
	if err != nil {
		return err
	}
	first := true
	for sent := 0; sent < len(data) || first; {
		n := min(len(data)-sent, common.FrameSize)
		f := &pb.PushDataFrame{Payload: data[sent : sent+n]}
		if first {
			f.DataId, f.Chain, first = id, chain[1:], false
		}
		if err := stream.Send(f); err != nil {
			break // the real error comes from CloseAndRecv
		}
		sent += n
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp.Bytes != int64(len(data)) {
		return fmt.Errorf("push: chunkserver got %d of %d bytes", resp.Bytes, len(data))
	}
	return nil
}

// ───────────────────────── Read ─────────────────────────

// ReadAt reads len(buf) bytes of path starting at offset. Like io.ReaderAt
// it returns io.EOF when fewer bytes exist. Regions inside the file that
// were never written read as zeros.
func (c *Client) ReadAt(path string, buf []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("negative offset")
	}
	n := 0
	for _, p := range common.SplitRange(offset, int64(len(buf)), c.chunkSize) {
		got, last, err := c.readChunk(path, p.ChunkIndex, p.ChunkOffset, buf[p.BufOffset:p.BufOffset+p.Length])
		if err != nil {
			return n, err
		}
		if got < int(p.Length) {
			if last {
				return n + got, io.EOF
			}
			// A chunk before the last one is shorter than chunk size:
			// the tail was never written, so it reads as zeros.
			clear(buf[p.BufOffset+int64(got) : p.BufOffset+p.Length])
		}
		n += int(p.Length)
	}
	return n, nil
}

// readChunk reads from one replica, trying others on failure (§5.2: a
// replica that fails checksum verification returns an error and the client
// reads elsewhere). It reports whether idx is the file's last chunk.
func (c *Client) readChunk(path string, idx, off int64, dst []byte) (int, bool, error) {
	var lastErr error
	// Retries refresh locations from the master. A shadow master may not
	// have heard about a new chunk's replicas yet (§5.1.3), so back off a
	// little between attempts.
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 1 {
			time.Sleep(time.Duration(attempt) * 150 * time.Millisecond)
		}
		fe, err := c.fileInfo(path, idx, attempt > 0)
		if err != nil {
			return 0, false, err
		}
		c.mu.Lock()
		loc, num := fe.chunks[idx], fe.numChunks
		c.mu.Unlock()
		if idx >= num {
			return 0, true, nil
		}
		if loc == nil {
			lastErr = fmt.Errorf("no location for chunk %d", idx)
			continue
		}
		for _, addr := range c.orderReplicas(loc.Replicas) {
			n, err := c.readReplica(addr, loc, off, dst)
			if err == nil {
				return n, idx == num-1, nil
			}
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no replicas")
	}
	return 0, false, fmt.Errorf("read %s chunk %d: all replicas failed: %w", path, idx, lastErr)
}

func (c *Client) readReplica(addr string, loc *pb.ChunkLocation, off int64, dst []byte) (int, error) {
	cli, err := c.cs(addr)
	if err != nil {
		return 0, err
	}
	ctx, cancel := c.ctx()
	defer cancel()
	stream, err := cli.Read(ctx, &pb.ReadRequest{
		Handle: loc.Handle, MinVersion: loc.Version, Offset: off, Length: int64(len(dst)),
	})
	if err != nil {
		return 0, err
	}
	n := 0
	for {
		f, err := stream.Recv()
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return 0, err
		}
		if n+len(f.Data) > len(dst) {
			return 0, fmt.Errorf("replica %s sent too much data", addr)
		}
		n += copy(dst[n:], f.Data)
	}
}

// ───────────────────────── Streaming helpers ─────────────────────────

// WriteFrom copies r into path starting at offset, in blocks of blockSize
// (default: one chunk). It returns the number of bytes written.
func (c *Client) WriteFrom(path string, offset int64, r io.Reader, blockSize int) (int64, error) {
	if blockSize <= 0 {
		blockSize = int(c.chunkSize)
	}
	buf := make([]byte, blockSize)
	var total int64
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if werr := c.Write(path, offset+total, buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

// ReadTo copies the whole of path into w.
func (c *Client) ReadTo(path string, w io.Writer, blockSize int) (int64, error) {
	if blockSize <= 0 {
		blockSize = int(c.chunkSize)
	}
	buf := make([]byte, blockSize)
	var total int64
	for {
		n, err := c.ReadAt(path, buf, total)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
		}
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}
