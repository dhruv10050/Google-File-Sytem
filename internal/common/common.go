// Package common holds types and helpers shared by the master, chunkservers
// and the client library.
package common

import (
	"fmt"
	"path"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	DefaultChunkSize   int64 = 64 << 20 // §2.5
	BlockSize          int64 = 64 << 10 // checksum granularity, §5.2
	DefaultReplication       = 3

	// MaxMsgSize bounds every gRPC message; data frames are much smaller.
	MaxMsgSize = 32 << 20
	// FrameSize is the payload size of one PushData/Read frame. Small frames
	// let each hop start forwarding sooner (§3.2 pipelining).
	FrameSize = 256 << 10
)

// ChunkHandle is the immutable, globally unique 64-bit chunk id (§2.3).
type ChunkHandle uint64

func (h ChunkHandle) String() string { return fmt.Sprintf("%016x", uint64(h)) }

// CleanPath normalizes an absolute GFS path. It returns an error for
// relative or empty paths.
func CleanPath(p string) (string, error) {
	if p == "" || !strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("path must be absolute: %q", p)
	}
	return path.Clean(p), nil
}

// Parent returns the parent directory of a clean absolute path ("/" for "/x").
func Parent(p string) string { return path.Dir(p) }

// Ancestors returns every proper ancestor of p from the root down, e.g.
// "/a/b/c" -> ["/", "/a", "/a/b"].
func Ancestors(p string) []string {
	if p == "/" {
		return nil
	}
	var out []string
	for d := Parent(p); ; d = Parent(d) {
		out = append(out, d)
		if d == "/" {
			break
		}
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// Piece is the part of a file-level byte range that falls in one chunk.
type Piece struct {
	ChunkIndex  int64
	ChunkOffset int64 // offset within the chunk
	BufOffset   int64 // offset within the caller's buffer
	Length      int64
}

// SplitRange splits [offset, offset+length) at chunk boundaries.
func SplitRange(offset, length, chunkSize int64) []Piece {
	var out []Piece
	var done int64
	for done < length {
		pos := offset + done
		idx := pos / chunkSize
		off := pos % chunkSize
		n := min(chunkSize-off, length-done)
		out = append(out, Piece{ChunkIndex: idx, ChunkOffset: off, BufOffset: done, Length: n})
		done += n
	}
	return out
}

// Dial opens a gRPC client connection configured for GFS traffic.
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMsgSize),
			grpc.MaxCallSendMsgSize(MaxMsgSize),
		),
	)
}

// ServerOptions are the gRPC server options shared by master and chunkserver.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(MaxMsgSize),
		grpc.MaxSendMsgSize(MaxMsgSize),
	}
}

// UnixNanoToTime converts a protobuf timestamp field.
func UnixNanoToTime(ns int64) time.Time { return time.Unix(0, ns) }
