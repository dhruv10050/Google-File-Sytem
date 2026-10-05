package master

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
)

// Checkpoint files (§2.6.3): <dir>/ckpt-<lsn:020d>.pb
//
//	| magic u32 | format u32 | payload_len u64 | payload (Checkpoint) | crc32c(payload) u32 |
//
// Written to a .tmp file, fsynced, renamed into place, then the directory is
// fsynced. Recovery uses the newest file that validates and skips any
// incomplete one.

const (
	ckptMagic  uint32 = 0x47465343 // "GFSC"
	ckptFormat uint32 = 1
)

func ckptName(lsn uint64) string { return fmt.Sprintf("ckpt-%020d.pb", lsn) }

func WriteCheckpoint(dir string, cp *pb.Checkpoint) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	payload, err := proto.Marshal(cp)
	if err != nil {
		return err
	}
	b := make([]byte, 16, 16+len(payload)+4)
	binary.LittleEndian.PutUint32(b[0:], ckptMagic)
	binary.LittleEndian.PutUint32(b[4:], ckptFormat)
	binary.LittleEndian.PutUint64(b[8:], uint64(len(payload)))
	b = append(b, payload...)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(payload, crcTable))

	final := filepath.Join(dir, ckptName(cp.Lsn))
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	syncDir(dir)
	return nil
}

func readCheckpoint(path string) (*pb.Checkpoint, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) < 20 || binary.LittleEndian.Uint32(b) != ckptMagic || binary.LittleEndian.Uint32(b[4:]) != ckptFormat {
		return nil, errors.New("bad checkpoint header")
	}
	n := binary.LittleEndian.Uint64(b[8:])
	if uint64(len(b)) != 16+n+4 {
		return nil, errors.New("checkpoint length mismatch")
	}
	payload := b[16 : 16+n]
	if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(b[16+n:]) {
		return nil, errors.New("checkpoint checksum mismatch")
	}
	cp := &pb.Checkpoint{}
	return cp, proto.Unmarshal(payload, cp)
}

// listCheckpoints returns checkpoint LSNs, newest first.
func listCheckpoints(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var lsns []uint64
	for _, e := range ents {
		name := e.Name()
		if strings.HasSuffix(name, ".tmp") {
			os.Remove(filepath.Join(dir, name)) // incomplete checkpoint from a crash
			continue
		}
		if !strings.HasPrefix(name, "ckpt-") || !strings.HasSuffix(name, ".pb") {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "ckpt-"), ".pb"), 10, 64)
		if err == nil {
			lsns = append(lsns, n)
		}
	}
	sort.Slice(lsns, func(i, j int) bool { return lsns[i] > lsns[j] })
	return lsns, nil
}

// LoadLatestCheckpoint returns the newest valid checkpoint, or nil if none.
func LoadLatestCheckpoint(dir string) (*pb.Checkpoint, error) {
	lsns, err := listCheckpoints(dir)
	if err != nil {
		return nil, err
	}
	for _, lsn := range lsns {
		cp, err := readCheckpoint(filepath.Join(dir, ckptName(lsn)))
		if err == nil {
			return cp, nil
		}
	}
	return nil, nil
}

// PruneCheckpoints keeps the newest `keep` checkpoints and returns the LSN
// of the oldest one kept (log records at or below it are no longer needed).
func PruneCheckpoints(dir string, keep int) (oldestKept uint64, err error) {
	lsns, err := listCheckpoints(dir)
	if err != nil || len(lsns) == 0 {
		return 0, err
	}
	for i, lsn := range lsns {
		if i >= keep {
			if err := os.Remove(filepath.Join(dir, ckptName(lsn))); err != nil {
				return 0, err
			}
		}
	}
	return lsns[min(keep, len(lsns))-1], nil
}
