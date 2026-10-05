package master

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/dhruv10050/Google-File-Sytem/api/gfspb"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

// A shadow master (§5.1.3) tails the primary's operation log and applies
// every record with the same State.Apply the primary used, so it is a
// replica of the metadata that may lag slightly. It also writes the records
// to its own log and checkpoints, so it holds a full copy of the log.
// Chunkservers heartbeat to it too, so it knows replica locations, but it
// never issues instructions; it serves read-only RPCs.

// StreamLog streams durable log records starting at from_lsn (primary only).
func (m *Master) StreamLog(req *pb.StreamLogRequest, stream pb.Master_StreamLogServer) error {
	if m.cfg.Shadow {
		return status.Error(codes.FailedPrecondition, "shadow master does not serve its log")
	}
	from := max(req.FromLsn, 1)
	for {
		recs, err := m.oplog.TailFrom(stream.Context(), from, 1024)
		if errors.Is(err, ErrTooOld) {
			return status.Error(codes.OutOfRange, err.Error())
		}
		if err != nil {
			return err
		}
		for _, r := range recs {
			if err := stream.Send(r); err != nil {
				return err
			}
			from = r.Lsn + 1
		}
	}
}

// FetchCheckpoint returns a consistent snapshot of the current state, used
// by a shadow that has fallen behind the retained log tail.
func (m *Master) FetchCheckpoint(ctx context.Context, _ *pb.Empty) (*pb.Checkpoint, error) {
	if m.cfg.Shadow {
		return nil, status.Error(codes.FailedPrecondition, "shadow master")
	}
	m.mu.RLock()
	cp := m.st.ToCheckpoint()
	m.mu.RUnlock()
	return cp, m.readDone(cp.Lsn)
}

func (m *Master) followPrimary() {
	conn, err := common.Dial(m.cfg.PrimaryAddr)
	if err != nil {
		m.log.Error("dial primary", "err", err)
		return
	}
	defer conn.Close()
	cli := pb.NewMasterClient(conn)
	for m.ctx.Err() == nil {
		err := m.tail(cli)
		if status.Code(err) == codes.OutOfRange {
			m.log.Info("behind the primary's retained log; fetching checkpoint")
			if err := m.installCheckpoint(cli); err != nil {
				m.log.Warn("fetch checkpoint", "err", err)
			}
			continue
		}
		if m.ctx.Err() == nil {
			m.log.Debug("log stream interrupted; retrying", "err", err)
			select {
			case <-m.ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

func (m *Master) tail(cli pb.MasterClient) error {
	m.mu.RLock()
	from := m.st.LSN + 1
	m.mu.RUnlock()
	stream, err := cli.StreamLog(m.ctx, &pb.StreamLogRequest{FromLsn: from})
	if err != nil {
		return err
	}
	for {
		rec, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		m.mu.Lock()
		if rec.Lsn <= m.st.LSN {
			m.mu.Unlock()
			continue
		}
		orphaned, err := m.st.Apply(rec)
		if err != nil {
			m.mu.Unlock()
			m.log.Error("shadow could not apply record; resyncing from checkpoint", "lsn", rec.Lsn, "err", err)
			return status.Error(codes.OutOfRange, "diverged")
		}
		m.oplog.Append(rec)
		m.afterApplyLocked(rec, orphaned)
		m.mu.Unlock()
		if m.sinceCkpt.Add(1) >= int64(m.cfg.CheckpointEveryRecords) {
			m.maybeCheckpoint()
		}
	}
}

func (m *Master) installCheckpoint(cli pb.MasterClient) error {
	ctx, cancel := context.WithTimeout(m.ctx, time.Minute)
	defer cancel()
	cp, err := cli.FetchCheckpoint(ctx, &pb.Empty{})
	if err != nil {
		return err
	}
	st, err := StateFromCheckpoint(cp)
	if err != nil {
		return err
	}
	if err := WriteCheckpoint(m.ckptDir(), cp); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.oplog.Rotate()
	m.st = st
	m.nextHandle = st.NextHandle
	for h := range m.soft {
		if st.Chunks[h] == nil {
			delete(m.soft, h)
		}
	}
	for h := range st.Chunks {
		if m.soft[h] == nil {
			m.soft[h] = newChunkSoft()
		}
	}
	m.sinceCkpt.Store(0)
	m.log.Info("installed checkpoint from primary", "lsn", cp.Lsn)
	return nil
}
