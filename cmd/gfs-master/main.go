// Command gfs-master runs the GFS master, or a read-only shadow master that
// follows the primary's operation log (--shadow --primary=host:port).
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dhruv10050/Google-File-Sytem/internal/common"
	"github.com/dhruv10050/Google-File-Sytem/internal/master"
)

func main() {
	var (
		listen    = flag.String("listen", ":7000", "listen address")
		dir       = flag.String("dir", "./data/master", "directory for the operation log and checkpoints")
		chunkMB   = flag.Int64("chunk-mb", common.DefaultChunkSize>>20, "chunk size in MiB")
		repl      = flag.Int("replication", common.DefaultReplication, "default replication factor")
		lease     = flag.Duration("lease", 60*time.Second, "lease duration")
		hb        = flag.Duration("heartbeat", time.Second, "chunkserver heartbeat interval")
		deadAfter = flag.Duration("dead-after", 0, "declare a chunkserver dead after this long without heartbeats (default 5x heartbeat)")
		clones    = flag.Int("max-clones-per-server", 4, "concurrent re-replications per chunkserver")
		clonesAll = flag.Int("max-clones", 0, "concurrent re-replications cluster-wide (0 = unlimited)")
		trash     = flag.Duration("trash-retention", 72*time.Hour, "keep deleted files under /.deleted this long before garbage collection (0 = delete at once)")
		gcEvery   = flag.Duration("gc-interval", time.Minute, "how often to garbage-collect deleted files")
		ckptEvery = flag.Int("checkpoint-every", 50000, "checkpoint after this many log records")
		shadow    = flag.Bool("shadow", false, "run as a read-only shadow master")
		primary   = flag.String("primary", "", "primary master address (shadow mode)")
		level     = flag.String("log", "info", "log level: debug, info, warn, error")
	)
	flag.Parse()
	if *shadow && *primary == "" {
		fmt.Fprintln(os.Stderr, "--shadow requires --primary")
		os.Exit(2)
	}
	log := common.NewLogger(*level, "master")
	if *trash == 0 {
		*trash = -1 // Config: 0 means the default
	}
	m, err := master.New(master.Config{
		ListenAddr: *listen, Dir: *dir, Logger: log,
		ChunkSize: *chunkMB << 20, DefaultReplication: int32(*repl),
		LeaseDuration: *lease, HeartbeatInterval: *hb, DeadAfter: *deadAfter,
		MaxClonesPerServer: *clones, MaxClonesCluster: *clonesAll,
		TrashRetention: *trash, GCInterval: *gcEvery,
		CheckpointEveryRecords: *ckptEvery, Shadow: *shadow, PrimaryAddr: *primary,
	})
	if err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	if err := m.Start(); err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	log.Info("master serving", "addr", m.Addr(), "shadow", *shadow)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down")
	m.Stop()
}
