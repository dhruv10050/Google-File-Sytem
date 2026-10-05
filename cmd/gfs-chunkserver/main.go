// Command gfs-chunkserver stores chunks on local disk and serves them.
package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/dhruv10050/Google-File-Sytem/internal/chunkserver"
	"github.com/dhruv10050/Google-File-Sytem/internal/common"
)

func main() {
	var (
		listen    = flag.String("listen", ":7001", "listen address")
		advertise = flag.String("advertise", "", "address other machines use to reach this server (default: hostname + listen port)")
		dir       = flag.String("dir", "./data/cs", "chunk storage directory")
		masters   = flag.String("masters", "localhost:7000", "comma-separated master addresses, primary first")
		fsync     = flag.Bool("fsync", false, "fsync chunk data on every write")
		bufMB     = flag.Int64("buffer-mb", 1024, "memory for pushed data awaiting a write")
		level     = flag.String("log", "info", "log level: debug, info, warn, error")
	)
	flag.Parse()
	log := common.NewLogger(*level, "chunkserver")
	s, err := chunkserver.New(chunkserver.Config{
		Dir: *dir, ListenAddr: *listen, Advertise: *advertise,
		Masters: strings.Split(*masters, ","), FsyncData: *fsync, BufferMB: *bufMB, Logger: log,
	})
	if err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	if err := s.Start(); err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	log.Info("chunkserver serving", "id", s.ID(), "addr", s.Addr())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down")
	s.Stop()
}
