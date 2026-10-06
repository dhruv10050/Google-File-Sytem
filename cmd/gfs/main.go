// Command gfs is the command-line client.
//
//	gfs [--masters host:port,...] <command> [args]
//
// Masters default to $GFS_MASTERS, then localhost:7000.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dhruv10050/Google-File-Sytem/pkg/client"
)

const usage = `usage: gfs [--masters host:port,...] <command> [args]

commands:
  mkdir [-p] <dir>                 create a directory (-p: and its parents)
  create [-r N] <file>             create an empty file with replication N
  ls [path]                        list a directory (default /)
  stat <path>                      show file or directory metadata
  put [-r N] <local> <file>        copy a local file (or - for stdin) into GFS
  get <file> [local]               copy a GFS file to a local file (default stdout)
  write <file> <offset> [local]    write a local file (or stdin) at offset
  append <file> [local]            append a local file (or stdin) as one record; prints its offset
  read <file> <offset> <length>    read a byte range to stdout
  rm <path>                        delete a file or an empty directory
  status                           show cluster status
`

func main() {
	masters := flag.String("masters", envOr("GFS_MASTERS", "localhost:7000"), "comma-separated master addresses, primary first")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	c, err := client.New(client.Options{Masters: strings.Split(*masters, ",")})
	if err != nil {
		fail(err)
	}
	defer c.Close()
	if err := run(c, flag.Arg(0), flag.Args()[1:]); err != nil {
		fail(err)
	}
}

func run(c *client.Client, cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	switch cmd {
	case "mkdir":
		p := fs.Bool("p", false, "create parents")
		fs.Parse(args)
		if err := need(fs, 1); err != nil {
			return err
		}
		return c.Mkdir(fs.Arg(0), *p)

	case "create":
		r := fs.Int("r", 0, "replication (0 = cluster default)")
		fs.Parse(args)
		if err := need(fs, 1); err != nil {
			return err
		}
		return c.Create(fs.Arg(0), *r)

	case "ls":
		fs.Parse(args)
		path := "/"
		if fs.NArg() > 0 {
			path = fs.Arg(0)
		}
		ents, err := c.List(path)
		if err != nil {
			return err
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		for _, e := range ents {
			if e.IsDir {
				fmt.Fprintf(w, "d\t%s/\t\n", e.Name)
			} else {
				fmt.Fprintf(w, "-\t%s\t%d chunks\n", e.Name, e.NumChunks)
			}
		}
		return w.Flush()

	case "stat":
		fs.Parse(args)
		if err := need(fs, 1); err != nil {
			return err
		}
		st, err := c.Stat(fs.Arg(0))
		if err != nil {
			return err
		}
		if st.IsDir {
			fmt.Println("directory")
			return nil
		}
		size, err := c.Size(fs.Arg(0))
		if err != nil {
			return err
		}
		fmt.Printf("file\nsize: %d\nchunks: %d\nreplication: %d\ncreated: %s\n",
			size, st.NumChunks, st.Replication, time.Unix(0, st.CtimeUnixNanos).Format(time.RFC3339))
		return nil

	case "put":
		r := fs.Int("r", 0, "replication (0 = cluster default)")
		fs.Parse(args)
		if err := need(fs, 2); err != nil {
			return err
		}
		in, closeIn, err := openIn(fs.Arg(0))
		if err != nil {
			return err
		}
		defer closeIn()
		if err := c.Create(fs.Arg(1), *r); err != nil {
			return err
		}
		start := time.Now()
		n, err := c.WriteFrom(fs.Arg(1), 0, in, 0)
		if err != nil {
			return err
		}
		report(n, start)
		return nil

	case "get":
		fs.Parse(args)
		if err := need(fs, 1); err != nil {
			return err
		}
		out := io.Writer(os.Stdout)
		if fs.NArg() > 1 && fs.Arg(1) != "-" {
			f, err := os.Create(fs.Arg(1))
			if err != nil {
				return err
			}
			defer f.Close()
			out = f
		}
		start := time.Now()
		n, err := c.ReadTo(fs.Arg(0), out, 0)
		if err != nil {
			return err
		}
		if out != os.Stdout {
			report(n, start)
		}
		return nil

	case "write":
		fs.Parse(args)
		if err := need(fs, 2); err != nil {
			return err
		}
		off, err := strconv.ParseInt(fs.Arg(1), 10, 64)
		if err != nil {
			return err
		}
		src := "-"
		if fs.NArg() > 2 {
			src = fs.Arg(2)
		}
		in, closeIn, err := openIn(src)
		if err != nil {
			return err
		}
		defer closeIn()
		_, err = c.WriteFrom(fs.Arg(0), off, in, 0)
		return err

	case "append":
		fs.Parse(args)
		if err := need(fs, 1); err != nil {
			return err
		}
		src := "-"
		if fs.NArg() > 1 {
			src = fs.Arg(1)
		}
		in, closeIn, err := openIn(src)
		if err != nil {
			return err
		}
		defer closeIn()
		rec, err := io.ReadAll(io.LimitReader(in, c.ChunkSize()/4+1))
		if err != nil {
			return err
		}
		off, err := c.RecordAppend(fs.Arg(0), rec)
		if err != nil {
			return err
		}
		fmt.Println(off)
		return nil

	case "read":
		fs.Parse(args)
		if err := need(fs, 3); err != nil {
			return err
		}
		off, err1 := strconv.ParseInt(fs.Arg(1), 10, 64)
		n, err2 := strconv.ParseInt(fs.Arg(2), 10, 64)
		if err := errors.Join(err1, err2); err != nil {
			return err
		}
		buf := make([]byte, n)
		got, err := c.ReadAt(fs.Arg(0), buf, off)
		if err != nil && err != io.EOF {
			return err
		}
		_, err = os.Stdout.Write(buf[:got])
		return err

	case "rm":
		fs.Parse(args)
		if err := need(fs, 1); err != nil {
			return err
		}
		return c.Delete(fs.Arg(0))

	case "status":
		st, err := c.ClusterStatus()
		if err != nil {
			return err
		}
		role := "primary"
		if st.IsShadow {
			role = "shadow"
		}
		fmt.Printf("master: %s (applied lsn %d)\nfiles: %d  chunks: %d  under-replicated: %d\nclones: %d in flight, %d completed\nreplica histogram (index = live replicas): %v\n\n",
			role, st.AppliedLsn, st.NumFiles, st.NumChunks, st.UnderReplicated, st.ClonesInFlight, st.ClonesCompleted, st.ReplicaHistogram)
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "SERVER\tADDRESS\tCHUNKS\tUSED\tFREE\tLAST HB")
		for _, s := range st.Servers {
			fmt.Fprintf(w, "%.8s\t%s\t%d\t%s\t%s\t%dms\n", s.ServerId, s.Address, s.NumChunks, human(s.DiskUsed), human(s.DiskFree), s.LastHeartbeatMsAgo)
		}
		return w.Flush()
	}
	flag.Usage()
	os.Exit(2)
	return nil
}

func need(fs *flag.FlagSet, n int) error {
	if fs.NArg() < n {
		return fmt.Errorf("%s: expected %d argument(s)\n\n%s", fs.Name(), n, usage)
	}
	return nil
}

func openIn(name string) (io.Reader, func(), error) {
	if name == "-" {
		return os.Stdin, func() {}, nil
	}
	f, err := os.Open(name)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { f.Close() }, nil
}

func report(n int64, start time.Time) {
	d := time.Since(start)
	fmt.Fprintf(os.Stderr, "%s in %s (%.1f MB/s)\n", human(n), d.Round(time.Millisecond), float64(n)/1e6/d.Seconds())
}

func human(b int64) string {
	const u = 1024
	if b < u {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "gfs:", err)
	os.Exit(1)
}
