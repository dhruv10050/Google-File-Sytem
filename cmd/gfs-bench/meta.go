package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dhruv10050/Google-File-Sytem/pkg/client"
)

// ───────────────────────── Metadata operations ─────────────────────────

// meta runs one phase of the metadata benchmark: -threads goroutines, each
// with its own client, perform -count operations of one kind. Files are
// <dir>/c<id>-t<thread>/f<i>, or with -shared all in <dir>/shared, where
// creations still run concurrently: they write-lock only their own name
// and read-lock the directory (§4.1). Phases run in order create, stat,
// delete over the same names.
func meta(args []string) error {
	fs := flag.NewFlagSet("meta", flag.ExitOnError)
	cf := commonFlags(fs)
	op := fs.String("op", "create", "create, stat or delete")
	dir := fs.String("dir", "/bench/meta", "base directory")
	shared := fs.Bool("shared", false, "all clients use one directory")
	threads := fs.Int("threads", 1, "concurrent clients in this process")
	count := fs.Int("count", 1000, "operations per thread")
	fs.Parse(args)

	type worker struct {
		c     *client.Client
		dir   string
		lat   []time.Duration
		errs  int
		first error
	}
	ws := make([]*worker, *threads)
	for t := range ws {
		c, err := newClient(*cf.masters, false)
		if err != nil {
			return err
		}
		defer c.Close()
		d := fmt.Sprintf("%s/c%d-t%d", *dir, *cf.id, t)
		if *shared {
			d = *dir + "/shared"
		}
		if *op == "create" {
			if err := c.Mkdir(d, true); err != nil && !strings.Contains(err.Error(), "exists") {
				return err
			}
		}
		ws[t] = &worker{c: c, dir: d, lat: make([]time.Duration, 0, *count)}
	}
	do := map[string]func(c *client.Client, p string) error{
		"create": func(c *client.Client, p string) error { return c.Create(p, 0) },
		"stat":   func(c *client.Client, p string) error { _, err := c.Stat(p); return err },
		"delete": func(c *client.Client, p string) error { return c.Delete(p) },
	}[*op]
	if do == nil {
		return fmt.Errorf("unknown -op %q", *op)
	}

	waitUntil(*cf.startAt)
	start := time.Now()
	var wg sync.WaitGroup
	for t, w := range ws {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < *count; i++ {
				p := fmt.Sprintf("%s/f%d-%d-%d", w.dir, *cf.id, t, i)
				t0 := time.Now()
				if err := do(w.c, p); err != nil {
					w.errs++
					if w.first == nil {
						w.first = fmt.Errorf("%s %s: %w", *op, p, err)
					}
					continue
				}
				w.lat = append(w.lat, time.Since(t0))
			}
		}()
	}
	wg.Wait()
	end := time.Now()
	var all []time.Duration
	errs := 0
	var first error
	for _, w := range ws {
		all = append(all, w.lat...)
		errs += w.errs
		if first == nil {
			first = w.first
		}
	}
	slices.Sort(all)
	pct := func(q float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[int(q*float64(len(all)-1))].Microseconds())
	}
	// "bytes" holds the number of successful operations.
	result("meta-"+*op, *cf.id, int64(len(all)), start, end,
		fmt.Sprintf("threads=%d;shared=%v;errors=%d;p50_us=%.0f;p99_us=%.0f", *threads, *shared, errs, pct(0.5), pct(0.99)))
	if first != nil {
		return fmt.Errorf("%d errors, first: %w", errs, first)
	}
	return nil
}

// ───────────────────────── Snapshots ─────────────────────────

// snapshotBench compares snapshots (§3.4) with copying the data through a
// client, measures the cost of the copy-on-write that the first write to a
// shared chunk pays, and checks that snapshots keep the original contents.
// It runs as a single process; the dataset (<ds>/f0..f<files-1>, written by
// `gfs-bench write`) must exist. Every measurement is one CSV line:
//
//	snapshot,<run>,host,<bytes>,start,end,what=file|dir|many;...
//	copy,<run>,host,<bytes>,start,end,what=file|dir
//	cow,<i>,host,<bytes>,start,end,phase=shared-first|unshared-first|rewrite;file=..;chunk=..
//	check,0,host,<bytes>,start,end,problems=N
func snapshotBench(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	cf := commonFlags(fs)
	ds := fs.String("ds", "/bench/ds", "dataset directory with files f0..")
	files := fs.Int("files", 8, "dataset files")
	runs := fs.Int("runs", 3, "repetitions of each snapshot and copy")
	cowFiles := fs.Int("cow-files", 3, "dataset files whose chunks get a copy-on-write write each")
	many := fs.Int("many", 1000, "files in the many-small-files snapshot (0 = skip)")
	fs.Parse(args)
	c, err := newClient(*cf.masters, *cf.avoidLocal)
	if err != nil {
		return err
	}
	defer c.Close()
	cs := c.ChunkSize()
	for _, d := range []string{"/bench/snap", "/bench/copy"} {
		if err := c.Mkdir(d, true); err != nil && !strings.Contains(err.Error(), "exists") {
			return err
		}
	}
	src := func(i int) string { return fmt.Sprintf("%s/f%d", *ds, i) }
	sizes := make([]int64, *files)
	var total int64
	for i := range sizes {
		if sizes[i], err = c.Size(src(i)); err != nil {
			return err
		}
		total += sizes[i]
	}
	chunksOf := func(n int64) int64 { return (n + cs - 1) / cs }
	var dsChunks int64
	for _, n := range sizes {
		dsChunks += chunksOf(n)
	}
	timed := func(f func() error) (time.Time, time.Time, error) {
		t0 := time.Now()
		err := f()
		return t0, time.Now(), err
	}
	fmt.Fprintf(os.Stderr, "dataset: %d files, %d bytes, %d chunks\n", *files, total, dsChunks)

	// Snapshot and copy, interleaved per run. The first file snapshot finds
	// the leases the dataset writers still hold and must revoke them.
	for r := 1; r <= *runs; r++ {
		s, e, err := timed(func() error { return c.Snapshot(src(0), fmt.Sprintf("/bench/snap/f0-r%d", r)) })
		if err != nil {
			return err
		}
		result("snapshot", r, sizes[0], s, e, fmt.Sprintf("what=file;files=1;chunks=%d", chunksOf(sizes[0])))
		s, e, err = timed(func() error { return c.Snapshot(*ds, fmt.Sprintf("/bench/snap/ds-r%d", r)) })
		if err != nil {
			return err
		}
		result("snapshot", r, total, s, e, fmt.Sprintf("what=dir;files=%d;chunks=%d", *files, dsChunks))

		s, e, err = timed(func() error { return copyFile(c, src(0), fmt.Sprintf("/bench/copy/f0-r%d", r)) })
		if err != nil {
			return err
		}
		result("copy", r, sizes[0], s, e, "what=file")
		d := fmt.Sprintf("/bench/copy/ds-r%d", r)
		if err := c.Mkdir(d, false); err != nil {
			return err
		}
		s, e, err = timed(func() error {
			var wg sync.WaitGroup
			errs := make([]error, *files)
			for i := 0; i < *files; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); errs[i] = copyFile(c, src(i), fmt.Sprintf("%s/f%d", d, i)) }()
			}
			wg.Wait()
			return errors.Join(errs...)
		})
		if err != nil {
			return err
		}
		result("copy", r, total, s, e, fmt.Sprintf("what=dir;files=%d", *files))
		for i := 0; i < *files; i++ { // keep disk use bounded
			c.Delete(fmt.Sprintf("%s/f%d", d, i))
		}
	}

	// Copy-on-write cost: the first write to each chunk of a shared file
	// waits for every replica to copy the chunk locally. Compare it with the
	// first write to an unshared chunk whose lease has lapsed (the client
	// copies were written minutes ago) and with rewriting a chunk under a
	// lease the client already holds.
	patch := make([]byte, 4096)
	fill(patch, seedOf("cow-patch"), 0)
	n := 0
	cow := func(p string, idx int64, phase string) error {
		s, e, err := timed(func() error { return c.Write(p, idx*cs, patch) })
		if err != nil {
			return fmt.Errorf("%s %s chunk %d: %w", phase, p, idx, err)
		}
		result("cow", n, int64(len(patch)), s, e, fmt.Sprintf("phase=%s;file=%s;chunk=%d", phase, p, idx))
		n++
		return nil
	}
	for i := 0; i < *cowFiles; i++ {
		for idx := int64(0); idx < chunksOf(sizes[i]); idx++ {
			if err := cow(src(i), idx, "shared-first"); err != nil {
				return err
			}
			if err := cow(src(i), idx, "rewrite"); err != nil {
				return err
			}
		}
	}
	for r := 1; r <= *runs; r++ {
		p := fmt.Sprintf("/bench/copy/f0-r%d", r)
		for idx := int64(0); idx < chunksOf(sizes[0]); idx++ {
			if err := cow(p, idx, "unshared-first"); err != nil {
				return err
			}
			if err := cow(p, idx, "rewrite"); err != nil {
				return err
			}
		}
	}

	// Many small files: each has one chunk with a live lease, so the first
	// snapshot revokes 1 lease per file; the next ones find none.
	if *many > 0 {
		m := "/bench/snap/many"
		if err := c.Mkdir(m, false); err != nil {
			return err
		}
		var wg sync.WaitGroup
		errs := make([]error, 16)
		for g := 0; g < 16; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := g; i < *many; i += 16 {
					p := fmt.Sprintf("%s/f%d", m, i)
					if err := c.Create(p, 0); err != nil {
						errs[g] = err
						return
					}
					if err := c.Write(p, 0, patch); err != nil {
						errs[g] = err
						return
					}
				}
			}()
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			return err
		}
		for r := 1; r <= *runs; r++ {
			s, e, err := timed(func() error { return c.Snapshot(m, fmt.Sprintf("/bench/snap/many-r%d", r)) })
			if err != nil {
				return err
			}
			leases := "revoked"
			if r > 1 {
				leases = "none"
			}
			result("snapshot", r, int64(*many*len(patch)), s, e, fmt.Sprintf("what=many;files=%d;chunks=%d;leases=%s", *many, *many, leases))
		}
	}

	// Contents. Sources that were patched must hold the patch at each chunk
	// start and the original bytes everywhere else (the copy-on-write copy
	// carried them over); every snapshot of them must still hold the
	// original bytes at the patched offsets. The unpatched snapshot files
	// are checked in full by `gfs-bench verify` afterwards.
	s := time.Now()
	var checked int64
	problems := 0
	report := func(format string, a ...any) {
		problems++
		fmt.Fprintf(os.Stderr, "PROBLEM: "+format+"\n", a...)
	}
	buf, want := make([]byte, 8<<20), make([]byte, 8<<20)
	for i := 0; i < *cowFiles; i++ {
		seed := seedOf(src(i))
		for off := int64(0); off < sizes[i]; off += int64(len(buf)) {
			k, err := c.ReadAt(src(i), buf, off)
			if err != nil && err != io.EOF {
				report("%s@%d: %v", src(i), off, err)
				break
			}
			fill(want[:k], seed, off)
			for cstart := (off + cs - 1) / cs * cs; cstart < off+int64(k); cstart += cs {
				copy(want[cstart-off:k], patch)
			}
			if !bytes.Equal(buf[:k], want[:k]) {
				report("%s@%d: content mismatch", src(i), off)
			}
			checked += int64(k)
		}
		var snaps []string
		for r := 1; r <= *runs; r++ {
			snaps = append(snaps, fmt.Sprintf("/bench/snap/ds-r%d/f%d", r, i))
			if i == 0 {
				snaps = append(snaps, fmt.Sprintf("/bench/snap/f0-r%d", r))
			}
		}
		for _, p := range snaps {
			for idx := int64(0); idx < chunksOf(sizes[i]); idx++ {
				k, err := c.ReadAt(p, buf[:len(patch)], idx*cs)
				if err != nil && err != io.EOF {
					report("%s chunk %d: %v", p, idx, err)
					continue
				}
				fill(want[:len(patch)], seed, idx*cs)
				if !bytes.Equal(buf[:k], want[:len(patch)]) {
					report("%s chunk %d: snapshot changed by a write to the source", p, idx)
				}
				checked += int64(k)
			}
		}
	}
	result("check", 0, checked, s, time.Now(), "problems="+strconv.Itoa(problems))
	if problems > 0 {
		return fmt.Errorf("%d problems", problems)
	}
	return nil
}

// copyFile copies src to a new file dst through the client in 8 MiB
// pieces, reading the next piece while the previous one is written.
func copyFile(c *client.Client, src, dst string) error {
	if err := c.Create(dst, 0); err != nil {
		return err
	}
	size, err := c.Size(src)
	if err != nil {
		return err
	}
	const piece = 8 << 20
	type part struct {
		off int64
		buf []byte
		err error
	}
	ch := make(chan part, 2)
	go func() {
		defer close(ch)
		for off := int64(0); off < size; off += piece {
			b := make([]byte, min(piece, size-off))
			k, err := c.ReadAt(src, b, off)
			if err == io.EOF && int64(k) == int64(len(b)) {
				err = nil
			}
			ch <- part{off, b[:k], err}
			if err != nil {
				return
			}
		}
	}()
	for p := range ch {
		if p.err != nil {
			return fmt.Errorf("read %s@%d: %w", src, p.off, p.err)
		}
		if err := c.Write(dst, p.off, p.buf); err != nil {
			return fmt.Errorf("write %s@%d: %w", dst, p.off, err)
		}
	}
	return nil
}
