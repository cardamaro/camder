package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rwcarlsen/goexif/exif"
)

var (
	flagSrcDir         = flag.String("src", "", "source directory")
	flagDestDir        = flag.String("dest", "", "dest directory")
	flagExclude        = flag.String("exclude", "", "file name exclusion")
	flagCleanupErrors  = flag.Bool("cleanup", false, "clean up dest files when hashes don't match")
	flagWorkers        = flag.Int("workers", 3, "number of workers")
	flagTick           = flag.Duration("tick", 1*time.Second, "ticker period")
	flagUpdate         = flag.Duration("update", 1*time.Second, "update period")
	flagAddRandomDelay = flag.Bool("add-delay", true, "add random delay if dry run")
	flagOverwrite      = flag.Bool("overwrite", false, "overwrite files if they exist")
	flagDryRun         = flag.Bool("dry-run", false, "dry run mode")
)

const (
	destDateFormat = "2006/01"
)

func skipFile(d fs.DirEntry) bool {
	if d.IsDir() {
		return true
	}
	if !strings.HasSuffix(strings.ToUpper(d.Name()), ".JPG") {
		return true
	}
	if strings.Contains(d.Name(), *flagExclude) {
		return true
	}

	return false
}

func hash(file io.ReadSeeker) (string, int64, error) {
	if _, err := file.Seek(0, 0); err != nil {
		return "", 0, err
	}

	h := md5.New()
	n, err := io.Copy(h, file)
	if err != nil {
		return "", 0, err
	}

	hs := fmt.Sprintf("%x", h.Sum(nil))

	return hs, n, nil
}

func extractDate(fp string) (time.Time, string, int64, error) {
	file, err := os.Open(fp)
	if err != nil {
		return time.Time{}, "", 0, fmt.Errorf("failed to open %s: %v", fp, err)
	}
	defer file.Close()

	x, err := exif.Decode(file)
	if err != nil {
		return time.Time{}, "", 0, fmt.Errorf("failed to decode EXIF in %s: %v", fp, err)
	}

	tm, err := x.DateTime()
	if err != nil {
		return time.Time{}, "", 0, fmt.Errorf("failed to extract date taken in %s: %v", fp, err)
	}

	hs, n, err := hash(file)
	if err != nil {
		return time.Time{}, "", 0, fmt.Errorf("failed to hash %s: %v", fp, err)
	}

	return tm, hs, n, nil
}

type Entry struct {
	Src   string
	Dest  string
	Hash  string
	Bytes int64
}

func replOne(e Entry) (int64, error) {
	if *flagDryRun {
		if *flagAddRandomDelay {
			time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)
		}
		return e.Bytes, nil
	}

	if err := maybeMkdirs(filepath.Dir(e.Dest)); err != nil {
		return 0, err
	}

	if _, err := os.Stat(e.Dest); !os.IsNotExist(err) {
		if !*flagOverwrite {
			return 0, fmt.Errorf("path exists and overwrite is not set: %s", e.Dest)
		}
	}

	// copy the file
	dst, err := os.Create(e.Dest)
	if err != nil {
		return 0, fmt.Errorf("failed to create %s: %v", e.Dest, err)
	}

	src, err := os.Open(e.Src)
	if err != nil {
		return 0, fmt.Errorf("failed to open src %s: %v", e.Src, err)
	}

	n, err := io.Copy(dst, src)
	if err != nil {
		return 0, fmt.Errorf("failed to copy %s -> %s: %v", e.Src, e.Dest, err)
	}

	if n != e.Bytes {
		return 0, fmt.Errorf("failed to copy, bytes don't match %s [%d] -> %s [%d]", e.Src, e.Bytes, e.Dest, n)
	}

	// verify the checksum
	hs, _, err := hash(dst)
	if err != nil {
		return 0, fmt.Errorf("failed to hash %s: %v", e.Src, err)
	}

	if hs != e.Hash {
		if *flagCleanupErrors {
			if err := os.Remove(e.Dest); err != nil {
				log.Printf("failed to remove %s: %v", e.Dest, err)
			}
		}
		return 0, fmt.Errorf("failed to match hash after copy %s [%s] -> %s [%s]", e.Src, e.Hash, e.Dest, hs)
	}

	return n, nil
}

func status(m []Entry, res chan result) {
	var (
		done   int
		err    int
		total  int64
		copied int64
		fps    int
		bps    int64
		ld     int
		lc     int64
	)

	for _, e := range m {
		total += e.Bytes
	}

	log.Printf("replicating %d files, %dMB", len(m), total/1024/1024)

	tt := time.Tick(*flagTick)
	ut := time.Tick(*flagUpdate)
	for {
		select {
		case <-tt:
			log.Printf("completed %.1f%% (errors %d) %d / %d, %dMB / %dMB (%d/s, %dMB/s)", 100*(float32(done)/float32(len(m))), err, done, len(m), copied/1024/1024, total/1024/1024, fps, bps)
		case <-ut:
			fps = done - ld
			bps = (copied - lc) / 1024 / 1024
			ld = done
			lc = copied
		case r := <-res:
			// accumulate
			done++
			if r.err != nil {
				log.Printf("error: %v", r.err)
				err++
			}

			copied += r.n
		}
	}
}

type result struct {
	path string
	n    int64
	err  error
}

func replicate(m []Entry) error {
	start := time.Now()

	par := *flagWorkers
	work := make(chan Entry, par*2)
	done := make(chan result, par*2)
	var wg sync.WaitGroup

	for i := 0; i < par; i++ {
		go func() {
			for e := range work {
				n, err := replOne(e)
				wg.Done()
				done <- result{path: e.Src, n: n, err: err}
			}
		}()
	}

	go status(m, done)

	for _, e := range m {
		wg.Add(1)
		work <- e
	}
	close(work)

	wg.Wait()

	log.Printf("done, replicated %d files in %s", len(m), time.Since(start))

	return nil
}

func maybeMkdirs(d string) error {
	if _, err := os.Stat(d); os.IsNotExist(err) {
		if err := os.MkdirAll(d, 0755); err != nil {
			return fmt.Errorf("failed to create dir %s: %v", d, err)
		}
	}
	return nil
}

func main() {
	var manifest []Entry

	flag.Parse()

	if *flagSrcDir == "" {
		log.Fatal("src required")
	}
	if *flagDestDir == "" {
		log.Fatal("dest required")
	}

	if err := maybeMkdirs(*flagDestDir); err != nil {
		log.Fatal(err)
	}

	log.Printf("replicating src dir: %s", *flagSrcDir)

	p := *flagSrcDir
	src := os.DirFS(p)

	if err := fs.WalkDir(src, ".", func(path string, d fs.DirEntry, err error) error {
		if skipFile(d) {
			return nil
		}

		fp := filepath.Join(p, path)

		log.Printf("extracting %s", fp)
		tm, hs, n, err := extractDate(fp)

		dp := filepath.Join(*flagDestDir, tm.Format(destDateFormat), d.Name())

		manifest = append(manifest, Entry{
			Src:   fp,
			Dest:  dp,
			Hash:  hs,
			Bytes: n,
		})

		return nil
	}); err != nil {
		log.Fatalf("failed to walk dir: %v", err)
	}

	if err := replicate(manifest); err != nil {
		log.Fatalf("failed to replicate: %v", err)
	}
}
