package main

import (
	"archive/tar"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/midbel/roll"
)

func main() {
	gid := flag.Int("g", 1000, "")
	uid := flag.Int("u", 1000, "")
	size := flag.Int("n", 0, "size per archive")
	count := flag.Int("c", 0, "files per archive")
	datadir := flag.String("d", "", "working directory")
	prefix := flag.String("p", "roll", "archive prefix")
	flag.Parse()

	next, err := open(*datadir, *prefix)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	r, err := roll.Roll(next, roll.WithThreshold(*size, *count))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer r.Close()

	err = filepath.Walk(flag.Arg(0), func(p string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() {
			return nil
		}
		h := roll.Header{
			File:    p,
			ModTime: i.ModTime(),
			Mode:    int64(i.FileMode()),
			Uid:     *uid,
			Gid:     *gid,
		}
		bs, err := ioutil.ReadFile(p)
		if err != nil {
			return err
		}
		_, err = r.WriteData(h, bs)
		return err
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func open(base, prefix string) (roll.NextFunc, error) {
	if err := os.MkdirAll(base, 0755); err != nil {
		return nil, err
	}
	next := func(i int, t time.Time) (io.WriteCloser, []io.Closer, error) {
		file := fmt.Sprintf("%s_%d_%s.tar", prefix, i, t.Format("20060102_150405"))
		w, err := os.Create(filepath.Join(base, file))
		if err != nil {
			return nil, nil, err
		}
		tw := tar.NewWriter(w)
		return tw, []io.Closer{w}, nil
	}
	return next, nil
}
