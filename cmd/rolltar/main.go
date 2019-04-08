package main

import (
	"archive/tar"
	"compress/gzip"
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
	mini := flag.Bool("z", false, "compress archive")
	flag.Parse()

	next, err := open(*datadir, *prefix, *mini)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	r, err := roll.Roll(next, roll.WithThreshold(*size, *count))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	err = filepath.Walk(flag.Arg(0), func(p string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() {
			return nil
		}
		bs, err := ioutil.ReadFile(p)
		if err != nil {
			return err
		}
		_, err = r.WriteData(bs, func(w io.Writer) error {
			var err error
			if w, ok := w.(*tar.Writer); ok {
				h, err := FileInfoHeader(p, i, len(bs), *uid, *gid)
				if err != nil {
					return err
				}
				err = w.WriteHeader(h)
			}
			return err
		}, nil)
		return err
	})
	if e := r.Close(); err == nil && e != nil {
		err = e
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func FileInfoHeader(p string, i os.FileInfo, siz, uid, gid int) (*tar.Header, error) {
	h, err := tar.FileInfoHeader(i, "")
	if err != nil {
		return nil, err
	}
	h.Name = p
	h.Size = int64(siz)
	h.Gid = gid
	h.Uid = uid

	return h, nil
}

func open(base, prefix string, mini bool) (roll.NextFunc, error) {
	if err := os.MkdirAll(base, 0755); err != nil {
		return nil, err
	}
	next := func(i int, t time.Time) (io.WriteCloser, []io.Closer, error) {
		file := fmt.Sprintf("%s_%06d_%s.tar", prefix, i, t.Format("20060102_150405"))
		if mini {
			file += ".gz"
		}
		var (
			cs []io.Closer
			wc io.WriteCloser
		)
		if w, err := os.Create(filepath.Join(base, file)); err != nil {
			return nil, nil, err
		} else {
			cs, wc = append(cs, w), w
		}
		if mini {
			g := gzip.NewWriter(wc)
			cs, wc = append(cs, g), g
		}
		tw := tar.NewWriter(wc)
		return tw, cs, nil
	}
	return next, nil
}
