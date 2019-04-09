package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/midbel/roll"
)

const (
	DefaultInterval = time.Minute * 5
	DefaultTimeout  = time.Minute
	DefaultName = "roll.log"
)

func main() {
	var h History

	flag.StringVar(&h.Dir, "y", "", "directory structure")
	flag.StringVar(&h.Mode, "m", "", "mode")
	flag.StringVar(&h.Basename, "b", DefaultName, "prefix")
	flag.IntVar(&h.Keep, "k", 7, "keep K files before deleting")
	flag.IntVar(&h.Mini, "n", 5, "compress N last files")
	flag.BoolVar(&h.Empty, "e", false, "keep empty files")
	tee := flag.Bool("tee", false, "copy stdin to stdout")
	maxCount := flag.Int("c", 0, "count threshold")
	maxSize := flag.Int("s", 0, "size threshold")
	timeout := flag.Duration("t", DefaultInterval, "timeout")
	interval := flag.Duration("d", DefaultTimeout, "interval")
	flag.Parse()

	options := []func(*roll.Roller){
		roll.WithThreshold(*maxSize, *maxCount),
		roll.WithInterval(*interval),
		roll.WithTimeout(*timeout),
	}
	h.Datadir = flag.Arg(0)

	w, err := roll.Roll(h.Open, options...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	done := make(chan struct{}, 1)
	go func() {
		var r io.Reader = os.Stdin
		if *tee {
			r = io.TeeReader(r, os.Stdout)
		}
		s := bufio.NewScanner(r)
		for i := 1; s.Scan(); i++ {
			t := s.Text()
			if _, err := io.WriteString(w, fmt.Sprintf("%d: %s\n", i, t)); err != nil {
				fmt.Fprintln(os.Stderr, err)
				break
			}
		}
		close(done)
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig)

	select {
	case <-sig:
	case <-done:
	}
	fmt.Println()
	w.Close()
}

type History struct {
	Keep  int
	Mini  int
	Empty bool

	Datadir  string
	Mode     string // configure suffix of files
	Dir      string // configure directory layout where files will be written
	Basename string // basename of files created
}

func (h *History) Open(i int, w time.Time) (io.WriteCloser, []io.Closer, error) {
	datadir := h.Datadir
	switch h.Dir {
	case "time":
		y := fmt.Sprintf("%04d", w.Year())
		d := fmt.Sprintf("%03d", w.YearDay())
		h := fmt.Sprintf("%02d", w.Hour())

		datadir = filepath.Join(datadir, y, d, h)
		if err := os.MkdirAll(datadir, 0755); err != nil {
			return nil, nil, err
		}
	default:
	}
	var suffix string
	switch h.Mode {
	case "version":
		i--
		if h.Keep > 0 {
			suffix = fmt.Sprintf("%06d", i%h.Keep)
		} else {
			suffix = fmt.Sprintf(".%06d", i)
		}
	case "hms":
		suffix = "-" + w.Format("150405")
	default:
		suffix = "-" + fmt.Sprint(w.Unix())
	}
	file := filepath.Join(datadir, h.Basename+suffix)
	wc, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		log.Println("open file", file)
	}

	return wc, nil, err
}
