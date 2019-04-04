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
)

func main() {
	var o roll.Options

	dir := flag.String("y", "", "directory structure")
	mode := flag.String("m", "", "mode")
	prefix := flag.String("p", "roll", "prefix")
	ext := flag.String("e", "txt", "extension")
	tee := flag.Bool("tee", false, "copy stdin to stdout")
	flag.IntVar(&o.MaxCount, "c", 0, "count threshold")
	flag.IntVar(&o.MaxSize, "s", 0, "size threshold")
	flag.DurationVar(&o.Timeout, "t", 0, "timeout")
	flag.DurationVar(&o.Interval, "d", 0, "interval")
	flag.BoolVar(&o.KeepEmpty, "k", false, "do not rotate when no bytes were written")
	flag.Parse()

	logger := log.New(os.Stderr, "[next] ", 0)
	o.Open = func(_ int, w time.Time) (io.WriteCloser, error) {
		datadir := flag.Arg(0)
		switch *dir {
		case "time":
			y := fmt.Sprintf("%04d", w.Year())
			d := fmt.Sprintf("%03d", w.YearDay())
			h := fmt.Sprintf("%02d", w.Hour())

			datadir = filepath.Join(datadir, y, d, h)
			if err := os.MkdirAll(datadir, 0755); err != nil {
				return nil, err
			}
		default:
		}

		var suffix string
		switch *mode {
		case "hms":
			suffix = w.Format("150405")
		default:
			suffix = fmt.Sprint(w.Unix())
		}
		n := fmt.Sprintf("%s_%s.%s", *prefix, suffix, *ext)
		logger.Println("open file", filepath.Join(datadir, n))
		return os.OpenFile(filepath.Join(datadir, n), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}

	w, err := roll.Roll(o)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	done := make(chan struct{}, 1)
	go func() {
		var r io.Reader = os.Stdin
		if *tee {
			r = io.TeeReader(os.Stdin, os.Stdout)
		}
		s := bufio.NewScanner(r)
		s.Split(bufio.ScanLines)
		for i := 1; s.Scan(); i++ {
			t := s.Text()
			if t == "" {
				break
			}
			if _, err := io.WriteString(w, fmt.Sprintf("%d: %s\n", i, t)); err != nil {
				fmt.Println(err)
				break
			}
		}
		fmt.Println("done:", s.Err())
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
