package roll

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Options struct {
	Timeout   time.Duration
	Interval  time.Duration
	MaxSize   int
	KeepEmpty bool
	Next      func(int, time.Time) (string, error)
}

type writer struct {
	datadir   string
	next      func(int, time.Time) (string, error)
	keepEmpty bool

	limit    int64
	interval time.Duration
	timeout  time.Duration

	mu      sync.Mutex
	file    *os.File
	writer  *bufio.Writer
	written int64
	err     error

	ticker *time.Ticker
	timer  *time.Timer
	exceed chan int
}

func Writer(d string, o Options) (io.WriteCloser, error) {
	i, err := os.Stat(d)
	if err != nil {
		return nil, err
	}
	if !i.IsDir() {
		return nil, fmt.Errorf("%s not a directory", d)
	}
	if o.Interval == 0 {
		o.Interval = time.Minute
	}
	if o.Timeout == 0 {
		o.Timeout = o.Interval + time.Second
	}
	w := writer{
		datadir:   d,
		interval:  o.Interval,
		timeout:   o.Timeout,
		timer:     time.NewTimer(o.Timeout),
		ticker:    time.NewTicker(o.Interval),
		limit:     -1,
		exceed:    make(chan int),
		keepEmpty: o.KeepEmpty,
	}
	if o.MaxSize > 0 {
		w.limit = int64(o.MaxSize)
	}
	if o.Next == nil {
		w.next = func(i int, n time.Time) (string, error) {
			return fmt.Sprintf("file-%06d-%d.bin", i, n.Unix()), nil
		}
	} else {
		w.next = o.Next
	}
	if err := w.createFile(0, time.Now()); err != nil {
		return nil, err
	}
	go w.rotate()

	return &w, nil
}

func (w *writer) Write(bs []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.writer.Write(bs)
	if err == nil {
		w.exceed <- n
	}
	return n, err
}

func (w *writer) Close() error {
	w.ticker.Stop()
	return w.flushAndClose()
}

func (w *writer) rotate() {
	// iter := 1
	for i := 1; ; i++ {
		var (
			expired bool
			err error
		)
		select {
		case n := <-w.timer.C:
			err = w.rotateFile(i, n)
			expired = true
		case n := <-w.ticker.C:
			err = w.rotateFile(i, n)
		case n := <-w.exceed:
			w.written += int64(n)
			if w.limit > 0 && w.written >= w.limit {
				err = w.rotateFile(i, time.Now())
			} else {
				i--
			}
		}
		if !expired {
			if !w.timer.Stop() {
				<-w.timer.C
			}
		}
		w.timer.Reset(w.timeout)
		if err != nil {
			w.err = err
		}
	}
}

func (w *writer) rotateFile(i int, n time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	err := w.flushAndClose()
	if err := w.createFile(i, n); err != nil {
		return err
	}

	return err
}

func (w *writer) flushAndClose() error {
	err := w.writer.Flush()
	if err := w.file.Close(); err != nil {
		return err
	}
	if !w.keepEmpty && w.written == 0 {
		os.Remove(w.file.Name())
	}
	w.written = 0
	return err
}

func (w *writer) createFile(i int, n time.Time) error {
	p, err := w.next(i, n)
	if err != nil {
		return err
	}
	p = filepath.Join(w.datadir, p)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	w.file, err = os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if w.writer == nil {
		w.writer = bufio.NewWriter(w.file)
	} else {
		w.writer.Reset(w.file)
	}
	return nil
}
