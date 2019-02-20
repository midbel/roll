package roll

import (
	"bufio"
	"os"
	"path/filepath"
	"sync"
	"time"
)

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

func (w *writer) Write(bs []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	w.mu.Lock()
	n, err := w.writer.Write(bs)
	w.mu.Unlock()

	if err == nil {
		go func(n int) {
			w.exceed <- n
		}(n)
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
			now     time.Time
		)
		select {
		case n := <-w.timer.C:
			now, expired = n, true
		case n := <-w.ticker.C:
			now = n
		case n := <-w.exceed:
			w.written += int64(n)
			if w.limit > 0 && w.written >= w.limit {
				now = time.Now()
			} else {
				i--
			}
		}
		w.err = w.rotateFile(i, now)
		if !expired {
			if !w.timer.Stop() {
				<-w.timer.C
			}
		}
		w.timer.Reset(w.timeout)
	}
}

func (w *writer) rotateFile(i int, n time.Time) error {
	if n.IsZero() {
		return nil
	}
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
