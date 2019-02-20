package roll

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type buffer struct {
	datadir string

	limit    int
	interval time.Duration
	timeout  time.Duration

	mu    sync.Mutex
	prime *bytes.Buffer
	spare *bytes.Buffer
	err   error

	ticker *time.Ticker
	timer  *time.Timer
	exceed chan int

	next NextFunc
}

func (b *buffer) Write(bs []byte) (int, error) {
	b.mu.Lock()
	n, err := b.prime.Write(bs)
	b.mu.Unlock()

	if err == nil {
		go func(n int) {
			b.exceed <- n
		}(n)
	}
	return n, err
}

func (b *buffer) Close() error {
	return b.flushAndClose(0, time.Now())
}

func (b *buffer) rotate() {
	for i := 0; ; i++ {
		var (
			now     time.Time
			expired bool
		)
		select {
		case n := <-b.timer.C:
			now, expired = n, true
		case n := <-b.ticker.C:
			now = n
		case n := <-b.exceed:
			w := b.prime.Len() + n
			if b.limit > 0 && w >= b.limit {
				now = time.Now()
			} else {
				i--
			}
		}
		b.err = b.rotateFile(i, now)
		if !expired {
			if !b.timer.Stop() {
				<-b.timer.C
			}
		}
		b.timer.Reset(b.timeout)
	}
}

func (b *buffer) rotateFile(i int, n time.Time) error {
	if n.IsZero() {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	b.prime, b.spare = b.spare, b.prime
	if b.spare.Len() == 0 {
		return nil
	}
	return b.flushAndClose(i, n)
}

func (b *buffer) flushAndClose(i int, n time.Time) error {
	defer b.spare.Reset()
	p, err := b.next(i, n)
	if err != nil {
		return nil
	}
	p = filepath.Join(b.datadir, p)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	return ioutil.WriteFile(p, b.spare.Bytes(), 0644)
}
