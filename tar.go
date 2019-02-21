package roll

import (
	"archive/tar"
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Header struct {
	Name    string
	Size    int64
	Mode    int64
	Uid     int
	Gid     int
	ModTime time.Time
}

func (h Header) tarHeader() *tar.Header {
	if h.ModTime.IsZero() {
		h.ModTime = time.Now()
	}
	return &tar.Header{
		Name:    h.Name,
		Size:    h.Size,
		Mode:    h.Mode,
		Uid:     h.Uid,
		Gid:     h.Gid,
		ModTime: h.ModTime,
		AccessTime: h.ModTime,
		ChangeTime: h.ModTime,
		Format:  tar.FormatGNU,
	}
}

type Tarball struct {
	datadir string

	limit    int
	interval time.Duration
	timeout  time.Duration

	mu      sync.Mutex
	writer  *tar.Writer
	buffer  bytes.Buffer
	err     error
	written int

	ticker *time.Ticker
	timer  *time.Timer
	exceed chan int

	next NextFunc
}

func Tar(d string, o Options) (*Tarball, error) {
	if err := checkOptions(d, &o); err != nil {
		return nil, err
	}
	t := Tarball{
		datadir:  d,
		interval: o.Interval,
		timeout:  o.Timeout,
		limit:    o.MaxSize,
		timer:    time.NewTimer(o.Timeout),
		ticker:   time.NewTicker(o.Interval),
		exceed:   make(chan int),
		next:     o.Next,
	}
	t.writer = tar.NewWriter(&t.buffer)
	go t.rotate()

	return &t, nil
}

func (t *Tarball) rotate() {
	for i := 0; ; i++ {
		var (
			now     time.Time
			expired bool
		)
		select {
		case n := <-t.timer.C:
			now, expired = n, true
		case n := <-t.ticker.C:
			now = n
		case <-t.exceed:
			t.written++
			if t.limit > 0 && t.written >= t.limit {
				now = time.Now()
			} else {
				i--
			}
		}
		t.err = t.rotateFile(i, now)
		if !expired {
			if !t.timer.Stop() {
				<-t.timer.C
			}
		}
		t.timer.Reset(t.timeout)
	}
}

func (t *Tarball) Write(h *Header, bs []byte) error {
	if len(bs) == 0 {
		return nil
	}
	t.mu.Lock()
	if err := t.writer.WriteHeader(h.tarHeader()); err != nil {
		return err
	}
	_, err := t.writer.Write(bs)
	t.mu.Unlock()
	if err == nil {
		t.exceed <- 1
	}
	return err
}

func (t *Tarball) Close() error {
	return t.flushAndClose(0, time.Now())
}

func (t *Tarball) rotateFile(i int, n time.Time) error {
	if n.IsZero() {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.written = 0

	if t.buffer.Len() > 0 {
		if err := t.flushAndClose(i, n); err != nil {
			return err
		}
	}
	t.buffer.Reset()
	t.writer = tar.NewWriter(&t.buffer)
	return nil
}

func (t *Tarball) flushAndClose(i int, n time.Time) error {
	if err := t.writer.Close(); err != nil {
		return err
	}
	p, err := t.next(i, n)
	if err != nil {
		return err
	}
	p = filepath.Join(t.datadir, p)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	return ioutil.WriteFile(p, t.buffer.Bytes(), 0644)
}
