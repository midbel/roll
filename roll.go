package roll

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	DefaultInterval = time.Minute
	DefaultTimeout  = DefaultInterval + time.Second
)

var MaxSize = 64 << 10

type Options struct {
	Timeout   time.Duration
	Interval  time.Duration
	MaxSize   int
	KeepEmpty bool
	Next      NextFunc
}

type NextFunc func(int, time.Time) (string, error)

func Buffer(d string, o Options) (io.WriteCloser, error) {
	if err := checkOptions(d, &o); err != nil {
		return nil, err
	}
	if o.MaxSize <= 0 {
		o.MaxSize = MaxSize
	}
	w := buffer{
		datadir:  d,
		interval: o.Interval,
		timeout:  o.Timeout,
		limit:    o.MaxSize,
		timer:    time.NewTimer(o.Timeout),
		ticker:   time.NewTicker(o.Interval),
		exceed:   make(chan int),
		next:     o.Next,
	}
	go w.rotate()

	return &w, nil
}

func File(d string, o Options) (io.WriteCloser, error) {
	if err := checkOptions(d, &o); err != nil {
		return nil, err
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
		next:      o.Next,
	}
	if o.MaxSize > 0 {
		w.limit = int64(o.MaxSize)
	}
	if err := w.createFile(0, time.Now()); err != nil {
		return nil, err
	}
	go w.rotate()

	return &w, nil
}

func checkOptions(d string, o *Options) error {
	i, err := os.Stat(d)
	if err != nil {
		return err
	}
	if !i.IsDir() {
		return fmt.Errorf("%s not a directory", d)
	}
	if o.Interval == 0 {
		o.Interval = DefaultInterval
	}
	if o.Timeout == 0 {
		o.Timeout = DefaultTimeout
	}
	if o.Next == nil {
		o.Next = next
	}
	return nil
}

func next(i int, n time.Time) (string, error) {
	return fmt.Sprintf("file-%06d-%d.bin", i, n.Unix()), nil
}

type roller struct {
	next     NextFunc
	limit    int64
	interval time.Duration
	timeout  time.Duration

	mu      sync.Mutex
	inner   io.WriteCloser
	written int64
	err     error

	ticker *time.Ticker
	timer  *time.Timer
	exceed chan int
}

func (r *roller) Write(bs []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	r.mu.Lock()
	n, err := r.inner.Write(bs)
	r.mu.Unlock()

	if err == nil {
		r.exceed <- n
	}
	return n, err
}

func (r *roller) Close() error {
	if r.err != nil {
		return r.err
	}
	return r.inner.Close()
}
