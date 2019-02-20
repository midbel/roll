package roll

import (
	"fmt"
	"io"
	"os"
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
	}
	if o.Next == nil {
		w.next = next
	} else {
		w.next = o.Next
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
	}
	if o.MaxSize > 0 {
		w.limit = int64(o.MaxSize)
	}
	if o.Next == nil {
		w.next = next
	} else {
		w.next = o.Next
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
	return nil
}

func next(i int, n time.Time) (string, error) {
	return fmt.Sprintf("file-%06d-%d.bin", i, n.Unix()), nil
}
