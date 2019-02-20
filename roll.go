package roll

import (
	"fmt"
	"io"
	"os"
	"time"
)

type Options struct {
	Timeout   time.Duration
	Interval  time.Duration
	MaxSize   int
	KeepEmpty bool
	Next      func(int, time.Time) (string, error)
}

func Buffer(d string) (io.WriteCloser, error) {
	return nil, nil
}

func File(d string, o Options) (io.WriteCloser, error) {
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

func next(i int, n time.Time) (string, error) {
	return fmt.Sprintf("file-%06d-%d.bin", i, n.Unix()), nil
}
