package roll

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Roller struct {
	writer io.WriteCloser
	open   NextFunc
	last   int
	once   sync.Once

	done    chan struct{}
	sema    chan struct{}
	written chan int
	roll    chan time.Time
	reset   chan time.Time

	timeout  time.Duration
	timer    *time.Timer
	ticker   *time.Ticker
	maxSize  int
	maxCount int
}

var sentinel = struct{}{}

type NextFunc func(int, time.Time) (io.WriteCloser, error)

func WithTimeout(every time.Duration) func(*Roller) {
	return func(r *Roller) {
		if every <= 0 {
			return
		}
		r.timer, r.timeout = time.NewTimer(every), every
	}
}

func WithInterval(every time.Duration) func(*Roller) {
	return func(r *Roller) {
		if every <= 0 {
			return
		}
		r.ticker = time.NewTicker(every)
	}
}

func WithThreshold(size, count int) func(*Roller) {
	return func(r *Roller) {
		r.maxSize, r.maxCount = size, count
	}
}

func Roll(next NextFunc, options ...func(*Roller)) (*Roller, error) {
	r := Roller{
		open:    next,
		written: make(chan int),
		roll:    make(chan time.Time, 1),
		reset:   make(chan time.Time, 1),
		sema:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	for _, o := range options {
		o(&r)
	}
	go r.run()

	wc, err := r.open(r.last+1, time.Now())
	if err != nil {
		return nil, err
	}
	r.writer = wc
	return &r, nil
}

func (r *Roller) Write(bs []byte) (int, error) {
	select {
	case n := <-r.roll:
		n = drainRoll(r.roll, n)
		if n.IsZero() {
			n = time.Now()
		}
		r.last++
		w, err := r.open(r.last, n)
		if err != nil {
			return 0, err
		}
		r.writer = w
	default:
	}
	r.acquire()
	defer r.release()
	n, err := r.writer.Write(bs)
	if err == nil {
		go func() {
			r.written <- n
		}()
	}
	return n, err
}

func (r *Roller) Close() error {
	var (
		closed bool
		err    error
	)
	r.once.Do(func() {
		close(r.done)
		err = r.closeWriter(r.writer)
		closed = true
	})
	if closed {
		return err
	}
	return fmt.Errorf("roller already closed")
}

func (r *Roller) Rotate() {
	r.reset <- time.Now()
}

func (r *Roller) run() {
	var interval, timeout <-chan time.Time
	if r.ticker != nil {
		defer r.ticker.Stop()
		interval = r.ticker.C
	}
	if r.timer != nil {
		defer r.timer.Stop()
		timeout = r.timer.C
	}
	logger := log.New(os.Stdout, "[rotate] ", 0)

	var written, count int
	for {
		var (
			now     time.Time
			expired bool
			reset   bool
		)
		select {
		case <-r.done:
			return
		case n := <-timeout:
			expired = true
			logger.Printf("timeout rotation @%s", n.Format("2006-01-02 15:04:05"))
		case n := <-interval:
			now, reset = n, true
			logger.Printf("automatic rotation @%s", n.Format("2006-01-02 15:04:05"))
		case n := <-r.reset:
			now, reset = n, true
		case n := <-r.written:
			if n > 0 {
				count++
				written += n
			}
			if (r.maxCount > 0 && count >= r.maxCount) || (r.maxSize > 0 && written > r.maxSize) {
				now = time.Now()
				logger.Printf("threshold rotation @%s (%d - %d)", now.Format("2006-01-02 15:04:05"), written, count)
			}
		}
		if !now.IsZero() || expired {
			if written > 0 || reset {
				select {
				case r.roll <- now:
				default:
					<-r.roll
					r.roll <- now
				}

				r.closeWriter(r.writer)
			}
			written, count = 0, 0
		}
		if r.timeout > 0 {
			r.timer.Reset(r.timeout)
		}
	}
}

func (r *Roller) closeWriter(old io.Closer) error {
	if old == nil {
		return nil
	}
	r.acquire()
	defer r.release()
	if f, ok := old.(interface{ Flush() error }); ok {
		f.Flush()
	}
	return old.Close()
}

func (r *Roller) acquire() { r.sema <- sentinel }
func (r *Roller) release() { <-r.sema }

func drainRoll(roll <-chan time.Time, n time.Time) time.Time {
	for {
		select {
		case w := <-roll:
			n = w
		default:
			return n
		}
	}
	return n
}
