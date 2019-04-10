package roll

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Option func(*Roller)

type Roller struct {
	writer  io.WriteCloser
	closers []io.Closer
	open    NextFunc
	last    int
	once    sync.Once

	done chan struct{}
	sema chan struct{}

	// channel use to trigger a rotation
	roll chan time.Time

	reset    chan time.Time // manual rotation
	interval chan time.Time // automatic rotation

	// timeout rotation
	// use time between two write before being triggered
	ping    chan struct{}
	timeout chan time.Time

	// threshold (size, count) rotation
	// use the number of write and/or bytes written to be triggered
	written   chan int
	threshold chan time.Time
}

var sentinel = struct{}{}

type NextFunc func(int, time.Time) (io.WriteCloser, []io.Closer, error)

type WriteFunc func(io.Writer) error

func noop(w io.Writer) error { return nil }

func WithTimeout(every time.Duration) Option {
	return func(r *Roller) {
		if every <= 0 {
			return
		}
		r.timeout, r.ping = make(chan time.Time), make(chan struct{})
		go r.runWithTimeout(every)
	}
}

func WithInterval(every time.Duration) Option {
	return func(r *Roller) {
		if every <= 0 {
			return
		}
		r.interval = make(chan time.Time)
		go r.runWithInterval(every)
	}
}

func WithThreshold(size, count int) Option {
	return func(r *Roller) {
		if size == 0 && count == 0 {
			return
		}
		r.threshold, r.written = make(chan time.Time), make(chan int)
		go r.runWithThreshold(size, count)
	}
}

func Roll(next NextFunc, options ...Option) (*Roller, error) {
	r := Roller{
		open:  next,
		roll:  make(chan time.Time, 1),
		reset: make(chan time.Time, 1),
		sema:  make(chan struct{}, 1),
		done:  make(chan struct{}),
	}
	for _, o := range options {
		o(&r)
	}
	go r.run()

	wc, cs, err := r.open(r.last+1, time.Now())
	if err != nil {
		return nil, err
	}
	r.writer = wc
	if len(cs) > 0 {
		r.closers = append(r.closers[:0], cs...)
	}
	return &r, nil
}

func (r *Roller) WriteData(bs []byte, before, after WriteFunc) (int, error) {
	if before == nil {
		before = noop
	}
	if after == nil {
		after = noop
	}

	r.acquire()
	defer r.release()

	if err := r.openNext(); err != nil {
		return 0, err
	}
	if err := before(r.writer); err != nil {
		return 0, err
	}
	n, err := r.writer.Write(bs)
	if err == nil {
		go func() {
			select {
			case r.written <- n:
			default:
			}
		}()
		go func() {
			select {
			case r.ping <- sentinel:
			default:
			}
		}()
	}
	if e := after(r.writer); err == nil && e != nil {
		err = e
	}
	return n, err
}

func (r *Roller) Write(bs []byte) (int, error) {
	return r.WriteData(bs, noop, noop)
}

func (r *Roller) Close() error {
	var (
		closed bool
		err    error
	)
	r.once.Do(func() {
		close(r.done)
		err = r.closeWriter(r.writer, r.closers)
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

func (r *Roller) openNext() error {
	select {
	case n := <-r.roll:
		n = drainRoll(r.roll, n)
		if n.IsZero() {
			n = time.Now()
		}
		r.last++
		w, cs, err := r.open(r.last+1, n)
		if err != nil {
			return err
		}
		r.writer = w
		if len(cs) > 0 {
			r.closers = append(r.closers[:0], cs...)
		}
	default:
	}
	return nil
}

func (r *Roller) closeWriter(old io.Closer, others []io.Closer) error {
	if old == nil {
		return nil
	}
	r.acquire()
	defer r.release()
	if f, ok := old.(interface{ Flush() error }); ok {
		f.Flush()
	}
	err := old.Close()
	for i := len(others) - 1; i >= 0; i-- {
		e := others[i].Close()
		if err == nil && e != nil {
			err = e
		}
	}
	return err
}

func (r *Roller) run() {
	logger := log.New(os.Stdout, "[rotate] ", 0)

	for {
		var now time.Time
		select {
		case n := <-r.timeout:
			logger.Printf("timeout rotation @%s", n.Format("2006-01-02 15:04:05"))
			now = n
		case n := <-r.interval:
			logger.Printf("automatic rotation @%s", n.Format("2006-01-02 15:04:05"))
			now = n
		case n := <-r.threshold:
			logger.Printf("threshold rotation @%s", n.Format("2006-01-02 15:04:05"))
			now = n
		case n := <-r.reset:
			logger.Printf("manual rotation @%s", n.Format("2006-01-02 15:04:05"))
			now = n
		}
		if !now.IsZero() {
			select {
			case r.roll <- now:
			default:
				<-r.roll
				r.roll <- now
			}

			r.closeWriter(r.writer, r.closers)
		}
	}
}

func (r *Roller) runWithThreshold(size, count int) {
	if size <= 0 && count <= 0 {
		return
	}
	var written, limit int
	for {
		select {
		case n := <-r.written:
			limit++
			written += n
			if (size > 0 && written >= size) || (count > 0 && limit >= count) {
				r.threshold <- time.Now()
				limit, written = 0, 0
			}
		case <-r.done:
			return
		}
	}
}

func (r *Roller) runWithTimeout(d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	for {
		select {
		case <-r.ping:
			// reset timer for next timeout
			if !t.Stop() {
				<-t.C
			}
			t.Reset(d)
		case <-r.done:
			return
		case n := <-t.C:
			// timeout because r.written has not been selected
			t.Reset(d)
			r.timeout <- n
		}
	}
}

func (r *Roller) runWithInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-r.done:
			return
		case n := <-t.C:
			r.interval <- n
		}
	}
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
}
