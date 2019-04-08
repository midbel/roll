package roll

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/midbel/tape"
	"github.com/midbel/tape/ar"
	"github.com/midbel/tape/cpio"
)

type Header struct {
	File    string
	ModTime time.Time
	Owner   int
	Group   int
	Mode    int64
}

func (h Header) isZero() bool {
	return h.File == "" && h.ModTime.IsZero()
}

func (h Header) Tar() tar.Header {
	return tar.Header{
		Name:    h.File,
		ModTime: h.ModTime,
		Mode:    h.Mode,
		Uid:     h.Owner,
		Gid:     h.Group,
	}
}

func (h Header) Tape() tape.Header {
	return tape.Header{
		Filename: h.File,
		ModTime:  h.ModTime,
		Uid:      int64(h.Owner),
		Gid:      int64(h.Group),
		Mode:     h.Mode,
	}
}

type Roller struct {
	writer  io.WriteCloser
	closers []io.Closer
	open    NextFunc
	last    int
	once    sync.Once

	done      chan struct{}
	sema      chan struct{}
	written   chan int
	roll      chan time.Time
	reset     chan time.Time // manual rotation
	timeout   chan time.Time // timeout rotation
	interval  chan time.Time // automatic rotation
	threshold chan time.Time // threshold (size, count) rotation
	delay     time.Duration
}

var (
	sentinel = struct{}{}
	zero     = Header{}
)

type NextFunc func(int, time.Time) (io.WriteCloser, []io.Closer, error)

func WithTimeout(every time.Duration) func(*Roller) {
	return func(r *Roller) {
		if every <= 0 {
			return
		}
		r.delay = every
		r.timeout = make(chan time.Time)
		go r.runWithTime(r.delay, r.timeout)
	}
}

func WithInterval(every time.Duration) func(*Roller) {
	return func(r *Roller) {
		if every <= 0 {
			return
		}
		r.interval = make(chan time.Time)
		go r.runWithTime(every, r.interval)
	}
}

func WithThreshold(size, count int) func(*Roller) {
	return func(r *Roller) {
		r.threshold = make(chan time.Time)
		r.written = make(chan int)
		go r.runWithThreshold(size, count)
	}
}

func Roll(next NextFunc, options ...func(*Roller)) (*Roller, error) {
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

func (r *Roller) WriteData(h Header, bs []byte) (int, error) {
	r.acquire()
	defer r.release()

	if err := r.openNext(); err != nil {
		return 0, err
	}
	if len(bs) == 0 {
		return 0, nil
	}

	if !h.isZero() {
		var err error
		switch w := r.writer.(type) {
		case *tar.Writer:
			hdr := h.Tar()
			hdr.Size = int64(len(bs))
			err = w.WriteHeader(&hdr)
		case *cpio.Writer:
			hdr := h.Tape()
			hdr.Length = int64(len(bs))
			err = w.WriteHeader(&hdr)
		case *ar.Writer:
			hdr := h.Tape()
			hdr.Length = int64(len(bs))
			err = w.WriteHeader(&hdr)
		default:
		}
		if err != nil {
			return 0, err
		}
	}
	n, err := r.writer.Write(bs)
	if err == nil {
		go func() {
			select {
			case r.written <- n:
			default:
			}
		}()
	}
	return n, err
}

func (r *Roller) Write(bs []byte) (int, error) {
	return r.WriteData(zero, bs)
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

func (r *Roller) openNext() error {
	select {
	case n := <-r.roll:
		n = drainRoll(r.roll, n)
		if n.IsZero() {
			n = time.Now()
		}
		r.last++
		w, cs, err := r.open(r.last, n)
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

func (r *Roller) closeWriter(old io.Closer) error {
	if old == nil {
		return nil
	}
	r.acquire()
	defer r.release()
	if f, ok := old.(interface{ Flush() error }); ok {
		f.Flush()
	}
	err := old.Close()
	for i := len(r.closers) - 1; i >= 0; i-- {
		e := r.closers[i].Close()
		if err == nil && e != nil {
			err = e
		}
	}
	return err
}

func (r *Roller) run() {
	logger := log.New(os.Stdout, "[rotate] ", 0)

	var last time.Time
	for {
		var now time.Time
		select {
		case n := <-r.timeout:
			if delta := n.Sub(last); !last.IsZero() && delta >= r.delay {
				logger.Printf("timeout rotation @%s (%s)", n.Format("2006-01-02 15:04:05"), delta)
				now, last = n, time.Time{}
			}
		case n := <-r.interval:
			logger.Printf("automatic rotation @%s", n.Format("2006-01-02 15:04:05"))
			now = n
		case n := <-r.threshold:
			logger.Printf("threshold rotation @%s", n.Format("2006-01-02 15:04:05"))
			now, last = n, n
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

			r.closeWriter(r.writer)
		}
	}
}

func (r *Roller) runWithThreshold(size, count int) {
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

func (r *Roller) runWithTime(d time.Duration, tick chan time.Time) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-r.done:
			return
		case n := <-t.C:
			tick <- n
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
