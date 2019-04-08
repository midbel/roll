package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

func main() {
	repeat := flag.Int("r", 0, "repeat")
	length := flag.Int("n", 80, "repeat")
	every := flag.Duration("e", time.Second, "print a line every tick")
	flag.Parse()

	r, err := os.Open(flag.Arg(0))
	if err != nil {
		return
	}
	defer r.Close()

	if err := Forever(r, *repeat, *length, *every); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func Forever(r io.ReadSeeker, repeat, length int, every time.Duration) error {
	tick := time.Tick(every)
	for i := 0; repeat <= 0 || i < repeat; i++ {
		s := bufio.NewScanner(r)
		if length > 0 {
			s.Split(scanBytes(length))
		}
		for s.Scan() {
			if _, err := os.Stdout.Write(append(s.Bytes(), '\n')); err != nil {
				return err
			}
			<-tick
		}
		if err := s.Err(); err != nil {
			return err
		}
		if _, err := r.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}
	return nil
}

func scanBytes(length int) bufio.SplitFunc {
	split := func(bs []byte, ateof bool) (int, []byte, error) {
		if ateof {
			return len(bs), bs, bufio.ErrFinalToken
		}
		if len(bs) < length {
			return 0, nil, nil
		}
		if bs[0] == '\n' {
			return 1, nil, nil
		}
		j := length
		for i := j; i >= 0; i-- {
			if bs[i] == ' ' {
				j++
				for i := j; i >= 0; i-- {
					if bs[i] == '\n' {
						j = i
						break
					}
				}
				break
			}
			j--
		}
		xs := make([]byte, j)
		return copy(xs, bs), xs, nil
	}
	return split
}
