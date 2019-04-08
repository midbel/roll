package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

func main() {
	bin := flag.Bool("b", false, "bin")
	repeat := flag.Int("r", 0, "repeat")
	length := flag.Int("n", 80, "length")
	every := flag.Duration("e", time.Second, "print a line every tick")
	flag.Parse()

	r, err := os.Open(flag.Arg(0))
	if err != nil {
		return
	}
	defer r.Close()

	var split bufio.SplitFunc
	if *bin {
		split = scanBytes(*length)
	} else {
		split = scanLines(*length)
	}
	for i := 0; *repeat <= 0 || i < *repeat; i++ {
		if err := Forever(r, split, *bin, *every); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if _, err := r.Seek(0, io.SeekStart); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func Forever(r io.ReadSeeker, split bufio.SplitFunc, bin bool, every time.Duration) error {
	tick := time.Tick(every)
	s := bufio.NewScanner(r)
	s.Split(split)

	var bs []byte
	for s.Scan() {
		if bin {
			ds := s.Bytes()
			xs := make([]byte, hex.EncodedLen(len(ds)))
			hex.Encode(xs, ds)
			bs = xs
		} else {
			bs = s.Bytes()
		}
		if _, err := os.Stdout.Write(append(bs, '\n')); err != nil {
			return err
		}
		<-tick
	}
	return s.Err()
}

const (
	newline = '\n'
	space   = ' '
)

func scanBytes(length int) bufio.SplitFunc {
	if length <= 0 {
		length = 16
	}
	return func(bs []byte, ateof bool) (int, []byte, error) {
		if ateof {
			return len(bs), bs, bufio.ErrFinalToken
		}
		if len(bs) < length {
			return 0, nil, nil
		}
		xs := make([]byte, length)
		return copy(xs, bs), xs, nil
	}
}

func scanLines(length int) bufio.SplitFunc {
	if length <= 0 {
		return bufio.ScanLines
	}
	return func(bs []byte, ateof bool) (int, []byte, error) {
		if ateof {
			return len(bs), bs, bufio.ErrFinalToken
		}
		if len(bs) < length {
			return 0, nil, nil
		}
		if bs[0] == newline {
			return 1, nil, nil
		}
		var j int
		for i := 0; i < length; i++ {
			if bs[i] == newline {
				j = i
				break
			}
			if bs[i] == space {
				j = i
			}
		}

		j++
		xs := make([]byte, j)
		return copy(xs, bs), xs, nil
	}
}
