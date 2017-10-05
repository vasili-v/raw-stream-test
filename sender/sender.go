package sender

import (
	"fmt"
	"io"
	"sync/atomic"
)

type Sender struct {
	w io.Writer

	buf []byte
	off int
	rem int

	start int
	end   int

	total int
	stats []int
}

func NewSender(w io.Writer, size, maxMsgCount int) *Sender {
	s := &Sender{
		w:   w,
		buf: make([]byte, size),
		rem: size,
	}

	if maxMsgCount > 0 {
		s.stats = make([]int, maxMsgCount)
	}

	return s
}

func (s *Sender) Send(msg []byte, count *uint64) error {
	if len(msg) > s.rem {
		err := s.Flush()
		if err != nil {
			return err
		}

		if len(msg) > s.rem {
			return s.drop(msg)
		}
	}

	s.put(msg)

	if s.rem <= 0 || count != nil && atomic.LoadUint64(count) <= uint64(s.end) {
		return s.Flush()
	}

	return nil
}

func (s *Sender) Flush() error {
	size := len(s.buf) - s.rem
	if size <= 0 {
		return nil
	}

	n, err := s.w.Write(s.buf[:size])
	if err != nil {
		return fmt.Errorf("(%d - %d) %s", s.start, s.end, err)
	}

	if n != size {
		return fmt.Errorf("(%d - %d) expected %d sent %d bytes", s.start, s.end, size, n)
	}

	s.off = 0
	s.rem = len(s.buf)

	s.count(s.end - s.start)
	s.start = s.end

	return nil
}

func (s *Sender) Stats(w io.Writer, dst string) {
	fmt.Fprintf(w, "sending stats for %s (total %d in %d):\n", dst, s.end, s.total)
	for i, c := range s.stats {
		if c > 0 {
			if i < len(s.stats)-1 {
				fmt.Fprintf(w, "\t%d: %d\n", i, c)
			} else {
				fmt.Fprintf(w, "\trest: %d\n", c)
			}
		}
	}
}

func (s *Sender) drop(msg []byte) error {
	n, err := s.w.Write(msg)
	if err != nil {
		return fmt.Errorf("%d %s", s.start, err)
	}

	if n != len(msg) {
		return fmt.Errorf("%d expected %d sent %d bytes", s.start, len(msg), n)
	}

	s.count(1)
	s.start++
	s.end++

	return nil
}

func (s *Sender) put(msg []byte) {
	n := copy(s.buf[s.off:], msg)
	s.off += n
	s.rem -= n
	s.end++
}

func (s *Sender) count(n int) {
	s.total++

	last := len(s.stats) - 1
	if n > 0 && n < last {
		s.stats[n]++
	} else if last >= 0 {
		s.stats[last]++
	}
}
