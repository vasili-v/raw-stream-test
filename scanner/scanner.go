package scanner

import (
	"encoding/binary"
	"fmt"
	"io"
)

type Scanner struct {
	MaxMsgSize int

	r      io.Reader
	buf    []byte
	inOff  int
	inSize int
	out    []byte
	outOff int
	err    error
}

const (
	bufferLength      = 64 * 1024
	defaultMaxMsgSize = 2 * 1024
)

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		MaxMsgSize: defaultMaxMsgSize,
		r:          r,
		buf:        make([]byte, bufferLength),
	}
}

func (s *Scanner) Scan() bool {
	s.out = nil
	s.outOff = 0

	buf := make([]byte, 2)
	for {
		var err error
		if s.inOff >= s.inSize {
			var n int
			n, err = s.r.Read(s.buf)
			s.inOff = 0
			s.inSize = n
		}

		for s.inOff < s.inSize {
			if len(s.out) > 0 {
				req := len(s.out) - s.outOff
				got := copy(s.out[s.outOff:], s.buf[s.inOff:s.inSize])
				s.inOff += got

				if got < req {
					s.outOff += got
				} else {
					s.outOff = 0

					return true
				}
			} else {
				req := len(buf) - s.outOff
				got := copy(buf[s.outOff:], s.buf[s.inOff:s.inSize])
				s.inOff += got

				if got < req {
					s.outOff += got
				} else {
					s.outOff = 0

					size := binary.BigEndian.Uint16(buf[:])
					if s.MaxMsgSize > 0 && int(size) > s.MaxMsgSize {
						s.err = fmt.Errorf("expected message at most of %d size but got %d", s.MaxMsgSize, size)
						return false
					}

					s.out = make([]byte, size)
				}
			}
		}

		if err == io.EOF {
			if s.outOff > 0 {
				if len(s.out) > 0 {
					s.err = fmt.Errorf("expected message of %d size but got %d", len(s.out), s.outOff)
				} else {
					s.err = fmt.Errorf("expected 2 bytes message size but got %d", s.outOff)
				}
			}
			break
		}

		if err != nil {
			s.err = err
			break
		}
	}

	return false
}

func (s *Scanner) Bytes() []byte {
	return s.out
}

func (s *Scanner) Err() error {
	return s.err
}
