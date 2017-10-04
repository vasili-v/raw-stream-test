package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vasili-v/raw-stream-test/scanner"
)

func handleConn(c net.Conn) {
	ch := make(chan []byte, limit)
	fmt.Printf("got connection from %s\n", c.RemoteAddr())
	defer func() {
		fmt.Printf("closing connection to %s\n", c.RemoteAddr())
		c.Close()
		for range ch {
		}
	}()

	var received uint64
	go readConn(c, ch, &received)
	writeConnBuf(c, ch, &received)
}

func readConn(c net.Conn, out chan []byte, count *uint64) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(out)
	}()

	s := scanner.NewScanner(c)
	th := make(chan int, limit)
	for s.Scan() {
		wg.Add(1)
		atomic.AddUint64(count, 1)

		th <- 0
		go handleMsg(s.Bytes(), out, func() {
			wg.Done()
			<-th
		})
	}

	if err := s.Err(); err != nil {
		fmt.Printf("reading from %s error: %s\n", c.RemoteAddr(), err)
	}
}

func writeConn(c net.Conn, out chan []byte, count *uint64) {
	i := 0
	for msg := range out {
		n, err := c.Write(msg)
		if err != nil {
			fmt.Printf("message %d sending to %s error: %s\n", i, c.RemoteAddr(), err)
			return
		}

		if n != len(msg) {
			fmt.Printf("message %d sending to %s incomplete: expected %d sent %d\n", i, c.RemoteAddr(), len(msg), n)
			return
		}
	}
}

func writeConnBuf(c net.Conn, out chan []byte, count *uint64) {
	buf := make([]byte, 1024)
	off := 0
	rem := len(buf)

	start := 0
	end := 0
	for msg := range out {
		if len(msg) > rem {
			size := len(buf) - rem
			n, err := c.Write(buf[:size])
			if err != nil {
				fmt.Printf("message %d - %d sending to %s error: %s\n", start, end, c.RemoteAddr(), err)
				return
			}

			if n != size {
				fmt.Printf("message %d - %d sending to %s incomplete: expected %d sent %d\n",
					start, end, c.RemoteAddr(), size, n)
				return
			}

			off = 0
			rem = len(buf)
			start = end
		}

		n := copy(buf[off:], msg)
		off += n
		rem -= n
		end++

		if rem <= 0 || atomic.LoadUint64(count)-uint64(end) <= 0 {
			size := len(buf) - rem
			n, err := c.Write(buf[:size])
			if err != nil {
				fmt.Printf("message %d - %d sending to %s error: %s\n", start, end, c.RemoteAddr(), err)
				return
			}

			if n != size {
				fmt.Printf("message %d - %d sending to %s incomplete: expected %d sent %d\n",
					start, end, c.RemoteAddr(), size, n)
				return
			}

			off = 0
			rem = len(buf)
			start = end
		}
	}
}

func handleMsg(req []byte, out chan []byte, f func()) {
	defer f()

	size := len(req)
	if size < 4 {
		fmt.Printf("expected at least 4 bytes in request but got %d\n", size)
		return
	}

	time.Sleep(2 * time.Microsecond)

	id := binary.BigEndian.Uint32(req)

	res := make([]byte, size+2)
	binary.BigEndian.PutUint16(res, uint16(size))
	binary.BigEndian.PutUint32(res[2:], id)
	for i := 6; i < len(res); i++ {
		res[i] = 0x55
	}

	out <- res
}

func main() {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		panic(fmt.Errorf("opening port error: %s", err))
	}

	for {
		c, err := ln.Accept()
		if err != nil {
			panic(fmt.Errorf("accepting connection error: %s", err))
		}

		go handleConn(c)
	}
}
