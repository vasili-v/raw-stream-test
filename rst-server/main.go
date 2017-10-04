package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/vasili-v/raw-stream-test/scanner"
)

func handleConn(c net.Conn) {
	ch := make(chan []byte)
	fmt.Printf("got connection from %s\n", c.RemoteAddr())
	defer func() {
		fmt.Printf("closing connection to %s\n", c.RemoteAddr())
		c.Close()
		for range ch {
		}
	}()

	go readConn(c, ch)
	writeConn(c, ch)
}

func readConn(c net.Conn, out chan []byte) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(out)
	}()

	s := scanner.NewScanner(c)
	th := make(chan int, limit)
	for s.Scan() {
		wg.Add(1)

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

func writeConn(c net.Conn, out chan []byte) {
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
