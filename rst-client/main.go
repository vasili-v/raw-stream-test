package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/vasili-v/raw-stream-test/scanner"
	"github.com/vasili-v/raw-stream-test/sender"
)

type pair struct {
	req []byte

	sent time.Time
	recv *time.Time
	dup  int
}

func main() {
	pairs := newPairs(total, msgSize)

	c, err := net.Dial("tcp", server)
	if err != nil {
		panic(fmt.Errorf("dialing error: %s", err))
	}
	defer c.Close()

	miss := 0
	ch := make(chan int)

	th := make(chan int, limit)
	count := len(pairs)
	scn := scanner.NewScanner(c)
	go func() {
		defer close(ch)

		for scn.Scan() {
			<-th
			msg := scn.Bytes()

			if len(msg) < 4 {
				panic(fmt.Errorf("expected message %d at least of 4 bytes but got %d", len(pairs)-count+1, len(msg)))
			}

			id := binary.BigEndian.Uint32(msg)
			//fmt.Fprintf(os.Stderr, "got response %d with id %d of %d size\n", len(pairs)-count+1, id, len(msg))
			if id < uint32(len(pairs)) {
				p := pairs[id]
				if p.recv == nil {
					t := time.Now()
					p.recv = &t
					count--
					if count <= 0 {
						return
					}
				} else {
					p.dup++
				}
			} else {
				miss++
			}
		}
	}()

	snd := sender.NewSender(c, bufSize, bufSize/msgSize+2)
	for _, p := range pairs {
		th <- 0

		p.sent = time.Now()
		if err := snd.Send(p.req, nil); err != nil {
			panic(fmt.Errorf("sending error: %s", err))
		}
	}

	if err := snd.Flush(); err != nil {
		panic(fmt.Errorf("sending error: %s", err))
	}

	snd.Stats(os.Stderr, c.RemoteAddr().String())

	if count > 0 {
		fmt.Fprintf(os.Stderr, "waiting for %d responses\n", count)
	}

	select {
	case <-ch:
	case <-time.After(timeout):
	}

	if err := scn.Err(); err != nil {
		panic(fmt.Errorf("reading error %s", err))
	}

	if count > 0 {
		panic(fmt.Errorf("couldn't receive %d responses", count))
	} else {
		fmt.Fprintf(os.Stderr, "got all %d responses\n", len(pairs))
	}

	if miss > 0 {
		panic(fmt.Errorf("got %d messages with invalid ids", miss))
	}

	dup := 0
	for _, p := range pairs {
		dup += p.dup
	}

	if dup > 0 {
		panic(fmt.Errorf("got %d duplicates", dup))
	}

	dump(pairs, "")
}

func newPairs(n, size int) []*pair {
	out := make([]*pair, n)
	fmt.Fprintf(os.Stderr, "making messages to send:\n")
	for i := range out {
		buf := make([]byte, size)
		binary.BigEndian.PutUint16(buf, uint16(len(buf)-2))
		binary.BigEndian.PutUint32(buf[2:], uint32(i))
		for i := 6; i < len(buf); i++ {
			buf[i] = 0xaa
		}

		if i < 3 {
			fmt.Fprintf(os.Stderr, "\t%d: % x\n", i, buf)
		} else if i == 3 {
			fmt.Fprintf(os.Stderr, "\t%d: ...\n", i)
		}

		out[i] = &pair{req: buf}
	}

	return out
}
