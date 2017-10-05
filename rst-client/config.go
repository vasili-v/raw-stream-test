package main

import (
	"flag"
	"time"
)

var (
	server    string
	total     int
	msgSize   int
	bufSize   int
	limit     int
	timeout   time.Duration
	waitsDump string
)

func init() {
	flag.StringVar(&server, "s", ":5555", "address:port of server")
	flag.IntVar(&total, "n", 5, "number of requests to send")
	flag.IntVar(&msgSize, "size", 60, "message size")
	flag.IntVar(&bufSize, "b", 1400, "sender buffer size")
	flag.IntVar(&limit, "l", 100, "limit for messages to send ahead")
	flag.DurationVar(&timeout, "t", 2*time.Minute, "time to wait for responses")
	flag.StringVar(&waitsDump, "w", "", "file to dump sending pauses")

	flag.Parse()
}
