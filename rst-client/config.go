package main

import (
	"flag"
	"time"
)

var (
	server  string
	total   int
	timeout time.Duration
)

func init() {
	flag.StringVar(&server, "s", ":5555", "address:port of server")
	flag.IntVar(&total, "n", 5, "number of requests to send")
	flag.DurationVar(&timeout, "t", 2*time.Minute, "time to wait for responses")

	flag.Parse()
}
