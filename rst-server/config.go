package main

import "flag"

var (
	address string
	bufSize int
	limit   int
)

func init() {
	flag.StringVar(&address, "a", ":5555", "address:port for requests")
	flag.IntVar(&bufSize, "b", 1400, "sender buffer size")
	flag.IntVar(&limit, "l", 500, "max number of requests to handle in parallel")

	flag.Parse()
}
