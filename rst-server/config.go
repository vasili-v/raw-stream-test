package main

import "flag"

var (
	address string
	limit   int
)

func init() {
	flag.StringVar(&address, "a", ":5555", "address:port for requests")
	flag.IntVar(&limit, "l", 500, "max number of requests to handle in parallel")

	flag.Parse()
}
