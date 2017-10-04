package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func dump(pairs []*pair, path string) error {
	tm := timings{
		Sends:    make([]int64, len(pairs)),
		Receives: make([]int64, len(pairs)),
		Pairs:    make([][]int64, len(pairs)),
	}

	sort.Sort(bySend(pairs))
	for i, p := range pairs {
		tm.Sends[i] = p.sent.UnixNano()
		if p.recv == nil {
			tm.Pairs[i] = []int64{p.sent.UnixNano()}
		} else {
			tm.Pairs[i] = []int64{
				p.sent.UnixNano(),
				p.recv.UnixNano(),
				p.recv.UnixNano() - p.sent.UnixNano(),
			}
		}
	}

	sort.Sort(byRecive(pairs))
	for i, p := range pairs {
		if p.recv != nil {
			tm.Receives[i] = p.recv.UnixNano()
		}
	}

	b, err := json.MarshalIndent(tm, "", "  ")
	if err != nil {
		return fmt.Errorf("can't marshal timings to JSON: %s", err)
	}

	f := os.Stdout
	dstName := "stdout"
	if len(path) > 0 {
		f, err = os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()

		dstName = fmt.Sprintf("file %s", path)
	}

	_, err = f.Write(b)
	if err != nil {
		return fmt.Errorf("can't dump JSON timings to %s: %s", dstName, err)
	}

	return nil
}
