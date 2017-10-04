package main

type bySend []*pair

func (s bySend) Len() int           { return len(s) }
func (s bySend) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s bySend) Less(i, j int) bool { return s[i].sent.Before(s[j].sent) }

type byRecive []*pair

func (s byRecive) Len() int      { return len(s) }
func (s byRecive) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byRecive) Less(i, j int) bool {
	if s[i].recv == nil {
		return false
	}

	if s[j].recv == nil {
		return true
	}

	return s[i].recv.Before(*s[j].recv)
}

type timings struct {
	Sends    []int64   `json:"sends"`
	Receives []int64   `json:"receives"`
	Pairs    [][]int64 `json:"pairs"`
}
