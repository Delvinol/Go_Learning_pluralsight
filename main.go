// HERE IN THIS PROJETC, WE ARE JUST TRYING TO SIMULATWE SOME NETWORK TRAFFIC / series of network calls
package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	requestCount = 50
	requestLatency = 200 * time.Millisecond
)

type result struct {
	status int
	err error
}

//Progress counts completed fetches and drives the on-screen progress line.
//main resets it at the start of each phase
var progress atomic.Int64

// startServer runs a real net/http server inside this process. Its handler
// sleeps, so the demo has latency without needing a network
func startServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(requestLatency)
		fmt.Fprintln(w, "OK")
	}))
}

func newClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			MaxConnsPerHost:     0,//no limits
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 5 * time.Second,
	}
}


func fetch(c *http.Client, url string) result {
	defer progress.Add(1)

	resp, err := c.Get(url)
	if err != nil {
		return result{err: err}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return result{status: resp.StatusCode}
}

func fetchAll(c *http.Client, urls []string) []result {
	out := make([]result, len(urls))
	for i, u := range urls {
		out[i] = fetch(c, u)
	}
	return out
}

//monitor samples the live goroutine count into peak on a fixed ticker, and --
// only when every > 0 -- prints a progress line every "every".
func monitor(stop <-chan struct{}, label string, every time.Duration, peak *atomic.Int64) {
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	next := time.Now().Add(every)
	for {
		select {
			case <-stop:
				return
			case now := <-t.C:
				if n := int64(runtime.NumGoroutine()); n > peak.Load() {
					peak.Store(n)
				}
				if every > 0 && now.After(next) {
					fmt.Printf(" %-10s %-11s goroutines: %d\n",
					 label, 
					 fmt.Sprintf("%d/%d", progress.Load(), requestCount),
					 runtime.NumGoroutine())
					next = now.Add(every)
				}
		}
	}
}

func okCount(rs []result) int {
	n := 0
	for _, r := range rs {
		if r.err == nil && r.status == http.StatusOK {
			n++
		}
	}
	return n
}

func printPhase(label string, rs []result, elapsed time.Duration, peak int64) {
	fmt.Printf("%-13s %d/%d ok in %-14s peak goroutines: %d\n",
		label, okCount(rs), requestCount, elapsed.Round(time.Millisecond), peak)
}

func main() {
	srv := startServer()
	client := newClient()

	urls := make([]string, requestCount)
	for i := range urls {
		urls[i] = srv.URL
	}

	fmt.Printf("Server: %s | requests: %d | latency per request: %s\n\n",
		srv.URL, requestCount, requestLatency)

	progress.Store(0)
	var seqPeak atomic.Int64
	stopSeq := make(chan struct{})
	go monitor(stopSeq, "sequential", 1*time.Second, &seqPeak)

	seqStart := time.Now()
	seq := fetchAll(client, urls)
	seqElapsed := time.Since(seqStart)
	close(stopSeq)

	printPhase("sequential", seq, seqElapsed, seqPeak.Load())

	srv.Close()
}