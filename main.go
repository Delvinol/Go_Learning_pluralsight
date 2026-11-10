// HERE IN THIS PROJETC, WE ARE JUST TRYING TO SIMULATWE SOME NETWORK TRAFFIC 
package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
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