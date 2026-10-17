package ratelimiter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AarishMansur/services/workers/channel"
	"github.com/AarishMansur/services/workers/client"
	"github.com/AarishMansur/services/workers/ratelimiter"
	"github.com/AarishMansur/services/workers/runner"
)

func TestWorkerHoldsTargetRPSWithin5Percent(t *testing.T) {
	if testing.Short() {
		t.Skip("60s accuracy run skipped in -short mode")
	}

	const targetRPS = 500.0
	runFor := 60 * time.Second
	if v := os.Getenv("RPS_TEST_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			runFor = d
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), runFor)
	defer cancel()

	pool := runner.NewPool(50, 4096, client.NewTunnedClient(50))
	var wg sync.WaitGroup
	pool.Start(ctx, &wg)

	lim := ratelimiter.New(targetRPS)

	job := channel.Job{
		Id:     1,
		Method: "GET",
		URL:    srv.URL,
		Headers: map[string]string{
			"User-Agent": "Go-LoadTester/1.0",
		},
	}

	go func() {
		for {
			if err := lim.Wait(ctx); err != nil {
				return
			}
			pool.Submit(job)
		}
	}()

	var completed, errs int64
	perSec := make([]int64, 0, runFor/time.Second+1)
	collectorDone := make(chan struct{})

	go func() {
		defer close(collectorDone)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case res := <-pool.ResultChan:
				atomic.AddInt64(&completed, 1)
				if res.Err != nil {
					atomic.AddInt64(&errs, 1)
				}
			case <-tick.C:
				cur := atomic.LoadInt64(&completed)
				perSec = append(perSec, cur-last)
				last = cur
			}
		}
	}()

	<-ctx.Done()
	<-collectorDone

	// drain results already sitting in the channel
	drainDeadline := time.After(2 * time.Second)
drain:
	for {
		select {
		case <-pool.ResultChan:
			atomic.AddInt64(&completed, 1)
		case <-drainDeadline:
			break drain
		default:
			time.Sleep(20 * time.Millisecond)
		}
	}
	cancel()
	wg.Wait()

	total := atomic.LoadInt64(&completed)
	got := float64(total) / runFor.Seconds()
	dev := (got - targetRPS) / targetRPS * 100

	t.Logf("[RESULT] target=%.0f rps | actual=%.1f rps | deviation=%.2f%% | total=%d | errors=%d",
		targetRPS, got, dev, total, atomic.LoadInt64(&errs))
	t.Logf("[RESULT] per-second RPS: %v", perSec)

	if len(perSec) > 1 {
		min, max := perSec[0], perSec[0]
		for _, v := range perSec {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		t.Logf("[RESULT] per-second min=%d max=%d", min, max)
	}

	if dev > 5 || dev < -5 {
		t.Fatalf("RPS deviation %.2f%% exceeds 5%% margin (target=%.0f actual=%.1f)", dev, targetRPS, got)
	}
}
