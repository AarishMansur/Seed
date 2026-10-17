package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/AarishMansur/services/workers/channel"
	"github.com/AarishMansur/services/workers/client"
	"github.com/AarishMansur/services/workers/ratelimiter"
	"github.com/AarishMansur/services/workers/runner"
)

func main() {
	profileName := flag.String("profile", "constant", "traffic profile: constant | ramp | spike")
	targetRPS := flag.Float64("rps", 500, "target RPS for constant profile")
	flag.Parse()

	var profile ratelimiter.Profile
	switch *profileName {
	case "ramp":
		profile = ratelimiter.Ramp{From: 100, To: 2000, Over: 30 * time.Second}
	case "spike":
		profile = ratelimiter.Spike{Base: 200, Peak: 2000, Period: 10 * time.Second, PeakFor: 2 * time.Second}
	default:
		profile = ratelimiter.Constant{RPSValue: *targetRPS}
	}

	fmt.Printf("--- Starting Day 3 Rate-Limited Load Engine [profile=%s] ---\n", *profileName)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	maxWorkers := 100
	httpClient := client.NewTunnedClient(maxWorkers)

	bufferSize := 10000
	pool := runner.NewPool(maxWorkers, bufferSize, httpClient)
	var wg sync.WaitGroup

	pool.Start(ctx, &wg)

	targetJob := channel.Job{
		Id:     1,
		Method: "GET",
		URL:    "http://127.0.0.1:8080/ping",
		Headers: map[string]string{
			"User-Agent": "Go-LoadTester/1.0",
		},
	}

	lim := ratelimiter.New(profile.RPS(0))
	go ratelimiter.Run(ctx, lim, profile)

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(pool.JobChan)
				return
			default:
				if err := lim.Wait(ctx); err != nil {
					close(pool.JobChan)
					return
				}
				pool.Submit(targetJob)
			}
		}
	}()

	var totalRequests int64
	var totalErrors int64

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		var lastCount int64

		for {
			select {
			case <-ctx.Done():
				return
			case res := <-pool.ResultChan:
				atomic.AddInt64(&totalRequests, 1)
				if res.Err != nil || (res.StatusCode >= 400 && res.StatusCode != 0) {
					atomic.AddInt64(&totalErrors, 1)
				}
			case <-ticker.C:
				current := atomic.LoadInt64(&totalRequests)
				errs := atomic.LoadInt64(&totalErrors)
				rps := current - lastCount
				lastCount = current

				fmt.Printf("[METRICS] Current RPS: %d req/s | Total Requests: %d | Errors: %d\n", rps, current, errs)
			}
		}
	}()

	fmt.Println("[MAIN] System operational. Press Ctrl+C to send cancellation context signal.")
	<-ctx.Done()

	fmt.Println("\n[MAIN] Graceful shutdown signal received! Waiting for workers to finish...")
	wg.Wait()
	fmt.Println("--- Worker Process Successfully Shut Down ---")
}
