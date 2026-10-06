package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/AarishMansur/services/workers/channel"
	"github.com/AarishMansur/services/workers/client"
	"github.com/AarishMansur/services/workers/runner"
)

func main() {
	fmt.Println("--- Starting Day 2 High-Throughput HTTP Load Engine ---")
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

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(pool.JobChan)
				return
			default:
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
