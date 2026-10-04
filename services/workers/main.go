package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/AarishMansur/services/workers/metrics"
	"github.com/AarishMansur/services/workers/ratelimiter"
	"github.com/AarishMansur/services/workers/runner"
)

func main() {
	fmt.Println("--- Starting Worker Engine Process ---")
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	m := metrics.New()
	_ = m

	l := ratelimiter.New(5)
	_ = l

	pool := runner.NewPool(3, 10)
	var wg sync.WaitGroup

	pool.Start(ctx, &wg)

	go func() {
		for i := 0; i <= 3; i++ {
			pool.Submit(runner.Task{ID: i})
			time.Sleep(300 * time.Millisecond)
		}
	}()

	fmt.Println("[MAIN] System operational. Press Ctrl+C to send cancellation context signal.")
	<-ctx.Done()
	fmt.Println("\n[MAIN] Graceful shutdown signal received! Waiting for workers to finish...")
	wg.Wait()
	fmt.Println("--- Worker Process Successfully Shut Down ---")
}
