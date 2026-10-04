package ratelimiter

import "fmt"

type ratelimiter struct {
	MaxConcurrency int
}

func New(maxConcurancy int) *ratelimiter {
	fmt.Printf("[RATELIMIT] Concurrency throttler set to max %d workers.\n", maxConcurancy)
	return &ratelimiter{MaxConcurrency: maxConcurancy}
}
