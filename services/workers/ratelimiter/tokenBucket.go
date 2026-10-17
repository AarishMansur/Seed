package ratelimiter

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

type TokenBucket struct {
	mu             sync.Mutex
	capacity       float64
	tokens         float64
	refillRate     float64
	lastRefillTime time.Time
}

func NewTokenBucket(requestsPerSecond float64, capacity int) (*TokenBucket, error) {
	if requestsPerSecond <= 0 || math.IsNaN(requestsPerSecond) || math.IsInf(requestsPerSecond, 0) {
		return nil, errors.New("request per second must be a positive number")
	}
	if capacity < 1 {
		return nil, errors.New("Capacity must be atleast 1")
	}
	now := time.Now()
	return &TokenBucket{
		capacity:       float64(capacity),
		tokens:         float64(capacity),
		refillRate:     requestsPerSecond,
		lastRefillTime: now,
	}, nil
}

func (b *TokenBucket) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.mu.Lock()
		b.refill(time.Now())

		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		waitSeconds := (1 - b.tokens) / b.refillRate
		waitDuration := time.Duration(waitSeconds * float64(time.Second))
		if waitDuration < time.Nanosecond {
			waitDuration = time.Nanosecond
		}
		b.mu.Unlock()
		timer := time.NewTicker(waitDuration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (b *TokenBucket) refill(now time.Time) {
	elapsed := now.Sub(b.lastRefillTime).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed * b.refillRate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.lastRefillTime = now
}
