package ratelimiter

import (
	"context"

	"golang.org/x/time/rate"
)

type Limiter struct {
	lim *rate.Limiter
}

func New(rps float64) *Limiter {
	burst := int(rps / 10)
	if burst < 1 {
		burst = 1
	}
	return &Limiter{lim: rate.NewLimiter(rate.Limit(rps), burst)}
}

func (l *Limiter) Wait(ctx context.Context) error {
	return l.lim.Wait(ctx)
}

func (l *Limiter) SetRate(rps float64) {
	l.lim.SetLimit(rate.Limit(rps))
}
