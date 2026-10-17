package ratelimiter

import (
	"context"
	"time"
)

type Profile interface {
	RPS(t time.Duration) float64
	Duration() time.Duration
}

type Constant struct {
	RPSValue float64
}

func (c Constant) RPS(time.Duration) float64 {
	return c.RPSValue
}

func (c Constant) Duration() time.Duration {
	return 60 * time.Second
}

type Ramp struct {
	From, To float64
	Over     time.Duration
}

func (r Ramp) RPS(t time.Duration) float64 {
	if t >= r.Over {
		return r.To
	}
	frac := float64(t) / float64(r.Over)
	return r.From + (r.To-r.From)*frac
}

func (r Ramp) Duration() time.Duration { return r.Over }

type Spike struct {
	Base, Peak float64
	Period     time.Duration
	PeakFor    time.Duration
}

func (s Spike) RPS(t time.Duration) float64 {
	phase := t % s.Period
	if phase < s.PeakFor {
		return s.Peak
	}
	return s.Base
}

func (s Spike) Duration() time.Duration { return 60 * time.Second }

// Run drives the limiter according to a profile

func Run(ctx context.Context, l *Limiter, p Profile) {
	start := time.Now()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	l.SetRate(p.RPS(0))
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			elapsed := time.Since(start)
			if p.Duration() > 0 && elapsed > p.Duration() {
				return
			}
			l.SetRate(p.RPS(elapsed))
		}
	}
}
