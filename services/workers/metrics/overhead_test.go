package metrics

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestGoroutineAndCPUUnderHighFreqTicks(t *testing.T) {
	baseline := runtime.NumGoroutine()
	cpuStart := processCPUTime()
	wallStart := time.Now()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for _, interval := range []time.Duration{time.Millisecond, 5 * time.Millisecond, 50 * time.Millisecond} {
		wg.Add(1)
		go func(d time.Duration) {
			defer wg.Done()
			tick := time.NewTicker(d)
			defer tick.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
				}
			}
		}(interval)
	}

	peak := baseline
	sampleTick := time.NewTicker(500 * time.Millisecond)
	deadline := time.After(5 * time.Second)
sample:
	for {
		select {
		case <-sampleTick.C:
			n := runtime.NumGoroutine()
			if n > peak {
				peak = n
			}
			t.Logf("[SAMPLE] goroutines=%d cpu=%.1f%%", n,
				float64(processCPUTime()-cpuStart)/float64(time.Since(wallStart))*100)
		case <-deadline:
			break sample
		}
	}
	sampleTick.Stop()

	close(stop)
	wg.Wait()

	cpuUsed := processCPUTime() - cpuStart
	wall := time.Since(wallStart)
	cpuPct := float64(cpuUsed) / float64(wall) * 100
	final := runtime.NumGoroutine()

	t.Logf("[OVERHEAD] baseline=%d peak=%d final=%d | cpu=%.1f%% over %s",
		baseline, peak, final, cpuPct, wall.Round(time.Millisecond))

	if peak < baseline+3 {
		t.Errorf("expected tick goroutines to show up, baseline=%d peak=%d", baseline, peak)
	}
	if final > baseline+1 {
		t.Errorf("goroutine leak: baseline=%d final=%d", baseline, final)
	}
}
