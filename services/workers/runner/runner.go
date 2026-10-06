package runner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/AarishMansur/services/workers/channel"
)

type Task struct {
	ID int
}

type WorkerPool struct {
	workerCount int
	client      *http.Client
	JobChan     chan channel.Job
	ResultChan  chan channel.Result
}

func NewPool(workerCount int, bufferSize int, client *http.Client) *WorkerPool {
	return &WorkerPool{
		workerCount: workerCount,
		client:      client,
		JobChan:     make(chan channel.Job, bufferSize),
		ResultChan:  make(chan channel.Result, bufferSize),
	}

}

func (p *WorkerPool) Start(ctx context.Context, wg *sync.WaitGroup) {
	fmt.Printf("[RUNNER] starting pool with %d worker channel", p.workerCount)
	for i := 1; i <= p.workerCount; i++ {
		wg.Add(1)
		go p.worker(ctx, wg, i)
	}
}

func (p *WorkerPool) worker(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	fmt.Printf("[RUNNER] worker %d listening for jobs..", id)
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[RUNNER] workerd %d recieved cancel signals. Shutting down\n", id)
			return
		case job, ok := <-p.JobChan:
			if !ok {
				fmt.Printf("[RUNNER] Worker %d channel closed. Exiting.\n", id)
				return
			}
			p.execute(ctx, job)
		}

	}
}

func (p *WorkerPool) execute(ctx context.Context, job channel.Job) {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, job.Method, job.URL, nil)
	if err != nil {
		p.ResultChan <- channel.Result{JobId: job.Id, Err: err}
		return
	}
	for k, v := range job.Headers {
		req.Header.Set(k, v)
	}
	resp, err := p.client.Do(req)
	duration := time.Since(start)

	if err != nil {
		p.ResultChan <- channel.Result{JobId: job.Id, Duration: duration, Err: err}
		return
	}

	bytesFetches, _ := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	p.ResultChan <- channel.Result{
		JobId:        job.Id,
		StatusCode:   resp.StatusCode,
		Duration:     duration,
		BytesFetched: bytesFetches,
		Err:          nil,
	}

}

func (p *WorkerPool) Submit(j channel.Job) {
	p.JobChan <- j
}
