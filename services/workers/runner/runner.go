package runner

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Task struct {
	ID int
}

type WorkerPool struct {
	workerCount int
	Taskchan    chan Task
}

func NewPool(workerCount int, bufferSize int) *WorkerPool {
	return &WorkerPool{
		workerCount: workerCount,
		Taskchan:    make(chan Task, bufferSize),
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
		case Task, ok := <-p.Taskchan:
			if !ok {
				fmt.Printf("[RUNNER] Worker %d channel closed. Exiting.\n", id)
				return
			}
			fmt.Printf("[RUNNER] worker %d processing Task #%d\n", id, Task.ID)
			time.Sleep(500 * time.Millisecond)
		}

	}
}

func (p *WorkerPool) Submit(t Task) {
	p.Taskchan <- t
}
