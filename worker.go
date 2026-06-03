package anvil

import (
	"context"
	"sync"
)

type Response[V any] struct {
	Value V
	Err   error
}

type Task[V any] struct {
	Exec   func(ctx context.Context) (V, error)
	Result chan Response[V]
}

type WorkerPool[V any] struct {
	tasksChan chan Task[V]
	wg        sync.WaitGroup
	size      int
}

func NewWorkerPool[V any](size, queueSize int) *WorkerPool[V] {
	return &WorkerPool[V]{
		tasksChan: make(chan Task[V], queueSize),
		size:      size,
	}
}

func (wp *WorkerPool[V]) Start(ctx context.Context) {
	for i := 0; i < wp.size; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx)
	}
}

func (wp *WorkerPool[V]) Submit(task Task[V]) {
	wp.tasksChan <- task
}

func (wp *WorkerPool[K]) Shutdown() {
	close(wp.tasksChan)
	wp.wg.Wait()
}

func (wp *WorkerPool[V]) worker(_ context.Context) {
	defer wp.wg.Done()
	for job := range wp.tasksChan {
		var res Response[V]

		v, err := job.Exec(context.Background())
		res.Value = v
		res.Err = err

		job.Result <- res
	}
}
