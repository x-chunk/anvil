package anvil

import (
	"context"
	"fmt"
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

func (wp *WorkerPool[V]) worker(ctx context.Context) {
	defer wp.wg.Done()
	for job := range wp.tasksChan {
		var res Response[V]

		// Keep draining the queue after cancellation so every submitted
		// task still gets a response.
		if err := ctx.Err(); err != nil {
			res.Err = err
		} else {
			res.Value, res.Err = safeExec(ctx, job.Exec)
		}

		job.Result <- res
	}
}

// safeExec runs exec and converts a panic into an error so that one bad
// task can't take down the worker (and the whole process).
func safeExec[V any](ctx context.Context, exec func(context.Context) (V, error)) (v V, err error) {
	defer func() {
		if r := recover(); r != nil {
			var zero V
			v, err = zero, fmt.Errorf("anvil: task panicked: %v", r)
		}
	}()
	return exec(ctx)
}
