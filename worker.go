package anvil

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrPoolClosed is returned by Submit after the pool has been shut down.
var ErrPoolClosed = errors.New("anvil: worker pool is closed")

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

	mu     sync.RWMutex
	closed bool
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

// Submit enqueues a task, blocking while the queue is full. It returns
// ErrPoolClosed if the pool has been shut down.
func (wp *WorkerPool[V]) Submit(task Task[V]) error {
	// The read lock keeps Shutdown from closing the channel under a
	// concurrent send. Workers keep draining the queue until it is closed,
	// so a Submit blocked on a full queue can't stall Shutdown forever.
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if wp.closed {
		return ErrPoolClosed
	}
	wp.tasksChan <- task
	return nil
}

// Shutdown stops accepting tasks, lets queued tasks finish and waits for
// the workers to exit. It is safe to call more than once.
func (wp *WorkerPool[V]) Shutdown() {
	wp.mu.Lock()
	if !wp.closed {
		wp.closed = true
		close(wp.tasksChan)
	}
	wp.mu.Unlock()

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

		// A nil Result means the caller doesn't care about the outcome;
		// sending on a nil channel would block this worker forever.
		if job.Result != nil {
			job.Result <- res
		}
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
