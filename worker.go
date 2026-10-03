package anvil

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrPoolClosed is returned by Submit after the pool has been shut down.
var ErrPoolClosed = errors.New("anvil: worker pool is closed")

// Response is the outcome of a Task: the value returned by Exec, or the
// error it returned. A panic in Exec and a cancelled pool context are also
// reported through Err.
type Response[V any] struct {
	Value V
	Err   error
}

// Task is a unit of work for a WorkerPool.
type Task[V any] struct {
	// Exec does the work. It receives the context passed to
	// WorkerPool.Start.
	Exec func(ctx context.Context) (V, error)

	// Result receives exactly one Response when the task completes. It may
	// be nil if the outcome is not needed. The worker sends on it
	// synchronously, so use a buffered channel (capacity >= 1) or make
	// sure a receiver is always waiting; otherwise the worker stays blocked.
	Result chan Response[V]
}

// WorkerPool runs tasks on a fixed number of goroutines fed from a bounded
// queue. Create it with NewWorkerPool, call Start once, submit tasks with
// Submit or SubmitCtx and finish with Shutdown.
type WorkerPool[V any] struct {
	tasksChan chan Task[V]
	wg        sync.WaitGroup
	size      int

	mu     sync.RWMutex
	closed bool

	startOnce sync.Once
}

// NewWorkerPool creates a pool of size workers with room for queueSize
// pending tasks. It panics if size is not positive. Workers are not running
// until Start is called.
func NewWorkerPool[V any](size, queueSize int) *WorkerPool[V] {
	if size <= 0 {
		panic("anvil: worker pool size must be positive")
	}
	return &WorkerPool[V]{
		tasksChan: make(chan Task[V], queueSize),
		size:      size,
	}
}

// Start launches the workers. Only the first call has an effect; later
// calls are ignored, so the pool never runs more than size workers.
func (wp *WorkerPool[V]) Start(ctx context.Context) {
	wp.startOnce.Do(func() {
		for range wp.size {
			wp.wg.Add(1)
			go wp.worker(ctx)
		}
	})
}

// Submit enqueues a task, blocking while the queue is full. It returns
// ErrPoolClosed if the pool has been shut down.
func (wp *WorkerPool[V]) Submit(task Task[V]) error {
	return wp.SubmitCtx(context.Background(), task)
}

// SubmitCtx is like Submit but gives up with ctx.Err() if ctx is done
// before the task could be enqueued.
func (wp *WorkerPool[V]) SubmitCtx(ctx context.Context, task Task[V]) error {
	// The read lock keeps Shutdown from closing the channel under a
	// concurrent send. Workers keep draining the queue until it is closed,
	// so a Submit blocked on a full queue can't stall Shutdown forever.
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if wp.closed {
		return ErrPoolClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case wp.tasksChan <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
