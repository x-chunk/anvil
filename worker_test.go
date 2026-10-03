package anvil

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for response")
		panic("unreachable")
	}
}

func TestWorkerPassesContextToExec(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, "v")

	wp := NewWorkerPool[string](1, 1)
	wp.Start(ctx)
	defer wp.Shutdown()

	res := make(chan Response[string], 1)
	wp.Submit(Task[string]{
		Result: res,
		Exec: func(ctx context.Context) (string, error) {
			return ctx.Value(key{}).(string), nil
		},
	})
	if got := recv(t, res); got.Value != "v" || got.Err != nil {
		t.Fatalf("got %+v, want value from the pool context", got)
	}
}

func TestWorkerSkipsTasksAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wp := NewWorkerPool[int](1, 1)
	wp.Start(ctx)
	defer wp.Shutdown()
	cancel()

	var ran atomic.Bool
	res := make(chan Response[int], 1)
	wp.Submit(Task[int]{
		Result: res,
		Exec: func(context.Context) (int, error) {
			ran.Store(true)
			return 1, nil
		},
	})
	if got := recv(t, res); !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", got.Err)
	}
	if ran.Load() {
		t.Fatal("Exec ran after the pool context was cancelled")
	}
}

func TestWorkerRecoversFromPanic(t *testing.T) {
	wp := NewWorkerPool[int](1, 2)
	wp.Start(context.Background())
	defer wp.Shutdown()

	bad, good := make(chan Response[int], 1), make(chan Response[int], 1)
	wp.Submit(Task[int]{
		Result: bad,
		Exec:   func(context.Context) (int, error) { panic("boom") },
	})
	wp.Submit(Task[int]{
		Result: good,
		Exec:   func(context.Context) (int, error) { return 7, nil },
	})

	if got := recv(t, bad); got.Err == nil {
		t.Fatal("expected an error from the panicking task")
	}
	// The single worker must have survived to run the next task.
	if got := recv(t, good); got.Value != 7 || got.Err != nil {
		t.Fatalf("got %+v, want 7", got)
	}
}

func TestWorkerNilResultChannel(t *testing.T) {
	wp := NewWorkerPool[int](1, 2)
	wp.Start(context.Background())
	defer wp.Shutdown()

	wp.Submit(Task[int]{Exec: func(context.Context) (int, error) { return 1, nil }})

	// The only worker must not be stuck on the fire-and-forget task.
	res := make(chan Response[int], 1)
	wp.Submit(Task[int]{
		Result: res,
		Exec:   func(context.Context) (int, error) { return 2, nil },
	})
	if got := recv(t, res); got.Value != 2 {
		t.Fatalf("got %+v, want 2", got)
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	wp := NewWorkerPool[int](2, 1)
	wp.Start(context.Background())

	wp.Shutdown()
	wp.Shutdown() // used to panic: close of closed channel
}

func TestSubmitAfterShutdown(t *testing.T) {
	wp := NewWorkerPool[int](1, 1)
	wp.Start(context.Background())
	wp.Shutdown()

	err := wp.Submit(Task[int]{Exec: func(context.Context) (int, error) { return 0, nil }})
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Submit err = %v, want ErrPoolClosed", err)
	}
}

func TestSubmitRacingShutdown(t *testing.T) {
	wp := NewWorkerPool[int](2, 1)
	wp.Start(context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wp.Submit(Task[int]{Exec: func(context.Context) (int, error) { return 0, nil }})
		}()
	}
	wp.Shutdown()
	wg.Wait()
}
