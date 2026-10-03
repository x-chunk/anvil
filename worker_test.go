package anvil

import (
	"context"
	"errors"
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
