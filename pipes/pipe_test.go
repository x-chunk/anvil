package pipes

import (
	"context"
	"testing"
	"time"

	"go.xchunk.org/anvil"
)

func recv[T any](t *testing.T, ch <-chan T) (T, bool) {
	t.Helper()
	select {
	case v, ok := <-ch:
		return v, ok
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for value")
		panic("unreachable")
	}
}

func TestPipeSyncForwardsMiddlewareResult(t *testing.T) {
	in, out := make(chan int), make(chan int)
	p := NewPipe(in, out, WithMiddleware(func(v int) int { return v * 2 }))

	go p.Start(context.Background())

	in <- 21
	if got, _ := recv(t, out); got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	close(in)
}

func TestPipeWorkerPoolForwardsResults(t *testing.T) {
	wp := anvil.NewWorkerPool[int](2, 1)
	wp.Start(context.Background())
	defer wp.Shutdown()

	in, out := make(chan int), make(chan int)
	p := NewPipe(in, out,
		WithAsync[int](),
		WithWorkerPool(wp),
		WithMiddleware(func(v int) int { return v + 1 }),
	)
	go p.Start(context.Background())

	// More items than pool size + queue: used to hang once workers
	// blocked on an unread result channel.
	const n = 10
	go func() {
		for i := 0; i < n; i++ {
			in <- i
		}
		close(in)
	}()

	sum := 0
	for i := 0; i < n; i++ {
		v, _ := recv(t, out)
		sum += v
	}
	if want := n*(n-1)/2 + n; sum != want {
		t.Fatalf("sum = %d, want %d", sum, want)
	}
	if _, ok := recv(t, out); ok {
		t.Fatal("out should be closed after in is closed")
	}
}

func TestPipeAsyncForwardsResults(t *testing.T) {
	in, out := make(chan int), make(chan int)
	p := NewPipe(in, out,
		WithAsync[int](),
		WithMiddleware(func(v int) int { return v * 10 }),
	)
	go p.Start(context.Background())

	in <- 1
	in <- 2
	close(in)

	got := 0
	for v := range out {
		got += v
	}
	if got != 30 {
		t.Fatalf("sum = %d, want 30", got)
	}
}
