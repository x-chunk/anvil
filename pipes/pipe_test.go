package pipes

import (
	"context"
	"testing"
	"time"
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
