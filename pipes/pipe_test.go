package pipes

import (
	"context"
	"errors"
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

func TestTransformPipeCancelDoesNotCloseIn(t *testing.T) {
	in, out := make(chan int, 1), make(chan string)
	p := NewTransformPipe(in, out, WithTransformMiddleware(func(v int) string { return "x" }))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	cancel()
	if err, _ := recv(t, done); err != context.Canceled {
		t.Fatalf("Start returned %v, want context.Canceled", err)
	}

	// Writing after cancellation must not panic on a closed channel.
	p.Push(1)
}

func TestStartReturnsOnCancelWithoutConsumer(t *testing.T) {
	cases := map[string]func(ctx context.Context, in chan int) error{
		"pipe": func(ctx context.Context, in chan int) error {
			return NewPipe(in, make(chan int)).Start(ctx)
		},
		"pipe-sync-middleware": func(ctx context.Context, in chan int) error {
			return NewPipe(in, make(chan int), WithMiddleware(func(v int) int { return v })).Start(ctx)
		},
		"pipe-async": func(ctx context.Context, in chan int) error {
			return NewPipe(in, make(chan int), WithAsync[int](), WithMiddleware(func(v int) int { return v })).Start(ctx)
		},
		"transform": func(ctx context.Context, in chan int) error {
			return NewTransformPipe(in, make(chan int), WithTransformMiddleware(func(v int) int { return v })).Start(ctx)
		},
	}
	for name, start := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			in := make(chan int, 1)
			in <- 1 // nobody reads out, so the send would block forever

			done := make(chan error, 1)
			go func() { done <- start(ctx, in) }()

			time.Sleep(50 * time.Millisecond)
			cancel()
			if err, _ := recv(t, done); err != context.Canceled {
				t.Fatalf("Start returned %v, want context.Canceled", err)
			}
		})
	}
}

func TestPipeReturnsErrorWhenWorkerPoolClosed(t *testing.T) {
	wp := anvil.NewWorkerPool[int](1, 1)
	wp.Start(context.Background())
	wp.Shutdown()

	in, out := make(chan int, 1), make(chan int)
	p := NewPipe(in, out,
		WithAsync[int](),
		WithWorkerPool(wp),
		WithMiddleware(func(v int) int { return v }),
	)
	in <- 1

	done := make(chan error, 1)
	go func() { done <- p.Start(context.Background()) }()

	if err, _ := recv(t, done); !errors.Is(err, anvil.ErrPoolClosed) {
		t.Fatalf("Start returned %v, want ErrPoolClosed", err)
	}
}

func TestPipeCancelWhileWorkerPoolQueueFull(t *testing.T) {
	wp := anvil.NewWorkerPool[int](1, 0)
	wp.Start(context.Background())
	defer wp.Shutdown()

	block := make(chan struct{})
	defer close(block)

	in, out := make(chan int, 3), make(chan int, 3)
	p := NewPipe(in, out,
		WithAsync[int](),
		WithWorkerPool(wp),
		WithMiddleware(func(v int) int { <-block; return v }),
	)
	for i := 0; i < 3; i++ {
		in <- i
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	// The single worker is stuck in the middleware and the queue has no
	// room, so Start is blocked in Submit.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Start returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

func TestPauseBlocksReadAndWriteUntilResume(t *testing.T) {
	in, out := make(chan int, 1), make(chan int, 1)
	p := NewPipe(in, out)

	p.Pause()

	wrote := make(chan struct{})
	go func() { p.Write(1); close(wrote) }()

	select {
	case <-wrote:
		t.Fatal("Write did not block while paused")
	case <-time.After(50 * time.Millisecond):
	}

	p.Resume()
	recv(t, wrote)

	// Same for Read.
	out <- 9
	p.Pause()
	read := make(chan int, 1)
	go func() { v, _ := p.Read(); read <- v }()
	select {
	case <-read:
		t.Fatal("Read did not block while paused")
	case <-time.After(50 * time.Millisecond):
	}
	p.Resume()
	if v, _ := recv(t, read); v != 9 {
		t.Fatalf("Read = %d, want 9", v)
	}
}

func TestPauseDoesNotAffectPushAndPull(t *testing.T) {
	in, out := make(chan int, 1), make(chan int, 1)
	p := NewPipe(in, out)
	p.Pause()
	defer p.Resume()

	p.Push(1) // must not block
	out <- 2
	if v, _ := p.Pull(); v != 2 {
		t.Fatalf("Pull = %d, want 2", v)
	}
}

func TestTransformPipePauseResume(t *testing.T) {
	in, out := make(chan int, 1), make(chan string, 1)
	p := NewTransformPipe(in, out, WithTransformMiddleware(func(v int) string { return "x" }))

	p.Pause()
	wrote := make(chan struct{})
	go func() { p.Write(1); close(wrote) }()
	select {
	case <-wrote:
		t.Fatal("Write did not block while paused")
	case <-time.After(50 * time.Millisecond):
	}
	p.Resume()
	recv(t, wrote)
}

func TestDeprecatedLockUnlockStillWork(t *testing.T) {
	in, out := make(chan int, 1), make(chan int, 1)
	p := NewPipe(in, out)

	p.Lock()
	wrote := make(chan struct{})
	go func() { p.Write(1); close(wrote) }()
	select {
	case <-wrote:
		t.Fatal("Write did not block after Lock")
	case <-time.After(50 * time.Millisecond):
	}
	p.Unlock()
	recv(t, wrote)
}

func TestLockedZeroValueIsUsable(t *testing.T) {
	var l Locked
	l.Wait() // not paused: must return immediately

	l.set(true)
	released := make(chan struct{})
	go func() { l.Wait(); close(released) }()

	select {
	case <-released:
		t.Fatal("Wait returned while paused")
	case <-time.After(50 * time.Millisecond):
	}
	l.set(false)
	recv(t, released)
}
