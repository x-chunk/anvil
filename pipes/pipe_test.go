package pipes

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.xchunk.org/anvil/v2/worker"
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
	wp := worker.New[int](2, 1)
	wp.Start(context.Background())
	defer wp.Shutdown()

	in, out := make(chan int), make(chan int)
	p := NewPipe(in, out,
		WithAsync[int, int](),
		WithWorkerPool[int, int](wp),
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
		WithAsync[int, int](),
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

func TestPipeCancelDoesNotCloseIn(t *testing.T) {
	in, out := make(chan int, 1), make(chan string)
	p := NewPipe(in, out, WithMiddleware(func(v int) string { return "x" }))

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
			return NewPipe(in, make(chan int), WithAsync[int, int](), WithMiddleware(func(v int) int { return v })).Start(ctx)
		},
		"transform": func(ctx context.Context, in chan int) error {
			return NewPipe(in, make(chan int), WithMiddleware(func(v int) int { return v })).Start(ctx)
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
	wp := worker.New[int](1, 1)
	wp.Start(context.Background())
	wp.Shutdown()

	in, out := make(chan int, 1), make(chan int)
	p := NewPipe(in, out,
		WithAsync[int, int](),
		WithWorkerPool[int, int](wp),
		WithMiddleware(func(v int) int { return v }),
	)
	in <- 1

	done := make(chan error, 1)
	go func() { done <- p.Start(context.Background()) }()

	if err, _ := recv(t, done); !errors.Is(err, worker.ErrClosed) {
		t.Fatalf("Start returned %v, want ErrPoolClosed", err)
	}
}

func TestPipeCancelWhileWorkerPoolQueueFull(t *testing.T) {
	wp := worker.New[int](1, 0)
	wp.Start(context.Background())
	defer wp.Shutdown()

	block := make(chan struct{})
	defer close(block)

	in, out := make(chan int, 3), make(chan int, 3)
	p := NewPipe(in, out,
		WithAsync[int, int](),
		WithWorkerPool[int, int](wp),
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

func TestPipePauseResume(t *testing.T) {
	in, out := make(chan int, 1), make(chan string, 1)
	p := NewPipe(in, out, WithMiddleware(func(v int) string { return "x" }))

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

func TestPipeWithoutMiddlewareForwards(t *testing.T) {
	in, out := make(chan string), make(chan string)
	go NewPipe(in, out).Start(context.Background())

	in <- "a"
	if got, _ := recv(t, out); got != "a" {
		t.Fatalf("got %q, want a", got)
	}
	close(in)
}

func TestPipeWithoutMiddlewareForwardsNilInterface(t *testing.T) {
	in, out := make(chan error), make(chan error)
	go NewPipe(in, out).Start(context.Background())

	in <- nil
	if got, ok := recv(t, out); !ok || got != nil {
		t.Fatalf("got %v, %v; want nil, true", got, ok)
	}
	close(in)
}

func TestPipeWithoutMiddlewareReturnsError(t *testing.T) {
	in, out := make(chan int), make(chan string)
	p := NewPipe[int, string](in, out)

	if err := p.Start(context.Background()); !errors.Is(err, ErrMiddlewareRequired) {
		t.Fatalf("Start returned %v, want ErrMiddlewareRequired", err)
	}
	if _, ok := <-out; ok {
		t.Fatal("out should be closed after a failed Start")
	}
}

func TestPipeAsync(t *testing.T) {
	in, out := make(chan int), make(chan string)
	p := NewPipe(in, out,
		WithAsync[int, string](),
		WithMiddleware(func(v int) string { return strconv.Itoa(v) }),
	)
	go p.Start(context.Background())

	go func() {
		for i := 0; i < 5; i++ {
			in <- i
		}
		close(in)
	}()

	var got []string
	for v := range out {
		got = append(got, v)
	}
	sort.Strings(got)
	if want := "0 1 2 3 4"; strings.Join(got, " ") != want {
		t.Fatalf("got %v, want %s", got, want)
	}
}

func TestPipeWorkerPool(t *testing.T) {
	wp := worker.New[string](2, 1)
	wp.Start(context.Background())
	defer wp.Shutdown()

	in, out := make(chan int), make(chan string)
	p := NewPipe(in, out,
		WithAsync[int, string](),
		WithWorkerPool[int, string](wp),
		WithMiddleware(func(v int) string { return strconv.Itoa(v * 2) }),
	)
	go p.Start(context.Background())

	go func() {
		for i := 1; i <= 6; i++ {
			in <- i
		}
		close(in)
	}()

	n := 0
	for range out {
		n++
	}
	if n != 6 {
		t.Fatalf("received %d results, want 6", n)
	}
}

func TestWithConcurrencyLimitsInFlightCalls(t *testing.T) {
	const limit, total = 3, 12

	var cur, peak atomic.Int32
	in, out := make(chan int), make(chan int, total)
	p := NewPipe(in, out,
		WithConcurrency[int, int](limit), // no WithAsync needed
		WithMiddleware(func(v int) int {
			n := cur.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			cur.Add(-1)
			return v
		}),
	)
	go p.Start(context.Background())

	go func() {
		for i := 0; i < total; i++ {
			in <- i
		}
		close(in)
	}()

	n := 0
	for range out {
		n++
	}
	if n != total {
		t.Fatalf("got %d results, want %d", n, total)
	}
	if got := peak.Load(); got != limit {
		t.Fatalf("peak concurrency = %d, want exactly %d", got, limit)
	}
}

func TestWithConcurrencyRejectsNonPositive(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()
	WithConcurrency[int, int](0)
}

func TestWithConcurrencyAndWorkerPoolLastWins(t *testing.T) {
	wp := worker.New[int](1, 1)

	p := NewPipe[int](nil, nil, WithWorkerPool[int, int](wp), WithConcurrency[int, int](2))
	if p.workerPool != nil || p.concurrency != 2 {
		t.Fatalf("WithConcurrency after WithWorkerPool: pool=%v concurrency=%d", p.workerPool, p.concurrency)
	}

	p = NewPipe[int](nil, nil, WithConcurrency[int, int](2), WithWorkerPool[int, int](wp))
	if p.workerPool != wp || p.concurrency != 0 {
		t.Fatalf("WithWorkerPool after WithConcurrency: pool=%v concurrency=%d", p.workerPool, p.concurrency)
	}
}

func parsePositive(_ context.Context, s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err == nil && n < 0 {
		err = errors.New("negative")
	}
	return n, err
}

func collect[T any](ch <-chan T) []T {
	var all []T
	for v := range ch {
		all = append(all, v)
	}
	return all
}

func TestMiddlewareErrSkipsFailedValuesAndReportsThem(t *testing.T) {
	for name, opts := range map[string][]Option[string, int]{
		"sync":        nil,
		"async":       {WithAsync[string, int]()},
		"concurrency": {WithConcurrency[string, int](2)},
	} {
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var errs []error

			in, out := make(chan string), make(chan int)
			opts = append(opts,
				WithMiddlewareErr(parsePositive),
				WithErrorHandler[string, int](func(err error) {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}),
			)
			go NewPipe(in, out, opts...).Start(context.Background())

			go func() {
				for _, s := range []string{"1", "x", "2", "-3", "4"} {
					in <- s
				}
				close(in)
			}()

			got := collect(out)
			sort.Ints(got)
			if fmt.Sprint(got) != "[1 2 4]" {
				t.Fatalf("results = %v, want [1 2 4]", got)
			}
			if len(errs) != 2 {
				t.Fatalf("handler got %d errors (%v), want 2", len(errs), errs)
			}
		})
	}
}

func TestMiddlewareErrWithoutHandlerDropsSilently(t *testing.T) {
	in, out := make(chan string), make(chan int)
	go NewPipe(in, out, WithMiddlewareErr(parsePositive)).Start(context.Background())

	go func() { in <- "bad"; in <- "7"; close(in) }()

	if got := collect(out); fmt.Sprint(got) != "[7]" {
		t.Fatalf("results = %v, want [7]", got)
	}
}

func TestMiddlewareErrReceivesStartContext(t *testing.T) {
	type key struct{}
	ctx := context.WithValue(context.Background(), key{}, 99)

	in, out := make(chan int), make(chan int)
	go NewPipe(in, out, WithMiddlewareErr(func(ctx context.Context, v int) (int, error) {
		return ctx.Value(key{}).(int), nil
	})).Start(ctx)

	in <- 0
	if got, _ := recv(t, out); got != 99 {
		t.Fatalf("got %d, want the value from the Start context", got)
	}
	close(in)
}

func TestWorkerPoolFailuresGoToErrorHandler(t *testing.T) {
	wp := worker.New[int](1, 1)
	wp.Start(context.Background())
	defer wp.Shutdown()

	errc := make(chan error, 1)
	in, out := make(chan string), make(chan int)
	p := NewPipe(in, out,
		WithAsync[string, int](),
		WithWorkerPool[string, int](wp),
		WithMiddlewareErr(parsePositive),
		WithErrorHandler[string, int](func(err error) { errc <- err }),
	)
	go p.Start(context.Background())

	go func() { in <- "nope"; in <- "5"; close(in) }()

	if got := collect(out); fmt.Sprint(got) != "[5]" {
		t.Fatalf("results = %v, want [5]", got)
	}
	if err, _ := recv(t, errc); err == nil {
		t.Fatal("handler got a nil error")
	}
}

func TestMiddlewareAndMiddlewareErrLastWins(t *testing.T) {
	plain := func(v int) int { return v }
	failing := func(context.Context, int) (int, error) { return 0, errTest }

	p := NewPipe[int](nil, nil, WithMiddleware(plain), WithMiddlewareErr(failing))
	if p.middleware != nil || p.errMiddleware == nil {
		t.Fatal("WithMiddlewareErr after WithMiddleware should replace it")
	}
	p = NewPipe[int](nil, nil, WithMiddlewareErr(failing), WithMiddleware(plain))
	if p.middleware == nil || p.errMiddleware != nil {
		t.Fatal("WithMiddleware after WithMiddlewareErr should replace it")
	}
}

var errTest = errors.New("test error")
