# anvil

Anvil — a small Go library of generic building blocks: a TTL cache, a worker
pool, channel pipes and a `Result[T]` type.

```sh
go get go.xchunk.org/anvil/v2
```

Requires Go 1.27 or newer. The code of the previous major version lives on the
`legacy/v1` branch (`go.xchunk.org/anvil`, v1.x tags).

## Packages

| Package | |
|---|---|
| [`cache`](cache) | In-memory TTL cache, safe for concurrent use. `GetOrSet` shares one load between concurrent callers, `SetWithTTL` sets a per-item lifetime, expired items are swept automatically. A TTL of 0 means no expiry. |
| [`cache/v2`](cache/v2) | Same API as `cache`, but expiry is checked against a shared coarse clock (an atomic refreshed every 1ms) instead of `time.Now`, which makes `Get` several times faster. Items may outlive their TTL by up to a tick. Sharded to keep parallel readers from contending, optionally bounded with `WithMaxEntries`, `GetOrSetContext` for cancelable waits. `WithClock` sets a custom clock. **Unstable.** Performance comparison with `cache`: see the [package docs](cache/v2/cache.go) and [`cache/get_bench_test.go`](cache/get_bench_test.go). |
| [`worker`](worker) | `Pool`: a fixed number of workers fed from a bounded queue. Context-aware, recovers from panics in tasks. |
| [`pipes`](pipes) | `Pipe[In, Out]`: connects channels through a (possibly concurrent, possibly fallible) processing step, with `Pause`/`Resume`. |
| [`result`](result) | `Result[T]`: a value or an error, with `Of`, `Map`, `AndThen`, `OrElse`, `MapErr`, `UnwrapOrElse`, `All`, `Must`. **Experimental** — the API may change in any release. |

## Examples

```go
// Cache: the loader runs once even if many goroutines ask for the same key.
c := cache.New[string, User](time.Minute)
u, err := c.GetOrSet("alice", func() (User, error) { return loadUser("alice") })
```

```go
// Worker pool.
wp := worker.New[int](4, 16) // 4 workers, queue of 16
wp.Start(ctx)
defer wp.Shutdown()

result := make(chan worker.Response[int], 1) // buffered: the worker never blocks on it
wp.Submit(worker.Task[int]{Result: result, Exec: compute})
res := <-result
```

```go
// Pipe: run a fallible step on up to 4 values at a time.
in, out := make(chan string), make(chan int)
p := pipes.NewPipe(in, out,
	pipes.WithMiddleware(func(_ context.Context, s string) (int, error) { return strconv.Atoi(s) }),
	pipes.WithConcurrency[string, int](4),
	pipes.WithErrorHandler[string, int](func(err error) { log.Println(err) }),
)
go p.Start(ctx)
```

```go
// Result: chain fallible steps with generic methods.
n := result.Of(strconv.Atoi("12")).
	Map(func(n int) int { return n * 2 }).
	UnwrapOr(-1)
```

More runnable examples live next to the code in `example_test.go` files;
see also `go doc`.

## Migrating from v1

| v1 | v2 |
|---|---|
| `anvil.NewCache`, `anvil.CacheItem` | `cache.New`, `cache.Item` |
| `Cache` with `ttl <= 0` expired items immediately | `ttl <= 0` means no expiry |
| `anvil.NewWorkerPool`, `anvil.WorkerPool`, `anvil.ErrPoolClosed` | `worker.New`, `worker.Pool`, `worker.ErrClosed` |
| `anvil.Result` and friends | `result.Result` and friends |
| `Map(r, f)`, `AndThen(r, f)` | `r.Map(f)`, `r.AndThen(f)` |
| `FSM`, `FSMComparable` | removed |
| `pipes.Pipe[T]`, `pipes.TransformPipe[In, Out]` | `pipes.Pipe[In, Out]` (`Pipe[T, T]` for one type) |
| `NewTransformPipe`, `WithTransform*` | `NewPipe`, `With*` |
| `Middleware` as `func(In) Out`, `ErrMiddleware`, `WithMiddlewareErr` | `Middleware` is `func(ctx, In) (Out, error)`, set with `WithMiddleware` |
| `WithAsync` | removed; use `WithConcurrency` or `WithWorkerPool` |
| `Pull`, `Push` | removed; `Read` and `Write` honor `Pause` |
| `Lock`, `Unlock`, `Locked` | removed; use `Pause` and `Resume` |

## Versioning

The module follows [semantic versioning](https://semver.org). Experimental
APIs are marked as such in their doc comments.
