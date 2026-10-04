# anvil

Anvil — a small Go library of generic building blocks: a TTL cache, a worker
pool, channel pipes and a `Result[T]` type.

```sh
go get go.xchunk.org/anvil/v2
```

Requires Go 1.27 or newer.

## What's inside

| | |
|---|---|
| `Cache[K, V]` | In-memory TTL cache. Safe for concurrent use. `GetOrSet` shares one load between concurrent callers, `SetWithTTL` sets a per-item lifetime, expired items are swept automatically. |
| `WorkerPool[V]` | Fixed number of workers fed from a bounded queue. Context-aware, recovers from panics in tasks. |
| `pipes` | `Pipe` / `TransformPipe`: connect channels through a (possibly async, possibly fallible) processing step, with `Pause`/`Resume`. |
| `Result[T]` | A value or an error, with `Of`, `Map`, `AndThen`, `Must`. **Experimental** — the API may change in any release. |

## Examples

```go
// Cache: the loader runs once even if many goroutines ask for the same key.
c := anvil.NewCache[string, User](time.Minute)
u, err := c.GetOrSet("alice", func() (User, error) { return loadUser("alice") })
```

```go
// Worker pool.
wp := anvil.NewWorkerPool[int](4, 16) // 4 workers, queue of 16
wp.Start(ctx)
defer wp.Shutdown()

result := make(chan anvil.Response[int], 1) // buffered: the worker never blocks on it
wp.Submit(anvil.Task[int]{Result: result, Exec: compute})
res := <-result
```

```go
// Pipe: run a fallible step on up to 4 values at a time.
in, out := make(chan string), make(chan int)
p := pipes.NewTransformPipe(in, out,
	pipes.WithTransformMiddlewareErr(func(_ context.Context, s string) (int, error) { return strconv.Atoi(s) }),
	pipes.WithTransformConcurrency[string, int](4),
	pipes.WithTransformErrorHandler[string, int](func(err error) { log.Println(err) }),
)
go p.Start(ctx)
```

More runnable examples live in `example_test.go` and `pipes/example_test.go`;
see also `go doc`.

## Versioning

The module follows [semantic versioning](https://semver.org). Deprecated and
experimental APIs are marked as such in their doc comments.
