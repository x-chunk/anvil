# Full review of `go.xchunk.org/anvil/v2`

Reviewed at `3e8bf4b` (tag `v2.2.0`, `master`). Scope: `worker`, `pipes`,
`result`, the module layout, docs and CI. The deprecated `cache` package is
out of scope.

Baseline: `go vet ./...` and `go test -race -count=1 ./...` pass on Go 1.27.0.
Findings marked **(reproduced)** were confirmed with throwaway tests that are
not part of this branch; the rest come from reading the code.

Severity: **High** = data loss, deadlock, crash or resource exhaustion in
plausible use; **Medium** = wrong or surprising behavior, API traps;
**Low** = polish.

## Summary

| ID | Sev | Package | Finding | Fix is breaking? |
|---|---|---|---|---|
| P1 | High | pipes | Worker-pool mode has no backpressure: unbounded goroutines and memory | No |
| P2 | High | pipes | Worker-pool mode hands the middleware the pool's context, not the pipe's | No (bug fix) |
| P3 | High | pipes | A middleware panic kills the process in `WithConcurrency` mode | No (bug fix) |
| W1 | High | worker | `Shutdown` deadlocks behind a blocked `Submit`; `SubmitCtx` ignores its context meanwhile | No |
| W2 | High | worker | One unread `Result` channel blocks a worker forever, and `Shutdown` with it | No |
| P4 | Medium | pipes | `Start` twice panics with "close of closed channel" | No |
| P5 | Medium | pipes | The pipe closes a channel it does not own; fan-in is impossible | Yes, has a compromise |
| P6 | Medium | pipes | `Write`/`Read` cannot be cancelled and hang forever after the pipe stops | No (additive) |
| P7 | Medium | pipes | `Pause`/`Resume` semantics are weak and the wait is not cancellable | Partly, has a compromise |
| P8 | Medium | pipes | Errors are dropped silently by default; the handler does not get the value | Yes, has a compromise |
| P9 | Medium | pipes | A cancelled pool context turns the pipe into a silent value shredder | No |
| P10 | Medium | pipes | Values in flight are lost without a trace on cancellation | No |
| W3 | Medium | worker | Lifecycle holes: `Shutdown` before/concurrent with `Start`, submit after cancel, worker leak | No |
| W4 | Medium | worker | Panic errors lose the stack, the panic value and `errors.Is` | No |
| W5 | Medium | worker | `Task`/`Result`-channel API is error-prone; `Pool[V]` is tied to one result type | Yes, has a compromise |
| W6 | Medium | worker | `Shutdown` cannot be bounded, and deadlocks when called from a task | No (additive) |
| A1 | Medium | module | Experimental `result` lives inside a stable module and dictates its Go version | Yes, has a compromise |
| A2 | Medium | module | GPL-3.0 on a building-blocks library | n/a (licensing) |
| R1 | Medium | result | `Error() error` accessor: confusing name, doc comment says `Err` | Experimental |
| R2 | Medium | result | `Err[T](nil)` and the zero `Result` are silently `Ok` | Experimental |
| P11 | Low | pipes | Options need explicit type arguments; `ErrMiddlewareRequired` is a runtime error for a static fact | Yes, has a compromise |
| P12 | Low | pipes | Missing input validation, `WithWorkerPool` coupled to a concrete type, concurrency ignored without a middleware | No |
| W7 | Low | worker | Missing validation (`queueSize`, nil `Exec`, nil ctx), no `TrySubmit`/introspection | No |
| R3 | Low | result | `Must` doc/panic message, no JSON support, `All` keeps only the first error | Experimental |
| A3 | Low | module | CI hardening, missing package docs, test gaps, no changelog | No |

## Is a new major version needed?

The module is already at `v2.2.0`, so "a major update" here means **v3**.

**No finding requires it.** Every High finding (P1, P2, P3, W1, W2) is an
implementation bug whose fix keeps all exported signatures and brings the
behavior in line with what the docs already promise. They belong in a patch
release (`v2.2.1`), and P1–P3 and W1 should not wait.

The design issues that a major version would resolve cleanly (P5, P8, P11,
W5, A1) all have additive compromises, listed with each finding. None of
them is dangerous once documented, so they are a reason to *collect* a v3
wish list, not to ship v3. A sensible v3 trigger would be a decision to
redesign `worker.Task` (W5) and the `pipes` options (P11) together, since
those two shape every call site.

Suggested order:

1. `v2.2.1`: P1, P2, P3, P4, P9, W1, W2 (with the non-breaking variant), W3, doc fixes.
2. `v2.3.0`: additive API from P5, P6, P7, P8, P10, W4, W6, W7, P12.
3. Any time: A1, A2, A3, R1–R3 (`result` is experimental, so its fixes are not bound by semver).

---

## pipes

### P1 (High) Worker-pool mode has no backpressure (reproduced)

**Where:** `164138-165` (`Start`, the `p.workerPool != nil` branch).

**Problem:** Every value gets its own goroutine that waits for the task's
response and then sends it to `out`. The worker itself is released as soon
as it writes into the buffered `resultCh`, so a slow or absent consumer of
`out` does not slow down the intake from `in`. Each pending value costs a
goroutine, a channel and the result. With a pool of 2 workers and a queue of
2, a pipe whose `out` nobody reads accepted 20 000 values and held 20 000
goroutines; `WithConcurrency(2)` in the same setup accepted 3. If `in` is
fed from the network, this is a memory-exhaustion vector.

The pool's `size` and `queueSize` look like the bound, but they only bound
the middleware calls, not the undelivered results.

**Fix:** Deliver results through one shared, unbuffered channel instead of a
channel and a goroutine per value:

- `Start` creates `results := make(chan worker.Response[Out])` and one
  collector goroutine that reads it, calls the error handler or sends to
  `out`.
- Every task uses `Result: results`. A worker now blocks on `results` while
  the collector is blocked on `out`, which is exactly the backpressure the
  pool sizes imply.
- `Start` counts submitted tasks; the collector keeps draining `results`
  until the count reaches zero, also after cancellation (dropping values
  then), so workers of a shared pool are never left blocked.

This also removes the per-value goroutine and allocation. No API change.

### P2 (High) Worker-pool mode passes the pool's context to the middleware (reproduced)

**Where:** `146142-147`.

```go
Exec: func(ctx context.Context) (Out, error) { // shadows the pipe's ctx
	return process(ctx, v)
},
```

**Problem:** `Middleware` is documented to receive "the context passed to
Start", meaning `Pipe.Start`. In pool mode the closure parameter shadows it
with the context given to `Pool.Start`. Context values (trace IDs, auth,
loggers) are missing, and cancelling the pipe does not cancel middleware
calls that are running or queued. The existing test
`TestMiddlewareErrReceivesStartContext` only covers the sequential mode.

**Fix:** Pass the pipe's context, cancelled additionally when the pool's
context is done:

```go
Exec: func(poolCtx context.Context) (Out, error) {
	ctx, cancel := context.WithCancel(ctx) // the pipe's ctx
	defer cancel()
	stop := context.AfterFunc(poolCtx, cancel)
	defer stop()
	return process(ctx, v)
},
```

Add the missing test for all three modes. Behavior change, but toward the
documented contract, so it is a patch-level fix.

### P3 (High) A middleware panic kills the process in `WithConcurrency` mode

**Where:** `184175-185` (the goroutine started per value);
sequential path at `135127-136`.

**Problem:** The three modes disagree on panics:

| Mode | Panic in the middleware |
|---|---|
| `WithWorkerPool` | recovered by the pool, reported to the error handler, value dropped |
| sequential | propagates out of `Start`; the caller can recover, `out` is still closed |
| `WithConcurrency` | raised in a goroutine the library owns: **the process dies**, nobody can recover |

Switching from `WithWorkerPool` to `WithConcurrency`, which the docs present
as equivalent alternatives, silently changes a handled error into a crash.
The same applies to a panic inside the error handler.

**Fix:** Wrap the middleware call in a recover in the concurrent goroutine
(and, for consistency, in the sequential path) and route the result through
the same error path as the pool does. Use the typed panic error proposed in
W4 so the three modes report the same thing. No API change.

### P4 (Medium) `Start` twice panics (reproduced)

**Where:** `11780-118`.

**Problem:** `Start` always closes `out` on return. A second call (after the
first one returned, or concurrently, or after an `ErrMiddlewareRequired`)
panics with "close of closed channel", or two loops compete for `in`.
Nothing documents that `Start` is single-use.

**Fix:** Guard with an `atomic.Bool`; the second call returns a new sentinel
`ErrAlreadyStarted` without touching the channels. Document that a `Pipe` is
single-use. Adding a sentinel is not breaking.

### P5 (Medium) The pipe closes a channel it does not own

**Where:** `5556` (`NewPipe` signature), `11787,115-118`.

**Problem:** `out` is created by the caller, but `Start` closes it
unconditionally. That rules out fan-in (two pipes writing to one channel
panic on the second close) and makes "who closes" a rule the caller has to
remember. The asymmetry is also surprising: the pipe never closes `in`
(there is a test for that) but always closes `out`. Both parameters are
bidirectional `chan`, so the signature does not say which side the pipe
reads or writes.

**Fix (ideal, breaking):** `NewPipe(in <-chan In, out chan<- Out, ...)` and
closing made explicit, or the pipe creates and returns `out` itself, which
makes ownership obvious.

**Compromise (no major):**

- Add `WithKeepOutOpen[In, Out]()` (name to taste) that disables the close;
  the default stays as it is. Fan-in then works with a caller-side
  `WaitGroup`.
- Do not change the parameter types to directional channels in v2: it would
  break `Read` and `Write`, which need the opposite directions, and any
  caller that passes the constructor as a function value.
- Document the ownership rule on `NewPipe` and `Start`.

### P6 (Medium) `Write` and `Read` cannot be cancelled (reproduced)

**Where:** `217205-218`.

**Problem:** Neither method takes a context or reports an error. After
`Start` has returned (cancellation, closed pool), `Write` on an unbuffered
`in` blocks forever and leaks the goroutine; the caller has no signal that
the pipe is gone. `Write` also panics if `in` was closed by the producer,
and there is no method to close `in` through the pipe, so the "convenience
wrappers" cover only half of the lifecycle.

**Fix (additive):**

- `Done() <-chan struct{}`, closed when `Start` returns, and `Err() error`
  with the reason.
- `WriteCtx(ctx, v) error` and `ReadCtx(ctx) (Out, error)` that select on
  the channel, `ctx.Done()` and `Done()`, returning a new `ErrStopped` in
  the last case.
- Optionally `CloseInput()` to finish the stream through the pipe.
- Keep `Write`/`Read` as they are and point to the new methods in their docs.

### P7 (Medium) `Pause`/`Resume` are weak and not cancellable

**Where:** `24740-51,220-248`.

**Problem:**

- Pause only gates *entry* into `Read`/`Write`. A call already blocked on
  the channel is not paused, and processing in `Start` is not paused at
  all, so "pause the pipe" does not stop values from flowing if anyone uses
  the channels directly, which the constructor encourages.
- The wait uses `sync.Cond`, which cannot be combined with a context or
  with the pipe stopping. A goroutine in a paused `Read`/`Write` stays
  blocked until someone calls `Resume`, even if the pipe is long gone.
- The state is a boolean, so two independent controllers cannot nest
  pauses: the first `Resume` releases both.

**Fix:** Replace the `Cond` with a channel that is open while paused and
closed on resume (swap under the mutex). That makes the gate selectable, so
`WriteCtx`/`ReadCtx` from P6 can honor cancellation while paused. This part
is internal and not breaking.

Pausing the processing loop itself is a behavior change, because the docs
state that `Start` is not affected. **Compromise:** add an opt-in
`WithPausableProcessing[In, Out]()` that makes `Start` stop taking values
from `in` while paused, and keep the default. Document the nesting rule or
leave it as is.

Internal cleanup while there: the gate's receiver is still called `l` and
the field `is`, leftovers of the removed `Lock` API; `paused` reads better.
The lazy `sync.Once` goes away with the channel version.

### P8 (Medium) Errors are dropped silently by default; the handler lacks the value

**Where:** `10698-107`, `pipes/options.go:49-59`.

**Problem:**

- Without `WithErrorHandler`, a failed value disappears: no log, no counter,
  and `Start` still returns `nil`. The safe behavior needs an opt-in, the
  lossy one is the default.
- The handler receives only the `error`, not the input value, so it cannot
  retry, dead-letter or even log which item failed.
- There is no way to stop the pipe on the first error.

**Fix (ideal, breaking):** make the failing value part of the handler
signature and make "stop on error" the default.

**Compromise (additive):**

- `WithFailureHandler[In, Out](func(ctx context.Context, v In, err error))`
  as the richer sibling of `WithErrorHandler`; In is inferable from the
  argument only partly, so both type arguments still have to be spelled out
  (see P11).
- `WithStopOnError[In, Out]()`: `Start` cancels the remaining work and
  returns the first error.
- State the silent-drop default prominently in the package doc, not only in
  the last sentence.

### P9 (Medium) A cancelled pool context turns the pipe into a value shredder (reproduced)

**Where:** `164138-165` together with `worker/worker.go:119-126`.

**Problem:** When the pool's context is cancelled but the pipe's is not, the
pool keeps accepting tasks and answers each with `context.Canceled`. The
pipe treats that as an ordinary per-value failure: it reports it to the
handler (if any), drops the value and keeps reading `in`. Three values in,
zero out, `Start` returned `nil`. Only a *shut down* pool stops the pipe.

**Fix:** In pool mode, treat the pool being unusable as fatal for the pipe:
`Start` returns an error when a response carries the pool context's error.
This needs the pool to make the case detectable, for example a sentinel
`worker.ErrStopped` wrapped into such responses (see W3), instead of
guessing from `context.Canceled`, which a middleware may return itself.

### P10 (Medium) Values in flight are lost without a trace on cancellation

**Where:** `183132-134,151-161,182-184`.

**Problem:** On cancellation, values already taken from `in` are discarded:
`send` gives up, and in pool mode the waiter picks randomly between a ready
result and `ctx.Done()`. Neither the handler nor the return value says how
many were lost. For a pipe between queues this is silent data loss on every
shutdown. `Start` can also return `nil` (input closed) when a cancellation
arrived during the final wait and dropped results.

**Fix:** This is inherent to cancel-means-stop, so the minimum is to
document it in `Start`. Additive improvements: report dropped values to the
failure handler from P8 with a sentinel `ErrDropped`, and offer a graceful
mode (`WithDrainTimeout`) where cancellation stops the intake but lets
in-flight values reach `out` for a bounded time.

### P11 (Low) Option ergonomics and a runtime check for a static fact

**Where:** `pipes/options.go:5-10`, `9528-31,85-96`.

**Problem:**

- `Option[In, Out]` is generic, so every option that does not mention both
  types needs `WithConcurrency[string, int](4)`. The package doc has to
  explain this up front, which is a sign the shape fights the language.
- Merging the v1 `Pipe[T]` and `TransformPipe[In, Out]` moved "a transform
  pipe needs a transform" from compile time to `ErrMiddlewareRequired` at
  `Start`, via `reflect.TypeFor` and an `any(v).(Out)` assertion per value.
  The error surfaces late, in a goroutine whose return value most callers
  drop (`go p.Start(ctx)` in every example).

**Fix (ideal, breaking):** take the middleware as a positional argument and
make the remaining options non-generic:

```go
func New[In, Out any](in <-chan In, out chan<- Out, mw Middleware[In, Out], opts ...Option) *Pipe[In, Out]
func Forward[T any](in <-chan T, out chan<- T, opts ...Option) *Pipe[T, T]
```

**Compromise (additive):** add `Forward[T]` (or `NewForward`) as the
statically checked way to build a pass-through pipe and recommend it in the
docs; keep `NewPipe` and the generic options. Non-generic options can only
come with a new constructor, so that part is v3 material and not urgent.

### P12 (Low) Smaller issues

- **Nil channels are not validated** (`5556`). A nil `out`
  panics inside `Start` on `close(nil)`; a nil `in` blocks until
  cancellation. Panic in `NewPipe` with a `pipes:` message, as
  `WithConcurrency` already does for its argument.
- **`WithWorkerPool` takes the concrete `*worker.Pool[Out]`**
  (`pipes/options.go:19`). The pipe only needs `SubmitCtx`. Accepting a
  small interface would let callers plug in their own executor and would
  decouple the packages. Changing the parameter type is source-compatible
  for callers that pass a `*worker.Pool`, but verify type inference on the
  minimum Go version before doing it in v2; otherwise add a second option.
- **Concurrency options are ignored without a middleware**
  (`126127`). Reasonable, but undocumented.
- **No ordered concurrent mode.** Both concurrent modes reorder; an
  `WithOrdered` option (reorder buffer keyed by sequence number) is a
  common need and purely additive.
- **Nil options** in `opts` panic with a nil-function call; skip or reject
  them explicitly.

---

## worker

### W1 (High) `Shutdown` deadlocks behind a blocked `Submit` (reproduced)

**Where:** `worker/worker.go:80-100` (`SubmitCtx`), `worker/worker.go:104-113`
(`Shutdown`).

**Problem:** `SubmitCtx` holds the read lock for as long as it is blocked on
a full queue. `Shutdown` needs the write lock. The comment argues that
workers keep draining, so the submitter always gets through, but that
assumes working workers. It fails when:

- `Start` was never called (or not yet): nothing drains, `Shutdown` hangs
  forever;
- all workers are stuck on an unread `Result` (W2);
- workers are simply busy with long tasks: `Shutdown` cannot even *begin*
  closing until a slot frees up.

Second effect: Go's `RWMutex` blocks new readers once a writer waits. Every
`SubmitCtx` that arrives after `Shutdown` started queues up on `RLock`,
outside the `select`, so its context is ignored. In the repro a `SubmitCtx`
with a 50 ms deadline was still blocked after 300 ms. It should have
returned `ErrClosed` or the deadline error.

**Fix:** Do not hold a lock across the blocking send. Signal closing with a
channel and count submitters separately:

```go
func (wp *Pool[V]) SubmitCtx(ctx context.Context, task Task[V]) error {
	wp.mu.Lock()
	if wp.closed {
		wp.mu.Unlock()
		return ErrClosed
	}
	wp.submitters.Add(1) // under mu, so Shutdown cannot miss it
	wp.mu.Unlock()
	defer wp.submitters.Done()

	select {
	case wp.tasksChan <- task:
		return nil
	case <-wp.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (wp *Pool[V]) Shutdown() {
	wp.mu.Lock()
	if !wp.closed {
		wp.closed = true
		close(wp.done)       // wakes every blocked submitter
		wp.submitters.Wait() // they return promptly now
		close(wp.tasksChan)  // no sender left, safe to close
	}
	wp.mu.Unlock()
	wp.wg.Wait()
}
```

Blocked submitters now get `ErrClosed` immediately, contexts are honored,
and `Shutdown` no longer depends on workers draining to make progress. No
API change.

### W2 (High) An unread `Result` channel blocks a worker forever (reproduced)

**Where:** `worker/worker.go:128-132`.

**Problem:** The worker sends the response with a bare `job.Result <- res`.
If the caller passed an unbuffered channel and stopped listening (timed out,
returned early on another error), that worker is gone for good: neither
cancelling the pool's context nor `Shutdown` releases it, and `Shutdown`
hangs on `wg.Wait()`. `size` such mistakes stop the pool entirely, and W1
then turns that into a deadlock for all submitters. The doc comment on
`Task.Result` warns about it, but one caller's mistake should not be able
to take down a shared pool.

**Fix:** The guarantee "exactly one Response" cannot be kept unconditionally
together with liveness; pick liveness once the pool is going away:

```go
select {
case job.Result <- res:
case <-wp.done: // set by Shutdown, see W1
	select {
	case job.Result <- res: // still deliver when it does not block
	default:
	}
}
```

Whether a cancelled pool context should also abandon the send is a judgment
call; doing it only on `Shutdown` is the conservative choice and keeps the
documented behavior for running pools. Update the doc on `Task.Result`
accordingly. The structural fix is W5 (the pool owns the result channel).

### W3 (Medium) Lifecycle holes (partly reproduced)

**Where:** `worker/worker.go:63-70,104-113,115-134`.

**Problems:**

- **`Shutdown` before `Start`** closes the queue and returns at once.
  Queued tasks never run and never get a response; their callers wait
  forever. A later `Start` does run them, long after `Shutdown` "finished".
- **`Start` concurrent with `Shutdown`** calls `wg.Add` while `wg.Wait` may
  be running with a zero counter, which `sync.WaitGroup` forbids.
- **Cancelling the context does not stop the pool.** Workers keep running
  to answer tasks with the context's error, `Submit` keeps returning `nil`
  for tasks that are certain to fail (reproduced), and the `size`
  goroutines live until `Shutdown` is called. "Context-aware" in the README
  suggests cancellation is enough to release the pool.
- **`ErrClosed`** is documented as "returned by Submit after the pool has
  been shut down"; it is also what `SubmitCtx` returns.

**Fix:**

- Track the state (`new → started → closed`) under the mutex. `Shutdown` on
  a never-started pool answers the queued tasks with `ErrClosed` itself.
  `Start` after `Shutdown` is a no-op. Take the mutex in `Start` around
  `wg.Add` to order it against `Shutdown`.
- On context cancellation, wrap the response error in a sentinel
  (`fmt.Errorf("%w: %w", ErrStopped, ctx.Err())`) so callers such as
  `pipes` (P9) can tell "the pool is dead" from "my task failed". Adding a
  sentinel and wrapping keeps `errors.Is(err, context.Canceled)` working.
- Make `SubmitCtx` fail fast with that error once the pool's context is
  done. This is a visible behavior change (an error instead of `nil` plus a
  failed response), small enough for a minor release with a changelog note.
- Document that `Shutdown` is required even after cancellation, or let
  workers exit on cancellation after draining what is already queued.

### W4 (Medium) Panic errors lose information (reproduced)

**Where:** `worker/worker.go:138-146`.

**Problem:** A recovered panic becomes
`fmt.Errorf("worker: task panicked: %v", r)`:

- no stack trace, so the origin of the panic is gone, which is the one
  thing needed to fix it;
- the panic value is flattened to text; `errors.Is(err, sentinel)` is false
  even when the task did `panic(sentinel)`;
- no way to detect "this was a panic" other than matching the message.

Also, `runtime.Goexit` in a task (for example `t.FailNow` from a test
helper) ends the worker without a response and shrinks the pool by one;
`recover` does not see it.

**Fix (additive):**

```go
// PanicError is the error reported for a task that panicked.
type PanicError struct {
	Value any    // the value passed to panic
	Stack []byte // debug.Stack() captured in the worker
}

func (e *PanicError) Error() string { return fmt.Sprintf("worker: task panicked: %v", e.Value) }
func (e *PanicError) Unwrap() error { err, _ := e.Value.(error); return err }
```

The message stays the same, so nothing that matched on it breaks. For
`Goexit`, detect the abnormal exit with a `completed` flag in a deferred
function, send a response and start a replacement worker.

### W5 (Medium) The `Task`/`Result`-channel API and the single result type

**Where:** `worker/worker.go:13-32,37`, every call site.

**Problems:**

- Getting one result back takes four steps: make a channel, remember to
  buffer it (W2 if you forget), build a `Task`, receive. The README example
  needs a comment to explain the buffer. `Submit`'s error is ignored in
  both the README and `ExamplePool`, which teaches callers to wait forever
  on a closed pool.
- `Result` is a bidirectional `chan Response[V]`; the pool only sends.
- `Pool[V]` fixes one result type per pool. A pool is a concurrency limit,
  which is naturally shared across task types, but a `Pool[int]` cannot run
  a task returning `string`. In `pipes` this means one pool cannot serve
  two pipes with different `Out` types, despite the doc saying "the pool
  can be shared between pipes".
- `Exec` receives the pool's context; the context given to `SubmitCtx` only
  covers the enqueueing. A task whose submitter gave up still runs, and
  there is no per-task cancellation.
- `Response[V]` duplicates `result.Result[T]`; `result.All` is documented
  as a way "to gather the responses of a batch of worker tasks", but there
  is no bridge, and a stable package cannot depend on an experimental one.

**Fix (ideal, breaking):** a non-generic `Pool` whose unit of work is
`func(context.Context)`, with typed results layered on top by a generic
function, and the pool owning the result channel.

**Compromise (additive, no major):**

```go
// Do runs fn on the pool and waits for its result. It returns early if ctx
// is done; fn then still receives a cancelled context.
func (wp *Pool[V]) Do(ctx context.Context, fn func(context.Context) (V, error)) (V, error)

// Go enqueues fn and returns a channel that receives its one Response.
func (wp *Pool[V]) Go(ctx context.Context, fn func(context.Context) (V, error)) (<-chan Response[V], error)
```

Both create the buffered channel internally, which removes W2 for their
callers, and both can derive the task's context from the pool's context and
the caller's `ctx`. Steer the docs and examples to them and keep
`Task`/`Submit` as the low-level form. For mixed result types, document
`Pool[any]` as the interim answer. Do not change `Task.Result` to
`chan<- Response[V]` in v2: it breaks callers that read the field back.
New fields on `Task` (for example a per-task context) are acceptable under
the usual Go rule that unkeyed struct literals are not covered by
compatibility.

### W6 (Medium) `Shutdown` cannot be bounded

**Where:** `worker/worker.go:104-113`.

**Problem:** `Shutdown` waits for every queued and running task with no
timeout and no context. A service with a shutdown deadline can only cancel
the `Start` context and hope every task honors it. Calling `Shutdown` from
inside a task deadlocks (`wg.Wait` waits for the calling worker), which is
not documented.

**Fix (additive):** `ShutdownCtx(ctx context.Context) error` that stops
intake, waits until the workers finish or `ctx` is done, and returns
`ctx.Err()` in the latter case (workers keep finishing in the background).
`Shutdown` becomes `ShutdownCtx(context.Background())`. Document the
re-entrancy restriction.

### W7 (Low) Smaller issues

- **`queueSize < 0`** reaches `make(chan, n)` and panics with a runtime
  message instead of a `worker:` one (`worker/worker.go:55-58`). Validate
  it next to `size`.
- **A nil `Exec`** is accepted by `Submit` and reported later as a panic
  with "nil pointer dereference" (reproduced). Reject it in `SubmitCtx`.
- **A nil context in `Start`** panics in the worker goroutine, outside
  `safeExec`, and kills the process. Reject it in `Start`.
- **No non-blocking submit and no introspection.** `TrySubmit` (returning a
  new `ErrQueueFull`) and `Len`/`Cap`/`Size` accessors are standard for a
  bounded queue and help with load shedding and metrics. P1's fix does not
  need them, but callers do.
- **Naming:** the receiver is `wp` and the field `tasksChan`, leftovers of
  `WorkerPool`; `p` and `tasks` match the current names.

---

## result (experimental)

The package is marked experimental, so none of these are bound by semver.
They are worth settling before it is declared stable.

### R1 (Medium) The error accessor is named `Error` and returns `error`

**Where:** `result/result.go:111-114`.

**Problem:** `func (r Result[T]) Error() error` has the name of the `error`
interface method but a different signature. `Result` therefore does not
implement `error`, yet reads as if it did, and the mismatch produces
confusing compiler messages when someone passes a `Result` where an `error`
is expected. The doc comment says "Err returns the underlying error", so
the name and its documentation already disagree.

**Fix:** Rename to `Err()`. A method and the package-level function `Err`
live in different namespaces, so there is no clash. Keep `Error()` as a
deprecated forwarder for one release if anyone already depends on it.

### R2 (Medium) `Err[T](nil)` and the zero value are silently `Ok`

**Where:** `result/result.go:24-26`, `result/result.go:9-12`.

**Problem:** The state is derived from `err != nil`. `Err[int](nil)`
returns a result for which `IsOk()` is true and the value is zero; so does
`var r Result[int]`. A function that returns `result.Err[T](err)` with an
accidentally nil `err` reports success with a zero value, which is the bug
class `Result` is meant to prevent. `MapErr` already guards against the
same thing ("a failure never turns into a success"), so the constructor is
inconsistent with it.

**Fix:** Make `Err` panic on a nil error (preferred, it is a programming
error), or substitute a sentinel `ErrNilError`. Document that the zero
`Result[T]` is `Ok` of the zero value.

### R3 (Low) Smaller issues

- **`Must`** is documented to panic "with err", but it panics with a wrapper
  whose text is "called Unwrap on an error result: ..."
  (`result/result.go:44-46`). Give it its own message or fix the comment.
- **JSON:** both fields are unexported, so `json.Marshal(result.Ok(1))`
  yields `{}` and silently loses the data. Implement `MarshalJSON` or state
  that `Result` is not meant for serialization.
- **`All`** keeps the first error only. A sibling that joins all errors
  with `errors.Join` (or returns the partial values) covers the batch use
  case named in its own doc comment.
- **`String`** prints the value with `%v`; a `Result` holding a secret ends
  up in logs through any `%v` of an enclosing struct. Worth a sentence in
  the doc.
- **Per-method "Experimental" notes are inconsistent:** `Map`, `AndThen`,
  `OrElse`, `MapErr` carry one, `Unwrap`, `Value`, `UnwrapOr`,
  `UnwrapOrElse`, `IsOk`, `Error`, `String` do not, although the type-level
  note covers everything. Keep the type-level note only.

---

## Module, docs and CI

### A1 (Medium) An experimental package inside a stable module

**Where:** `go.mod:3`, `result/`, `README.md` ("Versioning").

**Problem:**

- `result` uses generic methods and so pins the whole module to
  `go 1.27`. `worker` and `pipes` need nothing newer than `for range int`
  (Go 1.22) and `reflect.TypeFor` (Go 1.22). Users of the stable packages
  pay for the experimental one with a toolchain upgrade.
- "The API may change in any release" inside a `v2.x` module contradicts
  what `go get -u` and tools such as `gorelease`/`apidiff` assume about a
  tagged v2 module. A breaking change in `result` in `v2.3.0` breaks
  builds of everyone who imports it, with no version signal. The same
  repository already took the better route once: `cache` moved to its own
  module.

**Fix (ideal):** move `result` to its own module at `v0.x`
(`go.xchunk.org/result`, like `cache`), and lower the `go` directive of
anvil to what the stable packages need.

**Compromise:** carve it out as a nested module in this repository
(`result/go.mod`, tags `result/v0.x.y`) and leave a deprecated forwarding
package for a release or two, the way `cache` is handled now. If it stays
where it is, add an API-compatibility check to CI that excludes `result`
(see A3) and call out every breaking `result` change in the release notes.

### A2 (Medium) GPL-3.0 for a building-blocks library

**Where:** `LICENSE`.

**Problem:** The license is the GNU GPL v3. Any program that imports
`worker` or `pipes` has to be distributed under the GPL as well, which
excludes the library from most commercial and from permissively licensed
open-source code. For a utility library this is the single largest
constraint on adoption, and the README does not mention it. If it is
intended, say so in the README; if it came from a repository template,
reconsider (MIT, BSD-3-Clause, Apache-2.0, or LGPL/MPL-2.0 as a middle
ground). Relicensing needs the consent of all contributors and is
independent of the module version.

### A3 (Low) CI, docs, tests

- **CI hardening** (`.github/workflows/ci.yml`): no `permissions:` block,
  so the job token has the repository default; add
  `permissions: { contents: read }`. `staticcheck@latest` is fetched and
  executed unpinned on every run: pin a version. Actions are pinned by tag,
  not by commit SHA. CI runs only for `master`, so `legacy/v1`, which the
  README presents as maintained, has no checks.
- **No API-compatibility gate.** For a library with a semver promise, run
  `gorelease` or `apidiff` against the last tag on pull requests. It would
  also have flagged the items this review marks as breaking.
- **Missing package comments:** `worker` and `result` have no
  `// Package ...` doc, so pkg.go.dev shows an empty overview. Only
  `pipes` and the root have one.
- **Test gaps:** no goroutine-leak checks (P1, P6 and W2 would have been
  caught), no test for the middleware context in the concurrent modes (P2),
  no test for a panic in `WithConcurrency` mode (P3), no `Start`-twice test
  (P4), no `Shutdown`-before-`Start` test (W3), no benchmarks.
- **Examples ignore errors:** `wp.Submit(...)` and `go p.Start(ctx)` drop
  their results in the README and in `example_test.go`. Since `Start`
  reports `ErrMiddlewareRequired` and pool failures only through its return
  value, the examples should show it being checked.
- **No `CHANGELOG.md` and no `SECURITY.md`.** With behavior fixes like P2,
  P3 and W3 coming, a changelog is where users learn about them.
- **README:** the tagline still leads with "a TTL cache", which is the
  deprecated part.
