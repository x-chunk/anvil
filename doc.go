// Package anvil is a small library of generic building blocks. The module is
// split into packages that can be imported independently:
//
//   - go.xchunk.org/anvil/v2/cache: an in-memory TTL cache
//   - go.xchunk.org/anvil/v2/cache/v2: the same cache with a coarse, faster clock (unstable)
//   - go.xchunk.org/anvil/v2/worker: a worker pool
//   - go.xchunk.org/anvil/v2/pipes: channel pipes with a processing step
//   - go.xchunk.org/anvil/v2/result: a Result[T] type (experimental)
//
// The root package itself has no API.
package anvil
