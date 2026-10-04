package anvil

import "fmt"

// Result holds either a successful value of type T or an error.
//
// Experimental: Result is a new feature. Its API may change or be removed
// in any release without notice.
type Result[T any] struct {
	value T
	err   error
}

// Ok wraps a successful value into a Result.
//
// Experimental: see Result.
func Ok[T any](value T) Result[T] {
	return Result[T]{value: value}
}

// Err wraps an error into a Result.
//
// Experimental: see Result.
func Err[T any](err error) Result[T] {
	return Result[T]{err: err}
}

// Of wraps the usual Go (value, error) pair into a Result, so a call like
// Of(strconv.Atoi(s)) works directly. If err is non-nil the value is
// discarded.
//
// Experimental: see Result.
func Of[T any](value T, err error) Result[T] {
	if err != nil {
		return Err[T](err)
	}
	return Ok(value)
}

// Unwrap returns the value or panics if the result holds an error.
//
// The panic value is an error wrapping the result's error, so a recovered
// value can be inspected with errors.Is and errors.As.
func (r Result[T]) Unwrap() T {
	if r.err != nil {
		panic(fmt.Errorf("called Unwrap on an error result: %w", r.err))
	}
	return r.value
}

// Value returns the value and error as the usual Go pair. The value is the
// zero value of T if the result holds an error.
func (r Result[T]) Value() (T, error) {
	if r.err != nil {
		var zero T
		return zero, r.err
	}
	return r.value, nil
}

// UnwrapOr returns the value, or the provided fallback if there's an error.
func (r Result[T]) UnwrapOr(fallback T) T {
	if r.err != nil {
		return fallback
	}
	return r.value
}

// IsOk reports whether the result is successful.
func (r Result[T]) IsOk() bool {
	return r.err == nil
}

// Err returns the underlying error, or nil.
func (r Result[T]) Error() error {
	return r.err
}
