package result

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

var errTest = errors.New("test error")

func TestResultOk(t *testing.T) {
	r := Ok(42)
	if !r.IsOk() || r.Error() != nil {
		t.Fatalf("Ok result reports an error: %v", r.Error())
	}
	if got := r.Unwrap(); got != 42 {
		t.Fatalf("Unwrap = %d, want 42", got)
	}
	if got := r.UnwrapOr(7); got != 42 {
		t.Fatalf("UnwrapOr = %d, want 42", got)
	}
}

func TestResultErr(t *testing.T) {
	r := Err[int](errTest)
	if r.IsOk() || !errors.Is(r.Error(), errTest) {
		t.Fatalf("Err result: IsOk=%v Error=%v", r.IsOk(), r.Error())
	}
	if got := r.UnwrapOr(7); got != 7 {
		t.Fatalf("UnwrapOr = %d, want fallback 7", got)
	}
}

func TestResultUnwrapPanicsOnError(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Unwrap on an error result did not panic")
		}
	}()
	Err[int](errTest).Unwrap()
}

func TestResultUnwrapPanicWrapsError(t *testing.T) {
	defer func() {
		r := recover()
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panic value is %T, want error", r)
		}
		if !errors.Is(err, errTest) {
			t.Fatalf("panic error %v does not wrap the result error", err)
		}
	}()
	Err[int](errTest).Unwrap()
}

func TestOf(t *testing.T) {
	if r := Of(5, nil); !r.IsOk() || r.Unwrap() != 5 {
		t.Fatalf("Of(5, nil) = %+v", r)
	}
	r := Of(5, errTest)
	if r.IsOk() || !errors.Is(r.Error(), errTest) {
		t.Fatalf("Of(5, err) = %+v, want error result", r)
	}
	if got := r.UnwrapOr(-1); got != -1 {
		t.Fatalf("value of an error result leaked: %d", got)
	}
}

func TestResultValue(t *testing.T) {
	if v, err := Ok("x").Value(); v != "x" || err != nil {
		t.Fatalf("Ok.Value() = %q, %v", v, err)
	}
	if v, err := Err[string](errTest).Value(); v != "" || !errors.Is(err, errTest) {
		t.Fatalf("Err.Value() = %q, %v", v, err)
	}
	// Round trip with Of.
	if got := Of(Ok(3).Value()).Unwrap(); got != 3 {
		t.Fatalf("round trip = %d, want 3", got)
	}
}

func TestMust(t *testing.T) {
	if got := Must(1, nil); got != 1 {
		t.Fatalf("Must(1, nil) = %d", got)
	}

	defer func() {
		err, ok := recover().(error)
		if !ok || !errors.Is(err, errTest) {
			t.Fatalf("Must panicked with %v, want an error wrapping errTest", err)
		}
	}()
	Must(1, errTest)
}

func TestMap(t *testing.T) {
	double := func(v int) string { return strings.Repeat("x", v) }

	if got := Ok(3).Map(double).Unwrap(); got != "xxx" {
		t.Fatalf("Map(Ok(3)) = %q", got)
	}

	called := false
	r := Err[int](errTest).Map(func(int) string { called = true; return "" })
	if called {
		t.Fatal("Map called f on an error result")
	}
	if !errors.Is(r.Error(), errTest) {
		t.Fatalf("Map did not pass the error through: %v", r.Error())
	}
}

func TestAndThen(t *testing.T) {
	parse := func(s string) Result[int] { return Of(strconv.Atoi(s)) }

	if got := Ok("12").AndThen(parse).Unwrap(); got != 12 {
		t.Fatalf("AndThen(Ok(\"12\")) = %d", got)
	}
	if r := Ok("nope").AndThen(parse); r.IsOk() {
		t.Fatal("AndThen should return the error from f")
	}

	called := false
	r := Err[string](errTest).AndThen(func(string) Result[int] { called = true; return Ok(0) })
	if called {
		t.Fatal("AndThen called f on an error result")
	}
	if !errors.Is(r.Error(), errTest) {
		t.Fatalf("AndThen did not pass the error through: %v", r.Error())
	}
}

func TestUnwrapOrElse(t *testing.T) {
	called := false
	got := Ok(1).UnwrapOrElse(func(error) int { called = true; return 9 })
	if got != 1 || called {
		t.Fatalf("Ok: got %d, fallback called = %v; want 1, false", got, called)
	}

	var seen error
	got = Err[int](errTest).UnwrapOrElse(func(err error) int { seen = err; return 9 })
	if got != 9 || !errors.Is(seen, errTest) {
		t.Fatalf("Err: got %d, fallback saw %v; want 9, errTest", got, seen)
	}
}

func TestOrElse(t *testing.T) {
	called := false
	r := Ok(1).OrElse(func(error) Result[int] { called = true; return Ok(9) })
	if r.Unwrap() != 1 || called {
		t.Fatalf("Ok: got %+v, fallback called = %v", r, called)
	}

	if got := Err[int](errTest).OrElse(func(error) Result[int] { return Ok(9) }).Unwrap(); got != 9 {
		t.Fatalf("recovered value = %d, want 9", got)
	}

	other := errors.New("other")
	r = Err[int](errTest).OrElse(func(error) Result[int] { return Err[int](other) })
	if !errors.Is(r.Error(), other) {
		t.Fatalf("a failing fallback should replace the error, got %v", r.Error())
	}

	// The fallback gets the original error.
	var seen error
	Err[int](errTest).OrElse(func(err error) Result[int] { seen = err; return Ok(0) })
	if !errors.Is(seen, errTest) {
		t.Fatalf("fallback saw %v, want errTest", seen)
	}
}

func TestMapErr(t *testing.T) {
	called := false
	r := Ok(1).MapErr(func(err error) error { called = true; return err })
	if r.Unwrap() != 1 || called {
		t.Fatalf("Ok: got %+v, f called = %v", r, called)
	}

	wrapped := Err[int](errTest).MapErr(func(err error) error { return fmt.Errorf("step: %w", err) })
	if !errors.Is(wrapped.Error(), errTest) || wrapped.Error().Error() != "step: test error" {
		t.Fatalf("got %v, want the wrapped error", wrapped.Error())
	}

	// Steps can attach their own context.
	r2 := Err[int](errTest).
		MapErr(func(err error) error { return fmt.Errorf("read: %w", err) }).
		MapErr(func(err error) error { return fmt.Errorf("load config: %w", err) })
	if got := r2.Error().Error(); got != "load config: read: test error" {
		t.Fatalf("got %q", got)
	}

	// A nil from f must not turn a failure into a success.
	kept := Err[int](errTest).MapErr(func(error) error { return nil })
	if kept.IsOk() || !errors.Is(kept.Error(), errTest) {
		t.Fatalf("got %+v, want the original error kept", kept)
	}
}

func TestAll(t *testing.T) {
	got := All(Ok(1), Ok(2), Ok(3)).Unwrap()
	if fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("All = %v, want [1 2 3]", got)
	}

	empty := All[int]().Unwrap()
	if empty == nil || len(empty) != 0 {
		t.Fatalf("All() = %#v, want an empty non-nil slice", empty)
	}

	other := errors.New("other")
	r := All(Ok(1), Err[int](errTest), Err[int](other))
	if !errors.Is(r.Error(), errTest) {
		t.Fatalf("All returned %v, want the first error", r.Error())
	}
}

func TestAllDoesNotAliasInput(t *testing.T) {
	in := []Result[int]{Ok(1), Ok(2)}
	out := All(in...).Unwrap()
	out[0] = 99
	if v, _ := in[0].Value(); v != 1 {
		t.Fatal("modifying the result of All changed the input")
	}
}
