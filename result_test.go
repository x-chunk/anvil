package anvil

import (
	"errors"
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
