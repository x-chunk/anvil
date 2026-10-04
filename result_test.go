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
