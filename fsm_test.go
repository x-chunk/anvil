package anvil

import (
	"sync"
	"testing"
)

func TestFSMBasics(t *testing.T) {
	f := NewFSM[string, int](0)

	if _, ok := f.Get("a"); ok {
		t.Fatal("Get on an unknown id reported ok")
	}
	f.Init("a")
	if v, ok := f.Get("a"); !ok || v != 0 {
		t.Fatalf("after Init: %d, %v; want 0, true", v, ok)
	}
	f.Set("a", 5)
	f.Init("a") // must not overwrite an existing state
	if v, _ := f.Get("a"); v != 5 {
		t.Fatalf("Init overwrote state: got %d, want 5", v)
	}
	f.Reset("a")
	if v, _ := f.Get("a"); v != 0 {
		t.Fatalf("after Reset: %d, want 0", v)
	}
}

func TestFSMComparableBasics(t *testing.T) {
	f := NewFSMComparable[string, string]("idle")

	if f.IsInit("a") {
		t.Fatal("IsInit is true for an unknown id")
	}
	f.Init("a")
	if !f.IsInit("a") {
		t.Fatal("IsInit is false right after Init")
	}
	f.Set("a", "busy")
	if f.IsInit("a") {
		t.Fatal("IsInit is true after moving to another state")
	}
	f.Reset("a")
	if !f.IsInit("a") {
		t.Fatal("IsInit is false after Reset")
	}
}

func TestFSMConcurrentUse(t *testing.T) {
	f := NewFSM[int, int](0)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				f.Init(i)
				f.Set(i, i)
				f.Get(i)
				f.Reset(i)
			}
		}()
	}
	wg.Wait()
}

// Run with -race: IsInit used to read the map without holding the lock.
func TestFSMComparableConcurrentUse(t *testing.T) {
	f := NewFSMComparable[int, int](0)

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				f.Init(i)
				f.Set(i, i%3)
				f.IsInit(i)
				f.Get(i)
				f.Reset(i)
			}
		}()
	}
	wg.Wait()
}
