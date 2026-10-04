package anvil

import "sync"

// FSM is a mutex-guarded map of per-key states with a default state.
//
// Deprecated: FSM is not a state machine (no transitions or validation) and
// may be removed in a future release.
type FSM[K comparable, V any] struct {
	mu        sync.Mutex
	states    map[K]V
	initState V
}

// FSMComparable is FSM for comparable state values; it adds IsInit.
//
// Deprecated: FSMComparable is not a state machine (no transitions or
// validation) and may be removed in a future release.
type FSMComparable[K comparable, V comparable] struct {
	mu        sync.Mutex
	states    map[K]V
	initState V
}

// NewFSM returns an instance of FSM.
//
// Deprecated: see FSM.
func NewFSM[K comparable, V any](initState V) *FSM[K, V] {
	return &FSM[K, V]{
		states:    make(map[K]V),
		initState: initState,
	}
}

// NewFSMComparable returns an instance of FSMComparable.
//
// Deprecated: see FSMComparable.
func NewFSMComparable[K comparable, V comparable](initState V) *FSMComparable[K, V] {
	return &FSMComparable[K, V]{
		states:    make(map[K]V),
		initState: initState,
	}
}

func (fsm *FSM[K, V]) Get(id K) (V, bool) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if state, ok := fsm.states[id]; ok {
		return state, ok
	}
	var zero V
	return zero, false
}

func (fsm *FSMComparable[K, V]) Get(id K) (V, bool) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if state, ok := fsm.states[id]; ok {
		return state, ok
	}
	var zero V
	return zero, false
}

func (fsm *FSM[K, V]) Set(id K, val V) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.states[id] = val
}

func (fsm *FSMComparable[K, V]) Set(id K, val V) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	fsm.states[id] = val
}

func (fsm *FSM[K, V]) Init(id K) {
	if _, ok := fsm.Get(id); !ok {
		fsm.Set(id, fsm.initState)
	}
}

func (fsm *FSMComparable[K, V]) Init(id K) {
	if _, ok := fsm.Get(id); !ok {
		fsm.Set(id, fsm.initState)
	}
}

func (fsm *FSM[K, V]) Reset(id K) {
	fsm.Set(id, fsm.initState)
}

func (fsm *FSMComparable[K, V]) Reset(id K) {
	fsm.Set(id, fsm.initState)
}

func (fsm *FSMComparable[K, V]) IsInit(id K) bool {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	if state, ok := fsm.states[id]; ok {
		return state == fsm.initState
	}
	return false
}
