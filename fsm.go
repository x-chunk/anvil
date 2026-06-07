package anvil

import "sync"

type FSM[K comparable, V any] struct {
	mu        sync.Mutex
	states    map[K]V
	initState V
}

type FSMComparable[K comparable, V comparable] struct {
	mu        sync.Mutex
	states    map[K]V
	initState V
}

func NewFSM[K comparable, V any](initState V) *FSM[K, V] {
	return &FSM[K, V]{
		states:    make(map[K]V),
		initState: initState,
	}
}

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

func (fsm *FSMComparable[K, V]) IsNotInit(id K) bool {
	if state, ok := fsm.states[id]; ok {
		return state == fsm.initState
	}
	return false
}
