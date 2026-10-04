package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

// Clock is the time source a Cache checks expiry against.
//
// Nanotime returns the current time in nanoseconds on an arbitrary monotonic
// scale. Only differences between its values matter, so they need not relate
// to wall-clock time. An implementation must be safe for concurrent use, must
// never go backwards, and should return non-negative values. Nanotime is
// called on every Get of an item that can expire, so it should be cheap.
type Clock interface {
	Nanotime() int64
}

// DefaultTick is how often the clock shared by caches created without
// WithClock is refreshed.
const DefaultTick = time.Millisecond

// CoarseClock is a Clock that trades precision for speed: a background
// goroutine stores the current monotonic time into an atomic variable every
// tick, and Nanotime just loads it. Reading it costs about as much as reading
// an ordinary variable, while time.Now costs tens of nanoseconds and more on
// some virtual machines.
//
// Its value lags real time by up to one tick, and by more if the refreshing
// goroutine is delayed by the scheduler or a GC pause. A cache using it
// therefore never expires items early, but may keep returning them for up to
// a tick (plus such delays) past their ttl.
//
// Values are monotonic, so changes to the system's wall clock don't affect
// them. Values of different CoarseClocks are on different scales and must not
// be compared.
type CoarseClock struct {
	base time.Time
	now  atomic.Int64
	stop chan struct{}
	once sync.Once
}

// NewCoarseClock returns a running CoarseClock refreshed every tick. Call
// Stop when it is no longer needed to release its goroutine. It panics if
// tick is not positive.
func NewCoarseClock(tick time.Duration) *CoarseClock {
	if tick <= 0 {
		panic("cache: non-positive tick for NewCoarseClock")
	}
	c := &CoarseClock{base: time.Now(), stop: make(chan struct{})}
	go c.run(tick)
	return c
}

func (c *CoarseClock) run(tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.now.Store(int64(time.Since(c.base)))
		case <-c.stop:
			return
		}
	}
}

// Nanotime returns the time of the last refresh in nanoseconds since the
// clock was created.
func (c *CoarseClock) Nanotime() int64 {
	return c.now.Load()
}

// Stop stops refreshing the clock and releases its goroutine. Afterwards
// Nanotime keeps returning the last refreshed value, so items of caches using
// the clock stop expiring. Stop is safe to call more than once.
func (c *CoarseClock) Stop() {
	c.once.Do(func() { close(c.stop) })
}

var (
	sharedClock     *CoarseClock
	sharedClockOnce sync.Once
)

// defaultClock returns the process-wide CoarseClock used by caches created
// without WithClock. It is started on first use and never stopped, so it
// shows up as one long-lived goroutine (in goroutine dumps and in leak
// checkers such as goleak); this is intentional.
func defaultClock() *CoarseClock {
	sharedClockOnce.Do(func() { sharedClock = NewCoarseClock(DefaultTick) })
	return sharedClock
}
