package sync

import (
	"sync"
	"time"
)

// Wait is a WaitGroup that could use a time out
// with a timeout, Wait could either:
// return false if all tasks are done
// return true if timed out
type Wait struct {
	wg sync.WaitGroup
}

// Add adds delta, which may be negative, to the WaitGroup counter.
func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

// Done decrements the WaitGroup counter by one
func (w *Wait) Done() {
	w.wg.Done()
}

// Wait blocks until the WaitGroup counter is zero.
func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout blocks until the WaitGroup counter is zero or timeout
// returns true if timeout
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan struct{}, 1)
	go func() {
		defer close(c)
		// wg wait for task done somewhere else
		w.Wait()
		c <- struct{}{}
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
