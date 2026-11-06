package nats

import "context"

func (r *Runtime) isAccepting() bool {
	r.acceptMu.RLock()
	defer r.acceptMu.RUnlock()
	return r.accepting
}

func (r *Runtime) setAccepting(on bool) {
	r.acceptMu.Lock()
	r.accepting = on
	r.acceptMu.Unlock()
}

// dispatch runs fn on its own goroutine, bounded by the concurrency
// semaphore and tracked for drain. ctx is the handler context of the Start
// that opened the receiver. It reports false, and does not run fn, for a
// message arriving after the runtime stopped accepting or after ctx is done.
// The ctx check stops a receiver left over from a failed Start from running
// handlers once a later Start accepts again.
func (r *Runtime) dispatch(ctx context.Context, fn func()) bool {
	if !r.isAccepting() || ctx.Err() != nil {
		return false
	}
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		return false
	}

	// Checked again under the lock, so no handler is added to wg once Stop
	// has begun waiting.
	r.acceptMu.RLock()
	if !r.accepting || ctx.Err() != nil {
		r.acceptMu.RUnlock()
		<-r.sem
		return false
	}
	r.wg.Add(1)
	r.acceptMu.RUnlock()

	go func() {
		defer r.wg.Done()
		defer func() { <-r.sem }()
		fn()
	}()
	return true
}
