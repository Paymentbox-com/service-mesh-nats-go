package nats

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
// semaphore and tracked for drain. It reports false, and does not run fn,
// for a message arriving after the runtime stopped accepting.
func (r *Runtime) dispatch(fn func()) bool {
	if !r.isAccepting() {
		return false
	}
	select {
	case r.sem <- struct{}{}:
	case <-r.handlerCtx.Done():
		return false
	}

	// Checked again under the lock, so no handler is added to wg once Stop
	// has begun waiting.
	r.acceptMu.RLock()
	if !r.accepting {
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
