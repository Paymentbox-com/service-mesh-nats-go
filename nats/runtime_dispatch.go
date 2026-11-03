package nats

func (r *Runtime) isAccepting(jetStream bool) bool {
	r.acceptMu.RLock()
	defer r.acceptMu.RUnlock()
	return r.acceptingLocked(jetStream)
}

func (r *Runtime) acceptingLocked(jetStream bool) bool {
	if jetStream {
		return r.jsAccepting
	}
	return r.accepting
}

func (r *Runtime) setAccepting(on bool) {
	r.acceptMu.Lock()
	r.accepting = on
	r.jsAccepting = on
	r.acceptMu.Unlock()
}

// dispatch runs fn on its own goroutine, bounded by the concurrency
// semaphore and tracked for drain. It reports false, and does not run fn,
// for a message arriving after its kind of subscription stopped accepting.
func (r *Runtime) dispatch(jetStream bool, fn func()) bool {
	if !r.isAccepting(jetStream) {
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
	if !r.acceptingLocked(jetStream) {
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
