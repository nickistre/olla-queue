package balancer

import (
	"context"
	"testing"
	"time"

	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

// overflowPair is a local endpoint and an overflow endpoint that only takes
// requests after delay, both with one slot.
func overflowPair(delay time.Duration) (local, overflow *domain.Endpoint) {
	local = makeEndpoint("local", "http://local:1")
	overflow = makeEndpoint("overflow", "http://overflow:1")
	overflow.QueueAfter = delay
	return local, overflow
}

// agedCtx is a request whose earlier attempts already waited prior.
func agedCtx(prior time.Duration) context.Context {
	return context.WithValue(context.Background(), constants.ContextQueueOutcomeKey, &domain.QueueOutcome{Wait: prior})
}

func TestQueue_OverflowEndpointOnlyAfterQueueAfter(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	local, overflow := overflowPair(60 * time.Millisecond)
	eps := []*domain.Endpoint{local, overflow}
	fillSlots(t, q, local)

	// overflow is free, but a new request hasn't waited long enough for it.
	start := time.Now()
	ch := selectAsync(context.Background(), q, eps)
	waitForDepth(t, q, 1)

	// No release comes: the request's own timer has to dispatch it.
	r := mustReceive(t, ch)
	if r.err != nil || r.endpoint.URLString != overflow.URLString {
		t.Fatalf("should overflow once old enough: %+v", r)
	}
	if waited := time.Since(start); waited < 50*time.Millisecond {
		t.Fatalf("overflowed after %s, before queue_after", waited)
	}
	if s := q.Stats(); s.Endpoints[1].Name != "overflow" || s.Endpoints[1].QueueAfterSeconds != 0.06 {
		t.Fatalf("endpoint stats: %+v", s.Endpoints)
	}
}

func TestQueue_OverflowAtOnceWhenNoLocalEndpointIsUp(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	local, overflow := overflowPair(time.Minute)
	eps := []*domain.Endpoint{local, overflow}

	q.EndpointFailed(local)
	ep, err := q.Select(context.Background(), eps)
	if err != nil || ep.URLString != overflow.URLString {
		t.Fatalf("with local down, overflow should be used at once: %v %v", ep, err)
	}
}

func TestQueue_OverflowWhenLocalGoesDownWhileWaiting(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	local, overflow := overflowPair(time.Minute)
	eps := []*domain.Endpoint{local, overflow}
	fillSlots(t, q, local)

	ch := selectAsync(context.Background(), q, eps)
	waitForDepth(t, q, 1)
	q.PurgeDeadEndpoints([]*domain.Endpoint{overflow})
	if r := mustReceive(t, ch); r.err != nil || r.endpoint.URLString != overflow.URLString {
		t.Fatalf("waiter should overflow once local is down: %+v", r)
	}
}

func TestQueue_OverflowIsLastResort(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	local, overflow := overflowPair(time.Second)
	eps := []*domain.Endpoint{local, overflow}

	// Old enough for overflow, yet local is free: local wins.
	first, err := q.Select(agedCtx(time.Minute), eps)
	if err != nil || first.URLString != local.URLString {
		t.Fatalf("local should be preferred: %v %v", first, err)
	}
	// A retry that already waited past queue_after may overflow at once.
	second, err := q.Select(agedCtx(time.Minute), eps)
	if err != nil || second.URLString != overflow.URLString {
		t.Fatalf("aged retry should overflow at once: %v %v", second, err)
	}
}

func TestQueue_OverflowRespectsMaxConcurrent(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	local, overflow := overflowPair(time.Second)
	overflow.MaxConcurrent = 1
	eps := []*domain.Endpoint{local, overflow}
	fillSlots(t, q, local)

	if ep, err := q.Select(agedCtx(time.Minute), eps); err != nil || ep.URLString != overflow.URLString {
		t.Fatalf("first aged request should overflow: %v %v", ep, err)
	}
	ch := selectAsync(agedCtx(time.Minute), q, eps)
	waitForDepth(t, q, 1)
	select {
	case r := <-ch:
		t.Fatalf("overflow is full, second request should wait: %+v", r)
	default:
	}
}

func TestQueue_PinToOverflowCausesNoHoldForNewRequest(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	local, overflow := overflowPair(time.Minute)
	eps := []*domain.Endpoint{local, overflow}
	q.SetStickyHold(pins{"conv-1": overflow.URLString}.peek, time.Minute)
	busy, err := q.Select(context.Background(), []*domain.Endpoint{local})
	if err != nil {
		t.Fatal(err)
	}

	ch := selectAsync(stickyCtx("conv-1", ""), q, eps)
	waitForDepth(t, q, 1)
	if s := q.Stats(); s.Waiting[0].Holding {
		t.Fatal("a pin the request can't use yet should not hold it")
	}
	// The next turn goes back to local, not the overflow it was pinned to.
	q.DecrementConnections(busy)
	if r := mustReceive(t, ch); r.err != nil || r.endpoint.URLString != local.URLString {
		t.Fatalf("should go to local: %+v", r)
	}
}
