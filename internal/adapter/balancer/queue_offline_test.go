package balancer

import (
	"context"
	"testing"
	"time"

	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

// fillSlots takes the one default slot on each endpoint.
func fillSlots(t *testing.T, q *QueueWrapper, eps ...*domain.Endpoint) {
	t.Helper()
	for _, ep := range eps {
		if _, err := q.Select(context.Background(), []*domain.Endpoint{ep}); err != nil {
			t.Fatal(err)
		}
	}
}

func refreshCtx(fn domain.QueueRefresher) context.Context {
	return context.WithValue(context.Background(), constants.ContextQueueRefreshKey, fn)
}

func expectQueued(t *testing.T, ch <-chan queueResult) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("expected the request to stay queued, got %+v", r)
	case <-time.After(50 * time.Millisecond):
	}
}

// A waiter's endpoint copies still say healthy after the node dies; the
// health checker's report must win, or a freed slot on the dead node would
// draw the waiter onto it.
func TestQueue_DownEndpointIsNotHandedToWaiters(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	fillSlots(t, q, a, b)

	ch := selectAsync(context.Background(), q, []*domain.Endpoint{a, b})
	waitForDepth(t, q, 1)

	q.PurgeDeadEndpoints([]*domain.Endpoint{b}) // a went offline
	q.DecrementConnections(a)                   // its in-flight request failed
	expectQueued(t, ch)

	q.DecrementConnections(b)
	if r := mustReceive(t, ch); r.err != nil || r.endpoint.URLString != b.URLString {
		t.Fatalf("want dispatch to b, got %+v", r)
	}
}

func TestQueue_EndpointRecoveredMakesItUsableAgain(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	fillSlots(t, q, a, b)

	ch := selectAsync(context.Background(), q, []*domain.Endpoint{a, b})
	waitForDepth(t, q, 1)
	q.PurgeDeadEndpoints([]*domain.Endpoint{b})
	q.DecrementConnections(a)
	expectQueued(t, ch)

	q.EndpointRecovered(a)
	if r := mustReceive(t, ch); r.err != nil || r.endpoint.URLString != a.URLString {
		t.Fatalf("want dispatch to recovered a, got %+v", r)
	}
}

// A copy taken after the node was marked down, showing it routable, means the
// health checker saw it recover even if no recovery hook fired.
func TestQueue_FreshRoutableCopyClearsDown(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	fillSlots(t, q, a)
	q.DecrementConnections(a)
	q.PurgeDeadEndpoints(nil)

	stale := makeEndpoint("a", "http://a:1")
	if _, err := q.Select(context.Background(), []*domain.Endpoint{stale}); err == nil {
		t.Fatal("a stale copy of a down endpoint should not get a slot")
	}

	fresh := makeEndpoint("a", "http://a:1")
	fresh.LastChecked = time.Now().Add(time.Second)
	ep, err := q.Select(context.Background(), []*domain.Endpoint{fresh})
	if err != nil || ep.URLString != a.URLString {
		t.Fatalf("fresh copy should be used: %v %v", ep, err)
	}
}

// A node that was offline when the request arrived is not among its
// candidates; on recovery the request looks its endpoints up again.
func TestQueue_RecoveryAddsEndpointMissingFromCandidates(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	fillSlots(t, q, a)

	calls := make(chan struct{}, 4)
	refresh := func(context.Context) []*domain.Endpoint {
		calls <- struct{}{}
		return []*domain.Endpoint{makeEndpoint("a", "http://a:1"), makeEndpoint("b", "http://b:1")}
	}
	ch := selectAsync(refreshCtx(refresh), q, []*domain.Endpoint{a})
	waitForDepth(t, q, 1)

	q.EndpointRecovered(b)
	if r := mustReceive(t, ch); r.err != nil || r.endpoint.URLString != b.URLString {
		t.Fatalf("want dispatch to recovered b, got %+v", r)
	}
	if len(calls) != 1 {
		t.Fatalf("refresher ran %d times, want 1", len(calls))
	}
}

// The handler may have shaped the request for the backend types it chose
// (passthrough, path rewrites), so a recovered node of another type is not
// taken.
func TestQueue_RecoveryIgnoresOtherBackendTypes(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	a.Type = "llamacpp"
	fillSlots(t, q, a)

	refresh := func(context.Context) []*domain.Endpoint {
		b := makeEndpoint("b", "http://b:1")
		b.Type = "ollama"
		return []*domain.Endpoint{b}
	}
	ch := selectAsync(refreshCtx(refresh), q, []*domain.Endpoint{a})
	waitForDepth(t, q, 1)

	q.EndpointRecovered(makeEndpoint("b", "http://b:1"))
	expectQueued(t, ch)
	if got := q.Stats().Waiting[0].Candidates; got != 1 {
		t.Fatalf("candidates = %d, want 1", got)
	}
}

// A waiter that already lists the recovered node needs no lookup.
func TestQueue_RecoverySkipsRefreshWhenAlreadyCandidate(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	fillSlots(t, q, a)

	calls := make(chan struct{}, 4)
	refresh := func(context.Context) []*domain.Endpoint {
		calls <- struct{}{}
		return nil
	}
	ch := selectAsync(refreshCtx(refresh), q, []*domain.Endpoint{a})
	waitForDepth(t, q, 1)

	q.EndpointRecovered(a)
	expectQueued(t, ch)
	if len(calls) != 0 {
		t.Fatalf("refresher ran %d times, want 0", len(calls))
	}
}

// The retry handler reports a failed connection before it releases the slot,
// so the freed slot doesn't go to a waiter that would hit the same backend.
func TestQueue_FailedConnectionSlotIsNotHandedOn(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	fillSlots(t, q, a, b)

	ch := selectAsync(context.Background(), q, []*domain.Endpoint{a, b})
	waitForDepth(t, q, 1)

	q.EndpointFailed(a)
	q.DecrementConnections(a)
	expectQueued(t, ch)

	q.EndpointRecovered(a)
	if r := mustReceive(t, ch); r.err != nil || r.endpoint.URLString != a.URLString {
		t.Fatalf("want dispatch to a once recovered, got %+v", r)
	}
}
