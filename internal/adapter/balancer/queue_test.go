package balancer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

func defaultQueueConfig() config.QueueConfig {
	return config.QueueConfig{Enabled: true, MaxSize: 10, Timeout: time.Second, DefaultSlots: 1}
}

func makeQueue(t *testing.T, cfg config.QueueConfig) *QueueWrapper {
	t.Helper()
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), cfg, nil)
	t.Cleanup(q.Stop)
	return q
}

// selectAsync runs Select in a goroutine and returns a channel with its result.
func selectAsync(ctx context.Context, q *QueueWrapper, eps []*domain.Endpoint) <-chan queueResult {
	ch := make(chan queueResult, 1)
	go func() {
		ep, err := q.Select(ctx, eps)
		ch <- queueResult{endpoint: ep, err: err}
	}()
	return ch
}

// waitForDepth blocks until the queue holds n waiters, so tests don't race the enqueue.
func waitForDepth(t *testing.T, q *QueueWrapper, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if q.Stats().Depth == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue depth never reached %d (now %d)", n, q.Stats().Depth)
}

func mustReceive(t *testing.T, ch <-chan queueResult) queueResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Select to return")
		return queueResult{}
	}
}

func TestQueue_FastPathUntilSlotsFull(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	a.MaxConcurrent = 2
	eps := []*domain.Endpoint{a}

	for i := range 2 {
		ep, err := q.Select(context.Background(), eps)
		if err != nil || ep != a {
			t.Fatalf("select %d: got %v, %v", i, ep, err)
		}
	}
	if s := q.Stats(); s.Depth != 0 || s.Enqueued != 0 {
		t.Fatalf("fast path should not enqueue: %+v", s)
	}

	ch := selectAsync(context.Background(), q, eps)
	waitForDepth(t, q, 1)

	q.DecrementConnections(a)
	if r := mustReceive(t, ch); r.err != nil || r.endpoint != a {
		t.Fatalf("queued select: %+v", r)
	}
}

func TestQueue_FIFOOrder(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	eps := []*domain.Endpoint{a}

	if _, err := q.Select(context.Background(), eps); err != nil {
		t.Fatal(err)
	}

	var order []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := q.Select(context.Background(), eps); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			q.DecrementConnections(a)
		}(i)
		waitForDepth(t, q, i+1)
	}

	q.DecrementConnections(a)
	wg.Wait()
	if len(order) != 3 || order[0] != 0 || order[1] != 1 || order[2] != 2 {
		t.Fatalf("want FIFO order [0 1 2], got %v", order)
	}
}

func TestQueue_SkipsWaiterWhoseEndpointsAreFull(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")

	// Fill both, then queue a request for a (head) and one for b (behind it).
	for _, ep := range []*domain.Endpoint{a, b} {
		if _, err := q.Select(context.Background(), []*domain.Endpoint{ep}); err != nil {
			t.Fatal(err)
		}
	}
	forA := selectAsync(context.Background(), q, []*domain.Endpoint{a})
	waitForDepth(t, q, 1)
	forB := selectAsync(context.Background(), q, []*domain.Endpoint{b})
	waitForDepth(t, q, 2)

	// Freeing b must serve the request behind the head, not wait on a.
	q.DecrementConnections(b)
	if r := mustReceive(t, forB); r.endpoint != b {
		t.Fatalf("b waiter: %+v", r)
	}
	select {
	case r := <-forA:
		t.Fatalf("a waiter should still be queued, got %+v", r)
	default:
	}
	q.DecrementConnections(a)
	if r := mustReceive(t, forA); r.endpoint != a {
		t.Fatalf("a waiter: %+v", r)
	}
}

func TestQueue_Timeout(t *testing.T) {
	cfg := defaultQueueConfig()
	cfg.Timeout = 30 * time.Millisecond
	q := makeQueue(t, cfg)
	a := makeEndpoint("a", "http://a:1")
	eps := []*domain.Endpoint{a}
	if _, err := q.Select(context.Background(), eps); err != nil {
		t.Fatal(err)
	}

	outcome := &domain.QueueOutcome{}
	ctx := context.WithValue(context.Background(), constants.ContextQueueOutcomeKey, outcome)
	_, err := q.Select(ctx, eps)
	if !errors.Is(err, ErrQueueTimeout) {
		t.Fatalf("want ErrQueueTimeout, got %v", err)
	}
	if !outcome.Queued || outcome.Wait < cfg.Timeout {
		t.Fatalf("outcome not recorded: %+v", outcome)
	}
	if s := q.Stats(); s.TimedOut != 1 || s.Depth != 0 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestQueue_ContextCancelLeavesQueue(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	eps := []*domain.Endpoint{a}
	if _, err := q.Select(context.Background(), eps); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := selectAsync(ctx, q, eps)
	waitForDepth(t, q, 1)
	cancel()
	if r := mustReceive(t, ch); !errors.Is(r.err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %+v", r)
	}

	// The slot must still be reusable exactly once after release.
	q.DecrementConnections(a)
	if _, err := q.Select(context.Background(), eps); err != nil {
		t.Fatal(err)
	}
	if s := q.Stats(); s.Cancelled != 1 || s.Endpoints[0].InUse != 1 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestQueue_RejectsWhenFull(t *testing.T) {
	cfg := defaultQueueConfig()
	cfg.MaxSize = 1
	q := makeQueue(t, cfg)
	a := makeEndpoint("a", "http://a:1")
	eps := []*domain.Endpoint{a}
	if _, err := q.Select(context.Background(), eps); err != nil {
		t.Fatal(err)
	}
	ch := selectAsync(context.Background(), q, eps)
	waitForDepth(t, q, 1)

	if _, err := q.Select(context.Background(), eps); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
	q.DecrementConnections(a)
	mustReceive(t, ch)
}

func TestQueue_PurgeDeadEndpointsFailsStrandedWaiters(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	for _, ep := range []*domain.Endpoint{a, b} {
		if _, err := q.Select(context.Background(), []*domain.Endpoint{ep}); err != nil {
			t.Fatal(err)
		}
	}
	forA := selectAsync(context.Background(), q, []*domain.Endpoint{a})
	waitForDepth(t, q, 1)
	forB := selectAsync(context.Background(), q, []*domain.Endpoint{b})
	waitForDepth(t, q, 2)

	q.PurgeDeadEndpoints([]*domain.Endpoint{b})
	if r := mustReceive(t, forA); !errors.Is(r.err, ErrQueueEndpointsGone) {
		t.Fatalf("a waiter: %+v", r)
	}
	if q.Stats().Depth != 1 {
		t.Fatal("b waiter should still be queued")
	}
	q.DecrementConnections(b)
	mustReceive(t, forB)
}

func TestQueue_StopFailsWaiters(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	eps := []*domain.Endpoint{a}
	if _, err := q.Select(context.Background(), eps); err != nil {
		t.Fatal(err)
	}
	ch := selectAsync(context.Background(), q, eps)
	waitForDepth(t, q, 1)
	q.Stop()
	if r := mustReceive(t, ch); !errors.Is(r.err, ErrQueueClosed) {
		t.Fatalf("want ErrQueueClosed, got %+v", r)
	}
	if _, err := q.Select(context.Background(), eps); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("select after stop: %v", err)
	}
}

func TestQueue_NoRoutableEndpointsDoesNotQueue(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	a.Status = domain.StatusOffline
	if _, err := q.Select(context.Background(), []*domain.Endpoint{a}); err == nil {
		t.Fatal("expected the inner selector's error")
	}
	if q.Stats().Enqueued != 0 {
		t.Fatal("nothing routable should never be queued")
	}
}

type fixedSlots map[string]int

func (f fixedSlots) Slots(ep *domain.Endpoint) (int, string) {
	return f[ep.URLString], SlotSourceDiscovered
}

func TestQueue_WakeAfterCapacityGrows(t *testing.T) {
	slots := fixedSlots{"http://a:1": 1}
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), slots)
	t.Cleanup(q.Stop)
	a := makeEndpoint("a", "http://a:1")
	eps := []*domain.Endpoint{a}
	if _, err := q.Select(context.Background(), eps); err != nil {
		t.Fatal(err)
	}
	ch := selectAsync(context.Background(), q, eps)
	waitForDepth(t, q, 1)

	q.mu.Lock()
	slots["http://a:1"] = 2
	q.mu.Unlock()
	q.Wake()
	if r := mustReceive(t, ch); r.endpoint != a {
		t.Fatalf("waiter: %+v", r)
	}
}

// TestQueue_StressNeverExceedsSlots hammers the queue from many goroutines,
// some of which give up early, and checks that in-flight work never exceeds
// the slot limit and that no slot leaks.
func TestQueue_StressNeverExceedsSlots(t *testing.T) {
	cfg := defaultQueueConfig()
	cfg.MaxSize = 1000
	cfg.Timeout = 5 * time.Second
	q := makeQueue(t, cfg)
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	a.MaxConcurrent, b.MaxConcurrent = 3, 2
	eps := []*domain.Endpoint{a, b}

	var inFlight = map[string]*atomic.Int64{a.URLString: {}, b.URLString: {}}
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			if i%7 == 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Duration(i%5)*time.Millisecond)
				defer cancel()
			}
			ep, err := q.Select(ctx, eps)
			if err != nil {
				return
			}
			n := inFlight[ep.URLString].Add(1)
			if n > int64(ep.MaxConcurrent) {
				t.Errorf("%s exceeded slots: %d > %d", ep.Name, n, ep.MaxConcurrent)
			}
			time.Sleep(200 * time.Microsecond)
			inFlight[ep.URLString].Add(-1)
			q.DecrementConnections(ep)
		}(i)
	}
	wg.Wait()

	s := q.Stats()
	if s.Depth != 0 {
		t.Fatalf("queue not drained: %+v", s)
	}
	for _, e := range s.Endpoints {
		if e.InUse != 0 {
			t.Fatalf("slot leaked on %s: %+v", e.Name, e)
		}
	}
}

func TestQueue_StatsListsWaitersWithCallerAndPin(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("node-a", "http://a:1")
	eps := []*domain.Endpoint{a}
	if _, err := q.Select(context.Background(), eps); err != nil {
		t.Fatal(err)
	}
	q.SetPinLookup(func(keys []string) map[string]string {
		out := map[string]string{}
		for _, k := range keys {
			if k == "pinned-key" {
				out[k] = "http://a:1"
			}
		}
		return out
	})

	enqueue := func(id, ip, key string) <-chan queueResult {
		ctx := context.WithValue(context.Background(), constants.ContextQueueCallerKey,
			&domain.QueueCaller{RequestID: id, ClientIP: ip, Model: "qwen", UserAgent: "curl/8"})
		if key != "" {
			ctx = context.WithValue(ctx, constants.ContextStickyKeyKey, key)
		}
		return selectAsync(ctx, q, eps)
	}
	first := enqueue("req-1", "10.0.0.5", "pinned-key")
	waitForDepth(t, q, 1)
	second := enqueue("req-2", "10.0.0.6", "")
	waitForDepth(t, q, 2)

	s := q.Stats()
	if len(s.Waiting) != 2 {
		t.Fatalf("waiting = %+v", s.Waiting)
	}
	w0, w1 := s.Waiting[0], s.Waiting[1]
	if w0.RequestID != "req-1" || w0.ClientIP != "10.0.0.5" || w0.Model != "qwen" || w0.UserAgent != "curl/8" {
		t.Fatalf("first waiter caller: %+v", w0)
	}
	if w0.StickyEndpoint != "node-a" {
		t.Fatalf("first waiter should be pinned to node-a, got %q", w0.StickyEndpoint)
	}
	if w1.RequestID != "req-2" || w1.StickyEndpoint != "" {
		t.Fatalf("second waiter: %+v", w1)
	}
	if w0.EnqueuedAt.After(w1.EnqueuedAt) || w0.Candidates != 1 {
		t.Fatalf("listing should be oldest first: %+v", s.Waiting)
	}

	q.DecrementConnections(a)
	mustReceive(t, first)
	q.DecrementConnections(a)
	mustReceive(t, second)
	if s := q.Stats(); s.WaitsCompleted != 2 || s.WaitTotalMs <= 0 || len(s.Waiting) != 0 {
		t.Fatalf("counters after drain: %+v", s)
	}
}
