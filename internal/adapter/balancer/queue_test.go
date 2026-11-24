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
		if err != nil || ep == nil || ep.URLString != a.URLString {
			t.Fatalf("select %d: got %v, %v", i, ep, err)
		}
	}
	if s := q.Stats(); s.Depth != 0 || s.Enqueued != 0 {
		t.Fatalf("fast path should not enqueue: %+v", s)
	}

	ch := selectAsync(context.Background(), q, eps)
	waitForDepth(t, q, 1)

	q.DecrementConnections(a)
	if r := mustReceive(t, ch); r.err != nil || r.endpoint == nil || r.endpoint.URLString != a.URLString {
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
	if r := mustReceive(t, forB); r.endpoint == nil || r.endpoint.URLString != b.URLString {
		t.Fatalf("b waiter: %+v", r)
	}
	select {
	case r := <-forA:
		t.Fatalf("a waiter should still be queued, got %+v", r)
	default:
	}
	q.DecrementConnections(a)
	if r := mustReceive(t, forA); r.endpoint == nil || r.endpoint.URLString != a.URLString {
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

func (fixedSlots) ModelSlots(*domain.Endpoint, string) (int, bool)   { return 0, false }
func (fixedSlots) ModelSlotList(*domain.Endpoint) map[string]int     { return nil }
func (fixedSlots) GroupSlots(*domain.Endpoint, string) (int, string) { return 1, SlotSourceDefault }
func (fixedSlots) ModelState(*domain.Endpoint, string) ModelState    { return ModelStateUnknown }

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
	if r := mustReceive(t, ch); r.endpoint == nil || r.endpoint.URLString != a.URLString {
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
	s = q.Stats()
	if s.WaitsCompleted != 2 || s.WaitTotalMs <= 0 || len(s.Waiting) != 0 {
		t.Fatalf("counters after drain: %+v", s)
	}
	if s.MinWaitMs <= 0 || s.MinWaitMs > s.MaxWaitMs {
		t.Fatalf("wait range %v-%v", s.MinWaitMs, s.MaxWaitMs)
	}
}

// modelSlots is a resolver with an endpoint-wide limit plus per-model limits,
// like a llama-swap node running several models.
type modelSlots struct {
	models map[string]int
	groups map[string]int
	// loaded, when set, lists models reported loaded per endpoint URL;
	// anything else on that URL reads as cold. Unset: everything unknown.
	loaded   map[string][]string
	endpoint int
}

func (m modelSlots) ModelState(ep *domain.Endpoint, model string) ModelState {
	list, ok := m.loaded[ep.URLString]
	if !ok {
		return ModelStateUnknown
	}
	for _, l := range list {
		if l == model {
			return ModelStateLoaded
		}
	}
	return ModelStateCold
}

func (m modelSlots) Slots(*domain.Endpoint) (int, string) { return m.endpoint, SlotSourceDiscovered }
func (m modelSlots) ModelSlots(_ *domain.Endpoint, model string) (int, bool) {
	n, ok := m.models[model]
	return n, ok
}
func (m modelSlots) ModelSlotList(*domain.Endpoint) map[string]int { return m.models }
func (m modelSlots) GroupSlots(_ *domain.Endpoint, group string) (int, string) {
	if n, ok := m.groups[group]; ok {
		return n, SlotSourceConfig
	}
	return 1, SlotSourceDefault
}

func modelCtx(model string) context.Context {
	return context.WithValue(context.Background(), constants.ContextModelKey, model)
}

func TestQueue_PerModelLimitHoldsWhileEndpointHasRoom(t *testing.T) {
	res := modelSlots{endpoint: 5, models: map[string]int{"qwen": 1, "llama": 3}}
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), res)
	t.Cleanup(q.Stop)
	node := makeEndpoint("node", "http://node:1")
	eps := []*domain.Endpoint{node}

	first, err := q.Select(modelCtx("qwen"), eps)
	if err != nil {
		t.Fatal(err)
	}
	// The node has 4 endpoint slots left, but qwen's own single slot is taken.
	second := selectAsync(modelCtx("qwen"), q, eps)
	waitForDepth(t, q, 1)

	// Another model on the same node is not held up by qwen being full.
	for range 3 {
		if _, err := q.Select(modelCtx("llama"), eps); err != nil {
			t.Fatalf("llama should be admitted: %v", err)
		}
	}
	// A model the node has not loaded has no limit of its own; the endpoint
	// limit (5) still applies and is now reached: 1 qwen + 3 llama + this one.
	if _, err := q.Select(modelCtx("gemma"), eps); err != nil {
		t.Fatalf("unloaded model should be bounded only by the endpoint: %v", err)
	}

	s := q.Stats()
	got := map[string]QueueModelStats{}
	for _, m := range s.Endpoints[0].Models {
		got[m.Model] = m
	}
	if got["qwen"].InUse != 1 || got["qwen"].Slots != 1 || got["llama"].InUse != 3 || got["gemma"].InUse != 1 || got["gemma"].Slots != 0 {
		t.Fatalf("model stats: %+v", s.Endpoints[0].Models)
	}

	// Releasing qwen's slot dispatches the waiting qwen request.
	q.DecrementConnections(first)
	if r := mustReceive(t, second); r.err != nil {
		t.Fatalf("queued qwen request: %+v", r)
	}
}

func TestQueue_ModelResolverAppliesPerEndpoint(t *testing.T) {
	res := modelSlots{endpoint: 4, models: map[string]int{"qwen3:8b-native": 1}}
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), res)
	t.Cleanup(q.Stop)
	// An alias resolves to a backend-specific name; the per-model limit must
	// be looked up under that name, not the alias the client sent.
	q.SetModelResolver(func(context.Context, *domain.Endpoint) string { return "qwen3:8b-native" })
	node := makeEndpoint("node", "http://node:1")
	eps := []*domain.Endpoint{node}

	if _, err := q.Select(modelCtx("my-alias"), eps); err != nil {
		t.Fatal(err)
	}
	ch := selectAsync(modelCtx("my-alias"), q, eps)
	waitForDepth(t, q, 1)
	q.Stop()
	mustReceive(t, ch)
}

func TestQueue_ReleaseWithForeignPointerKeepsModelCountsRight(t *testing.T) {
	res := modelSlots{endpoint: 2, models: map[string]int{"qwen": 1}}
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), res)
	t.Cleanup(q.Stop)
	node := makeEndpoint("node", "http://node:1")
	eps := []*domain.Endpoint{node}

	if _, err := q.Select(modelCtx("qwen"), eps); err != nil {
		t.Fatal(err)
	}
	// Release with the caller's own pointer, not the copy Select returned.
	q.DecrementConnections(node)
	if s := q.Stats(); s.Endpoints[0].InUse != 0 || s.Endpoints[0].Models[0].InUse != 0 {
		t.Fatalf("slot not released: %+v", s.Endpoints[0])
	}
	if _, err := q.Select(modelCtx("qwen"), eps); err != nil {
		t.Fatalf("qwen slot should be free again: %v", err)
	}
}

func TestQueue_StressPerModelNeverExceedsSlots(t *testing.T) {
	res := modelSlots{endpoint: 4, models: map[string]int{"a": 1, "b": 2}}
	cfg := defaultQueueConfig()
	cfg.MaxSize, cfg.Timeout = 1000, 5*time.Second
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), cfg, res)
	t.Cleanup(q.Stop)
	node := makeEndpoint("node", "http://node:1")
	eps := []*domain.Endpoint{node}

	var total atomic.Int64
	perModel := map[string]*atomic.Int64{"a": {}, "b": {}, "c": {}}
	var wg sync.WaitGroup
	for i := range 150 {
		model := []string{"a", "b", "c"}[i%3]
		wg.Add(1)
		go func() {
			defer wg.Done()
			ep, err := q.Select(modelCtx(model), eps)
			if err != nil {
				return
			}
			if n := total.Add(1); n > 4 {
				t.Errorf("endpoint exceeded its 4 slots: %d", n)
			}
			if n := perModel[model].Add(1); model != "c" && n > int64(res.models[model]) {
				t.Errorf("model %s exceeded its %d slots: %d", model, res.models[model], n)
			}
			time.Sleep(200 * time.Microsecond)
			perModel[model].Add(-1)
			total.Add(-1)
			q.DecrementConnections(ep)
		}()
	}
	wg.Wait()
	s := q.Stats()
	for _, m := range s.Endpoints[0].Models {
		if m.InUse != 0 {
			t.Fatalf("model slot leaked: %+v", m)
		}
	}
	if s.Endpoints[0].InUse != 0 || s.Depth != 0 {
		t.Fatalf("not drained: %+v", s)
	}
}

func TestStoreMinTreatsZeroAsUnset(t *testing.T) {
	var v atomic.Int64
	storeMin(&v, 500)
	storeMin(&v, 900)
	storeMin(&v, 200)
	if v.Load() != 200 {
		t.Fatalf("min = %d, want 200", v.Load())
	}
}

// An overflow endpoint no request has needed yet must still appear in the
// stats, or the dashboard hides it until the first spill-over.
func TestQueue_StatsListsConfiguredEndpointsNotYetSeen(t *testing.T) {
	local := makeEndpoint("local", "http://local:1")
	overflow := makeEndpoint("overflow", "http://overflow:1")
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), nil)
	t.Cleanup(q.Stop)
	q.SetEndpointLister(func(context.Context) ([]*domain.Endpoint, error) {
		return []*domain.Endpoint{local, overflow}, nil
	})
	if _, err := q.Select(context.Background(), []*domain.Endpoint{local}); err != nil {
		t.Fatal(err)
	}

	byName := map[string]QueueEndpointStats{}
	for _, ep := range q.Stats().Endpoints {
		byName[ep.Name] = ep
	}
	if len(byName) != 2 {
		t.Fatalf("endpoints = %v, want local and overflow", byName)
	}
	if byName["local"].InUse != 1 || byName["overflow"].InUse != 0 {
		t.Fatalf("in use local=%d overflow=%d", byName["local"].InUse, byName["overflow"].InUse)
	}
	if byName["overflow"].Slots < 1 {
		t.Fatalf("overflow slots = %d", byName["overflow"].Slots)
	}
}
