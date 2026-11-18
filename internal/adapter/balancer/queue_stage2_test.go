package balancer

import (
	"context"
	"testing"
	"time"

	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

// pins is a sticky store stand-in for SetStickyHold.
type pins map[string]string

func (p pins) peek(key string) (string, bool) {
	url, ok := p[key]
	return url, ok
}

func stickyCtx(key, model string) context.Context {
	ctx := context.WithValue(context.Background(), constants.ContextStickyKeyKey, key)
	if model != "" {
		ctx = context.WithValue(ctx, constants.ContextModelKey, model)
	}
	return ctx
}

func TestQueue_StickyHoldWaitsForBusyPinnedEndpoint(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	eps := []*domain.Endpoint{a, b}
	q.SetStickyHold(pins{"conv-1": a.URLString}.peek, time.Minute)

	busy, err := q.Select(context.Background(), []*domain.Endpoint{a})
	if err != nil {
		t.Fatal(err)
	}
	// b is free, but conv-1's KV cache is on a: wait for a.
	ch := selectAsync(stickyCtx("conv-1", ""), q, eps)
	waitForDepth(t, q, 1)
	if s := q.Stats(); !s.Waiting[0].Holding {
		t.Fatalf("waiter should report holding: %+v", s.Waiting[0])
	}

	q.DecrementConnections(busy)
	r := mustReceive(t, ch)
	if r.err != nil || r.endpoint.URLString != a.URLString {
		t.Fatalf("held request should get its pinned endpoint a: %+v", r)
	}
	if s := q.Stats(); s.HoldKept != 1 || s.HoldMoved != 0 {
		t.Fatalf("hold counters: kept=%d moved=%d", s.HoldKept, s.HoldMoved)
	}
}

func TestQueue_StickyHoldLapsesToAnyFreeEndpoint(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	q.SetStickyHold(pins{"conv-1": a.URLString}.peek, 60*time.Millisecond)

	if _, err := q.Select(context.Background(), []*domain.Endpoint{a}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	// a stays busy throughout; nothing else happens, so only the hold timer
	// can move this request on to b.
	r := mustReceive(t, selectAsync(stickyCtx("conv-1", ""), q, []*domain.Endpoint{a, b}))
	if r.err != nil || r.endpoint.URLString != b.URLString {
		t.Fatalf("after the hold the request should take b: %+v", r)
	}
	if waited := time.Since(start); waited < 50*time.Millisecond {
		t.Fatalf("moved after %s, before the hold lapsed", waited)
	}
	if s := q.Stats(); s.HoldMoved != 1 {
		t.Fatalf("hold_moved = %d, want 1", s.HoldMoved)
	}
}

func TestQueue_NoHoldWithoutPinOrWhenOff(t *testing.T) {
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	other := makeEndpoint("other", "http://other:1")

	cases := map[string]struct {
		peek func(string) (string, bool)
		hold time.Duration
	}{
		"hold off":               {peek: pins{"conv-1": a.URLString}.peek, hold: 0},
		"no pin for the key":     {peek: pins{}.peek, hold: time.Minute},
		"pin on a non-candidate": {peek: pins{"conv-1": other.URLString}.peek, hold: time.Minute},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			q := makeQueue(t, defaultQueueConfig())
			q.SetStickyHold(tc.peek, tc.hold)
			if _, err := q.Select(context.Background(), []*domain.Endpoint{a}); err != nil {
				t.Fatal(err)
			}
			ep, err := q.Select(stickyCtx("conv-1", ""), []*domain.Endpoint{a, b})
			if err != nil || ep.URLString != b.URLString {
				t.Fatalf("should go straight to free b: %v %v", ep, err)
			}
		})
	}
}

// gpuNode is taki-like: one GPU slot shared by the swap models.
func gpuNode(loaded ...string) (modelSlots, *domain.Endpoint) {
	node := makeEndpoint("taki", "http://taki:8080")
	node.SlotGroups = []domain.SlotGroup{{Name: "gpu", Models: []string{"qwen*"}}}
	return modelSlots{
		endpoint: 5,
		models:   map[string]int{"qwen3.6": 1},
		groups:   map[string]int{"gpu": 1},
		loaded:   map[string][]string{node.URLString: loaded},
	}, node
}

func TestQueue_WarmFirstServesLoadedModelBeforeSwap(t *testing.T) {
	res, node := gpuNode("qwen3.6")
	cfg := defaultQueueConfig()
	cfg.WarmFirstMaxWait = time.Minute
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), cfg, res)
	t.Cleanup(q.Stop)
	eps := []*domain.Endpoint{node}

	running, err := q.Select(modelCtx("qwen3.6"), eps)
	if err != nil {
		t.Fatal(err)
	}
	cold := selectAsync(modelCtx("qwen3.8"), q, eps) // arrives first
	waitForDepth(t, q, 1)
	warm := selectAsync(modelCtx("qwen3.6"), q, eps)
	waitForDepth(t, q, 2)
	if s := q.Stats(); !s.Waiting[0].Cold || s.Waiting[1].Cold {
		t.Fatalf("cold flags: %+v", s.Waiting)
	}

	// The GPU slot frees: the loaded model's request goes first, so no swap
	// is forced while work for qwen3.6 is waiting.
	q.DecrementConnections(running)
	w := mustReceive(t, warm)
	select {
	case r := <-cold:
		t.Fatalf("cold request jumped the warm one: %+v", r)
	default:
	}
	q.DecrementConnections(w.endpoint)
	if r := mustReceive(t, cold); r.err != nil {
		t.Fatalf("cold request after the warm one: %+v", r)
	}
}

func TestQueue_WarmFirstAgeCapStopsStarvation(t *testing.T) {
	res, node := gpuNode("qwen3.6")
	cfg := defaultQueueConfig()
	cfg.WarmFirstMaxWait = 40 * time.Millisecond
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), cfg, res)
	t.Cleanup(q.Stop)
	eps := []*domain.Endpoint{node}

	running, err := q.Select(modelCtx("qwen3.6"), eps)
	if err != nil {
		t.Fatal(err)
	}
	cold := selectAsync(modelCtx("qwen3.8"), q, eps)
	waitForDepth(t, q, 1)
	time.Sleep(60 * time.Millisecond) // the cold request is now past the cap
	warm := selectAsync(modelCtx("qwen3.6"), q, eps)
	waitForDepth(t, q, 2)

	q.DecrementConnections(running)
	c := mustReceive(t, cold)
	if c.err != nil {
		t.Fatalf("aged cold request should go first: %+v", c)
	}
	select {
	case r := <-warm:
		t.Fatalf("warm request jumped an aged one: %+v", r)
	default:
	}
	q.DecrementConnections(c.endpoint)
	mustReceive(t, warm)
}

func TestQueue_PrefersEndpointWithModelLoaded(t *testing.T) {
	cold := makeEndpoint("cold", "http://cold:1")
	warm := makeEndpoint("warm", "http://warm:1")
	opaque := makeEndpoint("ollama", "http://ollama:1")
	res := modelSlots{endpoint: 4, loaded: map[string][]string{
		cold.URLString: {"other"},
		warm.URLString: {"qwen"},
	}}
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), res)
	t.Cleanup(q.Stop)

	// Round robin would alternate; the loaded endpoint wins every time.
	for range 3 {
		ep, err := q.Select(modelCtx("qwen"), []*domain.Endpoint{cold, warm, opaque})
		if err != nil || ep.URLString != warm.URLString {
			t.Fatalf("want the endpoint with qwen loaded, got %v %v", ep, err)
		}
	}
	// With no loaded option, an endpoint that does not report beats a cold load.
	ep, err := q.Select(modelCtx("gemma"), []*domain.Endpoint{cold, opaque})
	if err != nil || ep.URLString != opaque.URLString {
		t.Fatalf("want the non-reporting endpoint over a cold load, got %v %v", ep, err)
	}
}

func TestQueue_StickyHoldWithRealStickyWrapper(t *testing.T) {
	// makeWrapper's selector has no stats collector, which a release needs.
	sw := NewStickySessionWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultStickyConfig())
	sw.Start()
	t.Cleanup(sw.Stop)
	q := NewQueueWrapper(sw, defaultQueueConfig(), nil)
	t.Cleanup(q.Stop)
	q.SetStickyHold(sw.Peek, time.Minute)
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	eps := []*domain.Endpoint{a, b}

	// Turn one pins the conversation to whichever endpoint the strategy picks.
	ctx1, _ := injectKey(context.Background(), "conv-9", "session_header")
	first, err := q.Select(ctx1, eps)
	if err != nil {
		t.Fatal(err)
	}
	pinned := first.URLString

	// Turn two arrives while turn one still runs: the other endpoint is free,
	// but the request holds for the pinned one.
	ctx2, outcome := injectKey(context.Background(), "conv-9", "session_header")
	ch := selectAsync(ctx2, q, eps)
	waitForDepth(t, q, 1)

	// Peeking on every dispatch pass must not register as sticky traffic.
	before := sw.Stats()
	q.Wake()
	q.Wake()
	if after := sw.Stats(); after.Hits != before.Hits || after.Misses != before.Misses {
		t.Fatalf("hold lookups changed sticky stats: %+v -> %+v", before, after)
	}

	q.DecrementConnections(first)
	r := mustReceive(t, ch)
	if r.err != nil || r.endpoint.URLString != pinned {
		t.Fatalf("turn two should land on pinned %s: %+v", pinned, r)
	}
	if outcome.Result != "hit" {
		t.Fatalf("sticky outcome = %q, want hit", outcome.Result)
	}
}
