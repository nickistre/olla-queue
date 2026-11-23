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

func TestQueue_StickyHoldLapseCountsMovedWithRealStickyWrapper(t *testing.T) {
	// The sticky wrapper re-pins the key as the lapsed request moves, so the
	// counter must compare against the pin it held before the move.
	sw := NewStickySessionWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultStickyConfig())
	sw.Start()
	t.Cleanup(sw.Stop)
	q := NewQueueWrapper(sw, defaultQueueConfig(), nil)
	t.Cleanup(q.Stop)
	q.SetStickyHold(sw.Peek, 60*time.Millisecond)
	a := makeEndpoint("a", "http://a:1")
	b := makeEndpoint("b", "http://b:1")
	eps := []*domain.Endpoint{a, b}

	ctx1, _ := injectKey(context.Background(), "conv-7", "session_header")
	first, err := q.Select(ctx1, eps)
	if err != nil {
		t.Fatal(err)
	}
	other := b.URLString
	if first.URLString == b.URLString {
		other = a.URLString
	}

	// Turn one stays busy, so turn two waits out its hold and moves.
	ctx2, outcome := injectKey(context.Background(), "conv-7", "session_header")
	r := mustReceive(t, selectAsync(ctx2, q, eps))
	if r.err != nil || r.endpoint.URLString != other {
		t.Fatalf("after the hold turn two should move to %s: %+v", other, r)
	}
	if outcome.Result != "repin" {
		t.Fatalf("sticky outcome = %q, want repin", outcome.Result)
	}
	if pin, _ := sw.Peek("conv-7"); pin != other {
		t.Fatalf("pin = %q, want re-pinned to %s", pin, other)
	}
	if s := q.Stats(); s.HoldMoved != 1 || s.HoldKept != 0 {
		t.Fatalf("hold counters: kept=%d moved=%d, want 0/1", s.HoldKept, s.HoldMoved)
	}
}

// twoGroupNode is taki with an embeddings group beside the GPU group, and a
// discovered endpoint total of 1: only qwen3.6 is loaded.
func twoGroupNode() (modelSlots, *domain.Endpoint) {
	node := makeEndpoint("taki", "http://taki:8080")
	node.SlotGroups = []domain.SlotGroup{
		{Name: "emb", Models: []string{"nomic*"}},
		{Name: "gpu", Models: []string{"qwen*"}},
	}
	return modelSlots{
		endpoint: 1,
		models:   map[string]int{"qwen3.6": 1},
		groups:   map[string]int{"gpu": 1, "emb": 1},
	}, node
}

func TestQueue_DiscoveredEndpointTotalDoesNotCapOtherGroups(t *testing.T) {
	res, node := twoGroupNode()
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), res)
	t.Cleanup(q.Stop)
	eps := []*domain.Endpoint{node}

	if _, err := q.Select(modelCtx("qwen3.6"), eps); err != nil {
		t.Fatal(err)
	}
	// The GPU group is full, which fills the discovered total too, but the
	// embeddings group has its own slot.
	emb := selectAsync(modelCtx("nomic-embed"), q, eps)
	if r := mustReceive(t, emb); r.err != nil {
		t.Fatalf("embeddings request should not wait: %+v", r)
	}
}

func TestQueue_ConfiguredEndpointLimitCapsEveryGroup(t *testing.T) {
	res, node := twoGroupNode()
	node.MaxConcurrent = 1
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), res)
	t.Cleanup(q.Stop)
	eps := []*domain.Endpoint{node}

	running, err := q.Select(modelCtx("qwen3.6"), eps)
	if err != nil {
		t.Fatal(err)
	}
	emb := selectAsync(modelCtx("nomic-embed"), q, eps)
	waitForDepth(t, q, 1)
	q.DecrementConnections(running)
	if r := mustReceive(t, emb); r.err != nil {
		t.Fatalf("embeddings request after the slot freed: %+v", r)
	}
}

// graceQueue is a gpuNode queue with a swap grace, qwen3.6 loaded and running.
func graceQueue(t *testing.T, grace, warmCap time.Duration) (*QueueWrapper, []*domain.Endpoint, *domain.Endpoint) {
	t.Helper()
	res, node := gpuNode("qwen3.6")
	cfg := defaultQueueConfig()
	cfg.WarmFirstMaxWait = warmCap
	cfg.SwapGrace = grace
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), cfg, res)
	t.Cleanup(q.Stop)
	eps := []*domain.Endpoint{node}
	running, err := q.Select(modelCtx("qwen3.6"), eps)
	if err != nil {
		t.Fatal(err)
	}
	return q, eps, running
}

func TestQueue_SwapGraceKeepsSlotForNextTurn(t *testing.T) {
	q, eps, running := graceQueue(t, time.Second, time.Minute)
	cold := selectAsync(modelCtx("qwen3.8"), q, eps)
	waitForDepth(t, q, 1)

	// The turn ends; the next turn arrives inside the grace and gets the
	// slot, though a request for another model was waiting first.
	q.DecrementConnections(running)
	if s := q.Stats(); !s.Waiting[0].SwapGrace || s.SwapGraceSeconds != 1 {
		t.Fatalf("waiter should be held by swap grace: %+v", s)
	}
	next, err := q.Select(modelCtx("qwen3.6"), eps)
	if err != nil {
		t.Fatalf("next turn: %v", err)
	}
	select {
	case r := <-cold:
		t.Fatalf("cold request swapped the model out during the grace: %+v", r)
	default:
	}
	q.DecrementConnections(next)
	if q.Stats().Depth != 1 {
		t.Fatal("cold request should still be waiting")
	}
}

func TestQueue_SwapGraceLapsesWithoutNextTurn(t *testing.T) {
	q, eps, running := graceQueue(t, 50*time.Millisecond, time.Minute)
	cold := selectAsync(modelCtx("qwen3.8"), q, eps)
	waitForDepth(t, q, 1)

	start := time.Now()
	q.DecrementConnections(running)
	// No release follows: the grace's own timer has to dispatch it.
	if r := mustReceive(t, cold); r.err != nil {
		t.Fatalf("cold request after the grace: %+v", r)
	}
	if waited := time.Since(start); waited < 40*time.Millisecond {
		t.Fatalf("cold request went after %s, inside the grace", waited)
	}
}

func TestQueue_SwapGraceHoldsNewColdRequestsToo(t *testing.T) {
	q, eps, running := graceQueue(t, time.Second, time.Minute)
	q.DecrementConnections(running)

	// A request for another model arriving during the grace queues rather
	// than taking the free slot.
	cold := selectAsync(modelCtx("qwen3.8"), q, eps)
	waitForDepth(t, q, 1)
	select {
	case r := <-cold:
		t.Fatalf("new cold request took the slot during the grace: %+v", r)
	default:
	}
}

func TestQueue_SwapGraceStopsAtWarmFirstCap(t *testing.T) {
	q, eps, running := graceQueue(t, time.Second, 30*time.Millisecond)
	cold := selectAsync(modelCtx("qwen3.8"), q, eps)
	waitForDepth(t, q, 1)
	time.Sleep(50 * time.Millisecond) // past warm_first_max_wait

	q.DecrementConnections(running)
	if r := mustReceive(t, cold); r.err != nil {
		t.Fatalf("aged cold request should not be held: %+v", r)
	}
}

// multiSlotGPUNode is taki since qwen3.6 runs --parallel 4: the GPU group's
// limit is the loaded model's 4 slots, so it has room a request for another
// model could take.
func multiSlotGPUNode(t *testing.T, warmCap time.Duration, loaded ...string) (*QueueWrapper, *domain.Endpoint) {
	t.Helper()
	node := makeEndpoint("taki", "http://taki:8080")
	node.SlotGroups = []domain.SlotGroup{{Name: "gpu", Models: []string{"qwen*"}}}
	res := modelSlots{
		endpoint: 8,
		models:   map[string]int{"qwen3.6": 4, "qwen3.8": 2},
		groups:   map[string]int{"gpu": 4},
		loaded:   map[string][]string{node.URLString: loaded},
	}
	cfg := defaultQueueConfig()
	cfg.WarmFirstMaxWait = warmCap
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), cfg, res)
	t.Cleanup(q.Stop)
	return q, node
}

func assertWaiting(t *testing.T, ch <-chan queueResult, what string) {
	t.Helper()
	select {
	case r := <-ch:
		t.Fatalf("%s should still be waiting: %+v", what, r)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestQueue_ColdModelWaitsForSwapDomainToDrain(t *testing.T) {
	q, node := multiSlotGPUNode(t, time.Minute, "qwen3.6")
	eps := []*domain.Endpoint{node}

	running, err := q.Select(modelCtx("qwen3.6"), eps)
	if err != nil {
		t.Fatal(err)
	}
	// The group has 3 free slots, but taking one would swap qwen3.6 out
	// from under its running request.
	cold := selectAsync(modelCtx("qwen3.8"), q, eps)
	waitForDepth(t, q, 1)
	assertWaiting(t, cold, "cold request")

	q.DecrementConnections(running)
	if r := mustReceive(t, cold); r.err != nil {
		t.Fatalf("cold request once the domain drained: %+v", r)
	}
}

func TestQueue_LoadedModelWaitsWhileAColdModelLoads(t *testing.T) {
	q, node := multiSlotGPUNode(t, time.Minute, "qwen3.6")
	eps := []*domain.Endpoint{node}

	loading, err := q.Select(modelCtx("qwen3.8"), eps)
	if err != nil {
		t.Fatal(err)
	}
	// qwen3.8 is loading (discovery still says qwen3.6 is loaded). A request
	// for qwen3.6 now would swap straight back.
	back := selectAsync(modelCtx("qwen3.6"), q, eps)
	waitForDepth(t, q, 1)
	assertWaiting(t, back, "request for the outgoing model")

	q.DecrementConnections(loading)
	mustReceive(t, back)
}

func TestQueue_LoadedModelsShareADomainWithoutConflict(t *testing.T) {
	// A node that keeps both models in memory: no swap, so no conflict.
	q, node := multiSlotGPUNode(t, time.Minute, "qwen3.6", "qwen3.8")
	eps := []*domain.Endpoint{node}

	if _, err := q.Select(modelCtx("qwen3.6"), eps); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Select(modelCtx("qwen3.8"), eps); err != nil {
		t.Fatalf("both loaded, both should run: %v", err)
	}
}

func TestQueue_ColdModelWaitsForExternalTrafficOnLoadedModel(t *testing.T) {
	q, node := multiSlotGPUNode(t, time.Minute, "qwen3.6")
	eps := []*domain.Endpoint{node}
	// Another client is running qwen3.6 directly against llama-swap.
	q.ObserveBusy(node, BackendBusy{Valid: true, Total: 1, Models: map[string]int{"qwen3.6": 1}})

	cold := selectAsync(modelCtx("qwen3.8"), q, eps)
	waitForDepth(t, q, 1)
	assertWaiting(t, cold, "cold request")

	q.ObserveBusy(node, BackendBusy{Valid: true})
	mustReceive(t, cold)
}

func TestQueue_AgedColdRequestDrainsTheDomain(t *testing.T) {
	q, node := multiSlotGPUNode(t, 40*time.Millisecond, "qwen3.6")
	eps := []*domain.Endpoint{node}

	running, err := q.Select(modelCtx("qwen3.6"), eps)
	if err != nil {
		t.Fatal(err)
	}
	cold := selectAsync(modelCtx("qwen3.8"), q, eps)
	waitForDepth(t, q, 1)

	// Before the cap, the loaded model's requests still take free slots.
	early, err := q.Select(modelCtx("qwen3.6"), eps)
	if err != nil {
		t.Fatal(err)
	}
	q.DecrementConnections(early)

	time.Sleep(60 * time.Millisecond) // the cold request is now past the cap
	// Free slots remain, but a new qwen3.6 request queues so the domain can
	// drain; otherwise steady qwen3.6 traffic would starve the cold request.
	late := selectAsync(modelCtx("qwen3.6"), q, eps)
	waitForDepth(t, q, 2)
	assertWaiting(t, late, "request for the loaded model")

	q.DecrementConnections(running)
	c := mustReceive(t, cold)
	if c.err != nil {
		t.Fatalf("aged cold request: %+v", c)
	}
	assertWaiting(t, late, "request for the outgoing model while the cold one loads")
	q.DecrementConnections(c.endpoint)
	mustReceive(t, late)
}
