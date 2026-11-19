package balancer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/version"
)

// inflightMsg builds one llama-swap SSE payload: the inflight event JSON is
// itself a string inside the envelope, as the real server sends it.
func inflightMsg(t *testing.T, ev map[string]any) []byte {
	t.Helper()
	inner, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(map[string]string{"type": "inflight", "data": string(inner)})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestInflightFeed(t *testing.T) {
	f := newInflightFeed()

	// Before the snapshot the counts are incomplete, so nothing is reported.
	if _, changed := f.apply(inflightMsg(t, map[string]any{"operation": "upsert", "request": map[string]any{"id": "0", "model": "qwen"}})); changed {
		t.Fatal("reported before the first snapshot")
	}

	b, changed := f.apply(inflightMsg(t, map[string]any{"operation": "snapshot", "requests": []map[string]any{
		{"id": "1", "model": "qwen", "method": "POST"},
		{"id": "2", "model": "nomic", "method": "POST"},
		// A slot-discovery probe: model-dispatched, but not inference.
		{"id": "9", "model": "qwen", "method": "GET", "req_path": "/props"},
	}}))
	if !changed || !b.Valid || b.Total != 2 || b.Models["qwen"] != 1 || b.Models["nomic"] != 1 {
		t.Fatalf("after snapshot: %+v changed=%v", b, changed)
	}

	if _, changed := f.apply(inflightMsg(t, map[string]any{"operation": "upsert", "request": map[string]any{"id": "8", "model": "qwen", "method": "GET", "req_path": "/props"}})); changed {
		t.Fatal("a GET probe counted as a busy slot")
	}

	b, changed = f.apply(inflightMsg(t, map[string]any{"operation": "upsert", "request": map[string]any{"id": "3", "model": "qwen", "method": "POST"}}))
	if !changed || b.Total != 3 || b.Models["qwen"] != 2 {
		t.Fatalf("after new request: %+v", b)
	}

	// Upserts repeat while a response streams; they must not count twice.
	if _, changed := f.apply(inflightMsg(t, map[string]any{"operation": "upsert", "request": map[string]any{"id": "3", "model": "qwen", "method": "POST", "resp_bytes": 900}})); changed {
		t.Fatal("a progress upsert changed the counts")
	}

	b, changed = f.apply(inflightMsg(t, map[string]any{"operation": "remove", "id": "1"}))
	if !changed || b.Total != 2 || b.Models["qwen"] != 1 {
		t.Fatalf("after remove: %+v", b)
	}
	if _, changed := f.apply(inflightMsg(t, map[string]any{"operation": "remove", "id": "nope"})); changed {
		t.Fatal("removing an unknown id changed the counts")
	}

	// Other event types on the same stream are ignored.
	other, _ := json.Marshal(map[string]string{"type": "modelStatus", "data": "[]"})
	if _, changed := f.apply(other); changed {
		t.Fatal("a non-inflight event changed the counts")
	}
}

func TestQueue_ExternalLoadTakesSlots(t *testing.T) {
	res := modelSlots{endpoint: 5, models: map[string]int{"qwen": 1, "nomic": 4}}
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), res)
	t.Cleanup(q.Stop)
	node := makeEndpoint("taki", "http://taki:8080")
	eps := []*domain.Endpoint{node}

	// Another client holds qwen's only slot.
	q.ObserveBusy(node, BackendBusy{Valid: true, Total: 1, Models: map[string]int{"qwen": 1}})

	ch := selectAsync(modelCtx("qwen"), q, eps)
	waitForDepth(t, q, 1)

	// Embeddings are unaffected: nomic's slots are all free.
	if _, err := q.Select(modelCtx("nomic"), eps); err != nil {
		t.Fatalf("nomic should be admitted: %v", err)
	}
	s := q.Stats()
	if s.Endpoints[0].External != 1 {
		t.Fatalf("external = %d, want 1", s.Endpoints[0].External)
	}

	// The outside request finishes; the next reading frees qwen at once.
	q.ObserveBusy(node, BackendBusy{Valid: true, Total: 1, Models: map[string]int{"nomic": 1}})
	if r := mustReceive(t, ch); r.err != nil {
		t.Fatalf("queued qwen request: %+v", r)
	}
}

func TestQueue_ObserveBusySubtractsOwnRequests(t *testing.T) {
	res := modelSlots{endpoint: 4, models: map[string]int{"qwen": 2}}
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), res)
	t.Cleanup(q.Stop)
	node := makeEndpoint("node", "http://node:1")
	eps := []*domain.Endpoint{node}

	if _, err := q.Select(modelCtx("qwen"), eps); err != nil {
		t.Fatal(err)
	}
	// The backend sees our request and nobody else's: no outside traffic.
	q.ObserveBusy(node, BackendBusy{Valid: true, Total: 1, Models: map[string]int{"qwen": 1}})
	if _, err := q.Select(modelCtx("qwen"), eps); err != nil {
		t.Fatalf("our own request must not count as outside traffic: %v", err)
	}
	if s := q.Stats(); s.Endpoints[0].External != 0 {
		t.Fatalf("external = %d, want 0", s.Endpoints[0].External)
	}
}

func TestQueue_InvalidReadingFallsBackToOwnCounts(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	node := makeEndpoint("node", "http://node:1")
	node.MaxConcurrent = 1
	eps := []*domain.Endpoint{node}

	q.ObserveBusy(node, BackendBusy{Valid: true, Total: 1})
	ch := selectAsync(context.Background(), q, eps)
	waitForDepth(t, q, 1)

	// The feed dropped: stale outside counts must not keep blocking.
	q.ObserveBusy(node, BackendBusy{})
	if r := mustReceive(t, ch); r.err != nil {
		t.Fatalf("request should be admitted once the reading is gone: %+v", r)
	}
}

// sink records BusyTracker readings for assertions.
type sink struct {
	got map[string]BackendBusy
	mu  sync.Mutex
}

func (s *sink) observe(ep *domain.Endpoint, b BackendBusy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.got == nil {
		s.got = map[string]BackendBusy{}
	}
	s.got[ep.URLString] = b
}

func (s *sink) waitFor(t *testing.T, url string, ok func(BackendBusy) bool) BackendBusy {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		b, seen := s.got[url]
		s.mu.Unlock()
		if seen && ok(b) {
			return b
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Fatalf("no matching reading for %s; last: %+v", url, s.got[url])
	return BackendBusy{}
}

func TestBusyTracker_LlamaCppSlots(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			_, _ = w.Write([]byte(`{"total_slots": 3}`))
		case "/slots":
			_, _ = w.Write([]byte(`[{"id":0,"is_processing":true},{"id":1,"is_processing":false},{"id":2,"is_processing":true}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("llama", "llamacpp", srv.URL)
	disc := NewSlotDiscoverer(staticEndpoints(ep), 1, time.Hour, nil)
	disc.Refresh(context.Background())

	var s sink
	bt := NewBusyTracker(disc, s.observe, 20*time.Millisecond)
	bt.Start()
	defer bt.Stop()

	s.waitFor(t, ep.URLString, func(b BackendBusy) bool { return b.Valid && b.Total == 2 })
}

func TestBusyTracker_SlotsDisabledReadsAsUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			_, _ = w.Write([]byte(`{"total_slots": 2}`))
		default:
			// llama-server --no-slots
			http.Error(w, "This server does not support slots endpoint.", http.StatusNotImplemented)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("llama", "llamacpp", srv.URL)
	disc := NewSlotDiscoverer(staticEndpoints(ep), 1, time.Hour, nil)
	disc.Refresh(context.Background())

	var s sink
	bt := NewBusyTracker(disc, s.observe, 20*time.Millisecond)
	bt.Start()
	defer bt.Stop()

	s.waitFor(t, ep.URLString, func(b BackendBusy) bool { return !b.Valid })
}

func TestBusyTracker_RouterModePerModelWithoutAutoload(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		model := r.URL.Query().Get("model")
		switch r.URL.Path {
		case "/props":
			if model == "" {
				http.Error(w, "model name is missing", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"total_slots": 2}`))
		case "/models":
			_, _ = w.Write([]byte(`{"data":[{"id":"gemma","status":{"value":"loaded"}},{"id":"qwen","status":{"value":"loaded"}}]}`))
		case "/slots":
			if r.URL.Query().Get("autoload") != "false" {
				t.Errorf("/slots for %q would load the model", model)
			}
			busy := map[string]string{"gemma": "true", "qwen": "false"}[model]
			fmt.Fprintf(w, `[{"id":0,"is_processing":%s},{"id":1,"is_processing":false}]`, busy)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("router", "llamacpp", srv.URL)
	disc := NewSlotDiscoverer(staticEndpoints(ep), 1, time.Hour, nil)
	disc.Refresh(context.Background())

	var s sink
	bt := NewBusyTracker(disc, s.observe, 20*time.Millisecond)
	bt.Start()
	defer bt.Stop()

	s.waitFor(t, ep.URLString, func(b BackendBusy) bool {
		return b.Valid && b.Total == 1 && b.Models["gemma"] == 1 && b.Models["qwen"] == 0
	})
}

func TestBusyTracker_LlamaSwapFeed(t *testing.T) {
	events := make(chan []byte, 8)
	var streams sync.WaitGroup
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/running":
			_, _ = w.Write([]byte(`{"running":[{"model":"qwen","state":"ready"},{"model":"nomic","state":"ready"}]}`))
		case "/props":
			_, _ = w.Write([]byte(`{"total_slots": 1}`))
		case "/slots":
			t.Error("llama-swap must never be polled through /slots")
		case "/api/events":
			streams.Add(1)
			defer streams.Done()
			w.Header().Set("Content-Type", "text/event-stream")
			flusher := w.(http.Flusher)
			// The real server opens with other event types before inflight.
			fmt.Fprintf(w, "event:message\ndata:%s\n\n", `{"type":"modelStatus","data":"[]"}`)
			flusher.Flush()
			for {
				select {
				case msg, ok := <-events:
					if !ok {
						return
					}
					fmt.Fprintf(w, "event:message\ndata:%s\n\n", msg)
					flusher.Flush()
				case <-r.Context().Done():
					return
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("taki", "openai-compatible", srv.URL)
	disc := NewSlotDiscoverer(staticEndpoints(ep), 1, time.Hour, nil)
	disc.Refresh(context.Background())

	var s sink
	bt := NewBusyTracker(disc, s.observe, 20*time.Millisecond)
	bt.Start()

	events <- inflightMsg(t, map[string]any{"operation": "snapshot", "requests": []map[string]any{{"id": "1", "model": "qwen", "method": "POST"}}})
	s.waitFor(t, ep.URLString, func(b BackendBusy) bool { return b.Valid && b.Models["qwen"] == 1 })

	events <- inflightMsg(t, map[string]any{"operation": "upsert", "request": map[string]any{"id": "2", "model": "nomic", "method": "POST"}})
	s.waitFor(t, ep.URLString, func(b BackendBusy) bool { return b.Valid && b.Total == 2 && b.Models["nomic"] == 1 })

	events <- inflightMsg(t, map[string]any{"operation": "remove", "id": "1"})
	s.waitFor(t, ep.URLString, func(b BackendBusy) bool { return b.Valid && b.Total == 1 && b.Models["qwen"] == 0 })

	// The feed ending leaves nothing trusted behind.
	close(events)
	s.waitFor(t, ep.URLString, func(b BackendBusy) bool { return !b.Valid })

	bt.Stop()
	streams.Wait()
}

func TestQueue_ObserveBusyListsEndpointBeforeAnyRouting(t *testing.T) {
	q := makeQueue(t, defaultQueueConfig())
	node := makeEndpoint("taki", "http://taki:8080")
	q.ObserveBusy(node, BackendBusy{Valid: true, Total: 2})
	s := q.Stats()
	if len(s.Endpoints) != 1 || s.Endpoints[0].Name != "taki" || s.Endpoints[0].External != 2 {
		t.Fatalf("endpoints = %+v", s.Endpoints)
	}
}

func TestBusyTracker_ModelStatusTriggersRediscovery(t *testing.T) {
	var mu sync.Mutex
	running := `{"running":[{"model":"qwen-a","state":"ready"}]}`
	events := make(chan string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/running":
			mu.Lock()
			body := running
			mu.Unlock()
			_, _ = w.Write([]byte(body))
		case "/props":
			_, _ = w.Write([]byte(`{"total_slots": 1}`))
		case "/api/events":
			w.Header().Set("Content-Type", "text/event-stream")
			flusher := w.(http.Flusher)
			for {
				select {
				case msg := <-events:
					fmt.Fprintf(w, "event:message\ndata:%s\n\n", msg)
					flusher.Flush()
				case <-r.Context().Done():
					return
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("taki", "openai-compatible", srv.URL)
	disc := NewSlotDiscoverer(staticEndpoints(ep), 1, time.Hour, nil)
	disc.Refresh(context.Background())
	if _, ok := disc.ModelSlotList(ep)["qwen-a"]; !ok {
		t.Fatal("setup: qwen-a should be discovered")
	}

	var s sink
	bt := NewBusyTracker(disc, s.observe, 20*time.Millisecond)
	bt.Start()
	defer bt.Stop()

	// llama-swap swaps qwen-a out for qwen-b. The hourly refresh would miss
	// it; the modelStatus burst must trigger a re-probe straight away.
	mu.Lock()
	running = `{"running":[{"model":"qwen-b","state":"ready"}]}`
	mu.Unlock()
	for range 3 {
		events <- `{"type":"modelStatus","data":"[]"}`
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		list := disc.ModelSlotList(ep)
		if _, b := list["qwen-b"]; b {
			if _, a := list["qwen-a"]; !a {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("swap not picked up: %v", disc.ModelSlotList(ep))
}

func TestQueue_SlotGroupKeepsSwapModelsInOnePool(t *testing.T) {
	// taki: GPU models share one swap slot; embeddings have their own pool.
	res := modelSlots{endpoint: 5, models: map[string]int{"qwen3.6": 1, "nomic": 4, "qwen3.8": 1}, groups: map[string]int{"gpu": 1, "embed": 4}}
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), res)
	t.Cleanup(q.Stop)
	node := makeEndpoint("taki", "http://taki:8080")
	node.SlotGroups = []domain.SlotGroup{
		{Name: "embed", Models: []string{"nomic*"}},
		{Name: "gpu", Models: []string{"*"}},
	}
	eps := []*domain.Endpoint{node}

	running, err := q.Select(modelCtx("qwen3.6"), eps)
	if err != nil {
		t.Fatal(err)
	}
	// A request for the other GPU model would swap qwen3.6 out mid-request;
	// it must wait for the GPU group even though the node has 4 spare slots.
	swap := selectAsync(modelCtx("qwen3.8"), q, eps)
	waitForDepth(t, q, 1)
	// Embeddings live in their own group and go straight through.
	if _, err := q.Select(modelCtx("nomic"), eps); err != nil {
		t.Fatalf("embedding request should be admitted: %v", err)
	}

	got := map[string]QueueGroupStats{}
	for _, g := range q.Stats().Endpoints[0].Groups {
		got[g.Name] = g
	}
	if got["gpu"].InUse != 1 || got["gpu"].Slots != 1 || got["embed"].InUse != 1 {
		t.Fatalf("group stats: %+v", got)
	}

	q.DecrementConnections(running)
	if r := mustReceive(t, swap); r.err != nil {
		t.Fatalf("qwen3.8 should run once the GPU group frees: %+v", r)
	}
}

func TestQueue_SlotGroupCountsOutsideTraffic(t *testing.T) {
	res := modelSlots{endpoint: 5, models: map[string]int{"qwen3.6": 1, "qwen3.8": 1}, groups: map[string]int{"gpu": 1}}
	q := NewQueueWrapper(NewRoundRobinSelector(NewTestStatsCollector()), defaultQueueConfig(), res)
	t.Cleanup(q.Stop)
	node := makeEndpoint("taki", "http://taki:8080")
	node.SlotGroups = []domain.SlotGroup{{Name: "gpu", Models: []string{"qwen*"}}}

	// The cluster's Olla is running qwen3.6; a qwen3.8 request here would
	// force a swap under it.
	q.ObserveBusy(node, BackendBusy{Valid: true, Total: 1, Models: map[string]int{"qwen3.6": 1}})
	ch := selectAsync(modelCtx("qwen3.8"), q, []*domain.Endpoint{node})
	waitForDepth(t, q, 1)

	q.ObserveBusy(node, BackendBusy{Valid: true, Total: 0, Models: map[string]int{}})
	if r := mustReceive(t, ch); r.err != nil {
		t.Fatalf("qwen3.8 should run once outside traffic clears: %+v", r)
	}
}

func TestNewBackendRequest_UserAgent(t *testing.T) {
	ep := makeEndpoint("taki", "http://taki:8080")
	req, err := newBackendRequest(context.Background(), ep, "/props?model=m")
	if err != nil {
		t.Fatal(err)
	}
	if ua := req.Header.Get("User-Agent"); !strings.HasPrefix(ua, version.ShortName+"-Queue/") {
		t.Fatalf("user agent = %q", ua)
	}

	ep.Headers = map[string]string{"User-Agent": "custom"}
	req, err = newBackendRequest(context.Background(), ep, "/running")
	if err != nil {
		t.Fatal(err)
	}
	if ua := req.Header.Get("User-Agent"); ua != "custom" {
		t.Fatalf("endpoint header should win, got %q", ua)
	}
}
