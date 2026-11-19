package balancer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thushan/olla/internal/core/domain"
)

func probeEndpoint(name, typ, rawURL string) *domain.Endpoint {
	ep := makeEndpoint(name, rawURL)
	ep.URL, _ = url.Parse(rawURL)
	ep.Type = typ
	return ep
}

func staticEndpoints(eps ...*domain.Endpoint) func(context.Context) ([]*domain.Endpoint, error) {
	return func(context.Context) ([]*domain.Endpoint, error) { return eps, nil }
}

func TestSlotDiscoverer_LlamaCppProps(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			_, _ = w.Write([]byte(`{"total_slots": 4, "model_path": "x.gguf"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("llama", "llamacpp", srv.URL)
	var changes atomic.Int32
	d := NewSlotDiscoverer(staticEndpoints(ep), 1, 0, func() { changes.Add(1) })
	d.Refresh(context.Background())

	if n, src := d.Slots(ep); n != 4 || src != SlotSourceDiscovered {
		t.Fatalf("got %d/%s, want 4/discovered", n, src)
	}
	if changes.Load() != 1 {
		t.Fatalf("onChange calls = %d, want 1", changes.Load())
	}

	// An unchanged value must not re-trigger dispatch.
	d.Refresh(context.Background())
	if changes.Load() != 1 {
		t.Fatalf("onChange fired without a change")
	}
}

func TestSlotDiscoverer_LlamaSwapOnlyProbesReadyModelsWithoutAutoload(t *testing.T) {
	var mu sync.Mutex
	var probed []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/running":
			_, _ = w.Write([]byte(`{"running":[
				{"model":"qwen","state":"ready"},
				{"model":"llama","state":"ready"},
				{"model":"gemma","state":"starting"}]}`))
		case "/props":
			model := r.URL.Query().Get("model")
			mu.Lock()
			probed = append(probed, model)
			mu.Unlock()
			if r.URL.Query().Get("autoload") != "false" {
				t.Errorf("probe for %q would trigger a model load", model)
			}
			slots := map[string]string{"qwen": "2", "llama": "3"}[model]
			_, _ = w.Write([]byte(`{"total_slots": ` + slots + `}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("swap", "openai-compatible", srv.URL)
	d := NewSlotDiscoverer(staticEndpoints(ep), 1, 0, nil)
	d.Refresh(context.Background())

	if n, _ := d.Slots(ep); n != 5 {
		t.Fatalf("endpoint slots = %d, want 5 (2+3 from ready models)", n)
	}
	// Each model keeps its own count: qwen must not inherit the node's 5.
	if n, ok := d.ModelSlots(ep, "qwen"); !ok || n != 2 {
		t.Fatalf("qwen slots = %d/%v, want 2", n, ok)
	}
	if n, ok := d.ModelSlots(ep, "llama"); !ok || n != 3 {
		t.Fatalf("llama slots = %d/%v, want 3", n, ok)
	}
	// Still starting: no slot count of its own yet, so the cold-model gate
	// lets one request through while it loads.
	if n, ok := d.ModelSlots(ep, "gemma"); !ok || n != 1 {
		t.Fatalf("starting model limit = %d/%v, want the cold gate of 1", n, ok)
	}
	for _, m := range probed {
		if m == "gemma" {
			t.Fatal("a model that is not ready must never be probed")
		}
	}
}

func TestSlotDiscoverer_FallbacksAndOverrides(t *testing.T) {
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() || r.URL.Path != "/props" {
			http.Error(w, "nope", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"total_slots": 3}`))
	}))
	defer srv.Close()

	probed := probeEndpoint("llama", "llamacpp", srv.URL)
	pinned := probeEndpoint("pinned", "llamacpp", srv.URL+"/pinned")
	pinned.MaxConcurrent = 8
	ollama := probeEndpoint("ollama", "ollama", srv.URL+"/ollama")

	d := NewSlotDiscoverer(staticEndpoints(probed, pinned, ollama), 2, 0, nil)
	d.Refresh(context.Background())

	if n, src := d.Slots(pinned); n != 8 || src != SlotSourceConfig {
		t.Fatalf("config override: %d/%s", n, src)
	}
	if n, src := d.Slots(ollama); n != 2 || src != SlotSourceDefault {
		t.Fatalf("non-probed type: %d/%s", n, src)
	}

	// A failing probe keeps the last known value rather than dropping to default.
	fail.Store(true)
	d.Refresh(context.Background())
	if n, src := d.Slots(probed); n != 3 || src != SlotSourceDiscovered {
		t.Fatalf("after failed probe: %d/%s", n, src)
	}
}

func TestSlotDiscoverer_LlamaCppRouterModePerModel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			model := r.URL.Query().Get("model")
			if model == "" {
				http.Error(w, `{"error":"model name is missing from the request"}`, http.StatusBadRequest)
				return
			}
			if r.URL.Query().Get("autoload") != "false" {
				t.Errorf("probe for %q would load the model", model)
			}
			_, _ = w.Write([]byte(`{"total_slots": ` + map[string]string{"gemma": "4", "qwen": "1"}[model] + `}`))
		case "/models":
			_, _ = w.Write([]byte(`{"data":[
				{"id":"gemma","status":{"value":"loaded"}},
				{"id":"qwen","status":{"value":"loaded"}},
				{"id":"phi","status":{"value":"unloaded"}}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("router", "llamacpp", srv.URL)
	d := NewSlotDiscoverer(staticEndpoints(ep), 1, 0, nil)
	d.Refresh(context.Background())

	if n, src := d.Slots(ep); n != 5 || src != SlotSourceDiscovered {
		t.Fatalf("endpoint slots = %d/%s, want 5 discovered", n, src)
	}
	if n, _ := d.ModelSlots(ep, "qwen"); n != 1 {
		t.Fatalf("qwen slots = %d, want 1", n)
	}
	if got := d.ModelSlotList(ep); len(got) != 2 || got["gemma"] != 4 {
		t.Fatalf("model list = %v", got)
	}
}

func TestSlotDiscoverer_NothingLoadedClearsStaleModelLimits(t *testing.T) {
	var running atomic.Value
	running.Store(`{"running":[{"model":"qwen","state":"ready"}]}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/running":
			_, _ = w.Write([]byte(running.Load().(string)))
		case "/props":
			_, _ = w.Write([]byte(`{"total_slots": 2}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("swap", "openai-compatible", srv.URL)
	var changes atomic.Int32
	d := NewSlotDiscoverer(staticEndpoints(ep), 1, 0, func() { changes.Add(1) })
	d.Refresh(context.Background())
	if n, _ := d.ModelSlots(ep, "qwen"); n != 2 {
		t.Fatalf("qwen slots = %d, want 2", n)
	}

	// llama-swap unloaded everything (TTL expiry). The node must fall back to
	// the default limit, not 0, and qwen's old limit must not linger.
	running.Store(`{"running":[]}`)
	d.Refresh(context.Background())
	if n, src := d.Slots(ep); n != 1 || src != SlotSourceDefault {
		t.Fatalf("endpoint slots = %d/%s, want default 1", n, src)
	}
	// qwen's old limit of 2 must not linger; unloaded, it is cold-gated to 1.
	if n, _ := d.ModelSlots(ep, "qwen"); n != 1 {
		t.Fatalf("unloaded qwen limit = %d, want the cold gate of 1 (stale 2 survived?)", n)
	}
	if changes.Load() != 2 {
		t.Fatalf("onChange calls = %d, want 2", changes.Load())
	}
}

func TestSlotDiscoverer_ConfigLimitStillGetsPerModelDiscovery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/running":
			_, _ = w.Write([]byte(`{"running":[{"model":"qwen","state":"ready"}]}`))
		case "/props":
			_, _ = w.Write([]byte(`{"total_slots": 2}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("swap", "openai-compatible", srv.URL)
	ep.MaxConcurrent = 3
	d := NewSlotDiscoverer(staticEndpoints(ep), 1, 0, nil)
	d.Refresh(context.Background())

	if n, src := d.Slots(ep); n != 3 || src != SlotSourceConfig {
		t.Fatalf("endpoint slots = %d/%s, want config 3", n, src)
	}
	if n, ok := d.ModelSlots(ep, "qwen"); !ok || n != 2 {
		t.Fatalf("qwen slots = %d/%v, want discovered 2", n, ok)
	}
}

func TestSlotDiscoverer_PlainModelsListIsNotARouter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/models":
			// An OpenAI-style listing with no per-model status.
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-oss"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("vllm-ish", "openai-compatible", srv.URL)
	d := NewSlotDiscoverer(staticEndpoints(ep), 2, 0, nil)
	d.Refresh(context.Background())
	if n, src := d.Slots(ep); n != 2 || src != SlotSourceDefault {
		t.Fatalf("got %d/%s, want default 2", n, src)
	}
}

func TestSlotDiscoverer_GroupSlotsFromLoadedMembers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/running":
			_, _ = w.Write([]byte(`{"running":[{"model":"qwen3.6","state":"ready"},{"model":"nomic","state":"ready"}]}`))
		case "/props":
			_, _ = w.Write([]byte(`{"total_slots": ` + map[string]string{"qwen3.6": "1", "nomic": "4"}[r.URL.Query().Get("model")] + `}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ep := probeEndpoint("taki", "openai-compatible", srv.URL)
	ep.SlotGroups = []domain.SlotGroup{
		{Name: "embed", Models: []string{"nomic*"}},
		{Name: "gpu", Models: []string{"qwen*"}},
		{Name: "pinned", Models: []string{"gemma*"}, MaxConcurrent: 2},
	}
	d := NewSlotDiscoverer(staticEndpoints(ep), 1, 0, nil)
	d.Refresh(context.Background())

	for group, want := range map[string]struct {
		source string
		slots  int
	}{
		"gpu":    {SlotSourceDiscovered, 1},
		"embed":  {SlotSourceDiscovered, 4},
		"pinned": {SlotSourceConfig, 2},
		"empty":  {SlotSourceDefault, 1},
	} {
		if n, src := d.GroupSlots(ep, group); n != want.slots || src != want.source {
			t.Errorf("group %s = %d/%s, want %d/%s", group, n, src, want.slots, want.source)
		}
	}
}

// countingProps serves llama.cpp /props and counts the probes per endpoint.
func countingProps(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/props" {
			hits.Add(1)
			_, _ = w.Write([]byte(`{"total_slots": 2}`))
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestSlotDiscoverer_PerEndpointRefreshInterval(t *testing.T) {
	fastSrv, fastHits := countingProps(t)
	slowSrv, slowHits := countingProps(t)
	fast := probeEndpoint("fast", "llamacpp", fastSrv.URL)
	slow := probeEndpoint("slow", "llamacpp", slowSrv.URL)
	slow.SlotRefreshInterval = 5 * time.Minute
	d := NewSlotDiscoverer(staticEndpoints(fast, slow), 1, 30*time.Second, nil)
	ctx := context.Background()

	t0 := time.Now()
	if next := d.refreshDue(ctx, t0); next != 30*time.Second {
		t.Fatalf("first pass: next = %s, want the queue-wide 30s", next)
	}
	// 31s on: fast is due again, slow is not.
	next := d.refreshDue(ctx, t0.Add(31*time.Second))
	if fastHits.Load() != 2 || slowHits.Load() != 1 {
		t.Fatalf("probes at 31s: fast=%d slow=%d", fastHits.Load(), slowHits.Load())
	}
	if next > 30*time.Second {
		t.Fatalf("next = %s, want at most 30s", next)
	}
	d.refreshDue(ctx, t0.Add(5*time.Minute+time.Second))
	if slowHits.Load() != 2 {
		t.Fatalf("slow endpoint not probed after its own interval: %d", slowHits.Load())
	}
}

func TestSlotDiscoverer_ShorterEndpointIntervalWakesSooner(t *testing.T) {
	srv, _ := countingProps(t)
	ep := probeEndpoint("quick", "llamacpp", srv.URL)
	ep.SlotRefreshInterval = 5 * time.Second
	d := NewSlotDiscoverer(staticEndpoints(ep), 1, 30*time.Second, nil)
	if next := d.refreshDue(context.Background(), time.Now()); next != 5*time.Second {
		t.Fatalf("next = %s, want the endpoint's 5s", next)
	}
}

func TestSlotDiscoverer_DisabledEndpointIsNeverProbed(t *testing.T) {
	srv, hits := countingProps(t)
	ep := probeEndpoint("remote", "openai-compatible", srv.URL)
	ep.NoSlotDiscovery = true
	d := NewSlotDiscoverer(staticEndpoints(ep), 3, 30*time.Second, nil)
	ctx := context.Background()

	d.Refresh(ctx)
	d.refreshDue(ctx, time.Now())
	d.RefreshEndpoint(ctx, ep)
	if hits.Load() != 0 {
		t.Fatalf("disabled endpoint probed %d times", hits.Load())
	}
	if n, src := d.Slots(ep); n != 3 || src != SlotSourceDefault {
		t.Fatalf("got %d/%s, want the default 3", n, src)
	}
}
