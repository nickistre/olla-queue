package balancer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

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
	if _, ok := d.ModelSlots(ep, "gemma"); ok {
		t.Fatal("a model that is still starting has no slot count yet")
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
	if _, ok := d.ModelSlots(ep, "qwen"); ok {
		t.Fatal("stale per-model limit survived the model being unloaded")
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
