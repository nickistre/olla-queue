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
		t.Fatalf("slots = %d, want 5 (2+3 from ready models)", n)
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
