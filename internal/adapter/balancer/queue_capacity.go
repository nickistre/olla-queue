package balancer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/thushan/olla/internal/core/domain"
)

const (
	slotProbeTimeout = 5 * time.Second
	// slotProbeMaxBody bounds the /props read; the payload carries the chat
	// template and default generation settings, so it is not tiny.
	slotProbeMaxBody = 1 << 20
)

// slotProbeTypes are endpoint types worth asking for slot counts. llama-swap
// usually sits behind an openai-compatible entry, so those are probed too; any
// backend without /running, /props or a router /models just falls back.
var slotProbeTypes = map[string]struct{}{
	"llamacpp":          {},
	"openai-compatible": {},
	"openai":            {},
}

// discoveredSlots is what a probe learnt about one endpoint. models is nil for
// a single-model llama.cpp server, whose slots are simply the endpoint's.
type discoveredSlots struct {
	models map[string]int
	total  int // 0 = nothing usable learnt; the endpoint falls back
}

// SlotDiscoverer resolves endpoint and per-model concurrency limits, learning
// them from the backends where possible:
//
//   - llama-swap: GET /running for the loaded models, then
//     GET /props?model=<id>&autoload=false for each. autoload=false matters:
//     without it llama-swap would start (swap in) a model just to answer us.
//   - llama.cpp router mode (--models-dir/--models-preset): GET /models for
//     models whose status is "loaded", then the same per-model /props call.
//   - llama.cpp single model: GET /props -> total_slots, the --parallel value.
//
// Several models loaded on one node each have their own slots, so the node's
// total says nothing about how many requests one model can take. Multi-model
// discovery therefore records slots per model as well as their sum.
//
// Endpoint limit: max_concurrent, then the discovered total, then fallback.
// Model limit: the discovered per-model count, when there is one.
type SlotDiscoverer struct {
	client     *http.Client
	endpoints  func(ctx context.Context) ([]*domain.Endpoint, error)
	onChange   func()
	discovered map[string]discoveredSlots
	stopCh     chan struct{}
	interval   time.Duration
	fallback   int
	mu         sync.RWMutex
	stopOnce   sync.Once
}

// NewSlotDiscoverer builds a discoverer that refreshes every interval. endpoints
// supplies the endpoints to probe; onChange (optional) runs when any slot count
// changes, so the queue can dispatch into new capacity straight away.
func NewSlotDiscoverer(endpoints func(ctx context.Context) ([]*domain.Endpoint, error), fallback int, interval time.Duration, onChange func()) *SlotDiscoverer {
	if fallback <= 0 {
		fallback = 1
	}
	return &SlotDiscoverer{
		client:     &http.Client{Timeout: slotProbeTimeout},
		endpoints:  endpoints,
		onChange:   onChange,
		fallback:   fallback,
		interval:   interval,
		discovered: make(map[string]discoveredSlots),
		stopCh:     make(chan struct{}),
	}
}

// SetOnChange sets the change callback. The queue and discoverer reference each
// other, so one of them has to be wired after construction.
func (d *SlotDiscoverer) SetOnChange(fn func()) {
	d.mu.Lock()
	d.onChange = fn
	d.mu.Unlock()
}

// Slots implements SlotResolver: the endpoint-wide limit.
func (d *SlotDiscoverer) Slots(ep *domain.Endpoint) (int, string) {
	if ep.MaxConcurrent > 0 {
		return ep.MaxConcurrent, SlotSourceConfig
	}
	d.mu.RLock()
	ds, ok := d.discovered[ep.URLString]
	d.mu.RUnlock()
	if ok && ds.total > 0 {
		return ds.total, SlotSourceDiscovered
	}
	return d.fallback, SlotSourceDefault
}

// ModelSlots implements SlotResolver: the limit for one model on ep, if the
// backend reported one. A model that is not loaded has no limit of its own;
// the endpoint limit still applies, and loading it is the backend's call.
func (d *SlotDiscoverer) ModelSlots(ep *domain.Endpoint, model string) (int, bool) {
	if model == "" {
		return 0, false
	}
	d.mu.RLock()
	n, ok := d.discovered[ep.URLString].models[model]
	d.mu.RUnlock()
	return n, ok
}

// ModelSlotList implements SlotResolver for the stats view.
func (d *SlotDiscoverer) ModelSlotList(ep *domain.Endpoint) map[string]int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	models := d.discovered[ep.URLString].models
	out := make(map[string]int, len(models))
	for m, n := range models {
		out[m] = n
	}
	return out
}

// Start probes once immediately, then on every interval until Stop.
func (d *SlotDiscoverer) Start() {
	go func() {
		d.Refresh(context.Background())
		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.Refresh(context.Background())
			case <-d.stopCh:
				return
			}
		}
	}()
}

// Stop ends the refresh loop. Safe to call more than once.
func (d *SlotDiscoverer) Stop() {
	d.stopOnce.Do(func() { close(d.stopCh) })
}

// Refresh probes every eligible endpoint once. Failed probes keep the last
// known value: a transient error shouldn't collapse a 4-slot backend to 1.
// Endpoints with max_concurrent are still probed, because their per-model
// limits are discovered even when config sets the endpoint-wide one.
func (d *SlotDiscoverer) Refresh(ctx context.Context) {
	eps, err := d.endpoints(ctx)
	if err != nil {
		slog.Debug("slot discovery: listing endpoints failed", "error", err)
		return
	}

	changed := false
	for _, ep := range eps {
		if _, ok := slotProbeTypes[ep.Type]; !ok {
			continue
		}
		found, err := d.probe(ctx, ep)
		if err != nil {
			slog.Debug("slot discovery: probe failed", "endpoint", ep.Name, "error", err)
			continue
		}
		d.mu.Lock()
		if !sameSlots(d.discovered[ep.URLString], found) {
			d.discovered[ep.URLString] = found
			changed = true
			slog.Info("Discovered endpoint slots", "endpoint", ep.Name, "slots", found.total, "models", found.models)
		}
		d.mu.Unlock()
	}

	d.mu.RLock()
	onChange := d.onChange
	d.mu.RUnlock()
	if changed && onChange != nil {
		onChange()
	}
}

func sameSlots(a, b discoveredSlots) bool {
	if a.total != b.total || len(a.models) != len(b.models) || (a.models == nil) != (b.models == nil) {
		return false
	}
	for m, n := range a.models {
		if bn, ok := b.models[m]; !ok || bn != n {
			return false
		}
	}
	return true
}

func (d *SlotDiscoverer) probe(ctx context.Context, ep *domain.Endpoint) (discoveredSlots, error) {
	// llama-swap: /running lists processes with a state.
	if body, status, err := d.get(ctx, ep, "/running"); err == nil && status == http.StatusOK && gjson.GetBytes(body, "running").IsArray() {
		var loaded []string
		for _, m := range gjson.GetBytes(body, "running").Array() {
			if id := m.Get("model").String(); id != "" && m.Get("state").String() == "ready" {
				loaded = append(loaded, id)
			}
		}
		return d.probeModels(ctx, ep, loaded), nil
	}

	// Single-model llama.cpp answers /props directly. Router mode refuses
	// /props without ?model= (400), which sends us to /models below.
	body, status, err := d.get(ctx, ep, "/props")
	if err != nil {
		return discoveredSlots{}, err
	}
	if status == http.StatusOK {
		n, slotsErr := totalSlots(body)
		if slotsErr != nil {
			return discoveredSlots{}, slotsErr
		}
		return discoveredSlots{total: n}, nil
	}

	// llama.cpp router mode: /models carries a per-model status. A plain
	// OpenAI /models listing has no status, so it never counts as a router.
	body, status, err = d.get(ctx, ep, "/models")
	if err != nil {
		return discoveredSlots{}, err
	}
	if status != http.StatusOK || !gjson.GetBytes(body, "data.#.status").IsArray() || len(gjson.GetBytes(body, "data.#.status").Array()) == 0 {
		return discoveredSlots{}, fmt.Errorf("/props returned %d and /models is not a llama.cpp router listing", status)
	}
	var loaded []string
	for _, m := range gjson.GetBytes(body, "data").Array() {
		if id := m.Get("id").String(); id != "" && m.Get("status.value").String() == "loaded" {
			loaded = append(loaded, id)
		}
	}
	return d.probeModels(ctx, ep, loaded), nil
}

// probeModels reads total_slots for each loaded model. A multi-model node with
// nothing loaded is a valid answer (models empty, total 0): the endpoint then
// falls back to the default limit rather than keeping stale per-model limits.
func (d *SlotDiscoverer) probeModels(ctx context.Context, ep *domain.Endpoint, loaded []string) discoveredSlots {
	found := discoveredSlots{models: make(map[string]int, len(loaded))}
	for _, model := range loaded {
		body, status, err := d.get(ctx, ep, "/props?autoload=false&model="+url.QueryEscape(model))
		if err != nil || status != http.StatusOK {
			continue
		}
		if n, err := totalSlots(body); err == nil {
			found.models[model] = n
			found.total += n
		}
	}
	return found
}

func totalSlots(body []byte) (int, error) {
	n := gjson.GetBytes(body, "total_slots").Int()
	if n <= 0 {
		return 0, errors.New("no total_slots in /props response")
	}
	return int(n), nil
}

func (d *SlotDiscoverer) get(ctx context.Context, ep *domain.Endpoint, pathAndQuery string) ([]byte, int, error) {
	ref, err := url.Parse(pathAndQuery)
	if err != nil {
		return nil, 0, err
	}
	if ep.URL == nil {
		return nil, 0, fmt.Errorf("endpoint %s has no URL", ep.Name)
	}
	target := ep.URL.JoinPath(ref.Path)
	target.RawQuery = ref.RawQuery

	ctx, cancel := context.WithTimeout(ctx, slotProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range ep.Headers {
		req.Header.Set(k, v)
	}
	if ep.AuthHeaderName != "" {
		req.Header.Set(ep.AuthHeaderName, ep.AuthHeaderValue)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, slotProbeMaxBody))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}
