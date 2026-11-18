package balancer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
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

// Backend kinds discovery can tell apart. They decide how busy slots are
// watched (see BusyTracker).
const (
	backendLlamaCpp  = "llamacpp"  // single-model llama-server
	backendRouter    = "router"    // llama-server router mode
	backendLlamaSwap = "llamaswap" // llama-swap
)

// discoveredSlots is what a probe learnt about one endpoint. models is nil for
// a single-model llama.cpp server, whose slots are simply the endpoint's.
type discoveredSlots struct {
	models map[string]int
	kind   string
	total  int // 0 = nothing usable learnt; the endpoint falls back
}

// busyTarget is one discovered endpoint as the BusyTracker needs it.
type busyTarget struct {
	ep     *domain.Endpoint
	kind   string
	models []string
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
	// lastSeen holds the endpoint each discovered URL was probed through, so
	// the BusyTracker reuses its auth and headers.
	lastSeen map[string]*domain.Endpoint
	stopCh   chan struct{}
	interval time.Duration
	fallback int
	mu       sync.RWMutex
	stopOnce sync.Once
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
		lastSeen:   make(map[string]*domain.Endpoint),
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
// backend reported one.
//
// On a multi-model node (llama-swap, router mode) a model that is not loaded
// gets a limit of 1 until it reports ready: the first request starts the load,
// and the rest wait in Olla's queue instead of piling up behind the load (or
// a swap) inside the backend. Its real slot count is learnt once it is ready.
func (d *SlotDiscoverer) ModelSlots(ep *domain.Endpoint, model string) (int, bool) {
	if model == "" {
		return 0, false
	}
	d.mu.RLock()
	ds := d.discovered[ep.URLString]
	d.mu.RUnlock()
	if n, ok := ds.models[model]; ok {
		return n, true
	}
	if ds.kind == backendLlamaSwap || ds.kind == backendRouter {
		return 1, true
	}
	return 0, false
}

// GroupSlots implements SlotResolver: the group's max_concurrent, else the
// slots of its loaded models, else the fallback. Summing only loaded models is
// what makes a swap group behave: while qwen-a holds the group's one slot, a
// request for qwen-b sees the group full rather than the node's spare slots.
func (d *SlotDiscoverer) GroupSlots(ep *domain.Endpoint, group string) (int, string) {
	if g, ok := slotGroupConfig(ep, group); ok && g.MaxConcurrent > 0 {
		return g.MaxConcurrent, SlotSourceConfig
	}
	d.mu.RLock()
	sum := 0
	for m, n := range d.discovered[ep.URLString].models {
		if slotGroupOf(ep, m) == group {
			sum += n
		}
	}
	d.mu.RUnlock()
	if sum > 0 {
		return sum, SlotSourceDiscovered
	}
	return d.fallback, SlotSourceDefault
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
		if d.refreshOne(ctx, ep) {
			changed = true
		}
	}
	if changed {
		d.notify()
	}
}

// RefreshEndpoint re-probes one endpoint now, e.g. because llama-swap just
// reported a model starting or stopping. The periodic Refresh would catch the
// change too, but up to slot_refresh_interval later.
func (d *SlotDiscoverer) RefreshEndpoint(ctx context.Context, ep *domain.Endpoint) {
	if d.refreshOne(ctx, ep) {
		d.notify()
	}
}

func (d *SlotDiscoverer) refreshOne(ctx context.Context, ep *domain.Endpoint) bool {
	if _, ok := slotProbeTypes[ep.Type]; !ok {
		return false
	}
	found, err := d.probe(ctx, ep)
	if err != nil {
		slog.Debug("slot discovery: probe failed", "endpoint", ep.Name, "error", err)
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastSeen[ep.URLString] = ep
	if sameSlots(d.discovered[ep.URLString], found) {
		return false
	}
	d.discovered[ep.URLString] = found
	slog.Info("Discovered endpoint slots", "endpoint", ep.Name, "slots", found.total, "models", found.models)
	return true
}

func (d *SlotDiscoverer) notify() {
	d.mu.RLock()
	onChange := d.onChange
	d.mu.RUnlock()
	if onChange != nil {
		onChange()
	}
}

func sameSlots(a, b discoveredSlots) bool {
	if a.kind != b.kind || a.total != b.total || len(a.models) != len(b.models) || (a.models == nil) != (b.models == nil) {
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
		found := d.probeModels(ctx, ep, loaded)
		found.kind = backendLlamaSwap
		return found, nil
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
		return discoveredSlots{total: n, kind: backendLlamaCpp}, nil
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
	found := d.probeModels(ctx, ep, loaded)
	found.kind = backendRouter
	return found, nil
}

// busyTargets lists every endpoint discovery has identified, with its kind and
// loaded models. Endpoints are remembered from the last Refresh.
func (d *SlotDiscoverer) busyTargets() []busyTarget {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]busyTarget, 0, len(d.discovered))
	for url, ds := range d.discovered {
		ep, ok := d.lastSeen[url]
		if !ok || ds.kind == "" {
			continue
		}
		t := busyTarget{ep: ep, kind: ds.kind}
		for m := range ds.models {
			t.models = append(t.models, m)
		}
		sort.Strings(t.models)
		out = append(out, t)
	}
	return out
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
	ctx, cancel := context.WithTimeout(ctx, slotProbeTimeout)
	defer cancel()
	req, err := newBackendRequest(ctx, ep, pathAndQuery)
	if err != nil {
		return nil, 0, err
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
