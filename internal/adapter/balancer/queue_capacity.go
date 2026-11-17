package balancer

import (
	"context"
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
// backend without /running or /props just falls back to the default.
var slotProbeTypes = map[string]struct{}{
	"llamacpp":          {},
	"openai-compatible": {},
	"openai":            {},
}

// SlotDiscoverer resolves endpoint concurrency limits, learning them from the
// backends where possible:
//
//   - llama-swap: GET /running for the loaded models, then
//     GET /props?model=<id>&autoload=false for each. autoload=false matters:
//     without it llama-swap would start (swap in) a model just to answer us.
//   - llama.cpp: GET /props -> total_slots, which is the --parallel value.
//
// Resolution order is endpoint max_concurrent, then discovered, then fallback.
type SlotDiscoverer struct {
	client     *http.Client
	endpoints  func(ctx context.Context) ([]*domain.Endpoint, error)
	onChange   func()
	discovered map[string]int
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
		discovered: make(map[string]int),
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

// Slots implements SlotResolver.
func (d *SlotDiscoverer) Slots(ep *domain.Endpoint) (int, string) {
	if ep.MaxConcurrent > 0 {
		return ep.MaxConcurrent, SlotSourceConfig
	}
	d.mu.RLock()
	n, ok := d.discovered[ep.URLString]
	d.mu.RUnlock()
	if ok {
		return n, SlotSourceDiscovered
	}
	return d.fallback, SlotSourceDefault
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
func (d *SlotDiscoverer) Refresh(ctx context.Context) {
	eps, err := d.endpoints(ctx)
	if err != nil {
		slog.Debug("slot discovery: listing endpoints failed", "error", err)
		return
	}

	changed := false
	for _, ep := range eps {
		if ep.MaxConcurrent > 0 {
			continue
		}
		if _, ok := slotProbeTypes[ep.Type]; !ok {
			continue
		}
		slots, err := d.probe(ctx, ep)
		if err != nil {
			slog.Debug("slot discovery: probe failed", "endpoint", ep.Name, "error", err)
			continue
		}
		d.mu.Lock()
		if d.discovered[ep.URLString] != slots {
			d.discovered[ep.URLString] = slots
			changed = true
			slog.Info("Discovered endpoint slots", "endpoint", ep.Name, "slots", slots)
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

func (d *SlotDiscoverer) probe(ctx context.Context, ep *domain.Endpoint) (int, error) {
	if body, status, err := d.get(ctx, ep, "/running"); err == nil && status == http.StatusOK && gjson.GetBytes(body, "running").IsArray() {
		return d.probeLlamaSwap(ctx, ep, body)
	}

	body, status, err := d.get(ctx, ep, "/props")
	if err != nil {
		return 0, err
	}
	if status != http.StatusOK {
		return 0, fmt.Errorf("/props returned %d", status)
	}
	return totalSlots(body)
}

// probeLlamaSwap sums total_slots across ready models. Capacity is per endpoint
// in this version, so concurrently loaded models share one pool.
func (d *SlotDiscoverer) probeLlamaSwap(ctx context.Context, ep *domain.Endpoint, running []byte) (int, error) {
	sum := 0
	for _, m := range gjson.GetBytes(running, "running").Array() {
		model := m.Get("model").String()
		if model == "" || m.Get("state").String() != "ready" {
			continue
		}
		body, status, err := d.get(ctx, ep, "/props?autoload=false&model="+url.QueryEscape(model))
		if err != nil || status != http.StatusOK {
			continue
		}
		if n, err := totalSlots(body); err == nil {
			sum += n
		}
	}
	if sum == 0 {
		return 0, fmt.Errorf("llama-swap reported no ready models with slots")
	}
	return sum, nil
}

func totalSlots(body []byte) (int, error) {
	n := gjson.GetBytes(body, "total_slots").Int()
	if n <= 0 {
		return 0, fmt.Errorf("no total_slots in /props response")
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
