package balancer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/version"
)

const (
	// busyPollTimeout bounds one /slots read. It sits below the default poll
	// interval so a slow backend cannot stack polls up.
	busyPollTimeout = 900 * time.Millisecond
	// busyStreamMaxBackoff caps reconnect delay for a dropped llama-swap feed.
	busyStreamMaxBackoff = 30 * time.Second
	// busyStreamMaxLine fits llama-swap's modelStatus message, which carries
	// every configured model, on one SSE data line.
	busyStreamMaxLine = 4 << 20
	// modelChangeSettle delays the re-discovery a model change triggers. A
	// load sends a burst of modelStatus messages (progress, then ready), so
	// this waits for the burst to settle and probes once.
	modelChangeSettle = 500 * time.Millisecond
)

// BackendBusy is what a backend reports as running right now: every request,
// whoever sent it. Valid=false means the reading is gone (feed dropped, /slots
// disabled) and any earlier one must not be trusted.
type BackendBusy struct {
	Models map[string]int
	Total  int
	Valid  bool
}

// BusyTracker watches how busy discovered backends are, so the queue can tell
// slots taken by traffic that never passed through this Olla - another Olla
// instance, or a client talking to the backend directly - from its own.
//
//   - llama-swap: a live subscription to /api/events. Its "inflight" messages
//     give a snapshot on connect, then an upsert or remove per request, each
//     naming the model. This can never load a model, unlike polling
//     /upstream/<model>/slots, which would start a model that was unloaded
//     a moment earlier (and evict others in an exclusive swap group).
//   - llama.cpp single model: GET /slots, counting is_processing.
//   - llama.cpp router mode: GET /slots?model=<id>&autoload=false per loaded
//     model; autoload=false stops a probe from loading one.
type BusyTracker struct {
	disc     *SlotDiscoverer
	sink     func(ep *domain.Endpoint, b BackendBusy)
	poll     *http.Client
	stream   *http.Client
	watchers map[string]context.CancelFunc
	stopCh   chan struct{}
	interval time.Duration
	mu       sync.Mutex
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewBusyTracker reports readings to sink (normally QueueWrapper.ObserveBusy).
// interval paces the llama.cpp /slots poll and how often the set of watched
// endpoints is reconciled with discovery.
func NewBusyTracker(disc *SlotDiscoverer, sink func(ep *domain.Endpoint, b BackendBusy), interval time.Duration) *BusyTracker {
	return &BusyTracker{
		disc: disc,
		sink: sink,
		poll: &http.Client{Timeout: busyPollTimeout},
		// No client timeout: the feed is a long-lived stream. Reads end when
		// the watcher's context is cancelled.
		stream:   &http.Client{},
		watchers: make(map[string]context.CancelFunc),
		stopCh:   make(chan struct{}),
		interval: interval,
	}
}

// Start begins polling and watching. Stop ends everything it started.
func (t *BusyTracker) Start() {
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()
		for {
			t.tick()
			select {
			case <-ticker.C:
			case <-t.stopCh:
				return
			}
		}
	}()
}

// Stop ends polling and every llama-swap feed. Safe to call more than once.
func (t *BusyTracker) Stop() {
	t.stopOnce.Do(func() {
		close(t.stopCh)
		t.mu.Lock()
		for _, cancel := range t.watchers {
			cancel()
		}
		t.mu.Unlock()
		t.wg.Wait()
	})
}

// tick reconciles feeds with discovery and polls the llama.cpp backends.
func (t *BusyTracker) tick() {
	targets := t.disc.busyTargets()
	swaps := make(map[string]busyTarget)
	for _, tg := range targets {
		switch tg.kind {
		case backendLlamaSwap:
			swaps[tg.ep.URLString] = tg
		case backendLlamaCpp, backendRouter:
			t.pollSlots(tg)
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	select {
	case <-t.stopCh:
		return
	default:
	}
	for url, cancel := range t.watchers {
		if _, ok := swaps[url]; !ok {
			cancel()
			delete(t.watchers, url)
		}
	}
	for url, tg := range swaps {
		if _, ok := t.watchers[url]; ok {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.watchers[url] = cancel
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.watchLlamaSwap(ctx, tg.ep)
		}()
	}
}

func (t *BusyTracker) pollSlots(tg busyTarget) {
	ctx, cancel := context.WithTimeout(context.Background(), busyPollTimeout)
	defer cancel()

	if tg.kind == backendLlamaCpp {
		n, err := t.readSlots(ctx, tg.ep, "/slots")
		if err != nil {
			slog.Debug("busy tracking: /slots read failed", "endpoint", tg.ep.Name, "error", err)
			t.sink(tg.ep, BackendBusy{})
			return
		}
		t.sink(tg.ep, BackendBusy{Valid: true, Total: n})
		return
	}

	b := BackendBusy{Valid: true, Models: make(map[string]int, len(tg.models))}
	for _, m := range tg.models {
		n, err := t.readSlots(ctx, tg.ep, "/slots?autoload=false&model="+url.QueryEscape(m))
		if err != nil {
			slog.Debug("busy tracking: router /slots read failed", "endpoint", tg.ep.Name, "model", m, "error", err)
			t.sink(tg.ep, BackendBusy{})
			return
		}
		b.Models[m] = n
		b.Total += n
	}
	t.sink(tg.ep, b)
}

// readSlots counts slots with is_processing set. A server started with
// --no-slots answers non-200, which reads as "unknown", never as idle.
func (t *BusyTracker) readSlots(ctx context.Context, ep *domain.Endpoint, pathAndQuery string) (int, error) {
	req, err := newBackendRequest(ctx, ep, pathAndQuery)
	if err != nil {
		return 0, err
	}
	resp, err := t.poll.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%s returned %d", pathAndQuery, resp.StatusCode)
	}
	var slots []struct {
		IsProcessing bool `json:"is_processing"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&slots); err != nil {
		return 0, fmt.Errorf("decoding %s: %w", pathAndQuery, err)
	}
	busy := 0
	for _, s := range slots {
		if s.IsProcessing {
			busy++
		}
	}
	return busy, nil
}

// watchLlamaSwap keeps one /api/events subscription open, reconnecting with
// backoff, until ctx is cancelled.
func (t *BusyTracker) watchLlamaSwap(ctx context.Context, ep *domain.Endpoint) {
	backoff := t.interval
	for {
		err := t.streamLlamaSwap(ctx, ep)
		// Whatever was known is stale the moment the feed ends.
		t.sink(ep, BackendBusy{})
		if ctx.Err() != nil {
			return
		}
		slog.Debug("busy tracking: llama-swap feed ended, reconnecting", "endpoint", ep.Name, "error", err, "in", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, busyStreamMaxBackoff)
	}
}

func (t *BusyTracker) streamLlamaSwap(ctx context.Context, ep *domain.Endpoint) error {
	req, err := newBackendRequest(ctx, ep, "/api/events")
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := t.stream.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("/api/events returned %d", resp.StatusCode)
	}

	// A model starting, finishing loading or stopping changes which models
	// hold slots, so re-run discovery for this node rather than wait for the
	// next periodic refresh. Trailing-edge debounce: every message restarts
	// the timer, and the probe runs once the burst has settled.
	var settle *time.Timer
	defer func() {
		if settle != nil {
			settle.Stop()
		}
	}()
	modelChanged := func() {
		if settle != nil {
			settle.Stop()
		}
		settle = time.AfterFunc(modelChangeSettle, func() {
			if ctx.Err() == nil {
				t.disc.RefreshEndpoint(ctx, ep)
			}
		})
	}

	feed := newInflightFeed()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), busyStreamMaxLine)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data:")
		if !ok {
			continue
		}
		payload := []byte(strings.TrimSpace(data))
		if gjson.GetBytes(payload, "type").String() == "modelStatus" {
			// Includes the state sent on connect: after a reconnect it may
			// reflect a swap that happened while the feed was down.
			modelChanged()
			continue
		}
		if b, changed := feed.apply(payload); changed {
			t.sink(ep, b)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return errors.New("stream closed")
}

// inflightFeed folds llama-swap "inflight" messages into per-model counts.
type inflightFeed struct {
	byID   map[string]string // request id -> model
	synced bool              // a snapshot has arrived, so counts are complete
}

func newInflightFeed() *inflightFeed {
	return &inflightFeed{byID: make(map[string]string)}
}

// apply consumes one SSE data payload: {"type":"inflight","data":"<json>"}.
// It reports the new reading when the message changed anything. Upserts and
// removes before the first snapshot are applied but not reported, since the
// counts are only complete once the snapshot has landed.
func (f *inflightFeed) apply(payload []byte) (BackendBusy, bool) {
	if gjson.GetBytes(payload, "type").String() != "inflight" {
		return BackendBusy{}, false
	}
	ev := gjson.Parse(gjson.GetBytes(payload, "data").String())
	switch ev.Get("operation").String() {
	case "snapshot":
		f.byID = make(map[string]string)
		for _, r := range ev.Get("requests").Array() {
			if holdsSlot(r) {
				f.byID[r.Get("id").String()] = r.Get("model").String()
			}
		}
		f.synced = true
	case "upsert":
		r := ev.Get("request")
		id := r.Get("id").String()
		if id == "" || !holdsSlot(r) {
			return BackendBusy{}, false
		}
		model := r.Get("model").String()
		// Upserts also fire as a response streams in; only a new request or
		// a changed model alters the counts.
		if prev, ok := f.byID[id]; ok && prev == model {
			return BackendBusy{}, false
		}
		f.byID[id] = model
	case "remove":
		id := ev.Get("id").String()
		if _, ok := f.byID[id]; !ok {
			return BackendBusy{}, false
		}
		delete(f.byID, id)
	default:
		return BackendBusy{}, false
	}
	if !f.synced {
		return BackendBusy{}, false
	}
	b := BackendBusy{Valid: true, Models: make(map[string]int)}
	for _, model := range f.byID {
		b.Total++
		if model != "" {
			b.Models[model]++
		}
	}
	return b, true
}

// holdsSlot reports whether an in-flight entry is inference. llama-swap also
// tracks model-dispatched GETs such as /props, which is exactly what slot
// discovery (ours, and any other Olla's) calls; counting those would show a
// probe as a busy slot. Every inference route is a POST or a websocket
// upgrade, neither of which is a plain GET.
func holdsSlot(r gjson.Result) bool {
	return r.Get("method").String() != http.MethodGet
}

// newBackendRequest builds a GET to ep with the endpoint's outbound headers and
// auth, the same way slot discovery does.
func newBackendRequest(ctx context.Context, ep *domain.Endpoint, pathAndQuery string) (*http.Request, error) {
	ref, err := url.Parse(pathAndQuery)
	if err != nil {
		return nil, err
	}
	if ep.URL == nil {
		return nil, fmt.Errorf("endpoint %s has no URL", ep.Name)
	}
	target := ep.URL.JoinPath(ref.Path)
	target.RawQuery = ref.RawQuery
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	// Named like the health checker's probes so backend logs show who is
	// polling; set first so an endpoint's own User-Agent header still wins.
	req.Header.Set("User-Agent", fmt.Sprintf("%s-Queue/%s", version.ShortName, version.Version))
	for k, v := range ep.Headers {
		req.Header.Set(k, v)
	}
	if ep.AuthHeaderName != "" {
		req.Header.Set(ep.AuthHeaderName, ep.AuthHeaderValue)
	}
	return req, nil
}
