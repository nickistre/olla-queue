package balancer

import (
	"container/list"
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

var (
	// ErrQueueFull is returned when the wait queue is at max_size.
	ErrQueueFull = errors.New("request queue full")
	// ErrQueueTimeout is returned when a request waits longer than queue.timeout.
	ErrQueueTimeout = errors.New("timed out waiting in request queue")
	// ErrQueueClosed is returned to waiters still queued at shutdown.
	ErrQueueClosed = errors.New("request queue closed")
	// ErrQueueEndpointsGone is returned when every candidate of a queued request
	// went non-routable while it waited, so no slot can ever open for it.
	ErrQueueEndpointsGone = errors.New("no routable endpoints left for queued request")
)

// Slot sources reported in stats so operators can see where a limit came from.
const (
	SlotSourceConfig     = "config"
	SlotSourceDiscovered = "discovered"
	SlotSourceDefault    = "default"
)

// SlotResolver reports concurrency limits. Slots is the endpoint-wide limit and
// where it came from; ModelSlots is one model's own limit on that endpoint,
// when the backend reports one (several models loaded on a node each have
// their own slots, so the node total cannot stand in for any one of them).
type SlotResolver interface {
	Slots(ep *domain.Endpoint) (slots int, source string)
	ModelSlots(ep *domain.Endpoint, model string) (slots int, known bool)
	ModelSlotList(ep *domain.Endpoint) map[string]int
}

// staticSlots resolves from endpoint config only, falling back to a fixed default.
type staticSlots struct{ fallback int }

func (s staticSlots) Slots(ep *domain.Endpoint) (int, string) {
	if ep.MaxConcurrent > 0 {
		return ep.MaxConcurrent, SlotSourceConfig
	}
	return s.fallback, SlotSourceDefault
}

func (staticSlots) ModelSlots(*domain.Endpoint, string) (int, bool) { return 0, false }
func (staticSlots) ModelSlotList(*domain.Endpoint) map[string]int   { return nil }

// reservation records which endpoint and model a Select claimed, so the
// matching DecrementConnections releases the right per-model slot.
type reservation struct {
	url   string
	model string
}

// modelKey joins endpoint and model for the per-model counters. NUL cannot
// appear in a URL or a model name, so keys cannot collide.
func modelKey(url, model string) string { return url + "\x00" + model }

type queueResult struct {
	endpoint *domain.Endpoint
	err      error
}

type queueWaiter struct {
	ctx        context.Context
	enqueuedAt time.Time
	caller     domain.QueueCaller // copied at enqueue for the stats listing
	ready      chan queueResult   // buffered 1, written once under q.mu
	elem       *list.Element      // nil once removed from the queue
	stickyKey  string
	candidates []*domain.Endpoint
}

// QueueWrapper is a decorator that caps in-flight requests per endpoint and
// parks the overflow in a FIFO queue until a slot frees.
//
// The slot is reserved inside Select, not IncrementConnections, so choosing an
// endpoint and claiming capacity on it is one atomic step. The retry handler
// always pairs a successful Select with exactly one DecrementConnections, which
// is where the slot is released and the next waiter dispatched. There is no
// dispatcher goroutine: dispatch runs on the two events that can make progress
// possible (a slot freeing, or capacity growing).
type QueueWrapper struct {
	inner    domain.EndpointSelector
	resolver SlotResolver
	// pinLookup maps sticky keys to pinned endpoint URLs for the stats
	// listing. Optional: nil when sticky sessions are off.
	pinLookup func(keys []string) map[string]string
	// modelOf names the model a request will run on ep. Aliases can map one
	// requested name to different backend names per endpoint, so it is per
	// endpoint. Defaults to the requested model from the context.
	modelOf    func(ctx context.Context, ep *domain.Endpoint) string
	inUse      map[string]int
	modelInUse map[string]int
	// reservations is keyed by the endpoint copy Select hands out, which is
	// unique per request; see reserveLocked.
	reservations map[*domain.Endpoint]reservation
	known        map[string]*domain.Endpoint
	waiters      list.List
	cfg          config.QueueConfig
	stats        queueCounters
	mu           sync.Mutex
	closed       bool
}

type queueCounters struct {
	enqueued     atomic.Uint64
	dispatched   atomic.Uint64
	timedOut     atomic.Uint64
	rejectedFull atomic.Uint64
	cancelled    atomic.Uint64
	failed       atomic.Uint64
	waitTotalNs  atomic.Int64
	waitMaxNs    atomic.Int64
	// waitsDone counts requests that have left the queue for any reason, the
	// divisor for waitTotalNs. enqueued would over-count while some still wait.
	waitsDone atomic.Uint64
	peakDepth atomic.Int64
}

// NewQueueWrapper wraps inner with a slot-limited request queue. A nil resolver
// uses endpoint max_concurrent, then cfg.DefaultSlots.
func NewQueueWrapper(inner domain.EndpointSelector, cfg config.QueueConfig, resolver SlotResolver) *QueueWrapper {
	if cfg.DefaultSlots <= 0 {
		cfg.DefaultSlots = 1
	}
	if resolver == nil {
		resolver = staticSlots{fallback: cfg.DefaultSlots}
	}
	return &QueueWrapper{
		inner:        inner,
		resolver:     resolver,
		cfg:          cfg,
		inUse:        make(map[string]int),
		modelInUse:   make(map[string]int),
		reservations: make(map[*domain.Endpoint]reservation),
		known:        make(map[string]*domain.Endpoint),
		modelOf:      requestedModel,
	}
}

// requestedModel is the default modelOf: the model the client asked for.
func requestedModel(ctx context.Context, _ *domain.Endpoint) string {
	m, _ := ctx.Value(constants.ContextModelKey).(string)
	return m
}

// SetModelResolver replaces how the model for a request on an endpoint is
// named, e.g. to apply alias and canonical-name resolution.
func (q *QueueWrapper) SetModelResolver(fn func(ctx context.Context, ep *domain.Endpoint) string) {
	q.mu.Lock()
	q.modelOf = fn
	q.mu.Unlock()
}

// Name composes the inner balancer name.
func (q *QueueWrapper) Name() string {
	return "queue(" + q.inner.Name() + ")"
}

// IncrementConnections delegates for connection stats; the slot was already
// reserved by Select.
func (q *QueueWrapper) IncrementConnections(endpoint *domain.Endpoint) {
	q.inner.IncrementConnections(endpoint)
}

// DecrementConnections releases the slot reserved by Select and hands it to
// the oldest waiter that can use it.
func (q *QueueWrapper) DecrementConnections(endpoint *domain.Endpoint) {
	q.inner.DecrementConnections(endpoint)

	q.mu.Lock()
	q.releaseLocked(endpoint)
	q.dispatchLocked()
	q.mu.Unlock()
}

// Select returns an endpoint with a free slot, waiting in FIFO order if none
// has one. The wait ends on dispatch, ctx cancellation (client gone) or
// queue.timeout.
func (q *QueueWrapper) Select(ctx context.Context, endpoints []*domain.Endpoint) (*domain.Endpoint, error) {
	outcome, _ := ctx.Value(constants.ContextQueueOutcomeKey).(*domain.QueueOutcome)

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil, ErrQueueClosed
	}
	for _, ep := range endpoints {
		q.known[ep.URLString] = ep
	}

	// Taking a free slot straight away is always fair: every dispatch pass leaves
	// no waiter with a free candidate, so any slot free now is one no waiter can use.
	if free := q.freeLocked(ctx, endpoints); len(free) > 0 {
		ep, err := q.reserveLocked(ctx, free)
		q.mu.Unlock()
		return ep, err
	}

	if !q.anyRoutable(endpoints) {
		q.mu.Unlock()
		// Nothing to wait for; let the inner selector produce its usual error.
		return q.inner.Select(ctx, endpoints)
	}

	if q.waiters.Len() >= q.cfg.MaxSize {
		q.mu.Unlock()
		q.stats.rejectedFull.Add(1)
		return nil, ErrQueueFull
	}

	w := &queueWaiter{
		ctx:        ctx,
		candidates: endpoints,
		ready:      make(chan queueResult, 1),
		enqueuedAt: time.Now(),
	}
	if caller, ok := ctx.Value(constants.ContextQueueCallerKey).(*domain.QueueCaller); ok && caller != nil {
		w.caller = *caller
	}
	w.stickyKey, _ = ctx.Value(constants.ContextStickyKeyKey).(string)
	w.elem = q.waiters.PushBack(w)
	depth := int64(q.waiters.Len())
	q.mu.Unlock()

	q.stats.enqueued.Add(1)
	storeMax(&q.stats.peakDepth, depth)
	slog.Debug("request queued", "depth", depth, "candidates", len(endpoints))

	timer := time.NewTimer(q.cfg.Timeout)
	defer timer.Stop()

	var waitErr error
	select {
	case res := <-w.ready:
		return q.finishWait(w, outcome, res)
	case <-ctx.Done():
		waitErr = ctx.Err()
	case <-timer.C:
		waitErr = ErrQueueTimeout
	}

	q.mu.Lock()
	if w.elem != nil {
		q.waiters.Remove(w.elem)
		w.elem = nil
		q.mu.Unlock()
		if errors.Is(waitErr, ErrQueueTimeout) {
			q.stats.timedOut.Add(1)
		} else {
			q.stats.cancelled.Add(1)
		}
		q.recordWait(outcome, time.Since(w.enqueuedAt))
		return nil, waitErr
	}
	q.mu.Unlock()

	// Dispatch won the race: a slot is already reserved for us. Using it is
	// pointless if the client has gone, so hand it straight back.
	res := <-w.ready
	if res.err == nil && ctx.Err() != nil {
		q.mu.Lock()
		q.releaseLocked(res.endpoint)
		q.dispatchLocked()
		q.mu.Unlock()
		q.stats.cancelled.Add(1)
		return nil, ctx.Err()
	}
	return q.finishWait(w, outcome, res)
}

func (q *QueueWrapper) finishWait(w *queueWaiter, outcome *domain.QueueOutcome, res queueResult) (*domain.Endpoint, error) {
	waited := time.Since(w.enqueuedAt)
	q.recordWait(outcome, waited)
	if res.err != nil {
		return nil, res.err
	}
	q.stats.dispatched.Add(1)
	slog.Debug("queued request dispatched", "endpoint", res.endpoint.Name, "wait", waited)
	return res.endpoint, nil
}

func (q *QueueWrapper) recordWait(outcome *domain.QueueOutcome, waited time.Duration) {
	q.stats.waitTotalNs.Add(int64(waited))
	q.stats.waitsDone.Add(1)
	storeMax(&q.stats.waitMaxNs, int64(waited))
	// Accumulate: a retry after a failed attempt may queue again, and the
	// client cares about the total time spent waiting.
	if outcome != nil {
		outcome.Queued = true
		outcome.Wait += waited
	}
}

// SetPinLookup lets the stats listing show which endpoint each queued
// request's sticky session is pinned to.
func (q *QueueWrapper) SetPinLookup(fn func(keys []string) map[string]string) {
	q.mu.Lock()
	q.pinLookup = fn
	q.mu.Unlock()
}

// Wake runs a dispatch pass. Call it when slot capacity grows (e.g. a refresh
// discovered more slots) so waiters don't sit idle until the next release.
func (q *QueueWrapper) Wake() {
	q.mu.Lock()
	q.dispatchLocked()
	q.mu.Unlock()
}

// PurgeDeadEndpoints fails queued requests whose candidates are all outside the
// routable set, so they get an answer now instead of waiting out the timeout.
func (q *QueueWrapper) PurgeDeadEndpoints(routable []*domain.Endpoint) {
	alive := make(map[string]struct{}, len(routable))
	for _, ep := range routable {
		alive[ep.URLString] = struct{}{}
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	for e := q.waiters.Front(); e != nil; {
		next := e.Next()
		w := waiterAt(e)
		hasAlive := false
		for _, ep := range w.candidates {
			if _, ok := alive[ep.URLString]; ok {
				hasAlive = true
				break
			}
		}
		if !hasAlive {
			q.completeLocked(w, queueResult{err: ErrQueueEndpointsGone})
			q.stats.failed.Add(1)
		}
		e = next
	}
}

// Stop fails every queued request and rejects new ones. Safe to call more than once.
func (q *QueueWrapper) Stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for e := q.waiters.Front(); e != nil; {
		next := e.Next()
		q.completeLocked(waiterAt(e), queueResult{err: ErrQueueClosed})
		e = next
	}
}

// dispatchLocked walks the queue oldest-first and hands a slot to every waiter
// that has a free candidate. A waiter whose candidates are all full is skipped
// rather than blocking the line, so a request for a busy model never holds up
// one for an idle model.
func (q *QueueWrapper) dispatchLocked() {
	for e := q.waiters.Front(); e != nil; {
		next := e.Next()
		w := waiterAt(e)
		if free := q.freeLocked(w.ctx, w.candidates); len(free) > 0 {
			ep, err := q.reserveLocked(w.ctx, free)
			q.completeLocked(w, queueResult{endpoint: ep, err: err})
		}
		e = next
	}
}

func (q *QueueWrapper) completeLocked(w *queueWaiter, res queueResult) {
	q.waiters.Remove(w.elem)
	w.elem = nil
	w.ready <- res
}

// reserveLocked lets the inner strategy pick among free endpoints, then claims
// a slot on the endpoint and on the model. It returns a copy of the endpoint:
// the copy's pointer is unique to this request, so DecrementConnections can
// find exactly which model slot to release even if callers share endpoint
// pointers. The proxy path identifies endpoints by name and URL, never by
// pointer, so the copy is otherwise indistinguishable.
func (q *QueueWrapper) reserveLocked(ctx context.Context, free []*domain.Endpoint) (*domain.Endpoint, error) {
	ep, err := q.inner.Select(ctx, free)
	if err != nil {
		return nil, err
	}
	res := reservation{url: ep.URLString, model: q.modelOf(ctx, ep)}
	q.inUse[res.url]++
	if res.model != "" {
		q.modelInUse[modelKey(res.url, res.model)]++
	}
	claimed := *ep
	q.reservations[&claimed] = res
	return &claimed, nil
}

func (q *QueueWrapper) releaseLocked(ep *domain.Endpoint) {
	res, ok := q.reservations[ep]
	if ok {
		delete(q.reservations, ep)
	} else {
		// Not a pointer this queue handed out. Release some reservation on
		// the same endpoint so the per-model counts cannot drift upward.
		res = reservation{url: ep.URLString}
		for p, r := range q.reservations {
			if r.url == ep.URLString {
				res = r
				delete(q.reservations, p)
				break
			}
		}
	}
	// Guard against a stray decrement driving a count negative and
	// permanently over-admitting that endpoint.
	if q.inUse[res.url] > 0 {
		q.inUse[res.url]--
	}
	if res.model != "" {
		k := modelKey(res.url, res.model)
		if q.modelInUse[k] > 1 {
			q.modelInUse[k]--
		} else {
			delete(q.modelInUse, k)
		}
	}
}

// freeLocked filters to endpoints that have room for this request: under the
// endpoint-wide limit and, where the backend reported one, under the limit of
// the model the request will run.
func (q *QueueWrapper) freeLocked(ctx context.Context, endpoints []*domain.Endpoint) []*domain.Endpoint {
	var free []*domain.Endpoint
	for _, ep := range endpoints {
		if !ep.Status.IsRoutable() {
			continue
		}
		slots, _ := q.resolver.Slots(ep)
		if q.inUse[ep.URLString] >= slots {
			continue
		}
		if model := q.modelOf(ctx, ep); model != "" {
			if limit, known := q.resolver.ModelSlots(ep, model); known && q.modelInUse[modelKey(ep.URLString, model)] >= limit {
				continue
			}
		}
		free = append(free, ep)
	}
	return free
}

func (q *QueueWrapper) anyRoutable(endpoints []*domain.Endpoint) bool {
	for _, ep := range endpoints {
		if ep.Status.IsRoutable() {
			return true
		}
	}
	return false
}

// modelStatsLocked lists every model on ep with a reported limit or a slot in
// use, sorted by name for a stable payload.
func (q *QueueWrapper) modelStatsLocked(ep *domain.Endpoint) []QueueModelStats {
	byModel := make(map[string]*QueueModelStats)
	for model, n := range q.resolver.ModelSlotList(ep) {
		byModel[model] = &QueueModelStats{Model: model, Slots: n}
	}
	prefix := modelKey(ep.URLString, "")
	for k, n := range q.modelInUse {
		model, ok := strings.CutPrefix(k, prefix)
		if !ok {
			continue
		}
		if m, ok := byModel[model]; ok {
			m.InUse = n
		} else {
			byModel[model] = &QueueModelStats{Model: model, InUse: n}
		}
	}
	out := make([]QueueModelStats, 0, len(byModel))
	for _, m := range byModel {
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

// waiterAt unwraps a queue element. The list only ever holds *queueWaiter.
func waiterAt(e *list.Element) *queueWaiter {
	w, _ := e.Value.(*queueWaiter)
	return w
}

func storeMax(v *atomic.Int64, n int64) {
	for {
		cur := v.Load()
		if n <= cur || v.CompareAndSwap(cur, n) {
			return
		}
	}
}

// QueueModelStats is one model's slot use on an endpoint. Slots is 0 when the
// backend reported no limit for the model (it is then bounded only by the
// endpoint-wide limit).
type QueueModelStats struct {
	Model string `json:"model"`
	Slots int    `json:"slots"`
	InUse int    `json:"in_use"`
}

// QueueEndpointStats is the per-endpoint slot view.
type QueueEndpointStats struct {
	Name       string            `json:"name"`
	URL        string            `json:"url"`
	SlotSource string            `json:"slot_source"`
	Models     []QueueModelStats `json:"models"`
	Slots      int               `json:"slots"`
	InUse      int               `json:"in_use"`
}

// QueueWaiterStats describes one request waiting in the queue.
type QueueWaiterStats struct {
	EnqueuedAt time.Time `json:"enqueued_at"`
	RequestID  string    `json:"request_id"`
	ClientIP   string    `json:"client_ip"`
	Model      string    `json:"model"`
	UserAgent  string    `json:"user_agent"`
	// StickyEndpoint is empty when the request has no sticky session, or its
	// pin has expired or points at an endpoint the queue has not seen.
	StickyEndpoint string  `json:"sticky_endpoint"`
	WaitMs         float64 `json:"wait_ms"`
	Candidates     int     `json:"candidates"`
}

// QueueStats is a point-in-time snapshot of queue activity.
type QueueStats struct {
	Endpoints      []QueueEndpointStats `json:"endpoints"`
	Waiting        []QueueWaiterStats   `json:"waiting"`
	TimeoutSeconds float64              `json:"timeout_seconds"`
	AvgWaitMs      float64              `json:"avg_wait_ms"`
	MaxWaitMs      float64              `json:"max_wait_ms"`
	// WaitTotalMs and WaitsCompleted are monotonic counters, so a poller can
	// derive the mean wait over any window from two samples.
	WaitTotalMs    float64 `json:"wait_total_ms"`
	WaitsCompleted uint64  `json:"waits_completed"`
	Enqueued       uint64  `json:"enqueued"`
	Dispatched     uint64  `json:"dispatched"`
	TimedOut       uint64  `json:"timed_out"`
	RejectedFull   uint64  `json:"rejected_full"`
	Cancelled      uint64  `json:"cancelled"`
	Failed         uint64  `json:"failed"`
	Depth          int     `json:"depth"`
	PeakDepth      int     `json:"peak_depth"`
	MaxSize        int     `json:"max_size"`
	Enabled        bool    `json:"enabled"`
}

// Stats returns a snapshot of queue counters, per-endpoint slot usage and the
// requests currently waiting, oldest first.
func (q *QueueWrapper) Stats() QueueStats {
	now := time.Now()

	q.mu.Lock()
	depth := q.waiters.Len()
	eps := make([]QueueEndpointStats, 0, len(q.known))
	names := make(map[string]string, len(q.known))
	for url, ep := range q.known {
		slots, source := q.resolver.Slots(ep)
		names[url] = ep.Name
		eps = append(eps, QueueEndpointStats{
			Name:       ep.Name,
			URL:        url,
			Slots:      slots,
			SlotSource: source,
			InUse:      q.inUse[url],
			Models:     q.modelStatsLocked(ep),
		})
	}
	waiting := make([]QueueWaiterStats, 0, depth)
	keys := make([]string, 0, depth)
	for e := q.waiters.Front(); e != nil; e = e.Next() {
		w := waiterAt(e)
		waiting = append(waiting, QueueWaiterStats{
			RequestID:  w.caller.RequestID,
			ClientIP:   w.caller.ClientIP,
			Model:      w.caller.Model,
			UserAgent:  w.caller.UserAgent,
			EnqueuedAt: w.enqueuedAt,
			WaitMs:     float64(now.Sub(w.enqueuedAt)) / float64(time.Millisecond),
			Candidates: len(w.candidates),
		})
		keys = append(keys, w.stickyKey)
	}
	pinLookup := q.pinLookup
	q.mu.Unlock()

	sort.Slice(eps, func(i, j int) bool { return eps[i].Name < eps[j].Name })

	// Pins are resolved outside q.mu: the lookup walks the whole sticky store,
	// and dispatch must never wait on a dashboard poll.
	if pinLookup != nil && len(waiting) > 0 {
		pins := pinLookup(keys)
		for i, k := range keys {
			if url, ok := pins[k]; ok {
				waiting[i].StickyEndpoint = names[url]
			}
		}
	}

	waitTotalNs := q.stats.waitTotalNs.Load()
	done := q.stats.waitsDone.Load()
	var avgMs float64
	if done > 0 {
		avgMs = float64(waitTotalNs) / float64(done) / float64(time.Millisecond)
	}
	return QueueStats{
		Enabled:        true,
		Depth:          depth,
		PeakDepth:      int(q.stats.peakDepth.Load()),
		MaxSize:        q.cfg.MaxSize,
		TimeoutSeconds: q.cfg.Timeout.Seconds(),
		Enqueued:       q.stats.enqueued.Load(),
		Dispatched:     q.stats.dispatched.Load(),
		TimedOut:       q.stats.timedOut.Load(),
		RejectedFull:   q.stats.rejectedFull.Load(),
		Cancelled:      q.stats.cancelled.Load(),
		Failed:         q.stats.failed.Load(),
		AvgWaitMs:      avgMs,
		MaxWaitMs:      float64(q.stats.waitMaxNs.Load()) / float64(time.Millisecond),
		WaitTotalMs:    float64(waitTotalNs) / float64(time.Millisecond),
		WaitsCompleted: done,
		Endpoints:      eps,
		Waiting:        waiting,
	}
}
