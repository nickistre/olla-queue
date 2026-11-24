package balancer

import (
	"container/list"
	"context"
	"errors"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/util/pattern"
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
	// GroupSlots is the limit for one of ep's slot_groups.
	GroupSlots(ep *domain.Endpoint, group string) (slots int, source string)
	// ModelState says whether model is loaded on ep, as far as is known.
	ModelState(ep *domain.Endpoint, model string) ModelState
}

// ModelState is what is known about a model being resident on an endpoint.
type ModelState int

const (
	// ModelStateUnknown: the backend does not say (Ollama, vLLM, ...).
	ModelStateUnknown ModelState = iota
	// ModelStateLoaded: running now; a request starts without a load.
	ModelStateLoaded
	// ModelStateCold: a multi-model node that would have to load (and maybe
	// swap something out) first.
	ModelStateCold
)

// slotGroupOf names the slot group model belongs to on ep: the first group
// with a matching pattern, or "" when the endpoint has no group for it.
func slotGroupOf(ep *domain.Endpoint, model string) string {
	if model == "" {
		return ""
	}
	for _, g := range ep.SlotGroups {
		for _, p := range g.Models {
			if pattern.MatchesGlob(model, p) {
				return g.Name
			}
		}
	}
	return ""
}

func slotGroupConfig(ep *domain.Endpoint, group string) (domain.SlotGroup, bool) {
	for _, g := range ep.SlotGroups {
		if g.Name == group {
			return g, true
		}
	}
	return domain.SlotGroup{}, false
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

func (staticSlots) ModelState(*domain.Endpoint, string) ModelState { return ModelStateUnknown }

func (s staticSlots) GroupSlots(ep *domain.Endpoint, group string) (int, string) {
	if g, ok := slotGroupConfig(ep, group); ok && g.MaxConcurrent > 0 {
		return g.MaxConcurrent, SlotSourceConfig
	}
	return s.fallback, SlotSourceDefault
}

// reservation records which endpoint and model a Select claimed, so the
// matching DecrementConnections releases the right per-model slot.
// swapGrace is a live grace on one swap domain: until ends it, requests that
// would swap model out wait. timer re-runs dispatch when it ends.
type swapGrace struct {
	until time.Time
	timer *time.Timer
	model string
}

type reservation struct {
	url   string
	model string
	group string
}

// modelKey joins endpoint and model for the per-model counters. NUL cannot
// appear in a URL or a model name, so keys cannot collide.
func modelKey(url, model string) string { return url + "\x00" + model }

type queueResult struct {
	endpoint *domain.Endpoint
	err      error
}

type queueWaiter struct {
	enqueuedAt time.Time
	// holdUntil ends the wait for a busy pinned endpoint (sticky hold); zero
	// when holding is off. holdTimer wakes the queue at that moment, since no
	// slot release may come along to re-evaluate the request.
	holdUntil time.Time
	ctx       context.Context
	ready     chan queueResult // buffered 1, written once under q.mu
	elem      *list.Element    // nil once removed from the queue
	holdTimer *time.Timer
	// since is enqueuedAt less the time earlier attempts of this request
	// spent queued, so a retry keeps its age for queue_after and the grace.
	since time.Time
	// delayTimer wakes the queue when the request becomes old enough for the
	// next candidate's queue_after: that endpoint may sit idle, with no
	// release coming to re-run dispatch.
	delayTimer *time.Timer
	// refresher re-runs the handler's endpoint lookup; refresh signals that a
	// backend recovered and it is worth calling. Both nil when the handler
	// gave no refresher, so the wait never selects on refresh.
	refresher  domain.QueueRefresher
	refresh    chan struct{}
	caller     domain.QueueCaller // copied at enqueue for the stats listing
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
// dispatcher goroutine: dispatch runs on the events that can make progress
// possible (a slot freeing, capacity growing, outside load easing, or a
// sticky hold lapsing).
type QueueWrapper struct {
	inner    domain.EndpointSelector
	resolver SlotResolver
	// pinLookup maps sticky keys to pinned endpoint URLs for the stats
	// listing. Optional: nil when sticky sessions are off.
	pinLookup func(keys []string) map[string]string
	// listEndpoints returns every configured endpoint, so the stats listing
	// shows those the queue has not routed to yet. Optional.
	listEndpoints func(ctx context.Context) ([]*domain.Endpoint, error)
	// pinPeek resolves one sticky key to its pinned URL without touching the
	// session; with holdFor > 0 it drives sticky hold. Optional.
	pinPeek func(key string) (string, bool)
	// modelOf names the model a request will run on ep. Aliases can map one
	// requested name to different backend names per endpoint, so it is per
	// endpoint. Defaults to the requested model from the context.
	modelOf    func(ctx context.Context, ep *domain.Endpoint) string
	inUse      map[string]int
	modelInUse map[string]int
	// external holds slots the backend reports busy beyond this Olla's own
	// requests, per endpoint and per endpoint+model (see ObserveBusy).
	external      map[string]int
	modelExternal map[string]int
	// groupInUse and groupExternal are the same counts per slot group, keyed
	// by modelKey(url, group).
	groupInUse    map[string]int
	groupExternal map[string]int
	// reservations is keyed by the endpoint copy Select hands out, which is
	// unique per request; see reserveLocked.
	reservations map[*domain.Endpoint]reservation
	known        map[string]*domain.Endpoint
	// down holds endpoints the health checker has reported offline, with when.
	// A request's endpoint copies are snapshots from when it arrived, so their
	// Status goes stale while it waits; this is the live view.
	down    map[string]time.Time
	waiters list.List
	stats   queueCounters
	cfg     config.QueueConfig
	holdFor time.Duration
	// grace holds, per swap domain (slot group, else endpoint), when the swap
	// grace for the model that last finished there ends; see startGraceLocked.
	grace map[string]swapGrace
	// overflowed counts requests sent to each overflow endpoint, by URL.
	overflowed map[string]uint64
	mu         sync.Mutex
	closed     bool
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
	// waitMinNs is 0 until the first wait completes; see storeMin.
	waitMinNs atomic.Int64
	// waitsDone counts requests that have left the queue for any reason, the
	// divisor for waitTotalNs. enqueued would over-count while some still wait.
	waitsDone atomic.Uint64
	peakDepth atomic.Int64
	// holdKept counts queued requests that waited for and then got their
	// pinned endpoint; holdMoved those that went elsewhere despite a pin.
	holdKept  atomic.Uint64
	holdMoved atomic.Uint64
	// overflowed counts requests sent to an endpoint with queue_after.
	overflowed atomic.Uint64
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
		inner:         inner,
		resolver:      resolver,
		cfg:           cfg,
		inUse:         make(map[string]int),
		modelInUse:    make(map[string]int),
		external:      make(map[string]int),
		modelExternal: make(map[string]int),
		groupInUse:    make(map[string]int),
		groupExternal: make(map[string]int),
		reservations:  make(map[*domain.Endpoint]reservation),
		known:         make(map[string]*domain.Endpoint),
		down:          make(map[string]time.Time),
		grace:         make(map[string]swapGrace),
		overflowed:    make(map[string]uint64),
		modelOf:       requestedModel,
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
	q.startGraceLocked(endpoint, q.releaseLocked(endpoint))
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
	recovered := false
	for _, ep := range endpoints {
		q.known[ep.URLString] = ep
		if q.clearIfFreshLocked(ep) {
			recovered = true
		}
	}
	if recovered {
		// Waiters go first: the slot this request would take may be one they
		// have been waiting for.
		q.dispatchLocked()
	}

	now := time.Now()
	since := now
	if outcome != nil {
		since = now.Add(-outcome.Wait)
	}
	stickyKey, _ := ctx.Value(constants.ContextStickyKeyKey).(string)
	var holdUntil time.Time
	if q.holdFor > 0 && stickyKey != "" {
		holdUntil = now.Add(q.holdFor)
	}

	// Taking a free slot straight away is always fair: every dispatch pass
	// leaves no waiter able to use a free slot, so any slot this request can
	// take now is one no waiter wants. A holding waiter only wants its pinned
	// endpoint, which it gets first whenever that frees.
	if free := q.admissibleLocked(ctx, endpoints, stickyKey, holdUntil, since, now); len(free) > 0 {
		ep, err := q.reserveLocked(ctx, free)
		q.mu.Unlock()
		return ep, err
	}

	if !q.anyUpLocked(endpoints) {
		q.mu.Unlock()
		// Nothing to wait for. Copies that still look routable were taken
		// before the health checker marked them down, and the inner selector
		// would pick one; otherwise let it produce its usual error.
		for _, ep := range endpoints {
			if ep.Status.IsRoutable() {
				return nil, ErrQueueEndpointsGone
			}
		}
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
		enqueuedAt: now,
		since:      since,
		stickyKey:  stickyKey,
		holdUntil:  holdUntil,
	}
	if caller, ok := ctx.Value(constants.ContextQueueCallerKey).(*domain.QueueCaller); ok && caller != nil {
		w.caller = *caller
	}
	if refresher, ok := ctx.Value(constants.ContextQueueRefreshKey).(domain.QueueRefresher); ok && refresher != nil {
		w.refresher = refresher
		w.refresh = make(chan struct{}, 1)
	}
	if !holdUntil.IsZero() {
		// When the hold lapses the request may take any free endpoint, but a
		// free one may already be sitting idle with no release coming.
		w.holdTimer = time.AfterFunc(q.holdFor, q.Wake)
	}
	q.armDelayTimerLocked(w, now)
	w.elem = q.waiters.PushBack(w)
	depth := int64(q.waiters.Len())
	q.mu.Unlock()

	q.stats.enqueued.Add(1)
	storeMax(&q.stats.peakDepth, depth)
	slog.Debug("request queued", "depth", depth, "candidates", len(endpoints))

	res, waitErr := q.await(ctx, w)
	if waitErr == nil {
		return q.finishWait(w, outcome, res)
	}

	q.mu.Lock()
	if w.elem != nil {
		q.removeLocked(w)
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
	res = <-w.ready
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

// await blocks until w is dispatched, its context ends or queue.timeout
// passes, running the request's endpoint lookup again whenever a recovery
// signals it.
func (q *QueueWrapper) await(ctx context.Context, w *queueWaiter) (queueResult, error) {
	timer := time.NewTimer(q.cfg.Timeout)
	defer timer.Stop()
	for {
		select {
		case res := <-w.ready:
			return res, nil
		case <-w.refresh:
			// Outside q.mu: the lookup goes through the registry and discovery.
			q.addCandidates(w, w.refresher(ctx))
		case <-ctx.Done():
			return queueResult{}, ctx.Err()
		case <-timer.C:
			return queueResult{}, ErrQueueTimeout
		}
	}
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
	storeMin(&q.stats.waitMinNs, int64(waited))
	// Accumulate: a retry after a failed attempt may queue again, and the
	// client cares about the total time spent waiting.
	if outcome != nil {
		outcome.Queued = true
		outcome.Wait += waited
	}
}

// SetStickyHold makes a request whose pinned endpoint is busy wait up to hold
// for it instead of moving to another endpoint, which would lose the
// conversation's KV cache. peek must not touch the session (no hit/miss, no
// TTL slide). hold <= 0 turns holding off.
func (q *QueueWrapper) SetStickyHold(peek func(key string) (string, bool), hold time.Duration) {
	q.mu.Lock()
	q.pinPeek = peek
	q.holdFor = hold
	q.mu.Unlock()
}

// SetPinLookup lets the stats listing show which endpoint each queued
// request's sticky session is pinned to.
func (q *QueueWrapper) SetPinLookup(fn func(keys []string) map[string]string) {
	q.mu.Lock()
	q.pinLookup = fn
	q.mu.Unlock()
}

// SetEndpointLister lets the stats listing include configured endpoints the
// queue has not seen yet, such as an overflow endpoint no request has needed.
func (q *QueueWrapper) SetEndpointLister(fn func(ctx context.Context) ([]*domain.Endpoint, error)) {
	q.mu.Lock()
	q.listEndpoints = fn
	q.mu.Unlock()
}

// ObserveBusy takes a backend's own count of running requests (everyone's,
// ours included) and keeps the part that is not ours: those slots are taken
// as far as admission is concerned.
//
// Our share is read at the same moment, so the difference is what other
// clients hold. A request of ours the backend has not registered yet makes
// the difference too small; that is clamped at 0 and only ever under-counts,
// which errs towards the old behaviour rather than blocking. A request of
// ours that just finished but is still listed over-counts by one until the
// next reading, at most one poll interval (milliseconds on llama-swap's feed).
//
// An invalid reading clears what was known: a dropped feed or a server
// started with --no-slots falls back to counting only this Olla's traffic.
func (q *QueueWrapper) ObserveBusy(ep *domain.Endpoint, b BackendBusy) {
	q.mu.Lock()
	defer q.mu.Unlock()

	url := ep.URLString
	// List the endpoint in stats before any request is routed to it, so the
	// dashboard shows a busy backend from the first reading.
	if _, ok := q.known[url]; !ok {
		q.known[url] = ep
	}

	prefix := modelKey(url, "")
	for _, m := range []map[string]int{q.modelExternal, q.groupExternal} {
		for k := range m {
			if strings.HasPrefix(k, prefix) {
				delete(m, k)
			}
		}
	}
	if !b.Valid {
		delete(q.external, url)
	} else {
		q.external[url] = max(0, b.Total-q.inUse[url])
		groupRunning := make(map[string]int)
		for model, n := range b.Models {
			k := modelKey(url, model)
			if ext := n - q.modelInUse[k]; ext > 0 {
				q.modelExternal[k] = ext
			}
			if g := slotGroupOf(ep, model); g != "" {
				groupRunning[g] += n
			}
		}
		for g, n := range groupRunning {
			k := modelKey(url, g)
			if ext := n - q.groupInUse[k]; ext > 0 {
				q.groupExternal[k] = ext
			}
		}
	}
	// Outside traffic easing off frees slots that no release of ours will
	// signal, so every reading gets a dispatch pass (one walk of the queue).
	q.dispatchLocked()
}

// Wake runs a dispatch pass. Call it when slot capacity grows (e.g. a refresh
// discovered more slots) so waiters don't sit idle until the next release.
func (q *QueueWrapper) Wake() {
	q.mu.Lock()
	q.dispatchLocked()
	q.mu.Unlock()
}

// PurgeDeadEndpoints records which endpoints have left the routable set, so
// waiters stop being handed them, and fails queued requests whose candidates
// are all gone, so they get an answer now instead of waiting out the timeout.
// An endpoint back in the set counts as recovered.
func (q *QueueWrapper) PurgeDeadEndpoints(routable []*domain.Endpoint) {
	alive := make(map[string]struct{}, len(routable))
	for _, ep := range routable {
		alive[ep.URLString] = struct{}{}
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	// An endpoint going down can open a door too: with no endpoint without a
	// queue_after delay left up, overflow endpoints take requests at once.
	changed := false
	for url := range q.known {
		if _, ok := alive[url]; !ok {
			if _, already := q.down[url]; !already {
				q.down[url] = now
				changed = true
			}
		}
	}
	for url := range alive {
		if _, wasDown := q.down[url]; wasDown {
			delete(q.down, url)
			q.signalRefreshLocked(url)
			changed = true
		}
	}

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
	if changed {
		q.dispatchLocked()
	}
}

// EndpointRecovered is the health checker's recovery hook. The endpoint takes
// traffic again, and requests already queued are told to look it up: it was
// left out of their candidates if it was offline when they arrived.
func (q *QueueWrapper) EndpointRecovered(ep *domain.Endpoint) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.down, ep.URLString)
	q.signalRefreshLocked(ep.URLString)
	q.dispatchLocked()
}

// EndpointModelsUpdated is model discovery's hook: ep's model list was just
// registered, so it may now serve a model queued requests are waiting for.
// Those without it re-run their endpoint lookup; the signal coalesces, so a
// discovery cycle across many endpoints costs each waiter at most one lookup
// in flight.
func (q *QueueWrapper) EndpointModelsUpdated(ep *domain.Endpoint) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.signalRefreshLocked(ep.URLString)
}

// EndpointFailed marks ep down when a connection to it fails. The retry
// handler calls it before releasing the failed attempt's slot, so the slot
// isn't handed to a waiter that would hit the same dead backend. The health
// checker's recovery hook, or a newer routable copy, clears it.
func (q *QueueWrapper) EndpointFailed(ep *domain.Endpoint) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, already := q.down[ep.URLString]; !already {
		q.down[ep.URLString] = time.Now()
	}
}

// signalRefreshLocked nudges every waiter that could gain url as a candidate.
// The signal is a buffered slot, so a burst of recoveries costs one lookup.
func (q *QueueWrapper) signalRefreshLocked(url string) {
	for e := q.waiters.Front(); e != nil; e = e.Next() {
		w := waiterAt(e)
		if w.refresh == nil || hasURL(w.candidates, url) {
			continue
		}
		select {
		case w.refresh <- struct{}{}:
		default:
		}
	}
}

// addCandidates merges a refreshed endpoint list into a waiter's candidates.
// Only endpoints of a type the request already had are taken: the handler may
// have shaped the request for that backend type (passthrough, path rewrites),
// and a different type could need a different request.
func (q *QueueWrapper) addCandidates(w *queueWaiter, eps []*domain.Endpoint) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if w.elem == nil {
		return // dispatched or failed while the lookup ran
	}
	types := make(map[string]struct{}, len(w.candidates))
	for _, ep := range w.candidates {
		types[ep.Type] = struct{}{}
	}
	var added []*domain.Endpoint
	for _, ep := range eps {
		if hasURL(w.candidates, ep.URLString) {
			continue
		}
		if _, ok := types[ep.Type]; !ok {
			slog.Debug("queued request skipped a new endpoint of another type", "endpoint", ep.Name, "type", ep.Type)
			continue
		}
		q.known[ep.URLString] = ep
		q.clearIfFreshLocked(ep)
		added = append(added, ep)
	}
	if len(added) == 0 {
		return
	}
	// Clip so the append never writes into the retry handler's slice.
	w.candidates = append(slices.Clip(w.candidates), added...)
	q.armDelayTimerLocked(w, time.Now())
	slog.Debug("queued request gained candidates", "added", len(added), "candidates", len(w.candidates))
	q.dispatchLocked()
}

// clearIfFreshLocked drops ep from down when ep is a copy taken after the
// health checker marked it down and it was routable then. That catches a
// recovery the hooks missed, such as offline straight to busy.
func (q *QueueWrapper) clearIfFreshLocked(ep *domain.Endpoint) bool {
	downAt, isDown := q.down[ep.URLString]
	if !isDown || !ep.Status.IsRoutable() || !ep.LastChecked.After(downAt) {
		return false
	}
	delete(q.down, ep.URLString)
	q.signalRefreshLocked(ep.URLString)
	return true
}

// upLocked reports whether ep can take traffic now, going by the health
// checker's live view rather than the request's possibly stale copy.
func (q *QueueWrapper) upLocked(ep *domain.Endpoint) bool {
	if !ep.Status.IsRoutable() {
		return false
	}
	_, isDown := q.down[ep.URLString]
	return !isDown
}

func hasURL(eps []*domain.Endpoint, url string) bool {
	for _, ep := range eps {
		if ep.URLString == url {
			return true
		}
	}
	return false
}

// Stop fails every queued request and rejects new ones. Safe to call more than once.
func (q *QueueWrapper) Stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for _, g := range q.grace {
		g.timer.Stop()
	}
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
//
// With warm_first_max_wait set, the walk runs twice. The first pass serves
// requests whose model is already loaded on an endpoint they can use, plus any
// that have waited longer than the cap, in arrival order; the second serves
// the rest. A request that would load or swap a model therefore lets warm ones
// go first, so a swap is not forced while work for the loaded model waits,
// but never for longer than the cap.
func (q *QueueWrapper) dispatchLocked() {
	now := time.Now()
	if q.cfg.WarmFirstMaxWait > 0 {
		q.dispatchPassLocked(now, func(w *queueWaiter, free []*domain.Endpoint) bool {
			return now.Sub(w.enqueuedAt) >= q.cfg.WarmFirstMaxWait || q.anyLoadedLocked(w.ctx, free)
		})
	}
	q.dispatchPassLocked(now, func(*queueWaiter, []*domain.Endpoint) bool { return true })
}

func (q *QueueWrapper) dispatchPassLocked(now time.Time, eligible func(*queueWaiter, []*domain.Endpoint) bool) {
	for e := q.waiters.Front(); e != nil; {
		next := e.Next()
		w := waiterAt(e)
		if free := q.admissibleLocked(w.ctx, w.candidates, w.stickyKey, w.holdUntil, w.since, now); len(free) > 0 && eligible(w, free) {
			pinned, hadPin := q.holdPinLocked(w)
			ep, err := q.reserveLocked(w.ctx, free)
			if err == nil && hadPin {
				q.countHoldLocked(pinned, ep)
			}
			q.completeLocked(w, queueResult{endpoint: ep, err: err})
		}
		e = next
	}
}

// holdPinLocked returns the live pin of a queued request that holds for one.
// It must be read before reserving: the sticky selector re-pins the key when
// the request moves, so a later read would always see a match.
func (q *QueueWrapper) holdPinLocked(w *queueWaiter) (string, bool) {
	if w.holdUntil.IsZero() || q.pinPeek == nil {
		return "", false
	}
	return q.pinPeek(w.stickyKey)
}

// countHoldLocked records whether a queued request with a live pin got it.
func (q *QueueWrapper) countHoldLocked(pinned string, got *domain.Endpoint) {
	if pinned == got.URLString {
		q.stats.holdKept.Add(1)
	} else {
		q.stats.holdMoved.Add(1)
	}
}

func (q *QueueWrapper) completeLocked(w *queueWaiter, res queueResult) {
	q.removeLocked(w)
	w.ready <- res
}

func (q *QueueWrapper) removeLocked(w *queueWaiter) {
	q.waiters.Remove(w.elem)
	w.elem = nil
	if w.holdTimer != nil {
		w.holdTimer.Stop()
	}
	if w.delayTimer != nil {
		w.delayTimer.Stop()
	}
}

// admissibleLocked is freeLocked plus swap grace and sticky hold. While a
// request's hold lasts and its pinned endpoint is one of its candidates, that
// endpoint is the only one it will take. Once the hold lapses, or the pin is
// gone or points at an endpoint this request cannot use, any free candidate
// will do. since is when the request arrived, for the swap grace's age cap.
func (q *QueueWrapper) admissibleLocked(ctx context.Context, candidates []*domain.Endpoint, stickyKey string, holdUntil, since, now time.Time) []*domain.Endpoint {
	age := now.Sub(since)
	free := q.delayedLocked(candidates, q.graceLocked(ctx, q.freeLocked(ctx, candidates), since, now), age)
	if holdUntil.IsZero() || !now.Before(holdUntil) || q.pinPeek == nil {
		return free
	}
	pinned, ok := q.pinPeek(stickyKey)
	if !ok {
		return free
	}
	// A pin to an overflow endpoint this request can't use yet causes no hold.
	pinnedIsCandidate := false
	for _, ep := range q.delayedLocked(candidates, candidates, age) {
		if ep.URLString == pinned && q.upLocked(ep) {
			pinnedIsCandidate = true
			break
		}
	}
	if !pinnedIsCandidate {
		return free
	}
	for _, ep := range free {
		if ep.URLString == pinned {
			return []*domain.Endpoint{ep}
		}
	}
	return nil // pinned endpoint busy: wait for it
}

// delayedLocked drops endpoints from eps whose queue_after this request, of
// age, hasn't waited out yet. The delay is waived when no candidate without
// one is up: with nothing else to wait for, waiting only delays the answer.
func (q *QueueWrapper) delayedLocked(candidates, eps []*domain.Endpoint, age time.Duration) []*domain.Endpoint {
	if !anyDelayed(eps, age) {
		return eps
	}
	waived := true
	for _, ep := range candidates {
		if ep.QueueAfter <= 0 && q.upLocked(ep) {
			waived = false
			break
		}
	}
	if waived {
		return eps
	}
	out := eps[:0:0]
	for _, ep := range eps {
		if ep.QueueAfter <= age {
			out = append(out, ep)
		}
	}
	return out
}

func anyDelayed(eps []*domain.Endpoint, age time.Duration) bool {
	for _, ep := range eps {
		if ep.QueueAfter > age {
			return true
		}
	}
	return false
}

// lastResort leaves overflow endpoints (queue_after set) out while any other
// endpoint is free: they relieve a long wait, not share the normal load.
func lastResort(free []*domain.Endpoint) []*domain.Endpoint {
	var local []*domain.Endpoint
	for _, ep := range free {
		if ep.QueueAfter <= 0 {
			local = append(local, ep)
		}
	}
	if len(local) == 0 {
		return free
	}
	return local
}

// armDelayTimerLocked sets w's timer for the next queue_after boundary among
// its candidates, if one is still ahead.
func (q *QueueWrapper) armDelayTimerLocked(w *queueWaiter, now time.Time) {
	age := now.Sub(w.since)
	var next time.Duration
	for _, ep := range w.candidates {
		if d := ep.QueueAfter - age; d > 0 && (next == 0 || d < next) {
			next = d
		}
	}
	if w.delayTimer != nil {
		w.delayTimer.Stop()
		w.delayTimer = nil
	}
	if next > 0 {
		w.delayTimer = time.AfterFunc(next, q.Wake)
	}
}

// graceKey names the swap domain model runs in on ep: its slot group, else the
// whole endpoint.
func graceKey(ep *domain.Endpoint, model string) string {
	if g := slotGroupOf(ep, model); g != "" {
		return modelKey(ep.URLString, g)
	}
	return ep.URLString
}

// startGraceLocked opens a swap grace after a request for a loaded model
// finishes. A turn-by-turn client sends its next turn a moment later; without
// the grace, a waiting request for another model would take the free slot in
// that gap and swap this one out from under the conversation.
func (q *QueueWrapper) startGraceLocked(ep *domain.Endpoint, res reservation) {
	if q.cfg.SwapGrace <= 0 || q.cfg.WarmFirstMaxWait <= 0 || res.model == "" || q.closed {
		return
	}
	if q.resolver.ModelState(ep, res.model) != ModelStateLoaded {
		return
	}
	key := graceKey(ep, res.model)
	g := q.grace[key]
	if g.timer == nil {
		g.timer = time.AfterFunc(q.cfg.SwapGrace, q.Wake)
	} else {
		g.timer.Reset(q.cfg.SwapGrace)
	}
	g.until = time.Now().Add(q.cfg.SwapGrace)
	g.model = res.model
	q.grace[key] = g
}

// graceLocked drops free endpoints where running this request would swap out
// a model still inside its swap grace. It never holds a request past
// warm_first_max_wait from its arrival, and never one for the model the grace
// protects.
func (q *QueueWrapper) graceLocked(ctx context.Context, free []*domain.Endpoint, since, now time.Time) []*domain.Endpoint {
	if len(q.grace) == 0 || now.Sub(since) >= q.cfg.WarmFirstMaxWait {
		return free
	}
	out := free[:0:0]
	for _, ep := range free {
		if !q.inGraceLocked(ctx, ep, now) {
			out = append(out, ep)
		}
	}
	return out
}

func (q *QueueWrapper) inGraceLocked(ctx context.Context, ep *domain.Endpoint, now time.Time) bool {
	model := q.modelOf(ctx, ep)
	if model == "" {
		return false
	}
	g, ok := q.grace[graceKey(ep, model)]
	if !ok || !now.Before(g.until) || g.model == model {
		return false
	}
	return q.resolver.ModelState(ep, model) == ModelStateCold
}

// graceHeldLocked reports whether swap grace is all that keeps w waiting: it
// has free endpoints, but each would swap out a model inside its grace.
func (q *QueueWrapper) graceHeldLocked(w *queueWaiter, now time.Time) bool {
	free := q.freeLocked(w.ctx, w.candidates)
	return len(free) > 0 && len(q.graceLocked(w.ctx, free, w.since, now)) == 0
}

// anyLoadedLocked reports whether the request's model is loaded on one of
// endpoints.
func (q *QueueWrapper) anyLoadedLocked(ctx context.Context, endpoints []*domain.Endpoint) bool {
	for _, ep := range endpoints {
		if q.resolver.ModelState(ep, q.modelOf(ctx, ep)) == ModelStateLoaded {
			return true
		}
	}
	return false
}

// preferLoadedLocked narrows free endpoints to the best tier for this request:
// ones with its model loaded, else ones that do not report (Ollama and the
// like), else cold ones that would load or swap. The configured strategy then
// picks within the tier, so priority and connection balancing still apply.
func (q *QueueWrapper) preferLoadedLocked(ctx context.Context, free []*domain.Endpoint) []*domain.Endpoint {
	if len(free) < 2 {
		return free
	}
	var loaded, unknown []*domain.Endpoint
	for _, ep := range free {
		switch q.resolver.ModelState(ep, q.modelOf(ctx, ep)) {
		case ModelStateLoaded:
			loaded = append(loaded, ep)
		case ModelStateUnknown:
			unknown = append(unknown, ep)
		case ModelStateCold:
			// Last resort: only chosen when every free endpoint is cold, via
			// the fall-through to free below.
		}
	}
	switch {
	case len(loaded) > 0:
		return loaded
	case len(unknown) > 0:
		return unknown
	default:
		return free
	}
}

// reserveLocked lets the inner strategy pick among free endpoints, then claims
// a slot on the endpoint and on the model. It returns a copy of the endpoint:
// the copy's pointer is unique to this request, so DecrementConnections can
// find exactly which model slot to release even if callers share endpoint
// pointers. The proxy path identifies endpoints by name and URL, never by
// pointer, so the copy is otherwise indistinguishable.
func (q *QueueWrapper) reserveLocked(ctx context.Context, free []*domain.Endpoint) (*domain.Endpoint, error) {
	ep, err := q.inner.Select(ctx, q.preferLoadedLocked(ctx, lastResort(free)))
	if err != nil {
		return nil, err
	}
	res := reservation{url: ep.URLString, model: q.modelOf(ctx, ep)}
	res.group = slotGroupOf(ep, res.model)
	q.inUse[res.url]++
	if res.model != "" {
		q.modelInUse[modelKey(res.url, res.model)]++
	}
	if res.group != "" {
		q.groupInUse[modelKey(res.url, res.group)]++
	}
	if ep.QueueAfter > 0 {
		q.overflowed[res.url]++
		q.stats.overflowed.Add(1)
	}
	claimed := *ep
	q.reservations[&claimed] = res
	return &claimed, nil
}

func (q *QueueWrapper) releaseLocked(ep *domain.Endpoint) reservation {
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
		decrementOrDelete(q.modelInUse, modelKey(res.url, res.model))
	}
	if res.group != "" {
		decrementOrDelete(q.groupInUse, modelKey(res.url, res.group))
	}
	return res
}

func decrementOrDelete(m map[string]int, k string) {
	if m[k] > 1 {
		m[k]--
	} else {
		delete(m, k)
	}
}

// freeLocked filters to endpoints that have room for this request: under the
// endpoint-wide limit, under its slot group's limit and, where the backend
// reported one, under the limit of the model the request will run. On top of
// the limits, a request must not force a swap under running work (swapConflict)
// and must not hold up an aged request that is waiting to swap (draining).
//
// A grouped model skips the endpoint-wide limit unless max_concurrent sets it.
// The discovered total only sums loaded models (and falls back to
// default_slots), so it would let a busy group block an idle one alongside it.
func (q *QueueWrapper) freeLocked(ctx context.Context, endpoints []*domain.Endpoint) []*domain.Endpoint {
	var free []*domain.Endpoint
	now := time.Now()
	for _, ep := range endpoints {
		if !q.upLocked(ep) {
			continue
		}
		model := q.modelOf(ctx, ep)
		g := ""
		if model != "" {
			g = slotGroupOf(ep, model)
		}
		if g == "" || ep.MaxConcurrent > 0 {
			slots, _ := q.resolver.Slots(ep)
			if q.inUse[ep.URLString]+q.external[ep.URLString] >= slots {
				continue
			}
		}
		if model != "" {
			k := modelKey(ep.URLString, model)
			if limit, known := q.resolver.ModelSlots(ep, model); known && q.modelInUse[k]+q.modelExternal[k] >= limit {
				continue
			}
			if g != "" {
				gk := modelKey(ep.URLString, g)
				if limit, _ := q.resolver.GroupSlots(ep, g); q.groupInUse[gk]+q.groupExternal[gk] >= limit {
					continue
				}
			}
			if q.swapConflictLocked(ep, model) || q.drainingLocked(ep, model, now) {
				continue
			}
		}
		free = append(free, ep)
	}
	return free
}

// swapConflictLocked reports whether running model on ep now would swap a model
// out from under running work, or race one that is loading. A swap domain
// (slot group, else endpoint; see graceKey) holds one model at a time, so its
// limit is the loaded model's slots. With --parallel above 1 that leaves room
// a request for another model would otherwise take, forcing a swap while the
// loaded model's requests run.
//
// It is a conflict when another model has requests in flight in the same
// domain, Olla's or another client's, and either model is cold. Cold here
// means one that would have to load. A request for a cold model therefore
// waits for the domain to drain. While a cold model's request is in flight
// (it is loading), requests for the outgoing model wait too, because discovery
// still reports that one loaded until the swap shows up. Two models that are
// both loaded run side by side, as on a node that keeps several in memory.
func (q *QueueWrapper) swapConflictLocked(ep *domain.Endpoint, model string) bool {
	mine := q.resolver.ModelState(ep, model)
	if mine == ModelStateUnknown {
		return false
	}
	dom := graceKey(ep, model)
	prefix := modelKey(ep.URLString, "")
	conflicts := func(counts map[string]int) bool {
		for k, n := range counts {
			other, ok := strings.CutPrefix(k, prefix)
			if !ok || n <= 0 || other == model || graceKey(ep, other) != dom {
				continue
			}
			if mine == ModelStateCold || q.resolver.ModelState(ep, other) == ModelStateCold {
				return true
			}
		}
		return false
	}
	return conflicts(q.modelInUse) || conflicts(q.modelExternal)
}

// drainingLocked reports whether a request for model, loaded on ep, should
// queue so that its swap domain can drain for an older request. That request
// is one for a cold model in the same domain that has waited at least
// warm_first_max_wait (any time at all when warm-first is off). Without this,
// steady traffic for the loaded model would keep the domain busy, and the cold
// request, which needs it idle (swapConflictLocked), would never run. Past the
// cap it is arrival order again, as in dispatchLocked.
func (q *QueueWrapper) drainingLocked(ep *domain.Endpoint, model string, now time.Time) bool {
	if q.resolver.ModelState(ep, model) != ModelStateLoaded {
		return false
	}
	dom := graceKey(ep, model)
	for e := q.waiters.Front(); e != nil; e = e.Next() {
		w := waiterAt(e)
		if now.Sub(w.enqueuedAt) < q.cfg.WarmFirstMaxWait {
			break // arrival order: every later waiter is younger still
		}
		for _, c := range w.candidates {
			if c.URLString != ep.URLString {
				continue
			}
			wm := q.modelOf(w.ctx, c)
			if wm != "" && wm != model && graceKey(c, wm) == dom && q.resolver.ModelState(c, wm) == ModelStateCold {
				return true
			}
		}
	}
	return false
}

func (q *QueueWrapper) anyUpLocked(endpoints []*domain.Endpoint) bool {
	for _, ep := range endpoints {
		if q.upLocked(ep) {
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
	entry := func(model string) *QueueModelStats {
		m, ok := byModel[model]
		if !ok {
			m = &QueueModelStats{Model: model}
			byModel[model] = m
		}
		return m
	}
	for k, n := range q.modelInUse {
		if model, ok := strings.CutPrefix(k, prefix); ok {
			entry(model).InUse = n
		}
	}
	for k, n := range q.modelExternal {
		if model, ok := strings.CutPrefix(k, prefix); ok {
			entry(model).External = n
		}
	}
	out := make([]QueueModelStats, 0, len(byModel))
	for _, m := range byModel {
		m.Group = slotGroupOf(ep, m.Model)
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

// holdingLocked reports whether w is waiting for its busy pinned endpoint.
func (q *QueueWrapper) holdingLocked(w *queueWaiter, now time.Time) bool {
	if w.holdUntil.IsZero() || !now.Before(w.holdUntil) || q.pinPeek == nil {
		return false
	}
	pinned, ok := q.pinPeek(w.stickyKey)
	if !ok {
		return false
	}
	for _, ep := range q.delayedLocked(w.candidates, w.candidates, now.Sub(w.since)) {
		if ep.URLString == pinned {
			return true
		}
	}
	return false
}

// coldLocked reports whether no candidate endpoint has w's model loaded and at
// least one would have to load it.
func (q *QueueWrapper) coldLocked(w *queueWaiter) bool {
	cold := false
	for _, ep := range w.candidates {
		switch q.resolver.ModelState(ep, q.modelOf(w.ctx, ep)) {
		case ModelStateLoaded, ModelStateUnknown:
			return false
		case ModelStateCold:
			cold = true
		}
	}
	return cold
}

// groupStatsLocked lists ep's configured slot groups in config order.
func (q *QueueWrapper) groupStatsLocked(ep *domain.Endpoint) []QueueGroupStats {
	out := make([]QueueGroupStats, 0, len(ep.SlotGroups))
	for _, g := range ep.SlotGroups {
		slots, source := q.resolver.GroupSlots(ep, g.Name)
		k := modelKey(ep.URLString, g.Name)
		out = append(out, QueueGroupStats{
			Name:       g.Name,
			Slots:      slots,
			SlotSource: source,
			State:      q.groupStateLocked(ep, g),
			InUse:      q.groupInUse[k],
			External:   q.groupExternal[k],
		})
	}
	return out
}

// groupStateLocked is "loaded" when a member model is loaded, "cold" when
// the backend tracks loading and none is, so the next request loads one,
// and "unknown" when the backend doesn't say.
func (q *QueueWrapper) groupStateLocked(ep *domain.Endpoint, g domain.SlotGroup) string {
	for model := range q.resolver.ModelSlotList(ep) {
		if slotGroupOf(ep, model) == g.Name && q.resolver.ModelState(ep, model) == ModelStateLoaded {
			return GroupStateLoaded
		}
	}
	if len(g.Models) == 0 {
		return GroupStateUnknown
	}
	// No loaded member: ask about one by name. A backend that tracks loading
	// reports any model it isn't running as cold.
	switch q.resolver.ModelState(ep, g.Models[0]) {
	case ModelStateCold:
		return GroupStateCold
	case ModelStateLoaded:
		return GroupStateLoaded
	case ModelStateUnknown:
	}
	return GroupStateUnknown
}

func (q *QueueWrapper) swapGraceSeconds() float64 {
	if q.cfg.WarmFirstMaxWait <= 0 {
		return 0
	}
	return q.cfg.SwapGrace.Seconds()
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

// storeMin lowers v to n, treating 0 as unset: a completed wait always takes
// some time, so a real minimum is never 0.
func storeMin(v *atomic.Int64, n int64) {
	if n <= 0 {
		n = 1
	}
	for {
		cur := v.Load()
		if (cur != 0 && n >= cur) || v.CompareAndSwap(cur, n) {
			return
		}
	}
}

// QueueModelStats is one model's slot use on an endpoint. Slots is 0 when the
// backend reported no limit for the model (it is then bounded only by the
// endpoint-wide limit).
type QueueModelStats struct {
	Model string `json:"model"`
	// Group is the slot group the model belongs to; empty when none.
	Group string `json:"group,omitempty"`
	Slots int    `json:"slots"`
	InUse int    `json:"in_use"`
	// External is slots busy with traffic that did not come through this Olla.
	External int `json:"external"`
}

// QueueGroupStats is one slot group's use on an endpoint.
type QueueGroupStats struct {
	Name       string `json:"name"`
	SlotSource string `json:"slot_source"`
	// State is GroupStateLoaded, GroupStateCold or GroupStateUnknown.
	State    string `json:"state"`
	Slots    int    `json:"slots"`
	InUse    int    `json:"in_use"`
	External int    `json:"external"`
}

// QueueEndpointStats is the per-endpoint slot view.
type QueueEndpointStats struct {
	Name       string `json:"name"`
	URL        string `json:"url"`
	SlotSource string `json:"slot_source"`
	// Health is the endpoint's health status. The queue only has the copy it
	// last saw, so the stats handler overwrites it with the live one.
	Health string            `json:"health"`
	Models []QueueModelStats `json:"models"`
	Groups []QueueGroupStats `json:"groups"`
	Slots  int               `json:"slots"`
	InUse  int               `json:"in_use"`
	// External is slots the backend reports busy beyond this Olla's own.
	External int `json:"external"`
	// Down is set while the queue holds the endpoint as offline: reported by
	// the health checker or by a failed connection, until it recovers.
	Down bool `json:"down"`
	// QueueAfterSeconds is the endpoint's queue_after: requests use it only
	// once they have waited this long. 0 = no delay.
	QueueAfterSeconds float64 `json:"queue_after_seconds"`
	// Overflowed counts requests sent to this endpoint as an overflow one.
	Overflowed uint64 `json:"overflowed"`
}

// Slot group states for QueueGroupStats.State.
const (
	GroupStateLoaded  = "loaded"
	GroupStateCold    = "cold"
	GroupStateUnknown = "unknown"
)

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
	// Holding is true while the request waits for its busy pinned endpoint
	// (sticky hold) rather than taking another.
	Holding bool `json:"holding"`
	// Cold is true when none of its candidate endpoints has the model loaded,
	// so running it means a load, perhaps a swap.
	Cold bool `json:"cold"`
	// SwapGrace is true while the only free endpoints would swap out a model
	// whose swap grace is still running.
	SwapGrace bool `json:"swap_grace"`
}

// QueueStats is a point-in-time snapshot of queue activity.
type QueueStats struct {
	Endpoints      []QueueEndpointStats `json:"endpoints"`
	Waiting        []QueueWaiterStats   `json:"waiting"`
	TimeoutSeconds float64              `json:"timeout_seconds"`
	AvgWaitMs      float64              `json:"avg_wait_ms"`
	MaxWaitMs      float64              `json:"max_wait_ms"`
	// MinWaitMs is the shortest wait since boot; 0 until one completes.
	MinWaitMs float64 `json:"min_wait_ms"`
	// WaitTotalMs and WaitsCompleted are monotonic counters, so a poller can
	// derive the mean wait over any window from two samples.
	WaitTotalMs    float64 `json:"wait_total_ms"`
	WaitsCompleted uint64  `json:"waits_completed"`
	// HoldKept and HoldMoved count queued requests with a live sticky pin
	// that got their pinned endpoint, or went elsewhere after the hold.
	HoldKept    uint64  `json:"hold_kept"`
	HoldMoved   uint64  `json:"hold_moved"`
	HoldSeconds float64 `json:"hold_seconds"`
	// Overflowed counts requests sent to an overflow endpoint (queue_after),
	// whether they waited out its delay or it was waived.
	Overflowed uint64 `json:"overflowed"`
	// SwapGraceSeconds is proxy.queue.swap_grace; 0 when off.
	SwapGraceSeconds float64 `json:"swap_grace_seconds"`
	Enqueued         uint64  `json:"enqueued"`
	Dispatched       uint64  `json:"dispatched"`
	TimedOut         uint64  `json:"timed_out"`
	RejectedFull     uint64  `json:"rejected_full"`
	Cancelled        uint64  `json:"cancelled"`
	Failed           uint64  `json:"failed"`
	Depth            int     `json:"depth"`
	PeakDepth        int     `json:"peak_depth"`
	MaxSize          int     `json:"max_size"`
	Enabled          bool    `json:"enabled"`
}

// Stats returns a snapshot of queue counters, per-endpoint slot usage and the
// requests currently waiting, oldest first.
func (q *QueueWrapper) Stats() QueueStats {
	now := time.Now()

	// The lister is called outside q.mu so dispatch never waits on it.
	q.mu.Lock()
	lister := q.listEndpoints
	q.mu.Unlock()
	var configured []*domain.Endpoint
	if lister != nil {
		if all, err := lister(context.Background()); err == nil {
			configured = all
		}
	}

	q.mu.Lock()
	depth := q.waiters.Len()
	// Endpoints the queue has seen keep its copy; configured ones it has not
	// are listed too, so an idle overflow endpoint still shows its slots.
	listed := make(map[string]*domain.Endpoint, len(q.known)+len(configured))
	for _, ep := range configured {
		listed[ep.URLString] = ep
	}
	for url, ep := range q.known {
		listed[url] = ep
	}
	eps := make([]QueueEndpointStats, 0, len(listed))
	names := make(map[string]string, len(listed))
	for url, ep := range listed {
		slots, source := q.resolver.Slots(ep)
		names[url] = ep.Name
		_, isDown := q.down[url]
		eps = append(eps, QueueEndpointStats{
			Health:            ep.Status.String(),
			Down:              isDown,
			Name:              ep.Name,
			URL:               url,
			Slots:             slots,
			SlotSource:        source,
			InUse:             q.inUse[url],
			External:          q.external[url],
			Models:            q.modelStatsLocked(ep),
			Groups:            q.groupStatsLocked(ep),
			QueueAfterSeconds: ep.QueueAfter.Seconds(),
			Overflowed:        q.overflowed[url],
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
			Holding:    q.holdingLocked(w, now),
			Cold:       q.coldLocked(w),
			SwapGrace:  q.graceHeldLocked(w, now),
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
		Enabled:          true,
		Depth:            depth,
		PeakDepth:        int(q.stats.peakDepth.Load()),
		MaxSize:          q.cfg.MaxSize,
		TimeoutSeconds:   q.cfg.Timeout.Seconds(),
		Enqueued:         q.stats.enqueued.Load(),
		Dispatched:       q.stats.dispatched.Load(),
		TimedOut:         q.stats.timedOut.Load(),
		RejectedFull:     q.stats.rejectedFull.Load(),
		Cancelled:        q.stats.cancelled.Load(),
		Failed:           q.stats.failed.Load(),
		AvgWaitMs:        avgMs,
		MaxWaitMs:        float64(q.stats.waitMaxNs.Load()) / float64(time.Millisecond),
		MinWaitMs:        float64(q.stats.waitMinNs.Load()) / float64(time.Millisecond),
		WaitTotalMs:      float64(waitTotalNs) / float64(time.Millisecond),
		WaitsCompleted:   done,
		HoldKept:         q.stats.holdKept.Load(),
		HoldMoved:        q.stats.holdMoved.Load(),
		Overflowed:       q.stats.overflowed.Load(),
		HoldSeconds:      q.holdFor.Seconds(),
		SwapGraceSeconds: q.swapGraceSeconds(),
		Endpoints:        eps,
		Waiting:          waiting,
	}
}
