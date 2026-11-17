package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thushan/olla/internal/adapter/balancer"
	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

// TestAllProxies_Queue drives the real queue wrapper through both engines and
// checks the backend never sees more than its slot count, every request still
// succeeds, and the waits surface in X-Olla-Queue-Wait-Ms.
func TestAllProxies_Queue(t *testing.T) {
	for _, suite := range []ProxyTestSuite{SherpaTestSuite{}, OllaTestSuite{}} {
		t.Run(suite.Name(), func(t *testing.T) {
			var inFlight, peak atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := inFlight.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
				inFlight.Add(-1)
				w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer upstream.Close()

			endpoint := createTestEndpoint("slow", upstream.URL, domain.StatusHealthy)
			endpoint.MaxConcurrent = 1

			collector := createTestStatsCollector()
			queue := balancer.NewQueueWrapper(balancer.NewPrioritySelector(collector),
				config.QueueConfig{Enabled: true, MaxSize: 10, Timeout: 5 * time.Second, DefaultSlots: 1}, nil)
			defer queue.Stop()
			proxy := suite.CreateProxy(&mockDiscoveryService{endpoints: []*domain.Endpoint{endpoint}}, queue, suite.CreateConfig(), collector)

			const requests = 4
			var wg sync.WaitGroup
			var waited atomic.Int32
			for i := 0; i < requests; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					req, stats, rlog := createTestRequestWithBody(http.MethodPost, "/v1/chat/completions", `{"model":"m"}`)
					ctx := context.WithValue(req.Context(), constants.ContextQueueOutcomeKey, &domain.QueueOutcome{})
					req = req.WithContext(ctx)
					w := httptest.NewRecorder()
					if err := proxy.ProxyRequestToEndpoints(ctx, w, req, []*domain.Endpoint{endpoint}, stats, rlog); err != nil {
						t.Errorf("request failed: %v", err)
						return
					}
					if w.Code != http.StatusOK {
						t.Errorf("status %d", w.Code)
					}
					if w.Header().Get(constants.HeaderXOllaQueueWaitMs) != "" {
						waited.Add(1)
					}
				}()
			}
			wg.Wait()

			if p := peak.Load(); p != 1 {
				t.Fatalf("backend saw %d concurrent requests, want 1", p)
			}
			if waited.Load() < requests-1 {
				t.Fatalf("only %d responses carried the queue wait header, want >= %d", waited.Load(), requests-1)
			}
			if s := queue.Stats(); s.Depth != 0 || s.Endpoints[0].InUse != 0 {
				t.Fatalf("queue not drained: %+v", s)
			}
		})
	}
}

func TestAllProxies_QueueFullRejects(t *testing.T) {
	for _, suite := range []ProxyTestSuite{SherpaTestSuite{}, OllaTestSuite{}} {
		t.Run(suite.Name(), func(t *testing.T) {
			release := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				<-release
				_, _ = w.Write([]byte(`{}`))
			}))
			defer upstream.Close()

			endpoint := createTestEndpoint("busy", upstream.URL, domain.StatusHealthy)
			collector := createTestStatsCollector()
			queue := balancer.NewQueueWrapper(balancer.NewPrioritySelector(collector),
				config.QueueConfig{Enabled: true, MaxSize: 1, Timeout: 5 * time.Second, DefaultSlots: 1}, nil)
			defer queue.Stop()
			proxy := suite.CreateProxy(&mockDiscoveryService{endpoints: []*domain.Endpoint{endpoint}}, queue, suite.CreateConfig(), collector)

			send := func() error {
				req, stats, rlog := createTestRequestWithBody(http.MethodPost, "/v1/chat/completions", `{}`)
				return proxy.ProxyRequestToEndpoints(req.Context(), httptest.NewRecorder(), req, []*domain.Endpoint{endpoint}, stats, rlog)
			}

			// One request holds the slot, one waits; the third must be turned away.
			var wg sync.WaitGroup
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _ = send() }()
			}
			deadline := time.Now().Add(2 * time.Second)
			for queue.Stats().Depth != 1 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}

			if err := send(); !errors.Is(err, balancer.ErrQueueFull) {
				t.Fatalf("want ErrQueueFull, got %v", err)
			}
			close(release)
			wg.Wait()
		})
	}
}
