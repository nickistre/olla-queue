package proxy

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

// bodyRecorder keeps the request body a backend received.
type bodyRecorder struct {
	body []byte
	mu   sync.Mutex
}

func (b *bodyRecorder) record(r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	b.mu.Lock()
	b.body = data
	b.mu.Unlock()
}

func (b *bodyRecorder) decoded(t *testing.T) map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	require.NotEmpty(t, b.body, "backend received no body")
	var m map[string]any
	require.NoError(t, json.Unmarshal(b.body, &m))
	return m
}

// TestAllProxies_ReasoningRerenderedOnRetry sends a request with reasoning
// controls to a llama.cpp backend that resets the connection, so it fails
// over to an OpenRouter endpoint. The retry must be rendered from the
// client's original input in OpenRouter's shape, not from the llama.cpp
// rendering of the first attempt.
func TestAllProxies_ReasoningRerenderedOnRetry(t *testing.T) {
	t.Parallel()

	const clientBody = `{"model":"m","messages":[{"role":"user","content":"hi"}],` +
		`"reasoning_effort":"high","chat_template_kwargs":{"enable_thinking":false,"custom":"keep"}}`

	for _, engine := range []string{DefaultProxySherpa, DefaultProxyOlla} {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()

			var gotA, gotB bodyRecorder
			srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotA.record(r)
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					return
				}
				if tcp, ok := conn.(*net.TCPConn); ok {
					_ = tcp.SetLinger(0)
				}
				_ = conn.Close()
			}))
			t.Cleanup(srvA.Close)
			srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotB.record(r)
				w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			t.Cleanup(srvB.Close)

			epA := createTestEndpoint("llamacpp", srvA.URL, domain.StatusHealthy)
			epA.Type = domain.ProfileLlamaCpp
			epA.ReasoningDialect = constants.ReasoningDialectLlamaCpp
			epB := createTestEndpoint("openrouter", srvB.URL, domain.StatusHealthy)
			epB.Type = domain.ProfileOpenAICompatible
			epB.ReasoningDialect = constants.ReasoningDialectOpenRouter

			discovery := &mockDiscoveryService{endpoints: []*domain.Endpoint{epA, epB}}
			collector := createTestStatsCollector()
			selector := newMockEndpointSelector(collector)
			factory := NewFactory(collector, nil, createTestLogger())
			svc, err := factory.Create(engine, discovery, selector, &Configuration{})
			require.NoError(t, err)

			req, stats, rlog := createTestRequestWithBody(http.MethodPost, "/v1/chat/completions", clientBody)
			w := httptest.NewRecorder()
			require.NoError(t, svc.ProxyRequestToEndpoints(req.Context(), w, req, discovery.endpoints, stats, rlog))

			a := gotA.decoded(t)
			assert.Equal(t, "high", a["reasoning_effort"], "first attempt in llama.cpp shape")
			assert.Equal(t, map[string]any{"enable_thinking": true, "thinking": true, "custom": "keep"}, a["chat_template_kwargs"])
			assert.NotContains(t, a, "reasoning")

			b := gotB.decoded(t)
			assert.Equal(t, map[string]any{"effort": "high", "enabled": true}, b["reasoning"], "retry in OpenRouter shape")
			assert.NotContains(t, b, "reasoning_effort")
			assert.NotContains(t, b, "reasoning_budget_tokens")
			assert.Equal(t, map[string]any{"custom": "keep"}, b["chat_template_kwargs"], "unrelated kwargs survive")
			assert.Equal(t, "m", b["model"])
			assert.NotEmpty(t, b["messages"])

			assert.Equal(t, "chat_template_kwargs.enable_thinking", w.Header().Get(constants.HeaderXOllaReasoningConflict))
		})
	}
}

// TestAllProxies_ReasoningUntouchedWithoutFields checks both engines forward
// a body with no reasoning controls byte for byte.
func TestAllProxies_ReasoningUntouchedWithoutFields(t *testing.T) {
	t.Parallel()

	const clientBody = `{"model": "m",   "messages": [{"role":"user","content":"think it through"}]}`

	for _, engine := range []string{DefaultProxySherpa, DefaultProxyOlla} {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()

			var got bodyRecorder
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got.record(r)
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(srv.Close)

			ep := createTestEndpoint("llamacpp", srv.URL, domain.StatusHealthy)
			ep.ReasoningDialect = constants.ReasoningDialectLlamaCpp

			discovery := &mockDiscoveryService{endpoints: []*domain.Endpoint{ep}}
			collector := createTestStatsCollector()
			factory := NewFactory(collector, nil, createTestLogger())
			svc, err := factory.Create(engine, discovery, newMockEndpointSelector(collector), &Configuration{})
			require.NoError(t, err)

			req, stats, rlog := createTestRequestWithBody(http.MethodPost, "/v1/chat/completions", clientBody)
			require.NoError(t, svc.ProxyRequestToEndpoints(req.Context(), httptest.NewRecorder(), req, discovery.endpoints, stats, rlog))

			got.mu.Lock()
			defer got.mu.Unlock()
			assert.Equal(t, clientBody, string(got.body))
		})
	}
}
