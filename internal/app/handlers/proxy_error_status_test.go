package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thushan/olla/internal/adapter/balancer"
	"github.com/thushan/olla/internal/adapter/inspector"
	"github.com/thushan/olla/internal/adapter/proxy/core"
	"github.com/thushan/olla/internal/adapter/translator"
	"github.com/thushan/olla/internal/config"
	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/core/ports"
	"github.com/thushan/olla/internal/logger"
)

func TestProxyErrorStatus(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("endpoint selection failed: %w", err) }
	cases := []struct {
		err        error
		name       string
		retryAfter string
		status     int
	}{
		{name: "queue full", err: wrap(balancer.ErrQueueFull), status: http.StatusServiceUnavailable, retryAfter: "1"},
		{name: "queue timeout", err: wrap(balancer.ErrQueueTimeout), status: http.StatusServiceUnavailable, retryAfter: "1"},
		{name: "queue closed", err: wrap(balancer.ErrQueueClosed), status: http.StatusServiceUnavailable},
		{name: "endpoints gone", err: wrap(balancer.ErrQueueEndpointsGone), status: http.StatusServiceUnavailable},
		{name: "other", err: errors.New("connection refused"), status: http.StatusBadGateway},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			app := &Application{}
			app.handleProxyError(core.NewResponseStartedWriter(rec), tc.err)
			assert.Equal(t, tc.status, rec.Code)
			assert.Equal(t, tc.retryAfter, rec.Header().Get("Retry-After"))
		})
	}
}

// TestTranslatedStreaming_QueueRejectionIsNotAnEmptyStream covers a request
// rejected before any backend wrote a byte. The real Anthropic translator sets
// text/event-stream before writing message_start, so the old Content-Type guard
// let a 200 with an empty stream through instead of the error.
func TestTranslatedStreaming_QueueRejectionIsNotAnEmptyStream(t *testing.T) {
	t.Parallel()

	mockLogger := &mockStyledLogger{}
	trans := &mockTranslator{
		name:                  "anthropic-like",
		implementsErrorWriter: true,
		writeErrorFunc: func(w http.ResponseWriter, err error, statusCode int) {
			w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
			w.WriteHeader(statusCode)
			_ = json.NewEncoder(w).Encode(map[string]any{"type": "error", "error": map[string]any{"message": err.Error()}})
		},
		transformRequestFunc: func(ctx context.Context, r *http.Request) (*translator.TransformedRequest, error) {
			return &translator.TransformedRequest{
				OpenAIRequest: map[string]any{"model": "test-model", "stream": true},
				ModelName:     "test-model",
				IsStreaming:   true,
			}, nil
		},
		transformStreamingFunc: func(ctx context.Context, openaiStream io.Reader, w http.ResponseWriter, original *http.Request) error {
			w.Header().Set(constants.HeaderContentType, "text/event-stream")
			_, _ = io.WriteString(w, "event: message_start\n\n")
			_, err := io.Copy(io.Discard, openaiStream)
			return err
		},
	}

	app := &Application{
		logger: mockLogger,
		proxyService: &mockProxyService{
			proxyFunc: func(ctx context.Context, w http.ResponseWriter, r *http.Request, endpoints []*domain.Endpoint, stats *ports.RequestStats, rlog logger.StyledLogger) error {
				return fmt.Errorf("endpoint selection failed: %w", balancer.ErrQueueFull)
			},
		},
		statsCollector:   &mockStatsCollector{},
		repository:       &mockEndpointRepository{},
		inspectorChain:   inspector.NewChain(mockLogger),
		profileFactory:   &mockProfileFactory{},
		discoveryService: &mockDiscoveryServiceForTranslation{},
		Config:           &config.Config{},
	}

	body, _ := json.Marshal(map[string]any{
		"model": "test-model", "stream": true,
		"messages": []any{map[string]any{"role": "user", "content": "Hello"}},
	})
	req, err := http.NewRequest(http.MethodPost, "/test", bytes.NewReader(body))
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	app.translationHandler(trans).ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	assert.NotContains(t, rec.Body.String(), "message_start")
	assert.Contains(t, rec.Body.String(), "queue full")
}
