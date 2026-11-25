package core

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

func newReasoningRequest(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/olla/proxy/v1/chat/completions", bytes.NewBufferString(body))
	r.Header.Set(constants.HeaderContentType, constants.ContentTypeJSON)
	return r
}

func TestRewriteReasoningForEndpoint_RewritesAndSetsLength(t *testing.T) {
	t.Parallel()

	body := `{"model":"m","reasoning":{"effort":"high"},"messages":[]}`
	r := newReasoningRequest(body)
	w := httptest.NewRecorder()
	ep := &domain.Endpoint{Name: "local", ReasoningDialect: constants.ReasoningDialectLlamaCpp}

	RewriteReasoningForEndpoint(r, w, ep, "/v1/chat/completions", "req-1", nil)

	got, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	assert.Equal(t, int64(len(got)), r.ContentLength, "ContentLength must match the rewritten body")

	var m map[string]any
	require.NoError(t, json.Unmarshal(got, &m))
	assert.Equal(t, "high", m["reasoning_effort"])
	assert.NotContains(t, m, "reasoning")
	assert.Equal(t, map[string]any{"enable_thinking": true, "thinking": true}, m["chat_template_kwargs"])
	assert.Empty(t, w.Header().Get(constants.HeaderXOllaReasoningConflict))
}

func TestRewriteReasoningForEndpoint_ConflictHeader(t *testing.T) {
	t.Parallel()

	body := `{"model":"m","reasoning":{"effort":"low"},"reasoning_effort":"high","chat_template_kwargs":{"reasoning_effort":"medium"}}`
	r := newReasoningRequest(body)
	w := httptest.NewRecorder()
	ep := &domain.Endpoint{Name: "or", ReasoningDialect: constants.ReasoningDialectOpenRouter}

	RewriteReasoningForEndpoint(r, w, ep, "/api/v1/chat/completions", "req-2", nil)

	assert.Equal(t, "reasoning_effort,chat_template_kwargs.reasoning_effort",
		w.Header().Get(constants.HeaderXOllaReasoningConflict))
}

func TestRewriteReasoningForEndpoint_LeavesBodyAlone(t *testing.T) {
	t.Parallel()

	const withReasoning = `{"model":"m",  "reasoning_effort":"high"}`
	tests := []struct {
		name    string
		body    string
		dialect string
		path    string
	}{
		{"no reasoning fields", `{"model":"m",  "messages":[]}`, constants.ReasoningDialectLlamaCpp, "/v1/chat/completions"},
		{"passthrough dialect", withReasoning, constants.ReasoningDialectPassthrough, "/v1/chat/completions"},
		{"unset dialect", withReasoning, "", "/v1/chat/completions"},
		{"embeddings path", withReasoning, constants.ReasoningDialectLlamaCpp, "/v1/embeddings"},
		{"responses path", withReasoning, constants.ReasoningDialectOpenRouter, "/v1/responses"},
		{"invalid json", `{"reasoning_effort":"high"`, constants.ReasoningDialectLlamaCpp, "/v1/chat/completions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := newReasoningRequest(tt.body)
			before := r.ContentLength
			w := httptest.NewRecorder()
			ep := &domain.Endpoint{Name: "e", ReasoningDialect: tt.dialect}

			RewriteReasoningForEndpoint(r, w, ep, tt.path, "req", nil)

			got, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.body, string(got), "body must be byte-identical")
			assert.Equal(t, before, r.ContentLength)
		})
	}
}

// A retry to a passthrough endpoint must not carry the previous attempt's
// conflict header, since nothing was resolved for it.
func TestRewriteReasoningForEndpoint_ClearsStaleHeader(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	w.Header().Set(constants.HeaderXOllaReasoningConflict, "reasoning_effort")
	r := newReasoningRequest(`{"reasoning":{"effort":"low"},"reasoning_effort":"high"}`)

	RewriteReasoningForEndpoint(r, w, &domain.Endpoint{ReasoningDialect: constants.ReasoningDialectPassthrough}, "/v1/chat/completions", "req", nil)

	assert.Empty(t, w.Header().Get(constants.HeaderXOllaReasoningConflict))
}
