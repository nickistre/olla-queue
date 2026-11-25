package anthropic

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransformRequest_ReasoningMapping checks thinking and output_config.effort
// become the OpenAI-side reasoning object the proxy renders per backend.
func TestTransformRequest_ReasoningMapping(t *testing.T) {
	t.Parallel()

	const base = `"model":"claude","max_tokens":100,"messages":[{"role":"user","content":"hi"}]`

	tests := []struct {
		want  map[string]interface{}
		name  string
		extra string
	}{
		{name: "no thinking or effort", extra: ``, want: nil},
		{name: "enabled with budget", extra: `,"thinking":{"type":"enabled","budget_tokens":2048}`,
			want: map[string]interface{}{"enabled": true, "max_tokens": 2048}},
		{name: "enabled without budget", extra: `,"thinking":{"type":"enabled"}`,
			want: map[string]interface{}{"enabled": true}},
		{name: "adaptive", extra: `,"thinking":{"type":"adaptive"}`,
			want: map[string]interface{}{"enabled": true}},
		{name: "disabled", extra: `,"thinking":{"type":"disabled"}`,
			want: map[string]interface{}{"enabled": false}},
		{name: "effort alone", extra: `,"output_config":{"effort":"high"}`,
			want: map[string]interface{}{"effort": "high"}},
		{name: "adaptive with effort", extra: `,"thinking":{"type":"adaptive"},"output_config":{"effort":"max"}`,
			want: map[string]interface{}{"enabled": true, "effort": "max"}},
		{name: "disabled drops effort", extra: `,"thinking":{"type":"disabled"},"output_config":{"effort":"low"}`,
			want: map[string]interface{}{"enabled": false}},
		{name: "unknown thinking type ignored", extra: `,"thinking":{"type":"future"}`, want: nil},
		{name: "non-object thinking ignored", extra: `,"thinking":true`, want: nil},
		{name: "output_config without effort", extra: `,"output_config":{"format":{"type":"json"}}`, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := mustNewTranslator(createTestLogger(), createTestConfig())
			req := &http.Request{Body: io.NopCloser(bytes.NewReader([]byte("{" + base + tt.extra + "}")))}

			result, err := tr.TransformRequest(context.Background(), req)
			require.NoError(t, err)

			got, ok := result.OpenAIRequest["reasoning"]
			if tt.want == nil {
				assert.False(t, ok, "no reasoning object expected, got %v", got)
				return
			}
			require.True(t, ok)
			assert.Equal(t, tt.want, got)
			assert.NotContains(t, result.OpenAIRequest, "thinking")
			assert.NotContains(t, result.OpenAIRequest, "output_config")
		})
	}
}
