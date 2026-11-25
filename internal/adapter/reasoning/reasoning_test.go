package reasoning

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thushan/olla/internal/core/constants"
)

func boolPtr(b bool) *bool { return &b }
func intPtr(i int) *int    { return &i }

func TestParse_InputForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want Intent
	}{
		{"reasoning.enabled", `{"reasoning":{"enabled":true}}`, Intent{Enabled: boolPtr(true)}},
		{"reasoning.effort", `{"reasoning":{"effort":"high"}}`, Intent{Enabled: boolPtr(true), Effort: "high"}},
		{"reasoning.effort none", `{"reasoning":{"effort":"none"}}`, Intent{Enabled: boolPtr(false), Effort: "none"}},
		{"reasoning.max_tokens", `{"reasoning":{"max_tokens":2048}}`, Intent{Budget: intPtr(2048)}},
		{"reasoning.exclude", `{"reasoning":{"exclude":true}}`, Intent{Exclude: boolPtr(true)}},
		{"reasoning_effort", `{"reasoning_effort":"low"}`, Intent{Enabled: boolPtr(true), Effort: "low"}},
		{"reasoning_effort none", `{"reasoning_effort":"none"}`, Intent{Enabled: boolPtr(false), Effort: "none"}},
		{"reasoning_effort ultra is max", `{"reasoning_effort":"Ultra"}`, Intent{Enabled: boolPtr(true), Effort: "max"}},
		{"enable_thinking", `{"chat_template_kwargs":{"enable_thinking":false}}`, Intent{Enabled: boolPtr(false), Effort: "none"}},
		{"kwargs thinking", `{"chat_template_kwargs":{"thinking":true}}`, Intent{Enabled: boolPtr(true)}},
		{"kwargs reasoning_effort", `{"chat_template_kwargs":{"reasoning_effort":"medium"}}`, Intent{Enabled: boolPtr(true), Effort: "medium"}},
		{"reasoning_budget_tokens", `{"reasoning_budget_tokens":512}`, Intent{Budget: intPtr(512)}},
		{"reasoning_budget_tokens zero is off", `{"reasoning_budget_tokens":0}`, Intent{Enabled: boolPtr(false), Effort: "none"}},
		{"reasoning_budget_tokens unlimited", `{"reasoning_budget_tokens":-1}`, Intent{}},
		{"thinking_budget_tokens", `{"thinking_budget_tokens":100}`, Intent{Budget: intPtr(100)}},
		{"thinking_token_budget", `{"thinking_token_budget":200}`, Intent{Budget: intPtr(200)}},
		{"thinking_budget", `{"thinking_budget":300}`, Intent{Budget: intPtr(300)}},
		{"include_reasoning false", `{"include_reasoning":false}`, Intent{Exclude: boolPtr(true)}},
		{"think bool", `{"think":true}`, Intent{Enabled: boolPtr(true)}},
		{"think level", `{"think":"high"}`, Intent{Enabled: boolPtr(true), Effort: "high"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, conflicts, present := Parse([]byte(tt.body))
			require.True(t, present)
			assert.Empty(t, conflicts)
			got.fields = nil
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParse_NotPresent(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		`{"model":"m","messages":[{"role":"user","content":"think about reasoning"}]}`,
		`{"reasoning":"high"}`,                // wrong type
		`{"reasoning_effort":5}`,              // wrong type
		`{"reasoning":{"summary":"auto"}}`,    // nothing Olla models
		`{"chat_template_kwargs":{"x":true}}`, // unrelated kwargs
		`[1,2,3]`,
		`not json think`,
	} {
		_, _, present := Parse([]byte(body))
		assert.False(t, present, body)
	}
}

func TestParse_Precedence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		body          string
		wantEffort    string
		wantEnabled   *bool
		wantBudget    *int
		wantExclude   *bool
		wantConflicts []string
	}{
		{
			name:          "object effort beats flat effort",
			body:          `{"reasoning":{"effort":"low"},"reasoning_effort":"high"}`,
			wantEffort:    "low",
			wantEnabled:   boolPtr(true),
			wantConflicts: []string{"reasoning_effort"},
		},
		{
			name:        "agreeing sources are not conflicts",
			body:        `{"reasoning":{"effort":"high"},"reasoning_effort":"high","chat_template_kwargs":{"enable_thinking":true}}`,
			wantEffort:  "high",
			wantEnabled: boolPtr(true),
		},
		{
			name:          "flat effort beats kwargs",
			body:          `{"reasoning_effort":"medium","chat_template_kwargs":{"reasoning_effort":"low"}}`,
			wantEffort:    "medium",
			wantEnabled:   boolPtr(true),
			wantConflicts: []string{"chat_template_kwargs.reasoning_effort"},
		},
		{
			name:          "effort implies on, beating a kwargs off switch",
			body:          `{"reasoning_effort":"high","chat_template_kwargs":{"enable_thinking":false}}`,
			wantEffort:    "high",
			wantEnabled:   boolPtr(true),
			wantConflicts: []string{"chat_template_kwargs.enable_thinking"},
		},
		{
			name:          "disabled with an effort other than none",
			body:          `{"reasoning":{"enabled":false,"effort":"high"}}`,
			wantEffort:    "none",
			wantEnabled:   boolPtr(false),
			wantConflicts: []string{"reasoning.effort"},
		},
		{
			name:          "object off beats flat effort",
			body:          `{"reasoning":{"enabled":false},"reasoning_effort":"high"}`,
			wantEffort:    "none",
			wantEnabled:   boolPtr(false),
			wantConflicts: []string{"reasoning_effort"},
		},
		{
			name:          "explicit on drops an effort of none",
			body:          `{"reasoning":{"enabled":true},"reasoning_effort":"none"}`,
			wantEnabled:   boolPtr(true),
			wantConflicts: []string{"reasoning_effort"},
		},
		{
			name:          "object budget beats backend budget",
			body:          `{"reasoning":{"max_tokens":1000},"reasoning_budget_tokens":50}`,
			wantBudget:    intPtr(1000),
			wantConflicts: []string{"reasoning_budget_tokens"},
		},
		{
			name:          "backend budgets in order",
			body:          `{"reasoning_budget_tokens":10,"thinking_budget":20}`,
			wantBudget:    intPtr(10),
			wantConflicts: []string{"thinking_budget"},
		},
		{
			name:          "object exclude beats include_reasoning",
			body:          `{"reasoning":{"exclude":false},"include_reasoning":false}`,
			wantExclude:   boolPtr(false),
			wantConflicts: []string{"include_reasoning"},
		},
		{
			name:          "kwargs beat think",
			body:          `{"chat_template_kwargs":{"enable_thinking":true},"think":false}`,
			wantEnabled:   boolPtr(true),
			wantConflicts: []string{"think"},
		},
		{
			name:          "budget zero conflicts with an effort",
			body:          `{"reasoning_effort":"low","reasoning_budget_tokens":0}`,
			wantEffort:    "low",
			wantEnabled:   boolPtr(true),
			wantBudget:    intPtr(0),
			wantConflicts: []string{"reasoning_budget_tokens"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, conflicts, present := Parse([]byte(tt.body))
			require.True(t, present)
			assert.Equal(t, tt.wantEffort, got.Effort, "effort")
			assert.Equal(t, tt.wantEnabled, got.Enabled, "enabled")
			assert.Equal(t, tt.wantBudget, got.Budget, "budget")
			assert.Equal(t, tt.wantExclude, got.Exclude, "exclude")

			var fields []string
			for _, c := range conflicts {
				fields = append(fields, c.Field)
			}
			assert.Equal(t, tt.wantConflicts, fields)
		})
	}
}

func TestConflictFields(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "a,b", ConflictFields([]Conflict{{Field: "a"}, {Field: "b"}}))
	assert.Empty(t, ConflictFields(nil))
}

// renderJSON parses body, renders it into dialect and returns the result
// decoded, so golden comparisons ignore key order.
func renderJSON(t *testing.T, body, dialect, path string) map[string]any {
	t.Helper()
	in, _, present := Parse([]byte(body))
	require.True(t, present)
	out, err := Render([]byte(body), in, dialect, path)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(out, &got))
	return got
}

func decode(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(s), &m))
	return m
}

func TestRender_Dialects(t *testing.T) {
	t.Parallel()

	const chat = "/v1/chat/completions"
	const high = `{"model":"m","reasoning_effort":"high"}`
	const off = `{"model":"m","reasoning":{"enabled":false}}`
	const budget = `{"model":"m","reasoning":{"effort":"xhigh","max_tokens":4096,"exclude":true}}`
	const minimal = `{"model":"m","reasoning_effort":"minimal"}`

	tests := []struct {
		name    string
		body    string
		dialect string
		path    string
		want    string
	}{
		// OpenRouter
		{"openrouter effort", high, constants.ReasoningDialectOpenRouter, chat,
			`{"model":"m","reasoning":{"effort":"high","enabled":true}}`},
		{"openrouter off", off, constants.ReasoningDialectOpenRouter, chat,
			`{"model":"m","reasoning":{"enabled":false,"effort":"none"}}`},
		{"openrouter budget beats effort", budget, constants.ReasoningDialectOpenRouter, chat,
			`{"model":"m","reasoning":{"max_tokens":4096,"enabled":true,"exclude":true}}`},
		{"openrouter max is xhigh", `{"reasoning_effort":"max"}`, constants.ReasoningDialectOpenRouter, chat,
			`{"reasoning":{"effort":"xhigh","enabled":true}}`},
		{"openrouter from llama.cpp fields", `{"model":"m","chat_template_kwargs":{"enable_thinking":true},"reasoning_budget_tokens":256}`,
			constants.ReasoningDialectOpenRouter, chat,
			`{"model":"m","reasoning":{"max_tokens":256,"enabled":true}}`},
		{"openrouter keeps unmodelled object keys", `{"reasoning":{"effort":"low","summary":"auto"}}`,
			constants.ReasoningDialectOpenRouter, chat,
			`{"reasoning":{"effort":"low","enabled":true,"summary":"auto"}}`},

		// llama.cpp
		{"llamacpp effort", high, constants.ReasoningDialectLlamaCpp, chat,
			`{"model":"m","reasoning_effort":"high","chat_template_kwargs":{"enable_thinking":true,"thinking":true}}`},
		{"llamacpp off", off, constants.ReasoningDialectLlamaCpp, chat,
			`{"model":"m","chat_template_kwargs":{"enable_thinking":false,"thinking":false},"reasoning_budget_tokens":0}`},
		{"llamacpp clamps", budget, constants.ReasoningDialectLlamaCpp, chat,
			`{"model":"m","reasoning_effort":"high","chat_template_kwargs":{"enable_thinking":true,"thinking":true},"reasoning_budget_tokens":4096}`},
		{"llamacpp minimal is low", minimal, constants.ReasoningDialectLlamaCpp, chat,
			`{"model":"m","reasoning_effort":"low","chat_template_kwargs":{"enable_thinking":true,"thinking":true}}`},

		// vLLM / SGLang
		{"vllm", budget, constants.ReasoningDialectVLLM, chat,
			`{"model":"m","reasoning_effort":"high","chat_template_kwargs":{"enable_thinking":true,"thinking":true},"thinking_token_budget":4096,"include_reasoning":false}`},
		{"vllm off", off, constants.ReasoningDialectVLLM, chat,
			`{"model":"m","chat_template_kwargs":{"enable_thinking":false,"thinking":false}}`},
		{"sglang", budget, constants.ReasoningDialectSGLang, chat,
			`{"model":"m","reasoning_effort":"high","chat_template_kwargs":{"enable_thinking":true,"thinking":true},"thinking_budget":4096}`},

		// Ollama
		{"ollama v1", high, constants.ReasoningDialectOllama, chat,
			`{"model":"m","reasoning_effort":"high"}`},
		{"ollama v1 off", off, constants.ReasoningDialectOllama, chat,
			`{"model":"m","reasoning_effort":"none"}`},
		{"ollama native level", high, constants.ReasoningDialectOllama, "/api/chat",
			`{"model":"m","think":"high"}`},
		{"ollama native off", off, constants.ReasoningDialectOllama, "/api/generate",
			`{"model":"m","think":false}`},
		{"ollama native on", `{"model":"m","chat_template_kwargs":{"enable_thinking":true}}`, constants.ReasoningDialectOllama, "/api/chat",
			`{"model":"m","think":true}`},

		// LM Studio / generic OpenAI
		{"lmstudio clamps", budget, constants.ReasoningDialectLMStudio, chat,
			`{"model":"m","reasoning_effort":"high"}`},
		{"lmstudio off", off, constants.ReasoningDialectLMStudio, chat,
			`{"model":"m","reasoning_effort":"none"}`},
		{"openai keeps level", budget, constants.ReasoningDialectOpenAI, chat,
			`{"model":"m","reasoning_effort":"xhigh"}`},
		{"openai from think", `{"model":"m","think":"low"}`, constants.ReasoningDialectOpenAI, chat,
			`{"model":"m","reasoning_effort":"low"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, decode(t, tt.want), renderJSON(t, tt.body, tt.dialect, tt.path))
		})
	}
}

func TestRender_KeepsUnrelatedKwargs(t *testing.T) {
	t.Parallel()

	body := `{"model":"m","reasoning_effort":"high","chat_template_kwargs":{"enable_thinking":false,"custom":"x"}}`
	got := renderJSON(t, body, constants.ReasoningDialectOpenRouter, "/v1/chat/completions")
	assert.Equal(t, decode(t, `{"model":"m","reasoning":{"effort":"high","enabled":true},"chat_template_kwargs":{"custom":"x"}}`), got)

	// Emptied kwargs are dropped entirely.
	body = `{"model":"m","chat_template_kwargs":{"enable_thinking":true}}`
	got = renderJSON(t, body, constants.ReasoningDialectOpenAI, "/v1/chat/completions")
	assert.Equal(t, decode(t, `{"model":"m"}`), got)
}

func TestRender_PassthroughAndAbsent(t *testing.T) {
	t.Parallel()

	body := []byte(`{"model":"m",  "reasoning_effort":"high"}`)
	in, _, _ := Parse(body)
	out, err := Render(body, in, constants.ReasoningDialectPassthrough, "/v1/chat/completions")
	require.NoError(t, err)
	assert.Equal(t, body, out)

	plain := []byte(`{"model":"m",  "messages":[]}`)
	in, _, present := Parse(plain)
	assert.False(t, present)
	out, err = Render(plain, in, constants.ReasoningDialectLlamaCpp, "/v1/chat/completions")
	require.NoError(t, err)
	assert.Equal(t, plain, out)
}

func TestAppliesToPath(t *testing.T) {
	t.Parallel()

	for path, want := range map[string]bool{
		"/v1/chat/completions":                   true,
		"/olla/proxy/v1/chat/completions":        true,
		"/v1/completions":                        true,
		"/api/chat":                              true,
		"/api/generate":                          true,
		"/v1/responses":                          false,
		"/v1/embeddings":                         false,
		"/v1/messages":                           false,
		"/api/embed":                             false,
		"/engines/llama.cpp/v1/chat/completions": true,
	} {
		assert.Equal(t, want, AppliesToPath(path), path)
	}
}

func TestResolveDialect(t *testing.T) {
	t.Parallel()

	or := &url.URL{Scheme: "https", Host: "openrouter.ai", Path: "/api"}
	orSub := &url.URL{Scheme: "https", Host: "eu.openrouter.ai:443"}
	local := &url.URL{Scheme: "http", Host: "localhost:8080"}

	tests := []struct {
		name, override, typ, profile, want string
		u                                  *url.URL
	}{
		{"override wins", "llamacpp", "openai-compatible", "openai", "llamacpp", or},
		{"invalid override is passthrough", "bogus", "llamacpp", "llamacpp", "passthrough", local},
		{"openrouter host", "", "openai-compatible", "openai", "openrouter", or},
		{"openrouter subdomain on openai alias", "", "openai", "openai", "openrouter", orSub},
		{"openrouter host only for generic types", "", "vllm", "vllm", "vllm", or},
		{"profile default", "", "llamacpp", "llamacpp", "llamacpp", local},
		{"no profile default", "", "auto", "", "passthrough", local},
		{"lookalike host", "", "openai-compatible", "openai", "openai", &url.URL{Host: "notopenrouter.ai"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, ResolveDialect(tt.override, tt.typ, tt.u, tt.profile))
		})
	}
}
