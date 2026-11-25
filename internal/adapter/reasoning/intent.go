package reasoning

import (
	"bytes"
	"math"
	"strings"

	"github.com/tidwall/gjson"
)

// Canonical effort levels. Anything else a client sends is kept as-is so a
// backend with its own levels still receives them.
const (
	EffortNone    = "none"
	EffortMinimal = "minimal"
	EffortLow     = "low"
	EffortMedium  = "medium"
	EffortHigh    = "high"
	EffortXHigh   = "xhigh"
	EffortMax     = "max"
)

// Setting names used in Conflict.Setting.
const (
	SettingEnabled = "enabled"
	SettingEffort  = "effort"
	SettingBudget  = "budget"
	SettingExclude = "exclude"
)

// Input field names. Dotted names live inside an object.
const (
	fieldReasoning          = "reasoning"
	fieldReasoningEffort    = "reasoning_effort"
	fieldChatTemplateKwargs = "chat_template_kwargs"
	fieldIncludeReasoning   = "include_reasoning"
	fieldThink              = "think"

	keyEnabled        = "enabled"
	keyEffort         = "effort"
	keyMaxTokens      = "max_tokens"
	keyExclude        = "exclude"
	keyEnableThinking = "enable_thinking"
	keyThinking       = "thinking"

	fieldLlamaCppBudget = "reasoning_budget_tokens"
	fieldThinkingBudget = "thinking_budget_tokens"
	fieldVLLMBudget     = "thinking_token_budget"
	fieldSGLangBudget   = "thinking_budget"
)

// budgetFields are the backend-specific budget spellings, in precedence order.
var budgetFields = []string{fieldLlamaCppBudget, fieldThinkingBudget, fieldVLLMBudget, fieldSGLangBudget}

// Intent is the reasoning behaviour a client asked for, independent of the
// field names it used. Nil and empty mean "not specified": Render only writes
// what the client actually set, so backend defaults still apply otherwise.
type Intent struct {
	Enabled *bool
	Budget  *int
	Exclude *bool
	Effort  string
	// fields records every input field Parse understood, so Render strips
	// exactly those and leaves anything it didn't recognise in place.
	fields []string
}

// Conflict records an input field whose value lost to a higher-precedence one.
type Conflict struct {
	Field   string // the field that was ignored
	Winner  string // the field whose value was used
	Setting string
}

type candidate[T comparable] struct {
	val   T
	field string
}

type collector struct {
	effort  []candidate[string]
	enabled []candidate[bool]
	budget  []candidate[int]
	exclude []candidate[bool]
	fields  []string
}

// addEffort records an effort and the on/off state it implies: "none" means
// off, any other level means on. The implied state sits at the effort's own
// precedence, so an explicit flag in the same or a higher tier still wins.
func (c *collector) addEffort(field string, raw gjson.Result) bool {
	if raw.Type != gjson.String {
		return false
	}
	effort := normaliseEffort(raw.Str)
	if effort == "" {
		return false
	}
	c.effort = append(c.effort, candidate[string]{val: effort, field: field})
	c.enabled = append(c.enabled, candidate[bool]{val: effort != EffortNone, field: field})
	return true
}

func (c *collector) addBool(list *[]candidate[bool], field string, raw gjson.Result, invert bool) bool {
	if raw.Type != gjson.True && raw.Type != gjson.False {
		return false
	}
	v := raw.Bool()
	if invert {
		v = !v
	}
	*list = append(*list, candidate[bool]{val: v, field: field})
	return true
}

// addBudget records a token budget. A zero budget also implies thinking is
// off, which is what llama.cpp and vLLM do with it. A negative budget means
// "unlimited" on llama.cpp; that is the default everywhere, so the field is
// understood (and stripped) but sets nothing.
func (c *collector) addBudget(field string, raw gjson.Result) bool {
	if raw.Type != gjson.Number || raw.Num != math.Trunc(raw.Num) || math.Abs(raw.Num) > math.MaxInt32 {
		return false
	}
	n := int(raw.Num)
	if n >= 0 {
		c.budget = append(c.budget, candidate[int]{val: n, field: field})
	}
	if n == 0 {
		c.enabled = append(c.enabled, candidate[bool]{val: false, field: field})
	}
	return true
}

// Parse reads every reasoning control in a JSON request body and folds them
// into one Intent. present is false when the body carries no reasoning field
// at all, in which case callers must leave the body untouched.
//
// Precedence per setting, first present wins: the reasoning{} object, then
// reasoning_effort, then chat_template_kwargs and the backend budget fields,
// then the legacy include_reasoning and think. Every lower-precedence value
// that disagrees with the winner is reported as a Conflict.
func Parse(body []byte) (in Intent, conflicts []Conflict, present bool) {
	// Every recognised field name contains one of these, so most bodies skip
	// the JSON walk entirely.
	if !bytes.Contains(body, []byte("reason")) && !bytes.Contains(body, []byte("think")) {
		return Intent{}, nil, false
	}
	root := gjson.ParseBytes(body)
	if !root.IsObject() {
		return Intent{}, nil, false
	}

	var c collector
	c.collect(root)

	if len(c.fields) == 0 {
		return Intent{}, nil, false
	}

	in.fields = c.fields
	if v, ok := pick(c.enabled, SettingEnabled, &conflicts); ok {
		in.Enabled = &v
	}
	if v, ok := pick(c.effort, SettingEffort, &conflicts); ok {
		in.Effort = v
	}
	if v, ok := pick(c.budget, SettingBudget, &conflicts); ok {
		in.Budget = &v
	}
	if v, ok := pick(c.exclude, SettingExclude, &conflicts); ok {
		in.Exclude = &v
	}

	// The on/off decision has already absorbed the effort's implied state,
	// so bring the effort into line with it rather than send a contradiction.
	if in.Enabled != nil {
		switch {
		case !*in.Enabled:
			in.Effort = EffortNone
			in.Budget = nil
		case in.Effort == EffortNone:
			in.Effort = ""
		}
	}

	return in, dedupeConflicts(conflicts), true
}

// collect gathers every recognised field from the body, tier by tier, so
// each setting's candidates end up in precedence order.
func (c *collector) collect(root gjson.Result) {
	// Tier 1: the OpenRouter-style object.
	if obj := root.Get(fieldReasoning); obj.IsObject() {
		found := c.addBool(&c.enabled, fieldReasoning+"."+keyEnabled, obj.Get(keyEnabled), false)
		found = c.addEffort(fieldReasoning+"."+keyEffort, obj.Get(keyEffort)) || found
		found = c.addBudget(fieldReasoning+"."+keyMaxTokens, obj.Get(keyMaxTokens)) || found
		found = c.addBool(&c.exclude, fieldReasoning+"."+keyExclude, obj.Get(keyExclude), false) || found
		if found {
			c.fields = append(c.fields, fieldReasoning)
		}
	}

	// Tier 2: OpenAI's flat effort.
	if c.addEffort(fieldReasoningEffort, root.Get(fieldReasoningEffort)) {
		c.fields = append(c.fields, fieldReasoningEffort)
	}

	// Tier 3: chat template kwargs, then budget fields.
	if kw := root.Get(fieldChatTemplateKwargs); kw.IsObject() {
		for _, key := range []string{keyEnableThinking, keyThinking} {
			if c.addBool(&c.enabled, fieldChatTemplateKwargs+"."+key, kw.Get(key), false) {
				c.fields = append(c.fields, fieldChatTemplateKwargs+"."+key)
			}
		}
		if c.addEffort(fieldChatTemplateKwargs+"."+fieldReasoningEffort, kw.Get(fieldReasoningEffort)) {
			c.fields = append(c.fields, fieldChatTemplateKwargs+"."+fieldReasoningEffort)
		}
	}
	for _, f := range budgetFields {
		if c.addBudget(f, root.Get(f)) {
			c.fields = append(c.fields, f)
		}
	}

	// Tier 4: legacy fields.
	if c.addBool(&c.exclude, fieldIncludeReasoning, root.Get(fieldIncludeReasoning), true) {
		c.fields = append(c.fields, fieldIncludeReasoning)
	}
	if think := root.Get(fieldThink); think.Exists() {
		if c.addBool(&c.enabled, fieldThink, think, false) || c.addEffort(fieldThink, think) {
			c.fields = append(c.fields, fieldThink)
		}
	}
}

// pick returns the first candidate's value and reports every later one that
// disagrees with it.
func pick[T comparable](cands []candidate[T], setting string, conflicts *[]Conflict) (T, bool) {
	var zero T
	if len(cands) == 0 {
		return zero, false
	}
	win := cands[0]
	for _, c := range cands[1:] {
		if c.val != win.val {
			*conflicts = append(*conflicts, Conflict{Field: c.field, Winner: win.field, Setting: setting})
		}
	}
	return win.val, true
}

// dedupeConflicts keeps the first conflict per losing field: an effort can
// lose both as an effort and through the on/off state it implies.
func dedupeConflicts(in []Conflict) []Conflict {
	if len(in) < 2 {
		return in
	}
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, c := range in {
		if _, dup := seen[c.Field]; dup {
			continue
		}
		seen[c.Field] = struct{}{}
		out = append(out, c)
	}
	return out
}

// ConflictFields joins the losing field names for the conflict header.
func ConflictFields(conflicts []Conflict) string {
	names := make([]string, len(conflicts))
	for i, c := range conflicts {
		names[i] = c.Field
	}
	return strings.Join(names, ",")
}

func normaliseEffort(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "ultra" {
		return EffortMax
	}
	return s
}
