package reasoning

import (
	"fmt"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"github.com/thushan/olla/internal/core/constants"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// Render rewrites body so its reasoning controls are in the given dialect.
// The fields Parse recognised are removed first, then the dialect's own are
// written from the Intent. Unrelated chat_template_kwargs keys are kept, and
// the object is dropped if nothing is left in it. Top-level key order is not
// preserved; no backend depends on it.
//
// On error the original body is returned alongside it, so callers can always
// forward what Render gives back.
func Render(body []byte, in Intent, dialect, path string) ([]byte, error) {
	if dialect == "" || dialect == constants.ReasoningDialectPassthrough || len(in.fields) == 0 {
		return body, nil
	}

	var top map[string]jsoniter.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return body, fmt.Errorf("reasoning: decode body: %w", err)
	}
	if top == nil {
		return body, nil
	}

	r := renderer{top: top, in: in}
	r.loadKwargs()
	originalReasoning := top[fieldReasoning]
	r.strip()

	var err error
	switch dialect {
	case constants.ReasoningDialectOpenRouter:
		err = r.openRouter(originalReasoning)
	case constants.ReasoningDialectLlamaCpp:
		err = r.local(fieldLlamaCppBudget, true)
	case constants.ReasoningDialectVLLM:
		err = r.local(fieldVLLMBudget, false)
		if err == nil && in.Exclude != nil && *in.Exclude {
			err = r.set(fieldIncludeReasoning, false)
		}
	case constants.ReasoningDialectSGLang:
		err = r.local(fieldSGLangBudget, false)
	case constants.ReasoningDialectOllama:
		if isOllamaNativePath(path) {
			err = r.ollamaThink()
		} else {
			err = r.effortWithOff(clampLocal)
		}
	case constants.ReasoningDialectLMStudio:
		err = r.effortWithOff(clampLocal)
	case constants.ReasoningDialectOpenAI:
		err = r.effortWithOff(func(s string) string { return s })
	default:
		return body, fmt.Errorf("reasoning: unknown dialect %q", dialect)
	}
	if err != nil {
		return body, err
	}

	if err = r.storeKwargs(); err != nil {
		return body, err
	}
	out, err := json.Marshal(r.top)
	if err != nil {
		return body, fmt.Errorf("reasoning: encode body: %w", err)
	}
	return out, nil
}

type renderer struct {
	top    map[string]jsoniter.RawMessage
	kwargs map[string]jsoniter.RawMessage
	in     Intent
}

// loadKwargs decodes chat_template_kwargs when it is an object, keeping every
// key in it. A null or non-object value is meaningless to a template, so it
// is only replaced if the dialect has switches to write.
func (r *renderer) loadKwargs() {
	raw, ok := r.top[fieldChatTemplateKwargs]
	if !ok {
		return
	}
	if err := json.Unmarshal(raw, &r.kwargs); err != nil {
		r.kwargs = nil
	}
}

func (r *renderer) strip() {
	for _, f := range r.in.fields {
		if key, ok := strings.CutPrefix(f, fieldChatTemplateKwargs+"."); ok {
			delete(r.kwargs, key)
			continue
		}
		delete(r.top, f)
	}
}

func (r *renderer) set(key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("reasoning: encode %s: %w", key, err)
	}
	r.top[key] = raw
	return nil
}

func (r *renderer) setKwarg(key string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("reasoning: encode %s.%s: %w", fieldChatTemplateKwargs, key, err)
	}
	if r.kwargs == nil {
		r.kwargs = make(map[string]jsoniter.RawMessage, 2)
	}
	r.kwargs[key] = raw
	return nil
}

// storeKwargs writes chat_template_kwargs back, dropping it once empty so a
// backend that rejects unknown fields never sees an empty object it didn't
// ask for.
func (r *renderer) storeKwargs() error {
	_, had := r.top[fieldChatTemplateKwargs]
	if r.kwargs == nil {
		return nil
	}
	if len(r.kwargs) == 0 {
		if had {
			delete(r.top, fieldChatTemplateKwargs)
		}
		return nil
	}
	return r.set(fieldChatTemplateKwargs, r.kwargs)
}

func (r *renderer) disabled() bool {
	return r.in.Enabled != nil && !*r.in.Enabled
}

// openRouter writes the reasoning object. OpenRouter takes either max_tokens
// or effort, never both, so a known budget wins. Off is written as
// {enabled:false, effort:"none"}, the shape Hermes Agent sends. Keys of the
// client's own object that Olla doesn't model are carried over.
func (r *renderer) openRouter(original jsoniter.RawMessage) error {
	obj := make(map[string]any, 4)
	if len(original) > 0 {
		var orig map[string]jsoniter.RawMessage
		if err := json.Unmarshal(original, &orig); err == nil {
			for k, v := range orig {
				switch k {
				case keyEnabled, keyEffort, keyMaxTokens, keyExclude:
				default:
					obj[k] = v
				}
			}
		}
	}

	switch {
	case r.disabled():
		obj[keyEnabled] = false
		obj[keyEffort] = EffortNone
	default:
		if r.in.Budget != nil {
			obj[keyMaxTokens] = *r.in.Budget
		} else if r.in.Effort != "" {
			effort := r.in.Effort
			if effort == EffortMax {
				// OpenRouter's top level is xhigh.
				effort = EffortXHigh
			}
			obj[keyEffort] = effort
		}
		if r.in.Enabled != nil {
			obj[keyEnabled] = true
		}
	}
	if r.in.Exclude != nil {
		obj[keyExclude] = *r.in.Exclude
	}

	if len(obj) == 0 {
		delete(r.top, fieldReasoning)
		return nil
	}
	return r.set(fieldReasoning, obj)
}

// local covers llama.cpp, vLLM and SGLang: the client's effort unchanged,
// both template switches (Qwen and GLM read enable_thinking,
// DeepSeek V3.1 and Granite read thinking; Jinja ignores the other) and the
// backend's own budget field. llama.cpp also gets an explicit zero budget
// when off, for builds that predate the template switches.
//
// The effort is not clamped: these servers hand it to the model's chat
// template, and templates disagree on the levels (gpt-oss takes low, medium,
// high; Qwen3.8 takes low, medium, xhigh and rejects high), so any mapping
// breaks some model. The template's own error is clearer than a silent swap.
func (r *renderer) local(budgetField string, zeroBudgetWhenOff bool) error {
	if effort := r.in.Effort; effort != "" && effort != EffortNone && !r.disabled() {
		if err := r.set(fieldReasoningEffort, effort); err != nil {
			return err
		}
	}
	if r.in.Enabled != nil {
		if err := r.setKwarg(keyEnableThinking, *r.in.Enabled); err != nil {
			return err
		}
		if err := r.setKwarg(keyThinking, *r.in.Enabled); err != nil {
			return err
		}
	}
	switch {
	case r.disabled() && zeroBudgetWhenOff:
		return r.set(budgetField, 0)
	case r.in.Budget != nil && !r.disabled():
		return r.set(budgetField, *r.in.Budget)
	}
	return nil
}

// ollamaThink writes Ollama's native think: a level when one is known,
// otherwise a plain on/off.
func (r *renderer) ollamaThink() error {
	switch {
	case r.disabled():
		return r.set(fieldThink, false)
	case clampLocal(r.in.Effort) != "":
		return r.set(fieldThink, clampLocal(r.in.Effort))
	case r.in.Enabled != nil:
		return r.set(fieldThink, true)
	}
	return nil
}

// effortWithOff writes reasoning_effort alone, with "none" for off.
func (r *renderer) effortWithOff(clamp func(string) string) error {
	if r.disabled() {
		return r.set(fieldReasoningEffort, EffortNone)
	}
	if effort := clamp(r.in.Effort); effort != "" && effort != EffortNone {
		return r.set(fieldReasoningEffort, effort)
	}
	return nil
}

// clampLocal maps an effort onto low/medium/high for Ollama and LM Studio,
// whose servers (not the model template) accept only those levels. "none" maps to empty because off
// is expressed by the template switches instead.
func clampLocal(effort string) string {
	switch effort {
	case "", EffortNone:
		return ""
	case EffortMinimal:
		return EffortLow
	case EffortXHigh, EffortMax:
		return EffortHigh
	}
	return effort
}
