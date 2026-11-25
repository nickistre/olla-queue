// Package reasoning translates reasoning (thinking) controls between the
// request shapes different backends understand. A request may carry any mix
// of the OpenRouter object, OpenAI's reasoning_effort, chat template kwargs,
// backend budget fields or Ollama's think flag; Parse folds them into one
// Intent and Render writes that Intent in the chosen endpoint's dialect.
package reasoning

import (
	"net/url"
	"strings"

	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

// openRouterHost is matched as a suffix so regional or versioned subdomains
// still resolve to the OpenRouter dialect.
const openRouterHost = "openrouter.ai"

// ResolveDialect picks the dialect for an endpoint. An explicit endpoint
// override wins (e.g. llama-swap fronted as openai-compatible), then an
// OpenRouter URL on a generic OpenAI type, then the profile default. Anything
// unknown falls back to passthrough so an unrecognised backend never has its
// body changed.
func ResolveDialect(override, endpointType string, endpointURL *url.URL, profileDefault string) string {
	if override != "" {
		if constants.IsValidReasoningDialect(override) {
			return override
		}
		return constants.ReasoningDialectPassthrough
	}

	if isGenericOpenAIType(endpointType) && endpointURL != nil {
		host := strings.ToLower(endpointURL.Hostname())
		if host == openRouterHost || strings.HasSuffix(host, "."+openRouterHost) {
			return constants.ReasoningDialectOpenRouter
		}
	}

	if profileDefault != "" && constants.IsValidReasoningDialect(profileDefault) {
		return profileDefault
	}
	return constants.ReasoningDialectPassthrough
}

func isGenericOpenAIType(t string) bool {
	return t == domain.ProfileOpenAICompatible || t == "openai"
}

// HonoursExclude reports whether a dialect has a request switch for leaving
// reasoning text out of the response. Callers log when it is lost.
func HonoursExclude(dialect string) bool {
	return dialect == constants.ReasoningDialectOpenRouter || dialect == constants.ReasoningDialectVLLM
}

// AppliesToPath reports whether reasoning translation runs for a request
// path. Only chat and text completions carry these controls; the Responses
// API and everything else is left alone. Suffix matching copes with proxy
// prefixes and endpoint base paths.
func AppliesToPath(path string) bool {
	return isOpenAIPath(path) || isOllamaNativePath(path)
}

func isOpenAIPath(path string) bool {
	return strings.HasSuffix(path, constants.PathV1ChatCompletions) || strings.HasSuffix(path, constants.PathV1Completions)
}

func isOllamaNativePath(path string) bool {
	return strings.HasSuffix(path, "/api/chat") || strings.HasSuffix(path, "/api/generate")
}
