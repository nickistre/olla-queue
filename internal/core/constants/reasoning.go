package constants

// Reasoning dialects name the request shape a backend reads its reasoning
// (thinking) controls from. The per-attempt rewrite renders one canonical
// intent into the dialect of whichever endpoint the attempt lands on.
const (
	ReasoningDialectOpenRouter  = "openrouter"
	ReasoningDialectLlamaCpp    = "llamacpp"
	ReasoningDialectVLLM        = "vllm"
	ReasoningDialectSGLang      = "sglang"
	ReasoningDialectOllama      = "ollama"
	ReasoningDialectLMStudio    = "lmstudio"
	ReasoningDialectOpenAI      = "openai"
	ReasoningDialectPassthrough = "passthrough"
)

// HeaderXOllaReasoningConflict lists the request fields whose reasoning value
// lost to a higher-precedence field, so clients can see what was ignored.
const HeaderXOllaReasoningConflict = "X-Olla-Reasoning-Conflict"

// IsValidReasoningDialect reports whether name is a known dialect. Config
// validation uses it so a typo fails at startup rather than silently
// disabling translation for the endpoint.
func IsValidReasoningDialect(name string) bool {
	switch name {
	case ReasoningDialectOpenRouter, ReasoningDialectLlamaCpp, ReasoningDialectVLLM,
		ReasoningDialectSGLang, ReasoningDialectOllama, ReasoningDialectLMStudio,
		ReasoningDialectOpenAI, ReasoningDialectPassthrough:
		return true
	}
	return false
}
