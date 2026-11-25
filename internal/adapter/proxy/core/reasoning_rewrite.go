package core

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/thushan/olla/internal/adapter/reasoning"
	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/logger"
)

// RewriteReasoningForEndpoint rewrites the reasoning controls in the request
// body into the dialect of the endpoint this attempt is going to. It runs per
// attempt: the retry handler rewinds the body to the client's original bytes
// first, so a failover from llama.cpp to OpenRouter renders afresh from what
// the client sent rather than from the llama.cpp rendering.
//
// path is the backend path the request is sent to; only chat and text
// completions (and Ollama's native chat/generate) are rewritten. The body is
// left byte-identical when the endpoint is passthrough, the path doesn't
// apply, or the body has no reasoning field.
//
// Conflicting inputs are resolved by precedence, never rejected: the losing
// fields go in X-Olla-Reasoning-Conflict, which must be set here because the
// response headers are committed once the backend answers.
func RewriteReasoningForEndpoint(r *http.Request, w http.ResponseWriter, endpoint *domain.Endpoint, path, requestID string, log logger.StyledLogger) {
	// A previous attempt may have set the header for a dialect this one
	// doesn't use. Its value also tells a retry it has already warned.
	var warned string
	if w != nil {
		warned = w.Header().Get(constants.HeaderXOllaReasoningConflict)
		w.Header().Del(constants.HeaderXOllaReasoningConflict)
	}

	if endpoint == nil || r.Body == nil || r.Body == http.NoBody {
		return
	}
	dialect := endpoint.ReasoningDialect
	if dialect == "" || dialect == constants.ReasoningDialectPassthrough || !reasoning.AppliesToPath(path) {
		return
	}

	body, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		// Forward whatever was read; the backend reports the truncation.
		r.Body = io.NopCloser(bytes.NewReader(body))
		return
	}
	// Restore the original bytes up front so every early return forwards them.
	r.Body = io.NopCloser(bytes.NewReader(body))

	intent, conflicts, present := reasoning.Parse(body)
	if !present {
		return
	}

	rendered, err := reasoning.Render(body, intent, dialect, path)
	if err != nil {
		if log != nil {
			log.Debug("Reasoning translation skipped", "request_id", requestID, "endpoint", endpoint.Name, "error", err)
		}
		return
	}

	if len(conflicts) > 0 && w != nil {
		fields := reasoning.ConflictFields(conflicts)
		w.Header().Set(constants.HeaderXOllaReasoningConflict, fields)
		if log != nil && fields != warned {
			for _, c := range conflicts {
				log.Warn("Conflicting reasoning fields, lower-precedence value ignored",
					"request_id", requestID,
					"setting", c.Setting,
					"ignored", c.Field,
					"used", c.Winner)
			}
		}
	}

	if log != nil {
		if intent.Exclude != nil && *intent.Exclude && !reasoning.HonoursExclude(dialect) {
			log.Debug("Reasoning exclude has no equivalent on this backend, reasoning text will be returned",
				"request_id", requestID, "endpoint", endpoint.Name, "dialect", dialect)
		}
		// Only a real change is worth an info line; a client already speaking
		// the backend's dialect renders back to the same bytes.
		if !bytes.Equal(rendered, body) {
			out, _, _ := reasoning.Parse(rendered)
			log.Info("Reasoning parameters translated",
				"request_id", requestID,
				"endpoint", endpoint.Name,
				"from", strings.Join(intent.Formats(), ","),
				"to", dialect,
				"from_fields", strings.Join(intent.Inputs(), ","),
				"to_fields", strings.Join(out.Inputs(), ","))
		}
	}

	r.Body = io.NopCloser(bytes.NewReader(rendered))
	r.ContentLength = int64(len(rendered))
}
