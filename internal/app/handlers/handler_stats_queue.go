package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/thushan/olla/internal/adapter/balancer"
	"github.com/thushan/olla/internal/core/constants"
)

func (a *Application) queueStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(constants.HeaderContentType, constants.ContentTypeJSON)
	w.WriteHeader(http.StatusOK)

	var stats *balancer.QueueStats
	if a.queueStatsFn != nil {
		stats = a.queueStatsFn()
	}

	var body any = stats
	if stats == nil {
		// Same stable shape as /internal/stats/sticky when off, so callers
		// branch on "enabled" rather than on status codes.
		body = struct {
			Enabled bool `json:"enabled"`
		}{Enabled: false}
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		a.logger.Error("Failed to encode queue stats response", "error", err)
	}
}
