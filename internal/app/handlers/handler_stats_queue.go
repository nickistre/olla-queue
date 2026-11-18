package handlers

import (
	"context"
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

	if stats != nil {
		a.overlayQueueHealth(r.Context(), stats)
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

// overlayQueueHealth replaces each endpoint's health with the repository's
// live status. The queue reports the copy it last saw, which can be stale.
func (a *Application) overlayQueueHealth(ctx context.Context, stats *balancer.QueueStats) {
	if a.repository == nil {
		return
	}
	all, err := a.repository.GetAll(ctx)
	if err != nil {
		return
	}
	health := make(map[string]string, len(all))
	for _, ep := range all {
		health[ep.URLString] = ep.Status.String()
	}
	for i := range stats.Endpoints {
		if h, ok := health[stats.Endpoints[i].URL]; ok {
			stats.Endpoints[i].Health = h
		}
	}
}
