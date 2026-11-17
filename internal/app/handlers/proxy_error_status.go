package handlers

import (
	"errors"
	"net/http"

	"github.com/thushan/olla/internal/adapter/balancer"
)

// queueRetryAfterSeconds is the hint sent with queue rejections. The queue drains
// as fast as requests complete, so a short retry is the honest answer.
const queueRetryAfterSeconds = "1"

// proxyErrorStatus maps an engine error to the status the client should see.
// Queue rejections are capacity problems, not bad gateways, so they get 503;
// full and timed-out queues also carry Retry-After because waiting will help.
func proxyErrorStatus(err error) (status int, retryAfter string) {
	switch {
	case errors.Is(err, balancer.ErrQueueFull), errors.Is(err, balancer.ErrQueueTimeout):
		return http.StatusServiceUnavailable, queueRetryAfterSeconds
	case errors.Is(err, balancer.ErrQueueClosed), errors.Is(err, balancer.ErrQueueEndpointsGone):
		return http.StatusServiceUnavailable, ""
	default:
		return http.StatusBadGateway, ""
	}
}

// writeProxyErrorHeaders sets Retry-After when the error warrants it and returns the status.
func writeProxyErrorHeaders(w http.ResponseWriter, err error) int {
	status, retryAfter := proxyErrorStatus(err)
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	return status
}
