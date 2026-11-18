package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	proxyconfig "github.com/thushan/olla/internal/adapter/proxy/config"
	"github.com/thushan/olla/internal/core/domain"
	"github.com/thushan/olla/internal/core/ports"
)

// observingSelector logs failure reports and releases in call order.
type observingSelector struct {
	roundRobinSelector
	events []string
}

func (s *observingSelector) EndpointFailed(ep *domain.Endpoint) {
	s.events = append(s.events, "failed:"+ep.Name)
}

func (s *observingSelector) DecrementConnections(ep *domain.Endpoint) {
	s.events = append(s.events, "release:"+ep.Name)
}

// A queue hands a released slot to the next waiter at once, so it must hear
// about the dead backend first.
func TestExecuteWithRetry_ReportsConnectionFailureBeforeRelease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  func(name string) error
		name string
		want []string
	}{
		{name: "connection error", err: func(string) error { return &connectionResetError{} }, want: []string{"failed:a", "release:a", "release:b"}},
		{name: "circuit breaker open", err: cbOpenErr, want: []string{"release:a", "release:b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newPolicyRetryHandler(t, &testDiscoveryService{}, proxyconfig.RetryPolicy{})
			sel := &observingSelector{}
			proxyFunc := func(_ context.Context, w http.ResponseWriter, _ *http.Request, ep *domain.Endpoint, _ *ports.RequestStats) error {
				if ep.Name == "a" {
					return tt.err(ep.Name)
				}
				w.WriteHeader(http.StatusOK)
				return nil
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/chat", nil)
			err := h.ExecuteWithRetry(context.Background(), httptest.NewRecorder(), req,
				[]*domain.Endpoint{namedEndpoint("a"), namedEndpoint("b")}, sel, &ports.RequestStats{}, proxyFunc)

			assert.NoError(t, err)
			assert.Equal(t, tt.want, sel.events)
		})
	}
}
