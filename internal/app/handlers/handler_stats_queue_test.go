package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thushan/olla/internal/adapter/balancer"
	"github.com/thushan/olla/internal/core/domain"
)

func TestQueueStatsHandler(t *testing.T) {
	cases := []struct {
		fn          func() *balancer.QueueStats
		name        string
		wantEnabled bool
	}{
		{name: "not wired"},
		{name: "wired but disabled", fn: func() *balancer.QueueStats { return nil }},
		{name: "enabled", wantEnabled: true, fn: func() *balancer.QueueStats {
			return &balancer.QueueStats{Enabled: true, Depth: 3, MaxSize: 10}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := &Application{queueStatsFn: tc.fn}
			w := httptest.NewRecorder()
			app.queueStatsHandler(w, httptest.NewRequest(http.MethodGet, "/internal/stats/queue", nil))

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d", w.Code)
			}
			var got map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got["enabled"] != tc.wantEnabled {
				t.Fatalf("enabled = %v, want %v (%s)", got["enabled"], tc.wantEnabled, w.Body.String())
			}
			if tc.wantEnabled && got["depth"] != float64(3) {
				t.Fatalf("depth missing: %s", w.Body.String())
			}
		})
	}
}

func TestBuildQueueSummary(t *testing.T) {
	if (&Application{}).buildQueueSummary() != nil {
		t.Fatal("no provider: summary must be absent")
	}
	off := &Application{queueStatsFn: func() *balancer.QueueStats { return nil }}
	if off.buildQueueSummary() != nil {
		t.Fatal("queue disabled: summary must be absent")
	}
	on := &Application{queueStatsFn: func() *balancer.QueueStats {
		return &balancer.QueueStats{Enabled: true, Depth: 4, MaxSize: 10, WaitTotalMs: 1500, WaitsCompleted: 3, MinWaitMs: 100, MaxWaitMs: 900}
	}}
	s := on.buildQueueSummary()
	if s == nil || s.Depth != 4 || s.WaitTotalMs != 1500 || s.WaitsCompleted != 3 || s.MinWaitMs != 100 || s.MaxWaitMs != 900 {
		t.Fatalf("summary = %+v", s)
	}
}

func TestHashStatusResponse_QueueChangesETag(t *testing.T) {
	base := StatusResponse{}
	idle := StatusResponse{Queue: &QueueSummary{}}
	busy := StatusResponse{Queue: &QueueSummary{Depth: 2}}
	if hashStatusResponse(&base) == hashStatusResponse(&idle) {
		t.Fatal("queue off and queue idle must not share an ETag")
	}
	if hashStatusResponse(&idle) == hashStatusResponse(&busy) {
		t.Fatal("a change in queue depth must change the ETag")
	}
}

// The queue only has the endpoint copy it last saw; the handler reports the
// repository's live health instead.
func TestQueueStatsHandler_OverlaysLiveHealth(t *testing.T) {
	live := &domain.Endpoint{Name: "a", URLString: "http://a:1", Status: domain.StatusOffline}
	app := &Application{
		repository: &mockStatusEndpointRepository{endpoints: []*domain.Endpoint{live}},
		queueStatsFn: func() *balancer.QueueStats {
			return &balancer.QueueStats{Enabled: true, Endpoints: []balancer.QueueEndpointStats{
				{Name: "a", URL: "http://a:1", Health: "healthy"},
				{Name: "gone", URL: "http://gone:1", Health: "healthy"},
			}}
		},
	}
	w := httptest.NewRecorder()
	app.queueStatsHandler(w, httptest.NewRequest(http.MethodGet, "/internal/stats/queue", nil))

	var got balancer.QueueStats
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Endpoints[0].Health != "offline" {
		t.Fatalf("a health = %q, want offline", got.Endpoints[0].Health)
	}
	if got.Endpoints[1].Health != "healthy" {
		t.Fatalf("unknown endpoint keeps its own health, got %q", got.Endpoints[1].Health)
	}
}
