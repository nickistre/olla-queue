package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thushan/olla/internal/adapter/balancer"
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
		return &balancer.QueueStats{Enabled: true, Depth: 4, MaxSize: 10, WaitTotalMs: 1500, WaitsCompleted: 3}
	}}
	s := on.buildQueueSummary()
	if s == nil || s.Depth != 4 || s.WaitTotalMs != 1500 || s.WaitsCompleted != 3 {
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
