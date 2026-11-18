package handlers

import (
	"context"
	"net/url"
	"testing"

	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

func refreshEndpoint(raw string) *domain.Endpoint {
	u, _ := url.Parse(raw)
	return &domain.Endpoint{Name: raw, URL: u, URLString: raw, Status: domain.StatusHealthy}
}

func TestQueueRefresher_LeavesOriginalProfileAlone(t *testing.T) {
	profile := domain.NewRequestProfile("/v1/chat/completions")
	profile.AddSupportedProfile("llamacpp")
	pr := &proxyRequest{profile: profile}
	pr.resolveEndpoints = func(_ context.Context, p *proxyRequest) ([]*domain.Endpoint, error) {
		p.profile.RoutingDecision = &domain.ModelRoutingDecision{Reason: "rerun"}
		p.profile.ProviderScope = append(p.profile.ProviderScope, "ollama")
		p.profile.SetInspectionMeta("written", true)
		return []*domain.Endpoint{refreshEndpoint("http://a:1")}, nil
	}

	got := (&Application{}).queueRefresher(pr)(context.Background())
	if len(got) != 1 {
		t.Fatalf("got %d endpoints, want 1", len(got))
	}
	if profile.RoutingDecision != nil || len(profile.ProviderScope) != 0 {
		t.Fatalf("original profile changed: %+v", profile)
	}
	if _, ok := profile.InspectionMeta.Load("written"); ok {
		t.Fatal("original inspection meta changed")
	}
}

// An endpoint missing from the request's alias or path map would be sent the
// wrong model name or path, so it is left out.
func TestQueueRefresher_KeepsOnlyEndpointsTheRequestCanUse(t *testing.T) {
	both := []*domain.Endpoint{refreshEndpoint("http://a:1"), refreshEndpoint("http://b:1")}
	resolve := func(context.Context, *proxyRequest) ([]*domain.Endpoint, error) {
		return append([]*domain.Endpoint(nil), both...), nil
	}

	aliased := domain.NewRequestProfile("/v1/chat/completions")
	aliased.SetInspectionMeta(constants.ContextModelAliasMapKey, map[string]string{"http://a:1": "real-model"})
	pathed := &proxyRequest{profile: domain.NewRequestProfile("/v1/messages"), endpointPaths: map[string]string{"http://b:1": "/anthropic/v1/messages"}}

	cases := []struct {
		pr   *proxyRequest
		name string
		want string
	}{
		{name: "alias map", pr: &proxyRequest{profile: aliased}, want: "http://a:1"},
		{name: "path map", pr: pathed, want: "http://b:1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.pr.resolveEndpoints = resolve
			got := (&Application{}).queueRefresher(tc.pr)(context.Background())
			if len(got) != 1 || got[0].URLString != tc.want {
				t.Fatalf("got %v, want only %s", got, tc.want)
			}
		})
	}

	plain := &proxyRequest{profile: domain.NewRequestProfile("/v1/chat/completions"), resolveEndpoints: resolve}
	if got := (&Application{}).queueRefresher(plain)(context.Background()); len(got) != 2 {
		t.Fatalf("no maps: got %d endpoints, want 2", len(got))
	}
}
