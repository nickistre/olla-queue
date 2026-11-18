package handlers

import (
	"context"
	"slices"

	"github.com/puzpuzpuz/xsync/v4"

	"github.com/thushan/olla/internal/core/constants"
	"github.com/thushan/olla/internal/core/domain"
)

// queueRefresher lets a queued request look its endpoints up again when a
// backend recovers. The lookup runs on a copy of the routing state, so the
// decision already recorded for this request (headers, metrics, alias map)
// stays as it was. An endpoint the request couldn't be sent to as it stands,
// one missing from its alias rewrite map or its per-endpoint path map, is left
// out rather than sent the wrong model name or path.
func (a *Application) queueRefresher(pr *proxyRequest) domain.QueueRefresher {
	resolve := pr.resolveEndpoints
	paths := pr.endpointPaths
	var aliasMap map[string]string
	if pr.profile != nil {
		if raw, ok := pr.profile.InspectionMeta.Load(constants.ContextModelAliasMapKey); ok {
			aliasMap, _ = raw.(map[string]string)
		}
	}

	return func(ctx context.Context) []*domain.Endpoint {
		shadow := *pr
		shadow.profile = cloneRoutingProfile(pr.profile)
		endpoints, err := resolve(ctx, &shadow)
		if err != nil {
			return nil
		}
		usable := endpoints[:0]
		for _, ep := range endpoints {
			url := ep.GetURLString()
			if _, ok := aliasMap[url]; aliasMap != nil && !ok {
				continue
			}
			if _, ok := paths[url]; paths != nil && !ok {
				continue
			}
			usable = append(usable, ep)
		}
		return usable
	}
}

// cloneRoutingProfile copies the parts of a profile that endpoint resolution
// writes to, so a re-run can't change the original.
func cloneRoutingProfile(p *domain.RequestProfile) *domain.RequestProfile {
	if p == nil {
		return nil
	}
	c := *p
	c.SupportedBy = slices.Clone(p.SupportedBy)
	c.ProviderScope = slices.Clone(p.ProviderScope)
	c.InspectionMeta = xsync.NewMap[string, interface{}]()
	if p.InspectionMeta != nil {
		p.InspectionMeta.Range(func(k string, v interface{}) bool {
			c.InspectionMeta.Store(k, v)
			return true
		})
	}
	return &c
}
