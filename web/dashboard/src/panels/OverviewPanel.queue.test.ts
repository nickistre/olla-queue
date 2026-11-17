import { flushSync, mount, unmount } from 'svelte';
import { describe, it, expect, afterEach, beforeEach, vi } from 'vitest';
import { overview } from '../lib/stores/overview.svelte';
import { history } from '../lib/stores/history.svelte';
import OverviewPanel from './OverviewPanel.svelte';
import type { StatusResponse } from '../lib/types';

let component: ReturnType<typeof mount> | undefined;
beforeEach(() => history.reset());
afterEach(() => {
  if (component) unmount(component);
  document.body.innerHTML = '';
  history.reset();
});

function status(queue: StatusResponse['queue'], version: string): StatusResponse {
  return {
    timestamp: new Date().toISOString(),
    proxy: { engine: 'olla', profile: 'auto', balancer: 'priority' },
    endpoints: [],
    security: { status: 'normal', blocked_ips: 0, violations: { rate_limits: 0, size_limits: 0 } },
    system: {
      start_time: '2026-11-17T00:00:00Z',
      status: 'healthy',
      endpoints_up: '1/1',
      success_rate: '100%',
      avg_latency: '10ms',
      total_traffic: '1.00 KB',
      uptime: '1h',
      version,
      commit: 'abc',
      active_connections: 1,
      security_violations: 0,
      total_requests: 10,
      total_failures: 0,
      has_traffic: true,
    },
    ...(queue ? { queue } : {}),
  };
}

async function load(queue: StatusResponse['queue']) {
  const version = `v-${Math.random()}`;
  global.fetch = vi.fn(async (url: RequestInfo | URL) => {
    if (String(url).includes('/internal/status/endpoints')) {
      return { status: 200, ok: true, headers: { get: () => null }, json: async () => ({ endpoints: [] }) };
    }
    return { status: 200, ok: true, headers: { get: () => null }, json: async () => status(queue, version) };
  });
  overview.refresh();
  await vi.waitFor(() => expect(overview.data?.system?.version).toBe(version));
  flushSync();
}

const tileLabels = () => [...document.querySelectorAll('.tile .label')].map((l) => l.textContent?.trim());

describe('Overview queue tiles', () => {
  it('are absent while the queue is off', async () => {
    component = mount(OverviewPanel, { target: document.body });
    await load(undefined);
    expect(tileLabels()).not.toContain('Queued requests');
    expect(tileLabels()).not.toContain('Mean queue wait');
  });

  it('show depth against max size and the mean wait over the chart window', async () => {
    component = mount(OverviewPanel, { target: document.body });
    // Two polls: 3 requests left the queue between them, accruing 4.5s of wait.
    await load({ depth: 1, peak_depth: 2, max_size: 50, wait_total_ms: 500, waits_completed: 1, timed_out: 0, rejected_full: 0 });
    await load({ depth: 3, peak_depth: 4, max_size: 50, wait_total_ms: 5000, waits_completed: 4, timed_out: 1, rejected_full: 1 });

    const tile = (label: string) =>
      [...document.querySelectorAll('.tile')].find((t) => t.querySelector('.label')?.textContent?.trim() === label);
    const queued = tile('Queued requests');
    expect(queued?.querySelector('.value')?.textContent).toContain('3');
    expect(queued?.querySelector('.value')?.textContent).toContain('/ 50');
    expect(queued?.querySelector('.sub')?.textContent).toContain('2 turned away');
    expect(tile('Mean queue wait')?.querySelector('.value')?.textContent?.trim()).toBe('1.5s');
  });
});
