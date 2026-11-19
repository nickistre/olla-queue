import { flushSync, mount, unmount } from 'svelte';
import { describe, it, expect, afterEach, beforeEach, vi } from 'vitest';
import { overview } from '../lib/stores/overview.svelte';
import { history } from '../lib/stores/history.svelte';
import OverviewPanel from './OverviewPanel.svelte';
import type { StatusResponse } from '../lib/types';
import { fmtMs } from '../lib/format';

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

const tile = (label: string) =>
  [...document.querySelectorAll('.tile')].find((t) => t.querySelector('.label')?.textContent?.trim() === label);
const tileLabels = () => [...document.querySelectorAll('.tile .label')].map((l) => l.textContent?.trim());

describe('Overview queue tiles', () => {
  it('are absent while the queue is off', async () => {
    component = mount(OverviewPanel, { target: document.body });
    await load(undefined);
    expect(tileLabels()).not.toContain('Queued requests');
    expect(tileLabels()).not.toContain('Queue avg wait');
    expect(tileLabels()).not.toContain('Queue wait range');
  });

  it('show depth against max size, and the avg wait and range since boot', async () => {
    component = mount(OverviewPanel, { target: document.body });
    await load({ depth: 3, peak_depth: 4, max_size: 50, wait_total_ms: 6000, waits_completed: 4, min_wait_ms: 250, max_wait_ms: 3000, timed_out: 1, rejected_full: 1 });

    const queued = tile('Queued requests');
    expect(queued?.querySelector('.value')?.textContent).toContain('3');
    expect(queued?.querySelector('.value')?.textContent).toContain('/ 50');
    expect(queued?.querySelector('.sub')?.textContent).toContain('2 turned away');
    expect(tile('Queue avg wait')?.querySelector('.value')?.textContent?.trim()).toBe('1.5s');
    expect(tile('Queue avg wait')?.querySelector('.sub')?.textContent).toContain('across 4 queued requests');
    expect(tile('Queue wait range')?.querySelector('.value')?.textContent?.trim()).toBe(`${fmtMs(250)}–${fmtMs(3000)}`);
  });

  it('show a dash before anything has queued', async () => {
    component = mount(OverviewPanel, { target: document.body });
    await load({ depth: 0, peak_depth: 0, max_size: 50, wait_total_ms: 0, waits_completed: 0, min_wait_ms: 0, max_wait_ms: 0, timed_out: 0, rejected_full: 0 });
    expect(tile('Queue avg wait')?.querySelector('.value')?.textContent?.trim()).toBe('—');
    expect(tile('Queue wait range')?.querySelector('.value')?.textContent?.trim()).toBe('—');
    expect(tile('Queue avg wait')?.querySelector('.sub')?.textContent).toContain('nothing queued yet');
  });
});
