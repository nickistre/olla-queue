import { flushSync, mount, unmount } from 'svelte';
import { describe, it, expect, afterEach, vi } from 'vitest';
import { overview } from '../lib/stores/overview.svelte';
import { navigation } from '../lib/stores/navigation.svelte';
import NavTabs from './NavTabs.svelte';

let component: ReturnType<typeof mount> | undefined;
afterEach(() => {
  if (component) unmount(component);
  document.body.innerHTML = '';
  navigation.set('overview');
});

function status(withQueue: boolean, version: string) {
  return {
    timestamp: new Date().toISOString(),
    proxy: { engine: 'olla', profile: 'auto', balancer: 'priority' },
    endpoints: [],
    security: { status: 'normal', blocked_ips: 0, violations: { rate_limits: 0, size_limits: 0 } },
    system: { start_time: new Date().toISOString(), status: 'healthy', endpoints_up: '1/1', version },
    ...(withQueue
      ? { queue: { depth: 0, peak_depth: 0, max_size: 100, wait_total_ms: 0, waits_completed: 0, timed_out: 0, rejected_full: 0 } }
      : {}),
  };
}

async function loadStatus(withQueue: boolean) {
  // A distinct version per call lets the wait key on this exact payload,
  // since the overview store is a module singleton shared across tests.
  const version = `v-${withQueue}-${Math.random()}`;
  global.fetch = vi.fn(async () => ({
    status: 200,
    ok: true,
    headers: { get: () => null },
    json: async () => status(withQueue, version),
  }));
  overview.refresh();
  await vi.waitFor(() => expect(overview.data?.system?.version).toBe(version));
  flushSync();
}

const tabNames = () => [...document.querySelectorAll('[role="tab"]')].map((t) => t.textContent?.trim());

describe('NavTabs queue tab', () => {
  it('hides the Queue tab while the queue is off', async () => {
    component = mount(NavTabs, { target: document.body });
    await loadStatus(false);
    expect(tabNames()).toEqual(['Overview', 'Endpoints', 'Models']);
  });

  it('shows the Queue tab once /internal/status carries a queue summary', async () => {
    component = mount(NavTabs, { target: document.body });
    await loadStatus(true);
    expect(tabNames()).toEqual(['Overview', 'Endpoints', 'Models', 'Queue']);
  });

  it('keeps the Queue tab while it is the active panel, even with the queue off', async () => {
    component = mount(NavTabs, { target: document.body });
    await loadStatus(false);
    navigation.set('queue');
    flushSync();
    expect(tabNames()).toContain('Queue');
  });
});
