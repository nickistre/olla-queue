import { flushSync, mount, unmount } from 'svelte';
import { describe, it, expect, afterEach, vi } from 'vitest';
import { queue } from '../lib/stores/queue.svelte';
import QueuePanel from './QueuePanel.svelte';

let component: ReturnType<typeof mount> | undefined;
afterEach(() => {
  if (component) unmount(component);
  document.body.innerHTML = '';
});

function jsonResponse(body: unknown) {
  return { status: 200, ok: true, headers: { get: () => null }, json: async () => body };
}

async function renderWith(payload: Record<string, unknown>, ready: () => void) {
  global.fetch = vi.fn(async () => jsonResponse(payload));
  component = mount(QueuePanel, { target: document.body });
  // The scheduler is not running under test (App starts it), so tick by hand.
  queue.refresh();
  // The queue store is a module singleton; wait for this payload specifically.
  await vi.waitFor(() => ready());
  flushSync();
}

const busy = {
  enabled: true,
  depth: 2,
  peak_depth: 5,
  max_size: 100,
  timeout_seconds: 60,
  avg_wait_ms: 800,
  max_wait_ms: 2400,
  waits_completed: 9,
  enqueued: 11,
  dispatched: 9,
  timed_out: 1,
  rejected_full: 0,
  cancelled: 0,
  waiting: [
    {
      request_id: 'req-a',
      client_ip: '192.168.1.20',
      model: 'qwen3:8b',
      user_agent: 'claude-cli/2',
      sticky_endpoint: 'gpu-box',
      enqueued_at: new Date(Date.now() - 3000).toISOString(),
      wait_ms: 3000,
      candidates: 2,
    },
    {
      request_id: 'req-b',
      client_ip: '192.168.1.21',
      model: 'llama3.1:8b',
      user_agent: '',
      sticky_endpoint: '',
      enqueued_at: new Date(Date.now() - 1000).toISOString(),
      wait_ms: 1000,
      candidates: 1,
    },
  ],
  endpoints: [
    {
      name: 'gpu-box',
      url: 'http://gpu:8080',
      slot_source: 'discovered',
      slots: 5,
      in_use: 2,
      external: 1,
      models: [
        { model: 'qwen3:8b', slots: 2, in_use: 1, external: 1 },
        { model: 'llama3.1:8b', slots: 3, in_use: 1, external: 0 },
      ],
    },
  ],
};

describe('QueuePanel', () => {
  it('says the queue is off for a disabled payload', async () => {
    await renderWith({ enabled: false }, () => expect(queue.data?.enabled).toBe(false));
    expect(document.body.textContent).toContain('request queue is off');
    expect(document.querySelector('table')).toBeNull();
  });

  it('lists waiting requests with caller, model, sticky pin and entry time', async () => {
    await renderWith(busy, () => expect(queue.data?.waiting?.length).toBe(2));
    const rows = [...document.querySelectorAll('tbody tr')];
    const first = rows[0].textContent ?? '';
    expect(first).toContain('192.168.1.20');
    expect(first).toContain('qwen3:8b');
    expect(first).toContain('gpu-box');
    expect(first).toMatch(/\d{2}:\d{2}:\d{2}/);
    // User agent is surfaced as the client cell's tooltip.
    expect(rows[0].querySelector('[title="claude-cli/2"]')).toBeTruthy();
    // No sticky pin renders the no-data dash, not an empty pill.
    expect(rows[1].querySelector('.pill')).toBeNull();
    expect(document.body.textContent).toContain('Endpoint slots');
    expect(document.body.textContent).toContain('discovered');
  });

  it('shows the empty state when nothing is waiting', async () => {
    await renderWith({ ...busy, depth: 0, waiting: [] }, () => expect(queue.data?.waiting?.length).toBe(0));
    expect(document.body.textContent).toContain('Nothing is waiting');
  });
});

describe('QueuePanel per-model slots', () => {
  it('shows each model against its own limit and flags a full one', async () => {
    await renderWith(busy, () => expect(queue.data?.endpoints?.[0]?.models?.length).toBe(2));
    const pills = [...document.querySelectorAll('.model-slot')];
    const text = pills.map((p) => p.textContent?.replace(/\s+/g, ' ').trim());
    // One of qwen's two slots is ours and one is another client's: full.
    expect(text).toContain('qwen3:8b 1+1/2');
    expect(text).toContain('llama3.1:8b 1/3');
    expect(pills.find((p) => p.textContent?.includes('qwen3:8b'))?.classList.contains('is-full')).toBe(true);
    expect(pills.find((p) => p.textContent?.includes('llama3.1:8b'))?.classList.contains('is-full')).toBe(false);
  });
});

describe('QueuePanel outside traffic', () => {
  it('marks slots held by other clients on the endpoint row', async () => {
    await renderWith(busy, () => expect(queue.data?.endpoints?.[0]?.external).toBe(1));
    const cell = document.querySelector('.external[title]');
    expect(cell?.textContent).toBe('+1');
    expect(cell?.getAttribute('title')).toContain('did not come through this Olla');
    const qwen = [...document.querySelectorAll('.model-slot')].find((p) => p.textContent?.includes('qwen3:8b'));
    expect(qwen?.getAttribute('title')).toContain('1 by other clients');
  });
});
