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
  hold_seconds: 30,
  swap_grace_seconds: 3,
  hold_kept: 4,
  hold_moved: 1,
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
      holding: true,
      cold: false,
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
      holding: false,
      cold: true,
      swap_grace: true,
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
      health: 'healthy',
      down: false,
      models: [
        { model: 'qwen3:8b', group: 'gpu', slots: 2, in_use: 1, external: 1 },
        { model: 'llama3.1:8b', slots: 3, in_use: 1, external: 0 },
      ],
      groups: [{ name: 'gpu', slot_source: 'discovered', state: 'loaded', slots: 1, in_use: 1, external: 0 }],
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

// The Taki layout: one line per slot group beside the endpoint name, each with
// its own status and slots.
const taki = {
  ...busy,
  endpoints: [
    {
      name: 'Taki',
      url: 'http://taki:8080',
      slot_source: 'discovered',
      slots: 5,
      in_use: 1,
      external: 0,
      health: 'healthy',
      down: false,
      models: [
        { model: 'nomic-embed-text-v1.5', group: 'taki-embeddings', slots: 4, in_use: 0, external: 0 },
        { model: 'qwen3.6-35b-a3b', group: 'taki-gpu', slots: 1, in_use: 1, external: 0 },
      ],
      groups: [
        { name: 'taki-embeddings', slot_source: 'discovered', state: 'loaded', slots: 4, in_use: 0, external: 0 },
        { name: 'taki-gpu', slot_source: 'config', state: 'loaded', slots: 1, in_use: 1, external: 0 },
      ],
    },
  ],
};

// Each slot table row as its cells' text: [endpoint, status, slots, models, source].
function slotTableRows(): string[][] {
  const table = [...document.querySelectorAll('table')].at(-1)!;
  return [...table.querySelectorAll('tbody tr')].map((row) =>
    [...row.querySelectorAll('td')].map((td) => td.textContent?.replace(/\s+/g, ' ').trim() ?? ''),
  );
}

describe('QueuePanel slot groups', () => {
  it('gives each group its own row, labelled with its endpoint', async () => {
    await renderWith(taki, () => expect(queue.data?.endpoints?.[0]?.name).toBe('Taki'));
    expect(slotTableRows()).toEqual([
      ['Taki (taki-embeddings)', '● Available', '● 0/ 4', 'nomic-embed-text-v1.5 0/4', 'discovered'],
      ['Taki (taki-gpu)', '● Busy', '● 1/ 1', 'qwen3.6-35b-a3b 1/1', 'config'],
    ]);
  });

  it('puts models outside every group on an "ungrouped" row', async () => {
    await renderWith(busy, () => expect(queue.data?.endpoints?.[0]?.groups?.length).toBe(1));
    expect(slotTableRows().map((r) => r.slice(0, 2))).toEqual([
      ['gpu-box (gpu)', '● Busy'],
      ['gpu-box (ungrouped)', '● Available'],
    ]);
  });

  it('shows an endpoint without groups as one row under its own name', async () => {
    const plain = structuredClone(taki);
    plain.endpoints[0].groups = [];
    await renderWith(plain, () => expect(queue.data?.endpoints?.[0]?.groups?.length).toBe(0));
    expect(slotTableRows().map((r) => r.slice(0, 3))).toEqual([['Taki', '● Available', '● 1/ 5']]);
  });

  it('shows every row offline when the endpoint is down', async () => {
    const down = structuredClone(taki);
    down.endpoints[0].down = true;
    await renderWith(down, () => expect(queue.data?.endpoints?.[0]?.down).toBe(true));
    expect(slotTableRows().map((r) => r[1])).toEqual(['● Offline', '● Offline']);
  });

  it('shows a group with nothing loaded as cold', async () => {
    const cold = structuredClone(taki);
    cold.endpoints[0].groups[1] = { ...cold.endpoints[0].groups[1], state: 'cold', in_use: 0 };
    await renderWith(cold, () => expect(queue.data?.endpoints?.[0]?.groups?.[1]?.state).toBe('cold'));
    expect(slotTableRows().map((r) => r[1])).toEqual(['● Available', '● Cold']);
  });
});

describe('QueuePanel overflow endpoints', () => {
  it('tags an endpoint with queue_after', async () => {
    const overflow = { ...taki.endpoints[0], name: 'spill', url: 'http://spill:1', groups: [], models: [], queue_after_seconds: 300 };
    await renderWith({ ...busy, endpoints: [busy.endpoints[0], overflow] }, () => expect(queue.data?.endpoints?.length).toBe(2));
    const tags = [...document.querySelectorAll('.tag-after')];
    expect(tags.map((t) => t.textContent)).toEqual(['after 5m']);
    expect(tags[0].closest('td')?.textContent).toContain('spill');
  });

  it('counts requests sent to overflow endpoints, per endpoint', async () => {
    const spill = { ...taki.endpoints[0], name: 'spill', url: 'http://spill:1', groups: [], models: [], queue_after_seconds: 300, overflowed: 5 };
    const spare = { ...spill, name: 'spare', url: 'http://spare:1', overflowed: 2 };
    await renderWith({ ...busy, overflowed: 7, endpoints: [busy.endpoints[0], spill, spare] }, () =>
      expect(queue.data?.overflowed).toBe(7),
    );
    expect(document.body.textContent?.replace(/\s+/g, ' ')).toContain('Overflowed 7');
    expect(document.body.textContent?.replace(/\s+/g, ' ')).toContain('spill 5 · spare 2');
  });

  it('hides the overflow tile without overflow endpoints', async () => {
    await renderWith(busy, () => expect(queue.data?.endpoints?.length).toBe(1));
    expect(document.body.textContent).not.toContain('Overflowed');
  });
});

describe('QueuePanel stage 2 tags', () => {
  it('tags a holding request and a cold one, and shows hold counters', async () => {
    await renderWith(busy, () => expect(queue.data?.hold_kept).toBe(4));
    const rows = [...document.querySelectorAll('tbody tr')];
    expect(rows[0].querySelector('.tag-hold')?.textContent).toBe('holding');
    expect(rows[0].querySelector('.tag-cold')).toBeNull();
    expect(rows[1].querySelector('.tag-cold')?.textContent).toBe('cold');
    expect(rows[1].querySelector('.tag-hold')).toBeNull();
    expect(rows[1].querySelector('.tag-grace')?.textContent).toBe('grace');
    expect(rows[1].querySelector('.tag-grace')?.getAttribute('title')).toContain('3s');
    expect(rows[0].querySelector('.tag-grace')).toBeNull();
    expect(document.body.textContent).toContain('sticky 4 kept / 1 moved');
  });
});
