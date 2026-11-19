import { describe, it, expect } from 'vitest';
import { queueRows, fmtClockTime, slotFillPct, windowSpanMs, slotLines, slotStatus, slotStatusRank } from './queue';
import { windowMeanWaitMs, type Sample } from './stores/history.svelte';
import { parseQueue } from './runtime';
import type { QueueEndpointStats, QueueWaiter } from './types';

const waiter = (o: Partial<QueueWaiter> = {}): QueueWaiter => ({
  enqueued_at: '2026-11-17T10:00:00Z',
  request_id: 'req-1',
  client_ip: '10.0.0.5',
  model: 'qwen3:8b',
  user_agent: 'curl/8',
  sticky_endpoint: '',
  wait_ms: 1234,
  candidates: 2,
  holding: false,
  cold: false,
  swap_grace: false,
  ...o,
});

const sample = (o: Partial<Sample> = {}): Sample => ({
  t: 0,
  totalRequests: 0,
  totalBytes: 0,
  activeConnections: 0,
  queueDepth: 0,
  queueWaitTotalMs: 0,
  queueWaitsCompleted: 0,
  reqPerSec: 0,
  bytesPerSec: 0,
  ...o,
});

describe('queueRows', () => {
  it('numbers rows in backend order and counts the wait live from enqueued_at', () => {
    const at = Date.parse('2026-11-17T10:00:00Z');
    const rows = queueRows(
      [waiter(), waiter({ request_id: 'req-2', enqueued_at: '2026-11-17T10:00:02Z', sticky_endpoint: 'gpu-1' })],
      at + 5000,
    );
    expect(rows.map((r) => r.position)).toEqual([1, 2]);
    expect(rows[0].waited_ms).toBe(5000);
    expect(rows[1].waited_ms).toBe(3000);
    expect(rows[1].sticky_label).toBe('gpu-1');
  });

  it('falls back to the server wait when enqueued_at is unusable', () => {
    const [r] = queueRows([waiter({ enqueued_at: 'not-a-time' })], Date.now());
    expect(r.waited_ms).toBe(1234);
    expect(r.enqueued_ms).toBe(0);
  });

  it('never reports a negative wait when the browser clock is behind', () => {
    const at = Date.parse('2026-11-17T10:00:00Z');
    expect(queueRows([waiter()], at - 2000)[0].waited_ms).toBe(0);
  });
});

describe('queue formatting helpers', () => {
  it('fmtClockTime is empty for a missing instant', () => {
    expect(fmtClockTime('')).toBe('');
    expect(fmtClockTime('2026-11-17T10:00:00Z')).toMatch(/^\d{2}:\d{2}:\d{2}$/);
  });

  it('slotFillPct guards zero slots and caps at 100', () => {
    expect(slotFillPct(1, 0)).toBe(0);
    expect(slotFillPct(1, 4)).toBe(25);
    expect(slotFillPct(9, 4)).toBe(100);
  });

  it('windowSpanMs ignores error samples', () => {
    expect(windowSpanMs([sample({ t: 0 })])).toBe(0);
    expect(windowSpanMs([sample({ t: 0 }), sample({ t: 5000, error: true }), sample({ t: 10000 })])).toBe(10000);
  });
});

describe('windowMeanWaitMs', () => {
  it('divides wait accrued in the window by requests that left in it', () => {
    const list = [
      sample({ queueWaitTotalMs: 1000, queueWaitsCompleted: 2 }),
      sample({ queueWaitTotalMs: 4000, queueWaitsCompleted: 4 }),
      sample({ queueWaitTotalMs: 7000, queueWaitsCompleted: 5 }),
    ];
    // (7000-1000) / (5-2)
    expect(windowMeanWaitMs(list)).toBe(2000);
  });

  it('is null when nothing left the queue in the window', () => {
    expect(windowMeanWaitMs([sample({ queueWaitsCompleted: 3 }), sample({ queueWaitsCompleted: 3 })])).toBeNull();
  });

  it('is null when the queue is off', () => {
    expect(windowMeanWaitMs([sample({ queueDepth: null }), sample({ queueDepth: null })])).toBeNull();
  });

  it('starts a fresh window after a restart instead of going negative', () => {
    const list = [
      sample({ queueWaitTotalMs: 90000, queueWaitsCompleted: 30 }),
      // restart: counters back near zero
      sample({ queueWaitTotalMs: 100, queueWaitsCompleted: 1 }),
      sample({ queueWaitTotalMs: 700, queueWaitsCompleted: 3 }),
    ];
    expect(windowMeanWaitMs(list)).toBe(300);
  });

  it('skips failed polls', () => {
    const list = [
      sample({ queueWaitTotalMs: 0, queueWaitsCompleted: 0 }),
      sample({ error: true, queueDepth: null }),
      sample({ queueWaitTotalMs: 500, queueWaitsCompleted: 1 }),
    ];
    expect(windowMeanWaitMs(list)).toBe(500);
  });
});

describe('parseQueue', () => {
  it('accepts the disabled shape', () => {
    const q = parseQueue({ enabled: false });
    expect(q?.enabled).toBe(false);
    expect(q?.waiting).toEqual([]);
    expect(q?.endpoints).toEqual([]);
  });

  it('parses waiters and endpoints, treating null lists as empty', () => {
    const q = parseQueue({
      enabled: true,
      depth: 1,
      max_size: 10,
      waiting: [{ request_id: 'r', client_ip: '1.2.3.4', model: 'm', wait_ms: 10, candidates: 1 }],
      endpoints: null,
    });
    expect(q?.waiting[0].client_ip).toBe('1.2.3.4');
    expect(parseQueue({ enabled: true, endpoints: [{ name: 'n', slots: 2 }] })?.endpoints[0].models).toEqual([]);
    expect(
      parseQueue({ enabled: true, endpoints: [{ name: 'n', models: [{ model: 'q', slots: 2, in_use: 1 }] }] })
        ?.endpoints[0].models[0],
    ).toEqual({ model: 'q', group: '', slots: 2, in_use: 1, external: 0 });
    expect(q?.waiting[0].sticky_endpoint).toBe('');
    expect(q?.endpoints).toEqual([]);
  });

  it('rejects a malformed payload so the last good snapshot survives', () => {
    expect(parseQueue({ enabled: true, depth: 'three' })).toBeNull();
    expect(parseQueue({ enabled: true, waiting: 'nope' })).toBeNull();
    expect(parseQueue(null)).toBeNull();
  });
});

const endpoint = (o: Partial<QueueEndpointStats> = {}): QueueEndpointStats => ({
  name: 'taki',
  url: 'http://taki:1',
  slot_source: 'discovered',
  slots: 5,
  in_use: 0,
  external: 0,
  health: 'healthy',
  down: false,
  queue_after_seconds: 0,
  models: [],
  groups: [],
  ...o,
});
const line = (o: Partial<{ label: string; slots: number; in_use: number; external: number }> = {}) => ({
  label: '',
  slots: 2,
  in_use: 0,
  external: 0,
  ...o,
});

describe('slotStatus', () => {
  it.each([
    ['down by the queue', endpoint({ down: true }), line(), '', 'Offline'],
    ['offline health', endpoint({ health: 'offline' }), line(), '', 'Offline'],
    ['unhealthy health', endpoint({ health: 'unhealthy' }), line(), '', 'Offline'],
    ['config error', endpoint({ health: 'config_error' }), line(), '', 'Config error'],
    ['rate limited', endpoint({ health: 'rate_limited' }), line(), '', 'Rate limited'],
    ['not checked yet', endpoint({ health: 'unknown' }), line(), '', 'Unknown'],
    ['full with outside traffic', endpoint(), line({ in_use: 1, external: 1 }), '', 'Busy'],
    ['group under a configured endpoint-wide limit that is full', endpoint({ in_use: 5, slot_source: 'config' }), line({ label: 'gpu' }), '', 'Busy'],
    ['group under a discovered endpoint-wide total that is full', endpoint({ in_use: 5 }), line({ label: 'emb' }), 'loaded', 'Available'],
    ['cold group beside a full discovered total', endpoint({ slots: 1, in_use: 1 }), line({ label: 'emb', slots: 1 }), 'cold', 'Cold'],
    ['warming', endpoint({ health: 'warming' }), line(), '', 'Warming'],
    ['cold group', endpoint(), line({ label: 'gpu' }), 'cold', 'Cold'],
    ['room to spare', endpoint({ health: 'busy' }), line({ in_use: 1 }), 'loaded', 'Available'],
  ])('%s', (_name, e, l, state, want) => {
    expect(slotStatus(e, l, state).status).toBe(want);
  });

  it('ranks trouble ahead of availability', () => {
    expect(slotStatusRank('Offline')).toBeLessThan(slotStatusRank('Busy'));
    expect(slotStatusRank('Busy')).toBeLessThan(slotStatusRank('Available'));
  });
});

describe('slotLines', () => {
  it('is one endpoint-wide line without groups', () => {
    const lines = slotLines(endpoint({ in_use: 2, models: [{ model: 'm', group: '', slots: 0, in_use: 2, external: 0 }] }));
    expect(lines).toHaveLength(1);
    expect(lines[0]).toMatchObject({ label: '', slots: 5, in_use: 2, status: 'Available' });
    expect(lines[0].models.map((m) => m.model)).toEqual(['m']);
  });

  it('is one line per group with its models, then the ungrouped ones', () => {
    const lines = slotLines(
      endpoint({
        models: [
          { model: 'nomic', group: 'emb', slots: 4, in_use: 0, external: 0 },
          { model: 'qwen', group: 'gpu', slots: 1, in_use: 1, external: 0 },
          { model: 'tiny', group: '', slots: 0, in_use: 0, external: 0 },
        ],
        groups: [
          { name: 'emb', slot_source: 'discovered', state: 'loaded', slots: 4, in_use: 0, external: 0 },
          { name: 'gpu', slot_source: 'config', state: 'loaded', slots: 1, in_use: 1, external: 0 },
        ],
      }),
    );
    expect(lines.map((l) => [l.label, l.status, l.models.map((m) => m.model)])).toEqual([
      ['emb', 'Available', ['nomic']],
      ['gpu', 'Busy', ['qwen']],
      ['ungrouped', 'Available', ['tiny']],
    ]);
  });
});
