import { describe, it, expect } from 'vitest';
import { queueRows, fmtClockTime, slotFillPct, windowSpanMs } from './queue';
import { windowMeanWaitMs, type Sample } from './stores/history.svelte';
import { parseQueue } from './runtime';
import type { QueueWaiter } from './types';

const waiter = (o: Partial<QueueWaiter> = {}): QueueWaiter => ({
  enqueued_at: '2026-11-17T10:00:00Z',
  request_id: 'req-1',
  client_ip: '10.0.0.5',
  model: 'qwen3:8b',
  user_agent: 'curl/8',
  sticky_endpoint: '',
  wait_ms: 1234,
  candidates: 2,
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
    expect(q?.waiting[0].sticky_endpoint).toBe('');
    expect(q?.endpoints).toEqual([]);
  });

  it('rejects a malformed payload so the last good snapshot survives', () => {
    expect(parseQueue({ enabled: true, depth: 'three' })).toBeNull();
    expect(parseQueue({ enabled: true, waiting: 'nope' })).toBeNull();
    expect(parseQueue(null)).toBeNull();
  });
});
