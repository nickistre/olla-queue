// Pure view logic for the Queue panel and the Overview queue tiles, kept out
// of the components so the edge cases are unit-testable.
import type { QueueWaiter } from './types';
import type { Sample } from './stores/history.svelte';

// Omit<..., never> turns the interface into a mapped type, which picks up the
// implicit index signature SortableTable's Row constraint needs (same trick as
// ModelsPanel's ModelRow).
export type QueueRow = Omit<QueueWaiter, never> & {
  // 1-based place in line, in the order the backend lists them (oldest first).
  position: number;
  // Live wait, recomputed from enqueued_at on each clock tick so the column
  // keeps counting between 5s polls. Sort key for the Waiting column.
  waited_ms: number;
  enqueued_ms: number;
  sticky_label: string;
};

function parseIsoMs(iso: string): number | null {
  if (!iso) return null;
  const t = Date.parse(iso);
  return Number.isFinite(t) ? t : null;
}

/** View-model rows for the waiting table. Falls back to the server-side
 *  wait_ms when enqueued_at is missing or unparseable. */
export function queueRows(waiting: QueueWaiter[], nowMs: number): QueueRow[] {
  return waiting.map((w, i) => {
    const at = parseIsoMs(w.enqueued_at);
    return {
      ...w,
      position: i + 1,
      enqueued_ms: at ?? 0,
      waited_ms: at !== null ? Math.max(0, nowMs - at) : w.wait_ms,
      sticky_label: w.sticky_endpoint || '',
    };
  });
}

/** Wall-clock entry time as HH:MM:SS (24h), matching the StatusBanner's
 *  timestamp style. Empty for a missing instant. */
export function fmtClockTime(iso: string): string {
  const t = parseIsoMs(iso);
  if (t === null) return '';
  return new Date(t).toLocaleTimeString('en-AU', { hour12: false });
}

/** Share of slots in use, 0-100. A zero-slot endpoint reads as 0, not NaN. */
export function slotFillPct(inUse: number, slots: number): number {
  if (slots <= 0) return 0;
  return Math.min(100, (Math.max(0, inUse) / slots) * 100);
}

/** Time covered by the non-error samples the chart shows, for the "over the
 *  last N" label on the mean-wait tile. 0 with fewer than two samples. */
export function windowSpanMs(list: Sample[]): number {
  const good = list.filter((s) => !s.error);
  if (good.length < 2) return 0;
  return good[good.length - 1].t - good[0].t;
}
