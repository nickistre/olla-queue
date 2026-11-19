// Pure view logic for the Queue panel and the Overview queue tiles, kept out
// of the components so the edge cases are unit-testable.
import type { QueueEndpointStats, QueueModelStats, QueueWaiter } from './types';

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

export type SlotStatus =
  | 'Offline'
  | 'Rate limited'
  | 'Config error'
  | 'Unknown'
  | 'Busy'
  | 'Warming'
  | 'Cold'
  | 'Available';

/** One row of the Endpoint slots table: a slot group, the models outside
 *  every group, or (no groups) the whole endpoint. */
export interface SlotLine {
  // '' for an endpoint without groups.
  label: string;
  slots: number;
  in_use: number;
  external: number;
  slot_source: string;
  models: QueueModelStats[];
  status: SlotStatus;
  // Why the status is what it is, for the tooltip.
  reason: string;
}

// Worst first: sorting the Status column puts trouble at the top.
const STATUS_RANK: Record<SlotStatus, number> = {
  Offline: 0,
  'Config error': 1,
  'Rate limited': 2,
  Busy: 3,
  Unknown: 4,
  Warming: 5,
  Cold: 6,
  Available: 7,
};

export function slotStatusRank(s: SlotStatus): number {
  return STATUS_RANK[s];
}

const full = (taken: number, slots: number) => slots > 0 && taken >= slots;

/** Status of one line. The endpoint's health wins over slot use: an offline
 *  endpoint's free slots are no use. A group line is also busy when a
 *  configured endpoint-wide limit is full, since that caps every group on it;
 *  a discovered or default one doesn't cap groups. */
export function slotStatus(
  e: QueueEndpointStats,
  line: Pick<SlotLine, 'label' | 'slots' | 'in_use' | 'external'>,
  groupState = '',
): { status: SlotStatus; reason: string } {
  if (e.down || e.health === 'offline' || e.health === 'unhealthy') {
    return { status: 'Offline', reason: e.down && e.health !== 'offline' ? 'A connection to it failed; waiting for a health check' : 'Health checks are failing' };
  }
  if (e.health === 'config_error') return { status: 'Config error', reason: 'The endpoint is misconfigured' };
  if (e.health === 'rate_limited') return { status: 'Rate limited', reason: 'The backend is rate limiting requests' };
  if (e.health === 'unknown') return { status: 'Unknown', reason: 'Not health checked yet' };
  if (full(line.in_use + line.external, line.slots)) {
    return { status: 'Busy', reason: 'Every slot is taken; new requests wait' };
  }
  if (line.label !== '' && line.label !== 'ungrouped' && e.slot_source === 'config' && full(e.in_use + e.external, e.slots)) {
    return { status: 'Busy', reason: `The endpoint-wide limit of ${e.slots} is reached; new requests wait` };
  }
  if (e.health === 'warming') return { status: 'Warming', reason: 'The backend is starting up' };
  if (groupState === 'cold') {
    return { status: 'Cold', reason: 'No model in this group is loaded; the next request loads one, perhaps by swapping another out' };
  }
  return { status: 'Available', reason: 'A slot is free' };
}

/** The table rows for an endpoint: one per slot group, plus one for any
 *  models outside every group; a single endpoint-wide row without groups. */
export function slotLines(e: QueueEndpointStats): SlotLine[] {
  if (e.groups.length === 0) {
    const line = { label: '', slots: e.slots, in_use: e.in_use, external: e.external };
    return [{ ...line, slot_source: e.slot_source, models: e.models, ...slotStatus(e, line) }];
  }
  const lines: SlotLine[] = e.groups.map((g) => {
    const line = { label: g.name, slots: g.slots, in_use: g.in_use, external: g.external };
    return {
      ...line,
      slot_source: g.slot_source,
      models: e.models.filter((m) => m.group === g.name),
      ...slotStatus(e, line, g.state),
    };
  });
  const loose = e.models.filter((m) => !m.group);
  if (loose.length > 0) {
    // Ungrouped models are limited only by the endpoint, so the row shows
    // the endpoint's figures.
    const line = { label: 'ungrouped', slots: e.slots, in_use: e.in_use, external: e.external };
    lines.push({ ...line, slot_source: e.slot_source, models: loose, ...slotStatus(e, line) });
  }
  return lines;
}
