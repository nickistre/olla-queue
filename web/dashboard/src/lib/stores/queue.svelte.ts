// /internal/stats/queue poller for the Queue panel. Same 5s cadence as the
// overview, since queued requests move on the order of seconds. Started and
// stopped by QueuePanel so it only polls while the panel is open; the Overview
// tiles and chart read the lighter queue summary on /internal/status instead.
import type { QueueStatsResponse } from '../types';
import { createPollStore } from './poll-store.svelte';
import { parseQueue } from '../runtime';

const QUEUE_INTERVAL_MS = 5000;

export const queue = createPollStore<QueueStatsResponse>({
  name: 'queue',
  url: '/internal/stats/queue',
  intervalMs: QUEUE_INTERVAL_MS,
  parse: parseQueue,
});
