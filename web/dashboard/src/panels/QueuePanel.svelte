<script lang="ts">
  import { queue } from '../lib/stores/queue.svelte';
  import StatTile from '../components/StatTile.svelte';
  import StatusBanner from '../components/StatusBanner.svelte';
  import SortableTable from '../components/SortableTable.svelte';
  import type { Column, SortState } from '../components/SortableTable.svelte';
  import { fmtInt, fmtMs, fmtDuration } from '../lib/format';
  import {
    queueRows,
    fmtClockTime,
    slotFillPct,
    slotLines,
    slotStatusRank,
    type QueueRow,
    type SlotLine,
    type SlotStatus,
  } from '../lib/queue';
  import { getNow as liveNow } from '../lib/clock.svelte';
  import type { QueueEndpointStats, QueueModelStats } from '../lib/types';

  // This panel owns the queue store lifecycle: the waiting list is only
  // fetched while the panel is open.
  $effect(() => {
    queue.start();
    return () => queue.stop();
  });

  const loading = $derived(queue.status === 'loading');
  const data = $derived(queue.data);
  const enabled = $derived(data?.enabled === true);

  // The clock ticks every second, so the Waiting column keeps counting up
  // between polls instead of jumping in 5s steps.
  const now = $derived(liveNow());
  const rows = $derived<QueueRow[]>(queueRows(data?.waiting ?? [], now));
  const oldestMs = $derived(rows.length > 0 ? Math.max(...rows.map((r) => r.waited_ms)) : 0);
  // Overflow endpoints (queue_after), plus any that took requests before a
  // config change dropped their delay. The tile only shows when there are any.
  const overflowEps = $derived((data?.endpoints ?? []).filter((e) => e.queue_after_seconds > 0 || e.overflowed > 0));

  // Waiting requests: position order by default, which is also dispatch
  // order apart from skip-ahead for requests whose endpoints are all busy.
  const waitingColumns: Column[] = [
    { key: 'position', label: '#', sortable: true, num: true, align: 'right' },
    { key: 'client_ip', label: 'Client', sortable: true, sticky: true },
    { key: 'model', label: 'Model', sortable: true },
    {
      key: 'sticky_label',
      label: 'Sticky to',
      sortable: true,
      title: 'Endpoint this request’s sticky session is pinned to; it waits for that endpoint while the pin lasts',
    },
    { key: 'enqueued_ms', label: 'Entered', sortable: true, num: true, align: 'right' },
    { key: 'waited_ms', label: 'Waiting', sortable: true, num: true, align: 'right' },
    {
      key: 'candidates',
      label: 'Candidates',
      sortable: true,
      num: true,
      align: 'right',
      hideNarrow: true,
      title: 'Endpoints that can serve this request; it takes the first one to free a slot',
    },
    { key: 'request_id', label: 'Request ID', sortable: true, hideNarrow: true },
  ];
  const waitingSort: SortState = { key: 'position', dir: 'asc' };

  // One row per slot group, labelled "endpoint (group)"; an endpoint without
  // groups is a single row. Models outside every group get an "ungrouped" row.
  type SlotRow = Omit<SlotLine, never> & {
    endpoint: QueueEndpointStats;
    id: string;
    name: string;
    status_rank: number;
    fill_pct: number;
    models_in_use: number;
  };
  const slotRows = $derived<SlotRow[]>(
    (data?.endpoints ?? []).flatMap((e) =>
      slotLines(e).map((l) => ({
        ...l,
        endpoint: e,
        id: `${e.url}#${l.label}`,
        name: l.label ? `${e.name} (${l.label})` : e.name,
        status_rank: slotStatusRank(l.status),
        fill_pct: slotFillPct(l.in_use + l.external, l.slots),
        models_in_use: l.models.reduce((n, m) => n + m.in_use, 0),
      })),
    ),
  );
  const slotColumns: Column[] = [
    { key: 'name', label: 'Endpoint', sortable: true, sticky: true },
    {
      key: 'status_rank',
      label: 'Status',
      sortable: true,
      title: 'Available: a slot is free · Busy: full, requests wait · Cold: nothing in the group loaded · Offline: failing health checks or a failed connection',
    },
    { key: 'fill_pct', label: 'Slots in use', sortable: true, num: true, align: 'right' },
    {
      key: 'models_in_use',
      label: 'Per model',
      sortable: true,
      num: true,
      align: 'right',
      title: 'Slots each loaded model reports for itself. A model without its own count is limited only by the endpoint',
    },
    {
      key: 'slot_source',
      label: 'Limit from',
      sortable: true,
      title: 'config: max_concurrent on the endpoint · discovered: read from the backend · default: queue.default_slots',
    },
  ];
  const slotSort: SortState = { key: 'name', dir: 'asc' };

  // Outside traffic occupies slots just as ours does, so fullness counts both.
  function fillCls(l: SlotLine): string {
    const taken = l.in_use + l.external;
    if (taken >= l.slots && l.slots > 0) return 'amber';
    return taken > 0 ? 'green' : 'neutral';
  }

  const STATUS_CLS: Record<SlotStatus, string> = {
    Available: 'green',
    Busy: 'amber',
    Cold: 'blue',
    Warming: 'neutral',
    Unknown: 'neutral',
    Offline: 'red',
    'Rate limited': 'red',
    'Config error': 'red',
  };

  // With groups the endpoint-wide total has no line of its own, so it goes in
  // the name's tooltip. It caps the groups only when max_concurrent sets it.
  function endpointTitle(r: SlotRow): string {
    const e = r.endpoint;
    if (!r.label) return e.url;
    const what = r.label === 'ungrouped' ? 'Models outside every slot group, limited only by the endpoint' : `Slot group ${r.label}`;
    const ext = e.external > 0 ? ` +${e.external} other clients` : '';
    const caps = r.label === 'ungrouped' || e.slot_source === 'config' ? '' : ', not a limit on slot groups';
    return `${what}\n${e.url}: ${e.in_use}${ext} of ${e.slots} slots in use endpoint-wide (${e.slot_source}${caps})`;
  }

  function modelTitle(m: QueueModelStats): string {
    const ext = m.external > 0 ? `, ${m.external} by other clients` : '';
    return m.slots > 0
      ? `${m.model}: ${m.in_use} of ${m.slots} slots in use by Olla${ext}`
      : `${m.model}: ${m.in_use} in use by Olla${ext}, no per-model limit reported`;
  }
</script>

<div
  id="panel-queue"
  class="panel is-active"
  role="tabpanel"
  aria-labelledby="tab-queue"
  tabindex="0"
>
  <StatusBanner store={queue} />

  <div
    class="panel-data"
    data-state={queue.status === 'error' || queue.status === 'stale' ? queue.status : null}
  >
  {#if loading}
    <div class="section">
      <div class="tile-grid">
        {#each Array(5) as _, i}<div class="skeleton tile-skel"></div>{/each}
      </div>
    </div>
  {:else if data && !enabled}
    <div class="section">
      <p class="queue-off" role="status">
        The request queue is off. Set <code>proxy.queue.enabled: true</code> to cap in-flight requests per
        endpoint and queue the overflow.
      </p>
    </div>
  {:else if data}
    <div class="section">
      <div class="tile-grid">
        <StatTile label="Waiting now" sub="peak {fmtInt(data.peak_depth)} since boot">
          {#snippet children()}
            {fmtInt(data.depth)}<span class="unit">/ {fmtInt(data.max_size)}</span>
          {/snippet}
        </StatTile>
        <StatTile label="Oldest waiting">
          {#snippet children()}
            {#if rows.length > 0}{fmtMs(oldestMs)}{:else}<span class="dash">—</span>{/if}
          {/snippet}
          {#snippet subSnippet()}times out after {fmtDuration(data.timeout_seconds * 1000)}{/snippet}
        </StatTile>
        <StatTile label="Mean wait" sub="since boot · max {fmtMs(data.max_wait_ms)}">
          {#snippet children()}
            {#if data.waits_completed > 0}{fmtMs(data.avg_wait_ms)}{:else}<span class="dash">—</span>{/if}
          {/snippet}
        </StatTile>
        <StatTile label="Dispatched" value={fmtInt(data.dispatched)}>
          {#snippet subSnippet()}
            of {fmtInt(data.enqueued)} queued{#if data.hold_seconds > 0}{' · '}<span
                title="Requests that held for a busy pinned endpoint: kept it, or moved after {fmtDuration(data.hold_seconds * 1000)}"
                >sticky {fmtInt(data.hold_kept)} kept / {fmtInt(data.hold_moved)} moved</span
              >{/if}
          {/snippet}
        </StatTile>
        {#if overflowEps.length > 0}
          <StatTile label="Overflowed" value={fmtInt(data.overflowed)}>
            {#snippet subSnippet()}
              <span title="Requests sent to an overflow endpoint (queue_after) since boot: after waiting out its delay, or at once when no other endpoint for them was up"
                >{#each overflowEps as e, i (e.url)}{i > 0 ? ' · ' : ''}{e.name}
                  {fmtInt(e.overflowed)}{/each}</span
              >
            {/snippet}
          </StatTile>
        {/if}
        <StatTile label="Turned away" value={fmtInt(data.timed_out + data.rejected_full)}>
          {#snippet subSnippet()}
            {fmtInt(data.timed_out)} timed out · {fmtInt(data.rejected_full)} queue full{#if data.cancelled > 0}{' · '}{fmtInt(data.cancelled)} left{/if}
          {/snippet}
        </StatTile>
      </div>
    </div>

    <div class="section">
      <div class="section-head">
        <h3>Waiting requests</h3>
        <span class="section-note">oldest first · client IP honours trusted proxy headers</span>
      </div>
      {#if rows.length > 0}
        <SortableTable
          columns={waitingColumns}
          {rows}
          initialSort={waitingSort}
          rowId={(row) => row.request_id || `pos-${row.position}`}
        >
          {#snippet rowSnippet({ row: r, cellClass })}
            <td class={cellClass('position')}>{r.position}</td>
            <td class="col-sticky" title={r.user_agent || undefined}>{r.client_ip || '—'}</td>
            <td class="cell-clip" title={r.model}>
              {r.model || '—'}{#if r.cold}{' '}<span class="tag tag-cold" title="Not loaded on any endpoint it can use: running it means a load, perhaps a swap">cold</span>{/if}{#if r.swap_grace}{' '}<span class="tag tag-grace" title="Waiting up to {fmtDuration(data.swap_grace_seconds * 1000)} for the loaded model's next request before swapping it out">grace</span>{/if}
            </td>
            <td>
              {#if r.sticky_label}<span class="pill">{r.sticky_label}</span>{:else}<span class="dash">—</span>{/if}{#if r.holding}{' '}<span class="tag tag-hold" title="Waiting for its pinned endpoint to keep the conversation's KV cache, up to {fmtDuration(data.hold_seconds * 1000)}">holding</span>{/if}
            </td>
            <td class={cellClass('enqueued_ms')}>{fmtClockTime(r.enqueued_at) || '—'}</td>
            <td class={cellClass('waited_ms')}>{fmtMs(r.waited_ms)}</td>
            <td class={cellClass('candidates')}>{fmtInt(r.candidates)}</td>
            <td class="{cellClass('request_id')} cell-clip mono" title={r.request_id}>{r.request_id || '—'}</td>
          {/snippet}
        </SortableTable>
      {:else}
        <p class="queue-empty" role="status">Nothing is waiting. Every request found a free slot.</p>
      {/if}
    </div>

    <div class="section">
      <div class="section-head">
        <h3>Endpoint slots</h3>
        <span class="section-note"><span class="external">+N</span> = busy with other clients' traffic</span>
      </div>
      {#if slotRows.length > 0}
        <SortableTable columns={slotColumns} rows={slotRows} initialSort={slotSort} rowId={(row) => row.id}>
          {#snippet rowSnippet({ row: l, cellClass })}
            <td class="col-sticky" title={endpointTitle(l)}>
              {l.endpoint.name}{#if l.label}{' '}<span class="group-label" class:is-other={l.label === 'ungrouped'}
                  >({l.label})</span
                >{/if}{#if l.endpoint.queue_after_seconds > 0}{' '}<span
                  class="tag tag-after"
                  title="Overflow endpoint: only takes requests that have waited {fmtDuration(
                    l.endpoint.queue_after_seconds * 1000,
                  )}, or any request when no other endpoint for it is up">after {fmtDuration(l.endpoint.queue_after_seconds * 1000)}</span
                >{/if}
            </td>
            <td title={l.reason}>
              <span class="glyph g-{STATUS_CLS[l.status]}" aria-hidden="true">●</span>
              <span class="status status-{STATUS_CLS[l.status]}">{l.status}</span>
            </td>
            <td class={cellClass('fill_pct')}>
              <span class="glyph g-{fillCls(l)}" aria-hidden="true">●</span>
              {fmtInt(l.in_use)}{#if l.external > 0}<span class="external" title="Busy with traffic that did not come through this Olla">+{fmtInt(l.external)}</span>{/if}<span class="unit">/ {fmtInt(l.slots)}</span>
            </td>
            <td class={cellClass('models_in_use')}>
              {#if l.models.length > 0}
                <span class="model-slots">
                  {#each l.models as m (m.model)}
                    <span
                      class="pill model-slot"
                      class:is-full={m.slots > 0 && m.in_use + m.external >= m.slots}
                      title={modelTitle(m)}
                    >{m.model} <strong>{fmtInt(m.in_use)}{#if m.external > 0}<span class="external">+{fmtInt(m.external)}</span>{/if}{#if m.slots > 0}/{fmtInt(m.slots)}{/if}</strong></span>
                  {/each}
                </span>
              {:else}<span class="dash">—</span>{/if}
            </td>
            <td>{l.slot_source || '—'}</td>
          {/snippet}
        </SortableTable>
      {:else}
        <p class="queue-empty" role="status">No endpoints are configured.</p>
      {/if}
    </div>
  {/if}
  </div>
</div>

<style>
  .queue-off,
  .queue-empty {
    color: var(--text-dim);
    font-size: 0.8rem;
    margin: var(--space-2) 0;
  }
  .mono {
    font-size: 0.72rem;
  }
  .model-slots {
    display: inline-flex;
    flex-wrap: wrap;
    gap: 4px;
    justify-content: flex-end;
  }
  .external {
    color: var(--blue);
  }
  .group-label {
    color: var(--text-dim);
  }
  .glyph.g-blue {
    color: var(--blue);
  }
  .group-label.is-other {
    font-style: italic;
  }
  .status-green {
    color: var(--green);
  }
  .status-amber {
    color: var(--amber);
  }
  .status-red {
    color: var(--red);
  }
  .status-blue {
    color: var(--blue);
  }
  .status-neutral {
    color: var(--text-dim);
  }
  .tag {
    font-size: 0.66rem;
    padding: 0 4px;
    border: 1px solid currentColor;
    border-radius: var(--radius-sm);
  }
  .tag-cold {
    color: var(--blue);
  }
  .tag-after {
    color: var(--text-dim);
  }
  .tag-hold,
  .tag-grace {
    color: var(--amber);
  }
  .model-slot.is-full {
    color: var(--amber);
    border-color: var(--amber);
  }
</style>
