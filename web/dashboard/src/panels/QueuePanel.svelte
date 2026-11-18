<script lang="ts">
  import { queue } from '../lib/stores/queue.svelte';
  import StatTile from '../components/StatTile.svelte';
  import StatusBanner from '../components/StatusBanner.svelte';
  import SortableTable from '../components/SortableTable.svelte';
  import type { Column, SortState } from '../components/SortableTable.svelte';
  import { fmtInt, fmtMs, fmtDuration } from '../lib/format';
  import { queueRows, fmtClockTime, slotFillPct, type QueueRow } from '../lib/queue';
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

  type SlotRow = Omit<QueueEndpointStats, never> & { fill_pct: number; models_in_use: number };
  const slotRows = $derived<SlotRow[]>(
    (data?.endpoints ?? []).map((e) => ({
      ...e,
      fill_pct: slotFillPct(e.in_use, e.slots),
      models_in_use: e.models.reduce((n, m) => n + m.in_use, 0),
    })),
  );
  const slotColumns: Column[] = [
    { key: 'name', label: 'Endpoint', sortable: true, sticky: true },
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
  function fillCls(r: SlotRow): string {
    const taken = r.in_use + r.external;
    if (taken >= r.slots && r.slots > 0) return 'amber';
    return taken > 0 ? 'green' : 'neutral';
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
        <StatTile
          label="Dispatched"
          value={fmtInt(data.dispatched)}
          sub="of {fmtInt(data.enqueued)} queued"
        />
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
            <td class="cell-clip" title={r.model}>{r.model || '—'}</td>
            <td>{#if r.sticky_label}<span class="pill">{r.sticky_label}</span>{:else}<span class="dash">—</span>{/if}</td>
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
        <span class="section-note">endpoints appear once the queue has routed to them · <span class="external">+N</span> = busy with other clients' traffic</span>
      </div>
      {#if slotRows.length > 0}
        <SortableTable columns={slotColumns} rows={slotRows} initialSort={slotSort} rowId={(row) => row.url}>
          {#snippet rowSnippet({ row: e, cellClass })}
            <td class="col-sticky">{e.name}</td>
            <td class={cellClass('fill_pct')}>
              <span class="glyph g-{fillCls(e)}" aria-hidden="true">●</span>
              {fmtInt(e.in_use)}{#if e.external > 0}<span class="external" title="Busy with traffic that did not come through this Olla">+{fmtInt(e.external)}</span>{/if}<span class="unit">/ {fmtInt(e.slots)}</span>
            </td>
            <td class={cellClass('models_in_use')}>
              {#if e.models.length > 0}
                <span class="model-slots">
                  {#each e.models as m (m.model)}
                    <span
                      class="pill model-slot"
                      class:is-full={m.slots > 0 && m.in_use + m.external >= m.slots}
                      title={modelTitle(m)}
                    >{m.model} <strong>{fmtInt(m.in_use)}{#if m.external > 0}<span class="external">+{fmtInt(m.external)}</span>{/if}{#if m.slots > 0}/{fmtInt(m.slots)}{/if}</strong></span>
                  {/each}
                </span>
              {:else}<span class="dash">—</span>{/if}
            </td>
            <td>{e.slot_source || '—'}</td>
          {/snippet}
        </SortableTable>
      {:else}
        <p class="queue-empty" role="status">No requests have been routed through the queue yet.</p>
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
  .model-slot.is-full {
    color: var(--amber);
    border-color: var(--amber);
  }
</style>
