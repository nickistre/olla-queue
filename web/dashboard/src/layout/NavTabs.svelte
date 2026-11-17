<script lang="ts">
  // Tab presentation over the shared navigation store. Panels never import
  // this store directly, only App.svelte's router and this component do -
  // that indirection is what would let a sidebar presenter replace tabs
  // later without any panel change. navigatePanel also pushes a history
  // entry so browser back/forward traverses panel switches.
  import { navigation, type Section } from '../lib/stores/navigation.svelte';
  import { navigatePanel } from '../lib/router';
  import { overview } from '../lib/stores/overview.svelte';

  const LABELS: Record<Section, string> = {
    overview: 'Overview',
    endpoints: 'Endpoints',
    models: 'Models',
    queue: 'Queue',
  };

  // The Queue tab only earns its place while the request queue is on, which
  // /internal/status signals by carrying a queue summary. It also stays while
  // it is the active panel, so a #queue link never leaves the operator on a
  // panel with no selected tab.
  const queueOn = $derived(overview.data?.queue !== undefined);
  const visible = $derived(
    navigation.sections.filter((s) => s !== 'queue' || queueOn || navigation.current === 'queue'),
  );

  function activate(name: Section): void {
    navigatePanel(name);
    // Move focus to the newly-active tab once the DOM has updated.
    queueMicrotask(() => {
      document.getElementById(`tab-${name}`)?.focus();
    });
  }

  function onKeydown(e: KeyboardEvent): void {
    const idx = visible.indexOf(navigation.current);
    let next: number | null = null;
    if (e.key === 'ArrowRight') next = (idx + 1) % visible.length;
    else if (e.key === 'ArrowLeft') next = (idx - 1 + visible.length) % visible.length;
    else if (e.key === 'Home') next = 0;
    else if (e.key === 'End') next = visible.length - 1;
    if (next !== null) {
      e.preventDefault();
      activate(visible[next]);
    }
  }
</script>

<div class="tabs" role="tablist" tabindex="-1" aria-label="Dashboard sections" onkeydown={onKeydown}>
  {#each visible as name (name)}
    <button
      id="tab-{name}"
      class="tab"
      role="tab"
      aria-selected={navigation.current === name ? 'true' : 'false'}
      aria-controls="panel-{name}"
      tabindex={navigation.current === name ? 0 : -1}
      onclick={() => activate(name)}
    >
      {LABELS[name] ?? name}
    </button>
  {/each}
</div>
