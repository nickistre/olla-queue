---
title: Request Queue - Olla
description: Cap in-flight requests per backend and queue the overflow until a slot frees.
---

# Request Queue

> :memo: **Default Configuration**
> ```yaml
> proxy:
>   queue:
>     enabled: false
> ```
> The request queue is **opt-in**. Set `enabled: true` under `proxy.queue` to activate it.
>
> **Environment Variables**: `OLLA_PROXY_QUEUE_ENABLED`, `OLLA_PROXY_QUEUE_MAX_SIZE`, `OLLA_PROXY_QUEUE_TIMEOUT`

Without the queue, Olla forwards every request as soon as it arrives. The backend then queues internally, where Olla can't see it. A burst can land on one busy node while another sits idle, and the load balancer has nothing to work with except a connection count.

With the queue on, each endpoint has a number of **slots**. Olla only sends a request to an endpoint with a free slot. When every candidate endpoint is full, the request waits in Olla until a slot frees anywhere it can go, and is then routed to that endpoint.

## How it works

- **Slot reservation**: picking an endpoint and claiming one of its slots happen in a single step, so concurrent requests can never over-commit an endpoint.
- **Release**: the slot is released when the response finishes (or fails, or the client disconnects). Releasing a slot immediately dispatches the oldest waiting request that can use it.
- **FIFO with skip-ahead**: waiting requests are served oldest first. A request whose endpoints are all still full is skipped, so a request for a busy model never holds up one for an idle model.
- **Underlying strategy**: the configured `load_balancer` (and sticky sessions, if enabled) still choose between endpoints. They only ever see endpoints with a free slot.
- **Retries**: each failover attempt reserves and releases its own slot.

Nothing is written to the client while it waits, so streaming and non-streaming requests queue the same way.

## Slots per endpoint and per model

A request is only sent to an endpoint that has room at two levels:

- **The endpoint** is under its own limit, and
- **the model** the request will run is under its limit on that endpoint, when the backend reports one.

Both levels matter when one node runs several models at once (llama-swap, or llama.cpp's router mode). Each model is its own llama-server with its own `--parallel` slots. The node's total says nothing about how many requests any single model can take. Take a node running `qwen` with 2 slots and `llama` with 3: its total is 5, but a third `qwen` request waits even while the node has room.

### Endpoint limit

Resolved in this order:

1. `max_concurrent` on the endpoint in `discovery.static.endpoints`.
2. **Discovered** from the backend when `queue.discover_slots` is true (the default): the total of the slots below.
3. `queue.default_slots` (default `1`).

### Model limits

Model limits are only ever discovered. Discovery applies to `llamacpp`, `openai-compatible` and `openai` endpoints, including ones with `max_concurrent` set:

- **llama-swap**: `GET /running` lists the loaded models. Olla asks each `ready` model for `GET /props?model=<id>&autoload=false` and records its `total_slots`.
- **llama.cpp router mode** (`--models-dir` / `--models-preset`): `GET /models` lists models with a `status`. Each `loaded` one is asked for `/props?model=<id>&autoload=false` the same way.
- **llama.cpp, single model**: `GET /props` gives `total_slots` (the server's `--parallel`). With only one model there is no separate per-model limit.

`autoload=false` means a probe never loads or swaps a model.

On a multi-model node, a model that isn't loaded gets a limit of **1** until the backend reports it ready (the cold-model gate). The first request starts the load. Further requests wait in Olla's queue rather than piling up behind the load inside the backend. The model's real slot count is learnt as soon as it is ready.

On llama-swap, a model starting, finishing its load or stopping triggers re-discovery of that node about half a second after its events settle. Olla's view of what's loaded therefore follows a swap almost immediately, rather than waiting up to `slot_refresh_interval`.

### Slot groups

On one node, some models may share a resource while others don't. On a llama-swap node, for example, GPU models swap in and out of one GPU, while an embedding model runs from system RAM alongside them. The node's total slots then overstate what a GPU model can use. While `qwen3.6` holds the GPU's one slot, a request for `qwen3.8` sees four free embedding slots and would be sent along, forcing a swap under the running request.

`slot_groups` splits an endpoint's models into separate pools, each acting like its own node:

```yaml
discovery:
  static:
    endpoints:
      - name: taki
        url: http://taki:8080
        type: openai-compatible
        slot_groups:
          - name: taki-embeddings
            models: ["nomic-embed-*"]
          - name: taki-gpu
            models: ["*"]          # everything else
            max_concurrent: 1      # optional
```

- A model joins the **first** group with a matching pattern (`*` wildcard, case-insensitive). Models in no group are only bounded by the endpoint and their own model limit.
- A group's limit is its `max_concurrent`, else the slots of its **currently loaded** models, else `default_slots`. Counting only loaded models is what makes a swap group work: with `qwen3.6` (1 slot) loaded, `taki-gpu` has 1 slot, and a `qwen3.8` request waits until `qwen3.6` is idle. Then llama-swap swaps, as intended.
- A request needs room in its endpoint, its group and its model. Traffic from other clients counts against the group too.

The model name is the one the backend serves. If an [alias](model-aliases.md) maps to different names on different endpoints, each endpoint's limit is looked up under its own name.

Discovered counts refresh every `queue.slot_refresh_interval`. A failed probe keeps the last known value. When a model is unloaded, its limit is dropped on the next refresh. If nothing is loaded, the endpoint falls back to `default_slots` rather than 0. When a refresh finds more slots, waiting requests are dispatched straight away.

For backends that can't be probed (Ollama, vLLM, SGLang and others), set `max_concurrent` to match the backend's own concurrency, for example `OLLAMA_NUM_PARALLEL`.

## Choosing what runs next

When a slot frees, or a new request arrives, three rules decide which request runs and where.

### Hold for a busy sticky backend

With [sticky sessions](sticky-sessions.md) on, each conversation is pinned to the backend holding its KV cache. Normally, if that backend is busy and another is free, the request goes to the free one and the pin moves, so the cache is lost and the next turn starts cold. Set `proxy.sticky_sessions.hold_seconds` to make the request wait for its own backend instead:

```yaml
proxy:
  sticky_sessions:
    enabled: true
    hold_seconds: 30   # 0 (default) = move at once
```

- A request waits up to `hold_seconds` for its pinned backend, then takes any free one.
- This applies to new requests as well as queued ones. A new request whose pinned backend is busy queues even if other backends are free.
- A pin that has expired (`idle_ttl_seconds`), or that points at a backend unable to serve this request, causes no hold.
- Looking up the pin doesn't count as sticky traffic and doesn't extend the session.

The Queue tab tags a holding request **holding**. Its Dispatched tile counts how many holds **kept** their backend and how many **moved** after the hold ran out.

### Loaded models first

`proxy.queue.warm_first_max_wait` (default `10s`) reorders the queue so a swap isn't forced while work for the loaded model is waiting. Requests whose model is already loaded on a backend they can use run before requests that would load or swap a model.

- Among requests of the same kind, order is still arrival order.
- A request that has waited longer than `warm_first_max_wait` is served strictly in arrival order, so a request needing a swap can't starve.
- Set it to `0` for strict arrival order.

The Queue tab tags such requests **cold**: no backend they can use has their model loaded.

### Prefer a backend with the model loaded

When several backends have room, the request goes to one with its model already loaded. Next come backends that don't report what they have loaded (Ollama, vLLM and others), and last ones that would have to load the model. The configured `load_balancer` (priority, least connections, round robin) then picks among that set.

## Traffic from other clients

Olla counts its own requests against each limit, but a backend may also be serving clients that never pass through this Olla, such as another Olla instance or a script calling it directly. Without knowing that, Olla sees a slot as free while the backend is busy. The request then queues inside the backend, out of sight, and can run past `response_header_timeout`.

With `queue.track_external_load` on (the default), Olla watches what each discovered backend reports as running. Slots busy beyond Olla's own requests count as taken:

- **llama-swap**: a live subscription to `GET /api/events`. Its in-flight messages give a snapshot on connect, then one update per request start and finish, each naming the model. Olla never polls llama-swap's `/upstream/<model>/slots`, because that path loads a model that isn't running. On a node with an exclusive swap group, that would evict whatever is loaded.
- **llama.cpp**: `GET /slots` every `queue.busy_poll_interval` (default `1s`), counting slots with `is_processing`. In router mode it calls `/slots?model=<id>&autoload=false` per loaded model.

Olla takes the backend's count of running requests and subtracts its own at that moment. A request of Olla's that the backend hasn't registered yet can only make that count too low. A request that just finished can count against it until the next reading, which is milliseconds on llama-swap and at most one poll on llama.cpp. When outside traffic eases off, waiting requests are dispatched straight away.

If a reading isn't available, the outside count is cleared and Olla counts only its own traffic. That covers a dropped llama-swap feed and a llama-server started with `--no-slots`. Olla never blocks on stale data.

The Queue tab shows outside traffic as a blue `+N` next to Olla's own count, per endpoint, slot group and model.

## When a request can't be served

| Situation | Response |
|-----------|----------|
| Queue already holds `max_size` requests | `503` with `Retry-After: 1` |
| Waited longer than `queue.timeout` | `503` with `Retry-After: 1` |
| Every endpoint the request could use went unhealthy while it waited | `503` |
| Olla is shutting down | `503` |
| Client disconnected while waiting | Request is dropped from the queue |

## Observability

- `X-Olla-Queue-Wait-Ms`: a response header present when the request waited. It holds the total wait across retry attempts.
- `GET /internal/stats/queue`: current depth, peak depth, counters (enqueued, dispatched, timed out, rejected, cancelled), average and maximum wait, and per-endpoint `slots`, `in_use` and `slot_source` (`config`, `discovered` or `default`), with a `models` list giving each model's own `slots` and `in_use`. Returns `{"enabled": false}` when the queue is off.

```bash
curl http://localhost:40114/internal/stats/queue | jq
```

`/internal/stats/queue` also lists every waiting request, oldest first, with:
- request ID and client IP
- model and user agent
- the endpoint its sticky session is pinned to, if any
- when it entered the queue

The client IP honours `X-Forwarded-For` / `X-Real-IP` only from `server.rate_limits.trusted_proxy_cidrs` when `trust_proxy_headers` is on. Otherwise it is the connecting address. `/internal/status` carries a small `queue` summary (depth, peak, max size, wait counters) while the queue is on.

### Dashboard

While the queue is on, the dashboard at `/internal/ui/` shows it in three places:
- **Activity chart:** a dashed queue-depth line, with the current depth in the readout.
- **Overview tiles:** "Queued requests" (depth against `max_size`) and "Mean queue wait". The mean covers the chart's window (about 15 minutes), so it reflects current conditions and clears once the queue drains.
- **Queue tab:** the waiting requests and each endpoint's slot usage, including where its limit came from and each model's use against its own limit.

## Configuration

See [Request Queue](../configuration/reference.md#request-queue) in the configuration reference.
