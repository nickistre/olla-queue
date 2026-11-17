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

## Slots per endpoint

Slots are resolved per endpoint, in this order:

1. `max_concurrent` on the endpoint in `discovery.static.endpoints`.
2. **Discovered** from the backend when `queue.discover_slots` is true (the default). This applies to `llamacpp`, `openai-compatible` and `openai` endpoints:
    - **llama-swap**: `GET /running` lists the loaded models. Olla then asks each `ready` model for `GET /props?model=<id>&autoload=false` and adds up `total_slots`. `autoload=false` means the probe never loads or swaps a model.
    - **llama.cpp**: `GET /props` gives `total_slots`, which is the server's `--parallel` value.
3. `queue.default_slots` (default `1`).

Discovered counts refresh every `queue.slot_refresh_interval`. A failed probe keeps the last known value. When a refresh finds more slots, waiting requests are dispatched straight away.

!!! note "Per-endpoint, not per-model"
    Slots are counted per endpoint. When llama-swap runs several models at once, their slots are added together into one pool.

For backends that can't be probed (Ollama, vLLM, SGLang and others), set `max_concurrent` to match the backend's own concurrency, for example `OLLAMA_NUM_PARALLEL`.

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
- `GET /internal/stats/queue`: current depth, peak depth, counters (enqueued, dispatched, timed out, rejected, cancelled), average and maximum wait, and per-endpoint `slots`, `in_use` and `slot_source` (`config`, `discovered` or `default`). Returns `{"enabled": false}` when the queue is off.

```bash
curl http://localhost:40114/internal/stats/queue | jq
```

## Configuration

See [Request Queue](../configuration/reference.md#request-queue) in the configuration reference.
