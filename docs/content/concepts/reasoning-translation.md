---
title: Reasoning Translation - Thinking Controls Across Backends
description: How Olla rewrites reasoning (thinking) parameters into the shape each backend understands, including on retries and overflow to a different backend type.
keywords: olla reasoning, reasoning_effort, enable_thinking, openrouter reasoning, llama.cpp thinking, vllm thinking budget, ollama think
---

# Reasoning Translation

> :memo: **Default Configuration**
> ```yaml
> proxy:
>   reasoning_translation: true
> ```
> On by default. Set `false` to forward reasoning fields exactly as the client sent them.
>
> **Environment Variable**: `OLLA_PROXY_REASONING_TRANSLATION`

Every backend reads its thinking controls from a different place. OpenRouter wants a `reasoning` object, llama.cpp reads `reasoning_effort`, `chat_template_kwargs.enable_thinking` and `reasoning_budget_tokens`, vLLM and SGLang have their own budget fields, and Ollama's native API uses `think`. A request written for one backend is silently ignored by another.

That matters most when a request can land on more than one kind of backend, for example a local llama.cpp fleet with OpenRouter as a [`queue_after` overflow endpoint](request-queue.md#overflow-endpoints). Olla accepts any of these input shapes, works out one intended setting, and writes it in the shape the chosen endpoint reads. This happens on every attempt, so a retry or overflow onto a different backend type is rendered again from the client's original request.

## What the client can set

| Setting | Meaning | Input fields accepted |
|---|---|---|
| Enabled | thinking on or off | `reasoning.enabled`, `reasoning.effort: "none"`, `reasoning_effort: "none"`, `chat_template_kwargs.enable_thinking` / `.thinking`, a zero budget, Ollama `think` (bool) |
| Effort | `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max` (`ultra` is read as `max`) | `reasoning.effort`, `reasoning_effort`, `chat_template_kwargs.reasoning_effort`, Ollama `think: "low"` etc. |
| Budget | token cap on thinking | `reasoning.max_tokens`, `reasoning_budget_tokens`, `thinking_budget_tokens`, `thinking_token_budget` (vLLM), `thinking_budget` (SGLang) |
| Exclude | reason, but leave the reasoning text out of the response | `reasoning.exclude`, `include_reasoning` (inverted) |

An effort of `none` means thinking is off; any other effort means it is on, unless a higher-precedence field says otherwise.

Through the [Anthropic translator](api-translation.md), `thinking` and `output_config.effort` are mapped onto the `reasoning` object: `enabled` with `budget_tokens` sets the budget, `adaptive` means on, `disabled` means off. Anthropic passthrough requests are not touched.

## Precedence and conflicts

For each setting the first field present wins, in this order:

1. the `reasoning{}` object
2. `reasoning_effort`
3. backend-specific fields: `chat_template_kwargs.*` and the budget fields
4. legacy fields: `include_reasoning`, `think`

Requests are never rejected for conflicting values. When a lower-precedence field disagrees with the winner (for example `reasoning_effort: "high"` beside `reasoning.effort: "low"`, or `enable_thinking: false` beside an effort of `high`), Olla logs a warning with the request ID and lists the losing fields in the `X-Olla-Reasoning-Conflict` response header:

```
X-Olla-Reasoning-Conflict: reasoning_effort,chat_template_kwargs.enable_thinking
```

## What each backend receives

Olla removes every reasoning field it recognised, then writes the target dialect's own. Unrelated `chat_template_kwargs` keys are kept; the object is dropped if nothing is left in it.

| Dialect | Writes |
|---|---|
| `openrouter` | `reasoning: {effort \| max_tokens, enabled, exclude}`. Either `max_tokens` (when a budget is known) or `effort`, never both. Off is `{enabled: false, effort: "none"}`. `max` is sent as `xhigh`. |
| `llamacpp` | `reasoning_effort` clamped for gpt-oss templates (`minimal` to `low`, `xhigh`/`max` to `high`), `chat_template_kwargs.enable_thinking` and `.thinking` (Qwen/GLM read the first, DeepSeek V3.1/Granite the second), and `reasoning_budget_tokens` (0 when off). |
| `vllm` | clamped `reasoning_effort`, `chat_template_kwargs.enable_thinking`/`.thinking`, `thinking_token_budget`, and `include_reasoning: false` when Exclude is set. |
| `sglang` | clamped `reasoning_effort`, `chat_template_kwargs.enable_thinking`/`.thinking`, `thinking_budget`. |
| `ollama` | On `/v1` paths: `reasoning_effort` (`none` when off). On `/api/chat` and `/api/generate`: `think` as a bool or a level. |
| `lmstudio` | `reasoning_effort` clamped to `none`, `low`, `medium`, `high`. |
| `openai` | `reasoning_effort` only (`none` when off). Generic OpenAI-compatible, LiteLLM, Lemonade, LMDeploy, Docker Model Runner, oMLX. |
| `passthrough` | Nothing changes. Text Embeddings Inference, and any endpoint that opts out. |

Exclude has no request switch on most local backends. Olla logs it at debug level and the reasoning text comes back as usual.

The body is left byte-for-byte identical when it has no reasoning field, when the endpoint is `passthrough`, or when the path is not `/v1/chat/completions`, `/v1/completions`, `/api/chat` or `/api/generate`. The Responses API is not translated.

Each attempt that actually changes the body logs an info line, so a failover shows one line per backend:

```text
INFO Reasoning parameters translated request_id=… endpoint=openrouter from=openai to=openrouter from_fields=reasoning_effort to_fields=reasoning.effort,reasoning.enabled
```

`from` names the request shapes the client used (`openrouter`, `openai`, `chat_template_kwargs`, `llamacpp`, `vllm`, `sglang`, `ollama`), `to` is the endpoint's dialect, and the `_fields` keys list the exact fields on each side. A body already in the endpoint's dialect renders to the same bytes and is not logged.

## Choosing an endpoint's dialect

Resolved once when endpoints load; the first match wins:

1. The endpoint's `reasoning_dialect`.
2. `openrouter` for an `openai-compatible` (or `openai`) endpoint whose host ends in `openrouter.ai`.
3. The profile's `request.reasoning_dialect`: `llamacpp` for llama.cpp, `vllm` for vLLM and vLLM-MLX, `sglang`, `ollama`, `lmstudio`, `passthrough` for TEI, and `openai` for the rest.
4. Otherwise `passthrough` (for example `type: auto`).

An unknown `reasoning_dialect` fails config validation at startup.

```yaml
discovery:
  static:
    endpoints:
      - name: gpu-box
        url: http://192.168.0.10:8080
        type: llamacpp                 # llamacpp dialect from the profile
      - name: llama-swap
        url: http://192.168.0.11:9292
        type: openai-compatible
        reasoning_dialect: llamacpp    # llama-swap runs llama.cpp underneath
      - name: openrouter-overflow
        url: https://openrouter.ai/api
        type: openai-compatible        # openrouter.ai host selects openrouter
        queue_after: 20s
        sticky_sessions: false
        auth:
          type: bearer
          token: "${OPENROUTER_API_KEY}"
```

A client sending `{"reasoning_effort": "high"}` reaches `gpu-box` as `reasoning_effort: "high"` with `enable_thinking: true`, and if it overflows reaches OpenRouter as `reasoning: {effort: "high", enabled: true}`.

## Turning it off

`proxy.reasoning_translation: false` makes every endpoint `passthrough`. For a single endpoint, set its `reasoning_dialect: passthrough`.
