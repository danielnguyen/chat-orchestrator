# chat-orchestrator

Canonical runtime orchestration API for routing + profiles + observability.

## Canonical endpoint

- `POST /v1/chat`

## Request flow

Normal request flow:

`surface/client -> chat-orchestrator POST /v1/chat -> basic-memory-store/cognitive-runtime/LiteLLM as downstream services`

`basic-memory-store` remains the durable memory, retrieval, and trace substrate. It is not the normal chat entrypoint.

## Responsibilities

- Resolve/create conversation in `basic-memory-store`
- Optionally consume Cognitive Runtime interaction governance before downstream response shaping
- Retrieve context bundle from memory-store
- Optionally retrieve read-only external evidence from Data Source Aggregator via `/v1/context-pack`
- Inject retrieved memory and file snippets into the model prompt
- Resolve and apply mode profile
- Evaluate declarative router rules
- Apply manual override (policy-gated)
- Apply fallback when provider fails
- Call provider via LiteLLM-compatible API
- Persist assistant message and one trace document per request

## Local run

1. Install requirements from `api/requirements.txt`
2. Copy `api/.env.example` to `api/.env`, then adjust it for local host-run
3. Run `make dev-start` from repo root, or `uvicorn main:app --host 0.0.0.0 --port 4361 --reload` from `api/`

For local host-run, `api/.env` is the canonical app config. The repo-root `.env` is reserved for Docker Compose / containerized runs.

Typical local `api/.env` contents:

```bash
ORCH_API_KEY=dev-key
MEMORY_STORE_BASE_URL=http://127.0.0.1:4321
MEMORY_STORE_API_KEY=dev-local
COGNITIVE_RUNTIME_BASE_URL=http://127.0.0.1:4371
COGNITIVE_RUNTIME_API_KEY=
COGNITIVE_RUNTIME_TIMEOUT_MS=1500
COGNITIVE_RUNTIME_COMPANION_ENABLED=false
COGNITIVE_RUNTIME_INTERACTION_GOVERNANCE_ENABLED=false
LITELLM_BASE_URL=http://127.0.0.1:4000
LITELLM_API_KEY=
DSA_ENABLED=false
DSA_BASE_URL=http://localhost:5174
DSA_TIMEOUT_MS=5000
DSA_API_KEY=
ROUTER_RULES_PATH=router/rules.yaml
MODEL_REGISTRY_PATH=router/model_registry.yaml
ALLOW_MANUAL_OVERRIDE=true
DEFAULT_PROFILE_NAME=dev
OFFLINE_PROVIDER=litellm-local
OLLAMA_BASE_URL=
REQUEST_TIMEOUT_MS=30000
```

## Health check

- `GET /healthz`
- Returns:
  - `status`
  - `service`
  - `time` (ISO8601)
  - best-effort `dependencies.memory_store`

## Local vs Docker defaults

- Local app mode (recommended for day-to-day dev):
  - `chat-orchestrator` API: `http://127.0.0.1:4361`
  - `MEMORY_STORE_BASE_URL`: `http://127.0.0.1:4321`
  - `LITELLM_BASE_URL`: `http://127.0.0.1:4000`

- Docker compose mode (`docker-compose.yml` in this repo):
  - Use the repo-root `.env` for container-safe values.
  - Service-to-service URLs should use container hostnames such as `basic-memory-store` and `litellm`.
  - Router/model paths should use container paths such as `/app/api/router/rules.yaml`.

## Smoke validation

Assuming both apps are running, execute:

```bash
make smoke
```

The smoke flow:
- calls `POST /v1/chat`
- asserts JSON and `request_id` are present
- allows either successful response or valid failure JSON
- on success, verifies trace visibility via `basic-memory-store` `GET /v1/traces/{request_id}`
- optionally verifies summarized governance trace fields when `EXPECT_GOVERNANCE_STATUS` or `EXPECT_GOVERNANCE_POSTURE` are provided
- keeps normal chat ownership on `chat-orchestrator` while treating `basic-memory-store`, `cognitive-runtime`, and LiteLLM as downstream services

Optional smoke env vars:
- `CHAT_PAYLOAD_JSON` overrides the default chat request payload
- `EXPECT_GOVERNANCE_STATUS` checks `payload.retrieval.prompt_assembly.interaction_governance.status`
- `EXPECT_GOVERNANCE_POSTURE` checks `payload.retrieval.prompt_assembly.interaction_governance.response_posture`

Operator checks when runtime behavior looks wrong:
- confirm `basic-memory-store` is reachable for conversation, retrieval, and trace writes
- confirm `cognitive-runtime` `GET /healthz` succeeds before enabling governance consumption
- confirm `cognitive-runtime` `POST /v1/runtime/interaction-governance/evaluate` returns a typed result
- confirm `COGNITIVE_RUNTIME_INTERACTION_GOVERNANCE_ENABLED=false` leaves normal `/v1/chat` behavior unchanged
- confirm `COGNITIVE_RUNTIME_INTERACTION_GOVERNANCE_ENABLED=true` records a safe governance summary when `cognitive-runtime` is reachable
- confirm `COGNITIVE_RUNTIME_INTERACTION_GOVERNANCE_ENABLED=true` remains non-fatal and traces governance as failed or omitted when `cognitive-runtime` is unavailable
- confirm tense debugging inputs produce tactical governance posture with humor and commentary suppressed in the trace summary
- confirm malformed or malicious governance results do not leak raw prompt-facing guidance into traces or user-visible output
- confirm `cognitive-runtime` companion compile failures stay traceable and non-fatal to normal chat
- confirm user-facing answers do not include raw runtime exception text
- confirm `POST /v1/runtime/overlay` is reachable when runtime overlays are enabled
- confirm governance traces stay summarized and do not expose raw prompt text, raw private memory, hidden reasoning, raw runtime event payloads, raw runtime exception text, or implementation-planning identifiers

## File-backed retrieval behavior

When `basic-memory-store` returns `bundle.artifact_refs`, the orchestrator:
- injects bounded file snippets into the prompt as additive context
- keeps recent conversation history in the prompt
- returns `sources` in the `/v1/chat` response using the source refs returned by memory-store

File ingestion remains owned by `basic-memory-store`; `chat-orchestrator` does not own an ingestion pipeline.

## Optional Data Source Aggregator evidence retrieval

The Data Source Aggregator integration is optional and disabled by default.

`DSA` in the environment variable names stands for `Data Source Aggregator`.

- `DSA_ENABLED=false` keeps existing behavior unchanged.
- `DSA_BASE_URL` is the base URL for the Data Source Aggregator service.
- `DSA_TIMEOUT_MS=5000` is the recommended request timeout for Data Source Aggregator calls. `1500` can be too short when DSA fans out across multiple sources.
- `DSA_API_KEY` is optional for local development. When set, the orchestrator sends `X-API-Key: <DSA_API_KEY>` on DSA requests.
- The current integration uses `POST /v1/context-pack`.
- This path is read-only evidence retrieval; memory writes remain separate and continue to belong to `basic-memory-store`.
- Requests can opt in with `external_context_enabled=true` for the simple default behavior.
- Requests can also opt in with `external_context.enabled=true` and optionally target `source_ids`, `domain_tags`, `allowed_sensitivity`, and `max_results`.
- If both fields are present, either one being `true` enables DSA retrieval.
- `sensitivity=local_only` still wins and skips DSA even if external context is requested.
- The DSA API key is not included in orchestrator traces.

Manual smoke note:

1. Start Data Source Aggregator locally on port `5174` with vehicle/calendar configs.
2. Start `chat-orchestrator` with `DSA_ENABLED=true`, `DSA_BASE_URL=http://localhost:5174`, and `DSA_API_KEY` if DSA auth is enabled.
3. Send a chat request with `external_context_enabled=true` or a targeted `external_context` object and ask a vehicle or calendar question.
4. Confirm the response can use source-backed context and still succeeds if DSA is stopped.

## Prompt Assembly And Routing

Prompt assembly is explicit. The orchestrator assembles:

1. profile prompt overlay, when present
2. additive style guidance, when surface/profile inputs require it
3. additive response-shape guidance, when spoken-output or active-task inputs require it
4. retrieved memory and file snippet system messages
5. recent conversation history from memory-store
6. current request messages

Trace metadata records included/omitted prompt layers, retrieval snippet refs, and truncation status. Current behavior applies no additional truncation in the orchestrator layer.

Local/offline routing precedence is additive and traceable: request `sensitivity=local_only`, profile `routing_policy.local_only`, compatible manual override, router rule selection, profile cost/latency policy, then provider fallback. Local-only constraints continue to apply to fallback models.

## Integration Boundaries

- `cognitive-runtime` owns companion contracts and diagnostic surfaces.
- `cognitive-runtime` owns interaction classification policy and the interaction governance evaluation endpoint.
- `chat-orchestrator` consumes compiled companion policy overlays and does not own companion contract definition.
- `chat-orchestrator` can optionally consume Cognitive Runtime interaction governance when `COGNITIVE_RUNTIME_INTERACTION_GOVERNANCE_ENABLED=true`; the default remains `false`.
- `AssistantHandoff` captures orchestration output as refs, counts, statuses, and warning summaries.
- `CompanionPresentation` prepares prompt-facing presentation input from the handoff summary.
- `response_review` is a deterministic shadow review over model output and trace context.
- `response_action` remains opt-in `template_fallback` only; default behavior stays `shadow`.
- `basic-memory-store` remains outside companion/runtime contract ownership and continues to provide conversation, retrieval, and trace persistence only.
