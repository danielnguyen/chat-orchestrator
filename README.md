# chat-orchestrator

Canonical runtime orchestration API for routing + profiles + observability.

## Canonical endpoint

- `POST /v1/chat`

## Responsibilities

- Resolve/create conversation in `basic-memory-store`
- Retrieve context bundle from memory-store
- Inject retrieved memory and file snippets into the model prompt
- Resolve and apply mode profile
- Evaluate declarative router rules
- Apply manual override (policy-gated)
- Apply fallback when provider fails
- Call provider via LiteLLM-compatible API
- Persist assistant message and one trace document per request

## Local run

1. Install requirements from `api/requirements.txt`
2. Set env from `.env.example`
3. Run `uvicorn main:app --host 0.0.0.0 --port 4361 --reload` from `api/`

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
  - No host `ports` mapping by default (internal-only service).
  - Access via docker network service name from peer containers.

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

## File-backed retrieval behavior

When `basic-memory-store` returns `bundle.artifact_refs`, the orchestrator:
- injects bounded file snippets into the prompt as additive context
- keeps recent conversation history in the prompt
- returns `sources` in the `/v1/chat` response using the source refs returned by memory-store

File ingestion remains owned by `basic-memory-store`; `chat-orchestrator` does not own an ingestion pipeline.
