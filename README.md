# Chat Orchestrator

Chat Orchestrator is the canonical chat API for request routing, profile application, context assembly, provider selection, permissioned actions, fallback behavior, and request tracing.

## API

The canonical endpoint is:

- `POST /v1/chat`

Requests require the configured `X-API-Key`. The high-level flow is:

```text
client -> Chat Orchestrator -> Basic Memory Store / Cognitive Runtime / Data Source Aggregator -> model provider -> persisted response and trace
```

Basic Memory Store remains the durable memory, retrieval, and trace service. It is a downstream dependency, not the normal chat entry point.

## Responsibilities

- Resolve conversations and profiles through Basic Memory Store.
- Retrieve recent, semantic, and file-backed context.
- Optionally retrieve read-only external context from Data Source Aggregator.
- Apply surface, style, response-shape, privacy, and runtime guidance.
- Match capabilities and run permissioned action lifecycles when enabled.
- Select a model through declarative routing and policy-bounded overrides.
- Apply provider fallback without weakening local-only routing.
- Persist the assistant response and one bounded trace per request.

## Run locally

From the repository root:

```bash
python3 -m venv api/.venv
make dev-install
cp api/.env.example api/.env
make dev-start-reload
```

Configure `api/.env` for the local Basic Memory Store and model-provider endpoints before starting. Cognitive Runtime and Data Source Aggregator integrations are optional and disabled by default. Start from the documented local template in [`api/.env.example`](api/.env.example).

The application reads `api/.env` for a host-run. The repository-root `.env` is reserved for Docker Compose values.

## Health

- `GET /healthz`

The response includes service status, the current time, and a best-effort Basic Memory Store dependency status.

## Validation

Primary repository checks:

```bash
make dev-test
make dev-lint
make prompt-budget-test
make replay-test
make process-naming-check
make smoke
```

See [Validation](docs/validation.md) for smoke options, troubleshooting, replay behavior, and disposable composed checks.

## Documentation

- [Validation](docs/validation.md)
- [Runtime behavior](docs/runtime-behavior.md)
- [Adding an action connector](docs/action-connectors/README.md)

## Adding an action connector

An action connector supplies integration-specific mechanics to Chat Orchestrator's shared permissioned-action lifecycle; registering one does not grant it authority. The canonical current-behavior guide is [Adding an action connector](docs/action-connectors/README.md).

The guide covers Cognitive Runtime policy registration, matching Chat Orchestrator registration, connector implementation, explicit production registration, testing, and validation.
