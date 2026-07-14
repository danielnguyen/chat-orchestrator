# Runtime behavior

This document describes the current `POST /v1/chat` orchestration path and its integration boundaries.

## Request lifecycle

For each chat request, Chat Orchestrator:

1. resolves or creates a conversation through Basic Memory Store;
2. resolves the active profile and retrieves bounded conversation context;
3. optionally retrieves external read-only context from Data Source Aggregator;
4. resolves enabled Cognitive Runtime context and policy decisions;
5. assembles prompt layers within the configured budget;
6. selects a model and provider through routing policy;
7. invokes the provider or a policy-compatible fallback;
8. executes an authorized connector action when the capability flow permits it;
9. persists the assistant message and a bounded request trace; and
10. returns the answer, routing status, public sources, and any pending action.

Optional integrations are non-authoritative unless their owning policy explicitly supplies a decision. Registration or availability alone does not grant an action permission.

## File-backed retrieval

Basic Memory Store owns file ingestion, object storage, derivation, and retrieval. Chat Orchestrator does not implement a separate ingestion pipeline.

When the retrieval bundle contains `bundle.artifact_refs`, Chat Orchestrator:

- converts eligible file snippets into bounded system context;
- keeps recent conversation messages in the assembled prompt;
- applies truth, memory-hygiene, privacy, and prompt-budget decisions before provider invocation;
- includes only prompt-selected, user-visible file references in the response `sources`; and
- records structural source identifiers, counts, inclusion status, and omission reasons in the trace rather than copying full file contents.

If retrieval is unavailable or malformed, the request follows the bounded degradation path. It does not invent file context or expose dependency exception text.

## Optional Data Source Aggregator integration

Data Source Aggregator (DSA) provides read-only external context through `POST /v1/context-pack`. It is disabled by default.

Service configuration uses:

- `DSA_ENABLED`
- `DSA_BASE_URL`
- `DSA_TIMEOUT_MS`
- `DSA_API_KEY`

The current defaults are documented in [`api/.env.example`](../api/.env.example). When `DSA_API_KEY` is set, Chat Orchestrator sends it in the DSA `X-API-Key` header and does not include it in traces.

Both service-level enablement and request-level opt-in are needed. A request may use the simple `external_context_enabled` flag or the structured `external_context` object. If both are present, either one set to true opts in. An effective `local_only` policy always suppresses the external call.

Explicit `source_ids` are optional. Source selection remains owned by Data Source Aggregator; use `source_ids` only when the caller truly needs a bounded source subset.

### Basic request

```json
{
  "owner_id": "owner",
  "client_id": "client",
  "surface": "chat",
  "messages": [
    {
      "role": "user",
      "content": "Summarize the recent conversation."
    }
  ],
  "sensitivity": "private"
}
```

### Request with targeted external context

```json
{
  "owner_id": "owner",
  "client_id": "client",
  "surface": "chat",
  "messages": [
    {
      "role": "user",
      "content": "Check the maintenance source for recent service history."
    }
  ],
  "sensitivity": "private",
  "external_context": {
    "enabled": true,
    "source_ids": ["example_source"],
    "domain_tags": ["maintenance"],
    "allowed_sensitivity": "medium",
    "max_results": 5
  }
}
```

### DSA trace outcomes

The request trace reports one of these high-level outcomes:

| Status | Meaning |
| --- | --- |
| `disabled_by_service` | DSA is disabled in service configuration. |
| `disabled_by_request` | The request did not opt in. |
| `skipped_local_only` | Effective local-only policy prohibited the external call. |
| `success` | DSA returned usable items and external context was available for prompt assembly. |
| `success_no_items` | DSA returned no usable items; chat continued without external context. |
| `error` | The client was unavailable, timed out, returned an HTTP error, returned malformed data, or failed unexpectedly. |

Successful traces contain bounded fields such as item count, sources used, error codes, budget truncation, and whether context was injected. When valid diagnostics are supplied, Chat Orchestrator may also record selection mode, considered and selected source IDs, ranking mode, bounded per-source candidate counts, source score bands and reasons, and candidate truncation. Malformed diagnostics are omitted rather than copied through.

DSA failures are non-fatal to normal chat execution. Memory writes remain separate and continue to belong to Basic Memory Store.

For a manual integration check, start DSA at the configured base URL, enable it in `api/.env`, restart Chat Orchestrator, and send the targeted request above. Then stop DSA and repeat the request to confirm that chat continues with a bounded DSA error status.

## Prompt assembly and routing

Prompt assembly is explicit and budgeted. Depending on configuration and request context, the assembled messages can include:

1. profile overlay;
2. style guidance;
3. response-shape guidance;
4. enabled Cognitive Runtime guidance;
5. retrieved memory and file snippets;
6. external source context;
7. recent conversation history; and
8. current request messages.

The prompt trace records which bounded layers were included or omitted, source counts and references, budget decisions, and truncation status. Persisted traces exclude full provider prompts and raw private dependency content.

Routing considers the effective local-only constraint, a permitted manual model override, declarative router rules, profile cost and latency policy, and the provider fallback plan. A local-only request or profile can use only local providers, including during fallback. If no compatible local model exists, the request fails rather than routing externally.

Provider failure may produce a policy-compatible fallback or a degraded response. It must not cause a permissioned action to execute again.

## Integration boundaries

### Basic Memory Store

Basic Memory Store owns:

- conversation resolution and message persistence;
- profile resolution;
- recent, semantic, episodic, and file-backed retrieval;
- artifact ingestion and derivation; and
- request trace persistence and lookup.

Chat Orchestrator consumes these interfaces, assembles bounded context, and persists the final response and trace. It does not take ownership of memory or artifact storage.

### Cognitive Runtime

Cognitive Runtime owns:

- runtime identity, session, turn, and overlay contracts;
- interaction governance, persona containment, restraint, memory hygiene, and privacy decisions;
- world-state and relationship authority;
- canonical capability metadata, matching, action authority, confirmation, dispatch, verification policy, and action summaries; and
- companion-policy contracts and diagnostics.

Chat Orchestrator consumes enabled Cognitive Runtime results, validates them against bounded local registrations, and applies them through the shared lifecycle. Optional context and guidance integrations are disabled by default and degrade without exposing raw responses. Connector registration cannot override Cognitive Runtime policy.

Compiled companion policy is consumed as an overlay rather than redefined locally. Deterministic response review remains traceable, while response-action behavior defaults to `shadow`; `template_fallback` is an explicit opt-in mode.

### Data Source Aggregator and model providers

Data Source Aggregator owns source selection, retrieval, and its source diagnostics. Chat Orchestrator sends a bounded context-pack request and sanitizes the result before prompt assembly.

Model providers receive only the final assembled messages. Chat Orchestrator owns routing, fallback constraints, bounded provider diagnostics, response persistence, and the public API response.
