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

## Governed targeted evidence acquisition

The first governed evidence-acquisition path is disabled by default with
`EVIDENCE_ACQUISITION_ENABLED=false`. Enabling it also requires a configured
Cognitive Runtime, enabled interaction governance, and `DSA_ENABLED=true`. It
does not opt requests into external context: the request-level flag or structured
object is still required, and an effective `local_only` policy always wins.

For an eligible normal chat request, Chat Orchestrator uses the existing
interaction-governance result, asks Cognitive Runtime to derive a broad evidence
shape, reads the governed DSA source inventory, adapts the neutral source
capabilities, and asks Cognitive Runtime to compile an evidence plan. The initial
execution boundary proceeds only for a derived `targeted_lookup` whose plan is
ready (possibly with optional limitations) and whose only selected strategy is
either `targeted_retrieval` or `exact_fetch`.

Source IDs narrow semantic retrieval to governed source registries. They do not
identify exact items and continue to use one DSA context-pack call. The optional
structured `external_context.exact_source_refs` collection instead identifies
individual opaque records, each associated with a source ID. Exact references
require explicit external-context opt-in. A supported exact plan makes one
bounded DSA fetch call per normalized reference, requests no raw connector data,
attempts every declared reference without retry, and never falls back to
semantic search. Every response must match the declared source ID and exact
reference.

After prompt assembly, Chat Orchestrator reports requirement outcomes based on
what was actually acquired and what external context survived into provider
reasoning.
For exact fetch, every declared reference must return a valid untruncated result,
and every returned reference must survive in the final provider prompt. Partial,
missing, malformed, failed, truncated, or prompt-filtered exact coverage cannot
authorize a provider conclusion.
Cognitive Runtime evaluates those facts. An insufficient or unknown result
withholds an unsupported conclusion without calling the provider. A sufficient
result permits the existing single provider path; an optional limitation adds a
bounded disclosure. Targeted answers that claim exhaustive or absence-sensitive
coverage receive a disclosure that only the targeted sources were checked.
Provider prose cannot select or upgrade the plan, acquisition facts, sufficiency
status, or answer constraints.

The final request trace retains a bounded `prompt.evidence_acquisition` manifest.
It records structural shape, inventory, plan, acquisition, delivery, sufficiency,
and limitation outcomes; the exact persisted assistant-message identifier; and a
digest of the final user-visible answer. Exact manifests distinguish attempted,
returned, retained, omitted, and unsuccessful references and retain only bounded
attempt counts and outcomes. They do not copy fetch response bodies. The manifest
does not retain the question text,
source text, source titles or descriptions, provider output, credentials, raw
dependency errors, confidence values, prompts, or hidden reasoning. Existing
privacy suppression removes source and exact-reference identifiers while
retaining counts and statuses.

When the existing claim-capture boundary accepts a single-sentence claim backed
by one retained file source, Chat Orchestrator may link that claim record to the
same turn's acquisition manifest. The assistant message is persisted first, the
manifest is bound to that message and the exact final-answer digest, and the
request trace containing the bound manifest is persisted before the claim record
is created. Only the validated manifest identifier is added at the top level of
the claim-record request. The calibrated evidence reference remains the one file
reference actually used to support the claim; source inventories, acquisition
attempts, returned or retained external references, and sufficiency details are
not copied into claim support or calibration.

Manifest association is validated independently of provider text. It requires an
attempted acquisition, a ready plan, matching sufficient top-level and nested
outcomes, and exact agreement with the bound assistant-message identifier and
final-answer digest. A malformed, unsupported, insufficient, or mismatched
association skips claim-record persistence without retry or an unlinked fallback,
while preserving the assistant response and request trace. Claim diagnostics
retain only bounded association status and whether a link was established; they
do not duplicate the manifest identifier or body. Ordinary non-evidence claims
continue to use the legacy unlinked payload. This association does not expand the
current single-sentence, single-file claim-capture boundary, infer which external
item a provider used, or add acquisition-history explanation.

Ambiguous evidence tasks and unsupported plans or strategies return bounded,
provider-free responses. A `not_applicable` result continues through the existing
chat and optional DSA behavior. Briefs, capability and action flows, pending-action
continuations, and claim-explanation follow-ups remain outside governed execution;
an exact-reference request at one of those boundaries fails closed instead of
entering a legacy path. Bounded full context, structured queries, hybrid
acquisition, and execution of exhaustive, absence-sensitive, contradiction,
historical, comparison, or recommendation plans are not implemented here.
The public chat response fields are unchanged.

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

## Claim-record capture

Durable claim-record capture is disabled by default through
`CLAIM_RECORD_CAPTURE_ENABLED=false`. When enabled, it uses the configured
Cognitive Runtime client for calibration and Basic Memory Store for immutable
association with the persisted assistant message and request trace.

The initial supported path is deliberately narrow: a normal response must contain
one bounded, explicitly file-source-attributed factual sentence and exactly one
retained, user-visible file-backed source. The source identity must be present in
the normal trace reference set. A source being present is not sufficient by
itself; subjective, creative, humorous, or otherwise unattributed sentences are
skipped conservatively. Multi-sentence or structured answers, multiple or missing
sources, briefs, action responses, memory callbacks, and privacy-suppressed answers
are also skipped rather than attributed by guesswork.

Capture does not add another provider call or expose calibration metadata in the
chat response. Calibration or storage failure leaves the completed answer intact,
records only a bounded structural outcome in the request trace, and never
fabricates a durable record.

The same flag also enables a bounded follow-up explanation path. The generic
messages `How are you sure?`, `What supports that?`, `What supported that?`,
`What evidence supports that?`, and `What was that based on?` still target only
the immediately preceding bounded assistant answer. Chat Orchestrator loads only
the newest conversation-scoped claim-record group, requires exactly one claim in
that group, and requires its normalized anchor to equal that preceding answer.

An older retained claim can be targeted with one of these exact forms:

```text
What supports the statement "<exact retained claim anchor>"?
What supported the statement "<exact retained claim anchor>"?
How are you sure about the statement "<exact retained claim anchor>"?
```

The framing is case-insensitive and tolerates whitespace variation and one terminal
question mark or period. The quoted anchor uses straight double quotes and is
matched in full after whitespace normalization; its case and punctuation must
match. One lookup considers at most 20 scoped claim records. No fuzzy matching,
pagination, repeated lookup, or provider interpretation occurs, and duplicate
exact anchors are treated as ambiguous.

A supported record is rendered without retrieval or a model call, using only its
source type, claim class, confidence, evidence strength, freshness, and material
limitations. Opaque record and source identifiers, target text, and private record
content are not copied into traces or explanations. Malformed targets and missing,
ambiguous, incomplete, unsupported, or unavailable records produce an honest
deterministic fallback. No explanation performs fresh verification.

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
