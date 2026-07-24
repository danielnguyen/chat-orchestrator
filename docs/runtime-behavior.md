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
capabilities, and asks Cognitive Runtime to compile an evidence plan. Governed
execution proceeds for a derived `targeted_lookup` whose ready plan selects only
`targeted_retrieval` or `exact_fetch`, and for the bounded hybrid comparison
and bounded exhaustive-review contracts described below.

Source IDs narrow semantic retrieval to governed source registries. They do not
identify exact items and continue to use one DSA context-pack call. The optional
structured `external_context.exact_source_refs` collection instead identifies
individual opaque records, each associated with a source ID. Exact references
require explicit external-context opt-in. A supported exact plan makes one
bounded DSA fetch call per normalized reference, requests no raw connector data,
attempts every declared reference without retry, and never falls back to
semantic search. Every response must match the declared source ID and exact
reference.

Configured source inventory entries may also carry an optional strict
`scope_refs` object with bounded `time`, `version`, `domain`, and `project`
identifiers. Legacy entries without this object remain valid. A request may use
the same shape at `external_context.scope_refs`; requested dimensions match
configured values exactly and conjunctively, then narrow the source universe
already declared by exact references, source IDs, or domain tags. A selector
with no configured match stops before evidence-plan compilation or acquisition.
For an unrequested dimension, Chat Orchestrator derives a reference only when
every source in the non-empty declared universe has the same non-null configured
value. The resolved four references enter the existing declared scope and
acquisition premise sent to Cognitive Runtime. Per-source scope metadata is not
added to Cognitive Runtime source descriptors, provider evidence, retained
manifests, claim records, history output, or user-visible responses.

Context-pack items may also declare bounded `available_context` descriptors for
connector-owned expansion modes. The targeted path strictly validates and then
removes those descriptors without executing them.

The first hybrid path is limited to a ready `cross_source_comparison` plan over
two to eight selected sources, with `complete_for_selected_sources` completeness,
no exact references, no contradiction search, and material selected-source
coverage, cross-source comparison, and context-delivery requirements. Every
selected source must be available and support both targeted retrieval and context
expansion. Other hybrid task shapes remain unsupported.

Hybrid execution makes one targeted context-pack request over the exact planned
source IDs, requiring at least one result from each. In stable source order, it
chooses the first result that declares an expansion option and that result's
first connector-declared mode. It makes at most one sequential context call per
source, with no retry, replacement search, connector-specific inference, or
provider-selected target. Each call uses a bounded budget of five rows, 50,000
serialized bytes, and 12,000 text characters. Missing descriptors, empty or
malformed responses, dependency failure, and truncation remain explicit bounded
attempt outcomes.

Targeted items precede expanded items in the combined prompt evidence; expanded
items are grouped by planned source order, and duplicate references are removed
deterministically while preserving the targeted item. Descriptors, modes, URLs,
raw connector data, and cache internals do not enter the provider prompt.

After prompt assembly, Chat Orchestrator reports requirement outcomes based on
what was actually acquired and what external context survived into provider
reasoning.
For exact fetch, every declared reference must return a valid untruncated result,
and every returned reference must survive in the final provider prompt. Partial,
missing, malformed, failed, truncated, or prompt-filtered exact coverage cannot
authorize a provider conclusion.
Cognitive Runtime evaluates those facts. An insufficient or unknown result
withholds an unsupported conclusion without calling the provider. The
provider-free response names a bounded set of the actual material requirement
gaps and distinguishes failed, filtered, truncated, unsupported, unavailable,
unknown, missing, partial, excluded, not-attempted, and unresolved-contradiction
outcomes. It also states which class of conclusion is being withheld. Provider
text and provider fallback are not consulted for that rendering.

A sufficient result permits the existing single provider path. A
`sufficient_with_limitations` result adds a deterministic disclosure derived
from optional requirement evaluations, plan limitations, and bounded trusted
inventory counts when those counts are established. Distinct causes are
deduplicated, sorted, and capped; private source identifiers and provider prose
do not enter the disclosure.

Every governed successful targeted answer receives a stable statement that only
the targeted sources were checked. This applies to semantic retrieval and exact
fetch whether or not the provider uses universal wording.
Hybrid comparison facts are satisfied only when every planned source contributed
targeted evidence, one successful expansion, and prompt-retained evidence, with
at least two expanded source scopes surviving prompt budgeting. There is no
targeted-only fallback. Every governed successful comparison receives a stable
selected-source and bounded-context statement.
Provider prose cannot select or upgrade the plan, acquisition facts, sufficiency
status, limitation disclosure, answer constraints, or scope boundary. Reapplying
the final-answer policy does not duplicate or cross-apply policy-owned
paragraphs.

The bounded exhaustive executor accepts only a ready
`bounded_exhaustive_review` plan with no limitations, no exact references, the
sole `hybrid` strategy, complete declared-scope coverage, contradiction search,
and exactly five material requirements: authoritative inventory, complete scope
coverage, contradiction search, context delivery, and no material truncation.
Trusted DSA metadata must establish a complete `configured_sources` inventory
whose declared scope resolves to exactly one enabled, ready, authoritative
Google Sheets source advertising both search and context operations. Legacy,
partial, unknown, unavailable, multi-source, optional, supplemental, disabled,
or unavailable scope remains unsupported. Source names, categories, question
text, result content, and provider prose cannot confer authority or completeness.

This executor uses one targeted context-pack request only to discover a seed.
The response must associate that exact source across its items and diagnostics,
contain no errors, and include a result with the exact connector-declared
`configured_worksheet` mode. Descriptor descriptions are ignored; the existing
comparison path still selects its first connector-declared descriptor. The
exhaustive executor makes exactly one context call using the seed reference,
the exact named mode, and fixed limits of 20 rows, 50,000 serialized bytes, and
12,000 text characters. It does not retry, choose `nearby_rows`, try another
seed, fetch another source, or fall back to targeted evidence.

A positive context response must be untruncated and error-free and contain
exactly one raw-free, URL-free Google Sheets `spreadsheet_range` result with no
further expansion descriptors. Only that complete range enters prompt
assembly—the targeted seed is excluded. Authoritative-inventory and complete
scope facts come from trusted inventory and the successful expansion contract,
while context delivery, contradiction-search availability, and no-material-
truncation also require the complete range to survive prompt budgeting. Targeted
seed-search or candidate truncation remains visible in bounded diagnostics but
does not truncate the complete material evidence set after a successful full
expansion. Empty, missing-descriptor, malformed, failed, truncated, or
prompt-filtered outcomes remain explicit and block the provider through the
existing sufficiency gate. An empty worksheet does not prove absence.

The retained manifest records one bounded configured-worksheet attempt,
returned-versus-retained reference state, aggregate outcomes, and prompt-aware
requirement facts without storing worksheet text. Its identity changes with the
seed, mode, outcome, context query, returned count, and delivery state. Existing
identifier suppression clears sources, seed and returned references, context
modes, query identifiers, and attempt details while retaining safe aggregate
counts. Exhaustive acquisition does not broaden claim-support capture or the
historical explanation renderer. It supports neither multi-source exhaustive
review nor ready-with-limitations plans and adds no absence proof. Every
successful bounded exhaustive answer receives a stable statement that
completeness applies only to the declared source scope that was checked and not
to sources outside it.

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
Hybrid manifests additionally retain one bounded expansion attempt per planned
source and aggregate satisfied, unknown, failed, filtered, truncated, and
unsupported counts. Manifest identity includes the bounded target, declared
mode, and outcome history. Privacy suppression clears source IDs, seed
references, context modes, and internal context query IDs while preserving
aggregate attempt semantics. Expanded evidence is not automatically claim
support, and the existing single-file claim boundary is unchanged.

When the existing claim-capture boundary accepts a single-sentence claim backed
by one retained file source, Chat Orchestrator may link that claim record to the
same turn's acquisition manifest. Claim eligibility and calibration use the
post-privacy answer before evidence-policy limitation and scope paragraphs are
added. The resulting single-sentence claim remains the first paragraph of the
final answer. Policy-owned paragraphs are not calibrated and are not copied into
the claim record.

The complete governed final answer is then persisted as the assistant message.
The manifest is bound to that message and to a SHA-256 digest of the exact
complete UTF-8 response. The claim-anchor digest independently identifies only
the normalized calibrated first paragraph, so the two digests truthfully differ
when a limitation or scope paragraph is present. Before a linked claim is stored,
Chat Orchestrator requires the manifest digest to match the exact final answer
and the normalized first response paragraph to match the calibrated claim
exactly. Later-only, substring, paraphrased, headed, bulleted, empty, or modified
first paragraphs fail closed.

The request trace containing the bound manifest is persisted before the claim
record is created. Only the validated manifest identifier is added at the top
level of the claim-record request. The calibrated evidence reference remains the
one file reference actually used to support the claim; source inventories,
acquisition attempts, returned or retained external references, policy
paragraphs, and sufficiency details are not copied into claim support or
calibration.

Manifest association is validated independently of provider text. It requires an
attempted acquisition, a ready plan, matching sufficient top-level and nested
outcomes, exact agreement with the bound assistant-message identifier and full
response digest, and exact first-paragraph claim association. A malformed,
unsupported, insufficient, or mismatched association skips claim-record
persistence without retry or an unlinked fallback, while preserving the
assistant response and request trace. Claim diagnostics retain only bounded
association status and whether a link was established; they do not duplicate the
manifest identifier, response, or body. Ordinary non-evidence claims continue to
use the legacy unlinked payload. This association does not expand the current
single-sentence, single-file claim-capture boundary, infer which external item a
provider used, or treat every acquired item as claim support.

Ambiguous evidence tasks and unsupported plans or strategies return bounded,
provider-free responses. A `not_applicable` result continues through the existing
chat and optional DSA behavior. Briefs, capability and action flows, pending-action
continuations, and claim-explanation follow-ups remain outside governed execution;
an exact-reference request at one of those boundaries fails closed instead of
entering a legacy path. Bounded full context, structured queries, hybrid
acquisition outside the bounded cross-source comparison and exact one-source
exhaustive paths, and execution of absence-sensitive, broader contradiction,
historical, or recommendation plans are not implemented here. Hybrid manifests
are retained truthfully, but
the historical `What did you check?` renderer continues to support linked
targeted and exact manifests only. For the immediately previous response it
checks the manifest digest against the exact supplied assistant message and
associates the normalized first paragraph with the retained claim anchor. A
quoted-anchor explanation continues to select the exact normalized retained
claim and relies on the immutable claim/manifest association already validated
by the memory service; it does not fetch historical message content. This is a
compatibility check, not trace-first history resolution. Hybrid-specific
historical explanation remains separate work and is never reconstructed by a
provider.
The public chat response fields are unchanged.

Cognitive Runtime owns deterministic evidence next-step selection. After each
governed sufficiency evaluation, Chat Orchestrator submits the exact current
premise reconstructed from the compiled plan: its question-anchor digest, task
shape, declared scope, normalized trusted source inventory, and selected
strategies. It does not derive the premise or the selected next step from
provider text, evidence text, budgets, request identifiers, or result counts.
The returned selection is strictly associated with the evaluation, plan,
manifest, task shape, sufficiency status, current premise, unresolved material
requirements, and any locally validated proposed premise. A missing, malformed,
or mismatched selection fails closed before any provider call.

One narrow changed-premise follow-up is supported for a targeted lookup. When
initial evidence is insufficient or unknown, no deterministic clarification is
indicated, and a validated targeted result identifies an available eligible
source with exact-fetch capability, Chat Orchestrator deterministically selects
one source/reference pair in source-ID and opaque-reference order. It preserves
every declared scope field and user selector, adds only that discovered exact
reference, and asks Cognitive Runtime to compile the proposed plan. The
proposal is submitted only if that plan is an existing executable `exact_fetch`
composition and its premise is built from the compiled result. It never broadens
source IDs or categories, changes the question anchor or trusted inventory,
invents another scope, or treats a budget, retry identifier, or result-count
change as a new premise.

Additional acquisition occurs only when Cognitive Runtime selects
`perform_additional_acquisition` with the `changed_premise_allowed` guard and
the returned proposed-premise digest matches the local compiled proposal. Chat
Orchestrator then performs exactly one existing raw-free exact fetch, rebuilds
the prompt from the original sanitized request inputs, reapplies prompt
budgeting and reference-retention checks, reevaluates sufficiency, and requests
one final next-step selection under the promoted premise. There is no loop,
retry, alternate reference, larger budget, or second additional acquisition.
An unchanged or previously attempted premise, a failed fetch, an invalid
authorization, or another acquisition request after the one allowed attempt
remains provider-free.

The final selection controls response enforcement. A bounded sufficient answer
and an existing sufficient-with-limitations answer retain their current
provider, limitation, and task-boundary behavior. For insufficient or unknown
evidence, a CR-permitted qualified partial result is rendered
deterministically from substantive satisfied or partial requirements and actual
unresolved gaps; unrestricted provider prose is not invoked and the requested
conclusion remains withheld. Narrow clarification renders exactly one
policy-owned question. Unexamined-scope disclosure and unsupported-conclusion
withholding use the existing privacy-safe material-gap renderer. Provider and
fallback calls remain zero for those deterministic responses.

The final manifest remains bound to the final active plan, prompt-retained
evidence, assistant message, and complete response digest. It also retains at
most two bounded structural next-step selections and a bounded initial-attempt
summary showing the initial strategy, sufficiency, result and retention counts,
and whether a changed-premise exact fetch followed. Premise bodies, evidence
text, provider text, URLs, credentials, and unrestricted dependency errors are
not retained. Identifier suppression continues to remove source and reference
details where required. The final exact-fetch evidence alone determines claim
support; the initial targeted seed is not copied into a claim. Acquisition
history rendering is unchanged and no new-verification flow is added.

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
deterministic fallback. A support explanation performs no fresh verification.

Acquisition-history explanations use a separate response-and-trace-first path and
do not require a claim record. The exact immediate-prior forms are `What did you
check?`, `What did you examine?`, `Did you look at everything relevant?`, `What
might you have missed?`, and `What did you not check?`. Immediate resolution sends
the complete immediately preceding assistant response digest and its normalized
first paragraph to Basic Memory Store; neither service scans backward on a
mismatch. The quoted variants use the exact straight-double-quoted first paragraph
and let Basic Memory Store perform one bounded exact lookup. Multiple exact matches
are ambiguous rather than silently resolved to the newest response.

Chat Orchestrator validates the resolver scope and its bounded manifest-only
projection. It does not list claims or fetch a request trace for acquisition
history. Support explanations such as `How are you sure?` remain claim-record
based and never call the acquisition-history resolver merely because a claim has
a manifest link. Acquisition history remains available when governed evidence is
enabled even if claim capture was disabled for the original answer.

Successful history explanations describe only retained aggregate structure.
Targeted retrieval reports considered and selected source counts plus returned and
reasoning-delivered item counts. Exact fetch reports specified-reference attempts
and material outcomes. Hybrid comparison reports selected configured sources,
bounded context-expansion outcomes, and returned and retained references without
claiming every possible source was compared. Bounded exhaustive history describes
completeness only within the declared configured scope; a sufficient coverage
answer explicitly says that it does not establish universal coverage beyond that
scope. Limited, insufficient, and unknown histories preserve their recorded
qualification or withholding status. Privacy-suppressed manifests use retained
aggregate counts without reconstructing identifiers. A targeted attempt followed
by one authorized changed-premise exact fetch is described as those two bounded
steps, not as an unbounded retry. Older manifests without next-step history remain
compatible.

A pure historical explanation is provider-free and ends by stating that it did
not perform a new verification. It performs no memory retrieval, source
acquisition, runtime evidence planning, provider call, fallback call, or claim
capture. Missing, ambiguous, invalid, malformed, or unavailable resolver outcomes
produce deterministic bounded wording without exposing storage reason codes,
identifiers, digests, target text, source details, prompts, credentials, provider
text, exceptions, or hidden reasoning.

The bounded history questions may be followed by exactly `Check again.` or
`Verify again.`, including their exact quoted-target forms. In that compound mode,
the historical lookup and the new check remain separate. Evidence planning receives
the deterministic task `Verify this prior statement with a new evidence check:
"<normalized first paragraph>"`, while the stored user message remains unchanged.
The ordinary governed acquisition, prompt-budget, sufficiency, next-step, privacy,
and answer-boundary controls then apply to the new check. Historical evidence is
not reused as fresh evidence. A successful or limited result is labelled `Original
acquisition:` and `New verification:`; an insufficient or unknown result uses `New
verification attempt:` and remains provider-free; a check that cannot establish a
governed acquisition result uses `New verification unavailable:`. Compound labels
are policy-owned, the combined response is claim-capture-ineligible, and the
existing one-additional-acquisition maximum is unchanged. No actual-service
composed verification harness is added by this behavior.

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
