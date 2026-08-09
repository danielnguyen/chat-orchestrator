# Runtime behavior

This document describes the current `POST /v1/chat` orchestration path and its integration boundaries.

## Request lifecycle

For each chat request, Chat Orchestrator:

1. validates a supplied conversation or resolves an omitted conversation through Basic Memory Store;
2. identifies the final user-role entry as the current user turn;
3. when Cognitive Runtime is configured, admits that turn before persistence or other turn-scoped work;
4. resolves enabled turn-scoped policy from the admitted runtime session and turn;
5. persists the current user turn under the same UUID admitted by Cognitive Runtime;
6. resolves the active profile and retrieves bounded conversation context;
7. optionally retrieves external read-only context from Data Source Aggregator;
8. assembles prompt layers within the configured budget;
9. selects a model and provider through routing policy;
10. invokes the provider or a policy-compatible fallback;
11. executes an authorized connector action when the capability flow permits it;
12. persists the assistant message and a bounded request trace; and
13. returns the answer, routing status, public sources, and any pending action.

Optional integrations are non-authoritative unless their owning policy explicitly supplies a decision. Registration or availability alone does not grant an action permission.

## Conversation resolution

When a request supplies `conversation_id`, Chat Orchestrator performs one exact
Basic Memory Store lookup scoped to the request owner. It does not first invoke
the same-client resolver. Only an exact open conversation proceeds. Missing,
owner-mismatched, closed, superseded, malformed, or unavailable targets return
one bounded failure before message append, retrieval, profile resolution,
Cognitive Runtime calls, provider calls, capability or connector work, and trace
persistence. That response does not reveal whether the conversation exists, its
lifecycle state, or retained conversation content, and a superseded replacement
is not followed.

The conversation's originating client may differ from the current request
client. A valid continuation appends new messages with the current `client_id`
and current surface metadata while leaving historical provenance unchanged.

When `conversation_id` is omitted and Cognitive Runtime is configured, Chat
Orchestrator derives a timezone-aware UTC activity cutoff from its existing
1,800-second freshness horizon. It makes one owner-wide Basic Memory Store
request for open conversations updated at or after that cutoff, with a limit of
nine. Basic Memory Store applies this mechanical durable-activity filter before
the limit. Cognitive Runtime accepts at most eight candidates, so zero through
eight rows in the filtered population form a complete bounded set; nine rows
make the submitted first eight explicitly incomplete. The Basic Memory Store
cursor is validated but is not used as completeness evidence, and Chat
Orchestrator does not paginate or select by result order.

Each candidate sent to Cognitive Runtime contains only its durable conversation
ID, open lifecycle state, and durable update time. Cognitive Runtime combines
those facts with its existing thread state and returns `resume`, `create_new`,
`clarify`, `wait`, or `decline`. A resume uses the exact selected conversation
and binds the returned thread revision into atomic turn admission. Create-new
makes one direct durable conversation creation request and then admits without a
selected revision. Clarification, wait, decline, and mandatory dependency
failures return a null conversation ID before admission, message persistence,
profile or retrieval work, providers, capabilities, actions, claims, or traces.
Adapters store and reuse only non-null conversation IDs.

Selection does not use semantic retrieval, a provider or model, durable row
order, raw recency alone, titles, content, originating or current client,
current surface, message counts, embeddings, or adapter state. The surface is
selection context only and is not treated as a permission signal. Filtering the
candidate population by durable activity is candidate-set hygiene, not a
selection decision: Cognitive Runtime still evaluates eligibility and
staleness independently, and raw recency cannot choose a conversation. The
fixed 1,800-second freshness interval preserves the bounded compatibility
horizon; it does not establish a general presence policy.

When Cognitive Runtime is not configured, omitted IDs retain the same-owner,
same-client rolling resolver as explicit compatibility behavior. That path is
not cross-surface selection-protected. Supplied-ID validation remains unchanged.

## Turn admission and current-user persistence

When Cognitive Runtime is configured, Chat Orchestrator allocates one durable
message UUID for the final user-role entry and starts the runtime turn with that
UUID before current-user persistence, profile resolution, retrieval, provider,
capability, connector, action, claim, or trace work. Contended, unavailable, or
malformed admission stops with one bounded response and performs none of those
operations. A timeout, transport failure, or HTTP 502, 503, or 504 receives at
most one exact admission retry with the same request and message identities.

After admission, turn-scoped governance, persona containment, relationship
policy, and restraint use the admitted runtime session and turn identifiers.
Chat Orchestrator then appends only the final user-role entry; earlier user and
assistant entries remain bounded request context and are not re-persisted. The
Basic Memory Store append uses the admitted UUID. An ambiguous eligible append
failure receives one exact retry with the same UUID and payload. If durability
still cannot be confirmed, the runtime turn is abandoned best-effort and no
retrieval, provider, capability, action, claim, or trace work proceeds.

When Cognitive Runtime is not configured, the compatibility path still persists
only the final user-role entry and continues without fabricating an admission.
That path is not admission-protected. Assistant appends remain server-generated
and are not silently retried.

Each HTTP request still receives a fresh server-owned request ID. Completed
response replay across a resend or orchestrator restart is not provided. A
request without a user-role entry retains its existing compatibility behavior
without an invented durable current-user identity.

## Cognitive Runtime transport lifecycle

When Cognitive Runtime is configured, application startup opens one owned HTTP
client with a bounded connection pool. All Cognitive Runtime requests reuse that
client and its keep-alive connections. The default pool permits at most 20
connections, retains at most 10 idle connections, and expires idle keep-alive
connections after five seconds. Startup creates the client without making a
Cognitive Runtime request, and application shutdown closes the pool.

The generic transport sends each request once. A timeout or transport failure
invalidates and closes the affected client without replaying that request. A
later separate request may create one replacement client while the application
is still running. HTTP status errors, malformed responses, and response-policy
validation do not trigger transport replacement or replay. Exact retries owned
by an orchestration operation remain outside the generic transport.

When Cognitive Runtime is disabled, no Cognitive Runtime HTTP client is opened.
Memory Store, Data Source Aggregator, provider, source, and health-check clients
retain their existing transport behavior. Connection reuse alone does not
establish an end-to-end latency guarantee, add concurrent policy evaluation, or
authorize policy-result caching.

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

When governed evidence acquisition is disabled, the compatibility path still
requires request-level opt-in through `external_context_enabled` or an enabled
`external_context` object. When governed evidence acquisition is enabled and
its server dependencies are available, Cognitive Runtime owns admission: the
client fields supply optional source, scope, sensitivity, exact-reference, and
result-bound inputs, but do not decide whether evidence governance applies. An
effective `local_only` policy always suppresses the external call.

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
does not make evidence planning universal. For an eligible admitted normal turn,
Chat Orchestrator asks Cognitive Runtime for the evidence shape even when the
client supplied no external-context opt-in. An effective `local_only` policy,
pending capability continuation, matched capability path, or retrieval
suppression still prevents entry.

For an eligible normal chat request, Chat Orchestrator uses the existing
interaction-governance result and first asks Cognitive Runtime to derive a broad
evidence shape. A `not_applicable` result returns to ordinary answering without
listing, searching, fetching, or expanding DSA sources. A derived result then
reads the governed DSA source inventory, adapts the neutral source capabilities,
and asks Cognitive Runtime to compile an evidence plan. Governed execution
proceeds for a derived `targeted_lookup` whose ready plan selects only
`targeted_retrieval` or `exact_fetch`, and for the bounded hybrid comparison and
bounded exhaustive-review contracts described below. Bounded trace metadata
distinguishes `client_request`, `evidence_policy`, and `not_requested` activation
without storing the task text.

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
Cognitive Runtime evaluates those facts. An insufficient or unknown result never
authorizes the requested factual conclusion. Most unsupported, exhaustive,
absence-sensitive, contradiction-sensitive, historical, decision-support,
clarification, malformed, or dependency-failed paths remain provider-free. Their
bounded response names actual material requirement gaps and distinguishes failed,
filtered, truncated, unsupported, unavailable, unknown, missing, partial,
excluded, not-attempted, and unresolved-contradiction outcomes.

A sufficient result enters grounded evidence mode and permits one governed provider call with no capability
tools. That call receives a required prompt-budgeted response contract and must
return only a strict JSON object containing a closed conclusion disposition and
one to eight distinct source references with exact extractive excerpts. Each
reference must identify one prompt-retained external item, and each
whitespace-normalized excerpt must be a case-preserving, token-bounded substring
of that item. Returned-but-not-retained references, forged references,
paraphrases, case changes, reordered text, duplicate references, and malformed
JSON fail validation. There is no content repair, retry, or content-triggered
fallback call.

Chat Orchestrator renders the conclusion, neutral excerpt attribution,
limitation disclosure, and task-specific scope boundary from deterministic
templates. Arbitrary provider prose is never part of a governed evidence
answer. A malformed candidate produces a degraded, user-safe response without
raw provider content while retaining the truthful acquisition manifest and
provider-call count. Transport failures retain their existing fallback behavior
and reuse the same structured contract and prompt. A
`sufficient_with_limitations` result adds a deterministic disclosure derived
from optional requirement evaluations, plan limitations, and bounded trusted
inventory counts when those counts are established. Distinct causes are
deduplicated, sorted, and capped; private source identifiers and provider output
do not enter the disclosure.

Every governed successful targeted answer receives a stable statement that only
the targeted sources were checked. This applies to semantic retrieval and exact
fetch independently of provider wording.
Hybrid comparison facts are satisfied only when every planned source contributed
targeted evidence, one successful expansion, and prompt-retained evidence, with
at least two expanded source scopes surviving prompt budgeting. There is no
targeted-only fallback. Every governed successful comparison receives a stable
selected-source and bounded-context statement.
The provider candidate cannot select or upgrade the plan, acquisition facts,
sufficiency status, limitation disclosure, answer constraints, source authority,
or scope boundary. Reapplying the final-answer policy does not duplicate or
cross-apply policy-owned paragraphs.

For the exact Cognitive Runtime combination `targeted_lookup`, insufficient or
unknown evidence, unsupported conclusion withheld, and provider allowed, Chat
Orchestrator instead enters advisory evidence mode. Provider permission is not
conclusion permission. CO rebuilds the prompt once with mandatory advisory
guidance, removes the grounded JSON contract and external evidence context, and
makes one natural-language provider attempt with no tools. A transport fallback
receives the identical messages and prompt fingerprint, also with no tools.

After privacy enforcement, CO places the provider body between fixed statements
that verification was not established and that the body is only working
direction. An empty body receives a neutral next-verification suggestion. The
exact wrapped answer is degraded, source-free, bound to the acquisition manifest,
and persisted. Advisory mode exposes no capability descriptors to the provider,
parses or executes no capability request, creates no pending action or trusted
claim, appends no memory callback, and runs no claim capture or response-action
rewrite. The prompt trace retains only bounded mode, layer, tool, rebuild, and
fingerprint facts—not provider prose. CR, DSA, and next-step dependency failures
cannot be converted locally into advisory permission. Only configured DSA sources
can be acquired; this mode adds no connector or verification guarantee.

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
support.

For a valid governed candidate with exactly one selected excerpt and a
non-mixed disposition, Chat Orchestrator may create a narrow trusted
single-claim candidate after policy rendering and privacy enforcement. Claim
calibration receives only the policy-owned conclusion anchor and that validated
external-source reference; provider JSON, unselected retained evidence, and
merely acquired evidence are not claim support. An opaque reference that does
not fit the bounded claim identifier contract is represented by a deterministic
SHA-256-derived identifier. Multiple excerpts, mixed dispositions, compound
responses, privacy suppression, malformed candidates, actions, callbacks, and
briefs remain claim-ineligible. Existing artifact claim capture is unchanged.

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
one validated file or external reference actually used to support the claim;
source inventories, acquisition attempts, unselected returned or retained
external references, policy paragraphs, and sufficiency details are not copied
into claim support or calibration.

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
ordinary chat path without DSA acquisition when governed admission owns the turn.
Briefs, capability and action flows, pending-action
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
fallback calls remain zero for those deterministic responses. The one exception
is the exact low-risk targeted advisory combination described above: the
conclusion remains withheld while bounded provider guidance is allowed.

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
verification attempt:` when Cognitive Runtime grants advisory guidance, retaining
the fixed unverified wrapper; a check that cannot establish a
governed acquisition result uses `New verification unavailable:`. Compound labels
are policy-owned, the combined response is claim-capture-ineligible, and the
existing one-additional-acquisition maximum is unchanged. Provider candidate
JSON is neither reconstructed nor retained by history or compound rendering.

## Server-owned immediate-history follow-ups

`HISTORY_FOLLOWUP_ENABLED=false` keeps the established quoted and request-supplied
history behavior above unchanged. When enabled, the current user turn may enter a
server-owned immediate-history path without the client resending the previous
assistant response. `INTENT_CLASSIFIER_TIMEOUT_MS` defaults to 3000 milliseconds
and is bounded like other dependency timeouts.

Exact quoted targets are parsed first and remain on the separately bounded
exact-reference path. Exact canonical support, acquisition-checked, coverage,
gaps, and re-verification forms then use a deterministic fast path with no model
call. Only short, structurally plausible unresolved wording is
classifier-eligible; ordinary topic questions, advice about what the user should
check, questions about what another person checked, generic checksum
instructions, and materially long turns do not call the classifier.

Eligible paraphrases may make at most one LiteLLM call through the logical route
`intent_classifier`, initially mapped to the cloud model `gpt-5-mini`. This route
is independent of answer routing, so configuring a future local model changes no
durable contract. The request has no tools, uses a strict JSON-schema response
format and a small output-token limit, and carries only a bounded current user
turn plus a fixed classification instruction. It never receives previous
assistant text, retained records, evidence, source identity, owner or conversation
identity, profile overlays, hidden reasoning, or the answer prompt.

The classifier runs only after effective profile and pre-classification privacy
signals are available. A cloud route is unavailable to local-only requests or
profiles, disallowed providers, and high or restricted surface sensitivity. A
future local route may run under local-only policy. Missing or malformed route
configuration, unavailable LiteLLM, timeout, transport failure, malformed output,
and policy-disallowed classification fail closed to one narrow clarification.
There is no repair call, semantic retry, alternate classifier, or answer-model
fallback.

Every valid deterministic or classifier candidate receives one additional
interaction-governance evaluation from Cognitive Runtime. Chat Orchestrator
strictly validates the response scope and complete closed history policy.
Cognitive Runtime owns confidence thresholds, ambiguity, target-mode disposition,
and permission to attempt immediate history. Rejected and not-applicable results
preserve ordinary handling. Clarification is provider-free. A classifier claiming
an explicit reference without a parsed exact quote cannot select a target and
receives clarification.

Only an accepted immediate-previous policy with history lookup allowed makes one
`POST /v1/internal/immediate-history/resolve` call. The request contains only the
v2 schema version, current request, owner, conversation, surface, and explanation
kind. There is no automatic v1 fallback. Basic Memory Store owns selection of the
single newest durable assistant response and its exactly associated support or
acquisition record. Chat Orchestrator does not send prior response bytes or record identifiers, call the
legacy acquisition resolver for this path, list claim records, fetch traces,
search semantically, scan backward after a missing or invalid newest record, or
retry with another explanation kind.

The strict v2 response identifies whether the record was resolved directly or
through one BMS-owned root-lineage dereference. Successful responses include only
the minimal `history-root-lineage.v1` object: schema version, root assistant
message UUID, and support or acquisition kind. Chat Orchestrator validates this
envelope and its complete response invariants, but does not choose, construct,
modify, or dereference a root. The explanation kind supplied by Cognitive Runtime
remains authoritative.

Resolved support records and acquisition manifests use the existing deterministic
renderers. Support rendering accepts approved retained file, governed
external-source, tool-output, and integration-event reference shapes, but exposes
only closed structural facts such as evidence category, claim class, confidence,
strength, freshness, and bounded limitations. Acquisition rendering continues to
use the strict manifest projection for checked, coverage, and gaps questions.
Neither renderer exposes identifiers, URLs, excerpts, source names, raw summaries,
provider prose, or hidden reasoning. Pure historical output is persisted exactly
as returned with `selected_model = "not_called"`, current request identity, and
`response_kind = "claim_explanation"`. Only a successful pure history response
passes the unchanged BMS lineage through the dedicated assistant-message append
field. It is never placed in ordinary metadata or returned publicly. BMS stores
and validates it privately. A rejected lineaged append is not retried without
lineage and produces a bounded non-durable dependency response rather than a
false persistence success.

Fresh verification begins only when Cognitive Runtime explicitly permits it after
one valid immediate record resolves and that record supplies an exact bounded
target paragraph. Entry does not require client external-context opt-in. The
existing governed evidence path then runs once; historical
evidence is not reused as fresh evidence. Support-backed compounds use
`Original support:`. Acquisition-backed compounds retain `Original acquisition:`,
and the existing `New verification:`, `New verification attempt:`, and `New
verification unavailable:` labels remain policy-owned. If the governed path is
unavailable, the combined provider-free response says so rather than silently
returning only history or pretending a new check occurred.
Compound verification answers and ordinary answers never inherit an older root
lineage. A later history request therefore addresses the compound or ordinary
newest assistant response under BMS's direct-first, no-record-only rule. A bare
fresh-verification request after an acquisition-history explanation remains out
of scope because Cognitive Runtime classifies that wording as support; CO does
not infer acquisition from stored lineage.

When enabled, the request trace adds one bounded `history_followup` summary with
feature, deterministic match, eligibility, logical route, classifier status and
call count, closed candidate projection, confidence band, Cognitive Runtime
policy status and call count, lookup and clarification permissions, explicit
verification flags, Basic Memory Store status and call count, resolved record
kind, closed resolution source, zero-or-one lineage dereference count, bounded
lineage result, render status, fresh-verification entry status, and
historical-rendering answer-provider call count. It contains no turn text, prompts, raw classifier
output, lineage object, root assistant message ID, exact confidence reasoning,
record or source identifiers, provider
responses, or unrestricted exceptions. Classifier accounting is separate from
answer-provider `model_calls`.

This path does not implement vague arbitrary-history search, backward scanning,
provider reconstruction of history, classifier-selected records, automatic
verification, client-cache authority, or cross-device active-thread handoff.
Production deployment and deployed-client proof remain separate validation work.

## Integration boundaries

## Situated presence during response assembly

When Cognitive Runtime, interaction governance, and restraint are all enabled,
Chat Orchestrator evaluates situated presence once for the admitted current turn.
It derives visibility and constraint only from the request's typed surface context,
sends compact governance and restraint projections without message text, and
strictly validates the complete deterministic `situated-presence.v1` result against
the request projections, including branch gates, posture, confidence, and ordered
reasons. Private desktop, mobile, Telegram, and voice categories may be normal.
Shared, public, preview, active-task, no-expansion, missing, malformed, and unknown
context is treated conservatively. Spoken output alone is not a constraint.

The result is applied after profile and request style resolution. It monotonically
caps playfulness and challenge without raising either value, and preserves resolved
directness, warmth, and technical density. Tactical, brief, and minimal postures may
shorten sentences or remove analogies as suppression-oriented constraints. A
mandatory `Situated presence guidance:` prompt layer follows restraint and precedes
privacy guidance. That layer survives optional prompt-budget reductions and evidence
prompt rebuilds. If mandatory prompt content cannot fit, the existing bounded
prompt-budget failure is used instead of dropping the policy.

The provider remains responsible for natural wording. Allowed playfulness is
optional and current-turn-grounded, and brief attunement permits only a generic
steadying acknowledgment. Tactical, humorless, minimal, and silence-preferred
results remain response-shaping constraints rather than canned responses. No
final-answer classifier, deterministic rewrite, joke, sympathy phrase, or second
provider call is added. Provider fallback reuses the identical assembled messages
and does not reevaluate situated presence.

If the mandatory situated-presence inputs or runtime response are unavailable or
malformed, CO uses a local suppression-only envelope: no commentary, humor,
attunement, or optional challenge, with a minimal posture. This fallback cannot
grant a positive permission. Runtime-disabled requests, or requests without both
upstream policies enabled, retain existing behavior and make no situated-presence
call. Existing evidence, privacy, capability, confirmation, action, claim, and
dependency-owned forced response content remains authoritative.

Trace evidence records activation, conservative surface classification, bounded
gates and posture, policy versions, fallback status, and pre/post style clamp
fields. It contains no user text or provider response. Transport reuse does not by
itself prove semantic compliance for arbitrary provider prose, and this slice does
not add persistent presence state, a complete timing policy, tuning, watches, or
anti-annoyance scheduling.

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
