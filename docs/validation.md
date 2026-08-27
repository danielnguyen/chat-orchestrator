# Validation

Run these commands from the Chat Orchestrator repository root. They exercise local code and disposable development services; they must not be pointed at production data or credentials.

## HTTP smoke check

With Chat Orchestrator and Basic Memory Store running, use:

```bash
make smoke
```

The check sends a request to `POST /v1/chat`, verifies a JSON response and request ID, and, after a successful chat response, reads the corresponding trace from Basic Memory Store at `GET /v1/traces/{request_id}`. A bounded Chat Orchestrator failure response is accepted as a valid service response.

The default endpoints and keys can be overridden for a development environment:

| Variable | Purpose |
| --- | --- |
| `ORCH_BASE` | Chat Orchestrator base URL; defaults to `http://127.0.0.1:4361`. |
| `ORCH_API_KEY` | Value sent as the Chat Orchestrator `X-API-Key`. |
| `MEMORY_STORE_BASE_URL` | Basic Memory Store base URL; defaults to `http://127.0.0.1:4321`. |
| `MEMORY_STORE_API_KEY` | Value sent as the Basic Memory Store `X-API-Key`. |
| `CHAT_PAYLOAD_JSON` | Complete JSON body for the smoke chat request. |

Optional assertions can check summarized Cognitive Runtime output already present in the trace:

- `EXPECT_GOVERNANCE_STATUS`
- `EXPECT_GOVERNANCE_POSTURE`
- `EXPECT_PERSONA_STATUS`
- `EXPECT_PERSONA_DOMAIN`
- `EXPECT_PERSONA_RETRIEVAL_SCOPE_REASON`
- `EXPECT_RESTRAINT_STATUS`
- `EXPECT_RESTRAINT_POLICY`

Configure the corresponding Cognitive Runtime features in `api/.env`, restart Chat Orchestrator, and then set only the expectations needed for the smoke invocation. For example:

```bash
EXPECT_PERSONA_STATUS=included \
EXPECT_RESTRAINT_STATUS=included \
make smoke
```

## Operator troubleshooting

When a smoke check or chat request behaves unexpectedly:

1. Check Chat Orchestrator at `GET /healthz`.
2. Check that Basic Memory Store is reachable at its configured `GET /healthz` and accepts conversation, retrieval, message, and trace operations.
3. Confirm the model-provider URL and API key in `api/.env` match the running provider.
4. If Cognitive Runtime features are enabled, check its `GET /healthz` and the specific endpoint being consumed, such as `POST /v1/runtime/interaction-governance/evaluate`, `POST /v1/runtime/persona-containment/evaluate`, or `POST /v1/runtime/restraint/evaluate`.
5. Inspect the bounded request trace at Basic Memory Store `GET /v1/traces/{request_id}` for dependency status, routing, prompt-layer inclusion, fallback, and omission reasons.
6. Confirm that optional integrations are explicitly enabled in `api/.env`; their default disabled state should leave the normal chat path available.

Optional Cognitive Runtime guidance is designed to degrade safely when unavailable or malformed. Traces should contain bounded status and reason fields, not raw runtime responses or exception text.

## Deterministic replay

Run the versioned repository-local replay corpus with:

```bash
make replay-test
```

The replay suite executes the real `orchestrate_chat` path against deterministic boundary adapters. It covers successful composition and bounded degradation without depending on live providers. Replay snapshots are structural and exclude full prompts, provider responses, file contents, credentials, and unrestricted exception text.

Focused admission and durable-message checks are available with:

```bash
cd api
./.venv/bin/python -m pytest -q tests/test_orchestrate_flow.py
./.venv/bin/python -m pytest -q tests/test_orchestration_replay.py
./.venv/bin/python -m pytest -q tests/test_offline_fallback.py
./.venv/bin/python -m pytest -q tests/test_prompt_budget.py tests/test_prompt_budget_smoke.py
```

Run the evidence admission, local contract, advisory prompt, prompt-budget, and
orchestration regressions together with:

```bash
cd api
./.venv/bin/python -m pytest -q \
  tests/test_evidence_acquisition.py \
  tests/test_prompt_assembly.py \
  tests/test_prompt_budget.py \
  tests/test_orchestrate_flow.py
```

These tests prove CR shape admission without client opt-in, zero DSA operations
for not-applicable turns, strict local advisory-result validation, mutually
exclusive grounded and advisory prompt contracts, mandatory advisory-layer
budget survival, exact grounded JSON-schema capability checks, mandatory repair
layer budgeting, one-repair maximum, rejected-content isolation, structured
fallback prompt parity, and no advisory tools, actions, claims, callbacks, or
supported sources.

These suites also cover omitted-conversation acquisition and enforcement:
zero, eight, and nine-row Basic Memory Store pages; cursor-independent
completeness; all five Cognitive Runtime outcomes; nullable no-selection
responses; direct creation; selected-revision admission and retry; dependency
and revision-conflict barriers; supplied-ID isolation; runtime-disabled rolling
compatibility; and absence of candidate material from prompts and replay
snapshots. Run the replay corpus twice when checking determinism.

They also cover exact supplied-conversation retirement composition: the two
owner-scoped durable authorization reads around a Cognitive Runtime revision
snapshot, revision-bound admission inside the seven-day grace boundary,
strict over-horizon retirement, durable activity compare-and-set, close-result
reconciliation, persistent reservation cancellation and finalization rules,
the authoritative `non_current` disposition, and bounded create-new cleanup.

## Cognitive Runtime transport checks

Run the focused lifecycle and transport suite with:

```bash
cd api
./.venv/bin/python -m pytest -q tests/test_runtime_client.py
```

The focused suite verifies explicit startup and shutdown, bounded pool
configuration, repeated lifecycle calls, sequential and concurrent connection
reuse, transport invalidation, one-client replacement for later requests, and
the absence of transport-layer request replay. It also verifies that HTTP
status errors, malformed responses, response-validation failures, and
cancellation do not cause an unsafe retry or pool replacement.

Run the complete suite and deterministic replay with:

```bash
make dev-test
make replay-test
```

The pull-request composed smoke remains the actual-service compatibility check.
Transport reuse does not introduce concurrent policy calls, a composite runtime
endpoint, same-turn policy-result reuse, or an end-to-end latency-budget claim.

## Composed smoke check

Run the disposable multi-service topology with:

```bash
make composed-smoke
```

Current prerequisites are:

- Docker with Compose support;
- `git`, `curl`, `jq`, and `python3`; and
- sibling checkouts at `../basic-memory-store`, `../cognitive-runtime`, and
  `../data-source-aggregator`, with their local `main` branches updated to
  compatible current code.

The script performs its own compatibility preflight and stops before startup if a
sibling checkout is missing or incompatible. It builds the real Chat Orchestrator,
Cognitive Runtime, Data Source Aggregator, and Basic Memory Store HTTP services
together with PostgreSQL, Qdrant, a deterministic local OpenAI-compatible provider,
and a deterministic external-source fixture. The fixture supplies raw spreadsheet
cells below the real Google Sheets connector and local ICS documents to the real ICS
connector; it does not produce DSA envelopes, plans, sufficiency decisions,
manifests, traces, or policy responses. Service authentication and normal
owner/conversation scope remain enabled.

The evidence topology requires Chat Orchestrator current main at or after
`3802c3d30e9bb580a2d9597f521af52b7d6dc8dc`, Cognitive Runtime
`a61beb574a49f2d83f70008596c1183532b78f40`, Basic Memory Store
`e1d23cb1b1f3608efb4ee214ff5f03e5a55a5553`, and Data Source Aggregator
`e23f582e4aac32a12c7ad3c71278fc21e5697ea4`. The disposable DSA configuration
contains operator-owned material scope references for selected sources; those
values are emitted by the real DSA inventory and are never manufactured by the
external-source fixture.

Run only the focused evidence-acquisition proof with:

```bash
EVIDENCE_ACQUISITION_ONLY=1 make composed-smoke
```

That mode exercises real HTTP planning, inventory and capability discovery,
targeted retrieval, exact fetch, strategy-based hybrid acquisition across
comparison, contradiction-sensitive, and decision-support planning shapes,
configured-worksheet full-scope review, conservative partial-source coverage,
prompt retention, sufficiency, deterministic next-step selection, one bounded
changed-premise exact follow-up, provider gating, response qualification, durable
message/trace/manifest association, eligible claim support, response-first
acquisition history, privacy suppression, isolation, and compound new verification.
Positive records are created through the normal CO and BMS lifecycle. SQL reads are
used only to confirm durability; the two fail-closed history cases corrupt an
already-valid retained trace after that lifecycle has completed.

The formatted general-reasoning presentation proof additionally verifies that
newly executable ready plans reach the existing general reasoner and Cognitive
Runtime claim-support evaluator, without task-specific answer providers, tools,
retries, repair, or diagnostic reacquisition. Its partial-coverage case retains
usable evidence while preventing an unqualified complete conclusion.

Run only the server-owned admission and advisory composition proof with:

```bash
EVIDENCE_ADVISORY_ONLY=1 make composed-smoke
```

This mode pins Cognitive Runtime to
`a61beb574a49f2d83f70008596c1183532b78f40` and uses actual CO, CR, BMS, DSA,
PostgreSQL, Qdrant, provider, and source-fixture services. It proves an ordinary
not-applicable request makes one shape call and zero DSA calls; a
verification-dependent request enters acquisition without client opt-in and
returns a persisted, source-free advisory wrapper with zero tools, actions, or
claims; high-impact governance remains provider-blocked; primary failure and
fallback success share exact messages and fingerprints; and the established
grounded structured-evidence scenario remains unchanged. The pull-request
workflow runs this mode before the complete composed regression and records the
exact checked-out PR head.

Run only the conversation retirement composition proof with:

```bash
RETIREMENT_ONLY=1 make composed-smoke
```

This mode proves exact continuation inside the seven-day grace boundary; safe
idle durable closure and runtime revision fencing; conservative active,
contended, unavailable, and inconsistent outcomes; durable activity CAS after a
normal message append; reservation survival across Cognitive Runtime restart;
owner isolation; and bounded opportunistic cleanup that inspects no more than
four old open rows and closes no more than one before creating the current
conversation. It inspects disposable PostgreSQL and SQLite state to confirm
history retention, lifecycle, revision, and reservation outcomes. Direct
backdating and projection shaping exist only in the disposable harness.

Two focused final selectors are also available:

```bash
EVIDENCE_ACQUISITION_ONLY=1 EVIDENCE_SCENARIO=scope-references make composed-smoke
EVIDENCE_ACQUISITION_ONLY=1 EVIDENCE_SCENARIO=structured-answer-recovery make composed-smoke
```

`scope-references` exercises requested conjunctive narrowing, unanimous
unrequested derivation, missing material time scope, disabled malformed producer
metadata, selector mismatch, and privacy suppression through the real DSA and CR
contracts. `structured-answer-recovery` retains the existing adversarial
provider family, then proves structured primary and fallback success, a
provider-free unsupported route, successful one-call repair, exhausted repair,
and strict rejection of extra fields, non-extractive excerpts, and semantically
distinct universal, absence, contradiction, and full-compliance attempts. Its
history checks confirm that provider JSON, excerpts, malformed wording, validation
internals, and hidden scope metadata are not reconstructed.

Run only the server-owned immediate-history proof with:

```bash
HISTORY_FOLLOWUP_ONLY=1 make composed-smoke
```

This mode starts the same disposable topology—CO, CR, BMS, DSA, PostgreSQL,
Qdrant, the bounded OpenAI-compatible provider fixture, and the external-source
fixture—with history follow-ups disabled by default. It creates durable governed
answers through the actual services, then recreates only the orchestrator with
history enabled. PostgreSQL, BMS, CR, DSA, Qdrant, and the provider remain running,
so successful current-turn-only follow-ups prove that neither a client cache nor
orchestrator process state supplies the previous answer. The hosted workflow
checks out BMS at `e1d23cb1b1f3608efb4ee214ff5f03e5a55a5553`, builds that service,
and first probes both the unchanged v1 and strict v2 internal response shapes.

The scenarios cover chained acquisition explanations with the same private root,
chained support explanations followed by one governed bare fresh verification,
CO-only restart durability, ordinary-answer lineage termination, and malformed,
unsupported, wrong-kind, cross-owner, cross-conversation, surface-mismatched,
missing, recursive, and association-invalid stored lineage. They also retain
canonical support and acquisition wording without a model,
four natural paraphrases using one strict `intent_classifier` call to the configured
`gpt-5-mini` route, CR policy, BMS newest-response resolution, provider-free
historical rendering, classifier degradation and local-only denial, CR confidence
boundaries, no backward scan, one explicitly requested governed verification, and
unauthenticated BMS rejection. Classifier calls are counted separately from answer
provider calls: pure history has no answer-provider call, while explicit fresh
verification has one classifier call and one governed answer call after successful
history resolution. Pure history proves exactly one CR history-policy call, one
BMS v2 resolution call, zero answer-provider calls, and zero DSA calls. Traces
retain only the closed resolution source, dereference count, and lineage result;
public responses, provider prompts, CR and DSA payloads, and persisted traces
contain no lineage or root identity. Provider diagnostics retain only bounded
structural fields and the existing disposable normalized-message fixture data needed to prove the strict
schema, token limit, zero tools, fixed instruction, current-user-only input, and
absence of history or identifiers.

Historical support and acquisition follow-ups now render human-readable source
names, safe connector locations, contributions, and practical limitations from the
validated record saved with the original answer. Older records without source
summaries use a deterministic bounded fallback, while privacy-suppressed or malformed
metadata cannot expose source identifiers. These explanations do not run a fresh
search or provider call and do not expose raw acquisition diagnostics.
“What did you check?” lists only sources actually checked; coverage and gap answers
show considered-only, unavailable, or disabled sources separately as not covered.

Run only the distinct-client owner-memory proof with:

```bash
DISTINCT_CLIENT_MEMORY_ONLY=1 make composed-smoke
```

This mode uses three distinct clients under one owner in separate conversations. It
proves owner-scoped authorized memory retrieval for an allowed persona, exclusion
for a blocked persona, truthful current-client and source-client provenance, one
canonical durable fact, no Cognitive Runtime or persona-overlay copy, and isolation
from a different owner. Each request contains only its current turn and enters the
normal Chat Orchestrator path, so no adapter synchronization or client cache supplies
the shared memory.

Run only the runtime admission composition proof with:

```bash
RUNTIME_ADMISSION_COMPOSITION_ONLY=1 make composed-smoke
```

This mode creates one fresh open conversation, delays the winning provider
response once, and sends a distinct-client request while the first runtime turn
is active. It verifies one provider-backed winner and one bounded loser, exactly
one durable current-user message and one assistant response, equality between
the admitted input UUID and durable user-message UUID, truthful winning client
and surface provenance, no loser message, provider, claim, or action side
effect, one conversation, and a final idle runtime thread at revision two.

The proof uses Basic Memory Store at or after
`e1d23cb1b1f3608efb4ee214ff5f03e5a55a5553` and Cognitive Runtime at or after
`a61beb574a49f2d83f70008596c1183532b78f40`. It demonstrates overlap within the
actual disposable services. It does not demonstrate completed-response replay
across a fresh HTTP request or process restart.

Run only the omitted-conversation continuation proof with:

```bash
OMITTED_CONTINUATION_ONLY=1 make composed-smoke
```

This mode verifies six actual-service boundaries. A fresh owner with zero
candidates creates one real conversation without selecting another owner's
eligible thread. One idle candidate resumes across a distinct current client
and surface while preserving historical provenance and returning the shared
thread to idle at revision four. Two eligible candidates clarify with a null
conversation ID and no new durable or runtime state. An active candidate waits
without creating a losing session, message, provider call, action, claim, or
trace. Nine open conversations produce an incomplete bounded set and clarify
without pagination or side effects. Non-resume responses disclose no candidate
identity, client, surface, or timestamp.

The proof checks PostgreSQL conversation, message, trace, and claim counts;
Cognitive Runtime SQLite session, turn, event, surface, state, and revision
facts; request-scoped provider counts; current client and surface provenance;
and owner isolation. The workflow and local guard require Cognitive Runtime at
`a61beb574a49f2d83f70008596c1183532b78f40`, Basic Memory Store at
`e1d23cb1b1f3608efb4ee214ff5f03e5a55a5553`, and Chat Orchestrator at
`3802c3d30e9bb580a2d9597f521af52b7d6dc8dc`.

The distinct-client owner-memory proof does not by itself establish live-thread
continuation, automatic conversation selection, Telegram-to-Alexa handoff,
timing, presence, pause/resume, or contention. The omitted-conversation proof
does not establish a multi-page snapshot, universal presence permission,
replacement traversal, or completed-response replay across restart.

The focused mode and the complete regression both reset provider, DSA audit,
external-source, feature-toggle, and disposable conversation state. This is
deployment-equivalent composed proof for the repository contracts; it is not a
reconstruction of the original Telegram deployment or evidence for its historical
runtime state. Project checklist verdicts, production deployment, and Telegram
production proof remain separate post-merge work.

Normal pull-request validation uses `all`: it runs every established focused
scenario, both final recovery families, and only then the complete composed-smoke
regression. Standalone dispatches are diagnostic supplements and do not replace
that normal sequence.

The focused fixture never contacts Google or another mutable source system. Source
configuration, dummy non-secret credential structure, DSA audit state, CR state,
PostgreSQL data, and Qdrant data are disposable and removed during cleanup. A
failed hosted run may upload only filtered service lifecycle/access lines and
container status; full prompts, provider text, source content, credentials, and raw
exceptions are excluded.

Permanent scenario output is limited to fixed completion markers, exact bounded
counts, and stable assertion labels. Privacy cases must not print material scope
values, source identifiers or references, excerpts, candidate JSON, full prompts,
provider responses, or unrestricted dependency errors. This validation proves
runtime behavior only; project conformance remains subject to its separate
review process.

## Artifact composed smoke check

## Situated presence checks

Run the focused client, surface/style, prompt, budget, orchestration, replay, and
fallback coverage with:

```bash
cd api
./.venv/bin/python -m pytest -q tests/test_runtime_client.py
./.venv/bin/python -m pytest -q tests/test_style_envelope.py
./.venv/bin/python -m pytest -q tests/test_prompt_assembly.py
./.venv/bin/python -m pytest -q tests/test_prompt_budget.py tests/test_prompt_budget_smoke.py
./.venv/bin/python -m pytest -q tests/test_orchestrate_flow.py
./.venv/bin/python -m pytest -q tests/test_orchestration_replay.py
./.venv/bin/python -m pytest -q tests/test_offline_fallback.py
```

The tests cover conservative surface derivation, exact compact runtime payloads,
strict branch, posture, gate, confidence, and reason coherence validation;
suppression-only fallback; monotonic post-profile style clamping that preserves
stricter resolved values; mandatory prompt ordering and budget survival; evidence
rebuild; and identical primary/fallback messages with one policy call. The persisted
replay corpus remains unchanged and is run twice to prove deterministic output.

Run only the actual-service situated-presence proof with:

```bash
SITUATED_PRESENCE_ONLY=1 COMPOSED_RESTRAINT_ENABLED=true make composed-smoke
```

The proof uses Cognitive Runtime at or after
`92a8600f2cb99ed98d10721d23c8b65f3903a857` and Chat Orchestrator at or after
`f79034e32bfe6081de1af915779bc0cd157a781a`. It verifies private playful,
ordinary-question, tense tactical, generic steadying, public/constrained, and
provider-fallback cases against actual services. Assertions include governance,
restraint, and situated policy events; prompt-layer delivery; bounded style;
identical fallback fingerprints; durable trace and message counts; final idle
thread revision; and no unintended action authority.

This validation proves delivery and compliant fixture outcomes, not universal
semantic detection of arbitrary provider prose. It adds no concurrent runtime
policy calls, persistent presence state, complete timing decision, tunable behavior,
watch delivery, or proactive silence scheduling.

Run the file-ingestion and retrieval topology with:

```bash
make artifact-composed-smoke
```

Current prerequisites are:

- Docker with Compose support;
- `git`, `curl`, `jq`, and `python3`; and
- a sibling checkout at `../basic-memory-store`, with its local `main` branch updated to compatible current code.

This topology adds MinIO and exercises the Basic Memory Store artifact lifecycle, derived text retrieval, Chat Orchestrator prompt assembly, provider fallback, source filtering, and privacy suppression. All containers, databases, vectors, objects, and provider calls are disposable.

## Privacy and safety expectations

Validation output and traces must remain bounded:

- never use production provider keys, databases, object stores, or user data;
- do not expose full prompts, provider responses, credentials, presigned URL secrets, or raw dependency payloads;
- keep owner and conversation scopes isolated;
- omit incomplete, unrelated, or privacy-suppressed file content;
- keep fallback within the effective local-only policy; and
- treat malformed or unavailable optional dependencies as bounded degradation rather than a source of raw diagnostics.

The composed commands clean up their Docker resources on exit. If a run is interrupted, use the matching Compose file to remove its containers and volumes before retrying.
