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

Run only the focused evidence-acquisition proof with:

```bash
EVIDENCE_ACQUISITION_ONLY=1 make composed-smoke
```

That mode exercises real HTTP planning, inventory and capability discovery,
targeted retrieval, exact fetch, hybrid comparison, configured-worksheet review,
prompt retention, sufficiency, deterministic next-step selection, one bounded
changed-premise exact follow-up, provider gating, response qualification, durable
message/trace/manifest association, eligible claim support, response-first
acquisition history, privacy suppression, isolation, and compound new verification.
Positive records are created through the normal CO and BMS lifecycle. SQL reads are
used only to confirm durability; the two fail-closed history cases corrupt an
already-valid retained trace after that lifecycle has completed.

The focused fixture never contacts Google or another mutable source system. Source
configuration, dummy non-secret credential structure, DSA audit state, CR state,
PostgreSQL data, and Qdrant data are disposable and removed during cleanup. A
failed hosted run may upload only filtered service lifecycle/access lines and
container status; full prompts, provider text, source content, credentials, and raw
exceptions are excluded.

## Artifact composed smoke check

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
