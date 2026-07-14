# Adding an action connector

## What an action connector is

An action connector supplies integration-specific mechanics to the shared permissioned-action lifecycle. Cognitive Runtime (CR) owns capability policy and decisions; Chat Orchestrator (CO) owns orchestration, connector lookup, and connector execution. Merely registering a connector does not authorize it or make it eligible to run.

The lifecycle is:

```text
user request -> CR capability match and policy -> confirmation when required -> authorized dispatch -> one connector execution -> optional policy-required verification -> CR action summary
```

Start with the action you want CCP to perform, then define its bounded identity, policy, arguments, effect, and observable outcomes before writing integration code.

## Before you begin

Be able to answer all of these questions:

- What single bounded capability and target will the connector serve?
- Which external operation performs the effect?
- How do external results map to completed, partially executed, failed, or unknown?
- Is fresh external state needed before execution?
- If supported, how can the final state be verified?
- Which surfaces and personas may expose the capability?

Resolve unclear answers before implementation. In particular, do not let adapter behavior become an implicit policy decision.

## Files and ownership

| Concern | Owning repository | Typical file or symbol | What belongs there |
| --- | --- | --- | --- |
| Canonical capability metadata and matching | Cognitive Runtime | `api/services/capability_authorization.py`, `RegisteredCapability` | Capability ID, display metadata, domain, operation kind, match phrases, and allowed surfaces and personas. |
| Risk, authority, confirmation, dispatch, and verification policy | Cognitive Runtime | `decide_action_authority`, `decide_action_flow` | Eligibility, risk, authority, challenge state, dispatch permission, and whether post-effect verification is needed. |
| Action-summary contract | Cognitive Runtime | `ActionSummaryRequest`, `compose_action_summary` | Bounded execution and verification outcome validation, summary wording, and summary recording. |
| Matching orchestration registration | Chat Orchestrator | `CapabilityEntry`, `CapabilityPolicyShape` in `api/services/capabilities.py` | Provider tool identity, bounded argument schema, executor binding, local exposure gates, authorization and world-state constraints, and a shape that must agree with CR. |
| Connector implementation | Chat Orchestrator | `ActionConnector` in `api/services/action_connectors.py` | Typed integration mechanics described below. A conventional implementation path is `api/services/<integration>_action_connector.py`. |
| Explicit connector lookup | Chat Orchestrator | `ActionConnectorRegistry`, `orchestrate_chat` | One deterministic registry assembled from explicitly constructed connector instances. |
| Shared and composed coverage | Chat Orchestrator | `api/tests/test_capabilities.py`, `api/tests/test_orchestrate_flow.py` | Policy binding, continuation, dispatch, execution, verification, summary, replay, degradation, and privacy behavior. |
| Connector mechanics | Chat Orchestrator connector module | Connector methods and constructor-injected adapter | Normalization, deterministic continuation description and restoration, availability, optional revalidation, one execution, optional verification, bounded external parsing, and safe fallback presentation. |

Do not put any of the following in a connector:

- authority decisions or confirmation acceptance;
- dispatch permission or selection of whether verification is needed;
- execution retries or action-summary submission;
- arbitrary metadata; or
- credentials, endpoints, raw external responses, prompts, provider output, or exception text in traces.

## Implementation sequence

### Step 1 — Define the canonical capability in Cognitive Runtime

Add the canonical CR capability record and matching behavior. Define its:

- capability ID, display name, and description;
- domain and operation kind;
- risk and confirmation properties;
- allowed surfaces and personas; and
- reversible, dry-run, verification, and audit properties.

CR is authoritative. The matching CO registration must agree with these facts and cannot weaken or override CR policy.

### Step 2 — Add the matching Chat Orchestrator capability registration

Add a bounded `CapabilityEntry` and `CapabilityPolicyShape` in `api/services/capabilities.py`. Supply the provider tool name, bounded argument schema, `action_connector` executor binding, authorization and world-state constraints, local surface and persona gates, and a policy shape consistent with CR.

This registration mirrors canonical CR facts so CO can validate provider selection and run the shared lifecycle. Caller-provided metadata and connector presence cannot override it.

### Step 3 — Implement the connector

Use `api/services/<integration>_action_connector.py` unless the integration already has an approved module. Implement the structural `ActionConnector` protocol in this call order:

1. `capability_id` is the exact registered capability identity. It is static application identity, not an environment-derived value.
2. `effect_mode` is `simulated`, `live`, or `None` when the connector cannot supply a mode. It does not grant execution permission.
3. `revalidation_spec` identifies the bounded revalidation implementation, or is `None` when no fresh-state read is needed. It does not contain authority or trust policy.
4. `presentation` is a `ConnectorPresentation` containing non-empty, bounded, user-safe fallback wording for every `ConnectorOutcome`. The shared lifecycle chooses the actual outcome.
5. `normalize_arguments(arguments)` strictly validates a bounded mapping and returns `ConnectorArguments`. It is deterministic, side-effect free, and rejects extra or malformed values.
6. `describe_continuation(arguments)` returns a `ConnectorContinuationDescription` containing only a safe target and safe argument-specific confirmation text. It performs no external call and does not decide whether confirmation is needed.
7. `restore_continuation(description)` reconstructs the same normalized arguments from that bounded description. It performs no external call and must round-trip through normalization and the argument digest.
8. `check_availability(request)` receives a `ConnectorAvailabilityRequest` containing only already-selected `ConnectorClaimRef` values. It is a local, side-effect-free check that returns a bounded `ConnectorAvailabilityResult`; it receives no surface or persona input and makes no authority decision.
9. `revalidate(request)` returns a `ConnectorRevalidationResult`. When configured, it may make at most one bounded external read and must immediately convert observations into typed bounded fields.
10. `execute(request)` receives one `ConnectorExecutionRequest`, performs the consequential effect at most once, and returns one `ConnectorExecutionResult`. Completed, partial, failed, and unknown outcomes are never retried by the shared lifecycle.
11. `verify(request)` receives the normalized arguments and exact execution result in a `ConnectorVerificationRequest`. It runs only when CR policy calls for verification and returns one bounded `ConnectorVerificationResult`.

Raw external responses must be parsed into typed results immediately. Never place unrestricted adapter content in a connector result, trace, summary, continuation, or answer.

The `co.pending-action.v1` shape remains unchanged and never stores raw normalized arguments. On continuation, shared code looks up the capability and connector, restores and re-normalizes arguments, regenerates the description, and checks capability identity, connector identity, target, connector text, and argument digest before policy calls or side effects. Do not use an in-memory continuation map.

## Minimal neutral skeleton

This display-setting connector is illustrative pseudocode, not a production registration. It teaches the generic contract; `JellyfinActionConnector` is a concrete production reference, not the generic template.

```python
from services.action_connectors import (
    ConnectorArguments,
    ConnectorAvailabilityResult,
    ConnectorContinuationDescription,
    ConnectorExecutionResult,
    ConnectorInputError,
    ConnectorPresentation,
    ConnectorRevalidationResult,
    ConnectorVerificationResult,
    ExecutionStatus,
    RevalidationStatus,
    VerificationStatus,
)


class DisplaySettingConnector:
    capability_id = "fixture.display_setting_apply"
    effect_mode = "simulated"
    revalidation_spec = None
    presentation = ConnectorPresentation(
        pending_confirmation="Confirm before applying the display setting.",
        confirmation_rejected="The display setting was not applied.",
        execution_failed="I could not apply the display setting.",
        execution_unknown="The display setting outcome is unknown; I did not retry it.",
        partially_executed="The display setting was only partially applied; I did not retry it.",
        executed="I applied the display setting once without verification.",
        executed_verified="I applied and verified the display setting.",
        executed_unverified="I applied the display setting, but verification did not pass.",
    )

    def __init__(self, operations):
        self.operations = operations

    def normalize_arguments(self, arguments):
        if set(arguments) != {"target", "level"}:
            raise ConnectorInputError("schema_invalid_arguments")
        target = arguments.get("target")
        level = arguments.get("level")
        if (
            target != "fixture:display"
            or not isinstance(level, int)
            or isinstance(level, bool)
            or not 0 <= level <= 10
        ):
            raise ConnectorInputError("schema_invalid_arguments")
        return ConnectorArguments({"target": target, "level": level})

    def describe_continuation(self, arguments):
        values = self.normalize_arguments(arguments.as_dict()).values
        return ConnectorContinuationDescription(
            target=values["target"],
            confirmation_text=(
                f"Confirm display level {values['level']} for fixture:display."
            ),
        )

    def restore_continuation(self, description):
        prefix = "Confirm display level "
        suffix = " for fixture:display."
        text = description.confirmation_text
        if (
            description.target != "fixture:display"
            or not text.startswith(prefix)
            or not text.endswith(suffix)
        ):
            raise ConnectorInputError("continuation_mismatch")
        level_text = text[len(prefix) : -len(suffix)]
        if not level_text.isdigit():
            raise ConnectorInputError("continuation_mismatch")
        return self.normalize_arguments(
            {"target": description.target, "level": int(level_text)}
        )

    def check_availability(self, request):
        return ConnectorAvailabilityResult(True, "available")

    async def revalidate(self, request):
        return ConnectorRevalidationResult(
            RevalidationStatus.UNAVAILABLE,
            "revalidation_unavailable",
        )

    async def execute(self, request):
        # Adapter setup and bounded external parsing are integration-specific.
        external_status = await self.operations.apply_setting(
            request.arguments.as_dict()
        )
        accepted = {
            "completed": ExecutionStatus.COMPLETED,
            "partially_executed": ExecutionStatus.PARTIALLY_EXECUTED,
            "failed": ExecutionStatus.FAILED,
            "unknown": ExecutionStatus.UNKNOWN,
        }
        status = (
            accepted.get(external_status)
            if isinstance(external_status, str)
            else None
        ) or ExecutionStatus.UNKNOWN
        reason = {
            ExecutionStatus.COMPLETED: "setting_completed",
            ExecutionStatus.PARTIALLY_EXECUTED: "setting_partially_executed",
            ExecutionStatus.FAILED: "setting_failed",
            ExecutionStatus.UNKNOWN: "setting_outcome_unknown",
        }[status]
        return ConnectorExecutionResult(
            status=status,
            reason_code=reason,
            external_reason_code="bounded_adapter_result",
            external_call_count=1,
            effect_mode=self.effect_mode,
            target_label="fixture:display",
        )

    async def verify(self, request):
        return ConnectorVerificationResult(
            status=VerificationStatus.NOT_SUPPORTED,
            reason_code="verification_not_supported",
            external_reason_code="verification_not_supported",
            external_call_count=0,
            effect_mode=self.effect_mode,
            target_label="fixture:display",
        )
```

This example performs no revalidation and reports verification as unsupported. In a matching policy shape with verification disabled, the shared lifecycle calls `apply_setting` once and does not call `verify`.

## Register the connector

Two explicit registrations are needed:

1. Add the matching `CapabilityEntry` and `CapabilityPolicyShape` described above.
2. Construct exactly one connector instance in the explicit production `ActionConnectorRegistry` used by `orchestrate_chat`.

Inject adapters or bounded operation functions through the connector constructor:

```python
connector_registry = ActionConnectorRegistry(
    (
        IntegrationActionConnector(integration_operations),
        # Other explicitly constructed production connectors belong here.
    )
)
```

The names in this snippet stand for your connector and its injected operations. Do not read capability identity or authority from environment variables, add an implicit fallback connector, merge hidden registrations, or dynamically discover connectors.

## Map external outcomes

| External meaning | Shared execution status | User-facing rule |
| --- | --- | --- |
| The effect fully completed | `completed` | Report completion; claim verification only if it actually passed. |
| Some effect occurred, but not all | `partially_executed` | Always report a degraded partial outcome, even if later verification passes. |
| Execution definitely failed | `failed` | Report failure without claiming an effect or retrying it. |
| The effect cannot be determined | `unknown` | Preserve uncertainty and state that no retry occurred. |

Each connector explicitly defines the external statuses it accepts. Adding a shared `ExecutionStatus` value must not silently widen an existing adapter's accepted external vocabulary. Failed, partially executed, and unknown consequential outcomes are never retried automatically.

## Verification and revalidation

Revalidation is a current-state read before final authority and dispatch. Verification is a post-execution check after exactly one effect attempt.

Both operations are bounded and neither grants authority. Connector presence does not make verification mandatory; CR policy decides whether verification is needed. When verification is not needed, the shared lifecycle makes zero connector verification calls. When needed, it makes at most one.

Unavailable, unsupported, or malformed verification preserves the truthful execution outcome. A verification failure never causes an execution retry, and passed verification never turns a partial execution into ordinary success.

## Tests for every connector

Add focused and composed coverage in the current test files:

- `api/tests/test_action_connectors.py`
- `api/tests/test_capabilities.py`
- `api/tests/test_orchestrate_flow.py`

### Contract tests

- valid and invalid normalization;
- bounded execution, verification, revalidation, and presentation values; and
- registry lookup, malformed or unknown lookup, and duplicate rejection.

### Continuation tests

- deterministic description/restoration round-trip and argument-digest binding;
- tampered target, connector text, and digest rejection; and
- zero dependencies and zero side effects during restoration, normalization, and digest validation.

### Lifecycle tests

- first-turn pending behavior;
- accepted and rejected provider-free continuation;
- expired challenge and consumed replay; and
- exact confirmation, dispatch, execution, verification, and summary call counts.

### Outcome tests

- completed, partially executed, failed, and unknown execution;
- verification-needed and verification-not-needed paths;
- unavailable, malformed, and mismatched action summaries; and
- no effect retry or provider fallback after an attempted effect.

### Privacy tests

Prove that credentials, endpoints, raw external bodies, prompts, provider output, exception text, and unrestricted arguments do not enter traces, summaries, pending envelopes, or answers.

### Compatibility tests

Prove existing production connector behavior remains unchanged. Keep any relevant composed evaluator unchanged unless its scenario is deliberately revised in its owning repository.

## Validation commands

Run the focused and repository validation from Chat Orchestrator:

```bash
PYTHONDONTWRITEBYTECODE=1 api/.venv/bin/python -m pytest \
  -p no:cacheprovider \
  api/tests/test_action_connectors.py \
  api/tests/test_capabilities.py \
  api/tests/test_orchestrate_flow.py

make dev-test
make prompt-budget-test
make replay-test
make process-naming-check
```

If connector work could affect the existing Jellyfin path, the current composed evaluator is an optional compatibility reference. It is not a permanent validation step for every new connector, and `JellyfinActionConnector` is a compatibility case rather than a template.

```bash
cd ../projects
EVALUATOR_PATH="$(git ls-files '*/assurance/evaluate_jellyfin_safe_restart.py')"
test -f "$EVALUATOR_PATH"
../chat-orchestrator/api/.venv/bin/python \
  "$EVALUATOR_PATH" \
  --projects-root . \
  --cognitive-runtime-root ../cognitive-runtime \
  --chat-orchestrator-root ../chat-orchestrator \
  --output-root /tmp/action-connector-compatibility-check \
  --deterministic-asserted
```

The evaluator output should be disposable; the command above writes it under `/tmp`.

## Definition of done

A connector is complete only when:

- [ ] the canonical CR capability exists;
- [ ] the matching CO capability registration exists;
- [ ] exactly one connector instance is explicitly registered;
- [ ] arguments and continuation restoration are deterministic and bounded;
- [ ] the lifecycle enforces one effect attempt;
- [ ] policy-selected verification is bounded to one call;
- [ ] partial, failed, and unknown outcomes remain truthful and are not retried;
- [ ] CR receives one correct action summary;
- [ ] replay cannot repeat dispatch or the effect;
- [ ] privacy assertions pass;
- [ ] focused and full validation pass; and
- [ ] existing connectors remain compatible.

## Common mistakes

- Copying product-specific branches into shared orchestration.
- Putting policy or trust selection in the connector.
- Persisting raw arguments in the continuation.
- Interpreting missing confirmation as approval.
- Treating connector registration as authorization.
- Retrying unknown or partial consequential effects.
- Claiming success from provider prose.
- Allowing external status strings directly into traces.
- Accidentally widening an existing adapter's accepted status vocabulary.
- Adding dynamic plugin discovery.
