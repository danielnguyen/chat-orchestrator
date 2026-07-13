from __future__ import annotations

import json

import pytest
import services.capabilities as capability_service
from models import ChatRequest, ChatResponse
from pydantic import ValidationError
from services.action_connectors import (
    ActionConnectorRegistry,
    ConnectorVerificationResult,
    VerificationStatus,
)
from services.capabilities import (
    CapabilityEntry,
    CapabilityPolicyShape,
    CapabilityValidationError,
    RevalidationOutput,
    Revalidator,
    RevalidatorEntry,
    argument_digest,
    authorize_and_execute_capability,
    descriptor_fingerprint,
    filter_capability_descriptors_for_exposure,
    parse_pending_action_confirmation,
    parse_provider_capability_request,
    production_capability_registry,
    provider_descriptors,
    restore_pending_action_request,
    validate_and_digest_capability_request,
)
from services.jellyfin_action_connector import (
    JellyfinActionConnector,
    JellyfinOperations,
)
from services.orchestrate import _select_capability_claim_refs
from test_action_connectors import DisplaySettingConnector, DisplaySettingOperations


class FakeRuntime:
    def __init__(
        self,
        *,
        denied: set[str] | None = None,
        malformed: bool = False,
        phase_decisions: dict[str, dict[str, object]] | None = None,
        world_state_response: dict[str, object] | None = None,
        world_state_error: Exception | None = None,
        verification_error: Exception | None = None,
        verification_response: dict[str, object] | None = None,
        confirmation_error: Exception | None = None,
        confirmation_response: dict[str, object] | None = None,
        relationship_response: dict[str, object] | None = None,
        relationship_error: Exception | None = None,
    ):
        self.denied = denied or set()
        self.malformed = malformed
        self.phase_decisions = phase_decisions or {}
        self.world_state_response = world_state_response or {
            "included_claims": [],
            "excluded_claim_summaries": [],
            "prompt_content": None,
            "trace": {
                "included_claim_count": 0,
                "excluded_claim_count": 0,
                "stale_count": 0,
                "aging_count": 0,
                "expired_count": 0,
                "conflicted_count": 0,
                "confirmation_required": False,
            },
        }
        self.world_state_error = world_state_error
        self.verification_error = verification_error
        self.verification_response = verification_response
        self.confirmation_error = confirmation_error
        self.confirmation_response = confirmation_response
        self.relationship_response = relationship_response
        self.relationship_error = relationship_error
        self.calls = []
        self.confirmation_calls = []
        self.world_state_calls = []
        self.world_state_verification_calls = []
        self.relationship_calls = []
        self.executor_calls = 0

    async def authorize_capability(self, **kwargs):
        self.calls.append(kwargs)
        if self.malformed:
            return {"result": "bad"}
        phase_decision = self.phase_decisions.get(kwargs["authorization_phase"])
        if isinstance(phase_decision, list):
            phase_decision = phase_decision.pop(0)
        if phase_decision is not None:
            return {
                "result": {
                    "allowed": phase_decision.get("allowed", False),
                    "decision_code": phase_decision.get(
                        "decision_code",
                        "authorization_denied",
                    ),
                    "reason_codes": phase_decision.get(
                        "reason_codes",
                        [phase_decision.get("decision_code", "authorization_denied")],
                    ),
                    "challenge_ref": phase_decision.get("challenge_ref"),
                    "revalidation_selector": phase_decision.get("revalidation_selector"),
                    "relationship_ids_used": phase_decision.get(
                        "relationship_ids_used",
                        [],
                    ),
                    "world_state_claim_ids_used": phase_decision.get(
                        "world_state_claim_ids_used",
                        [],
                    ),
                }
            }
        relationship_ids = kwargs.get("selected_relationship_ids") or []
        requires_relationship = (
            kwargs["capability_id"] == "runtime.relationship_context.read"
        )
        allowed = (
            kwargs["capability_id"] not in self.denied
            and (not requires_relationship or bool(relationship_ids))
        )
        reason = "allowed" if allowed else "capability_domain_denied"
        if requires_relationship and not relationship_ids:
            reason = "missing_relationship_context"
        return {
            "result": {
                "allowed": allowed,
                "decision_code": "allowed" if allowed else "authorization_denied",
                "reason_codes": [reason],
                "relationship_ids_used": relationship_ids if allowed else [],
            }
        }

    async def world_state_resolve(self, **kwargs):
        self.executor_calls += 1
        self.world_state_calls.append(kwargs)
        if self.world_state_error is not None:
            raise self.world_state_error
        return self.world_state_response

    async def world_state_claim_verify(self, **kwargs):
        self.world_state_verification_calls.append(kwargs)
        if self.verification_error is not None:
            raise self.verification_error
        if self.verification_response is not None:
            return self.verification_response
        return {
            "claim": {
                "world_state_claim_id": kwargs["world_state_claim_id"],
                "verification_verifier_id": kwargs["verifier_id"],
                "verification_source_type": kwargs["verification_source_type"],
                "verification_source_ref": kwargs["verification_source_ref"],
                "last_verified_runtime_session_id": kwargs["runtime_session_id"],
                "last_verified_runtime_turn_id": kwargs["runtime_turn_id"],
                "state_authority": kwargs["resulting_authority"],
                "confidence": kwargs["resulting_confidence"],
                "freshness_state": kwargs["resulting_freshness_state"],
                "effective_freshness_state": kwargs["resulting_freshness_state"],
            }
        }

    async def confirm_capability(self, **kwargs):
        self.confirmation_calls.append(kwargs)
        if self.confirmation_error is not None:
            raise self.confirmation_error
        if self.confirmation_response is not None:
            return self.confirmation_response
        return {
            "request_id": kwargs["request_id"],
            "owner_id": kwargs["owner_id"],
            "conversation_id": kwargs["conversation_id"],
            "runtime_session_id": kwargs["runtime_session_id"],
            "runtime_turn_id": kwargs["runtime_turn_id"],
            "confirmation_challenge_ref": kwargs["confirmation_challenge_ref"],
            "confirmation_state": "accepted",
        }

    async def relationship_select(self, **kwargs):
        self.executor_calls += 1
        self.relationship_calls.append(kwargs)
        if self.relationship_error is not None:
            raise self.relationship_error
        if self.relationship_response is not None:
            return self.relationship_response
        return {
            "selected_relationships": [{"relationship_id": "rel_project"}],
            "prompt_content": None,
            "retrieval_scope_projection": {
                "applied": True,
                "relationship_ids": ["rel_project"],
                "entity_ids": ["entity_repo"],
                "relationship_scopes": ["project_context"],
                "reason_codes": ["eligible_relationship_scope_selected"],
            },
            "trace": {
                "selected_relationship_count": 1,
                "excluded_relationship_count": 0,
            },
        }


def _completion(message: dict[str, object]) -> dict[str, object]:
    return {"choices": [{"message": message}]}


def _call(provider_tool_name: str, arguments: dict[str, object]) -> dict[str, object]:
    return _completion(
        {
            "tool_calls": [
                {
                    "function": {
                        "name": provider_tool_name,
                        "arguments": json.dumps(arguments),
                    }
                }
            ]
        }
    )


def _allowed_phase_decisions() -> dict[str, dict[str, object]]:
    return {
        "selection": {
            "allowed": True,
            "decision_code": "allowed",
            "reason_codes": ["allowed"],
        },
        "dispatch": {
            "allowed": True,
            "decision_code": "allowed",
            "reason_codes": ["allowed"],
        },
    }


def _validated(provider_tool_name: str, arguments: dict[str, object]):
    request = parse_provider_capability_request(_call(provider_tool_name, arguments))
    assert request is not None
    return validate_and_digest_capability_request(
        request=request,
        exposed_capability_ids=[
            "runtime.world_state.read",
            "draft.local_message",
            "runtime.relationship_context.read",
            "jellyfin_restart",
        ],
        connector_registry=ActionConnectorRegistry((JellyfinActionConnector(None),)),
    )


async def _execute(
    runtime: FakeRuntime,
    validation_result,
    revalidators=None,
    confirmation=None,
    selected_relationship_ids=None,
):
    return await authorize_and_execute_capability(
        runtime=runtime,
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        active_persona_id="technical_architect",
        validation_result=validation_result,
        selected_relationship_ids=selected_relationship_ids,
        revalidators=revalidators,
        capability_confirmation=confirmation,
    )


def _trusted_revalidator(
    *,
    outputs: list[RevalidationOutput | dict[str, object]] | None = None,
    raises: Exception | None = None,
    revalidator_id: str = "trusted_refresh",
) -> Revalidator:
    entry = RevalidatorEntry(
        revalidator_id=revalidator_id,
        verifier_id="cr-verifier-local",
        verification_source_type="tool_output",
        verification_source_ref="local-deterministic-revalidator",
        supported_domains=("active_repository",),
        supported_attributes=("branch",),
        resulting_authority="verified_tool_output",
        resulting_confidence=0.9,
        resulting_freshness_state="fresh",
        ttl_seconds=300,
        revalidation_interval_seconds=120,
    )

    def verify(claim_ids):
        if raises is not None:
            raise raises
        if outputs is not None:
            return outputs
        return [
            RevalidationOutput(
                claim_id=claim_id,
                expected_value_digest=f"wsvalue_{claim_id}",
                observed_at="2026-07-06T00:00:00+00:00",
                verified_at="2026-07-06T00:00:01+00:00",
            )
            for claim_id in claim_ids
        ]

    return Revalidator(entry=entry, verify=verify)


def test_production_registry_contains_exact_executor_bound_entries():
    registry = production_capability_registry()

    assert [entry.capability_id for entry in registry] == [
        "runtime.world_state.read",
        "draft.local_message",
        "runtime.relationship_context.read",
        "jellyfin_restart",
    ]
    assert all(entry.executor_binding for entry in registry)
    assert all(not entry.capability_id.startswith("test.") for entry in registry)
    assert [entry.provider_tool_name for entry in registry] == [
        "runtime_world_state_read",
        "draft_local_message",
        "runtime_relationship_context_read",
        "jellyfin_safe_restart",
    ]
    assert all(entry.provider_tool_name != entry.capability_id for entry in registry)
    assert all("." not in entry.provider_tool_name for entry in registry)


def test_provider_descriptors_are_deterministic_and_fingerprint_stable():
    registry = list(reversed(production_capability_registry()))

    first = provider_descriptors(registry)
    second = provider_descriptors(registry)

    assert first == second
    assert [item["metadata"]["capability_id"] for item in first] == [
        "draft.local_message",
        "jellyfin_restart",
        "runtime.relationship_context.read",
        "runtime.world_state.read",
    ]
    assert [item["function"]["name"] for item in first] == [
        "draft_local_message",
        "jellyfin_safe_restart",
        "runtime_relationship_context_read",
        "runtime_world_state_read",
    ]
    assert [item["metadata"]["provider_tool_name"] for item in first] == [
        "draft_local_message",
        "jellyfin_safe_restart",
        "runtime_relationship_context_read",
        "runtime_world_state_read",
    ]
    assert descriptor_fingerprint(first) == descriptor_fingerprint(second)


@pytest.mark.asyncio
async def test_exposure_authorization_allows_non_relationship_capabilities_without_context():
    runtime = FakeRuntime()
    descriptors, trace = await filter_capability_descriptors_for_exposure(
        runtime=runtime,
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        active_persona_id="technical_architect",
    )

    assert trace["status"] == "ok"
    assert trace["exposed_capability_ids"] == [
        "runtime.world_state.read",
        "draft.local_message",
    ]
    assert trace["blocked_capability_ids"] == ["runtime.relationship_context.read"]
    assert trace["blocked_reasons"] == {
        "runtime.relationship_context.read": "missing_relationship_context"
    }
    assert len(descriptors) == 2
    assert [call["capability_id"] for call in runtime.calls] == [
        "runtime.world_state.read",
        "draft.local_message",
        "runtime.relationship_context.read",
    ]


@pytest.mark.asyncio
async def test_relationship_gated_descriptor_exposed_with_selected_relationship_context():
    runtime = FakeRuntime()
    descriptors, trace = await filter_capability_descriptors_for_exposure(
        runtime=runtime,
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        active_persona_id="technical_architect",
        selected_relationship_ids=["rel_project"],
    )

    descriptor_ids = [item["metadata"]["capability_id"] for item in descriptors]
    descriptor_names = [item["function"]["name"] for item in descriptors]
    assert "runtime.relationship_context.read" in descriptor_ids
    assert "runtime_relationship_context_read" in descriptor_names
    assert trace["blocked_capability_ids"] == []
    relationship_call = [
        call
        for call in runtime.calls
        if call["capability_id"] == "runtime.relationship_context.read"
    ][0]
    assert relationship_call["relationship_requirements"] == [
        {
            "relationship_scope": "project_context",
            "relationship_type": "works_on",
            "required_status": "active",
            "minimum_confidence": 0.8,
        }
    ]
    assert relationship_call["selected_relationship_ids"] == ["rel_project"]


@pytest.mark.asyncio
async def test_exposure_denial_removes_descriptor_from_provider_payload():
    descriptors, trace = await filter_capability_descriptors_for_exposure(
        runtime=FakeRuntime(denied={"draft.local_message"}),
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        active_persona_id="technical_architect",
    )

    descriptor_ids = [item["metadata"]["capability_id"] for item in descriptors]
    descriptor_names = [item["function"]["name"] for item in descriptors]
    assert descriptor_ids == ["runtime.world_state.read"]
    assert descriptor_names == ["runtime_world_state_read"]
    assert trace["blocked_capability_ids"] == [
        "draft.local_message",
        "runtime.relationship_context.read",
    ]
    assert trace["blocked_reasons"] == {
        "draft.local_message": "capability_domain_denied",
        "runtime.relationship_context.read": "missing_relationship_context",
    }


@pytest.mark.asyncio
async def test_exposure_runtime_unavailable_or_malformed_fails_closed():
    unavailable_descriptors, unavailable_trace = (
        await filter_capability_descriptors_for_exposure(
            runtime=None,
            request_id="rid",
            owner_id="owner",
            conversation_id="conv",
            surface="dev",
            runtime_session_id="rtsession_1",
            runtime_turn_id="rtturn_1",
            active_persona_id="technical_architect",
        )
    )
    malformed_descriptors, malformed_trace = await filter_capability_descriptors_for_exposure(
        runtime=FakeRuntime(malformed=True),
        request_id="rid",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_1",
        runtime_turn_id="rtturn_1",
        active_persona_id="technical_architect",
    )

    assert unavailable_descriptors == []
    assert unavailable_trace["status"] == "failed_closed"
    assert unavailable_trace["descriptor_count"] == 0
    assert malformed_descriptors == []
    assert malformed_trace["status"] == "failed_closed"
    assert malformed_trace["status_reason"] == "malformed_exposure_authorization"


def test_provider_normal_text_has_no_capability_request():
    assert parse_provider_capability_request(_completion({"content": "hello"})) is None


def test_exactly_one_provider_capability_call_parses_successfully():
    request = parse_provider_capability_request(
        _call("draft_local_message", {"body": "hello"})
    )

    assert request is not None
    assert request.capability_id == "draft.local_message"
    assert request.provider_tool_name == "draft_local_message"
    assert request.arguments == {"body": "hello"}


def test_provider_world_state_tool_name_maps_to_internal_capability_id():
    request = parse_provider_capability_request(
        _call("runtime_world_state_read", {"output_mode": "structured"})
    )

    assert request is not None
    assert request.capability_id == "runtime.world_state.read"
    assert request.provider_tool_name == "runtime_world_state_read"


def test_provider_relationship_context_tool_name_maps_to_internal_capability_id():
    request = parse_provider_capability_request(
        _call(
            "runtime_relationship_context_read",
            {"relationship_scope": "project_context", "relationship_type": "works_on"},
        )
    )

    assert request is not None
    assert request.capability_id == "runtime.relationship_context.read"
    assert request.provider_tool_name == "runtime_relationship_context_read"


@pytest.mark.parametrize(
    ("completion", "reason"),
    [
        (
            _completion(
                {
                    "tool_calls": [
                        {
                            "function": {
                                "name": "draft_local_message",
                                "arguments": "{\"body\":\"one\"}",
                            }
                        },
                        {
                            "function": {
                                "name": "runtime_world_state_read",
                                "arguments": "{}",
                            }
                        },
                    ]
                }
            ),
            "multiple_capability_calls",
        ),
        (_call("draft_local_message", {"body": "x" * 5000}), "oversized_arguments"),
        (
            _completion(
                {
                    "tool_calls": [
                        {
                            "function": {
                                "name": "draft_local_message",
                                "arguments": "{bad",
                            }
                        }
                    ]
                }
            ),
            "malformed_arguments",
        ),
    ],
)
def test_provider_capability_call_rejections(completion, reason):
    if reason in {"multiple_capability_calls", "malformed_arguments"}:
        with pytest.raises(CapabilityValidationError) as exc:
            parse_provider_capability_request(completion)
        assert exc.value.reason_code == reason
        return

    request = parse_provider_capability_request(completion)
    with pytest.raises(CapabilityValidationError) as exc:
        validate_and_digest_capability_request(
            request=request,
            exposed_capability_ids=["draft.local_message"],
        )
    assert exc.value.reason_code == reason


@pytest.mark.parametrize(
    ("provider_tool_name", "exposed_ids", "reason"),
    [
        ("integration_send_message", ["draft.local_message"], "unknown_capability_id"),
        ("draft_local_message", [], "capability_not_exposed"),
    ],
)
def test_unknown_and_hidden_provider_tool_names_are_rejected(
    provider_tool_name,
    exposed_ids,
    reason,
):
    if reason == "unknown_capability_id":
        with pytest.raises(CapabilityValidationError) as exc:
            parse_provider_capability_request(_call(provider_tool_name, {"body": "hello"}))
        assert exc.value.reason_code == reason
        return

    request = parse_provider_capability_request(_call(provider_tool_name, {"body": "hello"}))

    with pytest.raises(CapabilityValidationError) as exc:
        validate_and_digest_capability_request(
            request=request,
            exposed_capability_ids=exposed_ids,
        )

    assert exc.value.reason_code == reason


def test_hidden_relationship_gated_provider_call_rejected_before_executor():
    request = parse_provider_capability_request(
        _call(
            "runtime_relationship_context_read",
            {"relationship_scope": "project_context", "relationship_type": "works_on"},
        )
    )

    with pytest.raises(CapabilityValidationError) as exc:
        validate_and_digest_capability_request(
            request=request,
            exposed_capability_ids=["runtime.world_state.read", "draft.local_message"],
        )

    assert exc.value.reason_code == "capability_not_exposed"


@pytest.mark.parametrize(
    "provider_tool_name",
    [
        "draft.local_message",
        "runtime.world_state.read",
        "runtime.relationship_context.read",
    ],
)
def test_dotted_internal_id_as_provider_tool_name_is_rejected(provider_tool_name):
    with pytest.raises(CapabilityValidationError) as exc:
        parse_provider_capability_request(_call(provider_tool_name, {"body": "hello"}))

    assert exc.value.reason_code == "unknown_capability_id"


@pytest.mark.parametrize(
    "arguments",
    [
        {"body": ""},
        {"body": "hello", "send": True},
        {"body": "hello", "raw_url": "https://example.test/private"},
        {"body": "hello", "nested_tool_call": {"name": "send"}},
    ],
)
def test_schema_invalid_draft_arguments_are_rejected(arguments):
    request = parse_provider_capability_request(_call("draft_local_message", arguments))

    with pytest.raises(CapabilityValidationError) as exc:
        validate_and_digest_capability_request(
            request=request,
            exposed_capability_ids=["draft.local_message"],
        )

    assert exc.value.reason_code == "schema_invalid_arguments"


def test_world_state_arguments_normalize_and_digest_stably_without_raw_trace():
    request = parse_provider_capability_request(
        _call(
            "runtime_world_state_read",
            {
                "requested_domains": ["runtime_surface", "active_repository"],
                "output_mode": "structured",
            },
        )
    )

    first = validate_and_digest_capability_request(
        request=request,
        exposed_capability_ids=["runtime.world_state.read"],
    )
    second = validate_and_digest_capability_request(
        request=request,
        exposed_capability_ids=["runtime.world_state.read"],
    )

    assert first.normalized_arguments == {
        "output_mode": "structured",
        "requested_domains": ["active_repository", "runtime_surface"],
    }
    assert first.argument_digest == second.argument_digest
    assert "requested_domains" not in json.dumps(first.trace)
    assert "active_repository" not in json.dumps(first.trace)


def test_draft_arguments_normalize_and_digest_stably_without_raw_trace():
    request = parse_provider_capability_request(
        _call(
            "draft_local_message",
            {
                "body": "  Hello Daniel  ",
                "recipient_label": "reviewer",
                "subject": "Wave 3C",
                "tone": "direct",
                "format": "markdown",
            },
        )
    )

    first = validate_and_digest_capability_request(
        request=request,
        exposed_capability_ids=["draft.local_message"],
    )
    second = validate_and_digest_capability_request(
        request=request,
        exposed_capability_ids=["draft.local_message"],
    )

    assert first.normalized_arguments["body"] == "Hello Daniel"
    assert first.argument_digest == second.argument_digest
    serialized_trace = json.dumps(first.trace)
    assert "Hello Daniel" not in serialized_trace
    assert "recipient_label" not in serialized_trace


@pytest.mark.asyncio
async def test_world_state_read_authorizes_selection_before_dispatch_and_executes_once():
    runtime = FakeRuntime(
        phase_decisions=_allowed_phase_decisions(),
        world_state_response={
            "included_claims": [
                {
                    "world_state_claim_id": "claim-1",
                    "entity_id": "repo-1",
                    "attribute": "branch",
                    "domain": "active_repository",
                    "value_json": "PRIVATE-RAW-VALUE",
                }
            ],
            "excluded_claim_summaries": [],
            "prompt_content": "World state: PRIVATE-RAW-VALUE",
            "trace": {
                "included_claim_count": 1,
                "excluded_claim_count": 0,
                "stale_count": 0,
                "aging_count": 0,
                "expired_count": 0,
                "conflicted_count": 0,
                "confirmation_required": False,
            },
        },
    )

    result = await _execute(
        runtime,
        _validated(
            "runtime_world_state_read",
            {
                "requested_domains": ["active_repository"],
                "entity_id": "repo-1",
                "attribute": "branch",
                "output_mode": "structured",
            },
        ),
    )

    assert [call["authorization_phase"] for call in runtime.calls] == [
        "selection",
        "dispatch",
    ]
    assert runtime.world_state_calls[0]["requested_domains"] == ["active_repository"]
    assert result.trace["executor_called"] is True
    assert result.trace["executor_call_count"] == 1
    assert result.trace["executor_result_status"] == "ok"
    assert result.trace["executor_result"]["included_claim_count"] == 1
    assert "PRIVATE-RAW-VALUE" not in json.dumps(result.trace)
    assert "PRIVATE-RAW-VALUE" not in result.response_text


@pytest.mark.asyncio
async def test_local_message_draft_authorizes_selection_before_dispatch_and_never_sends():
    runtime = FakeRuntime(phase_decisions=_allowed_phase_decisions())

    result = await _execute(
        runtime,
        _validated(
            "draft_local_message",
            {"body": "send nothing", "recipient_label": "reviewer", "subject": "Draft"},
        ),
    )

    assert [call["authorization_phase"] for call in runtime.calls] == [
        "selection",
        "dispatch",
    ]
    assert runtime.world_state_calls == []
    assert result.trace["executor_called"] is True
    assert result.trace["executor_call_count"] == 1
    assert result.trace["executor_result"] == {
        "status": "ok",
        "draft_id": result.trace["executor_result"]["draft_id"],
        "local": True,
        "sent": False,
        "recipient_present": True,
        "subject_present": True,
        "body_char_count": 12,
        "tone": None,
        "format": "plain_text",
    }
    assert result.response_text == "I created a local unsent draft. Nothing was sent."
    assert "send nothing" not in json.dumps(result.trace)


@pytest.mark.asyncio
async def test_relationship_context_read_passes_selected_relationships_and_executes_once():
    runtime = FakeRuntime()

    result = await _execute(
        runtime,
        _validated(
            "runtime_relationship_context_read",
            {"relationship_scope": "project_context", "relationship_type": "works_on"},
        ),
        selected_relationship_ids=["rel_project"],
    )

    assert [call["authorization_phase"] for call in runtime.calls] == [
        "selection",
        "dispatch",
    ]
    assert [call["selected_relationship_ids"] for call in runtime.calls] == [
        ["rel_project"],
        ["rel_project"],
    ]
    assert runtime.calls[0]["relationship_requirements"] == [
        {
            "relationship_scope": "project_context",
            "relationship_type": "works_on",
            "required_status": "active",
            "minimum_confidence": 0.8,
        }
    ]
    assert len(runtime.relationship_calls) == 1
    assert runtime.relationship_calls[0]["requested_scopes"] == ["project_context"]
    assert runtime.relationship_calls[0]["relationship_types"] == ["works_on"]
    assert result.trace["executor_called"] is True
    assert result.trace["executor_call_count"] == 1
    assert result.trace["authorization"]["selection"]["relationship_ids"] == ["rel_project"]
    assert result.trace["authorization"]["dispatch"]["relationship_ids"] == ["rel_project"]
    assert result.trace["executor_result"]["relationship_ids"] == ["rel_project"]
    assert (
        result.response_text
        == "I read bounded project relationship context and found 1 authorized relationship(s)."
    )


@pytest.mark.asyncio
async def test_relationship_context_missing_selection_is_zero_executor():
    runtime = FakeRuntime()

    result = await _execute(
        runtime,
        _validated(
            "runtime_relationship_context_read",
            {"relationship_scope": "project_context", "relationship_type": "works_on"},
        ),
    )

    assert [call["authorization_phase"] for call in runtime.calls] == ["selection"]
    assert runtime.calls[0]["selected_relationship_ids"] == []
    assert result.trace["authorization"]["selection"]["status"] == "authorization_denied"
    assert result.trace["authorization"]["selection"]["reason_codes"] == [
        "missing_relationship_context"
    ]
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert runtime.relationship_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reason_code", "phase"),
    [
        ("revoked_relationship", "selection"),
        ("restricted_relationship", "selection"),
        ("conflicted_relationship", "selection"),
        ("expired_relationship", "selection"),
        ("provisional_relationship", "selection"),
        ("low_confidence_relationship", "selection"),
        ("relationship_scope_mismatch", "selection"),
        ("relationship_type_mismatch", "dispatch"),
        ("persona_relationship_scope_denied", "dispatch"),
        ("surface_relationship_scope_denied", "dispatch"),
    ],
)
async def test_relationship_context_ineligible_relationships_are_zero_executor(
    reason_code,
    phase,
):
    decisions = _allowed_phase_decisions()
    decisions[phase] = {
        "allowed": False,
        "decision_code": "authorization_denied",
        "reason_codes": [reason_code],
        "relationship_ids_used": [],
    }
    runtime = FakeRuntime(phase_decisions=decisions)

    result = await _execute(
        runtime,
        _validated(
            "runtime_relationship_context_read",
            {"relationship_scope": "project_context", "relationship_type": "works_on"},
        ),
        selected_relationship_ids=["rel_project"],
    )

    assert result.trace["authorization"][phase]["status"] == "authorization_denied"
    assert result.trace["authorization"][phase]["reason_codes"] == [reason_code]
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert runtime.relationship_calls == []


@pytest.mark.asyncio
async def test_relationship_context_trace_omits_raw_evidence_source_refs_and_scores():
    runtime = FakeRuntime(
        relationship_response={
            "selected_relationships": [
                {
                    "relationship_id": "rel_project",
                    "raw_evidence": "PRIVATE-REL-EVIDENCE",
                    "source_refs": [{"ref_id": "PRIVATE-SOURCE"}],
                    "private_details": "PRIVATE-REL-DETAILS",
                    "hidden_graph_score": 0.99,
                }
            ],
            "prompt_content": "PRIVATE-REL-PROMPT",
            "retrieval_scope_projection": {
                "applied": True,
                "relationship_ids": ["rel_project"],
                "entity_ids": ["entity_repo"],
                "relationship_scopes": ["project_context"],
                "reason_codes": ["eligible_relationship_scope_selected"],
            },
            "trace": {
                "selected_relationship_count": 1,
                "excluded_relationship_count": 0,
                "relationship_edges_used": ["rel_project"],
                "relationship_edges_excluded": [],
                "relationship_exclusion_reasons": {},
                "hidden_graph_score": 0.99,
            },
        }
    )

    result = await _execute(
        runtime,
        _validated(
            "runtime_relationship_context_read",
            {"relationship_scope": "project_context", "relationship_type": "works_on"},
        ),
        selected_relationship_ids=["rel_project"],
    )

    serialized = json.dumps(result.trace, sort_keys=True)
    assert "rel_project" in serialized
    assert "eligible_relationship_scope_selected" in serialized
    for forbidden in (
        "PRIVATE-REL-EVIDENCE",
        "PRIVATE-SOURCE",
        "PRIVATE-REL-DETAILS",
        "PRIVATE-REL-PROMPT",
        "hidden_graph_score",
        "raw_evidence",
        "source_refs",
        "private_details",
    ):
        assert forbidden not in serialized


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase", "decision_code", "expected_text"),
    [
        ("selection", "authorization_denied", "could not use"),
        ("dispatch", "authorization_denied", "could not use"),
        ("selection", "confirmation_required", "needs confirmation"),
        ("dispatch", "confirmation_required", "needs confirmation"),
        ("selection", "revalidation_required", "requires revalidation"),
        ("dispatch", "revalidation_required", "requires revalidation"),
    ],
)
async def test_authorization_blocks_are_zero_executor(
    phase,
    decision_code,
    expected_text,
):
    decisions = _allowed_phase_decisions()
    decisions[phase] = {
        "allowed": False,
        "decision_code": decision_code,
        "reason_codes": [decision_code],
        "challenge_ref": "challenge-1" if decision_code == "confirmation_required" else None,
        "revalidation_selector": (
            {"revalidator_id": "trusted-refresh", "world_state_claim_ids": ["claim-1"]}
            if decision_code == "revalidation_required"
            else None
        ),
    }
    runtime = FakeRuntime(phase_decisions=decisions)

    result = await _execute(
        runtime,
        _validated("runtime_world_state_read", {"output_mode": "summary"}),
    )

    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert runtime.world_state_calls == []
    assert expected_text in result.response_text
    assert result.trace["authorization"][phase]["status"] == decision_code
    assert "challenge-1" in json.dumps(result.trace) or decision_code != "confirmation_required"
    assert "claim-1" not in json.dumps(result.trace)


@pytest.mark.asyncio
async def test_valid_structured_confirmation_calls_cr_then_dispatches_with_confirmed_ref():
    validation_result = _validated("draft_local_message", {"body": "confirm me"})
    runtime = FakeRuntime(
        phase_decisions={
            "selection": {
                "allowed": False,
                "decision_code": "confirmation_required",
                "reason_codes": ["confirmation_required"],
                "challenge_ref": "challenge-1",
            },
            "dispatch": {
                "allowed": True,
                "decision_code": "allowed",
                "reason_codes": ["allowed"],
            },
        }
    )

    result = await _execute(
        runtime,
        validation_result,
        confirmation={
            "challenge_ref": "challenge-1",
            "capability_id": validation_result.capability_id,
            "argument_digest": validation_result.argument_digest,
            "confirmed": True,
        },
    )

    assert [call["authorization_phase"] for call in runtime.calls] == [
        "selection",
        "dispatch",
    ]
    assert len(runtime.confirmation_calls) == 1
    assert runtime.confirmation_calls[0] == {
        "request_id": "rid:draft.local_message:confirm",
        "owner_id": "owner",
        "conversation_id": "conv",
        "surface": "dev",
        "runtime_session_id": "rtsession_1",
        "runtime_turn_id": "rtturn_1",
        "confirmation_challenge_ref": "challenge-1",
        "capability_id": "draft.local_message",
        "operation_class": "draft",
        "argument_digest": validation_result.argument_digest,
        "confirmed": True,
    }
    assert runtime.calls[1]["confirmation_challenge_ref"] == "challenge-1"
    assert result.trace["confirmation"]["status"] == "accepted"
    assert result.trace["confirmation"]["call_count"] == 1
    assert result.trace["executor_called"] is True
    assert result.trace["executor_call_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "bad_value", "missing"),
    [
        ("request_id", "other-request", False),
        ("owner_id", "other-owner", False),
        ("conversation_id", "other-conv", False),
        ("runtime_session_id", "other-session", False),
        ("runtime_turn_id", "other-turn", False),
        ("owner_id", None, True),
    ],
)
async def test_cr_confirmation_response_binding_mismatch_is_zero_executor(
    field,
    bad_value,
    missing,
):
    validation_result = _validated("draft_local_message", {"body": "PRIVATE RAW BODY"})
    response = {
        "request_id": "rid:draft.local_message:confirm",
        "owner_id": "owner",
        "conversation_id": "conv",
        "runtime_session_id": "rtsession_1",
        "runtime_turn_id": "rtturn_1",
        "confirmation_challenge_ref": "challenge-1",
        "confirmation_state": "accepted",
    }
    if missing:
        response.pop(field)
    else:
        response[field] = bad_value
    runtime = FakeRuntime(
        phase_decisions={
            "selection": {
                "allowed": False,
                "decision_code": "confirmation_required",
                "reason_codes": ["confirmation_required"],
                "challenge_ref": "challenge-1",
            },
            "dispatch": {
                "allowed": True,
                "decision_code": "allowed",
                "reason_codes": ["allowed"],
            },
        },
        confirmation_response=response,
    )

    result = await _execute(
        runtime,
        validation_result,
        confirmation={
            "challenge_ref": "challenge-1",
            "capability_id": validation_result.capability_id,
            "argument_digest": validation_result.argument_digest,
            "confirmed": True,
            "raw_provider_payload": "PRIVATE RAW BODY",
            "raw_user_prose": "please confirm PRIVATE RAW BODY",
        },
    )

    assert [call["authorization_phase"] for call in runtime.calls] == ["selection"]
    assert result.trace["confirmation"]["status"] == "malformed"
    assert result.trace["confirmation"]["reason_code"] == "confirmation_response_mismatch"
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert runtime.world_state_calls == []
    assert len(runtime.confirmation_calls) == 1
    serialized_trace = json.dumps(result.trace)
    assert "PRIVATE RAW BODY" not in serialized_trace
    assert "raw_provider_payload" not in serialized_trace
    assert "raw_user_prose" not in serialized_trace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("confirmation", "expected_reason"),
    [
        (None, "confirmation_missing"),
        (
            {
                "challenge_ref": "challenge-1",
                "capability_id": "runtime.world_state.read",
                "argument_digest": "wrong",
                "confirmed": True,
            },
            "confirmation_capability_mismatch",
        ),
        (
            {
                "challenge_ref": "challenge-1",
                "capability_id": "draft.local_message",
                "argument_digest": "wrong",
                "confirmed": True,
            },
            "confirmation_argument_digest_mismatch",
        ),
        (
            {
                "challenge_ref": "bad ref with spaces",
                "capability_id": "draft.local_message",
                "argument_digest": "filled-by-test",
                "confirmed": True,
            },
            "malformed_challenge_ref",
        ),
        (
            {
                "challenge_ref": "challenge-1",
                "capability_id": "draft.local_message",
                "argument_digest": "filled-by-test",
                "confirmed": False,
            },
            "confirmation_not_confirmed",
        ),
        (
            {
                "challenge_ref": "challenge-1",
                "capability_id": "draft.local_message",
                "argument_digest": "filled-by-test",
            },
            "confirmation_not_confirmed",
        ),
    ],
)
async def test_structured_confirmation_shape_failures_are_zero_executor(
    confirmation,
    expected_reason,
):
    validation_result = _validated("draft_local_message", {"body": "confirm me"})
    if isinstance(confirmation, dict) and confirmation.get("argument_digest") == "filled-by-test":
        confirmation["argument_digest"] = validation_result.argument_digest
    runtime = FakeRuntime(
        phase_decisions={
            "selection": {
                "allowed": False,
                "decision_code": "confirmation_required",
                "reason_codes": ["confirmation_required"],
                "challenge_ref": "challenge-1",
            },
        }
    )

    result = await _execute(runtime, validation_result, confirmation=confirmation)

    assert result.trace["confirmation"]["reason_code"] == expected_reason
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert runtime.confirmation_calls == []
    assert runtime.world_state_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("confirmation_response", "confirmation_error", "expected_status", "expected_reason"),
    [
        (None, RuntimeError("offline"), "failed", "confirmation_unavailable"),
        ("not-a-dict", None, "malformed", "malformed_confirmation_response"),
        (
            {"confirmation_state": "accepted", "confirmation_challenge_ref": "other"},
            None,
            "malformed",
            "confirmation_response_mismatch",
        ),
        (
            {"confirmation_state": "rejected", "confirmation_challenge_ref": "challenge-1"},
            None,
            "failed",
            "rejected",
        ),
        (
            {"confirmation_state": "expired", "confirmation_challenge_ref": "challenge-1"},
            None,
            "failed",
            "expired",
        ),
        (
            {"confirmation_state": "consumed", "confirmation_challenge_ref": "challenge-1"},
            None,
            "failed",
            "consumed",
        ),
    ],
)
async def test_cr_confirmation_failures_are_zero_executor(
    confirmation_response,
    confirmation_error,
    expected_status,
    expected_reason,
):
    validation_result = _validated("draft_local_message", {"body": "confirm me"})
    runtime = FakeRuntime(
        phase_decisions={
            "selection": {
                "allowed": False,
                "decision_code": "confirmation_required",
                "reason_codes": ["confirmation_required"],
                "challenge_ref": "challenge-1",
            },
        },
        confirmation_response=confirmation_response,
        confirmation_error=confirmation_error,
    )

    result = await _execute(
        runtime,
        validation_result,
        confirmation={
            "challenge_ref": "challenge-1",
            "capability_id": validation_result.capability_id,
            "argument_digest": validation_result.argument_digest,
            "confirmed": True,
        },
    )

    assert result.trace["confirmation"]["status"] == expected_status
    assert result.trace["confirmation"]["reason_code"] == expected_reason
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert len(runtime.confirmation_calls) == 1


@pytest.mark.asyncio
async def test_confirmation_after_revalidation_rerun_can_confirm_then_dispatch():
    validation_result = _validated("runtime_world_state_read", {"output_mode": "summary"})
    runtime = FakeRuntime(
        phase_decisions={
            "selection": [
                {
                    "allowed": False,
                    "decision_code": "revalidation_required",
                    "reason_codes": ["world_state_revalidation_required"],
                    "revalidation_selector": {
                        "revalidator_id": "trusted_refresh",
                        "world_state_claim_ids": ["claim-1"],
                    },
                },
                {
                    "allowed": False,
                    "decision_code": "confirmation_required",
                    "reason_codes": ["confirmation_required"],
                    "challenge_ref": "challenge-rerun",
                },
            ],
            "dispatch": {
                "allowed": True,
                "decision_code": "allowed",
                "reason_codes": ["allowed"],
            },
        },
    )

    result = await _execute(
        runtime,
        validation_result,
        {"trusted_refresh": _trusted_revalidator()},
        confirmation={
            "challenge_ref": "challenge-rerun",
            "capability_id": validation_result.capability_id,
            "argument_digest": validation_result.argument_digest,
            "confirmed": True,
        },
    )

    assert [call["authorization_phase"] for call in runtime.calls] == [
        "selection",
        "selection",
        "dispatch",
    ]
    assert runtime.calls[2]["confirmation_challenge_ref"] == "challenge-rerun"
    assert result.trace["revalidation"]["status"] == "verified"
    assert result.trace["confirmation"]["status"] == "accepted"
    assert result.trace["executor_called"] is True
    assert result.trace["executor_call_count"] == 1


@pytest.mark.asyncio
async def test_digest_mutation_after_confirmation_before_dispatch_fails_closed():
    validation_result = _validated("draft_local_message", {"body": "confirm me"})
    runtime = FakeRuntime(
        phase_decisions={
            "selection": {
                "allowed": False,
                "decision_code": "confirmation_required",
                "reason_codes": ["confirmation_required"],
                "challenge_ref": "challenge-1",
            },
            "dispatch": {
                "allowed": True,
                "decision_code": "allowed",
                "reason_codes": ["allowed"],
            },
        }
    )
    original_confirm = runtime.confirm_capability

    async def mutate_then_confirm(**kwargs):
        validation_result.normalized_arguments["body"] = "mutated"
        return await original_confirm(**kwargs)

    runtime.confirm_capability = mutate_then_confirm

    result = await _execute(
        runtime,
        validation_result,
        confirmation={
            "challenge_ref": "challenge-1",
            "capability_id": validation_result.capability_id,
            "argument_digest": validation_result.argument_digest,
            "confirmed": True,
        },
    )

    assert [call["authorization_phase"] for call in runtime.calls] == ["selection"]
    assert result.trace["confirmation"]["status"] == "accepted"
    assert result.trace["failure_reason_code"] == "argument_digest_mismatch"
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0


@pytest.mark.asyncio
async def test_confirmation_trace_omits_raw_arguments_provider_payload_and_user_prose():
    validation_result = _validated("draft_local_message", {"body": "PRIVATE RAW BODY"})
    runtime = FakeRuntime(
        phase_decisions={
            "selection": {
                "allowed": False,
                "decision_code": "confirmation_required",
                "reason_codes": ["confirmation_required"],
                "challenge_ref": "challenge-1",
            },
            "dispatch": {
                "allowed": True,
                "decision_code": "allowed",
                "reason_codes": ["allowed"],
            },
        }
    )

    result = await _execute(
        runtime,
        validation_result,
        confirmation={
            "challenge_ref": "challenge-1",
            "capability_id": validation_result.capability_id,
            "argument_digest": validation_result.argument_digest,
            "confirmed": True,
            "raw_user_prose": "yes please use PRIVATE RAW BODY",
        },
    )

    serialized = json.dumps(result.trace["confirmation"])
    assert "PRIVATE RAW BODY" not in serialized
    assert "raw_user_prose" not in serialized
    assert result.trace["confirmation"] == {
        "status": "accepted",
        "challenge_ref_present": True,
        "accepted": True,
        "call_count": 1,
        "reason_code": "accepted",
        "capability_id": "draft.local_message",
        "argument_digest": validation_result.argument_digest,
        "confirmed_challenge_ref": "challenge-1",
    }


@pytest.mark.asyncio
async def test_revalidation_success_verifies_reruns_selection_then_dispatches_once():
    validation_result = _validated("runtime_world_state_read", {"output_mode": "summary"})
    runtime = FakeRuntime(
        phase_decisions={
            "selection": [
                {
                    "allowed": False,
                    "decision_code": "revalidation_required",
                    "reason_codes": ["world_state_revalidation_required"],
                    "revalidation_selector": {
                        "revalidator_id": "trusted_refresh",
                        "world_state_claim_ids": ["claim-1"],
                    },
                },
                {
                    "allowed": True,
                    "decision_code": "allowed",
                    "reason_codes": ["allowed"],
                },
            ],
            "dispatch": {
                "allowed": True,
                "decision_code": "allowed",
                "reason_codes": ["allowed"],
            },
        },
    )

    result = await _execute(
        runtime,
        validation_result,
        {"trusted_refresh": _trusted_revalidator()},
    )

    assert [call["authorization_phase"] for call in runtime.calls] == [
        "selection",
        "selection",
        "dispatch",
    ]
    assert runtime.calls[0]["argument_digest"] == validation_result.argument_digest
    assert runtime.calls[1]["argument_digest"] == validation_result.argument_digest
    assert len(runtime.world_state_verification_calls) == 1
    verify_call = runtime.world_state_verification_calls[0]
    assert verify_call == {
        "request_id": "rid:runtime.world_state.read:verify:0",
        "owner_id": "owner",
        "conversation_id": "conv",
        "surface": "dev",
        "runtime_session_id": "rtsession_1",
        "runtime_turn_id": "rtturn_1",
        "world_state_claim_id": "claim-1",
        "expected_value_digest": "wsvalue_claim-1",
        "verifier_id": "cr-verifier-local",
        "verification_source_type": "tool_output",
        "verification_source_ref": "local-deterministic-revalidator",
        "observed_at": "2026-07-06T00:00:00+00:00",
        "verified_at": "2026-07-06T00:00:01+00:00",
        "resulting_authority": "verified_tool_output",
        "resulting_confidence": 0.9,
        "resulting_freshness_state": "fresh",
        "resulting_ttl_seconds": 300,
        "resulting_revalidation_interval_seconds": 120,
    }
    assert result.trace["revalidation"] == {
        "status": "verified",
        "revalidator_id": "trusted_refresh",
        "selected_claim_count": 1,
        "configured_revalidator_matched": True,
        "verification_call_count": 1,
        "verification_success_count": 1,
        "verification_failure_count": 0,
        "rerun_selection_status": "allowed",
        "reason_code": "verified",
    }
    assert result.trace["executor_called"] is True
    assert result.trace["executor_call_count"] == 1
    assert "claim-1" not in json.dumps(result.trace)
    assert "wsvalue_claim-1" not in json.dumps(result.trace)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("selector", "revalidators", "reason_code"),
    [
        (
            {"revalidator_id": "missing", "world_state_claim_ids": ["claim-1"]},
            {},
            "unknown_revalidator_id",
        ),
        (None, {"trusted_refresh": _trusted_revalidator()}, "malformed_revalidation_selector"),
        (
            {
                "revalidator_id": "trusted_refresh",
                "world_state_claim_ids": [],
            },
            {"trusted_refresh": _trusted_revalidator()},
            "malformed_revalidation_selector",
        ),
    ],
)
async def test_revalidation_selector_failures_are_zero_executor(
    selector,
    revalidators,
    reason_code,
):
    runtime = FakeRuntime(
        phase_decisions={
            "selection": {
                "allowed": False,
                "decision_code": "revalidation_required",
                "reason_codes": ["world_state_revalidation_required"],
                "revalidation_selector": selector,
            }
        }
    )

    result = await _execute(
        runtime,
        _validated("runtime_world_state_read", {"output_mode": "summary"}),
        revalidators,
    )

    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert runtime.world_state_calls == []
    assert runtime.world_state_verification_calls == []
    assert result.trace["revalidation"]["reason_code"] == reason_code


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("revalidator", "reason_code"),
    [
        (_trusted_revalidator(raises=RuntimeError("down")), "revalidator_unavailable"),
        (
            _trusted_revalidator(
                outputs=[
                    RevalidationOutput(
                        claim_id="claim-1",
                        expected_value_digest="wsvalue_claim-1",
                        observed_at="2026-07-06T00:00:00+00:00",
                        verified_at="2026-07-06T00:00:01+00:00",
                        status="failed",
                        reason_code="source_unavailable",
                    )
                ]
            ),
            "source_unavailable",
        ),
        (_trusted_revalidator(outputs=[{"claim_id": "claim-1"}]), "malformed_revalidator_output"),
    ],
)
async def test_revalidator_output_failures_are_zero_executor(revalidator, reason_code):
    runtime = FakeRuntime(
        phase_decisions={
            "selection": {
                "allowed": False,
                "decision_code": "revalidation_required",
                "reason_codes": ["world_state_revalidation_required"],
                "revalidation_selector": {
                    "revalidator_id": "trusted_refresh",
                    "world_state_claim_ids": ["claim-1"],
                },
            }
        }
    )

    result = await _execute(
        runtime,
        _validated("runtime_world_state_read", {"output_mode": "summary"}),
        {"trusted_refresh": revalidator},
    )

    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert runtime.world_state_verification_calls == []
    assert result.trace["revalidation"]["reason_code"] == reason_code


@pytest.mark.asyncio
async def test_cr_verification_failure_blocks_with_zero_executor():
    runtime = FakeRuntime(
        verification_error=RuntimeError("verify failed"),
        phase_decisions={
            "selection": {
                "allowed": False,
                "decision_code": "revalidation_required",
                "reason_codes": ["world_state_revalidation_required"],
                "revalidation_selector": {
                    "revalidator_id": "trusted_refresh",
                    "world_state_claim_ids": ["claim-1"],
                },
            }
        },
    )

    result = await _execute(
        runtime,
        _validated("runtime_world_state_read", {"output_mode": "summary"}),
        {"trusted_refresh": _trusted_revalidator()},
    )

    assert len(runtime.world_state_verification_calls) == 1
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert result.trace["revalidation"]["reason_code"] == "verification_failed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "verification_response",
    [
        {
            "claim": {
                "world_state_claim_id": "different-claim",
                "verification_verifier_id": "cr-verifier-local",
                "verification_source_type": "tool_output",
                "verification_source_ref": "local-deterministic-revalidator",
                "value_json": "PRIVATE-RAW-VERIFY-PAYLOAD",
            }
        },
        {
            "claim": {
                "verification_verifier_id": "cr-verifier-local",
                "verification_source_type": "tool_output",
                "verification_source_ref": "local-deterministic-revalidator",
                "value_json": "PRIVATE-RAW-VERIFY-PAYLOAD",
            }
        },
    ],
)
async def test_cr_verification_claim_mismatch_blocks_before_rerun_or_dispatch(
    verification_response,
):
    runtime = FakeRuntime(
        verification_response=verification_response,
        phase_decisions={
            "selection": [
                {
                    "allowed": False,
                    "decision_code": "revalidation_required",
                    "reason_codes": ["world_state_revalidation_required"],
                    "revalidation_selector": {
                        "revalidator_id": "trusted_refresh",
                        "world_state_claim_ids": ["claim-1"],
                    },
                },
                {
                    "allowed": True,
                    "decision_code": "allowed",
                    "reason_codes": ["allowed"],
                },
            ],
            "dispatch": {
                "allowed": True,
                "decision_code": "allowed",
                "reason_codes": ["allowed"],
            },
        },
    )

    result = await _execute(
        runtime,
        _validated("runtime_world_state_read", {"output_mode": "summary"}),
        {"trusted_refresh": _trusted_revalidator()},
    )

    assert [call["authorization_phase"] for call in runtime.calls] == ["selection"]
    assert len(runtime.world_state_verification_calls) == 1
    assert runtime.world_state_calls == []
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert result.trace["authorization"]["dispatch"]["status"] == "not_requested"
    assert result.trace["revalidation"]["status"] == "blocked"
    assert result.trace["revalidation"]["reason_code"] == "verification_claim_mismatch"
    assert result.trace["revalidation"]["verification_call_count"] == 1
    assert result.trace["revalidation"]["verification_success_count"] == 0
    assert result.trace["revalidation"]["verification_failure_count"] == 1
    assert result.trace["revalidation"]["rerun_selection_status"] is None
    serialized_trace = json.dumps(result.trace)
    assert "wsvalue_claim-1" not in serialized_trace
    assert "PRIVATE-RAW-VERIFY-PAYLOAD" not in serialized_trace
    assert "different-claim" not in serialized_trace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rerun_decision", "expected_reason"),
    [
        (
            {
                "allowed": False,
                "decision_code": "authorization_denied",
                "reason_codes": ["capability_domain_denied"],
            },
            "capability_domain_denied",
        ),
        (
            {
                "allowed": False,
                "decision_code": "confirmation_required",
                "reason_codes": ["confirmation_required"],
                "challenge_ref": "challenge-rerun",
            },
            "confirmation_required",
        ),
        (
            {
                "allowed": False,
                "decision_code": "revalidation_required",
                "reason_codes": ["world_state_revalidation_required"],
                "revalidation_selector": {
                    "revalidator_id": "trusted_refresh",
                    "world_state_claim_ids": ["claim-1"],
                },
            },
            "revalidation_loop_blocked",
        ),
    ],
)
async def test_rerun_selection_blocks_without_loop_or_executor(
    rerun_decision,
    expected_reason,
):
    runtime = FakeRuntime(
        phase_decisions={
            "selection": [
                {
                    "allowed": False,
                    "decision_code": "revalidation_required",
                    "reason_codes": ["world_state_revalidation_required"],
                    "revalidation_selector": {
                        "revalidator_id": "trusted_refresh",
                        "world_state_claim_ids": ["claim-1"],
                    },
                },
                rerun_decision,
            ],
        }
    )

    result = await _execute(
        runtime,
        _validated("runtime_world_state_read", {"output_mode": "summary"}),
        {"trusted_refresh": _trusted_revalidator()},
    )

    assert [call["authorization_phase"] for call in runtime.calls] == [
        "selection",
        "selection",
    ]
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    if expected_reason == "confirmation_required":
        assert result.trace["revalidation"]["reason_code"] == "verified"
        assert result.trace["failure_reason_code"] == "confirmation_missing"
        assert (
            result.trace["authorization"]["selection"]["confirmation_challenge_ref"]
            == "challenge-rerun"
        )
    else:
        assert result.trace["revalidation"]["reason_code"] == expected_reason


@pytest.mark.asyncio
async def test_dispatch_after_revalidation_is_still_required_and_can_block():
    runtime = FakeRuntime(
        phase_decisions={
            "selection": [
                {
                    "allowed": False,
                    "decision_code": "revalidation_required",
                    "reason_codes": ["world_state_revalidation_required"],
                    "revalidation_selector": {
                        "revalidator_id": "trusted_refresh",
                        "world_state_claim_ids": ["claim-1"],
                    },
                },
                {
                    "allowed": True,
                    "decision_code": "allowed",
                    "reason_codes": ["allowed"],
                },
            ],
            "dispatch": {
                "allowed": False,
                "decision_code": "authorization_denied",
                "reason_codes": ["capability_domain_denied"],
            },
        }
    )

    result = await _execute(
        runtime,
        _validated("runtime_world_state_read", {"output_mode": "summary"}),
        {"trusted_refresh": _trusted_revalidator()},
    )

    assert [call["authorization_phase"] for call in runtime.calls] == [
        "selection",
        "selection",
        "dispatch",
    ]
    assert result.trace["revalidation"]["status"] == "verified"
    assert result.trace["authorization"]["dispatch"]["status"] == "authorization_denied"
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0


@pytest.mark.asyncio
async def test_argument_mutation_between_selection_and_dispatch_fails_closed():
    validation_result = _validated("draft_local_message", {"body": "original"})
    validation_result.normalized_arguments["body"] = "mutated"
    runtime = FakeRuntime(phase_decisions=_allowed_phase_decisions())

    result = await _execute(runtime, validation_result)

    assert [call["authorization_phase"] for call in runtime.calls] == ["selection"]
    assert result.trace["executor_called"] is False
    assert result.trace["executor_call_count"] == 0
    assert result.trace["failure_reason_code"] == "argument_digest_mismatch"


@pytest.mark.asyncio
async def test_executor_failure_does_not_fabricate_success():
    runtime = FakeRuntime(
        phase_decisions=_allowed_phase_decisions(),
        world_state_error=RuntimeError("runtime down"),
    )

    result = await _execute(
        runtime,
        _validated("runtime_world_state_read", {"output_mode": "summary"}),
    )

    assert result.trace["executor_called"] is True
    assert result.trace["executor_call_count"] == 1
    assert result.trace["executor_result_status"] == "failed"
    assert result.trace["response_status"] == "executor_failed"
    assert "success" not in result.response_text.lower()


def test_malformed_hidden_and_multiple_provider_requests_have_no_executor_path():
    with pytest.raises(CapabilityValidationError) as multiple:
        parse_provider_capability_request(
            _completion(
                {
                    "tool_calls": [
                        {"function": {"name": "draft_local_message", "arguments": "{}"}},
                        {"function": {"name": "runtime_world_state_read", "arguments": "{}"}},
                    ]
                }
            )
        )
    assert multiple.value.reason_code == "multiple_capability_calls"

    with pytest.raises(CapabilityValidationError) as hidden:
        parse_provider_capability_request(_call("hidden_tool", {}))
    assert hidden.value.reason_code == "unknown_capability_id"

    with pytest.raises(CapabilityValidationError) as malformed:
        parse_provider_capability_request(
            _completion(
                {
                    "tool_calls": [
                        {
                            "function": {
                                "name": "draft_local_message",
                                "arguments": "{bad",
                            }
                        }
                    ]
                }
            )
        )
    assert malformed.value.reason_code == "malformed_arguments"


def test_argument_digest_helper_matches_validation_digest():
    validation_result = _validated("draft_local_message", {"body": "hello"})

    assert validation_result.argument_digest == argument_digest(
        validation_result.capability_id,
        validation_result.normalized_arguments,
    )


_PENDING_EXPIRES_AT = "2026-07-12T01:00:00+00:00"
_JELLYFIN_CLAIMS = [{"claim_id": "claim_jellyfin_safe", "value_digest": "wsvalue_safe"}]


def _pending_envelope(digest: str) -> dict[str, object]:
    return {
        "schema_version": "co.pending-action.v1",
        "status": "pending_confirmation",
        "capability_id": "jellyfin_restart",
        "target": "service:jellyfin",
        "argument_digest": digest,
        "challenge_ref": "challenge-jellyfin-1",
        "challenge_expires_at": _PENDING_EXPIRES_AT,
        "confirmation_text": (
            "Confirm Restart Jellyfin. This may be difficult to reverse."
        ),
    }


class JellyfinStateMachine:
    def __init__(
        self,
        *,
        restart_status="completed",
        health_status="healthy",
        safety_status="safe",
    ):
        self.restart_status = restart_status
        self.health_status = health_status
        self.safety_status = safety_status
        self.status_inputs = []
        self.restart_inputs = []

    async def status(self, value):
        self.status_inputs.append(value)
        state = self.safety_status if value["purpose"] == "revalidation" else self.health_status
        if isinstance(state, BaseException):
            raise state
        if state == "malformed":
            return {"status": "unknown"}
        claims = value["claims"]
        if state == "mismatched":
            state = "safe" if value["purpose"] == "revalidation" else "healthy"
            claims = [{"claim_id": "claim_other", "value_digest": "wsvalue_other"}]
        return {
            "status": state,
            "reason_code": f"simulated_{state}",
            "observed_at": "2026-07-12T00:00:00+00:00",
            "verified_at": "2026-07-12T00:00:01+00:00",
            "claims": claims,
        }

    async def restart(self, value):
        self.restart_inputs.append(value)
        if isinstance(self.restart_status, BaseException):
            raise self.restart_status
        if self.restart_status == "malformed":
            return {"status": "completed", "reason_code": "simulated", "extra": "ignore"}
        return {
            "status": self.restart_status,
            "reason_code": f"simulated_{self.restart_status}",
        }

    def binding(self):
        return JellyfinOperations(
            effect_mode="simulated",
            status=self.status,
            restart=self.restart,
        )


class JellyfinPolicyRuntime:
    def __init__(self, *, selector_claim_id="claim_jellyfin_safe"):
        self.authorization_calls = []
        self.verification_calls = []
        self.confirmation_calls = []
        self.selection_count = 0
        self.dispatch_count = 0
        self.consumed = False
        self.selector_claim_id = selector_claim_id

    async def authorize_capability(self, **kwargs):
        self.authorization_calls.append(kwargs)
        stage = kwargs.get("authorization_" + "pha" + "se")
        if stage == "exposure":
            return {"result": {"allowed": True, "decision_code": "allowed"}}
        if stage == "dispatch":
            self.dispatch_count += 1
            allowed = not self.consumed
            self.consumed = self.consumed or allowed
            return {
                "result": {
                    "allowed": allowed,
                    "decision_code": "allowed" if allowed else "challenge_consumed",
                    "reason_codes": ["allowed" if allowed else "challenge_consumed"],
                    "challenge_ref": kwargs.get("confirmation_challenge_ref"),
                    "world_state_claim_ids_used": ["claim_jellyfin_safe"],
                }
            }
        self.selection_count += 1
        incoming = kwargs.get("confirmation_challenge_ref")
        if self.consumed:
            return {
                "result": {
                    "allowed": False,
                    "decision_code": "challenge_consumed",
                    "reason_codes": ["challenge_consumed"],
                }
            }
        if incoming not in {None, "challenge-jellyfin-1"}:
            return {
                "result": {
                    "allowed": False,
                    "decision_code": "challenge_mismatch",
                    "reason_codes": ["challenge_mismatch"],
                }
            }
        if self.selection_count % 2 == 1:
            return {
                "result": {
                    "allowed": False,
                    "decision_code": "revalidation_required",
                    "reason_codes": ["revalidation_required"],
                    "challenge_ref": incoming,
                    "revalidation_selector": {
                        "revalidator_id": "jellyfin_status",
                        "world_state_claim_ids": [self.selector_claim_id],
                    },
                    "world_state_claim_ids_used": [self.selector_claim_id],
                }
            }
        return {
            "result": {
                "allowed": False,
                "decision_code": "confirmation_required",
                "reason_codes": ["confirmation_required"],
                "challenge_ref": "challenge-jellyfin-1",
                "challenge_expires_at": _PENDING_EXPIRES_AT,
                "world_state_claim_ids_used": ["claim_jellyfin_safe"],
            }
        }

    async def world_state_claim_verify(self, **kwargs):
        self.verification_calls.append(kwargs)
        return {
            "claim": {
                "world_state_claim_id": kwargs["world_state_claim_id"],
                "verification_verifier_id": kwargs["verifier_id"],
                "verification_source_type": kwargs["verification_source_type"],
                "verification_source_ref": kwargs["verification_source_ref"],
                "last_verified_runtime_session_id": kwargs["runtime_session_id"],
                "last_verified_runtime_turn_id": kwargs["runtime_turn_id"],
            }
        }

    async def confirm_capability(self, **kwargs):
        self.confirmation_calls.append(kwargs)
        return {
            "request_id": kwargs["request_id"],
            "owner_id": kwargs["owner_id"],
            "conversation_id": kwargs["conversation_id"],
            "runtime_session_id": kwargs["runtime_session_id"],
            "runtime_turn_id": kwargs["runtime_turn_id"],
            "confirmation_challenge_ref": kwargs["confirmation_challenge_ref"],
            "confirmation_state": "accepted" if kwargs["confirmed"] else "rejected",
        }


def _jellyfin_validation():
    return _validated("jellyfin_safe_restart", {"target": "service:jellyfin"})


async def _jellyfin_execute(
    runtime,
    adapter,
    confirmation=None,
    *,
    connector=None,
    verification_required=True,
):
    connector = connector or JellyfinActionConnector(adapter.binding())
    return await authorize_and_execute_capability(
        runtime=runtime,
        request_id="rid-jellyfin",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_jellyfin",
        runtime_turn_id="rtturn_jellyfin",
        active_persona_id="technical_architect",
        validation_result=_jellyfin_validation(),
        selected_world_state_claims=_JELLYFIN_CLAIMS,
        connector_registry=ActionConnectorRegistry((connector,)),
        capability_confirmation=confirmation,
        post_execution_verification_required=verification_required,
    )


class RecordingJellyfinConnector(JellyfinActionConnector):
    def __init__(self, operations):
        super().__init__(operations)
        self.execute_calls = 0
        self.verify_calls = 0

    async def execute(self, request):
        self.execute_calls += 1
        return await super().execute(request)

    async def verify(self, request):
        self.verify_calls += 1
        return await super().verify(request)


def test_pending_action_models_retain_bounded_input_and_output():
    digest = _jellyfin_validation().argument_digest
    pending = _pending_envelope(digest)
    request = ChatRequest(
        owner_id="owner",
        surface="dev",
        messages=[{"role": "user", "content": "yes"}],
        capability_confirmation={"pending_action": pending, "confirmed": True},
    )
    dumped = request.model_dump()
    assert dumped["capability_confirmation"] == {
        "challenge_ref": None,
        "capability_id": None,
        "argument_digest": None,
        "confirmed": True,
        "pending_action": pending,
    }
    continuation, reason = parse_pending_action_confirmation(
        dumped["capability_confirmation"]
    )
    assert reason is None
    assert continuation.challenge_ref == "challenge-jellyfin-1"
    response = ChatResponse(
        request_id="rid",
        conversation_id="conv",
        profile_name="dev",
        selected_model="local-model",
        answer="Confirmation is pending.",
        status="ok",
        pending_action=pending,
    )
    assert response.model_dump()["pending_action"] == pending


def _display_entry():
    return CapabilityEntry(
        capability_id="fixture.display_setting_apply",
        provider_tool_name="fixture_display_setting_apply",
        operation_class="state_change",
        capability_domain="display_preferences",
        supported_surfaces=("dev",),
        executor_binding="action_connector",
        descriptor_metadata={"display_name": "Apply display setting", "description": "Test."},
        privacy_classification="bounded_setting_action",
        authorization_requirements={
            "relationship_requirements": [],
            "world_state_requirements": [
                {
                    "domain": "active_external_system",
                    "entity_id": "fixture:display",
                    "attribute": "setting_available",
                }
            ],
        },
        argument_schema={
            "type": "object",
            "additionalProperties": False,
            "required": ["target", "level"],
            "properties": {"target": {"type": "string"}, "level": {"type": "integer"}},
        },
        policy_shape=CapabilityPolicyShape(
            registry_domain="display_preferences",
            operation_kind="state_change",
            risk_level="low_display_change",
            requires_confirmation=True,
            reversible=True,
            dry_run_supported=True,
            verification_supported=False,
        ),
    )


def test_generic_pending_parser_and_connector_restoration_bind_digest(monkeypatch):
    entry = _display_entry()
    operations = DisplaySettingOperations()
    connector = DisplaySettingConnector(operations)
    arguments = connector.normalize_arguments({"target": "fixture:display", "level": 7})
    description = connector.describe_continuation(arguments)
    digest = argument_digest(entry.capability_id, arguments.as_dict())
    pending = {
        "schema_version": "co.pending-action.v1",
        "status": "pending_confirmation",
        "capability_id": entry.capability_id,
        "target": description.target,
        "argument_digest": digest,
        "challenge_ref": "challenge-display-1",
        "challenge_expires_at": "2026-07-14T01:00:00+00:00",
        "confirmation_text": description.confirmation_text,
    }
    continuation, reason = parse_pending_action_confirmation(
        {"pending_action": pending, "confirmed": True}
    )
    monkeypatch.setattr(capability_service, "capability_by_id", lambda value: entry)

    restored = restore_pending_action_request(
        continuation=continuation,
        connector_registry=ActionConnectorRegistry((connector,)),
    )

    assert reason is None
    assert restored.arguments == {"level": 7, "target": "fixture:display"}
    assert argument_digest(restored.capability_id, restored.arguments) == digest
    assert operations.apply_inputs == []


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("target", "not a safe target", "malformed_pending_action"),
        ("confirmation_text", "", "malformed_pending_action"),
        ("capability_id", "plain", "malformed_pending_action"),
    ],
)
def test_generic_pending_parser_rejects_malformed_bounded_values(field, value, reason):
    pending = {
        "schema_version": "co.pending-action.v1",
        "status": "pending_confirmation",
        "capability_id": "fixture.display_setting_apply",
        "target": "fixture:display",
        "argument_digest": "capargs_fixture",
        "challenge_ref": "challenge-display-1",
        "challenge_expires_at": "2026-07-14T01:00:00+00:00",
        "confirmation_text": "Confirm display level 7 for fixture:display.",
    }
    pending[field] = value
    continuation, parse_reason = parse_pending_action_confirmation(
        {"pending_action": pending, "confirmed": True}
    )
    assert continuation is None
    assert parse_reason == reason


def test_generic_claim_selection_matches_registered_facts_and_is_bounded():
    selected = _select_capability_claim_refs(
        _display_entry(),
        [
            {
                "domain": "active_external_system",
                "entity_id": "fixture:display",
                "attribute": "setting_available",
                "claim_id": "claim_b",
                "value_digest": "digest_b",
            },
            {
                "domain": "active_external_system",
                "entity_id": "fixture:display",
                "attribute": "setting_available",
                "claim_id": "claim_a",
                "value_digest": "digest_a",
            },
            {
                "domain": "active_external_system",
                "entity_id": "fixture:other",
                "attribute": "setting_available",
                "claim_id": "claim_other",
                "value_digest": "digest_other",
            },
        ],
    )
    assert selected == [
        {"claim_id": "claim_a", "value_digest": "digest_a"},
        {"claim_id": "claim_b", "value_digest": "digest_b"},
    ]
    assert _select_capability_claim_refs(
        CapabilityEntry(
            **{
                **_display_entry().__dict__,
                "authorization_requirements": {
                    "relationship_requirements": [],
                    "world_state_requirements": [],
                },
            }
        ),
        [],
    ) == []


@pytest.mark.parametrize(
    "confirmation",
    [
        {
            "pending_action": {**_pending_envelope("capargs_valid"), "extra": "no"},
            "confirmed": True,
        },
        {
            "pending_action": {
                **_pending_envelope("capargs_valid"),
                "challenge_ref": "",
            },
            "confirmed": True,
        },
        {"pending_action": _pending_envelope("capargs_valid"), "confirmed": True, "extra": "no"},
    ],
)
def test_pending_action_models_reject_unknown_or_malformed_fields(confirmation):
    with pytest.raises(ValidationError):
        ChatRequest(
            owner_id="owner",
            messages=[{"role": "user", "content": "yes"}],
            capability_confirmation=confirmation,
        )


def test_legacy_flat_confirmation_model_remains_accepted():
    request = ChatRequest(
        owner_id="owner",
        messages=[{"role": "user", "content": "confirm"}],
        capability_confirmation={
            "challenge_ref": "challenge-1",
            "capability_id": "draft.local_message",
            "argument_digest": "capargs_legacy",
            "confirmed": True,
        },
    )
    assert request.capability_confirmation.pending_action is None
    assert request.capability_confirmation.confirmed is True


def test_jellyfin_descriptor_and_fixed_target_are_exact():
    entry = production_capability_registry()[-1]
    descriptor = provider_descriptors([entry])[0]
    assert descriptor["metadata"] == {
        "capability_id": "jellyfin_restart",
        "provider_tool_name": "jellyfin_safe_restart",
        "operation_class": "high_impact",
        "capability_domain": "media_operations",
        "privacy_classification": "bounded_service_action",
        "descriptor_version": "co.capability-descriptor.v1",
        "schema_version": "co.capability-args.v1",
        "local_only": False,
    }
    assert entry.supported_surfaces == ("desktop", "dev")
    assert entry.enabled_surfaces == ("dev",)
    assert entry.enabled_personas == ("technical_architect",)
    assert descriptor["function"]["parameters"] == {
        "type": "object",
        "additionalProperties": False,
        "required": ["target"],
        "properties": {"target": {"type": "string", "enum": ["service:jellyfin"]}},
    }


def test_jellyfin_world_state_policy_is_owned_by_shared_capability_service():
    expected = [
        {
            "domain": "active_external_system",
            "entity_id": "service:jellyfin",
            "attribute": "restart_safe",
            "min_authority": "trusted_integration_event",
            "min_confidence": 0.9,
            "max_freshness_state": "fresh",
            "revalidator_id": "jellyfin_status",
        }
    ]
    assert capability_service.JELLYFIN_WORLD_STATE_REQUIREMENTS == expected
    assert production_capability_registry()[-1].authorization_requirements[
        "world_state_requirements"
    ] is capability_service.JELLYFIN_WORLD_STATE_REQUIREMENTS


@pytest.mark.parametrize(
    "arguments",
    [
        {},
        {"target": "service:other"},
        {"target": "https://service.invalid"},
        {"target": "media-host"},
        {"target": "restart jellyfin"},
        {"target": "service:jellyfin", "force": True},
    ],
)
def test_jellyfin_rejects_every_non_exact_argument_shape(arguments):
    request = parse_provider_capability_request(_call("jellyfin_safe_restart", arguments))
    with pytest.raises(CapabilityValidationError) as exc:
        validate_and_digest_capability_request(
            request=request,
            exposed_capability_ids=["jellyfin_restart"],
            connector_registry=ActionConnectorRegistry(
                (JellyfinActionConnector(None),)
            ),
        )
    assert exc.value.reason_code == "schema_invalid_arguments"


def test_connector_lookup_drives_jellyfin_normalization():
    class RecordingConnector(JellyfinActionConnector):
        def __init__(self):
            super().__init__(None)
            self.normalization_calls = []

        def normalize_arguments(self, arguments):
            self.normalization_calls.append(arguments)
            return super().normalize_arguments(arguments)

    connector = RecordingConnector()
    request = parse_provider_capability_request(
        _call("jellyfin_safe_restart", {"target": "service:jellyfin"})
    )
    result = validate_and_digest_capability_request(
        request=request,
        exposed_capability_ids=["jellyfin_restart"],
        connector_registry=ActionConnectorRegistry((connector,)),
    )
    assert connector.normalization_calls == [{"target": "service:jellyfin"}]
    assert result.normalized_arguments == {"target": "service:jellyfin"}


@pytest.mark.parametrize(
    "connector_registry",
    [None, ActionConnectorRegistry(())],
)
def test_connector_backed_normalization_requires_explicit_known_registry(
    connector_registry,
):
    request = parse_provider_capability_request(
        _call("jellyfin_safe_restart", {"target": "service:jellyfin"})
    )
    with pytest.raises(CapabilityValidationError) as exc:
        validate_and_digest_capability_request(
            request=request,
            exposed_capability_ids=["jellyfin_restart"],
            connector_registry=connector_registry,
        )
    assert exc.value.reason_code == "connector_unavailable"
    assert not hasattr(capability_service, "JellyfinActionConnector")


@pytest.mark.asyncio
async def test_shared_jellyfin_revalidator_mapping_cannot_be_overridden_by_connector():
    class PolicyClaimingConnector(JellyfinActionConnector):
        resulting_authority = "connector_declared"
        resulting_confidence = 0.1
        resulting_freshness_state = "stale"
        supported_domains = ("connector_domain",)
        supported_attributes = ("connector_attribute",)

        async def revalidate(self, request):
            result = await super().revalidate(request)
            object.__setattr__(result, "resulting_authority", "result_declared")
            object.__setattr__(result, "resulting_confidence", 0.2)
            object.__setattr__(result, "resulting_freshness_state", "expired")
            return result

    adapter = JellyfinStateMachine()
    connector = PolicyClaimingConnector(adapter.binding())
    revalidator = capability_service._connector_revalidator(
        connector=connector,
        request_id="rid-jellyfin-policy",
        normalized_arguments={"target": "service:jellyfin"},
        world_state_claims=_JELLYFIN_CLAIMS,
    )

    assert revalidator is not None
    assert revalidator.entry == RevalidatorEntry(
        revalidator_id="jellyfin_status",
        verifier_id="jellyfin_status",
        verification_source_type="tool_output",
        verification_source_ref="jellyfin_status",
        supported_domains=("active_external_system",),
        supported_attributes=("restart_safe",),
        resulting_authority="verified_tool_output",
        resulting_confidence=1.0,
        resulting_freshness_state="fresh",
    )
    outputs = await revalidator.verify(("claim_jellyfin_safe",))
    assert len(outputs) == 1
    assert outputs[0].source_type == "tool_output"
    assert outputs[0].source_ref == "jellyfin_status"
    assert outputs[0].resulting_authority == "verified_tool_output"
    assert outputs[0].confidence == 1.0
    assert outputs[0].freshness_state == "fresh"


@pytest.mark.asyncio
async def test_unknown_connector_blocks_connector_backed_execution():
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    result = await authorize_and_execute_capability(
        runtime=runtime,
        request_id="rid-jellyfin-missing-connector",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_jellyfin",
        runtime_turn_id="rtturn_jellyfin",
        active_persona_id="technical_architect",
        validation_result=_jellyfin_validation(),
        selected_world_state_claims=_JELLYFIN_CLAIMS,
        connector_registry=ActionConnectorRegistry(()),
    )
    assert result.trace["failure_reason_code"] == "connector_unavailable"
    assert runtime.authorization_calls == []
    assert adapter.status_inputs == []
    assert adapter.restart_inputs == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("surface", "persona", "reason"),
    [
        ("desktop", "technical_architect", "surface_not_enabled"),
        ("dev", "general_assistant", "persona_not_enabled"),
    ],
)
async def test_shared_surface_and_persona_gates_precede_connector_availability(
    surface,
    persona,
    reason,
):
    class RecordingAvailabilityConnector(JellyfinActionConnector):
        def __init__(self, operations):
            super().__init__(operations)
            self.availability_requests = []

        def check_availability(self, request):
            self.availability_requests.append(request)
            return super().check_availability(request)

    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    connector = RecordingAvailabilityConnector(adapter.binding())
    descriptors, trace = await filter_capability_descriptors_for_exposure(
        runtime=runtime,
        request_id="rid-shared-policy-gate",
        owner_id="owner",
        conversation_id="conv",
        surface=surface,
        runtime_session_id="rtsession_jellyfin",
        runtime_turn_id="rtturn_jellyfin",
        active_persona_id=persona,
        selected_world_state_claims=_JELLYFIN_CLAIMS,
        connector_registry=ActionConnectorRegistry((connector,)),
        allowed_capability_ids=["jellyfin_restart"],
    )

    assert descriptors == []
    assert trace["blocked_reasons"] == {"jellyfin_restart": reason}
    assert connector.availability_requests == []
    assert runtime.authorization_calls == []


@pytest.mark.asyncio
async def test_connector_availability_receives_no_surface_or_persona_policy_input():
    class RecordingAvailabilityConnector(JellyfinActionConnector):
        def __init__(self, operations):
            super().__init__(operations)
            self.availability_requests = []

        def check_availability(self, request):
            self.availability_requests.append(request)
            return super().check_availability(request)

    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    connector = RecordingAvailabilityConnector(adapter.binding())
    descriptors, _ = await filter_capability_descriptors_for_exposure(
        runtime=runtime,
        request_id="rid-connector-input",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_jellyfin",
        runtime_turn_id="rtturn_jellyfin",
        active_persona_id="technical_architect",
        selected_world_state_claims=_JELLYFIN_CLAIMS,
        connector_registry=ActionConnectorRegistry((connector,)),
        allowed_capability_ids=["jellyfin_restart"],
    )

    assert [item["function"]["name"] for item in descriptors] == [
        "jellyfin_safe_restart"
    ]
    assert len(connector.availability_requests) == 1
    request = connector.availability_requests[0]
    assert not hasattr(request, "surface")
    assert not hasattr(request, "active_persona_id")
    assert request.selected_claims


def test_capability_service_has_no_direct_jellyfin_adapter_path():
    assert not hasattr(capability_service, "_parse_jellyfin_status_result")
    assert not hasattr(capability_service, "_execute_jellyfin_restart")
    assert not hasattr(capability_service, "_JellyfinStatusVerifier")


@pytest.mark.asyncio
async def test_jellyfin_first_turn_and_accepted_continuation_keep_one_challenge():
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    first = await _jellyfin_execute(runtime, adapter)
    digest = _jellyfin_validation().argument_digest
    first_status_calls = [
        item for item in adapter.status_inputs if item["purpose"] == "revalidation"
    ]

    assert first.trace["response_status"] == "pending_confirmation"
    first_selection = first.trace["authorization"]["selection"]
    assert first_selection["confirmation_challenge_ref"] == "challenge-jellyfin-1"
    assert first.trace["authorization"]["selection"]["challenge_expires_at"] == _PENDING_EXPIRES_AT
    assert first.trace["revalidation"]["status_call_count"] == len(first_status_calls) == 1
    assert first.trace["confirmation"]["call_count"] == 0
    assert runtime.dispatch_count == 0
    assert adapter.restart_inputs == []

    before_second = len(first_status_calls)
    second = await _jellyfin_execute(
        runtime,
        adapter,
        {"pending_action": _pending_envelope(digest), "confirmed": True},
    )
    after_second = len(
        [item for item in adapter.status_inputs if item["purpose"] == "revalidation"]
    )
    second_status_calls = after_second - before_second

    assert second.trace["response_status"] == "executed_verified"
    second_selection = second.trace["authorization"]["selection"]
    assert second_selection["confirmation_challenge_ref"] == "challenge-jellyfin-1"
    assert second.trace["authorization"]["selection"]["challenge_expires_at"] == _PENDING_EXPIRES_AT
    assert second.trace["confirmation"]["status"] == "accepted"
    assert second.trace["confirmation"]["call_count"] == 1
    assert second.trace["revalidation"]["status_call_count"] == second_status_calls == 1
    second_dispatch = second.trace["authorization"]["dispatch"]
    assert second_dispatch["confirmation_challenge_ref"] == "challenge-jellyfin-1"
    assert runtime.dispatch_count == 1
    assert len(adapter.restart_inputs) == 1
    assert len(
        [item for item in adapter.status_inputs if item["purpose"] == "revalidation"]
    ) == 2
    assert len(
        [item for item in adapter.status_inputs if item["purpose"] == "post_restart"]
    ) == 1
    assert second.trace["effect_mode"] == "simulated"
    assert second.trace["restart_call_count"] == 1
    assert second.trace["post_restart_verification_call_count"] == 1

    replay = await _jellyfin_execute(
        runtime,
        adapter,
        {"pending_action": _pending_envelope(digest), "confirmed": True},
    )
    assert replay.trace["failure_reason_code"] == "challenge_consumed"
    assert len(adapter.restart_inputs) == 1
    assert runtime.dispatch_count == 1


@pytest.mark.asyncio
async def test_jellyfin_legacy_flat_confirmation_remains_accepted():
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    first = await _jellyfin_execute(runtime, adapter)
    selection = first.trace["authorization"]["selection"]
    result = await _jellyfin_execute(
        runtime,
        adapter,
        {
            "challenge_ref": selection["confirmation_challenge_ref"],
            "capability_id": "jellyfin_restart",
            "argument_digest": _jellyfin_validation().argument_digest,
            "confirmed": True,
        },
    )
    assert result.trace["confirmation"]["status"] == "accepted"
    assert result.trace["response_status"] == "executed_verified"
    assert runtime.dispatch_count == 1
    assert len(adapter.restart_inputs) == 1


@pytest.mark.asyncio
async def test_connector_execution_without_policy_verification_stays_executed():
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    connector = RecordingJellyfinConnector(adapter.binding())
    await _jellyfin_execute(
        runtime,
        adapter,
        connector=connector,
        verification_required=False,
    )
    confirmation = {
        "pending_action": _pending_envelope(_jellyfin_validation().argument_digest),
        "confirmed": True,
    }
    result = await _jellyfin_execute(
        runtime,
        adapter,
        confirmation,
        connector=connector,
        verification_required=False,
    )

    assert connector.execute_calls == len(adapter.restart_inputs) == 1
    assert connector.verify_calls == 0
    assert result.trace["response_status"] == "executed"
    assert result.trace["post_execution_verification"] == {
        "required": False,
        "method": None,
        "status": "not_required",
        "reason_code": None,
        "call_count": 0,
    }


@pytest.mark.asyncio
async def test_connector_execution_with_policy_verification_calls_verify_once():
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    connector = RecordingJellyfinConnector(adapter.binding())
    await _jellyfin_execute(runtime, adapter, connector=connector)
    confirmation = {
        "pending_action": _pending_envelope(_jellyfin_validation().argument_digest),
        "confirmed": True,
    }
    result = await _jellyfin_execute(
        runtime,
        adapter,
        confirmation,
        connector=connector,
    )

    assert connector.execute_calls == len(adapter.restart_inputs) == 1
    assert connector.verify_calls == 1
    assert result.trace["response_status"] == "executed_verified"
    assert result.trace["post_execution_verification"]["status"] == "verified"
    assert result.trace["post_execution_verification"]["call_count"] == 1


@pytest.mark.asyncio
async def test_required_but_unsupported_connector_verification_is_explicit():
    class UnsupportedVerificationConnector(RecordingJellyfinConnector):
        async def verify(self, request):
            self.verify_calls += 1
            return ConnectorVerificationResult(
                VerificationStatus.NOT_SUPPORTED,
                "verification_not_supported",
                "verification_not_supported",
                0,
                effect_mode="simulated",
                target_label="service:jellyfin",
            )

    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    connector = UnsupportedVerificationConnector(adapter.binding())
    await _jellyfin_execute(runtime, adapter, connector=connector)
    confirmation = {
        "pending_action": _pending_envelope(_jellyfin_validation().argument_digest),
        "confirmed": True,
    }
    result = await _jellyfin_execute(
        runtime,
        adapter,
        confirmation,
        connector=connector,
    )

    assert connector.execute_calls == len(adapter.restart_inputs) == 1
    assert connector.verify_calls == 1
    assert result.trace["response_status"] == "executed_unverified"
    assert result.trace["post_execution_verification"]["status"] == "not_supported"
    assert result.trace["post_execution_verification"]["reason_code"] == (
        "verification_not_supported"
    )


@pytest.mark.asyncio
async def test_jellyfin_rejection_records_once_without_dispatch():
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    await _jellyfin_execute(runtime, adapter)
    result = await _jellyfin_execute(
        runtime,
        adapter,
        {
            "pending_action": _pending_envelope(_jellyfin_validation().argument_digest),
            "confirmed": False,
        },
    )
    assert result.trace["confirmation"]["status"] == "rejected"
    assert result.trace["confirmation"]["call_count"] == 1
    assert runtime.dispatch_count == 0
    assert adapter.restart_inputs == []
    assert "No action was taken" in result.response_text


@pytest.mark.asyncio
async def test_mixed_pending_and_legacy_identity_fails_before_dependencies():
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    result = await _jellyfin_execute(
        runtime,
        adapter,
        {
            "pending_action": _pending_envelope(_jellyfin_validation().argument_digest),
            "confirmed": True,
            "challenge_ref": "challenge-jellyfin-1",
        },
    )
    assert result.trace["failure_reason_code"] == "mixed_confirmation_shapes"
    assert runtime.authorization_calls == []
    assert runtime.confirmation_calls == []
    assert adapter.status_inputs == []
    assert adapter.restart_inputs == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("surface", "persona", "with_adapter", "with_claim", "reason"),
    [
        ("desktop", "technical_architect", True, True, "surface_not_enabled"),
        ("dev", "general_assistant", True, True, "persona_not_enabled"),
        ("dev", "technical_architect", False, True, "jellyfin_operations_unavailable"),
        ("dev", "technical_architect", True, False, "restart_safe_claim_unavailable"),
    ],
)
async def test_jellyfin_exposure_fails_closed_when_local_inputs_are_missing(
    surface,
    persona,
    with_adapter,
    with_claim,
    reason,
):
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    descriptors, trace = await filter_capability_descriptors_for_exposure(
        runtime=runtime,
        request_id="rid-exposure",
        owner_id="owner",
        conversation_id="conv",
        surface=surface,
        runtime_session_id="rtsession_jellyfin",
        runtime_turn_id="rtturn_jellyfin",
        active_persona_id=persona,
        selected_world_state_claims=_JELLYFIN_CLAIMS if with_claim else [],
        connector_registry=ActionConnectorRegistry(
            (
                JellyfinActionConnector(
                    adapter.binding() if with_adapter else None
                ),
            )
        ),
        allowed_capability_ids=["jellyfin_restart"],
    )
    assert descriptors == []
    assert trace["blocked_reasons"] == {"jellyfin_restart": reason}
    assert runtime.authorization_calls == []


@pytest.mark.asyncio
async def test_jellyfin_exposure_sends_exact_registered_metadata_without_full_world_check():
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    descriptors, trace = await filter_capability_descriptors_for_exposure(
        runtime=runtime,
        request_id="rid-exposure",
        owner_id="owner",
        conversation_id="conv",
        surface="dev",
        runtime_session_id="rtsession_jellyfin",
        runtime_turn_id="rtturn_jellyfin",
        active_persona_id="technical_architect",
        selected_world_state_claims=_JELLYFIN_CLAIMS,
        connector_registry=ActionConnectorRegistry(
            (JellyfinActionConnector(adapter.binding()),)
        ),
        allowed_capability_ids=["jellyfin_restart"],
    )
    assert [item["function"]["name"] for item in descriptors] == ["jellyfin_safe_restart"]
    assert trace["exposed_capability_ids"] == ["jellyfin_restart"]
    call = runtime.authorization_calls[0]
    assert call["capability_domain"] == "media_operations"
    assert call["operation_class"] == "high_impact"
    assert call["supported_surfaces"] == ["desktop", "dev"]
    assert call["world_state_requirements"] == []
    assert call["selected_world_state_claim_ids"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("capability_id", "draft.local_message", "pending_action_mismatch"),
        ("target", "service:other", "pending_action_target_mismatch"),
        ("argument_digest", "capargs_wrong", "pending_action_digest_mismatch"),
    ],
)
async def test_invalid_pending_identity_creates_no_challenge_or_adapter_call(
    field,
    value,
    reason,
):
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    pending = _pending_envelope(_jellyfin_validation().argument_digest)
    pending[field] = value
    result = await _jellyfin_execute(
        runtime,
        adapter,
        {"pending_action": pending, "confirmed": True},
    )
    assert result.trace["failure_reason_code"] == reason
    assert runtime.authorization_calls == []
    assert runtime.confirmation_calls == []
    assert adapter.status_inputs == []
    assert adapter.restart_inputs == []


@pytest.mark.asyncio
async def test_wrong_challenge_is_denied_without_revalidation_or_replacement():
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine()
    pending = _pending_envelope(_jellyfin_validation().argument_digest)
    pending["challenge_ref"] = "challenge-other"
    result = await _jellyfin_execute(
        runtime,
        adapter,
        {"pending_action": pending, "confirmed": True},
    )
    assert result.trace["failure_reason_code"] == "challenge_mismatch"
    assert result.trace["authorization"]["selection"]["confirmation_challenge_ref"] is None
    assert runtime.confirmation_calls == []
    assert adapter.status_inputs == []
    assert adapter.restart_inputs == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "safety_status",
    ["unsafe", "unknown", "failed", RuntimeError("offline"), "malformed", "mismatched"],
)
async def test_jellyfin_revalidation_failures_never_issue_challenge_or_restart(safety_status):
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine(safety_status=safety_status)
    result = await _jellyfin_execute(runtime, adapter)
    actual_status_calls = len(
        [item for item in adapter.status_inputs if item["purpose"] == "revalidation"]
    )
    assert result.trace["response_status"] == "not_executed"
    assert result.trace["revalidation"]["status_call_count"] == actual_status_calls == 1
    assert result.trace["authorization"]["selection"]["confirmation_challenge_ref"] is None
    assert runtime.confirmation_calls == []
    assert runtime.dispatch_count == 0
    assert adapter.restart_inputs == []


@pytest.mark.asyncio
async def test_jellyfin_missing_selected_digest_records_zero_actual_status_calls():
    runtime = JellyfinPolicyRuntime(selector_claim_id="claim_jellyfin_other")
    adapter = JellyfinStateMachine()

    result = await _jellyfin_execute(runtime, adapter)

    assert adapter.status_inputs == []
    assert result.trace["revalidation"]["status_call_count"] == 0
    assert result.trace["revalidation"]["reason_code"] == "revalidator_claim_mismatch"
    assert runtime.verification_calls == []
    assert runtime.selection_count == 1
    assert runtime.confirmation_calls == []
    assert runtime.dispatch_count == 0
    assert adapter.restart_inputs == []
    assert (
        result.trace["authorization"]["selection"]["confirmation_challenge_ref"]
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("restart_status", "health_status", "response_status", "restart_count", "verify_count"),
    [
        ("failed", "healthy", "executor_failed", 1, 0),
        ("unknown", "healthy", "executor_unknown", 1, 0),
        (RuntimeError("offline"), "healthy", "executor_failed", 1, 0),
        ("malformed", "healthy", "executor_failed", 1, 0),
        ("completed", "unhealthy", "executed_unverified", 1, 1),
        ("completed", "unknown", "executed_unverified", 1, 1),
        ("completed", "failed", "executed_unverified", 1, 1),
        ("completed", RuntimeError("offline"), "executed_unverified", 1, 1),
        ("completed", "malformed", "executed_unverified", 1, 1),
        ("completed", "mismatched", "executed_unverified", 1, 1),
    ],
)
async def test_jellyfin_failure_outcomes_never_retry_or_claim_verified_success(
    restart_status,
    health_status,
    response_status,
    restart_count,
    verify_count,
):
    runtime = JellyfinPolicyRuntime()
    adapter = JellyfinStateMachine(
        restart_status=restart_status,
        health_status=health_status,
    )
    await _jellyfin_execute(runtime, adapter)
    result = await _jellyfin_execute(
        runtime,
        adapter,
        {
            "pending_action": _pending_envelope(_jellyfin_validation().argument_digest),
            "confirmed": True,
        },
    )
    assert result.trace["response_status"] == response_status
    assert len(adapter.restart_inputs) == restart_count
    post_calls = [item for item in adapter.status_inputs if item["purpose"] == "post_restart"]
    assert len(post_calls) == verify_count
    assert "verified it is healthy" not in result.response_text
