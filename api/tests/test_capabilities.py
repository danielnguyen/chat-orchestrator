from __future__ import annotations

import json

import pytest
from services.capabilities import (
    CapabilityValidationError,
    RevalidationOutput,
    Revalidator,
    RevalidatorEntry,
    argument_digest,
    authorize_and_execute_capability,
    descriptor_fingerprint,
    filter_capability_descriptors_for_exposure,
    parse_provider_capability_request,
    production_capability_registry,
    provider_descriptors,
    validate_and_digest_capability_request,
)


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
        self.calls = []
        self.world_state_calls = []
        self.world_state_verification_calls = []
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
        allowed = kwargs["capability_id"] not in self.denied
        return {
            "result": {
                "allowed": allowed,
                "decision_code": "allowed" if allowed else "authorization_denied",
                "reason_codes": ["allowed" if allowed else "capability_domain_denied"],
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
        exposed_capability_ids=["runtime.world_state.read", "draft.local_message"],
    )


async def _execute(runtime: FakeRuntime, validation_result, revalidators=None):
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
        revalidators=revalidators,
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
    ]
    assert all(entry.executor_binding for entry in registry)
    assert all(not entry.capability_id.startswith("test.") for entry in registry)
    assert [entry.provider_tool_name for entry in registry] == [
        "runtime_world_state_read",
        "draft_local_message",
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
        "runtime.world_state.read",
    ]
    assert [item["function"]["name"] for item in first] == [
        "draft_local_message",
        "runtime_world_state_read",
    ]
    assert [item["metadata"]["provider_tool_name"] for item in first] == [
        "draft_local_message",
        "runtime_world_state_read",
    ]
    assert descriptor_fingerprint(first) == descriptor_fingerprint(second)


@pytest.mark.asyncio
async def test_exposure_authorization_allows_both_production_capabilities():
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
    assert trace["blocked_capability_ids"] == []
    assert len(descriptors) == 2
    assert [call["capability_id"] for call in runtime.calls] == [
        "runtime.world_state.read",
        "draft.local_message",
    ]


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
    assert trace["blocked_capability_ids"] == ["draft.local_message"]
    assert trace["blocked_reasons"] == {
        "draft.local_message": "capability_domain_denied"
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


@pytest.mark.parametrize(
    "provider_tool_name",
    ["draft.local_message", "runtime.world_state.read"],
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
    assert result.trace["revalidation"]["reason_code"] == expected_reason
    if expected_reason == "confirmation_required":
        assert (
            result.trace["authorization"]["selection"]["confirmation_challenge_ref"]
            == "challenge-rerun"
        )


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
