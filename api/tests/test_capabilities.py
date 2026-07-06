from __future__ import annotations

import json

import pytest
from services.capabilities import (
    CapabilityValidationError,
    descriptor_fingerprint,
    filter_capability_descriptors_for_exposure,
    parse_provider_capability_request,
    production_capability_registry,
    provider_descriptors,
    validate_and_digest_capability_request,
)


class FakeRuntime:
    def __init__(self, *, denied: set[str] | None = None, malformed: bool = False):
        self.denied = denied or set()
        self.malformed = malformed
        self.calls = []

    async def authorize_capability(self, **kwargs):
        self.calls.append(kwargs)
        if self.malformed:
            return {"result": "bad"}
        allowed = kwargs["capability_id"] not in self.denied
        return {
            "result": {
                "allowed": allowed,
                "decision_code": "allowed" if allowed else "authorization_denied",
                "reason_codes": ["allowed" if allowed else "capability_domain_denied"],
            }
        }


def _completion(message: dict[str, object]) -> dict[str, object]:
    return {"choices": [{"message": message}]}


def _call(capability_id: str, arguments: dict[str, object]) -> dict[str, object]:
    return _completion(
        {
            "tool_calls": [
                {
                    "function": {
                        "name": capability_id,
                        "arguments": json.dumps(arguments),
                    }
                }
            ]
        }
    )


def test_production_registry_contains_exact_executor_bound_entries():
    registry = production_capability_registry()

    assert [entry.capability_id for entry in registry] == [
        "runtime.world_state.read",
        "draft.local_message",
    ]
    assert all(entry.executor_binding for entry in registry)
    assert all(not entry.capability_id.startswith("test.") for entry in registry)


def test_provider_descriptors_are_deterministic_and_fingerprint_stable():
    registry = list(reversed(production_capability_registry()))

    first = provider_descriptors(registry)
    second = provider_descriptors(registry)

    assert first == second
    assert [item["metadata"]["capability_id"] for item in first] == [
        "draft.local_message",
        "runtime.world_state.read",
    ]
    assert descriptor_fingerprint(first) == descriptor_fingerprint(second)


@pytest.mark.asyncio
async def test_exposure_authorization_allows_both_production_capabilities():
    descriptors, trace = await filter_capability_descriptors_for_exposure(
        runtime=FakeRuntime(),
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
    assert descriptor_ids == ["runtime.world_state.read"]
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
        _call("draft.local_message", {"body": "hello"})
    )

    assert request is not None
    assert request.capability_id == "draft.local_message"
    assert request.arguments == {"body": "hello"}


@pytest.mark.parametrize(
    ("completion", "reason"),
    [
        (
            _completion(
                {
                    "tool_calls": [
                        {
                            "function": {
                                "name": "draft.local_message",
                                "arguments": "{\"body\":\"one\"}",
                            }
                        },
                        {
                            "function": {
                                "name": "runtime.world_state.read",
                                "arguments": "{}",
                            }
                        },
                    ]
                }
            ),
            "multiple_capability_calls",
        ),
        (_call("draft.local_message", {"body": "x" * 5000}), "oversized_arguments"),
        (
            _completion(
                {
                    "tool_calls": [
                        {
                            "function": {
                                "name": "draft.local_message",
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
    ("capability_id", "exposed_ids", "reason"),
    [
        ("integration.send_message", ["draft.local_message"], "unknown_capability_id"),
        ("draft.local_message", [], "capability_not_exposed"),
    ],
)
def test_unknown_and_hidden_capability_ids_are_rejected(
    capability_id,
    exposed_ids,
    reason,
):
    request = parse_provider_capability_request(_call(capability_id, {"body": "hello"}))

    with pytest.raises(CapabilityValidationError) as exc:
        validate_and_digest_capability_request(
            request=request,
            exposed_capability_ids=exposed_ids,
        )

    assert exc.value.reason_code == reason


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
    request = parse_provider_capability_request(_call("draft.local_message", arguments))

    with pytest.raises(CapabilityValidationError) as exc:
        validate_and_digest_capability_request(
            request=request,
            exposed_capability_ids=["draft.local_message"],
        )

    assert exc.value.reason_code == "schema_invalid_arguments"


def test_world_state_arguments_normalize_and_digest_stably_without_raw_trace():
    request = parse_provider_capability_request(
        _call(
            "runtime.world_state.read",
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
            "draft.local_message",
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
