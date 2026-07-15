from __future__ import annotations

import hashlib

import pytest
from clients.memory_store import MemoryStoreClient
from clients.runtime import RuntimeClient
from services.claim_capture import (
    bind_assistant_message,
    calibrate_claim_capture,
    claim_record_payload,
    finish_claim_record_persistence,
    prepare_claim_capture,
)
from settings import Settings


def _source(*, freshness_state: str = "active", ref_id: str = "derived-text-1"):
    return {
        "owner_id": "owner",
        "artifact_id": "artifact-1",
        "file_path": "notes/current.txt",
        "snippet": "PRIVATE-SOURCE-CONTENT",
        "source_ref": {"ref_type": "derived_text", "ref_id": ref_id},
        "freshness_state": freshness_state,
    }


def _settings(**overrides):
    values = {
        "ORCH_API_KEY": "key",
        "MEMORY_STORE_BASE_URL": "http://memory",
        "MEMORY_STORE_API_KEY": "key",
        "LITELLM_BASE_URL": "http://models",
    }
    values.update(overrides)
    return Settings(**values)


def test_claim_capture_is_disabled_by_default_and_needs_runtime_configuration():
    assert _settings().claim_record_capture_enabled is False
    with pytest.raises(ValueError, match="requires Cognitive Runtime"):
        _settings(CLAIM_RECORD_CAPTURE_ENABLED=True)
    configured = _settings(
        CLAIM_RECORD_CAPTURE_ENABLED=True,
        COGNITIVE_RUNTIME_BASE_URL="http://runtime",
    )
    assert configured.claim_record_capture_enabled is True


def _public_source(*, ref_id: str = "derived-text-1"):
    return {
        "artifact_id": "artifact-1",
        "file_path": "notes/current.txt",
        "snippet": "PRIVATE-SOURCE-CONTENT",
        "source_ref": {"ref_type": "derived_text", "ref_id": ref_id},
    }


def _prepare(**overrides):
    values = {
        "enabled": True,
        "runtime_available": True,
        "runtime_session_id": "runtime-session-1",
        "runtime_turn_id": "runtime-turn-1",
        "answer": "The retained setting is active.",
        "is_brief": False,
        "pending_action_present": False,
        "capability_requested": False,
        "capability_executed": False,
        "callback_applied": False,
        "privacy_suppressed": False,
        "retained_artifacts": [_source()],
        "public_sources": [_public_source()],
        "trace_references": [
            {"ref_type": "derived_text", "ref_id": "derived-text-1"}
        ],
        "owner_id": "owner",
        "conversation_id": "conversation-1",
    }
    values.update(overrides)
    return prepare_claim_capture(**values)


def _digest(anchor: str) -> str:
    return f"sha256:{hashlib.sha256(anchor.encode()).hexdigest()}"


def _calibration_response(candidate, **overrides):
    result = {
        "claim_id": "claim-1",
        "claim_anchor": candidate.claim_anchor,
        "claim_anchor_digest": _digest(candidate.claim_anchor),
        "claim_class": "source_backed_fact",
        "calibration_status": "limited",
        "evidence_strength": "weak",
        "confidence": "low",
        "strongest_authority": "user_report",
        "freshness_summary": "current",
        "uncertainty_disclosure_required": True,
        "validated_evidence_references": [candidate.evidence_reference],
        "limitation_codes": ["low_authority_evidence", "single_source"],
        "user_safe_summary": "This claim has limited recorded support.",
    }
    result.update(overrides.pop("result", {}))
    response = {
        "request_id": "request-1",
        "owner_id": "owner",
        "conversation_id": "conversation-1",
        "surface": "vscode",
        "runtime_session_id": "runtime-session-1",
        "runtime_turn_id": "runtime-turn-1",
        "result": result,
    }
    response.update(overrides)
    return response


class _Runtime:
    def __init__(self, response=None, error: Exception | None = None):
        self.response = response
        self.error = error
        self.calls = []

    async def evaluate_claim_calibration(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.response


async def _calibrate(state, runtime):
    return await calibrate_claim_capture(
        runtime=runtime,
        state=state,
        request_id="request-1",
        owner_id="owner",
        conversation_id="conversation-1",
        surface="vscode",
        runtime_session_id="runtime-session-1",
        runtime_turn_id="runtime-turn-1",
    )


def test_single_sentence_and_one_retained_file_source_are_eligible():
    state = _prepare(answer="  The retained setting is active.  ")

    assert state.trace == {
        "enabled": True,
        "eligibility_status": "eligible",
        "calibration_status": "not_attempted",
        "persistence_status": "not_attempted",
        "reason_code": "single_claim_single_file_source",
        "runtime_call_count": 0,
        "storage_call_count": 0,
        "evidence_count": 1,
        "claim_id": None,
        "claim_anchor_digest": None,
    }
    assert state.candidate.claim_anchor == "The retained setting is active."
    assert state.candidate.evidence_reference == {
        "ref_type": "derived_text",
        "ref_id": "derived-text-1",
        "owner_id": "owner",
        "conversation_id": "conversation-1",
        "support_kind": "direct",
        "authority": "user_report",
        "freshness_state": "active",
    }


@pytest.mark.parametrize(
    ("freshness", "expected"),
    [
        ("active", "active"),
        ("stale", "stale"),
        ("superseded", "superseded"),
        ("corrected", "corrected"),
        ("unknown_freshness", "unknown_freshness"),
        ("not_applicable", "not_applicable"),
        ("unsupported", "unknown_freshness"),
        (None, "unknown_freshness"),
    ],
)
def test_file_source_freshness_is_mapped_conservatively(freshness, expected):
    state = _prepare(retained_artifacts=[_source(freshness_state=freshness)])
    assert state.candidate.evidence_reference["freshness_state"] == expected


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"enabled": False}, "disabled"),
        ({"runtime_available": False}, "runtime_unavailable"),
        ({"runtime_session_id": None}, "runtime_scope_unavailable"),
        ({"privacy_suppressed": True}, "privacy_suppressed"),
        ({"is_brief": True}, "brief_response"),
        ({"pending_action_present": True}, "action_response"),
        ({"capability_requested": True}, "action_response"),
        ({"capability_executed": True}, "action_response"),
        ({"callback_applied": True}, "callback_response"),
        ({"retained_artifacts": []}, "no_retained_file_source"),
        ({"public_sources": []}, "no_retained_file_source"),
        ({"retained_artifacts": [_source(), _source(ref_id="derived-text-2")]},
         "multiple_retained_file_sources"),
    ],
)
def test_unsupported_response_paths_skip_capture(overrides, reason):
    state = _prepare(**overrides)
    assert state.candidate is None
    assert state.trace["eligibility_status"] == "ineligible"
    assert state.trace["reason_code"] == reason


@pytest.mark.parametrize(
    "answer",
    [
        "First sentence. Second sentence.",
        "# Heading",
        "- list item",
        "```text\nvalue\n```",
        "First paragraph.\n\nSecond paragraph.",
        "Is this active?",
    ],
)
def test_ambiguous_or_structured_answers_skip_capture(answer):
    state = _prepare(answer=answer)
    assert state.candidate is None
    assert state.trace["eligibility_status"] == "ineligible"


def test_source_must_match_public_source_and_normal_trace_reference():
    assert _prepare(public_sources=[_public_source(ref_id="other")]).candidate is None
    assert _prepare(trace_references=[]).candidate is None
    assert _prepare(retained_artifacts=[_source(ref_id="opaque")]).candidate is None


def test_unretained_bundle_identity_is_not_selected_over_the_retained_source():
    state = _prepare(
        trace_references=[
            {"ref_type": "derived_text", "ref_id": "unretained-derived-text"},
            {"ref_type": "derived_text", "ref_id": "derived-text-1"},
        ]
    )
    assert state.candidate.evidence_reference["ref_id"] == "derived-text-1"


@pytest.mark.asyncio
async def test_calibration_forwards_only_bounded_scope_anchor_and_evidence():
    state = _prepare()
    runtime = _Runtime(_calibration_response(state.candidate))

    calibrated = await _calibrate(state, runtime)

    assert len(runtime.calls) == 1
    assert runtime.calls[0] == {
        "request_id": "request-1",
        "owner_id": "owner",
        "conversation_id": "conversation-1",
        "surface": "vscode",
        "runtime_session_id": "runtime-session-1",
        "runtime_turn_id": "runtime-turn-1",
        "claim_anchor": "The retained setting is active.",
        "evidence_references": [state.candidate.evidence_reference],
    }
    assert calibrated.trace["calibration_status"] == "completed"
    assert calibrated.calibration_result == _calibration_response(state.candidate)["result"]
    assert "PRIVATE-SOURCE-CONTENT" not in repr(runtime.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response_change",
    [
        {"owner_id": "other"},
        {"result": {"claim_anchor_digest": "sha256:" + "0" * 64}},
        {"result": {"validated_evidence_references": []}},
        {"result": {"metadata": {"raw": "PRIVATE"}}},
    ],
)
async def test_mismatched_or_malformed_calibration_is_rejected(response_change):
    state = _prepare()
    response = _calibration_response(state.candidate, **response_change)
    calibrated = await _calibrate(state, _Runtime(response))
    assert calibrated.calibration_result is None
    assert calibrated.trace["reason_code"] == "calibration_response_invalid"


@pytest.mark.asyncio
async def test_calibration_failure_is_bounded_and_non_persistent():
    state = await _calibrate(_prepare(), _Runtime(error=RuntimeError("PRIVATE-EXCEPTION")))
    assert state.trace["calibration_status"] == "failed"
    assert state.trace["reason_code"] == "calibration_unavailable"
    assert claim_record_payload(
        state=state,
        request_id="request-1",
        owner_id="owner",
        conversation_id="conversation-1",
        surface="vscode",
        runtime_session_id="runtime-session-1",
        runtime_turn_id="runtime-turn-1",
    ) is None
    assert "PRIVATE-EXCEPTION" not in repr(state.trace)


@pytest.mark.asyncio
async def test_message_binding_and_claim_record_forward_the_exact_runtime_result():
    initial = _prepare()
    state = await _calibrate(initial, _Runtime(_calibration_response(initial.candidate)))
    state = bind_assistant_message(state, {"message_id": "assistant-message-1"})
    payload = claim_record_payload(
        state=state,
        request_id="request-1",
        owner_id="owner",
        conversation_id="conversation-1",
        surface="vscode",
        runtime_session_id="runtime-session-1",
        runtime_turn_id="runtime-turn-1",
    )

    assert state.trace["persistence_status"] == "pending"
    assert payload["schema_version"] == "claim-record.v1"
    assert payload["assistant_message_id"] == "assistant-message-1"
    assert payload["calibration_result"] is state.calibration_result


@pytest.mark.asyncio
async def test_malformed_message_ack_prevents_claim_record_persistence():
    initial = _prepare()
    state = await _calibrate(initial, _Runtime(_calibration_response(initial.candidate)))
    state = bind_assistant_message(state, {"message_id": "bad id"})
    assert state.assistant_message_id is None
    assert state.trace["reason_code"] == "assistant_message_ack_invalid"


def test_claim_record_response_is_validated_without_copying_record_content_to_trace():
    state = ClaimCaptureStateForTest.persistable()
    payload = claim_record_payload(
        state=state,
        request_id="request-1",
        owner_id="owner",
        conversation_id="conversation-1",
        surface="vscode",
        runtime_session_id="runtime-session-1",
        runtime_turn_id="runtime-turn-1",
    )
    response = {
        "created": True,
        "record": {
            **{key: value for key, value in payload.items() if key != "calibration_result"},
            **payload["calibration_result"],
            "created_at": "2026-07-15T00:00:00+00:00",
        },
    }
    result = finish_claim_record_persistence(
        state=state,
        expected_payload=payload,
        response=response,
    )
    assert result.trace["persistence_status"] == "persisted"
    assert result.trace["storage_call_count"] == 1

    response["record"]["raw"] = "PRIVATE-RECORD-CONTENT"
    rejected = finish_claim_record_persistence(
        state=state,
        expected_payload=payload,
        response=response,
    )
    assert rejected.trace["persistence_status"] == "failed"
    assert "PRIVATE-RECORD-CONTENT" not in repr(rejected.trace)


class ClaimCaptureStateForTest:
    @staticmethod
    def persistable():
        initial = _prepare()
        result = _calibration_response(initial.candidate)["result"]
        from services.claim_capture import ClaimCaptureState

        return ClaimCaptureState(
            trace={
                **initial.trace,
                "calibration_status": "completed",
                "persistence_status": "pending",
                "claim_id": "claim-1",
                "claim_anchor_digest": result["claim_anchor_digest"],
            },
            candidate=initial.candidate,
            calibration_result=result,
            assistant_message_id="assistant-message-1",
        )


@pytest.mark.asyncio
async def test_runtime_client_posts_exact_contract_and_rejects_scope_mismatch(monkeypatch):
    client = RuntimeClient("http://runtime", None)
    candidate = _prepare().candidate
    calls = []

    async def post(path, *, json):
        calls.append((path, json))
        return _calibration_response(candidate)

    monkeypatch.setattr(client, "_post", post)
    response = await client.evaluate_claim_calibration(
        request_id="request-1",
        owner_id="owner",
        conversation_id="conversation-1",
        surface="vscode",
        runtime_session_id="runtime-session-1",
        runtime_turn_id="runtime-turn-1",
        claim_anchor=candidate.claim_anchor,
        evidence_references=[candidate.evidence_reference],
    )
    assert calls[0][0] == "/v1/runtime/claim-calibration/evaluate"
    assert response["result"]["claim_id"] == "claim-1"

    async def mismatched(path, *, json):
        return _calibration_response(candidate, owner_id="other")

    monkeypatch.setattr(client, "_post", mismatched)
    with pytest.raises(RuntimeError, match="claim_calibration_response_invalid"):
        await client.evaluate_claim_calibration(**calls[0][1])


@pytest.mark.asyncio
async def test_memory_client_sends_request_id_without_retry(monkeypatch):
    client = MemoryStoreClient("http://memory", "key")
    calls = []

    async def post(path, *, request_id=None, json):
        calls.append((path, request_id, json))
        return {"created": True, "record": {"claim_id": "claim-1"}}

    monkeypatch.setattr(client, "_post", post)
    payload = {"schema_version": "claim-record.v1"}
    await client.create_claim_record(request_id="request-1", payload=payload)
    assert calls == [("/v1/internal/claim-records", "request-1", payload)]
