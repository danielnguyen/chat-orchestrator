from __future__ import annotations

import copy
import hashlib

import pytest
from clients.memory_store import MemoryStoreClient
from services.claim_explanation import (
    is_claim_explanation_intent,
    resolve_claim_explanation,
)

ANCHOR = "The retained file reports that the setting is active."


def _digest(value: str = ANCHOR) -> str:
    normalized = " ".join(value.split())
    return f"sha256:{hashlib.sha256(normalized.encode()).hexdigest()}"


def _record(**overrides):
    record = {
        "claim_id": "claim-1",
        "schema_version": "claim-record.v1",
        "owner_id": "owner",
        "conversation_id": "conversation-1",
        "request_id": "request-1",
        "assistant_message_id": "assistant-1",
        "surface": "vscode",
        "runtime_session_id": "runtime-session-1",
        "runtime_turn_id": "runtime-turn-1",
        "claim_anchor": ANCHOR,
        "claim_anchor_digest": _digest(),
        "claim_class": "source_backed_fact",
        "calibration_status": "limited",
        "evidence_strength": "weak",
        "confidence": "low",
        "strongest_authority": "user_report",
        "freshness_summary": "current",
        "uncertainty_disclosure_required": True,
        "validated_evidence_references": [
            {
                "ref_type": "derived_text",
                "ref_id": "PRIVATE-OPAQUE-REFERENCE",
                "owner_id": "owner",
                "conversation_id": "conversation-1",
                "support_kind": "direct",
                "authority": "user_report",
                "freshness_state": "active",
            }
        ],
        "limitation_codes": ["low_authority_evidence", "single_source"],
        "user_safe_summary": "PRIVATE-STORED-SUMMARY",
        "created_at": "2026-07-15T00:00:00+00:00",
    }
    record.update(overrides)
    return record


def _messages(*, prior=ANCHOR, follow_up="How are you sure?"):
    return [
        {"role": "assistant", "content": prior},
        {"role": "user", "content": follow_up},
    ]


class _MemoryStore:
    def __init__(self, records=None, error: Exception | None = None):
        self.records = [_record()] if records is None else records
        self.error = error
        self.calls = []

    async def list_claim_records(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return {"records": copy.deepcopy(self.records)}


async def _resolve(memory_store=None, **overrides):
    values = {
        "enabled": True,
        "messages": _messages(),
        "memory_store": memory_store or _MemoryStore(),
        "owner_id": "owner",
        "conversation_id": "conversation-1",
    }
    values.update(overrides)
    return await resolve_claim_explanation(**values)


@pytest.mark.asyncio
async def test_memory_store_claim_record_list_is_scoped_and_bounded(monkeypatch):
    client = MemoryStoreClient("http://memory", "key")
    calls = []

    async def fake_get(path, *, params=None):
        calls.append((path, params))
        return {"records": []}

    monkeypatch.setattr(client, "_get", fake_get)
    assert await client.list_claim_records(
        owner_id="owner",
        conversation_id="conversation-1",
    ) == {"records": []}
    assert calls == [
        (
            "/v1/internal/claim-records",
            {"owner_id": "owner", "conversation_id": "conversation-1", "limit": 20},
        )
    ]
    with pytest.raises(ValueError, match="claim_record_limit_out_of_range"):
        await client.list_claim_records(
            owner_id="owner",
            conversation_id="conversation-1",
            limit=21,
        )


@pytest.mark.parametrize(
    "phrase",
    [
        "how are you sure",
        "what supports that",
        "what supported that",
        "what evidence supports that",
        "what was that based on",
        "  HOW   ARE YOU SURE?  ",
        "What supports that.",
    ],
)
def test_exact_supported_intents_are_normalized(phrase):
    assert is_claim_explanation_intent(phrase) is True


@pytest.mark.parametrize(
    "phrase",
    [
        "How are you sure this is safe?",
        "What supports the claim about Toronto?",
        'What supports "that"?',
        "How are you sure? Also check again.",
        "Please explain how are you sure",
        "how are you sure?!",
    ],
)
def test_additional_or_targeted_text_does_not_match(phrase):
    assert is_claim_explanation_intent(phrase) is False


@pytest.mark.asyncio
async def test_disabled_configuration_does_not_intercept_or_query_storage():
    memory_store = _MemoryStore()
    outcome = await _resolve(memory_store, enabled=False)
    assert outcome.handled is False
    assert memory_store.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "messages",
    [
        [{"role": "user", "content": "How are you sure?"}],
        [
            {"role": "assistant", "content": ANCHOR},
            {"role": "system", "content": "bounded"},
            {"role": "user", "content": "How are you sure?"},
        ],
        [
            {"role": "assistant", "content": "x" * 501},
            {"role": "user", "content": "How are you sure?"},
        ],
    ],
)
async def test_immediate_prior_assistant_is_required_without_backward_scan(messages):
    memory_store = _MemoryStore()
    outcome = await _resolve(memory_store, messages=messages)
    assert outcome.answer == (
        "I can’t safely identify which earlier statement you mean from the supplied "
        "conversation context. I did not perform a new verification."
    )
    assert outcome.trace["reason_code"] == "prior_assistant_unavailable"
    assert memory_store.calls == []


@pytest.mark.asyncio
async def test_latest_exact_record_renders_only_bounded_stored_semantics():
    memory_store = _MemoryStore()
    outcome = await _resolve(memory_store)
    assert memory_store.calls == [
        {"owner_id": "owner", "conversation_id": "conversation-1", "limit": 20}
    ]
    assert outcome.status == "ok"
    assert outcome.answer == (
        "I based that earlier statement on one retained file excerpt from the original "
        "retained record. The record classified it as a source-backed fact, with low "
        "confidence and weak support. The evidence was marked current. Only one "
        "supporting record was retained. The source was treated as user-provided "
        "material rather than independently authoritative. I did not perform a new "
        "verification for this explanation."
    )
    assert outcome.trace["reason_code"] == "latest_claim_record_resolved"
    serialized = repr((outcome.answer, outcome.trace))
    assert "PRIVATE-OPAQUE-REFERENCE" not in serialized
    assert "PRIVATE-STORED-SUMMARY" not in serialized


@pytest.mark.asyncio
async def test_artifact_wording_freshness_and_limitations_are_deterministic():
    record = _record(
        claim_class="verified_fact",
        evidence_strength="moderate",
        confidence="medium",
        freshness_summary="unknown",
        validated_evidence_references=[
            {
                **_record()["validated_evidence_references"][0],
                "ref_type": "artifact",
            }
        ],
        limitation_codes=["unknown_freshness", "single_source"],
    )
    outcome = await _resolve(_MemoryStore([record]))
    assert "one retained file record" in outcome.answer
    assert "a verified fact, with medium confidence and moderate support" in outcome.answer
    assert "The evidence freshness was unknown." in outcome.answer
    assert outcome.answer.index("Only one supporting") < outcome.answer.index(
        "freshness could not be established"
    )
    assert outcome.answer.endswith(
        "I did not perform a new verification for this explanation."
    )


@pytest.mark.asyncio
async def test_no_records_and_unavailable_storage_use_honest_provider_free_fallbacks():
    no_record = await _resolve(_MemoryStore([]))
    unavailable = await _resolve(
        _MemoryStore(error=RuntimeError("PRIVATE-DEPENDENCY-EXCEPTION"))
    )
    assert no_record.trace["reason_code"] == "no_claim_records"
    assert "don’t have a retained evidence record" in no_record.answer
    assert unavailable.trace["reason_code"] == "claim_records_unavailable"
    assert "couldn’t access the retained evidence record" in unavailable.answer
    assert "PRIVATE-DEPENDENCY-EXCEPTION" not in repr(unavailable)


@pytest.mark.asyncio
async def test_newest_group_is_ambiguous_and_older_groups_are_never_selected():
    ambiguous = await _resolve(
        _MemoryStore([_record(claim_id="claim-1"), _record(claim_id="claim-2")])
    )
    assert ambiguous.trace["reason_code"] == "ambiguous_latest_response"

    newest_other = _record(
        claim_id="claim-new",
        assistant_message_id="assistant-new",
        claim_anchor="A different answer.",
        claim_anchor_digest=_digest("A different answer."),
    )
    older_match = _record(claim_id="claim-old", assistant_message_id="assistant-old")
    mismatch = await _resolve(_MemoryStore([newest_other, older_match]))
    assert mismatch.trace["reason_code"] == "no_record_for_latest_response"
    assert mismatch.trace["matched_record_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("record", "reason"),
    [
        (_record(owner_id="other"), "record_invalid"),
        (_record(conversation_id="other"), "record_invalid"),
        (_record(claim_anchor_digest="sha256:" + "0" * 64), "record_invalid"),
        (_record(metadata={"raw": "PRIVATE"}), "claim_record_response_invalid"),
        (
            _record(
                validated_evidence_references=[
                    {**_record()["validated_evidence_references"][0], "ref_type": "message"}
                ]
            ),
            "record_unsupported",
        ),
        (_record(evidence_strength="none"), "record_insufficient"),
        (_record(limitation_codes=["no_supporting_evidence"]), "record_insufficient"),
    ],
)
async def test_invalid_unsupported_and_insufficient_records_fail_closed(record, reason):
    outcome = await _resolve(_MemoryStore([record]))
    assert outcome.status == "degraded"
    assert outcome.trace["reason_code"] == reason
    assert "incomplete or unsupported" in outcome.answer
    assert "PRIVATE" not in repr(outcome)
