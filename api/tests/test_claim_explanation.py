from __future__ import annotations

import copy
import hashlib

import pytest
from clients.memory_store import MemoryStoreClient
from services.claim_explanation import (
    ClaimExplanationIntent,
    is_claim_explanation_intent,
    parse_claim_explanation_intent,
    resolve_claim_explanation,
)

ANCHOR = "The retained file reports that the setting is active."
OLDER_ANCHOR = "According to this document, the service is healthy."


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


def _quoted_messages(*, anchor=OLDER_ANCHOR, prior="A newer answer."):
    return _messages(
        prior=prior,
        follow_up=f'What supports the statement "{anchor}"?',
    )


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
    assert parse_claim_explanation_intent(phrase) == ClaimExplanationIntent(
        mode="latest"
    )


@pytest.mark.parametrize(
    ("phrase", "target"),
    [
        (
            'What supports the statement "The retained file reports that the setting is active."?',
            ANCHOR,
        ),
        (
            'What supported the statement "According to this document, the service is healthy.".',
            OLDER_ANCHOR,
        ),
        (
            "How are you sure about the statement "
            '"The maintenance record lists the service as healthy."',
            "The maintenance record lists the service as healthy.",
        ),
        (
            "  WHAT   SUPPORTS  THE STATEMENT "
            '"According to   this document, the service is healthy." ? ',
            OLDER_ANCHOR,
        ),
    ],
)
def test_exact_quoted_intents_are_bounded_and_normalized(phrase, target):
    assert parse_claim_explanation_intent(phrase) == ClaimExplanationIntent(
        mode="quoted_anchor",
        target_anchor=target,
    )
    assert is_claim_explanation_intent(phrase) is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "follow_up",
    [
        'What supports the statement "   "?',
        f'What supports the statement "{"x" * 501}"?',
    ],
)
async def test_recognizable_invalid_quoted_target_is_handled_without_storage(follow_up):
    intent = parse_claim_explanation_intent(follow_up)
    assert intent == ClaimExplanationIntent(mode="quoted_anchor", target_anchor=None)
    memory_store = _MemoryStore()
    outcome = await _resolve(
        memory_store,
        messages=_messages(follow_up=follow_up),
    )
    assert outcome.handled is True
    assert outcome.trace["target_mode"] == "quoted_anchor"
    assert outcome.trace["reason_code"] == "quoted_target_invalid"
    assert "did not perform a new verification" in outcome.answer
    assert memory_store.calls == []


@pytest.mark.parametrize(
    "phrase",
    [
        "How are you sure this is safe?",
        "What supports the claim about Toronto?",
        'What supports "that"?',
        "How are you sure? Also check again.",
        "Please explain how are you sure",
        "how are you sure?!",
        "What supports the statement 'A prior answer.'?",
        "What supports the statement “A prior answer.”?",
        'What supports the statement "A prior" and "another"?',
        'What supports the statement "A prior answer.?',
        'What supports the statement "A prior answer."? Check again.',
    ],
)
def test_additional_or_targeted_text_does_not_match(phrase):
    assert is_claim_explanation_intent(phrase) is False
    assert parse_claim_explanation_intent(phrase) is None


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
async def test_quoted_anchor_resolves_one_exact_older_record():
    newer = _record(
        claim_id="claim-new",
        assistant_message_id="assistant-new",
        claim_anchor="A newer answer.",
        claim_anchor_digest=_digest("A newer answer."),
    )
    older = _record(
        claim_id="claim-old",
        assistant_message_id="assistant-old",
        claim_anchor=OLDER_ANCHOR,
        claim_anchor_digest=_digest(OLDER_ANCHOR),
    )
    memory_store = _MemoryStore([newer, older])

    outcome = await _resolve(
        memory_store,
        messages=_quoted_messages(prior=newer["claim_anchor"]),
    )

    assert outcome.status == "ok"
    assert outcome.trace == {
        "enabled": True,
        "intent_status": "matched",
        "target_mode": "quoted_anchor",
        "target_status": "resolved",
        "lookup_status": "completed",
        "resolution_status": "resolved",
        "render_status": "completed",
        "reason_code": "quoted_claim_record_resolved",
        "storage_call_count": 1,
        "provider_call_count": 0,
        "record_count": 2,
        "newest_group_count": 0,
        "matched_record_count": 1,
        "claim_id": "claim-old",
        "claim_anchor_digest": _digest(OLDER_ANCHOR),
    }
    assert memory_store.calls == [
        {"owner_id": "owner", "conversation_id": "conversation-1", "limit": 20}
    ]
    assert OLDER_ANCHOR not in outcome.answer
    assert "PRIVATE-OPAQUE-REFERENCE" not in repr(outcome)
    assert "PRIVATE-STORED-SUMMARY" not in repr(outcome)
    assert outcome.answer.endswith(
        "I did not perform a new verification for this explanation."
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "quoted_target",
    [
        "According to this document, the service is Healthy.",
        "According to this document, the service is healthy",
        "the service is healthy.",
    ],
)
async def test_quoted_anchor_requires_case_punctuation_and_full_anchor(quoted_target):
    record = _record(
        claim_anchor=OLDER_ANCHOR,
        claim_anchor_digest=_digest(OLDER_ANCHOR),
    )
    outcome = await _resolve(
        _MemoryStore([record]),
        messages=_quoted_messages(anchor=quoted_target),
    )
    assert outcome.status == "degraded"
    assert outcome.trace["reason_code"] == "quoted_claim_record_not_found"
    assert outcome.trace["matched_record_count"] == 0
    assert "did not perform a new verification" in outcome.answer


@pytest.mark.asyncio
@pytest.mark.parametrize("same_message", [False, True])
async def test_duplicate_quoted_anchors_are_ambiguous(same_message):
    records = [
        _record(
            claim_id=f"claim-{index}",
            assistant_message_id=("assistant-shared" if same_message else f"assistant-{index}"),
            claim_anchor=OLDER_ANCHOR,
            claim_anchor_digest=_digest(OLDER_ANCHOR),
        )
        for index in range(2)
    ]
    outcome = await _resolve(
        _MemoryStore(records),
        messages=_quoted_messages(),
    )
    assert outcome.status == "degraded"
    assert outcome.trace["reason_code"] == "ambiguous_quoted_claim"
    assert outcome.trace["matched_record_count"] == 2
    assert "more than one retained claim matching" in outcome.answer


@pytest.mark.asyncio
async def test_quoted_matching_validates_full_response_and_supported_record_shape():
    matching = _record(
        claim_id="claim-match",
        assistant_message_id="assistant-old",
        claim_anchor=OLDER_ANCHOR,
        claim_anchor_digest=_digest(OLDER_ANCHOR),
    )
    unrelated_invalid = _record(
        claim_id="claim-unrelated",
        assistant_message_id="assistant-new",
        claim_anchor="PRIVATE-UNRELATED-ANCHOR",
        claim_anchor_digest=_digest("PRIVATE-UNRELATED-ANCHOR"),
        owner_id="other",
    )
    invalid = await _resolve(
        _MemoryStore([unrelated_invalid, matching]),
        messages=_quoted_messages(),
    )
    assert invalid.trace["reason_code"] == "record_invalid"
    assert "PRIVATE-UNRELATED-ANCHOR" not in repr(invalid)

    unsupported = copy.deepcopy(matching)
    unsupported["validated_evidence_references"][0]["ref_type"] = "message"
    outcome = await _resolve(
        _MemoryStore([unsupported]),
        messages=_quoted_messages(),
    )
    assert outcome.trace["reason_code"] == "record_unsupported"
    assert outcome.status == "degraded"

    insufficient = copy.deepcopy(matching)
    insufficient["evidence_strength"] = "none"
    outcome = await _resolve(
        _MemoryStore([insufficient]),
        messages=_quoted_messages(),
    )
    assert outcome.trace["reason_code"] == "record_insufficient"
    assert outcome.status == "degraded"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"owner_id": "other"}, "record_invalid"),
        ({"conversation_id": "other"}, "record_invalid"),
        ({"claim_anchor_digest": "sha256:" + "0" * 64}, "record_invalid"),
        ({"metadata": {"raw": "PRIVATE-EXTRA"}}, "claim_record_response_invalid"),
    ],
)
async def test_quoted_matching_rejects_invalid_scoped_or_extra_records(changes, reason):
    record = _record(
        **{
            "claim_anchor": OLDER_ANCHOR,
            "claim_anchor_digest": _digest(OLDER_ANCHOR),
            **changes,
        }
    )
    outcome = await _resolve(
        _MemoryStore([record]),
        messages=_quoted_messages(),
    )
    assert outcome.status == "degraded"
    assert outcome.trace["reason_code"] == reason
    assert "PRIVATE-EXTRA" not in repr(outcome)


@pytest.mark.asyncio
async def test_quoted_target_dependency_failure_and_private_no_match_are_bounded():
    unavailable = await _resolve(
        _MemoryStore(error=RuntimeError("PRIVATE-DEPENDENCY-EXCEPTION")),
        messages=_quoted_messages(),
    )
    assert unavailable.trace["reason_code"] == "claim_records_unavailable"
    assert unavailable.trace["target_mode"] == "quoted_anchor"
    assert "PRIVATE-DEPENDENCY-EXCEPTION" not in repr(unavailable)

    private_target = "PRIVATE-UNMATCHED-TARGET."
    private_record_anchor = "PRIVATE-UNRELATED-RECORD."
    private_record = _record(
        claim_anchor=private_record_anchor,
        claim_anchor_digest=_digest(private_record_anchor),
    )
    no_match = await _resolve(
        _MemoryStore([private_record]),
        messages=_quoted_messages(anchor=private_target),
    )
    serialized = repr((no_match.answer, no_match.trace))
    assert no_match.trace["reason_code"] == "quoted_claim_record_not_found"
    assert private_target not in serialized
    assert private_record_anchor not in serialized


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
