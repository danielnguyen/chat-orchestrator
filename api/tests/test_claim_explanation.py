from __future__ import annotations

import copy
import hashlib

import pytest
from clients.memory_store import MemoryStoreClient
from services.claim_explanation import (
    ClaimExplanationIntent,
    ClaimExplanationOutcome,
    is_claim_explanation_intent,
    parse_claim_explanation_intent,
    resolve_claim_explanation,
)

ANCHOR = "The retained file reports that the setting is active."
OLDER_ANCHOR = "According to this document, the service is healthy."
MANIFEST_ID = "evidence_manifest_0123456789abcdef0123456789abcdef"


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


def _manifest(
    *,
    strategy="targeted_retrieval",
    status="sufficient_for_declared_scope",
    assistant_message_id="assistant-1",
    response_digest=None,
):
    exact = strategy == "exact_fetch"
    considered = ["source-a", "source-b"]
    returned = ["source-a:record-1", "source-b:record-2"]
    attempts = (
        [
            {
                "source_id": "source-a",
                "source_ref": returned[0],
                "outcome": "satisfied",
            },
            {
                "source_id": "source-b",
                "source_ref": returned[1],
                "outcome": "satisfied",
            },
        ]
        if exact
        else []
    )
    return {
        "enabled": True,
        "attempted": True,
        "status": status,
        "manifest_id": MANIFEST_ID,
        "assistant_message_id": assistant_message_id,
        "response_digest": response_digest or _digest(),
        "shape": {
            "derivation_status": "derived",
            "task_shape": "targeted_lookup",
            "candidate_count": 0,
            "clarification_required": False,
            "reason_codes": ["targeted_lookup_derived"],
        },
        "inventory": {
            "inventory_status": "complete_for_declared_scope",
            "inventory_source_count": 2,
            "declared_source_count": 2,
            "declared_category_count": 0,
            "available_source_count": 2,
            "unavailable_source_count": 0,
            "disabled_source_count": 0,
            "unknown_source_count": 0,
        },
        "plan": {
            "plan_id": "evidence_plan_1",
            "plan_status": "ready",
            "completeness_expectation": "targeted_scope",
            "contradiction_search_required": False,
            "selected_strategies": [strategy],
            "material_requirement_count": 2,
            "optional_requirement_count": 0,
            "limitation_codes": [],
        },
        "acquisition": {
            "strategy_attempted": strategy,
            "sources_considered": considered,
            "sources_selected": considered,
            "sources_used": considered,
            "source_references_returned": returned,
            "source_references_retained": returned,
            "source_references_filtered_or_omitted": [],
            "source_references_attempted": returned if exact else [],
            "source_references_unsuccessful": [],
            "exact_reference_attempts": attempts,
            "exact_reference_attempt_count": 2 if exact else 0,
            "exact_reference_successful_count": 2 if exact else 0,
            "exact_reference_unknown_count": 0,
            "exact_reference_failed_count": 0,
            "exact_reference_filtered_count": 0,
            "exact_reference_truncated_count": 0,
            "unavailable_source_ids": [],
            "failed_source_ids": [],
            "expansion_attempts": [],
            "item_count": 2,
            "usable_item_count": 2,
            "prompt_retained_item_count": 2,
            "dsa_outcome": "ok",
            "dsa_error_codes": [],
            "dsa_budget_truncation": False,
            "candidate_truncation": False,
            "context_delivery_status": "retained",
            "requirement_facts": [],
        },
        "sufficiency": {
            "evaluation_id": "evidence_eval_1",
            "status": status,
            "reason_codes": ["all_declared_requirements_satisfied"],
            "answer_constraints": [],
            "qualification_required": status == "sufficient_with_limitations",
            "additional_acquisition_required": False,
        },
        "private_prompt": "PRIVATE-PROMPT-CONTENT",
    }


def _trace(*, manifest=None, **overrides):
    trace = {
        "trace_id": "trace-1",
        "request_id": "request-1",
        "conversation_id": "conversation-1",
        "owner_id": "owner",
        "client_id": "vscode",
        "surface": "vscode",
        "profile": {},
        "retrieval": {"private": "PRIVATE-SOURCE-CONTENT"},
        "prompt": {
            "evidence_acquisition": manifest or _manifest(),
            "private_prompt": "PRIVATE-RAW-PROMPT",
        },
        "router_decision": {},
        "manual_override": {},
        "model_call": {"private": "PRIVATE-MODEL-CALL"},
        "model_calls": [],
        "fallback": {},
        "artifacts": {},
        "references": [],
        "cost": {},
        "latency_ms": 1,
        "status": "ok",
        "error": None,
        "created_at": "2026-07-17T00:00:00Z",
    }
    trace.update(overrides)
    return trace


def _suppressed_trace():
    trace = _trace()
    acquisition = trace["prompt"]["evidence_acquisition"]["acquisition"]
    for field in (
        "sources_considered",
        "sources_selected",
        "sources_used",
        "source_references_returned",
        "source_references_retained",
        "source_references_filtered_or_omitted",
        "source_references_attempted",
        "source_references_unsuccessful",
        "exact_reference_attempts",
        "unavailable_source_ids",
        "failed_source_ids",
    ):
        acquisition[f"{field}_count"] = len(acquisition[field])
        acquisition[field] = []
    acquisition["source_identifiers_suppressed"] = True
    return trace


class _MemoryStore:
    def __init__(
        self,
        records=None,
        error: Exception | None = None,
        trace=None,
        trace_error: Exception | None = None,
    ):
        self.records = [_record()] if records is None else records
        self.error = error
        self.calls = []
        self.trace = _trace() if trace is None else trace
        self.trace_error = trace_error
        self.trace_calls = []

    async def list_claim_records(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return {"records": copy.deepcopy(self.records)}

    async def get_trace(self, request_id):
        self.trace_calls.append(request_id)
        if self.trace_error is not None:
            raise self.trace_error
        return copy.deepcopy(self.trace)


async def _resolve(memory_store=None, **overrides):
    values = {
        "enabled": True,
        "messages": _messages(),
        "memory_store": memory_store or _MemoryStore(),
        "owner_id": "owner",
        "conversation_id": "conversation-1",
        "surface": "vscode",
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


@pytest.mark.parametrize(
    ("phrase", "question"),
    [
        ("what did you check", "checked"),
        ("what did you examine", "checked"),
        ("did you look at everything relevant", "coverage"),
        ("what might you have missed", "gaps"),
        ("what did you not check", "gaps"),
        ("  WHAT   DID YOU CHECK?  ", "checked"),
        ("What might you have missed.", "gaps"),
    ],
)
def test_exact_acquisition_intents_are_normalized(phrase, question):
    assert parse_claim_explanation_intent(phrase) == ClaimExplanationIntent(
        mode="latest",
        explanation_kind="acquisition",
        acquisition_question=question,
    )


@pytest.mark.parametrize(
    ("phrase", "question"),
    [
        (
            f'What did you check for the statement "{OLDER_ANCHOR}"?',
            "checked",
        ),
        (
            f'What did you examine for the statement "{OLDER_ANCHOR}".',
            "checked",
        ),
        (
            f'Did you look at everything relevant for the statement "{OLDER_ANCHOR}"?',
            "coverage",
        ),
        (
            f'What might you have missed for the statement "{OLDER_ANCHOR}"?',
            "gaps",
        ),
    ],
)
def test_exact_quoted_acquisition_intents_are_bounded(phrase, question):
    assert parse_claim_explanation_intent(phrase) == ClaimExplanationIntent(
        mode="quoted_anchor",
        target_anchor=OLDER_ANCHOR,
        explanation_kind="acquisition",
        acquisition_question=question,
    )


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "follow_up",
    [
        'What did you check for the statement "   "?',
        f'What might you have missed for the statement "{"x" * 501}"?',
    ],
)
async def test_invalid_quoted_acquisition_target_is_bounded_without_storage(
    follow_up,
):
    memory_store = _MemoryStore()
    outcome = await _resolve(
        memory_store,
        messages=_messages(follow_up=follow_up),
    )
    assert outcome.handled is True
    assert outcome.status == "degraded"
    assert outcome.trace["explanation_kind"] == "acquisition"
    assert outcome.trace["reason_code"] == "quoted_target_invalid"
    assert "did not perform a new verification" in outcome.answer
    assert memory_store.calls == []
    assert memory_store.trace_calls == []


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


@pytest.mark.parametrize(
    "phrase",
    [
        "What did you check? Check again.",
        "Can you tell me what you checked and then search more?",
        "Did you look at everything relevant about Toronto?",
        "Please explain what you might have missed.",
        f"What did you check for the statement '{OLDER_ANCHOR}'?",
        f"What did you check for the statement “{OLDER_ANCHOR}”?",
        f'What did you check for the statement "{OLDER_ANCHOR}" and "another"?',
        f'What did you check for the statement "{OLDER_ANCHOR}?',
        f'What did you check for the statement "{OLDER_ANCHOR}"? Check again.',
    ],
)
def test_acquisition_intent_rejects_additional_or_malformed_text(phrase):
    assert parse_claim_explanation_intent(phrase) is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("role", "content"),
    [
        ("assistant", "How are you sure?"),
        ("assistant", f'What supports the statement "{ANCHOR}"?'),
        ("assistant", "What did you check?"),
        ("system", "What evidence supports that?"),
        ("system", "Did you look at everything relevant?"),
    ],
)
async def test_supported_text_from_non_user_final_message_is_not_intercepted(
    role,
    content,
):
    memory_store = _MemoryStore()
    outcome = await _resolve(
        memory_store,
        messages=[{"role": role, "content": content}],
    )
    assert outcome.handled is False
    assert outcome.answer is None
    assert outcome.status is None
    assert outcome.trace == {}
    assert memory_store.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "messages",
    [
        None,
        [],
        ["How are you sure?"],
        [{"role": "user", "content": 7}],
        [{"role": "tool", "content": "How are you sure?"}],
    ],
)
async def test_missing_or_malformed_final_message_is_not_intercepted(messages):
    memory_store = _MemoryStore()
    outcome = await _resolve(memory_store, messages=messages)
    assert outcome == ClaimExplanationOutcome(False, None, None, {})
    assert memory_store.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content",
    [
        "How are you sure?",
        f'What supports the statement "{ANCHOR}"?',
    ],
)
async def test_supported_text_from_user_final_message_remains_handled(content):
    memory_store = _MemoryStore()
    outcome = await _resolve(
        memory_store,
        messages=[
            {"role": "assistant", "content": ANCHOR},
            {"role": "user", "content": content},
        ],
    )
    assert outcome.handled is True
    assert memory_store.calls == [
        {"owner_id": "owner", "conversation_id": "conversation-1", "limit": 20}
    ]


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
async def test_linked_claim_keeps_support_explanation_without_trace_lookup():
    memory_store = _MemoryStore(
        records=[_record(acquisition_manifest_id=MANIFEST_ID)],
        trace_error=AssertionError("support explanation must not fetch the trace"),
    )

    outcome = await _resolve(memory_store)

    assert outcome.status == "ok"
    assert outcome.answer.startswith("I based that earlier statement on")
    assert outcome.answer.endswith(
        "I did not perform a new verification for this explanation."
    )
    assert memory_store.trace_calls == []


@pytest.mark.asyncio
async def test_targeted_acquisition_explanation_is_exact_and_non_exhaustive():
    memory_store = _MemoryStore(
        records=[_record(acquisition_manifest_id=MANIFEST_ID)]
    )

    outcome = await _resolve(
        memory_store,
        messages=_messages(follow_up="What did you check?"),
    )

    assert outcome.status == "ok"
    assert outcome.answer == (
        "For that earlier answer, the retained record shows a targeted lookup. It "
        "considered 2 configured sources, selected 2, returned 2 items, and delivered "
        "2 to reasoning. The recorded evidence was sufficient for the declared "
        "targeted scope. This was not an exhaustive review of every potentially "
        "relevant source. I did not perform a new verification for this explanation."
    )
    assert memory_store.calls == [
        {"owner_id": "owner", "conversation_id": "conversation-1", "limit": 20}
    ]
    assert memory_store.trace_calls == ["request-1"]
    assert outcome.trace["explanation_kind"] == "acquisition"
    assert outcome.trace["acquisition_question"] == "checked"
    assert outcome.trace["storage_call_count"] == 2
    assert outcome.trace["provider_call_count"] == 0
    assert outcome.trace["aggregate_counts"]["sources_considered"] == 2
    serialized = repr(outcome)
    for prohibited in (
        MANIFEST_ID,
        "source-a",
        "record-1",
        "PRIVATE-PROMPT-CONTENT",
        "PRIVATE-SOURCE-CONTENT",
        "PRIVATE-MODEL-CALL",
    ):
        assert prohibited not in serialized


@pytest.mark.asyncio
async def test_prior_provider_text_cannot_select_manifest_or_rendered_history():
    provider_selected_manifest = "evidence_manifest_provider_selected"
    anchor = (
        "The retained file reports that "
        f"{provider_selected_manifest} checked every source."
    )
    record = _record(
        claim_anchor=anchor,
        claim_anchor_digest=_digest(anchor),
        acquisition_manifest_id=MANIFEST_ID,
    )
    manifest = _manifest()
    manifest["response_digest"] = _digest(anchor)
    outcome = await _resolve(
        _MemoryStore(
            records=[record],
            trace=_trace(manifest=manifest),
        ),
        messages=_messages(
            prior=anchor,
            follow_up="What did you check?",
        ),
    )

    assert outcome.status == "ok"
    assert outcome.answer.startswith(
        "For that earlier answer, the retained record shows a targeted lookup."
    )
    assert "considered 2 configured sources" in outcome.answer
    assert "not an exhaustive review" in outcome.answer
    assert provider_selected_manifest not in repr(outcome)


@pytest.mark.asyncio
async def test_exact_fetch_and_coverage_explanations_preserve_declared_boundaries():
    exact_store = _MemoryStore(
        records=[_record(acquisition_manifest_id=MANIFEST_ID)],
        trace=_trace(manifest=_manifest(strategy="exact_fetch")),
    )
    exact = await _resolve(
        exact_store,
        messages=_messages(follow_up="What did you examine?"),
    )
    assert exact.answer == (
        "For that earlier answer, the retained record shows exact fetches for 2 "
        "specified references. 2 were retrieved and 2 were delivered to reasoning. "
        "The recorded evidence was sufficient for that declared exact-reference "
        "scope. Sources or references outside that supplied scope were not established "
        "as examined. I did not perform a new verification for this explanation."
    )

    coverage = await _resolve(
        _MemoryStore(records=[_record(acquisition_manifest_id=MANIFEST_ID)]),
        messages=_messages(follow_up="Did you look at everything relevant?"),
    )
    assert coverage.answer.startswith("No—not universally.")
    assert "sufficient for the declared targeted scope" in coverage.answer
    assert "not an exhaustive review" in coverage.answer
    assert "everything relevant was checked" not in coverage.answer


@pytest.mark.asyncio
async def test_gap_and_limited_rendering_uses_only_retained_structural_limits():
    manifest = _manifest(status="sufficient_with_limitations")
    manifest["plan"]["plan_status"] = "ready_with_limitations"
    manifest["plan"]["limitation_codes"] = ["optional_source_unavailable"]
    manifest["inventory"].update(
        {
            "inventory_status": "partial",
            "inventory_source_count": 5,
            "available_source_count": 2,
            "unavailable_source_count": 1,
            "disabled_source_count": 1,
            "unknown_source_count": 1,
        }
    )
    acquisition = manifest["acquisition"]
    acquisition["source_references_retained"] = ["source-a:record-1"]
    acquisition["source_references_filtered_or_omitted"] = [
        "source-b:record-2"
    ]
    acquisition["prompt_retained_item_count"] = 1
    acquisition["dsa_budget_truncation"] = True
    acquisition["candidate_truncation"] = True
    outcome = await _resolve(
        _MemoryStore(
            records=[_record(acquisition_manifest_id=MANIFEST_ID)],
            trace=_trace(manifest=manifest),
        ),
        messages=_messages(follow_up="What might you have missed?"),
    )

    assert outcome.status == "ok"
    assert outcome.answer.startswith(
        "The retained record cannot identify unknown evidence outside its declared "
        "source scope."
    )
    for expected in (
        "sufficient only with recorded limitations",
        "1 configured source was unavailable",
        "1 configured source was disabled",
        "1 configured source had unknown availability",
        "1 returned reference was filtered or omitted before reasoning",
        "retained source inventory was partial",
        "truncated by the retrieval budget",
        "Candidate selection was truncated",
        "not an exhaustive review",
        "did not perform a new verification",
    ):
        assert expected in outcome.answer
    for prohibited in ("source-a", "source-b", "record-1", "record-2"):
        assert prohibited not in outcome.answer


@pytest.mark.asyncio
async def test_privacy_suppressed_manifest_preserves_aggregate_explanation():
    ordinary = await _resolve(
        _MemoryStore(records=[_record(acquisition_manifest_id=MANIFEST_ID)]),
        messages=_messages(follow_up="What did you check?"),
    )
    suppressed_store = _MemoryStore(
        records=[_record(acquisition_manifest_id=MANIFEST_ID)],
        trace=_suppressed_trace(),
    )
    suppressed = await _resolve(
        suppressed_store,
        messages=_messages(follow_up="What did you check?"),
    )

    assert suppressed.status == "ok"
    assert suppressed.answer == ordinary.answer
    serialized = repr(suppressed)
    assert "source-a" not in serialized
    assert "record-1" not in serialized

    inconsistent = _suppressed_trace()
    inconsistent["prompt"]["evidence_acquisition"]["acquisition"][
        "sources_considered"
    ] = ["PRIVATE-SOURCE-ID"]
    invalid = await _resolve(
        _MemoryStore(
            records=[_record(acquisition_manifest_id=MANIFEST_ID)],
            trace=inconsistent,
        ),
        messages=_messages(follow_up="What did you check?"),
    )
    assert invalid.status == "degraded"
    assert invalid.trace["reason_code"] == "acquisition_manifest_invalid"
    assert "PRIVATE-SOURCE-ID" not in repr(invalid)


@pytest.mark.asyncio
async def test_missing_link_and_trace_dependency_failure_are_honest():
    no_link_store = _MemoryStore(records=[_record()])
    no_link = await _resolve(
        no_link_store,
        messages=_messages(follow_up="What did you check?"),
    )
    assert no_link.answer == (
        "I don’t have a retained acquisition record linked to that earlier answer, "
        "so I can’t honestly say what was checked or missed. I did not perform a new "
        "verification."
    )
    assert no_link.status == "degraded"
    assert no_link_store.trace_calls == []

    unavailable_store = _MemoryStore(
        records=[_record(acquisition_manifest_id=MANIFEST_ID)],
        trace_error=RuntimeError("PRIVATE-TRACE-EXCEPTION"),
    )
    unavailable = await _resolve(
        unavailable_store,
        messages=_messages(follow_up="What did you check?"),
    )
    assert unavailable.answer == (
        "I couldn’t access the retained acquisition record for that earlier answer. "
        "I can’t honestly reconstruct what was checked from memory, and I did not "
        "perform a new verification."
    )
    assert unavailable.status == "degraded"
    assert unavailable_store.trace_calls == ["request-1"]
    assert "PRIVATE-TRACE-EXCEPTION" not in repr(unavailable)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "trace_malformed",
        "request_mismatch",
        "owner_mismatch",
        "conversation_mismatch",
        "surface_mismatch",
        "current_surface_mismatch",
        "trace_ineligible",
        "prompt_missing",
        "manifest_missing",
        "manifest_malformed",
        "manifest_id_mismatch",
        "assistant_mismatch",
        "digest_mismatch",
        "not_attempted",
        "plan_missing",
        "plan_not_ready",
        "sufficiency_missing",
        "top_insufficient",
        "nested_unknown",
        "status_disagreement",
    ],
)
async def test_acquisition_trace_association_failures_are_bounded(mutation):
    trace = _trace()
    surface = "vscode"
    if mutation == "trace_malformed":
        trace = ["PRIVATE-TRACE-BODY"]
    elif mutation == "request_mismatch":
        trace["request_id"] = "request-other"
    elif mutation == "owner_mismatch":
        trace["owner_id"] = "other"
    elif mutation == "conversation_mismatch":
        trace["conversation_id"] = "conversation-other"
    elif mutation == "surface_mismatch":
        trace["surface"] = "other"
    elif mutation == "current_surface_mismatch":
        surface = "other"
    elif mutation == "trace_ineligible":
        trace["status"] = "failed"
    elif mutation == "prompt_missing":
        trace.pop("prompt")
    elif mutation == "manifest_missing":
        trace["prompt"].pop("evidence_acquisition")
    elif mutation == "manifest_malformed":
        trace["prompt"]["evidence_acquisition"] = ["PRIVATE-MANIFEST"]
    else:
        manifest = trace["prompt"]["evidence_acquisition"]
        if mutation == "manifest_id_mismatch":
            manifest["manifest_id"] = "evidence_manifest_other"
        elif mutation == "assistant_mismatch":
            manifest["assistant_message_id"] = "assistant-other"
        elif mutation == "digest_mismatch":
            manifest["response_digest"] = "sha256:" + ("0" * 64)
        elif mutation == "not_attempted":
            manifest["attempted"] = False
        elif mutation == "plan_missing":
            manifest.pop("plan")
        elif mutation == "plan_not_ready":
            manifest["plan"]["plan_status"] = "unsupported"
        elif mutation == "sufficiency_missing":
            manifest.pop("sufficiency")
        elif mutation == "top_insufficient":
            manifest["status"] = "insufficient"
        elif mutation == "nested_unknown":
            manifest["sufficiency"]["status"] = "unknown"
        elif mutation == "status_disagreement":
            manifest["status"] = "sufficient_with_limitations"

    outcome = await _resolve(
        _MemoryStore(
            records=[_record(acquisition_manifest_id=MANIFEST_ID)],
            trace=trace,
        ),
        messages=_messages(follow_up="What did you check?"),
        surface=surface,
    )

    assert outcome.status == "degraded"
    assert outcome.answer == (
        "The retained acquisition record for that earlier answer was incomplete or "
        "did not match the response, so I can’t safely describe what was checked. I "
        "did not perform a new verification."
    )
    assert outcome.trace["reason_code"] == "acquisition_manifest_invalid"
    assert outcome.trace["provider_call_count"] == 0
    serialized = repr(outcome)
    for prohibited in (
        "PRIVATE-TRACE-BODY",
        "PRIVATE-MANIFEST",
        MANIFEST_ID,
        "request-other",
        "assistant-other",
    ):
        assert prohibited not in serialized


@pytest.mark.asyncio
async def test_exact_fetch_limitations_render_failed_unknown_and_truncated_counts():
    manifest = _manifest(
        strategy="exact_fetch",
        status="sufficient_with_limitations",
    )
    manifest["plan"]["plan_status"] = "ready_with_limitations"
    acquisition = manifest["acquisition"]
    acquisition["exact_reference_attempts"][0]["outcome"] = "failed"
    acquisition["exact_reference_attempts"][1]["outcome"] = "truncated"
    acquisition["exact_reference_successful_count"] = 0
    acquisition["exact_reference_failed_count"] = 1
    acquisition["exact_reference_truncated_count"] = 1
    acquisition["source_references_returned"] = []
    acquisition["source_references_retained"] = []
    acquisition["source_references_unsuccessful"] = [
        "source-a:record-1",
        "source-b:record-2",
    ]
    acquisition["item_count"] = 0
    acquisition["usable_item_count"] = 0
    acquisition["prompt_retained_item_count"] = 0
    acquisition["context_delivery_status"] = "retained"
    outcome = await _resolve(
        _MemoryStore(
            records=[_record(acquisition_manifest_id=MANIFEST_ID)],
            trace=_trace(manifest=manifest),
        ),
        messages=_messages(follow_up="What might you have missed?"),
    )
    assert outcome.status == "ok"
    assert "1 exact fetch failed" in outcome.answer
    assert "1 exact fetch was truncated" in outcome.answer
    assert "sufficient only with recorded limitations" in outcome.answer


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
async def test_quoted_acquisition_target_resolves_only_one_exact_older_record():
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
        acquisition_manifest_id=MANIFEST_ID,
    )
    older_manifest = _manifest(
        assistant_message_id="assistant-old",
        response_digest=_digest(OLDER_ANCHOR),
    )
    trace = _trace(
        manifest=older_manifest,
        request_id="request-1",
    )
    memory_store = _MemoryStore([newer, older], trace=trace)

    outcome = await _resolve(
        memory_store,
        messages=_messages(
            prior="A newer answer.",
            follow_up=(
                f'What did you check for the statement "{OLDER_ANCHOR}"?'
            ),
        ),
    )

    assert outcome.status == "ok"
    assert outcome.trace["target_mode"] == "quoted_anchor"
    assert outcome.trace["reason_code"] == "quoted_acquisition_record_resolved"
    assert memory_store.trace_calls == ["request-1"]
    assert OLDER_ANCHOR not in outcome.answer


@pytest.mark.asyncio
async def test_quoted_acquisition_missing_and_ambiguous_targets_do_not_choose():
    missing = await _resolve(
        _MemoryStore(
            records=[_record(acquisition_manifest_id=MANIFEST_ID)]
        ),
        messages=_messages(
            follow_up='What did you check for the statement "No retained match."?'
        ),
    )
    assert missing.status == "degraded"
    assert missing.trace["reason_code"] == "quoted_claim_record_not_found"

    duplicates = [
        _record(
            claim_id=f"claim-{index}",
            assistant_message_id=f"assistant-{index}",
            claim_anchor=OLDER_ANCHOR,
            claim_anchor_digest=_digest(OLDER_ANCHOR),
            acquisition_manifest_id=MANIFEST_ID,
        )
        for index in range(2)
    ]
    ambiguous = await _resolve(
        _MemoryStore(records=duplicates),
        messages=_messages(
            follow_up=(
                f'What did you check for the statement "{OLDER_ANCHOR}"?'
            )
        ),
    )
    assert ambiguous.status == "degraded"
    assert ambiguous.trace["reason_code"] == "ambiguous_quoted_claim"
    assert ambiguous.trace["matched_record_count"] == 2
    assert ambiguous.trace["acquisition_trace_lookup_status"] == "not_requested"


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
        (
            _record(acquisition_manifest_id="unsafe manifest id"),
            "claim_record_response_invalid",
        ),
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
