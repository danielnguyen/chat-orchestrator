from __future__ import annotations

import copy
import hashlib
import inspect
import re
from dataclasses import replace

import pytest
from clients.memory_store import MemoryStoreClient
from services.claim_explanation import (
    AcquisitionHistoryProjection,
    ClaimExplanationIntent,
    ClaimExplanationOutcome,
    _diagnose_acquisition_history_projection,
    _project_acquisition_history,
    _render_acquisition,
    is_claim_explanation_intent,
    parse_claim_explanation_intent,
    resolve_claim_explanation,
)

ANCHOR = "The retained file reports that the setting is active."
OLDER_ANCHOR = "According to this document, the service is healthy."
MANIFEST_ID = "evidence_manifest_0123456789abcdef0123456789abcdef"
TARGETED_BOUNDARY = (
    "This reflects only the targeted sources checked, not a complete search of "
    "every possible source."
)


def _digest(value: str = ANCHOR) -> str:
    normalized = " ".join(value.split())
    return f"sha256:{hashlib.sha256(normalized.encode()).hexdigest()}"


def _response_digest(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


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
            "expansion_attempt_count": 0,
            "expansion_successful_count": 0,
            "expansion_unknown_count": 0,
            "expansion_failed_count": 0,
            "expansion_filtered_count": 0,
            "expansion_truncated_count": 0,
            "expansion_unsupported_count": 0,
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
    }


def _hybrid_manifest(
    *,
    task_shape="cross_source_comparison",
    status="sufficient_for_declared_scope",
    next_steps=True,
):
    manifest = _manifest(status=status)
    exhaustive = task_shape == "bounded_exhaustive_review"
    source_ids = ["source-a"] if exhaustive else ["source-a", "source-b"]
    returned = ["range-a"] if exhaustive else ["range-a", "range-b"]
    seeds = ["seed-a"] if exhaustive else ["seed-a", "seed-b"]
    manifest["shape"]["task_shape"] = task_shape
    manifest["plan"].update(
        {
            "completeness_expectation": (
                "complete_for_declared_scope"
                if exhaustive
                else "complete_for_selected_sources"
            ),
            "contradiction_search_required": exhaustive,
            "selected_strategies": ["hybrid"],
            "material_requirement_count": 5 if exhaustive else 3,
        }
    )
    if status == "sufficient_with_limitations":
        manifest["plan"]["plan_status"] = "ready_with_limitations"
    inventory = manifest["inventory"]
    inventory.update(
        {
            "inventory_source_count": len(source_ids),
            "declared_source_count": len(source_ids),
            "available_source_count": len(source_ids),
        }
    )
    acquisition = manifest["acquisition"]
    acquisition.update(
        {
            "strategy_attempted": "hybrid",
            "sources_considered": source_ids,
            "sources_selected": source_ids,
            "sources_used": source_ids,
            "source_references_returned": returned,
            "source_references_retained": returned,
            "source_references_filtered_or_omitted": [],
            "source_references_attempted": seeds,
            "source_references_unsuccessful": [],
            "exact_reference_attempts": [],
            "exact_reference_attempt_count": 0,
            "exact_reference_successful_count": 0,
            "expansion_attempts": [
                {
                    "source_id": source_id,
                    "seed_source_ref": seed,
                    "context_mode": "configured_worksheet" if exhaustive else "nearby_rows",
                    "outcome": "satisfied",
                    "returned_reference_count": 1,
                }
                for source_id, seed in zip(source_ids, seeds, strict=True)
            ],
            "expansion_attempt_count": len(source_ids),
            "expansion_successful_count": len(source_ids),
            "item_count": len(returned),
            "usable_item_count": len(returned),
            "prompt_retained_item_count": len(returned),
        }
    )
    manifest["sufficiency"].update(
        {
            "status": status,
            "qualification_required": status == "sufficient_with_limitations",
            "additional_acquisition_required": status in {"insufficient", "unknown"},
        }
    )
    manifest["status"] = status
    if next_steps:
        manifest["next_steps"] = {
            "selection_count": 1,
            "selections": [
                {
                    "selection_id": "selection-1",
                    "evaluation_id": "evaluation-1",
                    "evidence_plan_id": "plan-1",
                    "acquisition_manifest_id": "manifest-1",
                    "selected_next_step": (
                        "answer_within_declared_scope"
                        if status == "sufficient_for_declared_scope"
                        else "provide_qualified_partial_answer"
                        if status == "sufficient_with_limitations"
                        else "withhold_unsupported_conclusion"
                    ),
                    "conclusion_disposition": (
                        "bounded_conclusion_allowed"
                        if status == "sufficient_for_declared_scope"
                        else "qualified_partial_only"
                        if status == "sufficient_with_limitations"
                        else "requested_conclusion_withheld"
                    ),
                    "provider_disposition": (
                        "allowed"
                        if status
                        in {
                            "sufficient_for_declared_scope",
                            "sufficient_with_limitations",
                        }
                        else "blocked"
                    ),
                    "reacquisition_guard": "not_applicable",
                    "clarification_target": None,
                    "reason_codes": ["bounded_policy_result"],
                    "additional_acquisition_executed": False,
                }
            ],
            "additional_acquisition_count": 0,
            "initial_attempt": None,
            "dependency_status": None,
        }
    return manifest


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
        "expansion_attempts",
        "unavailable_source_ids",
        "failed_source_ids",
    ):
        acquisition[f"{field}_count"] = len(acquisition[field])
        acquisition[field] = []
    acquisition["source_identifiers_suppressed"] = True
    return trace


def _set_path(value, path, replacement):
    target = value
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = replacement
    return value


@pytest.mark.parametrize(
    "manifest",
    [
        _manifest(),
        _manifest(strategy="exact_fetch"),
        _hybrid_manifest(),
        _hybrid_manifest(task_shape="bounded_exhaustive_review"),
        _suppressed_trace()["prompt"]["evidence_acquisition"],
    ],
)
def test_diagnosed_projection_preserves_valid_manifest_acceptance(manifest):
    diagnosed = _diagnose_acquisition_history_projection(manifest)

    assert isinstance(diagnosed, AcquisitionHistoryProjection)
    assert diagnosed.history is not None
    assert diagnosed.reason == "accepted"
    assert _project_acquisition_history(manifest) == diagnosed.history


def _history_with_truncation(*, task_shape="bounded_exhaustive_review"):
    history = _project_acquisition_history(_hybrid_manifest(task_shape=task_shape))
    assert history is not None
    return replace(history, budget_truncated=True, candidate_truncated=True)


def test_bounded_exhaustive_history_distinguishes_seed_search_truncation():
    answer = _render_acquisition(_history_with_truncation(), "coverage")

    assert (
        "The preliminary seed search was truncated, but the configured-scope "
        "expansion completed without truncation."
    ) in answer
    assert "Preliminary seed candidate selection was truncated." in answer
    assert "Acquisition was truncated by the retrieval budget." not in answer
    assert "Candidate selection was truncated." not in answer


def test_non_exhaustive_history_preserves_generic_truncation_wording():
    answer = _render_acquisition(
        _history_with_truncation(task_shape="cross_source_comparison"),
        "coverage",
    )

    assert "Acquisition was truncated by the retrieval budget." in answer
    assert "Candidate selection was truncated." in answer
    assert "configured-scope expansion completed without truncation" not in answer


def test_material_exhaustive_truncation_preserves_generic_wording():
    history = _history_with_truncation()
    history = replace(
        history,
        counts={
            **history.counts,
            "expansion_successful": 0,
            "expansion_truncated": 1,
        },
    )
    answer = _render_acquisition(history, "coverage")

    assert "Acquisition was truncated by the retrieval budget." in answer
    assert "Candidate selection was truncated." in answer
    assert "configured-scope expansion completed without truncation" not in answer


@pytest.mark.parametrize(
    ("manifest", "reason"),
    [
        (None, "manifest_not_object"),
        ({}, "manifest_top_level_keys_invalid"),
        (_set_path(_manifest(), ["enabled"], False), "manifest_enabled_invalid"),
        (_set_path(_manifest(), ["attempted"], False), "manifest_attempted_invalid"),
        (_set_path(_manifest(), ["manifest_id"], "unsafe id"), "manifest_id_invalid"),
        (
            _set_path(_manifest(), ["assistant_message_id"], "unsafe id"),
            "assistant_message_id_invalid",
        ),
        (
            _set_path(_manifest(), ["response_digest"], "invalid"),
            "response_digest_invalid",
        ),
        (
            _set_path(_manifest(), ["plan", "selected_strategies"], ["hybrid"]),
            "strategy_mismatch",
        ),
        (
            _set_path(_manifest(), ["inventory", "inventory_source_count"], -1),
            "inventory_count_invalid_inventory_source_count",
        ),
        (
            _set_path(
                _manifest(),
                ["acquisition", "sources_selected"],
                ["source-a", "source-c"],
            ),
            "selected_sources_not_subset_of_considered",
        ),
        (
            _set_path(
                _manifest(), ["acquisition", "prompt_retained_item_count"], 3
            ),
            "prompt_retained_count_exceeds_usable_count",
        ),
        (
            _set_path(
                _hybrid_manifest(),
                ["acquisition", "expansion_successful_count"],
                1,
            ),
            "expansion_attempt_projection_invalid",
        ),
        (
            _set_path(
                _hybrid_manifest(),
                ["next_steps", "selections", 0, "provider_disposition"],
                "blocked",
            ),
            "next_step_selection_consistency_invalid",
        ),
    ],
)
def test_diagnosed_projection_reasons_are_safe_and_wrapper_stays_fail_closed(
    manifest, reason
):
    diagnosed = _diagnose_acquisition_history_projection(manifest)

    assert diagnosed.history is None
    assert diagnosed.reason == reason
    assert _project_acquisition_history(manifest) is None
    assert __import__("re").fullmatch(r"[a-z0-9_]{1,120}", diagnosed.reason)
    assert "PRIVATE" not in diagnosed.reason


def test_all_projection_rejection_labels_are_bounded_and_privacy_safe():
    source = inspect.getsource(_diagnose_acquisition_history_projection)
    reasons = set(re.findall(r'reject\("([a-z0-9_]+)"\)', source))
    reasons.update(
        f"inventory_count_invalid_{field}"
        for field in (
            "inventory_source_count",
            "declared_source_count",
            "declared_category_count",
            "available_source_count",
            "unavailable_source_count",
            "disabled_source_count",
            "unknown_source_count",
        )
    )
    reasons.update(
        f"identity_projection_invalid_{field}"
        for field in (
            "sources_considered",
            "sources_selected",
            "sources_used",
            "source_references_returned",
            "source_references_retained",
            "source_references_filtered_or_omitted",
            "source_references_attempted",
            "source_references_unsuccessful",
            "unavailable_source_ids",
            "failed_source_ids",
        )
    )
    reasons.update(
        f"acquisition_count_invalid_{field}"
        for field in (
            "item_count",
            "usable_item_count",
            "prompt_retained_item_count",
        )
    )

    assert len(reasons) == 88
    assert all(re.fullmatch(r"[a-z0-9_]{1,120}", reason) for reason in reasons)
    assert all("private" not in reason for reason in reasons)


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
        self.resolution_calls = []

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

    async def resolve_acquisition_history(self, **kwargs):
        self.resolution_calls.append(copy.deepcopy(kwargs))
        if self.trace_error is not None:
            raise self.trace_error
        manifest = copy.deepcopy(self.trace.get("prompt", {}).get("evidence_acquisition"))
        target_mode = kwargs["target_mode"]
        target = kwargs["normalized_first_paragraph"]
        matching = [
            record for record in self.records if record.get("claim_anchor") == target
        ]
        if target_mode == "quoted_first_paragraph" and len(matching) > 1:
            return _resolution_response(
                kwargs,
                status="ambiguous",
                match_count=len(matching),
                reason="quoted_response_ambiguous",
            )
        if target_mode == "quoted_first_paragraph" and not matching:
            return _resolution_response(
                kwargs,
                status="no_record",
                match_count=0,
                reason="quoted_response_not_found",
            )
        if not isinstance(manifest, dict):
            return _resolution_response(
                kwargs,
                status="no_record",
                match_count=0,
                reason=(
                    "immediate_response_manifest_absent"
                    if target_mode == "immediate_previous"
                    else "quoted_response_manifest_absent"
                ),
            )
        supplied_digest = kwargs.get("response_digest")
        if (
            target_mode == "immediate_previous"
            and manifest.get("response_digest") != supplied_digest
        ):
            return _resolution_response(
                kwargs,
                status="no_record",
                match_count=0,
                reason="immediate_response_mismatch",
            )
        return _resolution_response(
            kwargs,
            status="resolved",
            match_count=1,
            reason=(
                "immediate_response_resolved"
                if target_mode == "immediate_previous"
                else "quoted_response_resolved"
            ),
            manifest=manifest,
        )


def _resolution_response(
    request,
    *,
    status,
    match_count,
    reason,
    manifest=None,
):
    response = {
        "schema_version": "acquisition-history-resolution.v1",
        "request_id": request["request_id"],
        "owner_id": request["owner_id"],
        "conversation_id": request["conversation_id"],
        "surface": request["surface"],
        "target_mode": request["target_mode"],
        "resolution_status": status,
        "match_count": match_count,
        "reason_code": reason,
        "record": None,
    }
    if status == "resolved":
        response["record"] = {
            "original_request_id": "request-1",
            "assistant_message_id": manifest["assistant_message_id"],
            "surface": request["surface"],
            "trace_status": "ok",
            "response_digest": manifest["response_digest"],
            "normalized_first_paragraph": request["normalized_first_paragraph"],
            "acquisition_manifest": manifest,
        }
    return response


async def _resolve(memory_store=None, **overrides):
    values = {
        "enabled": True,
        "messages": _messages(),
        "memory_store": memory_store or _MemoryStore(),
        "owner_id": "owner",
        "conversation_id": "conversation-1",
        "surface": "vscode",
        "request_id": "lookup-request-1",
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_mode", "response_digest"),
    [
        ("immediate_previous", "sha256:" + "1" * 64),
        ("quoted_first_paragraph", None),
    ],
)
async def test_memory_store_acquisition_history_request_is_exact(
    monkeypatch,
    target_mode,
    response_digest,
):
    client = MemoryStoreClient("http://memory", "key")
    calls = []

    async def fake_post(path, *, request_id=None, json):
        calls.append((path, request_id, copy.deepcopy(json)))
        return {
            **json,
            "resolution_status": "no_record",
            "match_count": 0,
            "reason_code": (
                "immediate_response_mismatch"
                if target_mode == "immediate_previous"
                else "quoted_response_not_found"
            ),
            "record": None,
        }

    monkeypatch.setattr(client, "_post", fake_post)
    response = await client.resolve_acquisition_history(
        request_id="lookup-1",
        owner_id="owner",
        conversation_id="conversation-1",
        surface="vscode",
        target_mode=target_mode,
        normalized_first_paragraph=ANCHOR,
        response_digest=response_digest,
    )
    assert response["resolution_status"] == "no_record"
    assert calls[0][0:2] == (
        "/v1/internal/acquisition-history/resolve",
        "lookup-1",
    )
    payload = calls[0][2]
    assert payload["schema_version"] == "acquisition-history-resolution.v1"
    assert ("response_digest" in payload) is (response_digest is not None)


@pytest.mark.asyncio
async def test_memory_store_acquisition_history_rejects_scope_mismatch(monkeypatch):
    client = MemoryStoreClient("http://memory", "key")

    async def fake_post(path, *, request_id=None, json):
        return {
            **json,
            "owner_id": "other-owner",
            "resolution_status": "no_record",
            "match_count": 0,
            "reason_code": "immediate_response_mismatch",
            "record": None,
        }

    monkeypatch.setattr(client, "_post", fake_post)
    with pytest.raises(RuntimeError, match="acquisition_history_response_context_mismatch"):
        await client.resolve_acquisition_history(
            request_id="lookup-1",
            owner_id="owner",
            conversation_id="conversation-1",
            surface="vscode",
            target_mode="immediate_previous",
            normalized_first_paragraph=ANCHOR,
            response_digest="sha256:" + "1" * 64,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target_mode", "response_digest", "error"),
    [
        (
            "immediate_previous",
            None,
            "acquisition_history_response_digest_required",
        ),
        (
            "quoted_first_paragraph",
            "sha256:" + "1" * 64,
            "acquisition_history_response_digest_not_allowed",
        ),
    ],
)
async def test_memory_store_acquisition_history_rejects_conflicting_targets(
    target_mode,
    response_digest,
    error,
):
    client = MemoryStoreClient("http://memory", "key")
    with pytest.raises(ValueError, match=error):
        await client.resolve_acquisition_history(
            request_id="lookup-1",
            owner_id="owner",
            conversation_id="conversation-1",
            surface="vscode",
            target_mode=target_mode,
            normalized_first_paragraph=ANCHOR,
            response_digest=response_digest,
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
        "Can you tell me what you checked and then search more?",
        "Did you look at everything relevant about Toronto?",
        "Please explain what you might have missed.",
        f"What did you check for the statement '{OLDER_ANCHOR}'?",
        f"What did you check for the statement “{OLDER_ANCHOR}”?",
        f'What did you check for the statement "{OLDER_ANCHOR}" and "another"?',
        f'What did you check for the statement "{OLDER_ANCHOR}?',
        "What did you check? Check again. Then summarize it.",
        "What did you check? Check again!",
        "Please check again.",
        f'What did you check for the statement "{OLDER_ANCHOR}"? Check again. Now compare it.',
    ],
)
def test_acquisition_intent_rejects_additional_or_malformed_text(phrase):
    assert parse_claim_explanation_intent(phrase) is None


@pytest.mark.parametrize(
    "phrase",
    [
        "What did you check? Check again.",
        "What did you examine? Verify again.",
        "Did you look at everything relevant? Check again.",
        "What might you have missed? Verify again.",
        "What did you not check? Check again.",
        f'What did you check for the statement "{OLDER_ANCHOR}"? Check again.',
    ],
)
def test_exact_compound_acquisition_intents_are_bounded(phrase):
    intent = parse_claim_explanation_intent(phrase)
    assert intent is not None
    assert intent.explanation_kind == "acquisition"
    assert intent.new_verification_requested is True


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
async def test_latest_bounded_response_keeps_support_explanation_wording():
    prior_response = f"{ANCHOR}\n\n{TARGETED_BOUNDARY}"
    memory_store = _MemoryStore(
        records=[_record(acquisition_manifest_id=MANIFEST_ID)],
        trace_error=AssertionError("support explanation must not fetch the trace"),
    )

    outcome = await _resolve(
        memory_store,
        messages=_messages(prior=prior_response),
    )

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
    assert memory_store.calls == []
    assert memory_store.trace_calls == []
    assert len(memory_store.resolution_calls) == 1
    assert outcome.trace["explanation_kind"] == "acquisition"
    assert outcome.trace["acquisition_question"] == "checked"
    assert outcome.trace["storage_call_count"] == 1
    assert outcome.trace["provider_call_count"] == 0
    assert outcome.trace["manifest_projection_status"] == "accepted"
    assert outcome.trace["manifest_projection_reason"] == "accepted"
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
async def test_latest_bounded_acquisition_response_validates_full_digest_and_claim():
    prior_response = f"{ANCHOR}\n\n{TARGETED_BOUNDARY}"
    manifest = _manifest(response_digest=_response_digest(prior_response))
    memory_store = _MemoryStore(
        records=[_record(acquisition_manifest_id=MANIFEST_ID)],
        trace=_trace(manifest=manifest),
    )

    outcome = await _resolve(
        memory_store,
        messages=_messages(
            prior=prior_response,
            follow_up="What did you check?",
        ),
    )

    assert outcome.status == "ok"
    assert outcome.answer == (
        "For that earlier answer, the retained record shows a targeted lookup. It "
        "considered 2 configured sources, selected 2, returned 2 items, and delivered "
        "2 to reasoning. The recorded evidence was sufficient for the declared "
        "targeted scope. This was not an exhaustive review of every potentially "
        "relevant source. I did not perform a new verification for this explanation."
    )
    assert manifest["response_digest"] == _response_digest(prior_response)
    assert manifest["response_digest"] != _digest(ANCHOR)
    assert memory_store.calls == []
    assert memory_store.trace_calls == []
    assert len(memory_store.resolution_calls) == 1
    serialized = repr(outcome)
    assert prior_response not in serialized
    assert manifest["response_digest"] not in serialized
    assert MANIFEST_ID not in serialized


@pytest.mark.asyncio
async def test_latest_bounded_acquisition_rejects_wrong_full_response_digest():
    prior_response = f"{ANCHOR}\n\n{TARGETED_BOUNDARY}"
    manifest = _manifest(response_digest=_response_digest(ANCHOR))

    outcome = await _resolve(
        _MemoryStore(
            records=[_record(acquisition_manifest_id=MANIFEST_ID)],
            trace=_trace(manifest=manifest),
        ),
        messages=_messages(
            prior=prior_response,
            follow_up="What did you check?",
        ),
    )

    assert outcome.status == "degraded"
    assert outcome.trace["reason_code"] == "acquisition_record_not_found"
    assert outcome.trace["manifest_projection_status"] == "not_attempted"
    assert outcome.trace["manifest_projection_reason"] == "not_attempted"
    assert outcome.trace["provider_call_count"] == 0
    assert prior_response not in repr(outcome)
    assert manifest["response_digest"] not in repr(outcome)


@pytest.mark.asyncio
async def test_immediate_resolver_record_must_match_submitted_first_paragraph():
    class WrongParagraphStore(_MemoryStore):
        async def resolve_acquisition_history(self, **kwargs):
            response = await super().resolve_acquisition_history(**kwargs)
            response["record"]["normalized_first_paragraph"] = "A changed paragraph."
            return response

    outcome = await _resolve(
        WrongParagraphStore(records=[]),
        messages=_messages(follow_up="What did you check?"),
    )

    assert outcome.status == "degraded"
    assert outcome.trace["resolution_status"] == "unavailable"
    assert outcome.trace["manifest_projection_status"] == "not_attempted"
    assert outcome.trace["manifest_projection_reason"] == "not_attempted"
    assert ANCHOR not in repr(outcome)


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
@pytest.mark.parametrize(
    ("task_shape", "expected"),
    [
        (
            "cross_source_comparison",
            "bounded comparison across 2 selected configured sources",
        ),
        (
            "bounded_exhaustive_review",
            "bounded exhaustive review of the declared configured scope",
        ),
    ],
)
async def test_trace_first_history_renders_hybrid_and_exhaustive_without_claim(
    task_shape,
    expected,
):
    manifest = _hybrid_manifest(task_shape=task_shape)
    store = _MemoryStore(records=[], trace=_trace(manifest=manifest))
    outcome = await _resolve(
        store,
        messages=_messages(follow_up="What did you check?"),
    )

    assert outcome.status == "ok"
    assert expected in outcome.answer
    assert outcome.answer.endswith(
        "I did not perform a new verification for this explanation."
    )
    assert store.calls == []
    assert store.trace_calls == []
    assert len(store.resolution_calls) == 1


@pytest.mark.asyncio
async def test_bounded_exhaustive_coverage_is_declared_scope_only():
    outcome = await _resolve(
        _MemoryStore(
            records=[],
            trace=_trace(
                manifest=_hybrid_manifest(task_shape="bounded_exhaustive_review")
            ),
        ),
        messages=_messages(follow_up="Did you look at everything relevant?"),
    )
    assert outcome.answer.startswith(
        "Within the declared bounded scope, yes. That does not establish universal "
        "coverage beyond it."
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "wording"),
    [
        ("sufficient_with_limitations", "sufficient only with recorded limitations"),
        ("insufficient", "marked the evidence insufficient"),
        ("unknown", "left evidence sufficiency unknown"),
    ],
)
async def test_history_renders_limited_insufficient_and_unknown(status, wording):
    outcome = await _resolve(
        _MemoryStore(records=[], trace=_trace(manifest=_hybrid_manifest(status=status))),
        messages=_messages(follow_up="What did you check?"),
    )
    assert outcome.status == "ok"
    assert wording in outcome.answer
    assert "requested conclusion was not established" in outcome.answer or status == (
        "sufficient_with_limitations"
    )


@pytest.mark.asyncio
async def test_older_manifest_without_next_steps_remains_compatible():
    outcome = await _resolve(
        _MemoryStore(
            records=[],
            trace=_trace(manifest=_hybrid_manifest(next_steps=False)),
        ),
        messages=_messages(follow_up="What did you check?"),
    )
    assert outcome.status == "ok"
    assert "bounded comparison" in outcome.answer


@pytest.mark.asyncio
async def test_changed_premise_targeted_to_exact_history_is_explicit():
    manifest = _manifest(strategy="exact_fetch")
    manifest["next_steps"] = {
        "selection_count": 2,
        "selections": [
            {
                "selection_id": "selection-1",
                "evaluation_id": "evaluation-1",
                "evidence_plan_id": "plan-1",
                "acquisition_manifest_id": "manifest-1",
                "selected_next_step": "perform_additional_acquisition",
                "conclusion_disposition": "requested_conclusion_withheld",
                "provider_disposition": "blocked",
                "reacquisition_guard": "changed_premise_allowed",
                "clarification_target": None,
                "reason_codes": ["changed_acquisition_premise_available"],
                "additional_acquisition_executed": True,
            },
            {
                "selection_id": "selection-2",
                "evaluation_id": "evaluation-2",
                "evidence_plan_id": "plan-2",
                "acquisition_manifest_id": "manifest-2",
                "selected_next_step": "answer_within_declared_scope",
                "conclusion_disposition": "bounded_conclusion_allowed",
                "provider_disposition": "allowed",
                "reacquisition_guard": "not_applicable",
                "clarification_target": None,
                "reason_codes": ["declared_scope_sufficient"],
                "additional_acquisition_executed": False,
            },
        ],
        "additional_acquisition_count": 1,
        "initial_attempt": {
            "strategy": "targeted_retrieval",
            "sufficiency_status": "insufficient",
            "result_count": 2,
            "retained_reference_count": 2,
            "changed_premise_exact_fetch_followed": True,
        },
        "dependency_status": None,
    }
    outcome = await _resolve(
        _MemoryStore(records=[], trace=_trace(manifest=manifest)),
        messages=_messages(follow_up="What did you check?"),
    )
    assert outcome.status == "ok"
    assert (
        "first performed a targeted lookup and then one authorized changed-premise "
        "exact fetch" in outcome.answer
    )
    assert "unbounded retry" not in outcome.answer


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
    assert invalid.trace["manifest_projection_status"] == "rejected"
    assert (
        invalid.trace["manifest_projection_reason"]
        == "identity_projection_invalid_sources_considered"
    )
    assert invalid.answer == (
        "The retained acquisition record failed association or privacy validation, "
        "so I can’t safely explain it. I did not perform a new verification for this "
        "explanation."
    )
    assert "PRIVATE-SOURCE-ID" not in repr(invalid)


@pytest.mark.asyncio
async def test_no_claim_link_resolves_and_resolver_dependency_failure_is_honest():
    no_link_store = _MemoryStore(records=[_record()])
    no_link = await _resolve(
        no_link_store,
        messages=_messages(follow_up="What did you check?"),
    )
    assert no_link.status == "ok"
    assert "retained record shows a targeted lookup" in no_link.answer
    assert no_link_store.calls == []
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
        "I couldn’t safely access the retained acquisition record for the specified "
        "response. I did not perform a new verification for this explanation."
    )
    assert unavailable.status == "degraded"
    assert unavailable_store.trace_calls == []
    assert len(unavailable_store.resolution_calls) == 1
    assert "PRIVATE-TRACE-EXCEPTION" not in repr(unavailable)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "extra_response_field",
        "contradictory_resolved_shape",
        "record_digest_mismatch",
        "manifest_extra_field",
        "manifest_status_disagreement",
        "next_step_disposition_mismatch",
    ],
)
async def test_acquisition_resolver_and_manifest_failures_are_bounded(mutation):
    class MutatingStore(_MemoryStore):
        async def resolve_acquisition_history(self, **kwargs):
            response = await super().resolve_acquisition_history(**kwargs)
            if mutation == "extra_response_field":
                response["private"] = "PRIVATE-RESPONSE"
            elif mutation == "contradictory_resolved_shape":
                response["record"] = None
            elif mutation == "record_digest_mismatch":
                response["record"]["response_digest"] = "sha256:" + "0" * 64
            elif mutation == "manifest_extra_field":
                response["record"]["acquisition_manifest"]["private"] = (
                    "PRIVATE-MANIFEST"
                )
            elif mutation == "manifest_status_disagreement":
                response["record"]["acquisition_manifest"]["status"] = "unknown"
            else:
                manifest = response["record"]["acquisition_manifest"]
                manifest["next_steps"] = _hybrid_manifest()["next_steps"]
                manifest["next_steps"]["selections"][0]["provider_disposition"] = (
                    "blocked"
                )
            return response

    outcome = await _resolve(
        MutatingStore(records=[]),
        messages=_messages(follow_up="What did you check?"),
    )

    assert outcome.status == "degraded"
    assert outcome.trace["resolution_status"] in {"invalid", "unavailable"}
    assert outcome.trace["provider_call_count"] == 0
    serialized = repr(outcome)
    assert "PRIVATE-RESPONSE" not in serialized
    assert "PRIVATE-MANIFEST" not in serialized
    assert MANIFEST_ID not in serialized


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
        response_digest=_response_digest(
            f"{OLDER_ANCHOR}\n\nHistorical response content is not fetched."
        ),
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
    assert outcome.trace["target_mode"] == "quoted_first_paragraph"
    assert outcome.trace["reason_code"] == "quoted_acquisition_record_resolved"
    assert memory_store.trace_calls == []
    assert memory_store.calls == []
    assert len(memory_store.resolution_calls) == 1
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
    assert missing.trace["reason_code"] == "acquisition_record_not_found"
    assert missing.trace["manifest_projection_status"] == "not_attempted"
    assert missing.trace["manifest_projection_reason"] == "not_attempted"

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
    assert ambiguous.trace["reason_code"] == "acquisition_record_ambiguous"
    assert ambiguous.trace["resolution_status"] == "ambiguous"
    assert ambiguous.trace["manifest_projection_status"] == "not_attempted"
    assert ambiguous.trace["manifest_projection_reason"] == "not_attempted"
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
