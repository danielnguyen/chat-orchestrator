from __future__ import annotations

import hashlib
import json

import pytest
from pydantic import ValidationError
from services.evidence_acquisition import (
    LIMITATION_SUFFIX,
    TARGETED_SCOPE_SUFFIX,
    WITHHELD_ANSWER,
    DsaSourceListResponse,
    begin_evidence_acquisition,
    bind_manifest_response,
    build_manifest_trace,
    enforce_final_answer,
    evaluate_acquisition_sufficiency,
    provider_allowed,
    suppress_manifest_identifiers,
    validate_context_pack_response,
)
from settings import Settings

SCOPE = {
    "request_id": "rid",
    "owner_id": "owner",
    "conversation_id": "conv",
    "surface": "dev",
    "runtime_session_id": "rtsession_1",
    "runtime_turn_id": "rtturn_1",
}
QUESTION = "Verify the record."
QUESTION_DIGEST = f"sha256:{hashlib.sha256(QUESTION.encode()).hexdigest()}"


def _settings(**overrides):
    values = {
        "ORCH_API_KEY": "key",
        "MEMORY_STORE_BASE_URL": "http://memory",
        "MEMORY_STORE_API_KEY": "key",
        "LITELM_BASE_URL": "http://models",
    }
    values["LITELLM_BASE_URL"] = values.pop("LITELM_BASE_URL")
    values.update(overrides)
    return Settings(**values)


def _shape_response(*, status="derived", shape="targeted_lookup"):
    result = {
        "derivation_id": "evidence_shape_1",
        "question_anchor": QUESTION,
        "question_anchor_digest": QUESTION_DIGEST,
        "derivation_status": status,
        "task_shape": shape if status == "derived" else None,
        "candidate_task_shapes": [shape] if status == "derived" else [],
        "evidence_scope_material": status != "not_applicable",
        "clarification_required": status == "ambiguous",
        "reason_codes": (
            ["explicit_evidence_language", "targeted_lookup_derived"]
            if status == "derived"
            else ["ordinary_chat_without_material_evidence_scope"]
            if status == "not_applicable"
            else ["multiple_incompatible_shapes"]
        ),
        "user_safe_summary": "Bounded result.",
    }
    return {**SCOPE, "result": result}


def _source(
    source_id,
    *,
    capabilities=None,
    enabled=True,
    status="ready",
    tags=None,
):
    return {
        "source_id": source_id,
        "display_name": f"Source {source_id}",
        "connector": "neutral_connector",
        "domain_tags": tags or ["records"],
        "sensitivity": "medium",
        "access_mode": "read_only",
        "capabilities": capabilities or ["profile", "search"],
        "enabled": enabled,
        "status": status,
        "last_checked_at": "2026-07-17T00:00:00Z",
        "last_error": None,
    }


def _plan_response(*, status="ready", requirements=None, limitations=None):
    requirements = requirements or [
        {
            "requirement_id": "targeted-evidence",
            "requirement_kind": "targeted_evidence",
            "criticality": "material",
        },
        {
            "requirement_id": "context-delivery",
            "requirement_kind": "context_delivery",
            "criticality": "material",
        },
    ]
    return {
        **SCOPE,
        "result": {
            "plan_id": "evidence_plan_1",
            "question_anchor": QUESTION,
            "question_anchor_digest": QUESTION_DIGEST,
            "task_shape": "targeted_lookup",
            "plan_status": status,
            "completeness_expectation": "targeted_scope",
            "contradiction_search_required": False,
            "eligible_source_ids": ["source_a"],
            "authoritative_source_ids": [],
            "selected_strategies": ["targeted_retrieval"],
            "declared_requirements": requirements,
            "limitation_codes": limitations or [],
            "user_safe_summary": "A strategy is available.",
        },
    }


def _sufficiency_response(
    manifest_id,
    *,
    status="sufficient_for_declared_scope",
    requirements=None,
):
    requirements = requirements or _plan_response()["result"]["declared_requirements"]
    evaluations = [
        {
            **requirement,
            "effective_outcome": (
                "unavailable"
                if requirement["criticality"] == "optional"
                and status == "sufficient_with_limitations"
                else "satisfied"
            ),
        }
        for requirement in requirements
    ]
    constraints = (
        []
        if status == "sufficient_for_declared_scope"
        else [
            "qualify_conclusion",
            "disclose_limitations",
            "identify_unexamined_scope",
        ]
        if status == "sufficient_with_limitations"
        else [
            "qualify_conclusion",
            "disclose_limitations",
            "identify_unexamined_scope",
            "additional_acquisition_or_clarification_required",
            "withhold_unqualified_conclusion",
        ]
    )
    reasons = (
        ["all_declared_requirements_satisfied"]
        if status == "sufficient_for_declared_scope"
        else ["optional_requirement_incomplete"]
        if status == "sufficient_with_limitations"
        else ["material_requirement_not_satisfied"]
    )
    return {
        **SCOPE,
        "evidence_plan_id": "evidence_plan_1",
        "acquisition_manifest_id": manifest_id,
        "result": {
            "evaluation_id": "evidence_eval_1",
            "task_shape": "targeted_lookup",
            "sufficiency_status": status,
            "evaluated_requirements": evaluations,
            "reason_codes": reasons,
            "answer_constraints": constraints,
            "qualification_required": status != "sufficient_for_declared_scope",
            "additional_acquisition_required": status in {"insufficient", "unknown"},
            "user_safe_summary": "Bounded sufficiency.",
        },
    }


class FakeRuntime:
    def __init__(
        self,
        *,
        shape=None,
        plan=None,
        sufficiency_status="sufficient_for_declared_scope",
    ):
        self.shape = shape or _shape_response()
        self.plan = plan or _plan_response()
        self.sufficiency_status = sufficiency_status
        self.calls = []

    async def derive_evidence_shape(self, **kwargs):
        self.calls.append(("shape", kwargs))
        return self.shape

    async def compile_evidence_plan(self, **kwargs):
        self.calls.append(("plan", kwargs))
        return self.plan

    async def evaluate_evidence_sufficiency(self, **kwargs):
        self.calls.append(("sufficiency", kwargs))
        return _sufficiency_response(
            kwargs["acquisition_manifest_id"],
            status=self.sufficiency_status,
            requirements=kwargs["declared_requirements"],
        )


class FakeDsa:
    def __init__(self, sources):
        self.sources = sources
        self.calls = []

    async def list_sources(self):
        self.calls.append("list_sources")
        return {"sources": self.sources}


@pytest.mark.parametrize(
    "overrides",
    [
        {"EVIDENCE_ACQUISITION_ENABLED": True},
        {
            "EVIDENCE_ACQUISITION_ENABLED": True,
            "COGNITIVE_RUNTIME_BASE_URL": "http://runtime",
        },
        {
            "EVIDENCE_ACQUISITION_ENABLED": True,
            "COGNITIVE_RUNTIME_BASE_URL": "http://runtime",
            "COGNITIVE_RUNTIME_INTERACTION_GOVERNANCE_ENABLED": True,
        },
    ],
)
def test_feature_flag_requires_runtime_governance_and_dsa(overrides):
    with pytest.raises(ValueError, match="evidence acquisition requires"):
        _settings(**overrides)


def test_feature_flag_is_disabled_by_default_and_valid_when_dependencies_enabled():
    assert _settings().evidence_acquisition_enabled is False
    configured = _settings(
        EVIDENCE_ACQUISITION_ENABLED=True,
        COGNITIVE_RUNTIME_BASE_URL="http://runtime",
        COGNITIVE_RUNTIME_INTERACTION_GOVERNANCE_ENABLED=True,
        DSA_ENABLED=True,
    )
    assert configured.evidence_acquisition_enabled is True


@pytest.mark.asyncio
async def test_begin_calls_shape_inventory_plan_and_maps_only_approved_capabilities():
    runtime = FakeRuntime()
    dsa = FakeDsa(
        [
            _source("source_b", capabilities=["profile"], status="unavailable"),
            _source("source_a", capabilities=["context", "search", "fetch", "profile"]),
            _source("source_c", enabled=False, status="ready"),
            _source("source_d", status="unknown"),
        ]
    )

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={"source_ids": ["source_a"], "domain_tags": ["records"]},
        **SCOPE,
    )

    assert state.supported_targeted_path is True
    assert dsa.calls == ["list_sources"]
    assert [name for name, _ in runtime.calls] == ["shape", "plan"]
    inventory = runtime.calls[1][1]["source_inventory"]
    assert inventory == [
        {
            "source_id": "source_a",
            "source_categories": ["records"],
            "capabilities": ["context_expansion", "exact_fetch", "targeted_retrieval"],
            "availability": "available",
            "authority_role": "unknown",
        },
        {
            "source_id": "source_b",
            "source_categories": ["records"],
            "capabilities": [],
            "availability": "unavailable",
            "authority_role": "unknown",
        },
        {
            "source_id": "source_c",
            "source_categories": ["records"],
            "capabilities": ["targeted_retrieval"],
            "availability": "disabled",
            "authority_role": "unknown",
        },
        {
            "source_id": "source_d",
            "source_categories": ["records"],
            "capabilities": ["targeted_retrieval"],
            "availability": "unknown",
            "authority_role": "unknown",
        },
    ]
    assert runtime.calls[1][1]["declared_scope"]["inventory_status"] == (
        "complete_for_declared_scope"
    )


@pytest.mark.asyncio
async def test_not_applicable_stops_before_inventory_and_follows_existing_path():
    runtime = FakeRuntime(shape=_shape_response(status="not_applicable"))
    dsa = FakeDsa([_source("source_a")])

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text="Explain photosynthesis.",
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.follow_existing_path is True
    assert dsa.calls == []
    assert [name for name, _ in runtime.calls] == ["shape"]


def test_inventory_rejects_duplicates_extras_and_unknown_capabilities():
    with pytest.raises(ValidationError):
        DsaSourceListResponse.model_validate(
            {"sources": [_source("source_a"), _source("source_a")]}
        )
    with pytest.raises(ValidationError):
        DsaSourceListResponse.model_validate(
            {"sources": [{**_source("source_a"), "metadata": {"raw": "private"}}]}
        )
    with pytest.raises(ValidationError):
        DsaSourceListResponse.model_validate(
            {"sources": [_source("source_a", capabilities=["search", "rank"])]}
        )


def _context_pack():
    return {
        "query_id": "query_1",
        "query": QUESTION,
        "sources_used": ["source_a"],
        "items": [
            {
                "result_id": "result_1",
                "source_type": "record",
                "source_id": "source_a",
                "source_name": "PRIVATE SOURCE NAME",
                "source_ref": "source_a:record_1",
                "retrieved_at": "2026-07-17T00:00:00Z",
                "source_modified_at": None,
                "title": "PRIVATE TITLE",
                "content_type": "text",
                "text": "PRIVATE SOURCE CONTENT",
                "confidence": "high",
                "warnings": [],
            }
        ],
        "warnings": [],
        "errors": [],
        "budget": {
            "max_results": 5,
            "returned_results": 1,
            "estimated_bytes": 80,
            "truncated": False,
        },
        "diagnostics": {
            "selection_mode": "explicit_source_ids",
            "considered_source_ids": ["source_a"],
            "selected_source_ids": ["source_a"],
            "source_diagnostics": [],
            "ranking_mode": "single_source",
            "candidate_counts_by_source": {"source_a": 1},
            "budget_truncated_candidates": False,
        },
    }


def _validated_context_pack(
    response=None,
    *,
    eligible_source_ids=("source_a",),
):
    return validate_context_pack_response(
        response or _context_pack(),
        expected_query=QUESTION,
        eligible_source_ids=eligible_source_ids,
    )


def test_context_pack_contract_rejects_raw_metadata_and_malformed_items():
    validated = _validated_context_pack()
    assert validated["query_id"] == "query_1"
    with pytest.raises(ValidationError):
        _validated_context_pack(
            {
                **_context_pack(),
                "items": [{**_context_pack()["items"][0], "raw": {"secret": "value"}}],
            }
        )
    for diagnostics in (
        {
            **_context_pack()["diagnostics"],
            "considered_source_ids": ["source_a", "source_a"],
        },
        {
            **_context_pack()["diagnostics"],
            "source_diagnostics": [
                {
                    "source_id": "source_a",
                    "score": 1,
                    "score_band": "eligible",
                    "reasons": ["bounded_match"],
                },
                {
                    "source_id": "source_a",
                    "score": 1,
                    "score_band": "eligible",
                    "reasons": ["bounded_match"],
                },
            ],
        },
    ):
        with pytest.raises(ValidationError):
            _validated_context_pack(
                {
                    **_context_pack(),
                    "diagnostics": diagnostics,
                }
            )
    with pytest.raises(ValidationError):
        _validated_context_pack(
            {
                **_context_pack(),
                "items": [{**_context_pack()["items"][0], "text": ""}],
            }
        )
    with pytest.raises(ValidationError):
        _validated_context_pack(
            {
                **_context_pack(),
                "items": [
                    {
                        **_context_pack()["items"][0],
                        "source_ref": "https://private.example/record?token=secret",
                    }
                ],
            }
        )


@pytest.mark.parametrize(
    ("mutation", "eligible_source_ids", "expected_error"),
    [
        (
            lambda response: response.update(query="Unrelated bounded question."),
            ("source_a",),
            "context_pack_query_mismatch",
        ),
        (
            lambda response: response.update(sources_used=["source_a", "source_b"]),
            ("source_a",),
            "context_source_not_eligible",
        ),
        (
            lambda response: response["items"][0].update(source_id="source_b"),
            ("source_a", "source_b"),
            "context_item_source_not_used",
        ),
        (
            lambda response: (
                response["items"][0].update(source_id="source_b"),
                response.update(sources_used=["source_a", "source_b"]),
            ),
            ("source_a",),
            "context_item_source_not_eligible",
        ),
        (
            lambda response: response["diagnostics"].update(
                considered_source_ids=["source_a", "source_b"]
            ),
            ("source_a",),
            "diagnostic_considered_source_not_eligible",
        ),
        (
            lambda response: response["diagnostics"].update(
                considered_source_ids=[],
                selected_source_ids=["source_a"],
            ),
            ("source_a",),
            "diagnostic_selected_source_not_considered",
        ),
        (
            lambda response: response["diagnostics"].update(selected_source_ids=[]),
            ("source_a",),
            "diagnostic_selected_source_mismatch",
        ),
        (
            lambda response: response["diagnostics"].update(
                source_diagnostics=[
                    {
                        "source_id": "source_b",
                        "score": 1,
                        "score_band": "eligible",
                        "reasons": ["bounded_match"],
                    }
                ]
            ),
            ("source_a", "source_b"),
            "source_diagnostic_not_considered",
        ),
        (
            lambda response: response["diagnostics"].update(
                candidate_counts_by_source={"source_b": 1}
            ),
            ("source_a", "source_b"),
            "candidate_count_source_not_selected",
        ),
    ],
    ids=[
        "query-mismatch",
        "source-used-outside-plan",
        "item-source-not-used",
        "item-source-outside-plan",
        "considered-source-outside-plan",
        "selected-source-not-considered",
        "selected-source-differs-from-used",
        "source-diagnostic-not-considered",
        "candidate-count-source-not-selected",
    ],
)
def test_context_pack_contract_rejects_plan_association_mismatch(
    mutation,
    eligible_source_ids,
    expected_error,
):
    response = _context_pack()
    mutation(response)
    with pytest.raises(ValueError, match=expected_error):
        _validated_context_pack(
            response,
            eligible_source_ids=eligible_source_ids,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retained_refs", "expected_status", "provider_is_allowed"),
    [
        ({"source_a:record_1"}, "sufficient_for_declared_scope", True),
        (set(), "insufficient", False),
    ],
)
async def test_actual_prompt_delivery_controls_sufficiency(
    retained_refs,
    expected_status,
    provider_is_allowed,
):
    runtime = FakeRuntime(sufficiency_status=expected_status)
    dsa = FakeDsa([_source("source_a")])
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )
    context = _validated_context_pack()

    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=context,
        dsa_trace={"status": "success", "called": True},
        retained_source_refs=retained_refs,
        **SCOPE,
    )

    facts = runtime.calls[-1][1]["acquisition_facts"]
    context_fact = next(
        fact for fact in facts if fact["requirement_id"] == "context-delivery"
    )
    assert context_fact["outcome"] == (
        "satisfied" if retained_refs else "filtered"
    )
    assert provider_allowed(state) is provider_is_allowed
    if not provider_is_allowed:
        assert state.forced_answer == WITHHELD_ANSWER


@pytest.mark.asyncio
async def test_non_returned_prompt_reference_is_unknown_and_not_retained():
    runtime = FakeRuntime(sufficiency_status="unknown")
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([_source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )
    context = _validated_context_pack()
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=context,
        dsa_trace={"status": "success", "called": True},
        retained_source_refs={"source_a:not_returned"},
        **SCOPE,
    )

    facts = runtime.calls[-1][1]["acquisition_facts"]
    assert {
        fact["requirement_id"]: fact["outcome"]
        for fact in facts
    }["context-delivery"] == "unknown"
    assert provider_allowed(state) is False
    manifest = build_manifest_trace(
        state=state,
        context_pack=context,
        dsa_trace={"status": "success", "called": True},
        retained_source_refs={"source_a:not_returned"},
    )
    assert manifest["acquisition"]["source_references_returned"] == [
        "source_a:record_1"
    ]
    assert manifest["acquisition"]["source_references_retained"] == []
    assert manifest["acquisition"]["context_delivery_status"] == "unknown"
    assert "not_returned" not in json.dumps(manifest, sort_keys=True)


@pytest.mark.asyncio
async def test_optional_limitation_allows_provider_and_is_disclosed_once():
    requirements = [
        *_plan_response()["result"]["declared_requirements"],
        {
            "requirement_id": "optional-selected-source-coverage",
            "requirement_kind": "selected_source_coverage",
            "criticality": "optional",
        },
    ]
    runtime = FakeRuntime(
        plan=_plan_response(
            status="ready_with_limitations",
            requirements=requirements,
            limitations=["optional_source_unavailable"],
        ),
        sufficiency_status="sufficient_with_limitations",
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([_source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=_validated_context_pack(),
        dsa_trace={"status": "success", "called": True},
        retained_source_refs={"source_a:record_1"},
        **SCOPE,
    )

    assert provider_allowed(state) is True
    answer = enforce_final_answer("The record gives the date.", state)
    assert answer.count(LIMITATION_SUFFIX) == 1
    assert enforce_final_answer(answer, state).count(LIMITATION_SUFFIX) == 1


@pytest.mark.asyncio
async def test_targeted_scope_overclaim_disclosure_is_bounded_and_idempotent():
    runtime = FakeRuntime()
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([_source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=_validated_context_pack(),
        dsa_trace={"status": "success", "called": True},
        retained_source_refs={"source_a:record_1"},
        **SCOPE,
    )

    ordinary = enforce_final_answer("The targeted record gives the date.", state)
    overclaim = enforce_final_answer("There is no record anywhere.", state)
    assert ordinary == "The targeted record gives the date."
    assert overclaim.count(TARGETED_SCOPE_SUFFIX) == 1
    assert enforce_final_answer(overclaim, state).count(TARGETED_SCOPE_SUFFIX) == 1


@pytest.mark.asyncio
async def test_manifest_association_and_privacy_exclude_raw_content():
    runtime = FakeRuntime()
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([_source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )
    context = _validated_context_pack()
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=context,
        dsa_trace={
            "status": "success",
            "called": True,
            "item_count": 1,
            "raw_item_count": 1,
        },
        retained_source_refs={"source_a:record_1"},
        **SCOPE,
    )
    manifest = build_manifest_trace(
        state=state,
        context_pack=context,
        dsa_trace={"status": "success", "called": True, "raw_item_count": 1},
        retained_source_refs={"source_a:record_1"},
    )
    bind_manifest_response(
        manifest,
        assistant_message_ack={"message_id": "assistant_1"},
        answer="The date is recorded.",
    )
    serialized = json.dumps(manifest, sort_keys=True)

    assert manifest["assistant_message_id"] == "assistant_1"
    assert manifest["response_digest"] == (
        f"sha256:{hashlib.sha256('The date is recorded.'.encode()).hexdigest()}"
    )
    assert set(manifest) == {
        "enabled",
        "attempted",
        "status",
        "manifest_id",
        "assistant_message_id",
        "response_digest",
        "shape",
        "inventory",
        "plan",
        "acquisition",
        "sufficiency",
    }
    assert set(manifest["shape"]) == {
        "derivation_status",
        "task_shape",
        "candidate_count",
        "clarification_required",
        "reason_codes",
    }
    assert set(manifest["inventory"]) == {
        "inventory_status",
        "inventory_source_count",
        "declared_source_count",
        "declared_category_count",
        "available_source_count",
        "unavailable_source_count",
        "disabled_source_count",
        "unknown_source_count",
    }
    assert set(manifest["plan"]) == {
        "plan_id",
        "plan_status",
        "completeness_expectation",
        "contradiction_search_required",
        "selected_strategies",
        "material_requirement_count",
        "optional_requirement_count",
        "limitation_codes",
    }
    assert "PRIVATE SOURCE CONTENT" not in serialized
    assert "PRIVATE TITLE" not in serialized
    assert "PRIVATE SOURCE NAME" not in serialized
    for prohibited in (
        "question_anchor",
        "task_text",
        "prompt_contents",
        "credentials",
        "confidence",
        "reasoning",
        "exception",
    ):
        assert prohibited not in serialized
    private = suppress_manifest_identifiers(manifest)
    assert private["acquisition"]["source_references_retained"] == []
    assert private["acquisition"]["source_references_retained_count"] == 1
