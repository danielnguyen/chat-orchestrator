from __future__ import annotations

import copy
import hashlib
import json

import pytest
from models import ChatRequest
from pydantic import ValidationError
from services.evidence_acquisition import (
    BOUNDED_EXHAUSTIVE_CONTEXT_BUDGET,
    COMPARISON_SCOPE_SUFFIX,
    CONFIGURED_WORKSHEET_CONTEXT_MODE,
    EXHAUSTIVE_SCOPE_SUFFIX,
    TARGETED_SCOPE_SUFFIX,
    WITHHELD_ANSWER,
    DsaItem,
    DsaSourceListResponse,
    EvidenceAcquisitionPremise,
    EvidenceAcquisitionState,
    NextStepResult,
    PlanResult,
    RequirementEvaluation,
    ShapeResult,
    SufficiencyResult,
    _acquisition_premise_digest,
    _build_acquisition_facts,
    _expected_sufficiency_constraints,
    _manifest_id,
    _provider_answer_claims_universal_scope,
    begin_evidence_acquisition,
    bind_manifest_response,
    build_current_acquisition_premise,
    build_manifest_trace,
    compile_safe_exact_fetch_proposal,
    deterministic_clarification_target,
    enforce_final_answer,
    evaluate_acquisition_sufficiency,
    execute_bounded_exhaustive_review,
    execute_exact_fetches,
    execute_hybrid_comparison,
    promote_exact_fetch_proposal,
    provider_allowed,
    retain_initial_attempt_summary,
    select_evidence_next_step,
    suppress_manifest_identifiers,
    validate_bounded_exhaustive_context_pack_response,
    validate_configured_worksheet_response,
    validate_context_pack_response,
    validate_context_response,
    validate_fetch_response,
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
    authority_role=None,
    display_name=None,
    connector="neutral_connector",
    last_error=None,
):
    source = {
        "source_id": source_id,
        "display_name": display_name or f"Source {source_id}",
        "connector": connector,
        "domain_tags": tags or ["records"],
        "sensitivity": "medium",
        "access_mode": "read_only",
        "capabilities": capabilities or ["profile", "search"],
        "enabled": enabled,
        "status": status,
        "last_checked_at": "2026-07-17T00:00:00Z",
        "last_error": last_error,
    }
    if authority_role is not None:
        source["authority_role"] = authority_role
    return source


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


def _exact_plan_response(
    *,
    eligible_source_ids=None,
    authoritative_source_ids=None,
    strategy="exact_fetch",
    status="ready",
):
    authoritative_source_ids = authoritative_source_ids or []
    requirements = [
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
    if authoritative_source_ids:
        requirements.append(
            {
                "requirement_id": "exact-authoritative-fetch",
                "requirement_kind": "exact_authoritative_fetch",
                "criticality": "material",
            }
        )
    response = _plan_response(status=status, requirements=requirements)
    response["result"].update(
        {
            "eligible_source_ids": eligible_source_ids or ["source_a"],
            "authoritative_source_ids": authoritative_source_ids,
            "selected_strategies": [strategy] if strategy else [],
        }
    )
    return response


def _hybrid_shape_response():
    question = "Compare the maintenance history in these two vehicle logs."
    response = _shape_response(shape="cross_source_comparison")
    response["result"].update(
        {
            "question_anchor": question,
            "question_anchor_digest": (
                f"sha256:{hashlib.sha256(question.encode()).hexdigest()}"
            ),
            "reason_codes": [
                "explicit_evidence_language",
                "comparison_requested",
            ],
        }
    )
    return response


def _hybrid_plan_response(
    *,
    eligible_source_ids=None,
    requirements=None,
    task_shape="cross_source_comparison",
    strategy="hybrid",
    completeness="complete_for_selected_sources",
    contradiction_required=False,
    status="ready",
):
    shape = _hybrid_shape_response()["result"]
    return {
        **SCOPE,
        "result": {
            "plan_id": "evidence_plan_hybrid",
            "question_anchor": shape["question_anchor"],
            "question_anchor_digest": shape["question_anchor_digest"],
            "task_shape": task_shape,
            "plan_status": status,
            "completeness_expectation": completeness,
            "contradiction_search_required": contradiction_required,
            "eligible_source_ids": eligible_source_ids
            or ["source_a", "source_b"],
            "authoritative_source_ids": [],
            "selected_strategies": [strategy] if strategy else [],
            "declared_requirements": requirements
            or [
                {
                    "requirement_id": "selected-source-coverage",
                    "requirement_kind": "selected_source_coverage",
                    "criticality": "material",
                },
                {
                    "requirement_id": "cross-source-comparison",
                    "requirement_kind": "cross_source_comparison",
                    "criticality": "material",
                },
                {
                    "requirement_id": "context-delivery",
                    "requirement_kind": "context_delivery",
                    "criticality": "material",
                },
            ],
            "limitation_codes": [],
            "user_safe_summary": "A bounded comparison strategy is available.",
        },
    }


def _exhaustive_shape_response():
    question = "Review every configured worksheet record."
    response = _shape_response(shape="bounded_exhaustive_review")
    response["result"].update(
        {
            "question_anchor": question,
            "question_anchor_digest": (
                f"sha256:{hashlib.sha256(question.encode()).hexdigest()}"
            ),
            "reason_codes": [
                "explicit_evidence_language",
                "exhaustive_scope_requested",
            ],
        }
    )
    return response


def _exhaustive_requirements():
    return [
        {
            "requirement_id": requirement_kind.replace("_", "-"),
            "requirement_kind": requirement_kind,
            "criticality": "material",
        }
        for requirement_kind in (
            "authoritative_inventory",
            "complete_scope_coverage",
            "contradiction_search",
            "context_delivery",
            "no_material_truncation",
        )
    ]


def _exhaustive_plan_response(
    *,
    eligible_source_ids=None,
    authoritative_source_ids=None,
    requirements=None,
    **overrides,
):
    shape = _exhaustive_shape_response()["result"]
    result = {
        "plan_id": "evidence_plan_exhaustive",
        "question_anchor": shape["question_anchor"],
        "question_anchor_digest": shape["question_anchor_digest"],
        "task_shape": "bounded_exhaustive_review",
        "plan_status": "ready",
        "completeness_expectation": "complete_for_declared_scope",
        "contradiction_search_required": True,
        "eligible_source_ids": eligible_source_ids
        if eligible_source_ids is not None
        else ["source_a"],
        "authoritative_source_ids": (
            authoritative_source_ids
            if authoritative_source_ids is not None
            else ["source_a"]
        ),
        "selected_strategies": ["hybrid"],
        "declared_requirements": (
            requirements if requirements is not None else _exhaustive_requirements()
        ),
        "limitation_codes": [],
        "user_safe_summary": "A bounded exhaustive strategy is available.",
    }
    result.update(overrides)
    return {**SCOPE, "result": result}


def _fetch_response(
    *,
    source_id="source_a",
    source_ref="connector:source_a:item-1",
    result=True,
    truncated=False,
):
    results = (
        [
            {
                "result_id": f"result-{source_id}",
                "source_type": "connector",
                "source_id": source_id,
                "source_name": "PRIVATE SOURCE NAME",
                "source_ref": source_ref,
                "retrieved_at": "2026-07-17T00:00:00Z",
                "source_modified_at": None,
                "cache_status": "live",
                "title": "PRIVATE TITLE",
                "content_type": "text",
                "text": f"PRIVATE EXACT CONTENT {source_ref}",
                "url": "https://private.invalid/item",
                "confidence": "high",
                "raw": None,
                "available_context": [
                    {
                        "context_mode": "surrounding",
                        "description": "PRIVATE CONTEXT DESCRIPTION",
                    }
                ],
                "warnings": [],
            }
        ]
        if result
        else []
    )
    return {
        "query_id": f"query-{source_id}",
        "answerable": bool(results),
        "confidence": "low" if results else "none",
        "retrieval_mode": "fetch",
        "results": results,
        "warnings": [],
        "errors": [],
        "budget": {
            "max_results": 1,
            "returned_results": len(results),
            "estimated_bytes": 80 if results else 0,
            "truncated": truncated,
        },
    }


def _context_response(
    *,
    source_id="source_a",
    source_ref=None,
    result=True,
    truncated=False,
):
    source_ref = source_ref or f"connector:{source_id}:expanded-1"
    results = (
        [
            {
                "result_id": f"context-{source_id}",
                "source_type": "connector",
                "source_id": source_id,
                "source_name": f"Source {source_id}",
                "source_ref": source_ref,
                "retrieved_at": "2026-07-17T00:00:00Z",
                "source_modified_at": None,
                "cache_status": "live",
                "title": f"Expanded {source_id}",
                "content_type": "text",
                "text": f"Expanded evidence from {source_id}.",
                "url": "https://private.invalid/context",
                "confidence": "high",
                "raw": None,
                "available_context": [],
                "warnings": [],
            }
        ]
        if result
        else []
    )
    return {
        "query_id": f"context-query-{source_id}",
        "answerable": bool(results),
        "confidence": "low" if results else "none",
        "retrieval_mode": "context",
        "results": results,
        "warnings": [],
        "errors": [],
        "budget": {
            "max_results": None,
            "returned_results": len(results),
            "estimated_bytes": 80 if results else 0,
            "truncated": truncated,
        },
    }


def _configured_worksheet_response(
    *,
    source_id="source_a",
    result=True,
    truncated=False,
    errors=None,
):
    results = (
        [
            {
                "result_id": "configured-worksheet-result",
                "source_type": "google_sheets",
                "source_id": source_id,
                "source_name": "PRIVATE CONFIGURED SOURCE",
                "source_ref": (
                    f"google_sheets:{source_id}:Maintenance!A2:E5"
                ),
                "retrieved_at": "2026-07-17T00:00:00Z",
                "source_modified_at": None,
                "cache_status": "live",
                "title": "PRIVATE CONFIGURED WORKSHEET TITLE",
                "content_type": "spreadsheet_range",
                "text": "PRIVATE COMPLETE CONFIGURED WORKSHEET CONTENT",
                "url": None,
                "confidence": "high",
                "raw": None,
                "available_context": [],
                "warnings": [],
            }
        ]
        if result
        else []
    )
    return {
        "query_id": "configured-worksheet-query",
        "answerable": bool(results),
        "confidence": "high" if results else "none",
        "retrieval_mode": "context",
        "results": results,
        "warnings": [],
        "errors": errors or [],
        "budget": {
            "max_results": 1,
            "returned_results": len(results),
            "estimated_bytes": 240 if results else 0,
            "truncated": truncated,
        },
    }


def _sufficiency_response(
    manifest_id,
    *,
    status="sufficient_for_declared_scope",
    requirements=None,
    task_shape="targeted_lookup",
    evidence_plan_id="evidence_plan_1",
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
    constraints = _expected_sufficiency_constraints(
        status,
        task_shape=task_shape,
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
        "evidence_plan_id": evidence_plan_id,
        "acquisition_manifest_id": manifest_id,
        "result": {
            "evaluation_id": "evidence_eval_1",
            "task_shape": task_shape,
            "sufficiency_status": status,
            "evaluated_requirements": evaluations,
            "reason_codes": reasons,
            "answer_constraints": constraints,
            "qualification_required": status != "sufficient_for_declared_scope",
            "additional_acquisition_required": status in {"insufficient", "unknown"},
            "user_safe_summary": "Bounded sufficiency.",
        },
    }


@pytest.mark.parametrize(
    ("status", "task_shape", "expected"),
    [
        ("sufficient_for_declared_scope", "bounded_exhaustive_review", []),
        (
            "sufficient_with_limitations",
            "contradiction_review",
            [
                "qualify_conclusion",
                "disclose_limitations",
                "identify_unexamined_scope",
            ],
        ),
        (
            "insufficient",
            "targeted_lookup",
            [
                "qualify_conclusion",
                "disclose_limitations",
                "identify_unexamined_scope",
                "additional_acquisition_or_clarification_required",
                "withhold_unqualified_conclusion",
            ],
        ),
        (
            "unknown",
            "cross_source_comparison",
            [
                "qualify_conclusion",
                "disclose_limitations",
                "identify_unexamined_scope",
                "additional_acquisition_or_clarification_required",
                "withhold_unqualified_conclusion",
            ],
        ),
        *[
            (
                status,
                task_shape,
                [
                    "qualify_conclusion",
                    "disclose_limitations",
                    "identify_unexamined_scope",
                    "additional_acquisition_or_clarification_required",
                    "withhold_unqualified_conclusion",
                    constraint,
                ],
            )
            for status in ("insufficient", "unknown")
            for task_shape, constraint in (
                ("bounded_exhaustive_review", "withhold_exhaustive_conclusion"),
                ("absence_or_coverage_check", "withhold_absence_conclusion"),
                (
                    "contradiction_review",
                    "withhold_contradiction_sensitive_conclusion",
                ),
            )
        ],
    ],
)
def test_expected_sufficiency_constraints_are_exact_and_task_specific(
    status,
    task_shape,
    expected,
):
    assert _expected_sufficiency_constraints(
        status,
        task_shape=task_shape,
    ) == expected


def _rendering_state(
    *,
    task_shape="targeted_lookup",
    status="sufficient_for_declared_scope",
    evaluations=None,
    limitation_codes=None,
    inventory=None,
    declared_scope=None,
):
    evaluations = evaluations or [
        {
            "requirement_id": "targeted-evidence",
            "requirement_kind": "targeted_evidence",
            "criticality": "material",
            "effective_outcome": "satisfied",
        }
    ]
    requirements = [
        {
            "requirement_id": evaluation["requirement_id"],
            "requirement_kind": evaluation["requirement_kind"],
            "criticality": evaluation["criticality"],
        }
        for evaluation in evaluations
    ]
    plan_data = _plan_response(
        status=(
            "ready_with_limitations"
            if status == "sufficient_with_limitations"
            else "ready"
        ),
        requirements=requirements,
        limitations=limitation_codes or [],
    )["result"]
    plan_data["task_shape"] = task_shape
    response = _sufficiency_response(
        "evidence_manifest_0123456789abcdef0123456789abcdef",
        status=status,
        requirements=requirements,
        task_shape=task_shape,
    )["result"]
    response["evaluated_requirements"] = evaluations
    return EvidenceAcquisitionState(
        enabled=True,
        attempted=True,
        status=status,
        inventory=(
            DsaSourceListResponse.model_validate(inventory)
            if inventory is not None
            else None
        ),
        declared_scope=declared_scope,
        plan=PlanResult.model_validate(plan_data),
        manifest_id="evidence_manifest_0123456789abcdef0123456789abcdef",
        sufficiency=SufficiencyResult.model_validate(response),
        forced_answer=(
            WITHHELD_ANSWER if status in {"insufficient", "unknown"} else None
        ),
    )


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
            task_shape=kwargs["task_shape"],
            evidence_plan_id=kwargs["evidence_plan_id"],
        )


class FakeDsa:
    def __init__(
        self,
        sources,
        *,
        inventory_metadata=None,
        source_response=None,
        fetch_responses=None,
        context_responses=None,
    ):
        self.sources = sources
        self.calls = []
        self.inventory_metadata = dict(inventory_metadata or {})
        self.source_response = source_response
        self.fetch_responses = list(fetch_responses or [])
        self.context_responses = list(context_responses or [])

    async def list_sources(self):
        self.calls.append("list_sources")
        if self.source_response is not None:
            return copy.deepcopy(self.source_response)
        return {
            **self.inventory_metadata,
            "sources": copy.deepcopy(self.sources),
        }

    async def fetch_source(self, **kwargs):
        self.calls.append(("fetch_source", kwargs))
        response = self.fetch_responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def context_source(self, **kwargs):
        self.calls.append(("context_source", kwargs))
        response = self.context_responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


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


def _chat_request_with_exact_refs(
    references,
    *,
    source_ids=None,
    external_context_enabled=True,
    nested_enabled=True,
):
    return {
        "owner_id": "owner",
        "surface": "dev",
        "messages": [{"role": "user", "content": QUESTION}],
        "external_context_enabled": external_context_enabled,
        "external_context": {
            "enabled": nested_enabled,
            "source_ids": source_ids,
            "exact_source_refs": references,
        },
    }


def test_exact_reference_public_contract_accepts_bounded_opaque_references():
    request = ChatRequest.model_validate(
        _chat_request_with_exact_refs(
            [
                {
                    "source_id": "source_a",
                    "source_ref": "connector:source_a:item-1",
                }
            ],
            source_ids=["source_a"],
        )
    )
    assert request.external_context is not None
    assert request.external_context.exact_source_refs is not None
    assert request.external_context.exact_source_refs[0].source_ref == (
        "connector:source_a:item-1"
    )
    assert request.model_dump()["external_context"]["exact_source_refs"] == [
        {
            "source_id": "source_a",
            "source_ref": "connector:source_a:item-1",
        }
    ]
    ordinary = ChatRequest.model_validate(
        {
            "owner_id": "owner",
            "surface": "dev",
            "messages": [{"role": "user", "content": QUESTION}],
            "external_context": {"enabled": True},
        }
    )
    assert "exact_source_refs" not in ordinary.model_dump()["external_context"]


def test_public_external_context_cannot_declare_inventory_trust():
    payload = _chat_request_with_exact_refs(
        [
            {
                "source_id": "source_a",
                "source_ref": "connector:source_a:item-1",
            }
        ],
        source_ids=["source_a"],
    )
    payload["external_context"].update(
        {
            "authority_role": "authoritative",
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
        }
    )

    request = ChatRequest.model_validate(payload)

    assert request.external_context is not None
    serialized = request.external_context.model_dump()
    assert "authority_role" not in serialized
    assert "inventory_scope" not in serialized
    assert "inventory_status" not in serialized


@pytest.mark.parametrize(
    "references",
    [
        [
            {
                "source_id": "source_a",
                "source_ref": "connector:source_a:item-1",
                "metadata": "private",
            }
        ],
        [{"source_id": "source_a", "source_ref": ""}],
        [{"source_id": "source_a", "source_ref": "has whitespace"}],
        [{"source_id": "source_a", "source_ref": "https://private.invalid/item"}],
        [{"source_id": "source_a", "source_ref": "opaque?token=private"}],
        [{"source_id": "source_a", "source_ref": "x" * 241}],
        [
            {"source_id": "source_a", "source_ref": "connector:source_a:item-1"},
            {"source_id": "source_a", "source_ref": "connector:source_a:item-1"},
        ],
        [
            {
                "source_id": "source_a",
                "source_ref": f"connector:source_a:item-{index}",
            }
            for index in range(17)
        ],
    ],
    ids=[
        "extra-field",
        "blank",
        "whitespace",
        "url",
        "query-string",
        "overlong",
        "duplicate",
        "over-limit",
    ],
)
def test_exact_reference_public_contract_rejects_unsafe_values(references):
    with pytest.raises(ValidationError):
        ChatRequest.model_validate(_chat_request_with_exact_refs(references))


def test_exact_reference_public_contract_rejects_scope_and_opt_in_mismatch():
    reference = {
        "source_id": "source_a",
        "source_ref": "connector:source_a:item-1",
    }
    for request in (
        _chat_request_with_exact_refs([reference], source_ids=["source_b"]),
        _chat_request_with_exact_refs(
            [reference],
            external_context_enabled=False,
        ),
        _chat_request_with_exact_refs([reference], nested_enabled=False),
    ):
        with pytest.raises(ValidationError):
            ChatRequest.model_validate(request)


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
        "unknown"
    )


class ProducerContractSufficiencyRuntime:
    def __init__(self, constraint_mutation=None):
        self.calls = []
        self.constraint_mutation = constraint_mutation

    async def evaluate_evidence_sufficiency(self, **kwargs):
        self.calls.append(kwargs)
        facts = {
            fact["requirement_id"]: fact["outcome"]
            for fact in kwargs["acquisition_facts"]
        }
        evaluations = [
            {
                **requirement,
                "effective_outcome": facts[requirement["requirement_id"]],
            }
            for requirement in kwargs["declared_requirements"]
        ]
        material_outcomes = [
            evaluation["effective_outcome"]
            for evaluation in evaluations
            if evaluation["criticality"] == "material"
        ]
        concrete_failures = {
            "partial",
            "not_attempted",
            "unavailable",
            "unsupported",
            "failed",
            "excluded",
            "filtered",
            "truncated",
            "unresolved_contradiction",
        }
        status = (
            "insufficient"
            if any(outcome in concrete_failures for outcome in material_outcomes)
            else "unknown"
            if any(outcome in {"missing", "unknown"} for outcome in material_outcomes)
            else "sufficient_for_declared_scope"
        )
        constraints = (
            []
            if status == "sufficient_for_declared_scope"
            else [
                "qualify_conclusion",
                "disclose_limitations",
                "identify_unexamined_scope",
                "additional_acquisition_or_clarification_required",
                "withhold_unqualified_conclusion",
            ]
        )
        task_constraint = {
            "bounded_exhaustive_review": "withhold_exhaustive_conclusion",
            "absence_or_coverage_check": "withhold_absence_conclusion",
            "contradiction_review": "withhold_contradiction_sensitive_conclusion",
        }.get(kwargs["task_shape"])
        if task_constraint is not None:
            constraints.append(task_constraint)
        if self.constraint_mutation is not None:
            constraints = self.constraint_mutation(list(constraints))
        return {
            **{
                key: kwargs[key]
                for key in (
                    "request_id",
                    "owner_id",
                    "conversation_id",
                    "surface",
                    "runtime_session_id",
                    "runtime_turn_id",
                    "evidence_plan_id",
                    "acquisition_manifest_id",
                )
            },
            "result": {
                "evaluation_id": "evidence_eval_producer_contract",
                "task_shape": kwargs["task_shape"],
                "sufficiency_status": status,
                "evaluated_requirements": evaluations,
                "reason_codes": [
                    "material_requirement_not_satisfied"
                    if status == "insufficient"
                    else "material_requirement_unknown"
                    if status == "unknown"
                    else "all_declared_requirements_satisfied"
                ],
                "answer_constraints": constraints,
                "qualification_required": status
                != "sufficient_for_declared_scope",
                "additional_acquisition_required": status
                in {"insufficient", "unknown"},
                "user_safe_summary": "Bounded producer response.",
            },
        }


async def _evaluate_filtered_exhaustive_sufficiency(runtime):
    state = _exhaustive_state()
    dsa = FakeDsa([], context_responses=[_configured_worksheet_response()])
    context, trace = await execute_bounded_exhaustive_review(
        state=state,
        dsa=dsa,
        targeted_context_pack=_exhaustive_targeted_context_pack(),
        dsa_trace={"called": True, "status": "success"},
    )
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=context,
        dsa_trace=trace,
        retained_source_refs=set(),
        **SCOPE,
    )
    return state


@pytest.mark.asyncio
async def test_producer_shaped_exhaustive_blocking_constraints_are_accepted():
    runtime = ProducerContractSufficiencyRuntime()

    state = await _evaluate_filtered_exhaustive_sufficiency(runtime)

    assert len(runtime.calls) == 1
    assert state.status == "insufficient"
    assert state.sufficiency is not None
    assert state.sufficiency.evaluation_id == "evidence_eval_producer_contract"
    assert state.sufficiency.sufficiency_status == "insufficient"
    assert state.sufficiency.answer_constraints == [
        "qualify_conclusion",
        "disclose_limitations",
        "identify_unexamined_scope",
        "additional_acquisition_or_clarification_required",
        "withhold_unqualified_conclusion",
        "withhold_exhaustive_conclusion",
    ]
    assert [
        evaluation.model_dump(mode="json")
        for evaluation in state.sufficiency.evaluated_requirements
    ] == [
        {
            **requirement,
            "effective_outcome": next(
                fact["outcome"]
                for fact in state.acquisition_facts
                if fact["requirement_id"] == requirement["requirement_id"]
            ),
        }
        for requirement in runtime.calls[0]["declared_requirements"]
    ]
    assert runtime.calls[0]["acquisition_facts"] == state.acquisition_facts
    assert state.forced_answer == WITHHELD_ANSWER


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "constraint_mutation",
    [
        lambda constraints: constraints[:-1],
        lambda constraints: [
            *constraints[:-1],
            "withhold_absence_conclusion",
        ],
        lambda constraints: [constraints[1], constraints[0], *constraints[2:]],
        lambda constraints: [*constraints, "withhold_absence_conclusion"],
    ],
    ids=["missing", "wrong", "reordered", "extra"],
)
async def test_malformed_exhaustive_constraints_fail_closed(constraint_mutation):
    state = await _evaluate_filtered_exhaustive_sufficiency(
        ProducerContractSufficiencyRuntime(constraint_mutation)
    )

    assert state.status == "sufficiency_dependency_failed"
    assert state.sufficiency is None
    assert state.forced_answer == WITHHELD_ANSWER


@pytest.mark.asyncio
async def test_specialized_constraint_on_targeted_lookup_fails_closed():
    setup_runtime = FakeRuntime(sufficiency_status="insufficient")
    state = await begin_evidence_acquisition(
        runtime=setup_runtime,
        dsa=FakeDsa([_source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )
    runtime = ProducerContractSufficiencyRuntime(
        lambda constraints: [*constraints, "withhold_exhaustive_conclusion"]
    )

    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=_validated_context_pack(),
        dsa_trace={"status": "success", "called": True},
        retained_source_refs=set(),
        **SCOPE,
    )

    assert state.status == "sufficiency_dependency_failed"
    assert state.sufficiency is None
    assert provider_allowed(state) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("producer_status", "runtime_status"),
    [
        ("complete", "complete_for_declared_scope"),
        ("partial", "partial"),
        ("unknown", "unknown"),
        ("unavailable", "unavailable"),
    ],
)
async def test_trusted_inventory_status_maps_exactly(
    producer_status,
    runtime_status,
):
    runtime = FakeRuntime()
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [
                _source(
                    "source_a",
                    authority_role="authoritative",
                )
            ],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": producer_status,
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.status == "acquisition_ready"
    plan_payload = runtime.calls[1][1]
    assert plan_payload["declared_scope"]["inventory_status"] == runtime_status
    assert plan_payload["source_inventory"][0]["authority_role"] == "authoritative"


@pytest.mark.asyncio
async def test_trusted_authority_and_scope_filters_reach_plan_without_inference():
    runtime = FakeRuntime()
    sources = [
        _source(
            "source_supplemental",
            tags=["records", "secondary"],
            authority_role="supplemental",
        ),
        _source(
            "source_authoritative",
            tags=["official", "records"],
            authority_role="authoritative",
        ),
        _source(
            "source_unknown",
            tags=["records"],
            authority_role="unknown",
        ),
    ]
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            sources,
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context={
            "source_ids": ["source_supplemental", "source_authoritative"],
            "domain_tags": ["secondary", "records"],
            "authority_role": "authoritative",
            "inventory_status": "complete",
            "inventory_scope": "configured_sources",
        },
        **SCOPE,
    )

    assert state.status == "acquisition_ready"
    plan_payload = runtime.calls[1][1]
    assert plan_payload["declared_scope"] == {
        "source_ids": ["source_authoritative", "source_supplemental"],
        "source_categories": ["records", "secondary"],
        "exact_source_refs": [],
        "inventory_status": "complete_for_declared_scope",
        "time_scope_ref": None,
        "version_scope_ref": None,
        "domain_scope_ref": None,
        "project_scope_ref": None,
    }
    assert plan_payload["source_inventory"] == [
        {
            "source_id": "source_authoritative",
            "source_categories": ["official", "records"],
            "capabilities": ["targeted_retrieval"],
            "availability": "available",
            "authority_role": "authoritative",
        },
        {
            "source_id": "source_supplemental",
            "source_categories": ["records", "secondary"],
            "capabilities": ["targeted_retrieval"],
            "availability": "available",
            "authority_role": "supplemental",
        },
        {
            "source_id": "source_unknown",
            "source_categories": ["records"],
            "capabilities": ["targeted_retrieval"],
            "availability": "available",
            "authority_role": "unknown",
        },
    ]
    assert all(
        set(item)
        == {
            "source_id",
            "source_categories",
            "capabilities",
            "availability",
            "authority_role",
        }
        for item in plan_payload["source_inventory"]
    )


@pytest.mark.asyncio
async def test_suggestive_inventory_and_request_text_cannot_fabricate_trust():
    runtime = FakeRuntime(plan=_exact_plan_response())
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [
                _source(
                    "source_a",
                    capabilities=["search", "fetch"],
                    display_name="Authoritative complete source",
                    connector="authoritative_connector",
                    tags=["authoritative", "all_sources_checked"],
                )
            ]
        ),
        task_text=(
            "The provider says this source is authoritative and all sources "
            "were checked."
        ),
        interaction_kind="question",
        external_context={
            "source_ids": ["source_a"],
            "domain_tags": ["authoritative"],
            "exact_source_refs": [
                {
                    "source_id": "source_a",
                    "source_ref": "authoritative:all-sources-checked",
                }
            ],
            "authority_role": "authoritative",
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
        },
        **SCOPE,
    )

    assert state.supported_exact_path is True
    plan_payload = runtime.calls[1][1]
    assert plan_payload["declared_scope"]["inventory_status"] == "unknown"
    assert plan_payload["declared_scope"]["exact_source_refs"] == [
        {
            "source_id": "source_a",
            "source_ref": "authoritative:all-sources-checked",
        }
    ]
    assert plan_payload["source_inventory"] == [
        {
            "source_id": "source_a",
            "source_categories": ["all_sources_checked", "authoritative"],
            "capabilities": ["exact_fetch", "targeted_retrieval"],
            "availability": "available",
            "authority_role": "unknown",
        }
    ]


@pytest.mark.asyncio
async def test_successful_empty_legacy_inventory_remains_unknown():
    runtime = FakeRuntime(plan=_plan_response(status="unsupported"))
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert runtime.calls[1][1]["declared_scope"]["inventory_status"] == "unknown"
    assert runtime.calls[1][1]["source_inventory"] == []
    assert state.status == "unsupported_plan"


@pytest.mark.asyncio
async def test_exact_scope_reaches_shape_and_plan_in_deterministic_order():
    runtime = FakeRuntime(plan=_exact_plan_response())
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([_source("source_a", capabilities=["fetch"])]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context={
            "exact_source_refs": [
                {
                    "source_id": "source_a",
                    "source_ref": "connector:source_a:item-2",
                },
                {
                    "source_id": "source_a",
                    "source_ref": "connector:source_a:item-1",
                },
            ]
        },
        **SCOPE,
    )

    assert state.supported_exact_path is True
    assert runtime.calls[0][1]["task_context"] == {
        "evidence_input_kinds": ["external_source"],
        "external_verification_required": True,
        "freshness_sensitive": False,
        "high_stakes_accuracy_required": False,
        "continuation_of_prior_evidence_task": False,
        "prior_task_shape": None,
    }
    assert runtime.calls[1][1]["declared_scope"]["exact_source_refs"] == [
        {
            "source_id": "source_a",
            "source_ref": "connector:source_a:item-1",
        },
        {
            "source_id": "source_a",
            "source_ref": "connector:source_a:item-2",
        },
    ]
    assert runtime.calls[1][1]["declared_scope"]["inventory_status"] == "unknown"
    assert runtime.calls[1][1]["source_inventory"][0]["authority_role"] == "unknown"


@pytest.mark.asyncio
async def test_exact_request_not_applicable_and_inconsistent_plans_fail_closed():
    reference_scope = {
        "exact_source_refs": [
            {
                "source_id": "source_a",
                "source_ref": "connector:source_a:item-1",
            }
        ]
    }
    not_applicable = await begin_evidence_acquisition(
        runtime=FakeRuntime(shape=_shape_response(status="not_applicable")),
        dsa=FakeDsa([_source("source_a", capabilities=["fetch"])]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=reference_scope,
        **SCOPE,
    )
    assert not_applicable.follow_existing_path is False
    assert not_applicable.forced_answer is not None

    for plan in (
        _exact_plan_response(strategy="targeted_retrieval"),
        _exact_plan_response(eligible_source_ids=["source_b"]),
        _exact_plan_response(authoritative_source_ids=["source_b"]),
    ):
        state = await begin_evidence_acquisition(
            runtime=FakeRuntime(plan=plan),
            dsa=FakeDsa([_source("source_a", capabilities=["fetch"])]),
            task_text=QUESTION,
            interaction_kind="question",
            external_context=reference_scope,
            **SCOPE,
        )
        assert state.status == "unsupported_plan"
        assert state.supported_governed_path is False
        assert state.forced_answer is not None


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


@pytest.mark.parametrize(
    "response",
    [
        {
            "inventory_scope": "configured_sources",
            "sources": [_source("source_a")],
        },
        {
            "inventory_status": "complete",
            "sources": [_source("source_a")],
        },
        {
            "inventory_scope": None,
            "inventory_status": None,
            "sources": [_source("source_a")],
        },
        {
            "inventory_scope": "https://private.invalid/sources",
            "inventory_status": "complete",
            "sources": [_source("source_a")],
        },
        {
            "inventory_scope": "configured_sources",
            "inventory_status": "complete_for_everything",
            "sources": [_source("source_a")],
        },
        {
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
            "sources": [_source("source_a", authority_role="owner_declared")],
        },
        {
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
            "sources": [
                {
                    **_source(
                        "source_a",
                        authority_role="authoritative",
                    ),
                    "connector_config": {"credential_ref": "PRIVATE CREDENTIAL"},
                }
            ],
        },
        {
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
            "sources": [_source("source_a")],
            "inventory_metadata": {"raw": "PRIVATE INVENTORY"},
        },
    ],
    ids=[
        "scope-only",
        "status-only",
        "explicit-null-pair",
        "unsupported-scope",
        "invalid-status",
        "invalid-authority",
        "source-extra",
        "top-level-extra",
    ],
)
def test_inventory_trust_metadata_is_strict(response):
    with pytest.raises(ValidationError):
        DsaSourceListResponse.model_validate(response)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_response",
    [
        {
            "inventory_scope": "configured_sources",
            "sources": [_source("source_a")],
        },
        {
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
            "sources": [
                {
                    **_source("source_a"),
                    "private_config": {
                        "url": "https://private.invalid",
                        "credential": "PRIVATE CREDENTIAL",
                        "content": "PRIVATE SOURCE CONTENT",
                    },
                }
            ],
        },
    ],
    ids=["incomplete-metadata", "unbounded-source-metadata"],
)
async def test_malformed_inventory_metadata_uses_bounded_dependency_failure(
    source_response,
):
    runtime = FakeRuntime()
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([], source_response=source_response),
        task_text="PRIVATE PROMPT CONTENT",
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.status == "inventory_dependency_failed"
    assert state.forced_answer is not None
    assert provider_allowed(state) is False
    assert [name for name, _ in runtime.calls] == ["shape"]
    trace = build_manifest_trace(
        state=state,
        context_pack=None,
        dsa_trace=None,
        retained_source_refs=None,
    )
    serialized = json.dumps(trace, sort_keys=True)
    for prohibited in (
        "PRIVATE PROMPT CONTENT",
        "PRIVATE CREDENTIAL",
        "PRIVATE SOURCE CONTENT",
        "private.invalid",
        "private_config",
    ):
        assert prohibited not in serialized


@pytest.mark.asyncio
async def test_planning_and_trace_keep_only_bounded_inventory_projection():
    runtime = FakeRuntime()
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [
                _source(
                    "source_a",
                    authority_role="supplemental",
                    display_name="PRIVATE DISPLAY NAME",
                    connector="private_connector",
                    last_error="PRIVATE HEALTH ERROR",
                )
            ],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "partial",
            },
        ),
        task_text="PRIVATE PROMPT CONTENT",
        interaction_kind="question",
        external_context={"source_ids": ["source_a"]},
        **SCOPE,
    )

    plan_payload = runtime.calls[1][1]
    assert plan_payload["source_inventory"] == [
        {
            "source_id": "source_a",
            "source_categories": ["records"],
            "capabilities": ["targeted_retrieval"],
            "availability": "available",
            "authority_role": "supplemental",
        }
    ]
    trace = build_manifest_trace(
        state=state,
        context_pack=None,
        dsa_trace=None,
        retained_source_refs=None,
    )
    serialized = json.dumps((plan_payload, trace), sort_keys=True)
    for prohibited in (
        "PRIVATE DISPLAY NAME",
        "private_connector",
        "PRIVATE HEALTH ERROR",
        "PRIVATE PROMPT CONTENT",
        "credentials",
        "connector_config",
    ):
        assert prohibited not in serialized


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


def test_context_pack_contract_accepts_legacy_and_explicit_empty_descriptors():
    legacy_item = DsaItem.model_validate(_context_pack()["items"][0])
    legacy = _validated_context_pack()
    explicit_empty_response = copy.deepcopy(_context_pack())
    explicit_empty_response["items"][0]["available_context"] = []
    explicit_empty_item = DsaItem.model_validate(
        explicit_empty_response["items"][0]
    )
    explicit_empty = _validated_context_pack(explicit_empty_response)

    assert legacy_item.available_context == []
    assert explicit_empty_item.available_context == []
    assert legacy == explicit_empty
    assert "available_context" not in legacy["items"][0]
    assert "available_context" not in explicit_empty["items"][0]


def test_context_pack_contract_validates_descriptor_order_then_removes_descriptors():
    response = copy.deepcopy(_context_pack())
    response["items"][0]["available_context"] = [
        {
            "context_mode": "nearby_rows",
            "description": "Fetch nearby rows.",
        },
        {
            "context_mode": "following",
            "description": "Fetch following context.",
        },
    ]

    validated_item = DsaItem.model_validate(response["items"][0])
    normalized = _validated_context_pack(response)

    assert [
        descriptor.context_mode
        for descriptor in validated_item.available_context
    ] == ["nearby_rows", "following"]
    assert "available_context" not in normalized["items"][0]
    assert normalized == _validated_context_pack()


def test_context_pack_contract_preserves_descriptors_only_when_requested():
    response = copy.deepcopy(_context_pack())
    response["items"][0]["available_context"] = [
        {
            "context_mode": "nearby_rows",
            "description": "Fetch nearby rows.",
        },
        {
            "context_mode": "following",
            "description": "Fetch following context.",
        },
    ]

    normalized = validate_context_pack_response(
        response,
        expected_query=QUESTION,
        eligible_source_ids=["source_a"],
        preserve_available_context=True,
        require_all_eligible_sources=True,
    )

    assert normalized["items"][0]["available_context"] == response["items"][0][
        "available_context"
    ]
    assert _validated_context_pack(response)["items"][0].get(
        "available_context"
    ) is None


def _exhaustive_state(
    *,
    sources=None,
    plan_overrides=None,
    requirements=None,
    inventory_metadata=None,
    declared_source_ids=None,
    declared_categories=None,
    exact_source_refs=None,
):
    configured_sources = sources or [
        _source(
            "source_a",
            capabilities=["profile", "search", "context"],
            authority_role="authoritative",
            connector="google_sheets",
        )
    ]
    inventory = DsaSourceListResponse.model_validate(
        {
            **(
                inventory_metadata
                if inventory_metadata is not None
                else {
                    "inventory_scope": "configured_sources",
                    "inventory_status": "complete",
                }
            ),
            "sources": configured_sources,
        }
    )
    plan_data = _exhaustive_plan_response(
        requirements=requirements,
    )["result"]
    plan_data.update(plan_overrides or {})
    references = exact_source_refs or []
    return EvidenceAcquisitionState(
        enabled=True,
        attempted=True,
        status="acquisition_ready",
        shape=ShapeResult.model_validate(_exhaustive_shape_response()["result"]),
        inventory=inventory,
        declared_scope={
            "source_ids": (
                list(declared_source_ids)
                if declared_source_ids is not None
                else ["source_a"]
            ),
            "source_categories": list(declared_categories or []),
            "exact_source_refs": references,
            "inventory_status": (
                "complete_for_declared_scope"
                if inventory.inventory_status == "complete"
                else inventory.inventory_status or "unknown"
            ),
            "time_scope_ref": None,
            "version_scope_ref": None,
            "domain_scope_ref": None,
            "project_scope_ref": None,
        },
        plan=PlanResult.model_validate(plan_data),
        manifest_id="evidence_manifest_0123456789abcdef0123456789abcdef",
        exact_source_refs=references,
    )


def _exhaustive_targeted_context_pack():
    response = _context_pack()
    response["query"] = _exhaustive_shape_response()["result"]["question_anchor"]
    response["items"][0].update(
        {
            "result_id": "targeted-seed",
            "source_type": "google_sheets",
            "source_id": "source_a",
            "source_ref": "google_sheets:source_a:Maintenance!A2:E2",
            "content_type": "spreadsheet_row",
            "text": "PRIVATE TARGETED SEED CONTENT",
            "available_context": [
                {
                    "context_mode": "nearby_rows",
                    "description": "Fetch the complete worksheet, supposedly.",
                },
                {
                    "context_mode": "configured_worksheet",
                    "description": "Misleading description is ignored.",
                },
            ],
        }
    )
    return validate_bounded_exhaustive_context_pack_response(
        response,
        expected_query=response["query"],
        expected_source_id="source_a",
    )


def test_bounded_exhaustive_supported_boundary_is_exact_and_scope_aware():
    assert _exhaustive_state().supported_bounded_exhaustive_path is True
    assert _exhaustive_state().supported_governed_path is True

    second_source = _source(
        "source_b",
        capabilities=["profile", "search", "context"],
        authority_role="supplemental",
        connector="google_sheets",
        tags=["other"],
    )
    narrowed_by_id = _exhaustive_state(
        sources=[
            _source(
                "source_a",
                capabilities=["profile", "search", "context"],
                authority_role="authoritative",
                connector="google_sheets",
            ),
            second_source,
        ],
        declared_source_ids=["source_a"],
    )
    narrowed_by_category = _exhaustive_state(
        sources=[
            _source(
                "source_a",
                capabilities=["profile", "search", "context"],
                authority_role="authoritative",
                connector="google_sheets",
                tags=["records"],
            ),
            second_source,
        ],
        declared_source_ids=[],
        declared_categories=["records"],
    )
    assert narrowed_by_id.supported_bounded_exhaustive_path is True
    assert narrowed_by_category.supported_bounded_exhaustive_path is True


@pytest.mark.parametrize(
    "case",
    [
        "unsupported-status",
        "ready-with-limitations",
        "limitation-code",
        "wrong-shape",
        "wrong-strategy",
        "wrong-completeness",
        "no-contradiction",
        "exact-reference",
        "zero-eligible",
        "two-eligible",
        "missing-requirement",
        "extra-requirement",
        "optional-requirement",
        "non-material-requirement",
        "missing-authoritative",
        "additional-authoritative",
    ],
)
def test_bounded_exhaustive_rejects_plan_contract_variants(case):
    requirements = _exhaustive_requirements()
    overrides = {}
    exact_source_refs = None
    if case == "unsupported-status":
        overrides["plan_status"] = "unsupported"
    elif case == "ready-with-limitations":
        overrides.update(
            {
                "plan_status": "ready_with_limitations",
                "limitation_codes": ["optional_source_unavailable"],
            }
        )
        requirements.append(
            {
                "requirement_id": "optional-selected-source-coverage",
                "requirement_kind": "selected_source_coverage",
                "criticality": "optional",
            }
        )
    elif case == "limitation-code":
        overrides["limitation_codes"] = ["required_capability_unavailable"]
    elif case == "wrong-shape":
        overrides["task_shape"] = "contradiction_review"
    elif case == "wrong-strategy":
        overrides["selected_strategies"] = ["bounded_full_context"]
    elif case == "wrong-completeness":
        overrides["completeness_expectation"] = "complete_for_selected_sources"
    elif case == "no-contradiction":
        overrides["contradiction_search_required"] = False
    elif case == "exact-reference":
        exact_source_refs = [
            {
                "source_id": "source_a",
                "source_ref": "google_sheets:source_a:Maintenance!A2:E2",
            }
        ]
    elif case == "zero-eligible":
        overrides["eligible_source_ids"] = []
        overrides["authoritative_source_ids"] = []
    elif case == "two-eligible":
        overrides["eligible_source_ids"] = ["source_a", "source_b"]
        overrides["authoritative_source_ids"] = ["source_a", "source_b"]
    elif case == "missing-requirement":
        requirements.pop()
    elif case == "extra-requirement":
        requirements.append(
            {
                "requirement_id": "targeted-evidence",
                "requirement_kind": "targeted_evidence",
                "criticality": "material",
            }
        )
    elif case == "optional-requirement":
        requirements[-1]["criticality"] = "optional"
    elif case == "non-material-requirement":
        requirements[0]["criticality"] = "optional"
    elif case == "missing-authoritative":
        overrides["authoritative_source_ids"] = []
    else:
        overrides["authoritative_source_ids"] = ["source_a", "source_b"]

    state = _exhaustive_state(
        plan_overrides=overrides,
        requirements=requirements,
        exact_source_refs=exact_source_refs,
    )
    assert state.supported_bounded_exhaustive_path is False


@pytest.mark.parametrize(
    ("case", "source_overrides"),
    [
        ("disabled", {"enabled": False, "status": "disabled"}),
        ("unavailable", {"status": "unavailable"}),
        ("unknown-status", {"status": "unknown"}),
        ("supplemental", {"authority_role": "supplemental"}),
        ("unknown-authority", {"authority_role": "unknown"}),
        ("wrong-connector", {"connector": "ics_calendar"}),
        ("missing-search", {"capabilities": ["profile", "context"]}),
        ("missing-context", {"capabilities": ["profile", "search"]}),
    ],
)
def test_bounded_exhaustive_rejects_untrusted_or_incapable_source(
    case,
    source_overrides,
):
    source_config = {
        "capabilities": ["profile", "search", "context"],
        "enabled": True,
        "status": "ready",
        "authority_role": "authoritative",
        "connector": "google_sheets",
    }
    source_config.update(source_overrides)
    source = _source(
        "source_a",
        capabilities=source_config["capabilities"],
        enabled=source_config["enabled"],
        status=source_config["status"],
        authority_role=source_config["authority_role"],
        connector=source_config["connector"],
        display_name="Authoritative complete official records",
        tags=["official"],
    )
    assert (
        _exhaustive_state(sources=[source]).supported_bounded_exhaustive_path
        is False
    ), case


@pytest.mark.parametrize(
    "inventory_metadata",
    [
        {},
        {
            "inventory_scope": "configured_sources",
            "inventory_status": "partial",
        },
        {
            "inventory_scope": "configured_sources",
            "inventory_status": "unknown",
        },
        {
            "inventory_scope": "configured_sources",
            "inventory_status": "unavailable",
        },
    ],
)
def test_bounded_exhaustive_rejects_untrusted_inventory_states(
    inventory_metadata,
):
    assert (
        _exhaustive_state(
            inventory_metadata=inventory_metadata
        ).supported_bounded_exhaustive_path
        is False
    )


def test_bounded_exhaustive_rejects_malformed_inventory_and_wider_universe():
    for metadata in (
        {"inventory_scope": "configured_sources"},
        {"inventory_status": "complete"},
        {
            "inventory_scope": None,
            "inventory_status": "complete",
        },
    ):
        with pytest.raises(ValidationError):
            DsaSourceListResponse.model_validate(
                {
                    **metadata,
                    "sources": [],
                }
            )

    second_source = _source(
        "source_b",
        capabilities=["profile", "search", "context"],
        authority_role="authoritative",
        connector="google_sheets",
    )
    assert (
        _exhaustive_state(
            sources=[
                _source(
                    "source_a",
                    capabilities=["profile", "search", "context"],
                    authority_role="authoritative",
                    connector="google_sheets",
                ),
                second_source,
            ],
            declared_source_ids=[],
        ).supported_bounded_exhaustive_path
        is False
    )
    assert (
        _exhaustive_state(
            declared_source_ids=["source_a", "missing_authoritative_source"]
        ).supported_bounded_exhaustive_path
        is False
    )


def test_bounded_exhaustive_context_pack_requires_exact_seed_association():
    valid = _exhaustive_targeted_context_pack()
    assert valid["items"][0]["available_context"][0]["context_mode"] == (
        "nearby_rows"
    )
    assert valid["items"][0]["available_context"][1]["context_mode"] == (
        "configured_worksheet"
    )

    mutations = []
    for mutation in (
        "errors",
        "missing-items",
        "wrong-count",
        "missing-diagnostics",
        "wrong-considered",
        "wrong-selected",
        "wrong-candidate",
        "wrong-source",
    ):
        response = copy.deepcopy(_context_pack())
        response["query"] = _exhaustive_shape_response()["result"][
            "question_anchor"
        ]
        response["items"][0].update(
            {
                "source_type": "google_sheets",
                "source_id": "source_a",
                "source_ref": "google_sheets:source_a:Maintenance!A2:E2",
                "content_type": "spreadsheet_row",
                "available_context": [],
            }
        )
        if mutation == "errors":
            response["errors"] = [{"code": "bounded_error"}]
        elif mutation == "missing-items":
            response["items"] = []
            response["sources_used"] = []
            response["budget"]["returned_results"] = 0
            response["diagnostics"]["selected_source_ids"] = []
            response["diagnostics"]["candidate_counts_by_source"] = {}
        elif mutation == "wrong-count":
            response["budget"]["returned_results"] = 0
        elif mutation == "missing-diagnostics":
            response["diagnostics"] = None
        elif mutation == "wrong-considered":
            response["diagnostics"]["considered_source_ids"] = []
        elif mutation == "wrong-selected":
            response["diagnostics"]["selected_source_ids"] = []
        elif mutation == "wrong-candidate":
            response["diagnostics"]["candidate_counts_by_source"] = {}
        else:
            response["items"][0]["source_id"] = "source_b"
        mutations.append(response)

    for response in mutations:
        with pytest.raises((ValidationError, ValueError)):
            validate_bounded_exhaustive_context_pack_response(
                response,
                expected_query=response["query"],
                expected_source_id="source_a",
            )


@pytest.mark.parametrize(
    ("mutation", "expected_outcome"),
    [
        ("valid", "satisfied"),
        ("empty", "unknown"),
        ("truncated", "truncated"),
        ("errors", "failed"),
        ("multiple", "filtered"),
        ("wrong-source", "filtered"),
        ("wrong-type", "filtered"),
        ("wrong-content", "filtered"),
        ("url", "filtered"),
        ("recursive-context", "filtered"),
    ],
)
def test_configured_worksheet_response_has_a_dedicated_strict_contract(
    mutation,
    expected_outcome,
):
    response = _configured_worksheet_response()
    if mutation == "empty":
        response = _configured_worksheet_response(result=False)
    elif mutation == "truncated":
        response["budget"]["truncated"] = True
    elif mutation == "errors":
        response = _configured_worksheet_response(
            result=False,
            errors=[{"code": "bounded_dependency_error"}],
        )
    elif mutation == "multiple":
        second = copy.deepcopy(response["results"][0])
        second["result_id"] = "configured-worksheet-result-2"
        second["source_ref"] = "google_sheets:source_a:Maintenance!A2:E6"
        response["results"].append(second)
        response["budget"]["returned_results"] = 2
    elif mutation == "wrong-source":
        response["results"][0]["source_id"] = "source_b"
    elif mutation == "wrong-type":
        response["results"][0]["source_type"] = "neutral_connector"
    elif mutation == "wrong-content":
        response["results"][0]["content_type"] = "spreadsheet_row"
    elif mutation == "url":
        response["results"][0]["url"] = "https://private.invalid/sheet"
    elif mutation == "recursive-context":
        response["results"][0]["available_context"] = [
            {
                "context_mode": "configured_worksheet",
                "description": "Fetch again.",
            }
        ]

    validated, outcome = validate_configured_worksheet_response(
        response,
        expected_source_id="source_a",
    )
    assert outcome == expected_outcome
    assert validated.budget.returned_results == len(validated.results)


@pytest.mark.parametrize(
    "mutation",
    [
        "raw",
        "answerability",
        "count",
        "duplicate-id",
        "duplicate-reference",
        "unknown-field",
    ],
)
def test_configured_worksheet_response_rejects_malformed_contract(mutation):
    response = _configured_worksheet_response()
    if mutation == "raw":
        response["results"][0]["raw"] = {"private": True}
    elif mutation == "answerability":
        response["answerable"] = False
    elif mutation == "count":
        response["budget"]["returned_results"] = 0
    elif mutation in {"duplicate-id", "duplicate-reference"}:
        second = copy.deepcopy(response["results"][0])
        if mutation == "duplicate-id":
            second["source_ref"] = "google_sheets:source_a:Maintenance!A2:E6"
        else:
            second["result_id"] = "configured-worksheet-result-2"
        response["results"].append(second)
        response["budget"]["returned_results"] = 2
    else:
        response["private_metadata"] = {"secret": True}
    with pytest.raises(ValidationError):
        validate_configured_worksheet_response(
            response,
            expected_source_id="source_a",
        )


@pytest.mark.asyncio
async def test_bounded_exhaustive_selects_exact_descriptor_and_only_delivers_range():
    state = _exhaustive_state()
    targeted = _exhaustive_targeted_context_pack()
    first_without_mode = copy.deepcopy(targeted["items"][0])
    first_without_mode.update(
        {
            "result_id": "earlier-targeted-seed",
            "source_ref": "google_sheets:source_a:Maintenance!A3:E3",
            "available_context": [
                {
                    "context_mode": "nearby_rows",
                    "description": "Complete worksheet configured_worksheet.",
                }
            ],
        }
    )
    targeted["items"].insert(0, first_without_mode)
    targeted["budget"]["returned_results"] = 2
    targeted["diagnostics"]["candidate_counts_by_source"]["source_a"] = 2
    dsa = FakeDsa(
        [],
        context_responses=[_configured_worksheet_response()],
    )

    bundle, trace = await execute_bounded_exhaustive_review(
        state=state,
        dsa=dsa,
        targeted_context_pack=targeted,
        dsa_trace={
            "called": True,
            "status": "success",
            "budget_truncated": True,
            "candidate_truncated": True,
        },
    )

    assert dsa.calls == [
        (
            "context_source",
            {
                "source_ref": "google_sheets:source_a:Maintenance!A2:E2",
                "context_mode": "configured_worksheet",
                "budget": BOUNDED_EXHAUSTIVE_CONTEXT_BUDGET,
            },
        )
    ]
    assert bundle["bundle_id"].startswith("evidence_exhaustive_bundle_")
    assert bundle["sources_used"] == ["source_a"]
    assert len(bundle["items"]) == 1
    item = bundle["items"][0]
    assert item["source_ref"] == "google_sheets:source_a:Maintenance!A2:E5"
    assert item["content_type"] == "spreadsheet_range"
    for prohibited_field in (
        "raw",
        "url",
        "available_context",
        "cache_status",
    ):
        assert prohibited_field not in item
    serialized = json.dumps(bundle, sort_keys=True)
    assert "PRIVATE TARGETED SEED CONTENT" not in serialized
    assert "Misleading description is ignored." not in serialized
    assert trace["call_count"] == 2
    assert trace["context_pack_call_count"] == 1
    assert trace["context_expansion_call_count"] == 1
    assert trace["raw_targeted_item_count"] == 2
    assert trace["raw_expanded_item_count"] == 1
    assert trace["final_combined_item_count"] == 1
    assert trace["search_budget_truncated"] is True
    assert trace["candidate_truncated"] is True
    assert state.expansion_attempts == [
        {
            "source_id": "source_a",
            "seed_source_ref": "google_sheets:source_a:Maintenance!A2:E2",
            "context_mode": "configured_worksheet",
            "outcome": "satisfied",
            "query_id": "configured-worksheet-query",
            "returned_reference_count": 1,
        }
    ]


@pytest.mark.asyncio
async def test_bounded_exhaustive_missing_exact_descriptor_is_unsupported_without_call():
    state = _exhaustive_state()
    targeted = _exhaustive_targeted_context_pack()
    targeted["items"][0]["available_context"] = [
        {
            "context_mode": "nearby_rows",
            "description": "Fetch every complete worksheet.",
        }
    ]
    dsa = FakeDsa([], context_responses=[])

    bundle, trace = await execute_bounded_exhaustive_review(
        state=state,
        dsa=dsa,
        targeted_context_pack=targeted,
        dsa_trace={"called": True, "status": "success"},
    )

    assert dsa.calls == []
    assert bundle["items"] == []
    assert state.expansion_attempts[0]["outcome"] == "unsupported"
    assert state.expansion_attempts[0]["context_mode"] == (
        CONFIGURED_WORKSHEET_CONTEXT_MODE
    )
    assert trace["call_count"] == 1
    assert trace["context_expansion_call_count"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected_outcome"),
    [
        (_configured_worksheet_response(result=False), "unknown"),
        (_configured_worksheet_response(truncated=True), "truncated"),
        (RuntimeError("PRIVATE DEPENDENCY FAILURE"), "failed"),
        (
            {
                **_configured_worksheet_response(),
                "private_metadata": {"secret": True},
            },
            "filtered",
        ),
    ],
)
async def test_bounded_exhaustive_failures_are_single_attempt_and_provider_safe(
    response,
    expected_outcome,
):
    state = _exhaustive_state()
    dsa = FakeDsa([], context_responses=[copy.deepcopy(response)])
    bundle, trace = await execute_bounded_exhaustive_review(
        state=state,
        dsa=dsa,
        targeted_context_pack=_exhaustive_targeted_context_pack(),
        dsa_trace={"called": True, "status": "success"},
    )
    assert len(dsa.calls) == 1
    assert state.expansion_attempts[0]["outcome"] == expected_outcome
    assert bundle["items"] == []
    assert trace["expansion_attempt_counts"][expected_outcome] == 1
    assert "PRIVATE DEPENDENCY FAILURE" not in json.dumps(
        (bundle, trace),
        sort_keys=True,
    )


def test_bounded_exhaustive_facts_identity_manifest_and_privacy_are_prompt_aware():
    state = _exhaustive_state()
    state.expansion_attempts = [
        {
            "source_id": "source_a",
            "seed_source_ref": "google_sheets:source_a:Maintenance!A2:E2",
            "context_mode": "configured_worksheet",
            "outcome": "satisfied",
            "query_id": "configured-worksheet-query",
            "returned_reference_count": 1,
        }
    ]
    bundle = {
        **_exhaustive_targeted_context_pack(),
        "bundle_id": "evidence_exhaustive_bundle_fixture",
        "sources_used": ["source_a"],
        "items": [
            {
                "result_id": "configured-worksheet-result",
                "source_type": "google_sheets",
                "source_id": "source_a",
                "source_name": "PRIVATE CONFIGURED SOURCE",
                "source_ref": "google_sheets:source_a:Maintenance!A2:E5",
                "retrieved_at": "2026-07-17T00:00:00+00:00",
                "source_modified_at": None,
                "title": "PRIVATE CONFIGURED WORKSHEET TITLE",
                "content_type": "spreadsheet_range",
                "text": "PRIVATE COMPLETE CONFIGURED WORKSHEET CONTENT",
                "confidence": "high",
                "warnings": [],
            }
        ],
        "budget": {
            "max_results": 1,
            "returned_results": 1,
            "estimated_bytes": 240,
            "truncated": False,
        },
        "raw_item_count": 1,
    }
    complete_ref = "google_sheets:source_a:Maintenance!A2:E5"
    satisfied = _build_acquisition_facts(
        plan=state.plan,
        context_pack=bundle,
        dsa_trace={
            "status": "included",
            "search_budget_truncated": True,
            "candidate_truncated": True,
            "expansion_budget_truncated": False,
        },
        retained_source_refs={complete_ref},
        expansion_attempts=state.expansion_attempts,
        bounded_exhaustive_path=True,
    )
    assert {
        item["requirement_id"]: item["outcome"]
        for item in satisfied
    } == {
        "authoritative-inventory": "satisfied",
        "complete-scope-coverage": "satisfied",
        "context-delivery": "satisfied",
        "contradiction-search": "satisfied",
        "no-material-truncation": "satisfied",
    }

    filtered = _build_acquisition_facts(
        plan=state.plan,
        context_pack=bundle,
        dsa_trace={"status": "included"},
        retained_source_refs=set(),
        expansion_attempts=state.expansion_attempts,
        bounded_exhaustive_path=True,
    )
    filtered_by_id = {
        item["requirement_id"]: item["outcome"]
        for item in filtered
    }
    assert filtered_by_id["complete-scope-coverage"] == "satisfied"
    for requirement_id in (
        "context-delivery",
        "contradiction-search",
        "no-material-truncation",
    ):
        assert filtered_by_id[requirement_id] == "filtered"

    unknown = _build_acquisition_facts(
        plan=state.plan,
        context_pack=bundle,
        dsa_trace={"status": "included"},
        retained_source_refs=None,
        expansion_attempts=state.expansion_attempts,
        bounded_exhaustive_path=True,
    )
    unknown_by_id = {
        item["requirement_id"]: item["outcome"]
        for item in unknown
    }
    assert unknown_by_id["complete-scope-coverage"] == "satisfied"
    assert unknown_by_id["context-delivery"] == "unknown"

    identity_retained = _manifest_id(
        scope=SCOPE,
        plan_id=state.plan.plan_id,
        selected_strategies=["hybrid"],
        declared_scope=state.declared_scope,
        expansion_attempts=state.expansion_attempts,
        delivery_identity={
            "returned_source_refs": [complete_ref],
            "retained_source_refs": [complete_ref],
            "retention_status": "satisfied",
        },
    )
    identity_omitted = _manifest_id(
        scope=SCOPE,
        plan_id=state.plan.plan_id,
        selected_strategies=["hybrid"],
        declared_scope=state.declared_scope,
        expansion_attempts=state.expansion_attempts,
        delivery_identity={
            "returned_source_refs": [complete_ref],
            "retained_source_refs": [],
            "retention_status": "filtered",
        },
    )
    assert identity_retained != identity_omitted

    state.acquisition_facts = satisfied
    manifest = build_manifest_trace(
        state=state,
        context_pack=bundle,
        dsa_trace={
            "called": True,
            "status": "included",
            "raw_item_count": 1,
            "raw_targeted_item_count": 1,
            "raw_expanded_item_count": 1,
        },
        retained_source_refs={complete_ref},
    )
    assert manifest["acquisition"]["source_references_returned"] == [
        complete_ref
    ]
    assert manifest["acquisition"]["source_references_retained"] == [
        complete_ref
    ]
    assert manifest["acquisition"]["expansion_attempt_count"] == 1
    assert "PRIVATE COMPLETE" not in json.dumps(manifest, sort_keys=True)
    suppressed = suppress_manifest_identifiers(manifest)
    assert suppressed["acquisition"]["expansion_attempts"] == []
    assert suppressed["acquisition"]["expansion_attempts_count"] == 1
    serialized = json.dumps(suppressed, sort_keys=True)
    for prohibited in (
        "source_a",
        "Maintenance!A2:E2",
        "Maintenance!A2:E5",
        "configured_worksheet",
        "configured-worksheet-query",
    ):
        assert prohibited not in serialized


def _hybrid_state(
    *,
    source_ids=None,
    capabilities=None,
    source_status="ready",
    plan_overrides=None,
    exact_source_refs=None,
):
    source_ids = source_ids or ["source_a", "source_b"]
    inventory = DsaSourceListResponse.model_validate(
        {
            "sources": [
                _source(
                    source_id,
                    capabilities=(
                        capabilities.get(source_id)
                        if isinstance(capabilities, dict)
                        else ["profile", "search", "context"]
                    ),
                    status=source_status,
                )
                for source_id in source_ids
            ]
        }
    )
    plan_data = _hybrid_plan_response(
        eligible_source_ids=source_ids,
    )["result"]
    plan_data.update(plan_overrides or {})
    return EvidenceAcquisitionState(
        enabled=True,
        attempted=True,
        status="acquisition_ready",
        shape=ShapeResult.model_validate(_hybrid_shape_response()["result"]),
        inventory=inventory,
        declared_scope={
            "source_ids": list(source_ids),
            "source_categories": [],
            "exact_source_refs": exact_source_refs or [],
            "inventory_status": "complete_for_declared_scope",
            "time_scope_ref": None,
            "version_scope_ref": None,
            "domain_scope_ref": None,
            "project_scope_ref": None,
        },
        plan=PlanResult.model_validate(plan_data),
        manifest_id="evidence_manifest_0123456789abcdef0123456789abcdef",
        exact_source_refs=exact_source_refs or [],
    )


def test_hybrid_supported_boundary_accepts_only_bounded_comparison():
    assert _hybrid_state().supported_hybrid_comparison_path is True
    limited_requirements = [
        *_hybrid_plan_response()["result"]["declared_requirements"],
        {
            "requirement_id": "optional-selected-source-coverage",
            "requirement_kind": "selected_source_coverage",
            "criticality": "optional",
        },
    ]
    assert (
        _hybrid_state(
            plan_overrides={
                "plan_status": "ready_with_limitations",
                "declared_requirements": limited_requirements,
                "limitation_codes": ["optional_source_unavailable"],
            }
        ).supported_hybrid_comparison_path
        is True
    )

    variants = [
        _hybrid_state(source_ids=["source_a"]),
        _hybrid_state(source_ids=[f"source_{index}" for index in range(9)]),
        _hybrid_state(
            plan_overrides={
                "declared_requirements": _hybrid_plan_response()["result"][
                    "declared_requirements"
                ][1:]
            }
        ),
        _hybrid_state(
            plan_overrides={
                "declared_requirements": [
                    *_hybrid_plan_response()["result"]["declared_requirements"],
                    {
                        "requirement_id": "targeted-evidence",
                        "requirement_kind": "targeted_evidence",
                        "criticality": "material",
                    },
                ]
            }
        ),
        _hybrid_state(
            plan_overrides={
                "task_shape": "bounded_exhaustive_review",
                "completeness_expectation": "complete_for_declared_scope",
                "contradiction_search_required": True,
            }
        ),
        _hybrid_state(
            plan_overrides={"completeness_expectation": "targeted_scope"}
        ),
        _hybrid_state(plan_overrides={"contradiction_search_required": True}),
        _hybrid_state(
            exact_source_refs=[
                {
                    "source_id": "source_a",
                    "source_ref": "connector:source_a:item-1",
                }
            ]
        ),
        _hybrid_state(
            capabilities={
                "source_a": ["profile", "search", "context"],
                "source_b": ["profile", "search"],
            }
        ),
        _hybrid_state(
            capabilities={
                "source_a": ["profile", "search", "context"],
                "source_b": ["profile", "context"],
            }
        ),
        _hybrid_state(source_status="unavailable"),
    ]
    assert all(
        state.supported_hybrid_comparison_path is False
        for state in variants
    )

    duplicate_plan = _hybrid_plan_response()["result"]
    duplicate_plan["eligible_source_ids"] = ["source_a", "source_a"]
    with pytest.raises(ValidationError):
        PlanResult.model_validate(duplicate_plan)


def _targeted_hybrid_context_pack():
    response = _context_pack()
    response["query"] = _hybrid_shape_response()["result"]["question_anchor"]
    response["sources_used"] = ["source_a", "source_b"]
    response["items"] = [
        {
            **response["items"][0],
            "result_id": "target-a",
            "source_id": "source_a",
            "source_ref": "connector:source_a:seed-a",
            "text": "Targeted source A.",
            "available_context": [
                {
                    "context_mode": "nearby_rows",
                    "description": "PRIVATE MODE A DESCRIPTION",
                },
                {
                    "context_mode": "second_mode",
                    "description": "PRIVATE SECOND DESCRIPTION",
                },
            ],
        },
        {
            **response["items"][0],
            "result_id": "target-b",
            "source_id": "source_b",
            "source_ref": "connector:source_b:seed-b",
            "text": "Targeted source B.",
            "available_context": [
                {
                    "context_mode": "upcoming_events",
                    "description": "PRIVATE MODE B DESCRIPTION",
                }
            ],
        },
    ]
    response["budget"]["returned_results"] = 2
    response["diagnostics"].update(
        {
            "considered_source_ids": ["source_a", "source_b"],
            "selected_source_ids": ["source_a", "source_b"],
            "candidate_counts_by_source": {"source_a": 1, "source_b": 1},
        }
    )
    return validate_context_pack_response(
        response,
        expected_query=response["query"],
        eligible_source_ids=["source_a", "source_b"],
        preserve_available_context=True,
        require_all_eligible_sources=True,
    )


def test_context_response_contract_is_strict_and_source_bound():
    valid = validate_context_response(
        _context_response(source_id="source_a"),
        expected_source_id="source_a",
    )
    assert valid.results[0].source_id == "source_a"

    malformed_responses = []
    wrong_source = _context_response(source_id="source_b")
    malformed_responses.append(wrong_source)
    wrong_mode = _context_response()
    wrong_mode["retrieval_mode"] = "fetch"
    malformed_responses.append(wrong_mode)
    wrong_answerability = _context_response()
    wrong_answerability["answerable"] = False
    malformed_responses.append(wrong_answerability)
    wrong_count = _context_response()
    wrong_count["budget"]["returned_results"] = 0
    malformed_responses.append(wrong_count)
    raw = _context_response()
    raw["results"][0]["raw"] = {"private": True}
    malformed_responses.append(raw)
    duplicate = _context_response()
    duplicate["results"].append(copy.deepcopy(duplicate["results"][0]))
    duplicate["budget"]["returned_results"] = 2
    malformed_responses.append(duplicate)
    extra = _context_response()
    extra["private_metadata"] = {"secret": True}
    malformed_responses.append(extra)

    for response in malformed_responses:
        with pytest.raises((ValidationError, ValueError)):
            validate_context_response(
                response,
                expected_source_id="source_a",
            )


@pytest.mark.asyncio
async def test_hybrid_execution_is_stable_bounded_and_deduplicated():
    state = _hybrid_state()
    targeted = _targeted_hybrid_context_pack()
    repeated_seed = _context_response(
        source_id="source_a",
        source_ref="connector:source_a:seed-a",
    )
    dsa = FakeDsa(
        [],
        context_responses=[
            repeated_seed,
            _context_response(source_id="source_b"),
        ],
    )

    combined, trace = await execute_hybrid_comparison(
        state=state,
        dsa=dsa,
        targeted_context_pack=targeted,
        dsa_trace={
            "called": True,
            "status": "success",
            "budget_truncated": False,
            "candidate_truncated": False,
        },
    )

    assert [call[1]["source_ref"] for call in dsa.calls] == [
        "connector:source_a:seed-a",
        "connector:source_b:seed-b",
    ]
    assert [call[1]["context_mode"] for call in dsa.calls] == [
        "nearby_rows",
        "upcoming_events",
    ]
    assert all(
        call[1]["budget"]
        == {
            "max_rows": 5,
            "max_bytes": 50000,
            "max_text_chars": 12000,
        }
        for call in dsa.calls
    )
    assert combined is not None
    assert [item["source_ref"] for item in combined["items"]] == [
        "connector:source_a:seed-a",
        "connector:source_b:seed-b",
        "connector:source_b:expanded-1",
    ]
    assert "available_context" not in combined["items"][0]
    assert "url" not in combined["items"][-1]
    assert "raw" not in combined["items"][-1]
    assert trace["context_pack_call_count"] == 1
    assert trace["context_expansion_call_count"] == 2
    assert trace["call_count"] == 3
    assert trace["raw_targeted_item_count"] == 2
    assert trace["raw_expanded_item_count"] == 2
    assert trace["final_combined_item_count"] == 3
    assert trace["expansion_attempt_counts"]["satisfied"] == 2
    assert trace["search_budget_truncated"] is False
    assert trace["expansion_budget_truncated"] is False
    assert combined["budget"] == {
        "max_results": 5,
        "returned_results": 4,
        "estimated_bytes": 240,
        "truncated": False,
    }


@pytest.mark.asyncio
async def test_hybrid_selects_first_descriptor_bearing_result_and_first_mode():
    state = _hybrid_state()
    targeted = _targeted_hybrid_context_pack()
    no_descriptor = {
        **copy.deepcopy(targeted["items"][0]),
        "result_id": "target-a-without-context",
        "source_ref": "connector:source_a:no-context",
        "available_context": [],
    }
    targeted["items"].insert(0, no_descriptor)
    targeted["budget"]["returned_results"] = 3
    targeted["diagnostics"]["candidate_counts_by_source"]["source_a"] = 2
    targeted = validate_context_pack_response(
        targeted,
        expected_query=targeted["query"],
        eligible_source_ids=["source_a", "source_b"],
        preserve_available_context=True,
        require_all_eligible_sources=True,
    )
    dsa = FakeDsa(
        [],
        context_responses=[
            _context_response(source_id="source_a"),
            _context_response(source_id="source_b"),
        ],
    )

    await execute_hybrid_comparison(
        state=state,
        dsa=dsa,
        targeted_context_pack=targeted,
        dsa_trace={"called": True, "status": "success"},
    )

    assert [call[1]["source_ref"] for call in dsa.calls] == [
        "connector:source_a:seed-a",
        "connector:source_b:seed-b",
    ]
    assert [call[1]["context_mode"] for call in dsa.calls] == [
        "nearby_rows",
        "upcoming_events",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected_outcome"),
    [
        (_context_response(result=False), "unknown"),
        (RuntimeError("PRIVATE DEPENDENCY"), "failed"),
        (
            {
                **_context_response(),
                "retrieval_mode": "fetch",
            },
            "filtered",
        ),
        (_context_response(truncated=True), "truncated"),
    ],
)
async def test_hybrid_execution_records_failures_without_retry(
    response,
    expected_outcome,
):
    state = _hybrid_state()
    targeted = _targeted_hybrid_context_pack()
    dsa = FakeDsa(
        [],
        context_responses=[
            response,
            _context_response(source_id="source_b"),
        ],
    )

    combined, trace = await execute_hybrid_comparison(
        state=state,
        dsa=dsa,
        targeted_context_pack=targeted,
        dsa_trace={"called": True, "status": "success"},
    )

    assert combined is not None
    assert len(dsa.calls) == 2
    assert state.expansion_attempts[0]["outcome"] == expected_outcome
    assert state.expansion_attempts[1]["outcome"] == "satisfied"
    assert trace["expansion_attempt_counts"][expected_outcome] == 1
    assert "PRIVATE DEPENDENCY" not in json.dumps(trace, sort_keys=True)


@pytest.mark.asyncio
async def test_hybrid_missing_descriptor_records_unsupported_and_continues():
    state = _hybrid_state()
    targeted = _targeted_hybrid_context_pack()
    targeted["items"][0]["available_context"] = []
    dsa = FakeDsa(
        [],
        context_responses=[_context_response(source_id="source_b")],
    )

    _, trace = await execute_hybrid_comparison(
        state=state,
        dsa=dsa,
        targeted_context_pack=targeted,
        dsa_trace={"called": True, "status": "success"},
    )

    assert len(dsa.calls) == 1
    assert state.expansion_attempts[0]["source_id"] == "source_a"
    assert state.expansion_attempts[0]["outcome"] == "unsupported"
    assert state.expansion_attempts[1]["outcome"] == "satisfied"
    assert trace["expansion_attempt_counts"]["unsupported"] == 1


def test_hybrid_facts_manifest_identity_and_privacy_are_prompt_aware():
    state = _hybrid_state()
    state.expansion_attempts = [
        {
            "source_id": "source_a",
            "seed_source_ref": "connector:source_a:seed-a",
            "context_mode": "nearby_rows",
            "outcome": "satisfied",
            "query_id": "query-a",
            "returned_reference_count": 1,
        },
        {
            "source_id": "source_b",
            "seed_source_ref": "connector:source_b:seed-b",
            "context_mode": "upcoming_events",
            "outcome": "satisfied",
            "query_id": "query-b",
            "returned_reference_count": 1,
        },
    ]
    context_pack = {
        **_targeted_hybrid_context_pack(),
        "items": [
            {
                **item,
                "available_context": [],
            }
            for item in _targeted_hybrid_context_pack()["items"]
        ],
    }
    retained = {
        "connector:source_a:seed-a",
        "connector:source_b:seed-b",
    }
    facts = _build_acquisition_facts(
        plan=state.plan,
        context_pack=context_pack,
        dsa_trace={"status": "included"},
        retained_source_refs=retained,
        expansion_attempts=state.expansion_attempts,
    )
    assert {item["requirement_id"]: item["outcome"] for item in facts} == {
        "context-delivery": "satisfied",
        "cross-source-comparison": "satisfied",
        "selected-source-coverage": "satisfied",
    }

    filtered = _build_acquisition_facts(
        plan=state.plan,
        context_pack=context_pack,
        dsa_trace={"status": "included"},
        retained_source_refs={"connector:source_a:seed-a"},
        expansion_attempts=state.expansion_attempts,
    )
    assert {item["requirement_id"]: item["outcome"] for item in filtered} == {
        "context-delivery": "filtered",
        "cross-source-comparison": "filtered",
        "selected-source-coverage": "filtered",
    }
    truncated = _build_acquisition_facts(
        plan=state.plan,
        context_pack=context_pack,
        dsa_trace={
            "status": "included",
            "budget_truncated": False,
            "candidate_truncated": True,
        },
        retained_source_refs=retained,
        expansion_attempts=state.expansion_attempts,
    )
    assert {item["requirement_id"]: item["outcome"] for item in truncated} == {
        "context-delivery": "satisfied",
        "cross-source-comparison": "truncated",
        "selected-source-coverage": "truncated",
    }

    identity_one = _manifest_id(
        scope=SCOPE,
        plan_id=state.plan.plan_id,
        selected_strategies=["hybrid"],
        declared_scope=state.declared_scope,
        expansion_attempts=state.expansion_attempts,
    )
    changed_attempts = copy.deepcopy(state.expansion_attempts)
    changed_attempts[0]["context_mode"] = "different_mode"
    identity_two = _manifest_id(
        scope=SCOPE,
        plan_id=state.plan.plan_id,
        selected_strategies=["hybrid"],
        declared_scope=state.declared_scope,
        expansion_attempts=changed_attempts,
    )
    assert identity_one != identity_two

    state.acquisition_facts = facts
    manifest = build_manifest_trace(
        state=state,
        context_pack=context_pack,
        dsa_trace={
            "called": True,
            "status": "included",
            "raw_item_count": 4,
            "expansion_attempt_counts": {
                "satisfied": 2,
                "unknown": 0,
                "failed": 0,
                "filtered": 0,
                "truncated": 0,
                "unsupported": 0,
            },
        },
        retained_source_refs=retained,
    )
    assert manifest["acquisition"]["expansion_attempt_count"] == 2
    assert manifest["acquisition"]["expansion_successful_count"] == 2
    assert manifest["acquisition"]["expansion_attempts"][0] == {
        "source_id": "source_a",
        "seed_source_ref": "connector:source_a:seed-a",
        "context_mode": "nearby_rows",
        "outcome": "satisfied",
        "returned_reference_count": 1,
    }
    assert "query-a" not in json.dumps(manifest, sort_keys=True)
    suppressed = suppress_manifest_identifiers(manifest)
    assert suppressed["acquisition"]["expansion_attempt_count"] == 2
    assert suppressed["acquisition"]["expansion_successful_count"] == 2
    assert suppressed["acquisition"]["expansion_attempts"] == []
    serialized = json.dumps(suppressed, sort_keys=True)
    for prohibited in (
        "source_a",
        "connector:source_a:seed-a",
        "nearby_rows",
        "query-a",
    ):
        assert prohibited not in serialized


def test_comparison_scope_boundary_is_unconditional_and_idempotent():
    state = _hybrid_state()
    state.sufficiency = _sufficiency_response(
        state.manifest_id,
        task_shape="cross_source_comparison",
        evidence_plan_id=state.plan.plan_id,
    )["result"]
    state.sufficiency = SufficiencyResult.model_validate(state.sufficiency)
    answer = enforce_final_answer("All records match.", state)
    assert answer.endswith(COMPARISON_SCOPE_SUFFIX)
    assert enforce_final_answer(answer, state).count(COMPARISON_SCOPE_SUFFIX) == 1
    assert enforce_final_answer("The selected records differ.", state) == (
        f"The selected records differ.\n\n{COMPARISON_SCOPE_SUFFIX}"
    )


@pytest.mark.parametrize(
    "available_context",
    [
        {"context_mode": "nearby_rows", "description": "Fetch nearby rows."},
        (
            {
                "context_mode": "nearby_rows",
                "description": "Fetch nearby rows.",
            },
        ),
        ["nearby_rows"],
        [
            {
                "context_mode": f"mode_{index}",
                "description": "Fetch bounded context.",
            }
            for index in range(17)
        ],
        [{"description": "Fetch nearby rows."}],
        [{"context_mode": "nearby_rows"}],
        [{"context_mode": "", "description": "Fetch nearby rows."}],
        [{"context_mode": "nearby rows", "description": "Fetch nearby rows."}],
        [
            {
                "context_mode": "https://private.invalid/context",
                "description": "Fetch nearby rows.",
            }
        ],
        [
            {
                "context_mode": "nearby_rows?window=1",
                "description": "Fetch nearby rows.",
            }
        ],
        [{"context_mode": "x" * 121, "description": "Fetch nearby rows."}],
        [{"context_mode": "nearby_rows", "description": "x" * 501}],
        [
            {
                "context_mode": "nearby_rows",
                "description": "Fetch nearby rows.",
            },
            {
                "context_mode": "nearby_rows",
                "description": "Fetch nearby rows again.",
            },
        ],
        [
            {
                "context_mode": "nearby_rows",
                "description": "Fetch nearby rows.",
                "metadata": {"private": True},
            }
        ],
        [
            {
                "context_mode": "nearby_rows",
                "description": "Fetch nearby rows.",
                "arguments": {"window": 5},
            }
        ],
        [
            {
                "context_mode": "nearby_rows",
                "description": "Fetch nearby rows.",
                "credentials": "PRIVATE CREDENTIAL",
            }
        ],
        [
            {
                "context_mode": "nearby_rows",
                "description": "Fetch nearby rows.",
                "url": "https://private.invalid/context",
            }
        ],
        [
            {
                "context_mode": "nearby_rows",
                "description": "Fetch nearby rows.",
                "raw": {"private": "PRIVATE RAW CONTENT"},
            }
        ],
        [
            {
                "context_mode": "nearby_rows",
                "description": "Fetch nearby rows.",
                "source_config": {"private": True},
            }
        ],
    ],
)
def test_context_pack_contract_rejects_malformed_descriptors(
    available_context,
):
    response = copy.deepcopy(_context_pack())
    response["items"][0]["available_context"] = available_context

    with pytest.raises(ValidationError):
        _validated_context_pack(response)


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
    with pytest.raises(ValidationError):
        _validated_context_pack(
            {
                **_context_pack(),
                "items": [
                    {
                        **_context_pack()["items"][0],
                        "unexpected_item_field": "not allowed",
                    }
                ],
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


def test_fetch_response_contract_accepts_real_shape_and_excludes_private_fields():
    validated = validate_fetch_response(
        _fetch_response(),
        expected_source_id="source_a",
        expected_source_ref="connector:source_a:item-1",
    )
    assert validated.retrieval_mode == "fetch"
    assert validated.results[0].source_ref == "connector:source_a:item-1"


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        (
            lambda response: response["results"][0].update(source_id="source_b"),
            "fetch_source_id_mismatch",
        ),
        (
            lambda response: response["results"][0].update(
                source_ref="connector:source_a:item-other"
            ),
            "fetch_source_reference_mismatch",
        ),
        (
            lambda response: response["results"][0].update(
                raw={"private": "PRIVATE RAW DATA"}
            ),
            "raw_fetch_data_not_allowed",
        ),
        (
            lambda response: response.update(answerable=False),
            "fetch_answerability_mismatch",
        ),
        (
            lambda response: response["budget"].update(returned_results=0),
            "fetch_result_count_mismatch",
        ),
        (
            lambda response: response.update(retrieval_mode="search"),
            "Input should be 'fetch'",
        ),
        (
            lambda response: response.update(metadata={"private": True}),
            "Extra inputs are not permitted",
        ),
    ],
    ids=[
        "wrong-source",
        "wrong-reference",
        "raw-data",
        "answerability",
        "result-count",
        "retrieval-mode",
        "unknown-field",
    ],
)
def test_fetch_response_contract_rejects_malformed_or_unassociated_results(
    mutation,
    expected_error,
):
    response = copy.deepcopy(_fetch_response())
    mutation(response)
    with pytest.raises((ValueError, ValidationError), match=expected_error):
        validate_fetch_response(
            response,
            expected_source_id="source_a",
            expected_source_ref="connector:source_a:item-1",
        )


@pytest.mark.asyncio
async def test_exact_execution_attempts_every_reference_in_deterministic_order():
    runtime = FakeRuntime(
        plan=_exact_plan_response(
            eligible_source_ids=["source_a", "source_b"],
        ),
        sufficiency_status="insufficient",
    )
    dsa = FakeDsa(
        [
            _source("source_a", capabilities=["fetch"]),
            _source("source_b", capabilities=["fetch"]),
        ],
        fetch_responses=[
            RuntimeError("PRIVATE DEPENDENCY ERROR"),
            _fetch_response(
                source_id="source_b",
                source_ref="connector:source_b:item-2",
            ),
        ],
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={
            "exact_source_refs": [
                {
                    "source_id": "source_b",
                    "source_ref": "connector:source_b:item-2",
                },
                {
                    "source_id": "source_a",
                    "source_ref": "connector:source_a:item-1",
                },
            ]
        },
        **SCOPE,
    )
    context, trace = await execute_exact_fetches(state=state, dsa=dsa)

    fetch_calls = [call for call in dsa.calls if isinstance(call, tuple)]
    assert [call[1]["source_ref"] for call in fetch_calls] == [
        "connector:source_a:item-1",
        "connector:source_b:item-2",
    ]
    assert all(call[1]["include_raw"] is False for call in fetch_calls)
    assert trace["call_count"] == 2
    assert state.exact_attempts == [
        {
            "source_id": "source_a",
            "source_ref": "connector:source_a:item-1",
            "outcome": "failed",
            "query_id": None,
        },
        {
            "source_id": "source_b",
            "source_ref": "connector:source_b:item-2",
            "outcome": "satisfied",
            "query_id": "query-source_b",
        },
    ]
    assert context is not None
    assert context["sources_used"] == ["source_b"]
    assert "PRIVATE DEPENDENCY ERROR" not in json.dumps(context, sort_keys=True)


@pytest.mark.asyncio
async def test_complete_exact_acquisition_and_prompt_delivery_control_sufficiency():
    requirements = _exact_plan_response(
        authoritative_source_ids=["source_a"]
    )["result"]["declared_requirements"]
    runtime = FakeRuntime(
        plan=_exact_plan_response(authoritative_source_ids=["source_a"]),
        sufficiency_status="sufficient_for_declared_scope",
    )
    dsa = FakeDsa(
        [_source("source_a", capabilities=["fetch"])],
        fetch_responses=[_fetch_response()],
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={
            "exact_source_refs": [
                {
                    "source_id": "source_a",
                    "source_ref": "connector:source_a:item-1",
                }
            ]
        },
        **SCOPE,
    )
    context, trace = await execute_exact_fetches(state=state, dsa=dsa)
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=context,
        dsa_trace=trace,
        retained_source_refs={"connector:source_a:item-1"},
        **SCOPE,
    )

    facts = runtime.calls[-1][1]["acquisition_facts"]
    assert facts == [
        {"requirement_id": "context-delivery", "outcome": "satisfied"},
        {
            "requirement_id": "exact-authoritative-fetch",
            "outcome": "satisfied",
        },
        {"requirement_id": "targeted-evidence", "outcome": "satisfied"},
    ]
    assert runtime.calls[-1][1]["declared_requirements"] == requirements
    assert provider_allowed(state) is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retained_refs", "expected_delivery"),
    [
        ({"connector:source_a:item-1"}, "filtered"),
        (
            {
                "connector:source_a:item-1",
                "connector:source_b:item-2",
                "connector:source_b:not-returned",
            },
            "unknown",
        ),
    ],
    ids=["one-reference-omitted", "unknown-reference-retained"],
)
async def test_exact_prompt_delivery_requires_every_returned_reference_and_no_unknowns(
    retained_refs,
    expected_delivery,
):
    runtime = FakeRuntime(
        plan=_exact_plan_response(
            eligible_source_ids=["source_a", "source_b"],
        ),
        sufficiency_status=(
            "unknown" if expected_delivery == "unknown" else "insufficient"
        ),
    )
    dsa = FakeDsa(
        [
            _source("source_a", capabilities=["fetch"]),
            _source("source_b", capabilities=["fetch"]),
        ],
        fetch_responses=[
            _fetch_response(),
            _fetch_response(
                source_id="source_b",
                source_ref="connector:source_b:item-2",
            ),
        ],
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={
            "exact_source_refs": [
                {
                    "source_id": "source_a",
                    "source_ref": "connector:source_a:item-1",
                },
                {
                    "source_id": "source_b",
                    "source_ref": "connector:source_b:item-2",
                },
            ]
        },
        **SCOPE,
    )
    context, trace = await execute_exact_fetches(state=state, dsa=dsa)
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=context,
        dsa_trace=trace,
        retained_source_refs=retained_refs,
        **SCOPE,
    )
    facts = {
        item["requirement_id"]: item["outcome"]
        for item in runtime.calls[-1][1]["acquisition_facts"]
    }
    assert facts["context-delivery"] == expected_delivery
    assert provider_allowed(state) is False
    manifest = build_manifest_trace(
        state=state,
        context_pack=context,
        dsa_trace=trace,
        retained_source_refs=retained_refs,
    )
    assert "connector:source_b:not-returned" not in manifest["acquisition"][
        "source_references_retained"
    ]


@pytest.mark.asyncio
async def test_authoritative_exact_requirement_uses_only_authoritative_attempts():
    runtime = FakeRuntime(
        plan=_exact_plan_response(
            eligible_source_ids=["source_a", "source_b"],
            authoritative_source_ids=["source_a"],
        ),
        sufficiency_status="insufficient",
    )
    dsa = FakeDsa(
        [
            _source("source_a", capabilities=["fetch"]),
            _source("source_b", capabilities=["fetch"]),
        ],
        fetch_responses=[
            RuntimeError("authoritative source failed"),
            _fetch_response(
                source_id="source_b",
                source_ref="connector:source_b:item-2",
            ),
        ],
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={
            "exact_source_refs": [
                {
                    "source_id": "source_a",
                    "source_ref": "connector:source_a:item-1",
                },
                {
                    "source_id": "source_b",
                    "source_ref": "connector:source_b:item-2",
                },
            ]
        },
        **SCOPE,
    )
    context, trace = await execute_exact_fetches(state=state, dsa=dsa)
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=context,
        dsa_trace=trace,
        retained_source_refs={"connector:source_b:item-2"},
        **SCOPE,
    )
    facts = {
        item["requirement_id"]: item["outcome"]
        for item in runtime.calls[-1][1]["acquisition_facts"]
    }
    assert facts["exact-authoritative-fetch"] == "failed"
    assert facts["targeted-evidence"] == "failed"
    assert provider_allowed(state) is False


@pytest.mark.asyncio
async def test_exact_optional_limitation_discloses_actual_scope_once():
    plan = _exact_plan_response(status="ready_with_limitations")
    plan["result"]["limitation_codes"] = ["optional_source_unavailable"]
    plan["result"]["declared_requirements"].append(
        {
            "requirement_id": "optional-selected-source-coverage",
            "requirement_kind": "selected_source_coverage",
            "criticality": "optional",
        }
    )
    runtime = FakeRuntime(
        plan=plan,
        sufficiency_status="sufficient_with_limitations",
    )
    dsa = FakeDsa(
        [_source("source_a", capabilities=["fetch"])],
        fetch_responses=[_fetch_response()],
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={
            "exact_source_refs": [
                {
                    "source_id": "source_a",
                    "source_ref": "connector:source_a:item-1",
                }
            ]
        },
        **SCOPE,
    )
    context, trace = await execute_exact_fetches(state=state, dsa=dsa)
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=context,
        dsa_trace=trace,
        retained_source_refs={"connector:source_a:item-1"},
        **SCOPE,
    )
    answer = enforce_final_answer("The exact record gives the date.", state)
    assert provider_allowed(state) is True
    limitation = "Limitation: an optional selected source was not available."
    assert answer == (
        f"The exact record gives the date.\n\n{limitation}\n\n"
        f"{TARGETED_SCOPE_SUFFIX}"
    )
    assert enforce_final_answer(answer, state).count(limitation) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected_outcome"),
    [
        (_fetch_response(result=False), "unknown"),
        (_fetch_response(truncated=True), "truncated"),
        (
            {
                **_fetch_response(),
                "retrieval_mode": "search",
            },
            "filtered",
        ),
        (RuntimeError("PRIVATE FAILURE"), "failed"),
    ],
    ids=["no-result", "truncated", "malformed", "dependency-failure"],
)
async def test_incomplete_exact_acquisition_never_satisfies_material_evidence(
    response,
    expected_outcome,
):
    runtime = FakeRuntime(
        plan=_exact_plan_response(),
        sufficiency_status=(
            "unknown" if expected_outcome == "unknown" else "insufficient"
        ),
    )
    dsa = FakeDsa(
        [_source("source_a", capabilities=["fetch"])],
        fetch_responses=[copy.deepcopy(response)],
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={
            "exact_source_refs": [
                {
                    "source_id": "source_a",
                    "source_ref": "connector:source_a:item-1",
                }
            ]
        },
        **SCOPE,
    )
    context, trace = await execute_exact_fetches(state=state, dsa=dsa)
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=context,
        dsa_trace=trace,
        retained_source_refs=set(),
        **SCOPE,
    )
    facts = {
        item["requirement_id"]: item["outcome"]
        for item in runtime.calls[-1][1]["acquisition_facts"]
    }
    assert facts["targeted-evidence"] == expected_outcome
    assert provider_allowed(state) is False


@pytest.mark.asyncio
async def test_exact_manifest_is_truthful_private_and_order_independent():
    async def build(references, responses):
        runtime = FakeRuntime(
            plan=_exact_plan_response(
                eligible_source_ids=["source_a", "source_b"],
            )
        )
        dsa = FakeDsa(
            [
                _source("source_a", capabilities=["fetch"]),
                _source("source_b", capabilities=["fetch"]),
            ],
            fetch_responses=responses,
        )
        state = await begin_evidence_acquisition(
            runtime=runtime,
            dsa=dsa,
            task_text=QUESTION,
            interaction_kind="question",
            external_context={"exact_source_refs": references},
            **SCOPE,
        )
        context, trace = await execute_exact_fetches(state=state, dsa=dsa)
        await evaluate_acquisition_sufficiency(
            state=state,
            runtime=runtime,
            context_pack=context,
            dsa_trace=trace,
            retained_source_refs={
                "connector:source_a:item-1",
                "connector:source_b:item-2",
            },
            **SCOPE,
        )
        return build_manifest_trace(
            state=state,
            context_pack=context,
            dsa_trace=trace,
            retained_source_refs={
                "connector:source_a:item-1",
                "connector:source_b:item-2",
            },
        )

    references = [
        {"source_id": "source_a", "source_ref": "connector:source_a:item-1"},
        {"source_id": "source_b", "source_ref": "connector:source_b:item-2"},
    ]
    responses = [
        _fetch_response(),
        _fetch_response(
            source_id="source_b",
            source_ref="connector:source_b:item-2",
        ),
    ]
    first = await build(references, copy.deepcopy(responses))
    second = await build(list(reversed(references)), copy.deepcopy(responses))

    assert first == second
    acquisition = first["acquisition"]
    assert acquisition["strategy_attempted"] == "exact_fetch"
    assert acquisition["exact_reference_attempt_count"] == 2
    assert acquisition["exact_reference_successful_count"] == 2
    assert acquisition["source_references_attempted"] == [
        "connector:source_a:item-1",
        "connector:source_b:item-2",
    ]
    assert acquisition["exact_reference_attempts"] == [
        {
            "source_id": "source_a",
            "source_ref": "connector:source_a:item-1",
            "outcome": "satisfied",
        },
        {
            "source_id": "source_b",
            "source_ref": "connector:source_b:item-2",
            "outcome": "satisfied",
        },
    ]
    assert acquisition["source_references_returned"] == (
        acquisition["source_references_retained"]
    )
    serialized = json.dumps(first, sort_keys=True)
    for prohibited in (
        "PRIVATE EXACT CONTENT",
        "PRIVATE TITLE",
        "PRIVATE SOURCE NAME",
        "PRIVATE CONTEXT DESCRIPTION",
        "https://private.invalid",
        '"confidence"',
    ):
        assert prohibited not in serialized
    suppressed = suppress_manifest_identifiers(first)
    assert suppressed["acquisition"]["source_references_attempted"] == []
    assert suppressed["acquisition"]["source_references_attempted_count"] == 2
    assert suppressed["acquisition"]["exact_reference_attempts"] == []
    assert suppressed["acquisition"]["exact_reference_attempts_count"] == 2


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
    limitation = "Limitation: an optional selected source was not available."
    assert answer == (
        f"The record gives the date.\n\n{limitation}\n\n"
        f"{TARGETED_SCOPE_SUFFIX}"
    )
    assert enforce_final_answer(answer, state).count(limitation) == 1


@pytest.mark.asyncio
async def test_targeted_scope_boundary_is_unconditional_and_idempotent():
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
    assert ordinary == (
        f"The targeted record gives the date.\n\n{TARGETED_SCOPE_SUFFIX}"
    )
    assert overclaim.count(TARGETED_SCOPE_SUFFIX) == 1
    assert enforce_final_answer(overclaim, state).count(TARGETED_SCOPE_SUFFIX) == 1


@pytest.mark.parametrize(
    ("task_shape", "boundary"),
    [
        ("targeted_lookup", TARGETED_SCOPE_SUFFIX),
        ("cross_source_comparison", COMPARISON_SCOPE_SUFFIX),
        ("bounded_exhaustive_review", EXHAUSTIVE_SCOPE_SUFFIX),
    ],
)
def test_governed_success_boundaries_follow_task_shape_not_provider_text(
    task_shape,
    boundary,
):
    state = _rendering_state(task_shape=task_shape)

    answer = enforce_final_answer("The checked record supports the result.", state)

    assert answer == f"The checked record supports the result.\n\n{boundary}"
    for other_boundary in {
        TARGETED_SCOPE_SUFFIX,
        COMPARISON_SCOPE_SUFFIX,
        EXHAUSTIVE_SCOPE_SUFFIX,
    } - {boundary}:
        assert other_boundary not in answer


@pytest.mark.parametrize(
    ("task_shape", "boundary", "provider_answer"),
    [
        (
            "targeted_lookup",
            TARGETED_SCOPE_SUFFIX,
            "Every possible source was fully examined.",
        ),
        (
            "cross_source_comparison",
            COMPARISON_SCOPE_SUFFIX,
            "All possible sources were checked.",
        ),
        (
            "bounded_exhaustive_review",
            EXHAUSTIVE_SCOPE_SUFFIX,
            "No evidence exists outside this result.",
        ),
        (
            "targeted_lookup",
            TARGETED_SCOPE_SUFFIX,
            "No evidence exists beyond the checked material.",
        ),
        (
            "cross_source_comparison",
            COMPARISON_SCOPE_SUFFIX,
            "The search was complete across every relevant source.",
        ),
    ],
)
def test_provider_universal_scope_claims_are_replaced(
    task_shape,
    boundary,
    provider_answer,
):
    state = _rendering_state(task_shape=task_shape)

    answer = enforce_final_answer(provider_answer, state)

    replacement = (
        "I withheld the generated answer because it claimed evidence coverage "
        "beyond the examined scope."
    )
    assert provider_answer not in answer
    assert answer == f"{replacement}\n\n{boundary}"
    assert answer.count(replacement) == 1
    assert answer.count(boundary) == 1


@pytest.mark.parametrize(
    ("task_shape", "boundary", "provider_answer"),
    [
        (
            "bounded_exhaustive_review",
            EXHAUSTIVE_SCOPE_SUFFIX,
            "Within the declared scope, all configured records were reviewed.",
        ),
        (
            "targeted_lookup",
            TARGETED_SCOPE_SUFFIX,
            "I cannot say every possible source was examined.",
        ),
        (
            "cross_source_comparison",
            COMPARISON_SCOPE_SUFFIX,
            "Not every potentially relevant source was checked.",
        ),
        (
            "targeted_lookup",
            TARGETED_SCOPE_SUFFIX,
            "No contradictory evidence was found in the selected records.",
        ),
    ],
)
def test_bounded_provider_scope_statements_are_preserved(
    task_shape,
    boundary,
    provider_answer,
):
    state = _rendering_state(task_shape=task_shape)

    answer = enforce_final_answer(provider_answer, state)

    assert answer == f"{provider_answer}\n\n{boundary}"
    assert "I withheld the generated answer" not in answer
    assert answer.count(boundary) == 1


@pytest.mark.parametrize(
    "provider_answer",
    [
        "# EVERY POSSIBLE SOURCE WAS FULLY EXAMINED.",
        "- All   possible\n sources\twere checked.",
        "**No evidence exists outside this result.**",
    ],
)
def test_provider_scope_claim_formatting_cannot_evade_replacement(provider_answer):
    state = _rendering_state()

    answer = enforce_final_answer(provider_answer, state)

    assert provider_answer not in answer
    assert answer.count("I withheld the generated answer") == 1
    assert answer.count(TARGETED_SCOPE_SUFFIX) == 1


_ENDORSED_QUOTED_SCOPE_CLAIMS = (
    'The report claimed, "Every possible source was fully examined," and that claim is correct.',
    'The earlier answer claimed, "Every possible source was fully examined." I agree.',
    'The phrase "All possible sources were checked" is not supported by the '
    "evidence, but it is nevertheless true.",
    'The report stated, "No evidence exists outside this result." That is correct.',
    'The earlier answer quoted, "The search was complete across every relevant '
    'source." The earlier answer was right.',
    'The report claimed, "Every possible source was fully examined," and I agree.',
)

_NON_ENDORSING_QUOTED_SCOPE_REFERENCES = (
    'The report claimed, "Every possible source was fully examined," and that claim is false.',
    'The earlier answer claimed, "Every possible source was fully examined." I disagree.',
    'The phrase "All possible sources were checked" is not supported by the '
    "evidence, and I reject it.",
    'The report stated, "No evidence exists outside this result." I have not verified that claim.',
    'The earlier answer quoted, "The search was complete across every relevant '
    'source." The earlier answer was wrong.',
    'The report claimed, "Every possible source was fully examined." I do not agree.',
)


@pytest.mark.parametrize(
    "provider_answer",
    [
        "Every possible source was fully examined.",
        "All possible sources were checked.",
        "No evidence exists outside this result.",
        "No evidence exists beyond the checked material.",
        "The search was complete across every relevant source.",
        "The evidence shows that every possible source was fully examined.",
        "According to the results, all possible sources were checked.",
        "I confirmed that no evidence exists outside this result.",
        '"Every possible source was fully examined."',
        "# EVERY POSSIBLE SOURCE WAS FULLY EXAMINED.",
        "- All   possible\n sources\twere checked.",
        "**No evidence exists outside this result.**",
    ],
)
def test_provider_scope_claim_helper_rejects_affirmative_assertions(
    provider_answer,
):
    assert _provider_answer_claims_universal_scope(provider_answer, []) is True


@pytest.mark.parametrize("provider_answer", _ENDORSED_QUOTED_SCOPE_CLAIMS)
def test_provider_scope_claim_helper_rejects_endorsed_quoted_assertions(
    provider_answer,
):
    assert _provider_answer_claims_universal_scope(provider_answer, []) is True


@pytest.mark.parametrize(
    "provider_answer",
    [
        'The report claimed, "Every possible source was fully examined," '
        "and that statement is true.",
        'The report quoted, "Every possible source was fully examined," '
        "and the phrase is accurate.",
        'The report claimed, "Every possible source was fully examined," '
        "but it is still correct.",
        'The report claimed, "Every possible source was fully examined," '
        "but that claim is supported.",
        'The report claimed, "Every possible source was fully examined." I concur.',
        'The report claimed, "Every possible source was fully examined." '
        "That is true.",
        'The report claimed, "Every possible source was fully examined." '
        "That claim is correct.",
        'The report claimed, "Every possible source was fully examined." '
        "This statement is accurate.",
        'The report claimed, "Every possible source was fully examined." '
        "The claim is supported.",
        'The report claimed, "Every possible source was fully examined." '
        "The report was right.",
        'The report claimed, "Every possible source was fully examined." '
        "The earlier answer was correct.",
    ],
)
def test_provider_scope_claim_helper_rejects_bounded_endorsement_vocabulary(
    provider_answer,
):
    assert _provider_answer_claims_universal_scope(provider_answer, []) is True


@pytest.mark.parametrize(
    "provider_answer",
    [
        "Within the declared scope, all configured records were reviewed.",
        "I cannot say every possible source was examined.",
        "Not every potentially relevant source was checked.",
        "No contradictory evidence was found in the selected records.",
        "Not every possible source was fully examined.",
        "It is false that all possible sources were checked.",
        "It is not true that no evidence exists outside this result.",
        "We cannot conclude that the search was complete across every relevant source.",
        'The earlier answer claimed, "Every possible source was fully examined."',
        'The phrase "All possible sources were checked" is not supported by the evidence.',
        'The user asked whether "no evidence exists outside this result."',
        'I rejected the statement "The search was complete across every relevant source."',
    ],
)
def test_provider_scope_claim_helper_allows_bounded_negated_and_metalinguistic_text(
    provider_answer,
):
    assert _provider_answer_claims_universal_scope(provider_answer, []) is False


@pytest.mark.parametrize(
    "provider_answer",
    _NON_ENDORSING_QUOTED_SCOPE_REFERENCES,
)
def test_provider_scope_claim_helper_allows_non_endorsing_quoted_references(
    provider_answer,
):
    assert _provider_answer_claims_universal_scope(provider_answer, []) is False


@pytest.mark.parametrize(
    "provider_answer",
    [
        "Every possible source was fully examined.",
        "All possible sources were checked.",
        "No evidence exists outside this result.",
        "No evidence exists beyond the checked material.",
        "The search was complete across every relevant source.",
        "The evidence shows that every possible source was fully examined.",
        "According to the results, all possible sources were checked.",
        "I confirmed that no evidence exists outside this result.",
        '"Every possible source was fully examined."',
    ],
)
def test_targeted_answer_boundary_replaces_affirmative_scope_claims(provider_answer):
    state = _rendering_state(task_shape="targeted_lookup")

    answer = enforce_final_answer(provider_answer, state)

    replacement = (
        "I withheld the generated answer because it claimed evidence coverage "
        "beyond the examined scope."
    )
    assert provider_answer not in answer
    assert answer == f"{replacement}\n\n{TARGETED_SCOPE_SUFFIX}"
    assert answer.count(replacement) == 1
    assert answer.count(TARGETED_SCOPE_SUFFIX) == 1


@pytest.mark.parametrize("provider_answer", _ENDORSED_QUOTED_SCOPE_CLAIMS)
def test_targeted_answer_boundary_replaces_endorsed_quoted_scope_claims(
    provider_answer,
):
    state = _rendering_state(task_shape="targeted_lookup")

    answer = enforce_final_answer(provider_answer, state)

    replacement = (
        "I withheld the generated answer because it claimed evidence coverage "
        "beyond the examined scope."
    )
    assert provider_answer not in answer
    assert answer == f"{replacement}\n\n{TARGETED_SCOPE_SUFFIX}"
    assert answer.count(replacement) == 1
    assert answer.count(TARGETED_SCOPE_SUFFIX) == 1


@pytest.mark.parametrize(
    "provider_answer",
    [
        "Not every possible source was fully examined.",
        "It is false that all possible sources were checked.",
        "It is not true that no evidence exists outside this result.",
        "We cannot conclude that the search was complete across every relevant source.",
        'The earlier answer claimed, "Every possible source was fully examined."',
        'The phrase "All possible sources were checked" is not supported by the evidence.',
        'The user asked whether "no evidence exists outside this result."',
        'I rejected the statement "The search was complete across every relevant source."',
    ],
)
def test_targeted_answer_boundary_preserves_negated_and_metalinguistic_text(
    provider_answer,
):
    state = _rendering_state(task_shape="targeted_lookup")

    answer = enforce_final_answer(provider_answer, state)

    assert answer == f"{provider_answer}\n\n{TARGETED_SCOPE_SUFFIX}"
    assert "I withheld the generated answer" not in answer
    assert answer.count(TARGETED_SCOPE_SUFFIX) == 1


@pytest.mark.parametrize(
    "provider_answer",
    _NON_ENDORSING_QUOTED_SCOPE_REFERENCES,
)
def test_targeted_answer_boundary_preserves_non_endorsing_quoted_references(
    provider_answer,
):
    state = _rendering_state(task_shape="targeted_lookup")

    answer = enforce_final_answer(provider_answer, state)

    assert answer == f"{provider_answer}\n\n{TARGETED_SCOPE_SUFFIX}"
    assert "I withheld the generated answer" not in answer
    assert answer.count(TARGETED_SCOPE_SUFFIX) == 1


@pytest.mark.parametrize(
    ("task_shape", "boundary"),
    [
        ("targeted_lookup", TARGETED_SCOPE_SUFFIX),
        ("cross_source_comparison", COMPARISON_SCOPE_SUFFIX),
        ("bounded_exhaustive_review", EXHAUSTIVE_SCOPE_SUFFIX),
    ],
)
def test_provider_authored_limitation_paragraph_is_preserved(
    task_shape,
    boundary,
):
    state = _rendering_state(task_shape=task_shape)
    provider_answer = (
        "The report supports the migration.\n\n"
        "Limitation: the report applies only to version 2."
    )

    answer = enforce_final_answer(provider_answer, state)

    assert answer == f"{provider_answer}\n\n{boundary}"
    assert enforce_final_answer(answer, state) == answer


@pytest.mark.parametrize(
    "provider_paragraph",
    [
        "Limitation: the report applies only to version 2.",
        "Limitation — the report applies only to version 2.",
        "Limitations: the report applies only to version 2.",
        "Limited to: version 2.",
    ],
)
def test_provider_limitation_like_paragraphs_are_not_policy_owned(
    provider_paragraph,
):
    state = _rendering_state()

    answer = enforce_final_answer(provider_paragraph, state)

    assert answer == f"{provider_paragraph}\n\n{TARGETED_SCOPE_SUFFIX}"
    assert enforce_final_answer(answer, state) == answer


def test_limited_answer_preserves_provider_limitation_before_policy_paragraphs():
    state = _rendering_state(
        status="sufficient_with_limitations",
        evaluations=[
            {
                "requirement_id": "optional-selected-source-coverage",
                "requirement_kind": "selected_source_coverage",
                "criticality": "optional",
                "effective_outcome": "unavailable",
            }
        ],
        limitation_codes=["optional_source_unavailable"],
    )
    provider_answer = (
        "The report supports the migration.\n\n"
        "Limitation: the report applies only to version 2."
    )
    policy_limitation = (
        "Limitation: an optional selected source was not available."
    )

    answer = enforce_final_answer(provider_answer, state)

    assert answer == (
        f"{provider_answer}\n\n{policy_limitation}\n\n"
        f"{TARGETED_SCOPE_SUFFIX}"
    )
    assert enforce_final_answer(answer, state) == answer
    assert answer.count(policy_limitation) == 1
    assert answer.count(TARGETED_SCOPE_SUFFIX) == 1


@pytest.mark.parametrize(
    "provider_answer",
    [
        "Nothing material was left unchecked.",
        "The implementation has no remaining omissions.",
        "The evidence conclusively settles the issue.",
        "The records account for the entire requirement set.",
        "There are no unresolved gaps.",
        "The checked entries support the result.",
        "Review coverage settles the requested question without qualification.",
    ],
)
def test_provider_paraphrases_cannot_avoid_exhaustive_scope_boundary(
    provider_answer,
):
    state = _rendering_state(task_shape="bounded_exhaustive_review")

    answer = enforce_final_answer(provider_answer, state)

    assert answer == f"{provider_answer}\n\n{EXHAUSTIVE_SCOPE_SUFFIX}"


def test_policy_paragraph_normalization_is_idempotent_and_shape_owned():
    state = _rendering_state(task_shape="bounded_exhaustive_review")
    provider_answer = (
        f"The records support the result.\n\n{TARGETED_SCOPE_SUFFIX}\n\n"
        f"{EXHAUSTIVE_SCOPE_SUFFIX}"
    )

    first = enforce_final_answer(provider_answer, state)
    second = enforce_final_answer(first, state)

    assert first == (
        f"The records support the result.\n\n{EXHAUSTIVE_SCOPE_SUFFIX}"
    )
    assert second == first
    assert first.count(EXHAUSTIVE_SCOPE_SUFFIX) == 1
    assert TARGETED_SCOPE_SUFFIX not in first
    assert COMPARISON_SCOPE_SUFFIX not in first


@pytest.mark.parametrize(
    ("unavailable_count", "expected"),
    [
        (1, "Limitation: 1 optional source was unavailable."),
        (2, "Limitation: 2 optional sources were unavailable."),
    ],
)
def test_optional_source_limitation_uses_trusted_scoped_inventory_count(
    unavailable_count,
    expected,
):
    source_ids = [f"source_{index}" for index in range(unavailable_count)]
    state = _rendering_state(
        status="sufficient_with_limitations",
        evaluations=[
            {
                "requirement_id": "optional-selected-source-coverage",
                "requirement_kind": "selected_source_coverage",
                "criticality": "optional",
                "effective_outcome": "unavailable",
            }
        ],
        limitation_codes=["optional_source_unavailable"],
        inventory={
            "sources": [
                _source(source_id, status="unavailable")
                for source_id in source_ids
            ]
        },
        declared_scope={
            "source_ids": source_ids,
            "source_categories": [],
        },
    )

    answer = enforce_final_answer("The available evidence supports the result.", state)

    assert answer == (
        f"The available evidence supports the result.\n\n{expected}\n\n"
        f"{TARGETED_SCOPE_SUFFIX}"
    )
    assert enforce_final_answer(answer, state) == answer


@pytest.mark.parametrize(
    ("limitation_code", "expected"),
    [
        (
            "source_inventory_partial",
            "the configured source inventory was partial, so optional source "
            "coverage remains incomplete",
        ),
        (
            "source_inventory_unknown",
            "the completeness of the configured source inventory was unknown, so "
            "optional source coverage could not be established",
        ),
        (
            "source_inventory_unavailable",
            "the configured source inventory was unavailable, so optional source "
            "coverage could not be established",
        ),
    ],
)
def test_inventory_limitation_disclosure_is_specific(
    limitation_code,
    expected,
):
    state = _rendering_state(
        status="sufficient_with_limitations",
        evaluations=[
            {
                "requirement_id": "optional-selected-source-coverage",
                "requirement_kind": "selected_source_coverage",
                "criticality": "optional",
                "effective_outcome": "unknown",
            }
        ],
        limitation_codes=[limitation_code],
    )

    answer = enforce_final_answer("Provider-controlled limitation text.", state)

    assert f"Limitation: {expected}" in answer
    assert "Provider-controlled limitation text." in answer
    assert answer.endswith(TARGETED_SCOPE_SUFFIX)


def test_multiple_optional_limitations_are_deduplicated_and_bounded():
    state = _rendering_state(
        status="sufficient_with_limitations",
        evaluations=[
            {
                "requirement_id": "optional-selected-source-coverage",
                "requirement_kind": "selected_source_coverage",
                "criticality": "optional",
                "effective_outcome": "unavailable",
            }
        ],
        limitation_codes=[
            "authoritative_source_unavailable",
            "optional_source_unavailable",
            "source_inventory_partial",
            "required_capability_unavailable",
            "declared_category_not_available",
        ],
        inventory={
            "sources": [_source("source_a", status="unavailable")],
        },
        declared_scope={"source_ids": ["source_a"], "source_categories": []},
    )

    first = enforce_final_answer(
        "Limitation: provider-chosen qualification.\n\nThe result is bounded.",
        state,
    )
    second = enforce_final_answer(first, state)

    assert first == second
    assert "Limitation: provider-chosen qualification." in first
    assert first.count("1 optional source was unavailable") == 1
    assert "Additional optional evidence limitations remained." in first
    assert first.endswith(TARGETED_SCOPE_SUFFIX)


@pytest.mark.parametrize(
    ("requirement_kind", "expected"),
    [
        ("authoritative_inventory", "authoritative source inventory"),
        ("targeted_evidence", "requested targeted evidence"),
        ("exact_authoritative_fetch", "exact authoritative item"),
        ("complete_scope_coverage", "complete declared source scope"),
        ("selected_source_coverage", "coverage of every selected source"),
        ("structured_absence_check", "absence-supporting check"),
        ("contradiction_search", "required contradiction search"),
        ("counterevidence_coverage", "counterevidence coverage"),
        ("historical_scope", "required historical scope"),
        ("historical_sequence_coverage", "historical sequence"),
        ("candidate_evidence_coverage", "candidate evidence coverage"),
        ("cross_source_comparison", "selected-source comparison"),
        ("context_delivery", "reasoning context"),
        ("no_material_truncation", "full delivery of the material evidence"),
    ],
)
def test_every_requirement_kind_has_a_user_safe_gap_description(
    requirement_kind,
    expected,
):
    state = _rendering_state(
        status="unknown",
        evaluations=[
            {
                "requirement_id": f"requirement-{requirement_kind}",
                "requirement_kind": requirement_kind,
                "criticality": "material",
                "effective_outcome": "unknown",
            }
        ],
    )

    answer = enforce_final_answer("PRIVATE PROVIDER ANSWER", state)

    assert expected in answer
    assert "PRIVATE PROVIDER ANSWER" not in answer
    assert answer.endswith("I’m withholding the requested conclusion.")


@pytest.mark.parametrize(
    ("outcome", "status", "expected"),
    [
        ("partial", "insufficient", "only partially established"),
        (
            "not_attempted",
            "insufficient",
            "required acquisition was not attempted",
        ),
        ("failed", "insufficient", "acquisition failed"),
        ("excluded", "insufficient", "required evidence was excluded"),
        ("filtered", "insufficient", "filtered or omitted before reasoning"),
        ("truncated", "insufficient", "material evidence was truncated"),
        ("unsupported", "insufficient", "required acquisition was unsupported"),
        ("unavailable", "insufficient", "required evidence scope was unavailable"),
        ("unknown", "unknown", "could not be established"),
        ("missing", "unknown", "required acquisition fact was missing"),
        (
            "unresolved_contradiction",
            "insufficient",
            "contradictory evidence remained unresolved",
        ),
    ],
)
def test_material_gap_wording_distinguishes_effective_outcomes(
    outcome,
    status,
    expected,
):
    state = _rendering_state(
        task_shape="bounded_exhaustive_review",
        status=status,
        evaluations=[
            {
                "requirement_id": "complete-scope-coverage",
                "requirement_kind": "complete_scope_coverage",
                "criticality": "material",
                "effective_outcome": outcome,
            }
        ],
    )

    answer = enforce_final_answer("PRIVATE PROVIDER ANSWER", state)

    assert expected in answer
    assert "PRIVATE PROVIDER ANSWER" not in answer
    assert answer.endswith("I’m withholding a complete-scope conclusion.")


@pytest.mark.parametrize(
    ("task_shape", "withholding"),
    [
        (
            "bounded_exhaustive_review",
            "I’m withholding a complete-scope conclusion.",
        ),
        (
            "absence_or_coverage_check",
            "I’m withholding an absence conclusion.",
        ),
        (
            "contradiction_review",
            "I’m withholding a contradiction-sensitive conclusion.",
        ),
        (
            "cross_source_comparison",
            "I’m withholding the requested conclusion.",
        ),
    ],
)
def test_blocked_response_uses_task_specific_withholding(
    task_shape,
    withholding,
):
    state = _rendering_state(
        task_shape=task_shape,
        status="unknown",
        evaluations=[
            {
                "requirement_id": "context-delivery",
                "requirement_kind": "context_delivery",
                "criticality": "material",
                "effective_outcome": "unknown",
            }
        ],
    )

    answer = enforce_final_answer("PRIVATE PROVIDER ANSWER", state)

    assert answer.endswith(withholding)
    assert "PRIVATE PROVIDER ANSWER" not in answer


def test_material_gap_rendering_is_bounded_deterministic_and_private():
    evaluations = [
        {
            "requirement_id": f"requirement-{index}",
            "requirement_kind": requirement_kind,
            "criticality": "material",
            "effective_outcome": "failed",
        }
        for index, requirement_kind in enumerate(
            [
                "targeted_evidence",
                "context_delivery",
                "contradiction_search",
                "counterevidence_coverage",
                "no_material_truncation",
            ]
        )
    ]
    state = _rendering_state(
        task_shape="contradiction_review",
        status="insufficient",
        evaluations=evaluations,
    )
    provider_text = (
        "PRIVATE SOURCE TEXT https://private.invalid credential=PRIVATE_SECRET"
    )

    first = enforce_final_answer(provider_text, state)
    second = enforce_final_answer("DIFFERENT PROVIDER TEXT", state)

    assert first == second
    assert "Additional material evidence requirements were also unresolved." in first
    assert "PRIVATE" not in first
    assert "https://" not in first
    assert first.endswith(
        "I’m withholding a contradiction-sensitive conclusion."
    )


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
        "next_steps",
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


def _next_step_test_state(
    *,
    sufficiency_status="insufficient",
    outcomes=None,
    capabilities=None,
    availability="available",
    declared_scope=None,
):
    plan = PlanResult.model_validate(_plan_response()["result"])
    scope = declared_scope or {
        "source_ids": ["source_a"],
        "source_categories": [],
        "exact_source_refs": [],
        "inventory_status": "complete_for_declared_scope",
        "time_scope_ref": None,
        "version_scope_ref": None,
        "domain_scope_ref": None,
        "project_scope_ref": None,
    }
    requirements = plan.declared_requirements
    outcome_by_kind = outcomes or {
        "targeted_evidence": "partial",
        "context_delivery": "satisfied",
    }
    sufficiency = SufficiencyResult.model_validate(
        {
            "evaluation_id": "evidence_eval_next",
            "task_shape": "targeted_lookup",
            "sufficiency_status": sufficiency_status,
            "evaluated_requirements": [
                {
                    **requirement.model_dump(mode="json"),
                    "effective_outcome": outcome_by_kind[
                        requirement.requirement_kind
                    ],
                }
                for requirement in requirements
            ],
            "reason_codes": ["material_requirement_not_satisfied"],
            "answer_constraints": [
                "qualify_conclusion",
                "disclose_limitations",
                "identify_unexamined_scope",
                "additional_acquisition_or_clarification_required",
                "withhold_unqualified_conclusion",
            ],
            "qualification_required": True,
            "additional_acquisition_required": True,
            "user_safe_summary": "More evidence is required.",
        }
    )
    return EvidenceAcquisitionState(
        enabled=True,
        attempted=True,
        status=sufficiency_status,
        shape=ShapeResult.model_validate(_shape_response()["result"]),
        inventory=DsaSourceListResponse.model_validate(
            {
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
                "sources": [
                    _source(
                        "source_a",
                        capabilities=capabilities or ["search", "fetch"],
                        status=(
                            "ready"
                            if availability == "available"
                            else "unavailable"
                        ),
                    )
                ],
            }
        ),
        declared_scope=scope,
        plan=plan,
        manifest_id="evidence_manifest_next",
        sufficiency=sufficiency,
        forced_answer=WITHHELD_ANSWER,
    )


def _next_step_result_payload(
    state,
    *,
    selected_next_step,
    conclusion_disposition,
    provider_disposition,
    reacquisition_guard="not_applicable",
    proposed_premise_digest=None,
    clarification_target=None,
):
    premise = build_current_acquisition_premise(state)
    return {
        "selection_id": "evidence_next_step_1",
        "evaluation_id": state.sufficiency.evaluation_id,
        "evidence_plan_id": state.plan.plan_id,
        "acquisition_manifest_id": state.manifest_id,
        "task_shape": state.plan.task_shape,
        "sufficiency_status": state.sufficiency.sufficiency_status,
        "selected_next_step": selected_next_step,
        "conclusion_disposition": conclusion_disposition,
        "provider_disposition": provider_disposition,
        "current_premise_digest": _acquisition_premise_digest(premise),
        "proposed_premise_digest": proposed_premise_digest,
        "reacquisition_guard": reacquisition_guard,
        "clarification_target": clarification_target,
        "unresolved_material_requirement_ids": sorted(
            evaluation.requirement_id
            for evaluation in state.sufficiency.evaluated_requirements
            if evaluation.criticality == "material"
            and evaluation.effective_outcome != "satisfied"
        ),
        "reason_codes": ["unsupported_conclusion_withheld"],
        "user_safe_summary": "A bounded next step was selected.",
    }


def test_current_acquisition_premise_uses_only_compiled_plan_inputs():
    state = _next_step_test_state()
    premise = build_current_acquisition_premise(state)
    reordered = EvidenceAcquisitionPremise.model_validate(
        {
            **premise.model_dump(mode="json"),
            "source_inventory": [
                {
                    **premise.source_inventory[0].model_dump(mode="json"),
                    "source_categories": list(
                        reversed(premise.source_inventory[0].source_categories)
                    ),
                    "capabilities": list(
                        reversed(premise.source_inventory[0].capabilities)
                    ),
                }
            ],
        }
    )

    assert premise.question_anchor_digest == state.plan.question_anchor_digest
    assert premise.task_shape == state.plan.task_shape
    assert premise.declared_scope.model_dump(mode="json") == state.declared_scope
    assert premise.selected_strategies == state.plan.selected_strategies
    assert _acquisition_premise_digest(reordered) == _acquisition_premise_digest(
        premise
    )
    serialized = json.dumps(premise.model_dump(mode="json"), sort_keys=True)
    for prohibited in ("request_id", "manifest_id", "provider", "PRIVATE"):
        assert prohibited not in serialized


@pytest.mark.parametrize(
    ("requirement_kind", "scope_updates", "expected"),
    [
        ("exact_authoritative_fetch", {}, "exact_reference"),
        ("historical_scope", {}, "time_scope"),
        (
            "complete_scope_coverage",
            {"source_ids": [], "source_categories": []},
            "source_scope",
        ),
        (
            "targeted_evidence",
            {"source_ids": [], "source_categories": []},
            None,
        ),
    ],
)
def test_clarification_target_is_derived_only_from_structural_uncertainty(
    requirement_kind,
    scope_updates,
    expected,
):
    scope = {
        "source_ids": ["source_a"],
        "source_categories": [],
        "exact_source_refs": [],
        "inventory_status": "complete_for_declared_scope",
        "time_scope_ref": None,
        "version_scope_ref": None,
        "domain_scope_ref": None,
        "project_scope_ref": None,
        **scope_updates,
    }
    state = _next_step_test_state(declared_scope=scope)
    state.sufficiency.evaluated_requirements = [
        RequirementEvaluation.model_validate(
            {
                "requirement_id": "uncertain-requirement",
                "requirement_kind": requirement_kind,
                "criticality": "material",
                "effective_outcome": "unknown",
            }
        )
    ]

    assert deterministic_clarification_target(state) == expected


@pytest.mark.asyncio
async def test_safe_exact_fetch_proposal_preserves_scope_and_uses_compiled_plan():
    state = _next_step_test_state()
    context_pack = {
        "items": [
            {"source_id": "source_a", "source_ref": "source_a:record_2"},
            {"source_id": "source_a", "source_ref": "source_a:record_1"},
        ]
    }

    class ProposalRuntime:
        def __init__(self):
            self.calls = []

        async def compile_evidence_plan(self, **kwargs):
            self.calls.append(kwargs)
            response = _exact_plan_response()
            response["result"]["plan_id"] = "evidence_plan_exact"
            return response

    runtime = ProposalRuntime()
    proposal = await compile_safe_exact_fetch_proposal(
        state=state,
        runtime=runtime,
        context_pack=context_pack,
        **SCOPE,
    )

    assert proposal is not None
    assert proposal.exact_reference == {
        "source_id": "source_a",
        "source_ref": "source_a:record_1",
    }
    assert proposal.declared_scope == {
        **state.declared_scope,
        "exact_source_refs": [proposal.exact_reference],
    }
    assert runtime.calls[0]["question_anchor"] == state.plan.question_anchor
    assert runtime.calls[0]["task_shape"] == state.plan.task_shape
    assert proposal.plan.selected_strategies == ["exact_fetch"]
    assert proposal.premise.selected_strategies == ["exact_fetch"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("capabilities", "availability", "source_id", "source_ref"),
    [
        (["search"], "available", "source_a", "source_a:record_1"),
        (
            ["search", "fetch"],
            "unavailable",
            "source_a",
            "source_a:record_1",
        ),
        (
            ["search", "fetch"],
            "available",
            "other_source",
            "other:record_1",
        ),
        (
            ["search", "fetch"],
            "available",
            "source_a",
            "https://private.example/record",
        ),
    ],
)
async def test_safe_exact_fetch_proposal_rejects_unsafe_or_ineligible_targets(
    capabilities,
    availability,
    source_id,
    source_ref,
):
    state = _next_step_test_state(
        capabilities=capabilities,
        availability=availability,
    )

    class Runtime:
        async def compile_evidence_plan(self, **kwargs):
            raise AssertionError("unsafe proposal must not compile")

    proposal = await compile_safe_exact_fetch_proposal(
        state=state,
        runtime=Runtime(),
        context_pack={
            "items": [{"source_id": source_id, "source_ref": source_ref}]
        },
        **SCOPE,
    )

    assert proposal is None


@pytest.mark.asyncio
async def test_next_step_selection_associates_result_and_blocks_provider():
    state = _next_step_test_state()

    class Runtime:
        def __init__(self):
            self.calls = []

        async def select_evidence_next_step(self, **kwargs):
            self.calls.append(kwargs)
            return {
                **SCOPE,
                "result": _next_step_result_payload(
                    state,
                    selected_next_step="withhold_unsupported_conclusion",
                    conclusion_disposition="requested_conclusion_withheld",
                    provider_disposition="blocked",
                ),
            }

    runtime = Runtime()
    result = await select_evidence_next_step(
        state=state,
        runtime=runtime,
        **SCOPE,
    )

    assert result is not None
    assert result.selected_next_step == "withhold_unsupported_conclusion"
    assert provider_allowed(state) is False
    assert runtime.calls[0]["current_premise"] == (
        build_current_acquisition_premise(state).model_dump(mode="json")
    )
    assert runtime.calls[0]["evaluated_requirements"] == [
        evaluation.model_dump(mode="json")
        for evaluation in state.sufficiency.evaluated_requirements
    ]
    assert len(state.next_step_history) == 1


@pytest.mark.parametrize(
    "updates",
    [
        {
            "selected_next_step": "perform_additional_acquisition",
            "reacquisition_guard": "unchanged_premise_blocked",
        },
        {
            "selected_next_step": "answer_within_declared_scope",
            "conclusion_disposition": "requested_conclusion_withheld",
            "provider_disposition": "blocked",
        },
        {
            "reacquisition_guard": "premise_already_attempted",
        },
        {
            "selected_next_step": "ask_narrow_clarification",
            "clarification_target": None,
        },
        {
            "selected_next_step": "disclose_unexamined_scope",
            "provider_disposition": "allowed",
        },
        {
            "unresolved_material_requirement_ids": [
                "targeted-evidence",
                "targeted-evidence",
            ]
        },
        {"reason_codes": ["unsupported_conclusion_withheld"] * 2},
    ],
)
def test_strict_next_step_model_rejects_contradictory_results(updates):
    state = _next_step_test_state()
    payload = _next_step_result_payload(
        state,
        selected_next_step="withhold_unsupported_conclusion",
        conclusion_disposition="requested_conclusion_withheld",
        provider_disposition="blocked",
    )
    payload.update(updates)

    with pytest.raises(ValidationError):
        NextStepResult.model_validate(payload)


@pytest.mark.parametrize(
    "guard",
    ["unchanged_premise_blocked", "premise_already_attempted"],
)
def test_guarded_qualified_partial_next_step_is_valid(guard):
    state = _next_step_test_state()
    payload = _next_step_result_payload(
        state,
        selected_next_step="provide_qualified_partial_answer",
        conclusion_disposition="qualified_partial_only",
        provider_disposition="allowed",
        reacquisition_guard=guard,
        proposed_premise_digest="sha256:" + ("1" * 64),
    )
    payload["reason_codes"] = [
        (
            "unchanged_acquisition_premise"
            if guard == "unchanged_premise_blocked"
            else "acquisition_premise_already_selected"
        ),
        "substantive_partial_evidence_available",
    ]

    result = NextStepResult.model_validate(payload)

    assert result.selected_next_step == "provide_qualified_partial_answer"
    assert result.provider_disposition == "allowed"
    assert result.conclusion_disposition == "qualified_partial_only"
    assert result.reacquisition_guard == guard


@pytest.mark.parametrize(
    "updates",
    [
        {"selected_next_step": "perform_additional_acquisition"},
        {"proposed_premise_digest": None},
        {"sufficiency_status": "sufficient_for_declared_scope"},
        {"sufficiency_status": "sufficient_with_limitations"},
    ],
)
@pytest.mark.parametrize(
    "guard",
    ["unchanged_premise_blocked", "premise_already_attempted"],
)
def test_blocked_reacquisition_guards_reject_invalid_acquisition_state(
    guard,
    updates,
):
    state = _next_step_test_state()
    payload = _next_step_result_payload(
        state,
        selected_next_step="provide_qualified_partial_answer",
        conclusion_disposition="qualified_partial_only",
        provider_disposition="allowed",
        reacquisition_guard=guard,
        proposed_premise_digest="sha256:" + ("1" * 64),
    )
    payload.update(updates)

    with pytest.raises(ValidationError):
        NextStepResult.model_validate(payload)


@pytest.mark.parametrize(
    "selected_next_step,conclusion_disposition,provider_disposition",
    [
        (
            "provide_qualified_partial_answer",
            "requested_conclusion_withheld",
            "blocked",
        ),
        (
            "disclose_unexamined_scope",
            "qualified_partial_only",
            "allowed",
        ),
        (
            "withhold_unsupported_conclusion",
            "qualified_partial_only",
            "allowed",
        ),
    ],
)
def test_guarded_fallback_keeps_step_specific_disposition_validation(
    selected_next_step,
    conclusion_disposition,
    provider_disposition,
):
    state = _next_step_test_state()
    payload = _next_step_result_payload(
        state,
        selected_next_step=selected_next_step,
        conclusion_disposition=conclusion_disposition,
        provider_disposition=provider_disposition,
        reacquisition_guard="unchanged_premise_blocked",
        proposed_premise_digest="sha256:" + ("1" * 64),
    )

    with pytest.raises(ValidationError):
        NextStepResult.model_validate(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "guard,guard_reason",
    [
        ("unchanged_premise_blocked", "unchanged_acquisition_premise"),
        (
            "premise_already_attempted",
            "acquisition_premise_already_selected",
        ),
    ],
)
@pytest.mark.parametrize(
    "sufficiency_status,outcomes",
    [
        (
            "insufficient",
            {
                "targeted_evidence": "partial",
                "context_delivery": "satisfied",
            },
        ),
        (
            "unknown",
            {
                "targeted_evidence": "satisfied",
                "context_delivery": "unknown",
            },
        ),
    ],
)
async def test_guarded_partial_selection_is_recorded_and_provider_free(
    guard,
    guard_reason,
    sufficiency_status,
    outcomes,
):
    state = _next_step_test_state(
        sufficiency_status=sufficiency_status,
        outcomes=outcomes,
    )

    class ProposalRuntime:
        async def compile_evidence_plan(self, **kwargs):
            return _exact_plan_response()

    proposal = await compile_safe_exact_fetch_proposal(
        state=state,
        runtime=ProposalRuntime(),
        context_pack={
            "items": [
                {
                    "source_id": "source_a",
                    "source_ref": "source_a:record_1",
                }
            ]
        },
        **SCOPE,
    )
    assert proposal is not None

    class SelectionRuntime:
        async def select_evidence_next_step(self, **kwargs):
            result = _next_step_result_payload(
                state,
                selected_next_step="provide_qualified_partial_answer",
                conclusion_disposition="qualified_partial_only",
                provider_disposition="allowed",
                reacquisition_guard=guard,
                proposed_premise_digest=_acquisition_premise_digest(
                    proposal.premise
                ),
            )
            result["reason_codes"] = [
                guard_reason,
                "substantive_partial_evidence_available",
            ]
            return {**SCOPE, "result": result}

    result = await select_evidence_next_step(
        state=state,
        runtime=SelectionRuntime(),
        proposal=proposal,
        **SCOPE,
    )

    assert result is not None
    assert result.reacquisition_guard == guard
    assert state.status == sufficiency_status
    assert state.next_step_failure is None
    assert state.next_step_history[0]["reacquisition_guard"] == guard
    assert provider_allowed(state) is False
    answer = enforce_final_answer("PRIVATE PROVIDER ANSWER", state)
    assert answer.startswith(
        "The available evidence establishes the requested targeted evidence"
    )
    assert "PRIVATE PROVIDER ANSWER" not in answer
    assert answer.endswith("I’m withholding the requested conclusion.")


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["provider_selected_target", "missing_local_proposal"])
async def test_next_step_selection_rejects_untrusted_follow_up(case):
    state = _next_step_test_state()
    if case == "provider_selected_target":
        state.sufficiency.evaluated_requirements = [
            RequirementEvaluation.model_validate(
                {
                    "requirement_id": "targeted-evidence",
                    "requirement_kind": "targeted_evidence",
                    "criticality": "material",
                    "effective_outcome": "unknown",
                }
            )
        ]

    class Runtime:
        async def select_evidence_next_step(self, **kwargs):
            if case == "provider_selected_target":
                result = _next_step_result_payload(
                    state,
                    selected_next_step="ask_narrow_clarification",
                    conclusion_disposition="requested_conclusion_withheld",
                    provider_disposition="blocked",
                    clarification_target="source_scope",
                )
            else:
                result = _next_step_result_payload(
                    state,
                    selected_next_step="perform_additional_acquisition",
                    conclusion_disposition="requested_conclusion_withheld",
                    provider_disposition="blocked",
                    reacquisition_guard="changed_premise_allowed",
                    proposed_premise_digest="sha256:" + ("1" * 64),
                )
            return {**SCOPE, "result": result}

    result = await select_evidence_next_step(
        state=state,
        runtime=Runtime(),
        proposal=None,
        clarification_target=(
            "exact_reference" if case == "provider_selected_target" else None
        ),
        **SCOPE,
    )

    assert result is None
    assert state.next_step is None
    assert state.next_step_failure == "dependency_failure"
    assert provider_allowed(state) is False


@pytest.mark.asyncio
async def test_safe_exact_fetch_proposal_rejects_non_exact_compiled_plan():
    state = _next_step_test_state()

    class Runtime:
        async def compile_evidence_plan(self, **kwargs):
            return _plan_response()

    proposal = await compile_safe_exact_fetch_proposal(
        state=state,
        runtime=Runtime(),
        context_pack={
            "items": [
                {
                    "source_id": "source_a",
                    "source_ref": "source_a:record_1",
                }
            ]
        },
        **SCOPE,
    )

    assert proposal is None


@pytest.mark.asyncio
async def test_changed_premise_authorization_promotes_exact_plan_once():
    state = _next_step_test_state()

    class Runtime:
        async def compile_evidence_plan(self, **kwargs):
            response = _exact_plan_response()
            response["result"]["plan_id"] = "evidence_plan_exact"
            return response

        async def select_evidence_next_step(self, **kwargs):
            proposed = EvidenceAcquisitionPremise.model_validate(
                kwargs["proposed_acquisition_premise"]
            )
            return {
                **SCOPE,
                "result": {
                    **_next_step_result_payload(
                        state,
                        selected_next_step="perform_additional_acquisition",
                        conclusion_disposition="requested_conclusion_withheld",
                        provider_disposition="blocked",
                        reacquisition_guard="changed_premise_allowed",
                        proposed_premise_digest=_acquisition_premise_digest(
                            proposed
                        ),
                    ),
                    "reason_codes": [
                        "changed_acquisition_premise_available"
                    ],
                },
            }

    runtime = Runtime()
    proposal = await compile_safe_exact_fetch_proposal(
        state=state,
        runtime=runtime,
        context_pack={
            "items": [
                {"source_id": "source_a", "source_ref": "source_a:record_1"}
            ]
        },
        **SCOPE,
    )
    assert proposal is not None
    await select_evidence_next_step(
        state=state,
        runtime=runtime,
        proposal=proposal,
        **SCOPE,
    )
    retain_initial_attempt_summary(
        state,
        context_pack={"items": [{"source_ref": "source_a:record_1"}]},
        retained_source_refs={"source_a:record_1"},
    )
    promote_exact_fetch_proposal(state, proposal)

    assert state.plan.plan_id == "evidence_plan_exact"
    assert state.exact_source_refs == [proposal.exact_reference]
    assert state.additional_acquisition_count == 1
    assert state.next_step_history[0]["additional_acquisition_executed"] is True
    with pytest.raises(ValueError, match="additional_acquisition_limit_reached"):
        promote_exact_fetch_proposal(state, proposal)
