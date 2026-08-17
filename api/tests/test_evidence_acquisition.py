from __future__ import annotations

import copy
import hashlib
import json
from decimal import ROUND_HALF_EVEN, Decimal, localcontext

import pytest
from models import ChatRequest
from pydantic import ValidationError
from services.evidence_acquisition import (
    AMBIGUOUS_ANSWER,
    BOUNDED_EXHAUSTIVE_CONTEXT_BUDGET,
    COMPARISON_SCOPE_SUFFIX,
    CONFIGURED_WORKSHEET_CONTEXT_MODE,
    HELPFUL_GROUNDED_RECOVERY_RESPONSE,
    MALFORMED_EVIDENCE_RESPONSE,
    NEXT_STEP_DEPENDENCY_ANSWER,
    TARGETED_SCOPE_SUFFIX,
    UNSUPPORTED_ANSWER,
    WITHHELD_ANSWER,
    AggregateNumericValidationError,
    AggregateSpec,
    DsaContextItem,
    DsaItem,
    DsaSourceListResponse,
    DsaStructuredFieldValues,
    EvidenceAcquisitionPremise,
    EvidenceAcquisitionState,
    EvidenceCandidateValidation,
    EvidenceInterpreterOutput,
    EvidenceSourceDescriptor,
    ExactFetchProposal,
    NextStepResult,
    PlanResult,
    RequirementEvaluation,
    SemanticInterpreterFailure,
    ShapeResult,
    SourceMatchResult,
    SufficiencyResult,
    ValidatedEvidenceExcerpt,
    _acquisition_premise_digest,
    _adapt_inventory,
    _build_acquisition_facts,
    _evidence_interpreter_inventory,
    _expected_sufficiency_constraints,
    _manifest_id,
    _resolve_declared_scope,
    _source_discovery_projection,
    _source_summaries,
    advisory_provider_allowed,
    begin_evidence_acquisition,
    bind_manifest_response,
    build_current_acquisition_premise,
    build_manifest_trace,
    compile_safe_exact_fetch_proposal,
    compute_aggregate_result,
    deterministic_clarification_target,
    enforce_final_answer,
    evaluate_acquisition_sufficiency,
    evidence_interpreter_messages,
    evidence_interpreter_response_format,
    execute_aggregate_values,
    execute_bounded_exhaustive_review,
    execute_exact_fetches,
    execute_hybrid_comparison,
    finalize_aggregate_conclusion,
    helpful_grounded_recovery_allowed,
    parse_evidence_interpreter_completion,
    promote_exact_fetch_proposal,
    provider_allowed,
    render_governed_evidence_answer,
    retain_initial_attempt_summary,
    select_evidence_next_step,
    suppress_manifest_identifiers,
    validate_bounded_exhaustive_context_pack_response,
    validate_configured_worksheet_response,
    validate_context_pack_response,
    validate_context_response,
    validate_evidence_response_candidate,
    validate_fetch_response,
    validate_structured_field_values_response,
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
    scope_refs=None,
    content_fields=None,
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
    if scope_refs is not None:
        source["scope_refs"] = scope_refs
    if content_fields is not None:
        source["content_fields"] = content_fields
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
    recovery_eligible=False,
):
    evaluations = evaluations or [
        {
            "requirement_id": "targeted-evidence",
            "requirement_kind": "targeted_evidence",
            "criticality": "material",
            "effective_outcome": "satisfied",
        },
        *(
            [
                {
                    "requirement_id": "context-delivery",
                    "requirement_kind": "context_delivery",
                    "criticality": "material",
                    "effective_outcome": "satisfied",
                }
            ]
            if recovery_eligible
            else []
        ),
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
        acquisition_facts=(
            [
                {
                    "requirement_id": evaluation["requirement_id"],
                    "outcome": evaluation["effective_outcome"],
                }
                for evaluation in evaluations
            ]
            if recovery_eligible
            else None
        ),
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
        auto_source_match=True,
    ):
        self.shape = shape or _shape_response()
        self.plan = plan or _plan_response()
        self.sufficiency_status = sufficiency_status
        self.auto_source_match = auto_source_match
        self.calls = []

    async def derive_evidence_shape(self, **kwargs):
        self.calls.append(("shape", kwargs))
        response = copy.deepcopy(self.shape)
        discovery = kwargs.get("task_context", {}).get("source_discovery")
        result = response.get("result", {})
        if self.auto_source_match and discovery is not None and "source_match" not in result:
            source_ids = sorted(source["source_id"] for source in discovery.get("sources", []))
            planned_ids = sorted(
                source_id
                for source_id in self.plan.get("result", {}).get("eligible_source_ids", [])
                if source_id in source_ids
            )
            if result.get("derivation_status") == "derived" and source_ids:
                result["source_match"] = {
                    "status": "matched",
                    "matched_source_ids": planned_ids or source_ids[:1],
                    "reason_codes": ["source_id_match"],
                }
            elif result.get("derivation_status") == "ambiguous":
                result["source_match"] = {
                    "status": "ambiguous",
                    "matched_source_ids": [],
                    "reason_codes": ["multiple_possible_source_matches"],
                }
            else:
                result["source_match"] = {
                    "status": ("no_match" if source_ids else "inventory_unavailable"),
                    "matched_source_ids": [],
                    "reason_codes": [
                        ("no_source_specific_match" if source_ids else "inventory_unavailable")
                    ],
                }
        return response

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


class SequentialShapeRuntime(FakeRuntime):
    def __init__(self, shapes, *, plan=None):
        super().__init__(shape=shapes[0], plan=plan, auto_source_match=False)
        self.shapes = [copy.deepcopy(shape) for shape in shapes]

    async def derive_evidence_shape(self, **kwargs):
        self.calls.append(("shape", kwargs))
        return copy.deepcopy(self.shapes.pop(0))


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
        self.list_request_ids = []
        self.operation_request_ids = []
        self.inventory_metadata = dict(inventory_metadata or {})
        self.source_response = source_response
        self.fetch_responses = list(fetch_responses or [])
        self.context_responses = list(context_responses or [])

    async def list_sources(self, *, request_id=None):
        self.list_request_ids.append(request_id)
        self.calls.append("list_sources")
        if self.source_response is not None:
            return copy.deepcopy(self.source_response)
        return {
            **self.inventory_metadata,
            "sources": copy.deepcopy(self.sources),
        }

    async def fetch_source(self, **kwargs):
        self.operation_request_ids.append(kwargs.pop("request_id", None))
        self.calls.append(("fetch_source", kwargs))
        response = self.fetch_responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def context_source(self, **kwargs):
        self.operation_request_ids.append(kwargs.pop("request_id", None))
        self.calls.append(("context_source", kwargs))
        response = self.context_responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _shape_with_source_match(
    *,
    status: str,
    matched_source_ids: list[str] | None = None,
    derivation_status: str = "derived",
) -> dict[str, object]:
    response = _shape_response(status=derivation_status)
    response["result"]["source_match"] = {
        "status": status,
        "matched_source_ids": matched_source_ids or [],
        "reason_codes": [
            {
                "matched": "source_id_match",
                "no_match": "no_source_specific_match",
                "ambiguous": "multiple_possible_source_matches",
                "inventory_unavailable": "inventory_unavailable",
            }[status]
        ],
    }
    return response


def _future_aggregate_shape(
    *, function="median", field_name="Fuel (L)"
):
    response = _shape_with_source_match(
        status="matched",
        matched_source_ids=["source_a"],
    )
    response["result"].update(
        {
            "task_shape": "aggregate",
            "candidate_task_shapes": ["aggregate"],
            "reason_codes": ["semantic_operation_hint"],
            "aggregate_spec": {"function": function, "field_name": field_name},
        }
    )
    response["result"]["source_match"]["reason_codes"] = [
        "semantic_candidate_validated"
    ]
    return response


def _future_aggregate_plan(*, function="median", field_name="Fuel (L)"):
    response = _plan_response(
        requirements=[
            {
                "requirement_id": "complete-scope",
                "requirement_kind": "complete_scope_coverage",
                "criticality": "material",
            },
            {
                "requirement_id": "context-delivery",
                "requirement_kind": "context_delivery",
                "criticality": "material",
            },
            {
                "requirement_id": "no-truncation",
                "requirement_kind": "no_material_truncation",
                "criticality": "material",
            },
        ]
    )
    response["result"].update(
        {
            "task_shape": "aggregate",
            "completeness_expectation": "complete_for_declared_scope",
            "eligible_source_ids": ["source_a"],
            "authoritative_source_ids": [],
            "selected_strategies": ["structured_field_values"],
            "aggregate_spec": {"function": function, "field_name": field_name},
        }
    )
    return response


@pytest.mark.parametrize(
    "function",
    ["median", "mean", "count", "sum", "minimum", "maximum"],
)
def test_aggregate_spec_accepts_closed_functions(function):
    spec = AggregateSpec.model_validate(
        {"function": function, "field_name": "Fuel (L)"}
    )
    assert spec.model_dump(mode="json") == {
        "function": function,
        "field_name": "Fuel (L)",
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"function": "average", "field_name": "Fuel (L)"},
        {"function": "median", "field_name": ""},
        {"function": "median", "field_name": " Fuel (L)"},
        {"function": "median", "field_name": "Fuel (L) "},
        {"function": "median", "field_name": "Fuel\n(L)"},
        {"function": "median", "field_name": "x" * 121},
        {"function": "median", "field_name": "Fuel (L)", "extra": True},
    ],
)
def test_aggregate_spec_rejects_malformed_contract(payload):
    with pytest.raises(ValidationError):
        AggregateSpec.model_validate(payload)


def test_shape_result_accepts_only_derived_aggregate_spec():
    aggregate = ShapeResult.model_validate(_future_aggregate_shape()["result"])
    assert aggregate.aggregate_spec == AggregateSpec(
        function="median", field_name="Fuel (L)"
    )

    missing = copy.deepcopy(_future_aggregate_shape()["result"])
    missing.pop("aggregate_spec")
    with pytest.raises(ValidationError):
        ShapeResult.model_validate(missing)

    nonaggregate_shapes = [
        "targeted_lookup",
        "bounded_exhaustive_review",
        "cross_source_comparison",
        "contradiction_review",
        "absence_or_coverage_check",
        "historical_reconstruction",
        "recommendation_or_decision_support",
    ]
    invalid_outcomes = [
        _shape_response(status="derived", shape=shape)["result"]
        for shape in nonaggregate_shapes
    ]
    invalid_outcomes.extend(
        [
            _shape_response(status="ambiguous")["result"],
            _shape_response(status="not_applicable")["result"],
        ]
    )
    for shape in invalid_outcomes:
        shape["aggregate_spec"] = {
            "function": "median",
            "field_name": "Fuel (L)",
        }
        with pytest.raises(ValidationError):
            ShapeResult.model_validate(shape)

    legacy = ShapeResult.model_validate(_shape_response()["result"])
    assert "aggregate_spec" not in legacy.model_dump(mode="json")


def test_plan_result_accepts_only_aggregate_plan_spec():
    aggregate = PlanResult.model_validate(_future_aggregate_plan()["result"])
    assert aggregate.selected_strategies == ["structured_field_values"]
    assert aggregate.aggregate_spec.field_name == "Fuel (L)"

    missing = copy.deepcopy(_future_aggregate_plan()["result"])
    missing.pop("aggregate_spec")
    with pytest.raises(ValidationError):
        PlanResult.model_validate(missing)

    nonaggregate = _plan_response()["result"]
    nonaggregate["aggregate_spec"] = {
        "function": "median",
        "field_name": "Fuel (L)",
    }
    with pytest.raises(ValidationError):
        PlanResult.model_validate(nonaggregate)

    malformed = copy.deepcopy(_future_aggregate_plan()["result"])
    malformed["aggregate_spec"]["field_name"] = " Fuel (L)"
    with pytest.raises(ValidationError):
        PlanResult.model_validate(malformed)

    legacy = PlanResult.model_validate(_plan_response()["result"])
    assert "aggregate_spec" not in legacy.model_dump(mode="json")


@pytest.mark.parametrize("status", ["ready", "ready_with_limitations", "unsupported"])
def test_aggregate_plan_statuses_and_field_limitation_are_contract_compatible(status):
    payload = _future_aggregate_plan()["result"]
    payload["plan_status"] = status
    if status == "ready_with_limitations":
        payload["declared_requirements"].append(
            {
                "requirement_id": "optional-field",
                "requirement_kind": "targeted_evidence",
                "criticality": "optional",
            }
        )
        payload["limitation_codes"] = ["aggregate_field_unavailable"]

    plan = PlanResult.model_validate(payload)

    assert plan.plan_status == status
    assert plan.aggregate_spec.field_name == "Fuel (L)"


def test_plan_source_descriptor_content_fields_are_bounded_exact_and_optional():
    base = {
        "source_id": "source_a",
        "source_categories": ["records"],
        "capabilities": ["targeted_retrieval"],
        "availability": "available",
        "authority_role": "authoritative",
    }
    legacy = EvidenceSourceDescriptor.model_validate(base)
    exact = EvidenceSourceDescriptor.model_validate(
        {**base, "content_fields": [" Fuel (L)", "Date", "Odometer"]}
    )
    assert "content_fields" not in legacy.model_dump(mode="json")
    assert exact.content_fields == [" Fuel (L)", "Date", "Odometer"]

    invalid_values = [
        None,
        [f"field-{index:02d}" for index in range(25)],
        [""],
        ["Field\nName"],
        ["Date", "Date"],
        ["Odometer", "Date"],
        [1],
        ["x" * 121],
    ]
    for content_fields in invalid_values:
        with pytest.raises(ValidationError):
            EvidenceSourceDescriptor.model_validate(
                {**base, "content_fields": content_fields}
            )


def test_adapt_inventory_adds_content_fields_only_when_explicitly_requested():
    source = _source(
        "source_a",
        content_fields=["Odometer", " Fuel (L)", "Date"],
    )
    inventory = DsaSourceListResponse.model_validate({"sources": [source]})

    legacy = _adapt_inventory(inventory)
    aggregate = _adapt_inventory(inventory, include_content_fields=True)

    assert "content_fields" not in legacy[0]
    assert aggregate[0]["content_fields"] == [
        " Fuel (L)",
        "Date",
        "Odometer",
    ]
    assert set(aggregate[0]) == {
        "source_id",
        "source_categories",
        "capabilities",
        "availability",
        "authority_role",
        "content_fields",
    }


@pytest.mark.asyncio
async def test_future_aggregate_contract_reaches_supported_execution_boundary():
    runtime = FakeRuntime(
        shape=_future_aggregate_shape(),
        plan=_future_aggregate_plan(),
        auto_source_match=False,
    )
    dsa = FakeDsa(
        [
            _source(
                "source_a",
                capabilities=["search", "context"],
                content_fields=["Odometer", "Fuel (L)", "Date"],
            )
        ],
        inventory_metadata={
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
        },
    )

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={"source_ids": ["source_a"]},
        **SCOPE,
    )

    assert state.status == "acquisition_ready"
    assert state.supported_aggregate_path is True
    assert state.shape.task_shape == "aggregate"
    assert state.declared_scope["source_ids"] == ["source_a"]
    assert state.plan.aggregate_spec == state.shape.aggregate_spec
    assert state.plan.selected_strategies == ["structured_field_values"]
    plan_call = [payload for name, payload in runtime.calls if name == "plan"]
    assert plan_call[0]["aggregate_spec"] == {
        "function": "median",
        "field_name": "Fuel (L)",
    }
    assert plan_call[0]["source_inventory"][0]["content_fields"] == [
        "Date",
        "Fuel (L)",
        "Odometer",
    ]
    assert [name for name, _ in runtime.calls] == ["shape", "plan"]
    assert dsa.calls == ["list_sources"]
    assert state.sufficiency is None
    assert state.next_step is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("function", "field_name"),
    [("mean", "Fuel (L)"), ("median", "Fuel (Gallons)")],
)
async def test_future_aggregate_plan_spec_mismatch_fails_closed(
    function,
    field_name,
):
    runtime = FakeRuntime(
        shape=_future_aggregate_shape(),
        plan=_future_aggregate_plan(function=function, field_name=field_name),
        auto_source_match=False,
    )
    dsa = FakeDsa(
        [_source("source_a", content_fields=["Fuel (L)"])],
        inventory_metadata={
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
        },
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={"source_ids": ["source_a"]},
        **SCOPE,
    )

    assert state.status == "plan_dependency_failed"
    assert state.plan is None
    assert dsa.calls == ["list_sources"]
    assert [name for name, _ in runtime.calls] == ["shape", "plan"]


@pytest.mark.asyncio
async def test_future_aggregate_authority_details_remain_private_in_trace():
    sentinel = "Fuel (L)"
    runtime = FakeRuntime(
        shape=_future_aggregate_shape(field_name=sentinel),
        plan=_future_aggregate_plan(field_name=sentinel),
        auto_source_match=False,
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [_source("source_a", content_fields=[sentinel])],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context={"source_ids": ["source_a"]},
        **SCOPE,
    )
    manifest = build_manifest_trace(
        state=state,
        context_pack=None,
        dsa_trace={"called": False, "status": "not_called"},
        retained_source_refs=set(),
    )
    serialized = json.dumps(manifest, sort_keys=True)
    assert sentinel not in serialized
    assert "content_fields" not in serialized
    assert "aggregate_spec" not in serialized
    assert manifest["shape"]["task_shape"] == "aggregate"
    assert manifest["plan"]["selected_strategies"] == ["structured_field_values"]


def _aggregate_ready_state(*, function="median", field_name="Reading"):
    return EvidenceAcquisitionState(
        enabled=True,
        attempted=True,
        status="acquisition_ready",
        request_id="rid",
        shape=ShapeResult.model_validate(
            _future_aggregate_shape(
                function=function,
                field_name=field_name,
            )["result"]
        ),
        inventory=DsaSourceListResponse.model_validate(
            {
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
                "sources": [
                    _source(
                        "source_a",
                        capabilities=["search", "context"],
                        content_fields=["Entry", field_name],
                    )
                ],
            }
        ),
        declared_scope={
            "source_ids": ["source_a"],
            "source_categories": [],
            "exact_source_refs": [],
            "inventory_status": "complete_for_declared_scope",
            "time_scope_ref": None,
            "version_scope_ref": None,
            "domain_scope_ref": None,
            "project_scope_ref": None,
        },
        plan=PlanResult.model_validate(
            _future_aggregate_plan(
                function=function,
                field_name=field_name,
            )["result"]
        ),
        manifest_id="evidence_manifest_0123456789abcdef0123456789abcdef",
    )


def _structured_field_response(
    values,
    *,
    field_name="Reading",
    source_id="source_a",
    result=True,
    truncated=False,
    errors=None,
):
    results = []
    if result:
        results.append(
            {
                "result_id": "structured-result",
                "source_type": "google_sheets",
                "source_id": source_id,
                "source_name": "Private source",
                "source_ref": f"google_sheets:{source_id}:Measurements!A2:C6",
                "retrieved_at": "2026-07-17T00:00:00Z",
                "source_modified_at": None,
                "cache_status": "live",
                "title": "Configured field values",
                "content_type": "structured_field_values",
                "text": "Retrieved configured records.",
                "url": None,
                "confidence": "high",
                "raw": None,
                "structured_data": {
                    "kind": "field_values",
                    "field_name": field_name,
                    "record_count": len(values),
                    "non_empty_value_count": sum(value is not None for value in values),
                    "values": values,
                },
                "available_context": [],
                "warnings": [],
            }
        )
    return {
        "query_id": "structured-query",
        "answerable": bool(results),
        "confidence": "high" if results else "none",
        "retrieval_mode": "context",
        "results": results,
        "warnings": [],
        "errors": errors or [],
        "budget": {
            "max_results": None,
            "returned_results": len(results),
            "estimated_bytes": 200 if results else 0,
            "truncated": truncated,
        },
    }


@pytest.mark.parametrize(
    "mutation",
    [
        "multi_source",
        "exact_ref",
        "wrong_strategy",
        "limitation",
        "missing_requirement",
        "source_unavailable",
        "missing_context",
        "missing_field",
        "shape_source_mismatch",
    ],
)
def test_supported_aggregate_path_rejects_every_malformed_authority(mutation):
    state = _aggregate_ready_state()
    if mutation == "multi_source":
        state.plan.eligible_source_ids.append("source_b")
    elif mutation == "exact_ref":
        state.declared_scope["exact_source_refs"] = [
            {"source_id": "source_a", "source_ref": "source_a:item"}
        ]
    elif mutation == "wrong_strategy":
        state.plan.selected_strategies = ["targeted_retrieval"]
    elif mutation == "limitation":
        state.plan.limitation_codes = ["aggregate_field_unavailable"]
    elif mutation == "missing_requirement":
        state.plan.declared_requirements.pop()
    elif mutation == "source_unavailable":
        state.inventory.sources[0].status = "unavailable"
    elif mutation == "missing_context":
        state.inventory.sources[0].capabilities = ["search"]
    elif mutation == "missing_field":
        state.inventory.sources[0].content_fields = ["Entry"]
    else:
        state.shape.source_match.matched_source_ids = ["source_b"]

    assert state.supported_aggregate_path is False


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        ("satisfied", "satisfied"),
        ("unknown", "unknown"),
        ("failed", "failed"),
        ("filtered", "filtered"),
        ("truncated", "truncated"),
        ("unsupported", "unsupported"),
    ],
)
def test_aggregate_outcome_maps_uniformly_to_material_requirements(outcome, expected):
    state = _aggregate_ready_state()
    facts = _build_acquisition_facts(
        plan=state.plan,
        context_pack=None,
        dsa_trace={"status": "empty"},
        retained_source_refs=set(),
        aggregate_execution={"outcome": outcome},
    )
    assert {fact["outcome"] for fact in facts} == {expected}


def test_aggregate_current_premise_binds_spec_and_content_fields():
    state = _aggregate_ready_state()
    premise = build_current_acquisition_premise(state)

    assert premise.aggregate_spec == state.plan.aggregate_spec
    assert premise.source_inventory[0].content_fields == ["Entry", "Reading"]
    assert _acquisition_premise_digest(premise) == (
        "sha256:c475d6003f2a6f51f235ccc9f338324c17e506eff29fa7d7aae5071a82cdd81a"
    )
    legacy = build_current_acquisition_premise(_next_step_test_state())
    assert "aggregate_spec" not in legacy.model_dump(mode="json")
    assert "content_fields" not in legacy.source_inventory[0].model_dump(mode="json")


def test_structured_field_value_contract_is_strict_and_counted():
    structured = DsaStructuredFieldValues.model_validate(
        {
            "kind": "field_values",
            "field_name": "Reading",
            "record_count": 3,
            "non_empty_value_count": 2,
            "values": ["10", None, "20"],
        }
    )
    assert structured.values == ["10", None, "20"]
    for update in (
        {"record_count": 2},
        {"non_empty_value_count": 1},
        {"field_name": " Reading"},
        {"values": ["10", 2, None]},
        {"extra": True},
    ):
        payload = structured.model_dump(mode="json")
        payload.update(update)
        with pytest.raises(ValidationError):
            DsaStructuredFieldValues.model_validate(payload)

    legacy_item = _context_response()["results"][0]
    serialized_legacy = DsaContextItem.model_validate(legacy_item).model_dump(mode="json")
    assert "structured_data" not in serialized_legacy

    legacy_item["structured_data"] = structured.model_dump(mode="json")
    with pytest.raises(ValidationError):
        DsaContextItem.model_validate(legacy_item)

    structured_item = _structured_field_response(["10", None, "20"])["results"][0]
    serialized_structured = DsaContextItem.model_validate(structured_item).model_dump(mode="json")
    assert serialized_structured["structured_data"]["kind"] == "field_values"


def test_structured_response_requires_exact_source_field_and_shape():
    valid, outcome = validate_structured_field_values_response(
        _structured_field_response(["10", None, "20"]),
        expected_source_id="source_a",
        expected_field_name="Reading",
    )
    assert outcome == "satisfied"
    assert valid.results[0].raw is None
    assert valid.results[0].available_context == []

    for response, expected in (
        (_structured_field_response([], result=False), "unknown"),
        (_structured_field_response(["10"], truncated=True), "truncated"),
        (_structured_field_response(["10"], source_id="source_b"), "filtered"),
        (_structured_field_response(["10"], field_name="Other"), "filtered"),
    ):
        _, outcome = validate_structured_field_values_response(
            response,
            expected_source_id="source_a",
            expected_field_name="Reading",
        )
        assert outcome == expected


@pytest.mark.parametrize(
    ("function", "expected"),
    [
        ("median", 'Median for "Reading": 27.875 (4 non-empty values across 5 records).'),
        ("mean", 'Mean for "Reading": 30.40625 (4 non-empty values across 5 records).'),
        ("count", 'Count for "Reading": 4 non-empty values across 5 records.'),
        ("sum", 'Sum for "Reading": 121.625 (4 non-empty values across 5 records).'),
        ("minimum", 'Minimum for "Reading": 10.125 (4 non-empty values across 5 records).'),
        ("maximum", 'Maximum for "Reading": 55.75 (4 non-empty values across 5 records).'),
    ],
)
def test_deterministic_aggregate_numeric_matrix(function, expected):
    structured = DsaStructuredFieldValues.model_validate(
        {
            "kind": "field_values",
            "field_name": "Reading",
            "record_count": 5,
            "non_empty_value_count": 4,
            "values": ["10.125", "20.25", None, "35.5", "55.75"],
        }
    )
    result = compute_aggregate_result(
        aggregate_spec=AggregateSpec(function=function, field_name="Reading"),
        structured_data=structured,
    )
    assert result.answer == expected


def test_aggregate_decimal_edges_and_count_semantics():
    def compute(function, values):
        structured = DsaStructuredFieldValues(
            kind="field_values",
            field_name="Reading",
            record_count=len(values),
            non_empty_value_count=sum(value is not None for value in values),
            values=values,
        )
        return compute_aggregate_result(
            aggregate_spec=AggregateSpec(function=function, field_name="Reading"),
            structured_data=structured,
        )

    assert compute("median", ["-1", "+1", "00012.500"]).rendered_value == "1"
    assert compute("median", ["-1", "+1"]).rendered_value == "0"
    assert compute("sum", ["-0.000"]).rendered_value == "0"
    assert compute("count", ["ready", None, "pending"]).answer == (
        'Count for "Reading": 2 non-empty values across 3 records.'
    )
    repeating = compute("mean", ["1", "0", "0"])
    assert repeating.approximate is True
    assert repeating.answer == (
        'Mean for "Reading": approximately '
        "0.3333333333333333333333333333333333 "
        "(3 non-empty values across 3 records)."
    )


def _max_bound_decimal_inputs():
    return "9" * 120, "0." + "0" * 117 + "1"


def _high_precision_canonical(value):
    rendered = format(value, "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def test_max_bound_sum_preserves_complete_fractional_tail():
    large, tiny = _max_bound_decimal_inputs()
    values = [large] * 249 + [tiny]
    structured = DsaStructuredFieldValues(
        kind="field_values",
        field_name="Reading",
        record_count=250,
        non_empty_value_count=250,
        values=values,
    )
    with localcontext() as oracle_context:
        oracle_context.prec = 300
        oracle_context.rounding = ROUND_HALF_EVEN
        expected = _high_precision_canonical(sum((Decimal(value) for value in values), Decimal(0)))
    result = compute_aggregate_result(
        aggregate_spec=AggregateSpec(function="sum", field_name="Reading"),
        structured_data=structured,
    )
    assert result.rendered_value == expected
    assert result.rendered_value.endswith("0" * 117 + "1")


def test_max_bound_even_median_is_exact_beyond_160_digits():
    large, tiny = _max_bound_decimal_inputs()
    structured = DsaStructuredFieldValues(
        kind="field_values",
        field_name="Reading",
        record_count=2,
        non_empty_value_count=2,
        values=[large, tiny],
    )
    with localcontext() as oracle_context:
        oracle_context.prec = 300
        oracle_context.rounding = ROUND_HALF_EVEN
        expected = _high_precision_canonical((Decimal(large) + Decimal(tiny)) / Decimal(2))
    result = compute_aggregate_result(
        aggregate_spec=AggregateSpec(function="median", field_name="Reading"),
        structured_data=structured,
    )
    assert result.rendered_value == expected


def test_max_bound_mean_uses_exact_numerator_before_precision_34_division():
    _, tiny = _max_bound_decimal_inputs()
    large = "9" * 119
    values = [large, tiny, "-" + large]
    structured = DsaStructuredFieldValues(
        kind="field_values",
        field_name="Reading",
        record_count=3,
        non_empty_value_count=3,
        values=values,
    )
    with localcontext() as oracle_context:
        oracle_context.prec = 300
        oracle_context.rounding = ROUND_HALF_EVEN
        numerator = sum((Decimal(value) for value in values), Decimal(0))
    with localcontext() as mean_context:
        mean_context.prec = 34
        mean_context.rounding = ROUND_HALF_EVEN
        expected = _high_precision_canonical(numerator / Decimal(3))
    result = compute_aggregate_result(
        aggregate_spec=AggregateSpec(function="mean", field_name="Reading"),
        structured_data=structured,
    )
    assert result.rendered_value == expected
    assert result.approximate is True


@pytest.mark.parametrize(
    "invalid",
    ["1e3", "1,000", "$12", "12 L", "NaN", "Infinity", "", " 12", "12 ", "1" * 121],
)
def test_numeric_aggregate_rejects_every_invalid_non_null_value(invalid):
    structured = DsaStructuredFieldValues(
        kind="field_values",
        field_name="Reading",
        record_count=2,
        non_empty_value_count=2,
        values=["10", invalid],
    )
    with pytest.raises(AggregateNumericValidationError):
        compute_aggregate_result(
            aggregate_spec=AggregateSpec(function="mean", field_name="Reading"),
            structured_data=structured,
        )


def test_all_null_numeric_filters_but_count_is_zero():
    structured = DsaStructuredFieldValues(
        kind="field_values",
        field_name="Reading",
        record_count=2,
        non_empty_value_count=0,
        values=[None, None],
    )
    assert (
        compute_aggregate_result(
            aggregate_spec=AggregateSpec(function="count", field_name="Reading"),
            structured_data=structured,
        ).rendered_value
        == "0"
    )
    with pytest.raises(AggregateNumericValidationError):
        compute_aggregate_result(
            aggregate_spec=AggregateSpec(function="sum", field_name="Reading"),
            structured_data=structured,
        )


@pytest.mark.asyncio
async def test_aggregate_executor_uses_direct_complete_vector_and_keeps_it_private():
    state = _aggregate_ready_state()
    dsa = FakeDsa(
        [],
        context_responses=[_structured_field_response(["10.125", "20.25", None, "35.5", "55.75"])],
    )
    pack, trace = await execute_aggregate_values(
        state=state,
        dsa=dsa,
        dsa_trace={"called": False, "status": "deferred_for_evidence_governance"},
    )

    assert pack is None
    assert state.aggregate_result == (
        'Median for "Reading": 27.875 (4 non-empty values across 5 records).'
    )
    assert dsa.calls == [
        (
            "context_source",
            {
                "source_id": "source_a",
                "context_mode": "configured_field_values",
                "field_name": "Reading",
                "budget": {
                    "max_rows": 250,
                    "max_bytes": 5_000_000,
                    "max_text_chars": 500,
                },
            },
        )
    ]
    assert trace["context_pack_call_count"] == 0
    assert trace["context_expansion_call_count"] == 1
    assert state.aggregate_execution["record_count"] == 5
    assert state.aggregate_execution["non_empty_value_count"] == 4
    serialized = json.dumps((trace, state.aggregate_delivery_identity), sort_keys=True)
    assert "55.75" not in serialized
    assert "Reading" not in serialized
    assert state.aggregate_delivery_identity["structured_data_digest"].startswith("sha256:")


def test_aggregate_facts_distinguish_numeric_filter_and_policy_blocks_result():
    state = _aggregate_ready_state()
    state.aggregate_result = 'Median for "Reading": 27.875 (4 non-empty values across 5 records).'
    state.aggregate_execution = {
        "outcome": "filtered",
        "numeric_validation_failed": True,
    }
    facts = _build_acquisition_facts(
        plan=state.plan,
        context_pack=None,
        dsa_trace={"status": "empty"},
        retained_source_refs=set(),
        aggregate_execution=state.aggregate_execution,
    )
    assert {fact["requirement_id"]: fact["outcome"] for fact in facts} == {
        "complete-scope": "satisfied",
        "context-delivery": "filtered",
        "no-truncation": "satisfied",
    }
    finalize_aggregate_conclusion(state)
    assert state.forced_answer == WITHHELD_ANSWER
    trace = build_manifest_trace(
        state=state,
        context_pack=None,
        dsa_trace={"called": True, "status": "empty"},
        retained_source_refs=set(),
    )
    serialized = json.dumps(trace, sort_keys=True)
    assert "Reading" not in serialized
    assert "27.875" not in serialized
    assert "content_fields" not in serialized


def test_source_discovery_projection_is_canonical_bounded_and_private():
    source_b = _source(
        "source_b",
        tags=["zeta", "alpha"],
        capabilities=["search", "profile"],
        enabled=False,
        status="ready",
        scope_refs={"project": "public-project", "time": "fy2026"},
        content_fields=["Zulu Field", " Fuel (L)", "Alpha Field"],
    )
    source_b["last_error"] = "PRIVATE-LAST-ERROR"
    source_b["sensitivity"] = "restricted"
    source_a = _source(
        "source_a",
        capabilities=["context", "fetch"],
        status="unavailable",
    )
    source_a["last_error"] = "PRIVATE-SECOND-ERROR"
    projection = _source_discovery_projection(
        DsaSourceListResponse.model_validate(
            {
                "inventory_scope": "configured_sources",
                "inventory_status": "partial",
                "sources": [source_b, source_a],
            }
        )
    )

    assert projection["inventory_status"] == "partial"
    assert [source["source_id"] for source in projection["sources"]] == [
        "source_a",
        "source_b",
    ]
    assert projection["sources"][0]["availability"] == "unavailable"
    assert projection["sources"][1]["availability"] == "disabled"
    assert projection["sources"][1]["domain_tags"] == ["alpha", "zeta"]
    assert projection["sources"][1]["capabilities"] == ["profile", "search"]
    assert projection["sources"][1]["scope_refs"] == {
        "time": "fy2026",
        "project": "public-project",
    }
    assert projection["sources"][1]["content_fields"] == [
        " Fuel (L)",
        "Alpha Field",
        "Zulu Field",
    ]
    serialized = json.dumps(projection, sort_keys=True)
    for prohibited in (
        "sensitivity",
        "access_mode",
        "last_checked_at",
        "last_error",
        "PRIVATE-LAST-ERROR",
        "PRIVATE-SECOND-ERROR",
    ):
        assert prohibited not in serialized


def test_source_discovery_projection_legacy_inventory_is_unknown_and_omits_scope():
    projection = _source_discovery_projection(
        DsaSourceListResponse.model_validate({"sources": [_source("source_a")]})
    )

    assert projection["inventory_status"] == "unknown"
    assert "scope_refs" not in projection["sources"][0]
    assert "content_fields" not in projection["sources"][0]


def _semantic_completion(payload):
    return {"choices": [{"message": {"content": json.dumps(payload)}}]}


def test_dsa_source_inventory_accepts_optional_bounded_content_fields():
    current = DsaSourceListResponse.model_validate({"sources": [_source("source_a")]})
    enriched_source = _source("source_a")
    enriched_source["content_fields"] = [
        "Start Time",
        "Remaining Fuel",
        "Comments/Repair Notes",
    ]
    enriched = DsaSourceListResponse.model_validate({"sources": [enriched_source]})

    assert current.sources[0].content_fields is None
    assert enriched.sources[0].content_fields == enriched_source["content_fields"]


@pytest.mark.parametrize(
    "content_fields",
    [
        None,
        [f"field-{index}" for index in range(25)],
        ["   "],
        ["Repeated", "Repeated"],
        ["x" * 121],
        ["Unsafe\nField"],
        [{"name": "Nested"}],
    ],
)
def test_dsa_source_inventory_rejects_malformed_content_fields(content_fields):
    source = _source("source_a")
    source["content_fields"] = content_fields

    with pytest.raises(ValidationError):
        DsaSourceListResponse.model_validate({"sources": [source]})


def test_evidence_interpreter_inventory_is_canonical_and_private():
    source_b = _source(
        "source_b",
        tags=["zeta", "alpha"],
        capabilities=["search", "profile"],
        scope_refs={"project": "harbor", "time": "fy2026"},
    )
    source_b["content_fields"] = ["Zulu Field", "Alpha Field"]
    source_b["last_error"] = "PRIVATE-ERROR-SENTINEL"
    source_a = _source("source_a", capabilities=["context", "fetch"])
    inventory = DsaSourceListResponse.model_validate(
        {"sources": [source_b, source_a]}
    )

    projected = _evidence_interpreter_inventory(inventory)
    messages = evidence_interpreter_messages(
        task_text="Which configured record applies?",
        source_list=inventory,
    )

    assert [source["source_id"] for source in projected] == ["source_a", "source_b"]
    assert projected[1]["domain_tags"] == ["alpha", "zeta"]
    assert projected[1]["capabilities"] == ["profile", "search"]
    assert projected[1]["content_fields"] == ["Alpha Field", "Zulu Field"]
    assert list(projected[1]["scope_refs"]) == ["time", "project"]
    serialized = json.dumps(messages, sort_keys=True)
    for forbidden in (
        "last_error",
        "PRIVATE-ERROR-SENTINEL",
        "connector",
        "sensitivity",
        "access_mode",
        "authority_role",
    ):
        assert forbidden not in serialized
    system_instruction = messages[0]["content"]
    assert "aggregate_function" in system_instruction
    assert "aggregate_field_name" in system_instruction
    assert "copy aggregate_field_name exactly from content_fields" in (
        system_instruction.lower()
    )
    assert "do not invent, trim, or normalize field names" in (
        system_instruction.lower()
    )


def test_evidence_interpreter_response_format_is_strict_and_closed():
    schema = evidence_interpreter_response_format()["json_schema"]

    assert schema["strict"] is True
    assert schema["schema"]["additionalProperties"] is False
    candidate_schema = schema["schema"]["properties"]["candidate_source_ids"]
    assert candidate_schema["type"] == "array"
    assert candidate_schema["maxItems"] == 3
    assert "uniqueItems" not in candidate_schema
    assert candidate_schema["items"] == {
        "type": "string",
        "minLength": 1,
        "maxLength": 120,
        "pattern": r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    }
    assert schema["schema"]["required"] == [
        "interpretation_status",
        "operation_hint",
        "candidate_source_ids",
        "aggregate_function",
        "aggregate_field_name",
    ]
    function_schema = schema["schema"]["properties"]["aggregate_function"]
    assert function_schema == {
        "anyOf": [
            {
                "type": "string",
                "enum": ["median", "mean", "count", "sum", "minimum", "maximum"],
            },
            {"type": "null"},
        ]
    }
    field_schema = schema["schema"]["properties"]["aggregate_field_name"]
    assert field_schema == {
        "anyOf": [
            {
                "type": "string",
                "minLength": 1,
                "maxLength": 120,
                "pattern": r"^(?!\s)[^\x00-\x1f\x7f]*\S$",
            },
            {"type": "null"},
        ]
    }


@pytest.mark.parametrize(
    "aggregate_function",
    ["median", "mean", "count", "sum", "minimum", "maximum"],
)
def test_evidence_interpreter_output_accepts_closed_aggregate_contract(
    aggregate_function,
):
    output = EvidenceInterpreterOutput.model_validate(
        {
            "interpretation_status": "resolved",
            "operation_hint": "aggregate",
            "candidate_source_ids": ["source_a"],
            "aggregate_function": aggregate_function,
            "aggregate_field_name": "Fuel (L)",
        }
    )

    assert output.aggregate_function == aggregate_function
    assert output.aggregate_field_name == "Fuel (L)"


def test_evidence_interpreter_output_preserves_legacy_and_nullable_contracts():
    legacy_lookup = EvidenceInterpreterOutput.model_validate(
        {
            "interpretation_status": "resolved",
            "operation_hint": "lookup",
            "candidate_source_ids": ["source_a"],
        }
    )
    legacy_aggregate = EvidenceInterpreterOutput.model_validate(
        {
            "interpretation_status": "resolved",
            "operation_hint": "aggregate",
            "candidate_source_ids": ["source_a"],
        }
    )
    nullable_lookup = EvidenceInterpreterOutput.model_validate(
        {
            "interpretation_status": "resolved",
            "operation_hint": "lookup",
            "candidate_source_ids": ["source_a"],
            "aggregate_function": None,
            "aggregate_field_name": None,
        }
    )

    assert legacy_lookup.aggregate_function is None
    assert legacy_aggregate.aggregate_field_name is None
    assert nullable_lookup.aggregate_function is None


def test_evidence_interpreter_output_rejects_duplicate_candidates():
    with pytest.raises(
        ValidationError,
        match="duplicate_semantic_candidate_source_id",
    ):
        EvidenceInterpreterOutput.model_validate(
            {
                "interpretation_status": "ambiguous",
                "operation_hint": "lookup",
                "candidate_source_ids": ["source_a", "source_a"],
                "aggregate_function": None,
                "aggregate_field_name": None,
            }
        )


@pytest.mark.parametrize(
    "payload",
    [
        {
            "interpretation_status": "resolved",
            "operation_hint": "aggregate",
            "candidate_source_ids": ["source_a"],
            "aggregate_function": "average",
            "aggregate_field_name": "Fuel (L)",
        },
        {
            "interpretation_status": "resolved",
            "operation_hint": "aggregate",
            "candidate_source_ids": ["source_a"],
            "aggregate_function": "median",
        },
        {
            "interpretation_status": "resolved",
            "operation_hint": "aggregate",
            "candidate_source_ids": ["source_a"],
            "aggregate_field_name": "Fuel (L)",
        },
        {
            "interpretation_status": "resolved",
            "operation_hint": "lookup",
            "candidate_source_ids": ["source_a"],
            "aggregate_function": "median",
            "aggregate_field_name": "Fuel (L)",
        },
        {
            "interpretation_status": "resolved",
            "operation_hint": "aggregate",
            "candidate_source_ids": ["source_a"],
            "aggregate_function": "median",
            "aggregate_field_name": " Fuel (L)",
        },
        {
            "interpretation_status": "resolved",
            "operation_hint": "aggregate",
            "candidate_source_ids": ["source_a"],
            "aggregate_function": "median",
            "aggregate_field_name": "Fuel\n(L)",
        },
        {
            "interpretation_status": "resolved",
            "operation_hint": "aggregate",
            "candidate_source_ids": ["source_a"],
            "aggregate_function": "median",
            "aggregate_field_name": "x" * 121,
        },
    ],
    ids=[
        "unknown-function",
        "function-only",
        "field-only",
        "details-on-lookup",
        "outer-whitespace",
        "control-character",
        "overlong-field",
    ],
)
def test_evidence_interpreter_output_rejects_invalid_aggregate_contract(payload):
    with pytest.raises(ValidationError):
        EvidenceInterpreterOutput.model_validate(payload)


@pytest.mark.parametrize(
    "completion",
    [
        "not-an-object",
        {},
        {"choices": []},
        {"choices": [{"message": {"content": "{}"}}, {"message": {"content": "{}"}}]},
        {"choices": [{"message": {"content": "{}", "tool_calls": [{"id": "x"}]}}]},
        {"choices": [{"message": {"content": "{}", "refusal": "no"}}]},
        {"choices": [{"message": {"content": " "}}]},
        {"choices": [{"message": {"content": "{"}}]},
        {"choices": [{"message": {"content": "[]"}}]},
        _semantic_completion(
            {
                "interpretation_status": "unsupported",
                "operation_hint": "lookup",
                "candidate_source_ids": [],
            }
        ),
        _semantic_completion(
            {
                "interpretation_status": "no_match",
                "operation_hint": "unsupported",
                "candidate_source_ids": [],
            }
        ),
        _semantic_completion(
            {
                "interpretation_status": "resolved",
                "operation_hint": "lookup",
                "candidate_source_ids": [],
            }
        ),
        _semantic_completion(
            {
                "interpretation_status": "ambiguous",
                "operation_hint": "lookup",
                "candidate_source_ids": ["source_a", "source_a"],
            }
        ),
        _semantic_completion(
            {
                "interpretation_status": "resolved",
                "operation_hint": "lookup",
                "candidate_source_ids": ["fabricated_source"],
            }
        ),
        _semantic_completion(
            {
                "interpretation_status": "no_match",
                "operation_hint": "unknown",
                "candidate_source_ids": [],
                "explanation": "not allowed",
            }
        ),
    ],
)
def test_evidence_interpreter_parser_rejects_malformed_completion(completion):
    with pytest.raises((ValidationError, ValueError)):
        parse_evidence_interpreter_completion(
            completion,
            inventory_source_ids={"source_a", "source_b"},
        )


def test_evidence_interpreter_parser_canonicalizes_candidate_ids():
    parsed = parse_evidence_interpreter_completion(
        _semantic_completion(
            {
                "interpretation_status": "ambiguous",
                "operation_hint": "comparison",
                "candidate_source_ids": ["source_b", "source_a"],
            }
        ),
        inventory_source_ids={"source_a", "source_b"},
    )

    assert parsed["candidate_source_ids"] == ["source_a", "source_b"]


def test_evidence_interpreter_parser_omits_provider_nulls_from_cr_advisory():
    parsed = parse_evidence_interpreter_completion(
        _semantic_completion(
            {
                "interpretation_status": "resolved",
                "operation_hint": "lookup",
                "candidate_source_ids": ["source_a"],
                "aggregate_function": None,
                "aggregate_field_name": None,
            }
        ),
        inventory_source_ids={"source_a"},
    )

    assert parsed == {
        "interpretation_status": "resolved",
        "operation_hint": "lookup",
        "candidate_source_ids": ["source_a"],
    }


def test_evidence_interpreter_parser_preserves_enriched_aggregate_details():
    parsed = parse_evidence_interpreter_completion(
        _semantic_completion(
            {
                "interpretation_status": "resolved",
                "operation_hint": "aggregate",
                "candidate_source_ids": ["source_a"],
                "aggregate_function": "median",
                "aggregate_field_name": "Fuel (L)",
            }
        ),
        inventory_source_ids={"source_a"},
    )

    assert parsed == {
        "interpretation_status": "resolved",
        "operation_hint": "aggregate",
        "candidate_source_ids": ["source_a"],
        "aggregate_function": "median",
        "aggregate_field_name": "Fuel (L)",
    }


@pytest.mark.parametrize(
    "payload",
    [
        {
            "status": "matched",
            "matched_source_ids": [],
            "reason_codes": ["source_id_match"],
        },
        {
            "status": "no_match",
            "matched_source_ids": ["source_a"],
            "reason_codes": ["no_source_specific_match"],
        },
        {
            "status": "matched",
            "matched_source_ids": ["source_b", "source_a"],
            "reason_codes": ["source_id_match"],
        },
        {
            "status": "ambiguous",
            "matched_source_ids": [],
            "reason_codes": [
                "multiple_possible_source_matches",
                "multiple_possible_source_matches",
            ],
        },
        {
            "status": "matched",
            "matched_source_ids": ["source_a"],
            "reason_codes": ["unsupported_reason"],
        },
        {
            "status": "matched",
            "matched_source_ids": ["source_a"],
            "reason_codes": ["source_id_match"],
            "extra": True,
        },
        {
            "status": "unsupported",
            "matched_source_ids": [],
            "reason_codes": ["no_source_specific_match"],
        },
        {
            "status": "matched",
            "matched_source_ids": [f"source_{index}" for index in range(33)],
            "reason_codes": ["source_id_match"],
        },
    ],
)
def test_source_match_consumer_rejects_malformed_contract(payload):
    with pytest.raises(ValidationError):
        SourceMatchResult.model_validate(payload)


def test_source_match_consumer_defaults_absent_probe_ids_for_legacy_cr():
    parsed = SourceMatchResult.model_validate(
        {
            "status": "ambiguous",
            "matched_source_ids": [],
            "reason_codes": ["multiple_possible_source_matches"],
        }
    )

    assert parsed.probe_source_ids == []
    assert "probe_source_ids" not in parsed.model_fields_set


def test_source_match_consumer_accepts_three_sorted_probe_ids():
    parsed = SourceMatchResult.model_validate(
        {
            "status": "ambiguous",
            "matched_source_ids": [],
            "probe_source_ids": ["source_a", "source_b", "source_c"],
            "reason_codes": ["semantic_candidates_ambiguous"],
        }
    )

    assert parsed.probe_source_ids == ["source_a", "source_b", "source_c"]


@pytest.mark.parametrize(
    "payload",
    [
        {
            "status": "ambiguous",
            "matched_source_ids": [],
            "probe_source_ids": ["source_a"],
            "reason_codes": ["semantic_candidates_ambiguous"],
        },
        {
            "status": "ambiguous",
            "matched_source_ids": [],
            "probe_source_ids": [
                "source_a",
                "source_b",
                "source_c",
                "source_d",
            ],
            "reason_codes": ["semantic_candidates_ambiguous"],
        },
        {
            "status": "ambiguous",
            "matched_source_ids": [],
            "probe_source_ids": ["source_a", "source_a"],
            "reason_codes": ["semantic_candidates_ambiguous"],
        },
        {
            "status": "ambiguous",
            "matched_source_ids": [],
            "probe_source_ids": ["source_b", "source_a"],
            "reason_codes": ["semantic_candidates_ambiguous"],
        },
        {
            "status": "matched",
            "matched_source_ids": ["source_a"],
            "probe_source_ids": ["source_b", "source_c"],
            "reason_codes": ["semantic_candidates_ambiguous"],
        },
        {
            "status": "no_match",
            "matched_source_ids": [],
            "probe_source_ids": ["source_a", "source_b"],
            "reason_codes": ["semantic_candidates_ambiguous"],
        },
        {
            "status": "inventory_unavailable",
            "matched_source_ids": [],
            "probe_source_ids": ["source_a", "source_b"],
            "reason_codes": ["semantic_candidates_ambiguous"],
        },
        {
            "status": "ambiguous",
            "matched_source_ids": [],
            "probe_source_ids": ["source_a", "source_b"],
            "reason_codes": ["multiple_possible_source_matches"],
        },
        {
            "status": "ambiguous",
            "matched_source_ids": ["source_a"],
            "probe_source_ids": ["source_b", "source_c"],
            "reason_codes": ["semantic_candidates_ambiguous"],
        },
    ],
    ids=[
        "one_probe_id",
        "four_probe_ids",
        "duplicate_probe_ids",
        "unsorted_probe_ids",
        "matched_status",
        "no_match_status",
        "inventory_unavailable_status",
        "missing_semantic_ambiguity_reason",
        "nonempty_matched_source_ids",
    ],
)
def test_source_match_consumer_rejects_invalid_probe_contract(payload):
    with pytest.raises(ValidationError):
        SourceMatchResult.model_validate(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "probe_source_ids",
    [
        ["source_a", "source_b"],
        ["source_a", "source_b", "source_c"],
    ],
    ids=["two_sources", "three_sources"],
)
async def test_bounded_probe_authorization_compiles_exact_acquisition_scope(
    probe_source_ids,
):
    first = _shape_with_source_match(
        status="no_match",
        derivation_status="not_applicable",
    )
    second = _shape_with_source_match(
        status="ambiguous",
        derivation_status="derived",
    )
    second["result"]["source_match"] = {
        "status": "ambiguous",
        "matched_source_ids": [],
        "probe_source_ids": probe_source_ids,
        "reason_codes": ["semantic_candidates_ambiguous"],
    }
    plan = _plan_response()
    plan["result"]["eligible_source_ids"] = probe_source_ids
    runtime = SequentialShapeRuntime([first, second], plan=plan)
    dsa = FakeDsa(
        [
            _source("source_a"),
            _source("source_b"),
            _source("source_c"),
            _source("source_decoy"),
        ],
        inventory_metadata={
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
        },
    )
    interpreter_calls = []

    async def interpreter(**kwargs):
        interpreter_calls.append(kwargs)
        return {
            "interpretation_status": "ambiguous",
            "operation_hint": "lookup",
            "candidate_source_ids": probe_source_ids,
        }

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=interpreter,
        **SCOPE,
    )

    assert state.status == "acquisition_ready"
    assert state.forced_answer is None
    assert state.shape.source_match.probe_source_ids == probe_source_ids
    assert state.shape.source_match.matched_source_ids == []
    assert state.declared_scope["source_ids"] == probe_source_ids
    assert state.declared_scope["exact_source_refs"] == []
    assert state.plan.eligible_source_ids == probe_source_ids
    assert state.plan.selected_strategies == ["targeted_retrieval"]
    assert state.sufficiency is None
    assert state.next_step is None
    assert state.next_step_selection_attempted is False
    assert state.semantic_interpreter == {
        "called": True,
        "status": "accepted",
        "reason": "validated",
        "interpretation_status": "ambiguous",
        "operation_hint": "lookup",
        "candidate_count": len(probe_source_ids),
    }
    assert len(interpreter_calls) == 1
    assert [name for name, _ in runtime.calls] == ["shape", "shape", "plan"]
    assert runtime.calls[-1][1]["declared_scope"]["source_ids"] == probe_source_ids
    assert dsa.calls == ["list_sources"]
    assert dsa.operation_request_ids == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "planned_source_ids",
    [
        ["source_a"],
        ["source_a", "source_b", "source_decoy"],
    ],
    ids=["plan_drops_probe", "plan_expands_probe"],
)
async def test_bounded_probe_rejects_plan_source_set_mismatch(planned_source_ids):
    probe_source_ids = ["source_a", "source_b"]
    first = _shape_with_source_match(
        status="no_match",
        derivation_status="not_applicable",
    )
    second = _shape_with_source_match(status="ambiguous")
    second["result"]["source_match"] = {
        "status": "ambiguous",
        "matched_source_ids": [],
        "probe_source_ids": probe_source_ids,
        "reason_codes": ["semantic_candidates_ambiguous"],
    }
    plan = _plan_response()
    plan["result"]["eligible_source_ids"] = planned_source_ids
    runtime = SequentialShapeRuntime([first, second], plan=plan)

    async def interpreter(**kwargs):
        return {
            "interpretation_status": "ambiguous",
            "operation_hint": "lookup",
            "candidate_source_ids": probe_source_ids,
        }

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [_source("source_a"), _source("source_b"), _source("source_decoy")],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=interpreter,
        **SCOPE,
    )

    assert state.status == "unsupported_plan"
    assert state.forced_answer == UNSUPPORTED_ANSWER
    assert state.declared_scope["source_ids"] == probe_source_ids
    assert state.plan.eligible_source_ids == planned_source_ids
    assert [name for name, _ in runtime.calls] == ["shape", "shape", "plan"]


def test_probe_manifest_trace_exposes_count_without_candidate_ids():
    shape = _shape_with_source_match(status="ambiguous")["result"]
    shape["source_match"] = {
        "status": "ambiguous",
        "matched_source_ids": [],
        "probe_source_ids": ["source_a", "source_b"],
        "reason_codes": ["semantic_candidates_ambiguous"],
    }
    state = EvidenceAcquisitionState(
        enabled=True,
        attempted=True,
        status="acquisition_ready",
        shape=ShapeResult.model_validate(shape),
    )

    manifest = build_manifest_trace(
        state=state,
        context_pack=None,
        dsa_trace=None,
        retained_source_refs=None,
    )

    source_match = manifest["shape"]["source_match"]
    assert source_match["status"] == "ambiguous"
    assert source_match["matched_source_ids"] == []
    assert source_match["probe_source_count"] == 2
    assert "probe_source_ids" not in source_match
    assert "source_a" not in json.dumps(source_match, sort_keys=True)
    assert "source_b" not in json.dumps(source_match, sort_keys=True)

    shape["source_match"].pop("probe_source_ids")
    no_probe = EvidenceAcquisitionState(
        enabled=True,
        attempted=True,
        status="source_scope_ambiguous",
        shape=ShapeResult.model_validate(shape),
    )
    no_probe_manifest = build_manifest_trace(
        state=no_probe,
        context_pack=None,
        dsa_trace=None,
        retained_source_refs=None,
    )
    assert "probe_source_count" not in no_probe_manifest["shape"]["source_match"]


@pytest.mark.asyncio
async def test_probe_source_id_outside_exact_inventory_fails_shape_consumption():
    shape = _shape_with_source_match(
        status="ambiguous",
        derivation_status="ambiguous",
    )
    shape["result"]["source_match"] = {
        "status": "ambiguous",
        "matched_source_ids": [],
        "probe_source_ids": ["source_a", "source_stale"],
        "reason_codes": ["semantic_candidates_ambiguous"],
    }
    runtime = FakeRuntime(shape=shape, auto_source_match=False)
    dsa = FakeDsa([_source("source_a"), _source("source_b")])

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.status == "shape_dependency_failed"
    assert state.forced_answer == UNSUPPORTED_ANSWER
    assert [name for name, _ in runtime.calls] == ["shape"]
    assert dsa.calls == ["list_sources"]


@pytest.mark.asyncio
async def test_inventory_precedes_shape_and_natural_match_bounds_plan_scope():
    order = []
    plan = _plan_response()
    runtime = FakeRuntime(
        shape=_shape_with_source_match(
            status="matched",
            matched_source_ids=["source_a"],
        ),
        plan=plan,
        auto_source_match=False,
    )
    original_derive = runtime.derive_evidence_shape

    async def ordered_derive(**kwargs):
        order.append("shape")
        return await original_derive(**kwargs)

    runtime.derive_evidence_shape = ordered_derive

    class OrderedDsa(FakeDsa):
        async def list_sources(self, *, request_id=None):
            order.append("inventory")
            return await super().list_sources(request_id=request_id)

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=OrderedDsa([_source("source_b"), _source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert order == ["inventory", "shape"]
    assert state.declared_scope["source_ids"] == ["source_a"]
    assert state.declared_scope["source_categories"] == []
    assert runtime.calls[1][1]["declared_scope"]["source_ids"] == ["source_a"]
    assert runtime.calls[1][1]["source_inventory"][0]["source_id"] == "source_a"


@pytest.mark.asyncio
async def test_not_applicable_no_match_uses_semantic_second_derivation_and_cr_scope():
    first = _shape_with_source_match(
        status="no_match",
        derivation_status="not_applicable",
    )
    second = _shape_with_source_match(
        status="matched",
        matched_source_ids=["source_a"],
    )
    second["result"]["source_match"]["reason_codes"] = [
        "semantic_candidate_validated"
    ]
    second["result"]["reason_codes"] = [
        "semantic_operation_hint",
        "targeted_lookup_derived",
    ]
    runtime = SequentialShapeRuntime([first, second])
    interpreter_calls = []

    async def interpreter(**kwargs):
        interpreter_calls.append(kwargs)
        return {
            "interpretation_status": "resolved",
            "operation_hint": "lookup",
            "candidate_source_ids": ["source_a"],
            "aggregate_function": None,
            "aggregate_field_name": None,
        }

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [_source("source_b"), _source("source_a")],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=interpreter,
        **SCOPE,
    )

    shape_calls = [call for call in runtime.calls if call[0] == "shape"]
    assert len(interpreter_calls) == 1
    assert len(shape_calls) == 2
    assert "semantic_advisory" not in shape_calls[0][1]["task_context"]
    assert shape_calls[1][1]["task_context"]["semantic_advisory"] == {
        "interpretation_status": "resolved",
        "operation_hint": "lookup",
        "candidate_source_ids": ["source_a"],
    }
    assert state.declared_scope["source_ids"] == ["source_a"]
    assert state.semantic_interpreter == {
        "called": True,
        "status": "accepted",
        "reason": "validated",
        "interpretation_status": "resolved",
        "operation_hint": "lookup",
        "candidate_count": 1,
    }


@pytest.mark.asyncio
async def test_explicit_and_deterministic_matches_bypass_semantic_interpreter():
    async def forbidden_interpreter(**kwargs):
        raise AssertionError(kwargs)

    explicit_runtime = FakeRuntime(
        shape=_shape_with_source_match(status="no_match"),
        auto_source_match=False,
    )
    explicit = await begin_evidence_acquisition(
        runtime=explicit_runtime,
        dsa=FakeDsa([_source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context={"source_ids": ["source_a"]},
        semantic_interpreter=forbidden_interpreter,
        **SCOPE,
    )
    deterministic_runtime = FakeRuntime(
        shape=_shape_with_source_match(
            status="matched",
            matched_source_ids=["source_a"],
        ),
        auto_source_match=False,
    )
    deterministic = await begin_evidence_acquisition(
        runtime=deterministic_runtime,
        dsa=FakeDsa([_source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=forbidden_interpreter,
        **SCOPE,
    )

    assert explicit.declared_scope["source_ids"] == ["source_a"]
    assert deterministic.declared_scope["source_ids"] == ["source_a"]
    assert len([call for call in explicit_runtime.calls if call[0] == "shape"]) == 1
    assert len([call for call in deterministic_runtime.calls if call[0] == "shape"]) == 1


@pytest.mark.asyncio
async def test_semantic_no_match_preserves_ordinary_path_and_no_content_history():
    no_match = _shape_with_source_match(
        status="no_match",
        derivation_status="not_applicable",
    )
    runtime = SequentialShapeRuntime([no_match, no_match])

    async def interpreter(**kwargs):
        return {
            "interpretation_status": "no_match",
            "operation_hint": "unknown",
            "candidate_source_ids": [],
        }

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [_source("source_a")],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=interpreter,
        **SCOPE,
    )
    manifest = build_manifest_trace(
        state=state,
        context_pack=None,
        dsa_trace={"called": False, "status": "not_called"},
        retained_source_refs=set(),
    )

    assert state.status == "not_applicable"
    assert state.follow_existing_path is True
    assert len([call for call in runtime.calls if call[0] == "shape"]) == 2
    assert manifest["acquisition"]["dsa_outcome"] == "not_called"
    assert manifest["acquisition"]["source_summaries"] == []


@pytest.mark.asyncio
async def test_semantic_failure_preserves_ordinary_but_clarifies_material_request():
    async def failed_interpreter(**kwargs):
        raise SemanticInterpreterFailure("dependency_failure")

    ordinary_shape = _shape_with_source_match(
        status="no_match",
        derivation_status="not_applicable",
    )
    ordinary_runtime = SequentialShapeRuntime([ordinary_shape])
    ordinary = await begin_evidence_acquisition(
        runtime=ordinary_runtime,
        dsa=FakeDsa(
            [_source("source_a")],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=failed_interpreter,
        **SCOPE,
    )
    material_runtime = SequentialShapeRuntime(
        [_shape_with_source_match(status="no_match")]
    )
    material = await begin_evidence_acquisition(
        runtime=material_runtime,
        dsa=FakeDsa(
            [_source("source_a")],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=failed_interpreter,
        **SCOPE,
    )

    assert ordinary.status == "not_applicable"
    assert ordinary.follow_existing_path is True
    assert ordinary.semantic_interpreter["reason"] == "dependency_failure"
    assert material.status == "semantic_interpreter_failed"
    assert material.forced_answer == AMBIGUOUS_ANSWER
    assert all(call[0] != "plan" for call in material_runtime.calls)


@pytest.mark.asyncio
async def test_semantic_ambiguity_and_aggregate_never_compile_a_plan():
    first = _shape_with_source_match(
        status="no_match",
        derivation_status="not_applicable",
    )
    ambiguous = _shape_with_source_match(
        status="ambiguous",
        derivation_status="ambiguous",
    )
    aggregate = _shape_with_source_match(
        status="matched",
        matched_source_ids=["source_a"],
        derivation_status="ambiguous",
    )
    aggregate["result"]["source_match"]["reason_codes"] = [
        "semantic_candidate_validated"
    ]
    aggregate["result"]["reason_codes"] = [
        "semantic_operation_hint",
        "semantic_operation_unsupported",
    ]

    async def ambiguous_interpreter(**kwargs):
        return {
            "interpretation_status": "ambiguous",
            "operation_hint": "latest",
            "candidate_source_ids": ["source_b", "source_a"],
        }

    async def aggregate_interpreter(**kwargs):
        return {
            "interpretation_status": "resolved",
            "operation_hint": "aggregate",
            "candidate_source_ids": ["source_a"],
        }

    ambiguous_runtime = SequentialShapeRuntime([first, ambiguous])
    ambiguous_state = await begin_evidence_acquisition(
        runtime=ambiguous_runtime,
        dsa=FakeDsa(
            [_source("source_a"), _source("source_b")],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=ambiguous_interpreter,
        **SCOPE,
    )
    aggregate_runtime = SequentialShapeRuntime([first, aggregate])
    aggregate_state = await begin_evidence_acquisition(
        runtime=aggregate_runtime,
        dsa=FakeDsa(
            [_source("source_a"), _source("source_b")],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=aggregate_interpreter,
        **SCOPE,
    )

    assert ambiguous_state.status == "ambiguous"
    assert ambiguous_state.shape.source_match.matched_source_ids == []
    assert aggregate_state.status == "ambiguous"
    assert aggregate_state.shape.source_match.matched_source_ids == ["source_a"]
    assert aggregate_state.shape.task_shape is None
    assert all(call[0] != "plan" for call in ambiguous_runtime.calls)
    assert all(call[0] != "plan" for call in aggregate_runtime.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("candidate_count", [1, 2, 3])
async def test_enriched_aggregate_reaches_cr_and_remains_fail_closed(candidate_count):
    candidate_ids = [f"source_{letter}" for letter in "abc"[:candidate_count]]
    first = _shape_with_source_match(
        status="no_match",
        derivation_status="not_applicable",
    )
    second = _shape_with_source_match(
        status="matched" if candidate_count == 1 else "ambiguous",
        matched_source_ids=candidate_ids if candidate_count == 1 else [],
        derivation_status="ambiguous",
    )
    second["result"]["source_match"]["reason_codes"] = [
        "semantic_candidate_validated"
        if candidate_count == 1
        else "semantic_candidates_ambiguous"
    ]
    second["result"]["reason_codes"] = [
        "semantic_operation_hint",
        "semantic_operation_unsupported",
    ]
    runtime = SequentialShapeRuntime([first, second])
    dsa = FakeDsa(
        [
            _source(
                source_id,
                content_fields=["Date", "Fuel (L)", "Odometer"],
            )
            for source_id in candidate_ids
        ],
        inventory_metadata={
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
        },
    )

    async def interpreter(**kwargs):
        return {
            "interpretation_status": (
                "resolved" if candidate_count == 1 else "ambiguous"
            ),
            "operation_hint": "aggregate",
            "candidate_source_ids": list(reversed(candidate_ids)),
            "aggregate_function": "median",
            "aggregate_field_name": "Fuel (L)",
        }

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=interpreter,
        **SCOPE,
    )

    shape_calls = [payload for name, payload in runtime.calls if name == "shape"]
    assert state.status == "ambiguous"
    assert state.shape.derivation_status == "ambiguous"
    assert state.shape.task_shape is None
    assert state.shape.candidate_task_shapes == []
    assert state.shape.source_match.probe_source_ids == []
    assert state.declared_scope is None
    assert state.plan is None
    assert state.semantic_interpreter == {
        "called": True,
        "status": "accepted",
        "reason": "validated",
        "interpretation_status": (
            "resolved" if candidate_count == 1 else "ambiguous"
        ),
        "operation_hint": "aggregate",
        "candidate_count": candidate_count,
    }
    assert len(shape_calls) == 2
    assert all(
        source["content_fields"] == ["Date", "Fuel (L)", "Odometer"]
        for source in shape_calls[1]["task_context"]["source_discovery"]["sources"]
    )
    assert shape_calls[1]["task_context"]["semantic_advisory"] == {
        "interpretation_status": (
            "resolved" if candidate_count == 1 else "ambiguous"
        ),
        "operation_hint": "aggregate",
        "candidate_source_ids": candidate_ids,
        "aggregate_function": "median",
        "aggregate_field_name": "Fuel (L)",
    }
    assert all(name != "plan" for name, _ in runtime.calls)
    assert dsa.calls == ["list_sources"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("candidate_fields", "aggregate_field_name"),
    [
        ([None], "Fuel (L)"),
        (["Date"], "Fuel (L)"),
        (["fuel (l)"], "Fuel (L)"),
        (["Fuel"], "Fuel (L)"),
        (["Fuel (L)", "Date"], "Fuel (L)"),
        (["Fuel (L)", "Fuel (L)", "Date"], "Fuel (L)"),
    ],
    ids=[
        "missing-content-fields",
        "field-absent",
        "case-mismatch",
        "substring-only",
        "second-candidate-missing",
        "third-candidate-missing",
    ],
)
async def test_enriched_aggregate_rejects_unsafe_candidate_field_membership(
    candidate_fields,
    aggregate_field_name,
):
    candidate_ids = [f"source_{letter}" for letter in "abc"[: len(candidate_fields)]]
    first = _shape_with_source_match(status="no_match", derivation_status="derived")
    sources = []
    for source_id, field in zip(candidate_ids, candidate_fields, strict=True):
        fields = None if field is None else [field]
        sources.append(_source(source_id, content_fields=fields))
    unrelated = _source("source_unrelated", content_fields=["Fuel (L)"])
    runtime = SequentialShapeRuntime([first])
    dsa = FakeDsa(
        [*sources, unrelated],
        inventory_metadata={
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
        },
    )

    async def interpreter(**kwargs):
        return {
            "interpretation_status": (
                "resolved" if len(candidate_ids) == 1 else "ambiguous"
            ),
            "operation_hint": "aggregate",
            "candidate_source_ids": candidate_ids,
            "aggregate_function": "median",
            "aggregate_field_name": aggregate_field_name,
        }

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=interpreter,
        **SCOPE,
    )

    assert state.status == "semantic_interpreter_failed"
    assert state.semantic_interpreter["reason"] == "malformed_response"
    assert [name for name, _ in runtime.calls] == ["shape"]
    assert state.plan is None
    assert dsa.calls == ["list_sources"]


@pytest.mark.asyncio
async def test_enriched_aggregate_field_is_private_in_manifest_trace():
    sentinel = "PRIVATE_AGGREGATE_FIELD"
    first = _shape_with_source_match(
        status="no_match",
        derivation_status="not_applicable",
    )
    second = _shape_with_source_match(
        status="matched",
        matched_source_ids=["source_a"],
        derivation_status="ambiguous",
    )
    second["result"]["source_match"]["reason_codes"] = [
        "semantic_candidate_validated"
    ]
    second["result"]["reason_codes"] = [
        "semantic_operation_hint",
        "semantic_operation_unsupported",
    ]
    runtime = SequentialShapeRuntime([first, second])

    async def interpreter(**kwargs):
        return {
            "interpretation_status": "resolved",
            "operation_hint": "aggregate",
            "candidate_source_ids": ["source_a"],
            "aggregate_function": "median",
            "aggregate_field_name": sentinel,
        }

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [_source("source_a", content_fields=[sentinel])],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=interpreter,
        **SCOPE,
    )
    manifest = build_manifest_trace(
        state=state,
        context_pack=None,
        dsa_trace={"called": False, "status": "not_called"},
        retained_source_refs=set(),
    )

    serialized_manifest = json.dumps(manifest, sort_keys=True)
    assert sentinel in json.dumps(runtime.calls[1][1], sort_keys=True)
    assert sentinel not in serialized_manifest
    assert "content_fields" not in serialized_manifest
    assert "aggregate_function" not in serialized_manifest


@pytest.mark.asyncio
async def test_cr_refusal_of_resolved_semantic_candidate_remains_authoritative():
    first = _shape_with_source_match(
        status="no_match",
        derivation_status="not_applicable",
    )
    refused = _shape_with_source_match(
        status="ambiguous",
        derivation_status="ambiguous",
    )
    refused["result"]["source_match"]["reason_codes"] = [
        "semantic_candidates_ambiguous"
    ]
    runtime = SequentialShapeRuntime([first, refused])

    async def interpreter(**kwargs):
        return {
            "interpretation_status": "resolved",
            "operation_hint": "lookup",
            "candidate_source_ids": ["source_a"],
        }

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [_source("source_a"), _source("source_b")],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "complete",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=interpreter,
        **SCOPE,
    )

    assert state.status == "ambiguous"
    assert state.shape.source_match.matched_source_ids == []
    assert state.declared_scope is None
    assert all(call[0] != "plan" for call in runtime.calls)


@pytest.mark.asyncio
async def test_partial_inventory_allows_positive_semantic_resolution_but_unknown_does_not():
    first = _shape_with_source_match(
        status="inventory_unavailable",
        derivation_status="not_applicable",
    )
    matched = _shape_with_source_match(
        status="matched",
        matched_source_ids=["source_a"],
    )
    matched["result"]["source_match"]["reason_codes"] = [
        "inventory_partial",
        "semantic_candidate_validated",
    ]
    calls = []

    async def interpreter(**kwargs):
        calls.append(kwargs)
        return {
            "interpretation_status": "resolved",
            "operation_hint": "lookup",
            "candidate_source_ids": ["source_a"],
        }

    partial = await begin_evidence_acquisition(
        runtime=SequentialShapeRuntime([first, matched]),
        dsa=FakeDsa(
            [_source("source_a")],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "partial",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=interpreter,
        **SCOPE,
    )
    unknown = await begin_evidence_acquisition(
        runtime=SequentialShapeRuntime([first]),
        dsa=FakeDsa(
            [_source("source_a")],
            inventory_metadata={
                "inventory_scope": "configured_sources",
                "inventory_status": "unknown",
            },
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        semantic_interpreter=interpreter,
        **SCOPE,
    )

    assert partial.declared_scope["source_ids"] == ["source_a"]
    assert len(calls) == 1
    assert unknown.semantic_interpreter["called"] is False


@pytest.mark.asyncio
async def test_natural_multiple_match_is_sorted_and_excludes_decoy():
    plan = _plan_response()
    plan["result"]["eligible_source_ids"] = ["source_a", "source_b"]
    runtime = FakeRuntime(
        shape=_shape_with_source_match(
            status="matched",
            matched_source_ids=["source_a", "source_b"],
        ),
        plan=plan,
        auto_source_match=False,
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([_source("source_decoy"), _source("source_b"), _source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.declared_scope["source_ids"] == ["source_a", "source_b"]
    assert "source_decoy" not in state.declared_scope["source_ids"]


@pytest.mark.asyncio
async def test_explicit_source_selector_has_precedence_over_natural_match():
    runtime = FakeRuntime(
        shape=_shape_with_source_match(
            status="matched",
            matched_source_ids=["source_b"],
        ),
        auto_source_match=False,
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([_source("source_a"), _source("source_b")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context={"source_ids": ["source_a"]},
        **SCOPE,
    )

    assert state.declared_scope["source_ids"] == ["source_a"]
    assert runtime.calls[1][1]["declared_scope"]["source_ids"] == ["source_a"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("match_status", "expected_status", "expected_answer"),
    [
        ("ambiguous", "source_scope_ambiguous", AMBIGUOUS_ANSWER),
        ("no_match", "source_scope_no_match", AMBIGUOUS_ANSWER),
        (
            "inventory_unavailable",
            "inventory_dependency_failed",
            UNSUPPORTED_ANSWER,
        ),
    ],
)
async def test_nondefinitive_natural_match_never_compiles_broad_plan(
    match_status,
    expected_status,
    expected_answer,
):
    runtime = FakeRuntime(
        shape=_shape_with_source_match(status=match_status),
        auto_source_match=False,
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([_source("source_a"), _source("source_b")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.status == expected_status
    assert state.forced_answer == expected_answer
    assert [name for name, _ in runtime.calls] == ["shape"]


@pytest.mark.asyncio
@pytest.mark.parametrize("malformation", ["missing", "outside_inventory"])
async def test_cr_discovery_contract_failure_is_not_retried(malformation):
    shape = _shape_response()
    if malformation == "outside_inventory":
        shape["result"]["source_match"] = {
            "status": "matched",
            "matched_source_ids": ["source_outside"],
            "reason_codes": ["source_id_match"],
        }
    runtime = FakeRuntime(shape=shape, auto_source_match=False)
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([_source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.status == "shape_dependency_failed"
    assert [name for name, _ in runtime.calls] == ["shape"]


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinary", [True, False])
async def test_inventory_dependency_failure_preserves_only_ordinary_path(ordinary):
    class FailingDsa(FakeDsa):
        async def list_sources(self, *, request_id=None):
            self.list_request_ids.append(request_id)
            raise RuntimeError("PRIVATE-INVENTORY-FAILURE")

    runtime = FakeRuntime(shape=_shape_response(status="not_applicable" if ordinary else "derived"))
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FailingDsa([]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert runtime.calls[0][1]["task_context"].get("source_discovery") is None
    assert state.inventory_discovery["outcome"] == "dependency_failure"
    assert state.follow_existing_path is ordinary
    assert state.status == ("not_applicable" if ordinary else "inventory_dependency_failed")


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinary", [True, False])
async def test_malformed_inventory_preserves_only_ordinary_path(ordinary):
    runtime = FakeRuntime(shape=_shape_response(status="not_applicable" if ordinary else "derived"))
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([], source_response={"sources": [{"source_id": "broken"}]}),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert runtime.calls[0][1]["task_context"].get("source_discovery") is None
    assert state.inventory_discovery["outcome"] == "malformed_response"
    assert state.follow_existing_path is ordinary
    assert state.status == ("not_applicable" if ordinary else "inventory_dependency_failed")


@pytest.mark.asyncio
async def test_unavailable_matched_source_identity_is_not_redirected():
    runtime = FakeRuntime(
        shape=_shape_with_source_match(
            status="matched",
            matched_source_ids=["source_unavailable"],
        ),
        auto_source_match=False,
    )
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [
                _source("source_available"),
                _source("source_unavailable", status="unavailable"),
            ]
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.declared_scope["source_ids"] == ["source_unavailable"]
    plan_call = runtime.calls[1][1]
    assert plan_call["declared_scope"]["source_ids"] == ["source_unavailable"]
    assert {
        source["source_id"]: source["availability"] for source in plan_call["source_inventory"]
    } == {
        "source_available": "available",
        "source_unavailable": "unavailable",
    }


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


def test_material_scope_selector_public_contract_normalizes_and_serializes():
    payload = _chat_request_with_exact_refs([], source_ids=["source_a"])
    payload["external_context"]["scope_refs"] = {
        "project": "firefox",
        "time": "fy2026",
    }

    request = ChatRequest.model_validate(payload)

    assert request.external_context is not None
    assert request.external_context.scope_refs is not None
    assert request.external_context.scope_refs.model_dump(exclude_none=True) == {
        "time": "fy2026",
        "project": "firefox",
    }
    assert request.model_dump()["external_context"]["scope_refs"] == {
        "time": "fy2026",
        "project": "firefox",
    }
    ordinary = ChatRequest.model_validate(
        {
            "owner_id": "owner",
            "surface": "dev",
            "messages": [{"role": "user", "content": QUESTION}],
            "external_context": {"enabled": True},
        }
    )
    assert "scope_refs" not in ordinary.model_dump()["external_context"]


@pytest.mark.parametrize(
    "scope_refs",
    [
        {},
        None,
        {"time": None},
        {"time": 2026},
        {"time": ""},
        {"time": "fy 2026"},
        {"time": "https://private.invalid/window"},
        {"time": "fy2026?private=true"},
        {"time": "fy/2026"},
        {"time": "x" * 121},
        {"owner": "private-owner"},
        {"time": "fy2026", "project": None},
    ],
    ids=[
        "empty",
        "null-object",
        "null-value",
        "non-string",
        "blank",
        "whitespace",
        "url",
        "query-string",
        "unsafe-character",
        "overlong",
        "unknown-key",
        "partially-malformed",
    ],
)
def test_material_scope_selector_public_contract_rejects_malformed_values(
    scope_refs,
):
    payload = _chat_request_with_exact_refs([])
    payload["external_context"]["scope_refs"] = scope_refs

    with pytest.raises(ValidationError):
        ChatRequest.model_validate(payload)


def test_external_context_outer_unknown_fields_remain_ignored_with_scope_selector():
    payload = _chat_request_with_exact_refs([])
    payload["external_context"].update(
        {
            "scope_refs": {"domain": "credential-management"},
            "future_compatibility_field": "ignored",
        }
    )

    request = ChatRequest.model_validate(payload)

    assert request.external_context is not None
    serialized = request.external_context.model_dump()
    assert serialized["scope_refs"]["domain"] == "credential-management"
    assert "future_compatibility_field" not in serialized


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


@pytest.mark.parametrize(
    "scope_refs",
    [
        {"time": "fy2026"},
        {"version": "release-152", "project": "firefox"},
        {
            "time": "fy2026",
            "version": "release-152",
            "domain": "credential-management",
            "project": "firefox",
        },
    ],
)
def test_dsa_source_inventory_accepts_legacy_and_strict_scope_metadata(scope_refs):
    inventory = DsaSourceListResponse.model_validate(
        {
            "sources": [
                _source("legacy_source"),
                _source("scoped_source", scope_refs=scope_refs),
            ]
        }
    )

    assert inventory.sources[0].scope_refs is None
    assert inventory.sources[1].scope_refs is not None
    assert inventory.sources[1].scope_refs.model_dump(exclude_none=True) == scope_refs


@pytest.mark.parametrize(
    "scope_refs",
    [
        {},
        None,
        {"time": None},
        {"time": 2026},
        {"time": "fy 2026"},
        {"version": "https://private.invalid/release"},
        {"domain": "credentials?private=true"},
        {"project": "fire/fox"},
        {"project": "x" * 121},
        {"unknown": "private"},
        {"time": "fy2026", "project": None},
    ],
    ids=[
        "empty",
        "null-object",
        "null-value",
        "non-string",
        "whitespace",
        "url",
        "query-string",
        "unsafe-character",
        "overlong",
        "unknown-key",
        "partially-malformed",
    ],
)
def test_dsa_source_inventory_rejects_malformed_scope_metadata(scope_refs):
    source = _source("source_a")
    source["scope_refs"] = scope_refs

    with pytest.raises(ValidationError):
        DsaSourceListResponse.model_validate({"sources": [source]})


@pytest.mark.asyncio
async def test_malformed_source_scope_metadata_fails_inventory_before_plan():
    runtime = FakeRuntime()
    source = _source("source_a")
    source["scope_refs"] = {"time": "fy 2026"}
    dsa = FakeDsa([source])

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={"scope_refs": {"time": "fy2026"}},
        **SCOPE,
    )

    assert state.status == "inventory_dependency_failed"
    assert state.plan is None
    assert dsa.calls == ["list_sources"]
    assert dsa.list_request_ids == ["rid"]
    assert [name for name, _ in runtime.calls] == ["shape"]


def _scope_source(
    source_id,
    *,
    scope_refs=None,
    tags=None,
):
    return _source(
        source_id,
        capabilities=["search", "fetch", "context"],
        tags=tags,
        scope_refs=scope_refs,
    )


def _resolved_scope(
    sources,
    *,
    external_context=None,
    exact_source_refs=None,
):
    return _resolve_declared_scope(
        DsaSourceListResponse.model_validate({"sources": sources}),
        external_context=external_context,
        exact_source_refs=exact_source_refs or [],
    )


@pytest.mark.parametrize(
    ("external_context", "expected_ids", "expected_refs"),
    [
        (
            {"scope_refs": {"time": "fy2026"}},
            ["source_a", "source_c"],
            {"time_scope_ref": "fy2026"},
        ),
        (
            {
                "scope_refs": {
                    "time": "fy2026",
                    "project": "firefox",
                }
            },
            ["source_a"],
            {"time_scope_ref": "fy2026", "project_scope_ref": "firefox"},
        ),
        (
            {
                "source_ids": ["source_b", "source_a"],
                "scope_refs": {"time": "fy2026"},
            },
            ["source_a"],
            {"time_scope_ref": "fy2026"},
        ),
        (
            {
                "domain_tags": ["selected"],
                "scope_refs": {"time": "fy2026"},
            },
            ["source_a"],
            {"time_scope_ref": "fy2026"},
        ),
    ],
    ids=["one-dimension", "conjunctive", "source-ids", "categories"],
)
def test_scope_selector_narrows_only_the_already_declared_universe(
    external_context,
    expected_ids,
    expected_refs,
):
    sources = [
        _scope_source(
            "source_c",
            scope_refs={"time": "fy2026", "project": "thunderbird"},
        ),
        _scope_source(
            "source_a",
            scope_refs={"time": "fy2026", "project": "firefox"},
            tags=["records", "selected"],
        ),
        _scope_source(
            "source_b",
            scope_refs={"time": "fy2025", "project": "firefox"},
            tags=["records", "selected"],
        ),
    ]

    scope, matched = _resolved_scope(
        sources,
        external_context=external_context,
    )

    assert matched is True
    assert scope["source_ids"] == expected_ids
    for field, value in expected_refs.items():
        assert scope[field] == value


def test_scope_selector_preserves_exact_references_only_when_all_sources_survive():
    references = [
        {"source_id": "source_a", "source_ref": "connector:a:item"},
        {"source_id": "source_b", "source_ref": "connector:b:item"},
    ]
    sources = [
        _scope_source("source_a", scope_refs={"time": "fy2026"}),
        _scope_source("source_b", scope_refs={"time": "fy2026"}),
    ]

    scope, matched = _resolved_scope(
        sources,
        external_context={
            "domain_tags": ["records"],
            "scope_refs": {"time": "fy2026"},
        },
        exact_source_refs=references,
    )

    assert matched is True
    assert scope["source_ids"] == ["source_a", "source_b"]
    assert scope["exact_source_refs"] == references

    sources[1]["scope_refs"] = {"time": "fy2025"}
    _, matched = _resolved_scope(
        sources,
        external_context={"scope_refs": {"time": "fy2026"}},
        exact_source_refs=references,
    )
    assert matched is False


@pytest.mark.parametrize(
    "external_context",
    [
        {"scope_refs": {"time": "fy2030"}},
        {
            "scope_refs": {
                "time": "fy2026",
                "project": "thunderbird",
            }
        },
    ],
    ids=["zero-matches", "dimensions-split-across-sources"],
)
@pytest.mark.asyncio
async def test_scope_selector_mismatch_stops_before_plan_or_acquisition(
    external_context,
):
    runtime = FakeRuntime()
    dsa = FakeDsa(
        [
            _scope_source(
                "source_a",
                scope_refs={"time": "fy2026", "project": "firefox"},
            ),
            _scope_source(
                "source_b",
                scope_refs={"time": "fy2025", "project": "thunderbird"},
            ),
        ]
    )

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context=external_context,
        **SCOPE,
    )

    assert state.status == "scope_selector_no_match"
    assert state.plan is None
    assert state.forced_answer == (
        "I can’t safely complete that evidence request with the currently "
        "available source capabilities."
    )
    assert dsa.calls == ["list_sources"]
    assert [name for name, _ in runtime.calls] == ["shape"]


@pytest.mark.parametrize(
    ("dimension", "declared_field"),
    [
        ("time", "time_scope_ref"),
        ("version", "version_scope_ref"),
        ("domain", "domain_scope_ref"),
        ("project", "project_scope_ref"),
    ],
)
def test_unrequested_scope_derivation_requires_unanimous_non_null_values(
    dimension,
    declared_field,
):
    value = f"{dimension}-shared"
    unanimous = [
        _scope_source("source_a", scope_refs={dimension: value}),
        _scope_source("source_b", scope_refs={dimension: value}),
    ]
    mixed = copy.deepcopy(unanimous)
    mixed[1]["scope_refs"][dimension] = f"{dimension}-other"
    missing = copy.deepcopy(unanimous)
    missing[1].pop("scope_refs")

    scope, _ = _resolved_scope(unanimous)
    assert scope[declared_field] == value
    scope, _ = _resolved_scope(mixed)
    assert scope[declared_field] is None
    scope, _ = _resolved_scope(missing)
    assert scope[declared_field] is None
    scope, _ = _resolved_scope(
        unanimous,
        external_context={"source_ids": ["source_a", "missing_source"]},
    )
    assert scope[declared_field] is None
    scope, _ = _resolved_scope([])
    assert scope[declared_field] is None


def test_requested_narrowing_precedes_independent_unrequested_derivation():
    scope, matched = _resolved_scope(
        [
            _scope_source(
                "source_a",
                scope_refs={"time": "fy2026", "project": "firefox"},
            ),
            _scope_source(
                "source_b",
                scope_refs={"time": "fy2025", "project": "thunderbird"},
            ),
        ],
        external_context={"scope_refs": {"project": "firefox"}},
    )

    assert matched is True
    assert scope["source_ids"] == ["source_a"]
    assert scope["project_scope_ref"] == "firefox"
    assert scope["time_scope_ref"] == "fy2026"


def test_categories_names_and_identifiers_cannot_manufacture_material_scope():
    scope, matched = _resolved_scope(
        [
            _source(
                "fy2026",
                display_name="Firefox release-152 credential-management",
                tags=["firefox", "credential-management"],
            )
        ],
        external_context={"domain_tags": ["credential-management"]},
    )

    assert matched is True
    assert scope["source_categories"] == ["credential-management"]
    assert scope["time_scope_ref"] is None
    assert scope["version_scope_ref"] is None
    assert scope["domain_scope_ref"] is None
    assert scope["project_scope_ref"] is None


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


@pytest.mark.asyncio
async def test_scope_selector_and_unanimous_refs_reach_cr_without_raw_metadata():
    plan = _plan_response()
    plan["result"]["eligible_source_ids"] = ["source_a"]
    runtime = FakeRuntime(plan=plan)
    dsa = FakeDsa(
        [
            _source(
                "source_b",
                scope_refs={
                    "time": "fy2025",
                    "version": "release-151",
                    "domain": "credential-management",
                    "project": "thunderbird",
                },
            ),
            _source(
                "source_a",
                scope_refs={
                    "time": "fy2026",
                    "version": "release-152",
                    "domain": "credential-management",
                    "project": "firefox",
                },
            ),
        ]
    )

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text=QUESTION,
        interaction_kind="question",
        external_context={"scope_refs": {"project": "firefox"}},
        **SCOPE,
    )

    assert state.status == "acquisition_ready"
    assert state.declared_scope == {
        "source_ids": ["source_a"],
        "source_categories": [],
        "exact_source_refs": [],
        "inventory_status": "unknown",
        "time_scope_ref": "fy2026",
        "version_scope_ref": "release-152",
        "domain_scope_ref": "credential-management",
        "project_scope_ref": "firefox",
    }
    plan_payload = runtime.calls[1][1]
    assert plan_payload["declared_scope"] == state.declared_scope
    assert plan_payload["source_inventory"] == _adapt_inventory(state.inventory)
    assert "scope_refs" not in json.dumps(plan_payload["source_inventory"])
    retained_manifest = build_manifest_trace(
        state=state,
        context_pack=None,
        dsa_trace={"called": False, "status": "not_called"},
        retained_source_refs=set(),
    )
    retained_text = json.dumps(retained_manifest, sort_keys=True)
    for raw_metadata in ("fy2025", "release-151", "thunderbird"):
        assert raw_metadata not in retained_text


@pytest.mark.asyncio
async def test_legacy_inventory_without_selector_uses_natural_matched_scope():
    runtime = FakeRuntime()
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([_source("source_a")]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.status == "acquisition_ready"
    assert state.declared_scope == {
        "source_ids": ["source_a"],
        "source_categories": [],
        "exact_source_refs": [],
        "inventory_status": "unknown",
        "time_scope_ref": None,
        "version_scope_ref": None,
        "domain_scope_ref": None,
        "project_scope_ref": None,
    }


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
async def test_successful_empty_legacy_inventory_fails_closed_without_scope():
    runtime = FakeRuntime(plan=_plan_response(status="unsupported"))
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa([]),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.status == "inventory_dependency_failed"
    assert [name for name, _ in runtime.calls] == ["shape"]


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
    task_context = runtime.calls[0][1]["task_context"]
    assert {key: value for key, value in task_context.items() if key != "source_discovery"} == {
        "evidence_input_kinds": ["external_source"],
        "external_verification_required": True,
        "freshness_sensitive": False,
        "high_stakes_accuracy_required": False,
        "continuation_of_prior_evidence_task": False,
        "prior_task_shape": None,
    }
    assert task_context["source_discovery"]["sources"][0]["source_id"] == "source_a"
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
async def test_not_applicable_uses_inventory_only_and_follows_existing_path():
    runtime = FakeRuntime(shape=_shape_response(status="not_applicable"))
    private_source_id = "private_inventory_source_sentinel"
    dsa = FakeDsa(
        [
            _source("source_a"),
            _source(
                private_source_id,
                enabled=False,
                status="disabled",
                display_name="PRIVATE-INVENTORY-DISPLAY-SENTINEL",
                tags=["PRIVATE_INVENTORY_TAG_SENTINEL"],
                scope_refs={"project": "PRIVATE_INVENTORY_SCOPE_SENTINEL"},
            ),
        ],
        inventory_metadata={
            "inventory_scope": "configured_sources",
            "inventory_status": "complete",
        },
    )

    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=dsa,
        task_text="Explain photosynthesis.",
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )

    assert state.follow_existing_path is True
    assert dsa.calls == ["list_sources"]
    assert state.inventory_discovery["outcome"] == "success"
    assert state.inventory_discovery["source_count"] == 2
    assert [name for name, _ in runtime.calls] == ["shape"]

    manifest = build_manifest_trace(
        state=state,
        context_pack=None,
        dsa_trace={
            "called": True,
            "status": "inventory_only",
            "inventory_discovery": state.inventory_discovery,
        },
        retained_source_refs=set(),
    )

    assert manifest["inventory"] == {
        "inventory_status": "unknown",
        "inventory_source_count": 0,
        "declared_source_count": 0,
        "declared_category_count": 0,
        "available_source_count": 0,
        "unavailable_source_count": 0,
        "disabled_source_count": 0,
        "unknown_source_count": 0,
    }
    assert manifest["acquisition"]["inventory_discovery"] == {
        "called": True,
        "outcome": "success",
        "inventory_status": "complete",
        "source_count": 2,
    }
    assert manifest["acquisition"]["dsa_outcome"] == "not_called"
    assert manifest["acquisition"]["strategy_attempted"] is None
    for field in (
        "sources_considered",
        "sources_selected",
        "sources_used",
        "source_summaries",
        "unavailable_source_ids",
        "failed_source_ids",
        "source_references_returned",
        "source_references_retained",
        "source_references_filtered_or_omitted",
        "source_references_attempted",
        "source_references_unsuccessful",
    ):
        assert manifest["acquisition"][field] == []
    for field in (
        "exact_reference_attempt_count",
        "expansion_attempt_count",
        "item_count",
        "usable_item_count",
        "prompt_retained_item_count",
    ):
        assert manifest["acquisition"][field] == 0
    serialized = json.dumps(manifest, sort_keys=True)
    for sentinel in (
        private_source_id,
        "PRIVATE-INVENTORY-DISPLAY-SENTINEL",
        "PRIVATE_INVENTORY_TAG_SENTINEL",
        "PRIVATE_INVENTORY_SCOPE_SENTINEL",
    ):
        assert sentinel not in serialized


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
    answer = _render_valid_answer(state)
    assert answer.endswith(COMPARISON_SCOPE_SUFFIX)
    assert answer.count(COMPARISON_SCOPE_SUFFIX) == 1


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
    answer = _render_valid_answer(state)
    assert provider_allowed(state) is True
    limitation = "Limitation: an optional selected source was not available."
    assert limitation in answer
    assert answer.endswith(TARGETED_SCOPE_SUFFIX)
    assert answer.count(limitation) == 1


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


def _optional_coverage_plan(limitation_codes):
    requirements = [
        *_plan_response()["result"]["declared_requirements"],
        {
            "requirement_id": "optional-selected-source-coverage",
            "requirement_kind": "selected_source_coverage",
            "criticality": "optional",
        },
    ]
    return PlanResult.model_validate(
        _plan_response(
            status="ready_with_limitations",
            requirements=requirements,
            limitations=limitation_codes,
        )["result"]
    )


@pytest.mark.parametrize(
    ("limitation_codes", "expected_outcome"),
    [
        (["source_inventory_partial"], "partial"),
        (["source_inventory_unknown"], "unknown"),
        (["source_inventory_unavailable"], "unavailable"),
        (["optional_source_unavailable"], "unavailable"),
        (["authoritative_source_unavailable"], "unavailable"),
        ([], "satisfied"),
        (
            [
                "source_inventory_partial",
                "source_inventory_unknown",
                "source_inventory_unavailable",
            ],
            "unavailable",
        ),
        (
            ["source_inventory_partial", "source_inventory_unknown"],
            "unknown",
        ),
    ],
    ids=[
        "partial-inventory",
        "unknown-inventory",
        "unavailable-inventory",
        "optional-source-unavailable",
        "authoritative-source-unavailable",
        "complete-coverage",
        "unavailable-precedence",
        "unknown-precedence",
    ],
)
def test_optional_selected_source_coverage_preserves_inventory_limitations(
    limitation_codes,
    expected_outcome,
):
    facts = _build_acquisition_facts(
        plan=_optional_coverage_plan(limitation_codes),
        context_pack=_validated_context_pack(),
        dsa_trace={"status": "success", "called": True},
        retained_source_refs={"source_a:record_1"},
    )

    assert {
        fact["requirement_id"]: fact["outcome"]
        for fact in facts
    } == {
        "context-delivery": "satisfied",
        "optional-selected-source-coverage": expected_outcome,
        "targeted-evidence": "satisfied",
    }


def test_material_selected_source_coverage_keeps_path_specific_outcome():
    requirements = [
        *_plan_response()["result"]["declared_requirements"],
        {
            "requirement_id": "material-selected-source-coverage",
            "requirement_kind": "selected_source_coverage",
            "criticality": "material",
        },
        {
            "requirement_id": "optional-selected-source-coverage",
            "requirement_kind": "selected_source_coverage",
            "criticality": "optional",
        },
    ]
    plan = PlanResult.model_validate(
        _plan_response(
            status="ready_with_limitations",
            requirements=requirements,
            limitations=["source_inventory_partial"],
        )["result"]
    )

    facts = _build_acquisition_facts(
        plan=plan,
        context_pack=_validated_context_pack(),
        dsa_trace={"status": "success", "called": True},
        retained_source_refs={"source_a:record_1"},
    )

    assert {
        fact["requirement_id"]: fact["outcome"]
        for fact in facts
    } == {
        "context-delivery": "satisfied",
        "material-selected-source-coverage": "unknown",
        "optional-selected-source-coverage": "partial",
        "targeted-evidence": "satisfied",
    }


class PartialInventorySufficiencyRuntime(FakeRuntime):
    def __init__(self, *, erase_limitation=False):
        super().__init__(
            plan={
                **_plan_response(
                    status="ready_with_limitations",
                    requirements=[
                        *_plan_response()["result"]["declared_requirements"],
                        {
                            "requirement_id": "optional-selected-source-coverage",
                            "requirement_kind": "selected_source_coverage",
                            "criticality": "optional",
                        },
                    ],
                    limitations=["source_inventory_partial"],
                ),
            }
        )
        self.erase_limitation = erase_limitation

    async def evaluate_evidence_sufficiency(self, **kwargs):
        self.calls.append(("sufficiency", kwargs))
        status = (
            "sufficient_for_declared_scope"
            if self.erase_limitation
            else "sufficient_with_limitations"
        )
        response = _sufficiency_response(
            kwargs["acquisition_manifest_id"],
            status=status,
            requirements=kwargs["declared_requirements"],
            task_shape=kwargs["task_shape"],
            evidence_plan_id=kwargs["evidence_plan_id"],
        )
        optional = next(
            item
            for item in response["result"]["evaluated_requirements"]
            if item["requirement_id"] == "optional-selected-source-coverage"
        )
        optional["effective_outcome"] = "partial"
        return response


@pytest.mark.asyncio
async def test_partial_inventory_fact_produces_limited_sufficiency():
    runtime = PartialInventorySufficiencyRuntime()
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

    sufficiency_call = runtime.calls[-1][1]
    assert {
        fact["requirement_id"]: fact["outcome"]
        for fact in sufficiency_call["acquisition_facts"]
    }["optional-selected-source-coverage"] == "partial"
    assert state.status == "sufficient_with_limitations"
    assert state.sufficiency is not None
    assert state.sufficiency.sufficiency_status == "sufficient_with_limitations"
    assert state.sufficiency.answer_constraints == [
        "qualify_conclusion",
        "disclose_limitations",
        "identify_unexamined_scope",
    ]
    assert provider_allowed(state) is True


@pytest.mark.asyncio
async def test_cr_cannot_erase_partial_inventory_limitation():
    runtime = PartialInventorySufficiencyRuntime(erase_limitation=True)
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

    assert state.status == "sufficiency_dependency_failed"
    assert state.sufficiency is None
    assert state.forced_answer == WITHHELD_ANSWER
    assert state.next_step is None
    assert state.next_step_selection_attempted is False
    assert provider_allowed(state) is False


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
    answer = _render_valid_answer(state)
    limitation = "Limitation: an optional selected source was not available."
    assert limitation in answer
    assert answer.endswith(TARGETED_SCOPE_SUFFIX)
    assert answer.count(limitation) == 1

def _candidate(
    *,
    disposition="supports",
    excerpts=None,
):
    return json.dumps(
        {
            "conclusion_disposition": disposition,
            "evidence_excerpts": excerpts
            or [
                {
                    "source_ref": "source_a:record_1",
                    "excerpt": "PRIVATE SOURCE CONTENT",
                }
            ],
        },
        separators=(",", ":"),
    )


def _render_valid_answer(state, *, disposition="supports"):
    validation, excerpts = validate_evidence_response_candidate(
        _candidate(disposition=disposition),
        context_pack=_validated_context_pack(),
        retained_source_refs=["source_a:record_1"],
    )
    return render_governed_evidence_answer(
        state=state,
        validation=validation,
        excerpts=excerpts,
    )


def test_governed_candidate_accepts_exact_retained_excerpt():
    validation, excerpts = validate_evidence_response_candidate(
        _candidate(),
        context_pack=_validated_context_pack(),
        retained_source_refs=["source_a:record_1"],
    )

    assert validation == EvidenceCandidateValidation(
        validation_status="valid",
        conclusion_disposition="supports",
        validated_excerpt_count=1,
        validated_source_references=("source_a:record_1",),
        failure_reason=None,
    )
    assert excerpts == (
        ValidatedEvidenceExcerpt(
            source_ref="source_a:record_1",
            excerpt="PRIVATE SOURCE CONTENT",
        ),
    )


def test_governed_candidate_preserves_order_and_normalizes_whitespace():
    context_pack = _validated_context_pack()
    context_pack["items"].append(
        {
            **context_pack["items"][0],
            "result_id": "result_2",
            "source_id": "source_b",
            "source_ref": "source_b:record_2",
            "text": "Second\n retained\t evidence.",
        }
    )
    context_pack["sources_used"] = ["source_a", "source_b"]
    validation, excerpts = validate_evidence_response_candidate(
        _candidate(
            disposition="mixed",
            excerpts=[
                {
                    "source_ref": "source_b:record_2",
                    "excerpt": "Second retained evidence.",
                },
                {
                    "source_ref": "source_a:record_1",
                    "excerpt": "PRIVATE SOURCE CONTENT",
                },
            ],
        ),
        context_pack=context_pack,
        retained_source_refs=[
            "source_a:record_1",
            "source_b:record_2",
        ],
    )

    assert validation.validation_status == "valid"
    assert validation.validated_source_references == (
        "source_b:record_2",
        "source_a:record_1",
    )
    assert [item.excerpt for item in excerpts] == [
        "Second retained evidence.",
        "PRIVATE SOURCE CONTENT",
    ]


def test_governed_candidate_accepts_punctuation_and_token_bounded_substring():
    context_pack = _validated_context_pack()
    context_pack["items"][0]["text"] = "Status: ready; evidence confirmed."
    validation, excerpts = validate_evidence_response_candidate(
        _candidate(
            excerpts=[
                {
                    "source_ref": "source_a:record_1",
                    "excerpt": "ready; evidence",
                }
            ]
        ),
        context_pack=context_pack,
        retained_source_refs=["source_a:record_1"],
    )

    assert validation.validation_status == "valid"
    assert excerpts[0].excerpt == "ready; evidence"


@pytest.mark.parametrize(
    ("content", "reason"),
    [
        ("free-form provider prose", "invalid_json"),
        ("null", "invalid_candidate"),
        ('"scalar"', "invalid_candidate"),
        ("{}", "invalid_candidate"),
        ('```json\n{"conclusion_disposition":"supports"}\n```', "invalid_json"),
        ('preamble {"conclusion_disposition":"supports"}', "invalid_json"),
        ('{"conclusion_disposition":"supports"} trailing', "invalid_json"),
        ('[{"conclusion_disposition":"supports"}]', "invalid_candidate"),
        ('{"conclusion_disposition":"unsupported","evidence_excerpts":[]}', "invalid_candidate"),
        ('{"conclusion_disposition":"supports","evidence_excerpts":[]}', "invalid_candidate"),
        (
            '{"conclusion_disposition":"supports",'
            '"conclusion_disposition":"mixed","evidence_excerpts":[]}',
            "invalid_json",
        ),
        (
            '{"conclusion_disposition":"supports","evidence_excerpts":'
            '[{"source_ref":"source_a:record_1","excerpt":"PRIVATE SOURCE CONTENT"}],'
            '"extra":true}',
            "invalid_candidate",
        ),
        (
            '{"conclusion_disposition":"supports","evidence_excerpts":'
            '[{"source_ref":"source_a:record_1","excerpt":null}]}',
            "invalid_candidate",
        ),
        (
            '{"conclusion_disposition":"supports","evidence_excerpts":'
            '[{"source_ref":1,"excerpt":"PRIVATE SOURCE CONTENT"}]}',
            "invalid_candidate",
        ),
        (
            '{"conclusion_disposition":"supports","evidence_excerpts":'
            '[{"source_ref":null,"excerpt":"PRIVATE SOURCE CONTENT"}]}',
            "invalid_candidate",
        ),
        (
            '{"conclusion_disposition":"supports","evidence_excerpts":['
            '{"source_ref":"source_a:record_1","excerpt":"PRIVATE SOURCE CONTENT"},'
            '{"source_ref":"source_b:record_2","excerpt":2}]}',
            "invalid_candidate",
        ),
    ],
)
def test_governed_candidate_rejects_malformed_structures(content, reason):
    validation, excerpts = validate_evidence_response_candidate(
        content,
        context_pack=_validated_context_pack(),
        retained_source_refs=["source_a:record_1"],
    )

    assert validation.validation_status == "invalid"
    assert validation.failure_reason == reason
    assert validation.validated_source_references == ()
    assert excerpts == ()


@pytest.mark.parametrize(
    ("excerpts", "reason"),
    [
        (
            [{"source_ref": "forged:record", "excerpt": "PRIVATE SOURCE CONTENT"}],
            "reference_not_retained",
        ),
        (
            [
                {
                    "source_ref": "source_a:record_1",
                    "excerpt": "private source content",
                }
            ],
            "excerpt_not_extractive",
        ),
        (
            [
                {
                    "source_ref": "source_a:record_1",
                    "excerpt": "PRIVATE CONTENT SOURCE",
                }
            ],
            "excerpt_not_extractive",
        ),
        (
            [
                {
                    "source_ref": "source_a:record_1",
                    "excerpt": "RIVATE",
                }
            ],
            "excerpt_token_boundary_invalid",
        ),
        (
            [
                {
                    "source_ref": "source_a:record_1",
                    "excerpt": "PRIVATE SOURCE CONTENT",
                },
                {
                    "source_ref": "source_a:record_1",
                    "excerpt": "PRIVATE SOURCE CONTENT",
                },
            ],
            "invalid_candidate",
        ),
        (
            [
                {
                    "source_ref": "source_a:record_1",
                    "excerpt": "x" * 501,
                }
            ],
            "invalid_candidate",
        ),
        (
            [
                {
                    "source_ref": "x" * 241,
                    "excerpt": "PRIVATE SOURCE CONTENT",
                }
            ],
            "invalid_candidate",
        ),
    ],
)
def test_governed_candidate_rejects_untrusted_or_nonextractive_evidence(
    excerpts,
    reason,
):
    validation, _ = validate_evidence_response_candidate(
        _candidate(excerpts=excerpts),
        context_pack=_validated_context_pack(),
        retained_source_refs=["source_a:record_1"],
    )

    assert validation.failure_reason == reason


def test_returned_but_not_prompt_retained_reference_is_rejected():
    validation, _ = validate_evidence_response_candidate(
        _candidate(),
        context_pack=_validated_context_pack(),
        retained_source_refs=[],
    )

    assert validation.failure_reason == "reference_not_retained"


@pytest.mark.parametrize(
    "task_shape",
    [
        "targeted_lookup",
        "cross_source_comparison",
        "bounded_exhaustive_review",
        "contradiction_review",
        "absence_or_coverage_check",
        "historical_reconstruction",
        "recommendation_or_decision_support",
    ],
)
@pytest.mark.parametrize(
    "disposition",
    ["supports", "does_not_support", "mixed", "descriptive"],
)
def test_governed_renderer_is_policy_owned_for_every_shape_and_disposition(
    task_shape,
    disposition,
):
    state = _rendering_state(task_shape=task_shape)
    validation, excerpts = validate_evidence_response_candidate(
        _candidate(disposition=disposition),
        context_pack=_validated_context_pack(),
        retained_source_refs=["source_a:record_1"],
    )

    first = render_governed_evidence_answer(
        state=state,
        validation=validation,
        excerpts=excerpts,
    )
    second = render_governed_evidence_answer(
        state=state,
        validation=validation,
        excerpts=excerpts,
    )

    assert first == second
    assert first.startswith("The retained evidence")
    assert "Retained evidence excerpt 1: PRIVATE SOURCE CONTENT" in first
    assert "source_a:record_1" not in first
    assert _candidate(disposition=disposition) not in first


def test_limited_governed_renderer_uses_existing_limitation_and_scope_boundary():
    state = _rendering_state(
        status="sufficient_with_limitations",
        evaluations=[
            {
                "requirement_id": "targeted-evidence",
                "requirement_kind": "targeted_evidence",
                "criticality": "material",
                "effective_outcome": "satisfied",
            },
            {
                "requirement_id": "optional-source",
                "requirement_kind": "selected_source_coverage",
                "criticality": "optional",
                "effective_outcome": "unavailable",
            },
        ],
        limitation_codes=["optional_source_unavailable"],
    )
    validation, excerpts = validate_evidence_response_candidate(
        _candidate(disposition="descriptive"),
        context_pack=_validated_context_pack(),
        retained_source_refs=["source_a:record_1"],
    )

    answer = render_governed_evidence_answer(
        state=state,
        validation=validation,
        excerpts=excerpts,
    )

    assert "Limitation:" in answer
    assert answer.endswith(TARGETED_SCOPE_SUFFIX)


def test_malformed_candidate_renderer_is_deterministic_and_content_free():
    state = _rendering_state()
    validation, excerpts = validate_evidence_response_candidate(
        "PRIVATE MALFORMED PROVIDER OUTPUT",
        context_pack=_validated_context_pack(),
        retained_source_refs=["source_a:record_1"],
    )

    answer = render_governed_evidence_answer(
        state=state,
        validation=validation,
        excerpts=excerpts,
    )

    assert answer == MALFORMED_EVIDENCE_RESPONSE
    assert "PRIVATE MALFORMED PROVIDER OUTPUT" not in answer


@pytest.mark.parametrize(
    "reason",
    [
        "invalid_json",
        "invalid_candidate",
        "reference_not_retained",
        "reference_not_unique",
        "excerpt_not_extractive",
        "excerpt_token_boundary_invalid",
    ],
)
def test_invalid_grounded_candidate_uses_helpful_recovery_only_after_successful_state(
    reason,
):
    state = _rendering_state(recovery_eligible=True)
    validation = EvidenceCandidateValidation(
        validation_status="invalid",
        conclusion_disposition=None,
        validated_excerpt_count=0,
        validated_source_references=(),
        failure_reason=reason,
    )

    assert helpful_grounded_recovery_allowed(
        state=state,
        validation=validation,
        provider_call_occurred=True,
    ) is True
    answer = render_governed_evidence_answer(
        state=state,
        validation=validation,
        excerpts=(),
        provider_call_occurred=True,
    )

    assert answer == HELPFUL_GROUNDED_RECOVERY_RESPONSE
    assert enforce_final_answer(answer, state) == answer
    assert reason not in answer


def test_helpful_grounded_recovery_preserves_trusted_limitation_disclosure():
    evaluations = [
        {
            "requirement_id": "targeted-evidence",
            "requirement_kind": "targeted_evidence",
            "criticality": "material",
            "effective_outcome": "satisfied",
        },
        {
            "requirement_id": "context-delivery",
            "requirement_kind": "context_delivery",
            "criticality": "material",
            "effective_outcome": "satisfied",
        },
        {
            "requirement_id": "selected-source-coverage",
            "requirement_kind": "selected_source_coverage",
            "criticality": "optional",
            "effective_outcome": "unavailable",
        },
    ]
    state = _rendering_state(
        status="sufficient_with_limitations",
        evaluations=evaluations,
        limitation_codes=["optional_source_unavailable"],
        recovery_eligible=True,
    )
    validation = EvidenceCandidateValidation(
        validation_status="invalid",
        conclusion_disposition=None,
        validated_excerpt_count=0,
        validated_source_references=(),
        failure_reason="invalid_json",
    )

    answer = render_governed_evidence_answer(
        state=state,
        validation=validation,
        excerpts=(),
        provider_call_occurred=True,
    )

    assert answer.startswith(HELPFUL_GROUNDED_RECOVERY_RESPONSE)
    assert answer.endswith("Limitation: an optional selected source was not available.")
    assert enforce_final_answer(answer, state) == answer
    assert TARGETED_SCOPE_SUFFIX not in answer


@pytest.mark.parametrize(
    "mutation",
    [
        "provider_not_called",
        "failed_material_acquisition",
        "no_retained_context",
        "insufficient",
        "unknown",
        "dependency_failure",
        "provider_blocked",
    ],
)
def test_helpful_grounded_recovery_rejects_untrusted_or_incomplete_state(mutation):
    state = _rendering_state(recovery_eligible=True)
    provider_call_occurred = True
    if mutation == "provider_not_called":
        provider_call_occurred = False
    elif mutation == "failed_material_acquisition":
        state.acquisition_facts[0]["outcome"] = "failed"
    elif mutation == "no_retained_context":
        state.acquisition_facts[1]["outcome"] = "unknown"
    elif mutation in {"insufficient", "unknown"}:
        state = _rendering_state(status=mutation, recovery_eligible=True)
    elif mutation == "dependency_failure":
        state.next_step_failure = "dependency_failure"
    elif mutation == "provider_blocked":
        state.forced_answer = WITHHELD_ANSWER
    validation = EvidenceCandidateValidation(
        validation_status="invalid",
        conclusion_disposition=None,
        validated_excerpt_count=0,
        validated_source_references=(),
        failure_reason="invalid_json",
    )

    assert helpful_grounded_recovery_allowed(
        state=state,
        validation=validation,
        provider_call_occurred=provider_call_occurred,
    ) is False
    assert render_governed_evidence_answer(
        state=state,
        validation=validation,
        excerpts=(),
        provider_call_occurred=provider_call_occurred,
    ) == MALFORMED_EVIDENCE_RESPONSE

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

    answer = _render_valid_answer(state)

    assert expected in answer
    assert answer.count(expected) == 1
    assert answer.endswith(TARGETED_SCOPE_SUFFIX)


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

    answer = _render_valid_answer(state)

    assert f"Limitation: {expected}" in answer
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

    first = _render_valid_answer(state)
    second = _render_valid_answer(state)

    assert first == second
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
        "source_match",
    }
    assert manifest["shape"]["source_match"] == {
        "status": "matched",
        "matched_source_ids": ["source_a"],
        "reason_codes": ["source_id_match"],
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


@pytest.mark.asyncio
async def test_manifest_retains_bounded_safe_source_summary_from_validated_dsa_fields():
    source_ref = "google_sheets:source_a:'Form responses 1'!A2:C3"
    runtime = FakeRuntime()
    state = await begin_evidence_acquisition(
        runtime=runtime,
        dsa=FakeDsa(
            [
                _source(
                    "source_a",
                    display_name="Migration records",
                    connector="google_sheets",
                    authority_role="authoritative",
                    tags=["operations"],
                )
            ]
        ),
        task_text=QUESTION,
        interaction_kind="question",
        external_context=None,
        **SCOPE,
    )
    context_raw = _context_pack()
    context_raw["items"][0]["source_ref"] = source_ref
    context = _validated_context_pack(context_raw)
    await evaluate_acquisition_sufficiency(
        state=state,
        runtime=runtime,
        context_pack=context,
        dsa_trace={"status": "success", "called": True, "raw_item_count": 1},
        retained_source_refs={source_ref},
        **SCOPE,
    )

    manifest = build_manifest_trace(
        state=state,
        context_pack=context,
        dsa_trace={"status": "success", "called": True, "raw_item_count": 1},
        retained_source_refs={source_ref},
    )

    assert manifest["acquisition"]["source_summaries"] == [
        {
            "source_id": "source_a",
            "display_name": "Migration records",
            "connector": "google_sheets",
            "authority_role": "authoritative",
            "domain_tags": ["operations"],
            "considered": True,
            "selected": True,
            "used": True,
            "returned_reference_count": 1,
            "retained_reference_count": 1,
            "safe_location_labels": [
                "Google Sheets tab “Form responses 1” — A2:C3"
            ],
            "contribution_reason_codes": ["retained_records_contributed"],
        }
    ]
    private = suppress_manifest_identifiers(manifest)
    assert private["acquisition"]["source_summaries"] == []
    assert private["acquisition"]["source_summaries_count"] == 1
    assert "Migration records" not in json.dumps(private, sort_keys=True)


def test_source_summary_uses_name_and_connector_from_ordinary_context_shape():
    source_ref = "google_sheets:source_a:'Form responses 1'!A2:C3"

    summaries = _source_summaries(
        inventory=None,
        items=[
            {
                "source_ref": source_ref,
                "source_name": "Migration records",
            }
        ],
        considered_sources=["source_a"],
        selected_sources=["source_a"],
        sources_used=["source_a"],
        returned_refs=[source_ref],
        retained_refs=[source_ref],
        exact_attempts=[],
    )

    assert summaries[0]["display_name"] == "Migration records"
    assert summaries[0]["connector"] == "google_sheets"
    assert summaries[0]["authority_role"] == "unknown"
    assert summaries[0]["domain_tags"] == []


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


def test_legacy_no_scope_premise_digest_remains_stable():
    premise = build_current_acquisition_premise(_next_step_test_state())

    assert _acquisition_premise_digest(premise) == (
        "sha256:6e798adb98a8e683748af8039a3c45e818565bcb6eecde51885fd9b0b00f1d67"
    )


def test_material_scope_premise_digest_is_canonical_for_source_and_field_order():
    base = build_current_acquisition_premise(_next_step_test_state()).model_dump(
        mode="json"
    )
    base["declared_scope"].update(
        {
            "time_scope_ref": "fy2026",
            "version_scope_ref": "release-152",
            "domain_scope_ref": "credential-management",
            "project_scope_ref": "firefox",
        }
    )
    first_source = base["source_inventory"][0]
    second_source = {
        **first_source,
        "source_id": "source_b",
        "source_categories": list(reversed(first_source["source_categories"])),
        "capabilities": list(reversed(first_source["capabilities"])),
    }
    base["source_inventory"] = [first_source, second_source]
    reordered = copy.deepcopy(base)
    reordered["source_inventory"] = list(reversed(reordered["source_inventory"]))
    reordered["declared_scope"] = dict(
        reversed(list(reordered["declared_scope"].items()))
    )

    assert _acquisition_premise_digest(
        EvidenceAcquisitionPremise.model_validate(base)
    ) == _acquisition_premise_digest(
        EvidenceAcquisitionPremise.model_validate(reordered)
    )


@pytest.mark.parametrize(
    ("scope_field", "value"),
    [
        ("time_scope_ref", "fy2026"),
        ("version_scope_ref", "release-152"),
        ("domain_scope_ref", "credential-management"),
        ("project_scope_ref", "firefox"),
    ],
)
def test_each_material_scope_reference_changes_existing_premise_identity(
    scope_field,
    value,
):
    state = _next_step_test_state()
    original = build_current_acquisition_premise(state)
    changed_data = original.model_dump(mode="json")
    changed_data["declared_scope"][scope_field] = value
    changed = EvidenceAcquisitionPremise.model_validate(changed_data)

    assert _acquisition_premise_digest(changed) != _acquisition_premise_digest(
        original
    )
    assert _manifest_id(
        scope=SCOPE,
        plan_id=state.plan.plan_id,
        selected_strategies=state.plan.selected_strategies,
        declared_scope=changed.declared_scope.model_dump(mode="json"),
    ) != _manifest_id(
        scope=SCOPE,
        plan_id=state.plan.plan_id,
        selected_strategies=state.plan.selected_strategies,
        declared_scope=original.declared_scope.model_dump(mode="json"),
    )


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


@pytest.mark.asyncio
@pytest.mark.parametrize("sufficiency_status", ["insufficient", "unknown"])
async def test_exact_targeted_advisory_result_clears_only_generic_withholding_barrier(
    sufficiency_status,
):
    state = _next_step_test_state(sufficiency_status=sufficiency_status)

    class Runtime:
        async def select_evidence_next_step(self, **kwargs):
            return {
                **SCOPE,
                "result": _next_step_result_payload(
                    state,
                    selected_next_step="withhold_unsupported_conclusion",
                    conclusion_disposition="requested_conclusion_withheld",
                    provider_disposition="allowed",
                ),
            }

    result = await select_evidence_next_step(
        state=state,
        runtime=Runtime(),
        **SCOPE,
    )

    assert result is not None
    assert result.selected_next_step == "withhold_unsupported_conclusion"
    assert result.conclusion_disposition == "requested_conclusion_withheld"
    assert result.provider_disposition == "allowed"
    assert result.reason_codes == ["unsupported_conclusion_withheld"]
    assert state.forced_answer is None
    assert advisory_provider_allowed(state) is True
    assert provider_allowed(state) is True


@pytest.mark.parametrize(
    "updates",
    [
        {"task_shape": "bounded_exhaustive_review"},
        {"task_shape": "absence_or_coverage_check"},
        {"task_shape": "contradiction_review"},
        {"task_shape": "historical_reconstruction"},
        {"task_shape": "recommendation_or_decision_support"},
        {"selected_next_step": "ask_narrow_clarification"},
        {"selected_next_step": "perform_additional_acquisition"},
        {"selected_next_step": "disclose_unexamined_scope"},
        {"reason_codes": ["unexamined_material_scope"]},
        {"unexpected_policy_authority": True},
    ],
)
def test_strict_next_step_model_rejects_broader_advisory_authority(updates):
    state = _next_step_test_state()
    payload = _next_step_result_payload(
        state,
        selected_next_step="withhold_unsupported_conclusion",
        conclusion_disposition="requested_conclusion_withheld",
        provider_disposition="allowed",
    )
    payload.update(updates)

    with pytest.raises(ValidationError):
        NextStepResult.model_validate(payload)


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
    "case",
    [
        "not_applicable_with_proposed_premise",
        "guard_without_proposed_premise",
        "changed_guard_without_additional_acquisition",
        "changed_guard_with_unchanged_premise",
        "unchanged_guard_with_changed_premise",
        "additional_acquisition_without_changed_guard",
        "answer_without_required_reason",
        "clarification_without_required_reason",
        "additional_acquisition_without_required_reason",
        "disclosure_without_required_reason",
        "qualified_partial_without_required_reason",
        "withheld_conclusion_without_required_reason",
        "clarification_target_on_non_clarification_step",
    ],
)
def test_strict_next_step_model_matches_runtime_structural_invariants(case):
    state = _next_step_test_state()
    current_digest = _acquisition_premise_digest(
        build_current_acquisition_premise(state)
    )
    changed_digest = "sha256:" + ("1" * 64)
    payload = _next_step_result_payload(
        state,
        selected_next_step="withhold_unsupported_conclusion",
        conclusion_disposition="requested_conclusion_withheld",
        provider_disposition="allowed",
    )

    if case == "not_applicable_with_proposed_premise":
        payload["proposed_premise_digest"] = changed_digest
    elif case == "guard_without_proposed_premise":
        payload["reacquisition_guard"] = "premise_already_attempted"
    elif case == "changed_guard_without_additional_acquisition":
        payload.update(
            reacquisition_guard="changed_premise_allowed",
            proposed_premise_digest=changed_digest,
        )
    elif case == "changed_guard_with_unchanged_premise":
        payload.update(
            selected_next_step="perform_additional_acquisition",
            provider_disposition="blocked",
            reacquisition_guard="changed_premise_allowed",
            proposed_premise_digest=current_digest,
            reason_codes=["changed_acquisition_premise_available"],
        )
    elif case == "unchanged_guard_with_changed_premise":
        payload.update(
            provider_disposition="blocked",
            reacquisition_guard="unchanged_premise_blocked",
            proposed_premise_digest=changed_digest,
        )
    elif case == "additional_acquisition_without_changed_guard":
        payload.update(
            selected_next_step="perform_additional_acquisition",
            provider_disposition="blocked",
            reacquisition_guard="premise_already_attempted",
            proposed_premise_digest=changed_digest,
            reason_codes=["changed_acquisition_premise_available"],
        )
    elif case == "answer_without_required_reason":
        payload.update(
            sufficiency_status="sufficient_for_declared_scope",
            selected_next_step="answer_within_declared_scope",
            conclusion_disposition="bounded_conclusion_allowed",
            provider_disposition="allowed",
            unresolved_material_requirement_ids=[],
            reason_codes=[],
        )
    elif case == "clarification_without_required_reason":
        payload.update(
            selected_next_step="ask_narrow_clarification",
            provider_disposition="blocked",
            clarification_target="source_scope",
            reason_codes=[],
        )
    elif case == "additional_acquisition_without_required_reason":
        payload.update(
            selected_next_step="perform_additional_acquisition",
            provider_disposition="blocked",
            reacquisition_guard="changed_premise_allowed",
            proposed_premise_digest=changed_digest,
            reason_codes=[],
        )
    elif case == "disclosure_without_required_reason":
        payload.update(
            selected_next_step="disclose_unexamined_scope",
            provider_disposition="blocked",
            reason_codes=[],
        )
    elif case == "qualified_partial_without_required_reason":
        payload.update(
            selected_next_step="provide_qualified_partial_answer",
            conclusion_disposition="qualified_partial_only",
            provider_disposition="allowed",
            reason_codes=[],
        )
    elif case == "withheld_conclusion_without_required_reason":
        payload["reason_codes"] = []
    elif case == "clarification_target_on_non_clarification_step":
        payload["clarification_target"] = "source_scope"
    else:  # pragma: no cover - the parameter list is closed above
        raise AssertionError("unsupported structural case")

    with pytest.raises(ValidationError):
        NextStepResult.model_validate(payload)


@pytest.mark.parametrize(
    "guard",
    ["unchanged_premise_blocked", "premise_already_attempted"],
)
def test_guarded_qualified_partial_next_step_is_valid(guard):
    state = _next_step_test_state()
    proposed_digest = (
        _acquisition_premise_digest(build_current_acquisition_premise(state))
        if guard == "unchanged_premise_blocked"
        else "sha256:" + ("1" * 64)
    )
    payload = _next_step_result_payload(
        state,
        selected_next_step="provide_qualified_partial_answer",
        conclusion_disposition="qualified_partial_only",
        provider_disposition="allowed",
        reacquisition_guard=guard,
        proposed_premise_digest=proposed_digest,
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
    proposed_digest = (
        _acquisition_premise_digest(build_current_acquisition_premise(state))
        if guard == "unchanged_premise_blocked"
        else "sha256:" + ("1" * 64)
    )
    payload = _next_step_result_payload(
        state,
        selected_next_step="provide_qualified_partial_answer",
        conclusion_disposition="qualified_partial_only",
        provider_disposition="allowed",
        reacquisition_guard=guard,
        proposed_premise_digest=proposed_digest,
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
        proposed_premise_digest=_acquisition_premise_digest(
            build_current_acquisition_premise(state)
        ),
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
    if guard == "unchanged_premise_blocked":
        proposal = ExactFetchProposal(
            plan=proposal.plan,
            declared_scope=proposal.declared_scope,
            exact_reference=proposal.exact_reference,
            premise=build_current_acquisition_premise(state),
        )

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
async def test_malformed_advisory_cannot_bypass_changed_premise_precedence():
    state = _next_step_test_state()

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
            return {
                **SCOPE,
                "result": _next_step_result_payload(
                    state,
                    selected_next_step="withhold_unsupported_conclusion",
                    conclusion_disposition="requested_conclusion_withheld",
                    provider_disposition="allowed",
                    reacquisition_guard="not_applicable",
                    proposed_premise_digest=_acquisition_premise_digest(
                        proposal.premise
                    ),
                ),
            }

    result = await select_evidence_next_step(
        state=state,
        runtime=SelectionRuntime(),
        proposal=proposal,
        **SCOPE,
    )

    assert result is None
    assert state.next_step is None
    assert state.next_step_history is None
    assert state.status == "next_step_dependency_failed"
    assert state.next_step_failure == "dependency_failure"
    assert state.forced_answer == NEXT_STEP_DEPENDENCY_ANSWER
    assert advisory_provider_allowed(state) is False
    assert provider_allowed(state) is False


@pytest.mark.asyncio
async def test_valid_exhausted_premise_advisory_remains_allowed():
    state = _next_step_test_state()

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
                selected_next_step="withhold_unsupported_conclusion",
                conclusion_disposition="requested_conclusion_withheld",
                provider_disposition="allowed",
                reacquisition_guard="premise_already_attempted",
                proposed_premise_digest=_acquisition_premise_digest(
                    proposal.premise
                ),
            )
            result["reason_codes"] = [
                "acquisition_premise_already_selected",
                "unsupported_conclusion_withheld",
            ]
            return {**SCOPE, "result": result}

    result = await select_evidence_next_step(
        state=state,
        runtime=SelectionRuntime(),
        proposal=proposal,
        **SCOPE,
    )

    assert result is not None
    assert result.reacquisition_guard == "premise_already_attempted"
    assert state.forced_answer is None
    assert len(state.next_step_history or []) == 1
    assert advisory_provider_allowed(state) is True
    assert provider_allowed(state) is True


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
