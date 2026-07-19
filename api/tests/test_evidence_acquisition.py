from __future__ import annotations

import copy
import hashlib
import json

import pytest
from models import ChatRequest
from pydantic import ValidationError
from services.evidence_acquisition import (
    COMPARISON_SCOPE_SUFFIX,
    LIMITATION_SUFFIX,
    TARGETED_SCOPE_SUFFIX,
    WITHHELD_ANSWER,
    DsaItem,
    DsaSourceListResponse,
    EvidenceAcquisitionState,
    PlanResult,
    ShapeResult,
    SufficiencyResult,
    _build_acquisition_facts,
    _manifest_id,
    begin_evidence_acquisition,
    bind_manifest_response,
    build_manifest_trace,
    enforce_final_answer,
    evaluate_acquisition_sufficiency,
    execute_exact_fetches,
    execute_hybrid_comparison,
    provider_allowed,
    suppress_manifest_identifiers,
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


def test_comparison_scope_overclaim_boundary_is_bounded_and_idempotent():
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
        "The selected records differ."
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
async def test_exact_optional_limitation_allows_one_bounded_disclosure():
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
    assert answer.count(LIMITATION_SUFFIX) == 1
    assert enforce_final_answer(answer, state).count(LIMITATION_SUFFIX) == 1


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
