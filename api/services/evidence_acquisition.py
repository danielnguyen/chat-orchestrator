from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Identifier = Annotated[
    str,
    Field(
        min_length=1,
        max_length=120,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]
Surface = Annotated[
    str,
    Field(
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    ),
]
TaskShape = Literal[
    "targeted_lookup",
    "bounded_exhaustive_review",
    "cross_source_comparison",
    "contradiction_review",
    "absence_or_coverage_check",
    "historical_reconstruction",
    "recommendation_or_decision_support",
]
InteractionKind = Literal[
    "question",
    "command",
    "joke_or_playful",
    "vent_or_expression",
    "brainstorm",
    "mistake_or_failure_report",
    "high_impact_decision",
    "tense_debugging",
    "ambiguous",
]
ShapeReasonCode = Literal[
    "source_context_present",
    "external_verification_required",
    "freshness_sensitive",
    "high_stakes_accuracy_required",
    "explicit_evidence_language",
    "targeted_lookup_derived",
    "exhaustive_scope_requested",
    "comparison_requested",
    "contradiction_requested",
    "absence_scope_requested",
    "historical_reconstruction_requested",
    "decision_support_requested",
    "prior_shape_inherited",
    "ordinary_chat_without_material_evidence_scope",
    "non_evidence_interaction",
    "ambiguous_interaction_without_shape_signal",
    "multiple_incompatible_shapes",
]
PlanLimitationCode = Literal[
    "declared_source_missing_from_inventory",
    "declared_category_not_available",
    "source_inventory_partial",
    "source_inventory_unknown",
    "source_inventory_unavailable",
    "authoritative_source_missing",
    "authoritative_source_unavailable",
    "required_capability_unavailable",
    "targeted_only_not_exhaustive",
    "absence_scope_not_enumerable",
    "insufficient_comparison_scope",
    "contradiction_search_not_supported",
    "historical_time_scope_missing",
    "historical_sequence_not_supported",
    "decision_support_scope_insufficient",
    "optional_source_unavailable",
]
RequirementKind = Literal[
    "authoritative_inventory",
    "targeted_evidence",
    "exact_authoritative_fetch",
    "complete_scope_coverage",
    "selected_source_coverage",
    "structured_absence_check",
    "contradiction_search",
    "counterevidence_coverage",
    "historical_scope",
    "historical_sequence_coverage",
    "candidate_evidence_coverage",
    "cross_source_comparison",
    "context_delivery",
    "no_material_truncation",
]
SufficiencyReasonCode = Literal[
    "all_declared_requirements_satisfied",
    "optional_requirement_incomplete",
    "material_requirement_not_satisfied",
    "material_requirement_unknown",
    "material_requirement_missing",
    "unresolved_material_contradiction",
    "exhaustive_scope_incomplete",
    "absence_scope_unproven",
    "contradiction_sensitive_scope_unresolved",
]
AnswerConstraint = Literal[
    "qualify_conclusion",
    "disclose_limitations",
    "identify_unexamined_scope",
    "additional_acquisition_or_clarification_required",
    "withhold_unqualified_conclusion",
    "withhold_exhaustive_conclusion",
    "withhold_absence_conclusion",
    "withhold_contradiction_sensitive_conclusion",
]

AMBIGUOUS_ANSWER = (
    "I need a narrower evidence request before I can determine what should be checked."
)
UNSUPPORTED_ANSWER = (
    "I can’t safely complete that evidence request with the currently available "
    "source capabilities."
)
WITHHELD_ANSWER = (
    "I couldn’t verify that from the available source context, so I’m not going "
    "to present an unsupported conclusion."
)
LIMITATION_SUFFIX = (
    "Some optional source scope was unavailable, so this answer is limited to the "
    "material evidence that was successfully checked."
)
TARGETED_SCOPE_SUFFIX = (
    "This reflects only the targeted sources checked, not a complete search of every "
    "possible source."
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ShapeResult(StrictModel):
    derivation_id: Identifier
    question_anchor: Annotated[str, Field(min_length=1, max_length=500)]
    question_anchor_digest: Annotated[
        str,
        Field(pattern=r"^sha256:[0-9a-f]{64}$", min_length=71, max_length=71),
    ]
    derivation_status: Literal["derived", "not_applicable", "ambiguous"]
    task_shape: TaskShape | None = None
    candidate_task_shapes: list[TaskShape] = Field(max_length=7)
    evidence_scope_material: bool
    clarification_required: bool
    reason_codes: list[ShapeReasonCode] = Field(max_length=17)
    user_safe_summary: Annotated[str, Field(min_length=1, max_length=500)]

    @model_validator(mode="after")
    def validate_outcome(self) -> ShapeResult:
        if len(set(self.candidate_task_shapes)) != len(self.candidate_task_shapes):
            raise ValueError("duplicate_candidate_task_shape")
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise ValueError("duplicate_shape_reason_code")
        if self.derivation_status == "derived":
            if self.task_shape is None or self.candidate_task_shapes != [self.task_shape]:
                raise ValueError("invalid_derived_shape")
            if not self.evidence_scope_material or self.clarification_required:
                raise ValueError("invalid_derived_shape_flags")
        elif self.derivation_status == "not_applicable":
            if self.task_shape is not None or self.candidate_task_shapes:
                raise ValueError("invalid_not_applicable_shape")
            if self.evidence_scope_material or self.clarification_required:
                raise ValueError("invalid_not_applicable_shape_flags")
        else:
            if self.task_shape is not None:
                raise ValueError("invalid_ambiguous_shape")
            if not self.evidence_scope_material or not self.clarification_required:
                raise ValueError("invalid_ambiguous_shape_flags")
        return self


class ShapeResponse(StrictModel):
    request_id: Identifier
    owner_id: Identifier
    conversation_id: Identifier
    surface: Surface
    runtime_session_id: Identifier
    runtime_turn_id: Identifier
    result: ShapeResult


class Requirement(StrictModel):
    requirement_id: Identifier
    requirement_kind: RequirementKind
    criticality: Literal["material", "optional"]


class ExactSourceReference(StrictModel):
    source_id: Identifier
    source_ref: Annotated[str, Field(min_length=1, max_length=240)]

    @field_validator("source_ref")
    @classmethod
    def validate_opaque_source_ref(cls, value: str) -> str:
        if re.search(r"\s|://|\?", value):
            raise ValueError("unsafe_source_reference")
        return value


class PlanResult(StrictModel):
    plan_id: Identifier
    question_anchor: Annotated[str, Field(min_length=1, max_length=500)]
    question_anchor_digest: Annotated[
        str,
        Field(pattern=r"^sha256:[0-9a-f]{64}$", min_length=71, max_length=71),
    ]
    task_shape: TaskShape
    plan_status: Literal["ready", "ready_with_limitations", "unsupported"]
    completeness_expectation: Literal[
        "targeted_scope",
        "complete_for_declared_scope",
        "complete_for_selected_sources",
        "complete_for_time_window",
        "bounded_decision_support",
    ]
    contradiction_search_required: bool
    eligible_source_ids: list[Identifier] = Field(max_length=32)
    authoritative_source_ids: list[Identifier] = Field(max_length=32)
    selected_strategies: list[
        Literal[
            "targeted_retrieval",
            "exact_fetch",
            "bounded_full_context",
            "structured_query",
            "hybrid",
        ]
    ] = Field(max_length=5)
    declared_requirements: list[Requirement] = Field(min_length=1, max_length=32)
    limitation_codes: list[PlanLimitationCode] = Field(max_length=16)
    user_safe_summary: Annotated[str, Field(min_length=1, max_length=500)]

    @model_validator(mode="after")
    def validate_collections(self) -> PlanResult:
        collections = (
            self.eligible_source_ids,
            self.authoritative_source_ids,
            self.selected_strategies,
            self.limitation_codes,
        )
        if any(len(set(items)) != len(items) for items in collections):
            raise ValueError("duplicate_plan_collection_value")
        ids = [item.requirement_id for item in self.declared_requirements]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate_evidence_requirement")
        if self.plan_status == "ready_with_limitations" and not any(
            item.criticality == "optional" for item in self.declared_requirements
        ):
            raise ValueError("limited_plan_requires_optional_requirement")
        return self


class PlanResponse(StrictModel):
    request_id: Identifier
    owner_id: Identifier
    conversation_id: Identifier
    surface: Surface
    runtime_session_id: Identifier
    runtime_turn_id: Identifier
    result: PlanResult


class RequirementEvaluation(StrictModel):
    requirement_id: Identifier
    requirement_kind: RequirementKind
    criticality: Literal["material", "optional"]
    effective_outcome: Literal[
        "satisfied",
        "partial",
        "not_attempted",
        "unavailable",
        "unsupported",
        "failed",
        "excluded",
        "filtered",
        "truncated",
        "unresolved_contradiction",
        "unknown",
        "missing",
    ]


class SufficiencyResult(StrictModel):
    evaluation_id: Identifier
    task_shape: TaskShape
    sufficiency_status: Literal[
        "sufficient_for_declared_scope",
        "sufficient_with_limitations",
        "insufficient",
        "unknown",
    ]
    evaluated_requirements: list[RequirementEvaluation] = Field(max_length=32)
    reason_codes: list[SufficiencyReasonCode] = Field(max_length=9)
    answer_constraints: list[AnswerConstraint] = Field(max_length=8)
    qualification_required: bool
    additional_acquisition_required: bool
    user_safe_summary: Annotated[str, Field(min_length=1, max_length=500)]

    @model_validator(mode="after")
    def validate_outcome(self) -> SufficiencyResult:
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise ValueError("duplicate_sufficiency_reason_code")
        if len(set(self.answer_constraints)) != len(self.answer_constraints):
            raise ValueError("duplicate_answer_constraint")
        qualification_expected = (
            self.sufficiency_status != "sufficient_for_declared_scope"
        )
        acquisition_expected = self.sufficiency_status in {"insufficient", "unknown"}
        if self.qualification_required != qualification_expected:
            raise ValueError("qualification_flag_mismatch")
        if self.additional_acquisition_required != acquisition_expected:
            raise ValueError("additional_acquisition_flag_mismatch")
        return self


class SufficiencyResponse(StrictModel):
    request_id: Identifier
    owner_id: Identifier
    conversation_id: Identifier
    surface: Surface
    runtime_session_id: Identifier
    runtime_turn_id: Identifier
    evidence_plan_id: Identifier
    acquisition_manifest_id: Identifier
    result: SufficiencyResult


class DsaSourceEntry(StrictModel):
    source_id: Identifier
    display_name: Annotated[str, Field(min_length=1, max_length=240)]
    connector: Identifier
    domain_tags: list[Identifier] = Field(max_length=8)
    sensitivity: Literal["low", "medium", "high", "restricted"]
    access_mode: Literal["read_only"]
    capabilities: list[Literal["search", "fetch", "context", "profile"]] = Field(
        max_length=4
    )
    enabled: bool
    status: Literal["ready", "unavailable", "disabled", "unknown"]
    last_checked_at: datetime | None
    last_error: Annotated[str, Field(max_length=240)] | None = None

    @model_validator(mode="after")
    def validate_collections(self) -> DsaSourceEntry:
        if len(set(self.domain_tags)) != len(self.domain_tags):
            raise ValueError("duplicate_source_category")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("duplicate_source_capability")
        return self


class DsaSourceListResponse(StrictModel):
    sources: list[DsaSourceEntry] = Field(max_length=32)

    @model_validator(mode="after")
    def validate_source_ids(self) -> DsaSourceListResponse:
        ids = [source.source_id for source in self.sources]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate_source_id")
        return self


class DsaBudget(StrictModel):
    max_results: int | None = Field(default=None, ge=1, le=1000)
    returned_results: int = Field(ge=0, le=1000)
    estimated_bytes: int = Field(ge=0, le=5_000_000)
    truncated: bool


class DsaAvailableContext(StrictModel):
    context_mode: Identifier
    description: Annotated[str, Field(min_length=1, max_length=500)]


class DsaItem(StrictModel):
    result_id: Identifier
    source_type: Identifier
    source_id: Identifier
    source_name: Annotated[str, Field(min_length=1, max_length=240)]
    source_ref: Annotated[str, Field(min_length=1, max_length=240)]
    retrieved_at: datetime
    source_modified_at: datetime | None = None
    title: Annotated[str, Field(min_length=1, max_length=500)]
    content_type: Identifier
    text: Annotated[str, Field(min_length=1, max_length=12000)]
    confidence: Literal["none", "low", "medium", "high"]
    available_context: list[DsaAvailableContext] = Field(
        default_factory=list,
        max_length=16,
    )
    warnings: list[Annotated[str, Field(max_length=160)]] = Field(max_length=12)

    @field_validator("available_context", mode="before")
    @classmethod
    def validate_available_context_collection(cls, value: object) -> object:
        if not isinstance(value, list):
            raise ValueError("available_context_must_be_list")
        return value

    @field_validator("available_context")
    @classmethod
    def validate_unique_context_modes(
        cls,
        value: list[DsaAvailableContext],
    ) -> list[DsaAvailableContext]:
        context_modes = [descriptor.context_mode for descriptor in value]
        if len(set(context_modes)) != len(context_modes):
            raise ValueError("duplicate_available_context_mode")
        return value

    @field_validator("source_ref")
    @classmethod
    def validate_opaque_source_ref(cls, value: str) -> str:
        if re.search(r"\s|://|\?", value):
            raise ValueError("unsafe_source_reference")
        return value


class DsaSourceDiagnostic(StrictModel):
    source_id: Identifier
    score: int = Field(ge=-10_000, le=10_000)
    score_band: Identifier
    reasons: list[Identifier] = Field(max_length=8)

    @model_validator(mode="after")
    def validate_reasons(self) -> DsaSourceDiagnostic:
        if len(set(self.reasons)) != len(self.reasons):
            raise ValueError("duplicate_source_diagnostic_reason")
        return self


class DsaDiagnostics(StrictModel):
    selection_mode: Identifier
    considered_source_ids: list[Identifier] = Field(max_length=32)
    selected_source_ids: list[Identifier] = Field(max_length=32)
    source_diagnostics: list[DsaSourceDiagnostic] = Field(max_length=32)
    ranking_mode: Identifier
    candidate_counts_by_source: dict[Identifier, Annotated[int, Field(ge=0, le=10000)]]
    budget_truncated_candidates: bool

    @model_validator(mode="after")
    def validate_collections(self) -> DsaDiagnostics:
        if len(set(self.considered_source_ids)) != len(self.considered_source_ids):
            raise ValueError("duplicate_considered_source")
        if len(set(self.selected_source_ids)) != len(self.selected_source_ids):
            raise ValueError("duplicate_selected_source")
        diagnostic_ids = [item.source_id for item in self.source_diagnostics]
        if len(set(diagnostic_ids)) != len(diagnostic_ids):
            raise ValueError("duplicate_source_diagnostic")
        return self


class DsaError(StrictModel):
    code: Identifier


class DsaContextPackResponse(StrictModel):
    query_id: Identifier
    query: Annotated[str, Field(min_length=1, max_length=500)]
    sources_used: list[Identifier] = Field(max_length=32)
    items: list[DsaItem] = Field(max_length=1000)
    warnings: list[Annotated[str, Field(max_length=160)]] = Field(max_length=12)
    errors: list[DsaError] = Field(max_length=12)
    budget: DsaBudget
    diagnostics: DsaDiagnostics | None = None

    @model_validator(mode="after")
    def validate_collections(self) -> DsaContextPackResponse:
        if len(set(self.sources_used)) != len(self.sources_used):
            raise ValueError("duplicate_source_used")
        refs = [item.source_ref for item in self.items]
        if len(set(refs)) != len(refs):
            raise ValueError("duplicate_source_reference")
        return self


class DsaFetchItem(StrictModel):
    result_id: Identifier
    source_type: Identifier
    source_id: Identifier
    source_name: Annotated[str, Field(min_length=1, max_length=240)]
    source_ref: Annotated[str, Field(min_length=1, max_length=240)]
    retrieved_at: datetime
    source_modified_at: datetime | None = None
    cache_status: Literal["live", "cached", "stale", "unknown"]
    title: Annotated[str, Field(min_length=1, max_length=500)]
    content_type: Identifier
    text: Annotated[str, Field(min_length=1, max_length=12000)]
    url: Annotated[str, Field(max_length=2048)] | None = None
    confidence: Literal["none", "low", "medium", "high"]
    raw: dict[str, Any] | None = None
    available_context: list[DsaAvailableContext] = Field(max_length=16)
    warnings: list[Annotated[str, Field(max_length=160)]] = Field(max_length=12)

    @field_validator("source_ref")
    @classmethod
    def validate_opaque_source_ref(cls, value: str) -> str:
        if re.search(r"\s|://|\?", value):
            raise ValueError("unsafe_source_reference")
        return value

    @model_validator(mode="after")
    def reject_raw_data(self) -> DsaFetchItem:
        if self.raw is not None:
            raise ValueError("raw_fetch_data_not_allowed")
        return self


class DsaFetchResponse(StrictModel):
    query_id: Identifier
    answerable: bool
    confidence: Literal["none", "low", "medium", "high"]
    retrieval_mode: Literal["fetch"]
    results: list[DsaFetchItem] = Field(max_length=1)
    warnings: list[Annotated[str, Field(max_length=160)]] = Field(max_length=12)
    errors: list[DsaError] = Field(max_length=12)
    budget: DsaBudget

    @model_validator(mode="after")
    def validate_result_accounting(self) -> DsaFetchResponse:
        if self.answerable != bool(self.results):
            raise ValueError("fetch_answerability_mismatch")
        if self.budget.returned_results != len(self.results):
            raise ValueError("fetch_result_count_mismatch")
        result_ids = [item.result_id for item in self.results]
        if len(set(result_ids)) != len(result_ids):
            raise ValueError("duplicate_fetch_result")
        return self


@dataclass
class EvidenceAcquisitionState:
    enabled: bool
    attempted: bool
    status: str
    shape: ShapeResult | None = None
    inventory: DsaSourceListResponse | None = None
    declared_scope: dict[str, Any] | None = None
    plan: PlanResult | None = None
    manifest_id: str | None = None
    sufficiency: SufficiencyResult | None = None
    forced_answer: str | None = None
    follow_existing_path: bool = False
    acquisition_facts: list[dict[str, str]] | None = None
    exact_source_refs: list[dict[str, str]] | None = None
    exact_attempts: list[dict[str, Any]] | None = None

    @property
    def supported_targeted_path(self) -> bool:
        return bool(
            self.plan
            and self.plan.task_shape == "targeted_lookup"
            and self.plan.plan_status in {"ready", "ready_with_limitations"}
            and self.plan.selected_strategies == ["targeted_retrieval"]
            and not self.exact_source_refs
        )

    @property
    def supported_exact_path(self) -> bool:
        if not (
            self.plan
            and self.plan.task_shape == "targeted_lookup"
            and self.plan.plan_status in {"ready", "ready_with_limitations"}
            and self.plan.selected_strategies == ["exact_fetch"]
            and self.exact_source_refs
        ):
            return False
        referenced_sources = {
            item["source_id"] for item in self.exact_source_refs
        }
        eligible_sources = set(self.plan.eligible_source_ids)
        authoritative_sources = set(self.plan.authoritative_source_ids)
        requirement_kinds = {
            item.requirement_kind for item in self.plan.declared_requirements
        }
        return bool(
            referenced_sources == eligible_sources
            and authoritative_sources.issubset(eligible_sources)
            and {"targeted_evidence", "context_delivery"}.issubset(
                requirement_kinds
            )
            and (
                ("exact_authoritative_fetch" in requirement_kinds)
                == bool(referenced_sources & authoritative_sources)
            )
        )

    @property
    def supported_governed_path(self) -> bool:
        return self.supported_targeted_path or self.supported_exact_path


def disabled_evidence_trace(*, enabled: bool, reason: str) -> dict[str, Any]:
    return {
        "enabled": enabled,
        "attempted": False,
        "status": reason,
        "manifest_id": None,
        "assistant_message_id": None,
        "response_digest": None,
        "shape": {},
        "inventory": {},
        "plan": {},
        "acquisition": {},
        "sufficiency": {},
    }


def _scope(
    *,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
) -> dict[str, str]:
    return {
        "request_id": request_id,
        "owner_id": owner_id,
        "conversation_id": conversation_id,
        "surface": surface,
        "runtime_session_id": runtime_session_id,
        "runtime_turn_id": runtime_turn_id,
    }


def _normalize_exact_source_refs(
    external_context: dict[str, Any] | None,
) -> list[dict[str, str]]:
    config = external_context if isinstance(external_context, dict) else {}
    raw_references = config.get("exact_source_refs")
    if raw_references is None:
        return []
    if not isinstance(raw_references, list) or len(raw_references) > 16:
        raise ValueError("invalid_exact_source_references")
    references = [
        ExactSourceReference.model_validate(item).model_dump(mode="json")
        for item in raw_references
    ]
    source_refs = [item["source_ref"] for item in references]
    if len(set(source_refs)) != len(source_refs):
        raise ValueError("duplicate_exact_source_reference")
    source_ids = config.get("source_ids")
    if isinstance(source_ids, list) and source_ids:
        declared_source_ids = set(source_ids)
        if any(item["source_id"] not in declared_source_ids for item in references):
            raise ValueError("exact_source_reference_source_mismatch")
    return sorted(
        references,
        key=lambda item: (item["source_id"], item["source_ref"]),
    )


def ineligible_exact_evidence_state(
    *,
    enabled: bool,
    reason: str,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    external_context: dict[str, Any] | None,
) -> EvidenceAcquisitionState:
    references = _normalize_exact_source_refs(external_context)
    scope = _scope(
        request_id=request_id,
        owner_id=owner_id,
        conversation_id=conversation_id,
        surface=surface,
        runtime_session_id=runtime_session_id or "runtime_session_unavailable",
        runtime_turn_id=runtime_turn_id or "runtime_turn_unavailable",
    )
    declared_scope = {
        "source_ids": sorted(
            {
                item
                for item in (
                    external_context.get("source_ids", [])
                    if isinstance(external_context, dict)
                    else []
                )
                if isinstance(item, str) and item
            }
        ),
        "source_categories": sorted(
            {
                item
                for item in (
                    external_context.get("domain_tags", [])
                    if isinstance(external_context, dict)
                    else []
                )
                if isinstance(item, str) and item
            }
        ),
        "exact_source_refs": references,
        "inventory_status": "unknown",
        "time_scope_ref": None,
        "version_scope_ref": None,
        "domain_scope_ref": None,
        "project_scope_ref": None,
    }
    state = EvidenceAcquisitionState(
        enabled=enabled,
        attempted=False,
        status=reason,
        declared_scope=declared_scope,
        exact_source_refs=references,
        forced_answer=UNSUPPORTED_ANSWER,
    )
    state.manifest_id = _manifest_id(
        scope=scope,
        plan_id=None,
        selected_strategies=[],
        declared_scope=declared_scope,
    )
    return state


def _manifest_id(
    *,
    scope: dict[str, str],
    plan_id: str | None,
    selected_strategies: list[str],
    declared_scope: dict[str, Any] | None,
    query_id: str | None = None,
    considered_source_ids: list[str] | None = None,
    selected_source_ids: list[str] | None = None,
    exact_attempts: list[dict[str, Any]] | None = None,
) -> str:
    normalized_attempts = sorted(
        [
            {
                "source_id": item.get("source_id"),
                "source_ref": item.get("source_ref"),
                "outcome": item.get("outcome"),
                "query_id": item.get("query_id"),
            }
            for item in (exact_attempts or [])
        ],
        key=lambda item: (
            str(item.get("source_id") or ""),
            str(item.get("source_ref") or ""),
        ),
    )
    material = {
        **scope,
        "plan_id": plan_id,
        "selected_strategies": sorted(selected_strategies),
        "declared_scope": declared_scope or {},
        "query_id": query_id,
        "considered_source_ids": sorted(considered_source_ids or []),
        "selected_source_ids": sorted(selected_source_ids or []),
        "exact_attempts": normalized_attempts,
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"evidence_manifest_{hashlib.sha256(encoded.encode()).hexdigest()[:32]}"


def _adapt_inventory(source_list: DsaSourceListResponse) -> list[dict[str, Any]]:
    capability_map = {
        "search": "targeted_retrieval",
        "fetch": "exact_fetch",
        "context": "context_expansion",
    }
    inventory = []
    for source in sorted(source_list.sources, key=lambda item: item.source_id):
        if not source.enabled or source.status == "disabled":
            availability = "disabled"
        elif source.status == "ready":
            availability = "available"
        elif source.status == "unavailable":
            availability = "unavailable"
        else:
            availability = "unknown"
        inventory.append(
            {
                "source_id": source.source_id,
                "source_categories": sorted(source.domain_tags),
                "capabilities": sorted(
                    {
                        capability_map[capability]
                        for capability in source.capabilities
                        if capability in capability_map
                    }
                ),
                "availability": availability,
                "authority_role": "unknown",
            }
        )
    return inventory


def _validate_scope_echo(
    model: ShapeResponse | PlanResponse | SufficiencyResponse,
    scope: dict[str, str],
) -> None:
    if any(getattr(model, field) != value for field, value in scope.items()):
        raise ValueError("dependency_scope_mismatch")


def _validate_supported_plan(state: EvidenceAcquisitionState) -> bool:
    plan = state.plan
    if plan is None or plan.task_shape != "targeted_lookup":
        return False
    if plan.plan_status not in {"ready", "ready_with_limitations"}:
        return False
    requirement_kinds = {
        requirement.requirement_kind for requirement in plan.declared_requirements
    }
    if not {"targeted_evidence", "context_delivery"}.issubset(requirement_kinds):
        return False
    exact_references = state.exact_source_refs or []
    if exact_references:
        if plan.selected_strategies != ["exact_fetch"]:
            return False
        referenced_sources = {item["source_id"] for item in exact_references}
        eligible_sources = set(plan.eligible_source_ids)
        authoritative_sources = set(plan.authoritative_source_ids)
        if referenced_sources != eligible_sources:
            return False
        if not authoritative_sources.issubset(eligible_sources):
            return False
        exact_authoritative_declared = (
            "exact_authoritative_fetch" in requirement_kinds
        )
        if exact_authoritative_declared != bool(
            referenced_sources & authoritative_sources
        ):
            return False
        return True
    return (
        plan.selected_strategies == ["targeted_retrieval"]
        and "exact_authoritative_fetch" not in requirement_kinds
    )


async def begin_evidence_acquisition(
    *,
    runtime: Any,
    dsa: Any,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
    task_text: str,
    interaction_kind: str,
    external_context: dict[str, Any] | None,
) -> EvidenceAcquisitionState:
    try:
        exact_source_refs = _normalize_exact_source_refs(external_context)
    except Exception:
        return ineligible_exact_evidence_state(
            enabled=True,
            reason="invalid_exact_source_references",
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
            external_context=None,
        )
    state = EvidenceAcquisitionState(
        enabled=True,
        attempted=True,
        status="shape_requested",
        exact_source_refs=exact_source_refs,
    )
    scope = _scope(
        request_id=request_id,
        owner_id=owner_id,
        conversation_id=conversation_id,
        surface=surface,
        runtime_session_id=runtime_session_id,
        runtime_turn_id=runtime_turn_id,
    )
    try:
        shape_raw = await runtime.derive_evidence_shape(
            **scope,
            task_text=task_text,
            interaction_kind=interaction_kind,
            task_context={
                "evidence_input_kinds": (
                    ["external_source"] if exact_source_refs else []
                ),
                "external_verification_required": bool(exact_source_refs),
                "freshness_sensitive": False,
                "high_stakes_accuracy_required": False,
                "continuation_of_prior_evidence_task": False,
                "prior_task_shape": None,
            },
        )
        shape_response = ShapeResponse.model_validate(shape_raw)
        _validate_scope_echo(shape_response, scope)
        state.shape = shape_response.result
        expected_digest = (
            f"sha256:{hashlib.sha256(state.shape.question_anchor.encode()).hexdigest()}"
        )
        if state.shape.question_anchor_digest != expected_digest:
            raise ValueError("shape_anchor_digest_mismatch")
    except Exception:
        state.status = "shape_dependency_failed"
        state.forced_answer = UNSUPPORTED_ANSWER
        state.manifest_id = _manifest_id(
            scope=scope,
            plan_id=None,
            selected_strategies=[],
            declared_scope=None,
        )
        return state

    if state.shape.derivation_status == "not_applicable":
        state.status = (
            "not_applicable_exact_request"
            if exact_source_refs
            else "not_applicable"
        )
        state.follow_existing_path = not exact_source_refs
        state.forced_answer = UNSUPPORTED_ANSWER if exact_source_refs else None
        state.manifest_id = _manifest_id(
            scope=scope,
            plan_id=None,
            selected_strategies=[],
            declared_scope=None,
        )
        return state
    if state.shape.derivation_status == "ambiguous":
        state.status = "ambiguous"
        state.forced_answer = AMBIGUOUS_ANSWER
        state.manifest_id = _manifest_id(
            scope=scope,
            plan_id=None,
            selected_strategies=[],
            declared_scope=None,
        )
        return state

    try:
        source_list_raw = await dsa.list_sources()
        state.inventory = DsaSourceListResponse.model_validate(source_list_raw)
    except Exception:
        state.status = "inventory_dependency_failed"
        state.forced_answer = UNSUPPORTED_ANSWER
        state.manifest_id = _manifest_id(
            scope=scope,
            plan_id=None,
            selected_strategies=[],
            declared_scope=None,
        )
        return state

    config = external_context if isinstance(external_context, dict) else {}
    source_ids = sorted(
        {
            item
            for item in config.get("source_ids", [])
            if isinstance(item, str) and item
        }
    )
    source_categories = sorted(
        {
            item
            for item in config.get("domain_tags", [])
            if isinstance(item, str) and item
        }
    )
    state.declared_scope = {
        "source_ids": source_ids,
        "source_categories": source_categories,
        "exact_source_refs": exact_source_refs,
        "inventory_status": "complete_for_declared_scope",
        "time_scope_ref": None,
        "version_scope_ref": None,
        "domain_scope_ref": None,
        "project_scope_ref": None,
    }
    try:
        plan_raw = await runtime.compile_evidence_plan(
            **scope,
            question_anchor=state.shape.question_anchor,
            task_shape=state.shape.task_shape,
            declared_scope=state.declared_scope,
            source_inventory=_adapt_inventory(state.inventory),
        )
        plan_response = PlanResponse.model_validate(plan_raw)
        _validate_scope_echo(plan_response, scope)
        if (
            plan_response.result.question_anchor != state.shape.question_anchor
            or plan_response.result.question_anchor_digest
            != state.shape.question_anchor_digest
            or plan_response.result.task_shape != state.shape.task_shape
        ):
            raise ValueError("plan_shape_mismatch")
        state.plan = plan_response.result
    except Exception:
        state.status = "plan_dependency_failed"
        state.forced_answer = UNSUPPORTED_ANSWER
        state.manifest_id = _manifest_id(
            scope=scope,
            plan_id=None,
            selected_strategies=[],
            declared_scope=state.declared_scope,
        )
        return state

    state.manifest_id = _manifest_id(
        scope=scope,
        plan_id=state.plan.plan_id,
        selected_strategies=state.plan.selected_strategies,
        declared_scope=state.declared_scope,
    )
    if not _validate_supported_plan(state):
        state.status = "unsupported_plan"
        state.forced_answer = UNSUPPORTED_ANSWER
        return state
    state.status = "acquisition_ready"
    return state


def validate_context_pack_response(
    response: dict[str, Any],
    *,
    expected_query: str,
    eligible_source_ids: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    validated = DsaContextPackResponse.model_validate(response)
    if validated.query != expected_query:
        raise ValueError("context_pack_query_mismatch")
    eligible_sources = set(eligible_source_ids)
    sources_used = set(validated.sources_used)
    for item in validated.items:
        if item.source_id not in eligible_sources:
            raise ValueError("context_item_source_not_eligible")
        if item.source_id not in sources_used:
            raise ValueError("context_item_source_not_used")
    if not sources_used.issubset(eligible_sources):
        raise ValueError("context_source_not_eligible")
    if validated.diagnostics is not None:
        considered_sources = set(validated.diagnostics.considered_source_ids)
        selected_sources = set(validated.diagnostics.selected_source_ids)
        if not considered_sources.issubset(eligible_sources):
            raise ValueError("diagnostic_considered_source_not_eligible")
        if not selected_sources.issubset(eligible_sources):
            raise ValueError("diagnostic_selected_source_not_eligible")
        if not selected_sources.issubset(considered_sources):
            raise ValueError("diagnostic_selected_source_not_considered")
        if selected_sources != sources_used:
            raise ValueError("diagnostic_selected_source_mismatch")
        if any(
            item.source_id not in considered_sources
            for item in validated.diagnostics.source_diagnostics
        ):
            raise ValueError("source_diagnostic_not_considered")
        if not set(validated.diagnostics.candidate_counts_by_source).issubset(
            selected_sources
        ):
            raise ValueError("candidate_count_source_not_selected")
    normalized = validated.model_dump(mode="json")
    for item in normalized["items"]:
        item.pop("available_context", None)
    return normalized


def validate_fetch_response(
    response: dict[str, Any],
    *,
    expected_source_id: str,
    expected_source_ref: str,
) -> DsaFetchResponse:
    validated = DsaFetchResponse.model_validate(response)
    for item in validated.results:
        if item.source_id != expected_source_id:
            raise ValueError("fetch_source_id_mismatch")
        if item.source_ref != expected_source_ref:
            raise ValueError("fetch_source_reference_mismatch")
    return validated


def _exact_bundle_id(
    *,
    plan_id: str,
    question_anchor_digest: str,
    attempts: list[dict[str, Any]],
) -> str:
    material = {
        "plan_id": plan_id,
        "question_anchor_digest": question_anchor_digest,
        "attempts": sorted(
            [
                {
                    "source_id": item["source_id"],
                    "source_ref": item["source_ref"],
                    "outcome": item["outcome"],
                    "query_id": item.get("query_id"),
                }
                for item in attempts
            ],
            key=lambda item: (item["source_id"], item["source_ref"]),
        ),
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"))
    return f"evidence_exact_bundle_{hashlib.sha256(encoded.encode()).hexdigest()[:32]}"


async def execute_exact_fetches(
    *,
    state: EvidenceAcquisitionState,
    dsa: Any,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if not state.supported_exact_path or state.plan is None or state.shape is None:
        return None, {
            "enabled": True,
            "called": False,
            "status": "not_called",
            "reason": "unsupported_exact_plan",
        }
    attempts: list[dict[str, Any]] = []
    safe_items: list[dict[str, Any]] = []
    aggregate_errors: set[str] = set()
    for reference in state.exact_source_refs or []:
        attempt: dict[str, Any] = {
            "source_id": reference["source_id"],
            "source_ref": reference["source_ref"],
            "outcome": "failed",
            "query_id": None,
        }
        try:
            response_raw = await dsa.fetch_source(
                source_ref=reference["source_ref"],
                include_raw=False,
                budget={
                    "max_results": 1,
                    "max_bytes": 50000,
                    "max_text_chars": 12000,
                },
            )
            if not isinstance(response_raw, dict):
                raise ValueError("malformed_fetch_response")
            response = validate_fetch_response(
                response_raw,
                expected_source_id=reference["source_id"],
                expected_source_ref=reference["source_ref"],
            )
            attempt["query_id"] = response.query_id
            if response.errors:
                attempt["outcome"] = "failed"
                aggregate_errors.update(item.code for item in response.errors)
            elif response.budget.truncated:
                attempt["outcome"] = "truncated"
                aggregate_errors.add("budget_truncated")
            elif not response.results:
                attempt["outcome"] = "unknown"
            else:
                attempt["outcome"] = "satisfied"
                for item in response.results:
                    safe_items.append(
                        {
                            "source_ref": item.source_ref,
                            "source_name": item.source_name,
                            "title": item.title,
                            "text": item.text,
                            "retrieved_at": item.retrieved_at.isoformat(),
                            "warnings": list(item.warnings),
                        }
                    )
        except ValueError:
            attempt["outcome"] = "filtered"
            aggregate_errors.add("malformed_response")
        except Exception:
            attempt["outcome"] = "failed"
            aggregate_errors.add("dependency_failure")
        attempts.append(attempt)

    state.exact_attempts = attempts
    successful_sources = sorted(
        {item["source_id"] for item in attempts if item["outcome"] == "satisfied"}
    )
    considered_sources = sorted({item["source_id"] for item in attempts})
    successful_counts = {
        source_id: sum(
            item["source_id"] == source_id and item["outcome"] == "satisfied"
            for item in attempts
        )
        for source_id in successful_sources
    }
    bundle = {
        "bundle_id": _exact_bundle_id(
            plan_id=state.plan.plan_id,
            question_anchor_digest=state.plan.question_anchor_digest,
            attempts=attempts,
        ),
        "query": state.plan.question_anchor,
        "sources_used": successful_sources,
        "items": safe_items,
        "errors": [{"code": code} for code in sorted(aggregate_errors)],
        "budget": {
            "max_results": len(attempts),
            "returned_results": len(safe_items),
            "estimated_bytes": sum(
                len(item["text"].encode("utf-8")) for item in safe_items
            ),
            "truncated": any(item["outcome"] == "truncated" for item in attempts),
        },
        "diagnostics": {
            "selection_mode": "exact_source_references",
            "considered_source_ids": considered_sources,
            "selected_source_ids": considered_sources,
            "source_diagnostics": [],
            "ranking_mode": "declared_exact_reference_order",
            "candidate_counts_by_source": successful_counts,
            "budget_truncated_candidates": any(
                item["outcome"] == "truncated" for item in attempts
            ),
        },
        "raw_item_count": len(safe_items),
    }
    outcome_counts = {
        outcome: sum(item["outcome"] == outcome for item in attempts)
        for outcome in ("satisfied", "unknown", "failed", "filtered", "truncated")
    }
    status = (
        "included"
        if outcome_counts["satisfied"] == len(attempts)
        else "error"
        if outcome_counts["failed"] or outcome_counts["filtered"]
        else "empty"
    )
    return bundle, {
        "enabled": True,
        "called": bool(attempts),
        "call_count": len(attempts),
        "status": status,
        "reason": "exact_fetch_completed",
        "error_code": (
            "malformed_response"
            if outcome_counts["filtered"]
            else "dependency_failure"
            if outcome_counts["failed"]
            else None
        ),
        "error_codes": sorted(aggregate_errors),
        "raw_item_count": len(safe_items),
        "budget_truncated": bool(outcome_counts["truncated"]),
        "candidate_truncated": False,
        "exact_attempt_counts": outcome_counts,
    }


def _delivery_reference_state(
    *,
    context_pack: dict[str, Any] | None,
    retained_source_refs: set[str] | None,
) -> tuple[str, set[str], set[str]]:
    items = (
        context_pack.get("items")
        if isinstance(context_pack, dict) and isinstance(context_pack.get("items"), list)
        else []
    )
    returned_refs = {
        item["source_ref"]
        for item in items
        if isinstance(item, dict) and isinstance(item.get("source_ref"), str)
    }
    if retained_source_refs is None:
        return "unknown", returned_refs, set()
    retained_refs = returned_refs.intersection(retained_source_refs)
    if retained_source_refs - returned_refs:
        return "unknown", returned_refs, retained_refs
    if retained_refs:
        return "satisfied", returned_refs, retained_refs
    if items and not retained_source_refs:
        return "filtered", returned_refs, set()
    return "unknown", returned_refs, set()


def _aggregate_exact_outcome(attempts: list[dict[str, Any]]) -> str:
    outcomes = {str(item.get("outcome")) for item in attempts}
    for outcome in ("truncated", "filtered", "failed", "unknown"):
        if outcome in outcomes:
            return outcome
    return "satisfied" if attempts and outcomes == {"satisfied"} else "unknown"


def _exact_delivery_reference_state(
    *,
    exact_source_refs: list[dict[str, str]],
    exact_attempts: list[dict[str, Any]],
    context_pack: dict[str, Any] | None,
    retained_source_refs: set[str] | None,
) -> tuple[str, set[str], set[str]]:
    _, returned_refs, retained_refs = _delivery_reference_state(
        context_pack=context_pack,
        retained_source_refs=retained_source_refs,
    )
    if retained_source_refs is None or retained_source_refs - returned_refs:
        return "unknown", returned_refs, retained_refs
    declared_refs = {item["source_ref"] for item in exact_source_refs}
    if _aggregate_exact_outcome(exact_attempts) != "satisfied":
        return "unknown", returned_refs, retained_refs
    if returned_refs == declared_refs and retained_refs == declared_refs:
        return "satisfied", returned_refs, retained_refs
    if returned_refs and declared_refs - retained_refs:
        return "filtered", returned_refs, retained_refs
    return "unknown", returned_refs, retained_refs


def _build_acquisition_facts(
    *,
    plan: PlanResult,
    context_pack: dict[str, Any] | None,
    dsa_trace: dict[str, Any],
    retained_source_refs: set[str] | None,
    exact_source_refs: list[dict[str, str]] | None = None,
    exact_attempts: list[dict[str, Any]] | None = None,
) -> list[dict[str, str]]:
    status = dsa_trace.get("status")
    error_code = dsa_trace.get("error_code")
    usable_count = len(context_pack.get("items", [])) if isinstance(context_pack, dict) else 0
    exact_path = plan.selected_strategies == ["exact_fetch"]
    if exact_path:
        delivery_outcome, _, _ = _exact_delivery_reference_state(
            exact_source_refs=exact_source_refs or [],
            exact_attempts=exact_attempts or [],
            context_pack=context_pack,
            retained_source_refs=retained_source_refs,
        )
    else:
        delivery_outcome, _, _ = _delivery_reference_state(
            context_pack=context_pack,
            retained_source_refs=retained_source_refs,
        )
    facts: list[dict[str, str]] = []
    for requirement in sorted(
        plan.declared_requirements,
        key=lambda item: item.requirement_id,
    ):
        if requirement.requirement_kind == "targeted_evidence":
            if exact_path:
                outcome = _aggregate_exact_outcome(exact_attempts or [])
            elif status == "error":
                outcome = "filtered" if error_code == "malformed_response" else "failed"
            elif usable_count:
                outcome = "satisfied"
            else:
                outcome = "unknown"
        elif requirement.requirement_kind == "context_delivery":
            outcome = delivery_outcome
        elif requirement.requirement_kind == "exact_authoritative_fetch":
            authoritative_sources = set(plan.authoritative_source_ids)
            authoritative_attempts = [
                item
                for item in (exact_attempts or [])
                if item.get("source_id") in authoritative_sources
            ]
            outcome = _aggregate_exact_outcome(authoritative_attempts)
        elif (
            requirement.requirement_kind == "selected_source_coverage"
            and requirement.criticality == "optional"
        ):
            outcome = (
                "unavailable"
                if {
                    "authoritative_source_unavailable",
                    "optional_source_unavailable",
                }
                & set(plan.limitation_codes)
                else "satisfied"
            )
        else:
            outcome = "unknown" if requirement.criticality == "material" else "satisfied"
        facts.append(
            {
                "requirement_id": requirement.requirement_id,
                "outcome": outcome,
            }
        )
    if len({fact["requirement_id"] for fact in facts}) != len(facts):
        raise ValueError("duplicate_acquisition_fact")
    return facts


async def evaluate_acquisition_sufficiency(
    *,
    state: EvidenceAcquisitionState,
    runtime: Any,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
    context_pack: dict[str, Any] | None,
    dsa_trace: dict[str, Any],
    retained_source_refs: set[str] | None,
) -> None:
    if not state.supported_governed_path or state.plan is None or state.manifest_id is None:
        return
    diagnostics = (
        context_pack.get("diagnostics")
        if isinstance(context_pack, dict)
        and isinstance(context_pack.get("diagnostics"), dict)
        else {}
    )
    scope = _scope(
        request_id=request_id,
        owner_id=owner_id,
        conversation_id=conversation_id,
        surface=surface,
        runtime_session_id=runtime_session_id,
        runtime_turn_id=runtime_turn_id,
    )
    state.manifest_id = _manifest_id(
        scope=scope,
        plan_id=state.plan.plan_id,
        selected_strategies=state.plan.selected_strategies,
        declared_scope=state.declared_scope,
        query_id=(
            context_pack.get("query_id")
            if isinstance(context_pack, dict)
            and isinstance(context_pack.get("query_id"), str)
            else None
        ),
        considered_source_ids=diagnostics.get("considered_source_ids", []),
        selected_source_ids=diagnostics.get("selected_source_ids", []),
        exact_attempts=state.exact_attempts,
    )
    facts = _build_acquisition_facts(
        plan=state.plan,
        context_pack=context_pack,
        dsa_trace=dsa_trace,
        retained_source_refs=retained_source_refs,
        exact_source_refs=state.exact_source_refs,
        exact_attempts=state.exact_attempts,
    )
    state.acquisition_facts = facts
    try:
        response_raw = await runtime.evaluate_evidence_sufficiency(
            **scope,
            evidence_plan_id=state.plan.plan_id,
            acquisition_manifest_id=state.manifest_id,
            task_shape=state.plan.task_shape,
            declared_requirements=[
                requirement.model_dump(mode="json")
                for requirement in state.plan.declared_requirements
            ],
            acquisition_facts=facts,
        )
        response = SufficiencyResponse.model_validate(response_raw)
        _validate_scope_echo(response, scope)
        if (
            response.evidence_plan_id != state.plan.plan_id
            or response.acquisition_manifest_id != state.manifest_id
            or response.result.task_shape != state.plan.task_shape
        ):
            raise ValueError("sufficiency_association_mismatch")
        expected_requirements = {
            requirement.requirement_id: (
                requirement.requirement_kind,
                requirement.criticality,
            )
            for requirement in state.plan.declared_requirements
        }
        evaluated = {
            item.requirement_id: (item.requirement_kind, item.criticality)
            for item in response.result.evaluated_requirements
        }
        if evaluated != expected_requirements:
            raise ValueError("sufficiency_requirement_mismatch")
        facts_by_id = {fact["requirement_id"]: fact["outcome"] for fact in facts}
        if any(
            item.effective_outcome != facts_by_id[item.requirement_id]
            for item in response.result.evaluated_requirements
        ):
            raise ValueError("sufficiency_fact_mismatch")
        material_outcomes = [
            item.effective_outcome
            for item in response.result.evaluated_requirements
            if item.criticality == "material"
        ]
        optional_outcomes = [
            item.effective_outcome
            for item in response.result.evaluated_requirements
            if item.criticality == "optional"
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
        expected_status = (
            "insufficient"
            if any(outcome in concrete_failures for outcome in material_outcomes)
            else "unknown"
            if any(outcome in {"missing", "unknown"} for outcome in material_outcomes)
            else "sufficient_with_limitations"
            if any(outcome != "satisfied" for outcome in optional_outcomes)
            else "sufficient_for_declared_scope"
        )
        if response.result.sufficiency_status != expected_status:
            raise ValueError("sufficiency_status_mismatch")
        expected_constraints = (
            []
            if expected_status == "sufficient_for_declared_scope"
            else [
                "qualify_conclusion",
                "disclose_limitations",
                "identify_unexamined_scope",
            ]
            if expected_status == "sufficient_with_limitations"
            else [
                "qualify_conclusion",
                "disclose_limitations",
                "identify_unexamined_scope",
                "additional_acquisition_or_clarification_required",
                "withhold_unqualified_conclusion",
            ]
        )
        if response.result.answer_constraints != expected_constraints:
            raise ValueError("sufficiency_constraints_mismatch")
        state.sufficiency = response.result
        state.status = response.result.sufficiency_status
        if response.result.sufficiency_status in {"insufficient", "unknown"}:
            state.forced_answer = WITHHELD_ANSWER
    except Exception:
        state.status = "sufficiency_dependency_failed"
        state.forced_answer = WITHHELD_ANSWER


def provider_allowed(state: EvidenceAcquisitionState | None) -> bool:
    if state is None or state.follow_existing_path:
        return True
    if state.forced_answer is not None:
        return False
    return bool(
        state.sufficiency
        and state.sufficiency.sufficiency_status
        in {"sufficient_for_declared_scope", "sufficient_with_limitations"}
    )


def enforce_final_answer(
    answer: str,
    state: EvidenceAcquisitionState | None,
) -> str:
    if state is None or state.follow_existing_path:
        return answer
    if state.forced_answer is not None:
        return state.forced_answer
    if state.sufficiency is None:
        return WITHHELD_ANSWER

    final = answer
    if state.sufficiency.sufficiency_status == "sufficient_with_limitations":
        final = _append_once(final, LIMITATION_SUFFIX)
    if state.plan and state.plan.task_shape == "targeted_lookup" and _overclaims_scope(final):
        final = _append_once(final, TARGETED_SCOPE_SUFFIX)
    return final


def _append_once(answer: str, sentence: str) -> str:
    stripped = answer.rstrip()
    if sentence in stripped:
        return stripped
    return f"{stripped}\n\n{sentence}" if stripped else sentence


_OVERCLAIM_PATTERN = re.compile(
    r"\b(?:all|every|complete|fully|none)\b|"
    r"\bno\s+(?:evidence|record)\b|"
    r"\bnothing\s+(?:exists|was\s+found)\b",
    re.IGNORECASE,
)


def _overclaims_scope(answer: str) -> bool:
    return bool(_OVERCLAIM_PATTERN.search(answer))


def _inventory_summary(
    inventory: DsaSourceListResponse | None,
    declared_scope: dict[str, Any] | None,
) -> dict[str, Any]:
    sources = inventory.sources if inventory is not None else []
    statuses = {"available": 0, "unavailable": 0, "disabled": 0, "unknown": 0}
    for source in sources:
        if not source.enabled or source.status == "disabled":
            statuses["disabled"] += 1
        elif source.status == "ready":
            statuses["available"] += 1
        elif source.status == "unavailable":
            statuses["unavailable"] += 1
        else:
            statuses["unknown"] += 1
    scope = declared_scope or {}
    return {
        "inventory_status": scope.get("inventory_status", "unknown"),
        "inventory_source_count": len(sources),
        "declared_source_count": len(scope.get("source_ids", [])),
        "declared_category_count": len(scope.get("source_categories", [])),
        "available_source_count": statuses["available"],
        "unavailable_source_count": statuses["unavailable"],
        "disabled_source_count": statuses["disabled"],
        "unknown_source_count": statuses["unknown"],
    }


def build_manifest_trace(
    *,
    state: EvidenceAcquisitionState,
    context_pack: dict[str, Any] | None,
    dsa_trace: dict[str, Any] | None,
    retained_source_refs: set[str] | None,
) -> dict[str, Any]:
    trace = dsa_trace if isinstance(dsa_trace, dict) else {}
    diagnostics = (
        context_pack.get("diagnostics")
        if isinstance(context_pack, dict)
        and isinstance(context_pack.get("diagnostics"), dict)
        else {}
    )
    items = (
        context_pack.get("items")
        if isinstance(context_pack, dict) and isinstance(context_pack.get("items"), list)
        else []
    )
    exact_path = state.supported_exact_path
    if exact_path:
        delivery_outcome, returned_ref_set, retained_ref_set = (
            _exact_delivery_reference_state(
                exact_source_refs=state.exact_source_refs or [],
                exact_attempts=state.exact_attempts or [],
                context_pack=context_pack,
                retained_source_refs=retained_source_refs,
            )
        )
    else:
        delivery_outcome, returned_ref_set, retained_ref_set = (
            _delivery_reference_state(
                context_pack=context_pack,
                retained_source_refs=retained_source_refs,
            )
        )
    returned_refs = sorted(returned_ref_set)
    retained_refs = sorted(retained_ref_set)
    omitted_refs = sorted(returned_ref_set - retained_ref_set)
    sources_used = sorted(
        context_pack.get("sources_used", [])
        if isinstance(context_pack, dict)
        else []
    )
    selected_sources = sorted(
        diagnostics.get("selected_source_ids", []) or sources_used
    )
    considered_sources = sorted(
        diagnostics.get("considered_source_ids", []) or selected_sources
    )
    plan = state.plan
    shape = state.shape
    exact_attempts = state.exact_attempts or []
    attempted_exact_refs = sorted(
        {
            item["source_ref"]
            for item in (state.exact_source_refs or [])
            if isinstance(item.get("source_ref"), str)
        }
    )
    unsuccessful_exact_refs = sorted(
        {
            str(item["source_ref"])
            for item in exact_attempts
            if item.get("outcome") != "satisfied"
            and isinstance(item.get("source_ref"), str)
        }
    )
    exact_outcome_counts = {
        outcome: sum(item.get("outcome") == outcome for item in exact_attempts)
        for outcome in ("satisfied", "unknown", "failed", "filtered", "truncated")
    }
    return {
        "enabled": state.enabled,
        "attempted": state.attempted,
        "status": state.status,
        "manifest_id": state.manifest_id,
        "assistant_message_id": None,
        "response_digest": None,
        "shape": {
            "derivation_status": shape.derivation_status if shape else "unavailable",
            "task_shape": shape.task_shape if shape else None,
            "candidate_count": len(shape.candidate_task_shapes) if shape else 0,
            "clarification_required": shape.clarification_required if shape else False,
            "reason_codes": list(shape.reason_codes) if shape else [],
        },
        "inventory": _inventory_summary(state.inventory, state.declared_scope),
        "plan": {
            "plan_id": plan.plan_id if plan else None,
            "plan_status": plan.plan_status if plan else "not_compiled",
            "completeness_expectation": (
                plan.completeness_expectation if plan else None
            ),
            "contradiction_search_required": (
                plan.contradiction_search_required if plan else False
            ),
            "selected_strategies": list(plan.selected_strategies) if plan else [],
            "material_requirement_count": (
                sum(item.criticality == "material" for item in plan.declared_requirements)
                if plan
                else 0
            ),
            "optional_requirement_count": (
                sum(item.criticality == "optional" for item in plan.declared_requirements)
                if plan
                else 0
            ),
            "limitation_codes": list(plan.limitation_codes) if plan else [],
        },
        "acquisition": {
            "strategy_attempted": (
                plan.selected_strategies[0]
                if state.supported_governed_path
                and plan
                and trace.get("called") is True
                else None
            ),
            "sources_considered": considered_sources,
            "sources_selected": selected_sources,
            "sources_used": sources_used,
            "source_references_returned": returned_refs,
            "source_references_retained": retained_refs,
            "source_references_filtered_or_omitted": omitted_refs,
            "source_references_attempted": attempted_exact_refs,
            "source_references_unsuccessful": unsuccessful_exact_refs,
            "exact_reference_attempts": sorted(
                [
                    {
                        "source_id": str(item["source_id"]),
                        "source_ref": str(item["source_ref"]),
                        "outcome": str(item["outcome"]),
                    }
                    for item in exact_attempts
                ],
                key=lambda item: (item["source_id"], item["source_ref"]),
            ),
            "exact_reference_attempt_count": len(exact_attempts),
            "exact_reference_successful_count": exact_outcome_counts["satisfied"],
            "exact_reference_unknown_count": exact_outcome_counts["unknown"],
            "exact_reference_failed_count": exact_outcome_counts["failed"],
            "exact_reference_filtered_count": exact_outcome_counts["filtered"],
            "exact_reference_truncated_count": exact_outcome_counts["truncated"],
            "unavailable_source_ids": sorted(
                source.source_id
                for source in (state.inventory.sources if state.inventory else [])
                if not source.enabled
                or source.status in {"unavailable", "disabled", "unknown"}
            ),
            "failed_source_ids": sorted(
                {
                    str(item["source_id"])
                    for item in exact_attempts
                    if item.get("outcome") == "failed"
                    and isinstance(item.get("source_id"), str)
                }
            ),
            "expansion_attempts": [],
            "item_count": int(trace.get("raw_item_count") or len(items)),
            "usable_item_count": len(items),
            "prompt_retained_item_count": len(retained_refs),
            "dsa_outcome": trace.get("status", "not_called"),
            "dsa_error_codes": sorted(
                {
                    code
                    for code in [
                        trace.get("error_code"),
                        *(trace.get("error_codes") or []),
                    ]
                    if isinstance(code, str)
                }
            ),
            "dsa_budget_truncation": bool(trace.get("budget_truncated")),
            "candidate_truncation": bool(trace.get("candidate_truncated")),
            "context_delivery_status": (
                "retained"
                if delivery_outcome == "satisfied"
                else "filtered"
                if delivery_outcome == "filtered"
                else "unknown"
            ),
            "requirement_facts": sorted(
                state.acquisition_facts or [],
                key=lambda item: item["requirement_id"],
            ),
        },
        "sufficiency": {
            "evaluation_id": state.sufficiency.evaluation_id if state.sufficiency else None,
            "status": (
                state.sufficiency.sufficiency_status if state.sufficiency else "not_evaluated"
            ),
            "reason_codes": (
                list(state.sufficiency.reason_codes) if state.sufficiency else []
            ),
            "answer_constraints": (
                list(state.sufficiency.answer_constraints) if state.sufficiency else []
            ),
            "qualification_required": (
                state.sufficiency.qualification_required if state.sufficiency else False
            ),
            "additional_acquisition_required": (
                state.sufficiency.additional_acquisition_required
                if state.sufficiency
                else False
            ),
        },
    }


def bind_manifest_response(
    manifest: dict[str, Any],
    *,
    assistant_message_ack: dict[str, Any],
    answer: str,
) -> None:
    message_id = assistant_message_ack.get("message_id")
    if not isinstance(message_id, str) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._:-]{0,119}",
        message_id,
    ):
        raise RuntimeError("assistant_message_ack_invalid")
    manifest["assistant_message_id"] = message_id
    manifest["response_digest"] = f"sha256:{hashlib.sha256(answer.encode()).hexdigest()}"


def suppress_manifest_identifiers(manifest: dict[str, Any]) -> dict[str, Any]:
    sanitized = json.loads(json.dumps(manifest))
    acquisition = sanitized.get("acquisition")
    if not isinstance(acquisition, dict):
        return sanitized
    identity_fields = (
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
    )
    for field in identity_fields:
        values = acquisition.get(field)
        acquisition[f"{field}_count"] = len(values) if isinstance(values, list) else 0
        acquisition[field] = []
    acquisition["source_identifiers_suppressed"] = True
    return sanitized
