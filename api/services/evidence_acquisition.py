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
AcquisitionStrategy = Literal[
    "targeted_retrieval",
    "exact_fetch",
    "bounded_full_context",
    "structured_query",
    "hybrid",
]
NextStep = Literal[
    "answer_within_declared_scope",
    "provide_qualified_partial_answer",
    "perform_additional_acquisition",
    "ask_narrow_clarification",
    "disclose_unexamined_scope",
    "withhold_unsupported_conclusion",
]
ConclusionDisposition = Literal[
    "bounded_conclusion_allowed",
    "qualified_partial_only",
    "requested_conclusion_withheld",
]
ProviderDisposition = Literal["allowed", "blocked"]
ReacquisitionGuard = Literal[
    "not_applicable",
    "changed_premise_allowed",
    "unchanged_premise_blocked",
    "premise_already_attempted",
]
ClarificationTarget = Literal[
    "question_scope",
    "source_scope",
    "exact_reference",
    "time_scope",
    "version_scope",
    "domain_scope",
    "project_scope",
]
NextStepReasonCode = Literal[
    "declared_scope_sufficient",
    "optional_limitations_remain",
    "material_uncertainty_requires_clarification",
    "changed_acquisition_premise_available",
    "unchanged_acquisition_premise",
    "acquisition_premise_already_selected",
    "substantive_partial_evidence_available",
    "unexamined_material_scope",
    "unsupported_conclusion_withheld",
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
NEXT_STEP_DEPENDENCY_ANSWER = (
    "I couldn’t determine a safe evidence next step, so I’m withholding the "
    "requested conclusion."
)
TARGETED_SCOPE_SUFFIX = (
    "This reflects only the targeted sources checked, not a complete search of every "
    "possible source."
)
COMPARISON_SCOPE_SUFFIX = (
    "This comparison is limited to the selected sources and bounded context checked, "
    "not every potentially relevant source."
)
EXHAUSTIVE_SCOPE_SUFFIX = (
    "This conclusion is complete only for the declared source scope that was checked; "
    "sources outside that scope were not examined."
)
CONFIGURED_WORKSHEET_CONTEXT_MODE = "configured_worksheet"
BOUNDED_EXHAUSTIVE_CONTEXT_BUDGET = {
    "max_rows": 20,
    "max_bytes": 50000,
    "max_text_chars": 12000,
}

_SCOPE_BOUNDARIES = {
    "targeted_lookup": TARGETED_SCOPE_SUFFIX,
    "cross_source_comparison": COMPARISON_SCOPE_SUFFIX,
    "bounded_exhaustive_review": EXHAUSTIVE_SCOPE_SUFFIX,
}
_REQUIREMENT_DESCRIPTIONS = {
    "authoritative_inventory": "the authoritative source inventory",
    "targeted_evidence": "the requested targeted evidence",
    "exact_authoritative_fetch": "the exact authoritative item",
    "complete_scope_coverage": "the complete declared source scope",
    "selected_source_coverage": "coverage of every selected source",
    "structured_absence_check": (
        "an absence-supporting check of the declared source set"
    ),
    "contradiction_search": "the required contradiction search",
    "counterevidence_coverage": "counterevidence coverage",
    "historical_scope": "the required historical scope",
    "historical_sequence_coverage": "the historical sequence",
    "candidate_evidence_coverage": "candidate evidence coverage",
    "cross_source_comparison": "the selected-source comparison",
    "context_delivery": (
        "delivery of the required acquired evidence to the reasoning context"
    ),
    "no_material_truncation": "full delivery of the material evidence",
}
_PLAN_LIMITATION_DESCRIPTIONS = {
    "declared_source_missing_from_inventory": (
        "a declared optional source was missing from the configured inventory"
    ),
    "declared_category_not_available": (
        "a declared optional source category was unavailable"
    ),
    "source_inventory_partial": (
        "the configured source inventory was partial, so optional source coverage "
        "remains incomplete"
    ),
    "source_inventory_unknown": (
        "the completeness of the configured source inventory was unknown, so "
        "optional source coverage could not be established"
    ),
    "source_inventory_unavailable": (
        "the configured source inventory was unavailable, so optional source "
        "coverage could not be established"
    ),
    "authoritative_source_missing": (
        "an optional authoritative source was not established"
    ),
    "required_capability_unavailable": (
        "a capability required for optional evidence was unavailable"
    ),
    "targeted_only_not_exhaustive": (
        "optional evidence was limited to targeted retrieval"
    ),
    "absence_scope_not_enumerable": (
        "optional source scope could not support an absence check"
    ),
    "insufficient_comparison_scope": (
        "optional source coverage was insufficient for comparison"
    ),
    "contradiction_search_not_supported": (
        "optional contradiction-search coverage was unsupported"
    ),
    "historical_time_scope_missing": (
        "the optional historical time scope was not established"
    ),
    "historical_sequence_not_supported": (
        "optional historical-sequence coverage was unsupported"
    ),
    "decision_support_scope_insufficient": (
        "optional decision-support evidence remained incomplete"
    ),
}
_SOURCE_AVAILABILITY_LIMITATIONS = {
    "authoritative_source_unavailable",
    "optional_source_unavailable",
}
_MAX_RENDERED_EVIDENCE_CAUSES = 3
_ADMINISTRATIVE_REQUIREMENT_KINDS = {
    "authoritative_inventory",
    "context_delivery",
    "no_material_truncation",
}
_CLARIFICATION_QUESTIONS = {
    "question_scope": "What exact question or conclusion should I evaluate?",
    "source_scope": "Which bounded source or source set should I examine?",
    "exact_reference": "Which exact source reference should I retrieve?",
    "time_scope": "What time period should I examine?",
    "version_scope": "Which version should I use?",
    "domain_scope": "Which domain should bound the review?",
    "project_scope": "Which project should bound the review?",
}


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
        requirement_shapes = [
            (item.requirement_kind, item.criticality)
            for item in self.declared_requirements
        ]
        if len(set(requirement_shapes)) != len(requirement_shapes):
            raise ValueError("duplicate_evidence_requirement_shape")
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


class EvidenceDeclaredScope(StrictModel):
    source_ids: list[Identifier] = Field(default_factory=list, max_length=32)
    source_categories: list[Identifier] = Field(default_factory=list, max_length=16)
    exact_source_refs: list[ExactSourceReference] = Field(
        default_factory=list,
        max_length=16,
    )
    inventory_status: Literal[
        "complete_for_declared_scope",
        "partial",
        "unknown",
        "unavailable",
    ]
    time_scope_ref: Identifier | None = None
    version_scope_ref: Identifier | None = None
    domain_scope_ref: Identifier | None = None
    project_scope_ref: Identifier | None = None

    @model_validator(mode="after")
    def validate_unique_scope_values(self) -> EvidenceDeclaredScope:
        if len(set(self.source_ids)) != len(self.source_ids):
            raise ValueError("duplicate_declared_source_id")
        if len(set(self.source_categories)) != len(self.source_categories):
            raise ValueError("duplicate_declared_source_category")
        source_refs = [reference.source_ref for reference in self.exact_source_refs]
        if len(set(source_refs)) != len(source_refs):
            raise ValueError("duplicate_exact_source_ref")
        if self.source_ids and any(
            reference.source_id not in self.source_ids
            for reference in self.exact_source_refs
        ):
            raise ValueError("exact_source_ref_outside_declared_source_ids")
        return self


class EvidenceSourceDescriptor(StrictModel):
    source_id: Identifier
    source_categories: list[Identifier] = Field(max_length=8)
    capabilities: list[
        Literal[
            "targeted_retrieval",
            "exact_fetch",
            "bounded_full_context",
            "structured_query",
            "context_expansion",
        ]
    ] = Field(max_length=5)
    availability: Literal["available", "unavailable", "disabled", "unknown"]
    authority_role: Literal["authoritative", "supplemental", "unknown"]

    @model_validator(mode="after")
    def validate_unique_source_values(self) -> EvidenceSourceDescriptor:
        if len(set(self.source_categories)) != len(self.source_categories):
            raise ValueError("duplicate_source_category")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("duplicate_source_capability")
        return self


class EvidenceAcquisitionPremise(StrictModel):
    question_anchor_digest: Annotated[
        str,
        Field(pattern=r"^sha256:[0-9a-f]{64}$", min_length=71, max_length=71),
    ]
    task_shape: TaskShape
    declared_scope: EvidenceDeclaredScope
    source_inventory: list[EvidenceSourceDescriptor] = Field(max_length=32)
    selected_strategies: list[AcquisitionStrategy] = Field(max_length=5)

    @model_validator(mode="after")
    def validate_unique_premise_values(self) -> EvidenceAcquisitionPremise:
        source_ids = [source.source_id for source in self.source_inventory]
        if len(set(source_ids)) != len(source_ids):
            raise ValueError("duplicate_source_descriptor")
        if len(set(self.selected_strategies)) != len(self.selected_strategies):
            raise ValueError("duplicate_acquisition_strategy")
        return self


class NextStepResult(StrictModel):
    selection_id: Identifier
    evaluation_id: Identifier
    evidence_plan_id: Identifier
    acquisition_manifest_id: Identifier
    task_shape: TaskShape
    sufficiency_status: Literal[
        "sufficient_for_declared_scope",
        "sufficient_with_limitations",
        "insufficient",
        "unknown",
    ]
    selected_next_step: NextStep
    conclusion_disposition: ConclusionDisposition
    provider_disposition: ProviderDisposition
    current_premise_digest: Annotated[
        str,
        Field(pattern=r"^sha256:[0-9a-f]{64}$", min_length=71, max_length=71),
    ]
    proposed_premise_digest: Annotated[
        str,
        Field(pattern=r"^sha256:[0-9a-f]{64}$", min_length=71, max_length=71),
    ] | None = None
    reacquisition_guard: ReacquisitionGuard
    clarification_target: ClarificationTarget | None = None
    unresolved_material_requirement_ids: list[Identifier] = Field(max_length=32)
    reason_codes: list[NextStepReasonCode] = Field(max_length=4)
    user_safe_summary: Annotated[str, Field(min_length=1, max_length=500)]

    @model_validator(mode="after")
    def validate_policy_combination(self) -> NextStepResult:
        if self.unresolved_material_requirement_ids != sorted(
            set(self.unresolved_material_requirement_ids)
        ):
            raise ValueError("unordered_unresolved_material_requirements")
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise ValueError("duplicate_next_step_reason_code")

        terminal = self.sufficiency_status in {
            "sufficient_for_declared_scope",
            "sufficient_with_limitations",
        }
        if self.sufficiency_status == "sufficient_for_declared_scope":
            expected = (
                "answer_within_declared_scope",
                "bounded_conclusion_allowed",
                "allowed",
                "not_applicable",
            )
            actual = (
                self.selected_next_step,
                self.conclusion_disposition,
                self.provider_disposition,
                self.reacquisition_guard,
            )
            if actual != expected:
                raise ValueError("invalid_sufficient_next_step")
        elif self.sufficiency_status == "sufficient_with_limitations":
            expected = (
                "provide_qualified_partial_answer",
                "qualified_partial_only",
                "allowed",
                "not_applicable",
            )
            actual = (
                self.selected_next_step,
                self.conclusion_disposition,
                self.provider_disposition,
                self.reacquisition_guard,
            )
            if actual != expected:
                raise ValueError("invalid_limited_next_step")
        elif self.conclusion_disposition == "bounded_conclusion_allowed":
            raise ValueError("invalid_nonterminal_conclusion_disposition")
        if (
            self.selected_next_step == "answer_within_declared_scope"
            and self.sufficiency_status != "sufficient_for_declared_scope"
        ):
            raise ValueError("invalid_bounded_answer_next_step")

        if terminal and (
            self.proposed_premise_digest is not None
            or self.clarification_target is not None
        ):
            raise ValueError("terminal_next_step_has_follow_up")
        if self.selected_next_step == "ask_narrow_clarification":
            if (
                self.clarification_target is None
                or self.provider_disposition != "blocked"
                or self.conclusion_disposition != "requested_conclusion_withheld"
                or self.reacquisition_guard != "not_applicable"
            ):
                raise ValueError("invalid_clarification_next_step")
        elif self.clarification_target is not None:
            raise ValueError("unexpected_clarification_target")

        if self.selected_next_step == "provide_qualified_partial_answer" and (
            self.conclusion_disposition != "qualified_partial_only"
            or self.provider_disposition != "allowed"
        ):
            raise ValueError("invalid_partial_answer_next_step")
        if self.selected_next_step in {
            "disclose_unexamined_scope",
            "withhold_unsupported_conclusion",
        } and (
            self.conclusion_disposition != "requested_conclusion_withheld"
            or self.provider_disposition != "blocked"
        ):
            raise ValueError("invalid_blocked_next_step")
        if self.selected_next_step == "perform_additional_acquisition":
            if (
                self.sufficiency_status not in {"insufficient", "unknown"}
                or self.reacquisition_guard != "changed_premise_allowed"
                or self.proposed_premise_digest is None
                or self.provider_disposition != "blocked"
                or self.conclusion_disposition != "requested_conclusion_withheld"
            ):
                raise ValueError("invalid_additional_acquisition_next_step")
        elif self.reacquisition_guard == "changed_premise_allowed":
            raise ValueError("unexpected_changed_premise_guard")
        if (
            self.reacquisition_guard
            in {"unchanged_premise_blocked", "premise_already_attempted"}
        ):
            if (
                self.sufficiency_status not in {"insufficient", "unknown"}
                or self.proposed_premise_digest is None
                or self.selected_next_step
                == "perform_additional_acquisition"
            ):
                raise ValueError("invalid_blocked_reacquisition")
        return self


class NextStepResponse(StrictModel):
    request_id: Identifier
    owner_id: Identifier
    conversation_id: Identifier
    surface: Surface
    runtime_session_id: Identifier
    runtime_turn_id: Identifier
    result: NextStepResult


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
    authority_role: Literal["authoritative", "supplemental", "unknown"] = "unknown"

    @model_validator(mode="after")
    def validate_collections(self) -> DsaSourceEntry:
        if len(set(self.domain_tags)) != len(self.domain_tags):
            raise ValueError("duplicate_source_category")
        if len(set(self.capabilities)) != len(self.capabilities):
            raise ValueError("duplicate_source_capability")
        return self


class DsaSourceListResponse(StrictModel):
    inventory_scope: Literal["configured_sources"] | None = None
    inventory_status: Literal["complete", "partial", "unknown", "unavailable"] | None = (
        None
    )
    sources: list[DsaSourceEntry] = Field(max_length=32)

    @model_validator(mode="after")
    def validate_inventory(self) -> DsaSourceListResponse:
        metadata_fields = {"inventory_scope", "inventory_status"}
        supplied_metadata_fields = metadata_fields & self.model_fields_set
        if supplied_metadata_fields and supplied_metadata_fields != metadata_fields:
            raise ValueError("incomplete_inventory_metadata")
        if supplied_metadata_fields and (
            self.inventory_scope is None or self.inventory_status is None
        ):
            raise ValueError("invalid_inventory_metadata")
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


class DsaContextItem(StrictModel):
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

    @field_validator("available_context")
    @classmethod
    def validate_unique_context_modes(
        cls,
        value: list[DsaAvailableContext],
    ) -> list[DsaAvailableContext]:
        modes = [descriptor.context_mode for descriptor in value]
        if len(set(modes)) != len(modes):
            raise ValueError("duplicate_available_context_mode")
        return value

    @model_validator(mode="after")
    def reject_raw_data(self) -> DsaContextItem:
        if self.raw is not None:
            raise ValueError("raw_context_data_not_allowed")
        return self


class DsaContextResponse(StrictModel):
    query_id: Identifier
    answerable: bool
    confidence: Literal["none", "low", "medium", "high"]
    retrieval_mode: Literal["context"]
    results: list[DsaContextItem] = Field(max_length=250)
    warnings: list[Annotated[str, Field(max_length=160)]] = Field(max_length=12)
    errors: list[DsaError] = Field(max_length=12)
    budget: DsaBudget

    @model_validator(mode="after")
    def validate_result_accounting(self) -> DsaContextResponse:
        if self.answerable != bool(self.results):
            raise ValueError("context_answerability_mismatch")
        if self.budget.returned_results != len(self.results):
            raise ValueError("context_result_count_mismatch")
        result_ids = [item.result_id for item in self.results]
        if len(set(result_ids)) != len(result_ids):
            raise ValueError("duplicate_context_result")
        source_refs = [item.source_ref for item in self.results]
        if len(set(source_refs)) != len(source_refs):
            raise ValueError("duplicate_context_source_reference")
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
    expansion_attempts: list[dict[str, Any]] | None = None
    next_step: NextStepResult | None = None
    next_step_selection_attempted: bool = False
    next_step_failure: str | None = None
    next_step_history: list[dict[str, Any]] | None = None
    initial_attempt_summary: dict[str, Any] | None = None
    additional_acquisition_count: int = 0

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
    def supported_hybrid_comparison_path(self) -> bool:
        plan = self.plan
        if not (
            plan
            and plan.task_shape == "cross_source_comparison"
            and plan.plan_status in {"ready", "ready_with_limitations"}
            and plan.selected_strategies == ["hybrid"]
            and not self.exact_source_refs
            and plan.completeness_expectation == "complete_for_selected_sources"
            and plan.contradiction_search_required is False
            and 2 <= len(plan.eligible_source_ids) <= 8
            and len(set(plan.eligible_source_ids)) == len(plan.eligible_source_ids)
            and self.inventory is not None
        ):
            return False
        required_material = {
            "selected_source_coverage",
            "cross_source_comparison",
            "context_delivery",
        }
        material = {
            item.requirement_kind
            for item in plan.declared_requirements
            if item.criticality == "material"
        }
        if material != required_material:
            return False
        optional = {
            item.requirement_kind
            for item in plan.declared_requirements
            if item.criticality == "optional"
        }
        if optional - {"selected_source_coverage"}:
            return False
        eligible = set(plan.eligible_source_ids)
        if not set(plan.authoritative_source_ids).issubset(eligible):
            return False
        inventory_by_id = {
            source.source_id: source for source in self.inventory.sources
        }
        for source_id in eligible:
            source = inventory_by_id.get(source_id)
            if (
                source is None
                or not source.enabled
                or source.status != "ready"
                or "search" not in source.capabilities
                or "context" not in source.capabilities
            ):
                return False
        return True

    @property
    def supported_bounded_exhaustive_path(self) -> bool:
        plan = self.plan
        inventory = self.inventory
        declared_scope = self.declared_scope
        required_material = {
            "authoritative_inventory",
            "complete_scope_coverage",
            "contradiction_search",
            "context_delivery",
            "no_material_truncation",
        }
        if not (
            plan
            and inventory
            and isinstance(declared_scope, dict)
            and plan.task_shape == "bounded_exhaustive_review"
            and plan.plan_status == "ready"
            and plan.completeness_expectation == "complete_for_declared_scope"
            and plan.contradiction_search_required is True
            and plan.selected_strategies == ["hybrid"]
            and plan.limitation_codes == []
            and not self.exact_source_refs
            and not declared_scope.get("exact_source_refs")
            and len(plan.eligible_source_ids) == 1
            and plan.authoritative_source_ids == plan.eligible_source_ids
            and len(plan.declared_requirements) == len(required_material)
            and all(
                requirement.criticality == "material"
                for requirement in plan.declared_requirements
            )
            and {
                requirement.requirement_kind
                for requirement in plan.declared_requirements
            }
            == required_material
            and inventory.inventory_scope == "configured_sources"
            and inventory.inventory_status == "complete"
            and declared_scope.get("inventory_status")
            == "complete_for_declared_scope"
        ):
            return False

        declared_source_ids = set(declared_scope.get("source_ids") or [])
        declared_categories = set(declared_scope.get("source_categories") or [])
        if declared_source_ids:
            scoped_sources = [
                source
                for source in inventory.sources
                if source.source_id in declared_source_ids
            ]
        elif declared_categories:
            scoped_sources = [
                source
                for source in inventory.sources
                if set(source.domain_tags) & declared_categories
            ]
        else:
            scoped_sources = list(inventory.sources)
        if len(scoped_sources) != 1:
            return False

        source = scoped_sources[0]
        eligible_source_id = plan.eligible_source_ids[0]
        if declared_source_ids and declared_source_ids != {source.source_id}:
            return False
        return bool(
            source.source_id == eligible_source_id
            and source.enabled
            and source.status == "ready"
            and source.authority_role == "authoritative"
            and source.connector == "google_sheets"
            and {"search", "context"}.issubset(source.capabilities)
        )

    @property
    def supported_governed_path(self) -> bool:
        return (
            self.supported_targeted_path
            or self.supported_exact_path
            or self.supported_hybrid_comparison_path
            or self.supported_bounded_exhaustive_path
        )


@dataclass(frozen=True)
class ExactFetchProposal:
    plan: PlanResult
    declared_scope: dict[str, Any]
    exact_reference: dict[str, str]
    premise: EvidenceAcquisitionPremise


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
    expansion_attempts: list[dict[str, Any]] | None = None,
    delivery_identity: dict[str, Any] | None = None,
    initial_attempt_summary: dict[str, Any] | None = None,
    next_step_history: list[dict[str, Any]] | None = None,
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
        "expansion_attempts": sorted(
            [
                {
                    "source_id": item.get("source_id"),
                    "seed_source_ref": item.get("seed_source_ref"),
                    "context_mode": item.get("context_mode"),
                    "outcome": item.get("outcome"),
                    "query_id": item.get("query_id"),
                    "returned_reference_count": item.get(
                        "returned_reference_count"
                    ),
                }
                for item in (expansion_attempts or [])
            ],
            key=lambda item: (
                str(item.get("source_id") or ""),
                str(item.get("seed_source_ref") or ""),
                str(item.get("context_mode") or ""),
            ),
        ),
        "initial_attempt_summary": initial_attempt_summary,
        "next_step_history": [
            {
                "selection_id": item.get("selection_id"),
                "evaluation_id": item.get("evaluation_id"),
                "evidence_plan_id": item.get("evidence_plan_id"),
                "acquisition_manifest_id": item.get("acquisition_manifest_id"),
                "selected_next_step": item.get("selected_next_step"),
                "conclusion_disposition": item.get("conclusion_disposition"),
                "provider_disposition": item.get("provider_disposition"),
                "reacquisition_guard": item.get("reacquisition_guard"),
                "clarification_target": item.get("clarification_target"),
                "reason_codes": sorted(item.get("reason_codes") or []),
                "additional_acquisition_executed": bool(
                    item.get("additional_acquisition_executed")
                ),
            }
            for item in (next_step_history or [])[:2]
        ],
    }
    if delivery_identity is not None:
        material["delivery_identity"] = {
            "returned_source_refs": sorted(
                delivery_identity.get("returned_source_refs") or []
            ),
            "retained_source_refs": sorted(
                delivery_identity.get("retained_source_refs") or []
            ),
            "retention_status": delivery_identity.get("retention_status"),
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
                "authority_role": source.authority_role,
            }
        )
    return inventory


def _adapt_inventory_status(source_list: DsaSourceListResponse) -> str:
    if (
        source_list.inventory_scope is None
        or source_list.inventory_status is None
    ):
        return "unknown"
    return {
        "complete": "complete_for_declared_scope",
        "partial": "partial",
        "unknown": "unknown",
        "unavailable": "unavailable",
    }[source_list.inventory_status]


def _acquisition_premise_digest(premise: EvidenceAcquisitionPremise) -> str:
    scope = premise.declared_scope.model_copy(
        update={
            "source_ids": sorted(premise.declared_scope.source_ids),
            "source_categories": sorted(premise.declared_scope.source_categories),
            "exact_source_refs": sorted(
                premise.declared_scope.exact_source_refs,
                key=lambda reference: (reference.source_ref, reference.source_id),
            ),
        }
    )
    inventory = sorted(
        [
            source.model_copy(
                update={
                    "source_categories": sorted(source.source_categories),
                    "capabilities": sorted(source.capabilities),
                }
            )
            for source in premise.source_inventory
        ],
        key=lambda source: source.source_id,
    )
    material = {
        "question_anchor_digest": premise.question_anchor_digest,
        "task_shape": premise.task_shape,
        "declared_scope": scope.model_dump(mode="json"),
        "source_inventory": [
            source.model_dump(mode="json") for source in inventory
        ],
        "selected_strategies": sorted(premise.selected_strategies),
    }
    encoded = json.dumps(
        material,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return f"sha256:{hashlib.sha256(encoded.encode()).hexdigest()}"


def build_current_acquisition_premise(
    state: EvidenceAcquisitionState,
) -> EvidenceAcquisitionPremise:
    if (
        state.plan is None
        or state.inventory is None
        or not isinstance(state.declared_scope, dict)
    ):
        raise ValueError("current_acquisition_premise_unavailable")
    return EvidenceAcquisitionPremise.model_validate(
        {
            "question_anchor_digest": state.plan.question_anchor_digest,
            "task_shape": state.plan.task_shape,
            "declared_scope": state.declared_scope,
            "source_inventory": _adapt_inventory(state.inventory),
            "selected_strategies": state.plan.selected_strategies,
        }
    )


def deterministic_clarification_target(
    state: EvidenceAcquisitionState,
) -> ClarificationTarget | None:
    if state.sufficiency is None:
        return None
    uncertain_material = {
        evaluation.requirement_kind
        for evaluation in state.sufficiency.evaluated_requirements
        if evaluation.criticality == "material"
        and evaluation.effective_outcome in {"missing", "unknown"}
    }
    scope = state.declared_scope or {}
    if (
        "exact_authoritative_fetch" in uncertain_material
        and not scope.get("exact_source_refs")
    ):
        return "exact_reference"
    if (
        "historical_scope" in uncertain_material
        and scope.get("time_scope_ref") is None
    ):
        return "time_scope"
    scope_kinds = {
        "authoritative_inventory",
        "complete_scope_coverage",
        "selected_source_coverage",
        "structured_absence_check",
        "contradiction_search",
        "counterevidence_coverage",
        "historical_scope",
        "historical_sequence_coverage",
        "candidate_evidence_coverage",
        "cross_source_comparison",
    }
    if uncertain_material & scope_kinds and not any(
        scope.get(field)
        for field in ("source_ids", "source_categories", "exact_source_refs")
    ):
        return "source_scope"
    return None


def _safe_exact_fetch_candidate(
    state: EvidenceAcquisitionState,
    context_pack: dict[str, Any] | None,
) -> dict[str, str] | None:
    if (
        state.plan is None
        or state.inventory is None
        or state.sufficiency is None
        or not state.supported_targeted_path
        or state.plan.task_shape != "targeted_lookup"
        or state.plan.selected_strategies != ["targeted_retrieval"]
        or state.exact_source_refs
        or state.sufficiency.sufficiency_status not in {"insufficient", "unknown"}
        or not isinstance(context_pack, dict)
    ):
        return None
    eligible = set(state.plan.eligible_source_ids)
    inventory = {
        source["source_id"]: source for source in _adapt_inventory(state.inventory)
    }
    candidates: list[dict[str, str]] = []
    for item in context_pack.get("items") or []:
        if not isinstance(item, dict):
            continue
        source_id = item.get("source_id")
        source_ref = item.get("source_ref")
        source = inventory.get(source_id) if isinstance(source_id, str) else None
        if (
            source_id not in eligible
            or not isinstance(source_ref, str)
            or source is None
            or source["availability"] != "available"
            or "exact_fetch" not in source["capabilities"]
        ):
            continue
        try:
            reference = ExactSourceReference.model_validate(
                {"source_id": source_id, "source_ref": source_ref}
            )
        except Exception:
            continue
        candidates.append(reference.model_dump(mode="json"))
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda item: (item["source_id"], item["source_ref"]),
    )[0]


async def compile_safe_exact_fetch_proposal(
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
) -> ExactFetchProposal | None:
    candidate = _safe_exact_fetch_candidate(state, context_pack)
    if (
        candidate is None
        or state.plan is None
        or state.shape is None
        or state.inventory is None
        or not isinstance(state.declared_scope, dict)
    ):
        return None
    proposed_scope = json.loads(json.dumps(state.declared_scope))
    proposed_scope["exact_source_refs"] = [candidate]
    try:
        scope = _scope(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
        )
        response_raw = await runtime.compile_evidence_plan(
            **scope,
            question_anchor=state.plan.question_anchor,
            task_shape=state.plan.task_shape,
            declared_scope=proposed_scope,
            source_inventory=_adapt_inventory(state.inventory),
        )
        response = PlanResponse.model_validate(response_raw)
        _validate_scope_echo(response, scope)
        plan = response.result
        if (
            plan.question_anchor != state.plan.question_anchor
            or plan.question_anchor_digest != state.plan.question_anchor_digest
            or plan.task_shape != state.plan.task_shape
            or plan.plan_status not in {"ready", "ready_with_limitations"}
            or plan.selected_strategies != ["exact_fetch"]
            or candidate["source_id"] not in plan.eligible_source_ids
        ):
            return None
        proposed_state = EvidenceAcquisitionState(
            enabled=state.enabled,
            attempted=True,
            status="acquisition_ready",
            shape=state.shape,
            inventory=state.inventory,
            declared_scope=proposed_scope,
            plan=plan,
            exact_source_refs=[candidate],
        )
        if not proposed_state.supported_exact_path:
            return None
        premise = build_current_acquisition_premise(proposed_state)
        return ExactFetchProposal(
            plan=plan,
            declared_scope=proposed_scope,
            exact_reference=candidate,
            premise=premise,
        )
    except Exception:
        return None


async def select_evidence_next_step(
    *,
    state: EvidenceAcquisitionState,
    runtime: Any,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
    proposal: ExactFetchProposal | None = None,
    clarification_target: ClarificationTarget | None = None,
) -> NextStepResult | None:
    state.next_step_selection_attempted = True
    state.next_step_failure = None
    if (
        state.plan is None
        or state.sufficiency is None
        or state.manifest_id is None
        or len(state.next_step_history or []) >= 2
    ):
        state.next_step = None
        state.next_step_failure = "dependency_failure"
        state.status = "next_step_dependency_failed"
        state.forced_answer = NEXT_STEP_DEPENDENCY_ANSWER
        return None
    try:
        scope = _scope(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
        )
        current_premise = build_current_acquisition_premise(state)
        response_raw = await runtime.select_evidence_next_step(
            **scope,
            evaluation_id=state.sufficiency.evaluation_id,
            evidence_plan_id=state.plan.plan_id,
            acquisition_manifest_id=state.manifest_id,
            evaluated_requirements=[
                evaluation.model_dump(mode="json")
                for evaluation in state.sufficiency.evaluated_requirements
            ],
            current_premise=current_premise.model_dump(mode="json"),
            proposed_acquisition_premise=(
                proposal.premise.model_dump(mode="json")
                if proposal is not None
                else None
            ),
            clarification_target=clarification_target,
        )
        response = NextStepResponse.model_validate(response_raw)
        _validate_scope_echo(response, scope)
        result = response.result
        if (
            result.evaluation_id != state.sufficiency.evaluation_id
            or result.evidence_plan_id != state.plan.plan_id
            or result.acquisition_manifest_id != state.manifest_id
            or result.task_shape != state.plan.task_shape
            or result.sufficiency_status != state.sufficiency.sufficiency_status
            or result.current_premise_digest
            != _acquisition_premise_digest(current_premise)
        ):
            raise ValueError("next_step_association_mismatch")
        proposed_digest = (
            _acquisition_premise_digest(proposal.premise)
            if proposal is not None
            else None
        )
        if result.proposed_premise_digest != proposed_digest:
            raise ValueError("next_step_proposed_premise_mismatch")
        if (
            result.selected_next_step == "ask_narrow_clarification"
            and result.clarification_target != clarification_target
        ):
            raise ValueError("next_step_clarification_mismatch")
        expected_unresolved = sorted(
            evaluation.requirement_id
            for evaluation in state.sufficiency.evaluated_requirements
            if evaluation.criticality == "material"
            and evaluation.effective_outcome != "satisfied"
        )
        if result.unresolved_material_requirement_ids != expected_unresolved:
            raise ValueError("next_step_requirement_mismatch")
        if (
            result.selected_next_step == "perform_additional_acquisition"
            and proposal is None
        ):
            raise ValueError("next_step_proposal_missing")
        state.next_step = result
        state.next_step_history = [
            *(state.next_step_history or []),
            {
                "selection_id": result.selection_id,
                "evaluation_id": result.evaluation_id,
                "evidence_plan_id": result.evidence_plan_id,
                "acquisition_manifest_id": result.acquisition_manifest_id,
                "selected_next_step": result.selected_next_step,
                "conclusion_disposition": result.conclusion_disposition,
                "provider_disposition": result.provider_disposition,
                "reacquisition_guard": result.reacquisition_guard,
                "clarification_target": result.clarification_target,
                "reason_codes": list(result.reason_codes),
                "additional_acquisition_executed": False,
            },
        ]
        return result
    except Exception:
        state.next_step = None
        state.next_step_failure = "dependency_failure"
        state.status = "next_step_dependency_failed"
        state.forced_answer = NEXT_STEP_DEPENDENCY_ANSWER
        return None


def retain_initial_attempt_summary(
    state: EvidenceAcquisitionState,
    *,
    context_pack: dict[str, Any] | None,
    retained_source_refs: set[str] | None,
) -> None:
    items = (
        context_pack.get("items")
        if isinstance(context_pack, dict)
        and isinstance(context_pack.get("items"), list)
        else []
    )
    state.initial_attempt_summary = {
        "strategy": (
            state.plan.selected_strategies[0]
            if state.plan and state.plan.selected_strategies
            else None
        ),
        "sufficiency_status": (
            state.sufficiency.sufficiency_status if state.sufficiency else None
        ),
        "result_count": len(items),
        "retained_reference_count": (
            len(retained_source_refs) if retained_source_refs is not None else None
        ),
        "changed_premise_exact_fetch_followed": True,
    }


def promote_exact_fetch_proposal(
    state: EvidenceAcquisitionState,
    proposal: ExactFetchProposal,
) -> None:
    if state.additional_acquisition_count >= 1:
        raise ValueError("additional_acquisition_limit_reached")
    if state.next_step is None or (
        state.next_step.selected_next_step != "perform_additional_acquisition"
        or state.next_step.reacquisition_guard != "changed_premise_allowed"
        or state.next_step.proposed_premise_digest
        != _acquisition_premise_digest(proposal.premise)
    ):
        raise ValueError("additional_acquisition_not_authorized")
    state.additional_acquisition_count = 1
    if state.next_step_history:
        state.next_step_history[-1]["additional_acquisition_executed"] = True
    state.plan = proposal.plan
    state.declared_scope = proposal.declared_scope
    state.exact_source_refs = [proposal.exact_reference]
    state.exact_attempts = None
    state.expansion_attempts = None
    state.acquisition_facts = None
    state.sufficiency = None
    state.next_step = None
    state.next_step_selection_attempted = False
    state.next_step_failure = None
    state.forced_answer = None
    state.status = "acquisition_ready"


def _validate_scope_echo(
    model: ShapeResponse | PlanResponse | SufficiencyResponse | NextStepResponse,
    scope: dict[str, str],
) -> None:
    if any(getattr(model, field) != value for field, value in scope.items()):
        raise ValueError("dependency_scope_mismatch")


def _validate_supported_plan(state: EvidenceAcquisitionState) -> bool:
    plan = state.plan
    if (
        state.supported_hybrid_comparison_path
        or state.supported_bounded_exhaustive_path
    ):
        return True
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
        "inventory_status": _adapt_inventory_status(state.inventory),
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
    preserve_available_context: bool = False,
    require_all_eligible_sources: bool = False,
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
    if require_all_eligible_sources and sources_used != eligible_sources:
        raise ValueError("context_source_selection_incomplete")
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
    if not preserve_available_context:
        for item in normalized["items"]:
            item.pop("available_context", None)
    return normalized


def validate_bounded_exhaustive_context_pack_response(
    response: dict[str, Any],
    *,
    expected_query: str,
    expected_source_id: str,
) -> dict[str, Any]:
    normalized = validate_context_pack_response(
        response,
        expected_query=expected_query,
        eligible_source_ids=[expected_source_id],
        preserve_available_context=True,
        require_all_eligible_sources=True,
    )
    if normalized["errors"]:
        raise ValueError("context_pack_errors_present")
    if normalized["budget"]["returned_results"] != len(normalized["items"]):
        raise ValueError("context_pack_result_count_mismatch")
    if not normalized["items"]:
        raise ValueError("context_pack_seed_missing")
    diagnostics = normalized.get("diagnostics")
    if not isinstance(diagnostics, dict):
        raise ValueError("context_pack_diagnostics_missing")
    expected_sources = {expected_source_id}
    if set(diagnostics.get("considered_source_ids") or []) != expected_sources:
        raise ValueError("context_pack_considered_source_mismatch")
    if set(diagnostics.get("selected_source_ids") or []) != expected_sources:
        raise ValueError("context_pack_selected_source_mismatch")
    if set(diagnostics.get("candidate_counts_by_source") or {}) != expected_sources:
        raise ValueError("context_pack_candidate_source_mismatch")
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


def validate_context_response(
    response: dict[str, Any],
    *,
    expected_source_id: str,
) -> DsaContextResponse:
    validated = DsaContextResponse.model_validate(response)
    if any(
        item.source_id != expected_source_id
        for item in validated.results
    ):
        raise ValueError("context_source_id_mismatch")
    return validated


def validate_configured_worksheet_response(
    response: dict[str, Any],
    *,
    expected_source_id: str,
) -> tuple[DsaContextResponse, str]:
    validated = DsaContextResponse.model_validate(response)
    if validated.budget.truncated:
        return validated, "truncated"
    if validated.errors:
        return validated, "failed"
    if not validated.results:
        return validated, "unknown"
    if len(validated.results) != 1:
        return validated, "filtered"
    item = validated.results[0]
    if (
        item.source_id != expected_source_id
        or item.source_type != "google_sheets"
        or item.content_type != "spreadsheet_range"
        or item.url is not None
        or item.available_context
    ):
        return validated, "filtered"
    return validated, "satisfied"


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


def _hybrid_bundle_id(
    *,
    plan_id: str,
    question_anchor_digest: str,
    query_id: str | None,
    attempts: list[dict[str, Any]],
) -> str:
    material = {
        "plan_id": plan_id,
        "question_anchor_digest": question_anchor_digest,
        "query_id": query_id,
        "attempts": sorted(
            [
                {
                    "source_id": item.get("source_id"),
                    "seed_source_ref": item.get("seed_source_ref"),
                    "context_mode": item.get("context_mode"),
                    "outcome": item.get("outcome"),
                    "query_id": item.get("query_id"),
                    "returned_reference_count": item.get(
                        "returned_reference_count"
                    ),
                }
                for item in attempts
            ],
            key=lambda item: (
                str(item.get("source_id") or ""),
                str(item.get("seed_source_ref") or ""),
                str(item.get("context_mode") or ""),
            ),
        ),
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"))
    return f"evidence_hybrid_bundle_{hashlib.sha256(encoded.encode()).hexdigest()[:32]}"


def _bounded_exhaustive_bundle_id(
    *,
    plan_id: str,
    question_anchor_digest: str,
    targeted_query_id: str | None,
    attempt: dict[str, Any],
) -> str:
    material = {
        "plan_id": plan_id,
        "question_anchor_digest": question_anchor_digest,
        "targeted_query_id": targeted_query_id,
        "attempt": {
            "source_id": attempt.get("source_id"),
            "seed_source_ref": attempt.get("seed_source_ref"),
            "context_mode": attempt.get("context_mode"),
            "outcome": attempt.get("outcome"),
            "query_id": attempt.get("query_id"),
            "returned_reference_count": attempt.get(
                "returned_reference_count"
            ),
        },
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"))
    return (
        "evidence_exhaustive_bundle_"
        f"{hashlib.sha256(encoded.encode()).hexdigest()[:32]}"
    )


def _prompt_safe_context_item(item: DsaContextItem) -> dict[str, Any]:
    return {
        "result_id": item.result_id,
        "source_type": item.source_type,
        "source_id": item.source_id,
        "source_name": item.source_name,
        "source_ref": item.source_ref,
        "retrieved_at": item.retrieved_at.isoformat(),
        "source_modified_at": (
            item.source_modified_at.isoformat()
            if item.source_modified_at is not None
            else None
        ),
        "title": item.title,
        "content_type": item.content_type,
        "text": item.text,
        "confidence": item.confidence,
        "warnings": list(item.warnings),
    }


def _prompt_safe_targeted_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        key: item.get(key)
        for key in (
            "result_id",
            "source_type",
            "source_id",
            "source_name",
            "source_ref",
            "retrieved_at",
            "source_modified_at",
            "title",
            "content_type",
            "text",
            "confidence",
            "warnings",
        )
    }


async def execute_bounded_exhaustive_review(
    *,
    state: EvidenceAcquisitionState,
    dsa: Any,
    targeted_context_pack: dict[str, Any] | None,
    dsa_trace: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if (
        not state.supported_bounded_exhaustive_path
        or state.plan is None
        or state.shape is None
    ):
        return {}, {
            **dsa_trace,
            "status": "error",
            "reason": "unsupported_bounded_exhaustive_plan",
            "error_code": "unsupported_bounded_exhaustive_plan",
        }

    source_id = state.plan.eligible_source_ids[0]
    targeted_items = (
        [
            item
            for item in targeted_context_pack.get("items", [])
            if isinstance(item, dict)
        ]
        if isinstance(targeted_context_pack, dict)
        else []
    )
    target: dict[str, Any] | None = None
    for item in targeted_items:
        descriptors = item.get("available_context")
        if not isinstance(descriptors, list):
            continue
        if any(
            isinstance(descriptor, dict)
            and descriptor.get("context_mode")
            == CONFIGURED_WORKSHEET_CONTEXT_MODE
            for descriptor in descriptors
        ):
            target = item
            break

    attempt: dict[str, Any] = {
        "source_id": source_id,
        "seed_source_ref": (
            target.get("source_ref")
            if isinstance(target, dict)
            else targeted_items[0].get("source_ref")
            if targeted_items
            else None
        ),
        "context_mode": CONFIGURED_WORKSHEET_CONTEXT_MODE,
        "outcome": "unsupported",
        "query_id": None,
        "returned_reference_count": 0,
    }
    context_call_count = 0
    aggregate_error_codes = {
        code
        for code in [
            dsa_trace.get("error_code"),
            *(dsa_trace.get("error_codes") or []),
        ]
        if isinstance(code, str)
    }
    safe_items: list[dict[str, Any]] = []
    raw_expanded_item_count = 0
    expansion_estimated_bytes = 0
    expansion_truncated = False

    if targeted_context_pack is None:
        attempt["outcome"] = (
            "filtered"
            if dsa_trace.get("error_code") == "malformed_response"
            else "failed"
        )
    elif not isinstance(target, dict) or not isinstance(
        attempt["seed_source_ref"], str
    ):
        attempt["outcome"] = "unsupported"
    else:
        context_call_count = 1
        try:
            response_raw = await dsa.context_source(
                source_ref=attempt["seed_source_ref"],
                context_mode=CONFIGURED_WORKSHEET_CONTEXT_MODE,
                budget=dict(BOUNDED_EXHAUSTIVE_CONTEXT_BUDGET),
            )
            if not isinstance(response_raw, dict):
                raise ValueError("malformed_context_response")
            response, outcome = validate_configured_worksheet_response(
                response_raw,
                expected_source_id=source_id,
            )
            attempt["query_id"] = response.query_id
            attempt["returned_reference_count"] = len(response.results)
            attempt["outcome"] = outcome
            raw_expanded_item_count = len(response.results)
            expansion_estimated_bytes = response.budget.estimated_bytes
            expansion_truncated = outcome == "truncated"
            if outcome == "satisfied":
                safe_items = [
                    _prompt_safe_context_item(response.results[0])
                ]
            elif outcome == "truncated":
                aggregate_error_codes.add("budget_truncated")
            elif outcome == "failed":
                aggregate_error_codes.update(
                    error.code for error in response.errors
                )
            elif outcome == "filtered":
                aggregate_error_codes.add("malformed_response")
        except (ValueError, TypeError):
            attempt["outcome"] = "filtered"
            aggregate_error_codes.add("malformed_response")
        except Exception:
            attempt["outcome"] = "failed"
            aggregate_error_codes.add("dependency_failure")

    state.expansion_attempts = [attempt]
    targeted_query_id = (
        targeted_context_pack.get("query_id")
        if isinstance(targeted_context_pack, dict)
        and isinstance(targeted_context_pack.get("query_id"), str)
        else None
    )
    diagnostics = (
        targeted_context_pack.get("diagnostics")
        if isinstance(targeted_context_pack, dict)
        and isinstance(targeted_context_pack.get("diagnostics"), dict)
        else {
            "selection_mode": "planned_source_seed_search",
            "considered_source_ids": [source_id],
            "selected_source_ids": [source_id],
            "source_diagnostics": [],
            "ranking_mode": "single_source",
            "candidate_counts_by_source": {source_id: 0},
            "budget_truncated_candidates": False,
        }
    )
    targeted_budget = (
        targeted_context_pack.get("budget")
        if isinstance(targeted_context_pack, dict)
        and isinstance(targeted_context_pack.get("budget"), dict)
        else {}
    )
    sources_used = [source_id] if safe_items else []
    bundle = {
        "bundle_id": _bounded_exhaustive_bundle_id(
            plan_id=state.plan.plan_id,
            question_anchor_digest=state.plan.question_anchor_digest,
            targeted_query_id=targeted_query_id,
            attempt=attempt,
        ),
        "query_id": targeted_query_id,
        "query": state.plan.question_anchor,
        "sources_used": sources_used,
        "items": safe_items,
        "errors": [
            {"code": code} for code in sorted(aggregate_error_codes)
        ],
        "budget": {
            "max_results": 1,
            "returned_results": len(safe_items),
            "estimated_bytes": expansion_estimated_bytes,
            "truncated": expansion_truncated,
        },
        "diagnostics": diagnostics,
        "raw_item_count": raw_expanded_item_count,
    }
    outcome_counts = {
        outcome: int(attempt["outcome"] == outcome)
        for outcome in (
            "satisfied",
            "unknown",
            "failed",
            "filtered",
            "truncated",
            "unsupported",
        )
    }
    status = (
        "included"
        if attempt["outcome"] == "satisfied"
        else "error"
        if attempt["outcome"] in {"failed", "filtered"}
        else "empty"
    )
    return bundle, {
        **dsa_trace,
        "called": True,
        "call_count": 1 + context_call_count,
        "context_pack_call_count": 1,
        "context_expansion_call_count": context_call_count,
        "status": status,
        "reason": "bounded_exhaustive_acquisition_completed",
        "error_code": (
            "malformed_response"
            if attempt["outcome"] == "filtered"
            else "dependency_failure"
            if attempt["outcome"] == "failed"
            else "budget_truncated"
            if attempt["outcome"] == "truncated"
            else None
        ),
        "error_codes": sorted(aggregate_error_codes),
        "raw_targeted_item_count": len(targeted_items),
        "raw_expanded_item_count": raw_expanded_item_count,
        "raw_item_count": raw_expanded_item_count,
        "final_combined_item_count": len(safe_items),
        "expansion_attempt_counts": outcome_counts,
        "budget_truncated": bool(
            dsa_trace.get("budget_truncated")
            or targeted_budget.get("truncated")
            or expansion_truncated
        ),
        "search_budget_truncated": bool(
            dsa_trace.get("budget_truncated")
            or targeted_budget.get("truncated")
        ),
        "expansion_budget_truncated": expansion_truncated,
        "candidate_truncated": bool(
            dsa_trace.get("candidate_truncated")
            or diagnostics.get("budget_truncated_candidates")
        ),
    }


async def execute_hybrid_comparison(
    *,
    state: EvidenceAcquisitionState,
    dsa: Any,
    targeted_context_pack: dict[str, Any],
    dsa_trace: dict[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if (
        not state.supported_hybrid_comparison_path
        or state.plan is None
        or state.shape is None
    ):
        return None, {
            **dsa_trace,
            "status": "error",
            "reason": "unsupported_hybrid_plan",
            "error_code": "unsupported_hybrid_plan",
        }

    targeted_items = [
        item
        for item in targeted_context_pack.get("items", [])
        if isinstance(item, dict)
    ]
    attempts: list[dict[str, Any]] = []
    expanded_items_by_source: dict[str, list[dict[str, Any]]] = {}
    aggregate_error_codes = {
        code
        for code in [
            dsa_trace.get("error_code"),
            *(dsa_trace.get("error_codes") or []),
        ]
        if isinstance(code, str)
    }
    raw_expanded_item_count = 0
    expansion_truncated = False
    context_call_count = 0
    targeted_budget = targeted_context_pack.get("budget")
    aggregate_max_results = (
        int(targeted_budget.get("max_results") or 0)
        if isinstance(targeted_budget, dict)
        else 0
    )
    aggregate_estimated_bytes = (
        int(targeted_budget.get("estimated_bytes") or 0)
        if isinstance(targeted_budget, dict)
        else 0
    )

    for source_id in sorted(state.plan.eligible_source_ids):
        source_items = [
            item for item in targeted_items if item.get("source_id") == source_id
        ]
        target = next(
            (
                item
                for item in source_items
                if isinstance(item.get("available_context"), list)
                and item["available_context"]
            ),
            None,
        )
        descriptor = (
            target["available_context"][0]
            if isinstance(target, dict)
            and isinstance(target.get("available_context"), list)
            and target["available_context"]
            else None
        )
        attempt: dict[str, Any] = {
            "source_id": source_id,
            "seed_source_ref": (
                target.get("source_ref")
                if isinstance(target, dict)
                else source_items[0].get("source_ref")
                if source_items
                else None
            ),
            "context_mode": (
                descriptor.get("context_mode")
                if isinstance(descriptor, dict)
                else None
            ),
            "outcome": "unsupported",
            "query_id": None,
            "returned_reference_count": 0,
        }
        if (
            not isinstance(target, dict)
            or not isinstance(attempt["seed_source_ref"], str)
            or not isinstance(attempt["context_mode"], str)
        ):
            attempts.append(attempt)
            continue

        context_call_count += 1
        try:
            response_raw = await dsa.context_source(
                source_ref=attempt["seed_source_ref"],
                context_mode=attempt["context_mode"],
                budget={
                    "max_rows": 5,
                    "max_bytes": 50000,
                    "max_text_chars": 12000,
                },
            )
            if not isinstance(response_raw, dict):
                raise ValueError("malformed_context_response")
            response = validate_context_response(
                response_raw,
                expected_source_id=source_id,
            )
            attempt["query_id"] = response.query_id
            attempt["returned_reference_count"] = len(response.results)
            raw_expanded_item_count += len(response.results)
            aggregate_max_results += response.budget.max_results or 0
            aggregate_estimated_bytes += response.budget.estimated_bytes
            if response.budget.truncated:
                attempt["outcome"] = "truncated"
                expansion_truncated = True
                aggregate_error_codes.add("budget_truncated")
            elif response.errors:
                attempt["outcome"] = "failed"
                aggregate_error_codes.update(item.code for item in response.errors)
            elif not response.results:
                attempt["outcome"] = "unknown"
            else:
                attempt["outcome"] = "satisfied"
            if attempt["outcome"] in {"satisfied", "truncated"}:
                expanded_items_by_source[source_id] = [
                    _prompt_safe_context_item(item)
                    for item in response.results
                ]
        except ValueError:
            attempt["outcome"] = "filtered"
            aggregate_error_codes.add("malformed_response")
        except Exception:
            attempt["outcome"] = "failed"
            aggregate_error_codes.add("dependency_failure")
        attempts.append(attempt)

    state.expansion_attempts = attempts
    combined_items: list[dict[str, Any]] = []
    seen_refs: set[str] = set()
    for item in targeted_items:
        source_ref = item.get("source_ref")
        if not isinstance(source_ref, str) or source_ref in seen_refs:
            continue
        seen_refs.add(source_ref)
        combined_items.append(_prompt_safe_targeted_item(item))
    for source_id in sorted(state.plan.eligible_source_ids):
        for item in expanded_items_by_source.get(source_id, []):
            source_ref = item.get("source_ref")
            if not isinstance(source_ref, str) or source_ref in seen_refs:
                continue
            seen_refs.add(source_ref)
            combined_items.append(item)

    raw_targeted_item_count = len(targeted_items)
    raw_total_item_count = raw_targeted_item_count + raw_expanded_item_count
    outcome_counts = {
        outcome: sum(item["outcome"] == outcome for item in attempts)
        for outcome in (
            "satisfied",
            "unknown",
            "failed",
            "filtered",
            "truncated",
            "unsupported",
        )
    }
    if attempts and outcome_counts["satisfied"] == len(attempts):
        status = "included"
    elif combined_items:
        status = "partial"
    elif outcome_counts["failed"] or outcome_counts["filtered"]:
        status = "error"
    else:
        status = "empty"
    sources_used = sorted(
        {
            item["source_id"]
            for item in combined_items
            if isinstance(item.get("source_id"), str)
        }
    )
    query_id = targeted_context_pack.get("query_id")
    bundle = {
        "bundle_id": _hybrid_bundle_id(
            plan_id=state.plan.plan_id,
            question_anchor_digest=state.plan.question_anchor_digest,
            query_id=query_id if isinstance(query_id, str) else None,
            attempts=attempts,
        ),
        "query_id": query_id,
        "query": state.plan.question_anchor,
        "sources_used": sources_used,
        "items": combined_items,
        "errors": [
            {"code": code} for code in sorted(aggregate_error_codes)
        ],
        "budget": {
            "max_results": aggregate_max_results,
            "returned_results": raw_total_item_count,
            "estimated_bytes": aggregate_estimated_bytes,
            "truncated": bool(
                dsa_trace.get("budget_truncated") or expansion_truncated
            ),
        },
        "diagnostics": targeted_context_pack.get("diagnostics"),
        "raw_item_count": raw_total_item_count,
    }
    return bundle, {
        **dsa_trace,
        "called": True,
        "call_count": 1 + context_call_count,
        "context_pack_call_count": 1,
        "context_expansion_call_count": context_call_count,
        "status": status,
        "reason": "hybrid_acquisition_completed",
        "error_code": (
            "malformed_response"
            if outcome_counts["filtered"]
            else "dependency_failure"
            if outcome_counts["failed"]
            else None
        ),
        "error_codes": sorted(aggregate_error_codes),
        "raw_targeted_item_count": raw_targeted_item_count,
        "raw_expanded_item_count": raw_expanded_item_count,
        "raw_item_count": raw_total_item_count,
        "final_combined_item_count": len(combined_items),
        "expansion_attempt_counts": outcome_counts,
        "budget_truncated": bool(
            dsa_trace.get("budget_truncated") or expansion_truncated
        ),
        "search_budget_truncated": bool(dsa_trace.get("budget_truncated")),
        "expansion_budget_truncated": expansion_truncated,
        "candidate_truncated": bool(dsa_trace.get("candidate_truncated")),
    }


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


def _hybrid_non_satisfied_outcome(
    attempts: list[dict[str, Any]],
    dsa_trace: dict[str, Any],
) -> str:
    if not attempts and dsa_trace.get("status") == "error":
        return (
            "filtered"
            if dsa_trace.get("error_code") == "malformed_response"
            else "failed"
        )
    outcomes = {str(item.get("outcome")) for item in attempts}
    for outcome in (
        "truncated",
        "filtered",
        "failed",
        "unsupported",
        "unknown",
    ):
        if outcome in outcomes:
            return outcome
    return "unknown"


def _hybrid_fact_outcomes(
    *,
    plan: PlanResult,
    attempts: list[dict[str, Any]],
    context_pack: dict[str, Any] | None,
    dsa_trace: dict[str, Any],
    retained_source_refs: set[str] | None,
) -> tuple[str, str, str]:
    planned_sources = set(plan.eligible_source_ids)
    items = (
        context_pack.get("items")
        if isinstance(context_pack, dict)
        and isinstance(context_pack.get("items"), list)
        else []
    )
    returned_refs: set[str] = set()
    source_by_ref: dict[str, str] = {}
    combined_sources: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        source_ref = item.get("source_ref")
        source_id = item.get("source_id")
        if not isinstance(source_ref, str) or not isinstance(source_id, str):
            continue
        returned_refs.add(source_ref)
        source_by_ref[source_ref] = source_id
        combined_sources.add(source_id)

    attempts_by_source = {
        str(item.get("source_id")): str(item.get("outcome"))
        for item in attempts
        if isinstance(item.get("source_id"), str)
    }
    all_expansions_satisfied = bool(planned_sources) and all(
        attempts_by_source.get(source_id) == "satisfied"
        for source_id in planned_sources
    )
    material_truncated = bool(
        dsa_trace.get("budget_truncated")
        or dsa_trace.get("candidate_truncated")
    )
    base_failure = _hybrid_non_satisfied_outcome(attempts, dsa_trace)
    if retained_source_refs is None:
        return (
            base_failure if not all_expansions_satisfied else "unknown",
            base_failure if len({
                source_id
                for source_id, outcome in attempts_by_source.items()
                if outcome == "satisfied"
            }) < 2 else "unknown",
            "unknown",
        )
    if retained_source_refs - returned_refs:
        return (
            base_failure if not all_expansions_satisfied else "unknown",
            "unknown",
            "unknown",
        )

    retained_sources = {
        source_by_ref[source_ref]
        for source_ref in retained_source_refs
        if source_ref in source_by_ref
    }
    if material_truncated:
        coverage_outcome = "truncated"
    elif not all_expansions_satisfied:
        coverage_outcome = base_failure
    elif not planned_sources.issubset(combined_sources):
        coverage_outcome = "unknown"
    elif not planned_sources.issubset(retained_sources):
        coverage_outcome = "filtered"
    else:
        coverage_outcome = "satisfied"

    successful_sources = {
        source_id
        for source_id, outcome in attempts_by_source.items()
        if outcome == "satisfied"
    }
    if material_truncated:
        comparison_outcome = "truncated"
    elif len(successful_sources) < 2:
        comparison_outcome = base_failure
    elif len(successful_sources & retained_sources) < 2:
        comparison_outcome = "filtered"
    else:
        comparison_outcome = "satisfied"

    if planned_sources.issubset(retained_sources) and all_expansions_satisfied:
        delivery_outcome = "satisfied"
    elif items:
        delivery_outcome = "filtered"
    else:
        delivery_outcome = "unknown"
    return coverage_outcome, comparison_outcome, delivery_outcome


def _bounded_exhaustive_fact_outcomes(
    *,
    attempts: list[dict[str, Any]],
    context_pack: dict[str, Any] | None,
    retained_source_refs: set[str] | None,
) -> dict[str, str]:
    attempt_outcome = (
        str(attempts[0].get("outcome"))
        if len(attempts) == 1
        else "unknown"
    )
    if attempt_outcome not in {
        "satisfied",
        "unknown",
        "failed",
        "filtered",
        "truncated",
        "unsupported",
    }:
        attempt_outcome = "unknown"
    delivery_outcome, _, _ = _delivery_reference_state(
        context_pack=context_pack,
        retained_source_refs=retained_source_refs,
    )
    if attempt_outcome != "satisfied":
        delivery_outcome = attempt_outcome
    return {
        "authoritative_inventory": "satisfied",
        "complete_scope_coverage": attempt_outcome,
        "context_delivery": delivery_outcome,
        "contradiction_search": (
            delivery_outcome
            if attempt_outcome == "satisfied"
            else attempt_outcome
        ),
        "no_material_truncation": (
            delivery_outcome
            if attempt_outcome == "satisfied"
            else attempt_outcome
        ),
    }


def _build_acquisition_facts(
    *,
    plan: PlanResult,
    context_pack: dict[str, Any] | None,
    dsa_trace: dict[str, Any],
    retained_source_refs: set[str] | None,
    exact_source_refs: list[dict[str, str]] | None = None,
    exact_attempts: list[dict[str, Any]] | None = None,
    expansion_attempts: list[dict[str, Any]] | None = None,
    bounded_exhaustive_path: bool = False,
) -> list[dict[str, str]]:
    status = dsa_trace.get("status")
    error_code = dsa_trace.get("error_code")
    usable_count = len(context_pack.get("items", [])) if isinstance(context_pack, dict) else 0
    exact_path = plan.selected_strategies == ["exact_fetch"]
    hybrid_path = (
        plan.task_shape == "cross_source_comparison"
        and plan.selected_strategies == ["hybrid"]
    )
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
    hybrid_outcomes = (
        _hybrid_fact_outcomes(
            plan=plan,
            attempts=expansion_attempts or [],
            context_pack=context_pack,
            dsa_trace=dsa_trace,
            retained_source_refs=retained_source_refs,
        )
        if hybrid_path
        else None
    )
    exhaustive_outcomes = (
        _bounded_exhaustive_fact_outcomes(
            attempts=expansion_attempts or [],
            context_pack=context_pack,
            retained_source_refs=retained_source_refs,
        )
        if bounded_exhaustive_path
        else None
    )
    facts: list[dict[str, str]] = []
    for requirement in sorted(
        plan.declared_requirements,
        key=lambda item: item.requirement_id,
    ):
        if exhaustive_outcomes is not None:
            outcome = exhaustive_outcomes.get(
                requirement.requirement_kind,
                "unknown",
            )
        elif (
            hybrid_path
            and requirement.requirement_kind == "selected_source_coverage"
            and requirement.criticality == "material"
        ):
            outcome = hybrid_outcomes[0] if hybrid_outcomes else "unknown"
        elif hybrid_path and requirement.requirement_kind == "cross_source_comparison":
            outcome = hybrid_outcomes[1] if hybrid_outcomes else "unknown"
        elif hybrid_path and requirement.requirement_kind == "context_delivery":
            outcome = hybrid_outcomes[2] if hybrid_outcomes else "unknown"
        elif requirement.requirement_kind == "targeted_evidence":
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
            outcome = "unknown"
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
    delivery_identity = None
    if state.supported_bounded_exhaustive_path:
        retention_status, returned_refs, retained_refs = (
            _delivery_reference_state(
                context_pack=context_pack,
                retained_source_refs=retained_source_refs,
            )
        )
        delivery_identity = {
            "returned_source_refs": sorted(returned_refs),
            "retained_source_refs": sorted(retained_refs),
            "retention_status": retention_status,
        }
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
        expansion_attempts=state.expansion_attempts,
        delivery_identity=delivery_identity,
        initial_attempt_summary=state.initial_attempt_summary,
        next_step_history=state.next_step_history,
    )
    facts = _build_acquisition_facts(
        plan=state.plan,
        context_pack=context_pack,
        dsa_trace=dsa_trace,
        retained_source_refs=retained_source_refs,
        exact_source_refs=state.exact_source_refs,
        exact_attempts=state.exact_attempts,
        expansion_attempts=state.expansion_attempts,
        bounded_exhaustive_path=state.supported_bounded_exhaustive_path,
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
    if state.next_step_selection_attempted:
        if state.next_step is None or state.sufficiency is None:
            return False
        return bool(
            state.next_step.provider_disposition == "allowed"
            and (
                (
                    state.next_step.selected_next_step
                    == "answer_within_declared_scope"
                    and state.sufficiency.sufficiency_status
                    == "sufficient_for_declared_scope"
                )
                or (
                    state.next_step.selected_next_step
                    == "provide_qualified_partial_answer"
                    and state.sufficiency.sufficiency_status
                    == "sufficient_with_limitations"
                )
            )
        )
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
    if state.next_step_selection_attempted:
        next_step = state.next_step
        if next_step is None:
            return state.forced_answer or NEXT_STEP_DEPENDENCY_ANSWER
        if next_step.selected_next_step == "ask_narrow_clarification":
            if next_step.clarification_target is None:
                return NEXT_STEP_DEPENDENCY_ANSWER
            return _CLARIFICATION_QUESTIONS[next_step.clarification_target]
        if next_step.selected_next_step == "provide_qualified_partial_answer" and (
            state.sufficiency is not None
            and state.sufficiency.sufficiency_status in {"insufficient", "unknown"}
        ):
            return _render_qualified_partial_answer(state.sufficiency)
        if next_step.selected_next_step in {
            "disclose_unexamined_scope",
            "withhold_unsupported_conclusion",
        }:
            if state.sufficiency is None:
                return NEXT_STEP_DEPENDENCY_ANSWER
            return _render_blocked_answer(state.sufficiency)
        if next_step.selected_next_step == "perform_additional_acquisition":
            return NEXT_STEP_DEPENDENCY_ANSWER
    if (
        state.sufficiency is not None
        and state.sufficiency.sufficiency_status in {"insufficient", "unknown"}
    ):
        return _render_blocked_answer(state.sufficiency)
    if state.forced_answer is not None:
        return state.forced_answer
    if state.sufficiency is None:
        return WITHHELD_ANSWER

    policy_paragraphs: list[str] = []
    if state.sufficiency.sufficiency_status == "sufficient_with_limitations":
        policy_paragraphs.append(_render_limitation_disclosure(state))
    boundary = _SCOPE_BOUNDARIES.get(state.sufficiency.task_shape)
    if boundary:
        policy_paragraphs.append(boundary)
    return _compose_policy_answer(answer, policy_paragraphs)


def _compose_policy_answer(answer: str, policy_paragraphs: list[str]) -> str:
    paragraphs = [
        paragraph.strip()
        for paragraph in re.split(r"\n\s*\n", answer.strip())
        if paragraph.strip()
    ]
    owned_policy_paragraphs = {
        *_SCOPE_BOUNDARIES.values(),
        *policy_paragraphs,
    }
    provider_paragraphs = [
        paragraph
        for paragraph in paragraphs
        if paragraph not in owned_policy_paragraphs
    ]
    return "\n\n".join([*provider_paragraphs, *policy_paragraphs])


def _render_requirement_outcome(
    requirement_kind: RequirementKind,
    outcome: str,
    *,
    optional: bool = False,
) -> str | None:
    if outcome == "satisfied":
        return None
    description = _REQUIREMENT_DESCRIPTIONS[requirement_kind]
    if optional:
        description = f"optional {description}"
    outcome_descriptions = {
        "partial": f"{description} was only partially established",
        "not_attempted": (
            f"{description} was not established because the required acquisition "
            "was not attempted"
        ),
        "unavailable": (
            f"{description} was not established because the required evidence "
            "scope was unavailable"
        ),
        "unsupported": (
            f"{description} was not established because the required acquisition "
            "was unsupported"
        ),
        "failed": f"{description} was not established because acquisition failed",
        "excluded": (
            f"{description} was not established because required evidence was "
            "excluded"
        ),
        "filtered": (
            f"{description} was not established because required evidence was "
            "filtered or omitted before reasoning"
        ),
        "truncated": (
            f"{description} was not established because material evidence was "
            "truncated"
        ),
        "unresolved_contradiction": (
            f"{description} was not established because contradictory evidence "
            "remained unresolved"
        ),
        "unknown": (
            f"{description} could not be established from the available "
            "acquisition facts"
        ),
        "missing": (
            f"{description} could not be established because the required "
            "acquisition fact was missing"
        ),
    }
    return outcome_descriptions.get(
        outcome,
        f"{description} could not be established",
    )


def _scoped_inventory_sources(
    state: EvidenceAcquisitionState,
) -> list[DsaSourceEntry]:
    if state.inventory is None:
        return []
    scope = state.declared_scope or {}
    source_ids = set(scope.get("source_ids") or [])
    categories = set(scope.get("source_categories") or [])
    if source_ids:
        return [
            source
            for source in state.inventory.sources
            if source.source_id in source_ids
        ]
    if categories:
        return [
            source
            for source in state.inventory.sources
            if set(source.domain_tags) & categories
        ]
    return list(state.inventory.sources)


def _unavailable_source_limitation(
    state: EvidenceAcquisitionState,
) -> str:
    count = sum(
        1
        for source in _scoped_inventory_sources(state)
        if not source.enabled or source.status != "ready"
    )
    if count == 1:
        return "1 optional source was unavailable"
    if count > 1:
        return f"{count} optional sources were unavailable"
    return "an optional selected source was not available"


def _bounded_clauses(
    clauses: list[str],
) -> tuple[list[str], bool]:
    normalized = sorted(set(clauses))
    return (
        normalized[:_MAX_RENDERED_EVIDENCE_CAUSES],
        len(normalized) > _MAX_RENDERED_EVIDENCE_CAUSES,
    )


def _join_clauses(clauses: list[str]) -> str:
    if len(clauses) == 1:
        return clauses[0]
    if len(clauses) == 2:
        return f"{clauses[0]} and {clauses[1]}"
    return f"{'; '.join(clauses[:-1])}; and {clauses[-1]}"


def _render_limitation_disclosure(state: EvidenceAcquisitionState) -> str:
    assert state.sufficiency is not None
    plan_codes = set(state.plan.limitation_codes if state.plan else [])
    clauses: list[str] = []
    if plan_codes & _SOURCE_AVAILABILITY_LIMITATIONS:
        clauses.append(_unavailable_source_limitation(state))
    for code in sorted(plan_codes - _SOURCE_AVAILABILITY_LIMITATIONS):
        description = _PLAN_LIMITATION_DESCRIPTIONS.get(code)
        if description:
            clauses.append(description)

    source_availability_rendered = bool(
        plan_codes & _SOURCE_AVAILABILITY_LIMITATIONS
    )
    inventory_limitation_rendered = bool(
        plan_codes
        & {
            "source_inventory_partial",
            "source_inventory_unknown",
            "source_inventory_unavailable",
        }
    )
    for evaluation in sorted(
        state.sufficiency.evaluated_requirements,
        key=lambda item: (
            item.requirement_kind,
            item.effective_outcome,
            item.requirement_id,
        ),
    ):
        if evaluation.criticality != "optional":
            continue
        if (
            (source_availability_rendered or inventory_limitation_rendered)
            and evaluation.requirement_kind == "selected_source_coverage"
            and evaluation.effective_outcome
            in {"partial", "unavailable", "unknown"}
        ):
            continue
        rendered = _render_requirement_outcome(
            evaluation.requirement_kind,
            evaluation.effective_outcome,
            optional=True,
        )
        if rendered:
            clauses.append(rendered)

    if not clauses:
        clauses = ["optional evidence scope remained incomplete"]
    bounded, omitted = _bounded_clauses(clauses)
    disclosure = f"Limitation: {_join_clauses(bounded)}."
    if omitted:
        disclosure = (
            f"{disclosure} Additional optional evidence limitations remained."
        )
    return disclosure


def _withholding_sentence(task_shape: TaskShape) -> str:
    if task_shape == "bounded_exhaustive_review":
        return "I’m withholding a complete-scope conclusion."
    if task_shape == "absence_or_coverage_check":
        return "I’m withholding an absence conclusion."
    if task_shape == "contradiction_review":
        return "I’m withholding a contradiction-sensitive conclusion."
    return "I’m withholding the requested conclusion."


def _render_blocked_answer(sufficiency: SufficiencyResult) -> str:
    clauses = [
        rendered
        for evaluation in sorted(
            sufficiency.evaluated_requirements,
            key=lambda item: (
                item.requirement_kind,
                item.effective_outcome,
                item.requirement_id,
            ),
        )
        if evaluation.criticality == "material"
        if (
            rendered := _render_requirement_outcome(
                evaluation.requirement_kind,
                evaluation.effective_outcome,
            )
        )
    ]
    if not clauses:
        clauses = ["the required material evidence was not established"]
    bounded, omitted = _bounded_clauses(clauses)
    lead = (
        "I can’t support the requested conclusion because"
        if sufficiency.sufficiency_status == "insufficient"
        else "I couldn’t establish whether the requested conclusion is supported because"
    )
    response = f"{lead} {_join_clauses(bounded)}."
    if omitted:
        response = (
            f"{response} Additional material evidence requirements were also "
            "unresolved."
        )
    return f"{response} {_withholding_sentence(sufficiency.task_shape)}"


def _render_qualified_partial_answer(sufficiency: SufficiencyResult) -> str:
    supported = sorted(
        {
            (
                f"{_REQUIREMENT_DESCRIPTIONS[evaluation.requirement_kind]} "
                "was partially established"
                if evaluation.effective_outcome == "partial"
                else _REQUIREMENT_DESCRIPTIONS[evaluation.requirement_kind]
            )
            for evaluation in sufficiency.evaluated_requirements
            if evaluation.criticality == "material"
            and evaluation.requirement_kind
            not in _ADMINISTRATIVE_REQUIREMENT_KINDS
            and evaluation.effective_outcome in {"satisfied", "partial"}
        }
    )
    unresolved = [
        rendered
        for evaluation in sorted(
            sufficiency.evaluated_requirements,
            key=lambda item: (
                item.requirement_kind,
                item.effective_outcome,
                item.requirement_id,
            ),
        )
        if evaluation.criticality == "material"
        if (
            rendered := _render_requirement_outcome(
                evaluation.requirement_kind,
                evaluation.effective_outcome,
            )
        )
    ]
    supported_bounded, supported_omitted = _bounded_clauses(supported)
    unresolved_bounded, unresolved_omitted = _bounded_clauses(unresolved)
    supported_text = (
        _join_clauses(supported_bounded)
        if supported_bounded
        else "some substantive evidence"
    )
    unresolved_text = (
        _join_clauses(unresolved_bounded)
        if unresolved_bounded
        else "material evidence remains unresolved"
    )
    response = (
        f"The available evidence establishes {supported_text}. However, "
        f"{unresolved_text}."
    )
    if supported_omitted or unresolved_omitted:
        response = (
            f"{response} Additional evidence details remain outside this bounded "
            "summary."
        )
    return f"{response} {_withholding_sentence(sufficiency.task_shape)}"


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
    hybrid_path = state.supported_hybrid_comparison_path
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
        if hybrid_path and state.plan is not None:
            _, _, delivery_outcome = _hybrid_fact_outcomes(
                plan=state.plan,
                attempts=state.expansion_attempts or [],
                context_pack=context_pack,
                dsa_trace=trace,
                retained_source_refs=retained_source_refs,
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
    expansion_attempts = state.expansion_attempts or []
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
    expansion_outcome_counts = {
        outcome: sum(item.get("outcome") == outcome for item in expansion_attempts)
        for outcome in (
            "satisfied",
            "unknown",
            "failed",
            "filtered",
            "truncated",
            "unsupported",
        )
    }
    attempted_expansion_refs = {
        str(item["seed_source_ref"])
        for item in expansion_attempts
        if isinstance(item.get("seed_source_ref"), str)
    }
    unsuccessful_expansion_refs = {
        str(item["seed_source_ref"])
        for item in expansion_attempts
        if item.get("outcome") != "satisfied"
        and isinstance(item.get("seed_source_ref"), str)
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
            "source_references_attempted": sorted(
                set(attempted_exact_refs) | attempted_expansion_refs
            ),
            "source_references_unsuccessful": sorted(
                set(unsuccessful_exact_refs) | unsuccessful_expansion_refs
            ),
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
                | {
                    str(item["source_id"])
                    for item in expansion_attempts
                    if item.get("outcome") == "failed"
                    and isinstance(item.get("source_id"), str)
                }
            ),
            "expansion_attempts": sorted(
                [
                    {
                        "source_id": str(item["source_id"]),
                        "seed_source_ref": item.get("seed_source_ref"),
                        "context_mode": item.get("context_mode"),
                        "outcome": str(item["outcome"]),
                        "returned_reference_count": int(
                            item.get("returned_reference_count") or 0
                        ),
                    }
                    for item in expansion_attempts
                ],
                key=lambda item: (
                    item["source_id"],
                    str(item.get("seed_source_ref") or ""),
                    str(item.get("context_mode") or ""),
                ),
            ),
            "expansion_attempt_count": len(expansion_attempts),
            "expansion_successful_count": expansion_outcome_counts["satisfied"],
            "expansion_unknown_count": expansion_outcome_counts["unknown"],
            "expansion_failed_count": expansion_outcome_counts["failed"],
            "expansion_filtered_count": expansion_outcome_counts["filtered"],
            "expansion_truncated_count": expansion_outcome_counts["truncated"],
            "expansion_unsupported_count": expansion_outcome_counts["unsupported"],
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
        "next_steps": {
            "selection_count": len(state.next_step_history or []),
            "selections": [
                {
                    "selection_id": item.get("selection_id"),
                    "evaluation_id": item.get("evaluation_id"),
                    "evidence_plan_id": item.get("evidence_plan_id"),
                    "acquisition_manifest_id": item.get(
                        "acquisition_manifest_id"
                    ),
                    "selected_next_step": item.get("selected_next_step"),
                    "conclusion_disposition": item.get(
                        "conclusion_disposition"
                    ),
                    "provider_disposition": item.get("provider_disposition"),
                    "reacquisition_guard": item.get("reacquisition_guard"),
                    "clarification_target": item.get("clarification_target"),
                    "reason_codes": sorted(item.get("reason_codes") or []),
                    "additional_acquisition_executed": bool(
                        item.get("additional_acquisition_executed")
                    ),
                }
                for item in (state.next_step_history or [])[:2]
            ],
            "additional_acquisition_count": state.additional_acquisition_count,
            "initial_attempt": state.initial_attempt_summary,
            "dependency_status": (
                state.next_step_failure
                if state.next_step_selection_attempted
                else None
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
        "expansion_attempts",
        "unavailable_source_ids",
        "failed_source_ids",
    )
    for field in identity_fields:
        values = acquisition.get(field)
        acquisition[f"{field}_count"] = len(values) if isinstance(values, list) else 0
        acquisition[field] = []
    acquisition["source_identifiers_suppressed"] = True
    return sanitized
