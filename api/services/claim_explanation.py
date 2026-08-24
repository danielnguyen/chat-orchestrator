from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

_SUPPORTED_INTENTS = frozenset(
    {
        "how are you sure",
        "what supports that",
        "what supported that",
        "what evidence supports that",
        "what was that based on",
    }
)
_ACQUISITION_INTENTS = {
    "what did you check": "checked",
    "what did you examine": "checked",
    "did you look at everything relevant": "coverage",
    "what might you have missed": "gaps",
    "what did you not check": "gaps",
}
_QUOTED_INTENT_RE = re.compile(
    r'(?:what\s+supports\s+the\s+statement|'
    r'what\s+supported\s+the\s+statement|'
    r'how\s+are\s+you\s+sure\s+about\s+the\s+statement)'
    r'\s+"(?P<anchor>[^"\r\n]*)"\s*[?.]?\s*',
    re.IGNORECASE,
)
_QUOTED_ACQUISITION_INTENT_RE = re.compile(
    r"(?:(?P<checked>what\s+did\s+you\s+(?:check|examine))|"
    r"(?P<coverage>did\s+you\s+look\s+at\s+everything\s+relevant)|"
    r"(?P<gaps>what\s+(?:might\s+you\s+have\s+missed|did\s+you\s+not\s+check)))"
    r'\s+for\s+the\s+statement\s+"(?P<anchor>[^"\r\n]*)"\s*[?.]?\s*',
    re.IGNORECASE,
)
_COMPOUND_SUFFIX_RE = r"(?P<recheck>check|verify)\s+again\."
_COMPOUND_ACQUISITION_INTENT_RE = re.compile(
    r"(?:(?P<checked>what\s+did\s+you\s+(?:check|examine))|"
    r"(?P<coverage>did\s+you\s+look\s+at\s+everything\s+relevant)|"
    r"(?P<gaps>what\s+(?:might\s+you\s+have\s+missed|did\s+you\s+not\s+check)))"
    r"\?\s+" + _COMPOUND_SUFFIX_RE + r"\s*",
    re.IGNORECASE,
)
_QUOTED_COMPOUND_ACQUISITION_INTENT_RE = re.compile(
    r"(?:(?P<checked>what\s+did\s+you\s+(?:check|examine))|"
    r"(?P<coverage>did\s+you\s+look\s+at\s+everything\s+relevant)|"
    r"(?P<gaps>what\s+(?:might\s+you\s+have\s+missed|did\s+you\s+not\s+check)))"
    r'\s+for\s+the\s+statement\s+"(?P<anchor>[^"\r\n]*)"\?\s+'
    + _COMPOUND_SUFFIX_RE
    + r"\s*",
    re.IGNORECASE,
)
_PARAGRAPH_SEPARATOR = re.compile(r"\r?\n[ \t]*\r?\n")
_RESPONSE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_CLASSIFIER_MAX_CHARACTERS = 320
_CLASSIFIER_MAX_WORDS = 48
_HISTORY_CLASSIFIER_INTENTS = {
    "not_history_followup",
    "support_explanation",
    "acquisition_checked",
    "acquisition_coverage",
    "acquisition_gaps",
    "new_verification_request",
    "ambiguous_history_followup",
}

_TARGET_UNAVAILABLE = (
    "I can’t safely identify which earlier statement you mean from the supplied "
    "conversation context. I did not perform a new verification."
)
_NO_RECORD = (
    "I don’t have a retained evidence record for that immediately previous answer, "
    "so I can’t honestly say what supported it. I did not perform a new verification."
)
_AMBIGUOUS = (
    "I found more than one retained claim for the immediately previous answer, so I "
    "can’t safely choose one. I did not perform a new verification."
)
_QUOTED_NO_RECORD = (
    "I don’t have a retained evidence record matching that quoted earlier statement, "
    "so I can’t honestly say what supported it. I did not perform a new verification."
)
_QUOTED_AMBIGUOUS = (
    "I found more than one retained claim matching that quoted earlier statement, so "
    "I can’t safely choose one. I did not perform a new verification."
)
_DEPENDENCY_UNAVAILABLE = (
    "I couldn’t access the retained evidence record for that earlier answer. I can’t "
    "honestly reconstruct its support from memory, and I did not perform a new "
    "verification."
)
_INVALID_RECORD = (
    "The retained evidence record for that earlier answer was incomplete or "
    "unsupported, so I can’t safely explain its support. I did not perform a new "
    "verification."
)
_NO_NEW_VERIFICATION = "I didn’t run another search or verification for this explanation."
_ACQUISITION_TARGET_UNAVAILABLE = (
    "I can’t safely identify which earlier answer you mean from the supplied "
    "conversation context, so I can’t say what was checked. I did not perform a new "
    "verification."
)
_ACQUISITION_RESOLUTION_NO_RECORD = (
    "I couldn’t resolve a retained acquisition record for the specified response. "
    "I did not perform a new verification for this explanation."
)
_ACQUISITION_RESOLUTION_AMBIGUOUS = (
    "More than one exact prior response matched, so I did not select an acquisition "
    "record. I did not perform a new verification for this explanation."
)
_ACQUISITION_RESOLUTION_INVALID = (
    "The retained acquisition record failed association or privacy validation, so I "
    "can’t safely explain it. I did not perform a new verification for this explanation."
)
_ACQUISITION_RESOLUTION_UNAVAILABLE = (
    "I couldn’t safely access the retained acquisition record for the specified "
    "response. I did not perform a new verification for this explanation."
)

Identifier = Annotated[
    str,
    Field(min_length=1, max_length=120, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"),
]
BoundedText = Annotated[str, Field(min_length=1, max_length=500)]
ClaimEvidenceAuthority = Literal[
    "peer_reviewed_evidence",
    "clinical_guidance",
    "manufacturer_guidance",
    "tool_output",
    "trusted_integration",
    "user_report",
    "runtime_inference",
    "speculation",
    "unknown",
]


class ClaimEvidenceReference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ref_type: Literal[
        "message",
        "derived_text",
        "artifact",
        "external_source",
        "world_state_claim",
        "tool_output",
        "integration_event",
    ]
    ref_id: Identifier
    owner_id: Identifier
    conversation_id: Identifier | None = None
    support_kind: Literal["direct", "corroborating", "contextual", "contradictory"]
    authority: ClaimEvidenceAuthority
    freshness_state: Literal[
        "active",
        "stale",
        "superseded",
        "corrected",
        "unknown_freshness",
        "not_applicable",
    ]


class ClaimRecordV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_id: Identifier
    schema_version: Literal["claim-record.v1"]
    owner_id: Identifier
    conversation_id: Identifier
    request_id: Identifier
    assistant_message_id: Identifier
    surface: Annotated[str, Field(min_length=1, max_length=64)]
    runtime_session_id: Identifier
    runtime_turn_id: Identifier
    acquisition_manifest_id: Identifier | None = None
    claim_anchor: BoundedText
    claim_anchor_digest: Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
    claim_class: Literal[
        "verified_fact",
        "source_backed_fact",
        "manufacturer_guidance",
        "expert_consensus",
        "runtime_inference",
        "speculation",
        "unknown",
    ]
    calibration_status: Literal["supported", "limited", "unsupported"]
    evidence_strength: Literal["strong", "moderate", "weak", "none"]
    confidence: Literal["high", "medium", "low", "unknown"]
    strongest_authority: ClaimEvidenceAuthority
    freshness_summary: Literal["current", "mixed", "stale", "unknown", "not_applicable"]
    uncertainty_disclosure_required: bool
    validated_evidence_references: list[ClaimEvidenceReference] = Field(max_length=16)
    limitation_codes: list[
        Literal[
            "no_supporting_evidence",
            "context_only",
            "low_authority_evidence",
            "stale_evidence",
            "unknown_freshness",
            "superseded_or_corrected_evidence",
            "contradictory_evidence",
            "single_source",
            "inference_dominant",
            "speculation_only",
        ]
    ] = Field(max_length=10)
    user_safe_summary: BoundedText
    created_at: Annotated[str, Field(min_length=1, max_length=80)]

    @field_validator("claim_anchor", mode="before")
    @classmethod
    def normalize_anchor(cls, value: Any) -> Any:
        return normalize_text(value) if isinstance(value, str) else value

    @model_validator(mode="after")
    def validate_collections(self) -> "ClaimRecordV1":
        identities = [
            (reference.ref_type, reference.ref_id)
            for reference in self.validated_evidence_references
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate_evidence_reference")
        if len(self.limitation_codes) != len(set(self.limitation_codes)):
            raise ValueError("duplicate_limitation_code")
        return self


class ClaimSupportMaterialExclusion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_ref_id: Identifier
    reason: Annotated[str, Field(min_length=1, max_length=160)]

    @field_validator("reason", mode="before")
    @classmethod
    def normalize_reason(cls, value: Any) -> Any:
        return normalize_text(value) if isinstance(value, str) else value


class ClaimSupportDerivationRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    derivation_id: Identifier
    operation: Literal["divide", "mean"]
    canonical_inputs: list[Annotated[str, Field(min_length=1, max_length=160)]] = (
        Field(min_length=1, max_length=16)
    )
    canonical_result: Annotated[str, Field(min_length=1, max_length=160)]
    execution_digest: Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
    executor_version: Identifier
    supporting_evidence_ref_ids: list[Identifier] = Field(
        default_factory=list,
        max_length=16,
    )
    input_basis: Literal["system_established", "model_interpreted"]

    @model_validator(mode="after")
    def validate_reference_uniqueness(self):
        if len(self.supporting_evidence_ref_ids) != len(
            set(self.supporting_evidence_ref_ids)
        ):
            raise ValueError("duplicate_derivation_evidence_reference")
        return self


class ClaimSupportRecordPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_digest: Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
    supporting_evidence_ref_ids: list[Identifier] = Field(
        default_factory=list, max_length=16
    )
    counterevidence_ref_ids: list[Identifier] = Field(
        default_factory=list, max_length=16
    )
    material_exclusions: list[ClaimSupportMaterialExclusion] = Field(
        default_factory=list, max_length=16
    )
    executed_derivations: list[ClaimSupportDerivationRecord] = Field(
        default_factory=list, max_length=16
    )
    material_scope_limitations: list[Identifier] = Field(
        default_factory=list, max_length=10
    )
    calibration_status: Literal["supported", "limited", "unsupported"]
    conclusion_disposition: Literal["allowed", "qualified", "withheld"]
    qualification_required: bool
    limitation_codes: list[Identifier] = Field(default_factory=list, max_length=10)

    @model_validator(mode="after")
    def validate_support_skeleton(self):
        collections = (
            self.supporting_evidence_ref_ids,
            self.counterevidence_ref_ids,
            [item.evidence_ref_id for item in self.material_exclusions],
            [item.derivation_id for item in self.executed_derivations],
            self.material_scope_limitations,
            self.limitation_codes,
        )
        if any(len(values) != len(set(values)) for values in collections):
            raise ValueError("duplicate_support_metadata")
        if set(self.supporting_evidence_ref_ids) & set(self.counterevidence_ref_ids):
            raise ValueError("conflicting_evidence_role")
        known_refs = (
            set(self.supporting_evidence_ref_ids)
            | set(self.counterevidence_ref_ids)
            | {item.evidence_ref_id for item in self.material_exclusions}
        )
        for derivation in self.executed_derivations:
            if not set(derivation.supporting_evidence_ref_ids) <= known_refs:
                raise ValueError("derivation_evidence_reference_unknown")
        if (
            self.calibration_status == "supported"
            and self.conclusion_disposition == "withheld"
        ):
            raise ValueError("support_disposition_incoherent")
        if self.conclusion_disposition == "allowed" and self.qualification_required:
            raise ValueError("support_qualification_incoherent")
        return self


class ClaimRecordV2(ClaimRecordV1):
    schema_version: Literal["claim-record.v2"]
    presented_to_user: bool
    support: ClaimSupportRecordPayload

    @model_validator(mode="after")
    def validate_v2_support(self):
        expected_strength = "weak" if self.validated_evidence_references else "none"
        if (
            self.claim_class != "runtime_inference"
            or self.evidence_strength != expected_strength
            or self.confidence != "unknown"
            or self.strongest_authority != "unknown"
            or self.freshness_summary != "unknown"
            or self.uncertainty_disclosure_required is not True
        ):
            raise ValueError("v2_compatibility_projection_not_neutral")
        if any(
            reference.support_kind != "contextual"
            or reference.authority != "unknown"
            or reference.freshness_state != "unknown_freshness"
            for reference in self.validated_evidence_references
        ):
            raise ValueError("v2_evidence_projection_not_neutral")
        if self.support.claim_digest != self.claim_anchor_digest:
            raise ValueError("support_claim_digest_mismatch")
        if self.support.calibration_status != self.calibration_status:
            raise ValueError("support_calibration_mismatch")
        evidence_ids = {
            reference.ref_id for reference in self.validated_evidence_references
        }
        support_ids = (
            set(self.support.supporting_evidence_ref_ids)
            | set(self.support.counterevidence_ref_ids)
            | {
                exclusion.evidence_ref_id
                for exclusion in self.support.material_exclusions
            }
        )
        if not support_ids <= evidence_ids:
            raise ValueError("support_evidence_reference_unknown")
        return self


ImmediateHistoryClaimRecord = Annotated[
    ClaimRecordV1 | ClaimRecordV2,
    Field(discriminator="schema_version"),
]

# The legacy list endpoint remains a v1-only compatibility surface.
ClaimRecord = ClaimRecordV1


class ClaimRecordListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    records: list[ClaimRecord] = Field(max_length=20)


AcquisitionResolutionStatus = Literal["resolved", "no_record", "ambiguous", "invalid"]
AcquisitionResolutionReason = Literal[
    "immediate_response_resolved",
    "immediate_response_mismatch",
    "immediate_response_trace_absent",
    "immediate_response_manifest_absent",
    "quoted_response_resolved",
    "quoted_response_not_found",
    "quoted_response_ambiguous",
    "quoted_response_trace_absent",
    "quoted_response_manifest_absent",
    "trace_scope_mismatch",
    "assistant_message_request_mismatch",
    "manifest_association_invalid",
    "manifest_privacy_boundary_invalid",
]


class AcquisitionHistoryResolvedRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    original_request_id: Identifier
    assistant_message_id: Identifier
    surface: Annotated[str, Field(min_length=1, max_length=64)]
    trace_status: Literal["ok", "degraded"]
    response_digest: Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
    normalized_first_paragraph: BoundedText
    acquisition_manifest: dict[str, Any]


class AcquisitionHistoryResolveResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["acquisition-history-resolution.v1"]
    request_id: Identifier
    owner_id: Identifier
    conversation_id: Identifier
    surface: Annotated[str, Field(min_length=1, max_length=64)]
    target_mode: Literal["immediate_previous", "quoted_first_paragraph"]
    resolution_status: AcquisitionResolutionStatus
    match_count: Annotated[int, Field(ge=0, le=50)]
    reason_code: AcquisitionResolutionReason
    record: AcquisitionHistoryResolvedRecord | None = None

    @model_validator(mode="after")
    def validate_resolution(self) -> "AcquisitionHistoryResolveResponse":
        if self.resolution_status == "resolved":
            if self.record is None or self.match_count != 1:
                raise ValueError("resolved_acquisition_record_required")
        elif self.record is not None:
            raise ValueError("unresolved_acquisition_record_not_allowed")
        if self.resolution_status == "ambiguous" and self.match_count < 2:
            raise ValueError("ambiguous_acquisition_match_count_invalid")
        return self


class HistoryFollowupClassifierOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    intent: Literal[
        "not_history_followup",
        "support_explanation",
        "acquisition_checked",
        "acquisition_coverage",
        "acquisition_gaps",
        "new_verification_request",
        "ambiguous_history_followup",
    ]
    confidence: float = Field(ge=0.0, le=1.0)
    target_mode: Literal["immediate_previous", "explicit_reference"]
    new_verification_requested: bool

    @model_validator(mode="after")
    def validate_consistency(self):
        if self.intent == "new_verification_request" and not self.new_verification_requested:
            raise ValueError("new_verification_intent_requires_explicit_request")
        if (
            self.intent in {"not_history_followup", "ambiguous_history_followup"}
            and self.new_verification_requested
        ):
            raise ValueError("nonactionable_intent_forbids_verification")
        return self


class ImmediateHistoryRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    record_kind: Literal["support", "acquisition"]
    assistant_message_id: Identifier
    original_request_id: Identifier
    support_record: ImmediateHistoryClaimRecord | None = None
    acquisition_record: AcquisitionHistoryResolvedRecord | None = None

    @model_validator(mode="after")
    def validate_record_kind(self):
        if self.record_kind == "support":
            if self.support_record is None or self.acquisition_record is not None:
                raise ValueError("support_immediate_record_invalid")
            if (
                isinstance(self.support_record, ClaimRecordV2)
                and not self.support_record.presented_to_user
            ):
                raise ValueError("shadow_support_immediate_record_invalid")
        elif self.acquisition_record is None or self.support_record is not None:
            raise ValueError("acquisition_immediate_record_invalid")
        return self


class HistoryRootLineage(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["history-root-lineage.v1"]
    root_assistant_message_id: Annotated[str, Field(min_length=36, max_length=36)]
    record_kind: Literal["support", "acquisition"]

    @field_validator("root_assistant_message_id")
    @classmethod
    def validate_root_message_id(cls, value: str) -> str:
        try:
            UUID(value)
        except (TypeError, ValueError, AttributeError):
            raise ValueError("history_root_lineage_message_id_invalid") from None
        return value


class ImmediateHistoryResolveResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["immediate-history-resolution.v2"]
    request_id: Identifier
    owner_id: Identifier
    conversation_id: Identifier
    surface: Annotated[str, Field(min_length=1, max_length=64)]
    explanation_kind: Literal["support", "acquisition"]
    resolution_status: Literal[
        "resolved", "no_record", "ambiguous", "invalid", "unavailable"
    ]
    resolution_source: Literal["direct_record", "root_lineage", "none"]
    lineage_dereference_count: Literal[0, 1]
    match_count: int = Field(ge=0, le=2)
    reason_code: Literal[
        "direct_support_record_resolved",
        "direct_acquisition_record_resolved",
        "root_lineage_support_record_resolved",
        "root_lineage_acquisition_record_resolved",
        "direct_record_absent_lineage_absent",
        "lineage_root_missing",
        "lineage_root_unresolvable",
        "lineage_root_not_direct_record_owner",
        "direct_support_record_ambiguous",
        "direct_response_invalid",
        "direct_support_record_invalid",
        "direct_acquisition_record_invalid",
        "lineage_malformed",
        "lineage_version_unsupported",
        "lineage_record_kind_mismatch",
        "lineage_owner_mismatch",
        "lineage_conversation_mismatch",
        "lineage_surface_mismatch",
        "lineage_root_role_invalid",
        "lineage_root_recursive",
        "lineage_root_association_invalid",
        "history_store_unavailable",
    ]
    record: ImmediateHistoryRecord | None = None
    history_root_lineage: HistoryRootLineage | None = None

    @model_validator(mode="after")
    def validate_resolution(self):
        direct_reasons = {
            "support": "direct_support_record_resolved",
            "acquisition": "direct_acquisition_record_resolved",
        }
        root_reasons = {
            "support": "root_lineage_support_record_resolved",
            "acquisition": "root_lineage_acquisition_record_resolved",
        }
        if self.resolution_status == "resolved":
            if (
                self.record is None
                or self.history_root_lineage is None
                or self.match_count != 1
                or self.record.record_kind != self.explanation_kind
                or self.history_root_lineage.record_kind != self.explanation_kind
                or self.history_root_lineage.root_assistant_message_id
                != self.record.assistant_message_id
            ):
                raise ValueError("resolved_immediate_history_v2_shape_invalid")
            if self.resolution_source == "direct_record":
                if (
                    self.lineage_dereference_count != 0
                    or self.reason_code != direct_reasons[self.explanation_kind]
                ):
                    raise ValueError("direct_immediate_history_v2_shape_invalid")
            elif self.resolution_source == "root_lineage":
                if (
                    self.lineage_dereference_count != 1
                    or self.reason_code != root_reasons[self.explanation_kind]
                ):
                    raise ValueError("root_immediate_history_v2_shape_invalid")
            else:
                raise ValueError("resolved_immediate_history_v2_source_invalid")
            return self

        if (
            self.resolution_source != "none"
            or self.record is not None
            or self.history_root_lineage is not None
        ):
            raise ValueError("unresolved_immediate_history_v2_shape_invalid")
        if self.resolution_status == "ambiguous":
            if (
                self.explanation_kind != "support"
                or self.reason_code != "direct_support_record_ambiguous"
                or self.lineage_dereference_count != 0
                or self.match_count != 2
            ):
                raise ValueError("ambiguous_immediate_history_v2_shape_invalid")
            return self
        if self.match_count != 0:
            raise ValueError("unresolved_immediate_history_v2_match_count_invalid")

        no_record_reasons = {
            "direct_record_absent_lineage_absent",
            "lineage_root_missing",
            "lineage_root_unresolvable",
            "lineage_root_not_direct_record_owner",
        }
        invalid_reasons = {
            "direct_response_invalid",
            "direct_support_record_invalid",
            "direct_acquisition_record_invalid",
            "lineage_malformed",
            "lineage_version_unsupported",
            "lineage_record_kind_mismatch",
            "lineage_owner_mismatch",
            "lineage_conversation_mismatch",
            "lineage_surface_mismatch",
            "lineage_root_role_invalid",
            "lineage_root_recursive",
            "lineage_root_association_invalid",
        }
        if self.resolution_status == "no_record":
            if self.reason_code not in no_record_reasons:
                raise ValueError("no_record_immediate_history_v2_reason_invalid")
        elif self.resolution_status == "invalid":
            if self.reason_code not in invalid_reasons:
                raise ValueError("invalid_immediate_history_v2_reason_invalid")
        elif self.resolution_status == "unavailable":
            if self.reason_code != "history_store_unavailable":
                raise ValueError("unavailable_immediate_history_v2_reason_invalid")
        else:
            raise ValueError("immediate_history_v2_status_invalid")

        if (
            self.reason_code == "direct_support_record_invalid"
            and self.explanation_kind != "support"
        ):
            raise ValueError("support_immediate_history_v2_reason_kind_invalid")
        if (
            self.reason_code == "direct_acquisition_record_invalid"
            and self.explanation_kind != "acquisition"
        ):
            raise ValueError("acquisition_immediate_history_v2_reason_kind_invalid")

        prelookup_reasons = {
            "direct_record_absent_lineage_absent",
            "direct_response_invalid",
            "direct_support_record_invalid",
            "direct_acquisition_record_invalid",
            "lineage_malformed",
            "lineage_version_unsupported",
            "lineage_record_kind_mismatch",
        }
        if self.reason_code in prelookup_reasons and self.lineage_dereference_count != 0:
            raise ValueError("prelookup_immediate_history_v2_dereference_invalid")
        if (
            self.reason_code not in prelookup_reasons
            and self.reason_code != "history_store_unavailable"
            and self.lineage_dereference_count != 1
        ):
            raise ValueError("root_immediate_history_v2_dereference_invalid")
        return self


@dataclass(frozen=True)
class ClaimExplanationIntent:
    mode: Literal["latest", "quoted_anchor"]
    target_anchor: str | None = None
    explanation_kind: Literal["support", "acquisition"] = "support"
    acquisition_question: Literal["checked", "coverage", "gaps"] | None = None
    new_verification_requested: bool = False


@dataclass(frozen=True)
class ClaimExplanationOutcome:
    handled: bool
    answer: str | None
    status: Literal["ok", "degraded"] | None
    trace: dict[str, Any]
    new_verification_requested: bool = False
    verification_target: str | None = None
    history_root_lineage: dict[str, Any] | None = dataclass_field(
        default=None,
        repr=False,
    )


def normalize_text(value: str) -> str:
    return " ".join(value.split())


def history_classifier_response_format() -> dict[str, Any]:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "history_followup_classification",
            "strict": True,
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "intent": {
                        "type": "string",
                        "enum": sorted(_HISTORY_CLASSIFIER_INTENTS),
                    },
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "target_mode": {
                        "type": "string",
                        "enum": ["immediate_previous", "explicit_reference"],
                    },
                    "new_verification_requested": {"type": "boolean"},
                },
                "required": [
                    "intent",
                    "confidence",
                    "target_mode",
                    "new_verification_requested",
                ],
            },
        },
    }


def parse_history_classifier_completion(value: Any) -> dict[str, Any]:
    allowed_fields = {
        "choices",
        "usage",
        "id",
        "object",
        "created",
        "model",
        "system_fingerprint",
    }
    if not isinstance(value, dict) or set(value) - allowed_fields:
        raise ValueError("classifier_completion_invalid")
    choices = value.get("choices")
    if not isinstance(choices, list) or len(choices) != 1:
        raise ValueError("classifier_choices_invalid")
    choice = choices[0]
    if not isinstance(choice, dict):
        raise ValueError("classifier_choice_invalid")
    message = choice.get("message")
    if not isinstance(message, dict) or message.get("tool_calls") not in (None, []):
        raise ValueError("classifier_tool_call_forbidden")
    if message.get("refusal") not in (None, ""):
        raise ValueError("classifier_refusal_invalid")
    if set(message) - {"role", "content", "refusal", "tool_calls"}:
        raise ValueError("classifier_message_invalid")
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("classifier_content_invalid")
    try:
        decoded = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError("classifier_json_invalid") from exc
    if not isinstance(decoded, dict):
        raise ValueError("classifier_json_object_required")
    output = HistoryFollowupClassifierOutput.model_validate(decoded)
    return {"source": "classifier", **output.model_dump()}


def deterministic_history_followup_candidate(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str):
        return None
    normalized = normalize_text(value).casefold()
    if normalized.endswith(("?", ".")):
        normalized = normalized[:-1].rstrip()
    if normalized in {"verify that again", "check that again"}:
        return {
            "source": "deterministic",
            "intent": "new_verification_request",
            "confidence": 1.0,
            "target_mode": "immediate_previous",
            "new_verification_requested": True,
        }
    intent = parse_claim_explanation_intent(value)
    if intent is None or intent.mode != "latest":
        return None
    if intent.explanation_kind == "support":
        history_intent = "support_explanation"
    else:
        history_intent = {
            "checked": "acquisition_checked",
            "coverage": "acquisition_coverage",
            "gaps": "acquisition_gaps",
        }[intent.acquisition_question or "checked"]
    return {
        "source": "deterministic",
        "intent": history_intent,
        "confidence": 1.0,
        "target_mode": "immediate_previous",
        "new_verification_requested": intent.new_verification_requested,
    }


def classifier_eligible_history_followup(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = normalize_text(value)
    if (
        not text
        or len(text) > _CLASSIFIER_MAX_CHARACTERS
        or len(text.split()) > _CLASSIFIER_MAX_WORDS
    ):
        return False
    normalized = text.casefold()
    if re.search(r"\b(?:should\s+(?:i|we|you)|how\s+(?:do|can)\s+i)\b", normalized):
        return False
    if re.search(r"\bwhat\s+did\s+(?!you\b)[a-z][a-z'-]*\s+(?:check|review|examine)", normalized):
        return False
    deictic = re.search(
        r"\b(?:that|this|previous|earlier|prior|your answer|this answer|that answer|conclusion)\b",
        normalized,
    ) is not None
    second_person = re.search(
        r"\b(?:you|your)\b",
        normalized,
    ) is not None
    concept = re.search(
        r"\b(?:support|supported|basis|based|evidence|source|sources|sure|"
        r"conclusion|record|records|check|checked|review|reviewed|examine|examined|"
        r"consult|consulted|cover|covered|everything|miss|missed|skip|skipped|gap|"
        r"gaps|verify|verification|recheck)\b",
        normalized,
    ) is not None
    interrogative = (
        "?" in text
        or re.match(
            r"^(?:how|what|where|which|did|do|can|could|would|anything|was|were)\b",
            normalized,
        )
        is not None
    )
    return bool(interrogative and concept and (deictic or second_person))


def parse_claim_explanation_intent(value: Any) -> ClaimExplanationIntent | None:
    if not isinstance(value, str):
        return None
    compound_match = _COMPOUND_ACQUISITION_INTENT_RE.fullmatch(value.strip())
    quoted_compound_match = _QUOTED_COMPOUND_ACQUISITION_INTENT_RE.fullmatch(
        value.strip()
    )
    if compound_match is not None or quoted_compound_match is not None:
        match = quoted_compound_match or compound_match
        if match is None:
            return None
        target_anchor = None
        mode: Literal["latest", "quoted_anchor"] = "latest"
        if quoted_compound_match is not None:
            mode = "quoted_anchor"
            target_anchor = normalize_text(quoted_compound_match.group("anchor"))
            if not target_anchor or len(target_anchor) > 500:
                target_anchor = None
        acquisition_question = next(
            question
            for question in ("checked", "coverage", "gaps")
            if match.group(question) is not None
        )
        return ClaimExplanationIntent(
            mode=mode,
            target_anchor=target_anchor,
            explanation_kind="acquisition",
            acquisition_question=acquisition_question,
            new_verification_requested=True,
        )
    normalized = normalize_text(value).casefold()
    if normalized.endswith(("?", ".")):
        normalized = normalized[:-1].rstrip()
    if normalized in _SUPPORTED_INTENTS:
        return ClaimExplanationIntent(mode="latest")
    acquisition_question = _ACQUISITION_INTENTS.get(normalized)
    if acquisition_question is not None:
        return ClaimExplanationIntent(
            mode="latest",
            explanation_kind="acquisition",
            acquisition_question=acquisition_question,
        )

    match = _QUOTED_INTENT_RE.fullmatch(value.strip())
    if match is not None:
        target_anchor = normalize_text(match.group("anchor"))
        if not target_anchor or len(target_anchor) > 500:
            target_anchor = None
        return ClaimExplanationIntent(
            mode="quoted_anchor",
            target_anchor=target_anchor,
        )

    acquisition_match = _QUOTED_ACQUISITION_INTENT_RE.fullmatch(value.strip())
    if acquisition_match is None:
        return None
    target_anchor = normalize_text(acquisition_match.group("anchor"))
    if not target_anchor or len(target_anchor) > 500:
        target_anchor = None
    acquisition_question = next(
        question
        for question in ("checked", "coverage", "gaps")
        if acquisition_match.group(question) is not None
    )
    return ClaimExplanationIntent(
        mode="quoted_anchor",
        target_anchor=target_anchor,
        explanation_kind="acquisition",
        acquisition_question=acquisition_question,
    )


def is_claim_explanation_intent(value: Any) -> bool:
    return parse_claim_explanation_intent(value) is not None


def _trace(
    *,
    reason_code: str,
    target_mode: Literal["immediate_previous", "quoted_anchor"] = "immediate_previous",
    **updates: Any,
) -> dict[str, Any]:
    trace = {
        "enabled": True,
        "intent_status": "matched",
        "target_mode": target_mode,
        "target_status": "not_resolved",
        "lookup_status": "not_requested",
        "resolution_status": "not_resolved",
        "render_status": "not_attempted",
        "reason_code": reason_code,
        "storage_call_count": 0,
        "provider_call_count": 0,
        "record_count": 0,
        "newest_group_count": 0,
        "matched_record_count": 0,
        "claim_id": None,
        "claim_anchor_digest": None,
    }
    trace.update(updates)
    return trace


def _fallback(answer: str, reason_code: str, **trace_updates: Any) -> ClaimExplanationOutcome:
    return ClaimExplanationOutcome(
        handled=True,
        answer=answer,
        status="degraded",
        trace=_trace(reason_code=reason_code, **trace_updates),
    )


def _prior_assistant(
    messages: Any,
    intent: ClaimExplanationIntent,
) -> str | None:
    if not isinstance(messages, list) or len(messages) < 2:
        return None
    final = messages[-1]
    prior = messages[-2]
    if (
        not isinstance(final, dict)
        or final.get("role") != "user"
        or intent.mode != "latest"
        or not isinstance(prior, dict)
        or prior.get("role") != "assistant"
        or not isinstance(prior.get("content"), str)
    ):
        return None
    content = prior["content"]
    first_paragraph = _normalized_first_response_paragraph(content)
    if first_paragraph is None or len(first_paragraph) > 500:
        return None
    return content


def _digest(anchor: str) -> str:
    return f"sha256:{hashlib.sha256(anchor.encode()).hexdigest()}"


def _response_digest(content: str) -> str:
    return f"sha256:{hashlib.sha256(content.encode('utf-8')).hexdigest()}"


def _normalized_first_response_paragraph(content: Any) -> str | None:
    if not isinstance(content, str) or not content:
        return None
    first_paragraph = _PARAGRAPH_SEPARATOR.split(content, maxsplit=1)[0]
    normalized = normalize_text(first_paragraph)
    return normalized or None


_CLAIM_CLASS_WORDING = {
    "verified_fact": "a verified fact",
    "source_backed_fact": "a source-backed fact",
    "manufacturer_guidance": "manufacturer guidance",
    "expert_consensus": "expert consensus",
    "runtime_inference": "an inference",
}
_STRENGTH_WORDING = {
    "strong": "strong support",
    "moderate": "moderate support",
    "weak": "weak support",
}
_FRESHNESS_WORDING = {
    "current": "The evidence was marked current.",
    "mixed": "The evidence had mixed freshness.",
    "stale": "The evidence was marked stale.",
    "unknown": "The evidence freshness was unknown.",
}
_LIMITATION_WORDING = {
    "single_source": "Only one supporting record was retained.",
    "low_authority_evidence": (
        "The source was treated as user-provided material rather than independently "
        "authoritative."
    ),
    "stale_evidence": "The retained evidence was marked stale.",
    "unknown_freshness": "The evidence freshness could not be established.",
    "superseded_or_corrected_evidence": (
        "The record indicates that evidence had been superseded or corrected."
    ),
    "contradictory_evidence": "The retained record included contradictory evidence.",
    "inference_dominant": "The conclusion depended mainly on inference.",
    "speculation_only": "The recorded support was speculative.",
}
_LIMITATION_ORDER = tuple(_LIMITATION_WORDING)


def _render(record: ImmediateHistoryClaimRecord) -> str:
    evidence_type = record.validated_evidence_references[0].ref_type
    evidence_wording = {
        "derived_text": "one retained file excerpt",
        "artifact": "one retained file record",
        "external_source": "one governed external-source record",
        "tool_output": "one retained tool result",
        "integration_event": "one retained integration event",
    }[evidence_type]
    direct = record.validated_evidence_references[0].support_kind == "direct"
    sentences = [f"That earlier answer was supported by {evidence_wording}."]
    sentences.append(
        "It directly supported the answer."
        if direct
        else "It provided background rather than direct support."
    )
    freshness = {
        "current": "It was marked current when the answer was given.",
        "mixed": "Some of it may have been older than other parts.",
        "stale": "It was marked stale, so the answer may no longer be current.",
        "unknown": "Its age could not be confirmed.",
        "not_applicable": "Its age was not relevant to that answer.",
    }.get(record.freshness_summary)
    if freshness is not None:
        sentences.append(freshness)
    sentences.append("The saved support details do not include a safe source name.")
    limitations = set(record.limitation_codes)
    sentences.extend(
        _LIMITATION_WORDING[code] for code in _LIMITATION_ORDER if code in limitations
    )
    sentences.append(_NO_NEW_VERIFICATION)
    return " ".join(sentences)


def _record_support_status(
    record: ImmediateHistoryClaimRecord,
    *,
    owner_id: str,
    conversation_id: str,
) -> Literal["supported", "unsupported", "insufficient", "invalid"]:
    if record.owner_id != owner_id or record.conversation_id != conversation_id:
        return "invalid"
    if record.claim_anchor_digest != _digest(record.claim_anchor):
        return "invalid"
    if isinstance(record, ClaimRecordV2):
        if (
            not record.presented_to_user
            or record.support.conclusion_disposition == "withheld"
            or record.support.calibration_status == "unsupported"
            or (
                not record.support.supporting_evidence_ref_ids
                and not record.support.executed_derivations
            )
        ):
            return "insufficient"
        return "supported"
    if (
        record.calibration_status == "unsupported"
        or record.evidence_strength == "none"
        or record.claim_class in {"unknown", "speculation"}
        or {"no_supporting_evidence", "context_only"} & set(record.limitation_codes)
    ):
        return "insufficient"
    if len(record.validated_evidence_references) != 1:
        return "unsupported"
    reference = record.validated_evidence_references[0]
    supported_reference = (
        reference.ref_type in {"artifact", "derived_text"}
        and reference.authority == "user_report"
        and record.strongest_authority == "user_report"
    ) or (
        reference.ref_type == "external_source"
        and reference.authority
        in {
            "peer_reviewed_evidence",
            "clinical_guidance",
            "manufacturer_guidance",
            "trusted_integration",
        }
        and record.strongest_authority == reference.authority
    ) or (
        reference.ref_type in {"tool_output", "integration_event"}
        and reference.authority in {"tool_output", "trusted_integration"}
        and record.strongest_authority == reference.authority
    )
    if (
        not supported_reference
        or reference.support_kind != "direct"
        or reference.owner_id != owner_id
        or reference.conversation_id not in {None, conversation_id}
    ):
        return "unsupported"
    return "supported"


def _record_matches_scope(
    record: ImmediateHistoryClaimRecord,
    *,
    owner_id: str,
    conversation_id: str,
) -> bool:
    if (
        record.owner_id != owner_id
        or record.conversation_id != conversation_id
        or record.claim_anchor_digest != _digest(record.claim_anchor)
    ):
        return False
    return all(
        reference.owner_id == owner_id
        and reference.conversation_id in {None, conversation_id}
        for reference in record.validated_evidence_references
    )


@dataclass(frozen=True)
class AcquisitionHistory:
    task_shape: Literal[
        "no_acquisition",
        "ordinary_context_augmentation",
        "targeted_lookup",
        "cross_source_comparison",
        "bounded_exhaustive_review",
    ]
    strategy: Literal[
        "none",
        "context_augmentation",
        "targeted_retrieval",
        "exact_fetch",
        "hybrid",
    ]
    sufficiency_status: Literal[
        "not_evaluated",
        "sufficient_for_declared_scope",
        "sufficient_with_limitations",
        "insufficient",
        "unknown",
    ]
    inventory_status: Literal[
        "complete_for_declared_scope",
        "partial",
        "unknown",
        "unavailable",
    ]
    counts: dict[str, int]
    limitation_codes: tuple[str, ...]
    budget_truncated: bool
    candidate_truncated: bool
    qualification_required: bool
    additional_acquisition_required: bool
    identifiers_suppressed: bool
    changed_premise_exact_follow_up: bool
    final_next_step: str | None
    source_summaries: tuple[SourceHistorySummary, ...]


@dataclass(frozen=True)
class SourceHistorySummary:
    source_id: str
    display_name: str
    connector: str | None
    authority_role: str
    domain_tags: tuple[str, ...]
    considered: bool
    selected: bool
    used: bool
    returned_reference_count: int
    retained_reference_count: int
    safe_location_labels: tuple[str, ...]
    contribution_reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class AcquisitionHistoryProjection:
    history: AcquisitionHistory | None
    reason: str


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,119}$")
_SAFE_SOURCE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,119}$")
_GOOGLE_SHEETS_SOURCE_REF = re.compile(
    r"^google_sheets:(?P<source_id>[A-Za-z0-9][A-Za-z0-9_-]{0,119}):"
    r"(?P<sheet>'(?:[^'\r\n]|'')+'|[A-Za-z0-9_]+)!"
    r"(?P<range>[A-Z]+[1-9][0-9]*:[A-Z]+[1-9][0-9]*)$"
)
_SECRET_LIKE_TEXT = re.compile(
    r"\b(?:api[ _-]?key|authorization|bearer|credential|password|passwd|"
    r"private[ _-]?key|secret|session[ _-]?token|access[ _-]?token)\b",
    re.IGNORECASE,
)
_SOURCE_SUMMARY_REASON_CODES = {
    "retained_records_contributed",
    "returned_records_not_retained",
    "selected_no_result",
    "considered_not_selected",
    "exact_reference_retrieved",
    "source_unavailable",
    "source_disabled",
}
_MANIFEST_IDENTITY_FIELDS = (
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
_PLAN_LIMITATION_CODES = {
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
}
_SHAPE_REASON_CODES = {
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
}


def _bounded_count(value: Any, *, maximum: int = 10000) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 <= value <= maximum else None


def _valid_source_reference(value: Any) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= 240
        and value == value.strip()
        and re.search(r"[\x00-\x1f\x7f]", value) is None
        and "://" not in value
        and "?" not in value
    )


def _safe_source_summary_text(value: Any, *, maximum: int = 160) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split())
    if (
        not normalized
        or len(normalized) > maximum
        or re.search(r"[\x00-\x1f\x7f]", value)
        or "://" in normalized
        or "www." in normalized.casefold()
        or "/" in normalized
        or "\\" in normalized
        or re.match(r"^[A-Za-z]:", normalized)
        or re.match(r"^(?:#{1,6}|>|[-+*]|\d+[.)])\s", normalized)
        or re.search(r"\[[^]]*\]\s*\(", normalized)
        or _SECRET_LIKE_TEXT.search(normalized)
    ):
        return None
    return normalized


def _fallback_source_display_name(source_id: str) -> str:
    aliases = {
        "vehicle_log_primary": "Primary vehicle maintenance log",
        "vehicle_log_ev": "EV maintenance log",
    }
    if source_id in aliases:
        return aliases[source_id]
    words = re.sub(r"[_-]+", " ", source_id).strip()
    return words[:1].upper() + words[1:] if words else "Source"


def _source_id_from_reference(reference: str) -> str | None:
    parts = reference.split(":", 2)
    if len(parts) != 3 or _SAFE_SOURCE_ID.fullmatch(parts[1]) is None:
        return None
    return parts[1]


def _google_sheets_location_labels(
    source_id: str,
    references: set[str],
) -> tuple[str, ...]:
    ranges_by_sheet: dict[str, set[str]] = {}
    for reference in references:
        match = _GOOGLE_SHEETS_SOURCE_REF.fullmatch(reference)
        if match is None or match.group("source_id") != source_id:
            continue
        sheet = match.group("sheet")
        if sheet.startswith("'"):
            sheet = sheet[1:-1].replace("''", "'")
        safe_sheet = _safe_source_summary_text(sheet, maximum=80)
        if safe_sheet is None:
            continue
        ranges_by_sheet.setdefault(safe_sheet, set()).add(match.group("range"))
    labels = []
    for sheet in sorted(ranges_by_sheet, key=str.casefold):
        ranges = sorted(ranges_by_sheet[sheet])[:8]
        label = f"Google Sheets tab “{sheet}” — {', '.join(ranges)}"
        if _safe_source_summary_text(label, maximum=240) is not None:
            labels.append(label)
    return tuple(labels[:8])


def _references_by_source(
    references: set[str],
    *,
    known_source_ids: set[str] | None = None,
) -> dict[str, set[str]]:
    grouped: dict[str, set[str]] = {}
    for reference in references:
        source_id = next(
            (
                candidate
                for candidate in sorted(known_source_ids or set(), key=len, reverse=True)
                if reference.startswith(f"{candidate}:")
                or f":{candidate}:" in reference
            ),
            None,
        )
        if source_id is None:
            source_id = _source_id_from_reference(reference)
        if source_id is not None:
            grouped.setdefault(source_id, set()).add(reference)
    return grouped


def _legacy_source_summaries(
    *,
    considered: set[str],
    selected: set[str],
    used: set[str],
    returned: set[str],
    retained: set[str],
    unavailable: set[str],
    failed: set[str],
) -> tuple[SourceHistorySummary, ...]:
    known_ids = considered | selected | used | unavailable | failed
    returned_by_source = _references_by_source(returned, known_source_ids=known_ids)
    retained_by_source = _references_by_source(retained, known_source_ids=known_ids)
    source_ids = known_ids | set(returned_by_source) | set(retained_by_source)
    summaries = []
    for source_id in source_ids:
        returned_refs = returned_by_source.get(source_id, set())
        retained_refs = retained_by_source.get(source_id, set())
        reasons = []
        if retained_refs:
            reasons.append("retained_records_contributed")
        if returned_refs - retained_refs:
            reasons.append("returned_records_not_retained")
        if source_id in selected and not returned_refs:
            reasons.append("selected_no_result")
        if source_id in considered and source_id not in selected:
            reasons.append("considered_not_selected")
        if source_id in unavailable or source_id in failed:
            reasons.append("source_unavailable")
        references = returned_refs | retained_refs
        connector = (
            next(iter(references)).split(":", 1)[0] if references else None
        )
        summaries.append(
            SourceHistorySummary(
                source_id=source_id,
                display_name=_fallback_source_display_name(source_id),
                connector=connector,
                authority_role="unknown",
                domain_tags=(),
                considered=source_id in considered,
                selected=source_id in selected,
                used=source_id in used,
                returned_reference_count=len(returned_refs),
                retained_reference_count=len(retained_refs),
                safe_location_labels=_google_sheets_location_labels(
                    source_id,
                    retained_refs,
                ),
                contribution_reason_codes=tuple(reasons),
            )
        )
    return tuple(
        sorted(
            summaries,
            key=lambda item: (item.display_name.casefold(), item.source_id),
        )[:32]
    )


def _project_source_summaries(
    acquisition: dict[str, Any],
    *,
    suppressed: bool,
    considered: set[str] | None,
    selected: set[str] | None,
    used: set[str] | None,
    returned: set[str] | None,
    retained: set[str] | None,
    unavailable: set[str] | None,
    failed: set[str] | None,
) -> tuple[SourceHistorySummary, ...] | None:
    raw = acquisition.get("source_summaries")
    if raw is None:
        if "source_summaries_count" in acquisition:
            return None
        if suppressed:
            return ()
        assert considered is not None
        assert selected is not None
        assert used is not None
        assert returned is not None
        assert retained is not None
        assert unavailable is not None
        assert failed is not None
        return _legacy_source_summaries(
            considered=considered,
            selected=selected,
            used=used,
            returned=returned,
            retained=retained,
            unavailable=unavailable,
            failed=failed,
        )
    if not isinstance(raw, list) or len(raw) > 32:
        return None
    if suppressed:
        count = _bounded_count(acquisition.get("source_summaries_count"), maximum=32)
        return () if raw == [] and count is not None else None
    if "source_summaries_count" in acquisition:
        return None
    if any(value is None for value in (considered, selected, used, returned, retained)):
        return None
    expected_ids = set(considered or set()) | set(unavailable or set()) | set(failed or set())
    returned_by_source = _references_by_source(
        returned or set(),
        known_source_ids=expected_ids,
    )
    retained_by_source = _references_by_source(
        retained or set(),
        known_source_ids=expected_ids,
    )
    summaries = []
    observed_ids = set()
    required_keys = {
        "source_id",
        "display_name",
        "connector",
        "authority_role",
        "domain_tags",
        "considered",
        "selected",
        "used",
        "returned_reference_count",
        "retained_reference_count",
        "safe_location_labels",
        "contribution_reason_codes",
    }
    for item in raw:
        if not isinstance(item, dict) or set(item) != required_keys:
            return None
        source_id = item.get("source_id")
        display_name = _safe_source_summary_text(item.get("display_name"))
        connector = item.get("connector")
        authority_role = item.get("authority_role")
        domain_tags = item.get("domain_tags")
        locations = item.get("safe_location_labels")
        reasons = item.get("contribution_reason_codes")
        returned_count = _bounded_count(item.get("returned_reference_count"), maximum=1000)
        retained_count = _bounded_count(item.get("retained_reference_count"), maximum=1000)
        if (
            not isinstance(source_id, str)
            or _SAFE_SOURCE_ID.fullmatch(source_id) is None
            or source_id in observed_ids
            or display_name is None
            or not isinstance(connector, str)
            or _SAFE_IDENTIFIER.fullmatch(connector) is None
            or authority_role not in {"authoritative", "supplemental", "unknown"}
            or not isinstance(domain_tags, list)
            or len(domain_tags) > 8
            or any(
                not isinstance(tag, str) or _SAFE_IDENTIFIER.fullmatch(tag) is None
                for tag in domain_tags
            )
            or len(domain_tags) != len(set(domain_tags))
            or any(
                not isinstance(item.get(field), bool)
                for field in ("considered", "selected", "used")
            )
            or returned_count is None
            or retained_count is None
            or retained_count > returned_count
            or not isinstance(locations, list)
            or len(locations) > 8
            or any(_safe_source_summary_text(label, maximum=240) is None for label in locations)
            or len(locations) != len(set(locations))
            or not isinstance(reasons, list)
            or len(reasons) > 7
            or any(code not in _SOURCE_SUMMARY_REASON_CODES for code in reasons)
            or len(reasons) != len(set(reasons))
        ):
            return None
        returned_refs = returned_by_source.get(source_id, set())
        retained_refs = retained_by_source.get(source_id, set())
        if (
            item["considered"] != (source_id in (considered or set()))
            or item["selected"] != (source_id in (selected or set()))
            or item["used"] != (source_id in (used or set()))
            or returned_count != len(returned_refs)
            or retained_count != len(retained_refs)
            or tuple(locations) != _google_sheets_location_labels(source_id, retained_refs)
        ):
            return None
        observed_ids.add(source_id)
        summaries.append(
            SourceHistorySummary(
                source_id=source_id,
                display_name=display_name,
                connector=connector,
                authority_role=authority_role,
                domain_tags=tuple(domain_tags),
                considered=item["considered"],
                selected=item["selected"],
                used=item["used"],
                returned_reference_count=returned_count,
                retained_reference_count=retained_count,
                safe_location_labels=tuple(locations),
                contribution_reason_codes=tuple(reasons),
            )
        )
    if observed_ids != expected_ids:
        return None
    if raw != sorted(raw, key=lambda item: (item["display_name"].casefold(), item["source_id"])):
        return None
    return tuple(summaries)


def _identity_projection(
    acquisition: dict[str, Any],
    *,
    field: str,
    suppressed: bool,
    maximum: int,
) -> tuple[int, set[str] | None] | None:
    values = acquisition.get(field)
    if not isinstance(values, list) or len(values) > maximum:
        return None
    if suppressed:
        retained_count = _bounded_count(
            acquisition.get(f"{field}_count"),
            maximum=maximum,
        )
        if values or retained_count is None:
            return None
        return retained_count, None
    references = field.startswith("source_references_")
    if any(
        not isinstance(value, str)
        or (
            references
            and not _valid_source_reference(value)
        )
        or (not references and _SAFE_IDENTIFIER.fullmatch(value) is None)
        for value in values
    ) or len(values) != len(set(values)):
        return None
    return len(values), set(values)


def _exact_attempt_projection(
    acquisition: dict[str, Any],
    *,
    suppressed: bool,
) -> tuple[int, dict[str, int]] | None:
    attempts = acquisition.get("exact_reference_attempts")
    if not isinstance(attempts, list) or len(attempts) > 16:
        return None
    if suppressed:
        retained_count = _bounded_count(
            acquisition.get("exact_reference_attempts_count"),
            maximum=16,
        )
        if attempts or retained_count is None:
            return None
        attempt_count = retained_count
        observed_outcomes = None
    else:
        normalized: list[tuple[str, str, str]] = []
        for attempt in attempts:
            if (
                not isinstance(attempt, dict)
                or set(attempt) != {"source_id", "source_ref", "outcome"}
                or not isinstance(attempt.get("source_id"), str)
                or _SAFE_IDENTIFIER.fullmatch(attempt["source_id"]) is None
                or not _valid_source_reference(attempt.get("source_ref"))
                or attempt.get("outcome")
                not in {"satisfied", "unknown", "failed", "filtered", "truncated"}
            ):
                return None
            normalized.append(
                (
                    str(attempt["source_id"]),
                    str(attempt["source_ref"]),
                    str(attempt["outcome"]),
                )
            )
        if len({item[1] for item in normalized}) != len(normalized):
            return None
        attempt_count = len(normalized)
        observed_outcomes = {
            outcome: sum(item[2] == outcome for item in normalized)
            for outcome in ("satisfied", "unknown", "failed", "filtered", "truncated")
        }

    declared_attempt_count = _bounded_count(
        acquisition.get("exact_reference_attempt_count"),
        maximum=16,
    )
    outcome_counts = {
        outcome: _bounded_count(
            acquisition.get(f"exact_reference_{field}_count"),
            maximum=16,
        )
        for outcome, field in (
            ("satisfied", "successful"),
            ("unknown", "unknown"),
            ("failed", "failed"),
            ("filtered", "filtered"),
            ("truncated", "truncated"),
        )
    }
    if (
        declared_attempt_count is None
        or any(value is None for value in outcome_counts.values())
        or declared_attempt_count != attempt_count
        or sum(int(value) for value in outcome_counts.values()) != attempt_count
        or (
            observed_outcomes is not None
            and any(
                outcome_counts[outcome] != count
                for outcome, count in observed_outcomes.items()
            )
        )
    ):
        return None
    return attempt_count, {
        outcome: int(value) for outcome, value in outcome_counts.items()
    }


def _expansion_attempt_projection(
    acquisition: dict[str, Any],
    *,
    suppressed: bool,
) -> tuple[int, dict[str, int]] | None:
    attempts = acquisition.get("expansion_attempts")
    if not isinstance(attempts, list) or len(attempts) > 16:
        return None
    outcomes = ("satisfied", "unknown", "failed", "filtered", "truncated", "unsupported")
    if suppressed:
        retained_count = _bounded_count(
            acquisition.get("expansion_attempts_count"), maximum=16
        )
        if attempts or retained_count is None:
            return None
        attempt_count = retained_count
        observed = None
    else:
        observed = {outcome: 0 for outcome in outcomes}
        seen: set[tuple[str, str]] = set()
        for attempt in attempts:
            if not isinstance(attempt, dict) or set(attempt) != {
                "source_id",
                "seed_source_ref",
                "context_mode",
                "outcome",
                "returned_reference_count",
            }:
                return None
            source_id = attempt.get("source_id")
            source_ref = attempt.get("seed_source_ref")
            mode = attempt.get("context_mode")
            outcome = attempt.get("outcome")
            returned = _bounded_count(attempt.get("returned_reference_count"), maximum=64)
            if (
                not isinstance(source_id, str)
                or _SAFE_IDENTIFIER.fullmatch(source_id) is None
                or (
                    source_ref is not None
                    and not _valid_source_reference(source_ref)
                )
                or (
                    mode is not None
                    and (
                        not isinstance(mode, str)
                        or not mode
                        or len(mode) > 80
                    )
                )
                or outcome not in outcomes
                or returned is None
                or (
                    outcome == "satisfied"
                    and (
                        not isinstance(source_ref, str)
                        or not isinstance(mode, str)
                        or returned == 0
                    )
                )
                or (
                    isinstance(source_ref, str)
                    and (source_id, source_ref) in seen
                )
            ):
                return None
            if isinstance(source_ref, str):
                seen.add((source_id, source_ref))
            observed[outcome] += 1
        attempt_count = len(attempts)

    declared_count = _bounded_count(acquisition.get("expansion_attempt_count"), maximum=16)
    declared_outcomes = {
        outcome: _bounded_count(
            acquisition.get(
                f"expansion_{outcome if outcome != 'satisfied' else 'successful'}_count"
            ),
            maximum=16,
        )
        for outcome in outcomes
    }
    if (
        declared_count is None
        or any(value is None for value in declared_outcomes.values())
        or declared_count != attempt_count
        or sum(int(value) for value in declared_outcomes.values()) != attempt_count
        or (
            observed is not None
            and any(declared_outcomes[key] != value for key, value in observed.items())
        )
    ):
        return None
    return attempt_count, {key: int(value) for key, value in declared_outcomes.items()}


def _next_step_selection_is_consistent(
    selection: dict[str, Any],
    *,
    task_shape: str,
    sufficiency_status: str,
) -> bool:
    step = selection["selected_next_step"]
    conclusion = selection["conclusion_disposition"]
    provider = selection["provider_disposition"]
    guard = selection["reacquisition_guard"]
    target = selection["clarification_target"]
    executed = selection["additional_acquisition_executed"]
    blocked_guard = guard in {
        "unchanged_premise_blocked",
        "premise_already_attempted",
    }
    if step == "perform_additional_acquisition":
        return (
            conclusion == "requested_conclusion_withheld"
            and provider == "blocked"
            and guard == "changed_premise_allowed"
            and target is None
            and executed
        )
    if executed or guard == "changed_premise_allowed":
        return False
    if step == "answer_within_declared_scope":
        return (
            conclusion == "bounded_conclusion_allowed"
            and provider == "allowed"
            and guard == "not_applicable"
            and target is None
        )
    if step == "provide_qualified_partial_answer":
        return (
            conclusion == "qualified_partial_only"
            and provider == "allowed"
            and (guard == "not_applicable" or blocked_guard)
            and target is None
        )
    if step == "ask_narrow_clarification":
        return (
            conclusion == "requested_conclusion_withheld"
            and provider == "blocked"
            and guard == "not_applicable"
            and target is not None
        )
    if step == "disclose_unexamined_scope":
        return (
            conclusion == "requested_conclusion_withheld"
            and provider == "blocked"
            and (guard == "not_applicable" or blocked_guard)
            and target is None
        )
    if step != "withhold_unsupported_conclusion":
        return False
    if (
        conclusion != "requested_conclusion_withheld"
        or target is not None
        or (guard != "not_applicable" and not blocked_guard)
    ):
        return False
    if provider == "blocked":
        return True
    reason_codes = selection["reason_codes"]
    return (
        provider == "allowed"
        and task_shape == "targeted_lookup"
        and sufficiency_status in {"insufficient", "unknown"}
        and "unsupported_conclusion_withheld" in reason_codes
        and (
            guard != "unchanged_premise_blocked"
            or "unchanged_acquisition_premise" in reason_codes
        )
        and (
            guard != "premise_already_attempted"
            or "acquisition_premise_already_selected" in reason_codes
        )
    )


def _diagnose_acquisition_history_projection(
    manifest: Any,
) -> AcquisitionHistoryProjection:
    def reject(reason: str) -> AcquisitionHistoryProjection:
        return AcquisitionHistoryProjection(history=None, reason=reason)

    if not isinstance(manifest, dict):
        return reject("manifest_not_object")
    if frozenset(manifest) not in {
        frozenset(
            {
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
        ),
        frozenset(
            {
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
        ),
    }:
        return reject("manifest_top_level_keys_invalid")
    accepted_statuses = {
        "sufficient_for_declared_scope",
        "sufficient_with_limitations",
        "insufficient",
        "unknown",
    }
    plan = manifest.get("plan")
    sufficiency = manifest.get("sufficiency")
    shape = manifest.get("shape")
    inventory = manifest.get("inventory")
    acquisition = manifest.get("acquisition")
    status = manifest.get("status")
    ordinary_context = status == "not_applicable"
    if manifest.get("enabled") is not True:
        return reject("manifest_enabled_invalid")
    if manifest.get("attempted") is not True:
        return reject("manifest_attempted_invalid")
    if (
        not isinstance(manifest.get("manifest_id"), str)
        or _SAFE_IDENTIFIER.fullmatch(manifest["manifest_id"]) is None
    ):
        return reject("manifest_id_invalid")
    if (
        not isinstance(manifest.get("assistant_message_id"), str)
        or _SAFE_IDENTIFIER.fullmatch(manifest["assistant_message_id"]) is None
    ):
        return reject("assistant_message_id_invalid")
    if (
        not isinstance(manifest.get("response_digest"), str)
        or _RESPONSE_DIGEST_RE.fullmatch(manifest["response_digest"]) is None
    ):
        return reject("response_digest_invalid")
    if status not in accepted_statuses and not ordinary_context:
        return reject("manifest_status_invalid")
    if not isinstance(plan, dict):
        return reject("plan_missing")
    if ordinary_context:
        if plan != {
            "plan_id": None,
            "plan_status": "not_compiled",
            "completeness_expectation": None,
            "contradiction_search_required": False,
            "selected_strategies": [],
            "material_requirement_count": 0,
            "optional_requirement_count": 0,
            "limitation_codes": [],
        }:
            return reject("ordinary_plan_invalid")
    elif plan.get("plan_status") not in {"ready", "ready_with_limitations"}:
        return reject("plan_status_invalid")
    if not isinstance(sufficiency, dict):
        return reject("sufficiency_missing")
    if ordinary_context:
        if sufficiency != {
            "evaluation_id": None,
            "status": "not_evaluated",
            "reason_codes": [],
            "answer_constraints": [],
            "qualification_required": False,
            "additional_acquisition_required": False,
        }:
            return reject("ordinary_sufficiency_invalid")
    elif sufficiency.get("status") not in accepted_statuses:
        return reject("sufficiency_status_invalid")
    if not ordinary_context and sufficiency.get("status") != status:
        return reject("manifest_sufficiency_status_mismatch")
    if not isinstance(shape, dict):
        return reject("shape_missing")
    if ordinary_context:
        reason_codes = shape.get("reason_codes")
        if (
            set(shape)
            != {
                "derivation_status",
                "task_shape",
                "candidate_count",
                "clarification_required",
                "reason_codes",
            }
            or shape.get("derivation_status") != "not_applicable"
            or shape.get("task_shape") is not None
            or shape.get("candidate_count") != 0
            or shape.get("clarification_required") is not False
            or not isinstance(reason_codes, list)
            or not reason_codes
            or len(reason_codes) > 17
            or any(code not in _SHAPE_REASON_CODES for code in reason_codes)
            or len(reason_codes) != len(set(reason_codes))
        ):
            return reject("ordinary_shape_invalid")
    elif shape.get("derivation_status") != "derived":
        return reject("shape_derivation_status_invalid")
    if not ordinary_context and shape.get("task_shape") not in {
        "targeted_lookup",
        "cross_source_comparison",
        "bounded_exhaustive_review",
    }:
        return reject("task_shape_invalid")
    if shape.get("clarification_required") is not False:
        return reject("clarification_required_invalid")
    if not isinstance(inventory, dict):
        return reject("inventory_missing")
    if not isinstance(acquisition, dict):
        return reject("acquisition_missing")

    task_shape = (
        "ordinary_context_augmentation"
        if ordinary_context
        else shape["task_shape"]
    )
    selected_strategies = plan.get("selected_strategies")
    strategy = (
        "context_augmentation"
        if ordinary_context
        else acquisition.get("strategy_attempted")
    )
    if ordinary_context:
        if acquisition.get("strategy_attempted") is not None:
            return reject("ordinary_strategy_invalid")
    elif (
        not isinstance(selected_strategies, list)
        or len(selected_strategies) != 1
        or selected_strategies[0]
        not in {"targeted_retrieval", "exact_fetch", "hybrid"}
    ):
        return reject("selected_strategies_invalid")
    if not ordinary_context and strategy != selected_strategies[0]:
        return reject("strategy_mismatch")
    if not isinstance(plan.get("contradiction_search_required"), bool):
        return reject("contradiction_search_flag_invalid")
    expected_composition = {
        ("targeted_lookup", "targeted_retrieval"): "targeted_scope",
        ("targeted_lookup", "exact_fetch"): "targeted_scope",
        ("cross_source_comparison", "hybrid"): "complete_for_selected_sources",
        ("bounded_exhaustive_review", "hybrid"): "complete_for_declared_scope",
    }
    if (
        not ordinary_context
        and plan.get("completeness_expectation")
        != expected_composition.get((task_shape, strategy))
    ):
        return reject("completeness_expectation_mismatch")

    limitation_codes = plan.get("limitation_codes")
    if (
        not isinstance(limitation_codes, list)
        or len(limitation_codes) > 16
        or any(code not in _PLAN_LIMITATION_CODES for code in limitation_codes)
        or len(limitation_codes) != len(set(limitation_codes))
    ):
        return reject("limitation_codes_invalid")
    inventory_status = inventory.get("inventory_status")
    if inventory_status not in {
        "complete_for_declared_scope",
        "partial",
        "unknown",
        "unavailable",
    }:
        return reject("inventory_status_invalid")

    inventory_counts: dict[str, int] = {}
    for field in (
        "inventory_source_count",
        "declared_source_count",
        "declared_category_count",
        "available_source_count",
        "unavailable_source_count",
        "disabled_source_count",
        "unknown_source_count",
    ):
        count = _bounded_count(inventory.get(field), maximum=64)
        if count is None:
            return reject(f"inventory_count_invalid_{field}")
        inventory_counts[field] = count

    suppressed = acquisition.get("source_identifiers_suppressed", False)
    if not isinstance(suppressed, bool):
        return reject("source_identifiers_suppressed_invalid")
    identity_values: dict[str, tuple[int, set[str] | None]] = {}
    for field in _MANIFEST_IDENTITY_FIELDS:
        projected = _identity_projection(
            acquisition,
            field=field,
            suppressed=suppressed,
            maximum=64,
        )
        if projected is None:
            return reject(f"identity_projection_invalid_{field}")
        identity_values[field] = projected

    attempts = _exact_attempt_projection(acquisition, suppressed=suppressed)
    expansions = _expansion_attempt_projection(acquisition, suppressed=suppressed)
    if attempts is None:
        return reject("exact_attempt_projection_invalid")
    if expansions is None:
        return reject("expansion_attempt_projection_invalid")
    exact_attempt_count, exact_outcomes = attempts
    expansion_attempt_count, expansion_outcomes = expansions

    considered_count, considered_values = identity_values["sources_considered"]
    selected_count, selected_values = identity_values["sources_selected"]
    used_count, used_values = identity_values["sources_used"]
    returned_count, returned_values = identity_values[
        "source_references_returned"
    ]
    retained_count, retained_values = identity_values[
        "source_references_retained"
    ]
    omitted_count, omitted_values = identity_values[
        "source_references_filtered_or_omitted"
    ]
    attempted_count, attempted_values = identity_values[
        "source_references_attempted"
    ]
    unsuccessful_count, unsuccessful_values = identity_values[
        "source_references_unsuccessful"
    ]
    if selected_count > considered_count:
        return reject("selected_count_exceeds_considered")
    if used_count > selected_count:
        return reject("used_count_exceeds_selected")
    if retained_count > returned_count:
        return reject("retained_count_exceeds_returned")
    if omitted_count != returned_count - retained_count:
        return reject("omitted_count_mismatch")
    if not (
        exact_attempt_count
        <= attempted_count
        <= exact_attempt_count + expansion_attempt_count
    ):
        return reject("attempted_reference_count_out_of_bounds")
    if not (
        exact_attempt_count - exact_outcomes["satisfied"]
        <= unsuccessful_count
        <= (
            exact_attempt_count
            - exact_outcomes["satisfied"]
            + expansion_attempt_count
            - expansion_outcomes["satisfied"]
        )
    ):
        return reject("unsuccessful_reference_count_out_of_bounds")
    if not suppressed and not selected_values.issubset(considered_values):
        return reject("selected_sources_not_subset_of_considered")
    if not suppressed and not used_values.issubset(selected_values):
        return reject("used_sources_not_subset_of_selected")
    if not suppressed and not retained_values.issubset(returned_values):
        return reject("retained_references_not_subset_of_returned")
    if not suppressed and omitted_values != returned_values - retained_values:
        return reject("omitted_reference_set_mismatch")
    if (
        not suppressed
        and strategy == "exact_fetch"
        and unsuccessful_values != attempted_values - returned_values
    ):
        return reject("exact_unsuccessful_reference_set_mismatch")

    source_summaries = _project_source_summaries(
        acquisition,
        suppressed=suppressed,
        considered=considered_values,
        selected=selected_values,
        used=used_values,
        returned=returned_values,
        retained=retained_values,
        unavailable=identity_values["unavailable_source_ids"][1],
        failed=identity_values["failed_source_ids"][1],
    )
    if source_summaries is None:
        return reject("source_summaries_invalid")

    acquisition_counts: dict[str, int] = {}
    for field in (
        "item_count",
        "usable_item_count",
        "prompt_retained_item_count",
    ):
        count = _bounded_count(acquisition.get(field), maximum=10000)
        if count is None:
            return reject(f"acquisition_count_invalid_{field}")
        acquisition_counts[field] = count
    if acquisition_counts["usable_item_count"] > acquisition_counts["item_count"]:
        return reject("usable_item_count_exceeds_item_count")
    if (
        acquisition_counts["prompt_retained_item_count"]
        > acquisition_counts["usable_item_count"]
    ):
        return reject("prompt_retained_count_exceeds_usable_count")
    if returned_count != acquisition_counts["usable_item_count"]:
        return reject("returned_reference_count_mismatch")
    if retained_count != acquisition_counts["prompt_retained_item_count"]:
        return reject("retained_reference_count_mismatch")
    if acquisition.get("context_delivery_status") not in {
        "retained",
        "filtered",
        "unknown",
    }:
        return reject("context_delivery_status_invalid")
    if (
        ordinary_context
        and acquisition.get("dsa_outcome") != "not_called"
        and acquisition.get("context_delivery_status") != "retained"
    ):
        return reject("ordinary_delivery_status_invalid")
    if not isinstance(acquisition.get("dsa_budget_truncation"), bool):
        return reject("dsa_budget_truncation_invalid")
    if not isinstance(acquisition.get("candidate_truncation"), bool):
        return reject("candidate_truncation_invalid")
    if not isinstance(sufficiency.get("qualification_required"), bool):
        return reject("qualification_required_invalid")
    if not isinstance(sufficiency.get("additional_acquisition_required"), bool):
        return reject("additional_acquisition_required_invalid")
    no_acquisition = bool(
        ordinary_context and acquisition.get("dsa_outcome") == "not_called"
    )
    if no_acquisition:
        if (
            inventory_status != "unknown"
            or any(inventory_counts.values())
            or suppressed
            or any(acquisition_counts.values())
            or any(count for count, _ in identity_values.values())
            or source_summaries
            or acquisition.get("dsa_error_codes") != []
            or acquisition.get("requirement_facts") != []
            or acquisition.get("context_delivery_status") != "unknown"
            or acquisition.get("dsa_budget_truncation") is not False
            or acquisition.get("candidate_truncation") is not False
            or exact_attempt_count
            or expansion_attempt_count
        ):
            return reject("ordinary_acquisition_invalid")
    elif ordinary_context and (
        acquisition.get("dsa_outcome") != "success"
        or acquisition.get("dsa_error_codes") != []
        or acquisition.get("requirement_facts") != []
        or acquisition.get("context_delivery_status") != "retained"
        or acquisition_counts["prompt_retained_item_count"] == 0
        or exact_attempt_count
        or expansion_attempt_count
        or attempted_count
        or unsuccessful_count
    ):
        return reject("ordinary_acquisition_invalid")
    if strategy == "targeted_retrieval" and (
        exact_attempt_count or expansion_attempt_count
    ):
        return reject("targeted_strategy_attempt_accounting_invalid")
    if strategy == "exact_fetch" and (
        exact_attempt_count == 0 or expansion_attempt_count
    ):
        return reject("exact_strategy_attempt_accounting_invalid")
    if strategy == "hybrid" and (
        exact_attempt_count or expansion_attempt_count == 0
    ):
        return reject("hybrid_strategy_attempt_accounting_invalid")

    next_steps = manifest.get("next_steps")
    if ordinary_context and next_steps is not None and next_steps != {
        "selection_count": 0,
        "selections": [],
        "additional_acquisition_count": 0,
        "initial_attempt": None,
        "dependency_status": None,
    }:
        return reject("ordinary_next_steps_invalid")
    changed_follow_up = False
    final_next_step = None
    if next_steps is not None:
        if not isinstance(next_steps, dict) or set(next_steps) != {
            "selection_count",
            "selections",
            "additional_acquisition_count",
            "initial_attempt",
            "dependency_status",
        }:
            return reject("next_steps_shape_invalid")
        selection_count = _bounded_count(next_steps.get("selection_count"), maximum=2)
        additional_count = _bounded_count(
            next_steps.get("additional_acquisition_count"), maximum=1
        )
        selections = next_steps.get("selections")
        if selection_count is None or additional_count is None:
            return reject("next_step_selection_count_invalid")
        if (
            not isinstance(selections, list)
            or len(selections) != selection_count
            or len(selections) > 2
        ):
            return reject("next_step_selections_invalid")
        allowed_steps = {
            "answer_within_declared_scope",
            "provide_qualified_partial_answer",
            "perform_additional_acquisition",
            "ask_narrow_clarification",
            "disclose_unexamined_scope",
            "withhold_unsupported_conclusion",
        }
        allowed_conclusions = {
            "bounded_conclusion_allowed",
            "qualified_partial_only",
            "requested_conclusion_withheld",
        }
        allowed_providers = {"allowed", "blocked"}
        allowed_guards = {
            "not_applicable",
            "changed_premise_allowed",
            "unchanged_premise_blocked",
            "premise_already_attempted",
        }
        allowed_targets = {
            None,
            "question_scope",
            "source_scope",
            "exact_reference",
            "time_scope",
            "version_scope",
            "domain_scope",
            "project_scope",
        }
        for selection in selections:
            expected_fields = {
                "selection_id",
                "evaluation_id",
                "evidence_plan_id",
                "acquisition_manifest_id",
                "selected_next_step",
                "conclusion_disposition",
                "provider_disposition",
                "reacquisition_guard",
                "clarification_target",
                "reason_codes",
                "additional_acquisition_executed",
            }
            if not isinstance(selection, dict) or set(selection) != expected_fields:
                return reject("next_step_selection_fields_invalid")
            if selection.get("selected_next_step") not in allowed_steps:
                return reject("next_step_selection_enum_invalid")
            if any(
                not isinstance(selection.get(field), str)
                or _SAFE_IDENTIFIER.fullmatch(selection[field]) is None
                for field in (
                    "selection_id",
                    "evaluation_id",
                    "evidence_plan_id",
                    "acquisition_manifest_id",
                )
            ):
                return reject("next_step_selection_identifier_invalid")
            if (
                selection.get("conclusion_disposition") not in allowed_conclusions
                or selection.get("provider_disposition") not in allowed_providers
                or selection.get("reacquisition_guard") not in allowed_guards
                or selection.get("clarification_target") not in allowed_targets
            ):
                return reject("next_step_selection_enum_invalid")
            reason_codes = selection.get("reason_codes")
            if (
                not isinstance(reason_codes, list)
                or len(reason_codes) > 16
                or any(
                    not isinstance(code, str) or not code or len(code) > 120
                    for code in reason_codes
                )
                or len(reason_codes) != len(set(reason_codes))
                or reason_codes != sorted(reason_codes)
            ):
                return reject("next_step_reason_codes_invalid")
            if not isinstance(
                selection.get("additional_acquisition_executed"), bool
            ):
                return reject("next_step_selection_enum_invalid")
            if not _next_step_selection_is_consistent(
                selection,
                task_shape=task_shape,
                sufficiency_status=sufficiency["status"],
            ):
                return reject("next_step_selection_consistency_invalid")
        final_next_step = (
            selections[-1]["selected_next_step"] if selections else None
        )
        initial_attempt = next_steps.get("initial_attempt")
        if initial_attempt is not None:
            if not isinstance(initial_attempt, dict) or set(initial_attempt) != {
                "strategy",
                "sufficiency_status",
                "result_count",
                "retained_reference_count",
                "changed_premise_exact_fetch_followed",
            }:
                return reject("initial_attempt_shape_invalid")
            if initial_attempt.get("strategy") != "targeted_retrieval":
                return reject("initial_attempt_strategy_invalid")
            if initial_attempt.get("sufficiency_status") not in accepted_statuses:
                return reject("initial_attempt_status_invalid")
            if (
                _bounded_count(initial_attempt.get("result_count")) is None
                or _bounded_count(initial_attempt.get("retained_reference_count"))
                is None
            ):
                return reject("initial_attempt_count_invalid")
            if initial_attempt.get("changed_premise_exact_fetch_followed") is not True:
                return reject("initial_attempt_followup_flag_invalid")
        changed_follow_up = bool(
            additional_count == 1
            and strategy == "exact_fetch"
            and isinstance(initial_attempt, dict)
            and initial_attempt.get("strategy") == "targeted_retrieval"
            and any(
                selection.get("selected_next_step") == "perform_additional_acquisition"
                and selection.get("additional_acquisition_executed") is True
                for selection in selections
            )
        )
        executed_count = sum(
            selection["additional_acquisition_executed"] for selection in selections
        )
        if next_steps.get("dependency_status") not in {None, "dependency_failure"}:
            return reject("next_step_dependency_status_invalid")
        if executed_count != additional_count:
            return reject("next_step_execution_count_mismatch")
        if additional_count == 0 and initial_attempt is not None:
            return reject("unexpected_initial_attempt")
        if additional_count == 1 and not changed_follow_up:
            return reject("changed_followup_inconsistent")

    counts = {
        **inventory_counts,
        "sources_considered": considered_count,
        "sources_selected": selected_count,
        "sources_used": used_count,
        "references_returned": returned_count,
        "references_retained": retained_count,
        "references_omitted": omitted_count,
        "exact_attempts": exact_attempt_count,
        "exact_successful": exact_outcomes["satisfied"],
        "exact_unknown": exact_outcomes["unknown"],
        "exact_failed": exact_outcomes["failed"],
        "exact_filtered": exact_outcomes["filtered"],
        "exact_truncated": exact_outcomes["truncated"],
        "exact_unsuccessful": unsuccessful_count,
        "expansion_attempts": expansion_attempt_count,
        "expansion_successful": expansion_outcomes["satisfied"],
        "expansion_unknown": expansion_outcomes["unknown"],
        "expansion_failed": expansion_outcomes["failed"],
        "expansion_filtered": expansion_outcomes["filtered"],
        "expansion_truncated": expansion_outcomes["truncated"],
        "expansion_unsupported": expansion_outcomes["unsupported"],
        "unavailable_sources": identity_values["unavailable_source_ids"][0],
        "failed_sources": identity_values["failed_source_ids"][0],
        **acquisition_counts,
    }
    if no_acquisition:
        task_shape = "no_acquisition"
        strategy = "none"
    history = AcquisitionHistory(
        task_shape=task_shape,
        strategy=strategy,
        sufficiency_status=("not_evaluated" if ordinary_context else status),
        inventory_status=inventory_status,
        counts=counts,
        limitation_codes=tuple(limitation_codes),
        budget_truncated=acquisition["dsa_budget_truncation"],
        candidate_truncated=acquisition["candidate_truncation"],
        qualification_required=sufficiency["qualification_required"],
        additional_acquisition_required=sufficiency[
            "additional_acquisition_required"
        ],
        identifiers_suppressed=suppressed,
        changed_premise_exact_follow_up=changed_follow_up,
        final_next_step=final_next_step,
        source_summaries=source_summaries,
    )
    return AcquisitionHistoryProjection(history=history, reason="accepted")


def _project_acquisition_history(manifest: Any) -> AcquisitionHistory | None:
    return _diagnose_acquisition_history_projection(manifest).history


def _count_phrase(count: int, singular: str, plural: str | None = None) -> str:
    return f"{count} {singular if count == 1 else plural or singular + 's'}"


def _requested_source_check_completed(history: AcquisitionHistory) -> bool:
    counts = history.counts
    return (
        history.task_shape == "bounded_exhaustive_review"
        and history.strategy == "hybrid"
        and counts["expansion_attempts"] > 0
        and counts["expansion_successful"] == counts["expansion_attempts"]
        and counts["expansion_truncated"] == 0
        and counts["references_retained"] > 0
    )


def _limitation_sentences(
    history: AcquisitionHistory,
    *,
    include_unknown_inventory: bool = True,
) -> list[str]:
    counts = history.counts
    sentences = []
    for field, singular in (
        ("unavailable_source_count", "source was unavailable"),
        ("disabled_source_count", "source was disabled"),
        ("unknown_source_count", "source had unknown availability"),
    ):
        count = counts[field]
        if count:
            plural = singular.replace("source was", "sources were").replace(
                "source had",
                "sources had",
            )
            sentences.append(f"{_count_phrase(count, singular, plural)}.")
    if counts["references_omitted"]:
        omitted = _count_phrase(
            counts["references_omitted"],
            "returned reference was",
            "returned references were",
        )
        sentences.append(f"{omitted} not used in the earlier answer.")
    for field, singular, plural in (
        (
            "exact_failed",
            "requested item could not be retrieved",
            "requested items could not be retrieved",
        ),
        (
            "exact_unknown",
            "requested item returned no result",
            "requested items returned no result",
        ),
        (
            "exact_filtered",
            "requested item was not used",
            "requested items were not used",
        ),
        ("exact_truncated", "requested item was truncated", "requested items were truncated"),
    ):
        count = counts[field]
        if count:
            sentences.append(f"{_count_phrase(count, singular, plural)}.")
    for field, singular, plural in (
        ("expansion_failed", "broader source check failed", "broader source checks failed"),
        (
            "expansion_unknown",
            "broader source check had an unknown outcome",
            "broader source checks had unknown outcomes",
        ),
        (
            "expansion_filtered",
            "broader source check was not used",
            "broader source checks were not used",
        ),
        (
            "expansion_truncated",
            "broader source check was truncated",
            "broader source checks were truncated",
        ),
        (
            "expansion_unsupported",
            "broader source check was unavailable",
            "broader source checks were unavailable",
        ),
    ):
        count = counts[field]
        if count:
            sentences.append(f"{_count_phrase(count, singular, plural)}.")
    if history.inventory_status == "partial":
        sentences.append("The available source list was incomplete.")
    elif history.inventory_status == "unknown" and include_unknown_inventory:
        sentences.append("It was not known whether the available source list was complete.")
    elif history.inventory_status == "unavailable":
        sentences.append("The source list was unavailable.")
    configured_scope_expansion_completed = _requested_source_check_completed(history)
    if history.budget_truncated:
        sentences.append(
            "The preliminary search was truncated, but the complete requested-source "
            "check finished without truncation."
            if configured_scope_expansion_completed
            else "The lookup was truncated by its result limit."
        )
    if history.candidate_truncated:
        sentences.append(
            "The preliminary candidate list was truncated."
            if configured_scope_expansion_completed
            else "The candidate list was truncated."
        )
    if (
        "optional_source_unavailable" in history.limitation_codes
        and not counts["unavailable_source_count"]
    ):
        sentences.append("An optional source was unavailable.")
    return sentences


def _source_was_checked(source: SourceHistorySummary) -> bool:
    reasons = set(source.contribution_reason_codes)
    return bool(
        source.selected
        or source.used
        or source.returned_reference_count
        or source.retained_reference_count
        or "exact_reference_retrieved" in reasons
    )


def _source_summary_lines(
    history: AcquisitionHistory,
    *,
    view: Literal["checked", "not_covered", "gaps"],
) -> list[str]:
    lines = []
    for source in history.source_summaries:
        checked = _source_was_checked(source)
        reasons = set(source.contribution_reason_codes)
        gap_reasons = {
            "returned_records_not_retained",
            "selected_no_result",
            "considered_not_selected",
            "source_unavailable",
            "source_disabled",
        }
        not_covered_reasons = {
            "considered_not_selected",
            "source_unavailable",
            "source_disabled",
        }
        if view == "checked" and not checked:
            continue
        if view == "not_covered" and (checked or not reasons & not_covered_reasons):
            continue
        if view == "gaps" and not reasons & gap_reasons:
            continue
        location = (
            f" — {'; '.join(source.safe_location_labels)}"
            if view == "checked" and source.safe_location_labels
            else ""
        )
        details = []
        if view == "checked" and source.retained_reference_count:
            details.append(
                f"contributed {_count_phrase(source.retained_reference_count, 'record')} "
                "used in the earlier answer"
            )
        if "returned_records_not_retained" in reasons:
            omitted = source.returned_reference_count - source.retained_reference_count
            if omitted:
                details.append(
                    f"returned {_count_phrase(omitted, 'additional record')} that was not used"
                )
        if "selected_no_result" in reasons:
            details.append("was checked but returned no records")
        if "considered_not_selected" in reasons:
            details.append("was considered but not selected for the lookup")
        if view == "checked" and "exact_reference_retrieved" in reasons:
            details.append("included the exact requested reference")
        if "source_unavailable" in reasons:
            details.append("was unavailable during the original lookup")
        if "source_disabled" in reasons:
            details.append("was disabled during the original lookup")
        if not details:
            details.append(
                "was checked during the original lookup"
                if checked
                else "was outside the sources checked in the original lookup"
            )
        lines.append(f"- {source.display_name}{location}: {'; '.join(details)}.")
    return lines


def _render_acquisition(
    history: AcquisitionHistory,
    question: Literal["checked", "coverage", "gaps"],
    *,
    include_no_new_verification: bool = True,
) -> str:
    if history.task_shape == "no_acquisition":
        opening = {
            "checked": "I didn’t run an evidence acquisition for the original answer.",
            "coverage": (
                "No evidence acquisition was run for the original answer, so there "
                "is no checked source scope to describe as complete."
            ),
            "gaps": (
                "No evidence acquisition was run for the original answer, so there "
                "is no checked source set whose gaps I can enumerate."
            ),
        }[question]
        return (
            f"{opening}\n\n{_NO_NEW_VERIFICATION}"
            if include_no_new_verification
            else opening
        )

    counts = history.counts
    exhaustive = history.task_shape == "bounded_exhaustive_review"
    complete_scope = (
        exhaustive
        and history.sufficiency_status == "sufficient_for_declared_scope"
        and (
            (not history.budget_truncated and not history.candidate_truncated)
            or _requested_source_check_completed(history)
        )
    )
    if question == "coverage":
        opening = (
            "Yes—within the requested source set, but not beyond it."
            if complete_scope
            else "No. The original lookup did not cover every possible source."
        )
    elif question == "gaps":
        opening = "Known gaps from the original lookup:"
    else:
        opening = "I checked:"

    lines = [opening]
    if history.identifiers_suppressed:
        lines.append(
            "The saved explanation covers "
            f"{_count_phrase(counts['sources_considered'], 'source')}, "
            "but it does not include source names or locations."
        )
        if counts["references_retained"]:
            verb = "was" if counts["references_retained"] == 1 else "were"
            lines.append(
                f"{_count_phrase(counts['references_retained'], 'record')} "
                f"{verb} used in the earlier answer."
            )
    else:
        if question == "checked":
            lines.extend(_source_summary_lines(history, view="checked"))
        elif question == "coverage":
            checked_lines = _source_summary_lines(history, view="checked")
            if checked_lines:
                lines.append("Checked:")
                lines.extend(checked_lines)
            not_covered_lines = _source_summary_lines(history, view="not_covered")
            if not_covered_lines:
                lines.append("Not covered:")
                lines.extend(not_covered_lines)
        else:
            lines.extend(_source_summary_lines(history, view="gaps"))

    if len(lines) > 1:
        lines.append("")

    if history.task_shape == "ordinary_context_augmentation":
        boundary = (
            "This was a normal source lookup for the earlier answer, not a complete "
            "review of every possible source."
        )
    elif history.task_shape == "targeted_lookup" and history.strategy == "exact_fetch":
        boundary = (
            "Only the specifically requested records were checked; other sources were "
            "outside the original request."
        )
    elif history.task_shape == "targeted_lookup":
        boundary = (
            "This was limited to the targeted sources, not every potentially relevant "
            "source."
        )
    elif history.task_shape == "cross_source_comparison":
        boundary = (
            "The comparison covered the selected sources only, not every possible source."
        )
    elif exhaustive:
        boundary = (
            "Coverage applies only to the requested source set, not to sources outside it."
        )
    else:
        raise ValueError("unsupported_acquisition_history_composition")

    limitations = _limitation_sentences(
        history,
        include_unknown_inventory=not (
            question == "checked"
            and history.task_shape == "ordinary_context_augmentation"
        ),
    )
    if question == "gaps" and not limitations:
        lines.append(f"- {boundary}")
    else:
        lines.extend(limitations)
        lines.append(boundary)
    if history.sufficiency_status == "sufficient_with_limitations":
        lines.append("The earlier answer was usable only with those limits.")
    elif history.sufficiency_status in {"insufficient", "unknown"}:
        lines.append("The original lookup did not establish the requested conclusion.")
    if history.changed_premise_exact_follow_up:
        lines.append(
            "The original request included one additional exact check after its first lookup."
        )
    if include_no_new_verification:
        lines.append(_NO_NEW_VERIFICATION)
    return "\n".join(lines)


def _acquisition_resolution_trace(
    *,
    intent: ClaimExplanationIntent,
    resolution_status: str,
    status: str,
    manifest_projection_status: str = "not_attempted",
    manifest_projection_reason: str = "not_attempted",
    counts: dict[str, int] | None = None,
    identifiers_suppressed: bool = False,
) -> dict[str, Any]:
    reason_code = {
        "resolved": (
            "latest_acquisition_record_resolved"
            if intent.mode == "latest"
            else "quoted_acquisition_record_resolved"
        ),
        "no_record": "acquisition_record_not_found",
        "ambiguous": "acquisition_record_ambiguous",
        "invalid": "acquisition_manifest_invalid",
        "unavailable": "acquisition_resolver_unavailable",
    }.get(resolution_status, "acquisition_resolution_failed")
    return {
        "enabled": True,
        "intent_status": "matched",
        "explanation_kind": "acquisition",
        "acquisition_question": intent.acquisition_question,
        "target_mode": (
            "immediate_previous"
            if intent.mode == "latest"
            else "quoted_first_paragraph"
        ),
        "compound_mode": intent.new_verification_requested,
        "historical_only": not intent.new_verification_requested,
        "lookup_status": status,
        "resolution_status": resolution_status,
        "reason_code": reason_code,
        "claim_record_lookup_status": "not_requested",
        "acquisition_trace_lookup_status": "not_requested",
        "manifest_resolution_status": resolution_status,
        "manifest_projection_status": manifest_projection_status,
        "manifest_projection_reason": manifest_projection_reason,
        "storage_call_count": 1,
        "provider_call_count": 0,
        "aggregate_counts": counts or {},
        "privacy_suppression_applied": identifiers_suppressed,
    }


def _without_no_new_verification(answer: str) -> str:
    return answer.replace(f" {_NO_NEW_VERIFICATION}", "").replace(
        _NO_NEW_VERIFICATION, ""
    ).strip()


async def _resolve_acquisition_explanation(
    *,
    intent: ClaimExplanationIntent,
    messages: Any,
    memory_store: Any,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
) -> ClaimExplanationOutcome:
    manifest_projection_status = "not_attempted"
    manifest_projection_reason = "not_attempted"
    prior_response: str | None = None
    if intent.mode == "latest":
        prior_response = _prior_assistant(messages, intent)
        target = _normalized_first_response_paragraph(prior_response)
        if prior_response is None or target is None:
            return _fallback(
                _ACQUISITION_TARGET_UNAVAILABLE,
                "prior_assistant_unavailable",
                explanation_kind="acquisition",
                target_mode="immediate_previous",
            )
        target_mode = "immediate_previous"
        response_digest = _response_digest(prior_response)
    else:
        target = intent.target_anchor
        if target is None:
            return _fallback(
                _ACQUISITION_TARGET_UNAVAILABLE,
                "quoted_target_invalid",
                explanation_kind="acquisition",
                target_mode="quoted_first_paragraph",
            )
        target_mode = "quoted_first_paragraph"
        response_digest = None

    try:
        payload = await memory_store.resolve_acquisition_history(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            target_mode=target_mode,
            normalized_first_paragraph=target,
            response_digest=response_digest,
        )
        response = AcquisitionHistoryResolveResponse.model_validate(payload)
    except Exception:
        answer = _ACQUISITION_RESOLUTION_UNAVAILABLE
        if intent.new_verification_requested:
            answer = _without_no_new_verification(answer)
        return ClaimExplanationOutcome(
            handled=True,
            answer=answer,
            status="degraded",
            trace=_acquisition_resolution_trace(
                intent=intent,
                resolution_status="unavailable",
                status="failed",
            ),
            new_verification_requested=intent.new_verification_requested,
            verification_target=(target if intent.new_verification_requested else None),
        )

    expected_scope = {
        "request_id": request_id,
        "owner_id": owner_id,
        "conversation_id": conversation_id,
        "surface": surface,
        "target_mode": target_mode,
    }
    if any(getattr(response, field) != value for field, value in expected_scope.items()):
        response = None
    if response is not None:
        allowed_reasons = {
            ("immediate_previous", "resolved"): {"immediate_response_resolved"},
            ("immediate_previous", "no_record"): {
                "immediate_response_mismatch",
                "immediate_response_trace_absent",
                "immediate_response_manifest_absent",
            },
            ("immediate_previous", "invalid"): {
                "trace_scope_mismatch",
                "assistant_message_request_mismatch",
                "manifest_association_invalid",
                "manifest_privacy_boundary_invalid",
            },
            ("quoted_first_paragraph", "resolved"): {"quoted_response_resolved"},
            ("quoted_first_paragraph", "no_record"): {
                "quoted_response_not_found",
                "quoted_response_trace_absent",
                "quoted_response_manifest_absent",
            },
            ("quoted_first_paragraph", "ambiguous"): {
                "quoted_response_ambiguous"
            },
            ("quoted_first_paragraph", "invalid"): {
                "trace_scope_mismatch",
                "assistant_message_request_mismatch",
                "manifest_association_invalid",
                "manifest_privacy_boundary_invalid",
            },
        }
        if response.reason_code not in allowed_reasons.get(
            (target_mode, response.resolution_status), set()
        ):
            response = None

    if response is None:
        answer = _ACQUISITION_RESOLUTION_UNAVAILABLE
        resolution_status = "unavailable"
        lookup_status = "invalid"
        history = None
    elif response.resolution_status == "no_record":
        answer = _ACQUISITION_RESOLUTION_NO_RECORD
        resolution_status = "no_record"
        lookup_status = "completed"
        history = None
    elif response.resolution_status == "ambiguous":
        answer = _ACQUISITION_RESOLUTION_AMBIGUOUS
        resolution_status = "ambiguous"
        lookup_status = "completed"
        history = None
    elif response.resolution_status == "invalid":
        answer = _ACQUISITION_RESOLUTION_INVALID
        resolution_status = "invalid"
        lookup_status = "completed"
        history = None
    else:
        record = response.record
        if (
            record is None
            or record.surface != surface
            or record.normalized_first_paragraph != target
            or record.acquisition_manifest.get("assistant_message_id")
            != record.assistant_message_id
            or record.acquisition_manifest.get("response_digest")
            != record.response_digest
            or (
                target_mode == "immediate_previous"
                and record.response_digest != response_digest
            )
        ):
            answer = _ACQUISITION_RESOLUTION_UNAVAILABLE
            resolution_status = "unavailable"
            lookup_status = "invalid"
            history = None
        else:
            projection = _diagnose_acquisition_history_projection(
                record.acquisition_manifest
            )
            history = projection.history
            if history is None:
                manifest_projection_status = "rejected"
                manifest_projection_reason = projection.reason
                answer = _ACQUISITION_RESOLUTION_INVALID
                resolution_status = "invalid"
                lookup_status = "completed"
            else:
                manifest_projection_status = "accepted"
                manifest_projection_reason = "accepted"
                answer = _render_acquisition(
                    history,
                    intent.acquisition_question or "checked",
                    include_no_new_verification=not intent.new_verification_requested,
                )
                resolution_status = "resolved"
                lookup_status = "completed"

    if intent.new_verification_requested and history is None:
        answer = _without_no_new_verification(answer)
    return ClaimExplanationOutcome(
        handled=True,
        answer=answer,
        status="ok" if history is not None else "degraded",
        trace=_acquisition_resolution_trace(
            intent=intent,
            resolution_status=resolution_status,
            status=lookup_status,
            manifest_projection_status=manifest_projection_status,
            manifest_projection_reason=manifest_projection_reason,
            counts=history.counts if history is not None else None,
            identifiers_suppressed=(
                history.identifiers_suppressed if history is not None else False
            ),
        ),
        new_verification_requested=intent.new_verification_requested,
        verification_target=(target if intent.new_verification_requested else None),
    )


def _immediate_history_trace(
    *,
    explanation_kind: str,
    acquisition_question: str | None,
    resolution_status: str,
    reason_code: str,
    render_status: str,
    resolution_source: str = "none",
    lineage_dereference_count: int = 0,
    lineage_result: str = "absent",
    resolved_record_kind: str | None = None,
    manifest_projection_status: str = "not_applicable",
    manifest_projection_reason: str = "not_applicable",
) -> dict[str, Any]:
    return {
        "enabled": True,
        "intent_status": "matched",
        "explanation_kind": explanation_kind,
        "acquisition_question": acquisition_question,
        "target_mode": "immediate_previous",
        "lookup_status": "completed",
        "resolution_status": resolution_status,
        "resolution_source": resolution_source,
        "lineage_dereference_count": lineage_dereference_count,
        "lineage_result": lineage_result,
        "reason_code": reason_code,
        "render_status": render_status,
        "resolved_record_kind": resolved_record_kind,
        "manifest_projection_status": manifest_projection_status,
        "manifest_projection_reason": manifest_projection_reason,
        "storage_call_count": 1,
        "provider_call_count": 0,
        "privacy_suppression_applied": True,
    }


def _immediate_history_fallback_answer(explanation_kind: str, status: str) -> str:
    if explanation_kind == "support":
        return {
            "no_record": _NO_RECORD,
            "ambiguous": _AMBIGUOUS,
            "invalid": _INVALID_RECORD,
            "unavailable": _DEPENDENCY_UNAVAILABLE,
        }.get(status, _DEPENDENCY_UNAVAILABLE)
    return {
        "no_record": _ACQUISITION_RESOLUTION_NO_RECORD,
        "ambiguous": _ACQUISITION_RESOLUTION_AMBIGUOUS,
        "invalid": _ACQUISITION_RESOLUTION_INVALID,
        "unavailable": _ACQUISITION_RESOLUTION_UNAVAILABLE,
    }.get(status, _ACQUISITION_RESOLUTION_UNAVAILABLE)


async def resolve_immediate_claim_explanation(
    *,
    policy: dict[str, Any],
    memory_store: Any,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
) -> ClaimExplanationOutcome:
    explanation_kind = policy["explanation_kind"]
    acquisition_question = policy.get("acquisition_question")
    payload: Any = None
    try:
        payload = await memory_store.resolve_immediate_history(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            explanation_kind=explanation_kind,
        )
        response = ImmediateHistoryResolveResponse.model_validate(payload)
        expected_scope = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "explanation_kind": explanation_kind,
        }
        if any(getattr(response, key) != value for key, value in expected_scope.items()):
            raise ValueError("immediate_history_scope_mismatch")
    except Exception:
        return ClaimExplanationOutcome(
            handled=True,
            answer=_immediate_history_fallback_answer(explanation_kind, "unavailable"),
            status="degraded",
            trace=_immediate_history_trace(
                explanation_kind=explanation_kind,
                acquisition_question=acquisition_question,
                resolution_status="unavailable",
                reason_code="history_store_unavailable",
                render_status="not_attempted",
                lineage_result=(
                    "rejected"
                    if isinstance(payload, dict)
                    and payload.get("history_root_lineage") is not None
                    else "absent"
                ),
            ),
        )

    if response.resolution_status != "resolved":
        lineage_result = (
            "rejected" if response.reason_code.startswith("lineage_") else "absent"
        )
        return ClaimExplanationOutcome(
            handled=True,
            answer=_immediate_history_fallback_answer(
                explanation_kind, response.resolution_status
            ),
            status="degraded",
            trace=_immediate_history_trace(
                explanation_kind=explanation_kind,
                acquisition_question=acquisition_question,
                resolution_status=response.resolution_status,
                reason_code=response.reason_code,
                render_status="not_attempted",
                resolution_source=response.resolution_source,
                lineage_dereference_count=response.lineage_dereference_count,
                lineage_result=lineage_result,
            ),
        )

    record = response.record
    if record is None:
        raise AssertionError("validated resolved immediate history requires a record")
    verification_requested = bool(
        policy.get("new_verification_requested")
        and policy.get("new_verification_allowed_after_history_resolution")
    )
    answer: str | None = None
    target: str | None = None
    manifest_projection_status = "not_applicable"
    manifest_projection_reason = "not_applicable"
    if record.record_kind == "support" and record.support_record is not None:
        support = record.support_record
        if (
            support.request_id == record.original_request_id
            and support.assistant_message_id == record.assistant_message_id
            and support.surface == surface
            and _record_matches_scope(
                support,
                owner_id=owner_id,
                conversation_id=conversation_id,
            )
            and _record_support_status(
                support,
                owner_id=owner_id,
                conversation_id=conversation_id,
            )
            == "supported"
        ):
            answer = _render(support)
            target = support.claim_anchor
    elif record.record_kind == "acquisition" and record.acquisition_record is not None:
        acquisition = record.acquisition_record
        if (
            acquisition.original_request_id == record.original_request_id
            and acquisition.assistant_message_id == record.assistant_message_id
            and acquisition.surface == surface
            and acquisition.acquisition_manifest.get("assistant_message_id")
            == record.assistant_message_id
            and acquisition.acquisition_manifest.get("response_digest")
            == acquisition.response_digest
        ):
            projection = _diagnose_acquisition_history_projection(
                acquisition.acquisition_manifest
            )
            manifest_projection_reason = projection.reason
            if projection.history is not None:
                manifest_projection_status = "accepted"
                answer = _render_acquisition(
                    projection.history,
                    acquisition_question or "checked",
                )
                target = acquisition.normalized_first_paragraph
            else:
                manifest_projection_status = "rejected"

    if answer is None or target is None:
        reason_code = (
            "direct_support_record_invalid"
            if explanation_kind == "support"
            else "direct_acquisition_record_invalid"
        )
        return ClaimExplanationOutcome(
            handled=True,
            answer=_immediate_history_fallback_answer(explanation_kind, "invalid"),
            status="degraded",
            trace=_immediate_history_trace(
                explanation_kind=explanation_kind,
                acquisition_question=acquisition_question,
                resolution_status="invalid",
                reason_code=reason_code,
                render_status="rejected",
                resolution_source=response.resolution_source,
                lineage_dereference_count=response.lineage_dereference_count,
                lineage_result="accepted",
                manifest_projection_status=manifest_projection_status,
                manifest_projection_reason=manifest_projection_reason,
            ),
        )
    if verification_requested:
        answer = _without_no_new_verification(answer)
    return ClaimExplanationOutcome(
        handled=True,
        answer=answer,
        status="ok",
        trace=_immediate_history_trace(
            explanation_kind=explanation_kind,
            acquisition_question=acquisition_question,
            resolution_status="resolved",
            reason_code=response.reason_code,
            render_status="completed",
            resolution_source=response.resolution_source,
            lineage_dereference_count=response.lineage_dereference_count,
            lineage_result="accepted",
            resolved_record_kind=record.record_kind,
            manifest_projection_status=manifest_projection_status,
            manifest_projection_reason=manifest_projection_reason,
        ),
        new_verification_requested=verification_requested,
        verification_target=target if verification_requested else None,
        history_root_lineage=(
            response.history_root_lineage.model_dump(mode="json")
            if not verification_requested and response.history_root_lineage is not None
            else None
        ),
    )


async def resolve_claim_explanation(
    *,
    enabled: bool,
    acquisition_history_enabled: bool | None = None,
    messages: Any,
    memory_store: Any,
    request_id: str = "claim-explanation-request",
    owner_id: str,
    conversation_id: str,
    surface: str | None = None,
) -> ClaimExplanationOutcome:
    if (
        not isinstance(messages, list)
        or not messages
        or not isinstance(messages[-1], dict)
        or messages[-1].get("role") != "user"
        or not isinstance(messages[-1].get("content"), str)
    ):
        return ClaimExplanationOutcome(False, None, None, {})

    intent = parse_claim_explanation_intent(messages[-1]["content"])
    if intent is None:
        return ClaimExplanationOutcome(False, None, None, {})
    acquisition_explanation = intent.explanation_kind == "acquisition"
    if acquisition_explanation:
        history_enabled = (
            enabled
            if acquisition_history_enabled is None
            else acquisition_history_enabled
        )
        if not history_enabled:
            return ClaimExplanationOutcome(False, None, None, {})
        return await _resolve_acquisition_explanation(
            intent=intent,
            messages=messages,
            memory_store=memory_store,
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface or "unknown",
        )
    if not enabled:
        return ClaimExplanationOutcome(False, None, None, {})

    target_mode: Literal["immediate_previous", "quoted_anchor"] = (
        "immediate_previous" if intent.mode == "latest" else "quoted_anchor"
    )
    prior_answer = None
    if intent.mode == "latest":
        prior_answer = _prior_assistant(messages, intent)
        if prior_answer is None:
            return _fallback(
                _TARGET_UNAVAILABLE,
                "prior_assistant_unavailable",
                target_mode=target_mode,
            )
    elif intent.target_anchor is None:
        return _fallback(
            _TARGET_UNAVAILABLE,
            "quoted_target_invalid",
            target_mode=target_mode,
            target_status="invalid",
        )

    try:
        payload = await memory_store.list_claim_records(
            owner_id=owner_id,
            conversation_id=conversation_id,
            limit=20,
        )
    except Exception:
        return _fallback(
            _DEPENDENCY_UNAVAILABLE,
            "claim_records_unavailable",
            target_mode=target_mode,
            target_status="resolved",
            lookup_status="failed",
            storage_call_count=1,
        )

    try:
        response = ClaimRecordListResponse.model_validate(payload)
    except ValidationError:
        return _fallback(
            _INVALID_RECORD,
            "claim_record_response_invalid",
            target_mode=target_mode,
            target_status="resolved",
            lookup_status="completed",
            storage_call_count=1,
        )

    records = response.records
    if not records:
        if intent.mode == "quoted_anchor":
            return _fallback(
                _QUOTED_NO_RECORD,
                "quoted_claim_record_not_found",
                target_mode=target_mode,
                target_status="resolved",
                lookup_status="completed",
                resolution_status="no_record",
                storage_call_count=1,
            )
        return _fallback(
            _NO_RECORD,
            "no_claim_records",
            target_mode=target_mode,
            target_status="resolved",
            lookup_status="completed",
            resolution_status="no_record",
            storage_call_count=1,
        )

    if not all(
        _record_matches_scope(
            record,
            owner_id=owner_id,
            conversation_id=conversation_id,
        )
        for record in records
    ):
        return _fallback(
            _INVALID_RECORD,
            "record_invalid",
            target_mode=target_mode,
            target_status="resolved",
            lookup_status="completed",
            resolution_status="invalid",
            storage_call_count=1,
            record_count=len(records),
        )

    counts = {
        "target_mode": target_mode,
        "target_status": "resolved",
        "lookup_status": "completed",
        "storage_call_count": 1,
        "record_count": len(records),
    }
    if intent.mode == "quoted_anchor":
        matching_records = [
            record for record in records if record.claim_anchor == intent.target_anchor
        ]
        if not matching_records:
            return _fallback(
                _QUOTED_NO_RECORD,
                "quoted_claim_record_not_found",
                resolution_status="no_record",
                matched_record_count=0,
                **counts,
            )
        if len(matching_records) > 1:
            return _fallback(
                _QUOTED_AMBIGUOUS,
                "ambiguous_quoted_claim",
                resolution_status="ambiguous",
                matched_record_count=len(matching_records),
                **counts,
            )
        record = matching_records[0]
    else:
        newest_message_id = records[0].assistant_message_id
        newest_group: list[ClaimRecord] = []
        for record in records:
            if record.assistant_message_id != newest_message_id:
                break
            newest_group.append(record)
        counts["newest_group_count"] = len(newest_group)
        if len(newest_group) > 1:
            return _fallback(
                _AMBIGUOUS,
                "ambiguous_latest_response",
                resolution_status="ambiguous",
                **counts,
            )

        record = newest_group[0]
        if (
            _normalized_first_response_paragraph(prior_answer)
            != record.claim_anchor
        ):
            return _fallback(
                _NO_RECORD,
                "no_record_for_latest_response",
                resolution_status="no_record",
                **counts,
            )

    support_status = _record_support_status(
        record,
        owner_id=owner_id,
        conversation_id=conversation_id,
    )
    if support_status != "supported":
        reason = {
            "invalid": "record_invalid",
            "unsupported": "record_unsupported",
            "insufficient": "record_insufficient",
        }[support_status]
        return _fallback(
            _INVALID_RECORD,
            reason,
            resolution_status=support_status,
            matched_record_count=1,
            **counts,
        )

    return ClaimExplanationOutcome(
        handled=True,
        answer=_render(record),
        status="ok",
        trace=_trace(
            reason_code=(
                "latest_claim_record_resolved"
                if intent.mode == "latest"
                else "quoted_claim_record_resolved"
            ),
            resolution_status="resolved",
            render_status="completed",
            matched_record_count=1,
            claim_id=record.claim_id,
            claim_anchor_digest=record.claim_anchor_digest,
            **counts,
        ),
    )
