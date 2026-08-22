from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, replace
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

_IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
_IDENTIFIER_RE = re.compile(_IDENTIFIER_PATTERN)
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_PARAGRAPH_SEPARATOR = re.compile(r"\r?\n[ \t]*\r?\n")
_SUPPORTED_FILE_REFERENCE_TYPES = {"artifact", "derived_text"}
_SUPPORTED_REFERENCE_TYPES = {
    "message",
    "derived_text",
    "artifact",
    "external_source",
    "world_state_claim",
    "tool_output",
    "integration_event",
}
_FRESHNESS_STATES = {
    "active",
    "stale",
    "superseded",
    "corrected",
    "unknown_freshness",
    "not_applicable",
}
_FILE_SOURCE_NOUN = r"(?:file|document|report|record|notice|entry|table|log|source)"
_EVIDENTIAL_VERB = r"(?:states|reports|lists|shows|indicates|records|documents|confirms)"
_SOURCE_MODIFIER = r"[A-Za-z][A-Za-z0-9-]{0,31}"
_SOURCE_SUBJECT_ATTRIBUTION = re.compile(
    rf"^(?:the|this)\s+(?:{_SOURCE_MODIFIER}\s+){{0,3}}"
    rf"{_FILE_SOURCE_NOUN}\s+{_EVIDENTIAL_VERB}\s+.+\.$",
    re.IGNORECASE,
)
_ACCORDING_TO_ATTRIBUTION = re.compile(
    rf"^according\s+to\s+(?:the|this)\s+(?:{_SOURCE_MODIFIER}\s+){{0,3}}"
    rf"{_FILE_SOURCE_NOUN},\s+.+\.$",
    re.IGNORECASE,
)
_TRUSTED_GOVERNED_CLAIM_ANCHORS = {
    "The retained evidence supports the requested conclusion.",
    "The retained evidence does not support the requested conclusion.",
    "The retained evidence supports only the following bounded description.",
}


class _EvidenceReference(BaseModel):
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
    ref_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    owner_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    conversation_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=120,
        pattern=_IDENTIFIER_PATTERN,
    )
    support_kind: Literal["direct", "corroborating", "contextual", "contradictory"]
    authority: Literal[
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
    freshness_state: Literal[
        "active",
        "stale",
        "superseded",
        "corrected",
        "unknown_freshness",
        "not_applicable",
    ]


class _CalibrationResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    claim_anchor: str = Field(min_length=1, max_length=500)
    claim_anchor_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
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
    strongest_authority: Literal[
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
    freshness_summary: Literal["current", "mixed", "stale", "unknown", "not_applicable"]
    uncertainty_disclosure_required: bool
    validated_evidence_references: list[_EvidenceReference] = Field(max_length=16)
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
    user_safe_summary: str = Field(min_length=1, max_length=500)

    @field_validator("claim_anchor", mode="before")
    @classmethod
    def normalize_claim_anchor(cls, value: Any) -> Any:
        return " ".join(value.split()) if isinstance(value, str) else value

    @field_validator("limitation_codes")
    @classmethod
    def reject_duplicate_limitations(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate_limitation_code")
        return value


class _CalibrationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    owner_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    conversation_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    surface: str = Field(min_length=1, max_length=64)
    runtime_session_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    runtime_turn_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    result: _CalibrationResult


class _StoredClaimRecord(_CalibrationResult):
    schema_version: Literal["claim-record.v1"]
    owner_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    conversation_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    request_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    assistant_message_id: str = Field(
        min_length=1,
        max_length=120,
        pattern=_IDENTIFIER_PATTERN,
    )
    surface: str = Field(min_length=1, max_length=64)
    runtime_session_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    runtime_turn_id: str = Field(min_length=1, max_length=120, pattern=_IDENTIFIER_PATTERN)
    acquisition_manifest_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=120,
        pattern=_IDENTIFIER_PATTERN,
    )
    created_at: str = Field(min_length=1, max_length=80)


class _ClaimRecordResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    created: bool
    record: _StoredClaimRecord


@dataclass(frozen=True)
class ClaimCaptureCandidate:
    claim_anchor: str
    evidence_reference: dict[str, Any]


@dataclass(frozen=True)
class TrustedGovernedClaim:
    claim_anchor: str
    source_ref: str


@dataclass(frozen=True)
class ClaimCaptureState:
    trace: dict[str, Any]
    candidate: ClaimCaptureCandidate | None = None
    calibration_result: dict[str, Any] | None = None
    assistant_message_id: str | None = None
    acquisition_manifest_id: str | None = None
    acquisition_manifest_required: bool = False


def _trace(
    *,
    enabled: bool,
    eligibility_status: str,
    reason_code: str,
    evidence_count: int = 0,
) -> dict[str, Any]:
    return {
        "enabled": enabled,
        "eligibility_status": eligibility_status,
        "calibration_status": "not_attempted",
        "persistence_status": "not_attempted",
        "reason_code": reason_code,
        "runtime_call_count": 0,
        "storage_call_count": 0,
        "evidence_count": evidence_count,
        "claim_id": None,
        "claim_anchor_digest": None,
        "acquisition_manifest_status": "not_applicable",
        "acquisition_manifest_linked": False,
    }


def _ineligible(*, enabled: bool, reason_code: str) -> ClaimCaptureState:
    return ClaimCaptureState(
        trace=_trace(
            enabled=enabled,
            eligibility_status="ineligible",
            reason_code=reason_code,
        )
    )


def _valid_identifier(value: Any) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 120
        and _IDENTIFIER_RE.fullmatch(value) is not None
    )


def _normalized_claim_anchor(answer: Any) -> tuple[str | None, str | None]:
    if not isinstance(answer, str) or not answer.strip():
        return None, "empty_answer"
    if len(" ".join(answer.split())) > 500:
        return None, "claim_anchor_too_long"
    if "```" in answer or "\n" in answer:
        return None, "structured_answer"
    stripped = answer.strip()
    if re.match(r"^(?:#{1,6}\s|[-*+]\s|\d+[.)]\s)", stripped):
        return None, "structured_answer"
    normalized = " ".join(stripped.split())
    if normalized.endswith("?"):
        return None, "question_answer"
    sentence_marks = re.findall(r"[.!?](?=\s|$)", normalized)
    if len(sentence_marks) != 1 or normalized[-1:] not in {".", "!"}:
        return None, "multi_sentence_answer"
    if not (
        _SOURCE_SUBJECT_ATTRIBUTION.fullmatch(normalized)
        or _ACCORDING_TO_ATTRIBUTION.fullmatch(normalized)
    ):
        return None, "factual_source_attribution_unavailable"
    return normalized, None


def _source_identity(source: Any) -> tuple[str, str] | None:
    if not isinstance(source, dict):
        return None
    source_ref = source.get("source_ref")
    if not isinstance(source_ref, dict):
        return None
    ref_type = source_ref.get("ref_type")
    ref_id = source_ref.get("ref_id")
    if ref_type not in _SUPPORTED_FILE_REFERENCE_TYPES or not _valid_identifier(ref_id):
        return None
    return ref_type, ref_id


def governed_external_reference_id(source_ref: str) -> str:
    if _valid_identifier(source_ref):
        return source_ref
    digest = hashlib.sha256(source_ref.encode("utf-8")).hexdigest()
    return f"external-source:{digest}"


def prepare_claim_capture(
    *,
    enabled: bool,
    compound_verification_requested: bool = False,
    trusted_governed_claim: TrustedGovernedClaim | None = None,
    runtime_available: bool,
    runtime_session_id: Any,
    runtime_turn_id: Any,
    answer: Any,
    is_brief: bool,
    pending_action_present: bool,
    capability_requested: bool,
    capability_executed: bool,
    callback_applied: bool,
    privacy_suppressed: bool,
    retained_artifacts: Any,
    public_sources: Any,
    trace_references: Any,
    owner_id: Any,
    conversation_id: Any,
) -> ClaimCaptureState:
    if not enabled:
        return _ineligible(enabled=False, reason_code="disabled")
    if compound_verification_requested:
        return _ineligible(
            enabled=True,
            reason_code="compound_verification_response",
        )
    if not runtime_available:
        return _ineligible(enabled=True, reason_code="runtime_unavailable")
    if not _valid_identifier(runtime_session_id) or not _valid_identifier(runtime_turn_id):
        return _ineligible(enabled=True, reason_code="runtime_scope_unavailable")
    if privacy_suppressed:
        return _ineligible(enabled=True, reason_code="privacy_suppressed")
    if is_brief:
        return _ineligible(enabled=True, reason_code="brief_response")
    if pending_action_present or capability_requested or capability_executed:
        return _ineligible(enabled=True, reason_code="action_response")
    if callback_applied:
        return _ineligible(enabled=True, reason_code="callback_response")
    if trusted_governed_claim is not None:
        claim_anchor = trusted_governed_claim.claim_anchor
        if claim_anchor not in _TRUSTED_GOVERNED_CLAIM_ANCHORS:
            return _ineligible(
                enabled=True,
                reason_code="trusted_governed_claim_invalid",
            )
        if not trusted_governed_claim.source_ref or not _valid_identifier(owner_id):
            return _ineligible(
                enabled=True,
                reason_code="source_identity_unavailable",
            )
        evidence_reference = {
            "ref_type": "external_source",
            "ref_id": governed_external_reference_id(
                trusted_governed_claim.source_ref
            ),
            "owner_id": owner_id,
            "conversation_id": None,
            "support_kind": "direct",
            "authority": "trusted_integration",
            "freshness_state": "unknown_freshness",
        }
        return ClaimCaptureState(
            trace=_trace(
                enabled=True,
                eligibility_status="eligible",
                reason_code="single_claim_single_external_source",
                evidence_count=1,
            ),
            candidate=ClaimCaptureCandidate(
                claim_anchor=claim_anchor,
                evidence_reference=evidence_reference,
            ),
        )
    claim_anchor, answer_reason = _normalized_claim_anchor(answer)
    if answer_reason is not None or claim_anchor is None:
        return _ineligible(enabled=True, reason_code=answer_reason or "empty_answer")
    if not isinstance(retained_artifacts, list) or not retained_artifacts:
        return _ineligible(enabled=True, reason_code="no_retained_file_source")
    if len(retained_artifacts) != 1:
        return _ineligible(enabled=True, reason_code="multiple_retained_file_sources")
    if not isinstance(public_sources, list) or not public_sources:
        return _ineligible(enabled=True, reason_code="no_retained_file_source")
    if len(public_sources) != 1:
        return _ineligible(enabled=True, reason_code="multiple_retained_file_sources")
    retained = retained_artifacts[0]
    public = public_sources[0]
    retained_identity = _source_identity(retained)
    public_identity = _source_identity(public)
    if retained_identity is None or public_identity != retained_identity:
        return _ineligible(enabled=True, reason_code="source_identity_unavailable")
    if not isinstance(retained, dict) or retained.get("artifact_id") != public.get("artifact_id"):
        return _ineligible(enabled=True, reason_code="source_identity_unavailable")
    if retained.get("owner_id") != owner_id:
        return _ineligible(enabled=True, reason_code="source_identity_unavailable")
    if not _valid_identifier(owner_id) or not _valid_identifier(conversation_id):
        return _ineligible(enabled=True, reason_code="source_identity_unavailable")
    trace_identities = {
        (reference.get("ref_type"), reference.get("ref_id"))
        for reference in trace_references
        if isinstance(reference, dict)
        and reference.get("ref_type") in _SUPPORTED_REFERENCE_TYPES
        and _valid_identifier(reference.get("ref_id"))
    } if isinstance(trace_references, list) else set()
    if retained_identity not in trace_identities:
        return _ineligible(enabled=True, reason_code="source_identity_unavailable")
    freshness = retained.get("freshness_state")
    if freshness not in _FRESHNESS_STATES:
        freshness = "unknown_freshness"
    evidence_reference = {
        "ref_type": retained_identity[0],
        "ref_id": retained_identity[1],
        "owner_id": owner_id,
        "conversation_id": conversation_id,
        "support_kind": "direct",
        "authority": "user_report",
        "freshness_state": freshness,
    }
    return ClaimCaptureState(
        trace=_trace(
            enabled=True,
            eligibility_status="eligible",
            reason_code="single_claim_single_file_source",
            evidence_count=1,
        ),
        candidate=ClaimCaptureCandidate(
            claim_anchor=claim_anchor,
            evidence_reference=evidence_reference,
        ),
    )


def _anchor_digest(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _response_digest(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _normalized_first_response_paragraph(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    first_paragraph = _PARAGRAPH_SEPARATOR.split(value, maxsplit=1)[0]
    normalized = " ".join(first_paragraph.split())
    return normalized or None


async def calibrate_claim_capture(
    *,
    runtime: Any,
    state: ClaimCaptureState,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
) -> ClaimCaptureState:
    if state.candidate is None:
        return state
    trace = {**state.trace, "runtime_call_count": 1}
    try:
        raw_response = await runtime.evaluate_claim_calibration(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
            claim_anchor=state.candidate.claim_anchor,
            evidence_references=[state.candidate.evidence_reference],
        )
    except Exception:
        return replace(
            state,
            trace={
                **trace,
                "calibration_status": "failed",
                "reason_code": "calibration_unavailable",
            },
        )
    try:
        response = _CalibrationResponse.model_validate(raw_response)
    except ValidationError:
        return replace(
            state,
            trace={
                **trace,
                "calibration_status": "failed",
                "reason_code": "calibration_response_invalid",
            },
        )
    expected_scope = {
        "request_id": request_id,
        "owner_id": owner_id,
        "conversation_id": conversation_id,
        "surface": surface,
        "runtime_session_id": runtime_session_id,
        "runtime_turn_id": runtime_turn_id,
    }
    response_scope = response.model_dump(mode="json", exclude={"result"})
    result = response.result.model_dump(mode="json")
    expected_evidence = [state.candidate.evidence_reference]
    if (
        response_scope != expected_scope
        or result.get("claim_anchor") != state.candidate.claim_anchor
        or result.get("claim_anchor_digest") != _anchor_digest(state.candidate.claim_anchor)
        or result.get("validated_evidence_references") != expected_evidence
        or not _DIGEST_RE.fullmatch(str(result.get("claim_anchor_digest", "")))
    ):
        return replace(
            state,
            trace={
                **trace,
                "calibration_status": "failed",
                "reason_code": "calibration_response_invalid",
            },
        )
    return replace(
        state,
        calibration_result=result,
        trace={
            **trace,
            "calibration_status": "completed",
            "reason_code": trace["reason_code"],
            "claim_id": result["claim_id"],
            "claim_anchor_digest": result["claim_anchor_digest"],
        },
    )


def bind_assistant_message(
    state: ClaimCaptureState,
    acknowledgement: Any,
) -> ClaimCaptureState:
    if state.calibration_result is None:
        return state
    message_id = acknowledgement.get("message_id") if isinstance(acknowledgement, dict) else None
    if not _valid_identifier(message_id):
        return replace(
            state,
            trace={
                **state.trace,
                "persistence_status": "failed",
                "reason_code": "assistant_message_ack_invalid",
            },
        )
    return replace(
        state,
        assistant_message_id=message_id,
        trace={**state.trace, "persistence_status": "pending"},
    )


def bind_acquisition_manifest(
    state: ClaimCaptureState,
    manifest: Any,
    *,
    final_answer: Any,
) -> ClaimCaptureState:
    if manifest is None or state.candidate is None:
        return replace(
            state,
            acquisition_manifest_id=None,
            acquisition_manifest_required=False,
            trace={
                **state.trace,
                "acquisition_manifest_status": "not_applicable",
                "acquisition_manifest_linked": False,
            },
        )

    invalid_trace = {
        **state.trace,
        "persistence_status": "not_attempted",
        "reason_code": "acquisition_manifest_association_invalid",
        "acquisition_manifest_status": "invalid",
        "acquisition_manifest_linked": False,
    }
    if (
        state.calibration_result is None
        or state.assistant_message_id is None
        or not isinstance(manifest, dict)
    ):
        return replace(
            state,
            acquisition_manifest_id=None,
            acquisition_manifest_required=True,
            trace=invalid_trace,
        )

    manifest_id = manifest.get("manifest_id")
    plan = manifest.get("plan")
    sufficiency = manifest.get("sufficiency")
    top_level_status = manifest.get("status")
    nested_status = (
        sufficiency.get("status")
        if isinstance(sufficiency, dict)
        else None
    )
    accepted_statuses = {
        "sufficient_for_declared_scope",
        "sufficient_with_limitations",
    }
    valid = (
        _valid_identifier(manifest_id)
        and manifest.get("attempted") is True
        and isinstance(plan, dict)
        and plan.get("plan_status") in {"ready", "ready_with_limitations"}
        and top_level_status in accepted_statuses
        and nested_status in accepted_statuses
        and top_level_status == nested_status
        and manifest.get("assistant_message_id") == state.assistant_message_id
        and manifest.get("response_digest") == _response_digest(final_answer)
        and _normalized_first_response_paragraph(final_answer)
        == state.calibration_result.get("claim_anchor")
    )
    if not valid:
        return replace(
            state,
            acquisition_manifest_id=None,
            acquisition_manifest_required=True,
            trace=invalid_trace,
        )

    return replace(
        state,
        acquisition_manifest_id=manifest_id,
        acquisition_manifest_required=True,
        trace={
            **state.trace,
            "acquisition_manifest_status": "bound",
            "acquisition_manifest_linked": True,
        },
    )


def claim_record_payload(
    *,
    state: ClaimCaptureState,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
) -> dict[str, Any] | None:
    if state.calibration_result is None or state.assistant_message_id is None:
        return None
    if state.acquisition_manifest_required and state.acquisition_manifest_id is None:
        return None
    payload = {
        "schema_version": "claim-record.v1",
        "request_id": request_id,
        "owner_id": owner_id,
        "conversation_id": conversation_id,
        "assistant_message_id": state.assistant_message_id,
        "surface": surface,
        "runtime_session_id": runtime_session_id,
        "runtime_turn_id": runtime_turn_id,
        "calibration_result": state.calibration_result,
    }
    if state.acquisition_manifest_id is not None:
        payload["acquisition_manifest_id"] = state.acquisition_manifest_id
    return payload


def claim_support_record_payload(
    *,
    reasoning_result: dict[str, Any],
    presented_to_user: bool,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    assistant_message_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
    acquisition_manifest_id: str | None,
) -> dict[str, Any] | None:
    proposal = reasoning_result.get("proposal")
    executions = reasoning_result.get("executions")
    authority = reasoning_result.get("authority_context")
    cr_result = reasoning_result.get("cr_result")
    if not all(
        isinstance(value, dict)
        for value in (proposal, authority, cr_result)
    ) or not isinstance(executions, list):
        return None
    claim = proposal.get("proposed_claim")
    claim_digest = cr_result.get("claim_digest")
    if (
        not isinstance(claim, str)
        or not isinstance(claim_digest, str)
        or claim_digest != _anchor_digest(claim)
    ):
        return None
    supporting = set(cr_result.get("validated_supporting_evidence_ref_ids") or [])
    counter = set(cr_result.get("validated_counterevidence_ref_ids") or [])
    exclusions = {
        item.get("evidence_ref_id")
        for item in cr_result.get("validated_material_exclusions") or []
        if isinstance(item, dict)
    }
    evidence_references = []
    for reference in authority.get("evidence_references") or []:
        if not isinstance(reference, dict):
            return None
        ref_id = reference.get("ref_id")
        if ref_id not in supporting | counter | exclusions:
            continue
        evidence_references.append(
            {
                "ref_type": "external_source",
                "ref_id": ref_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "support_kind": "contextual",
                "authority": "unknown",
                "freshness_state": "unknown_freshness",
            }
        )
    status = cr_result.get("calibration_status")
    scalar_limitations: list[str] = []
    if status in {"limited", "unsupported"}:
        scalar_limitations.append("inference_dominant")
    if not supporting:
        scalar_limitations.append("no_supporting_evidence")
    if counter:
        scalar_limitations.append("contradictory_evidence")
    support = {
        "claim_digest": claim_digest,
        "supporting_evidence_ref_ids": sorted(supporting),
        "counterevidence_ref_ids": sorted(counter),
        "material_exclusions": cr_result.get("validated_material_exclusions") or [],
        "executed_derivations": [
            {
                key: record[key]
                for key in (
                    "derivation_id",
                    "operation",
                    "canonical_inputs",
                    "canonical_result",
                    "execution_digest",
                    "executor_version",
                    "supporting_evidence_ref_ids",
                    "input_basis",
                )
            }
            for record in executions
        ],
        "material_scope_limitations": sorted(
            {
                *(
                    ["complete_scope_not_established"]
                    if authority.get("complete_declared_scope_required") is True
                    and authority.get("complete_declared_scope_established") is not True
                    else []
                ),
                *(
                    ["material_acquisition_limited"]
                    if authority.get("material_acquisition_limited") is True
                    else []
                ),
            }
        ),
        "calibration_status": status,
        "conclusion_disposition": cr_result.get("conclusion_disposition"),
        "qualification_required": cr_result.get("qualification_required"),
        "limitation_codes": cr_result.get("limitation_codes") or [],
    }
    payload = {
        "schema_version": "claim-record.v2",
        "request_id": request_id,
        "owner_id": owner_id,
        "conversation_id": conversation_id,
        "assistant_message_id": assistant_message_id,
        "surface": surface,
        "runtime_session_id": runtime_session_id,
        "runtime_turn_id": runtime_turn_id,
        "presented_to_user": presented_to_user,
        "calibration_result": {
            "claim_id": cr_result.get("claim_id"),
            "claim_anchor": claim,
            "claim_anchor_digest": claim_digest,
            "claim_class": "runtime_inference",
            "calibration_status": status,
            "evidence_strength": "weak" if evidence_references else "none",
            "confidence": "unknown",
            "strongest_authority": "unknown",
            "freshness_summary": "unknown",
            "uncertainty_disclosure_required": True,
            "validated_evidence_references": evidence_references,
            "limitation_codes": scalar_limitations,
            "user_safe_summary": cr_result.get("user_safe_summary"),
        },
        "support": support,
    }
    if acquisition_manifest_id is not None:
        payload["acquisition_manifest_id"] = acquisition_manifest_id
    return payload


def claim_support_record_response_valid(
    *,
    expected_payload: dict[str, Any],
    response: Any,
) -> bool:
    if not isinstance(response, dict) or set(response) != {"created", "record"}:
        return False
    if not isinstance(response.get("created"), bool):
        return False
    record = response.get("record")
    if not isinstance(record, dict) or not isinstance(record.get("created_at"), str):
        return False
    expected = {
        "claim_id": expected_payload["calibration_result"]["claim_id"],
        "schema_version": "claim-record.v2",
        "owner_id": expected_payload["owner_id"],
        "conversation_id": expected_payload["conversation_id"],
        "request_id": expected_payload["request_id"],
        "assistant_message_id": expected_payload["assistant_message_id"],
        "surface": expected_payload["surface"],
        "runtime_session_id": expected_payload["runtime_session_id"],
        "runtime_turn_id": expected_payload["runtime_turn_id"],
        "presented_to_user": expected_payload["presented_to_user"],
        **{
            key: value
            for key, value in expected_payload["calibration_result"].items()
            if key != "claim_id"
        },
        "support": expected_payload["support"],
    }
    if "acquisition_manifest_id" in expected_payload:
        expected["acquisition_manifest_id"] = expected_payload[
            "acquisition_manifest_id"
        ]
    return all(record.get(key) == value for key, value in expected.items())


def shadow_claim_record_payload(
    *,
    shadow_result: dict[str, Any],
    request_id: str,
    owner_id: str,
    conversation_id: str,
    assistant_message_id: str,
    surface: str,
    runtime_session_id: str,
    runtime_turn_id: str,
    acquisition_manifest_id: str | None,
) -> dict[str, Any] | None:
    return claim_support_record_payload(
        reasoning_result=shadow_result,
        presented_to_user=False,
        request_id=request_id,
        owner_id=owner_id,
        conversation_id=conversation_id,
        assistant_message_id=assistant_message_id,
        surface=surface,
        runtime_session_id=runtime_session_id,
        runtime_turn_id=runtime_turn_id,
        acquisition_manifest_id=acquisition_manifest_id,
    )


def shadow_claim_record_response_valid(
    *,
    expected_payload: dict[str, Any],
    response: Any,
) -> bool:
    return claim_support_record_response_valid(
        expected_payload=expected_payload,
        response=response,
    )


def finish_claim_record_persistence(
    *,
    state: ClaimCaptureState,
    expected_payload: dict[str, Any] | None = None,
    response: Any = None,
    failed: bool = False,
) -> ClaimCaptureState:
    trace = {**state.trace, "storage_call_count": 1}
    if failed or expected_payload is None:
        return replace(
            state,
            trace={
                **trace,
                "persistence_status": "failed",
                "reason_code": "claim_record_persistence_failed",
            },
        )
    try:
        bounded_response = _ClaimRecordResponse.model_validate(response)
    except ValidationError:
        bounded_response = None
    record = bounded_response.record.model_dump(mode="json") if bounded_response else None
    expected_record = {
        "claim_id": expected_payload.get("calibration_result", {}).get("claim_id"),
        "schema_version": expected_payload.get("schema_version"),
        "owner_id": expected_payload.get("owner_id"),
        "conversation_id": expected_payload.get("conversation_id"),
        "request_id": expected_payload.get("request_id"),
        "assistant_message_id": expected_payload.get("assistant_message_id"),
        "surface": expected_payload.get("surface"),
        "runtime_session_id": expected_payload.get("runtime_session_id"),
        "runtime_turn_id": expected_payload.get("runtime_turn_id"),
        "acquisition_manifest_id": expected_payload.get("acquisition_manifest_id"),
        **{
            key: value
            for key, value in expected_payload.get("calibration_result", {}).items()
            if key != "claim_id"
        },
    }
    if record is None or any(record.get(key) != value for key, value in expected_record.items()):
        return replace(
            state,
            trace={
                **trace,
                "persistence_status": "failed",
                "reason_code": "claim_record_persistence_failed",
            },
        )
    return replace(
        state,
        trace={
            **trace,
            "persistence_status": "persisted",
            "reason_code": trace["reason_code"],
        },
    )


def mark_trace_status_update_failed(state: ClaimCaptureState) -> ClaimCaptureState:
    return replace(
        state,
        trace={
            **state.trace,
            "reason_code": "trace_status_update_failed",
        },
    )
