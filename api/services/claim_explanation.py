from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Annotated, Any, Literal

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
_NO_NEW_VERIFICATION = "I did not perform a new verification for this explanation."
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


class ClaimRecord(BaseModel):
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
    def validate_collections(self) -> "ClaimRecord":
        identities = [
            (reference.ref_type, reference.ref_id)
            for reference in self.validated_evidence_references
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate_evidence_reference")
        if len(self.limitation_codes) != len(set(self.limitation_codes)):
            raise ValueError("duplicate_limitation_code")
        return self


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


def normalize_text(value: str) -> str:
    return " ".join(value.split())


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


def _render(record: ClaimRecord) -> str:
    evidence_type = record.validated_evidence_references[0].ref_type
    evidence_wording = {
        "derived_text": "one retained file excerpt",
        "artifact": "one retained file record",
    }[evidence_type]
    sentences = [
        (
            f"I based that earlier statement on {evidence_wording} from the original "
            f"retained record. The record classified it as "
            f"{_CLAIM_CLASS_WORDING[record.claim_class]}, with {record.confidence} "
            f"confidence and {_STRENGTH_WORDING[record.evidence_strength]}."
        )
    ]
    freshness = _FRESHNESS_WORDING.get(record.freshness_summary)
    if freshness:
        sentences.append(freshness)
    limitations = set(record.limitation_codes)
    sentences.extend(
        _LIMITATION_WORDING[code] for code in _LIMITATION_ORDER if code in limitations
    )
    sentences.append(_NO_NEW_VERIFICATION)
    return " ".join(sentences)


def _record_support_status(
    record: ClaimRecord,
    *,
    owner_id: str,
    conversation_id: str,
) -> Literal["supported", "unsupported", "insufficient", "invalid"]:
    if record.owner_id != owner_id or record.conversation_id != conversation_id:
        return "invalid"
    if record.claim_anchor_digest != _digest(record.claim_anchor):
        return "invalid"
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
    if (
        reference.ref_type not in {"artifact", "derived_text"}
        or reference.support_kind != "direct"
        or reference.authority != "user_report"
        or record.strongest_authority != "user_report"
        or reference.owner_id != owner_id
        or reference.conversation_id not in {None, conversation_id}
    ):
        return "unsupported"
    return "supported"


def _record_matches_scope(
    record: ClaimRecord,
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
        "targeted_lookup",
        "cross_source_comparison",
        "bounded_exhaustive_review",
    ]
    strategy: Literal["targeted_retrieval", "exact_fetch", "hybrid"]
    sufficiency_status: Literal[
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


@dataclass(frozen=True)
class AcquisitionHistoryProjection:
    history: AcquisitionHistory | None
    reason: str


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,119}$")
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


def _bounded_count(value: Any, *, maximum: int = 10000) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 <= value <= maximum else None


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
            and (
                not value
                or len(value) > 240
                or re.search(r"\s|://|\?", value) is not None
            )
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
                or not isinstance(attempt.get("source_ref"), str)
                or not attempt["source_ref"]
                or len(attempt["source_ref"]) > 240
                or re.search(r"\s|://|\?", attempt["source_ref"]) is not None
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
                    and (
                        not isinstance(source_ref, str)
                        or not source_ref
                        or len(source_ref) > 240
                        or re.search(r"\s|://|\?", source_ref) is not None
                    )
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


def _next_step_selection_is_consistent(selection: dict[str, Any]) -> bool:
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
    return (
        step in {"disclose_unexamined_scope", "withhold_unsupported_conclusion"}
        and conclusion == "requested_conclusion_withheld"
        and provider == "blocked"
        and (guard == "not_applicable" or blocked_guard)
        and target is None
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
    if status not in accepted_statuses:
        return reject("manifest_status_invalid")
    if not isinstance(plan, dict):
        return reject("plan_missing")
    if plan.get("plan_status") not in {"ready", "ready_with_limitations"}:
        return reject("plan_status_invalid")
    if not isinstance(sufficiency, dict):
        return reject("sufficiency_missing")
    if sufficiency.get("status") not in accepted_statuses:
        return reject("sufficiency_status_invalid")
    if sufficiency.get("status") != status:
        return reject("manifest_sufficiency_status_mismatch")
    if not isinstance(shape, dict):
        return reject("shape_missing")
    if shape.get("derivation_status") != "derived":
        return reject("shape_derivation_status_invalid")
    if shape.get("task_shape") not in {
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

    task_shape = shape["task_shape"]
    selected_strategies = plan.get("selected_strategies")
    strategy = acquisition.get("strategy_attempted")
    if (
        not isinstance(selected_strategies, list)
        or len(selected_strategies) != 1
        or selected_strategies[0]
        not in {"targeted_retrieval", "exact_fetch", "hybrid"}
    ):
        return reject("selected_strategies_invalid")
    if strategy != selected_strategies[0]:
        return reject("strategy_mismatch")
    if not isinstance(plan.get("contradiction_search_required"), bool):
        return reject("contradiction_search_flag_invalid")
    expected_composition = {
        ("targeted_lookup", "targeted_retrieval"): "targeted_scope",
        ("targeted_lookup", "exact_fetch"): "targeted_scope",
        ("cross_source_comparison", "hybrid"): "complete_for_selected_sources",
        ("bounded_exhaustive_review", "hybrid"): "complete_for_declared_scope",
    }
    if plan.get("completeness_expectation") != expected_composition.get(
        (task_shape, strategy)
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
    if not isinstance(acquisition.get("dsa_budget_truncation"), bool):
        return reject("dsa_budget_truncation_invalid")
    if not isinstance(acquisition.get("candidate_truncation"), bool):
        return reject("candidate_truncation_invalid")
    if not isinstance(sufficiency.get("qualification_required"), bool):
        return reject("qualification_required_invalid")
    if not isinstance(sufficiency.get("additional_acquisition_required"), bool):
        return reject("additional_acquisition_required_invalid")
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
            if not _next_step_selection_is_consistent(selection):
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
    history = AcquisitionHistory(
        task_shape=task_shape,
        strategy=strategy,
        sufficiency_status=status,
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
    )
    return AcquisitionHistoryProjection(history=history, reason="accepted")


def _project_acquisition_history(manifest: Any) -> AcquisitionHistory | None:
    return _diagnose_acquisition_history_projection(manifest).history


def _count_phrase(count: int, singular: str, plural: str | None = None) -> str:
    return f"{count} {singular if count == 1 else plural or singular + 's'}"


def _limitation_sentences(history: AcquisitionHistory) -> list[str]:
    counts = history.counts
    sentences = []
    for field, singular in (
        ("unavailable_source_count", "configured source was unavailable"),
        ("disabled_source_count", "configured source was disabled"),
        ("unknown_source_count", "configured source had unknown availability"),
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
        sentences.append(
            f"{omitted} filtered or omitted before reasoning."
        )
    for field, singular, plural in (
        ("exact_failed", "exact fetch failed", "exact fetches failed"),
        ("exact_unknown", "exact fetch returned no result", "exact fetches returned no result"),
        (
            "exact_filtered",
            "exact fetch response was filtered",
            "exact fetch responses were filtered",
        ),
        ("exact_truncated", "exact fetch was truncated", "exact fetches were truncated"),
    ):
        count = counts[field]
        if count:
            sentences.append(f"{_count_phrase(count, singular, plural)}.")
    for field, singular, plural in (
        ("expansion_failed", "context expansion failed", "context expansions failed"),
        (
            "expansion_unknown",
            "context expansion had an unknown outcome",
            "context expansions had unknown outcomes",
        ),
        (
            "expansion_filtered",
            "context expansion was filtered",
            "context expansions were filtered",
        ),
        (
            "expansion_truncated",
            "context expansion was truncated",
            "context expansions were truncated",
        ),
        (
            "expansion_unsupported",
            "context expansion was unsupported",
            "context expansions were unsupported",
        ),
    ):
        count = counts[field]
        if count:
            sentences.append(f"{_count_phrase(count, singular, plural)}.")
    if history.inventory_status == "partial":
        sentences.append("The retained source inventory was partial.")
    elif history.inventory_status == "unknown":
        sentences.append("The completeness of the retained source inventory was unknown.")
    elif history.inventory_status == "unavailable":
        sentences.append("The retained source inventory was unavailable.")
    if history.budget_truncated:
        sentences.append("Acquisition was truncated by the retrieval budget.")
    if history.candidate_truncated:
        sentences.append("Candidate selection was truncated.")
    if (
        "optional_source_unavailable" in history.limitation_codes
        and not counts["unavailable_source_count"]
    ):
        sentences.append("Optional source scope was unavailable.")
    return sentences


def _render_acquisition(
    history: AcquisitionHistory,
    question: Literal["checked", "coverage", "gaps"],
    *,
    include_no_new_verification: bool = True,
) -> str:
    counts = history.counts
    sentences: list[str] = []
    if question == "coverage":
        if (
            history.task_shape == "bounded_exhaustive_review"
            and history.sufficiency_status == "sufficient_for_declared_scope"
        ):
            sentences.append(
                "Within the declared bounded scope, yes. That does not establish "
                "universal coverage beyond it."
            )
        else:
            sentences.append("No—not universally.")
    elif question == "gaps":
        sentences.append(
            "The retained record cannot identify unknown evidence outside its declared "
            "source scope."
        )

    if history.changed_premise_exact_follow_up:
        sentences.append(
            "The original turn first performed a targeted lookup and then one "
            "authorized changed-premise exact fetch."
        )

    if history.task_shape == "targeted_lookup" and history.strategy == "targeted_retrieval":
        sentences.append(
            "For that earlier answer, the retained record shows a targeted lookup. "
            f"It considered {_count_phrase(counts['sources_considered'], 'configured source')}, "
            f"selected {counts['sources_selected']}, returned "
            f"{_count_phrase(counts['references_returned'], 'item')}, and delivered "
            f"{counts['references_retained']} to reasoning."
        )
        sufficient_scope = "the declared targeted scope"
        boundary = (
            "This was not an exhaustive review of every potentially relevant source."
        )
    elif history.task_shape == "targeted_lookup" and history.strategy == "exact_fetch":
        attempt_count = counts["exact_attempts"]
        sentences.append(
            "For that earlier answer, the retained record shows "
            + (
                "an exact fetch for 1 specified reference."
                if attempt_count == 1
                else f"exact fetches for {attempt_count} specified references."
            )
            + " "
            + (
                "It was retrieved and delivered to reasoning."
                if counts["exact_successful"] == 1
                and counts["references_retained"] == 1
                else (
                    f"{counts['exact_successful']} were retrieved and "
                    f"{counts['references_retained']} were delivered to reasoning."
                )
            )
        )
        sufficient_scope = "that declared exact-reference scope"
        boundary = (
            "Sources or references outside that supplied scope were not established "
            "as examined."
        )
    elif history.task_shape == "cross_source_comparison" and history.strategy == "hybrid":
        sentences.append(
            "For that earlier answer, the retained record shows a bounded comparison "
            f"across {counts['sources_selected']} selected configured sources from "
            f"{counts['sources_considered']} considered. It attempted "
            f"{_count_phrase(counts['expansion_attempts'], 'context expansion')}, "
            f"returned {_count_phrase(counts['references_returned'], 'reference')}, "
            f"and delivered {counts['references_retained']} to reasoning."
        )
        sufficient_scope = "the selected-source comparison scope"
        boundary = (
            "Only the selected sources and delivered bounded context were examined; "
            "this was not a comparison of every possible source."
        )
    elif history.task_shape == "bounded_exhaustive_review" and history.strategy == "hybrid":
        sentences.append(
            "For that earlier answer, the retained record shows a bounded exhaustive "
            f"review of the declared configured scope. It considered "
            f"{_count_phrase(counts['sources_considered'], 'configured source')}, "
            f"selected {counts['sources_selected']}, returned "
            f"{_count_phrase(counts['references_returned'], 'reference')}, and "
            f"delivered {counts['references_retained']} to reasoning."
        )
        sufficient_scope = "the declared bounded source scope"
        boundary = (
            "Completeness applies only within that declared bounded scope, not to "
            "sources outside it."
        )
    else:
        raise ValueError("unsupported_acquisition_history_composition")

    if history.sufficiency_status == "sufficient_for_declared_scope":
        sentences.append(f"The recorded evidence was sufficient for {sufficient_scope}.")
    elif history.sufficiency_status == "sufficient_with_limitations":
        sentences.append(
            "The recorded evidence was sufficient only with recorded limitations."
        )
    elif history.sufficiency_status == "insufficient":
        sentences.append(
            "The record marked the evidence insufficient, so the requested conclusion "
            "was not established."
        )
    else:
        sentences.append(
            "The record left evidence sufficiency unknown, so the requested conclusion "
            "was not established."
        )
    sentences.extend(_limitation_sentences(history))
    if history.final_next_step == "ask_narrow_clarification":
        sentences.append("The recorded next step was a narrow clarification.")
    elif history.final_next_step == "disclose_unexamined_scope":
        sentences.append("The recorded next step was to disclose unexamined scope.")
    elif history.final_next_step == "withhold_unsupported_conclusion":
        sentences.append("The recorded next step was to withhold the unsupported conclusion.")
    elif history.final_next_step == "provide_qualified_partial_answer":
        sentences.append("The recorded next step was a qualified partial response.")
    sentences.append(boundary)
    if include_no_new_verification:
        sentences.append(_NO_NEW_VERIFICATION)
    return " ".join(sentences)


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
