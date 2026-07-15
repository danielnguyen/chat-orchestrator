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
_QUOTED_INTENT_RE = re.compile(
    r'(?:what\s+supports\s+the\s+statement|'
    r'what\s+supported\s+the\s+statement|'
    r'how\s+are\s+you\s+sure\s+about\s+the\s+statement)'
    r'\s+"(?P<anchor>[^"\r\n]*)"\s*[?.]?\s*',
    re.IGNORECASE,
)

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


@dataclass(frozen=True)
class ClaimExplanationIntent:
    mode: Literal["latest", "quoted_anchor"]
    target_anchor: str | None = None


@dataclass(frozen=True)
class ClaimExplanationOutcome:
    handled: bool
    answer: str | None
    status: Literal["ok", "degraded"] | None
    trace: dict[str, Any]


def normalize_text(value: str) -> str:
    return " ".join(value.split())


def parse_claim_explanation_intent(value: Any) -> ClaimExplanationIntent | None:
    if not isinstance(value, str):
        return None
    normalized = normalize_text(value).casefold()
    if normalized.endswith(("?", ".")):
        normalized = normalized[:-1].rstrip()
    if normalized in _SUPPORTED_INTENTS:
        return ClaimExplanationIntent(mode="latest")

    match = _QUOTED_INTENT_RE.fullmatch(value.strip())
    if match is None:
        return None
    target_anchor = normalize_text(match.group("anchor"))
    if not target_anchor or len(target_anchor) > 500:
        target_anchor = None
    return ClaimExplanationIntent(
        mode="quoted_anchor",
        target_anchor=target_anchor,
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
    normalized = normalize_text(prior["content"])
    return normalized if normalized and len(normalized) <= 500 else None


def _digest(anchor: str) -> str:
    return f"sha256:{hashlib.sha256(anchor.encode()).hexdigest()}"


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


async def resolve_claim_explanation(
    *,
    enabled: bool,
    messages: Any,
    memory_store: Any,
    owner_id: str,
    conversation_id: str,
) -> ClaimExplanationOutcome:
    final_content = messages[-1].get("content") if isinstance(messages, list) and messages else None
    intent = parse_claim_explanation_intent(final_content)
    if not enabled or intent is None:
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
        if record.claim_anchor != prior_answer:
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
