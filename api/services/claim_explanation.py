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
    r"(?P<gaps>what\s+might\s+you\s+have\s+missed))"
    r'\s+for\s+the\s+statement\s+"(?P<anchor>[^"\r\n]*)"\s*[?.]?\s*',
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
_ACQUISITION_TARGET_UNAVAILABLE = (
    "I can’t safely identify which earlier answer you mean from the supplied "
    "conversation context, so I can’t say what was checked. I did not perform a new "
    "verification."
)
_ACQUISITION_NO_RECORD = (
    "I don’t have a retained claim record for that immediately previous answer, so I "
    "can’t honestly say what was checked or missed. I did not perform a new "
    "verification."
)
_ACQUISITION_QUOTED_NO_RECORD = (
    "I don’t have a retained claim record matching that quoted earlier statement, so "
    "I can’t honestly say what was checked or missed. I did not perform a new "
    "verification."
)
_ACQUISITION_AMBIGUOUS = (
    "I found more than one retained claim for that earlier answer, so I can’t safely "
    "choose an acquisition record. I did not perform a new verification."
)
_ACQUISITION_DEPENDENCY_UNAVAILABLE = (
    "I couldn’t access the retained claim record for that earlier answer. I can’t "
    "honestly reconstruct what was checked from memory, and I did not perform a new "
    "verification."
)
_NO_LINKED_MANIFEST = (
    "I don’t have a retained acquisition record linked to that earlier answer, so I "
    "can’t honestly say what was checked or missed. I did not perform a new "
    "verification."
)
_TRACE_UNAVAILABLE = (
    "I couldn’t access the retained acquisition record for that earlier answer. I "
    "can’t honestly reconstruct what was checked from memory, and I did not perform a "
    "new verification."
)
_INVALID_ACQUISITION_RECORD = (
    "The retained acquisition record for that earlier answer was incomplete or did "
    "not match the response, so I can’t safely describe what was checked. I did not "
    "perform a new verification."
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


@dataclass(frozen=True)
class ClaimExplanationIntent:
    mode: Literal["latest", "quoted_anchor"]
    target_anchor: str | None = None
    explanation_kind: Literal["support", "acquisition"] = "support"
    acquisition_question: Literal["checked", "coverage", "gaps"] | None = None


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


@dataclass(frozen=True)
class AcquisitionHistory:
    strategy: Literal["targeted_retrieval", "exact_fetch"]
    sufficiency_status: Literal[
        "sufficient_for_declared_scope",
        "sufficient_with_limitations",
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


def _project_acquisition_history(
    trace: Any,
    *,
    record: ClaimRecord,
    owner_id: str,
    conversation_id: str,
    surface: str | None,
) -> AcquisitionHistory | None:
    if (
        not isinstance(trace, dict)
        or trace.get("request_id") != record.request_id
        or trace.get("owner_id") != owner_id
        or trace.get("conversation_id") != conversation_id
        or trace.get("surface") != record.surface
        or (surface is not None and surface != record.surface)
        or trace.get("status") not in {"ok", "degraded"}
    ):
        return None
    prompt = trace.get("prompt")
    manifest = (
        prompt.get("evidence_acquisition")
        if isinstance(prompt, dict)
        else None
    )
    if not isinstance(manifest, dict):
        return None
    accepted_statuses = {
        "sufficient_for_declared_scope",
        "sufficient_with_limitations",
    }
    plan = manifest.get("plan")
    sufficiency = manifest.get("sufficiency")
    shape = manifest.get("shape")
    inventory = manifest.get("inventory")
    acquisition = manifest.get("acquisition")
    status = manifest.get("status")
    if (
        manifest.get("manifest_id") != record.acquisition_manifest_id
        or manifest.get("assistant_message_id") != record.assistant_message_id
        or manifest.get("response_digest") != record.claim_anchor_digest
        or manifest.get("attempted") is not True
        or status not in accepted_statuses
        or not isinstance(plan, dict)
        or plan.get("plan_status") not in {"ready", "ready_with_limitations"}
        or not isinstance(sufficiency, dict)
        or sufficiency.get("status") not in accepted_statuses
        or sufficiency.get("status") != status
        or not isinstance(shape, dict)
        or shape.get("derivation_status") != "derived"
        or shape.get("task_shape") != "targeted_lookup"
        or shape.get("clarification_required") is not False
        or not isinstance(inventory, dict)
        or not isinstance(acquisition, dict)
    ):
        return None

    selected_strategies = plan.get("selected_strategies")
    strategy = acquisition.get("strategy_attempted")
    if (
        not isinstance(selected_strategies, list)
        or len(selected_strategies) != 1
        or selected_strategies[0] not in {"targeted_retrieval", "exact_fetch"}
        or strategy != selected_strategies[0]
        or not isinstance(plan.get("contradiction_search_required"), bool)
        or plan.get("completeness_expectation") != "targeted_scope"
    ):
        return None

    limitation_codes = plan.get("limitation_codes")
    if (
        not isinstance(limitation_codes, list)
        or len(limitation_codes) > 16
        or any(code not in _PLAN_LIMITATION_CODES for code in limitation_codes)
        or len(limitation_codes) != len(set(limitation_codes))
    ):
        return None
    inventory_status = inventory.get("inventory_status")
    if inventory_status not in {
        "complete_for_declared_scope",
        "partial",
        "unknown",
        "unavailable",
    }:
        return None

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
            return None
        inventory_counts[field] = count

    suppressed = acquisition.get("source_identifiers_suppressed", False)
    if not isinstance(suppressed, bool):
        return None
    identity_values: dict[str, tuple[int, set[str] | None]] = {}
    for field in _MANIFEST_IDENTITY_FIELDS:
        projected = _identity_projection(
            acquisition,
            field=field,
            suppressed=suppressed,
            maximum=64,
        )
        if projected is None:
            return None
        identity_values[field] = projected

    attempts = _exact_attempt_projection(acquisition, suppressed=suppressed)
    if attempts is None:
        return None
    exact_attempt_count, exact_outcomes = attempts

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
    if (
        selected_count > considered_count
        or used_count > selected_count
        or retained_count > returned_count
        or omitted_count != returned_count - retained_count
        or attempted_count != exact_attempt_count
        or unsuccessful_count
        != exact_attempt_count - exact_outcomes["satisfied"]
    ):
        return None
    if not suppressed and (
        not selected_values.issubset(considered_values)
        or not used_values.issubset(selected_values)
        or not retained_values.issubset(returned_values)
        or omitted_values != returned_values - retained_values
        or unsuccessful_values != attempted_values - returned_values
    ):
        return None

    acquisition_counts: dict[str, int] = {}
    for field in (
        "item_count",
        "usable_item_count",
        "prompt_retained_item_count",
    ):
        count = _bounded_count(acquisition.get(field), maximum=10000)
        if count is None:
            return None
        acquisition_counts[field] = count
    if (
        acquisition_counts["usable_item_count"] > acquisition_counts["item_count"]
        or acquisition_counts["prompt_retained_item_count"]
        > acquisition_counts["usable_item_count"]
        or returned_count != acquisition_counts["usable_item_count"]
        or retained_count != acquisition_counts["prompt_retained_item_count"]
        or acquisition.get("context_delivery_status") != "retained"
        or not isinstance(acquisition.get("dsa_budget_truncation"), bool)
        or not isinstance(acquisition.get("candidate_truncation"), bool)
        or not isinstance(sufficiency.get("qualification_required"), bool)
        or not isinstance(sufficiency.get("additional_acquisition_required"), bool)
        or (strategy == "targeted_retrieval" and exact_attempt_count != 0)
        or (strategy == "exact_fetch" and exact_attempt_count == 0)
    ):
        return None

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
        "unavailable_sources": identity_values["unavailable_source_ids"][0],
        "failed_sources": identity_values["failed_source_ids"][0],
        **acquisition_counts,
    }
    return AcquisitionHistory(
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
    )


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
) -> str:
    counts = history.counts
    sentences: list[str] = []
    if question == "coverage":
        sentences.append("No—not universally.")
    elif question == "gaps":
        sentences.append(
            "The retained record cannot identify unknown evidence outside its declared "
            "source scope."
        )

    if history.strategy == "targeted_retrieval":
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
    else:
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

    if history.sufficiency_status == "sufficient_for_declared_scope":
        sentences.append(f"The recorded evidence was sufficient for {sufficient_scope}.")
    else:
        sentences.append(
            "The recorded evidence was sufficient only with recorded limitations."
        )
    sentences.extend(_limitation_sentences(history))
    sentences.append(boundary)
    sentences.append(_NO_NEW_VERIFICATION)
    return " ".join(sentences)


async def resolve_claim_explanation(
    *,
    enabled: bool,
    messages: Any,
    memory_store: Any,
    owner_id: str,
    conversation_id: str,
    surface: str | None = None,
) -> ClaimExplanationOutcome:
    if not enabled:
        return ClaimExplanationOutcome(False, None, None, {})
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
    acquisition_trace = {
        "explanation_kind": "acquisition",
        "acquisition_question": intent.acquisition_question,
        "claim_record_lookup_status": "not_requested",
        "acquisition_trace_lookup_status": "not_requested",
        "manifest_resolution_status": "not_requested",
        "acquisition_status": "not_resolved",
        "aggregate_counts": {},
    } if acquisition_explanation else {}

    target_mode: Literal["immediate_previous", "quoted_anchor"] = (
        "immediate_previous" if intent.mode == "latest" else "quoted_anchor"
    )
    prior_answer = None
    if intent.mode == "latest":
        prior_answer = _prior_assistant(messages, intent)
        if prior_answer is None:
            return _fallback(
                (
                    _ACQUISITION_TARGET_UNAVAILABLE
                    if acquisition_explanation
                    else _TARGET_UNAVAILABLE
                ),
                "prior_assistant_unavailable",
                target_mode=target_mode,
                **acquisition_trace,
            )
    elif intent.target_anchor is None:
        return _fallback(
            (
                _ACQUISITION_TARGET_UNAVAILABLE
                if acquisition_explanation
                else _TARGET_UNAVAILABLE
            ),
            "quoted_target_invalid",
            target_mode=target_mode,
            target_status="invalid",
            **acquisition_trace,
        )

    try:
        payload = await memory_store.list_claim_records(
            owner_id=owner_id,
            conversation_id=conversation_id,
            limit=20,
        )
    except Exception:
        return _fallback(
            (
                _ACQUISITION_DEPENDENCY_UNAVAILABLE
                if acquisition_explanation
                else _DEPENDENCY_UNAVAILABLE
            ),
            "claim_records_unavailable",
            target_mode=target_mode,
            target_status="resolved",
            lookup_status="failed",
            storage_call_count=1,
            **(
                {
                    **acquisition_trace,
                    "claim_record_lookup_status": "failed",
                }
                if acquisition_explanation
                else {}
            ),
        )

    try:
        response = ClaimRecordListResponse.model_validate(payload)
    except ValidationError:
        return _fallback(
            (
                _INVALID_ACQUISITION_RECORD
                if acquisition_explanation
                else _INVALID_RECORD
            ),
            "claim_record_response_invalid",
            target_mode=target_mode,
            target_status="resolved",
            lookup_status="completed",
            storage_call_count=1,
            **(
                {
                    **acquisition_trace,
                    "claim_record_lookup_status": "completed",
                    "manifest_resolution_status": "invalid",
                    "acquisition_status": "invalid",
                }
                if acquisition_explanation
                else {}
            ),
        )

    records = response.records
    if not records:
        if intent.mode == "quoted_anchor":
            return _fallback(
                (
                    _ACQUISITION_QUOTED_NO_RECORD
                    if acquisition_explanation
                    else _QUOTED_NO_RECORD
                ),
                "quoted_claim_record_not_found",
                target_mode=target_mode,
                target_status="resolved",
                lookup_status="completed",
                resolution_status="no_record",
                storage_call_count=1,
                **(
                    {
                        **acquisition_trace,
                        "claim_record_lookup_status": "completed",
                    }
                    if acquisition_explanation
                    else {}
                ),
            )
        return _fallback(
            _ACQUISITION_NO_RECORD if acquisition_explanation else _NO_RECORD,
            "no_claim_records",
            target_mode=target_mode,
            target_status="resolved",
            lookup_status="completed",
            resolution_status="no_record",
            storage_call_count=1,
            **(
                {
                    **acquisition_trace,
                    "claim_record_lookup_status": "completed",
                }
                if acquisition_explanation
                else {}
            ),
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
            (
                _INVALID_ACQUISITION_RECORD
                if acquisition_explanation
                else _INVALID_RECORD
            ),
            "record_invalid",
            target_mode=target_mode,
            target_status="resolved",
            lookup_status="completed",
            resolution_status="invalid",
            storage_call_count=1,
            record_count=len(records),
            **(
                {
                    **acquisition_trace,
                    "claim_record_lookup_status": "completed",
                    "manifest_resolution_status": "invalid",
                    "acquisition_status": "invalid",
                }
                if acquisition_explanation
                else {}
            ),
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
                (
                    _ACQUISITION_QUOTED_NO_RECORD
                    if acquisition_explanation
                    else _QUOTED_NO_RECORD
                ),
                "quoted_claim_record_not_found",
                resolution_status="no_record",
                matched_record_count=0,
                **counts,
                **(
                    {
                        **acquisition_trace,
                        "claim_record_lookup_status": "completed",
                    }
                    if acquisition_explanation
                    else {}
                ),
            )
        if len(matching_records) > 1:
            return _fallback(
                _ACQUISITION_AMBIGUOUS if acquisition_explanation else _QUOTED_AMBIGUOUS,
                "ambiguous_quoted_claim",
                resolution_status="ambiguous",
                matched_record_count=len(matching_records),
                **counts,
                **(
                    {
                        **acquisition_trace,
                        "claim_record_lookup_status": "completed",
                    }
                    if acquisition_explanation
                    else {}
                ),
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
                _ACQUISITION_AMBIGUOUS if acquisition_explanation else _AMBIGUOUS,
                "ambiguous_latest_response",
                resolution_status="ambiguous",
                **counts,
                **(
                    {
                        **acquisition_trace,
                        "claim_record_lookup_status": "completed",
                    }
                    if acquisition_explanation
                    else {}
                ),
            )

        record = newest_group[0]
        if record.claim_anchor != prior_answer:
            return _fallback(
                _ACQUISITION_NO_RECORD if acquisition_explanation else _NO_RECORD,
                "no_record_for_latest_response",
                resolution_status="no_record",
                **counts,
                **(
                    {
                        **acquisition_trace,
                        "claim_record_lookup_status": "completed",
                    }
                    if acquisition_explanation
                    else {}
                ),
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
            (
                _INVALID_ACQUISITION_RECORD
                if acquisition_explanation
                else _INVALID_RECORD
            ),
            reason,
            resolution_status=support_status,
            matched_record_count=1,
            **counts,
            **(
                {
                    **acquisition_trace,
                    "claim_record_lookup_status": "completed",
                    "manifest_resolution_status": "invalid",
                    "acquisition_status": "invalid",
                }
                if acquisition_explanation
                else {}
            ),
        )

    if acquisition_explanation:
        if record.acquisition_manifest_id is None:
            return _fallback(
                _NO_LINKED_MANIFEST,
                "acquisition_manifest_not_linked",
                resolution_status="resolved",
                matched_record_count=1,
                **counts,
                **{
                    **acquisition_trace,
                    "claim_record_lookup_status": "completed",
                    "manifest_resolution_status": "not_linked",
                    "acquisition_status": "unavailable",
                },
            )
        try:
            retained_trace = await memory_store.get_trace(record.request_id)
        except Exception:
            return _fallback(
                _TRACE_UNAVAILABLE,
                "acquisition_trace_unavailable",
                resolution_status="resolved",
                matched_record_count=1,
                storage_call_count=2,
                **{
                    key: value
                    for key, value in counts.items()
                    if key != "storage_call_count"
                },
                **{
                    **acquisition_trace,
                    "claim_record_lookup_status": "completed",
                    "acquisition_trace_lookup_status": "failed",
                    "manifest_resolution_status": "unavailable",
                    "acquisition_status": "unavailable",
                },
            )
        history = _project_acquisition_history(
            retained_trace,
            record=record,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
        )
        if history is None:
            return _fallback(
                _INVALID_ACQUISITION_RECORD,
                "acquisition_manifest_invalid",
                resolution_status="resolved",
                matched_record_count=1,
                storage_call_count=2,
                **{
                    key: value
                    for key, value in counts.items()
                    if key != "storage_call_count"
                },
                **{
                    **acquisition_trace,
                    "claim_record_lookup_status": "completed",
                    "acquisition_trace_lookup_status": "completed",
                    "manifest_resolution_status": "invalid",
                    "acquisition_status": "invalid",
                },
            )
        return ClaimExplanationOutcome(
            handled=True,
            answer=_render_acquisition(
                history,
                intent.acquisition_question or "checked",
            ),
            status="ok",
            trace=_trace(
                reason_code=(
                    "latest_acquisition_record_resolved"
                    if intent.mode == "latest"
                    else "quoted_acquisition_record_resolved"
                ),
                resolution_status="resolved",
                render_status="completed",
                matched_record_count=1,
                claim_id=record.claim_id,
                claim_anchor_digest=record.claim_anchor_digest,
                storage_call_count=2,
                **{
                    key: value
                    for key, value in counts.items()
                    if key != "storage_call_count"
                },
                **{
                    **acquisition_trace,
                    "claim_record_lookup_status": "completed",
                    "acquisition_trace_lookup_status": "completed",
                    "manifest_resolution_status": "resolved",
                    "acquisition_status": history.sufficiency_status,
                    "aggregate_counts": history.counts,
                },
            ),
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
