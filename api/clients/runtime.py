from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator
from services.privacy_context import validate_privacy_policy_result

_PREFERRED_COMPANION_COMPILE_PATH = "/v1/companion/profile/compile"
_COMPAT_COMPANION_COMPILE_PATH = "/v1/companion/policy/compile"
_COMPANION_ENDPOINT_KEY = "_cognitive_runtime_compile_endpoint"

_HISTORY_INTENTS = {
    "not_history_followup",
    "support_explanation",
    "acquisition_checked",
    "acquisition_coverage",
    "acquisition_gaps",
    "new_verification_request",
    "ambiguous_history_followup",
}
_HISTORY_REASON_CODES = {
    "no_candidate",
    "not_history_candidate",
    "ambiguous_candidate",
    "deterministic_candidate_accepted",
    "classifier_candidate_accepted",
    "classifier_confidence_requires_clarification",
    "classifier_confidence_rejected",
    "explicit_reference_routed",
}
_HISTORY_PROJECTION = {
    "support_explanation": ("support", None),
    "acquisition_checked": ("acquisition", "checked"),
    "acquisition_coverage": ("acquisition", "coverage"),
    "acquisition_gaps": ("acquisition", "gaps"),
    "new_verification_request": ("support", None),
}

_CONTINUATION_TIMING = {
    "resume": "resume_previous_thread",
    "create_new": "answer_now",
    "clarify": "ask_clarifying_question",
    "wait": "pause_or_wait",
    "decline": "close_turn",
}
_CONTINUATION_REASONS = {
    "candidate_set_incomplete",
    "no_candidates",
    "one_eligible_candidate",
    "multiple_eligible_candidates",
    "active_thread_present",
    "contended_thread_present",
    "unavailable_thread_present",
    "runtime_state_missing",
    "runtime_state_inconsistent",
    "runtime_session_missing",
    "candidate_stale",
    "candidate_not_open",
    "no_eligible_candidates",
}
_CONTINUATION_CREATE_NEW_REASONS = (
    "candidate_not_open",
    "runtime_state_missing",
    "runtime_session_missing",
    "candidate_stale",
)
_CONTINUATION_DECLINE_REASONS = (
    "contended_thread_present",
    "unavailable_thread_present",
    "runtime_state_inconsistent",
)


class _ClaimSupportExclusion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_ref_id: str = Field(min_length=1, max_length=120)
    reason: str = Field(min_length=1, max_length=240)


_ClaimSupportIdentifier = Annotated[
    str,
    Field(min_length=1, max_length=120, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"),
]
_ClaimSupportLimitation = Annotated[
    str,
    Field(min_length=1, max_length=80, pattern=r"^[a-z][a-z0-9_]*$"),
]


class _ClaimSupportResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_id: str = Field(min_length=1, max_length=120)
    claim_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    calibration_status: Literal["supported", "limited", "unsupported"]
    conclusion_disposition: Literal["allowed", "qualified", "withheld"]
    qualification_required: bool
    limitation_codes: list[_ClaimSupportLimitation] = Field(max_length=17)
    validated_supporting_evidence_ref_ids: list[_ClaimSupportIdentifier] = Field(
        max_length=16
    )
    validated_counterevidence_ref_ids: list[_ClaimSupportIdentifier] = Field(
        max_length=16
    )
    validated_material_exclusions: list[_ClaimSupportExclusion] = Field(max_length=16)
    validated_executed_derivation_ref_ids: list[_ClaimSupportIdentifier] = Field(
        max_length=16
    )
    user_safe_summary: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_bounded_sets(self):
        collections = (
            self.limitation_codes,
            self.validated_supporting_evidence_ref_ids,
            self.validated_counterevidence_ref_ids,
            self.validated_executed_derivation_ref_ids,
        )
        if any(len(items) != len(set(items)) for items in collections):
            raise ValueError("claim_support_duplicate_reference")
        if set(self.validated_supporting_evidence_ref_ids) & set(
            self.validated_counterevidence_ref_ids
        ):
            raise ValueError("claim_support_conflicting_reference_role")
        return self


class _ClaimSupportResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    owner_id: str
    conversation_id: str
    surface: str
    runtime_session_id: str
    runtime_turn_id: str
    result: _ClaimSupportResult


_RETIREMENT_REASONS = {
    "safe_idle_retirement_reserved",
    "existing_retirement_reservation",
    "candidate_not_open",
    "durable_activity_not_over_horizon",
    "runtime_activity_not_over_horizon",
    "runtime_state_missing",
    "runtime_state_inconsistent",
    "runtime_thread_active",
    "runtime_thread_contended",
    "runtime_thread_unavailable",
}
_RETIREMENT_RESERVED_REASONS = {
    "safe_idle_retirement_reserved",
    "existing_retirement_reservation",
}

_SITUATED_VISIBILITY = {"private", "shared", "public", "unknown"}
_SITUATED_CONSTRAINT = {"normal", "constrained", "unknown"}
_SITUATED_INTERACTION_KINDS = {
    "command",
    "question",
    "brainstorm",
    "joke_or_playful",
    "vent_or_expression",
    "mistake_or_failure_report",
    "tense_debugging",
    "high_impact_decision",
    "ambiguous",
}
_SITUATED_TENSION_LEVELS = {"low", "medium", "high"}
_SITUATED_PRIVACY_HINTS = {"normal", "private", "sensitive"}
_SITUATED_RESPONSE_POSTURES = {
    "direct",
    "supportive",
    "tactical",
    "brief",
    "reflective",
    "playful",
    "silent_or_minimal",
}
_SITUATED_RESTRAINT_POLICIES = {
    "answer_normally",
    "short_answer",
    "defer_expansion",
    "ask_clarifying_question",
    "do_not_retrieve",
    "do_not_personalize",
    "suppress_proactive_output",
}
_SITUATED_ATTUNEMENT = {"none", "minimal", "brief"}
_SITUATED_CHALLENGE = {"none", "low", "medium"}
_SITUATED_REASON_ORDER = (
    "upstream_confidence_insufficient",
    "tense_context",
    "tactical_response_required",
    "high_impact_context",
    "brief_steadying_allowed",
    "light_commentary_allowed",
    "low_risk_commentary_allowed",
    "ambiguous_context",
    "surface_public",
    "surface_shared",
    "surface_visibility_unknown",
    "surface_constrained",
    "surface_constraint_unknown",
    "privacy_sensitive",
    "proactive_output_suppressed",
    "personalization_suppressed",
    "confirmation_required",
    "upstream_commentary_suppressed",
    "upstream_humor_suppressed",
    "brevity_preferred",
    "clarification_preferred",
)


def _strict_situated_projection(
    value: Any,
    *,
    expected_keys: set[str],
    labels: dict[str, set[str]],
    booleans: set[str],
    confidence: bool,
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != expected_keys:
        raise ValueError("situated_presence_request_invalid")
    for field, allowed in labels.items():
        if not isinstance(value.get(field), str) or value[field] not in allowed:
            raise ValueError("situated_presence_request_invalid")
    if any(type(value.get(field)) is not bool for field in booleans):
        raise ValueError("situated_presence_request_invalid")
    if confidence and (
        not isinstance(value.get("confidence"), float)
        or not 0.0 <= value["confidence"] <= 1.0
    ):
        raise ValueError("situated_presence_request_invalid")
    return dict(value)


def _ordered_situated_reasons(reasons: set[str]) -> list[str]:
    if not reasons:
        reasons.add("ambiguous_context")
    return [reason for reason in _SITUATED_REASON_ORDER if reason in reasons][:8]


def _situated_context_reasons(
    *,
    surface_context: dict[str, str],
    governance: dict[str, Any],
    restraint: dict[str, Any],
) -> set[str]:
    reasons: set[str] = set()
    visibility_reason = {
        "public": "surface_public",
        "shared": "surface_shared",
        "unknown": "surface_visibility_unknown",
    }
    constraint_reason = {
        "constrained": "surface_constrained",
        "unknown": "surface_constraint_unknown",
    }
    if reason := visibility_reason.get(surface_context["visibility"]):
        reasons.add(reason)
    if reason := constraint_reason.get(surface_context["constraint"]):
        reasons.add(reason)
    if not governance["commentary_allowed"]:
        reasons.add("upstream_commentary_suppressed")
    if not governance["humor_allowed"]:
        reasons.add("upstream_humor_suppressed")
    if governance["privacy_sensitivity_hint"] != "normal":
        reasons.add("privacy_sensitive")
    if governance["requires_confirmation"]:
        reasons.add("confirmation_required")
    if restraint["proactive_output_suppressed"]:
        reasons.add("proactive_output_suppressed")
    if restraint["personalization_suppressed"]:
        reasons.add("personalization_suppressed")
    if restraint["brevity_preferred"]:
        reasons.add("brevity_preferred")
    if restraint["clarification_preferred"]:
        reasons.add("clarification_preferred")
    return reasons


def _situated_brief_or_upstream_posture(
    *, governance: dict[str, Any], restraint: dict[str, Any]
) -> str:
    posture = governance["response_posture"]
    if restraint["brevity_preferred"] and posture not in {"direct", "tactical"}:
        return "brief"
    return posture


def _situated_presence_v1_result_is_coherent(
    *,
    result: dict[str, Any],
    surface_context: dict[str, str],
    governance: dict[str, Any],
    restraint: dict[str, Any],
) -> bool:
    """Compare a response with the pinned contract without producing a substitute."""
    surface_allows = (
        surface_context["visibility"] == "private"
        and surface_context["constraint"] == "normal"
    )
    common = {
        "surface_allows_commentary": surface_allows,
        "action_implication_allowed": False,
        "policy_version": "situated-presence.v1",
    }
    if min(governance["confidence"], restraint["confidence"]) < 0.60:
        return result == {
            "commentary_allowed": False,
            "humor_allowed": False,
            "emotional_attunement_allowed": "none",
            "challenge_allowed": "none",
            "silence_preferred": True,
            **common,
            "response_posture": "silent_or_minimal",
            "reason_summary": ["upstream_confidence_insufficient"],
        }

    reasons = _situated_context_reasons(
        surface_context=surface_context,
        governance=governance,
        restraint=restraint,
    )
    kind = governance["interaction_kind"]
    commentary = (
        governance["commentary_allowed"]
        and surface_allows
        and governance["tension_level"] == "low"
        and kind not in {"tense_debugging", "high_impact_decision", "ambiguous"}
        and governance["privacy_sensitivity_hint"] == "normal"
        and not governance["requires_confirmation"]
        and not restraint["clarification_preferred"]
    )

    if kind == "tense_debugging" or governance["tension_level"] == "high":
        reasons.update({"tense_context", "tactical_response_required"})
        return result == {
            "commentary_allowed": False,
            "humor_allowed": False,
            "emotional_attunement_allowed": (
                "minimal"
                if governance["privacy_sensitivity_hint"] == "normal"
                and not restraint["personalization_suppressed"]
                else "none"
            ),
            "challenge_allowed": "medium",
            "silence_preferred": False,
            **common,
            "response_posture": "tactical",
            "reason_summary": _ordered_situated_reasons(reasons),
        }

    if kind == "high_impact_decision":
        reasons.add("high_impact_context")
        return result == {
            "commentary_allowed": False,
            "humor_allowed": False,
            "emotional_attunement_allowed": (
                "minimal"
                if governance["privacy_sensitivity_hint"] == "normal"
                and not restraint["personalization_suppressed"]
                else "none"
            ),
            "challenge_allowed": "low",
            "silence_preferred": False,
            **common,
            "response_posture": (
                "brief"
                if restraint["brevity_preferred"]
                or governance["response_posture"] in {"brief", "silent_or_minimal"}
                else "direct"
            ),
            "reason_summary": _ordered_situated_reasons(reasons),
        }

    if kind in {"vent_or_expression", "mistake_or_failure_report"}:
        attunement_allowed = (
            surface_allows
            and governance["privacy_sensitivity_hint"] in {"normal", "private"}
            and not restraint["clarification_preferred"]
        )
        if attunement_allowed:
            attunement = "brief"
            reasons.add("brief_steadying_allowed")
        elif (
            governance["privacy_sensitivity_hint"] == "sensitive"
            or restraint["clarification_preferred"]
        ):
            attunement = "none"
        else:
            attunement = "minimal"
        return result == {
            "commentary_allowed": False,
            "humor_allowed": False,
            "emotional_attunement_allowed": attunement,
            "challenge_allowed": (
                "low" if kind == "mistake_or_failure_report" else "none"
            ),
            "silence_preferred": False,
            **common,
            "response_posture": (
                "brief"
                if restraint["brevity_preferred"]
                or restraint["personalization_suppressed"]
                or restraint["proactive_output_suppressed"]
                or governance["privacy_sensitivity_hint"] != "normal"
                or not surface_allows
                else "supportive"
            ),
            "reason_summary": _ordered_situated_reasons(reasons),
        }

    if kind == "joke_or_playful":
        humor = commentary and governance["humor_allowed"]
        if humor:
            reasons.add("light_commentary_allowed")
        elif commentary:
            reasons.add("low_risk_commentary_allowed")
        silence = not commentary and (
            restraint["clarification_preferred"] or not surface_allows
        )
        return result == {
            "commentary_allowed": commentary,
            "humor_allowed": humor,
            "emotional_attunement_allowed": "none",
            "challenge_allowed": "low" if commentary else "none",
            "silence_preferred": silence,
            **common,
            "response_posture": (
                "brief"
                if commentary and restraint["brevity_preferred"]
                else "playful"
                if commentary
                else "silent_or_minimal"
                if silence
                else _situated_brief_or_upstream_posture(
                    governance=governance, restraint=restraint
                )
            ),
            "reason_summary": _ordered_situated_reasons(reasons),
        }

    if kind == "ambiguous":
        reasons.add("ambiguous_context")
        return result == {
            "commentary_allowed": False,
            "humor_allowed": False,
            "emotional_attunement_allowed": "none",
            "challenge_allowed": "none",
            "silence_preferred": True,
            **common,
            "response_posture": "silent_or_minimal",
            "reason_summary": _ordered_situated_reasons(reasons),
        }

    if commentary:
        reasons.add("low_risk_commentary_allowed")
    silence = (
        restraint["proactive_output_suppressed"]
        and governance["response_posture"] == "silent_or_minimal"
    )
    return result == {
        "commentary_allowed": commentary,
        "humor_allowed": False,
        "emotional_attunement_allowed": "none",
        "challenge_allowed": "low" if kind == "brainstorm" else "none",
        "silence_preferred": silence,
        **common,
        "response_posture": (
            "silent_or_minimal"
            if silence
            else _situated_brief_or_upstream_posture(
                governance=governance, restraint=restraint
            )
        ),
        "reason_summary": _ordered_situated_reasons(reasons),
    }


def _validate_situated_presence_response(
    response: Any,
    *,
    scope: dict[str, str],
    surface_context: dict[str, str],
    governance: dict[str, Any],
    restraint: dict[str, Any],
) -> dict[str, Any]:
    expected_top = {"schema_version", *scope, "result"}
    if not isinstance(response, dict) or set(response) != expected_top:
        raise RuntimeError("situated_presence_response_invalid")
    if response.get("schema_version") != "situated-presence.v1":
        raise RuntimeError("situated_presence_response_invalid")
    if any(response.get(field) != value for field, value in scope.items()):
        raise RuntimeError("situated_presence_response_context_mismatch")

    result = response.get("result")
    expected_result = {
        "commentary_allowed",
        "humor_allowed",
        "emotional_attunement_allowed",
        "challenge_allowed",
        "silence_preferred",
        "surface_allows_commentary",
        "response_posture",
        "action_implication_allowed",
        "reason_summary",
        "policy_version",
    }
    if not isinstance(result, dict) or set(result) != expected_result:
        raise RuntimeError("situated_presence_response_invalid")
    bool_fields = (
        "commentary_allowed",
        "humor_allowed",
        "silence_preferred",
        "surface_allows_commentary",
        "action_implication_allowed",
    )
    if any(type(result.get(field)) is not bool for field in bool_fields):
        raise RuntimeError("situated_presence_response_invalid")
    if (
        result.get("emotional_attunement_allowed") not in _SITUATED_ATTUNEMENT
        or result.get("challenge_allowed") not in _SITUATED_CHALLENGE
        or result.get("response_posture") not in _SITUATED_RESPONSE_POSTURES
        or result.get("policy_version") != "situated-presence.v1"
        or result.get("action_implication_allowed") is not False
    ):
        raise RuntimeError("situated_presence_response_invalid")
    reasons = result.get("reason_summary")
    if (
        not isinstance(reasons, list)
        or not 1 <= len(reasons) <= 8
        or any(not isinstance(reason, str) for reason in reasons)
        or len(reasons) != len(set(reasons))
        or reasons != [reason for reason in _SITUATED_REASON_ORDER if reason in reasons]
    ):
        raise RuntimeError("situated_presence_response_invalid")

    if not _situated_presence_v1_result_is_coherent(
        result=result,
        surface_context=surface_context,
        governance=governance,
        restraint=restraint,
    ):
        raise RuntimeError("situated_presence_response_invalid")
    return response


def _continuation_outcome_is_coherent(
    *,
    outcome: str,
    candidate_count: int,
    eligible_count: int,
    reason_codes: list[str],
) -> bool:
    if outcome == "resume":
        return eligible_count == 1 and reason_codes == ["one_eligible_candidate"]
    if outcome == "create_new":
        if eligible_count != 0:
            return False
        if candidate_count == 0:
            return reason_codes == ["no_candidates"]
        if reason_codes[0] != "no_eligible_candidates":
            return False
        detail_reasons = reason_codes[1:]
        return detail_reasons == [
            reason
            for reason in _CONTINUATION_CREATE_NEW_REASONS
            if reason in detail_reasons
        ]
    if outcome == "clarify":
        return (
            eligible_count == 0
            and reason_codes == ["candidate_set_incomplete"]
        ) or (
            eligible_count >= 2
            and reason_codes == ["multiple_eligible_candidates"]
        )
    if outcome == "wait":
        return reason_codes == ["active_thread_present"]
    if outcome == "decline":
        return reason_codes == [
            reason for reason in _CONTINUATION_DECLINE_REASONS if reason in reason_codes
        ]
    return False


def _bounded_runtime_identifier(value: Any) -> bool:
    return isinstance(value, str) and bool(value) and len(value) <= 120


def _parse_aware_runtime_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise RuntimeError("runtime_timestamp_invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise RuntimeError("runtime_timestamp_invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RuntimeError("runtime_timestamp_invalid")
    return parsed


def _require_aware_runtime_datetime(value: datetime) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("runtime_timestamp_timezone_required")


def _optional_uuid_ids_equivalent(actual: Any, expected: str | None) -> bool:
    if expected is None:
        return actual is None
    if not isinstance(actual, str):
        return False
    if actual == expected:
        return True
    try:
        return UUID(actual) == UUID(expected)
    except (TypeError, ValueError):
        return False


class _HistoryFollowupPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    status: str
    intent: str | None = None
    candidate_source: str | None = None
    target_mode: str | None = None
    explanation_kind: str | None = None
    acquisition_question: str | None = None
    history_lookup_allowed: bool
    new_verification_requested: bool
    new_verification_allowed_after_history_resolution: bool
    clarification_required: bool
    confidence_band: str
    reason_codes: list[str] = Field(min_length=1, max_length=4)

    @model_validator(mode="after")
    def validate_closed_policy(self):
        if self.status not in {
            "not_applicable",
            "accepted",
            "clarification_required",
            "rejected",
            "explicit_reference",
        }:
            raise ValueError("history_policy_status_invalid")
        if self.intent is not None and self.intent not in _HISTORY_INTENTS:
            raise ValueError("history_policy_intent_invalid")
        if self.candidate_source not in {None, "deterministic", "classifier"}:
            raise ValueError("history_policy_source_invalid")
        if self.target_mode not in {None, "immediate_previous", "explicit_reference"}:
            raise ValueError("history_policy_target_invalid")
        if self.explanation_kind not in {None, "support", "acquisition"}:
            raise ValueError("history_policy_explanation_kind_invalid")
        if self.acquisition_question not in {None, "checked", "coverage", "gaps"}:
            raise ValueError("history_policy_acquisition_question_invalid")
        if self.confidence_band not in {"not_applicable", "low", "medium", "high"}:
            raise ValueError("history_policy_confidence_band_invalid")
        if any(code not in _HISTORY_REASON_CODES for code in self.reason_codes):
            raise ValueError("history_policy_reason_code_invalid")
        if len(self.reason_codes) != len(set(self.reason_codes)):
            raise ValueError("history_policy_reason_codes_duplicate")
        projection = _HISTORY_PROJECTION.get(self.intent or "")
        if projection is None:
            if self.explanation_kind is not None or self.acquisition_question is not None:
                raise ValueError("history_policy_projection_invalid")
        elif (self.explanation_kind, self.acquisition_question) != projection:
            raise ValueError("history_policy_projection_invalid")
        if self.intent is not None and (
            self.candidate_source is None or self.target_mode is None
        ):
            raise ValueError("history_policy_candidate_projection_incomplete")
        if self.intent == "new_verification_request" and not self.new_verification_requested:
            raise ValueError("history_policy_verification_intent_inconsistent")
        if (
            self.intent in {"not_history_followup", "ambiguous_history_followup"}
            and self.new_verification_requested
        ):
            raise ValueError("history_policy_nonactionable_verification_inconsistent")
        if self.status == "accepted":
            expected_reason = (
                "deterministic_candidate_accepted"
                if self.candidate_source == "deterministic"
                else "classifier_candidate_accepted"
            )
            if (
                self.intent not in _HISTORY_PROJECTION
                or self.target_mode != "immediate_previous"
                or not self.history_lookup_allowed
                or self.clarification_required
                or self.confidence_band != "high"
                or self.new_verification_allowed_after_history_resolution
                != self.new_verification_requested
                or self.reason_codes != [expected_reason]
            ):
                raise ValueError("accepted_history_policy_inconsistent")
        elif self.status == "clarification_required":
            expected_reason = (
                "ambiguous_candidate"
                if self.intent == "ambiguous_history_followup"
                else "classifier_confidence_requires_clarification"
            )
            if (
                self.history_lookup_allowed
                or not self.clarification_required
                or self.new_verification_allowed_after_history_resolution
                or self.reason_codes != [expected_reason]
                or (
                    self.intent != "ambiguous_history_followup"
                    and (
                        self.candidate_source != "classifier"
                        or self.confidence_band != "medium"
                    )
                )
            ):
                raise ValueError("clarification_history_policy_inconsistent")
        elif (
            self.history_lookup_allowed
            or self.clarification_required
            or self.new_verification_allowed_after_history_resolution
        ):
            raise ValueError("nonaccepted_history_policy_inconsistent")
        if self.status == "explicit_reference" and self.target_mode != "explicit_reference":
            raise ValueError("explicit_reference_history_policy_inconsistent")
        if self.status == "explicit_reference" and self.reason_codes != [
            "explicit_reference_routed"
        ]:
            raise ValueError("explicit_reference_history_policy_inconsistent")
        if self.status == "rejected" and (
            self.candidate_source != "classifier"
            or self.confidence_band != "low"
            or self.reason_codes != ["classifier_confidence_rejected"]
        ):
            raise ValueError("rejected_history_policy_inconsistent")
        if self.status == "not_applicable" and (
            self.intent != "not_history_followup"
            or self.reason_codes != ["not_history_candidate"]
        ):
            raise ValueError("not_applicable_history_policy_inconsistent")
        return self


def validate_history_followup_policy_response(
    response: Any,
    *,
    scope: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(response, dict) or any(
        response.get(field) != value for field, value in scope.items()
    ):
        raise RuntimeError("history_followup_policy_response_context_mismatch")
    result = response.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("history_followup_policy_response_invalid")
    try:
        policy = _HistoryFollowupPolicy.model_validate(
            result.get("history_followup_policy")
        )
    except Exception as exc:
        raise RuntimeError("history_followup_policy_response_invalid") from exc
    return policy.model_dump()


class RuntimeClient:
    def __init__(
        self,
        base_url: str,
        api_key: str | None,
        timeout_ms: int = 30000,
        *,
        max_connections: int = 20,
        max_keepalive_connections: int = 10,
        keepalive_expiry: float = 5.0,
        client_factory: Callable[..., httpx.AsyncClient] | None = None,
    ) -> None:
        if isinstance(max_connections, bool) or not isinstance(max_connections, int):
            raise ValueError("runtime_client_max_connections_invalid")
        if max_connections <= 0:
            raise ValueError("runtime_client_max_connections_invalid")
        if (
            isinstance(max_keepalive_connections, bool)
            or not isinstance(max_keepalive_connections, int)
            or max_keepalive_connections < 0
            or max_keepalive_connections > max_connections
        ):
            raise ValueError("runtime_client_max_keepalive_connections_invalid")
        if (
            isinstance(keepalive_expiry, bool)
            or not isinstance(keepalive_expiry, (int, float))
            or keepalive_expiry <= 0
        ):
            raise ValueError("runtime_client_keepalive_expiry_invalid")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout_ms / 1000
        self.max_connections = max_connections
        self.max_keepalive_connections = max_keepalive_connections
        self.keepalive_expiry = float(keepalive_expiry)
        self.last_companion_compile_endpoint: str | None = None
        self._client_factory = client_factory or httpx.AsyncClient
        self._client: httpx.AsyncClient | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._started = False
        self._closed = False

    def _new_client(self) -> httpx.AsyncClient:
        headers = {"X-API-Key": self.api_key} if self.api_key else None
        return self._client_factory(
            base_url=self.base_url,
            headers=headers,
            timeout=self.timeout,
            limits=httpx.Limits(
                max_connections=self.max_connections,
                max_keepalive_connections=self.max_keepalive_connections,
                keepalive_expiry=self.keepalive_expiry,
            ),
        )

    async def open(self) -> None:
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("runtime_client_closed")
            if self._started:
                return
            if self._client is None:
                self._client = self._new_client()
            self._started = True

    async def close(self) -> None:
        client: httpx.AsyncClient | None = None
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            client = self._client
            self._client = None
        if client is not None:
            await client.aclose()

    async def _client_for_request(self) -> httpx.AsyncClient:
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("runtime_client_closed")
            if not self._started:
                raise RuntimeError("runtime_client_not_started")
            if self._client is None:
                self._client = self._new_client()
            return self._client

    async def _invalidate_client(self, failed_client: httpx.AsyncClient) -> None:
        should_close = False
        async with self._lifecycle_lock:
            if self._client is failed_client:
                self._client = None
                should_close = True
        if should_close:
            await failed_client.aclose()

    async def _post(self, path: str, *, json: dict[str, Any]) -> dict[str, Any]:
        client = await self._client_for_request()
        try:
            resp = await client.post(path, json=json)
            resp.raise_for_status()
            return resp.json()
        except httpx.TransportError:
            try:
                await self._invalidate_client(client)
            except Exception:
                pass
            raise

    async def overlay(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/runtime/overlay",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "surface": surface,
            },
        )

    async def evaluate_claim_calibration(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        claim_anchor: str,
        evidence_references: list[dict[str, Any]],
    ) -> dict[str, Any]:
        scope = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "runtime_session_id": runtime_session_id,
            "runtime_turn_id": runtime_turn_id,
        }
        response = await self._post(
            "/v1/runtime/claim-calibration/evaluate",
            json={
                **scope,
                "claim_anchor": claim_anchor,
                "evidence_references": evidence_references,
            },
        )
        if not isinstance(response, dict) or any(
            response.get(field) != value for field, value in scope.items()
        ):
            raise RuntimeError("claim_calibration_response_invalid")
        return response

    async def evaluate_claim_support(
        self,
        *,
        request_id: str,
        authority_context: dict[str, Any],
        proposal: dict[str, Any],
    ) -> dict[str, Any]:
        response = await self._post(
            "/v1/runtime/claim-support/evaluate",
            json={
                "request_id": request_id,
                "authority_context": authority_context,
                "proposal": proposal,
            },
        )
        try:
            validated = _ClaimSupportResponse.model_validate(response)
        except Exception as exc:
            raise RuntimeError("claim_support_response_invalid") from exc
        expected_scope = {
            "request_id": request_id,
            "owner_id": authority_context.get("owner_id"),
            "conversation_id": authority_context.get("conversation_id"),
            "surface": authority_context.get("surface"),
            "runtime_session_id": authority_context.get("runtime_session_id"),
            "runtime_turn_id": authority_context.get("runtime_turn_id"),
        }
        payload = validated.model_dump(mode="json")
        if any(payload.get(field) != value for field, value in expected_scope.items()):
            raise RuntimeError("claim_support_response_invalid")
        return payload

    async def derive_evidence_shape(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        task_text: str,
        interaction_kind: str,
        task_context: dict[str, Any],
    ) -> dict[str, Any]:
        scope = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "runtime_session_id": runtime_session_id,
            "runtime_turn_id": runtime_turn_id,
        }
        response = await self._post(
            "/v1/runtime/evidence-shapes/derive",
            json={
                **scope,
                "task_text": task_text,
                "interaction_kind": interaction_kind,
                "task_context": task_context,
            },
        )
        if not isinstance(response, dict) or any(
            response.get(field) != value for field, value in scope.items()
        ):
            raise RuntimeError("evidence_shape_response_invalid")
        return response

    async def compile_evidence_plan(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        question_anchor: str,
        task_shape: str,
        declared_scope: dict[str, Any],
        source_inventory: list[dict[str, Any]],
        aggregate_spec: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        scope = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "runtime_session_id": runtime_session_id,
            "runtime_turn_id": runtime_turn_id,
        }
        payload = {
            **scope,
            "question_anchor": question_anchor,
            "task_shape": task_shape,
            "declared_scope": declared_scope,
            "source_inventory": source_inventory,
        }
        if aggregate_spec is not None:
            payload["aggregate_spec"] = aggregate_spec
        response = await self._post(
            "/v1/runtime/evidence-plans/compile",
            json=payload,
        )
        if not isinstance(response, dict) or any(
            response.get(field) != value for field, value in scope.items()
        ):
            raise RuntimeError("evidence_plan_response_invalid")
        return response

    async def evaluate_evidence_sufficiency(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        evidence_plan_id: str,
        acquisition_manifest_id: str,
        task_shape: str,
        declared_requirements: list[dict[str, Any]],
        acquisition_facts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        scope = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "runtime_session_id": runtime_session_id,
            "runtime_turn_id": runtime_turn_id,
        }
        response = await self._post(
            "/v1/runtime/evidence-sufficiency/evaluate",
            json={
                **scope,
                "evidence_plan_id": evidence_plan_id,
                "acquisition_manifest_id": acquisition_manifest_id,
                "task_shape": task_shape,
                "declared_requirements": declared_requirements,
                "acquisition_facts": acquisition_facts,
            },
        )
        expected = {
            **scope,
            "evidence_plan_id": evidence_plan_id,
            "acquisition_manifest_id": acquisition_manifest_id,
        }
        if not isinstance(response, dict) or any(
            response.get(field) != value for field, value in expected.items()
        ):
            raise RuntimeError("evidence_sufficiency_response_invalid")
        return response

    async def select_evidence_next_step(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        evaluation_id: str,
        evidence_plan_id: str,
        acquisition_manifest_id: str,
        evaluated_requirements: list[dict[str, Any]],
        current_premise: dict[str, Any],
        proposed_acquisition_premise: dict[str, Any] | None = None,
        clarification_target: str | None = None,
    ) -> dict[str, Any]:
        scope = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "runtime_session_id": runtime_session_id,
            "runtime_turn_id": runtime_turn_id,
        }
        payload: dict[str, Any] = {
            **scope,
            "evaluation_id": evaluation_id,
            "evidence_plan_id": evidence_plan_id,
            "acquisition_manifest_id": acquisition_manifest_id,
            "evaluated_requirements": evaluated_requirements,
            "current_premise": current_premise,
        }
        if proposed_acquisition_premise is not None:
            payload["proposed_acquisition_premise"] = proposed_acquisition_premise
        if clarification_target is not None:
            payload["clarification_target"] = clarification_target
        response = await self._post(
            "/v1/runtime/evidence-next-steps/select",
            json=payload,
        )
        expected = {
            **scope,
            "evaluation_id": evaluation_id,
            "evidence_plan_id": evidence_plan_id,
            "acquisition_manifest_id": acquisition_manifest_id,
        }
        result = response.get("result") if isinstance(response, dict) else None
        if (
            not isinstance(response, dict)
            or not isinstance(result, dict)
            or any(response.get(field) != value for field, value in scope.items())
            or any(result.get(field) != value for field, value in expected.items()
                if field not in scope)
        ):
            raise RuntimeError("evidence_next_step_response_invalid")
        return response

    async def resolve_session(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        surface_session_id: str | None = None,
        active_mode: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if surface_session_id is not None:
            payload["surface_session_id"] = surface_session_id
        if active_mode is not None:
            payload["active_mode"] = active_mode
        return await self._post("/v1/runtime/sessions/resolve", json=payload)

    async def start_turn(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        input_message_id: str | None = None,
        intent_class: str | None = None,
        expected_thread_revision: int | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if input_message_id is not None:
            payload["input_message_id"] = input_message_id
        if intent_class is not None:
            payload["intent_class"] = intent_class
        if expected_thread_revision is not None:
            if (
                isinstance(expected_thread_revision, bool)
                or not isinstance(expected_thread_revision, int)
                or expected_thread_revision < 0
            ):
                raise ValueError("expected_thread_revision_invalid")
            payload["expected_thread_revision"] = expected_thread_revision
        response = await self._post("/v1/runtime/turns/start", json=payload)
        if not isinstance(response, dict):
            raise RuntimeError("runtime_turn_response_invalid")
        session = response.get("runtime_session")
        turn = response.get("runtime_turn")
        if not isinstance(session, dict) or not isinstance(turn, dict):
            raise RuntimeError("runtime_turn_response_invalid")

        runtime_session_id = session.get("runtime_session_id")
        runtime_turn_id = turn.get("runtime_turn_id")
        if not _bounded_runtime_identifier(runtime_session_id) or not _bounded_runtime_identifier(
            runtime_turn_id
        ):
            raise RuntimeError("runtime_turn_response_invalid")
        if (
            session.get("owner_id") != owner_id
            or session.get("conversation_id") != conversation_id
            or session.get("surface") != surface
            or turn.get("runtime_session_id") != runtime_session_id
            or not _optional_uuid_ids_equivalent(
                turn.get("input_message_id"), input_message_id
            )
        ):
            raise RuntimeError("runtime_turn_response_context_mismatch")
        if turn.get("turn_status") not in {"received", "retrieving", "responding"}:
            raise RuntimeError("runtime_turn_response_invalid")

        event = response.get("event")
        if event is not None:
            if not isinstance(event, dict):
                raise RuntimeError("runtime_turn_response_invalid")
            if (
                event.get("runtime_session_id") != runtime_session_id
                or event.get("runtime_turn_id") != runtime_turn_id
                or event.get("event_type") != "turn_started"
            ):
                raise RuntimeError("runtime_turn_response_context_mismatch")
        return response

    async def resolve_thread(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
    ) -> dict[str, Any]:
        response = await self._post(
            "/v1/runtime/threads/resolve",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
            },
        )
        expected_fields = {
            "owner_id",
            "conversation_id",
            "state",
            "revision",
            "active_runtime_session_id",
            "active_runtime_turn_id",
            "active_surface",
            "participating_surfaces",
            "participating_session_count",
            "last_activity_at",
            "created_at",
            "updated_at",
        }
        if not isinstance(response, dict) or set(response) != expected_fields:
            raise RuntimeError("runtime_thread_response_invalid")
        if (
            response.get("owner_id") != owner_id
            or response.get("conversation_id") != conversation_id
        ):
            raise RuntimeError("runtime_thread_response_context_mismatch")
        state = response.get("state")
        revision = response.get("revision")
        surfaces = response.get("participating_surfaces")
        session_count = response.get("participating_session_count")
        optional_identifiers = (
            "active_runtime_session_id",
            "active_runtime_turn_id",
            "active_surface",
        )
        if (
            state not in {"idle", "active", "contended", "unavailable"}
            or isinstance(revision, bool)
            or not isinstance(revision, int)
            or revision < 0
            or not isinstance(surfaces, list)
            or len(surfaces) > 32
            or any(
                not isinstance(surface, str) or not surface or len(surface) > 64
                for surface in surfaces
            )
            or surfaces != sorted(set(surfaces))
            or isinstance(session_count, bool)
            or not isinstance(session_count, int)
            or session_count < len(surfaces)
            or len(surfaces) < 32
            and session_count != len(surfaces)
            or any(
                response.get(field) is not None
                and not _bounded_runtime_identifier(response.get(field))
                for field in optional_identifiers
            )
        ):
            raise RuntimeError("runtime_thread_response_invalid")
        active_values = [response.get(field) for field in optional_identifiers]
        if state == "idle" and any(value is not None for value in active_values):
            raise RuntimeError("runtime_thread_response_invalid")
        if state == "active" and any(value is None for value in active_values):
            raise RuntimeError("runtime_thread_response_invalid")
        try:
            for field in ("last_activity_at", "created_at", "updated_at"):
                _parse_aware_runtime_datetime(response.get(field))
        except RuntimeError:
            raise RuntimeError("runtime_thread_response_invalid") from None
        return response

    async def reserve_retirement(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        lifecycle_state: str,
        durable_updated_at: datetime,
        retirement_before: datetime,
    ) -> dict[str, Any]:
        _require_aware_runtime_datetime(durable_updated_at)
        _require_aware_runtime_datetime(retirement_before)
        if lifecycle_state not in {"open", "closed", "superseded"}:
            raise ValueError("retirement_lifecycle_state_invalid")
        response = await self._post(
            "/v1/runtime/retirements/reserve",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "lifecycle_state": lifecycle_state,
                "durable_updated_at": durable_updated_at.isoformat(),
                "retirement_before": retirement_before.isoformat(),
            },
        )
        if not isinstance(response, dict) or set(response) != {
            "schema_version",
            "request_id",
            "owner_id",
            "conversation_id",
            "result",
        }:
            raise RuntimeError("retirement_reservation_response_invalid")
        if (
            response.get("schema_version") != "runtime-retirement-reservation.v1"
            or response.get("request_id") != request_id
            or response.get("owner_id") != owner_id
            or response.get("conversation_id") != conversation_id
        ):
            raise RuntimeError("retirement_reservation_response_context_mismatch")
        result = response.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("retirement_reservation_response_invalid")
        outcome = result.get("outcome")
        expected_result_fields = {"outcome", "reason_codes", "policy_version"}
        if outcome == "reserved":
            expected_result_fields.update(
                {
                    "reservation_id",
                    "reserved_thread_revision",
                    "reserved_durable_updated_at",
                }
            )
        if set(result) != expected_result_fields:
            raise RuntimeError("retirement_reservation_response_invalid")
        reasons = result.get("reason_codes")
        reason = reasons[0] if isinstance(reasons, list) and len(reasons) == 1 else None
        if (
            outcome not in {"reserved", "wait", "decline"}
            or reason not in _RETIREMENT_REASONS
            or result.get("policy_version") != "conversation-retirement-safety.v1"
        ):
            raise RuntimeError("retirement_reservation_response_invalid")
        reservation_id = result.get("reservation_id")
        revision = result.get("reserved_thread_revision")
        reserved_activity = result.get("reserved_durable_updated_at")
        if outcome == "reserved":
            if (
                reason not in _RETIREMENT_RESERVED_REASONS
                or not _bounded_runtime_identifier(reservation_id)
                or isinstance(revision, bool)
                or not isinstance(revision, int)
                or revision < 0
            ):
                raise RuntimeError("retirement_reservation_response_invalid")
            try:
                _parse_aware_runtime_datetime(reserved_activity)
            except RuntimeError:
                raise RuntimeError("retirement_reservation_response_invalid") from None
        elif (
            reservation_id is not None
            or revision is not None
            or reserved_activity is not None
            or reason in _RETIREMENT_RESERVED_REASONS
            or outcome == "wait"
            and reason != "runtime_thread_active"
            or outcome == "decline"
            and reason == "runtime_thread_active"
        ):
            raise RuntimeError("retirement_reservation_response_invalid")
        return response

    async def cancel_retirement(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        reservation_id: str,
        reserved_thread_revision: int,
    ) -> dict[str, Any]:
        return await self._retirement_mutation(
            operation="cancel",
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            reservation_id=reservation_id,
            reserved_thread_revision=reserved_thread_revision,
        )

    async def finalize_retirement(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        reservation_id: str,
        reserved_thread_revision: int,
    ) -> dict[str, Any]:
        return await self._retirement_mutation(
            operation="finalize",
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            reservation_id=reservation_id,
            reserved_thread_revision=reserved_thread_revision,
        )

    async def _retirement_mutation(
        self,
        *,
        operation: str,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        reservation_id: str,
        reserved_thread_revision: int,
    ) -> dict[str, Any]:
        if operation not in {"cancel", "finalize"}:
            raise ValueError("retirement_operation_invalid")
        if not _bounded_runtime_identifier(reservation_id):
            raise ValueError("retirement_reservation_id_invalid")
        if (
            isinstance(reserved_thread_revision, bool)
            or not isinstance(reserved_thread_revision, int)
            or reserved_thread_revision < 0
        ):
            raise ValueError("retirement_thread_revision_invalid")
        response = await self._post(
            f"/v1/runtime/retirements/{operation}",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "reservation_id": reservation_id,
                "reserved_thread_revision": reserved_thread_revision,
            },
        )
        common = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "reservation_id": reservation_id,
        }
        if operation == "cancel":
            expected_fields = {
                "schema_version",
                *common,
                "thread_revision",
                "outcome",
            }
            if not isinstance(response, dict) or set(response) != expected_fields:
                raise RuntimeError("retirement_cancellation_response_invalid")
            if (
                response.get("schema_version") != "runtime-retirement-cancellation.v1"
                or response.get("outcome") != "cancelled"
                or response.get("thread_revision") != reserved_thread_revision
            ):
                raise RuntimeError("retirement_cancellation_response_invalid")
        else:
            expected_fields = {
                "schema_version",
                *common,
                "previous_thread_revision",
                "fenced_thread_revision",
                "outcome",
            }
            if not isinstance(response, dict) or set(response) != expected_fields:
                raise RuntimeError("retirement_finalization_response_invalid")
            if (
                response.get("schema_version") != "runtime-retirement-finalization.v1"
                or response.get("outcome") != "finalized"
                or response.get("previous_thread_revision")
                != reserved_thread_revision
                or response.get("fenced_thread_revision")
                != reserved_thread_revision + 1
            ):
                raise RuntimeError("retirement_finalization_response_invalid")
        if any(response.get(field) != value for field, value in common.items()):
            raise RuntimeError(f"retirement_{operation}_response_context_mismatch")
        return response

    async def select_continuation(
        self,
        *,
        request_id: str,
        owner_id: str,
        surface: str,
        candidate_set_complete: bool,
        stale_after_seconds: int,
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if type(candidate_set_complete) is not bool:
            raise ValueError("continuation_selection_request_invalid")
        if (
            isinstance(stale_after_seconds, bool)
            or not isinstance(stale_after_seconds, int)
            or not 60 <= stale_after_seconds <= 86400
            or len(candidates) > 8
        ):
            raise ValueError("continuation_selection_request_invalid")
        candidate_ids: set[str] = set()
        candidate_payloads: list[dict[str, Any]] = []
        for candidate in candidates:
            if not isinstance(candidate, dict) or set(candidate) != {
                "conversation_id",
                "lifecycle_state",
                "durable_updated_at",
            }:
                raise ValueError("continuation_selection_request_invalid")
            conversation_id = candidate.get("conversation_id")
            lifecycle_state = candidate.get("lifecycle_state")
            durable_updated_at = candidate.get("durable_updated_at")
            try:
                canonical_id = str(UUID(conversation_id))
                parsed_updated_at = datetime.fromisoformat(durable_updated_at)
            except (TypeError, ValueError, AttributeError):
                raise ValueError("continuation_selection_request_invalid") from None
            if (
                not _bounded_runtime_identifier(conversation_id)
                or conversation_id != canonical_id
                or lifecycle_state not in {"open", "closed", "superseded"}
                or not isinstance(durable_updated_at, str)
                or not durable_updated_at
                or parsed_updated_at.tzinfo is None
                or parsed_updated_at.utcoffset() is None
                or conversation_id in candidate_ids
            ):
                raise ValueError("continuation_selection_request_invalid")
            candidate_ids.add(conversation_id)
            candidate_payloads.append(dict(candidate))

        response = await self._post(
            "/v1/runtime/continuations/select",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "surface": surface,
                "candidate_set_complete": candidate_set_complete,
                "stale_after_seconds": stale_after_seconds,
                "candidates": candidate_payloads,
            },
        )
        if not isinstance(response, dict) or set(response) != {
            "schema_version",
            "request_id",
            "owner_id",
            "surface",
            "result",
        }:
            raise RuntimeError("continuation_selection_response_invalid")
        if (
            response.get("schema_version") != "runtime-continuation-selection.v1"
            or response.get("request_id") != request_id
            or response.get("owner_id") != owner_id
            or response.get("surface") != surface
        ):
            raise RuntimeError("continuation_selection_response_context_mismatch")
        result = response.get("result")
        if not isinstance(result, dict) or set(result) != {
            "outcome",
            "timing_policy",
            "selected_conversation_id",
            "selected_thread_revision",
            "candidate_count",
            "eligible_candidate_count",
            "reason_codes",
            "policy_version",
        }:
            raise RuntimeError("continuation_selection_response_invalid")
        outcome = result.get("outcome")
        reason_codes = result.get("reason_codes")
        candidate_count = result.get("candidate_count")
        eligible_count = result.get("eligible_candidate_count")
        revision = result.get("selected_thread_revision")
        selected_id = result.get("selected_conversation_id")
        if (
            outcome not in _CONTINUATION_TIMING
            or result.get("timing_policy") != _CONTINUATION_TIMING[outcome]
            or result.get("policy_version") != "continuation-selection.v1"
            or isinstance(candidate_count, bool)
            or not isinstance(candidate_count, int)
            or candidate_count != len(candidates)
            or isinstance(eligible_count, bool)
            or not isinstance(eligible_count, int)
            or not 0 <= eligible_count <= candidate_count
            or not isinstance(reason_codes, list)
            or not 1 <= len(reason_codes) <= 8
            or any(
                not isinstance(reason, str) or reason not in _CONTINUATION_REASONS
                for reason in reason_codes
            )
            or len(reason_codes) != len(set(reason_codes))
        ):
            raise RuntimeError("continuation_selection_response_invalid")
        if not _continuation_outcome_is_coherent(
            outcome=outcome,
            candidate_count=candidate_count,
            eligible_count=eligible_count,
            reason_codes=reason_codes,
        ):
            raise RuntimeError("continuation_selection_response_invalid")
        if outcome == "resume":
            if (
                selected_id not in candidate_ids
                or isinstance(revision, bool)
                or not isinstance(revision, int)
                or revision < 0
            ):
                raise RuntimeError("continuation_selection_response_context_mismatch")
        elif selected_id is not None or revision is not None:
            raise RuntimeError("continuation_selection_response_context_mismatch")
        return response

    async def update_turn(
        self,
        *,
        request_id: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        turn_status: str,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/runtime/turns/update",
            json={
                "request_id": request_id,
                "runtime_session_id": runtime_session_id,
                "runtime_turn_id": runtime_turn_id,
                "turn_status": turn_status,
            },
        )

    async def complete_turn(
        self,
        *,
        request_id: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        turn_status: str,
        continuation_state: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "runtime_session_id": runtime_session_id,
            "runtime_turn_id": runtime_turn_id,
            "turn_status": turn_status,
        }
        if continuation_state is not None:
            payload["continuation_state"] = continuation_state
        return await self._post("/v1/runtime/turns/complete", json=payload)

    async def resolve_identity(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        return await self._post("/v1/runtime/identity/resolve", json=payload)

    async def world_state_resolve(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        active_persona_id: str | None = None,
        requested_domains: list[str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if active_persona_id is not None:
            payload["active_persona_id"] = active_persona_id
        if requested_domains:
            payload["requested_domains"] = requested_domains
        return await self._post("/v1/world-state/resolve", json=payload)

    async def world_state_claim_verify(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        world_state_claim_id: str,
        expected_value_digest: str,
        verification_source_type: str,
        verification_source_ref: str,
        observed_at: str,
        verified_at: str,
        resulting_authority: str,
        resulting_confidence: float,
        resulting_freshness_state: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        verifier_id: str | None = None,
        resulting_ttl_seconds: int | None = None,
        resulting_revalidation_interval_seconds: int | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "world_state_claim_id": world_state_claim_id,
            "expected_value_digest": expected_value_digest,
            "verification_source_type": verification_source_type,
            "verification_source_ref": verification_source_ref,
            "observed_at": observed_at,
            "verified_at": verified_at,
            "resulting_authority": resulting_authority,
            "resulting_confidence": resulting_confidence,
            "resulting_freshness_state": resulting_freshness_state,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if verifier_id is not None:
            payload["verifier_id"] = verifier_id
        if resulting_ttl_seconds is not None:
            payload["resulting_ttl_seconds"] = resulting_ttl_seconds
        if resulting_revalidation_interval_seconds is not None:
            payload["resulting_revalidation_interval_seconds"] = (
                resulting_revalidation_interval_seconds
            )
        return await self._post("/v1/world-state/claims/verify", json=payload)

    async def relationship_select(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        active_persona_id: str | None = None,
        requested_scopes: list[str] | None = None,
        entity_ids: list[str] | None = None,
        relationship_types: list[str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if active_persona_id is not None:
            payload["active_persona_id"] = active_persona_id
        if requested_scopes:
            payload["requested_scopes"] = requested_scopes
        if entity_ids:
            payload["entity_ids"] = entity_ids
        if relationship_types:
            payload["relationship_types"] = relationship_types
        return await self._post("/v1/relationships/select", json=payload)

    async def authorize_capability(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str,
        runtime_turn_id: str | None,
        active_persona_id: str,
        authorization_phase: str,
        capability_id: str,
        capability_domain: str,
        operation_class: str,
        argument_digest: str | None = None,
        supported_surfaces: list[str] | None = None,
        relationship_requirements: list[dict[str, Any]] | None = None,
        selected_relationship_ids: list[str] | None = None,
        world_state_requirements: list[dict[str, Any]] | None = None,
        selected_world_state_claim_ids: list[str] | None = None,
        confirmation_challenge_ref: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "runtime_session_id": runtime_session_id,
            "runtime_turn_id": runtime_turn_id,
            "active_persona_id": active_persona_id,
            "authorization_phase": authorization_phase,
            "capability_id": capability_id,
            "capability_domain": capability_domain,
            "operation_class": operation_class,
            "argument_digest": argument_digest,
            "supported_surfaces": supported_surfaces or [],
            "relationship_requirements": relationship_requirements or [],
            "selected_relationship_ids": selected_relationship_ids or [],
            "world_state_requirements": world_state_requirements or [],
            "selected_world_state_claim_ids": selected_world_state_claim_ids or [],
            "confirmation_challenge_ref": confirmation_challenge_ref,
        }
        return await self._post("/v1/capabilities/authorize", json=payload)

    async def match_capability(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        active_persona_id: str,
        current_user_text: str,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/capabilities/match",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "surface": surface,
                "active_persona_id": active_persona_id,
                "current_user_text": current_user_text,
            },
        )

    async def discover_capabilities(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        active_persona_id: str,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/capabilities/discover",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "surface": surface,
                "active_persona_id": active_persona_id,
            },
        )

    async def action_authority(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        active_persona_id: str,
        capability_id: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        target_resolution_state: str = "resolved",
        world_state_freshness: str = "unknown",
        consequence_flags: dict[str, bool] | None = None,
        interaction_governance_kind: str | None = None,
        interaction_governance_tension: str | None = None,
        user_authorization_signal: str = "explicit",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "active_persona_id": active_persona_id,
            "capability_id": capability_id,
            "target_resolution_state": target_resolution_state,
            "world_state_freshness": world_state_freshness,
            "consequence_flags": consequence_flags or {},
            "user_authorization_signal": user_authorization_signal,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if interaction_governance_kind is not None:
            payload["interaction_governance_kind"] = interaction_governance_kind
        if interaction_governance_tension is not None:
            payload["interaction_governance_tension"] = interaction_governance_tension
        return await self._post("/v1/capabilities/authority", json=payload)

    async def action_flow(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        active_persona_id: str,
        capability_id: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        flow_intent: str = "execution_requested",
        target_resolution_state: str = "resolved",
        target_label: str | None = None,
        world_state_freshness: str = "unknown",
        affects_multiple_systems: bool = False,
        consequence_flags: dict[str, bool] | None = None,
        interaction_governance_kind: str | None = None,
        interaction_governance_tension: str | None = None,
        user_authorization_signal: str = "explicit",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "active_persona_id": active_persona_id,
            "capability_id": capability_id,
            "flow_intent": flow_intent,
            "target_resolution_state": target_resolution_state,
            "world_state_freshness": world_state_freshness,
            "affects_multiple_systems": affects_multiple_systems,
            "consequence_flags": consequence_flags or {},
            "user_authorization_signal": user_authorization_signal,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if target_label is not None:
            payload["target_label"] = target_label
        if interaction_governance_kind is not None:
            payload["interaction_governance_kind"] = interaction_governance_kind
        if interaction_governance_tension is not None:
            payload["interaction_governance_tension"] = interaction_governance_tension
        return await self._post("/v1/capabilities/flow", json=payload)

    async def action_summary(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str,
        runtime_turn_id: str | None,
        capability_id: str,
        active_persona_id: str,
        risk_level: str,
        authority_level: str,
        confirmation_status: str,
        policy_reason_codes: list[str],
        execution_status: str,
        verification_status: str,
        execution_reason_code: str | None = None,
        verification_reason_code: str | None = None,
        degradation_reason: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "runtime_session_id": runtime_session_id,
            "capability_id": capability_id,
            "active_persona_id": active_persona_id,
            "risk_level": risk_level,
            "authority_level": authority_level,
            "confirmation_status": confirmation_status,
            "policy_reason_codes": policy_reason_codes,
            "execution_status": execution_status,
            "verification_status": verification_status,
        }
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if execution_reason_code is not None:
            payload["execution_reason_code"] = execution_reason_code
        if verification_reason_code is not None:
            payload["verification_reason_code"] = verification_reason_code
        if degradation_reason is not None:
            payload["degradation_reason"] = degradation_reason
        return await self._post("/v1/capabilities/action-summary", json=payload)

    async def confirm_capability(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        confirmation_challenge_ref: str,
        capability_id: str,
        operation_class: str,
        argument_digest: str,
        confirmed: bool,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/capabilities/confirm",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "surface": surface,
                "runtime_session_id": runtime_session_id,
                "runtime_turn_id": runtime_turn_id,
                "confirmation_challenge_ref": confirmation_challenge_ref,
                "capability_id": capability_id,
                "operation_class": operation_class,
                "argument_digest": argument_digest,
                "confirmed": confirmed,
            },
        )

    async def compile_companion_policy(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        requested_scene: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if requested_scene is not None:
            payload["requested_scene"] = requested_scene

        self.last_companion_compile_endpoint = _PREFERRED_COMPANION_COMPILE_PATH
        try:
            response = await self._post(_PREFERRED_COMPANION_COMPILE_PATH, json=payload)
            return _with_compile_endpoint(response, _PREFERRED_COMPANION_COMPILE_PATH)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code not in {404, 405}:
                raise

        self.last_companion_compile_endpoint = _COMPAT_COMPANION_COMPILE_PATH
        response = await self._post(_COMPAT_COMPANION_COMPILE_PATH, json=payload)
        return _with_compile_endpoint(response, _COMPAT_COMPANION_COMPILE_PATH)

    async def evaluate_interrupt(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        current_user_text: str | None = None,
        recent_messages: list[dict[str, Any]] | None = None,
        requested_scene: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if current_user_text is not None:
            payload["current_user_text"] = current_user_text
        if recent_messages is not None:
            payload["recent_messages"] = recent_messages
        if requested_scene is not None:
            payload["requested_scene"] = requested_scene
        return await self._post("/v1/interrupt/evaluate", json=payload)

    async def evaluate_interaction_governance(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        surface_session_id: str | None = None,
        active_mode: str | None = None,
        current_user_text: str | None = None,
        recent_messages: list[dict[str, Any]] | None = None,
        surface_metadata_json: dict[str, Any] | None = None,
        history_followup_candidate: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if surface_session_id is not None:
            payload["surface_session_id"] = surface_session_id
        if active_mode is not None:
            payload["active_mode"] = active_mode
        if current_user_text is not None:
            payload["current_user_text"] = current_user_text
        if recent_messages is not None:
            payload["recent_messages"] = recent_messages
        if surface_metadata_json is not None:
            payload["surface_metadata_json"] = surface_metadata_json
        if history_followup_candidate is not None:
            payload["history_followup_candidate"] = history_followup_candidate
        response = await self._post(
            "/v1/runtime/interaction-governance/evaluate", json=payload
        )
        if history_followup_candidate is not None:
            scope = {
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "surface": surface,
            }
            if runtime_session_id is not None:
                scope["runtime_session_id"] = runtime_session_id
            if runtime_turn_id is not None:
                scope["runtime_turn_id"] = runtime_turn_id
            validate_history_followup_policy_response(response, scope=scope)
        return response

    async def evaluate_persona_containment(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        active_persona_id: str | None = None,
        requested_persona_id: str | None = None,
        persona_scope_hint: str | None = None,
        interaction_kind: str | None = None,
        current_user_text: str | None = None,
        recent_messages: list[dict[str, Any]] | None = None,
        surface_metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if active_persona_id is not None:
            payload["active_persona_id"] = active_persona_id
        if requested_persona_id is not None:
            payload["requested_persona_id"] = requested_persona_id
        if persona_scope_hint is not None:
            payload["persona_scope_hint"] = persona_scope_hint
        if interaction_kind is not None:
            payload["interaction_kind"] = interaction_kind
        if current_user_text is not None:
            payload["current_user_text"] = current_user_text
        if recent_messages is not None:
            payload["recent_messages"] = recent_messages
        if surface_metadata_json is not None:
            payload["surface_metadata_json"] = surface_metadata_json
        return await self._post("/v1/runtime/persona-containment/evaluate", json=payload)

    async def evaluate_restraint(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        interaction_kind: str | None = None,
        response_posture: str | None = None,
        active_persona_id: str | None = None,
        capability_domain: str | None = None,
        current_user_text: str | None = None,
        recent_messages: list[dict[str, Any]] | None = None,
        surface_metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if interaction_kind is not None:
            payload["interaction_kind"] = interaction_kind
        if response_posture is not None:
            payload["response_posture"] = response_posture
        if active_persona_id is not None:
            payload["active_persona_id"] = active_persona_id
        if capability_domain is not None:
            payload["capability_domain"] = capability_domain
        if current_user_text is not None:
            payload["current_user_text"] = current_user_text
        if recent_messages is not None:
            payload["recent_messages"] = recent_messages
        if surface_metadata_json is not None:
            payload["surface_metadata_json"] = surface_metadata_json
        return await self._post("/v1/runtime/restraint/evaluate", json=payload)

    async def evaluate_situated_presence(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        surface_context: dict[str, Any],
        interaction_governance: dict[str, Any],
        restraint: dict[str, Any],
    ) -> dict[str, Any]:
        scope = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "runtime_session_id": runtime_session_id,
            "runtime_turn_id": runtime_turn_id,
        }
        if (
            any(not _bounded_runtime_identifier(value) for value in scope.values())
            or len(surface) > 64
        ):
            raise ValueError("situated_presence_request_invalid")
        surface_projection = _strict_situated_projection(
            surface_context,
            expected_keys={"visibility", "constraint"},
            labels={
                "visibility": _SITUATED_VISIBILITY,
                "constraint": _SITUATED_CONSTRAINT,
            },
            booleans=set(),
            confidence=False,
        )
        governance_projection = _strict_situated_projection(
            interaction_governance,
            expected_keys={
                "interaction_kind",
                "tension_level",
                "commentary_allowed",
                "humor_allowed",
                "action_allowed",
                "requires_confirmation",
                "privacy_sensitivity_hint",
                "response_posture",
                "confidence",
            },
            labels={
                "interaction_kind": _SITUATED_INTERACTION_KINDS,
                "tension_level": _SITUATED_TENSION_LEVELS,
                "privacy_sensitivity_hint": _SITUATED_PRIVACY_HINTS,
                "response_posture": _SITUATED_RESPONSE_POSTURES,
            },
            booleans={
                "commentary_allowed",
                "humor_allowed",
                "action_allowed",
                "requires_confirmation",
            },
            confidence=True,
        )
        restraint_projection = _strict_situated_projection(
            restraint,
            expected_keys={
                "restraint_policy",
                "proactive_output_suppressed",
                "personalization_suppressed",
                "brevity_preferred",
                "clarification_preferred",
                "confidence",
            },
            labels={"restraint_policy": _SITUATED_RESTRAINT_POLICIES},
            booleans={
                "proactive_output_suppressed",
                "personalization_suppressed",
                "brevity_preferred",
                "clarification_preferred",
            },
            confidence=True,
        )
        response = await self._post(
            "/v1/runtime/situated-presence/evaluate",
            json={
                **scope,
                "surface_context": surface_projection,
                "interaction_governance": governance_projection,
                "restraint": restraint_projection,
            },
        )
        return _validate_situated_presence_response(
            response,
            scope=scope,
            surface_context=surface_projection,
            governance=governance_projection,
            restraint=restraint_projection,
        )

    async def evaluate_memory_hygiene(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "items": items,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        return await self._post("/v1/runtime/memory-hygiene/evaluate", json=payload)

    async def evaluate_privacy_context(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        surface_category: str | None = None,
        sensitivity_level: str,
        sensitivity_domains: list[str],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "sensitivity_level": sensitivity_level,
            "sensitivity_domains": sensitivity_domains,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if surface_category is not None:
            payload["surface_category"] = surface_category

        response = await self._post("/v1/runtime/privacy-context/evaluate", json=payload)
        if not isinstance(response, dict):
            raise ValueError("malformed_privacy_context_response")
        result = validate_privacy_policy_result(response.get("result"))
        if result is None:
            raise ValueError("invalid_privacy_context_result")
        validated_response = dict(response)
        validated_response["result"] = result
        return validated_response

    async def reset(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        reason: str,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/runtime/state/reset",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "surface": surface,
                "reason": reason,
            },
        )


def _with_compile_endpoint(response: Any, endpoint: str) -> Any:
    if isinstance(response, dict):
        enriched = dict(response)
        enriched[_COMPANION_ENDPOINT_KEY] = endpoint
        return enriched
    return response
