from __future__ import annotations

from typing import Any

_PRIVATE_SURFACE_CATEGORIES = {
    "desktop_private",
    "mobile_private",
    "telegram_private",
    "voice_private",
}
_SHARED_SURFACE_CATEGORIES = {"car_voice_possible_passenger"}
_PUBLIC_SURFACE_CATEGORIES = {
    "glasses_public_or_semi_public",
    "notification_preview",
}
_KNOWN_SURFACE_CATEGORIES = (
    _PRIVATE_SURFACE_CATEGORIES
    | _SHARED_SURFACE_CATEGORIES
    | _PUBLIC_SURFACE_CATEGORIES
)
_INTERACTION_KINDS = {
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
_TENSION_LEVELS = {"low", "medium", "high"}
_PRIVACY_HINTS = {"normal", "private", "sensitive"}
_RESPONSE_POSTURES = {
    "direct",
    "supportive",
    "tactical",
    "brief",
    "reflective",
    "playful",
    "silent_or_minimal",
}
_RESTRAINT_POLICIES = {
    "answer_normally",
    "short_answer",
    "defer_expansion",
    "ask_clarifying_question",
    "do_not_retrieve",
    "do_not_personalize",
    "suppress_proactive_output",
}


def derive_situated_surface_context(
    payload: dict[str, Any],
) -> tuple[dict[str, str], dict[str, Any]]:
    supplied = payload.get("surface_context")
    surface_context = supplied if isinstance(supplied, dict) else {}
    raw_category = surface_context.get("surface_category")
    category = (
        raw_category.strip().lower()
        if isinstance(raw_category, str) and raw_category.strip()
        else None
    )

    if category in _PRIVATE_SURFACE_CATEGORIES:
        visibility = "private"
    elif category in _SHARED_SURFACE_CATEGORIES:
        visibility = "shared"
    elif category in _PUBLIC_SURFACE_CATEGORIES:
        visibility = "public"
    else:
        visibility = "unknown"

    source_fields: list[str] = []
    if category in _KNOWN_SURFACE_CATEGORIES or category == "unknown_surface":
        source_fields.append("surface_context.surface_category")

    constrained = category == "notification_preview"
    if surface_context.get("active_task_mode") is True:
        constrained = True
        source_fields.append("surface_context.active_task_mode")
    if surface_context.get("allows_expansion") is False:
        constrained = True
        source_fields.append("surface_context.allows_expansion")

    if constrained:
        constraint = "constrained"
        reason = "explicit_surface_constraint"
    elif category in _KNOWN_SURFACE_CATEGORIES:
        constraint = "normal"
        reason = "recognized_surface_context"
    else:
        constraint = "unknown"
        reason = "surface_context_unknown"

    derived = {"visibility": visibility, "constraint": constraint}
    trace = {
        **derived,
        "source_fields": list(dict.fromkeys(source_fields)),
        "reason": reason,
    }
    return derived, trace


def situated_presence_disabled_trace(
    *,
    runtime_configured: bool,
    interaction_governance_enabled: bool,
    restraint_enabled: bool,
) -> dict[str, Any]:
    if not runtime_configured:
        reason = "runtime_not_configured"
    elif not interaction_governance_enabled:
        reason = "interaction_governance_not_enabled"
    else:
        reason = "restraint_not_enabled"
    return {
        "activated": False,
        "attempted": False,
        "status": "disabled",
        "included": False,
        "runtime_call_status": "not_called",
        "schema_version": None,
        "policy_version": None,
        "surface_context": None,
        "fallback_status": "not_active",
        "failure_category": None,
        "reason": reason,
    }


def _strict_bool(value: Any) -> bool:
    if type(value) is not bool:
        raise ValueError("situated_presence_projection_invalid")
    return value


def _strict_confidence(value: Any) -> float:
    if not isinstance(value, float) or not 0.0 <= value <= 1.0:
        raise ValueError("situated_presence_projection_invalid")
    return value


def _label(value: Any, allowed: set[str]) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError("situated_presence_projection_invalid")
    return value


def _compact_governance(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("situated_presence_governance_unavailable")
    return {
        "interaction_kind": _label(value.get("interaction_kind"), _INTERACTION_KINDS),
        "tension_level": _label(value.get("tension_level"), _TENSION_LEVELS),
        "commentary_allowed": _strict_bool(value.get("commentary_allowed")),
        "humor_allowed": _strict_bool(value.get("humor_allowed")),
        "action_allowed": _strict_bool(value.get("action_allowed")),
        "requires_confirmation": _strict_bool(value.get("requires_confirmation")),
        "privacy_sensitivity_hint": _label(
            value.get("privacy_sensitivity_hint"), _PRIVACY_HINTS
        ),
        "response_posture": _label(value.get("response_posture"), _RESPONSE_POSTURES),
        "confidence": _strict_confidence(value.get("confidence")),
    }


def _compact_restraint(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("situated_presence_restraint_unavailable")
    return {
        "restraint_policy": _label(value.get("restraint_policy"), _RESTRAINT_POLICIES),
        "proactive_output_suppressed": _strict_bool(
            value.get("proactive_output_suppressed")
        ),
        "personalization_suppressed": _strict_bool(
            value.get("personalization_suppressed")
        ),
        "brevity_preferred": _strict_bool(value.get("brevity_preferred")),
        "clarification_preferred": _strict_bool(value.get("clarification_preferred")),
        "confidence": _strict_confidence(value.get("confidence")),
    }


def _bounded_identifier(value: Any, *, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError("situated_presence_scope_unavailable")
    return value


def _failure_category(error: BaseException) -> str:
    error_types = {error_type.__name__ for error_type in type(error).__mro__}
    if "TimeoutException" in error_types:
        return "transport_timeout"
    if "TransportError" in error_types:
        return "transport_failure"
    if "HTTPStatusError" in error_types:
        return "dependency_http_failure"
    if isinstance(error, ValueError):
        return "mandatory_input_invalid"
    if isinstance(error, RuntimeError):
        return "response_invalid"
    return "dependency_unavailable"


def _fallback_result(*, surface_allows_commentary: bool) -> dict[str, Any]:
    return {
        "commentary_allowed": False,
        "humor_allowed": False,
        "emotional_attunement_allowed": "none",
        "challenge_allowed": "none",
        "silence_preferred": True,
        "surface_allows_commentary": surface_allows_commentary,
        "response_posture": "silent_or_minimal",
        "action_implication_allowed": False,
        "reason_summary": ["situated_presence_unavailable"],
        "policy_version": "situated-presence.local-fallback.v1",
    }


def _result_trace(
    result: dict[str, Any],
    *,
    activated: bool,
    status: str,
    runtime_call_status: str,
    schema_version: str | None,
    surface_trace: dict[str, Any],
    fallback_status: str,
    failure_category: str | None,
) -> dict[str, Any]:
    return {
        "activated": activated,
        "attempted": activated,
        "status": status,
        "included": True,
        "runtime_call_status": runtime_call_status,
        "schema_version": schema_version,
        "policy_version": result.get("policy_version"),
        "surface_context": surface_trace,
        "commentary_allowed": result.get("commentary_allowed"),
        "humor_allowed": result.get("humor_allowed"),
        "emotional_attunement_allowed": result.get(
            "emotional_attunement_allowed"
        ),
        "challenge_allowed": result.get("challenge_allowed"),
        "silence_preferred": result.get("silence_preferred"),
        "surface_allows_commentary": result.get("surface_allows_commentary"),
        "response_posture": result.get("response_posture"),
        "action_implication_allowed": result.get("action_implication_allowed"),
        "reason_summary": list(result.get("reason_summary") or [])[:8],
        "fallback_status": fallback_status,
        "failure_category": failure_category,
    }


async def resolve_situated_presence(
    *,
    runtime: Any | None,
    interaction_governance_enabled: bool,
    restraint_enabled: bool,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    payload: dict[str, Any],
    interaction_governance: dict[str, Any] | None,
    restraint: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    activated = bool(
        runtime is not None
        and interaction_governance_enabled
        and restraint_enabled
    )
    if not activated:
        return None, situated_presence_disabled_trace(
            runtime_configured=runtime is not None,
            interaction_governance_enabled=interaction_governance_enabled,
            restraint_enabled=restraint_enabled,
        )

    surface_projection, surface_trace = derive_situated_surface_context(payload)
    surface_allows = (
        surface_projection["visibility"] == "private"
        and surface_projection["constraint"] == "normal"
    )
    try:
        scope = {
            "request_id": _bounded_identifier(request_id, maximum=120),
            "owner_id": _bounded_identifier(owner_id, maximum=120),
            "conversation_id": _bounded_identifier(conversation_id, maximum=120),
            "surface": _bounded_identifier(surface, maximum=64),
            "runtime_session_id": _bounded_identifier(
                runtime_session_id, maximum=120
            ),
            "runtime_turn_id": _bounded_identifier(runtime_turn_id, maximum=120),
        }
        response = await runtime.evaluate_situated_presence(
            **scope,
            surface_context=surface_projection,
            interaction_governance=_compact_governance(interaction_governance),
            restraint=_compact_restraint(restraint),
        )
        if not isinstance(response, dict) or not isinstance(response.get("result"), dict):
            raise RuntimeError("situated_presence_response_invalid")
        result = dict(response["result"])
        return result, _result_trace(
            result,
            activated=True,
            status="included",
            runtime_call_status="included",
            schema_version=response.get("schema_version"),
            surface_trace=surface_trace,
            fallback_status="not_used",
            failure_category=None,
        )
    except Exception as error:
        result = _fallback_result(surface_allows_commentary=surface_allows)
        return result, _result_trace(
            result,
            activated=True,
            status="fallback",
            runtime_call_status="failed",
            schema_version=None,
            surface_trace=surface_trace,
            fallback_status="suppression_only",
            failure_category=_failure_category(error),
        )


def build_situated_presence_guidance(result: dict[str, Any] | None) -> str:
    if not isinstance(result, dict):
        return ""

    lines = ["Situated presence guidance:"]
    if result.get("commentary_allowed") is True:
        lines.append(
            "- One small current-turn-grounded observation is permitted only if it "
            "adds value; do not force it."
        )
    else:
        lines.append("- Omit optional commentary and casual asides.")

    if result.get("humor_allowed") is True:
        lines.append(
            "- Light playfulness is permitted inside the resolved style; do not force a joke."
        )
    else:
        lines.append("- Do not add jokes, teasing, or playful flourishes.")

    attunement = result.get("emotional_attunement_allowed")
    if attunement == "brief":
        lines.append(
            "- One brief steadying acknowledgment may be used; do not infer feelings, "
            "diagnose, personalize from memory, or turn it into automatic coaching."
        )
    elif attunement == "minimal":
        lines.append("- Keep any acknowledgment minimal and factual.")
    else:
        lines.append("- Do not interpret the user’s emotions.")

    posture = result.get("response_posture")
    if posture == "tactical":
        lines.append("- Lead with concrete practical guidance and omit casual framing.")
    elif posture in {"brief", "silent_or_minimal"}:
        lines.append("- Keep the response brief and omit optional social framing.")
    elif posture == "direct":
        lines.append("- Use a direct response posture.")
    elif posture == "playful":
        lines.append("- A light response posture is permitted within the stated limits.")

    if result.get("silence_preferred") is True:
        lines.append("- Answer required content only; omit optional social framing and detours.")

    challenge = result.get("challenge_allowed")
    if challenge == "medium":
        lines.append("- Use clear, proportionate challenge where it helps the task.")
    elif challenge == "low":
        lines.append("- Keep any challenge light and proportionate.")
    else:
        lines.append("- Do not add optional challenge or pushback.")

    lines.append(
        "- This guidance never authorizes an action or replaces required confirmation."
    )
    return "\n".join(lines)
