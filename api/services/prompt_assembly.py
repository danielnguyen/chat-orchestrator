from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from services.assistant_handoff import AssistantHandoff
from services.companion_presentation import CompanionPresentation
from services.privacy_context import (
    PRIVACY_SENSITIVITY_LEVELS,
    PRIVACY_SURFACE_CATEGORIES,
    PRIVACY_ZONES,
)

VALID_ROLES = {"user", "assistant", "system", "tool"}
VALID_GOVERNANCE_RESPONSE_POSTURES = {
    "direct",
    "supportive",
    "tactical",
    "brief",
    "reflective",
    "playful",
    "silent_or_minimal",
}
VALID_GOVERNANCE_PRIVACY_HINTS = {"normal", "private", "sensitive"}
SAFE_GOVERNANCE_LABEL = re.compile(r"^[a-zA-Z0-9_.:-]+$")
VALID_RESTRAINT_POLICIES = {
    "answer_normally",
    "short_answer",
    "defer_expansion",
    "ask_clarifying_question",
    "do_not_retrieve",
    "do_not_personalize",
    "suppress_proactive_output",
}
VALID_PRIVACY_REASON_CODES = re.compile(r"^[a-z0-9_.:-]+$")
PROMPT_OVERLAY_MAX_CHARS = 240
PROMPT_INJECTION_MARKERS = (
    "ignore system",
    "ignore developer",
    "ignore prior",
    "ignore previous",
    "system prompt",
    "developer prompt",
    "developer message",
    "developer instructions",
    "prior instructions",
    "previous instructions",
)


@dataclass(frozen=True)
class PromptAssembly:
    messages: list[dict[str, str]]
    trace: dict[str, Any]


def _layer_trace(
    name: str,
    messages: list[dict[str, str]],
    *,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "included": bool(messages),
        "message_count": len(messages),
        "metadata": metadata or {},
    }


def _memory_hygiene_prefix(item: dict[str, Any]) -> str:
    memory_hygiene = item.get("memory_hygiene")
    if not isinstance(memory_hygiene, dict):
        return ""
    framing = memory_hygiene.get("framing")
    if framing == "parked_or_historical":
        return "[historical/parked context] "
    if framing == "stale_or_unverified":
        return "[stale or unverified context] "
    if framing == "unknown_or_unverified":
        return "[freshness unknown; do not treat as current] "
    return ""


def _truth_framing(item: dict[str, Any]) -> str:
    framing = item.get("_truth_framing")
    if isinstance(framing, str) and framing:
        return framing
    memory_hygiene = item.get("memory_hygiene")
    if isinstance(memory_hygiene, dict) and isinstance(memory_hygiene.get("framing"), str):
        return memory_hygiene["framing"]
    return "current"


def _is_current_truth_item(item: dict[str, Any]) -> bool:
    return _truth_framing(item) in {"current", "corrected_replacement"}


def build_recent_history(retrieval_bundle: dict[str, Any]) -> list[dict[str, str]]:
    bundle = retrieval_bundle.get("bundle", {})
    messages: list[dict[str, str]] = []

    recent = bundle.get("recent", []) or []
    for item in recent:
        role = item.get("role")
        content = item.get("content", "")
        if role in VALID_ROLES and content:
            messages.append({"role": role, "content": f"{_memory_hygiene_prefix(item)}{content}"})
    return messages


def build_retrieval_messages(retrieval_bundle: dict[str, Any]) -> list[dict[str, str]]:
    bundle = retrieval_bundle.get("bundle", {})
    messages: list[dict[str, str]] = []

    semantic = bundle.get("semantic", []) or []
    artifact_refs = bundle.get("artifact_refs", []) or []
    truth_annotated = any(
        isinstance(item, dict) and ("_truth_framing" in item or "memory_hygiene" in item)
        for item in [*semantic, *artifact_refs]
    )

    current_lines: list[str] = ["Current memory evidence:"]
    historical_lines: list[str] = ["Historical or unverified memory context:"]
    current_count = 0
    historical_count = 0

    if truth_annotated:
        for item in semantic:
            target = current_lines if _is_current_truth_item(item) else historical_lines
            if target is current_lines:
                current_count += 1
            else:
                historical_count += 1
            created_at = item.get("created_at", "")
            role = item.get("role", "")
            content = item.get("content", "")
            target.append(f"- {_memory_hygiene_prefix(item)}[{created_at}] {role}: {content}")

        for item in artifact_refs:
            target = current_lines if _is_current_truth_item(item) else historical_lines
            if target is current_lines:
                current_count += 1
            else:
                historical_count += 1
            repo_name = item.get("repo_name")
            file_path = item.get("file_path", "")
            label = f"{repo_name}/{file_path}" if repo_name else file_path
            target.append(
                f"- {_memory_hygiene_prefix(item)}[{label}] {item.get('snippet', '')}"
            )

    if not truth_annotated and semantic:
        lines = ["Retrieved memory excerpts:"]
        for item in semantic:
            created_at = item.get("created_at", "")
            role = item.get("role", "")
            content = item.get("content", "")
            lines.append(
                f"- {_memory_hygiene_prefix(item)}[{created_at}] {role}: {content}"
            )
        messages.append({"role": "system", "content": "\n".join(lines)})

    if not truth_annotated and artifact_refs:
        lines = ["Retrieved file snippets:"]
        for item in artifact_refs:
            repo_name = item.get("repo_name")
            file_path = item.get("file_path", "")
            label = f"{repo_name}/{file_path}" if repo_name else file_path
            lines.append(
                f"- {_memory_hygiene_prefix(item)}[{label}] {item.get('snippet', '')}"
            )
        messages.append({"role": "system", "content": "\n".join(lines)})

    if not truth_annotated:
        return messages

    if current_count or historical_count:
        guidance = ["Memory truth guidance:"]
        if current_count:
            guidance.append(
                "- Use current canonical evidence as the primary basis for "
                "current-state or next-action claims."
            )
            guidance.append("- Use supported active derived context only as augmentation.")
        if historical_count:
            guidance.append(
                "- Do not use historical, parked, stale, expired, or unknown "
                "context as proof of what is current."
            )
            guidance.append(
                "- When only historical or unverified memory context is available, "
                "state uncertainty or describe it historically."
            )
        if current_count == 0 and historical_count:
            guidance.append(
                "- The current state is not established by memory context in this turn."
            )
        guidance.append(
            "- Do not mention internal freshness, provider, fallback, or orchestration mechanics."
        )
        messages.append({"role": "system", "content": "\n".join(guidance)})

    if current_count:
        messages.append({"role": "system", "content": "\n".join(current_lines)})
    if historical_count:
        messages.append({"role": "system", "content": "\n".join(historical_lines)})

    return messages


def retrieval_snippet_trace(retrieval_bundle: dict[str, Any]) -> dict[str, Any]:
    bundle = retrieval_bundle.get("bundle", {})
    semantic = bundle.get("semantic", []) or []
    artifact_refs = bundle.get("artifact_refs", []) or []
    return {
        "current_count": sum(
            1
            for item in [*semantic, *artifact_refs]
            if isinstance(item, dict) and _is_current_truth_item(item)
        ),
        "historical_or_unverified_count": sum(
            1
            for item in [*semantic, *artifact_refs]
            if isinstance(item, dict) and not _is_current_truth_item(item)
        ),
        "semantic": [
            {
                "message_id": item.get("message_id"),
                "created_at": item.get("created_at"),
                "role": item.get("role"),
                "score": item.get("score"),
                "truth_framing": _truth_framing(item),
            }
            for item in semantic
        ],
        "artifact_refs": [
            {
                "artifact_id": item.get("artifact_id"),
                "file_path": item.get("file_path"),
                "repo_name": item.get("repo_name"),
                "relevance_score": item.get("relevance_score"),
                "truth_framing": _truth_framing(item),
            }
            for item in artifact_refs
        ],
    }


def build_external_context_messages(context_pack: dict[str, Any] | None) -> list[dict[str, str]]:
    if not isinstance(context_pack, dict):
        return []

    items = context_pack.get("items") or []
    lines = ["External source context:"]
    included = 0
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if not isinstance(text, str) or not text:
            continue
        source_name = item.get("source_name") or "Unknown source"
        title = item.get("title") or "Untitled"
        source_ref = item.get("source_ref") or "unknown"
        lines.extend(
            [
                f"[{index}] {source_name} — {title}",
                f"source_ref: {source_ref}",
                text,
            ]
        )
        included += 1

    if included == 0:
        return []
    return [{"role": "system", "content": "\n".join(lines)}]


def external_context_trace(context_pack: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(context_pack, dict):
        return {"item_count": 0, "sources_used": [], "source_refs": []}

    items = context_pack.get("items") or []
    source_refs = []
    for item in items:
        if isinstance(item, dict) and item.get("source_ref"):
            source_refs.append(item["source_ref"])

    return {
        "item_count": len(source_refs),
        "sources_used": context_pack.get("sources_used", []) or [],
        "source_refs": source_refs,
    }


def _validated_governance_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _validated_governance_label(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    if not SAFE_GOVERNANCE_LABEL.fullmatch(value):
        return None
    return value


def _validated_label_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        item
        for item in value
        if isinstance(item, str) and SAFE_GOVERNANCE_LABEL.fullmatch(item)
    ]


def _validated_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _validated_confidence(value: Any) -> float | int | None:
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _sanitize_prompt_overlay(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    overlay = value.strip()
    if not overlay or len(overlay) > PROMPT_OVERLAY_MAX_CHARS:
        return None
    lowered = overlay.lower()
    if any(marker in lowered for marker in PROMPT_INJECTION_MARKERS):
        return None
    return overlay


def _sanitize_interaction_governance(governance: dict[str, Any] | None) -> dict[str, Any]:
    governance = governance if isinstance(governance, dict) else {}
    response_posture = governance.get("response_posture")
    privacy_hint = governance.get("privacy_sensitivity_hint")
    reason_summary = governance.get("reason_summary", [])
    safe_reason_summary: list[str] = []
    if isinstance(reason_summary, list):
        safe_reason_summary = [
            item
            for item in reason_summary
            if isinstance(item, str) and SAFE_GOVERNANCE_LABEL.fullmatch(item)
        ]

    return {
        "interaction_kind": governance.get("interaction_kind"),
        "response_posture": (
            response_posture
            if isinstance(response_posture, str)
            and response_posture in VALID_GOVERNANCE_RESPONSE_POSTURES
            else None
        ),
        "commentary_allowed": _validated_governance_bool(
            governance.get("commentary_allowed")
        ),
        "humor_allowed": _validated_governance_bool(governance.get("humor_allowed")),
        "clarifying_question_allowed": _validated_governance_bool(
            governance.get("clarifying_question_allowed")
        ),
        "action_allowed": _validated_governance_bool(governance.get("action_allowed")),
        "requires_confirmation": _validated_governance_bool(
            governance.get("requires_confirmation")
        ),
        "persona_scope_hint": _validated_governance_label(
            governance.get("persona_scope_hint")
        ),
        "privacy_sensitivity_hint": (
            privacy_hint
            if isinstance(privacy_hint, str)
            and privacy_hint in VALID_GOVERNANCE_PRIVACY_HINTS
            else None
        ),
        "confidence": governance.get("confidence"),
        "reason_summary": safe_reason_summary,
    }


def build_interaction_governance_messages(
    governance: dict[str, Any] | None,
) -> list[dict[str, str]]:
    sanitized = _sanitize_interaction_governance(governance)

    lines = ["Interaction guidance:"]

    response_posture = sanitized.get("response_posture")
    if isinstance(response_posture, str):
        lines.append(f"- Adopt a {response_posture} response posture.")
        if response_posture == "tactical":
            lines.append("- Prefer direct operational help and next concrete steps.")

    if sanitized.get("humor_allowed") is False:
        lines.append("- Do not add jokes or playful commentary.")
    if sanitized.get("commentary_allowed") is False:
        lines.append("- Avoid extra meta-commentary.")
    if sanitized.get("clarifying_question_allowed") is True:
        lines.append("- Ask a clarifying question when needed to move the task forward safely.")
    if sanitized.get("action_allowed") is False:
        lines.append("- Do not imply that any external action has been performed.")
    if sanitized.get("requires_confirmation") is True:
        lines.append("- Confirm before treating this turn as an action command.")

    privacy_hint = sanitized.get("privacy_sensitivity_hint")
    if privacy_hint in {"private", "sensitive"}:
        lines.append("- Avoid unnecessary disclosure or over-specific sensitive details.")

    persona_scope_hint = sanitized.get("persona_scope_hint")
    if isinstance(persona_scope_hint, str):
        lines.append(f"- Stay within the hinted scope: {persona_scope_hint}.")

    if len(lines) == 1:
        return []
    return [{"role": "system", "content": "\n".join(lines)}]


def interaction_governance_trace(governance_trace: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(governance_trace, dict):
        governance_trace = {}

    sanitized = _sanitize_interaction_governance(governance_trace)

    return {
        "attempted": governance_trace.get("attempted", False),
        "status": governance_trace.get("status", "not_requested"),
        "included": governance_trace.get("included", False),
        "runtime_call_status": governance_trace.get("runtime_call_status"),
        "interaction_kind": governance_trace.get("interaction_kind"),
        "response_posture": sanitized.get("response_posture"),
        "commentary_allowed": sanitized.get("commentary_allowed"),
        "humor_allowed": sanitized.get("humor_allowed"),
        "action_allowed": sanitized.get("action_allowed"),
        "requires_confirmation": sanitized.get("requires_confirmation"),
        "privacy_sensitivity_hint": sanitized.get("privacy_sensitivity_hint"),
        "confidence": sanitized.get("confidence"),
        "reason_summary": sanitized.get("reason_summary", []),
        "omission_reason": governance_trace.get("omission_reason"),
    }


def _sanitize_persona_containment(
    persona_containment: dict[str, Any] | None,
) -> dict[str, Any]:
    persona_containment = persona_containment if isinstance(persona_containment, dict) else {}
    return {
        "active_persona_id": _validated_governance_label(
            persona_containment.get("active_persona_id")
        ),
        "capability_domain": _validated_governance_label(
            persona_containment.get("capability_domain")
        ),
        "allowed_memory_domains": _validated_label_list(
            persona_containment.get("allowed_memory_domains")
        ),
        "blocked_memory_domains": _validated_label_list(
            persona_containment.get("blocked_memory_domains")
        ),
        "allowed_world_state_domains": _validated_label_list(
            persona_containment.get("allowed_world_state_domains")
        ),
        "allowed_relationship_domains": _validated_label_list(
            persona_containment.get("allowed_relationship_domains")
        ),
        "allowed_tool_domains": _validated_label_list(
            persona_containment.get("allowed_tool_domains")
        ),
        "cross_scope_access_allowed": _validated_bool(
            persona_containment.get("cross_scope_access_allowed")
        ),
        "cross_scope_reason": _validated_governance_label(
            persona_containment.get("cross_scope_reason")
        ),
        "confidence": _validated_confidence(persona_containment.get("confidence")),
        "reason_summary": _validated_label_list(persona_containment.get("reason_summary")),
    }


def build_persona_containment_messages(
    persona_containment: dict[str, Any] | None,
) -> list[dict[str, str]]:
    sanitized = _sanitize_persona_containment(persona_containment)
    lines = ["Persona containment guidance:"]

    active_persona_id = sanitized.get("active_persona_id")
    capability_domain = sanitized.get("capability_domain")
    if isinstance(active_persona_id, str):
        lines.append(f"- Stay within the active persona: {active_persona_id}.")
    if isinstance(capability_domain, str):
        lines.append(f"- Keep the response within the capability domain: {capability_domain}.")

    if sanitized.get("allowed_memory_domains"):
        domains = ", ".join(sanitized["allowed_memory_domains"])
        lines.append(f"- Memory scope hints for this turn: {domains}.")
    if sanitized.get("blocked_memory_domains"):
        domains = ", ".join(sanitized["blocked_memory_domains"])
        lines.append(f"- Treat these memory domains as blocked scope hints: {domains}.")
    if sanitized.get("allowed_tool_domains"):
        domains = ", ".join(sanitized["allowed_tool_domains"])
        lines.append(f"- Tool scope hints for this turn: {domains}.")

    if (
        sanitized.get("allowed_memory_domains")
        or sanitized.get("blocked_memory_domains")
        or sanitized.get("allowed_world_state_domains")
        or sanitized.get("allowed_relationship_domains")
        or sanitized.get("allowed_tool_domains")
    ):
        lines.append(
            "- Treat domain lists as scope guidance only; do not imply retrieval, tool access, world-state access, or relationship access occurred."
        )

    if sanitized.get("cross_scope_access_allowed") is True:
        lines.append("- Cross-scope bridging is allowed only for this turn.")
    elif sanitized.get("cross_scope_access_allowed") is False:
        lines.append(
            "- Do not bridge blocked or unrelated domains unless the user explicitly requests it."
        )

    if len(lines) == 1:
        return []
    return [{"role": "system", "content": "\n".join(lines)}]


def persona_containment_trace(persona_trace: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(persona_trace, dict):
        persona_trace = {}

    sanitized = _sanitize_persona_containment(persona_trace)
    return {
        "attempted": persona_trace.get("attempted", False),
        "status": persona_trace.get("status", "not_requested"),
        "included": persona_trace.get("included", False),
        "active_persona_id": sanitized.get("active_persona_id"),
        "capability_domain": sanitized.get("capability_domain"),
        "allowed_memory_domains": sanitized.get("allowed_memory_domains", []),
        "blocked_memory_domains": sanitized.get("blocked_memory_domains", []),
        "allowed_world_state_domains": sanitized.get("allowed_world_state_domains", []),
        "allowed_relationship_domains": sanitized.get(
            "allowed_relationship_domains", []
        ),
        "allowed_tool_domains": sanitized.get("allowed_tool_domains", []),
        "cross_scope_access_allowed": sanitized.get("cross_scope_access_allowed"),
        "cross_scope_reason": sanitized.get("cross_scope_reason"),
        "confidence": sanitized.get("confidence"),
        "reason_summary": sanitized.get("reason_summary", []),
        "retrieval_scope_requested": persona_trace.get("retrieval_scope_requested"),
        "retrieval_scope_used": persona_trace.get("retrieval_scope_used"),
        "retrieval_scope_status": persona_trace.get("retrieval_scope_status"),
        "retrieval_scope_reason": persona_trace.get("retrieval_scope_reason"),
        "artifact_request_status": persona_trace.get("artifact_request_status"),
        "artifact_request_reason": persona_trace.get("artifact_request_reason"),
        "artifact_result_status": persona_trace.get("artifact_result_status"),
        "artifact_result_reason": persona_trace.get("artifact_result_reason"),
        "artifact_result_count_omitted": persona_trace.get(
            "artifact_result_count_omitted"
        ),
        "domain_retrieval_scope_status": persona_trace.get(
            "domain_retrieval_scope_status"
        ),
        "domain_retrieval_scope_reason": persona_trace.get(
            "domain_retrieval_scope_reason"
        ),
        "tool_scope_status": persona_trace.get("tool_scope_status"),
        "tool_scope_reason": persona_trace.get("tool_scope_reason"),
        "omission_reason": persona_trace.get("omission_reason"),
    }


def _sanitize_restraint(restraint: dict[str, Any] | None) -> dict[str, Any]:
    restraint = restraint if isinstance(restraint, dict) else {}
    policy = restraint.get("restraint_policy")
    return {
        "restraint_policy": (
            policy
            if isinstance(policy, str) and policy in VALID_RESTRAINT_POLICIES
            else None
        ),
        "domains": _validated_label_list(restraint.get("domains")),
        "reason": _validated_governance_label(restraint.get("reason")),
        "prompt_overlay": _sanitize_prompt_overlay(restraint.get("prompt_overlay")),
        "confidence": _validated_confidence(restraint.get("confidence")),
        "reason_summary": _validated_label_list(restraint.get("reason_summary")),
        "retrieval_suppressed": _validated_bool(restraint.get("retrieval_suppressed")),
        "personalization_suppressed": _validated_bool(
            restraint.get("personalization_suppressed")
        ),
        "proactive_output_suppressed": _validated_bool(
            restraint.get("proactive_output_suppressed")
        ),
        "brevity_preferred": _validated_bool(restraint.get("brevity_preferred")),
        "clarification_preferred": _validated_bool(
            restraint.get("clarification_preferred")
        ),
    }


def build_restraint_messages(restraint: dict[str, Any] | None) -> list[dict[str, str]]:
    sanitized = _sanitize_restraint(restraint)
    lines = ["Restraint guidance:"]

    prompt_overlay = sanitized.get("prompt_overlay")
    if isinstance(prompt_overlay, str):
        lines.append(f"- {prompt_overlay}")

    restraint_policy = sanitized.get("restraint_policy")
    if isinstance(restraint_policy, str) and restraint_policy != "answer_normally":
        lines.append(f"- Apply the {restraint_policy} restraint policy.")

    if sanitized.get("domains"):
        lines.append(
            f"- Affected restraint domains: {', '.join(sanitized['domains'])}."
        )

    if sanitized.get("retrieval_suppressed") is True:
        lines.append("- Do not assume retrieval or prior context should be surfaced.")
    if sanitized.get("personalization_suppressed") is True:
        lines.append("- Avoid unnecessary personal framing.")
    if sanitized.get("proactive_output_suppressed") is True:
        lines.append("- Do not add unsolicited follow-ups or proactive nudges.")
    if sanitized.get("brevity_preferred") is True and restraint_policy != "short_answer":
        lines.append("- Keep the response brief.")
    if sanitized.get("clarification_preferred") is True and (
        restraint_policy != "ask_clarifying_question"
    ):
        lines.append("- Ask one clarifying question instead of assuming details.")

    if len(lines) == 1:
        return []
    return [{"role": "system", "content": "\n".join(lines)}]


def restraint_trace(restraint_trace_data: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(restraint_trace_data, dict):
        restraint_trace_data = {}

    sanitized = _sanitize_restraint(restraint_trace_data)
    return {
        "attempted": restraint_trace_data.get("attempted", False),
        "status": restraint_trace_data.get("status", "not_requested"),
        "included": restraint_trace_data.get("included", False),
        "restraint_policy": sanitized.get("restraint_policy"),
        "domains": sanitized.get("domains", []),
        "reason": sanitized.get("reason"),
        "confidence": sanitized.get("confidence"),
        "reason_summary": sanitized.get("reason_summary", []),
        "retrieval_suppressed": sanitized.get("retrieval_suppressed"),
        "personalization_suppressed": sanitized.get("personalization_suppressed"),
        "proactive_output_suppressed": sanitized.get("proactive_output_suppressed"),
        "brevity_preferred": sanitized.get("brevity_preferred"),
        "clarification_preferred": sanitized.get("clarification_preferred"),
        "omission_reason": restraint_trace_data.get("omission_reason"),
    }


def _sanitize_privacy_context(privacy_context: dict[str, Any] | None) -> dict[str, Any]:
    privacy_context = privacy_context if isinstance(privacy_context, dict) else {}
    surface_type = privacy_context.get("surface_type")
    privacy_zone = privacy_context.get("privacy_zone")
    sensitivity_level = privacy_context.get("sensitivity_level")
    reason_codes = privacy_context.get("reason_codes")
    sanitized_reason_codes: list[str] = []
    if isinstance(reason_codes, list):
        sanitized_reason_codes = [
            item
            for item in reason_codes
            if isinstance(item, str) and VALID_PRIVACY_REASON_CODES.fullmatch(item)
        ]

    return {
        "surface_type": (
            surface_type
            if isinstance(surface_type, str) and surface_type in PRIVACY_SURFACE_CATEGORIES
            else None
        ),
        "privacy_zone": (
            privacy_zone if isinstance(privacy_zone, str) and privacy_zone in PRIVACY_ZONES else None
        ),
        "sensitivity_level": (
            sensitivity_level
            if isinstance(sensitivity_level, str) and sensitivity_level in PRIVACY_SENSITIVITY_LEVELS
            else None
        ),
        "sensitive_detail_allowed": _validated_bool(
            privacy_context.get("sensitive_detail_allowed")
        ),
        "notification_detail_allowed": _validated_bool(
            privacy_context.get("notification_detail_allowed")
        ),
        "voice_detail_allowed": _validated_bool(privacy_context.get("voice_detail_allowed")),
        "screen_detail_allowed": _validated_bool(privacy_context.get("screen_detail_allowed")),
        "redaction_required": _validated_bool(privacy_context.get("redaction_required")),
        "safe_summary_required": _validated_bool(
            privacy_context.get("safe_summary_required")
        ),
        "reason_codes": sanitized_reason_codes,
    }


def build_privacy_context_messages(
    privacy_context: dict[str, Any] | None,
) -> list[dict[str, str]]:
    sanitized = _sanitize_privacy_context(privacy_context)
    surface_type = sanitized.get("surface_type")
    if not isinstance(surface_type, str):
        return []

    lines = [f"Privacy context guidance:", f"- Active channel type: {surface_type}."]
    if sanitized.get("sensitive_detail_allowed") is True:
        lines.append("- Sensitive detail is allowed on this surface only when otherwise safe.")
    else:
        lines.append("- Sensitive detail is not allowed on this surface.")

    if sanitized.get("safe_summary_required") is True or sanitized.get("redaction_required") is True:
        lines.append("- Use a safe summary or full withholding instead of detailed disclosure.")
        lines.append("- Never expose raw memory details when a safe summary is required.")

    if surface_type == "notification_preview":
        lines.append("- Keep notification output minimal and redirect to a private surface.")
    elif surface_type == "car_voice_possible_passenger":
        lines.append("- Keep car voice output concise and withhold private detail.")
    elif surface_type == "glasses_public_or_semi_public":
        lines.append("- Keep public-display output concise and avoid private specifics.")
    elif surface_type == "voice_private":
        lines.append("- Keep voice output concise and avoid oversharing.")
    elif surface_type == "unknown_surface":
        lines.append("- Stay conservative because the current surface is not trusted as private.")

    return [{"role": "system", "content": "\n".join(lines)}]


def privacy_context_trace(privacy_context_trace_data: dict[str, Any] | None) -> dict[str, Any]:
    privacy_context_trace_data = (
        privacy_context_trace_data if isinstance(privacy_context_trace_data, dict) else {}
    )
    sanitized = _sanitize_privacy_context(privacy_context_trace_data)
    return {
        "attempted": privacy_context_trace_data.get("attempted", False),
        "status": privacy_context_trace_data.get("status", "not_requested"),
        "included": privacy_context_trace_data.get("included", False),
        "runtime_call_status": privacy_context_trace_data.get("runtime_call_status"),
        "policy_source": privacy_context_trace_data.get("policy_source"),
        "surface_type": sanitized.get("surface_type"),
        "privacy_zone": sanitized.get("privacy_zone"),
        "sensitivity_level": sanitized.get("sensitivity_level"),
        "sensitivity_domain_count": privacy_context_trace_data.get(
            "sensitivity_domain_count",
            0,
        ),
        "sensitive_detail_allowed": sanitized.get("sensitive_detail_allowed"),
        "notification_detail_allowed": sanitized.get("notification_detail_allowed"),
        "voice_detail_allowed": sanitized.get("voice_detail_allowed"),
        "screen_detail_allowed": sanitized.get("screen_detail_allowed"),
        "redaction_required": sanitized.get("redaction_required"),
        "safe_summary_required": sanitized.get("safe_summary_required"),
        "reason_codes": sanitized.get("reason_codes", []),
        "fallback_applied": privacy_context_trace_data.get("fallback_applied"),
        "fallback_reason": privacy_context_trace_data.get("fallback_reason"),
        "enforcement_required": privacy_context_trace_data.get("enforcement_required"),
        "action_taken": privacy_context_trace_data.get("action_taken"),
        "template_id": privacy_context_trace_data.get("template_id"),
        "sources_suppressed_count": privacy_context_trace_data.get(
            "sources_suppressed_count",
            0,
        ),
        "trace_bundle_suppressed": privacy_context_trace_data.get(
            "trace_bundle_suppressed",
            False,
        ),
        "brief_text_suppressed": privacy_context_trace_data.get(
            "brief_text_suppressed",
            False,
        ),
    }


def assemble_prompt(
    *,
    profile: dict[str, Any],
    retrieval_bundle: dict[str, Any],
    current_messages: list[dict[str, str]],
    handoff: AssistantHandoff | None = None,
    presentation: CompanionPresentation | None = None,
    style_guidance: str | None = None,
    style_trace: dict[str, Any] | None = None,
    response_shape_guidance: str | None = None,
    response_shape_trace: dict[str, Any] | None = None,
    surface_presence_trace: dict[str, Any] | None = None,
    companion_overlays: list[dict[str, Any]] | None = None,
    companion_trace: dict[str, Any] | None = None,
    runtime_identity: dict[str, Any] | None = None,
    runtime_identity_trace: dict[str, Any] | None = None,
    world_state: dict[str, Any] | None = None,
    world_state_trace: dict[str, Any] | None = None,
    relationship_context: dict[str, Any] | None = None,
    relationship_context_trace: dict[str, Any] | None = None,
    runtime_overlay: dict[str, Any] | None = None,
    runtime_trace: dict[str, Any] | None = None,
    interaction_governance: dict[str, Any] | None = None,
    interaction_governance_trace_data: dict[str, Any] | None = None,
    persona_containment: dict[str, Any] | None = None,
    persona_containment_trace_data: dict[str, Any] | None = None,
    restraint: dict[str, Any] | None = None,
    restraint_trace_data: dict[str, Any] | None = None,
    memory_hygiene_trace_data: dict[str, Any] | None = None,
    privacy_context: dict[str, Any] | None = None,
    privacy_context_trace_data: dict[str, Any] | None = None,
    interrupt_trace: dict[str, Any] | None = None,
    external_context_pack: dict[str, Any] | None = None,
    dsa_trace: dict[str, Any] | None = None,
) -> PromptAssembly:
    messages: list[dict[str, str]] = []
    layers: list[dict[str, Any]] = []

    system_prompt = profile.get("prompt_overlay", "")
    profile_messages = [{"role": "system", "content": system_prompt}] if system_prompt else []
    messages.extend(profile_messages)
    layers.append(_layer_trace("profile_overlay", profile_messages))

    style_trace_out = dict(style_trace or {})
    style_messages = [{"role": "system", "content": style_guidance}] if style_guidance else []
    messages.extend(style_messages)
    layers.append(
        _layer_trace(
            "style_guidance",
            style_messages,
            metadata={
                "source_fields": style_trace_out.get("source_fields", []),
                "recognized_profile_fields": style_trace_out.get(
                    "recognized_profile_fields", []
                ),
                "recognized_request_fields": style_trace_out.get(
                    "recognized_request_fields", []
                ),
                "guidance_flags": style_trace_out.get("guidance_flags", {}),
                "resolved_envelope": style_trace_out.get("resolved_envelope", {}),
                "omission_reason": style_trace_out.get("omission_reason"),
            },
        )
    )

    response_shape_trace_out = dict(response_shape_trace or {})
    response_shape_messages = (
        [{"role": "system", "content": response_shape_guidance}]
        if response_shape_guidance
        else []
    )
    messages.extend(response_shape_messages)
    layers.append(
        _layer_trace(
            "response_shape",
            response_shape_messages,
            metadata={
                "source_fields": response_shape_trace_out.get("source_fields", []),
                "guidance_flags": response_shape_trace_out.get("guidance_flags", {}),
                "resolved_shape": response_shape_trace_out.get("resolved_shape", {}),
                "continuation_state": response_shape_trace_out.get("continuation_state"),
                "abbreviation_reason": response_shape_trace_out.get("abbreviation_reason"),
                "omission_reason": response_shape_trace_out.get("omission_reason"),
            },
        )
    )

    presentation_input = presentation.prompt_input if presentation is not None else None
    companion_overlays_in = (
        presentation_input.companion_overlays
        if presentation_input is not None
        else companion_overlays
    )
    runtime_overlay_in = (
        presentation_input.runtime_overlay if presentation_input is not None else runtime_overlay
    )

    companion_messages: list[dict[str, str]] = []
    companion_trace_out = dict(companion_trace or {})
    companion_omission_reason = companion_trace_out.get("omission_reason")
    companion_overlay_metadata: list[dict[str, Any]] = []
    invalid_companion_roles: list[str | None] = []
    for overlay in companion_overlays_in or []:
        if not isinstance(overlay, dict):
            invalid_companion_roles.append(None)
            continue
        overlay_type = overlay.get("overlay_type")
        overlay_id = overlay.get("overlay_id")
        role = overlay.get("role")
        content = overlay.get("content")
        if role == "system" and isinstance(content, str) and content:
            companion_messages.append({"role": "system", "content": content})
            companion_overlay_metadata.append(
                {"overlay_id": overlay_id, "overlay_type": overlay_type}
            )
        else:
            invalid_companion_roles.append(overlay_type)

    if companion_messages:
        messages.extend(companion_messages)
        companion_trace_out.update(
            {
                "status": "included",
                "included": True,
                "included_overlays": companion_overlay_metadata,
            }
        )
        if invalid_companion_roles:
            companion_trace_out["omitted_overlay_types"] = invalid_companion_roles
            companion_trace_out["omission_reason"] = "invalid_companion_overlay_role"
    elif companion_overlays_in and invalid_companion_roles:
        companion_omission_reason = "invalid_companion_overlay_role"
        companion_trace_out.update(
            {
                "status": "omitted",
                "included": False,
                "included_overlays": [],
                "omission_reason": companion_omission_reason,
                "invalid_overlay_types": invalid_companion_roles,
            }
        )

    runtime_overlay_ids = []
    if runtime_overlay_in and runtime_overlay_in.get("overlay_id"):
        runtime_overlay_ids.append(runtime_overlay_in["overlay_id"])

    companion_trace_out.setdefault("companion_profile_id", companion_trace_out.get("profile_id"))
    companion_trace_out.setdefault(
        "companion_profile_version", companion_trace_out.get("profile_version")
    )
    companion_trace_out.setdefault(
        "interaction_contract_id", companion_trace_out.get("contract_id")
    )
    companion_trace_out.setdefault(
        "interaction_contract_version", companion_trace_out.get("contract_version")
    )
    companion_trace_out.setdefault(
        "companion_policy_warnings", companion_trace_out.get("warnings", [])
    )
    companion_trace_out["companion_overlay_ids"] = [
        item.get("overlay_id") for item in companion_overlay_metadata if item.get("overlay_id")
    ]
    companion_trace_out["runtime_overlay_ids"] = runtime_overlay_ids
    companion_trace_out.setdefault(
        "cognitive_runtime_compile_status", companion_trace_out.get("status")
    )
    companion_trace_out.setdefault("cognitive_runtime_compile_error", None)
    companion_trace_out.setdefault("cognitive_runtime_compile_endpoint", None)

    companion_metadata = {
        "profile_id": companion_trace_out.get("profile_id"),
        "profile_version": companion_trace_out.get("profile_version"),
        "contract_id": companion_trace_out.get("contract_id"),
        "contract_version": companion_trace_out.get("contract_version"),
        "contract_trace": companion_trace_out.get("contract_trace"),
        "interaction_contract": companion_trace_out.get("interaction_contract"),
        "scene_id": companion_trace_out.get("scene_id"),
        "scene_confidence": companion_trace_out.get("scene_confidence"),
        "scene_source": companion_trace_out.get("scene_source"),
        "warnings": companion_trace_out.get("warnings", []),
        "companion_profile_id": companion_trace_out.get("companion_profile_id"),
        "companion_profile_version": companion_trace_out.get(
            "companion_profile_version"
        ),
        "interaction_contract_id": companion_trace_out.get("interaction_contract_id"),
        "interaction_contract_version": companion_trace_out.get(
            "interaction_contract_version"
        ),
        "companion_policy_warnings": companion_trace_out.get(
            "companion_policy_warnings", []
        ),
        "companion_overlay_ids": companion_trace_out.get("companion_overlay_ids", []),
        "runtime_overlay_ids": companion_trace_out.get("runtime_overlay_ids", []),
        "cognitive_runtime_compile_status": companion_trace_out.get(
            "cognitive_runtime_compile_status"
        ),
        "cognitive_runtime_compile_error": companion_trace_out.get(
            "cognitive_runtime_compile_error"
        ),
        "cognitive_runtime_compile_endpoint": companion_trace_out.get(
            "cognitive_runtime_compile_endpoint"
        ),
        "included_overlays": companion_overlay_metadata,
        "omitted_overlay_types": invalid_companion_roles,
        "omission_reason": companion_omission_reason,
    }
    layers.append(
        _layer_trace(
            "companion_policy",
            companion_messages,
            metadata=companion_metadata,
        )
    )

    interaction_governance_messages = build_interaction_governance_messages(
        interaction_governance
    )
    governance_trace_out = interaction_governance_trace(interaction_governance_trace_data)
    if interaction_governance_messages:
        messages.extend(interaction_governance_messages)
    elif governance_trace_out.get("attempted") and governance_trace_out.get("status") == "included":
        governance_trace_out.update(
            {
                "status": "failed",
                "included": False,
                "runtime_call_status": "unusable",
                "omission_reason": "unusable_interaction_governance_response",
            }
        )
    layers.append(
        _layer_trace(
            "interaction_governance",
            interaction_governance_messages,
            metadata={
                "runtime_call_status": governance_trace_out.get("runtime_call_status"),
                "interaction_kind": governance_trace_out.get("interaction_kind"),
                "response_posture": governance_trace_out.get("response_posture"),
                "commentary_allowed": governance_trace_out.get("commentary_allowed"),
                "humor_allowed": governance_trace_out.get("humor_allowed"),
                "action_allowed": governance_trace_out.get("action_allowed"),
                "requires_confirmation": governance_trace_out.get(
                    "requires_confirmation"
                ),
                "privacy_sensitivity_hint": governance_trace_out.get(
                    "privacy_sensitivity_hint"
                ),
                "confidence": governance_trace_out.get("confidence"),
                "reason_summary": governance_trace_out.get("reason_summary", []),
                "omission_reason": governance_trace_out.get("omission_reason"),
            },
        )
    )

    persona_containment_messages = build_persona_containment_messages(persona_containment)
    persona_containment_trace_out = persona_containment_trace(
        persona_containment_trace_data
    )
    if persona_containment_messages:
        messages.extend(persona_containment_messages)
    elif (
        persona_containment_trace_out.get("attempted")
        and persona_containment_trace_out.get("status") == "included"
    ):
        persona_containment_trace_out.update(
            {
                "status": "failed",
                "included": False,
                "omission_reason": "unusable_persona_containment_response",
            }
        )
    layers.append(
        _layer_trace(
            "persona_containment",
            persona_containment_messages,
            metadata={
                "active_persona_id": persona_containment_trace_out.get("active_persona_id"),
                "capability_domain": persona_containment_trace_out.get("capability_domain"),
                "allowed_memory_domains": persona_containment_trace_out.get(
                    "allowed_memory_domains", []
                ),
                "blocked_memory_domains": persona_containment_trace_out.get(
                    "blocked_memory_domains", []
                ),
                "allowed_world_state_domains": persona_containment_trace_out.get(
                    "allowed_world_state_domains", []
                ),
                "allowed_relationship_domains": persona_containment_trace_out.get(
                    "allowed_relationship_domains", []
                ),
                "allowed_tool_domains": persona_containment_trace_out.get(
                    "allowed_tool_domains", []
                ),
                "cross_scope_access_allowed": persona_containment_trace_out.get(
                    "cross_scope_access_allowed"
                ),
                "cross_scope_reason": persona_containment_trace_out.get(
                    "cross_scope_reason"
                ),
                "confidence": persona_containment_trace_out.get("confidence"),
                "reason_summary": persona_containment_trace_out.get("reason_summary", []),
                "retrieval_scope_requested": persona_containment_trace_out.get(
                    "retrieval_scope_requested"
                ),
                "retrieval_scope_used": persona_containment_trace_out.get(
                    "retrieval_scope_used"
                ),
                "retrieval_scope_status": persona_containment_trace_out.get(
                    "retrieval_scope_status"
                ),
                "retrieval_scope_reason": persona_containment_trace_out.get(
                    "retrieval_scope_reason"
                ),
                "artifact_request_status": persona_containment_trace_out.get(
                    "artifact_request_status"
                ),
                "artifact_request_reason": persona_containment_trace_out.get(
                    "artifact_request_reason"
                ),
                "artifact_result_status": persona_containment_trace_out.get(
                    "artifact_result_status"
                ),
                "artifact_result_reason": persona_containment_trace_out.get(
                    "artifact_result_reason"
                ),
                "artifact_result_count_omitted": persona_containment_trace_out.get(
                    "artifact_result_count_omitted"
                ),
                "domain_retrieval_scope_status": persona_containment_trace_out.get(
                    "domain_retrieval_scope_status"
                ),
                "domain_retrieval_scope_reason": persona_containment_trace_out.get(
                    "domain_retrieval_scope_reason"
                ),
                "tool_scope_status": persona_containment_trace_out.get(
                    "tool_scope_status"
                ),
                "tool_scope_reason": persona_containment_trace_out.get(
                    "tool_scope_reason"
                ),
                "omission_reason": persona_containment_trace_out.get("omission_reason"),
            },
        )
    )

    restraint_messages = build_restraint_messages(restraint)
    restraint_trace_out = restraint_trace(restraint_trace_data)
    if restraint_messages:
        messages.extend(restraint_messages)
    elif restraint_trace_out.get("attempted") and restraint_trace_out.get("status") == "included":
        restraint_trace_out.update(
            {
                "status": "failed",
                "included": False,
                "omission_reason": "unusable_restraint_response",
            }
        )
    layers.append(
        _layer_trace(
            "restraint",
            restraint_messages,
            metadata={
                "restraint_policy": restraint_trace_out.get("restraint_policy"),
                "domains": restraint_trace_out.get("domains", []),
                "reason": restraint_trace_out.get("reason"),
                "confidence": restraint_trace_out.get("confidence"),
                "reason_summary": restraint_trace_out.get("reason_summary", []),
                "retrieval_suppressed": restraint_trace_out.get("retrieval_suppressed"),
                "personalization_suppressed": restraint_trace_out.get(
                    "personalization_suppressed"
                ),
                "proactive_output_suppressed": restraint_trace_out.get(
                    "proactive_output_suppressed"
                ),
                "brevity_preferred": restraint_trace_out.get("brevity_preferred"),
                "clarification_preferred": restraint_trace_out.get(
                    "clarification_preferred"
                ),
                "omission_reason": restraint_trace_out.get("omission_reason"),
            },
        )
    )

    privacy_context_messages = build_privacy_context_messages(privacy_context)
    privacy_context_trace_out = privacy_context_trace(privacy_context_trace_data)
    if privacy_context_messages:
        messages.extend(privacy_context_messages)
        layers.append(
            _layer_trace(
                "privacy_context",
                privacy_context_messages,
                metadata={
                    "runtime_call_status": privacy_context_trace_out.get("runtime_call_status"),
                    "policy_source": privacy_context_trace_out.get("policy_source"),
                    "surface_type": privacy_context_trace_out.get("surface_type"),
                    "privacy_zone": privacy_context_trace_out.get("privacy_zone"),
                    "sensitivity_level": privacy_context_trace_out.get("sensitivity_level"),
                    "sensitive_detail_allowed": privacy_context_trace_out.get(
                        "sensitive_detail_allowed"
                    ),
                    "safe_summary_required": privacy_context_trace_out.get(
                        "safe_summary_required"
                    ),
                },
            )
        )

    runtime_identity_messages: list[dict[str, str]] = []
    runtime_identity_trace_out = dict(runtime_identity_trace or {})
    runtime_identity_omission_reason = runtime_identity_trace_out.get("omission_reason")
    if runtime_identity and runtime_identity.get("content"):
        runtime_identity_messages.append(
            {"role": "system", "content": runtime_identity["content"]}
        )
        messages.extend(runtime_identity_messages)
    layers.append(
        _layer_trace(
            "runtime_identity",
            runtime_identity_messages,
            metadata={
                "active_persona_id": runtime_identity.get("active_persona_id")
                if runtime_identity
                else None,
                "surface_id": runtime_identity.get("surface_id") if runtime_identity else None,
                "capability_domain": runtime_identity.get("capability_domain")
                if runtime_identity
                else None,
                "advisory_memory_scope_summary": runtime_identity.get(
                    "advisory_memory_scope_summary", []
                )
                if runtime_identity
                else [],
                "advisory_tool_permission_summary": runtime_identity.get(
                    "advisory_tool_permission_summary", []
                )
                if runtime_identity
                else [],
                "omission_reason": runtime_identity_omission_reason,
            },
        )
    )

    world_state_messages: list[dict[str, str]] = []
    world_state_trace_out = dict(world_state_trace or {})
    world_state_omission_reason = world_state_trace_out.get("omission_reason")
    if world_state and world_state.get("prompt_content"):
        world_state_messages.append({"role": "system", "content": world_state["prompt_content"]})
        messages.extend(world_state_messages)
    layers.append(
        _layer_trace(
            "world_state",
            world_state_messages,
            metadata={
                "included_claim_count": world_state_trace_out.get("included_claim_count", 0),
                "excluded_claim_count": world_state_trace_out.get("excluded_claim_count", 0),
                "stale_count": world_state_trace_out.get("stale_count", 0),
                "aging_count": world_state_trace_out.get("aging_count", 0),
                "expired_count": world_state_trace_out.get("expired_count", 0),
                "conflicted_count": world_state_trace_out.get("conflicted_count", 0),
                "active_persona_id": world_state_trace_out.get("active_persona_id"),
                "allowed_domains": world_state_trace_out.get("allowed_domains", []),
                "confirmation_required": world_state_trace_out.get("confirmation_required", False),
                "omission_reason": world_state_omission_reason,
            },
        )
    )

    relationship_context_messages: list[dict[str, str]] = []
    relationship_context_trace_out = dict(relationship_context_trace or {})
    relationship_context_omission_reason = relationship_context_trace_out.get(
        "omission_reason"
    )
    if relationship_context and relationship_context.get("prompt_content"):
        relationship_context_messages.append(
            {"role": "system", "content": relationship_context["prompt_content"]}
        )
        messages.extend(relationship_context_messages)
    layers.append(
        _layer_trace(
            "relationship_context",
            relationship_context_messages,
            metadata={
                "selected_relationship_count": relationship_context_trace_out.get(
                    "selected_relationship_count", 0
                ),
                "excluded_relationship_count": relationship_context_trace_out.get(
                    "excluded_relationship_count", 0
                ),
                "relationship_edges_used": relationship_context_trace_out.get(
                    "relationship_edges_used", []
                ),
                "relationship_edges_excluded": relationship_context_trace_out.get(
                    "relationship_edges_excluded", []
                ),
                "relationship_exclusion_reasons": relationship_context_trace_out.get(
                    "relationship_exclusion_reasons", {}
                ),
                "relationship_context_overlay_applied": relationship_context_trace_out.get(
                    "relationship_context_overlay_applied", False
                ),
                "relationship_conflicts": relationship_context_trace_out.get(
                    "relationship_conflicts", []
                ),
                "relationship_confirmation_required": relationship_context_trace_out.get(
                    "relationship_confirmation_required", False
                ),
                "active_persona_id": relationship_context_trace_out.get(
                    "active_persona_id"
                ),
                "allowed_relationship_scopes": relationship_context_trace_out.get(
                    "allowed_relationship_scopes", []
                ),
                "omission_reason": relationship_context_omission_reason,
            },
        )
    )

    runtime_messages: list[dict[str, str]] = []
    runtime_trace_out = dict(runtime_trace or {})
    runtime_omission_reason = runtime_trace_out.get("omission_reason")
    if runtime_overlay_in and runtime_overlay_in.get("content"):
        role = runtime_overlay_in.get("role", "system")
        if role == "system":
            runtime_messages.append({"role": "system", "content": runtime_overlay_in["content"]})
            messages.extend(runtime_messages)
        else:
            runtime_omission_reason = "invalid_runtime_overlay_role"
            runtime_trace_out.update(
                {
                    "status": "omitted",
                    "included": False,
                    "omission_reason": runtime_omission_reason,
                }
            )
    layers.append(
        _layer_trace(
            "runtime_overlay",
            runtime_messages,
            metadata={
                "runtime_state_id": runtime_overlay_in.get("runtime_state_id")
                if runtime_overlay_in
                else None,
                "overlay_id": runtime_overlay_in.get("overlay_id") if runtime_overlay_in else None,
                "overlay_type": (
                    runtime_overlay_in.get("overlay_type")
                    if runtime_overlay_in
                    else None
                ),
                "source_fields": runtime_overlay_in.get("source_fields", [])
                if runtime_overlay_in
                else [],
                "omission_reason": runtime_omission_reason,
            },
        )
    )

    external_context_messages = build_external_context_messages(external_context_pack)
    messages.extend(external_context_messages)
    layers.append(
        _layer_trace(
            "external_source_context",
            external_context_messages,
            metadata=external_context_trace(external_context_pack),
        )
    )

    retrieval_messages = build_retrieval_messages(retrieval_bundle)
    messages.extend(retrieval_messages)
    layers.append(
        _layer_trace(
            "retrieval_augmentation",
            retrieval_messages,
            metadata={"snippets": retrieval_snippet_trace(retrieval_bundle)},
        )
    )

    recent_history = build_recent_history(retrieval_bundle)
    messages.extend(recent_history)
    layers.append(_layer_trace("recent_history", recent_history))

    messages.extend(current_messages)
    layers.append(_layer_trace("current_messages", current_messages))

    trace = {
        "layers": layers,
        "included_layers": [layer["name"] for layer in layers if layer["included"]],
        "omitted_layers": [layer["name"] for layer in layers if not layer["included"]],
        "truncation": {"applied": False, "reason": None},
        "handoff": handoff.trace_summary() if handoff is not None else None,
        "presentation": presentation.trace_summary() if presentation is not None else None,
        "style": style_trace_out or {"attempted": False, "status": "not_requested"},
        "response_shape": response_shape_trace_out
        or {"attempted": False, "status": "not_requested"},
        "surface_presence": surface_presence_trace
        or {"attempted": False, "status": "not_requested"},
        "companion_policy": companion_trace_out
        or {"attempted": False, "status": "not_requested"},
        "interaction_governance": governance_trace_out
        or {"attempted": False, "status": "not_requested", "included": False},
        "persona_containment": persona_containment_trace_out
        or {"attempted": False, "status": "not_requested", "included": False},
        "restraint": restraint_trace_out
        or {"attempted": False, "status": "not_requested", "included": False},
        "memory_hygiene": memory_hygiene_trace_data
        or {"attempted": False, "status": "not_requested", "included": False},
        "privacy_context": privacy_context_trace_out
        or {"attempted": False, "status": "not_requested", "included": False},
        "runtime_identity": runtime_identity_trace_out
        or {"attempted": False, "status": "not_requested"},
        "world_state": world_state_trace_out or {"attempted": False, "status": "not_requested"},
        "relationship_context": relationship_context_trace_out
        or {"attempted": False, "status": "not_requested"},
        "runtime": runtime_trace_out or {"attempted": False, "status": "not_requested"},
        "dsa": dsa_trace or {"enabled": False, "called": False, "status": "disabled"},
        "message_count": len(messages),
    }
    if interrupt_trace is not None:
        trace["interrupt_policy"] = interrupt_trace

    return PromptAssembly(messages=messages, trace=trace)
