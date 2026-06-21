from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

PRIVACY_SURFACE_CATEGORIES = {
    "desktop_private",
    "mobile_private",
    "telegram_private",
    "voice_private",
    "car_voice_possible_passenger",
    "glasses_public_or_semi_public",
    "notification_preview",
    "unknown_surface",
}
PRIVACY_ZONES = {
    "private",
    "shared_or_uncertain",
    "public_or_semi_public",
    "preview_limited",
    "unknown",
}
PRIVACY_SENSITIVITY_LEVELS = {
    "normal",
    "sensitive",
    "highly_sensitive",
    "unknown",
}
PRIVACY_SENSITIVITY_DOMAINS = {
    "personal",
    "health",
    "financial",
    "work",
}
_PRIVATE_DETAIL_SURFACES = {
    "desktop_private",
    "mobile_private",
    "telegram_private",
    "voice_private",
}
_NOTIFICATION_ALIASES = {"notification", "notification_preview"}
_GLASSES_ALIASES = {"glasses"}
_SURFACE_RANK = {
    "normal": 0,
    "sensitive": 1,
    "highly_sensitive": 2,
    "unknown": 3,
}
_RETRIEVAL_SECTIONS = ("recent", "semantic", "artifact_refs")
_RESTRICTED_RETRIEVAL_LAYER = {
    "semantic": [],
    "artifact_refs": [],
}


@dataclass(frozen=True)
class DerivedPrivacyContext:
    surface_category: str
    sensitivity_level: str
    sensitivity_domains: list[str]


@dataclass(frozen=True)
class PrivacyBoundaryResult:
    final_answer: str
    enforced: bool
    template_id: str | None
    action_taken: str
    sources_suppressed_count: int
    trace_bundle_suppressed: bool
    brief_text_suppressed: bool


def disabled_privacy_trace() -> dict[str, Any]:
    return {
        "attempted": False,
        "status": "disabled",
        "included": False,
        "runtime_call_status": "disabled",
        "policy_source": "disabled",
        "fallback_applied": False,
        "enforcement_required": False,
        "action_taken": "none",
        "template_id": None,
        "sources_suppressed_count": 0,
        "trace_bundle_suppressed": False,
        "brief_text_suppressed": False,
    }


def _normalize_label(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    return normalized or None


def _normalize_domain(value: Any) -> str | None:
    normalized = _normalize_label(value)
    if normalized == "finance":
        normalized = "financial"
    if normalized in PRIVACY_SENSITIVITY_DOMAINS:
        return normalized
    return None


def _normalize_level(value: Any) -> str | None:
    normalized = _normalize_label(value)
    if normalized in PRIVACY_SENSITIVITY_LEVELS:
        return normalized
    return None


def _escalate_level(current: str, candidate: str | None) -> str:
    if candidate is None:
        return current
    if _SURFACE_RANK[candidate] > _SURFACE_RANK[current]:
        return candidate
    return current


def _iter_policy_metadata(retrieval_bundle: dict[str, Any]) -> list[dict[str, Any]]:
    bundle = retrieval_bundle.get("bundle", {})
    metadata_items: list[dict[str, Any]] = []
    for section in _RETRIEVAL_SECTIONS:
        items = bundle.get(section)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            policy_metadata = item.get("policy_metadata")
            if isinstance(policy_metadata, dict):
                metadata_items.append(policy_metadata)
    return metadata_items


def derive_privacy_context(
    *,
    payload: dict[str, Any],
    retrieval_bundle: dict[str, Any],
) -> DerivedPrivacyContext:
    surface_context = payload.get("surface_context")
    surface_context = surface_context if isinstance(surface_context, dict) else {}

    explicit_surface_category = _normalize_label(surface_context.get("surface_category"))
    if explicit_surface_category in PRIVACY_SURFACE_CATEGORIES:
        surface_category = explicit_surface_category
    else:
        surface_type = _normalize_label(surface_context.get("surface_type"))
        if surface_type in PRIVACY_SURFACE_CATEGORIES:
            surface_category = surface_type
        else:
            top_level_surface = _normalize_label(payload.get("surface"))
            if top_level_surface in PRIVACY_SURFACE_CATEGORIES:
                surface_category = top_level_surface
            elif top_level_surface == "car" or surface_type == "car":
                surface_category = "car_voice_possible_passenger"
            elif (
                top_level_surface in _NOTIFICATION_ALIASES
                or surface_type in _NOTIFICATION_ALIASES
            ):
                surface_category = "notification_preview"
            elif top_level_surface in _GLASSES_ALIASES or surface_type in _GLASSES_ALIASES:
                surface_category = "glasses_public_or_semi_public"
            else:
                surface_category = "unknown_surface"

    base_sensitivity = {
        "public": "normal",
        "private": "sensitive",
        "local_only": "highly_sensitive",
    }.get(payload.get("sensitivity"), "unknown")
    sensitivity_level = _escalate_level(
        base_sensitivity,
        _normalize_level(surface_context.get("sensitivity_level")),
    )

    sensitivity_domains: list[str] = []

    explicit_domains = surface_context.get("sensitivity_domains")
    if isinstance(explicit_domains, list):
        for domain in explicit_domains:
            normalized_domain = _normalize_domain(domain)
            if normalized_domain and normalized_domain not in sensitivity_domains:
                sensitivity_domains.append(normalized_domain)

    for policy_metadata in _iter_policy_metadata(retrieval_bundle):
        sensitivity_level = _escalate_level(
            sensitivity_level,
            _normalize_level(policy_metadata.get("sensitivity")),
        )
        memory_domains = policy_metadata.get("memory_domains")
        if not isinstance(memory_domains, list):
            continue
        for domain in memory_domains:
            normalized_domain = _normalize_domain(domain)
            if normalized_domain and normalized_domain not in sensitivity_domains:
                sensitivity_domains.append(normalized_domain)

    if sensitivity_domains:
        sensitivity_level = _escalate_level(sensitivity_level, "sensitive")

    return DerivedPrivacyContext(
        surface_category=surface_category,
        sensitivity_level=sensitivity_level,
        sensitivity_domains=sensitivity_domains,
    )


def privacy_fallback_policy(
    *,
    surface_category: str,
    sensitivity_level: str,
    fallback_reason: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    allow_detail = (
        surface_category in _PRIVATE_DETAIL_SURFACES and sensitivity_level == "normal"
    )
    applicable_voice = surface_category in {"voice_private", "car_voice_possible_passenger"}
    applicable_notification = surface_category == "notification_preview"
    if allow_detail:
        result = {
            "privacy_zone": "private",
            "surface_type": surface_category,
            "sensitivity_level": sensitivity_level,
            "sensitive_detail_allowed": True,
            "notification_detail_allowed": applicable_notification,
            "voice_detail_allowed": applicable_voice,
            "screen_detail_allowed": not applicable_voice,
            "redaction_required": False,
            "safe_summary_required": False,
            "reason_codes": ["fallback_private_normal"],
        }
    else:
        result = {
            "privacy_zone": "unknown",
            "surface_type": surface_category,
            "sensitivity_level": sensitivity_level,
            "sensitive_detail_allowed": False,
            "notification_detail_allowed": False,
            "voice_detail_allowed": False,
            "screen_detail_allowed": False,
            "redaction_required": True,
            "safe_summary_required": True,
            "reason_codes": ["fallback_conservative_restriction"],
        }
    trace = {
        "attempted": True,
        "status": "included",
        "included": True,
        "runtime_call_status": "fallback",
        "policy_source": "fallback",
        "surface_type": result["surface_type"],
        "privacy_zone": result["privacy_zone"],
        "sensitivity_level": result["sensitivity_level"],
        "sensitivity_domain_count": 0,
        "sensitive_detail_allowed": result["sensitive_detail_allowed"],
        "notification_detail_allowed": result["notification_detail_allowed"],
        "voice_detail_allowed": result["voice_detail_allowed"],
        "screen_detail_allowed": result["screen_detail_allowed"],
        "redaction_required": result["redaction_required"],
        "safe_summary_required": result["safe_summary_required"],
        "reason_codes": result["reason_codes"],
        "fallback_applied": True,
        "fallback_reason": fallback_reason,
        "enforcement_required": False,
        "action_taken": "none",
        "template_id": None,
        "sources_suppressed_count": 0,
        "trace_bundle_suppressed": False,
        "brief_text_suppressed": False,
    }
    return result, trace


def _applicable_channel_allowed(policy: dict[str, Any]) -> bool:
    surface_type = policy.get("surface_type")
    if surface_type == "notification_preview":
        return policy.get("notification_detail_allowed") is True
    if surface_type in {"voice_private", "car_voice_possible_passenger"}:
        return policy.get("voice_detail_allowed") is True
    return policy.get("screen_detail_allowed") is True


def _template_for_surface(surface_type: str) -> tuple[str, str]:
    templates = {
        "notification_preview": (
            "notification_private_update",
            "A private update is available. Open a private surface for details.",
        ),
        "car_voice_possible_passenger": (
            "car_voice_private_update",
            "Relevant private information exists, but details are withheld in the car.",
        ),
        "glasses_public_or_semi_public": (
            "glasses_private_update",
            "A private update exists. Use a private screen for details.",
        ),
        "voice_private": (
            "voice_private_update",
            "Sensitive details are withheld from voice output.",
        ),
        "unknown_surface": (
            "unknown_surface_private_update",
            "Details cannot safely be shown on this surface.",
        ),
    }
    return templates.get(
        surface_type,
        (
            "screen_private_update",
            "Sensitive details are withheld on this surface.",
        ),
    )


def apply_privacy_boundary(
    *,
    policy: dict[str, Any],
    answer: str,
    sources: list[dict[str, Any]],
) -> PrivacyBoundaryResult:
    must_replace = (
        policy.get("redaction_required") is True
        or policy.get("safe_summary_required") is True
        or policy.get("sensitive_detail_allowed") is not True
        or not _applicable_channel_allowed(policy)
    )
    if not must_replace:
        return PrivacyBoundaryResult(
            final_answer=answer,
            enforced=False,
            template_id=None,
            action_taken="none",
            sources_suppressed_count=0,
            trace_bundle_suppressed=False,
            brief_text_suppressed=False,
        )

    template_id, safe_answer = _template_for_surface(
        str(policy.get("surface_type") or "unknown_surface")
    )
    return PrivacyBoundaryResult(
        final_answer=safe_answer,
        enforced=True,
        template_id=template_id,
        action_taken="replaced_with_safe_template",
        sources_suppressed_count=len(sources),
        trace_bundle_suppressed=True,
        brief_text_suppressed=True,
    )


def restricted_retrieval_trace_summary(retrieval_bundle: dict[str, Any]) -> dict[str, Any]:
    bundle = retrieval_bundle.get("bundle", {})
    recent = bundle.get("recent") if isinstance(bundle.get("recent"), list) else []
    semantic = bundle.get("semantic") if isinstance(bundle.get("semantic"), list) else []
    artifact_refs = (
        bundle.get("artifact_refs") if isinstance(bundle.get("artifact_refs"), list) else []
    )
    return {
        "privacy_suppressed": True,
        "recent_item_count": len(recent),
        "semantic_item_count": len(semantic),
        "artifact_count": len(artifact_refs),
    }


def sanitize_prompt_trace_for_privacy(
    prompt_trace: dict[str, Any],
    retrieval_bundle: dict[str, Any],
) -> dict[str, Any]:
    sanitized = deepcopy(prompt_trace)
    retrieval_summary = restricted_retrieval_trace_summary(retrieval_bundle)

    handoff = sanitized.get("handoff")
    if isinstance(handoff, dict) and isinstance(handoff.get("retrieval"), dict):
        handoff["retrieval"] = {
            "query_present": handoff["retrieval"].get("query_present", False),
            "semantic_count": retrieval_summary["semantic_item_count"],
            "artifact_ref_count": retrieval_summary["artifact_count"],
            "recent_history_count": retrieval_summary["recent_item_count"],
            "observed_metadata": {
                "has_code_like_content": bool(
                    ((handoff["retrieval"].get("observed_metadata") or {}).get(
                        "has_code_like_content",
                        False,
                    ))
                )
            },
            "privacy_suppressed": True,
        }

    presentation = sanitized.get("presentation")
    if isinstance(presentation, dict) and isinstance(presentation.get("retrieval"), dict):
        presentation["retrieval"] = {
            "semantic_count": retrieval_summary["semantic_item_count"],
            "artifact_ref_count": retrieval_summary["artifact_count"],
            "recent_history_count": retrieval_summary["recent_item_count"],
            "privacy_suppressed": True,
        }

    layers = sanitized.get("layers")
    if isinstance(layers, list):
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            if layer.get("name") == "retrieval_augmentation":
                layer["metadata"] = {
                    "privacy_suppressed": True,
                    "semantic_count": retrieval_summary["semantic_item_count"],
                    "artifact_count": retrieval_summary["artifact_count"],
                    "snippets": dict(_RESTRICTED_RETRIEVAL_LAYER),
                }
    return sanitized


def validate_privacy_policy_result(result: Any) -> dict[str, Any] | None:
    if not isinstance(result, dict):
        return None
    privacy_zone = result.get("privacy_zone")
    surface_type = result.get("surface_type")
    sensitivity_level = result.get("sensitivity_level")
    reason_codes = result.get("reason_codes")
    if privacy_zone not in PRIVACY_ZONES:
        return None
    if surface_type not in PRIVACY_SURFACE_CATEGORIES:
        return None
    if sensitivity_level not in PRIVACY_SENSITIVITY_LEVELS:
        return None
    if not isinstance(reason_codes, list):
        return None
    validated_reason_codes: list[str] = []
    for code in reason_codes:
        normalized = _normalize_label(code)
        if normalized is None:
            return None
        validated_reason_codes.append(normalized)
    bool_fields = (
        "sensitive_detail_allowed",
        "notification_detail_allowed",
        "voice_detail_allowed",
        "screen_detail_allowed",
        "redaction_required",
        "safe_summary_required",
    )
    validated: dict[str, Any] = {
        "privacy_zone": privacy_zone,
        "surface_type": surface_type,
        "sensitivity_level": sensitivity_level,
        "reason_codes": validated_reason_codes,
    }
    for field in bool_fields:
        value = result.get(field)
        if not isinstance(value, bool):
            return None
        validated[field] = value
    return validated
