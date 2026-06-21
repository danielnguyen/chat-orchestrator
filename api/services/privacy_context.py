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
_PRIVACY_SENSITIVE_FALLBACK_LEVEL = "sensitive"


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


def _merge_metadata_fields(mapping: dict[str, Any]) -> tuple[str | None, list[str], bool]:
    recognized = False
    level = _normalize_level(mapping.get("sensitivity_level"))
    if level is None:
        level = _normalize_level(mapping.get("sensitivity"))
    if level is not None:
        recognized = True

    domains: list[str] = []
    for key in ("sensitivity_domains", "memory_domains", "domain_tags"):
        values = mapping.get(key)
        if not isinstance(values, list):
            continue
        for value in values:
            normalized = _normalize_domain(value)
            if normalized is not None and normalized not in domains:
                domains.append(normalized)
                recognized = True

    policy_metadata = mapping.get("policy_metadata")
    if isinstance(policy_metadata, dict):
        nested_level, nested_domains, nested_recognized = _merge_metadata_fields(policy_metadata)
        if nested_level is not None and level is None:
            level = nested_level
        for domain in nested_domains:
            if domain not in domains:
                domains.append(domain)
        recognized = recognized or nested_recognized

    return level, domains, recognized


def _apply_metadata(
    *,
    current_level: str,
    current_domains: list[str],
    metadata: dict[str, Any],
    conservative_on_missing: bool = False,
) -> tuple[str, list[str]]:
    level, domains, recognized = _merge_metadata_fields(metadata)
    next_level = _escalate_level(current_level, level)
    next_domains = list(current_domains)
    for domain in domains:
        if domain not in next_domains:
            next_domains.append(domain)
    if conservative_on_missing and not recognized:
        next_level = _escalate_level(next_level, _PRIVACY_SENSITIVE_FALLBACK_LEVEL)
    if next_domains:
        next_level = _escalate_level(next_level, "sensitive")
    return next_level, next_domains


def derive_privacy_context(
    *,
    payload: dict[str, Any],
    retrieval_bundle: dict[str, Any],
    external_context_pack: dict[str, Any] | None = None,
    runtime_identity: dict[str, Any] | None = None,
    runtime_overlay: dict[str, Any] | None = None,
    world_state: dict[str, Any] | None = None,
    relationship_context: dict[str, Any] | None = None,
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
        sensitivity_level, sensitivity_domains = _apply_metadata(
            current_level=sensitivity_level,
            current_domains=sensitivity_domains,
            metadata=policy_metadata,
        )

    external_items = (
        external_context_pack.get("items")
        if isinstance(external_context_pack, dict) and isinstance(external_context_pack.get("items"), list)
        else []
    )
    for item in external_items:
        if not isinstance(item, dict) or not isinstance(item.get("text"), str) or not item.get("text"):
            continue
        sensitivity_level, sensitivity_domains = _apply_metadata(
            current_level=sensitivity_level,
            current_domains=sensitivity_domains,
            metadata=item,
            conservative_on_missing=True,
        )

    for context in (runtime_identity, runtime_overlay, world_state, relationship_context):
        if not isinstance(context, dict):
            continue
        prompt_bearing = isinstance(context.get("content"), str) and bool(context.get("content"))
        prompt_bearing = prompt_bearing or (
            isinstance(context.get("prompt_content"), str) and bool(context.get("prompt_content"))
        )
        if not prompt_bearing:
            continue
        sensitivity_level, sensitivity_domains = _apply_metadata(
            current_level=sensitivity_level,
            current_domains=sensitivity_domains,
            metadata=context,
            conservative_on_missing=True,
        )

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
        runtime_summary = presentation.get("runtime")
        if isinstance(runtime_summary, dict):
            presentation["runtime"] = {
                "status": runtime_summary.get("status"),
                "overlay_present": bool(runtime_summary.get("overlay_ref")),
                "omission_reason": runtime_summary.get("omission_reason"),
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
            elif layer.get("name") == "companion_policy":
                metadata = layer.get("metadata") if isinstance(layer.get("metadata"), dict) else {}
                included_overlays = (
                    metadata.get("included_overlays", [])
                    if isinstance(metadata.get("included_overlays"), list)
                    else []
                )
                layer["metadata"] = {
                    "profile_id": metadata.get("profile_id"),
                    "profile_version": metadata.get("profile_version"),
                    "contract_id": metadata.get("contract_id"),
                    "contract_version": metadata.get("contract_version"),
                    "scene_id": metadata.get("scene_id"),
                    "scene_confidence": metadata.get("scene_confidence"),
                    "scene_source": metadata.get("scene_source"),
                    "warnings": metadata.get("warnings", []),
                    "companion_profile_id": metadata.get("companion_profile_id"),
                    "companion_profile_version": metadata.get("companion_profile_version"),
                    "interaction_contract_id": metadata.get("interaction_contract_id"),
                    "interaction_contract_version": metadata.get(
                        "interaction_contract_version"
                    ),
                    "companion_policy_warnings": metadata.get(
                        "companion_policy_warnings",
                        [],
                    ),
                    "companion_overlay_count": len(metadata.get("companion_overlay_ids", []) or []),
                    "runtime_overlay_count": len(metadata.get("runtime_overlay_ids", []) or []),
                    "included_overlay_count": len(included_overlays),
                    "omitted_overlay_type_count": len(
                        metadata.get("omitted_overlay_types", []) or []
                    ),
                    "cognitive_runtime_compile_status": metadata.get(
                        "cognitive_runtime_compile_status"
                    ),
                    "cognitive_runtime_compile_error": metadata.get(
                        "cognitive_runtime_compile_error"
                    ),
                    "cognitive_runtime_compile_endpoint": metadata.get(
                        "cognitive_runtime_compile_endpoint"
                    ),
                    "omission_reason": metadata.get("omission_reason"),
                }
            elif layer.get("name") == "external_source_context":
                metadata = layer.get("metadata") if isinstance(layer.get("metadata"), dict) else {}
                layer["metadata"] = {
                    "item_count": metadata.get("item_count", 0),
                    "source_count": len(metadata.get("sources_used", []) or []),
                    "privacy_suppressed": True,
                }
            elif layer.get("name") == "persona_containment":
                metadata = layer.get("metadata") if isinstance(layer.get("metadata"), dict) else {}
                layer["metadata"] = {
                    "allowed_memory_domain_count": len(metadata.get("allowed_memory_domains", []) or []),
                    "blocked_memory_domain_count": len(metadata.get("blocked_memory_domains", []) or []),
                    "allowed_world_state_domain_count": len(metadata.get("allowed_world_state_domains", []) or []),
                    "allowed_relationship_domain_count": len(
                        metadata.get("allowed_relationship_domains", []) or []
                    ),
                    "allowed_tool_domain_count": len(metadata.get("allowed_tool_domains", []) or []),
                    "cross_scope_access_allowed": metadata.get("cross_scope_access_allowed"),
                    "retrieval_scope_status": metadata.get("retrieval_scope_status"),
                    "retrieval_scope_reason": metadata.get("retrieval_scope_reason"),
                    "artifact_request_status": metadata.get("artifact_request_status"),
                    "artifact_result_status": metadata.get("artifact_result_status"),
                    "domain_retrieval_scope_status": metadata.get("domain_retrieval_scope_status"),
                    "tool_scope_status": metadata.get("tool_scope_status"),
                    "omission_reason": metadata.get("omission_reason"),
                }
            elif layer.get("name") == "world_state":
                metadata = layer.get("metadata") if isinstance(layer.get("metadata"), dict) else {}
                layer["metadata"] = {
                    "included_claim_count": metadata.get("included_claim_count", 0),
                    "excluded_claim_count": metadata.get("excluded_claim_count", 0),
                    "stale_count": metadata.get("stale_count", 0),
                    "aging_count": metadata.get("aging_count", 0),
                    "expired_count": metadata.get("expired_count", 0),
                    "conflicted_count": metadata.get("conflicted_count", 0),
                    "confirmation_required": metadata.get("confirmation_required", False),
                    "allowed_domain_count": len(metadata.get("allowed_domains", []) or []),
                    "omission_reason": metadata.get("omission_reason"),
                }
            elif layer.get("name") == "relationship_context":
                metadata = layer.get("metadata") if isinstance(layer.get("metadata"), dict) else {}
                layer["metadata"] = {
                    "selected_relationship_count": metadata.get("selected_relationship_count", 0),
                    "excluded_relationship_count": metadata.get("excluded_relationship_count", 0),
                    "relationship_edges_used_count": len(metadata.get("relationship_edges_used", []) or []),
                    "relationship_edges_excluded_count": len(
                        metadata.get("relationship_edges_excluded", []) or []
                    ),
                    "relationship_exclusion_reason_count": len(
                        metadata.get("relationship_exclusion_reasons", {}) or {}
                    ),
                    "relationship_context_overlay_applied": metadata.get(
                        "relationship_context_overlay_applied",
                        False,
                    ),
                    "relationship_conflict_count": len(metadata.get("relationship_conflicts", []) or []),
                    "relationship_confirmation_required": metadata.get(
                        "relationship_confirmation_required",
                        False,
                    ),
                    "allowed_relationship_scope_count": len(
                        metadata.get("allowed_relationship_scopes", []) or []
                    ),
                    "omission_reason": metadata.get("omission_reason"),
                }
            elif layer.get("name") == "runtime_identity":
                metadata = layer.get("metadata") if isinstance(layer.get("metadata"), dict) else {}
                layer["metadata"] = {
                    "capability_domain_present": bool(metadata.get("capability_domain")),
                    "advisory_memory_scope_count": len(
                        metadata.get("advisory_memory_scope_summary", []) or []
                    ),
                    "advisory_tool_permission_count": len(
                        metadata.get("advisory_tool_permission_summary", []) or []
                    ),
                    "omission_reason": metadata.get("omission_reason"),
                }
            elif layer.get("name") == "runtime_overlay":
                metadata = layer.get("metadata") if isinstance(layer.get("metadata"), dict) else {}
                layer["metadata"] = {
                    "overlay_type": metadata.get("overlay_type"),
                    "source_field_count": len(metadata.get("source_fields", []) or []),
                    "omission_reason": metadata.get("omission_reason"),
                }

    dsa_trace = sanitized.get("dsa")
    if isinstance(dsa_trace, dict):
        source_diagnostics = dsa_trace.get("source_diagnostics", [])
        candidate_counts = dsa_trace.get("candidate_counts_by_source", {})
        sanitized["dsa"] = {
            "capability_enabled": dsa_trace.get("capability_enabled"),
            "enabled": dsa_trace.get("enabled"),
            "called": dsa_trace.get("called"),
            "status": dsa_trace.get("status"),
            "reason": dsa_trace.get("reason"),
            "allowed_sensitivity": dsa_trace.get("allowed_sensitivity"),
            "max_results": dsa_trace.get("max_results"),
            "item_count": dsa_trace.get("item_count", 0),
            "errors_count": dsa_trace.get("errors_count", 0),
            "error_codes": dsa_trace.get("error_codes", []),
            "budget_truncated": dsa_trace.get("budget_truncated", False),
            "context_injected": dsa_trace.get("context_injected", False),
            "diagnostics_status": dsa_trace.get("diagnostics_status"),
            "selection_mode": dsa_trace.get("selection_mode"),
            "ranking_mode": dsa_trace.get("ranking_mode"),
            "selected_source_count": len(dsa_trace.get("selected_source_ids", []) or []),
            "considered_source_count": len(dsa_trace.get("considered_source_ids", []) or []),
            "source_diagnostics_count": len(source_diagnostics) if isinstance(source_diagnostics, list) else 0,
            "candidate_source_count": len(candidate_counts) if isinstance(candidate_counts, dict) else 0,
            "candidate_truncated": dsa_trace.get("candidate_truncated", False),
        }

    companion_trace = sanitized.get("companion_policy")
    if isinstance(companion_trace, dict):
        sanitized["companion_policy"] = {
            **companion_trace,
            "companion_overlay_count": len(companion_trace.get("companion_overlay_ids", []) or []),
            "runtime_overlay_count": len(companion_trace.get("runtime_overlay_ids", []) or []),
        }
        sanitized["companion_policy"].pop("companion_overlay_ids", None)
        sanitized["companion_policy"].pop("runtime_overlay_ids", None)

    persona_trace = sanitized.get("persona_containment")
    if isinstance(persona_trace, dict):
        sanitized["persona_containment"] = {
            "attempted": persona_trace.get("attempted", False),
            "status": persona_trace.get("status"),
            "included": persona_trace.get("included"),
            "allowed_memory_domain_count": len(persona_trace.get("allowed_memory_domains", []) or []),
            "blocked_memory_domain_count": len(persona_trace.get("blocked_memory_domains", []) or []),
            "allowed_world_state_domain_count": len(
                persona_trace.get("allowed_world_state_domains", []) or []
            ),
            "allowed_relationship_domain_count": len(
                persona_trace.get("allowed_relationship_domains", []) or []
            ),
            "allowed_tool_domain_count": len(persona_trace.get("allowed_tool_domains", []) or []),
            "cross_scope_access_allowed": persona_trace.get("cross_scope_access_allowed"),
            "cross_scope_reason": persona_trace.get("cross_scope_reason"),
            "retrieval_scope_status": persona_trace.get("retrieval_scope_status"),
            "retrieval_scope_reason": persona_trace.get("retrieval_scope_reason"),
            "artifact_request_status": persona_trace.get("artifact_request_status"),
            "artifact_request_reason": persona_trace.get("artifact_request_reason"),
            "artifact_result_status": persona_trace.get("artifact_result_status"),
            "artifact_result_reason": persona_trace.get("artifact_result_reason"),
            "artifact_result_count_omitted": persona_trace.get("artifact_result_count_omitted"),
            "domain_retrieval_scope_status": persona_trace.get("domain_retrieval_scope_status"),
            "domain_retrieval_scope_reason": persona_trace.get("domain_retrieval_scope_reason"),
            "tool_scope_status": persona_trace.get("tool_scope_status"),
            "tool_scope_reason": persona_trace.get("tool_scope_reason"),
            "omission_reason": persona_trace.get("omission_reason"),
        }

    world_state_trace = sanitized.get("world_state")
    if isinstance(world_state_trace, dict):
        sanitized["world_state"] = {
            "attempted": world_state_trace.get("attempted", False),
            "status": world_state_trace.get("status"),
            "included": world_state_trace.get("included"),
            "included_claim_count": world_state_trace.get("included_claim_count", 0),
            "excluded_claim_count": world_state_trace.get("excluded_claim_count", 0),
            "stale_count": world_state_trace.get("stale_count", 0),
            "aging_count": world_state_trace.get("aging_count", 0),
            "expired_count": world_state_trace.get("expired_count", 0),
            "conflicted_count": world_state_trace.get("conflicted_count", 0),
            "confirmation_required": world_state_trace.get("confirmation_required", False),
            "allowed_domain_count": len(world_state_trace.get("allowed_domains", []) or []),
            "omission_reason": world_state_trace.get("omission_reason"),
        }

    relationship_trace = sanitized.get("relationship_context")
    if isinstance(relationship_trace, dict):
        sanitized["relationship_context"] = {
            "attempted": relationship_trace.get("attempted", False),
            "status": relationship_trace.get("status"),
            "included": relationship_trace.get("included"),
            "selected_relationship_count": relationship_trace.get("selected_relationship_count", 0),
            "excluded_relationship_count": relationship_trace.get("excluded_relationship_count", 0),
            "relationship_edges_used_count": len(
                relationship_trace.get("relationship_edges_used", []) or []
            ),
            "relationship_edges_excluded_count": len(
                relationship_trace.get("relationship_edges_excluded", []) or []
            ),
            "relationship_exclusion_reason_count": len(
                relationship_trace.get("relationship_exclusion_reasons", {}) or {}
            ),
            "relationship_context_overlay_applied": relationship_trace.get(
                "relationship_context_overlay_applied",
                False,
            ),
            "relationship_conflict_count": len(
                relationship_trace.get("relationship_conflicts", []) or []
            ),
            "relationship_confirmation_required": relationship_trace.get(
                "relationship_confirmation_required",
                False,
            ),
            "allowed_relationship_scope_count": len(
                relationship_trace.get("allowed_relationship_scopes", []) or []
            ),
            "omission_reason": relationship_trace.get("omission_reason"),
        }

    runtime_identity_trace = sanitized.get("runtime_identity")
    if isinstance(runtime_identity_trace, dict):
        sanitized["runtime_identity"] = {
            "attempted": runtime_identity_trace.get("attempted", False),
            "status": runtime_identity_trace.get("status"),
            "included": runtime_identity_trace.get("included"),
            "persona_resolution_reason": runtime_identity_trace.get("persona_resolution_reason"),
            "persona_override_source": runtime_identity_trace.get("persona_override_source"),
            "surface_type": runtime_identity_trace.get("surface_type"),
            "surface_display_name": runtime_identity_trace.get("surface_display_name"),
            "advisory_memory_scope_count": len(
                runtime_identity_trace.get("advisory_memory_scope_summary", []) or []
            ),
            "advisory_tool_permission_count": len(
                runtime_identity_trace.get("advisory_tool_permission_summary", []) or []
            ),
            "omission_reason": runtime_identity_trace.get("omission_reason"),
        }

    runtime_trace = sanitized.get("runtime")
    if isinstance(runtime_trace, dict):
        sanitized["runtime"] = {
            "attempted": runtime_trace.get("attempted", False),
            "status": runtime_trace.get("status"),
            "included": runtime_trace.get("included"),
            "reset_after_turn": runtime_trace.get("reset_after_turn", False),
            "overlay_type": runtime_trace.get("overlay_type"),
            "source_field_count": len(runtime_trace.get("source_fields", []) or []),
            "omission_reason": runtime_trace.get("omission_reason"),
        }

    handoff_runtime = handoff.get("runtime") if isinstance(handoff, dict) else None
    if isinstance(handoff_runtime, dict):
        handoff["runtime"] = {
            "status": handoff_runtime.get("status"),
            "overlay_present": bool(handoff_runtime.get("overlay_ref")),
            "source_field_count": handoff_runtime.get("source_field_count", 0),
            "omission_reason": handoff_runtime.get("omission_reason"),
            "reset_after_turn": handoff_runtime.get("reset_after_turn", False),
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
