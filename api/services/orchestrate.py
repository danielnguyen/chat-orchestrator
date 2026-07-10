from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import httpx
import yaml
from clients.data_source_aggregator import DataSourceAggregatorClient
from clients.litellm import LiteLLMClient
from clients.memory_store import MemoryStoreClient
from router.engine import evaluate_route
from services.assistant_handoff import build_assistant_handoff
from services.briefing import generate_brief
from services.capabilities import (
    CapabilityValidationError,
    authorize_and_execute_capability,
    capability_follow_up_summary,
    capability_validation_failure_trace,
    ensure_draft_local_unsent_truth,
    filter_capability_descriptors_for_exposure,
    parse_provider_capability_request,
    provider_text,
    validate_and_digest_capability_request,
)
from services.companion_presentation import build_companion_presentation
from services.fallback import resolve_provider_attempt_plan
from services.memory_hygiene import apply_memory_hygiene, disabled_memory_hygiene_trace
from services.memory_recall_composition import (
    build_recall_candidates,
    compose_memory_recall_context,
)
from services.privacy_context import (
    apply_privacy_boundary,
    derive_privacy_context,
    disabled_privacy_trace,
    privacy_fallback_policy,
    privacy_policy_requires_suppression,
    restricted_retrieval_trace_summary,
    sanitize_prompt_trace_for_privacy,
    validate_privacy_runtime_response,
)
from services.profile_apply import apply_profile_to_request
from services.prompt_assembly import assemble_prompt
from services.prompt_budget import PromptBudgetContract, PromptBudgetError
from services.response_action import ResponseActionInput, apply_response_action
from services.response_review import ResponseReviewInput, review_response
from services.response_shape import (
    build_response_shape_guidance_block,
    resolve_response_shape,
)
from services.routing_contract import routing_trace_metadata
from services.style_envelope import build_style_guidance_block, resolve_style_envelope
from services.surface_presence import apply_surface_presence_outcome, resolve_surface_presence


def _extract_last_user_text(messages: list[dict[str, str]]) -> str:
    for msg in reversed(messages):
        if msg.get("role") == "user":
            return msg.get("content", "")
    return ""


def _runtime_disabled_trace() -> dict[str, Any]:
    return {"attempted": False, "status": "disabled", "included": False}


def _companion_disabled_trace() -> dict[str, Any]:
    return {
        "attempted": False,
        "status": "disabled",
        "included": False,
        "cognitive_runtime_compile_status": "disabled",
        "cognitive_runtime_compile_error": None,
        "cognitive_runtime_compile_endpoint": None,
    }


def _runtime_session_disabled_trace() -> dict[str, Any]:
    return {"attempted": False, "status": "disabled", "included": False}


def _turn_state_disabled_trace() -> dict[str, Any]:
    return {"attempted": False, "status": "disabled", "included": False}


def _runtime_identity_disabled_trace() -> dict[str, Any]:
    return {"attempted": False, "status": "disabled", "included": False}


def _world_state_disabled_trace() -> dict[str, Any]:
    return {"attempted": False, "status": "disabled", "included": False}


def _relationship_context_disabled_trace() -> dict[str, Any]:
    return {"attempted": False, "status": "disabled", "included": False}


def _capability_registry_disabled_trace() -> dict[str, Any]:
    return {
        "enabled": False,
        "status": "disabled",
        "context_included": False,
        "action_taken": False,
        "match": {"attempted": False, "status": "disabled"},
        "discovery": {"attempted": False, "status": "disabled"},
        "authority": {"attempted": False, "status": "disabled"},
    }


def _selected_relationship_ids_from_trace(trace: dict[str, Any] | None) -> list[str]:
    if not isinstance(trace, dict):
        return []
    value = trace.get("relationship_edges_used")
    if not isinstance(value, list):
        return []
    selected: list[str] = []
    for item in value:
        if not isinstance(item, str) or not SAFE_SCOPE_ID.fullmatch(item):
            continue
        if item not in selected:
            selected.append(item)
        if len(selected) >= 64:
            break
    return selected


def _interaction_governance_disabled_trace() -> dict[str, Any]:
    return {
        "attempted": False,
        "status": "disabled",
        "included": False,
        "runtime_call_status": "disabled",
    }


def _persona_containment_disabled_trace() -> dict[str, Any]:
    return {
        "attempted": False,
        "status": "disabled",
        "included": False,
        "retrieval_scope_status": "not_enforced",
        "retrieval_scope_reason": "retrieval_scope_not_enforced",
    }


def _ensure_persona_containment_trace_defaults(
    persona_trace: dict[str, Any] | None,
) -> dict[str, Any]:
    trace = persona_trace if isinstance(persona_trace, dict) else {}
    trace.setdefault("artifact_request_status", "not_enforced")
    trace.setdefault("artifact_request_reason", "artifact_request_not_enforced")
    trace.setdefault("artifact_result_status", "not_applied")
    trace.setdefault("artifact_result_reason", "artifact_result_suppression_not_applied")
    trace.setdefault("domain_retrieval_scope_status", "deferred")
    trace.setdefault(
        "domain_retrieval_scope_reason",
        "domain_aware_retrieval_enforcement_deferred",
    )
    trace.setdefault("tool_scope_status", "deferred")
    trace.setdefault("tool_scope_reason", "tool_enforcement_deferred")
    return trace


def _set_artifact_result_trace(
    *,
    persona_trace: dict[str, Any],
    status: str,
    reason: str,
) -> None:
    persona_trace["artifact_result_status"] = status
    persona_trace["artifact_result_reason"] = reason


def _persona_containment_lock_active(persona_containment: dict[str, Any] | None) -> bool:
    return (
        isinstance(persona_containment, dict)
        and persona_containment.get("cross_scope_access_allowed") is False
    )


def _privacy_context_disabled_trace() -> dict[str, Any]:
    return disabled_privacy_trace()


@dataclass(frozen=True)
class RetrievalBoundaryResult:
    retrieval: dict[str, Any] | None
    include_artifacts: bool | None
    allowed_memory_domains: list[str] | None
    blocked_memory_domains: list[str] | None


@dataclass(frozen=True)
class MandatoryRetrievalPolicy:
    containment_policy: dict[str, Any] | None
    relationship_context: dict[str, Any] | None
    relationship_trace: dict[str, Any]
    validation_trace: dict[str, Any]


SAFE_POLICY_LABEL = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
SAFE_SCOPE_ID = re.compile(r"^[A-Za-z0-9_.:/@-]{1,160}$")
ARTIFACT_CONTENT_CLASSES = {
    "document",
    "code",
    "image",
    "screenshot",
    "audio",
    "video",
    "other",
}
ARTIFACT_POLICY_FIELDS = {
    "enforcement_mode",
    "allowed_content_classes",
    "allowed_domains",
    "maximum_sensitivity",
    "surface_content_capabilities",
    "reason_codes",
}
RESULT_POLICY_FIELDS = {
    "memory_domains",
    "sensitivity",
    "content_class",
    "entity_ids",
    "relationship_ids",
    "relationship_scopes",
}
SENSITIVITY_RANK = {"low": 0, "medium": 1, "high": 2}
VALID_RESULT_ROLES = {"user", "assistant", "system", "tool"}
RESULT_SOURCE_REF_FIELDS = {"ref_type", "ref_id"}
PROVENANCE_SOURCE_REF_FIELDS = {
    "ref_type",
    "ref_id",
    "support_kind",
    "span",
    "field_path",
    "note",
    "metadata",
}
PROVENANCE_SOURCE_REF_REQUIRED_FIELDS = {"ref_type", "ref_id", "support_kind"}
PROVENANCE_REQUIRED_FIELDS = {
    "derived_id",
    "owner_id",
    "derivation_type",
    "source_refs",
    "derivation_version",
    "created_at",
    "status",
    "provenance_status",
}
RESULT_PROVENANCE_FIELDS = {
    "derived_id",
    "owner_id",
    "derivation_type",
    "source_refs",
    "derivation_version",
    "created_at",
    "status",
    "effective_status",
    "confidence",
    "explanation",
    "generation_trace_id",
    "compatibility_defaults",
    "provenance_status",
    "retrieval_reason",
}
CONTRADICTORY_PROVENANCE_STATUS_MARKERS = {
    "failed",
    "failure",
    "error",
    "invalid",
    "missing",
    "unavailable",
    "incomplete",
}
RETRIEVAL_DEBUG_STATUSES = {
    "ok",
    "available",
    "unavailable",
    "degraded",
    "disabled",
    "failed",
    "missing",
    "malformed",
    "suppressed",
}
RETRIEVAL_DEBUG_REASONS = {
    "fallback_provider_used",
    "mandatory_result_boundary_failed_closed",
    "malformed_retrieval_envelope",
    "memory_domain_not_allowed",
    "no_retrieval_results",
    "prompt_budget_exceeded",
    "relationship_projection_filtered",
    "retrieval_suppressed_true",
    "restraint_policy_do_not_retrieve",
    "source_unavailable",
    "vector_unavailable",
}
CODE_LIKE_RE = re.compile(
    r"\b(def|class|function|import|from|return|const|let|var|package)\b|[{};]",
    re.IGNORECASE,
)


def _sanitize_memory_domain_list(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    cleaned = [item for item in value if isinstance(item, str) and item]
    return cleaned or None


def _policy_label_list(value: Any, *, limit: int, required: bool = False) -> list[str] | None:
    if not isinstance(value, list) or len(value) > limit:
        return None
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            return None
        cleaned = item.strip()
        if not SAFE_POLICY_LABEL.fullmatch(cleaned):
            return None
        normalized = cleaned.lower().replace("-", "_")
        if normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    if required and not out:
        return None
    return out


def _artifact_class_list(value: Any, *, limit: int) -> list[str] | None:
    classes = _policy_label_list(value, limit=limit)
    if classes is None:
        return None
    if any(item not in ARTIFACT_CONTENT_CLASSES for item in classes):
        return None
    return classes


def _scope_id_list(value: Any, *, limit: int) -> list[str] | None:
    if not isinstance(value, list) or len(value) > limit:
        return None
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            return None
        cleaned = item.strip()
        if not SAFE_SCOPE_ID.fullmatch(cleaned):
            return None
        if cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


def _validate_artifact_access_policy(
    value: Any,
    *,
    allowed_memory_domains: list[str],
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(value, dict):
        return None, "missing_artifact_access_policy"
    if set(value) - ARTIFACT_POLICY_FIELDS:
        return None, "unexpected_artifact_policy_fields"
    if value.get("enforcement_mode") != "mandatory":
        return None, "invalid_artifact_policy_enforcement_mode"
    allowed_content_classes = _artifact_class_list(
        value.get("allowed_content_classes"),
        limit=8,
    )
    allowed_domains = _policy_label_list(value.get("allowed_domains"), limit=16)
    surface_content_capabilities = _artifact_class_list(
        value.get("surface_content_capabilities"),
        limit=8,
    )
    reason_codes = _policy_label_list(value.get("reason_codes"), limit=8)
    maximum_sensitivity = value.get("maximum_sensitivity")
    if maximum_sensitivity not in {"low", "medium", "high"}:
        return None, "invalid_artifact_policy_sensitivity"
    if (
        allowed_content_classes is None
        or allowed_domains is None
        or surface_content_capabilities is None
        or reason_codes is None
    ):
        return None, "malformed_artifact_access_policy"
    if not set(allowed_domains).issubset(set(allowed_memory_domains)):
        return None, "artifact_domains_outside_allowed_memory_domains"
    if not set(allowed_content_classes).issubset(set(surface_content_capabilities)):
        return None, "artifact_classes_outside_surface_capabilities"
    return {
        "enforcement_mode": "mandatory",
        "allowed_content_classes": allowed_content_classes,
        "allowed_domains": allowed_domains,
        "maximum_sensitivity": maximum_sensitivity,
        "surface_content_capabilities": surface_content_capabilities,
        "reason_codes": reason_codes,
    }, None


def _validate_relationship_projection(value: Any) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(value, dict):
        return None, "missing_relationship_scope_projection"
    if not isinstance(value.get("applied"), bool):
        return None, "malformed_relationship_scope_projection"
    relationship_ids = _scope_id_list(value.get("relationship_ids"), limit=64)
    entity_ids = _scope_id_list(value.get("entity_ids"), limit=64)
    relationship_scopes = _policy_label_list(value.get("relationship_scopes"), limit=16)
    reason_codes = _policy_label_list(value.get("reason_codes"), limit=8)
    if (
        relationship_ids is None
        or entity_ids is None
        or relationship_scopes is None
        or reason_codes is None
    ):
        return None, "malformed_relationship_scope_projection"
    if value.get("applied") is True and not relationship_ids and not entity_ids:
        return None, "empty_applied_relationship_scope_projection"
    if value.get("applied") is False and (relationship_ids or entity_ids or relationship_scopes):
        return None, "contradictory_unapplied_relationship_scope_projection"
    return {
        "applied": value["applied"],
        "relationship_ids": relationship_ids,
        "entity_ids": entity_ids,
        "relationship_scopes": relationship_scopes,
        "reason_codes": reason_codes,
    }, None


def _validate_persona_containment_policy(
    persona_containment: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    trace = {
        "mandatory_containment_requested": True,
        "policy_validation_status": "failed",
        "policy_validation_reason": None,
    }
    if not isinstance(persona_containment, dict):
        trace["policy_validation_reason"] = "persona_containment_unavailable"
        return None, trace
    allowed_memory_domains = _policy_label_list(
        persona_containment.get("allowed_memory_domains"),
        limit=16,
        required=True,
    )
    blocked_memory_domains = _policy_label_list(
        persona_containment.get("blocked_memory_domains"),
        limit=16,
    )
    if allowed_memory_domains is None:
        trace["policy_validation_reason"] = "malformed_allowed_memory_domains"
        return None, trace
    if blocked_memory_domains is None:
        trace["policy_validation_reason"] = "malformed_blocked_memory_domains"
        return None, trace
    artifact_policy, artifact_reason = _validate_artifact_access_policy(
        persona_containment.get("artifact_access_policy"),
        allowed_memory_domains=allowed_memory_domains,
    )
    if artifact_policy is None:
        trace["policy_validation_reason"] = artifact_reason
        return None, trace
    trace.update(
        {
            "policy_validation_status": "valid",
            "policy_validation_reason": "mandatory_containment_policy_valid",
            "allowed_memory_domain_count": len(allowed_memory_domains),
            "blocked_memory_domain_count": len(blocked_memory_domains),
            "artifact_content_class_count": len(artifact_policy["allowed_content_classes"]),
            "artifact_domain_count": len(artifact_policy["allowed_domains"]),
        }
    )
    return {
        "enforcement_mode": "mandatory",
        "allowed_memory_domains": allowed_memory_domains,
        "blocked_memory_domains": blocked_memory_domains,
        "artifact_access_policy": artifact_policy,
    }, trace


def _empty_retrieval_bundle(
    *,
    request_id: str,
    conversation_id: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "conversation_id": conversation_id,
        "bundle": {
            "recent": [],
            "semantic": [],
            "artifact_refs": [],
            "observed_metadata": {},
            "retrieval_debug": {
                "degraded": False,
                "fallback": False,
                "suppressed": True,
                "suppression_reason": reason,
            },
        },
    }


def _restraint_suppression_reason(restraint: dict[str, Any] | None) -> str | None:
    if not isinstance(restraint, dict):
        return None
    if restraint.get("retrieval_suppressed") is True:
        return "retrieval_suppressed_true"
    if restraint.get("restraint_policy") == "do_not_retrieve":
        return "restraint_policy_do_not_retrieve"
    return None


def _classify_turn_policy_metadata(
    *,
    persona_containment: dict[str, Any] | None,
    relationship_projection: dict[str, Any] | None,
    request_sensitivity: Any,
    interaction_governance: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(persona_containment, dict):
        return None, "persona_containment_unavailable"
    allowed_domains = _policy_label_list(
        persona_containment.get("allowed_memory_domains"),
        limit=16,
        required=True,
    )
    capability_domain = persona_containment.get("capability_domain")
    if (
        not isinstance(capability_domain, str)
        or not SAFE_POLICY_LABEL.fullmatch(capability_domain.strip())
        or allowed_domains is None
    ):
        return None, "capability_domain_unavailable"
    normalized_domain = capability_domain.strip().lower().replace("-", "_")
    if normalized_domain not in allowed_domains:
        return None, "capability_domain_outside_allowed_domains"
    sensitivity = "medium"
    if request_sensitivity in {"public", "low"}:
        sensitivity = "low"
    elif request_sensitivity in {"high", "local_only", "restricted"}:
        sensitivity = "high"
    hint = (
        interaction_governance.get("privacy_sensitivity_hint")
        if isinstance(interaction_governance, dict)
        else None
    )
    if hint in {"private", "sensitive", "high"}:
        sensitivity = "high"
    metadata: dict[str, Any] = {
        "memory_domains": [normalized_domain],
        "sensitivity": sensitivity,
    }
    if isinstance(relationship_projection, dict) and relationship_projection.get("applied") is True:
        metadata["entity_ids"] = list(relationship_projection.get("entity_ids") or [])
        metadata["relationship_ids"] = list(relationship_projection.get("relationship_ids") or [])
        metadata["relationship_scopes"] = list(
            relationship_projection.get("relationship_scopes") or []
        )
    elif isinstance(relationship_projection, dict):
        metadata["entity_ids"] = []
        metadata["relationship_ids"] = []
        metadata["relationship_scopes"] = []
    return metadata, None


def _apply_persona_containment_retrieval_boundary(
    *,
    retrieval: dict[str, Any] | None,
    persona_containment: dict[str, Any] | None,
    persona_containment_trace: dict[str, Any] | None,
) -> RetrievalBoundaryResult:
    trace = _ensure_persona_containment_trace_defaults(persona_containment_trace)
    requested_scope = retrieval.get("scope") if isinstance(retrieval, dict) else None
    trace["retrieval_scope_requested"] = requested_scope
    allowed_memory_domains = (
        _sanitize_memory_domain_list(persona_containment.get("allowed_memory_domains"))
        if isinstance(persona_containment, dict)
        else None
    )
    blocked_memory_domains = (
        _sanitize_memory_domain_list(persona_containment.get("blocked_memory_domains"))
        if isinstance(persona_containment, dict)
        else None
    )
    has_domain_filters = bool(allowed_memory_domains or blocked_memory_domains)
    trace["domain_retrieval_scope_status"] = (
        "requested_tagged_only" if has_domain_filters else "not_requested"
    )
    trace["domain_retrieval_scope_reason"] = (
        "tagged_domain_filters_forwarded_from_persona_containment"
        if has_domain_filters
        else "domain_retrieval_filters_not_requested"
    )

    if not _persona_containment_lock_active(persona_containment):
        trace["retrieval_scope_used"] = requested_scope
        if (
            isinstance(persona_containment, dict)
            and persona_containment.get("cross_scope_access_allowed") is True
        ):
            trace["retrieval_scope_reason"] = "cross_scope_access_allowed"
            trace["artifact_request_reason"] = "cross_scope_access_allowed"
        return RetrievalBoundaryResult(
            retrieval=retrieval if isinstance(retrieval, dict) else None,
            include_artifacts=None,
            allowed_memory_domains=allowed_memory_domains,
            blocked_memory_domains=blocked_memory_domains,
        )

    effective_retrieval = dict(retrieval) if isinstance(retrieval, dict) else {}
    effective_retrieval["scope"] = "conversation"
    trace["retrieval_scope_used"] = "conversation"
    trace["retrieval_scope_status"] = "request_boundary_enforced"
    trace["retrieval_scope_reason"] = "conversation_scope_enforced_under_containment_lock"
    trace["artifact_request_status"] = "mandatory_policy_forwarded"
    trace["artifact_request_reason"] = "artifact_search_governed_by_mandatory_policy"
    return RetrievalBoundaryResult(
        retrieval=effective_retrieval,
        include_artifacts=None,
        allowed_memory_domains=allowed_memory_domains,
        blocked_memory_domains=blocked_memory_domains,
    )


def _validate_result_policy_metadata(
    value: Any,
    *,
    artifact: bool,
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(value, dict):
        return None, "missing_policy_metadata"
    if set(value) - RESULT_POLICY_FIELDS:
        return None, "unexpected_policy_metadata_fields"
    memory_domains = _policy_label_list(value.get("memory_domains"), limit=16, required=True)
    sensitivity = value.get("sensitivity")
    content_class = value.get("content_class")
    entity_ids = _scope_id_list(value.get("entity_ids", []), limit=64)
    relationship_ids = _scope_id_list(value.get("relationship_ids", []), limit=64)
    relationship_scopes = _policy_label_list(value.get("relationship_scopes", []), limit=16)
    if (
        memory_domains is None
        or sensitivity not in {"low", "medium", "high", "restricted"}
        or entity_ids is None
        or relationship_ids is None
        or relationship_scopes is None
    ):
        return None, "malformed_policy_metadata"
    if content_class is not None:
        content_classes = _artifact_class_list([content_class], limit=1)
        if content_classes is None:
            return None, "malformed_policy_metadata"
        content_class = content_classes[0]
    if artifact and content_class is None:
        return None, "missing_artifact_content_class"
    return {
        "memory_domains": memory_domains,
        "sensitivity": sensitivity,
        "content_class": content_class,
        "entity_ids": entity_ids,
        "relationship_ids": relationship_ids,
        "relationship_scopes": relationship_scopes,
    }, None


def _relationship_projection_allows(
    metadata: dict[str, Any],
    relationship_projection: dict[str, Any] | None,
) -> tuple[bool, str | None]:
    if not isinstance(relationship_projection, dict) or "applied" not in relationship_projection:
        return True, None
    record_relationships = set(metadata.get("relationship_ids") or [])
    record_entities = set(metadata.get("entity_ids") or [])
    if relationship_projection.get("applied") is not True:
        if record_relationships:
            return False, "relationship_projection_mismatch"
        return True, None
    selected_relationships = set(relationship_projection.get("relationship_ids") or [])
    selected_entities = set(relationship_projection.get("entity_ids") or [])
    if record_relationships:
        if not selected_relationships.intersection(record_relationships):
            return False, "relationship_projection_mismatch"
    elif record_entities:
        if not selected_entities.intersection(record_entities):
            return False, "relationship_projection_mismatch"
    else:
        return False, "relationship_projection_mismatch"
    selected_scopes = set(relationship_projection.get("relationship_scopes") or [])
    record_scopes = set(metadata.get("relationship_scopes") or [])
    if selected_scopes and record_scopes and not selected_scopes.intersection(record_scopes):
        return False, "relationship_scope_mismatch"
    return True, None


def _result_policy_allows_message(
    metadata: dict[str, Any],
    *,
    containment_policy: dict[str, Any],
    relationship_projection: dict[str, Any] | None,
) -> tuple[bool, str | None]:
    allowed_domains = set(containment_policy.get("allowed_memory_domains") or [])
    blocked_domains = set(containment_policy.get("blocked_memory_domains") or [])
    record_domains = set(metadata.get("memory_domains") or [])
    if metadata.get("sensitivity") == "restricted":
        return False, "restricted_sensitivity"
    if not record_domains.intersection(allowed_domains):
        return False, "memory_domain_not_allowed"
    if record_domains.intersection(blocked_domains):
        return False, "blocked_memory_domain"
    return _relationship_projection_allows(metadata, relationship_projection)


def _result_policy_allows_artifact(
    metadata: dict[str, Any],
    *,
    containment_policy: dict[str, Any],
    relationship_projection: dict[str, Any] | None,
) -> tuple[bool, str | None]:
    artifact_policy = containment_policy.get("artifact_access_policy")
    if not isinstance(artifact_policy, dict):
        return False, "missing_artifact_policy"
    allowed_memory_domains = set(containment_policy.get("allowed_memory_domains") or [])
    allowed_artifact_domains = set(artifact_policy.get("allowed_domains") or [])
    blocked_domains = set(containment_policy.get("blocked_memory_domains") or [])
    record_domains = set(metadata.get("memory_domains") or [])
    sensitivity = metadata.get("sensitivity")
    content_class = metadata.get("content_class")
    if sensitivity == "restricted":
        return False, "restricted_sensitivity"
    if not record_domains.intersection(allowed_memory_domains):
        return False, "memory_domain_not_allowed"
    if not record_domains.intersection(allowed_artifact_domains):
        return False, "artifact_domain_not_allowed"
    if record_domains.intersection(blocked_domains):
        return False, "blocked_memory_domain"
    if content_class not in set(artifact_policy.get("allowed_content_classes") or []):
        return False, "artifact_content_class_not_allowed"
    if content_class not in set(artifact_policy.get("surface_content_capabilities") or []):
        return False, "artifact_content_class_not_surface_capable"
    ceiling = artifact_policy.get("maximum_sensitivity")
    if SENSITIVITY_RANK.get(sensitivity, 99) > SENSITIVITY_RANK.get(ceiling, -1):
        return False, "artifact_sensitivity_above_ceiling"
    return _relationship_projection_allows(metadata, relationship_projection)


def _count_collection(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0


def _record_omission(trace: dict[str, Any], reason: str | None) -> None:
    reason_key = _sanitize_trace_string(reason, max_length=80) or "unknown"
    omissions = trace.setdefault("omission_counts_by_reason", {})
    omissions[reason_key] = int(omissions.get(reason_key, 0)) + 1


def _valid_result_source_ref(value: Any) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    if set(value) - RESULT_SOURCE_REF_FIELDS:
        return None
    ref_type = value.get("ref_type")
    ref_id = _bounded_contract_text(value.get("ref_id"), max_length=160)
    if ref_type not in {"message", "derived_text"} or not ref_id:
        return None
    return {"ref_type": ref_type, "ref_id": ref_id}


def _bounded_contract_text(value: Any, *, max_length: int) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned or len(cleaned) > max_length:
        return None
    return cleaned


def _valid_result_source_ref_list(value: Any) -> list[dict[str, str]] | None:
    if not isinstance(value, list) or len(value) > 50:
        return None
    refs: list[dict[str, str]] = []
    for item in value:
        source_ref = _valid_result_source_ref(item)
        if source_ref is None:
            return None
        refs.append(source_ref)
    return refs


def _valid_result_source_checks(value: Any) -> list[dict[str, str]] | None:
    if not isinstance(value, list) or not value or len(value) > 50:
        return None
    checks: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            return None
        ref_type = _bounded_contract_text(item.get("ref_type"), max_length=64)
        ref_id = _bounded_contract_text(item.get("ref_id"), max_length=160)
        support_kind = _bounded_contract_text(item.get("support_kind"), max_length=64)
        availability = _bounded_contract_text(item.get("availability"), max_length=64)
        if not ref_type or not ref_id or not support_kind or availability != "available":
            return None
        checks.append(
            {
                "ref_type": ref_type,
                "ref_id": ref_id,
                "support_kind": support_kind,
                "availability": availability,
            }
        )
    return checks


def _bounded_json_scalar(value: Any) -> Any:
    if isinstance(value, str):
        return value if len(value) <= 160 else None
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    return None


def _valid_provenance_metadata(value: Any) -> dict[str, Any] | None:
    if value is None:
        return {}
    if not isinstance(value, dict) or len(value) > 8:
        return None
    out: dict[str, Any] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            return None
        cleaned_key = key.strip()
        if not cleaned_key or len(cleaned_key) > 64:
            return None
        if item is None:
            out[cleaned_key] = None
            continue
        scalar = _bounded_json_scalar(item)
        if scalar is None:
            return None
        out[cleaned_key] = scalar
    return out


def _valid_provenance_source_ref(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    if set(value) - PROVENANCE_SOURCE_REF_FIELDS:
        return None
    if not PROVENANCE_SOURCE_REF_REQUIRED_FIELDS.issubset(value):
        return None
    ref_type = _bounded_contract_text(value.get("ref_type"), max_length=64)
    ref_id = _bounded_contract_text(value.get("ref_id"), max_length=160)
    support_kind = _bounded_contract_text(value.get("support_kind"), max_length=64)
    if not ref_type or not ref_id or not support_kind:
        return None
    out: dict[str, Any] = {
        "ref_type": ref_type,
        "ref_id": ref_id,
        "support_kind": support_kind,
    }
    for key in ("span", "field_path", "note"):
        if key in value:
            bounded = _bounded_contract_text(value.get(key), max_length=160)
            if bounded is None:
                return None
            out[key] = bounded
    if "metadata" in value:
        metadata = _valid_provenance_metadata(value.get("metadata"))
        if metadata is None:
            return None
        if metadata:
            out["metadata"] = metadata
    return out


def _valid_provenance_source_ref_list(value: Any) -> list[dict[str, Any]] | None:
    if not isinstance(value, list) or not value or len(value) > 50:
        return None
    refs: list[dict[str, Any]] = []
    for item in value:
        source_ref = _valid_provenance_source_ref(item)
        if source_ref is None:
            return None
        refs.append(source_ref)
    return refs


def _provenance_status_contradicts_completion(value: Any) -> bool:
    cleaned = _bounded_contract_text(value, max_length=80)
    if cleaned is None:
        return True
    normalized = cleaned.lower()
    return any(marker in normalized for marker in CONTRADICTORY_PROVENANCE_STATUS_MARKERS)


def _valid_artifact_provenance(value: Any, *, owner_id: str) -> tuple[bool, str | None]:
    if value is None:
        return True, None
    if not isinstance(value, dict):
        return False, "malformed_provenance"
    if set(value) - RESULT_PROVENANCE_FIELDS:
        return False, "malformed_provenance"
    if not PROVENANCE_REQUIRED_FIELDS.issubset(value):
        return False, "malformed_provenance"
    if value.get("owner_id") != owner_id:
        return False, "provenance_owner_mismatch"
    if value.get("provenance_status") != "complete":
        return False, "contradictory_provenance"
    if _provenance_status_contradicts_completion(value.get("status")):
        return False, "contradictory_provenance"
    if value.get("effective_status") is not None and _provenance_status_contradicts_completion(
        value.get("effective_status")
    ):
        return False, "contradictory_provenance"
    if _valid_provenance_source_ref_list(value.get("source_refs")) is None:
        return False, "malformed_provenance_source_refs"
    for key in ("derived_id", "derivation_type", "derivation_version", "created_at"):
        if _bounded_contract_text(value.get(key), max_length=160) is None:
            return False, "malformed_provenance"
    confidence = value.get("confidence")
    if confidence is not None and (
        not isinstance(confidence, (int, float))
        or isinstance(confidence, bool)
        or not math.isfinite(float(confidence))
    ):
        return False, "malformed_provenance"
    for key in ("generation_trace_id", "retrieval_reason"):
        if (
            value.get(key) is not None
            and _bounded_contract_text(
                value.get(key),
                max_length=160,
            )
            is None
        ):
            return False, "malformed_provenance"
    if (
        value.get("explanation") is not None
        and _bounded_contract_text(
            value.get("explanation"),
            max_length=500,
        )
        is None
    ):
        return False, "malformed_provenance"
    compatibility_defaults = value.get("compatibility_defaults", [])
    if (
        compatibility_defaults is not None
        and _sanitize_trace_string_list(
            compatibility_defaults,
            limit=20,
            item_max_length=80,
        )
        != compatibility_defaults
    ):
        return False, "malformed_provenance"
    return True, None


def _retained_observed_metadata(
    *,
    recent: list[dict[str, Any]],
    semantic: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
) -> dict[str, Any]:
    estimated_chars = 0
    has_code_like_content = False
    mime_types: list[str] = []
    for item in [*recent, *semantic]:
        content = item.get("content")
        if isinstance(content, str):
            estimated_chars += len(content)
            has_code_like_content = has_code_like_content or bool(CODE_LIKE_RE.search(content))
    for item in artifacts:
        snippet = item.get("snippet")
        if isinstance(snippet, str):
            estimated_chars += len(snippet)
            has_code_like_content = has_code_like_content or bool(CODE_LIKE_RE.search(snippet))
        policy_metadata = item.get("policy_metadata")
        if isinstance(policy_metadata, dict) and policy_metadata.get("content_class") == "code":
            has_code_like_content = True
    return {
        "mime_types": mime_types,
        "has_artifacts": bool(artifacts),
        "has_code_like_content": has_code_like_content,
        "estimated_chars": estimated_chars,
    }


def _bounded_retrieval_debug(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, Any] = {}
    for key in ("degraded", "fallback", "suppressed"):
        if isinstance(value.get(key), bool):
            out[key] = value[key]
    for key in ("fallback_reason", "suppression_reason", "vector_status"):
        cleaned = _sanitize_trace_string(value.get(key), max_length=120)
        if cleaned in RETRIEVAL_DEBUG_STATUSES or cleaned in RETRIEVAL_DEBUG_REASONS:
            out[key] = cleaned
    reason_codes = [
        code
        for code in _sanitize_trace_string_list(value.get("reason_codes"), limit=20)
        if code in RETRIEVAL_DEBUG_REASONS
    ]
    if reason_codes:
        out["reason_codes"] = reason_codes
    return out


def _bounded_bms_diagnostics(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    summary = _trace_doctrine_summary({"diagnostics": value})
    return summary if isinstance(summary, dict) else {}


def _apply_persona_containment_result_boundary(
    *,
    retrieval_bundle: Any,
    request_id: str,
    conversation_id: str,
    owner_id: str,
    retrieval: dict[str, Any] | None,
    containment_policy: dict[str, Any] | None,
    relationship_projection: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    trace: dict[str, Any] = {
        "enforcement_mode": (
            "mandatory" if isinstance(containment_policy, dict) else "not_applicable"
        ),
        "validation_status": "not_applied",
        "envelope_validation_failed": False,
        "input_counts": {"recent": 0, "semantic": 0, "artifact_refs": 0},
        "retained_counts": {"recent": 0, "semantic": 0, "artifact_refs": 0},
        "omission_counts_by_reason": {},
        "relationship_policy_applied": bool(
            isinstance(relationship_projection, dict)
            and relationship_projection.get("applied") is True
        ),
        "artifact_policy_applied": isinstance(containment_policy, dict)
        and isinstance(containment_policy.get("artifact_access_policy"), dict),
        "post_budget_survivor_filter_removed_sources": False,
    }
    if not isinstance(containment_policy, dict):
        return retrieval_bundle if isinstance(retrieval_bundle, dict) else {}, trace

    empty = _empty_retrieval_bundle(
        request_id=request_id,
        conversation_id=conversation_id,
        reason="mandatory_result_boundary_failed_closed",
    )
    if not isinstance(retrieval_bundle, dict):
        trace["validation_status"] = "failed_closed"
        trace["envelope_validation_failed"] = True
        _record_omission(trace, "malformed_retrieval_envelope")
        return empty, trace
    bundle = retrieval_bundle.get("bundle")
    if (
        retrieval_bundle.get("request_id") != request_id
        or retrieval_bundle.get("conversation_id") != conversation_id
        or not isinstance(bundle, dict)
    ):
        trace["validation_status"] = "failed_closed"
        trace["envelope_validation_failed"] = True
        _record_omission(trace, "retrieval_envelope_mismatch")
        return empty, trace

    recent_raw = bundle.get("recent")
    semantic_raw = bundle.get("semantic")
    artifacts_raw = bundle.get("artifact_refs")
    trace["input_counts"] = {
        "recent": _count_collection(recent_raw),
        "semantic": _count_collection(semantic_raw),
        "artifact_refs": _count_collection(artifacts_raw),
    }
    if not isinstance(recent_raw, list):
        recent_raw = []
        _record_omission(trace, "malformed_recent_collection")
    if not isinstance(semantic_raw, list):
        semantic_raw = []
        _record_omission(trace, "malformed_semantic_collection")
    if not isinstance(artifacts_raw, list):
        artifacts_raw = []
        _record_omission(trace, "malformed_artifact_collection")

    scope = retrieval.get("scope") if isinstance(retrieval, dict) else None
    min_score = retrieval.get("min_score") if isinstance(retrieval, dict) else None
    if (
        not isinstance(min_score, (int, float))
        or isinstance(min_score, bool)
        or not math.isfinite(float(min_score))
    ):
        min_score = None

    def keep_message(item: Any) -> tuple[dict[str, Any] | None, str | None]:
        if not isinstance(item, dict):
            return None, "malformed_message_record"
        message_id = _sanitize_trace_string(item.get("message_id"), max_length=160)
        role = item.get("role")
        content = item.get("content")
        source_ref = _valid_result_source_ref(item.get("source_ref"))
        if not message_id:
            return None, "missing_message_id"
        if item.get("owner_id") != owner_id:
            return None, "owner_mismatch"
        if scope == "conversation" and item.get("conversation_id") != conversation_id:
            return None, "conversation_mismatch"
        if role not in VALID_RESULT_ROLES:
            return None, "invalid_message_role"
        if not isinstance(content, str) or len(content) > 20000:
            return None, "malformed_message_content"
        if source_ref is None:
            return None, "malformed_source_ref"
        metadata, reason = _validate_result_policy_metadata(
            item.get("policy_metadata"),
            artifact=False,
        )
        if metadata is None:
            return None, reason
        allowed, reason = _result_policy_allows_message(
            metadata,
            containment_policy=containment_policy,
            relationship_projection=relationship_projection,
        )
        if not allowed:
            return None, reason
        kept: dict[str, Any] = {
            "message_id": message_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "role": role,
            "content": content,
            "source_ref": source_ref,
            "policy_metadata": metadata,
        }
        for key in (
            "evidence_role",
            "created_at",
            "source_availability",
            "freshness_state",
            "durable_status",
            "last_verified_at",
            "source_kind",
            "supersedes",
            "superseded_by",
        ):
            value = item.get(key)
            if isinstance(value, str):
                kept[key] = value
        score = item.get("score")
        if (
            isinstance(score, (int, float))
            and not isinstance(score, bool)
            and math.isfinite(float(score))
        ):
            kept["score"] = score
        memory_id = _bounded_contract_text(item.get("memory_id"), max_length=160)
        if memory_id:
            kept["memory_id"] = memory_id
        return kept, None

    def keep_artifact(item: Any) -> tuple[dict[str, Any] | None, str | None]:
        if not isinstance(item, dict):
            return None, "malformed_artifact_record"
        artifact_id = _sanitize_trace_string(item.get("artifact_id"), max_length=160)
        source_ref = _valid_result_source_ref(item.get("source_ref"))
        if not artifact_id:
            return None, "missing_artifact_id"
        if item.get("owner_id") != owner_id:
            return None, "owner_mismatch"
        if not isinstance(item.get("file_path"), str) or not item.get("file_path"):
            return None, "malformed_artifact_path"
        if not isinstance(item.get("snippet"), str):
            return None, "malformed_artifact_snippet"
        if source_ref is None:
            return None, "malformed_source_ref"
        if item.get("source_availability") != "available":
            return None, "source_unavailable"
        score = item.get("relevance_score")
        if min_score is not None and score is None:
            return None, "missing_relevance_score"
        if score is not None and (
            not isinstance(score, (int, float))
            or isinstance(score, bool)
            or not math.isfinite(float(score))
        ):
            return None, "malformed_relevance_score"
        if min_score is not None and score is not None and score < min_score:
            return None, "relevance_score_below_minimum"
        provenance_ok, reason = _valid_artifact_provenance(
            item.get("provenance"),
            owner_id=owner_id,
        )
        if not provenance_ok:
            return None, reason
        metadata, reason = _validate_result_policy_metadata(
            item.get("policy_metadata"),
            artifact=True,
        )
        if metadata is None:
            return None, reason
        allowed, reason = _result_policy_allows_artifact(
            metadata,
            containment_policy=containment_policy,
            relationship_projection=relationship_projection,
        )
        if not allowed:
            return None, reason
        kept = {
            "artifact_id": artifact_id,
            "owner_id": owner_id,
            "file_path": item["file_path"],
            "snippet": item["snippet"],
            "source_ref": source_ref,
            "source_availability": "available",
            "policy_metadata": metadata,
        }
        for key in (
            "evidence_role",
            "repo_name",
            "freshness_state",
            "durable_status",
            "last_verified_at",
            "source_kind",
            "supersedes",
            "superseded_by",
        ):
            value = item.get(key)
            if isinstance(value, str):
                kept[key] = value
        if score is not None:
            kept["relevance_score"] = score
        confidence = item.get("confidence")
        if (
            isinstance(confidence, (int, float))
            and not isinstance(confidence, bool)
            and math.isfinite(float(confidence))
        ):
            kept["confidence"] = confidence
        memory_id = _bounded_contract_text(item.get("memory_id"), max_length=160)
        if memory_id:
            kept["memory_id"] = memory_id
        provenance = item.get("provenance")
        if provenance is not None:
            kept["provenance"] = provenance
        source_checks = _valid_result_source_checks(item.get("source_checks"))
        if source_checks is not None:
            kept["source_checks"] = source_checks
        return kept, None

    retained_recent: list[dict[str, Any]] = []
    retained_semantic: list[dict[str, Any]] = []
    retained_artifacts: list[dict[str, Any]] = []
    for collection, output, checker in (
        (recent_raw, retained_recent, keep_message),
        (semantic_raw, retained_semantic, keep_message),
        (artifacts_raw, retained_artifacts, keep_artifact),
    ):
        for item in collection:
            kept, reason = checker(item)
            if kept is None:
                _record_omission(trace, reason)
            else:
                output.append(kept)

    observed_metadata = _retained_observed_metadata(
        recent=retained_recent,
        semantic=retained_semantic,
        artifacts=retained_artifacts,
    )
    token_estimate_total = (
        max(1, observed_metadata["estimated_chars"] // 4)
        if (observed_metadata["estimated_chars"] > 0)
        else 0
    )
    filtered_bundle = {
        "request_id": request_id,
        "conversation_id": conversation_id,
        "bundle": {
            "recent": retained_recent,
            "semantic": retained_semantic,
            "artifact_refs": retained_artifacts,
            "observed_metadata": observed_metadata,
            "retrieval_debug": _bounded_retrieval_debug(bundle.get("retrieval_debug")),
            "token_estimate_total": token_estimate_total,
        },
    }
    diagnostics = _bounded_bms_diagnostics(retrieval_bundle.get("diagnostics"))
    if diagnostics:
        filtered_bundle["diagnostics"] = diagnostics
    trace["retained_counts"] = {
        "recent": len(retained_recent),
        "semantic": len(retained_semantic),
        "artifact_refs": len(retained_artifacts),
    }
    trace["validation_status"] = "filtered"
    return filtered_bundle, trace


def _restraint_disabled_trace() -> dict[str, Any]:
    return {
        "attempted": False,
        "status": "disabled",
        "included": False,
    }


def _sanitize_trace_string(value: Any, *, max_length: int = 120) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return cleaned[:max_length]


def _sanitize_trace_string_list(
    value: Any,
    *,
    limit: int = 20,
    item_max_length: int = 120,
) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for item in value:
        cleaned = _sanitize_trace_string(item, max_length=item_max_length)
        if cleaned:
            items.append(cleaned)
        if len(items) >= limit:
            break
    return items


def _sanitize_trace_int(
    value: Any,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    if minimum is not None and value < minimum:
        return None
    if maximum is not None and value > maximum:
        return maximum
    return value


def _sanitize_trace_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _capability_discovery_requested(text: str) -> bool:
    normalized = " ".join(text.casefold().split())
    return any(
        phrase in normalized
        for phrase in (
            "what can you control",
            "what actions can you take",
            "what capabilities do you have",
            "what can you do",
        )
    )


def _capability_registry_base_trace(*, enabled: bool, reason: str | None = None) -> dict[str, Any]:
    status = "not_requested" if enabled else "disabled"
    trace = {
        "enabled": enabled,
        "status": status,
        "context_included": False,
        "action_taken": False,
        "match": {"attempted": False, "status": status},
        "discovery": {"attempted": False, "status": status},
        "authority": {"attempted": False, "status": status},
    }
    if reason is not None:
        trace["reason"] = reason
    return trace


def _safe_capability_example(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    capability_id = _sanitize_trace_string(value.get("capability_id"), max_length=120)
    display_name = _sanitize_trace_string(value.get("display_name"), max_length=120)
    operation_kind = _sanitize_trace_string(value.get("operation_kind"), max_length=80)
    risk_level = _sanitize_trace_string(value.get("risk_level"), max_length=80)
    reason_codes = _sanitize_trace_string_list(value.get("reason_codes"), limit=8)
    if capability_id is None and display_name is None:
        return None
    return {
        "capability_id": capability_id,
        "display_name": display_name,
        "operation_kind": operation_kind,
        "risk_level": risk_level,
        "reason_codes": reason_codes or [],
    }


def _safe_capability_record(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    capability_id = _sanitize_trace_string(value.get("capability_id"), max_length=120)
    if capability_id is None:
        return None
    return {
        "capability_id": capability_id,
        "display_name": _sanitize_trace_string(value.get("display_name"), max_length=120),
        "domain": _sanitize_trace_string(value.get("domain"), max_length=80),
        "operation_kind": _sanitize_trace_string(value.get("operation_kind"), max_length=80),
        "risk_level": _sanitize_trace_string(value.get("risk_level"), max_length=80),
        "requires_confirmation": _sanitize_trace_bool(value.get("requires_confirmation")),
        "reversible": _sanitize_trace_bool(value.get("reversible")),
        "dry_run_supported": _sanitize_trace_bool(value.get("dry_run_supported")),
        "verification_supported": _sanitize_trace_bool(value.get("verification_supported")),
    }


VALID_ACTION_RISK_LEVELS = {
    "read_only",
    "low_reversible",
    "medium_requires_confirmation",
    "high_requires_confirmation",
    "blocked",
}
VALID_ACTION_AUTHORITY_LEVELS = {
    "answer_only",
    "suggest_only",
    "prepare_only",
    "execute_low_risk",
    "execute_after_confirmation",
    "blocked",
}


def _safe_action_authority_decision(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict) or value.get("action_taken") is not False:
        return None
    capability_id = _sanitize_trace_string(value.get("capability_id"), max_length=120)
    risk_level = _sanitize_trace_string(value.get("risk_level"), max_length=80)
    authority_level = _sanitize_trace_string(value.get("authority_level"), max_length=80)
    requires_confirmation = _sanitize_trace_bool(value.get("requires_confirmation"))
    allowed = _sanitize_trace_bool(value.get("allowed"))
    if (
        capability_id is None
        or risk_level not in VALID_ACTION_RISK_LEVELS
        or authority_level not in VALID_ACTION_AUTHORITY_LEVELS
        or requires_confirmation is None
        or allowed is None
    ):
        return None
    return {
        "capability_id": capability_id,
        "risk_level": risk_level,
        "authority_level": authority_level,
        "requires_confirmation": requires_confirmation,
        "allowed": allowed,
        "reason_summary": _sanitize_trace_string_list(value.get("reason_summary"), limit=16)
        or [],
        "action_taken": False,
    }


def _capability_registry_prompt_messages(trace: dict[str, Any]) -> list[dict[str, str]]:
    if trace.get("context_included") is not True:
        return []
    discovery = trace.get("discovery") if isinstance(trace.get("discovery"), dict) else {}
    if discovery.get("status") == "included":
        allowed = discovery.get("allowed_examples") if isinstance(discovery, dict) else []
        blocked = discovery.get("blocked_examples") if isinstance(discovery, dict) else []
        lines = [
            "Capability registry context:",
            "- Summarize only the listed allowed and unavailable examples.",
            "- Do not mention internal endpoints or tracing details.",
            "- Do not say that an action was completed.",
        ]
        allowed_names = [
            item.get("display_name") or item.get("capability_id")
            for item in allowed[:6]
            if isinstance(item, dict) and (item.get("display_name") or item.get("capability_id"))
        ]
        blocked_names = [
            item.get("display_name") or item.get("capability_id")
            for item in blocked[:6]
            if isinstance(item, dict) and (item.get("display_name") or item.get("capability_id"))
        ]
        if allowed_names:
            lines.append(f"- Allowed examples: {', '.join(allowed_names)}.")
        if blocked_names:
            lines.append(f"- Unavailable examples: {', '.join(blocked_names)}.")
        return [{"role": "system", "content": "\n".join(lines)}]

    match = trace.get("match") if isinstance(trace.get("match"), dict) else {}
    capability = match.get("capability") if isinstance(match.get("capability"), dict) else None
    if match.get("matched") is True and capability:
        authority = trace.get("authority") if isinstance(trace.get("authority"), dict) else {}
        label = capability.get("display_name") or capability.get("capability_id")
        operation_kind = capability.get("operation_kind")
        risk_level = capability.get("risk_level")
        requires_confirmation = capability.get("requires_confirmation")
        lines = [
            "Capability registry context:",
            f"- The runtime matched a registered capability: {label}.",
            "- Treat this as registry context only; do not execute the action.",
            "- Do not say that the action was completed.",
        ]
        if authority.get("status") == "included":
            lines.append(f"- Authority level: {authority.get('authority_level')}.")
            lines.append(f"- Authority risk level: {authority.get('risk_level')}.")
            lines.append(
                "- Authority requires confirmation: "
                f"{str(authority.get('requires_confirmation')).lower()}."
            )
            lines.append(f"- Authority allowed: {str(authority.get('allowed')).lower()}.")
        if operation_kind:
            lines.append(f"- Operation kind: {operation_kind}.")
        if risk_level:
            lines.append(f"- Risk level: {risk_level}.")
        if requires_confirmation is not None:
            lines.append(f"- Requires confirmation: {str(requires_confirmation).lower()}.")
        return [{"role": "system", "content": "\n".join(lines)}]
    return []


def _capability_completion_claim(text: str) -> bool:
    normalized = " ".join(text.casefold().split())
    return any(
        claim in normalized
        for claim in (
            "done",
            "completed",
            "i turned",
            "i restarted",
            "i sent",
            "i changed",
            "it is on",
        )
    )


def _apply_capability_registry_response_boundary(text: str, trace: dict[str, Any]) -> str:
    override = _capability_registry_forced_response(trace)
    if override is not None:
        return override
    match = trace.get("match") if isinstance(trace.get("match"), dict) else {}
    if (
        match.get("matched") is True
        and trace.get("action_taken") is False
        and _capability_completion_claim(text)
    ):
        capability = match.get("capability") if isinstance(match.get("capability"), dict) else {}
        label = capability.get("display_name") or capability.get("capability_id")
        if isinstance(label, str) and label:
            return f"I found the registered capability for {label}, but I did not execute it."
        return "I found a matching registered capability, but I did not execute it."
    return text


def _capability_registry_forced_response(trace: dict[str, Any]) -> str | None:
    match = trace.get("match") if isinstance(trace.get("match"), dict) else {}
    if match.get("matched") is not True or trace.get("action_taken") is not False:
        return None
    authority = trace.get("authority") if isinstance(trace.get("authority"), dict) else {}
    if authority.get("status") == "failed":
        return "I found a matching registered capability, but I did not execute it."
    authority_level = authority.get("authority_level")
    requires_confirmation = authority.get("requires_confirmation")
    if authority_level == "blocked":
        return "That registered capability is not available for execution here. I did not execute it."
    if authority_level == "execute_after_confirmation" or requires_confirmation is True:
        return "That action requires explicit confirmation before I proceed. I did not execute it."
    if authority_level == "execute_low_risk":
        return "I found that this is allowed as a low-risk capability, but I did not execute it."
    return None


def _authority_context_inputs(
    interaction_governance_trace: dict[str, Any] | None,
) -> dict[str, Any]:
    trace = interaction_governance_trace if isinstance(interaction_governance_trace, dict) else {}
    return {
        "target_resolution_state": "resolved",
        "world_state_freshness": "unknown",
        "consequence_flags": {},
        "interaction_governance_kind": _sanitize_trace_string(
            trace.get("interaction_kind"),
            max_length=80,
        ),
        "interaction_governance_tension": _sanitize_trace_string(
            trace.get("tension_level"),
            max_length=80,
        ),
        "user_authorization_signal": "explicit",
    }


async def _resolve_capability_registry_context(
    *,
    runtime: Any | None,
    enabled: bool,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    active_persona_id: str | None,
    current_user_text: str,
    interaction_governance_trace: dict[str, Any] | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not enabled:
        return [], _capability_registry_disabled_trace()
    if runtime is None:
        return [], _capability_registry_base_trace(
            enabled=True,
            reason="runtime_client_not_configured",
        )
    if not active_persona_id:
        return [], _capability_registry_base_trace(
            enabled=True,
            reason="active_persona_unavailable",
        )

    discovery_requested = _capability_discovery_requested(current_user_text)
    trace = _capability_registry_base_trace(enabled=True)
    try:
        if discovery_requested:
            response = await runtime.discover_capabilities(
                request_id=f"{request_id}:capability-discovery",
                owner_id=owner_id,
                conversation_id=conversation_id,
                surface=surface,
                active_persona_id=active_persona_id,
            )
            result = response.get("result") if isinstance(response, dict) else None
            if not isinstance(result, dict) or result.get("action_taken") is not False:
                trace["status"] = "failed"
                trace["reason"] = "malformed_capability_discovery_response"
                trace["discovery"] = {
                    "attempted": True,
                    "status": "failed",
                    "reason": "malformed_capability_discovery_response",
                }
                return [], trace
            allowed = [
                safe
                for item in (result.get("allowed_examples") or [])
                if (safe := _safe_capability_example(item)) is not None
            ][:16]
            blocked = [
                safe
                for item in (result.get("blocked_examples") or [])
                if (safe := _safe_capability_example(item)) is not None
            ][:16]
            trace.update({"status": "included", "context_included": True})
            trace["discovery"] = {
                "attempted": True,
                "status": "included",
                "registry_available": _sanitize_trace_bool(result.get("registry_available")),
                "reason_codes": _sanitize_trace_string_list(result.get("reason_codes"), limit=8)
                or [],
                "allowed_examples": allowed,
                "blocked_examples": blocked,
                "allowed_count": len(allowed),
                "blocked_count": len(blocked),
            }
        else:
            response = await runtime.match_capability(
                request_id=f"{request_id}:capability-match",
                owner_id=owner_id,
                conversation_id=conversation_id,
                surface=surface,
                active_persona_id=active_persona_id,
                current_user_text=current_user_text,
            )
            result = response.get("result") if isinstance(response, dict) else None
            if not isinstance(result, dict) or result.get("action_taken") is not False:
                trace["status"] = "failed"
                trace["reason"] = "malformed_capability_match_response"
                trace["match"] = {
                    "attempted": True,
                    "status": "failed",
                    "reason": "malformed_capability_match_response",
                }
                return [], trace
            capability = _safe_capability_record(result.get("capability"))
            matched = _sanitize_trace_bool(result.get("capability_matched"))
            reason_codes = _sanitize_trace_string_list(result.get("reason_codes"), limit=8) or []
            trace.update({"status": "included", "context_included": bool(capability)})
            trace["match"] = {
                "attempted": True,
                "status": "included",
                "matched": matched,
                "matched_capability_id": capability.get("capability_id") if capability else None,
                "capability": capability,
                "reason_codes": reason_codes,
            }
            if matched is True and capability:
                authority_inputs = _authority_context_inputs(interaction_governance_trace)
                authority_response = await runtime.action_authority(
                    request_id=f"{request_id}:capability-authority",
                    owner_id=owner_id,
                    conversation_id=conversation_id,
                    surface=surface,
                    runtime_session_id=runtime_session_id,
                    runtime_turn_id=runtime_turn_id,
                    active_persona_id=active_persona_id,
                    capability_id=capability["capability_id"],
                    **authority_inputs,
                )
                authority_result = (
                    authority_response.get("result")
                    if isinstance(authority_response, dict)
                    else None
                )
                authority = _safe_action_authority_decision(authority_result)
                if authority is None:
                    trace["status"] = "failed"
                    trace["reason"] = "malformed_capability_authority_response"
                    trace["authority"] = {
                        "attempted": True,
                        "status": "failed",
                        "reason": "malformed_capability_authority_response",
                        "action_taken": False,
                    }
                    return _capability_registry_prompt_messages(trace), trace
                trace["authority"] = {
                    "attempted": True,
                    "status": "included",
                    **authority,
                }
    except Exception:
        trace["status"] = "failed"
        key = "discovery" if discovery_requested else "match"
        if (
            not discovery_requested
            and isinstance(trace.get("match"), dict)
            and trace["match"].get("matched") is True
        ):
            trace["reason"] = "capability_authority_unavailable"
            key = "authority"
        else:
            trace["reason"] = "capability_registry_unavailable"
        failure_trace = {
            "attempted": True,
            "status": "failed",
            "reason": trace["reason"],
        }
        if key == "authority":
            failure_trace["action_taken"] = False
        trace[key] = failure_trace
        return [], trace

    return _capability_registry_prompt_messages(trace), trace


def _safe_error_summary(error: BaseException) -> dict[str, str]:
    error_type = type(error).__name__[:80]
    status_code = None
    if isinstance(error, httpx.HTTPStatusError) and error.response is not None:
        status_code = error.response.status_code
    return {
        "error_type": error_type,
        "error_code": f"http_{status_code}" if status_code is not None else error_type,
    }


def _model_attempt(
    *,
    provider: str | None,
    model: str | None,
    status: str,
    latency_ms: int,
    error: BaseException | None = None,
) -> dict[str, Any]:
    attempt: dict[str, Any] = {
        "provider": _sanitize_trace_string(provider, max_length=80),
        "model": _sanitize_trace_string(model, max_length=160),
        "status": status,
        "latency_ms": max(0, latency_ms),
    }
    if error is not None:
        attempt.update(_safe_error_summary(error))
    return attempt


def _filtered_retrieval_ids(retrieval_bundle: dict[str, Any]) -> dict[str, list[str]]:
    bundle = retrieval_bundle.get("bundle") if isinstance(retrieval_bundle, dict) else {}
    bundle = bundle if isinstance(bundle, dict) else {}
    semantic = bundle.get("semantic")
    artifacts = bundle.get("artifact_refs")
    return {
        "semantic_message_ids": [
            item["message_id"]
            for item in semantic
            if isinstance(item, dict) and isinstance(item.get("message_id"), str)
        ][:50]
        if isinstance(semantic, list)
        else [],
        "artifact_ids": [
            item["artifact_id"]
            for item in artifacts
            if isinstance(item, dict) and isinstance(item.get("artifact_id"), str)
        ][:50]
        if isinstance(artifacts, list)
        else [],
    }


def _apply_post_budget_survivor_trace(
    *,
    result_boundary_trace: dict[str, Any],
    retrieval_bundle: dict[str, Any],
    prompt_trace: dict[str, Any],
) -> None:
    filtered_ids = _filtered_retrieval_ids(retrieval_bundle)
    retained = prompt_trace.get("retained_source_ids")
    if not isinstance(retained, dict):
        return
    retained_semantic = {
        item for item in retained.get("semantic_message_ids") or [] if isinstance(item, str)
    }
    retained_artifacts = {
        item for item in retained.get("artifact_ids") or [] if isinstance(item, str)
    }
    filtered_semantic = set(filtered_ids["semantic_message_ids"])
    filtered_artifacts = set(filtered_ids["artifact_ids"])
    result_boundary_trace["post_budget_survivor_filter_removed_sources"] = bool(
        filtered_semantic - retained_semantic or filtered_artifacts - retained_artifacts
    )
    result_boundary_trace["post_budget_retained_counts"] = {
        "semantic": len(retained_semantic.intersection(filtered_semantic)),
        "artifact_refs": len(retained_artifacts.intersection(filtered_artifacts)),
    }


def _provider_attempt_evidence(
    *,
    ordinal: int,
    prompt_fingerprint: dict[str, Any],
    prompt_trace: dict[str, Any],
) -> dict[str, Any]:
    retained = prompt_trace.get("retained_source_ids")
    retained = retained if isinstance(retained, dict) else {}
    semantic_ids = [
        item for item in retained.get("semantic_message_ids") or [] if isinstance(item, str)
    ][:20]
    artifact_ids = [item for item in retained.get("artifact_ids") or [] if isinstance(item, str)][
        :20
    ]
    return {
        "attempt_ordinal": max(1, ordinal),
        "prompt_fingerprint": prompt_fingerprint.get("fingerprint"),
        "prompt_message_count": prompt_fingerprint.get("message_count"),
        "prompt_role_sequence": prompt_fingerprint.get("role_sequence"),
        "retained_semantic_message_count": len(semantic_ids),
        "retained_artifact_count": len(artifact_ids),
        "retained_semantic_message_ids": semantic_ids,
        "retained_artifact_ids": artifact_ids,
    }


def _capability_follow_up_empty_trace(status: str = "not_attempted") -> dict[str, Any]:
    return {
        "status": status,
        "call_count": 0,
        "used_final_text": False,
        "reason_code": status,
    }


def _capability_fallback_trace(
    *,
    descriptor_fingerprint_value: Any,
) -> dict[str, Any]:
    return {
        "primary_descriptor_fingerprint": _sanitize_trace_string(
            descriptor_fingerprint_value,
            max_length=120,
        ),
        "descriptor_fingerprint": None,
        "same_descriptor_fingerprint": None,
        "blocked_after_dispatch": False,
    }


def _capability_follow_up_messages(summary: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "Produce the final user-facing answer from this bounded capability result. "
                "Do not request or describe tool calls. Do not include raw private values. "
                "For local drafts, clearly state that the draft is local and unsent."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {"capability_result": summary},
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    ]


async def _attempt_capability_follow_up(
    *,
    litellm: Any,
    request_id: str,
    model: str,
    execution_result: Any,
) -> tuple[str, dict[str, Any]]:
    summary = capability_follow_up_summary(execution_result=execution_result)
    trace = {
        **_capability_follow_up_empty_trace("attempted"),
        "call_count": 1,
        "summary": summary,
    }
    try:
        completion = await litellm.chat(
            request_id=f"{request_id}:capability-follow-up",
            model=model,
            messages=_capability_follow_up_messages(summary),
        )
    except Exception:
        trace.update({"status": "failed", "reason_code": "provider_follow_up_failed"})
        return execution_result.response_text, trace
    try:
        recursive_request = parse_provider_capability_request(completion)
    except CapabilityValidationError as exc:
        reason = (
            "multiple_tool_calls_blocked"
            if exc.reason_code == "multiple_capability_calls"
            else "recursive_tool_call_blocked"
        )
        trace.update(
            {
                "status": "recursive_tool_call_blocked",
                "reason_code": reason,
            }
        )
        return execution_result.response_text, trace
    if recursive_request is not None:
        trace.update(
            {
                "status": "recursive_tool_call_blocked",
                "reason_code": "recursive_tool_call_blocked",
            }
        )
        return execution_result.response_text, trace
    text = provider_text(completion).strip()
    if not text:
        trace.update({"status": "malformed", "reason_code": "empty_follow_up_text"})
        return execution_result.response_text, trace
    final_text = ensure_draft_local_unsent_truth(text, capability_summary=summary)
    trace.update(
        {
            "status": "completed",
            "used_final_text": True,
            "reason_code": "completed",
        }
    )
    return final_text, trace


def _sanitize_model_call_for_privacy(model_call: dict[str, Any]) -> dict[str, Any]:
    sanitized = dict(model_call)
    sanitized.pop("retained_semantic_message_ids", None)
    sanitized.pop("retained_artifact_ids", None)
    return sanitized


def _bounded_source_ref(value: Any) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    ref_type = _sanitize_trace_string(value.get("ref_type"), max_length=80)
    ref_id = _sanitize_trace_string(value.get("ref_id"), max_length=160)
    if not ref_type or not ref_id:
        return None
    return {"ref_type": ref_type, "ref_id": ref_id}


def _trace_references(retrieval_bundle: dict[str, Any]) -> list[dict[str, str]]:
    bundle = retrieval_bundle.get("bundle")
    if not isinstance(bundle, dict):
        return []
    references: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for collection_name in ("recent", "semantic", "artifact_refs"):
        collection = bundle.get(collection_name)
        if not isinstance(collection, list):
            continue
        for item in collection:
            if not isinstance(item, dict):
                continue
            source_ref = _bounded_source_ref(item.get("source_ref"))
            if source_ref is None:
                continue
            key = (source_ref["ref_type"], source_ref["ref_id"])
            if key in seen:
                continue
            references.append(source_ref)
            seen.add(key)
            if len(references) >= 20:
                return references
    return references


def _recall_context(
    *,
    surface: str,
    sensitivity: Any,
    requested_scene: Any,
    interaction_governance: dict[str, Any] | None,
    companion_trace: dict[str, Any] | None,
) -> dict[str, Any]:
    scene_id = None
    if isinstance(companion_trace, dict):
        scene_id = companion_trace.get("scene_id")
    if not isinstance(scene_id, str) or not scene_id:
        scene_id = requested_scene if isinstance(requested_scene, str) else None
    urgency = "medium"
    if isinstance(interaction_governance, dict):
        posture = interaction_governance.get("response_posture")
        if posture == "tactical":
            urgency = "high"
        elif posture in {"reflective", "supportive"}:
            urgency = "low"
    normalized_sensitivity = sensitivity if isinstance(sensitivity, str) else "private"
    if normalized_sensitivity in {"public", "low"}:
        recall_sensitivity = "low"
    elif normalized_sensitivity in {"high", "local_only", "restricted"}:
        recall_sensitivity = "high"
    else:
        recall_sensitivity = "medium"
    return {
        "scene_id": scene_id,
        "surface": surface,
        "urgency": urgency,
        "sensitivity": recall_sensitivity,
    }


def _privacy_safe_memory_recall_trace(trace: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(trace, dict):
        return {"status": "privacy_suppressed"}
    recall = trace.get("recall") if isinstance(trace.get("recall"), dict) else {}
    episodes = trace.get("episodes") if isinstance(trace.get("episodes"), dict) else {}
    dependency = trace.get("dependency") if isinstance(trace.get("dependency"), dict) else {}
    return {
        "status": trace.get("status", "composed"),
        "privacy_suppressed": True,
        "provider_context_included": False,
        "recall": {
            "candidate_count": recall.get("candidate_count", 0),
            "decision_count": recall.get("decision_count", 0),
            "suppressed_count": len(recall.get("suppressed_ids") or []),
            "strategy_counts": recall.get("strategy_counts", {}),
        },
        "episodes": {
            "decision_count": episodes.get("decision_count", 0),
            "prompt_eligible_count": episodes.get("prompt_eligible_count", 0),
        },
        "dependency": {
            "recall_status": dependency.get("recall_status"),
            "episode_status": dependency.get("episode_status"),
        },
        "omission_count": trace.get("omission_count", 0),
        "final_callback_applied": trace.get("final_callback_applied"),
    }


def _privacy_safe_brief_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    safe = {
        key: value
        for key, value in metadata.items()
        if key not in {"raw_model_answer", "shaped_answer"}
    }
    grounding = safe.get("grounding")
    if isinstance(grounding, dict):
        safe["grounding"] = {
            "source_count": grounding.get("source_count", 0),
            "uncertainty_count": grounding.get("uncertainty_count", 0),
            "omission_count": grounding.get("omission_count", 0),
            "conflict_count": grounding.get("conflict_count", 0),
            "privacy_suppressed": True,
        }
    return safe


def _merge_brief_grounding(*groundings: dict[str, Any] | None) -> dict[str, Any]:
    sources: list[dict[str, Any]] = []
    uncertainty: list[str] = []
    omissions: list[dict[str, str]] = []
    conflicts: list[str] = []
    for grounding in groundings:
        if not isinstance(grounding, dict):
            continue
        source_items = grounding.get("sources")
        if isinstance(source_items, list):
            sources.extend(item for item in source_items if isinstance(item, dict))
        uncertainty_items = grounding.get("uncertainty")
        if isinstance(uncertainty_items, list):
            uncertainty.extend(item for item in uncertainty_items if isinstance(item, str))
        omission_items = grounding.get("omissions")
        if isinstance(omission_items, list):
            omissions.extend(item for item in omission_items if isinstance(item, dict))
        conflict_items = grounding.get("conflicts")
        if isinstance(conflict_items, list):
            conflicts.extend(item for item in conflict_items if isinstance(item, str))
    return {
        "source_count": len(sources),
        "sources": sources[:20],
        "uncertainty": uncertainty[:12],
        "omissions": omissions[:20],
        "conflicts": conflicts[:12],
    }


def _retained_external_source_refs(prompt_trace: dict[str, Any] | None) -> set[str] | None:
    if not isinstance(prompt_trace, dict):
        return None
    layers = prompt_trace.get("layers")
    if not isinstance(layers, list):
        return None
    for layer in layers:
        if not isinstance(layer, dict) or layer.get("name") != "external_source_context":
            continue
        if layer.get("included") is not True:
            return set()
        metadata = layer.get("metadata") if isinstance(layer.get("metadata"), dict) else {}
        refs = metadata.get("source_refs")
        if not isinstance(refs, list):
            return set()
        retained: set[str] = set()
        for ref in refs:
            sanitized = _sanitize_trace_string(ref, max_length=240)
            if sanitized:
                retained.add(sanitized)
        return retained
    return None


def _external_context_brief_grounding(
    *,
    context_pack: dict[str, Any] | None,
    dsa_trace: dict[str, Any] | None,
    prompt_trace: dict[str, Any] | None,
) -> dict[str, Any]:
    sources: list[dict[str, Any]] = []
    uncertainty: list[str] = []
    omissions: list[dict[str, str]] = []
    retained_refs = _retained_external_source_refs(prompt_trace)

    if not isinstance(context_pack, dict):
        if isinstance(dsa_trace, dict) and dsa_trace.get("enabled") is True:
            reason = _sanitize_trace_string(dsa_trace.get("reason"), max_length=80)
            status = _sanitize_trace_string(dsa_trace.get("status"), max_length=80)
            omissions.append(
                {
                    "reason": reason or status or "external_context_unavailable",
                    "source_id": "external_context",
                }
            )
        return {
            "source_count": 0,
            "sources": [],
            "uncertainty": [],
            "omissions": omissions,
            "conflicts": [],
        }

    items = context_pack.get("items")
    if not isinstance(items, list):
        items = []

    for index, item in enumerate(items[:20], start=1):
        if not isinstance(item, dict):
            omissions.append(
                {"reason": "malformed_external_context_item", "source_id": f"external:{index}"}
            )
            continue
        source_ref = _sanitize_trace_string(item.get("source_ref"), max_length=240)
        source_name = _sanitize_trace_string(item.get("source_name"), max_length=120)
        if not source_ref:
            omissions.append(
                {"reason": "missing_external_source_ref", "source_id": source_name or "unknown"}
            )
            continue
        if retained_refs is not None and source_ref not in retained_refs:
            omissions.append(
                {"reason": "external_context_prompt_omitted", "source_id": source_ref}
            )
            continue

        retrieved_at = _sanitize_trace_string(item.get("retrieved_at"), max_length=80)
        freshness_state = (
            _sanitize_trace_string(item.get("freshness_state"), max_length=80)
            or ("retrieved" if retrieved_at else "unknown_freshness")
        )
        warnings = _sanitize_trace_string_list(
            item.get("warnings"),
            limit=6,
            item_max_length=80,
        )
        lowered_markers = " ".join([freshness_state, *warnings]).lower()
        if not retrieved_at:
            uncertainty.append(f"{source_ref}: unknown_freshness")
        elif any(marker in lowered_markers for marker in ("stale", "expired", "outdated")):
            uncertainty.append(f"{source_ref}: {freshness_state}")

        source: dict[str, Any] = {
            "kind": "external_context",
            "id": source_ref,
            "state": freshness_state,
            "source_ref": source_ref,
        }
        if source_name:
            source["source_name"] = source_name
        title = _sanitize_trace_string(item.get("title"), max_length=160)
        if title:
            source["title"] = title
        if retrieved_at:
            source["retrieved_at"] = retrieved_at
        sources.append(source)

    errors = context_pack.get("errors")
    if isinstance(errors, list):
        for error in errors[:12]:
            if not isinstance(error, dict):
                continue
            code = _sanitize_trace_string(error.get("code"), max_length=80)
            if code:
                omissions.append({"reason": code, "source_id": "external_context"})

    return {
        "source_count": len(sources),
        "sources": sources[:20],
        "uncertainty": uncertainty[:12],
        "omissions": omissions[:20],
        "conflicts": [],
    }


SAFE_DOCTRINE_CODE = re.compile(r"^[a-z0-9_.:-]{1,120}$")
SAFE_DOCTRINE_STATUS = re.compile(r"^[a-z0-9_.:-]{1,80}$")
DOCTRINE_REASON_CODES = {
    "advanced_dependency_unavailable",
    "augmented_retrieval_failed",
    "canonical_evidence_used",
    "compare_mode_completed",
    "compare_mode_degraded",
    "compare_mode_requested",
    "derivative_active",
    "derivative_augmentation_used",
    "derivative_contradicted",
    "derivative_corrected",
    "derivative_forgotten_or_demoted",
    "derivative_ineligible",
    "derivative_parked",
    "derivative_retracted",
    "derivative_stale",
    "derivative_superseded",
    "derivative_unknown_freshness",
    "derivative_unsupported_validation_state",
    "fallback_to_raw",
    "provenance_missing_or_invalid",
    "retrieval_failed",
    "source_missing_or_unavailable",
    "validation_violation",
}
DOCTRINE_FALLBACK_REASONS = {
    "augmented_retrieval_failed",
    "malformed_vector_result",
    "missing_canonical_source",
    "vector_unavailable",
}
DOCTRINE_CONTRACT_VERSIONS = {
    "raw-retrieval-debug.v1",
}
DOCTRINE_RETRIEVAL_MODES = {
    "augmented",
    "compare",
    "raw",
}
DOCTRINE_DIAGNOSTIC_STATUSES = {
    "degraded",
    "failed",
    "ok",
}
DOCTRINE_DERIVATIVE_OMISSION_REASONS = {
    "cross_owner_derivative_provenance",
    "cross_owner_derivative_source_ref",
    "derivative_source_lookup_unavailable",
    "malformed_derivative_provenance",
    "malformed_derivative_source_ref",
    "missing_derivative_source_record",
    "missing_derivative_source_refs",
}
DOCTRINE_DERIVATIVE_STATES = {
    "active",
    "contradicted",
    "corrected",
    "forgotten_or_demoted",
    "parked",
    "retracted",
    "stale",
    "superseded",
    "unknown_freshness",
    "unsupported_validation_state",
}
DOCTRINE_ARTIFACT_OMISSION_REASONS = {
    "artifact_retrieval_unavailable",
    "augmented_retrieval_failed",
    "cross_owner_derivative_provenance",
    "cross_owner_derivative_source_ref",
    "derivative_source_lookup_unavailable",
    "malformed_artifact_result",
    "malformed_derivative_provenance",
    "malformed_derivative_source_ref",
    "missing_derivative_source",
    "missing_derivative_source_record",
    "missing_derivative_source_refs",
}
DOCTRINE_RETRIEVAL_STATUSES = {
    "degraded",
    "failed",
    "not_requested",
    "not_run",
    "ok",
    "unavailable",
}
DOCTRINE_COUNT_KEYS = {
    "derivative_source_checks_attempted",
    "source_available_count",
    "source_missing_count",
    "source_malformed_count",
    "source_unavailable_count",
    "source_owner_mismatch_count",
    "derived_degraded_count",
    "lifecycle_restricted_derived_count",
}


def _sanitize_doctrine_code(value: Any, *, max_length: int = 120) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()[:max_length]
    if not SAFE_DOCTRINE_CODE.fullmatch(cleaned):
        return None
    return cleaned


def _sanitize_doctrine_status(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()[:80]
    if not SAFE_DOCTRINE_STATUS.fullmatch(cleaned):
        return None
    return cleaned


def _sanitize_doctrine_allowlisted_status(value: Any, allowed_values: set[str]) -> str | None:
    cleaned = _sanitize_doctrine_status(value)
    if cleaned and cleaned in allowed_values:
        return cleaned
    return None


def _sanitize_doctrine_reason_list(
    value: Any,
    *,
    allowed_values: set[str],
    limit: int = 20,
) -> list[str]:
    if not isinstance(value, list):
        return []
    reasons: list[str] = []
    for item in value:
        cleaned = _sanitize_doctrine_code(item)
        if cleaned and cleaned in allowed_values:
            reasons.append(cleaned)
        if len(reasons) >= limit:
            break
    return list(dict.fromkeys(reasons))


def _sanitize_doctrine_counts(
    value: Any,
    *,
    allowed_keys: set[str],
    limit: int = 20,
) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    counts: dict[str, int] = {}
    for key, raw_count in list(value.items())[:limit]:
        cleaned_key = _sanitize_doctrine_code(key, max_length=80)
        count = _sanitize_trace_int(raw_count, minimum=0, maximum=10000)
        if cleaned_key and cleaned_key in allowed_keys and count is not None:
            counts[cleaned_key] = count
    return counts


def _trace_doctrine_summary(retrieval_bundle: dict[str, Any]) -> dict[str, Any]:
    diagnostics = retrieval_bundle.get("diagnostics")
    if diagnostics is None:
        return {"diagnostics_status": "absent"}
    if not isinstance(diagnostics, dict):
        return {"diagnostics_status": "invalid"}

    summary: dict[str, Any] = {"diagnostics_status": "included"}
    contract_version = _sanitize_doctrine_allowlisted_status(
        diagnostics.get("contract_version"),
        DOCTRINE_CONTRACT_VERSIONS,
    )
    mode = _sanitize_doctrine_allowlisted_status(
        diagnostics.get("mode"),
        DOCTRINE_RETRIEVAL_MODES,
    )
    status = _sanitize_doctrine_allowlisted_status(
        diagnostics.get("status"),
        DOCTRINE_DIAGNOSTIC_STATUSES,
    )
    if contract_version:
        summary["contract_version"] = contract_version
    if mode:
        summary["mode"] = mode
    if status:
        summary["status"] = status
    for key in ("canonical_used", "derived_used", "fallback_to_raw"):
        value = _sanitize_trace_bool(diagnostics.get(key))
        if value is not None:
            summary[key] = value

    reason_codes = _sanitize_doctrine_reason_list(
        diagnostics.get("reason_codes"),
        allowed_values=DOCTRINE_REASON_CODES,
    )
    if reason_codes:
        summary["reason_codes"] = reason_codes
    fallback_reasons = _sanitize_doctrine_reason_list(
        diagnostics.get("fallback_reasons"),
        allowed_values=DOCTRINE_FALLBACK_REASONS,
    )
    if fallback_reasons:
        summary["fallback_reasons"] = fallback_reasons

    provenance = diagnostics.get("provenance_summary")
    provenance_summary: dict[str, Any] = {}
    if isinstance(provenance, dict):
        for key in DOCTRINE_COUNT_KEYS:
            count = _sanitize_trace_int(provenance.get(key), minimum=0, maximum=10000)
            if count is not None:
                provenance_summary[key] = count
        omission_counts = _sanitize_doctrine_counts(
            provenance.get("derivative_omissions_by_reason"),
            allowed_keys=DOCTRINE_DERIVATIVE_OMISSION_REASONS,
        )
        if omission_counts:
            provenance_summary["derivative_omissions_by_reason"] = omission_counts
    if provenance_summary:
        summary["provenance_summary"] = provenance_summary

    validation = diagnostics.get("validation")
    validation_summary: dict[str, Any] = {}
    if isinstance(validation, dict):
        for key in ("vector_retrieval_status", "derivative_retrieval_status"):
            value = _sanitize_doctrine_status(validation.get(key))
            if value and value in DOCTRINE_RETRIEVAL_STATUSES:
                validation_summary[key] = value
        for key in DOCTRINE_COUNT_KEYS:
            count = _sanitize_trace_int(validation.get(key), minimum=0, maximum=10000)
            if count is not None:
                validation_summary[key] = count
        state_counts = _sanitize_doctrine_counts(
            validation.get("derivative_state_counts"),
            allowed_keys=DOCTRINE_DERIVATIVE_STATES,
        )
        if state_counts:
            validation_summary["derivative_state_counts"] = state_counts
        omission_reasons = _sanitize_doctrine_reason_list(
            validation.get("artifact_omission_reasons"),
            allowed_values=DOCTRINE_ARTIFACT_OMISSION_REASONS,
        )
        if omission_reasons:
            validation_summary["artifact_omission_reasons"] = omission_reasons
    if validation_summary:
        summary["validation"] = validation_summary
    return summary


def _trace_artifacts(retrieval_bundle: dict[str, Any]) -> dict[str, Any]:
    bundle = retrieval_bundle.get("bundle")
    bundle = bundle if isinstance(bundle, dict) else {}
    artifact_refs = bundle.get("artifact_refs")
    artifact_refs = artifact_refs if isinstance(artifact_refs, list) else []
    retrieval_debug = bundle.get("retrieval_debug")
    retrieval_debug = retrieval_debug if isinstance(retrieval_debug, dict) else {}
    artifact_ids = [
        artifact_id
        for item in artifact_refs[:20]
        if isinstance(item, dict)
        and (
            artifact_id := _sanitize_trace_string(
                item.get("artifact_id"),
                max_length=160,
            )
        )
    ]
    reason = _sanitize_trace_string(
        retrieval_debug.get("artifact_fallback_reason") or retrieval_debug.get("fallback"),
        max_length=120,
    )
    status = "included" if artifact_ids else ("degraded" if reason else "omitted")
    return {
        "status": status,
        "artifact_count": len(artifact_refs),
        "included_ids": artifact_ids,
        "source_reference_count": len(
            [
                item
                for item in artifact_refs
                if isinstance(item, dict) and _bounded_source_ref(item.get("source_ref"))
            ]
        ),
        "reason": reason or ("no_artifacts_returned" if not artifact_ids else None),
    }


def _trace_prompt(prompt_trace: dict[str, Any] | None) -> dict[str, Any]:
    trace = prompt_trace if isinstance(prompt_trace, dict) else {}
    layers = trace.get("layers")
    layers = layers if isinstance(layers, list) else []
    structural_layers = []
    for layer in layers[:30]:
        if not isinstance(layer, dict):
            continue
        structural_layers.append(
            {
                "name": _sanitize_trace_string(layer.get("name"), max_length=80),
                "included": bool(layer.get("included")),
                "message_count": _sanitize_trace_int(
                    layer.get("message_count"),
                    minimum=0,
                    maximum=1000,
                )
                or 0,
            }
        )
    runtime = trace.get("runtime")
    runtime = runtime if isinstance(runtime, dict) else {}
    retrieval_layer = next(
        (layer for layer in structural_layers if layer.get("name") == "retrieval_augmentation"),
        None,
    )
    message_count = _sanitize_trace_int(
        trace.get("message_count"),
        minimum=0,
        maximum=5000,
    )
    prompt_budget = trace.get("prompt_budget")
    prompt_budget = prompt_budget if isinstance(prompt_budget, dict) else {}
    return {
        "layers": structural_layers,
        "ordered_layer_names": [layer["name"] for layer in structural_layers if layer.get("name")],
        "included_layers": [
            layer["name"] for layer in structural_layers if layer.get("name") and layer["included"]
        ],
        "omitted_layers": [
            layer["name"]
            for layer in structural_layers
            if layer.get("name") and not layer["included"]
        ],
        "message_count": message_count or 0,
        "layer_count": len(structural_layers),
        "runtime_overlay": {
            "included": bool(runtime.get("included")),
            "status": _sanitize_trace_string(runtime.get("status"), max_length=80),
            "omission_reason": _sanitize_trace_string(
                runtime.get("omission_reason"),
                max_length=120,
            ),
        },
        "retrieval": {
            "included": bool(retrieval_layer and retrieval_layer["included"]),
        },
        "provider_prompt": trace.get("provider_prompt", {}),
        "provider_fallback_context": trace.get("provider_fallback_context", {}),
        "result_boundary": trace.get("result_boundary", {}),
        "token_accounting": {
            "status": ("estimated" if prompt_budget else "estimate_unavailable"),
            "budget_enforcement": ("enforced" if prompt_budget else "not_enforced"),
        },
        "prompt_budget": prompt_budget,
    }


def _prompt_fingerprint(messages: list[dict[str, str]]) -> dict[str, Any]:
    normalized = [
        {
            "role": str(message.get("role", "")),
            "content": str(message.get("content", "")),
        }
        for message in messages
        if isinstance(message, dict)
    ]
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return {
        "algorithm": "sha256",
        "fingerprint": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "message_count": len(normalized),
        "role_sequence": [item["role"] for item in normalized],
    }


def _public_answer_sources(sources: Any) -> list[dict[str, Any]]:
    if not isinstance(sources, list):
        return []
    public_sources: list[dict[str, Any]] = []
    for source in sources:
        if not isinstance(source, dict):
            continue
        public_source: dict[str, Any] = {}
        artifact_id = _sanitize_trace_string(source.get("artifact_id"), max_length=160)
        if artifact_id:
            public_source["artifact_id"] = artifact_id
        repo_name = _sanitize_trace_string(source.get("repo_name"), max_length=160)
        if repo_name:
            public_source["repo_name"] = repo_name
        file_path = _sanitize_trace_string(source.get("file_path"), max_length=240)
        if file_path:
            public_source["file_path"] = file_path
        snippet = source.get("snippet")
        if isinstance(snippet, str):
            public_source["snippet"] = snippet[:4000]
        relevance_score = source.get("relevance_score")
        if isinstance(relevance_score, (int, float)) and not isinstance(relevance_score, bool):
            public_source["relevance_score"] = relevance_score
        source_ref = _valid_result_source_ref(source.get("source_ref"))
        if source_ref is not None:
            public_source["source_ref"] = source_ref
        if public_source:
            public_sources.append(public_source)
    return public_sources


def _trace_retrieval(retrieval_bundle: dict[str, Any]) -> dict[str, Any]:
    bundle = retrieval_bundle.get("bundle")
    bundle = bundle if isinstance(bundle, dict) else {}
    recent = bundle.get("recent")
    semantic = bundle.get("semantic")
    artifact_refs = bundle.get("artifact_refs")
    debug = bundle.get("retrieval_debug")
    debug = debug if isinstance(debug, dict) else {}

    def structural_items(value: Any, *, artifact: bool = False) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        output: list[dict[str, Any]] = []
        for item in value[:50]:
            if not isinstance(item, dict):
                continue
            structural: dict[str, Any] = {
                "source_ref": _bounded_source_ref(item.get("source_ref")),
                "freshness_state": _sanitize_trace_string(
                    item.get("freshness_state"),
                    max_length=80,
                ),
            }
            if artifact:
                structural["artifact_id"] = _sanitize_trace_string(
                    item.get("artifact_id"),
                    max_length=160,
                )
                structural["file_path"] = _sanitize_trace_string(
                    item.get("file_path"),
                    max_length=240,
                )
                structural["relevance_score"] = item.get("relevance_score")
            else:
                structural["message_id"] = _sanitize_trace_string(
                    item.get("message_id"),
                    max_length=160,
                )
                structural["role"] = _sanitize_trace_string(
                    item.get("role"),
                    max_length=40,
                )
                structural["created_at"] = _sanitize_trace_string(
                    item.get("created_at"),
                    max_length=80,
                )
                structural["score"] = item.get("score")
            output.append({key: value for key, value in structural.items() if value is not None})
        return output

    return {
        "recent": structural_items(recent),
        "semantic": structural_items(semantic),
        "artifact_refs": structural_items(artifact_refs, artifact=True),
        "recent_count": len(recent) if isinstance(recent, list) else 0,
        "semantic_count": len(semantic) if isinstance(semantic, list) else 0,
        "artifact_count": len(artifact_refs) if isinstance(artifact_refs, list) else 0,
        "degraded": bool(debug.get("degraded") or debug.get("fallback")),
        "fallback_reason": _sanitize_trace_string(
            debug.get("fallback_reason") or debug.get("fallback"),
            max_length=120,
        ),
        "vector_status": _sanitize_trace_string(
            debug.get("vector_status"),
            max_length=80,
        ),
        "doctrine_summary": _trace_doctrine_summary(retrieval_bundle),
        "references": _trace_references(retrieval_bundle),
    }


def _sanitize_context_pack_errors(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    errors_out: list[dict[str, str]] = []
    for item in value[:10]:
        if not isinstance(item, dict):
            continue
        code = _sanitize_trace_string(item.get("code"), max_length=80)
        if not code:
            continue
        errors_out.append({"code": code})
    return errors_out


def _sanitize_context_pack_budget(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    budget_out: dict[str, Any] = {}
    max_results = _sanitize_trace_int(value.get("max_results"), minimum=1, maximum=1000)
    returned_results = _sanitize_trace_int(
        value.get("returned_results"),
        minimum=0,
        maximum=1000,
    )
    estimated_bytes = _sanitize_trace_int(
        value.get("estimated_bytes"),
        minimum=0,
        maximum=5_000_000,
    )
    truncated = _sanitize_trace_bool(value.get("truncated"))
    if max_results is not None:
        budget_out["max_results"] = max_results
    if returned_results is not None:
        budget_out["returned_results"] = returned_results
    if estimated_bytes is not None:
        budget_out["estimated_bytes"] = estimated_bytes
    if truncated is not None:
        budget_out["truncated"] = truncated
    return budget_out


def _sanitize_context_pack_source_diagnostics(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    diagnostics_out: list[dict[str, Any]] = []
    for item in value[:10]:
        if not isinstance(item, dict):
            continue
        source_id = _sanitize_trace_string(item.get("source_id"), max_length=80)
        score_band = _sanitize_trace_string(item.get("score_band"), max_length=32)
        if not source_id:
            continue
        entry: dict[str, Any] = {"source_id": source_id}
        score = _sanitize_trace_int(item.get("score"), minimum=-10_000, maximum=10_000)
        if score is not None:
            entry["score"] = score
        if score_band:
            entry["score_band"] = score_band
        reasons = _sanitize_trace_string_list(
            item.get("reasons"),
            limit=6,
            item_max_length=64,
        )
        if reasons:
            entry["reasons"] = reasons
        diagnostics_out.append(entry)
    return diagnostics_out


def _sanitize_context_pack_diagnostics(value: Any) -> tuple[dict[str, Any] | None, str]:
    if value is None:
        return None, "absent"
    if not isinstance(value, dict):
        return None, "invalid"

    diagnostics_out: dict[str, Any] = {}
    selection_mode = _sanitize_trace_string(value.get("selection_mode"), max_length=40)
    ranking_mode = _sanitize_trace_string(value.get("ranking_mode"), max_length=40)
    considered_source_ids = _sanitize_trace_string_list(
        value.get("considered_source_ids"),
        limit=20,
        item_max_length=80,
    )
    selected_source_ids = _sanitize_trace_string_list(
        value.get("selected_source_ids"),
        limit=20,
        item_max_length=80,
    )
    source_diagnostics = _sanitize_context_pack_source_diagnostics(value.get("source_diagnostics"))

    candidate_counts_by_source: dict[str, int] = {}
    raw_candidate_counts = value.get("candidate_counts_by_source")
    if isinstance(raw_candidate_counts, dict):
        for raw_source_id, raw_count in list(raw_candidate_counts.items())[:20]:
            source_id = _sanitize_trace_string(raw_source_id, max_length=80)
            count = _sanitize_trace_int(raw_count, minimum=0, maximum=10_000)
            if source_id and count is not None:
                candidate_counts_by_source[source_id] = count

    budget_truncated_candidates = _sanitize_trace_bool(value.get("budget_truncated_candidates"))

    if selection_mode:
        diagnostics_out["selection_mode"] = selection_mode
    if considered_source_ids:
        diagnostics_out["considered_source_ids"] = considered_source_ids
    if selected_source_ids:
        diagnostics_out["selected_source_ids"] = selected_source_ids
    if source_diagnostics:
        diagnostics_out["source_diagnostics"] = source_diagnostics
    if ranking_mode:
        diagnostics_out["ranking_mode"] = ranking_mode
    if candidate_counts_by_source:
        diagnostics_out["candidate_counts_by_source"] = candidate_counts_by_source
    if budget_truncated_candidates is not None:
        diagnostics_out["budget_truncated_candidates"] = budget_truncated_candidates

    if not diagnostics_out:
        return None, "invalid"
    return diagnostics_out, "included"


def _build_dsa_trace_base(
    *,
    capability_enabled: bool,
    request_enabled: bool,
    external_context_config: dict[str, Any],
    allowed_sensitivity: str,
    max_results: int | None,
) -> dict[str, Any]:
    return {
        "capability_enabled": capability_enabled,
        "enabled": request_enabled,
        "called": False,
        "requested_source_ids": _sanitize_trace_string_list(
            external_context_config.get("source_ids"),
            limit=20,
            item_max_length=80,
        ),
        "requested_domain_tags": _sanitize_trace_string_list(
            external_context_config.get("domain_tags"),
            limit=20,
            item_max_length=80,
        ),
        "allowed_sensitivity": _sanitize_trace_string(
            allowed_sensitivity,
            max_length=40,
        ),
        "max_results": max_results if max_results is not None else 5,
    }


def _normalize_external_context_config(
    external_context: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(external_context, dict):
        return {}

    normalized: dict[str, Any] = {}

    source_ids = external_context.get("source_ids")
    if isinstance(source_ids, list):
        cleaned_source_ids = [item for item in source_ids if isinstance(item, str) and item]
        if cleaned_source_ids:
            normalized["source_ids"] = cleaned_source_ids

    domain_tags = external_context.get("domain_tags")
    if isinstance(domain_tags, list):
        cleaned_domain_tags = [item for item in domain_tags if isinstance(item, str) and item]
        if cleaned_domain_tags:
            normalized["domain_tags"] = cleaned_domain_tags

    if external_context.get("enabled") is not None:
        normalized["enabled"] = bool(external_context.get("enabled"))

    allowed_sensitivity = external_context.get("allowed_sensitivity")
    if isinstance(allowed_sensitivity, str) and allowed_sensitivity:
        normalized["allowed_sensitivity"] = allowed_sensitivity

    max_results = external_context.get("max_results")
    if isinstance(max_results, int):
        normalized["max_results"] = max_results

    return normalized


def _bounded_recent_messages(
    messages: list[dict[str, Any]], limit: int = 12
) -> list[dict[str, str]]:
    bounded: list[dict[str, str]] = []
    for item in messages[-limit:]:
        role = item.get("role")
        content = item.get("content")
        if isinstance(role, str) and isinstance(content, str) and role and content:
            bounded.append({"role": role, "content": content})
    return bounded


def _build_dsa_budget(max_results: int | None) -> dict[str, int]:
    budget = {
        "max_results": 5,
        "max_bytes": 50000,
        "max_text_chars": 12000,
    }
    if max_results is not None:
        budget["max_results"] = max_results
    return budget


def _sanitize_context_pack(response: dict[str, Any]) -> dict[str, Any]:
    items_out = []
    for item in response.get("items") or []:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if not isinstance(text, str) or not text:
            continue
        items_out.append(
            {
                "source_ref": item.get("source_ref"),
                "source_name": item.get("source_name"),
                "title": item.get("title"),
                "text": text,
                "retrieved_at": item.get("retrieved_at"),
                "freshness_state": _sanitize_trace_string(
                    item.get("freshness_state"),
                    max_length=80,
                ),
                "warnings": item.get("warnings", []),
                "sensitivity": item.get("sensitivity"),
                "sensitivity_level": item.get("sensitivity_level"),
                "sensitivity_domains": item.get("sensitivity_domains"),
                "domain_tags": item.get("domain_tags"),
                "policy_metadata": item.get("policy_metadata"),
            }
        )

    errors_out = _sanitize_context_pack_errors(response.get("errors"))
    budget_out = _sanitize_context_pack_budget(response.get("budget"))
    diagnostics_out, diagnostics_status = _sanitize_context_pack_diagnostics(
        response.get("diagnostics")
    )

    return {
        "query": _sanitize_trace_string(response.get("query"), max_length=500),
        "sources_used": _sanitize_trace_string_list(
            response.get("sources_used"),
            limit=20,
            item_max_length=80,
        ),
        "items": items_out,
        "errors": errors_out,
        "budget": budget_out,
        "diagnostics": diagnostics_out,
        "diagnostics_status": diagnostics_status,
    }


async def _resolve_external_context(
    *,
    dsa: DataSourceAggregatorClient | None,
    dsa_enabled: bool,
    external_context_enabled: bool,
    external_context: dict[str, Any] | None,
    external_calls_allowed: bool,
    query: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    external_context_config = _normalize_external_context_config(external_context)
    allowed_sensitivity = external_context_config.get("allowed_sensitivity", "medium")
    max_results = external_context_config.get("max_results")
    dsa_trace_base = _build_dsa_trace_base(
        capability_enabled=dsa_enabled,
        request_enabled=external_context_enabled,
        external_context_config=external_context_config,
        allowed_sensitivity=allowed_sensitivity,
        max_results=max_results,
    )
    if not dsa_enabled:
        return None, {
            **dsa_trace_base,
            "status": "disabled_by_service",
            "reason": "deployment_capability_disabled",
        }
    if not external_context_enabled:
        return None, {
            **dsa_trace_base,
            "status": "disabled_by_request",
            "reason": "request_opt_in_absent",
        }
    if not external_calls_allowed:
        return None, {
            **dsa_trace_base,
            "status": "skipped_local_only",
            "reason": "local_only_policy",
        }
    if dsa is None:
        return None, {
            **dsa_trace_base,
            "status": "error",
            "reason": "client_not_configured",
            "error_code": "client_not_configured",
        }
    try:
        response = await dsa.context_pack(
            query=query,
            source_ids=external_context_config.get("source_ids"),
            domain_tags=external_context_config.get("domain_tags"),
            allowed_sensitivity=allowed_sensitivity,
            budget=_build_dsa_budget(max_results),
        )
        if not isinstance(response, dict):
            return None, {
                **dsa_trace_base,
                "called": True,
                "status": "error",
                "reason": "malformed_response",
                "error_code": "malformed_response",
            }
        context_pack = _sanitize_context_pack(response)
        diagnostics = context_pack.get("diagnostics")
        errors = context_pack.get("errors", [])
        item_count = len(context_pack.get("items", []))
        sources_used = context_pack.get("sources_used", [])
        budget = context_pack.get("budget", {})
        error_codes = [
            error["code"]
            for error in errors
            if isinstance(error, dict) and isinstance(error.get("code"), str)
        ]
        dsa_trace = {
            **dsa_trace_base,
            "called": True,
            "status": "success" if item_count > 0 else "success_no_items",
            "reason": (
                "items_included_with_bounded_errors"
                if item_count > 0 and error_codes
                else "items_included"
                if item_count > 0
                else "bounded_errors_returned"
                if error_codes
                else "no_usable_items"
            ),
            "item_count": item_count,
            "sources_used": sources_used,
            "errors_count": len(error_codes),
            "error_codes": error_codes,
            "budget_truncated": bool(budget.get("truncated")),
            "context_injected": item_count > 0,
            "diagnostics_status": context_pack.get("diagnostics_status"),
        }
        if diagnostics:
            dsa_trace["selection_mode"] = diagnostics.get("selection_mode")
            dsa_trace["selected_source_ids"] = diagnostics.get("selected_source_ids", [])
            dsa_trace["ranking_mode"] = diagnostics.get("ranking_mode")
            dsa_trace["candidate_counts_by_source"] = diagnostics.get(
                "candidate_counts_by_source",
                {},
            )
            dsa_trace["candidate_truncated"] = bool(diagnostics.get("budget_truncated_candidates"))
            if diagnostics.get("considered_source_ids"):
                dsa_trace["considered_source_ids"] = diagnostics["considered_source_ids"]
            if diagnostics.get("source_diagnostics"):
                dsa_trace["source_diagnostics"] = diagnostics["source_diagnostics"]
        return context_pack, {
            **dsa_trace,
        }
    except httpx.TimeoutException:
        return None, {
            **dsa_trace_base,
            "called": True,
            "status": "error",
            "reason": "timeout",
            "error_code": "timeout",
        }
    except httpx.HTTPError as exc:
        error_code = "http_error"
        if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
            error_code = f"http_{exc.response.status_code}"
        return None, {
            **dsa_trace_base,
            "called": True,
            "status": "error",
            "reason": "http_failure",
            "error_code": error_code,
        }
    except Exception:
        return None, {
            **dsa_trace_base,
            "called": True,
            "status": "error",
            "reason": "unexpected_failure",
            "error_code": "unexpected_error",
        }


async def _resolve_companion_policy(
    *,
    runtime: Any | None,
    enabled: bool,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    requested_scene: str | None = None,
) -> tuple[list[dict[str, Any]] | None, dict[str, Any]]:
    if not enabled:
        return None, _companion_disabled_trace()
    if runtime is None:
        return None, {
            "attempted": False,
            "status": "failed",
            "included": False,
            "error_type": "RuntimeClientNotConfigured",
            "omission_reason": "runtime_client_not_configured",
            "cognitive_runtime_compile_status": "failed",
            "cognitive_runtime_compile_error": "RuntimeClientNotConfigured",
            "cognitive_runtime_compile_endpoint": None,
        }

    try:
        response = await runtime.compile_companion_policy(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            requested_scene=requested_scene,
        )
    except Exception as e:
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(e).__name__,
            "omission_reason": "companion_policy_unavailable",
            "cognitive_runtime_compile_status": "failed",
            "cognitive_runtime_compile_error": str(e) or type(e).__name__,
            "cognitive_runtime_compile_endpoint": getattr(
                runtime,
                "last_companion_compile_endpoint",
                None,
            ),
        }

    if not isinstance(response, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(response).__name__,
            "omission_reason": "malformed_companion_policy_response",
            "cognitive_runtime_compile_status": "failed",
            "cognitive_runtime_compile_error": type(response).__name__,
            "cognitive_runtime_compile_endpoint": None,
        }

    compile_endpoint = response.get("_cognitive_runtime_compile_endpoint")
    overlays = response.get("overlays")
    warnings = response.get("warnings", [])
    if not isinstance(warnings, list):
        warnings = ["malformed_companion_policy_warnings"]
    base_trace = {
        "attempted": True,
        "profile_id": response.get("profile_id"),
        "profile_version": response.get("profile_version"),
        "contract_id": response.get("contract_id"),
        "contract_version": response.get("contract_version"),
        "scene_id": response.get("scene_id"),
        "scene_confidence": response.get("scene_confidence"),
        "scene_source": response.get("scene_source"),
        "warnings": warnings,
        "companion_profile_id": response.get("profile_id"),
        "companion_profile_version": response.get("profile_version"),
        "interaction_contract_id": response.get("contract_id"),
        "interaction_contract_version": response.get("contract_version"),
        "companion_policy_warnings": warnings,
        "cognitive_runtime_compile_status": "included",
        "cognitive_runtime_compile_error": None,
        "cognitive_runtime_compile_endpoint": compile_endpoint,
    }
    interaction_contract = response.get("interaction_contract")
    if isinstance(interaction_contract, dict):
        base_trace["interaction_contract"] = interaction_contract
    contract_trace = response.get("contract_trace")
    if isinstance(contract_trace, dict):
        base_trace["contract_trace"] = contract_trace
    if not isinstance(overlays, list) or not overlays:
        return None, {
            **base_trace,
            "status": "omitted",
            "included": False,
            "cognitive_runtime_compile_status": "omitted",
            "cognitive_runtime_compile_error": "companion_overlays_missing",
            "omission_reason": "companion_overlays_missing",
        }

    included_overlays = []
    for overlay in overlays:
        if isinstance(overlay, dict):
            included_overlays.append(
                {
                    "overlay_id": overlay.get("overlay_id"),
                    "overlay_type": overlay.get("overlay_type"),
                }
            )

    return overlays, {
        **base_trace,
        "status": "included",
        "included": True,
        "included_overlays": included_overlays,
        "companion_overlay_ids": [
            item["overlay_id"] for item in included_overlays if item.get("overlay_id")
        ],
    }


async def _resolve_runtime_session(
    *,
    runtime: Any | None,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if runtime is None:
        return None, {
            "attempted": False,
            "status": "failed",
            "included": False,
            "error_type": "RuntimeClientNotConfigured",
            "omission_reason": "runtime_client_not_configured",
        }
    try:
        response = await runtime.resolve_session(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
        )
    except Exception as e:
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(e).__name__,
            "omission_reason": "runtime_session_unavailable",
        }
    session = response.get("runtime_session") if isinstance(response, dict) else None
    if not isinstance(session, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(response).__name__,
            "omission_reason": "malformed_runtime_session_response",
        }
    return session, {
        "attempted": True,
        "status": "included",
        "included": True,
        "runtime_session_id": session.get("runtime_session_id"),
        "session_status": session.get("status"),
        "surface": session.get("surface"),
    }


def _runtime_session_trace_from_session(
    session: dict[str, Any] | None,
    *,
    attempted: bool,
    omission_reason: str,
    error_type: str | None = None,
) -> dict[str, Any]:
    if not isinstance(session, dict):
        trace = {
            "attempted": attempted,
            "status": "failed",
            "included": False,
            "omission_reason": omission_reason,
        }
        if error_type is not None:
            trace["error_type"] = error_type
        return trace
    return {
        "attempted": attempted,
        "status": "included",
        "included": True,
        "runtime_session_id": session.get("runtime_session_id"),
        "session_status": session.get("status"),
        "surface": session.get("surface"),
    }


async def _start_runtime_turn(
    *,
    runtime: Any | None,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    input_message_id: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if runtime is None:
        return None, {
            "attempted": False,
            "status": "failed",
            "included": False,
            "error_type": "RuntimeClientNotConfigured",
            "omission_reason": "runtime_client_not_configured",
        }
    try:
        response = await runtime.start_turn(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            input_message_id=input_message_id,
        )
    except Exception as e:
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(e).__name__,
            "omission_reason": "runtime_turn_unavailable",
        }
    turn = response.get("runtime_turn") if isinstance(response, dict) else None
    session = response.get("runtime_session") if isinstance(response, dict) else None
    if not isinstance(turn, dict) or not isinstance(session, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(response).__name__,
            "omission_reason": "malformed_runtime_turn_response",
        }
    return response, {
        "attempted": True,
        "status": "included",
        "included": True,
        "runtime_session_id": session.get("runtime_session_id"),
        "runtime_turn_id": turn.get("runtime_turn_id"),
        "turn_status": turn.get("turn_status"),
    }


async def _advance_runtime_turn(
    *,
    runtime: Any | None,
    turn_state_trace: dict[str, Any],
    request_id: str,
    turn_status: str,
) -> None:
    runtime_session_id = turn_state_trace.get("runtime_session_id")
    runtime_turn_id = turn_state_trace.get("runtime_turn_id")
    if runtime is None or not runtime_session_id or not runtime_turn_id:
        return
    try:
        response = await runtime.update_turn(
            request_id=request_id,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
            turn_status=turn_status,
        )
        if isinstance(response, dict):
            turn = response.get("runtime_turn", {}) or {}
            turn_state_trace["turn_status"] = turn.get("turn_status", turn_status)
    except Exception as e:
        turn_state_trace.setdefault("warnings", []).append(type(e).__name__)


async def _complete_runtime_turn(
    *,
    runtime: Any | None,
    turn_state_trace: dict[str, Any],
    request_id: str,
    turn_status: str,
) -> None:
    runtime_session_id = turn_state_trace.get("runtime_session_id")
    runtime_turn_id = turn_state_trace.get("runtime_turn_id")
    if runtime is None or not runtime_session_id or not runtime_turn_id:
        return
    try:
        response = await runtime.complete_turn(
            request_id=request_id,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
            turn_status=turn_status,
        )
        if isinstance(response, dict):
            turn = response.get("runtime_turn", {}) or {}
            turn_state_trace["turn_status"] = turn.get("turn_status", turn_status)
            turn_state_trace["completed"] = True
    except Exception as e:
        turn_state_trace.setdefault("warnings", []).append(type(e).__name__)


async def _resolve_runtime_identity(
    *,
    runtime: Any | None,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if runtime is None:
        return None, {
            "attempted": False,
            "status": "failed",
            "included": False,
            "error_type": "RuntimeClientNotConfigured",
            "omission_reason": "runtime_client_not_configured",
        }
    try:
        response = await runtime.resolve_identity(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
        )
    except Exception as e:
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(e).__name__,
            "omission_reason": "runtime_identity_unavailable",
        }
    if not isinstance(response, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(response).__name__,
            "omission_reason": "malformed_runtime_identity_response",
        }
    identity = response.get("runtime_identity")
    trace = response.get("trace") or {}
    if not isinstance(identity, dict) or not isinstance(trace, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": "malformed_identity_payload",
            "omission_reason": "malformed_runtime_identity_response",
        }
    return identity, {
        "attempted": True,
        "status": "included",
        "included": True,
        "runtime_session_id": trace.get("runtime_session_id"),
        "active_persona_id": trace.get("active_persona_id"),
        "persona_resolution_reason": trace.get("persona_resolution_reason"),
        "persona_override_source": trace.get("persona_override_source"),
        "surface_id": trace.get("surface_id"),
        "surface_type": trace.get("surface_type"),
        "surface_display_name": trace.get("surface_display_name"),
        "advisory_memory_scope_summary": trace.get("advisory_memory_scope_summary", []),
        "advisory_tool_permission_summary": trace.get("advisory_tool_permission_summary", []),
    }


async def _resolve_runtime_overlay(
    *,
    runtime: Any | None,
    enable_runtime_overlays: bool,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if not enable_runtime_overlays:
        return None, _runtime_disabled_trace()
    if runtime is None:
        return None, {
            "attempted": False,
            "status": "failed",
            "included": False,
            "error_type": "RuntimeClientNotConfigured",
        }

    try:
        response = await runtime.overlay(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
        )
    except Exception as e:
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(e).__name__,
        }

    if not isinstance(response, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(response).__name__,
            "omission_reason": "malformed_runtime_overlay_response",
        }
    state = response.get("runtime_state") or {}
    overlay = response.get("overlay")
    if not isinstance(state, dict):
        state = {}
    base_trace = {
        "attempted": True,
        "runtime_state_id": state.get("runtime_state_id"),
        "reset_after_turn": bool(state.get("reset_after_turn", False)),
    }
    if response.get("omitted") or not overlay:
        return None, {
            **base_trace,
            "status": "omitted",
            "included": False,
            "omission_reason": response.get("omission_reason") or "overlay_missing",
        }
    if not isinstance(overlay, dict):
        return None, {
            **base_trace,
            "status": "failed",
            "included": False,
            "error_type": type(overlay).__name__,
            "omission_reason": "malformed_runtime_overlay_response",
        }

    return overlay, {
        **base_trace,
        "status": "included",
        "included": True,
        "overlay_id": overlay.get("overlay_id"),
        "overlay_type": overlay.get("overlay_type"),
        "source_fields": overlay.get("source_fields", []),
    }


async def _resolve_world_state(
    *,
    runtime: Any | None,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    active_persona_id: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if runtime is None:
        return None, {
            "attempted": False,
            "status": "failed",
            "included": False,
            "error_type": "RuntimeClientNotConfigured",
            "omission_reason": "runtime_client_not_configured",
        }
    try:
        response = await runtime.world_state_resolve(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            active_persona_id=active_persona_id,
        )
    except Exception as e:
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(e).__name__,
            "omission_reason": "world_state_unavailable",
        }
    if not isinstance(response, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(response).__name__,
            "omission_reason": "malformed_world_state_response",
        }
    trace = response.get("trace")
    if not isinstance(trace, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": "malformed_world_state_trace",
            "omission_reason": "malformed_world_state_response",
        }
    included_claims = response.get("included_claims")
    prompt_content = response.get("prompt_content")
    if not isinstance(included_claims, list):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": "malformed_world_state_claims",
            "omission_reason": "malformed_world_state_response",
        }
    base_trace = {
        "attempted": True,
        "active_persona_id": trace.get("active_persona_id"),
        "allowed_domains": trace.get("allowed_domains", []),
        "included_claim_count": trace.get("included_claim_count", 0),
        "excluded_claim_count": trace.get("excluded_claim_count", 0),
        "stale_count": trace.get("stale_count", 0),
        "aging_count": trace.get("aging_count", 0),
        "expired_count": trace.get("expired_count", 0),
        "conflicted_count": trace.get("conflicted_count", 0),
        "confirmation_required": bool(trace.get("confirmation_required", False)),
    }
    if not isinstance(prompt_content, str) or not prompt_content:
        return None, {
            **base_trace,
            "status": "omitted",
            "included": False,
            "omission_reason": "empty_world_state",
        }
    world_state_out: dict[str, Any] = {"prompt_content": prompt_content}
    for key in (
        "sensitivity",
        "sensitivity_level",
        "sensitivity_domains",
        "domain_tags",
        "policy_metadata",
    ):
        if key in response:
            world_state_out[key] = response.get(key)
        elif key in trace:
            world_state_out[key] = trace.get(key)
    return world_state_out, {
        **base_trace,
        "status": "included",
        "included": True,
    }


async def _resolve_relationship_context(
    *,
    runtime: Any | None,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    active_persona_id: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if runtime is None:
        return None, {
            "attempted": False,
            "status": "failed",
            "included": False,
            "error_type": "RuntimeClientNotConfigured",
            "omission_reason": "runtime_client_not_configured",
        }
    try:
        response = await runtime.relationship_select(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            active_persona_id=active_persona_id,
        )
    except Exception as e:
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(e).__name__,
            "omission_reason": "relationship_context_unavailable",
        }
    if not isinstance(response, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": type(response).__name__,
            "omission_reason": "malformed_relationship_context_response",
        }
    trace = response.get("trace")
    selected_relationships = response.get("selected_relationships")
    prompt_content = response.get("prompt_content")
    projection = response.get("retrieval_scope_projection")
    if not isinstance(trace, dict) or not isinstance(selected_relationships, list):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "error_type": "malformed_relationship_context_payload",
            "omission_reason": "malformed_relationship_context_response",
        }
    base_trace = {
        "attempted": True,
        "selected_relationship_count": trace.get("selected_relationship_count", 0),
        "excluded_relationship_count": trace.get("excluded_relationship_count", 0),
        "relationship_edges_used": trace.get("relationship_edges_used", []),
        "relationship_edges_excluded": trace.get("relationship_edges_excluded", []),
        "relationship_exclusion_reasons": trace.get("relationship_exclusion_reasons", {}),
        "relationship_context_overlay_applied": bool(
            trace.get("relationship_context_overlay_applied", False)
        ),
        "relationship_conflicts": trace.get("relationship_conflicts", []),
        "relationship_confirmation_required": bool(
            trace.get("relationship_confirmation_required", False)
        ),
        "active_persona_id": trace.get("active_persona_id"),
        "allowed_relationship_scopes": trace.get("allowed_relationship_scopes", []),
    }
    if isinstance(projection, dict):
        base_trace["retrieval_scope_projection_raw"] = {
            "applied": projection.get("applied"),
            "relationship_ids": projection.get("relationship_ids"),
            "entity_ids": projection.get("entity_ids"),
            "relationship_scopes": projection.get("relationship_scopes"),
            "reason_codes": projection.get("reason_codes"),
        }
    if not isinstance(prompt_content, str) or not prompt_content:
        return None, {
            **base_trace,
            "status": "omitted",
            "included": False,
            "omission_reason": "empty_relationship_context",
        }
    relationship_context_out: dict[str, Any] = {"prompt_content": prompt_content}
    for key in (
        "sensitivity",
        "sensitivity_level",
        "sensitivity_domains",
        "domain_tags",
        "policy_metadata",
    ):
        if key in response:
            relationship_context_out[key] = response.get(key)
        elif key in trace:
            relationship_context_out[key] = trace.get(key)
    return relationship_context_out, {
        **base_trace,
        "status": "included",
        "included": True,
    }


async def _resolve_mandatory_retrieval_policy(
    *,
    runtime: Any | None,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    persona_containment: dict[str, Any] | None,
) -> MandatoryRetrievalPolicy:
    base_policy, validation_trace = _validate_persona_containment_policy(persona_containment)
    if base_policy is None:
        return MandatoryRetrievalPolicy(
            containment_policy=None,
            relationship_context=None,
            relationship_trace={
                "attempted": False,
                "status": "skipped",
                "included": False,
                "omission_reason": validation_trace.get("policy_validation_reason"),
                "retrieval_scope_projection_applied": None,
                "relationship_id_count": 0,
                "entity_id_count": 0,
                "relationship_scope_count": 0,
            },
            validation_trace=validation_trace,
        )

    relationship_context, relationship_trace = await _resolve_relationship_context(
        runtime=runtime,
        request_id=request_id,
        owner_id=owner_id,
        conversation_id=conversation_id,
        surface=surface,
        runtime_session_id=runtime_session_id,
        active_persona_id=persona_containment.get("active_persona_id"),
    )
    projection_raw = relationship_trace.get("retrieval_scope_projection_raw")
    if projection_raw is None and isinstance(relationship_trace, dict):
        projection_raw = relationship_trace.get("retrieval_scope_projection")
    relationship_trace.pop("retrieval_scope_projection_raw", None)
    relationship_trace.pop("retrieval_scope_projection", None)
    projection, projection_reason = _validate_relationship_projection(projection_raw)
    if projection is None:
        validation_trace.update(
            {
                "policy_validation_status": "failed",
                "policy_validation_reason": projection_reason,
            }
        )
        relationship_trace.update(
            {
                "status": "failed",
                "included": False,
                "omission_reason": projection_reason,
                "retrieval_scope_projection_applied": None,
                "selected_relationship_count": 0,
                "excluded_relationship_count": 0,
                "relationship_edges_used": [],
                "relationship_edges_excluded": [],
                "relationship_exclusion_reasons": {},
                "relationship_context_overlay_applied": False,
                "relationship_conflicts": [],
                "relationship_confirmation_required": False,
                "allowed_relationship_scopes": [],
                "relationship_id_count": 0,
                "entity_id_count": 0,
                "relationship_scope_count": 0,
            }
        )
        return MandatoryRetrievalPolicy(
            containment_policy=None,
            relationship_context=None,
            relationship_trace=relationship_trace,
            validation_trace=validation_trace,
        )

    relationship_trace.update(
        {
            "retrieval_scope_projection_applied": projection["applied"],
            "relationship_edges_used": projection["relationship_ids"],
            "allowed_relationship_scopes": projection["relationship_scopes"],
            "relationship_id_count": len(projection["relationship_ids"]),
            "entity_id_count": len(projection["entity_ids"]),
            "relationship_scope_count": len(projection["relationship_scopes"]),
        }
    )
    return MandatoryRetrievalPolicy(
        containment_policy={
            **base_policy,
            "relationship_scope_projection": projection,
        },
        relationship_context=relationship_context,
        relationship_trace=relationship_trace,
        validation_trace=validation_trace,
    )


async def _resolve_interrupt_policy(
    *,
    runtime: Any | None,
    interrupt_policy_mode: str,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    current_user_text: str,
    recent_messages: list[dict[str, str]],
    requested_scene: str | None = None,
) -> dict[str, Any] | None:
    if interrupt_policy_mode != "evaluate_only":
        return None
    if runtime is None:
        return {
            "attempted": False,
            "status": "failed",
            "included": False,
            "mode": interrupt_policy_mode,
            "error_type": "RuntimeClientNotConfigured",
            "omission_reason": "runtime_client_not_configured",
        }

    try:
        response = await runtime.evaluate_interrupt(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            current_user_text=current_user_text,
            recent_messages=recent_messages,
            requested_scene=requested_scene,
        )
    except Exception as e:
        return {
            "attempted": True,
            "status": "failed",
            "included": False,
            "mode": interrupt_policy_mode,
            "error_type": type(e).__name__,
            "omission_reason": "interrupt_policy_unavailable",
        }

    if not isinstance(response, dict):
        return {
            "attempted": True,
            "status": "failed",
            "included": False,
            "mode": interrupt_policy_mode,
            "error_type": type(response).__name__,
            "omission_reason": "malformed_interrupt_policy_response",
        }

    return {
        "attempted": True,
        "status": "included",
        "included": True,
        "mode": interrupt_policy_mode,
        "trigger_class": response.get("trigger_class"),
        "confidence": response.get("confidence"),
        "style_selected": response.get("style_selected"),
        "should_interrupt": bool(response.get("should_interrupt", False)),
        "should_defer": bool(response.get("should_defer", True)),
        "reason_json": response.get("reason_json", {}),
        "contract_constraints_applied": response.get("contract_constraints_applied", {}),
        "warnings": response.get("warnings", []),
        "debug": response.get("debug", {}),
        "user_visible_suppressed": True,
    }


async def _resolve_interaction_governance(
    *,
    runtime: Any | None,
    enabled: bool,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    surface_session_id: str | None,
    active_mode: str | None,
    current_user_text: str,
    recent_messages: list[dict[str, str]],
    surface_metadata_json: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if not enabled:
        return None, _interaction_governance_disabled_trace()
    if runtime is None:
        return None, {
            "attempted": False,
            "status": "failed",
            "included": False,
            "runtime_call_status": "unavailable",
            "error_type": "RuntimeClientNotConfigured",
            "omission_reason": "runtime_client_not_configured",
        }

    try:
        response = await runtime.evaluate_interaction_governance(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
            surface_session_id=surface_session_id,
            active_mode=active_mode,
            current_user_text=current_user_text or None,
            recent_messages=recent_messages,
            surface_metadata_json=surface_metadata_json,
        )
    except Exception as e:
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "runtime_call_status": "failed",
            "error_type": type(e).__name__,
            "omission_reason": "interaction_governance_unavailable",
        }

    if not isinstance(response, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "runtime_call_status": "malformed",
            "error_type": type(response).__name__,
            "omission_reason": "malformed_interaction_governance_response",
        }

    result = response.get("result")
    if not isinstance(result, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "runtime_call_status": "malformed",
            "error_type": "malformed_interaction_governance_payload",
            "omission_reason": "malformed_interaction_governance_response",
        }

    reason_summary = result.get("reason_summary", [])
    if not isinstance(reason_summary, list):
        reason_summary = []

    return result, {
        "attempted": True,
        "status": "included",
        "included": True,
        "runtime_call_status": "included",
        "interaction_kind": result.get("interaction_kind"),
        "response_posture": result.get("response_posture"),
        "commentary_allowed": result.get("commentary_allowed"),
        "humor_allowed": result.get("humor_allowed"),
        "action_allowed": result.get("action_allowed"),
        "requires_confirmation": result.get("requires_confirmation"),
        "privacy_sensitivity_hint": result.get("privacy_sensitivity_hint"),
        "confidence": result.get("confidence"),
        "reason_summary": reason_summary,
    }


async def _resolve_persona_containment(
    *,
    runtime: Any | None,
    enabled: bool,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    persona_scope_hint: str | None,
    interaction_kind: str | None,
    current_user_text: str,
    recent_messages: list[dict[str, str]],
    surface_metadata_json: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if not enabled:
        return None, _persona_containment_disabled_trace()
    if runtime is None:
        return None, {
            "attempted": False,
            "status": "failed",
            "included": False,
            "omission_reason": "runtime_client_not_configured",
            "retrieval_scope_status": "not_enforced",
            "retrieval_scope_reason": "retrieval_scope_not_enforced",
        }

    try:
        response = await runtime.evaluate_persona_containment(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
            persona_scope_hint=persona_scope_hint,
            interaction_kind=interaction_kind,
            current_user_text=current_user_text or None,
            recent_messages=recent_messages,
            surface_metadata_json=surface_metadata_json,
        )
    except Exception:
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "omission_reason": "persona_containment_unavailable",
            "retrieval_scope_status": "not_enforced",
            "retrieval_scope_reason": "retrieval_scope_not_enforced",
        }

    if not isinstance(response, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "omission_reason": "malformed_persona_containment_response",
            "retrieval_scope_status": "not_enforced",
            "retrieval_scope_reason": "retrieval_scope_not_enforced",
        }

    result = response.get("result")
    if not isinstance(result, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "omission_reason": "malformed_persona_containment_response",
            "retrieval_scope_status": "not_enforced",
            "retrieval_scope_reason": "retrieval_scope_not_enforced",
        }

    reason_summary = result.get("reason_summary", [])
    if not isinstance(reason_summary, list):
        reason_summary = []

    return result, {
        "attempted": True,
        "status": "included",
        "included": True,
        "active_persona_id": result.get("active_persona_id"),
        "capability_domain": result.get("capability_domain"),
        "allowed_memory_domains": result.get("allowed_memory_domains", []),
        "blocked_memory_domains": result.get("blocked_memory_domains", []),
        "allowed_world_state_domains": result.get("allowed_world_state_domains", []),
        "allowed_relationship_domains": result.get("allowed_relationship_domains", []),
        "allowed_tool_domains": result.get("allowed_tool_domains", []),
        "cross_scope_access_allowed": result.get("cross_scope_access_allowed"),
        "cross_scope_reason": result.get("cross_scope_reason"),
        "confidence": result.get("confidence"),
        "reason_summary": reason_summary,
        "retrieval_scope_status": "not_enforced",
        "retrieval_scope_reason": "retrieval_scope_not_enforced",
    }


async def _resolve_restraint(
    *,
    runtime: Any | None,
    enabled: bool,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    interaction_kind: str | None,
    response_posture: str | None,
    active_persona_id: str | None,
    capability_domain: str | None,
    current_user_text: str,
    recent_messages: list[dict[str, str]],
    surface_metadata_json: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if not enabled:
        return None, _restraint_disabled_trace()
    if runtime is None:
        return None, {
            "attempted": False,
            "status": "failed",
            "included": False,
            "omission_reason": "runtime_client_not_configured",
        }

    try:
        response = await runtime.evaluate_restraint(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
            interaction_kind=interaction_kind,
            response_posture=response_posture,
            active_persona_id=active_persona_id,
            capability_domain=capability_domain,
            current_user_text=current_user_text or None,
            recent_messages=recent_messages,
            surface_metadata_json=surface_metadata_json,
        )
    except Exception:
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "omission_reason": "restraint_unavailable",
        }

    if not isinstance(response, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "omission_reason": "malformed_restraint_response",
        }

    result = response.get("result")
    if not isinstance(result, dict):
        return None, {
            "attempted": True,
            "status": "failed",
            "included": False,
            "omission_reason": "malformed_restraint_response",
        }

    reason_summary = result.get("reason_summary", [])
    if not isinstance(reason_summary, list):
        reason_summary = []

    return result, {
        "attempted": True,
        "status": "included",
        "included": True,
        "restraint_policy": result.get("restraint_policy"),
        "domains": result.get("domains", []),
        "reason": result.get("reason"),
        "confidence": result.get("confidence"),
        "reason_summary": reason_summary,
        "retrieval_suppressed": result.get("retrieval_suppressed"),
        "personalization_suppressed": result.get("personalization_suppressed"),
        "proactive_output_suppressed": result.get("proactive_output_suppressed"),
        "brevity_preferred": result.get("brevity_preferred"),
        "clarification_preferred": result.get("clarification_preferred"),
    }


async def _resolve_privacy_context(
    *,
    runtime: Any | None,
    enabled: bool,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    payload: dict[str, Any],
    retrieval_bundle: dict[str, Any],
    external_context_pack: dict[str, Any] | None = None,
    runtime_identity: dict[str, Any] | None = None,
    runtime_overlay: dict[str, Any] | None = None,
    world_state: dict[str, Any] | None = None,
    relationship_context: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    if not enabled:
        return None, _privacy_context_disabled_trace()

    derived = derive_privacy_context(
        payload=payload,
        retrieval_bundle=retrieval_bundle,
        external_context_pack=external_context_pack,
        runtime_identity=runtime_identity,
        runtime_overlay=runtime_overlay,
        world_state=world_state,
        relationship_context=relationship_context,
    )

    def _fallback(fallback_reason: str) -> tuple[dict[str, Any], dict[str, Any]]:
        result, trace = privacy_fallback_policy(
            surface_category=derived.surface_category,
            sensitivity_level=derived.sensitivity_level,
            fallback_reason=fallback_reason,
        )
        trace["sensitivity_domain_count"] = len(derived.sensitivity_domains)
        return result, trace

    if runtime is None:
        return _fallback("runtime_client_not_configured")

    try:
        response = await runtime.evaluate_privacy_context(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_id,
            runtime_turn_id=runtime_turn_id,
            surface_category=derived.surface_category,
            sensitivity_level=derived.sensitivity_level,
            sensitivity_domains=derived.sensitivity_domains,
        )
    except httpx.TimeoutException:
        return _fallback("runtime_timeout")
    except httpx.HTTPError:
        return _fallback("runtime_http_failure")
    except ValueError:
        return _fallback("invalid_runtime_result")
    except Exception:
        return _fallback("runtime_unavailable")

    result = validate_privacy_runtime_response(
        response,
        request_id=request_id,
        owner_id=owner_id,
        conversation_id=conversation_id,
        surface=surface,
        runtime_session_id=runtime_session_id,
        runtime_turn_id=runtime_turn_id,
        surface_category=derived.surface_category,
        sensitivity_level=derived.sensitivity_level,
    )
    if result is None:
        return _fallback("invalid_runtime_result")

    return result, {
        "attempted": True,
        "status": "included",
        "included": True,
        "runtime_call_status": "included",
        "policy_source": "runtime",
        "surface_type": result.get("surface_type"),
        "privacy_zone": result.get("privacy_zone"),
        "sensitivity_level": result.get("sensitivity_level"),
        "sensitivity_domain_count": len(derived.sensitivity_domains),
        "sensitive_detail_allowed": result.get("sensitive_detail_allowed"),
        "notification_detail_allowed": result.get("notification_detail_allowed"),
        "voice_detail_allowed": result.get("voice_detail_allowed"),
        "screen_detail_allowed": result.get("screen_detail_allowed"),
        "redaction_required": result.get("redaction_required"),
        "safe_summary_required": result.get("safe_summary_required"),
        "reason_codes": result.get("reason_codes", []),
        "fallback_applied": False,
        "fallback_reason": None,
        "enforcement_required": False,
        "action_taken": "none",
        "template_id": None,
        "sources_suppressed_count": 0,
        "trace_bundle_suppressed": False,
        "brief_text_suppressed": False,
    }


async def _reset_runtime_after_turn(
    *,
    runtime: Any | None,
    runtime_trace: dict[str, Any],
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
) -> None:
    if not runtime_trace.get("reset_after_turn"):
        return
    if runtime is None:
        runtime_trace["reset"] = {
            "attempted": False,
            "status": "failed",
            "error_type": "RuntimeClientNotConfigured",
        }
        return
    try:
        response = await runtime.reset(
            request_id=request_id,
            owner_id=owner_id,
            conversation_id=conversation_id,
            surface=surface,
            reason="reset_after_turn",
        )
        runtime_trace["reset"] = {
            "attempted": True,
            "status": "ok",
            "reset": bool(response.get("reset", False)),
        }
    except Exception as e:
        runtime_trace["reset"] = {
            "attempted": True,
            "status": "failed",
            "error_type": type(e).__name__,
        }


def _compute_signals(payload: dict[str, Any], retrieval_bundle: dict[str, Any]) -> dict[str, Any]:
    text = _extract_last_user_text(payload["messages"]) or ""
    code_like = any(token in text for token in ("```", "def ", "class ", "import ", "SELECT "))
    observed = retrieval_bundle.get("bundle", {}).get("observed_metadata", {})
    has_code = code_like or bool(observed.get("has_code_like_content"))
    return {
        "sensitivity": payload.get("sensitivity", "private"),
        "has_code": has_code,
        "model_override_present": bool(payload.get("model_override")),
    }


def _load_model_registry(path: str) -> dict[str, Any]:
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return data.get("models", {}) or {}
    except Exception:
        return {}


def _latency_rank(bucket: str) -> int:
    return {"fast": 0, "medium": 1, "slow": 2}.get(bucket, 3)


def _model_provider(model: str, registry: dict[str, Any], fallback_provider: str | None) -> str:
    info = registry.get(model, {}) if isinstance(registry, dict) else {}
    return str(info.get("provider") or fallback_provider or "cloud")


def _policy_pick_model(
    registry: dict[str, Any],
    *,
    provider: str | None,
    cost_mode: str | None,
    latency_mode: str | None,
) -> str | None:
    candidates = []
    for name, info in registry.items():
        if provider and info.get("provider") != provider:
            continue
        candidates.append((name, info))
    if not candidates:
        return None

    if latency_mode == "fast":
        candidates.sort(
            key=lambda x: (
                _latency_rank(str(x[1].get("avg_latency_bucket", ""))),
                float(x[1].get("cost_per_1k_tokens", 1e9)),
            )
        )
        return candidates[0][0]

    if cost_mode == "low":
        candidates.sort(
            key=lambda x: (
                float(x[1].get("cost_per_1k_tokens", 1e9)),
                _latency_rank(str(x[1].get("avg_latency_bucket", ""))),
            )
        )
        return candidates[0][0]

    candidates.sort(
        key=lambda x: (
            _latency_rank(str(x[1].get("avg_latency_bucket", ""))),
            float(x[1].get("cost_per_1k_tokens", 1e9)),
            x[0],
        )
    )
    return candidates[0][0]


async def _create_error_trace(
    *,
    memory_store: MemoryStoreClient,
    request_id: str,
    conversation_id: str,
    payload: dict[str, Any],
    profile: dict[str, Any],
    retrieval_bundle: dict[str, Any],
    last_user_text: str,
    route: dict[str, Any],
    selected_model: str | None,
    selected_provider: str | None,
    sensitivity_local_only: bool,
    profile_local_only: bool,
    effective_local_only: bool,
    override_requested: str | None,
    override_applied: bool,
    override_reason: str | None,
    failure_reason: str,
    started: float,
    fallback_used: bool = False,
    prompt_trace: dict[str, Any] | None = None,
    surface_presence_trace: dict[str, Any] | None = None,
    dsa_trace: dict[str, Any] | None = None,
    model_calls: list[dict[str, Any]] | None = None,
    model_call: dict[str, Any] | None = None,
    privacy_policy: dict[str, Any] | None = None,
    privacy_enforced: bool | None = None,
) -> None:
    enforce_privacy = (
        privacy_policy_requires_suppression(privacy_policy)
        if privacy_enforced is None
        else privacy_enforced
    )
    persisted_prompt_trace = prompt_trace or {}
    if enforce_privacy:
        persisted_prompt_trace = sanitize_prompt_trace_for_privacy(
            persisted_prompt_trace,
            retrieval_bundle,
        )
        persisted_prompt_trace.pop("retained_source_ids", None)
        prompt_budget = persisted_prompt_trace.get("prompt_budget")
        if isinstance(prompt_budget, dict):
            prompt_budget.pop("retained_source_ids", None)
    persisted_model_calls = (
        [_sanitize_model_call_for_privacy(call) for call in model_calls or []]
        if enforce_privacy
        else model_calls or []
    )
    persisted_model_call = (
        _sanitize_model_call_for_privacy(model_call)
        if enforce_privacy and isinstance(model_call, dict)
        else model_call
    )
    references = [] if enforce_privacy else _trace_references(retrieval_bundle)
    await memory_store.create_trace(
        request_id=request_id,
        payload={
            "request_id": request_id,
            "conversation_id": conversation_id,
            "owner_id": payload["owner_id"],
            "client_id": payload.get("client_id"),
            "surface": payload.get("surface", "unknown"),
            "profile": {
                "name": profile["profile_name"],
                "version": profile["profile_version"],
                "effective_profile_ref": profile["effective_profile_ref"],
            },
            "retrieval": {
                "query_present": bool(last_user_text),
                "bundle": (
                    restricted_retrieval_trace_summary(retrieval_bundle)
                    if enforce_privacy
                    else _trace_retrieval(retrieval_bundle)
                ),
                "prompt_assembly": {
                    **persisted_prompt_trace,
                    "surface_presence": apply_surface_presence_outcome(
                        surface_presence_trace,
                        fallback_active=fallback_used,
                        unavailable=True,
                    ),
                },
            },
            "prompt": _trace_prompt(persisted_prompt_trace),
            "router_decision": {
                "rule_id": route.get("rule_id"),
                "selected_model": selected_model,
                "provider": selected_provider,
                "rationale": route.get("rationale"),
                "fallbacks": route.get("fallbacks", []),
                "routing_contract": routing_trace_metadata(
                    sensitivity=payload.get("sensitivity", "private"),
                    request_local_only=sensitivity_local_only,
                    profile_local_only=profile_local_only,
                    effective_local_only=effective_local_only,
                    manual_override_requested=override_requested,
                    manual_override_applied=override_applied,
                    manual_override_rejection_reason=override_reason,
                    selected_model=selected_model,
                    selected_provider=selected_provider,
                    fallback_used=fallback_used,
                    failure_reason=failure_reason,
                ),
            },
            "manual_override": {
                "requested_model": override_requested,
                "applied": override_applied,
                "rejection_reason": override_reason,
            },
            "model_call": persisted_model_call
            or {
                "provider": selected_provider,
                "model": selected_model,
                "status": "failed",
                "latency_ms": None,
                "error_code": failure_reason,
            },
            "model_calls": persisted_model_calls,
            "fallback": {
                "triggered": fallback_used,
                "reason": "provider_error" if fallback_used else None,
            },
            "dsa": (
                persisted_prompt_trace.get("dsa", {})
                if enforce_privacy
                else dsa_trace or {"enabled": False, "called": False, "status": "disabled"}
            ),
            "artifacts": (
                {
                    "status": "omitted",
                    "artifact_count": len(
                        retrieval_bundle.get("bundle", {}).get("artifact_refs", [])
                    ),
                    "included_ids": [],
                    "source_reference_count": 0,
                    "reason": "privacy_suppressed",
                }
                if enforce_privacy
                else _trace_artifacts(retrieval_bundle)
            ),
            "references": references,
            "cost": {},
            "latency_ms": int((perf_counter() - started) * 1000),
            "status": "failed",
            "error": failure_reason,
            "created_at": datetime.now(UTC).isoformat(),
        },
    )


async def orchestrate_chat(
    *,
    payload: dict[str, Any],
    memory_store: MemoryStoreClient,
    litellm: LiteLLMClient,
    rules_path: str,
    model_registry_path: str,
    allow_manual_override: bool,
    request_id: str,
    runtime: Any | None = None,
    enable_runtime_overlays: bool = False,
    companion_policy_enabled: bool = False,
    interaction_governance_enabled: bool = False,
    persona_containment_enabled: bool = False,
    restraint_enabled: bool = False,
    memory_hygiene_enabled: bool = False,
    privacy_context_enabled: bool = False,
    capability_registry_enabled: bool = False,
    response_action_mode: str = "shadow",
    interrupt_policy_mode: str = "off",
    dsa: DataSourceAggregatorClient | None = None,
    dsa_enabled: bool = False,
    prompt_output_token_reserve: int = 2048,
    prompt_context_safety_margin: int = 256,
    capability_revalidators: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = perf_counter()
    surface = payload.get("surface", "unknown")

    resolved = await memory_store.resolve_conversation(
        owner_id=payload["owner_id"],
        client_id=payload.get("client_id"),
    )
    conversation_id = payload.get("conversation_id") or resolved["conversation_id"]

    last_user_text = _extract_last_user_text(payload["messages"])
    recent_messages = _bounded_recent_messages(payload["messages"])
    surface_context = payload.get("surface_context")
    surface_metadata_json = surface_context if isinstance(surface_context, dict) else None
    active_mode = (
        surface_context.get("active_mode")
        if isinstance(surface_context, dict) and isinstance(surface_context.get("active_mode"), str)
        else None
    )
    surface_session_id = (
        payload.get("surface_session_id")
        if isinstance(payload.get("surface_session_id"), str)
        else None
    )

    last_user_message_id = None
    turn_response: dict[str, Any] | None = None
    turn_state_trace: dict[str, Any] = _turn_state_disabled_trace()
    runtime_session_trace: dict[str, Any] = _runtime_session_disabled_trace()
    interaction_governance: dict[str, Any] | None = None
    interaction_governance_trace: dict[str, Any] = _interaction_governance_disabled_trace()
    persona_containment: dict[str, Any] | None = None
    persona_containment_trace: dict[str, Any] = _persona_containment_disabled_trace()
    restraint: dict[str, Any] | None = None
    restraint_trace: dict[str, Any] = _restraint_disabled_trace()
    capability_registry_messages: list[dict[str, str]] = []
    capability_registry_trace: dict[str, Any] = _capability_registry_disabled_trace()
    mandatory_policy = MandatoryRetrievalPolicy(
        containment_policy=None,
        relationship_context=None,
        relationship_trace=_relationship_context_disabled_trace(),
        validation_trace={
            "mandatory_containment_requested": False,
            "policy_validation_status": "not_requested",
        },
    )
    turn_policy_metadata: dict[str, Any] | None = None
    turn_policy_omission_reason: str | None = "mandatory_containment_not_requested"
    result_boundary_trace: dict[str, Any] = {
        "enforcement_mode": "not_applicable",
        "validation_status": "not_applied",
        "envelope_validation_failed": False,
        "input_counts": {"recent": 0, "semantic": 0, "artifact_refs": 0},
        "retained_counts": {"recent": 0, "semantic": 0, "artifact_refs": 0},
        "omission_counts_by_reason": {},
        "relationship_policy_applied": False,
        "artifact_policy_applied": False,
        "post_budget_survivor_filter_removed_sources": False,
    }

    if persona_containment_enabled:
        runtime_session, runtime_session_trace = await _resolve_runtime_session(
            runtime=runtime,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
        )
        (
            interaction_governance,
            interaction_governance_trace,
        ) = await _resolve_interaction_governance(
            runtime=runtime,
            enabled=interaction_governance_enabled,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            runtime_turn_id=None,
            surface_session_id=surface_session_id,
            active_mode=active_mode,
            current_user_text=last_user_text,
            recent_messages=recent_messages,
            surface_metadata_json=surface_metadata_json,
        )
        persona_containment, persona_containment_trace = await _resolve_persona_containment(
            runtime=runtime,
            enabled=True,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            runtime_turn_id=None,
            persona_scope_hint=(
                interaction_governance.get("persona_scope_hint")
                if isinstance(interaction_governance, dict)
                else None
            ),
            interaction_kind=(
                interaction_governance.get("interaction_kind")
                if isinstance(interaction_governance, dict)
                else None
            ),
            current_user_text=last_user_text,
            recent_messages=recent_messages,
            surface_metadata_json=surface_metadata_json,
        )
        mandatory_policy = await _resolve_mandatory_retrieval_policy(
            runtime=runtime,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            persona_containment=persona_containment,
        )
        persona_containment_trace.update(mandatory_policy.validation_trace)
        restraint, restraint_trace = await _resolve_restraint(
            runtime=runtime,
            enabled=restraint_enabled,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            runtime_turn_id=None,
            interaction_kind=(
                interaction_governance.get("interaction_kind")
                if isinstance(interaction_governance, dict)
                else None
            ),
            response_posture=(
                interaction_governance.get("response_posture")
                if isinstance(interaction_governance, dict)
                else None
            ),
            active_persona_id=(
                persona_containment.get("active_persona_id")
                if isinstance(persona_containment, dict)
                else None
            ),
            capability_domain=(
                persona_containment.get("capability_domain")
                if isinstance(persona_containment, dict)
                else None
            ),
            current_user_text=last_user_text,
            recent_messages=recent_messages,
            surface_metadata_json=surface_metadata_json,
        )
        relationship_projection = (
            mandatory_policy.containment_policy.get("relationship_scope_projection")
            if isinstance(mandatory_policy.containment_policy, dict)
            else None
        )
        turn_policy_metadata, turn_policy_omission_reason = _classify_turn_policy_metadata(
            persona_containment=persona_containment,
            relationship_projection=relationship_projection,
            request_sensitivity=payload.get("sensitivity"),
            interaction_governance=interaction_governance,
        )

    for msg in payload["messages"]:
        if msg["role"] == "user":
            saved = await memory_store.add_message(
                conversation_id=conversation_id,
                owner_id=payload["owner_id"],
                role="user",
                content=msg["content"],
                client_id=payload.get("client_id"),
                metadata={"surface": surface},
                policy_metadata=turn_policy_metadata,
            )
            last_user_message_id = saved.get("message_id") if isinstance(saved, dict) else None

    turn_response, turn_state_trace = await _start_runtime_turn(
        runtime=runtime,
        request_id=request_id,
        owner_id=payload["owner_id"],
        conversation_id=conversation_id,
        surface=surface,
        input_message_id=last_user_message_id,
    )
    if not persona_containment_enabled:
        runtime_session = (
            turn_response.get("runtime_session") if isinstance(turn_response, dict) else None
        )
        runtime_session_trace = _runtime_session_trace_from_session(
            runtime_session,
            attempted=bool(turn_state_trace.get("attempted")),
            omission_reason=turn_state_trace.get(
                "omission_reason",
                "runtime_session_missing_from_turn_response",
            ),
            error_type=turn_state_trace.get("error_type"),
        )
        (
            interaction_governance,
            interaction_governance_trace,
        ) = await _resolve_interaction_governance(
            runtime=runtime,
            enabled=interaction_governance_enabled,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            runtime_turn_id=turn_state_trace.get("runtime_turn_id"),
            surface_session_id=surface_session_id,
            active_mode=active_mode,
            current_user_text=last_user_text,
            recent_messages=recent_messages,
            surface_metadata_json=surface_metadata_json,
        )
        persona_containment, persona_containment_trace = await _resolve_persona_containment(
            runtime=runtime,
            enabled=False,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            runtime_turn_id=turn_state_trace.get("runtime_turn_id"),
            persona_scope_hint=None,
            interaction_kind=None,
            current_user_text=last_user_text,
            recent_messages=recent_messages,
            surface_metadata_json=surface_metadata_json,
        )
        restraint, restraint_trace = await _resolve_restraint(
            runtime=runtime,
            enabled=restraint_enabled,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            runtime_turn_id=turn_state_trace.get("runtime_turn_id"),
            interaction_kind=(
                interaction_governance.get("interaction_kind")
                if isinstance(interaction_governance, dict)
                else None
            ),
            response_posture=(
                interaction_governance.get("response_posture")
                if isinstance(interaction_governance, dict)
                else None
            ),
            active_persona_id=None,
            capability_domain=None,
            current_user_text=last_user_text,
            recent_messages=recent_messages,
            surface_metadata_json=surface_metadata_json,
        )

    profile = await memory_store.resolve_profile(
        owner_id=payload["owner_id"],
        surface=surface,
        requested_profile=payload.get("requested_profile"),
        client_id=payload.get("client_id"),
    )

    effective_payload = apply_profile_to_request(profile, payload)
    style_envelope, style_trace = resolve_style_envelope(effective_payload, profile)
    style_guidance = build_style_guidance_block(style_envelope, style_trace)
    response_shape, response_shape_trace = resolve_response_shape(
        effective_payload,
        style_envelope,
        style_trace,
    )
    response_shape_guidance = build_response_shape_guidance_block(
        response_shape, response_shape_trace
    )
    surface_presence_trace = resolve_surface_presence(effective_payload, response_shape)
    routing_policy = profile.get("routing_policy", {}) or {}
    sensitivity_local_only = effective_payload.get("sensitivity") == "local_only"
    profile_local_only = bool(routing_policy.get("local_only", False))
    local_only = sensitivity_local_only or profile_local_only
    cost_mode = routing_policy.get("cost_mode")
    latency_mode = routing_policy.get("latency_mode")
    retrieval_boundary = _apply_persona_containment_retrieval_boundary(
        retrieval=(
            effective_payload.get("retrieval")
            if isinstance(effective_payload.get("retrieval"), dict)
            else None
        ),
        persona_containment=persona_containment,
        persona_containment_trace=persona_containment_trace,
    )
    external_context_request = effective_payload.get("external_context")
    external_context_enabled = bool(
        effective_payload.get("external_context_enabled", False)
    ) or bool(
        isinstance(external_context_request, dict)
        and external_context_request.get("enabled") is True
    )
    memory_hygiene_result = None
    privacy_context = None
    privacy_context_trace = _privacy_context_disabled_trace()

    try:
        await _advance_runtime_turn(
            runtime=runtime,
            turn_state_trace=turn_state_trace,
            request_id=request_id,
            turn_status="retrieving",
        )
        suppression_reason = _restraint_suppression_reason(restraint)
        dependency_failure_reason = None
        if persona_containment_enabled and mandatory_policy.containment_policy is None:
            dependency_failure_reason = (
                mandatory_policy.validation_trace.get("policy_validation_reason")
                or "mandatory_containment_unavailable"
            )
        retrieval_dispatch_trace = {
            "mandatory_containment_requested": bool(persona_containment_enabled),
            "policy_validation_status": mandatory_policy.validation_trace.get(
                "policy_validation_status"
            ),
            "bms_retrieval_call_issued": False,
            "bms_retrieval_call_suppressed": False,
            "suppression_or_dependency_reason": None,
            "relationship_projection_applied": mandatory_policy.relationship_trace.get(
                "retrieval_scope_projection_applied"
            ),
            "relationship_id_count": mandatory_policy.relationship_trace.get(
                "relationship_id_count",
                0,
            ),
            "entity_id_count": mandatory_policy.relationship_trace.get("entity_id_count", 0),
            "relationship_scope_count": mandatory_policy.relationship_trace.get(
                "relationship_scope_count",
                0,
            ),
            "relationship_scope_projection": (
                mandatory_policy.containment_policy.get("relationship_scope_projection")
                if isinstance(mandatory_policy.containment_policy, dict)
                else None
            ),
            "neutral_persistence_classification": (
                "applied" if turn_policy_metadata is not None else "omitted"
            ),
            "neutral_persistence_omission_reason": (
                None if turn_policy_metadata is not None else turn_policy_omission_reason
            ),
        }
        if suppression_reason or dependency_failure_reason:
            reason = suppression_reason or dependency_failure_reason or "retrieval_suppressed"
            retrieval_dispatch_trace.update(
                {
                    "bms_retrieval_call_suppressed": True,
                    "suppression_or_dependency_reason": reason,
                }
            )
            _set_artifact_result_trace(
                persona_trace=persona_containment_trace,
                status="not_requested",
                reason=reason,
            )
            if suppression_reason:
                restraint_trace["retrieval_enforcement_status"] = "suppressed"
                restraint_trace["retrieval_enforcement_reason"] = suppression_reason
            retrieval_bundle = _empty_retrieval_bundle(
                request_id=request_id,
                conversation_id=conversation_id,
                reason=reason,
            )
        else:
            retrieve_bundle_kwargs: dict[str, Any] = {
                "request_id": request_id,
                "conversation_id": conversation_id,
                "owner_id": payload["owner_id"],
                "query": last_user_text,
                "retrieval": retrieval_boundary.retrieval,
                "include_artifacts": (
                    None if persona_containment_enabled else retrieval_boundary.include_artifacts
                ),
            }
            if persona_containment_enabled:
                retrieve_bundle_kwargs["containment_policy"] = mandatory_policy.containment_policy
            else:
                if retrieval_boundary.allowed_memory_domains is not None:
                    retrieve_bundle_kwargs["allowed_memory_domains"] = (
                        retrieval_boundary.allowed_memory_domains
                    )
                if retrieval_boundary.blocked_memory_domains is not None:
                    retrieve_bundle_kwargs["blocked_memory_domains"] = (
                        retrieval_boundary.blocked_memory_domains
                    )

            retrieval_bundle = await memory_store.retrieve_bundle(
                **retrieve_bundle_kwargs,
            )
            retrieval_dispatch_trace["bms_retrieval_call_issued"] = True
            if (
                not isinstance(retrieval_bundle, dict)
                and mandatory_policy.containment_policy is None
            ):
                raise RuntimeError("malformed_retrieval_response")
            retrieval_bundle, result_boundary_trace = _apply_persona_containment_result_boundary(
                retrieval_bundle=retrieval_bundle,
                request_id=request_id,
                conversation_id=conversation_id,
                owner_id=payload["owner_id"],
                retrieval=retrieval_boundary.retrieval,
                containment_policy=mandatory_policy.containment_policy,
                relationship_projection=(
                    mandatory_policy.containment_policy.get("relationship_scope_projection")
                    if isinstance(mandatory_policy.containment_policy, dict)
                    else None
                ),
            )
            if (
                persona_containment_enabled
                and result_boundary_trace.get("validation_status") == "failed_closed"
            ):
                failure_reasons = result_boundary_trace.get("omission_counts_by_reason")
                failure_reason = None
                if isinstance(failure_reasons, dict):
                    failure_reason = next(iter(failure_reasons), None)
                _set_artifact_result_trace(
                    persona_trace=persona_containment_trace,
                    status="failed_closed",
                    reason=(
                        _sanitize_trace_string(failure_reason, max_length=80)
                        or "mandatory_artifact_result_boundary_failed_closed"
                    ),
                )
            elif persona_containment_enabled and isinstance(retrieval_bundle, dict):
                _set_artifact_result_trace(
                    persona_trace=persona_containment_trace,
                    status="validated",
                    reason="mandatory_artifact_result_boundary_applied",
                )
        memory_hygiene_result = await apply_memory_hygiene(
            runtime=runtime,
            enabled=memory_hygiene_enabled,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            runtime_turn_id=turn_state_trace.get("runtime_turn_id"),
            retrieval_bundle=retrieval_bundle,
        )
        retrieval_bundle = memory_hygiene_result.retrieval_bundle
        external_context_pack, dsa_trace = await _resolve_external_context(
            dsa=dsa,
            dsa_enabled=dsa_enabled,
            external_context_enabled=external_context_enabled,
            external_context=(
                external_context_request if isinstance(external_context_request, dict) else None
            ),
            external_calls_allowed=not local_only,
            query=last_user_text,
        )
        companion_overlays, companion_trace = await _resolve_companion_policy(
            runtime=runtime,
            enabled=companion_policy_enabled,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            requested_scene=payload.get("requested_scene"),
        )
        recall_context = _recall_context(
            surface=surface,
            sensitivity=effective_payload.get("sensitivity", "private"),
            requested_scene=payload.get("requested_scene"),
            interaction_governance=interaction_governance,
            companion_trace=companion_trace,
        )
        recall_response = None
        episode_response = None
        memory_recall_dependency_trace: dict[str, Any] = {
            "recall_status": "not_requested",
            "episode_status": "not_requested",
        }
        recall_candidates = build_recall_candidates(retrieval_bundle)
        if recall_candidates and hasattr(memory_store, "select_recall"):
            try:
                recall_response = await memory_store.select_recall(
                    request_id=request_id,
                    owner_id=payload["owner_id"],
                    context=recall_context,
                    candidates=recall_candidates,
                )
                memory_recall_dependency_trace["recall_status"] = "included"
            except Exception:
                recall_response = None
                memory_recall_dependency_trace["recall_status"] = "dependency_unavailable"
                memory_recall_dependency_trace["recall_failure_policy"] = (
                    "no_additional_recall_context"
                )
        elif recall_candidates:
            memory_recall_dependency_trace["recall_status"] = "client_unavailable"
        if hasattr(memory_store, "retrieve_episode_callbacks"):
            try:
                episode_response = await memory_store.retrieve_episode_callbacks(
                    request_id=request_id,
                    owner_id=payload["owner_id"],
                    context=recall_context,
                    limit=10,
                )
                memory_recall_dependency_trace["episode_status"] = "included"
            except Exception:
                episode_response = None
                memory_recall_dependency_trace["episode_status"] = "dependency_unavailable"
                memory_recall_dependency_trace["episode_failure_policy"] = "no_episode_callbacks"
        else:
            memory_recall_dependency_trace["episode_status"] = "client_unavailable"
        memory_recall_composition = compose_memory_recall_context(
            retrieval_bundle=retrieval_bundle,
            recall_response=recall_response,
            episode_response=episode_response,
        )
        retrieval_bundle = memory_recall_composition.retrieval_bundle
        memory_recall_trace = {
            **memory_recall_composition.trace,
            "dependency": memory_recall_dependency_trace,
            "context": recall_context,
        }
        interrupt_trace = await _resolve_interrupt_policy(
            runtime=runtime,
            interrupt_policy_mode=interrupt_policy_mode,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            current_user_text=last_user_text,
            recent_messages=effective_payload["messages"],
            requested_scene=payload.get("requested_scene"),
        )
        runtime_identity, runtime_identity_trace = await _resolve_runtime_identity(
            runtime=runtime,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
        )
        world_state, world_state_trace = await _resolve_world_state(
            runtime=runtime,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            active_persona_id=runtime_identity_trace.get("active_persona_id"),
        )
        if persona_containment_enabled:
            relationship_context = mandatory_policy.relationship_context
            relationship_context_trace = mandatory_policy.relationship_trace
        else:
            relationship_context, relationship_context_trace = await _resolve_relationship_context(
                runtime=runtime,
                request_id=request_id,
                owner_id=payload["owner_id"],
                conversation_id=conversation_id,
                surface=surface,
                runtime_session_id=runtime_session_trace.get("runtime_session_id"),
                active_persona_id=runtime_identity_trace.get("active_persona_id"),
            )
        capability_registry_messages, capability_registry_trace = (
            await _resolve_capability_registry_context(
                runtime=runtime,
                enabled=capability_registry_enabled,
                request_id=request_id,
                owner_id=payload["owner_id"],
                conversation_id=conversation_id,
                surface=surface,
                runtime_session_id=runtime_session_trace.get("runtime_session_id"),
                runtime_turn_id=turn_state_trace.get("runtime_turn_id"),
                active_persona_id=runtime_identity_trace.get("active_persona_id"),
                current_user_text=last_user_text,
                interaction_governance_trace=interaction_governance_trace,
            )
        )
        runtime_overlay, runtime_trace = await _resolve_runtime_overlay(
            runtime=runtime,
            enable_runtime_overlays=enable_runtime_overlays,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
        )
        privacy_context, privacy_context_trace = await _resolve_privacy_context(
            runtime=runtime,
            enabled=privacy_context_enabled,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            runtime_turn_id=turn_state_trace.get("runtime_turn_id"),
            payload=effective_payload,
            retrieval_bundle=retrieval_bundle,
            external_context_pack=external_context_pack,
            runtime_identity=runtime_identity,
            runtime_overlay=runtime_overlay,
            world_state=world_state,
            relationship_context=relationship_context,
        )
        privacy_prompt_suppressed = privacy_context_enabled and privacy_policy_requires_suppression(
            privacy_context
        )
        provider_retrieval_bundle = (
            _empty_retrieval_bundle(
                request_id=request_id,
                conversation_id=conversation_id,
                reason="privacy_prompt_suppression",
            )
            if privacy_prompt_suppressed
            else retrieval_bundle
        )
        provider_memory_recall_messages = (
            [] if privacy_prompt_suppressed else memory_recall_composition.prompt_messages
        )
        provider_memory_recall_trace = (
            _privacy_safe_memory_recall_trace(memory_recall_trace)
            if privacy_prompt_suppressed
            else memory_recall_trace
        )
        signals = _compute_signals(effective_payload, retrieval_bundle)
        registry = _load_model_registry(model_registry_path)

        override_requested = effective_payload.get("model_override")
        override = override_requested if allow_manual_override else None
        override_reason = None
        if override_requested and not allow_manual_override:
            override_reason = "disabled"
        if override and local_only and _model_provider(override, registry, None) != "local":
            override = None
            override_reason = "rejected_local_only"

        route = evaluate_route(
            rules_path=rules_path,
            model_registry_path=model_registry_path,
            signals=signals,
            model_override=override,
        )

        selected_model = route["selected_model"]
        selected_provider = _model_provider(selected_model, registry, route.get("provider"))

        if local_only and selected_provider != "local":
            local_candidate = _policy_pick_model(
                registry,
                provider="local",
                cost_mode=cost_mode,
                latency_mode=latency_mode,
            )
            if not local_candidate:
                await _create_error_trace(
                    memory_store=memory_store,
                    request_id=request_id,
                    conversation_id=conversation_id,
                    payload=effective_payload,
                    profile=profile,
                    retrieval_bundle=retrieval_bundle,
                    last_user_text=last_user_text,
                    route=route,
                    selected_model=selected_model,
                    selected_provider=selected_provider,
                    sensitivity_local_only=sensitivity_local_only,
                    profile_local_only=profile_local_only,
                    effective_local_only=local_only,
                    override_requested=override_requested,
                    override_applied=bool(override),
                    override_reason=override_reason,
                    failure_reason="no_local_model_available",
                    started=started,
                    prompt_trace={
                        "style": style_trace,
                        "response_shape": response_shape_trace,
                        "companion_policy": companion_trace,
                        "interaction_governance": interaction_governance_trace,
                        "persona_containment": persona_containment_trace,
                        "restraint": restraint_trace,
                        "retrieval_dispatch": retrieval_dispatch_trace,
                        "memory_hygiene": (
                            memory_hygiene_result.trace
                            if memory_hygiene_result is not None
                            else disabled_memory_hygiene_trace(retrieval_bundle)
                        ),
                        "privacy_context": privacy_context_trace,
                        "world_state": world_state_trace,
                        "relationship_context": _relationship_context_disabled_trace(),
                        "capability_registry": capability_registry_trace,
                        "runtime": runtime_trace,
                        "dsa": dsa_trace,
                        "memory_episode_recall_composition": provider_memory_recall_trace,
                    },
                    surface_presence_trace=surface_presence_trace,
                    dsa_trace=dsa_trace,
                    privacy_policy=privacy_context,
                )
                raise RuntimeError("local_only policy active but no local model available")
            selected_model = local_candidate
            selected_provider = "local"

        policy_candidate = _policy_pick_model(
            registry,
            provider=selected_provider,
            cost_mode=cost_mode,
            latency_mode=latency_mode,
        )
        if policy_candidate:
            selected_model = policy_candidate
            selected_provider = _model_provider(selected_model, registry, selected_provider)
        provider_attempt_plan = resolve_provider_attempt_plan(
            registry=registry,
            route=route,
            selected_model=selected_model,
            selected_provider=selected_provider,
            local_only=local_only,
            cost_mode=cost_mode,
            latency_mode=latency_mode,
            policy_pick_model=_policy_pick_model,
            model_provider=_model_provider,
        )

        status = "ok"
        fallback_used = False
        model_error = None
        model_calls: list[dict[str, Any]] = []
        if capability_registry_enabled:
            capability_descriptors = []
            capability_exposure_trace = {
                "schema_version": "capability-exposure.v1",
                "status": "not_requested",
                "reason": "registry_context_only",
                "descriptor_count": 0,
                "descriptor_fingerprint": None,
                "exposed_capability_ids": [],
                "blocked_capability_ids": [],
                "blocked_reasons": {},
            }
        else:
            (
                capability_descriptors,
                capability_exposure_trace,
            ) = await filter_capability_descriptors_for_exposure(
                runtime=runtime,
                request_id=request_id,
                owner_id=payload["owner_id"],
                conversation_id=conversation_id,
                surface=surface,
                runtime_session_id=runtime_session_trace.get("runtime_session_id"),
                runtime_turn_id=turn_state_trace.get("runtime_turn_id"),
                active_persona_id=runtime_identity_trace.get("active_persona_id"),
                selected_relationship_ids=_selected_relationship_ids_from_trace(
                    relationship_context_trace
                ),
            )

        handoff = build_assistant_handoff(
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            route=route,
            selected_model=selected_model,
            selected_provider=selected_provider,
            effective_local_only=local_only,
            manual_override_requested=override_requested,
            manual_override_applied=bool(override),
            manual_override_rejection_reason=override_reason,
            style_trace=style_trace,
            response_shape_trace=response_shape_trace,
            surface_presence_trace=surface_presence_trace,
            companion_overlays=companion_overlays,
            companion_trace=companion_trace,
            runtime_overlay=runtime_overlay,
            runtime_trace=runtime_trace,
            retrieval_query=last_user_text,
            retrieval_bundle=provider_retrieval_bundle,
            interrupt_trace=interrupt_trace,
        )

        presentation = build_companion_presentation(handoff)

        try:
            prompt = assemble_prompt(
                profile=profile,
                retrieval_bundle=provider_retrieval_bundle,
                current_messages=[*capability_registry_messages, *effective_payload["messages"]],
                handoff=handoff,
                presentation=presentation,
                style_guidance=style_guidance,
                style_trace=style_trace,
                response_shape_guidance=response_shape_guidance,
                response_shape_trace=response_shape_trace,
                surface_presence_trace=surface_presence_trace,
                companion_overlays=companion_overlays,
                companion_trace=companion_trace,
                interaction_governance=interaction_governance,
                interaction_governance_trace_data=interaction_governance_trace,
                persona_containment=persona_containment,
                persona_containment_trace_data=persona_containment_trace,
                restraint=restraint,
                restraint_trace_data=restraint_trace,
                memory_hygiene_trace_data=(
                    memory_hygiene_result.trace
                    if memory_hygiene_result is not None
                    else disabled_memory_hygiene_trace(retrieval_bundle)
                ),
                privacy_context=privacy_context,
                privacy_context_trace_data=privacy_context_trace,
                runtime_identity=runtime_identity,
                runtime_identity_trace=runtime_identity_trace,
                world_state=world_state,
                world_state_trace=world_state_trace,
                relationship_context=relationship_context,
                relationship_context_trace=relationship_context_trace,
                runtime_overlay=runtime_overlay,
                runtime_trace=runtime_trace,
                interrupt_trace=interrupt_trace,
                external_context_pack=external_context_pack,
                dsa_trace=dsa_trace,
                memory_recall_messages=provider_memory_recall_messages,
                memory_recall_trace=provider_memory_recall_trace,
                prompt_budget_contract=PromptBudgetContract(
                    attempts=provider_attempt_plan,
                    output_token_reserve=prompt_output_token_reserve,
                    context_safety_margin=prompt_context_safety_margin,
                    profile_prompt_budget=(
                        profile.get("prompt_budget") if isinstance(profile, dict) else None
                    ),
                ),
            )
        except PromptBudgetError as budget_error:
            truncation_applied = bool(budget_error.trace.get("omission_or_truncation_occurred"))
            truncation_reason = None
            if truncation_applied:
                truncation_reason = (
                    "optional_context_reduced"
                    if budget_error.trace.get("failure_reason")
                    else budget_error.trace.get("status")
                )
            budget_prompt_trace = {
                "prompt_budget": budget_error.trace,
                "truncation": {
                    "applied": truncation_applied,
                    "reason": truncation_reason,
                },
                "style": style_trace,
                "response_shape": response_shape_trace,
                "companion_policy": companion_trace,
                "interaction_governance": interaction_governance_trace,
                "persona_containment": persona_containment_trace,
                "result_boundary": result_boundary_trace,
                "restraint": restraint_trace,
                "retrieval_dispatch": retrieval_dispatch_trace,
                "memory_hygiene": (
                    memory_hygiene_result.trace
                    if memory_hygiene_result is not None
                    else disabled_memory_hygiene_trace(retrieval_bundle)
                ),
                "privacy_context": privacy_context_trace,
                "world_state": world_state_trace,
                "relationship_context": relationship_context_trace,
                "capability_registry": capability_registry_trace,
                "runtime": runtime_trace,
                "dsa": dsa_trace,
                "memory_episode_recall_composition": provider_memory_recall_trace,
                "message_count": 0,
            }
            await _create_error_trace(
                memory_store=memory_store,
                request_id=request_id,
                conversation_id=conversation_id,
                payload=effective_payload,
                profile=profile,
                retrieval_bundle=retrieval_bundle,
                last_user_text=last_user_text,
                route=route,
                selected_model=selected_model,
                selected_provider=selected_provider,
                sensitivity_local_only=sensitivity_local_only,
                profile_local_only=profile_local_only,
                effective_local_only=local_only,
                override_requested=override_requested,
                override_applied=bool(override),
                override_reason=override_reason,
                failure_reason=budget_error.reason,
                started=started,
                prompt_trace=budget_prompt_trace,
                surface_presence_trace=surface_presence_trace,
                dsa_trace=dsa_trace,
                privacy_policy=privacy_context,
            )
            await _complete_runtime_turn(
                runtime=runtime,
                turn_state_trace=turn_state_trace,
                request_id=request_id,
                turn_status="abandoned",
            )
            raise RuntimeError(budget_error.reason) from budget_error
        messages = prompt.messages
        prompt.trace["capability_registry"] = capability_registry_trace
        prompt.trace["retrieval_dispatch"] = retrieval_dispatch_trace
        prompt.trace["result_boundary"] = result_boundary_trace
        _apply_post_budget_survivor_trace(
            result_boundary_trace=result_boundary_trace,
            retrieval_bundle=retrieval_bundle,
            prompt_trace=prompt.trace,
        )
        prompt_fingerprint = _prompt_fingerprint(messages)
        prompt.trace["provider_prompt"] = {
            **prompt_fingerprint,
            "rebuilt_between_attempts": False,
        }
        prompt.trace["capabilities"] = {
            "exposure": capability_exposure_trace,
            "validation": {
                "validation_status": "not_requested",
                "schema_version": capability_exposure_trace.get("schema_version"),
            },
            "follow_up": _capability_follow_up_empty_trace(),
            "fallback": _capability_fallback_trace(
                descriptor_fingerprint_value=capability_exposure_trace.get("descriptor_fingerprint")
            ),
            "dispatch_completed": False,
            "executor_call_count": 0,
        }

        await _advance_runtime_turn(
            runtime=runtime,
            turn_state_trace=turn_state_trace,
            request_id=request_id,
            turn_status="responding",
        )
        model_started = perf_counter()
        try:
            completion = await litellm.chat(
                request_id=request_id,
                model=selected_model,
                messages=messages,
                tools=capability_descriptors,
            )
            model_calls.append(
                {
                    **_model_attempt(
                        provider=selected_provider,
                        model=selected_model,
                        status="ok",
                        latency_ms=int((perf_counter() - model_started) * 1000),
                    ),
                    **_provider_attempt_evidence(
                        ordinal=1,
                        prompt_fingerprint=prompt_fingerprint,
                        prompt_trace=prompt.trace,
                    ),
                    "capability_descriptor_fingerprint": capability_exposure_trace.get(
                        "descriptor_fingerprint"
                    ),
                    "capability_descriptor_count": capability_exposure_trace.get(
                        "descriptor_count"
                    ),
                }
            )
        except Exception as e:
            model_calls.append(
                {
                    **_model_attempt(
                        provider=selected_provider,
                        model=selected_model,
                        status="failed",
                        latency_ms=int((perf_counter() - model_started) * 1000),
                        error=e,
                    ),
                    **_provider_attempt_evidence(
                        ordinal=1,
                        prompt_fingerprint=prompt_fingerprint,
                        prompt_trace=prompt.trace,
                    ),
                    "capability_descriptor_fingerprint": capability_exposure_trace.get(
                        "descriptor_fingerprint"
                    ),
                    "capability_descriptor_count": capability_exposure_trace.get(
                        "descriptor_count"
                    ),
                }
            )
            fallback_attempt = provider_attempt_plan[1] if len(provider_attempt_plan) > 1 else None
            if fallback_attempt:
                fallback_used = True
                status = "degraded"
                prompt.trace["provider_fallback_context"] = {
                    "same_sanitized_messages_reused": True,
                    "prompt_fingerprint": prompt_fingerprint["fingerprint"],
                    "message_count": prompt_fingerprint["message_count"],
                }
                fallback_fingerprint = capability_exposure_trace.get("descriptor_fingerprint")
                prompt.trace["capabilities"]["fallback"].update(
                    {
                        "descriptor_fingerprint": fallback_fingerprint,
                        "same_descriptor_fingerprint": fallback_fingerprint
                        == capability_exposure_trace.get("descriptor_fingerprint"),
                    }
                )
                selected_model = fallback_attempt.model
                selected_provider = fallback_attempt.provider
                fallback_started = perf_counter()
                try:
                    completion = await litellm.chat(
                        request_id=request_id,
                        model=selected_model,
                        messages=messages,
                        tools=capability_descriptors,
                    )
                    model_calls.append(
                        {
                            **_model_attempt(
                                provider=selected_provider,
                                model=selected_model,
                                status="ok",
                                latency_ms=int((perf_counter() - fallback_started) * 1000),
                            ),
                            **_provider_attempt_evidence(
                                ordinal=2,
                                prompt_fingerprint=prompt_fingerprint,
                                prompt_trace=prompt.trace,
                            ),
                            "capability_descriptor_fingerprint": capability_exposure_trace.get(
                                "descriptor_fingerprint"
                            ),
                            "capability_descriptor_count": capability_exposure_trace.get(
                                "descriptor_count"
                            ),
                        }
                    )
                    model_error = model_calls[0].get("error_code")
                except Exception as fallback_error:
                    model_calls.append(
                        {
                            **_model_attempt(
                                provider=selected_provider,
                                model=selected_model,
                                status="failed",
                                latency_ms=int((perf_counter() - fallback_started) * 1000),
                                error=fallback_error,
                            ),
                            **_provider_attempt_evidence(
                                ordinal=2,
                                prompt_fingerprint=prompt_fingerprint,
                                prompt_trace=prompt.trace,
                            ),
                            "capability_descriptor_fingerprint": capability_exposure_trace.get(
                                "descriptor_fingerprint"
                            ),
                            "capability_descriptor_count": capability_exposure_trace.get(
                                "descriptor_count"
                            ),
                        }
                    )
                    final_attempt = dict(model_calls[-1])
                    await _create_error_trace(
                        memory_store=memory_store,
                        request_id=request_id,
                        conversation_id=conversation_id,
                        payload=effective_payload,
                        profile=profile,
                        retrieval_bundle=retrieval_bundle,
                        last_user_text=last_user_text,
                        route=route,
                        selected_model=selected_model,
                        selected_provider=selected_provider,
                        sensitivity_local_only=sensitivity_local_only,
                        profile_local_only=profile_local_only,
                        effective_local_only=local_only,
                        override_requested=override_requested,
                        override_applied=bool(override),
                        override_reason=override_reason,
                        failure_reason=final_attempt["error_code"],
                        started=started,
                        fallback_used=True,
                        prompt_trace=prompt.trace,
                        surface_presence_trace=surface_presence_trace,
                        dsa_trace=dsa_trace,
                        model_calls=model_calls,
                        model_call=final_attempt,
                        privacy_policy=privacy_context,
                    )
                    raise
            else:
                final_attempt = dict(model_calls[-1])
                await _create_error_trace(
                    memory_store=memory_store,
                    request_id=request_id,
                    conversation_id=conversation_id,
                    payload=effective_payload,
                    profile=profile,
                    retrieval_bundle=retrieval_bundle,
                    last_user_text=last_user_text,
                    route=route,
                    selected_model=selected_model,
                    selected_provider=selected_provider,
                    sensitivity_local_only=sensitivity_local_only,
                    profile_local_only=profile_local_only,
                    effective_local_only=local_only,
                    override_requested=override_requested,
                    override_applied=bool(override),
                    override_reason=override_reason,
                    failure_reason=final_attempt["error_code"],
                    started=started,
                    prompt_trace=prompt.trace,
                    surface_presence_trace=surface_presence_trace,
                    dsa_trace=dsa_trace,
                    model_calls=model_calls,
                    model_call=final_attempt,
                    privacy_policy=privacy_context,
                )
                await _complete_runtime_turn(
                    runtime=runtime,
                    turn_state_trace=turn_state_trace,
                    request_id=request_id,
                    turn_status="abandoned",
                )
                raise

        raw_answer = _apply_capability_registry_response_boundary(
            provider_text(completion),
            capability_registry_trace,
        )
        capability_request = None
        try:
            capability_request = parse_provider_capability_request(completion)
            if capability_request is not None:
                if capability_registry_trace.get("context_included") is True:
                    prompt.trace["capabilities"]["validation"] = {
                        "validation_status": "not_requested",
                        "reason_code": "registry_context_only",
                    }
                    prompt.trace["capabilities"]["execution"] = {
                        "executor_called": False,
                        "executor_call_count": 0,
                        "executor_result_status": "not_called",
                        "failure_reason_code": "registry_context_only",
                        "response_status": "not_executed",
                    }
                    prompt.trace["capabilities"]["follow_up"] = _capability_follow_up_empty_trace()
                    prompt.trace["capabilities"]["dispatch_completed"] = False
                    prompt.trace["capabilities"]["executor_call_count"] = 0
                    raw_answer = _capability_registry_forced_response(
                        capability_registry_trace
                    ) or "I found a registered capability, but I did not execute it."
                else:
                    validation_result = validate_and_digest_capability_request(
                        request=capability_request,
                        exposed_capability_ids=capability_exposure_trace.get(
                            "exposed_capability_ids",
                            [],
                        ),
                    )
                    prompt.trace["capabilities"]["validation"] = validation_result.trace
                    execution_result = await authorize_and_execute_capability(
                        runtime=runtime,
                        request_id=request_id,
                        owner_id=payload["owner_id"],
                        conversation_id=conversation_id,
                        surface=surface,
                        runtime_session_id=runtime_session_trace.get("runtime_session_id"),
                        runtime_turn_id=turn_state_trace.get("runtime_turn_id"),
                        active_persona_id=runtime_identity_trace.get("active_persona_id"),
                        validation_result=validation_result,
                        selected_relationship_ids=_selected_relationship_ids_from_trace(
                            relationship_context_trace
                        ),
                        revalidators=capability_revalidators,
                        capability_confirmation=payload.get("capability_confirmation"),
                    )
                    prompt.trace["capabilities"]["execution"] = execution_result.trace
                    executor_call_count = execution_result.trace.get("executor_call_count")
                    if isinstance(executor_call_count, int):
                        prompt.trace["capabilities"]["executor_call_count"] = executor_call_count
                    dispatch_completed = (
                        execution_result.trace.get("executor_called") is True
                        and execution_result.trace.get("executor_call_count") == 1
                    )
                    prompt.trace["capabilities"]["dispatch_completed"] = dispatch_completed
                    if dispatch_completed:
                        prompt.trace["capabilities"]["fallback"]["blocked_after_dispatch"] = True
                    if execution_result.trace.get("response_status") == "executed":
                        raw_answer, follow_up_trace = await _attempt_capability_follow_up(
                            litellm=litellm,
                            request_id=request_id,
                            model=selected_model,
                            execution_result=execution_result,
                        )
                        prompt.trace["capabilities"]["follow_up"] = follow_up_trace
                    else:
                        raw_answer = execution_result.response_text
            else:
                prompt.trace["capabilities"]["follow_up"] = _capability_follow_up_empty_trace()
                prompt.trace["capabilities"]["dispatch_completed"] = False
                prompt.trace["capabilities"]["executor_call_count"] = 0
        except CapabilityValidationError as exc:
            requested_capability_id = None
            requested_provider_tool_name = None
            if capability_request is not None:
                requested_capability_id = capability_request.capability_id
                requested_provider_tool_name = capability_request.provider_tool_name
            prompt.trace["capabilities"]["validation"] = capability_validation_failure_trace(
                exc.reason_code,
                requested_capability_id,
                requested_provider_tool_name,
            )
            prompt.trace["capabilities"]["execution"] = {
                "executor_called": False,
                "executor_call_count": 0,
                "executor_result_status": "not_called",
                "failure_reason_code": exc.reason_code,
                "response_status": "not_executed",
            }
            prompt.trace["capabilities"]["follow_up"] = _capability_follow_up_empty_trace()
            prompt.trace["capabilities"]["dispatch_completed"] = False
            prompt.trace["capabilities"]["executor_call_count"] = 0
            raw_answer = "I could not use that capability request safely."
        response_review = review_response(
            ResponseReviewInput(
                candidate_text=raw_answer,
                handoff=handoff,
                presentation=presentation,
                prompt_trace=prompt.trace,
            )
        )
        response_action = apply_response_action(
            ResponseActionInput(
                mode=response_action_mode,
                candidate_text=raw_answer,
                response_review=response_review,
            )
        )
        prompt.trace["response_review"] = response_review.to_trace()
        prompt.trace["response_action"] = response_action.to_trace()
        candidate_answer = response_action.candidate_text
        if memory_recall_composition.explicit_callbacks and not privacy_prompt_suppressed:
            callback_text = memory_recall_composition.explicit_callbacks[0]
            if callback_text and callback_text not in candidate_answer:
                candidate_answer = f"{callback_text}\n\n{candidate_answer}"
            prompt.trace["memory_episode_recall_composition"]["final_callback_applied"] = True
        else:
            prompt.trace["memory_episode_recall_composition"]["final_callback_applied"] = False
        answer = candidate_answer
        brief_metadata = {"enabled": False}
        if effective_payload.get("response_mode") == "brief":
            brief_grounding = _merge_brief_grounding(
                memory_recall_composition.brief_grounding,
                _external_context_brief_grounding(
                    context_pack=external_context_pack,
                    dsa_trace=dsa_trace,
                    prompt_trace=prompt.trace,
                ),
            )
            brief_result = generate_brief(
                content=candidate_answer,
                brief_type=effective_payload.get("brief_type", "general"),
                depth_level=effective_payload.get("brief_depth") or 1,
                surface=effective_payload.get("surface", payload.get("surface", "chat")),
                source="explicit_user_request",
                explicit_request=True,
                grounding=brief_grounding,
            )
            answer = brief_result.rendered
            brief_metadata = {
                **brief_result.debug,
                "raw_model_answer": raw_answer,
                "shaped_answer": answer,
            }

        retained_artifact_ids = set(
            (prompt.trace.get("retained_source_ids") or {}).get("artifact_ids") or []
        )
        artifact_refs_for_sources = retrieval_bundle.get("bundle", {}).get(
            "artifact_refs",
            [],
        )
        if retained_artifact_ids:
            artifact_refs_for_sources = [
                item
                for item in artifact_refs_for_sources
                if isinstance(item, dict) and item.get("artifact_id") in retained_artifact_ids
            ]
        elif prompt.trace.get("prompt_budget") and not (
            privacy_context_enabled and privacy_policy_requires_suppression(privacy_context)
        ):
            artifact_refs_for_sources = []
        answer_sources = _public_answer_sources(artifact_refs_for_sources)
        if privacy_context_enabled and privacy_context is not None:
            privacy_boundary = apply_privacy_boundary(
                policy=privacy_context,
                answer=answer,
                sources=answer_sources,
            )
        else:
            privacy_boundary = apply_privacy_boundary(
                policy={
                    "sensitive_detail_allowed": True,
                    "screen_detail_allowed": True,
                    "redaction_required": False,
                    "safe_summary_required": False,
                    "surface_type": "desktop_private",
                },
                answer=answer,
                sources=answer_sources,
            )
        answer = privacy_boundary.final_answer
        if privacy_boundary.enforced:
            answer_sources = []
            if brief_metadata.get("enabled") is True:
                brief_metadata = _privacy_safe_brief_metadata(brief_metadata)
                brief_metadata["text_suppressed"] = True
        prompt.trace["privacy_context"] = {
            **prompt.trace.get("privacy_context", privacy_context_trace),
            "enforcement_required": privacy_boundary.enforced,
            "action_taken": privacy_boundary.action_taken,
            "template_id": privacy_boundary.template_id,
            "sources_suppressed_count": privacy_boundary.sources_suppressed_count,
            "trace_bundle_suppressed": privacy_boundary.trace_bundle_suppressed,
            "brief_text_suppressed": privacy_boundary.brief_text_suppressed,
        }

        await memory_store.add_message(
            conversation_id=conversation_id,
            owner_id=payload["owner_id"],
            role="assistant",
            content=answer,
            client_id=payload.get("client_id"),
            metadata={"request_id": request_id, "selected_model": selected_model},
            policy_metadata=turn_policy_metadata,
        )
        prompt.trace["answer_persistence"] = {
            "assistant_message_persisted": True,
            "persistence_acknowledged": True,
            "persisted_role": "assistant",
            "neutral_policy_metadata": (
                "applied" if turn_policy_metadata is not None else "omitted"
            ),
            "neutral_policy_metadata_omission_reason": (
                None if turn_policy_metadata is not None else turn_policy_omission_reason
            ),
        }

        await _reset_runtime_after_turn(
            runtime=runtime,
            runtime_trace=runtime_trace,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
        )
        await _complete_runtime_turn(
            runtime=runtime,
            turn_state_trace=turn_state_trace,
            request_id=request_id,
            turn_status="completed",
        )
        prompt.trace["runtime"] = runtime_trace
        prompt.trace["runtime_session"] = runtime_session_trace
        prompt.trace["turn_state"] = turn_state_trace
        prompt.trace["runtime_identity"] = runtime_identity_trace
        prompt.trace["relationship_context"] = relationship_context_trace
        prompt.trace["retrieval_dispatch"] = retrieval_dispatch_trace
        prompt.trace["memory_hygiene"] = (
            memory_hygiene_result.trace
            if memory_hygiene_result is not None
            else disabled_memory_hygiene_trace(retrieval_bundle)
        )
        prompt.trace["surface_presence"] = apply_surface_presence_outcome(
            surface_presence_trace,
            fallback_active=fallback_used,
        )
        persisted_prompt_trace = (
            sanitize_prompt_trace_for_privacy(prompt.trace, retrieval_bundle)
            if privacy_boundary.enforced
            else prompt.trace
        )
        if privacy_boundary.enforced and isinstance(persisted_prompt_trace, dict):
            persisted_prompt_trace.pop("retained_source_ids", None)
            persisted_prompt_trace["memory_episode_recall_composition"] = (
                _privacy_safe_memory_recall_trace(
                    persisted_prompt_trace.get("memory_episode_recall_composition")
                )
            )
            for layer in persisted_prompt_trace.get("layers") or []:
                if (
                    isinstance(layer, dict)
                    and layer.get("name") == "memory_episode_recall_composition"
                ):
                    layer["metadata"] = _privacy_safe_memory_recall_trace(layer.get("metadata"))
            prompt_budget = persisted_prompt_trace.get("prompt_budget")
            if isinstance(prompt_budget, dict):
                prompt_budget.pop("retained_source_ids", None)
        persisted_dsa_trace = (
            persisted_prompt_trace.get("dsa", {}) if privacy_boundary.enforced else dsa_trace
        )
        persisted_model_calls = (
            [_sanitize_model_call_for_privacy(call) for call in model_calls]
            if privacy_boundary.enforced
            else model_calls
        )
        persisted_retrieval = {
            "query_present": bool(last_user_text),
            "bundle": (
                restricted_retrieval_trace_summary(retrieval_bundle)
                if privacy_boundary.enforced
                else _trace_retrieval(retrieval_bundle)
            ),
            "prompt_assembly": persisted_prompt_trace,
        }
        effective_model_call = {
            **persisted_model_calls[-1],
            "brief": {
                key: value
                for key, value in brief_metadata.items()
                if key not in {"raw_model_answer", "shaped_answer"}
            },
        }
        references = _trace_references(retrieval_bundle)

        await memory_store.create_trace(
            request_id=request_id,
            payload={
                "request_id": request_id,
                "conversation_id": conversation_id,
                "owner_id": payload["owner_id"],
                "client_id": payload.get("client_id"),
                "surface": surface,
                "profile": {
                    "name": profile["profile_name"],
                    "version": profile["profile_version"],
                    "effective_profile_ref": profile["effective_profile_ref"],
                },
                "retrieval": persisted_retrieval,
                "prompt": _trace_prompt(persisted_prompt_trace),
                "router_decision": {
                    "rule_id": route.get("rule_id"),
                    "selected_model": selected_model,
                    "provider": selected_provider,
                    "rationale": route.get("rationale"),
                    "fallbacks": route.get("fallbacks", []),
                    "routing_contract": routing_trace_metadata(
                        sensitivity=effective_payload.get("sensitivity", "private"),
                        request_local_only=sensitivity_local_only,
                        profile_local_only=profile_local_only,
                        effective_local_only=local_only,
                        manual_override_requested=override_requested,
                        manual_override_applied=bool(override),
                        manual_override_rejection_reason=override_reason,
                        selected_model=selected_model,
                        selected_provider=selected_provider,
                        fallback_used=fallback_used,
                    ),
                },
                "manual_override": {
                    "requested_model": override_requested,
                    "applied": bool(override),
                    "rejection_reason": override_reason,
                },
                "model_call": effective_model_call,
                "model_calls": persisted_model_calls,
                "fallback": {
                    "triggered": fallback_used,
                    "reason": "provider_error" if fallback_used else None,
                },
                "dsa": persisted_dsa_trace,
                "artifacts": (
                    {
                        "status": "omitted",
                        "artifact_count": len(
                            retrieval_bundle.get("bundle", {}).get("artifact_refs", [])
                        ),
                        "included_ids": [],
                        "source_reference_count": 0,
                        "reason": "privacy_suppressed",
                    }
                    if privacy_boundary.enforced
                    else _trace_artifacts(retrieval_bundle)
                ),
                "references": [] if privacy_boundary.enforced else references,
                "cost": {},
                "latency_ms": int((perf_counter() - started) * 1000),
                "status": status,
                "error": model_error,
                "created_at": datetime.now(UTC).isoformat(),
            },
        )

        return {
            "request_id": request_id,
            "conversation_id": conversation_id,
            "profile_name": profile["profile_name"],
            "selected_model": selected_model,
            "answer": answer,
            "status": status,
            "sources": answer_sources,
        }
    except Exception:
        if turn_state_trace.get("runtime_turn_id") and not turn_state_trace.get("completed"):
            await _complete_runtime_turn(
                runtime=runtime,
                turn_state_trace=turn_state_trace,
                request_id=request_id,
                turn_status="abandoned",
            )
        raise
