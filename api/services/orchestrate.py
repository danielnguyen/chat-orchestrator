from __future__ import annotations

import hashlib
import json
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
from services.companion_presentation import build_companion_presentation
from services.fallback import resolve_provider_attempt_plan
from services.memory_hygiene import apply_memory_hygiene, disabled_memory_hygiene_trace
from services.privacy_context import (
    apply_privacy_boundary,
    derive_privacy_context,
    disabled_privacy_trace,
    privacy_fallback_policy,
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


def _sanitize_memory_domain_list(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    cleaned = [item for item in value if isinstance(item, str) and item]
    return cleaned or None


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
    trace["artifact_request_status"] = "request_boundary_enforced"
    trace["artifact_request_reason"] = "artifact_search_disabled_under_containment_lock"
    return RetrievalBoundaryResult(
        retrieval=effective_retrieval,
        include_artifacts=False,
        allowed_memory_domains=allowed_memory_domains,
        blocked_memory_domains=blocked_memory_domains,
    )


def _apply_persona_containment_result_boundary(
    *,
    retrieval_bundle: dict[str, Any],
    persona_containment: dict[str, Any] | None,
    persona_containment_trace: dict[str, Any] | None,
) -> dict[str, Any]:
    trace = _ensure_persona_containment_trace_defaults(persona_containment_trace)
    if not _persona_containment_lock_active(persona_containment):
        return retrieval_bundle

    bundle = retrieval_bundle.get("bundle")
    if not isinstance(bundle, dict):
        trace["artifact_result_reason"] = "retrieval_bundle_missing"
        return retrieval_bundle

    artifact_refs = bundle.get("artifact_refs")
    if not isinstance(artifact_refs, list) or not artifact_refs:
        trace["artifact_result_reason"] = "no_artifact_results_returned"
        return retrieval_bundle

    sanitized_bundle = dict(bundle)
    sanitized_bundle["artifact_refs"] = []
    trace["artifact_result_status"] = "suppressed"
    trace["artifact_result_reason"] = "unexpected_artifact_results_omitted_under_containment_lock"
    trace["artifact_result_count_omitted"] = len(artifact_refs)
    return {**retrieval_bundle, "bundle": sanitized_bundle}


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


SAFE_DOCTRINE_CODE = re.compile(r"^[a-z0-9_.:-]{1,120}$")
SAFE_DOCTRINE_STATUS = re.compile(r"^[a-z0-9_.:-]{1,80}$")
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


def _sanitize_doctrine_reason_list(value: Any, *, limit: int = 20) -> list[str]:
    if not isinstance(value, list):
        return []
    reasons: list[str] = []
    for item in value:
        cleaned = _sanitize_doctrine_code(item)
        if cleaned:
            reasons.append(cleaned)
        if len(reasons) >= limit:
            break
    return list(dict.fromkeys(reasons))


def _sanitize_doctrine_counts(value: Any, *, limit: int = 20) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    counts: dict[str, int] = {}
    for key, raw_count in list(value.items())[:limit]:
        cleaned_key = _sanitize_doctrine_code(key, max_length=80)
        count = _sanitize_trace_int(raw_count, minimum=0, maximum=10000)
        if cleaned_key and count is not None:
            counts[cleaned_key] = count
    return counts


def _trace_doctrine_summary(retrieval_bundle: dict[str, Any]) -> dict[str, Any]:
    diagnostics = retrieval_bundle.get("diagnostics")
    if diagnostics is None:
        return {"diagnostics_status": "absent"}
    if not isinstance(diagnostics, dict):
        return {"diagnostics_status": "invalid"}

    summary: dict[str, Any] = {"diagnostics_status": "included"}
    contract_version = _sanitize_doctrine_status(diagnostics.get("contract_version"))
    mode = _sanitize_doctrine_status(diagnostics.get("mode"))
    status = _sanitize_doctrine_status(diagnostics.get("status"))
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

    reason_codes = _sanitize_doctrine_reason_list(diagnostics.get("reason_codes"))
    if reason_codes:
        summary["reason_codes"] = reason_codes
    fallback_reasons = _sanitize_doctrine_reason_list(diagnostics.get("fallback_reasons"))
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
            if value:
                validation_summary[key] = value
        for key in DOCTRINE_COUNT_KEYS:
            count = _sanitize_trace_int(validation.get(key), minimum=0, maximum=10000)
            if count is not None:
                validation_summary[key] = count
        state_counts = _sanitize_doctrine_counts(validation.get("derivative_state_counts"))
        if state_counts:
            validation_summary["derivative_state_counts"] = state_counts
        omission_reasons = _sanitize_doctrine_reason_list(
            validation.get("artifact_omission_reasons"),
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
        "token_accounting": {
            "status": (
                "estimated"
                if isinstance(trace.get("prompt_budget"), dict)
                else "estimate_unavailable"
            ),
            "budget_enforcement": (
                "enforced" if isinstance(trace.get("prompt_budget"), dict) else "not_enforced"
            ),
        },
        "prompt_budget": trace.get("prompt_budget", {}),
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
    private_keys = {
        "memory_hygiene",
        "provenance",
        "qualification_reasons",
        "source_checks",
    }
    public_sources: list[dict[str, Any]] = []
    for source in sources:
        if not isinstance(source, dict):
            continue
        public_sources.append(
            {
                key: value
                for key, value in source.items()
                if isinstance(key, str) and not key.startswith("_") and key not in private_keys
            }
        )
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
        "retrieval_debug": {
            "truth_qualification": (
                debug.get("truth_qualification")
                if isinstance(debug.get("truth_qualification"), dict)
                else {}
            )
        },
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
) -> None:
    references = _trace_references(retrieval_bundle)
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
                "bundle": _trace_retrieval(retrieval_bundle),
                "prompt_assembly": {
                    **(prompt_trace or {}),
                    "surface_presence": apply_surface_presence_outcome(
                        surface_presence_trace,
                        fallback_active=fallback_used,
                        unavailable=True,
                    ),
                },
            },
            "prompt": _trace_prompt(prompt_trace),
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
            "model_call": model_call
            or {
                "provider": selected_provider,
                "model": selected_model,
                "status": "failed",
                "latency_ms": None,
                "error_code": failure_reason,
            },
            "model_calls": model_calls or [],
            "fallback": {
                "triggered": fallback_used,
                "reason": "provider_error" if fallback_used else None,
            },
            "dsa": dsa_trace or {"enabled": False, "called": False, "status": "disabled"},
            "artifacts": _trace_artifacts(retrieval_bundle),
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
    response_action_mode: str = "shadow",
    interrupt_policy_mode: str = "off",
    dsa: DataSourceAggregatorClient | None = None,
    dsa_enabled: bool = False,
    prompt_output_token_reserve: int = 2048,
    prompt_context_safety_margin: int = 256,
) -> dict[str, Any]:
    started = perf_counter()
    surface = payload.get("surface", "unknown")

    resolved = await memory_store.resolve_conversation(
        owner_id=payload["owner_id"],
        client_id=payload.get("client_id"),
    )
    conversation_id = payload.get("conversation_id") or resolved["conversation_id"]

    # Persist incoming user messages first.
    last_user_message_id = None
    for msg in payload["messages"]:
        if msg["role"] == "user":
            saved = await memory_store.add_message(
                conversation_id=conversation_id,
                owner_id=payload["owner_id"],
                role="user",
                content=msg["content"],
                client_id=payload.get("client_id"),
                metadata={"surface": surface},
            )
            last_user_message_id = saved.get("message_id") if isinstance(saved, dict) else None

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

    turn_response, turn_state_trace = await _start_runtime_turn(
        runtime=runtime,
        request_id=request_id,
        owner_id=payload["owner_id"],
        conversation_id=conversation_id,
        surface=surface,
        input_message_id=last_user_message_id,
    )
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
    interaction_governance, interaction_governance_trace = await _resolve_interaction_governance(
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
        enabled=persona_containment_enabled,
        request_id=request_id,
        owner_id=payload["owner_id"],
        conversation_id=conversation_id,
        surface=surface,
        runtime_session_id=runtime_session_trace.get("runtime_session_id"),
        runtime_turn_id=turn_state_trace.get("runtime_turn_id"),
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
        retrieve_bundle_kwargs: dict[str, Any] = {
            "request_id": request_id,
            "conversation_id": conversation_id,
            "owner_id": payload["owner_id"],
            "query": last_user_text,
            "retrieval": retrieval_boundary.retrieval,
            "include_artifacts": retrieval_boundary.include_artifacts,
        }
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
        if not isinstance(retrieval_bundle, dict):
            raise RuntimeError("malformed_retrieval_response")
        retrieval_bundle = _apply_persona_containment_result_boundary(
            retrieval_bundle=retrieval_bundle,
            persona_containment=persona_containment,
            persona_containment_trace=persona_containment_trace,
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
        relationship_context, relationship_context_trace = await _resolve_relationship_context(
            runtime=runtime,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=surface,
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            active_persona_id=runtime_identity_trace.get("active_persona_id"),
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
                        "memory_hygiene": (
                            memory_hygiene_result.trace
                            if memory_hygiene_result is not None
                            else disabled_memory_hygiene_trace(retrieval_bundle)
                        ),
                        "privacy_context": privacy_context_trace,
                        "world_state": world_state_trace,
                        "relationship_context": _relationship_context_disabled_trace(),
                        "runtime": runtime_trace,
                        "dsa": dsa_trace,
                    },
                    surface_presence_trace=surface_presence_trace,
                    dsa_trace=dsa_trace,
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
            retrieval_bundle=retrieval_bundle,
            interrupt_trace=interrupt_trace,
        )

        presentation = build_companion_presentation(handoff)

        try:
            prompt = assemble_prompt(
                profile=profile,
                retrieval_bundle=retrieval_bundle,
                current_messages=effective_payload["messages"],
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
                "restraint": restraint_trace,
                "memory_hygiene": (
                    memory_hygiene_result.trace
                    if memory_hygiene_result is not None
                    else disabled_memory_hygiene_trace(retrieval_bundle)
                ),
                "privacy_context": privacy_context_trace,
                "world_state": world_state_trace,
                "relationship_context": relationship_context_trace,
                "runtime": runtime_trace,
                "dsa": dsa_trace,
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
            )
            await _complete_runtime_turn(
                runtime=runtime,
                turn_state_trace=turn_state_trace,
                request_id=request_id,
                turn_status="abandoned",
            )
            raise RuntimeError(budget_error.reason) from budget_error
        messages = prompt.messages
        prompt_fingerprint = _prompt_fingerprint(messages)
        prompt.trace["provider_prompt"] = {
            **prompt_fingerprint,
            "rebuilt_between_attempts": False,
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
            )
            model_calls.append(
                _model_attempt(
                    provider=selected_provider,
                    model=selected_model,
                    status="ok",
                    latency_ms=int((perf_counter() - model_started) * 1000),
                )
            )
        except Exception as e:
            model_calls.append(
                _model_attempt(
                    provider=selected_provider,
                    model=selected_model,
                    status="failed",
                    latency_ms=int((perf_counter() - model_started) * 1000),
                    error=e,
                )
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
                selected_model = fallback_attempt.model
                selected_provider = fallback_attempt.provider
                fallback_started = perf_counter()
                try:
                    completion = await litellm.chat(
                        request_id=request_id,
                        model=selected_model,
                        messages=messages,
                    )
                    model_calls.append(
                        _model_attempt(
                            provider=selected_provider,
                            model=selected_model,
                            status="ok",
                            latency_ms=int((perf_counter() - fallback_started) * 1000),
                        )
                    )
                    model_error = model_calls[0].get("error_code")
                except Exception as fallback_error:
                    model_calls.append(
                        _model_attempt(
                            provider=selected_provider,
                            model=selected_model,
                            status="failed",
                            latency_ms=int((perf_counter() - fallback_started) * 1000),
                            error=fallback_error,
                        )
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
                )
                await _complete_runtime_turn(
                    runtime=runtime,
                    turn_state_trace=turn_state_trace,
                    request_id=request_id,
                    turn_status="abandoned",
                )
                raise

        raw_answer = completion["choices"][0]["message"]["content"]
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
        answer = candidate_answer
        brief_metadata = {"enabled": False}
        if effective_payload.get("response_mode") == "brief":
            brief_result = generate_brief(
                content=candidate_answer,
                brief_type=effective_payload.get("brief_type", "general"),
                depth_level=effective_payload.get("brief_depth") or 1,
                surface=effective_payload.get("surface", payload.get("surface", "chat")),
                source="explicit_user_request",
                explicit_request=True,
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
        elif prompt.trace.get("prompt_budget"):
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
                brief_metadata = {
                    key: value
                    for key, value in brief_metadata.items()
                    if key not in {"raw_model_answer", "shaped_answer"}
                }
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
        )
        prompt.trace["answer_persistence"] = {
            "assistant_message_persisted": True,
            "persistence_acknowledged": True,
            "persisted_role": "assistant",
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
            prompt_budget = persisted_prompt_trace.get("prompt_budget")
            if isinstance(prompt_budget, dict):
                prompt_budget.pop("retained_source_ids", None)
        persisted_dsa_trace = (
            persisted_prompt_trace.get("dsa", {}) if privacy_boundary.enforced else dsa_trace
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
            **model_calls[-1],
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
                "model_calls": model_calls,
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
