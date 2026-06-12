from __future__ import annotations

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
from services.fallback import choose_fallback
from services.profile_apply import apply_profile_to_request
from services.prompt_assembly import assemble_prompt
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


def _dsa_disabled_trace(enabled: bool) -> dict[str, Any]:
    return {
        "enabled": enabled,
        "called": False,
        "status": "disabled" if not enabled else "not_requested",
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
            }
        )

    return {
        "query": response.get("query"),
        "sources_used": response.get("sources_used", []) or [],
        "items": items_out,
        "errors": response.get("errors", []) or [],
        "budget": response.get("budget", {}) or {},
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
    dsa_trace_base: dict[str, Any] = {
        "enabled": external_context_enabled,
        "requested_source_ids": external_context_config.get("source_ids", []),
        "requested_domain_tags": external_context_config.get("domain_tags", []),
        "allowed_sensitivity": allowed_sensitivity,
        "max_results": max_results if max_results is not None else 5,
    }
    if not dsa_enabled:
        return None, _dsa_disabled_trace(False)
    if not external_context_enabled:
        return None, _dsa_disabled_trace(True)
    if not external_calls_allowed:
        return None, {**dsa_trace_base, "called": False, "status": "skipped_local_only"}
    if dsa is None:
        return None, {
            **dsa_trace_base,
            "called": False,
            "status": "error",
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
        context_pack = _sanitize_context_pack(response if isinstance(response, dict) else {})
        return context_pack, {
            **dsa_trace_base,
            "called": True,
            "status": "success",
            "item_count": len(context_pack.get("items", [])),
            "sources_used": context_pack.get("sources_used", []),
        }
    except httpx.TimeoutException:
        return None, {
            **dsa_trace_base,
            "called": True,
            "status": "error",
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
            "error_code": error_code,
        }
    except Exception:
        return None, {
            **dsa_trace_base,
            "called": True,
            "status": "error",
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
        "advisory_tool_permission_summary": trace.get(
            "advisory_tool_permission_summary", []
        ),
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

    state = response.get("runtime_state") or {}
    overlay = response.get("overlay")
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
    return {"prompt_content": prompt_content}, {
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
) -> None:
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
                "query": last_user_text,
                "bundle": retrieval_bundle.get("bundle", {}),
                "prompt_assembly": {
                    **(prompt_trace or {}),
                    "surface_presence": apply_surface_presence_outcome(
                        surface_presence_trace,
                        fallback_active=fallback_used,
                        unavailable=True,
                    ),
                },
            },
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
            "model_call": {
                "provider": selected_provider,
                "model": selected_model,
                "latency_ms": None,
                "error": failure_reason,
            },
            "fallback": {
                "triggered": fallback_used,
                "reason": "provider_error" if fallback_used else None,
            },
            "dsa": dsa_trace or {"enabled": False, "called": False, "status": "disabled"},
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
    response_action_mode: str = "shadow",
    interrupt_policy_mode: str = "off",
    dsa: DataSourceAggregatorClient | None = None,
    dsa_enabled: bool = False,
) -> dict[str, Any]:
    started = perf_counter()

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
                metadata={"surface": payload.get("surface", "unknown")},
            )
            last_user_message_id = saved.get("message_id") if isinstance(saved, dict) else None

    profile = await memory_store.resolve_profile(
        owner_id=payload["owner_id"],
        surface=payload.get("surface", "unknown"),
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
    last_user_text = _extract_last_user_text(payload["messages"])
    routing_policy = profile.get("routing_policy", {}) or {}
    sensitivity_local_only = effective_payload.get("sensitivity") == "local_only"
    profile_local_only = bool(routing_policy.get("local_only", False))
    local_only = sensitivity_local_only or profile_local_only
    cost_mode = routing_policy.get("cost_mode")
    latency_mode = routing_policy.get("latency_mode")
    external_context_request = effective_payload.get("external_context")
    external_context_enabled = bool(effective_payload.get("external_context_enabled", False)) or bool(
        isinstance(external_context_request, dict) and external_context_request.get("enabled") is True
    )

    external_context_pack, dsa_trace = await _resolve_external_context(
        dsa=dsa,
        dsa_enabled=dsa_enabled,
        external_context_enabled=external_context_enabled,
        external_context=external_context_request if isinstance(external_context_request, dict) else None,
        external_calls_allowed=not local_only,
        query=last_user_text,
    )
    turn_response, turn_state_trace = await _start_runtime_turn(
        runtime=runtime,
        request_id=request_id,
        owner_id=payload["owner_id"],
        conversation_id=conversation_id,
        surface=payload.get("surface", "unknown"),
        input_message_id=last_user_message_id,
    )
    runtime_session = turn_response.get("runtime_session") if isinstance(turn_response, dict) else None
    runtime_session_trace = _runtime_session_trace_from_session(
        runtime_session,
        attempted=bool(turn_state_trace.get("attempted")),
        omission_reason=turn_state_trace.get(
            "omission_reason",
            "runtime_session_missing_from_turn_response",
        ),
        error_type=turn_state_trace.get("error_type"),
    )
    try:
        await _advance_runtime_turn(
            runtime=runtime,
            turn_state_trace=turn_state_trace,
            request_id=request_id,
            turn_status="retrieving",
        )
        retrieval_bundle = await memory_store.retrieve_bundle(
            request_id=request_id,
            conversation_id=conversation_id,
            owner_id=payload["owner_id"],
            query=last_user_text,
            retrieval=effective_payload.get("retrieval"),
        )
        companion_overlays, companion_trace = await _resolve_companion_policy(
            runtime=runtime,
            enabled=companion_policy_enabled,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=payload.get("surface", "unknown"),
            requested_scene=payload.get("requested_scene"),
        )
        interrupt_trace = await _resolve_interrupt_policy(
            runtime=runtime,
            interrupt_policy_mode=interrupt_policy_mode,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=payload.get("surface", "unknown"),
            current_user_text=last_user_text,
            recent_messages=effective_payload["messages"],
            requested_scene=payload.get("requested_scene"),
        )
        runtime_identity, runtime_identity_trace = await _resolve_runtime_identity(
            runtime=runtime,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=payload.get("surface", "unknown"),
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
        )
        world_state, world_state_trace = await _resolve_world_state(
            runtime=runtime,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=payload.get("surface", "unknown"),
            runtime_session_id=runtime_session_trace.get("runtime_session_id"),
            active_persona_id=runtime_identity_trace.get("active_persona_id"),
        )
        runtime_overlay, runtime_trace = await _resolve_runtime_overlay(
            runtime=runtime,
            enable_runtime_overlays=enable_runtime_overlays,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=payload.get("surface", "unknown"),
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
                        "world_state": world_state_trace,
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

        status = "ok"
        fallback_used = False
        model_error = None

        handoff = build_assistant_handoff(
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=payload.get("surface", "unknown"),
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
            runtime_identity=runtime_identity,
            runtime_identity_trace=runtime_identity_trace,
            world_state=world_state,
            world_state_trace=world_state_trace,
            runtime_overlay=runtime_overlay,
            runtime_trace=runtime_trace,
            interrupt_trace=interrupt_trace,
            external_context_pack=external_context_pack,
            dsa_trace=dsa_trace,
        )
        messages = prompt.messages

        model_started = perf_counter()
        await _advance_runtime_turn(
            runtime=runtime,
            turn_state_trace=turn_state_trace,
            request_id=request_id,
            turn_status="responding",
        )
        try:
            completion = await litellm.chat(
                request_id=request_id,
                model=selected_model,
                messages=messages,
            )
        except Exception as e:  # pragma: no cover
            fallback = choose_fallback(route)
            if fallback:
                fallback_used = True
                status = "degraded"
                fallback_model = fallback["selected_model"]
                fallback_provider = _model_provider(
                    fallback_model,
                    registry,
                    fallback.get("provider"),
                )
                if local_only and fallback_provider != "local":
                    local_fallback = _policy_pick_model(
                        registry,
                        provider="local",
                        cost_mode=cost_mode,
                        latency_mode=latency_mode,
                    )
                    if not local_fallback:
                        await _create_error_trace(
                            memory_store=memory_store,
                            request_id=request_id,
                            conversation_id=conversation_id,
                            payload=effective_payload,
                            profile=profile,
                            retrieval_bundle=retrieval_bundle,
                            last_user_text=last_user_text,
                            route=route,
                            selected_model=fallback_model,
                            selected_provider=fallback_provider,
                            sensitivity_local_only=sensitivity_local_only,
                            profile_local_only=profile_local_only,
                            effective_local_only=local_only,
                            override_requested=override_requested,
                            override_applied=bool(override),
                            override_reason=override_reason,
                            failure_reason="no_local_model_available",
                            started=started,
                            fallback_used=True,
                            prompt_trace=prompt.trace,
                            surface_presence_trace=surface_presence_trace,
                            dsa_trace=dsa_trace,
                        )
                        await _complete_runtime_turn(
                            runtime=runtime,
                            turn_state_trace=turn_state_trace,
                            request_id=request_id,
                            turn_status="abandoned",
                        )
                        raise RuntimeError("local_only policy active but no local fallback available")
                    fallback_model = local_fallback
                    fallback_provider = "local"
                selected_model = fallback_model
                selected_provider = fallback_provider
                completion = await litellm.chat(
                    request_id=request_id,
                    model=selected_model,
                    messages=messages,
                )
                model_error = str(e)
            else:
                await _complete_runtime_turn(
                    runtime=runtime,
                    turn_state_trace=turn_state_trace,
                    request_id=request_id,
                    turn_status="abandoned",
                )
                raise

        model_latency_ms = int((perf_counter() - model_started) * 1000)

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

        await memory_store.add_message(
            conversation_id=conversation_id,
            owner_id=payload["owner_id"],
            role="assistant",
            content=answer,
            client_id=payload.get("client_id"),
            metadata={"request_id": request_id, "selected_model": selected_model},
        )

        await _reset_runtime_after_turn(
            runtime=runtime,
            runtime_trace=runtime_trace,
            request_id=request_id,
            owner_id=payload["owner_id"],
            conversation_id=conversation_id,
            surface=payload.get("surface", "unknown"),
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
        prompt.trace["surface_presence"] = apply_surface_presence_outcome(
            surface_presence_trace,
            fallback_active=fallback_used,
        )

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
                    "query": last_user_text,
                    "bundle": retrieval_bundle.get("bundle", {}),
                    "prompt_assembly": prompt.trace,
                },
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
                "model_call": {
                    "provider": selected_provider,
                    "model": selected_model,
                    "latency_ms": model_latency_ms,
                    "error": model_error,
                    "brief": brief_metadata,
                },
                "fallback": {
                    "triggered": fallback_used,
                    "reason": "provider_error" if fallback_used else None,
                },
                "dsa": dsa_trace,
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
            "sources": retrieval_bundle.get("bundle", {}).get("artifact_refs", []),
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
