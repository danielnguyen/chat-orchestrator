from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AssistantHandoff:
    request: dict[str, Any]
    routing: dict[str, Any]
    presentation: dict[str, Any]
    companion: dict[str, Any]
    runtime: dict[str, Any]
    retrieval: dict[str, Any]
    warnings: dict[str, Any]

    def trace_summary(self) -> dict[str, Any]:
        companion_overlays = self.companion.get("overlays") or []
        runtime_overlay = self.runtime.get("overlay") or {}
        retrieval_bundle = self.retrieval.get("bundle") or {}
        semantic = retrieval_bundle.get("semantic") or []
        artifact_refs = retrieval_bundle.get("artifact_refs") or []
        recent = retrieval_bundle.get("recent") or []

        companion_overlay_refs = []
        for overlay in companion_overlays:
            if isinstance(overlay, dict):
                companion_overlay_refs.append(
                    {
                        "overlay_id": overlay.get("overlay_id"),
                        "overlay_type": overlay.get("overlay_type"),
                    }
                )

        runtime_overlay_ref = None
        if isinstance(runtime_overlay, dict) and (
            runtime_overlay.get("overlay_id") or runtime_overlay.get("overlay_type")
        ):
            runtime_overlay_ref = {
                "overlay_id": runtime_overlay.get("overlay_id"),
                "overlay_type": runtime_overlay.get("overlay_type"),
            }

        return {
            "request": {
                "request_id": self.request.get("request_id"),
                "owner_id": self.request.get("owner_id"),
                "conversation_id": self.request.get("conversation_id"),
                "surface": self.request.get("surface"),
            },
            "routing": {
                "selected_model": self.routing.get("selected_model"),
                "selected_provider": self.routing.get("selected_provider"),
                "rule_id": self.routing.get("rule_id"),
                "fallback_candidate_count": len(self.routing.get("fallbacks") or []),
                "local_only": bool(self.routing.get("effective_local_only", False)),
                "manual_override_applied": bool(
                    self.routing.get("manual_override_applied", False)
                ),
                "manual_override_requested": bool(
                    self.routing.get("manual_override_requested")
                ),
                "manual_override_rejection_reason": self.routing.get(
                    "manual_override_rejection_reason"
                ),
            },
            "presentation": {
                "style_status": (self.presentation.get("style_trace") or {}).get("status"),
                "response_shape_status": (
                    self.presentation.get("response_shape_trace") or {}
                ).get("status"),
                "surface_presence_state": (
                    self.presentation.get("surface_presence_trace") or {}
                ).get("presence_state"),
                "surface_presence_reason": (
                    self.presentation.get("surface_presence_trace") or {}
                ).get("reason"),
            },
            "companion": {
                "status": (self.companion.get("trace") or {}).get("status"),
                "overlay_count": len(companion_overlay_refs),
                "overlay_refs": companion_overlay_refs,
                "overlay_ids": [
                    item["overlay_id"] for item in companion_overlay_refs if item.get("overlay_id")
                ],
                "omission_reason": (self.companion.get("trace") or {}).get("omission_reason"),
                "compile_status": (self.companion.get("trace") or {}).get(
                    "cognitive_runtime_compile_status"
                ),
                "compile_endpoint": (self.companion.get("trace") or {}).get(
                    "cognitive_runtime_compile_endpoint"
                ),
            },
            "runtime": {
                "status": (self.runtime.get("trace") or {}).get("status"),
                "runtime_state_id": (self.runtime.get("trace") or {}).get("runtime_state_id"),
                "overlay_ref": runtime_overlay_ref,
                "overlay_ids": [runtime_overlay_ref["overlay_id"]]
                if runtime_overlay_ref and runtime_overlay_ref.get("overlay_id")
                else [],
                "source_field_count": len(runtime_overlay.get("source_fields") or [])
                if isinstance(runtime_overlay, dict)
                else 0,
                "omission_reason": (self.runtime.get("trace") or {}).get("omission_reason"),
                "reset_after_turn": bool((self.runtime.get("trace") or {}).get("reset_after_turn")),
            },
            "retrieval": {
                "query_present": bool(self.retrieval.get("query")),
                "semantic_count": len(semantic),
                "artifact_ref_count": len(artifact_refs),
                "recent_history_count": len(recent),
                "semantic_refs": [
                    {
                        "message_id": item.get("message_id"),
                        "created_at": item.get("created_at"),
                        "role": item.get("role"),
                    }
                    for item in semantic
                    if isinstance(item, dict)
                ],
                "artifact_refs": [
                    {
                        "artifact_id": item.get("artifact_id"),
                        "file_path": item.get("file_path"),
                        "repo_name": item.get("repo_name"),
                    }
                    for item in artifact_refs
                    if isinstance(item, dict)
                ],
                "observed_metadata": {
                    "has_code_like_content": bool(
                        (retrieval_bundle.get("observed_metadata") or {}).get(
                            "has_code_like_content", False
                        )
                    )
                },
            },
            "warnings": {
                "fallback_active": bool(self.warnings.get("fallback_active", False)),
                "model_error_present": bool(self.warnings.get("model_error")),
                "companion_warning_count": len(
                    ((self.companion.get("trace") or {}).get("warnings") or [])
                ),
                "interrupt_status": (self.warnings.get("interrupt_trace") or {}).get("status"),
            },
        }


def build_assistant_handoff(
    *,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    route: dict[str, Any],
    selected_model: str,
    selected_provider: str,
    effective_local_only: bool,
    manual_override_requested: str | None,
    manual_override_applied: bool,
    manual_override_rejection_reason: str | None,
    style_trace: dict[str, Any],
    response_shape_trace: dict[str, Any],
    surface_presence_trace: dict[str, Any],
    companion_overlays: list[dict[str, Any]] | None,
    companion_trace: dict[str, Any],
    runtime_overlay: dict[str, Any] | None,
    runtime_trace: dict[str, Any],
    retrieval_query: str,
    retrieval_bundle: dict[str, Any],
    interrupt_trace: dict[str, Any] | None,
    fallback_active: bool = False,
    model_error: str | None = None,
) -> AssistantHandoff:
    return AssistantHandoff(
        request={
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        },
        routing={
            "selected_model": selected_model,
            "selected_provider": selected_provider,
            "rule_id": route.get("rule_id"),
            "rationale": route.get("rationale"),
            "fallbacks": route.get("fallbacks", []),
            "effective_local_only": effective_local_only,
            "manual_override_requested": manual_override_requested,
            "manual_override_applied": manual_override_applied,
            "manual_override_rejection_reason": manual_override_rejection_reason,
        },
        presentation={
            "style_trace": style_trace,
            "response_shape_trace": response_shape_trace,
            "surface_presence_trace": surface_presence_trace,
        },
        companion={
            "overlays": companion_overlays or [],
            "trace": companion_trace,
        },
        runtime={
            "overlay": runtime_overlay or {},
            "trace": runtime_trace,
        },
        retrieval={
            "query": retrieval_query,
            "bundle": retrieval_bundle.get("bundle", {}),
        },
        warnings={
            "fallback_active": fallback_active,
            "model_error": model_error,
            "interrupt_trace": interrupt_trace,
        },
    )
