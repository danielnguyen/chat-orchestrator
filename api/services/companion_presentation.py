from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.assistant_handoff import AssistantHandoff


@dataclass(frozen=True)
class PromptPresentationInput:
    companion_overlays: list[dict[str, Any]]
    runtime_overlay: dict[str, Any]
    style_trace: dict[str, Any]
    response_shape_trace: dict[str, Any]
    surface_presence_trace: dict[str, Any]
    retrieval_summary: dict[str, Any]
    scene_metadata: dict[str, Any]
    warnings: dict[str, Any]


@dataclass(frozen=True)
class CompanionPresentation:
    prompt_input: PromptPresentationInput
    routing: dict[str, Any]
    companion: dict[str, Any]
    runtime: dict[str, Any]
    retrieval: dict[str, Any]
    warnings: dict[str, Any]

    def trace_summary(self) -> dict[str, Any]:
        companion_overlays = self.prompt_input.companion_overlays or []
        runtime_overlay = self.prompt_input.runtime_overlay or {}
        retrieval_summary = self.prompt_input.retrieval_summary or {}
        scene_metadata = self.prompt_input.scene_metadata or {}

        overlay_refs = []
        for overlay in companion_overlays:
            if isinstance(overlay, dict):
                overlay_refs.append(
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
            "routing": {
                "selected_model": self.routing.get("selected_model"),
                "selected_provider": self.routing.get("selected_provider"),
                "rule_id": self.routing.get("rule_id"),
            },
            "style": {
                "status": self.prompt_input.style_trace.get("status"),
                "source_field_count": len(self.prompt_input.style_trace.get("source_fields", [])),
            },
            "response_shape": {
                "status": self.prompt_input.response_shape_trace.get("status"),
                "source_field_count": len(
                    self.prompt_input.response_shape_trace.get("source_fields", [])
                ),
                "continuation_state": self.prompt_input.response_shape_trace.get(
                    "continuation_state"
                ),
            },
            "surface_presence": {
                "status": self.prompt_input.surface_presence_trace.get("status"),
                "presence_state": self.prompt_input.surface_presence_trace.get(
                    "presence_state"
                ),
                "reason": self.prompt_input.surface_presence_trace.get("reason"),
            },
            "companion": {
                "status": self.companion.get("status"),
                "overlay_count": len(overlay_refs),
                "overlay_refs": overlay_refs,
                "overlay_ids": [
                    item["overlay_id"] for item in overlay_refs if item.get("overlay_id")
                ],
                "scene_id": scene_metadata.get("scene_id"),
                "profile_id": scene_metadata.get("profile_id"),
                "contract_id": scene_metadata.get("contract_id"),
                "omission_reason": self.companion.get("omission_reason"),
            },
            "runtime": {
                "status": self.runtime.get("status"),
                "runtime_state_id": self.runtime.get("runtime_state_id"),
                "overlay_ref": runtime_overlay_ref,
                "overlay_ids": [runtime_overlay_ref["overlay_id"]]
                if runtime_overlay_ref and runtime_overlay_ref.get("overlay_id")
                else [],
                "omission_reason": self.runtime.get("omission_reason"),
            },
            "retrieval": {
                "semantic_count": retrieval_summary.get("semantic_count", 0),
                "artifact_ref_count": retrieval_summary.get("artifact_ref_count", 0),
                "recent_history_count": retrieval_summary.get("recent_history_count", 0),
                "semantic_refs": retrieval_summary.get("semantic_refs", []),
                "artifact_refs": retrieval_summary.get("artifact_refs", []),
            },
            "warnings": {
                "fallback_active": bool(self.warnings.get("fallback_active", False)),
                "model_error_present": bool(self.warnings.get("model_error_present", False)),
                "companion_warning_count": self.warnings.get("companion_warning_count", 0),
            },
        }


def build_companion_presentation(handoff: AssistantHandoff) -> CompanionPresentation:
    companion_trace = handoff.companion.get("trace") or {}
    runtime_trace = handoff.runtime.get("trace") or {}
    retrieval_bundle = handoff.retrieval.get("bundle") or {}
    semantic = retrieval_bundle.get("semantic") or []
    artifact_refs = retrieval_bundle.get("artifact_refs") or []
    recent = retrieval_bundle.get("recent") or []

    return CompanionPresentation(
        prompt_input=PromptPresentationInput(
            companion_overlays=list(handoff.companion.get("overlays") or []),
            runtime_overlay=dict(handoff.runtime.get("overlay") or {}),
            style_trace=dict(handoff.presentation.get("style_trace") or {}),
            response_shape_trace=dict(handoff.presentation.get("response_shape_trace") or {}),
            surface_presence_trace=dict(handoff.presentation.get("surface_presence_trace") or {}),
            retrieval_summary={
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
            },
            scene_metadata={
                "scene_id": companion_trace.get("scene_id"),
                "profile_id": companion_trace.get("profile_id"),
                "contract_id": companion_trace.get("contract_id"),
            },
            warnings={
                "fallback_active": bool(handoff.warnings.get("fallback_active", False)),
                "model_error_present": bool(handoff.warnings.get("model_error")),
                "companion_warning_count": len(companion_trace.get("warnings", []) or []),
            },
        ),
        routing={
            "selected_model": handoff.routing.get("selected_model"),
            "selected_provider": handoff.routing.get("selected_provider"),
            "rule_id": handoff.routing.get("rule_id"),
        },
        companion={
            "status": companion_trace.get("status"),
            "omission_reason": companion_trace.get("omission_reason"),
        },
        runtime={
            "status": runtime_trace.get("status"),
            "runtime_state_id": runtime_trace.get("runtime_state_id"),
            "omission_reason": runtime_trace.get("omission_reason"),
        },
        retrieval={
            "semantic_count": len(semantic),
            "artifact_ref_count": len(artifact_refs),
            "recent_history_count": len(recent),
        },
        warnings={
            "fallback_active": bool(handoff.warnings.get("fallback_active", False)),
            "model_error_present": bool(handoff.warnings.get("model_error")),
            "companion_warning_count": len(companion_trace.get("warnings", []) or []),
        },
    )
