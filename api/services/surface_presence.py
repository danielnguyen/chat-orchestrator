from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from services.response_shape import ResponseShape

PresenceState = Literal["idle", "briefing", "fallback", "unavailable"]
VOICE_SURFACES = {"voice", "car", "alexa"}


class SurfacePresence(BaseModel):
    presence_state: PresenceState = "idle"
    surface_type: str | None = None
    spoken_output: bool = False
    active_task_mode: bool = False
    fallback_active: bool = False
    reason: str | None = None
    source_fields: list[str] = []


def _normalize_surface_type(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {"", "unknown"}:
        return None
    return normalized


def surface_presence_trace_metadata(
    *,
    presence: SurfacePresence,
    included: bool,
) -> dict[str, Any]:
    return {
        "attempted": True,
        "status": "included" if included else "not_requested",
        "included": included,
        "presence_state": presence.presence_state,
        "reason": presence.reason,
        "source_fields": presence.source_fields,
        "surface_type": presence.surface_type,
        "spoken_output": presence.spoken_output,
        "active_task_mode": presence.active_task_mode,
        "fallback_active": presence.fallback_active,
    }


def resolve_surface_presence(
    payload: dict[str, Any],
    response_shape: ResponseShape,
) -> dict[str, Any]:
    surface_context = payload.get("surface_context") or {}
    if not isinstance(surface_context, dict):
        surface_context = {}

    surface_type = _normalize_surface_type(surface_context.get("surface_type"))
    if surface_type is None:
        surface_type = _normalize_surface_type(payload.get("surface"))

    source_fields: list[str] = []
    if payload.get("surface"):
        source_fields.append("surface")
    if surface_context.get("surface_type") is not None:
        source_fields.append("surface_context.surface_type")
    if surface_context.get("spoken_output") is not None:
        source_fields.append("surface_context.spoken_output")
    if surface_context.get("active_task_mode") is not None:
        source_fields.append("surface_context.active_task_mode")
    if response_shape:
        source_fields.append("response_shape.resolved_shape")

    spoken_output = bool(response_shape.spoken_output)
    active_task_mode = bool(response_shape.active_task_mode)

    if spoken_output or surface_type in VOICE_SURFACES:
        presence_state: PresenceState = "briefing"
        reason = "spoken_output_surface"
    else:
        presence_state = "idle"
        reason = "default_completed_turn"

    presence = SurfacePresence(
        presence_state=presence_state,
        surface_type=surface_type,
        spoken_output=spoken_output,
        active_task_mode=active_task_mode,
        fallback_active=False,
        reason=reason,
        source_fields=source_fields,
    )
    return surface_presence_trace_metadata(presence=presence, included=True)


def apply_surface_presence_outcome(
    trace: dict[str, Any] | None,
    *,
    fallback_active: bool = False,
    unavailable: bool = False,
) -> dict[str, Any]:
    presence_trace = dict(trace or {})
    source_fields = list(presence_trace.get("source_fields", []))

    if unavailable:
        if "model_call.fallback" not in source_fields:
            source_fields.append("model_call.fallback")
        presence_trace.update(
            {
                "attempted": True,
                "status": "included",
                "included": True,
                "presence_state": "unavailable",
                "fallback_active": False,
                "reason": "request_failed",
                "source_fields": source_fields,
            }
        )
        return presence_trace

    if fallback_active:
        if "model_call.fallback" not in source_fields:
            source_fields.append("model_call.fallback")
        presence_trace.update(
            {
                "attempted": True,
                "status": "included",
                "included": True,
                "presence_state": "fallback",
                "fallback_active": True,
                "reason": "provider_fallback_used",
                "source_fields": source_fields,
            }
        )

    return presence_trace
