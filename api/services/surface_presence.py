from __future__ import annotations

import asyncio
from time import monotonic
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from services.response_shape import ResponseShape

PRESENCE_STREAM_SECONDS = 15.0
PRESENCE_STREAM_POLL_SECONDS = 0.5
PRESENCE_STREAM_MAX_EVENTS = 100

PresenceState = Literal[
    "idle", "listening", "thinking", "briefing", "alerting", "awaiting_confirmation",
    "suppressed", "unavailable", "fallback",
]
VOICE_SURFACES = {"voice", "car", "alexa"}
_TRANSITIONS = {
    "idle": {"idle", "thinking", "briefing", "alerting", "awaiting_confirmation",
             "suppressed", "unavailable", "fallback"},
    "thinking": {"thinking", "idle", "briefing", "alerting", "awaiting_confirmation",
                 "suppressed", "unavailable", "fallback"},
    "briefing": {"briefing", "idle", "suppressed", "unavailable", "fallback"},
    "alerting": {"alerting", "idle", "awaiting_confirmation", "suppressed",
                 "unavailable", "fallback"},
    "awaiting_confirmation": {"awaiting_confirmation", "idle", "suppressed",
                              "unavailable", "fallback"},
    "suppressed": {"suppressed", "unavailable"},
    "unavailable": {"unavailable"},
    "fallback": {"fallback", "unavailable"},
    # No current adapter supplies an authoritative capture lifecycle. The vocabulary
    # is recognized, but neither a voice label nor a runtime permission activates it.
    "listening": set(),
}
_SOURCES = {
    "surface", "surface_context.surface_type", "surface_context.spoken_output",
    "surface_context.active_task_mode", "response_shape.resolved_shape", "model_call.fallback",
    "runtime_turn.turn_status", "response.completed", "capabilities.pending_confirmation",
    "interrupt_policy.response_selected", "runtime_timing.result", "privacy_context.enforcement",
    "surface_presence.transition", "companion_policy.validated",
}
_REASONS = {
    "default_completed_turn", "spoken_output_surface", "provider_fallback_used", "request_failed",
    "admitted_turn_processing", "pending_confirmation", "selected_intervention",
    "policy_output_withheld", "unknown_presence_state", "invalid_presence_signal",
    "invalid_runtime_binding", "invalid_transition", "listening_signal_unavailable",
}


class PresenceTurnContext(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    request_id: str = Field(min_length=1, max_length=120)
    owner_id: str = Field(min_length=1, max_length=120)
    conversation_id: str = Field(min_length=1, max_length=120)
    surface: str = Field(min_length=1, max_length=64)
    runtime_session_id: str = Field(min_length=1, max_length=120)
    runtime_turn_id: str = Field(min_length=1, max_length=120)


class PresenceCompanionContext(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    profile_id: str = Field(min_length=1, max_length=120)
    profile_version: int = Field(ge=1)
    scene_id: str = Field(min_length=1, max_length=64)
    scene_source: Literal[
        "requested_scene", "runtime_state", "runtime_interaction_mode",
        "general", "fallback_general",
    ]


class SurfacePresence(BaseModel):
    model_config = ConfigDict(strict=True)

    presence_state: PresenceState = "idle"
    surface_type: str | None = None
    spoken_output: bool = False
    active_task_mode: bool = False
    fallback_active: bool = False
    reason: str | None = None
    source_fields: list[str] = Field(default_factory=list)
    turn_context: PresenceTurnContext | None = None
    companion: PresenceCompanionContext | None = None


def _normalize_surface_type(value: Any) -> str | None:
    if not isinstance(value, str) or not value or len(value) > 64:
        return None
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {"", "unknown"}:
        return None
    return normalized


def _project(
    presence: SurfacePresence, state: PresenceState, reason: str, source: str,
) -> dict[str, Any]:
    previous = presence.presence_state
    accepted = state in _TRANSITIONS[previous]
    if not accepted:
        # A terminal/suppressed projection cannot be made active by another event
        # on this turn. A new admitted turn starts a new projection instead.
        state = previous if previous in {"suppressed", "unavailable"} else "fallback"
        reason, source = "invalid_transition", "surface_presence.transition"
    presence = presence.model_copy(update={
        "presence_state": state, "reason": reason,
        "source_fields": list(dict.fromkeys([*presence.source_fields, source])),
    })
    return {
        **surface_presence_trace_metadata(presence=presence, included=True),
        "previous_state": previous,
        "transition_status": (
            "rejected" if not accepted else "unchanged" if previous == state else "accepted"
        ),
    }


def _valid_admission(payload: dict[str, Any], admitted: Any, request_id: Any) -> bool:
    if not isinstance(admitted, dict):
        return False
    session, turn, event = (admitted.get(key) for key in (
        "runtime_session", "runtime_turn", "event",
    ))
    if not all(isinstance(value, dict) for value in (session, turn, event)):
        return False
    fields = (request_id, session.get("runtime_session_id"), turn.get("runtime_turn_id"))
    if any(not isinstance(value, str) or not value.strip() or len(value) > 120
           for value in fields):
        return False
    event_payload = event.get("event_payload_json")
    return (
        all(isinstance(payload.get(key), str) and bool(payload[key].strip())
            and len(payload[key]) <= 120
            and session.get(key) == payload[key] for key in ("owner_id", "conversation_id"))
        and session.get("surface") == payload.get("surface", "unknown")
        and session.get("status") == "active"
        and session.get("closed_at") is None
        and turn.get("runtime_session_id") == session["runtime_session_id"]
        and turn.get("turn_status") in ("received", "retrieving", "responding")
        and turn.get("completed_at") is None
        and event.get("event_type") == "turn_started"
        and event.get("runtime_session_id") == session["runtime_session_id"]
        and event.get("runtime_turn_id") == turn["runtime_turn_id"]
        and isinstance(event_payload, dict) and event_payload.get("request_id") == request_id
    )


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
        **({"turn_context": presence.turn_context.model_dump()} if presence.turn_context else {}),
        **({"companion": presence.companion.model_dump()} if presence.companion else {}),
    }


def resolve_surface_presence(
    payload: dict[str, Any],
    response_shape: ResponseShape,
    *,
    admitted_turn: dict[str, Any] | None = None,
    request_id: str | None = None,
    companion_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict) or not isinstance(response_shape, ResponseShape):
        return _project(SurfacePresence(), "fallback", "invalid_presence_signal",
                        "surface_presence.transition")
    if ((payload.get("surface_context") is not None
         and not isinstance(payload["surface_context"], dict))
            or (payload.get("surface") is not None and not isinstance(payload["surface"], str))
            or type(response_shape.spoken_output) is not bool
            or type(response_shape.active_task_mode) is not bool):
        return _project(SurfacePresence(), "fallback", "invalid_presence_signal",
                        "surface_presence.transition")
    surface_context = payload.get("surface_context") or {}
    if not isinstance(surface_context, dict):
        surface_context = {}
    if (any(surface_context.get(key) is not None and type(surface_context[key]) is not bool
            for key in ("spoken_output", "active_task_mode"))
            or (surface_context.get("surface_type") is not None
                and not isinstance(surface_context["surface_type"], str))):
        return _project(SurfacePresence(), "fallback", "invalid_presence_signal",
                        "surface_presence.transition")

    # Delivery hints cannot relabel the admitted integration surface.
    surface_type = _normalize_surface_type(payload.get("surface"))

    source_fields: list[str] = []
    if payload.get("surface"):
        source_fields.append("surface")
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
    if admitted_turn is not None:
        if not _valid_admission(payload, admitted_turn, request_id):
            return _project(presence, "fallback", "invalid_runtime_binding",
                            "surface_presence.transition")
        try:
            presence.turn_context = PresenceTurnContext(
                request_id=request_id, owner_id=payload["owner_id"],
                conversation_id=payload["conversation_id"],
                surface=payload.get("surface", "unknown"),
                runtime_session_id=admitted_turn["runtime_session"]["runtime_session_id"],
                runtime_turn_id=admitted_turn["runtime_turn"]["runtime_turn_id"],
            )
        except ValidationError:
            return _project(presence, "fallback", "invalid_runtime_binding",
                            "surface_presence.transition")
        # Only the internal, already validated compile result can supply identity.
        # An omitted/failed policy never borrows labels from client scene/context.
        if (isinstance(companion_policy, dict) and companion_policy.get("included") is True
                and companion_policy.get("status") == "included"
                and companion_policy.get("turn_binding") == presence.turn_context.model_dump()):
            try:
                presence.companion = PresenceCompanionContext.model_validate({
                    key: companion_policy.get(key) for key in PresenceCompanionContext.model_fields
                })
            except ValidationError:
                pass
            else:
                presence.source_fields.append("companion_policy.validated")
        return {
            **_project(presence.model_copy(update={"presence_state": "idle"}),
                       "thinking", "admitted_turn_processing", "runtime_turn.turn_status"),
            # A fresh turn does not establish the previous UI/device state.
            "previous_state": None, "transition_status": "initialized",
        }
    return surface_presence_trace_metadata(presence=presence, included=True)


def apply_surface_presence_outcome(
    trace: dict[str, Any] | None,
    *,
    fallback_active: bool = False,
    unavailable: bool = False,
    outcome: Literal[
        "completed", "confirmation_pending", "intervention", "suppressed", "privacy_withheld",
    ] | None = None,
) -> dict[str, Any]:
    """Project a server-selected outcome; never read an event from client/model text.

    This describes presentation only. In particular, confirmation_pending must be
    selected from an existing pending-action envelope, never used to issue one.
    Suppressed may still accompany a safe acknowledgment or privacy placeholder.
    """
    try:
        if not isinstance(trace, dict) or "presence_state" not in trace:
            raise ValueError
        presence = SurfacePresence.model_validate({
            key: value for key, value in trace.items() if key in SurfacePresence.model_fields
        })
        if (presence.reason not in _REASONS
                or any(s not in _SOURCES for s in presence.source_fields)
                or (presence.fallback_active
                    and "model_call.fallback" not in presence.source_fields)
                or type(fallback_active) is not bool or type(unavailable) is not bool):
            raise ValueError
    except (ValidationError, ValueError, TypeError):
        presence = SurfacePresence()
        if unavailable is True:
            return _project(presence, "unavailable", "request_failed", "model_call.fallback")
        if fallback_active is True:
            presence.fallback_active = True
            return _project(presence, "fallback", "provider_fallback_used", "model_call.fallback")
        return _project(presence, "fallback", "unknown_presence_state",
                        "surface_presence.transition")
    if outcome == "privacy_withheld":
        presence.companion = None
        presence.source_fields = [
            s for s in presence.source_fields if s != "companion_policy.validated"
        ]
    if unavailable:
        presence.fallback_active = False
        return _project(presence, "unavailable", "request_failed", "model_call.fallback")
    presence.fallback_active = presence.fallback_active or fallback_active
    if fallback_active and "model_call.fallback" not in presence.source_fields:
        presence.source_fields = [*presence.source_fields, "model_call.fallback"]
    if presence.presence_state == "unavailable":
        presence.fallback_active = False
    if presence.presence_state == "listening":
        return _project(SurfacePresence(), "fallback", "listening_signal_unavailable",
                        "surface_presence.transition")
    if outcome == "suppressed":
        return _project(presence, "suppressed", "policy_output_withheld",
                        "runtime_timing.result")
    if outcome == "privacy_withheld":
        return _project(presence, "suppressed", "policy_output_withheld",
                        "privacy_context.enforcement")
    if outcome == "confirmation_pending":
        return _project(presence, "awaiting_confirmation", "pending_confirmation",
                        "capabilities.pending_confirmation")
    if outcome == "intervention":
        return _project(presence, "alerting", "selected_intervention",
                        "interrupt_policy.response_selected")
    if outcome not in (None, "completed"):
        return _project(presence, "fallback", "invalid_presence_signal",
                        "surface_presence.transition")
    if fallback_active or presence.fallback_active:
        return _project(presence, "fallback", "provider_fallback_used", "model_call.fallback")
    if outcome == "completed":
        spoken = presence.spoken_output or presence.surface_type in VOICE_SURFACES
        return _project(presence, "briefing" if spoken else "idle",
                        "spoken_output_surface" if spoken else "default_completed_turn",
                        "response.completed")
    return surface_presence_trace_metadata(presence=presence, included=True)


async def publish_surface_presence(
    *, runtime: Any, memory_store: Any, trace: dict[str, Any] | None, stage: str,
) -> bool:
    """Observe a CO-selected outcome over the owner-accepted internal trust boundary.

    This does not authenticate CR's callers or authorize any action. Failure must
    not alter canonical chat; lifecycle-aware readers report unavailable instead.
    """
    if not callable(getattr(runtime, "publish_surface_presence", None)):
        return False
    try:
        presence = SurfacePresence.model_validate({
            key: value for key, value in (trace or {}).items()
            if key in SurfacePresence.model_fields
        })
        if presence.turn_context is None or presence.presence_state == "listening":
            return False
        binding = presence.turn_context.model_dump()
        conversation = await asyncio.wait_for(memory_store.get_conversation(
            owner_id=binding["owner_id"], conversation_id=binding["conversation_id"],
        ), timeout=1.0)
        if any(conversation.get(key) != binding[key] for key in ("owner_id", "conversation_id")):
            return False
        if conversation.get("lifecycle_state") != "open":
            return False
        # No identity, scene, text, reason, provider or trace data crosses this seam.
        await asyncio.wait_for(runtime.publish_surface_presence(
            **binding, stage=stage, state=presence.presence_state,
        ), timeout=1.0)
        return True
    except Exception:
        return False


async def stream_surface_presence(*, request: Any, initial: Any, read: Any, after: str | None):
    """Bounded pull stream: no worker, task registry, queue or cross-client state."""
    from models import PresenceProjection

    def frame(kind, projection):
        identifier = f"id: {projection.cursor}\n" if kind != "unavailable" else ""
        return f"{identifier}event: {kind}\ndata: {projection.model_dump_json()}\n\n"

    deadline = monotonic() + PRESENCE_STREAM_SECONDS
    cursor = after or initial.cursor
    emitted = 0
    unavailable_cursor = None
    batch = initial
    if after is None:
        projection = initial.current or PresenceProjection(
            state="unavailable", cursor=initial.cursor,
        )
        yield frame("snapshot", projection)
        emitted += 1
        unavailable_cursor = initial.cursor if initial.current is None else None
    while emitted < PRESENCE_STREAM_MAX_EVENTS and monotonic() < deadline:
        if await request.is_disconnected():
            return
        for event in batch.events:
            if emitted >= PRESENCE_STREAM_MAX_EVENTS:
                return
            yield frame("transition", event)
            cursor = event.cursor
            emitted += 1
            unavailable_cursor = None
        if batch.current is None and not batch.has_more and unavailable_cursor != cursor:
            yield frame("unavailable", PresenceProjection(state="unavailable", cursor=cursor))
            emitted += 1
            unavailable_cursor = cursor
        if not batch.has_more:
            await asyncio.sleep(PRESENCE_STREAM_POLL_SECONDS)
        if await request.is_disconnected() or monotonic() >= deadline:
            return
        try:
            batch = await asyncio.wait_for(read(cursor), timeout=2.0)
        except asyncio.CancelledError:
            raise
        except Exception:
            yield frame("unavailable", PresenceProjection(state="unavailable", cursor=cursor))
            return
