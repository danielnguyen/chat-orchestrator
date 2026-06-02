from __future__ import annotations

from typing import Any, Literal

from models import StyleEnvelope
from pydantic import BaseModel

VOICE_SURFACES = {"voice", "car", "alexa"}

ContinuationState = Literal["none", "abbreviated", "expandable", "suppressed"]


class ResponseShape(BaseModel):
    spoken_output: bool = False
    active_task_mode: bool = False
    concise_first_answer: bool = False
    max_sentence_count: int | None = None
    avoid_markdown: bool = False
    allows_expansion: bool = False
    expansion_marker_allowed: bool = False
    continuation_state: ContinuationState = "none"
    abbreviation_reason: str | None = None
    latency_preference: Literal["normal", "low", "lowest"] | None = None
    confirmation_style: str | None = None


def _normalize_surface_type(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {"", "unknown"}:
        return None
    return normalized


def response_shape_trace_metadata(
    *,
    resolved_shape: ResponseShape,
    source_fields: list[str],
    guidance_flags: dict[str, Any],
    included: bool,
) -> dict[str, Any]:
    return {
        "attempted": True,
        "status": "included" if included else "not_requested",
        "included": included,
        "source_fields": source_fields,
        "guidance_flags": guidance_flags,
        "resolved_shape": resolved_shape.model_dump(),
        "continuation_state": resolved_shape.continuation_state,
        "abbreviation_reason": resolved_shape.abbreviation_reason,
        "omission_reason": None if included else "no_response_shape_signal",
    }


def resolve_response_shape(
    payload: dict[str, Any],
    style_envelope: StyleEnvelope,
    style_trace: dict[str, Any] | None = None,
) -> tuple[ResponseShape, dict[str, Any]]:
    surface_context = payload.get("surface_context") or {}
    if not isinstance(surface_context, dict):
        surface_context = {}

    surface_type = _normalize_surface_type(surface_context.get("surface_type"))
    if surface_type is None:
        surface_type = _normalize_surface_type(payload.get("surface"))

    interaction_mode = surface_context.get("interaction_mode")
    output_format = surface_context.get("output_format")
    spoken_output = surface_context.get("spoken_output")
    active_task_mode = bool(surface_context.get("active_task_mode"))
    allows_expansion = bool(surface_context.get("allows_expansion"))
    latency_preference = surface_context.get("latency_preference")
    verbosity_target = surface_context.get("verbosity_target")

    source_fields: list[str] = []

    if interaction_mode == "voice_mediated":
        source_fields.append("surface_context.interaction_mode")
    if output_format == "speech":
        source_fields.append("surface_context.output_format")
    if surface_context.get("spoken_output") is not None:
        source_fields.append("surface_context.spoken_output")
    if surface_type in VOICE_SURFACES and interaction_mode is None and spoken_output is None:
        source_fields.append("surface_context.surface_type")
    if active_task_mode:
        source_fields.append("surface_context.active_task_mode")
    if surface_context.get("allows_expansion") is not None:
        source_fields.append("surface_context.allows_expansion")
    if latency_preference is not None:
        source_fields.append("surface_context.latency_preference")
    if verbosity_target is not None:
        source_fields.append("surface_context.verbosity_target")
    if style_trace and style_trace.get("included"):
        source_fields.append("style.resolved_envelope")

    inferred_spoken_output = False
    if interaction_mode == "voice_mediated" or output_format == "speech":
        inferred_spoken_output = True
    elif surface_type in VOICE_SURFACES:
        inferred_spoken_output = True
    if spoken_output is None:
        spoken_output = inferred_spoken_output
    spoken_output = bool(spoken_output)

    concise_first_answer = spoken_output or active_task_mode
    avoid_markdown = spoken_output

    max_sentence_count: int | None = None
    if spoken_output:
        max_sentence_count = 2
    if active_task_mode:
        max_sentence_count = min(max_sentence_count or 2, 2)

    continuation_state: ContinuationState = "none"
    abbreviation_reason: str | None = None
    expansion_marker_allowed = False
    confirmation_style: str | None = None

    if active_task_mode:
        confirmation_style = "decisive"
    elif spoken_output:
        confirmation_style = "brief"

    compression_signal = (
        spoken_output
        or interaction_mode == "voice_mediated"
        or output_format == "speech"
        or latency_preference in {"low", "lowest"}
        or verbosity_target == "short"
    )
    should_abbreviate = concise_first_answer and compression_signal

    if should_abbreviate:
        continuation_state = "abbreviated"
        if active_task_mode and spoken_output:
            abbreviation_reason = "spoken_active_task"
        elif spoken_output:
            abbreviation_reason = "spoken_output"
        elif interaction_mode == "voice_mediated":
            abbreviation_reason = "voice_mediated"
        elif output_format == "speech":
            abbreviation_reason = "speech_output"
        elif latency_preference in {"low", "lowest"}:
            abbreviation_reason = "latency_preference"
        elif verbosity_target == "short":
            abbreviation_reason = "verbosity_target"
        else:
            abbreviation_reason = "concise_shape"

    if continuation_state != "none" and allows_expansion:
        low_latency = latency_preference in {"low", "lowest"}
        short_verbosity = verbosity_target == "short"
        if low_latency or short_verbosity:
            continuation_state = "expandable"
            expansion_marker_allowed = True

    shape = ResponseShape(
        spoken_output=spoken_output,
        active_task_mode=active_task_mode,
        concise_first_answer=concise_first_answer,
        max_sentence_count=max_sentence_count,
        avoid_markdown=avoid_markdown,
        allows_expansion=allows_expansion,
        expansion_marker_allowed=expansion_marker_allowed,
        continuation_state=continuation_state,
        abbreviation_reason=abbreviation_reason,
        latency_preference=latency_preference,
        confirmation_style=confirmation_style,
    )

    guidance_flags = {
        "spoken_output": shape.spoken_output,
        "active_task_mode": shape.active_task_mode,
        "avoid_markdown": shape.avoid_markdown,
        "concise_first_answer": shape.concise_first_answer,
        "allows_expansion": shape.allows_expansion,
        "expansion_marker_allowed": shape.expansion_marker_allowed,
    }
    included = bool(
        spoken_output
        or active_task_mode
        or output_format == "speech"
        or interaction_mode == "voice_mediated"
    )
    trace = response_shape_trace_metadata(
        resolved_shape=shape,
        source_fields=source_fields,
        guidance_flags=guidance_flags,
        included=included,
    )
    return shape, trace


def build_response_shape_guidance_block(shape: ResponseShape, trace: dict[str, Any]) -> str:
    if not trace.get("included"):
        return ""

    lines: list[str] = []

    if shape.spoken_output:
        lines.extend(
            [
                "- Write for spoken delivery with plain, speakable text.",
                "- Prefer one or two short sentences before adding detail.",
                "- Avoid markdown-heavy formatting, tables, and long bullet lists.",
            ]
        )

    if shape.concise_first_answer:
        lines.append("- Lead with the answer before any supporting detail.")

    if shape.active_task_mode:
        lines.extend(
            [
                "- Keep cognitive load low and avoid optional detours.",
                "- Avoid multi-branch explanations unless the user asks for options.",
                "- Avoid open-ended follow-up questions unless they are required to proceed.",
            ]
        )

    if shape.expansion_marker_allowed and shape.continuation_state == "expandable":
        lines.append(
            "- If the answer is intentionally abbreviated, you may mention that more "
            "detail is available if needed."
        )

    if not lines:
        return ""
    return "Response shape guidance:\n" + "\n".join(dict.fromkeys(lines))
