from __future__ import annotations

from typing import Any

from models import StyleEnvelope, StyleEnvelopeOverride

VOICE_SURFACES = {"voice", "car", "alexa"}
TEXT_COMPACT_SURFACES = {"telegram"}


def _normalize_surface_type(value: str | None) -> str | None:
    if not value:
        return None
    normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized in {"", "unknown"}:
        return None
    return normalized


def _override_from_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    override = StyleEnvelopeOverride.model_validate(value)
    return override.model_dump(exclude_none=True)


def style_trace_metadata(
    *,
    resolved_envelope: StyleEnvelope,
    source_fields: list[str],
    guidance_flags: dict[str, Any],
    recognized_profile_fields: list[str],
    recognized_request_fields: list[str],
    included: bool,
) -> dict[str, Any]:
    return {
        "attempted": True,
        "status": "included" if included else "not_requested",
        "included": included,
        "source_fields": source_fields,
        "recognized_profile_fields": recognized_profile_fields,
        "recognized_request_fields": recognized_request_fields,
        "guidance_flags": guidance_flags,
        "resolved_envelope": resolved_envelope.model_dump(),
        "omission_reason": None if included else "no_style_signal",
    }


def resolve_style_envelope(
    payload: dict[str, Any], profile: dict[str, Any]
) -> tuple[StyleEnvelope, dict[str, Any]]:
    surface_context = payload.get("surface_context") or {}
    if not isinstance(surface_context, dict):
        surface_context = {}
    requested_style = _override_from_mapping(surface_context.get("style_envelope"))
    profile_style = _override_from_mapping(profile.get("response_style"))

    resolved = StyleEnvelope()
    source_fields: list[str] = []

    if profile_style:
        resolved = resolved.model_copy(update=profile_style)
        source_fields.append("profile.response_style")

    surface_type = _normalize_surface_type(surface_context.get("surface_type"))
    if surface_type is None:
        surface_type = _normalize_surface_type(payload.get("surface"))
    interaction_mode = surface_context.get("interaction_mode")
    spoken_output = surface_context.get("spoken_output")
    active_task_mode = bool(surface_context.get("active_task_mode"))

    if interaction_mode is None and surface_type in VOICE_SURFACES:
        interaction_mode = "voice_mediated"
    if spoken_output is None and (
        interaction_mode == "voice_mediated" or surface_type in VOICE_SURFACES
    ):
        spoken_output = True
    spoken_output = bool(spoken_output) if spoken_output is not None else False

    heuristic_updates: dict[str, Any] = {}
    guidance_flags = {
        "text_compact": surface_type in TEXT_COMPACT_SURFACES and not spoken_output,
        "spoken_output": spoken_output,
        "active_task_mode": active_task_mode,
    }

    if guidance_flags["text_compact"]:
        heuristic_updates.update({"sentence_length": "medium", "formality_range": "neutral"})
        source_fields.append("surface_context.surface_type")

    if spoken_output:
        heuristic_updates.update(
            {
                "sentence_length": "short",
                "playfulness_budget": "none",
                "analogy_density": "none",
                "technical_density": "low",
            }
        )
        source_fields.append("surface_context.spoken_output")

    if active_task_mode:
        heuristic_updates.update(
            {
                "directness": "high",
                "sentence_length": "short",
                "playfulness_budget": "none",
                "analogy_density": "none",
                "technical_density": "low",
            }
        )
        source_fields.append("surface_context.active_task_mode")

    if heuristic_updates:
        resolved = resolved.model_copy(update=heuristic_updates)

    if requested_style:
        resolved = resolved.model_copy(update=requested_style)
        source_fields.append("surface_context.style_envelope")

    included = bool(profile_style or heuristic_updates or requested_style)
    trace = style_trace_metadata(
        resolved_envelope=resolved,
        source_fields=source_fields,
        guidance_flags=guidance_flags,
        recognized_profile_fields=sorted(profile_style.keys()),
        recognized_request_fields=sorted(requested_style.keys()),
        included=included,
    )
    return resolved, trace


def build_style_guidance_block(envelope: StyleEnvelope, trace: dict[str, Any]) -> str:
    if not trace.get("included"):
        return ""

    flags = trace.get("guidance_flags", {}) if isinstance(trace, dict) else {}
    lines: list[str] = []

    if flags.get("text_compact"):
        lines.append(
            "- Keep the response compact and easy to scan in text without using "
            "speech-style phrasing."
        )
    if flags.get("spoken_output"):
        lines.append(
            "- Write for spoken delivery with short, speakable sentences and minimal formatting."
        )
    if flags.get("active_task_mode"):
        lines.append("- Lead with the answer, keep cognitive load low, and avoid optional detours.")

    if envelope.directness == "high":
        lines.append("- Be direct and decisive.")
    elif envelope.directness == "low":
        lines.append("- Use a gentler, less forceful tone.")

    if envelope.sentence_length == "short" and not flags.get("spoken_output"):
        lines.append("- Prefer short sentences.")
    elif envelope.sentence_length == "medium" and not flags.get("text_compact"):
        lines.append("- Keep sentences moderately concise.")

    if envelope.analogy_density == "none":
        lines.append("- Avoid analogies unless they are necessary.")
    elif envelope.analogy_density == "low":
        lines.append("- Use analogies sparingly.")

    if envelope.technical_density == "low":
        lines.append("- Keep technical detail light unless it is necessary for the answer.")
    elif envelope.technical_density == "high":
        lines.append("- Include technical detail when it materially helps.")

    if envelope.playfulness_budget == "none":
        lines.append("- Skip playful flourishes.")
    elif envelope.playfulness_budget == "medium":
        lines.append("- Light playfulness is acceptable when it fits the user request.")

    if envelope.repetition_sensitivity == "high":
        lines.append("- Avoid repeated framing and repeated conclusions.")

    if not lines:
        return ""
    return "Style guidance:\n" + "\n".join(dict.fromkeys(lines))
