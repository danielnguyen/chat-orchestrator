from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.assistant_handoff import AssistantHandoff
from services.companion_presentation import CompanionPresentation

VALID_ROLES = {"user", "assistant", "system", "tool"}


@dataclass(frozen=True)
class PromptAssembly:
    messages: list[dict[str, str]]
    trace: dict[str, Any]


def _layer_trace(
    name: str,
    messages: list[dict[str, str]],
    *,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "included": bool(messages),
        "message_count": len(messages),
        "metadata": metadata or {},
    }


def build_recent_history(retrieval_bundle: dict[str, Any]) -> list[dict[str, str]]:
    bundle = retrieval_bundle.get("bundle", {})
    messages: list[dict[str, str]] = []

    recent = bundle.get("recent", []) or []
    for item in recent:
        role = item.get("role")
        content = item.get("content", "")
        if role in VALID_ROLES and content:
            messages.append({"role": role, "content": content})
    return messages


def build_retrieval_messages(retrieval_bundle: dict[str, Any]) -> list[dict[str, str]]:
    bundle = retrieval_bundle.get("bundle", {})
    messages: list[dict[str, str]] = []

    semantic = bundle.get("semantic", []) or []
    if semantic:
        lines = ["Retrieved memory excerpts:"]
        for item in semantic:
            created_at = item.get("created_at", "")
            role = item.get("role", "")
            content = item.get("content", "")
            lines.append(f"- [{created_at}] {role}: {content}")
        messages.append({"role": "system", "content": "\n".join(lines)})

    artifact_refs = bundle.get("artifact_refs", []) or []
    if artifact_refs:
        lines = ["Retrieved file snippets:"]
        for item in artifact_refs:
            repo_name = item.get("repo_name")
            file_path = item.get("file_path", "")
            label = f"{repo_name}/{file_path}" if repo_name else file_path
            lines.append(f"- [{label}] {item.get('snippet', '')}")
        messages.append({"role": "system", "content": "\n".join(lines)})

    return messages


def retrieval_snippet_trace(retrieval_bundle: dict[str, Any]) -> dict[str, Any]:
    bundle = retrieval_bundle.get("bundle", {})
    semantic = bundle.get("semantic", []) or []
    artifact_refs = bundle.get("artifact_refs", []) or []
    return {
        "semantic": [
            {
                "message_id": item.get("message_id"),
                "created_at": item.get("created_at"),
                "role": item.get("role"),
                "score": item.get("score"),
            }
            for item in semantic
        ],
        "artifact_refs": [
            {
                "artifact_id": item.get("artifact_id"),
                "file_path": item.get("file_path"),
                "repo_name": item.get("repo_name"),
                "relevance_score": item.get("relevance_score"),
            }
            for item in artifact_refs
        ],
    }


def build_external_context_messages(context_pack: dict[str, Any] | None) -> list[dict[str, str]]:
    if not isinstance(context_pack, dict):
        return []

    items = context_pack.get("items") or []
    lines = ["External source context:"]
    included = 0
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if not isinstance(text, str) or not text:
            continue
        source_name = item.get("source_name") or "Unknown source"
        title = item.get("title") or "Untitled"
        source_ref = item.get("source_ref") or "unknown"
        lines.extend(
            [
                f"[{index}] {source_name} — {title}",
                f"source_ref: {source_ref}",
                text,
            ]
        )
        included += 1

    if included == 0:
        return []
    return [{"role": "system", "content": "\n".join(lines)}]


def external_context_trace(context_pack: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(context_pack, dict):
        return {"item_count": 0, "sources_used": [], "source_refs": []}

    items = context_pack.get("items") or []
    source_refs = []
    for item in items:
        if isinstance(item, dict) and item.get("source_ref"):
            source_refs.append(item["source_ref"])

    return {
        "item_count": len(source_refs),
        "sources_used": context_pack.get("sources_used", []) or [],
        "source_refs": source_refs,
    }


def build_interaction_governance_messages(
    governance: dict[str, Any] | None,
) -> list[dict[str, str]]:
    if not isinstance(governance, dict):
        return []

    lines = ["Interaction guidance:"]

    response_posture = governance.get("response_posture")
    if isinstance(response_posture, str) and response_posture:
        lines.append(f"- Adopt a {response_posture} response posture.")
        if response_posture == "tactical":
            lines.append("- Prefer direct operational help and next concrete steps.")

    if governance.get("humor_allowed") is False:
        lines.append("- Do not add jokes or playful commentary.")
    if governance.get("commentary_allowed") is False:
        lines.append("- Avoid extra meta-commentary.")
    if governance.get("clarifying_question_allowed") is True:
        lines.append("- Ask a clarifying question when needed to move the task forward safely.")
    if governance.get("action_allowed") is False:
        lines.append("- Do not imply that any external action has been performed.")
    if governance.get("requires_confirmation") is True:
        lines.append("- Confirm before treating this turn as an action command.")

    privacy_hint = governance.get("privacy_sensitivity_hint")
    if privacy_hint in {"private", "sensitive"}:
        lines.append("- Avoid unnecessary disclosure or over-specific sensitive details.")

    persona_scope_hint = governance.get("persona_scope_hint")
    if isinstance(persona_scope_hint, str) and persona_scope_hint:
        lines.append(f"- Stay within the hinted scope: {persona_scope_hint}.")

    if len(lines) == 1:
        return []
    return [{"role": "system", "content": "\n".join(lines)}]


def interaction_governance_trace(governance_trace: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(governance_trace, dict):
        governance_trace = {}

    return {
        "attempted": governance_trace.get("attempted", False),
        "status": governance_trace.get("status", "not_requested"),
        "included": governance_trace.get("included", False),
        "runtime_call_status": governance_trace.get("runtime_call_status"),
        "interaction_kind": governance_trace.get("interaction_kind"),
        "response_posture": governance_trace.get("response_posture"),
        "commentary_allowed": governance_trace.get("commentary_allowed"),
        "humor_allowed": governance_trace.get("humor_allowed"),
        "action_allowed": governance_trace.get("action_allowed"),
        "requires_confirmation": governance_trace.get("requires_confirmation"),
        "privacy_sensitivity_hint": governance_trace.get("privacy_sensitivity_hint"),
        "confidence": governance_trace.get("confidence"),
        "reason_summary": governance_trace.get("reason_summary", []),
        "omission_reason": governance_trace.get("omission_reason"),
    }


def assemble_prompt(
    *,
    profile: dict[str, Any],
    retrieval_bundle: dict[str, Any],
    current_messages: list[dict[str, str]],
    handoff: AssistantHandoff | None = None,
    presentation: CompanionPresentation | None = None,
    style_guidance: str | None = None,
    style_trace: dict[str, Any] | None = None,
    response_shape_guidance: str | None = None,
    response_shape_trace: dict[str, Any] | None = None,
    surface_presence_trace: dict[str, Any] | None = None,
    companion_overlays: list[dict[str, Any]] | None = None,
    companion_trace: dict[str, Any] | None = None,
    runtime_identity: dict[str, Any] | None = None,
    runtime_identity_trace: dict[str, Any] | None = None,
    world_state: dict[str, Any] | None = None,
    world_state_trace: dict[str, Any] | None = None,
    relationship_context: dict[str, Any] | None = None,
    relationship_context_trace: dict[str, Any] | None = None,
    runtime_overlay: dict[str, Any] | None = None,
    runtime_trace: dict[str, Any] | None = None,
    interaction_governance: dict[str, Any] | None = None,
    interaction_governance_trace_data: dict[str, Any] | None = None,
    interrupt_trace: dict[str, Any] | None = None,
    external_context_pack: dict[str, Any] | None = None,
    dsa_trace: dict[str, Any] | None = None,
) -> PromptAssembly:
    messages: list[dict[str, str]] = []
    layers: list[dict[str, Any]] = []

    system_prompt = profile.get("prompt_overlay", "")
    profile_messages = [{"role": "system", "content": system_prompt}] if system_prompt else []
    messages.extend(profile_messages)
    layers.append(_layer_trace("profile_overlay", profile_messages))

    style_trace_out = dict(style_trace or {})
    style_messages = [{"role": "system", "content": style_guidance}] if style_guidance else []
    messages.extend(style_messages)
    layers.append(
        _layer_trace(
            "style_guidance",
            style_messages,
            metadata={
                "source_fields": style_trace_out.get("source_fields", []),
                "recognized_profile_fields": style_trace_out.get(
                    "recognized_profile_fields", []
                ),
                "recognized_request_fields": style_trace_out.get(
                    "recognized_request_fields", []
                ),
                "guidance_flags": style_trace_out.get("guidance_flags", {}),
                "resolved_envelope": style_trace_out.get("resolved_envelope", {}),
                "omission_reason": style_trace_out.get("omission_reason"),
            },
        )
    )

    response_shape_trace_out = dict(response_shape_trace or {})
    response_shape_messages = (
        [{"role": "system", "content": response_shape_guidance}]
        if response_shape_guidance
        else []
    )
    messages.extend(response_shape_messages)
    layers.append(
        _layer_trace(
            "response_shape",
            response_shape_messages,
            metadata={
                "source_fields": response_shape_trace_out.get("source_fields", []),
                "guidance_flags": response_shape_trace_out.get("guidance_flags", {}),
                "resolved_shape": response_shape_trace_out.get("resolved_shape", {}),
                "continuation_state": response_shape_trace_out.get("continuation_state"),
                "abbreviation_reason": response_shape_trace_out.get("abbreviation_reason"),
                "omission_reason": response_shape_trace_out.get("omission_reason"),
            },
        )
    )

    presentation_input = presentation.prompt_input if presentation is not None else None
    companion_overlays_in = (
        presentation_input.companion_overlays
        if presentation_input is not None
        else companion_overlays
    )
    runtime_overlay_in = (
        presentation_input.runtime_overlay if presentation_input is not None else runtime_overlay
    )

    companion_messages: list[dict[str, str]] = []
    companion_trace_out = dict(companion_trace or {})
    companion_omission_reason = companion_trace_out.get("omission_reason")
    companion_overlay_metadata: list[dict[str, Any]] = []
    invalid_companion_roles: list[str | None] = []
    for overlay in companion_overlays_in or []:
        if not isinstance(overlay, dict):
            invalid_companion_roles.append(None)
            continue
        overlay_type = overlay.get("overlay_type")
        overlay_id = overlay.get("overlay_id")
        role = overlay.get("role")
        content = overlay.get("content")
        if role == "system" and isinstance(content, str) and content:
            companion_messages.append({"role": "system", "content": content})
            companion_overlay_metadata.append(
                {"overlay_id": overlay_id, "overlay_type": overlay_type}
            )
        else:
            invalid_companion_roles.append(overlay_type)

    if companion_messages:
        messages.extend(companion_messages)
        companion_trace_out.update(
            {
                "status": "included",
                "included": True,
                "included_overlays": companion_overlay_metadata,
            }
        )
        if invalid_companion_roles:
            companion_trace_out["omitted_overlay_types"] = invalid_companion_roles
            companion_trace_out["omission_reason"] = "invalid_companion_overlay_role"
    elif companion_overlays_in and invalid_companion_roles:
        companion_omission_reason = "invalid_companion_overlay_role"
        companion_trace_out.update(
            {
                "status": "omitted",
                "included": False,
                "included_overlays": [],
                "omission_reason": companion_omission_reason,
                "invalid_overlay_types": invalid_companion_roles,
            }
        )

    runtime_overlay_ids = []
    if runtime_overlay_in and runtime_overlay_in.get("overlay_id"):
        runtime_overlay_ids.append(runtime_overlay_in["overlay_id"])

    companion_trace_out.setdefault("companion_profile_id", companion_trace_out.get("profile_id"))
    companion_trace_out.setdefault(
        "companion_profile_version", companion_trace_out.get("profile_version")
    )
    companion_trace_out.setdefault(
        "interaction_contract_id", companion_trace_out.get("contract_id")
    )
    companion_trace_out.setdefault(
        "interaction_contract_version", companion_trace_out.get("contract_version")
    )
    companion_trace_out.setdefault(
        "companion_policy_warnings", companion_trace_out.get("warnings", [])
    )
    companion_trace_out["companion_overlay_ids"] = [
        item.get("overlay_id") for item in companion_overlay_metadata if item.get("overlay_id")
    ]
    companion_trace_out["runtime_overlay_ids"] = runtime_overlay_ids
    companion_trace_out.setdefault(
        "cognitive_runtime_compile_status", companion_trace_out.get("status")
    )
    companion_trace_out.setdefault("cognitive_runtime_compile_error", None)
    companion_trace_out.setdefault("cognitive_runtime_compile_endpoint", None)

    companion_metadata = {
        "profile_id": companion_trace_out.get("profile_id"),
        "profile_version": companion_trace_out.get("profile_version"),
        "contract_id": companion_trace_out.get("contract_id"),
        "contract_version": companion_trace_out.get("contract_version"),
        "contract_trace": companion_trace_out.get("contract_trace"),
        "interaction_contract": companion_trace_out.get("interaction_contract"),
        "scene_id": companion_trace_out.get("scene_id"),
        "scene_confidence": companion_trace_out.get("scene_confidence"),
        "scene_source": companion_trace_out.get("scene_source"),
        "warnings": companion_trace_out.get("warnings", []),
        "companion_profile_id": companion_trace_out.get("companion_profile_id"),
        "companion_profile_version": companion_trace_out.get(
            "companion_profile_version"
        ),
        "interaction_contract_id": companion_trace_out.get("interaction_contract_id"),
        "interaction_contract_version": companion_trace_out.get(
            "interaction_contract_version"
        ),
        "companion_policy_warnings": companion_trace_out.get(
            "companion_policy_warnings", []
        ),
        "companion_overlay_ids": companion_trace_out.get("companion_overlay_ids", []),
        "runtime_overlay_ids": companion_trace_out.get("runtime_overlay_ids", []),
        "cognitive_runtime_compile_status": companion_trace_out.get(
            "cognitive_runtime_compile_status"
        ),
        "cognitive_runtime_compile_error": companion_trace_out.get(
            "cognitive_runtime_compile_error"
        ),
        "cognitive_runtime_compile_endpoint": companion_trace_out.get(
            "cognitive_runtime_compile_endpoint"
        ),
        "included_overlays": companion_overlay_metadata,
        "omitted_overlay_types": invalid_companion_roles,
        "omission_reason": companion_omission_reason,
    }
    layers.append(
        _layer_trace(
            "companion_policy",
            companion_messages,
            metadata=companion_metadata,
        )
    )

    interaction_governance_messages = build_interaction_governance_messages(
        interaction_governance
    )
    if interaction_governance_messages:
        messages.extend(interaction_governance_messages)
    governance_trace_out = interaction_governance_trace(interaction_governance_trace_data)
    layers.append(
        _layer_trace(
            "interaction_governance",
            interaction_governance_messages,
            metadata={
                "runtime_call_status": governance_trace_out.get("runtime_call_status"),
                "interaction_kind": governance_trace_out.get("interaction_kind"),
                "response_posture": governance_trace_out.get("response_posture"),
                "commentary_allowed": governance_trace_out.get("commentary_allowed"),
                "humor_allowed": governance_trace_out.get("humor_allowed"),
                "action_allowed": governance_trace_out.get("action_allowed"),
                "requires_confirmation": governance_trace_out.get(
                    "requires_confirmation"
                ),
                "privacy_sensitivity_hint": governance_trace_out.get(
                    "privacy_sensitivity_hint"
                ),
                "confidence": governance_trace_out.get("confidence"),
                "reason_summary": governance_trace_out.get("reason_summary", []),
                "omission_reason": governance_trace_out.get("omission_reason"),
            },
        )
    )

    runtime_identity_messages: list[dict[str, str]] = []
    runtime_identity_trace_out = dict(runtime_identity_trace or {})
    runtime_identity_omission_reason = runtime_identity_trace_out.get("omission_reason")
    if runtime_identity and runtime_identity.get("content"):
        runtime_identity_messages.append(
            {"role": "system", "content": runtime_identity["content"]}
        )
        messages.extend(runtime_identity_messages)
    layers.append(
        _layer_trace(
            "runtime_identity",
            runtime_identity_messages,
            metadata={
                "active_persona_id": runtime_identity.get("active_persona_id")
                if runtime_identity
                else None,
                "surface_id": runtime_identity.get("surface_id") if runtime_identity else None,
                "capability_domain": runtime_identity.get("capability_domain")
                if runtime_identity
                else None,
                "advisory_memory_scope_summary": runtime_identity.get(
                    "advisory_memory_scope_summary", []
                )
                if runtime_identity
                else [],
                "advisory_tool_permission_summary": runtime_identity.get(
                    "advisory_tool_permission_summary", []
                )
                if runtime_identity
                else [],
                "omission_reason": runtime_identity_omission_reason,
            },
        )
    )

    world_state_messages: list[dict[str, str]] = []
    world_state_trace_out = dict(world_state_trace or {})
    world_state_omission_reason = world_state_trace_out.get("omission_reason")
    if world_state and world_state.get("prompt_content"):
        world_state_messages.append({"role": "system", "content": world_state["prompt_content"]})
        messages.extend(world_state_messages)
    layers.append(
        _layer_trace(
            "world_state",
            world_state_messages,
            metadata={
                "included_claim_count": world_state_trace_out.get("included_claim_count", 0),
                "excluded_claim_count": world_state_trace_out.get("excluded_claim_count", 0),
                "stale_count": world_state_trace_out.get("stale_count", 0),
                "aging_count": world_state_trace_out.get("aging_count", 0),
                "expired_count": world_state_trace_out.get("expired_count", 0),
                "conflicted_count": world_state_trace_out.get("conflicted_count", 0),
                "active_persona_id": world_state_trace_out.get("active_persona_id"),
                "allowed_domains": world_state_trace_out.get("allowed_domains", []),
                "confirmation_required": world_state_trace_out.get("confirmation_required", False),
                "omission_reason": world_state_omission_reason,
            },
        )
    )

    relationship_context_messages: list[dict[str, str]] = []
    relationship_context_trace_out = dict(relationship_context_trace or {})
    relationship_context_omission_reason = relationship_context_trace_out.get(
        "omission_reason"
    )
    if relationship_context and relationship_context.get("prompt_content"):
        relationship_context_messages.append(
            {"role": "system", "content": relationship_context["prompt_content"]}
        )
        messages.extend(relationship_context_messages)
    layers.append(
        _layer_trace(
            "relationship_context",
            relationship_context_messages,
            metadata={
                "selected_relationship_count": relationship_context_trace_out.get(
                    "selected_relationship_count", 0
                ),
                "excluded_relationship_count": relationship_context_trace_out.get(
                    "excluded_relationship_count", 0
                ),
                "relationship_edges_used": relationship_context_trace_out.get(
                    "relationship_edges_used", []
                ),
                "relationship_edges_excluded": relationship_context_trace_out.get(
                    "relationship_edges_excluded", []
                ),
                "relationship_exclusion_reasons": relationship_context_trace_out.get(
                    "relationship_exclusion_reasons", {}
                ),
                "relationship_context_overlay_applied": relationship_context_trace_out.get(
                    "relationship_context_overlay_applied", False
                ),
                "relationship_conflicts": relationship_context_trace_out.get(
                    "relationship_conflicts", []
                ),
                "relationship_confirmation_required": relationship_context_trace_out.get(
                    "relationship_confirmation_required", False
                ),
                "active_persona_id": relationship_context_trace_out.get(
                    "active_persona_id"
                ),
                "allowed_relationship_scopes": relationship_context_trace_out.get(
                    "allowed_relationship_scopes", []
                ),
                "omission_reason": relationship_context_omission_reason,
            },
        )
    )

    runtime_messages: list[dict[str, str]] = []
    runtime_trace_out = dict(runtime_trace or {})
    runtime_omission_reason = runtime_trace_out.get("omission_reason")
    if runtime_overlay_in and runtime_overlay_in.get("content"):
        role = runtime_overlay_in.get("role", "system")
        if role == "system":
            runtime_messages.append({"role": "system", "content": runtime_overlay_in["content"]})
            messages.extend(runtime_messages)
        else:
            runtime_omission_reason = "invalid_runtime_overlay_role"
            runtime_trace_out.update(
                {
                    "status": "omitted",
                    "included": False,
                    "omission_reason": runtime_omission_reason,
                }
            )
    layers.append(
        _layer_trace(
            "runtime_overlay",
            runtime_messages,
            metadata={
                "runtime_state_id": runtime_overlay_in.get("runtime_state_id")
                if runtime_overlay_in
                else None,
                "overlay_id": runtime_overlay_in.get("overlay_id") if runtime_overlay_in else None,
                "overlay_type": (
                    runtime_overlay_in.get("overlay_type")
                    if runtime_overlay_in
                    else None
                ),
                "source_fields": runtime_overlay_in.get("source_fields", [])
                if runtime_overlay_in
                else [],
                "omission_reason": runtime_omission_reason,
            },
        )
    )

    external_context_messages = build_external_context_messages(external_context_pack)
    messages.extend(external_context_messages)
    layers.append(
        _layer_trace(
            "external_source_context",
            external_context_messages,
            metadata=external_context_trace(external_context_pack),
        )
    )

    retrieval_messages = build_retrieval_messages(retrieval_bundle)
    messages.extend(retrieval_messages)
    layers.append(
        _layer_trace(
            "retrieval_augmentation",
            retrieval_messages,
            metadata={"snippets": retrieval_snippet_trace(retrieval_bundle)},
        )
    )

    recent_history = build_recent_history(retrieval_bundle)
    messages.extend(recent_history)
    layers.append(_layer_trace("recent_history", recent_history))

    messages.extend(current_messages)
    layers.append(_layer_trace("current_messages", current_messages))

    trace = {
        "layers": layers,
        "included_layers": [layer["name"] for layer in layers if layer["included"]],
        "omitted_layers": [layer["name"] for layer in layers if not layer["included"]],
        "truncation": {"applied": False, "reason": None},
        "handoff": handoff.trace_summary() if handoff is not None else None,
        "presentation": presentation.trace_summary() if presentation is not None else None,
        "style": style_trace_out or {"attempted": False, "status": "not_requested"},
        "response_shape": response_shape_trace_out
        or {"attempted": False, "status": "not_requested"},
        "surface_presence": surface_presence_trace
        or {"attempted": False, "status": "not_requested"},
        "companion_policy": companion_trace_out
        or {"attempted": False, "status": "not_requested"},
        "interaction_governance": governance_trace_out
        or {"attempted": False, "status": "not_requested", "included": False},
        "runtime_identity": runtime_identity_trace_out
        or {"attempted": False, "status": "not_requested"},
        "world_state": world_state_trace_out or {"attempted": False, "status": "not_requested"},
        "relationship_context": relationship_context_trace_out
        or {"attempted": False, "status": "not_requested"},
        "runtime": runtime_trace_out or {"attempted": False, "status": "not_requested"},
        "dsa": dsa_trace or {"enabled": False, "called": False, "status": "disabled"},
        "message_count": len(messages),
    }
    if interrupt_trace is not None:
        trace["interrupt_policy"] = interrupt_trace

    return PromptAssembly(messages=messages, trace=trace)
