from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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


def assemble_prompt(
    *,
    profile: dict[str, Any],
    retrieval_bundle: dict[str, Any],
    current_messages: list[dict[str, str]],
    companion_overlays: list[dict[str, Any]] | None = None,
    companion_trace: dict[str, Any] | None = None,
    runtime_overlay: dict[str, Any] | None = None,
    runtime_trace: dict[str, Any] | None = None,
) -> PromptAssembly:
    messages: list[dict[str, str]] = []
    layers: list[dict[str, Any]] = []

    system_prompt = profile.get("prompt_overlay", "")
    profile_messages = [{"role": "system", "content": system_prompt}] if system_prompt else []
    messages.extend(profile_messages)
    layers.append(_layer_trace("profile_overlay", profile_messages))

    companion_messages: list[dict[str, str]] = []
    companion_trace_out = dict(companion_trace or {})
    companion_omission_reason = companion_trace_out.get("omission_reason")
    companion_overlay_metadata: list[dict[str, Any]] = []
    invalid_companion_roles: list[str | None] = []
    for overlay in companion_overlays or []:
        overlay_type = overlay.get("overlay_type")
        overlay_id = overlay.get("overlay_id")
        role = overlay.get("role", "system")
        content = overlay.get("content", "")
        if role == "system" and content:
            companion_messages.append({"role": "system", "content": content})
            companion_overlay_metadata.append(
                {"overlay_id": overlay_id, "overlay_type": overlay_type}
            )
        elif content:
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
    elif companion_overlays and invalid_companion_roles:
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

    companion_metadata = {
        "profile_id": companion_trace_out.get("profile_id"),
        "profile_version": companion_trace_out.get("profile_version"),
        "contract_id": companion_trace_out.get("contract_id"),
        "contract_version": companion_trace_out.get("contract_version"),
        "scene_id": companion_trace_out.get("scene_id"),
        "scene_confidence": companion_trace_out.get("scene_confidence"),
        "scene_source": companion_trace_out.get("scene_source"),
        "warnings": companion_trace_out.get("warnings", []),
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

    runtime_messages: list[dict[str, str]] = []
    runtime_trace_out = dict(runtime_trace or {})
    runtime_omission_reason = runtime_trace_out.get("omission_reason")
    if runtime_overlay and runtime_overlay.get("content"):
        role = runtime_overlay.get("role", "system")
        if role == "system":
            runtime_messages.append({"role": "system", "content": runtime_overlay["content"]})
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
                "runtime_state_id": runtime_overlay.get("runtime_state_id")
                if runtime_overlay
                else None,
                "overlay_id": runtime_overlay.get("overlay_id") if runtime_overlay else None,
                "overlay_type": runtime_overlay.get("overlay_type") if runtime_overlay else None,
                "source_fields": runtime_overlay.get("source_fields", [])
                if runtime_overlay
                else [],
                "omission_reason": runtime_omission_reason,
            },
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

    return PromptAssembly(
        messages=messages,
        trace={
            "layers": layers,
            "included_layers": [layer["name"] for layer in layers if layer["included"]],
            "omitted_layers": [layer["name"] for layer in layers if not layer["included"]],
            "truncation": {"applied": False, "reason": None},
            "companion_policy": companion_trace_out
            or {"attempted": False, "status": "not_requested"},
            "runtime": runtime_trace_out or {"attempted": False, "status": "not_requested"},
            "message_count": len(messages),
        },
    )
