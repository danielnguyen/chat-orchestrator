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
    runtime_overlay: dict[str, Any] | None = None,
    runtime_trace: dict[str, Any] | None = None,
) -> PromptAssembly:
    messages: list[dict[str, str]] = []
    layers: list[dict[str, Any]] = []

    system_prompt = profile.get("prompt_overlay", "")
    profile_messages = [{"role": "system", "content": system_prompt}] if system_prompt else []
    messages.extend(profile_messages)
    layers.append(_layer_trace("profile_overlay", profile_messages))

    runtime_messages: list[dict[str, str]] = []
    if runtime_overlay and runtime_overlay.get("content"):
        role = runtime_overlay.get("role", "system")
        if role in VALID_ROLES:
            runtime_messages.append({"role": role, "content": runtime_overlay["content"]})
            messages.extend(runtime_messages)
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
                "omission_reason": (runtime_trace or {}).get("omission_reason"),
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
            "runtime": runtime_trace or {"attempted": False, "status": "not_requested"},
            "message_count": len(messages),
        },
    )
