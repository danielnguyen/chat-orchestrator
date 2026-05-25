from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any

import yaml
from clients.litellm import LiteLLMClient
from clients.memory_store import MemoryStoreClient
from router.engine import evaluate_route
from services.fallback import choose_fallback
from services.prompt_assembly import assemble_prompt
from services.profile_apply import apply_profile_to_request
from services.routing_contract import routing_trace_metadata


def _extract_last_user_text(messages: list[dict[str, str]]) -> str:
    for msg in reversed(messages):
        if msg.get("role") == "user":
            return msg.get("content", "")
    return ""


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
                "prompt_assembly": prompt_trace or {},
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
) -> dict[str, Any]:
    started = perf_counter()

    resolved = await memory_store.resolve_conversation(
        owner_id=payload["owner_id"],
        client_id=payload.get("client_id"),
    )
    conversation_id = payload.get("conversation_id") or resolved["conversation_id"]

    # Persist incoming user messages first.
    for msg in payload["messages"]:
        if msg["role"] == "user":
            await memory_store.add_message(
                conversation_id=conversation_id,
                owner_id=payload["owner_id"],
                role="user",
                content=msg["content"],
                client_id=payload.get("client_id"),
                metadata={"surface": payload.get("surface", "unknown")},
            )

    profile = await memory_store.resolve_profile(
        owner_id=payload["owner_id"],
        surface=payload.get("surface", "unknown"),
        requested_profile=payload.get("requested_profile"),
        client_id=payload.get("client_id"),
    )

    effective_payload = apply_profile_to_request(profile, payload)
    last_user_text = _extract_last_user_text(payload["messages"])
    retrieval_bundle = await memory_store.retrieve_bundle(
        request_id=request_id,
        conversation_id=conversation_id,
        owner_id=payload["owner_id"],
        query=last_user_text,
        retrieval=effective_payload.get("retrieval"),
    )
    signals = _compute_signals(effective_payload, retrieval_bundle)
    registry = _load_model_registry(model_registry_path)
    routing_policy = profile.get("routing_policy", {}) or {}
    sensitivity_local_only = effective_payload.get("sensitivity") == "local_only"
    profile_local_only = bool(routing_policy.get("local_only", False))
    local_only = sensitivity_local_only or profile_local_only
    cost_mode = routing_policy.get("cost_mode")
    latency_mode = routing_policy.get("latency_mode")

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

    prompt = assemble_prompt(
        profile=profile,
        retrieval_bundle=retrieval_bundle,
        current_messages=effective_payload["messages"],
    )
    messages = prompt.messages

    model_started = perf_counter()
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
            fallback_provider = _model_provider(fallback_model, registry, fallback.get("provider"))
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
            raise

    model_latency_ms = int((perf_counter() - model_started) * 1000)

    answer = completion["choices"][0]["message"]["content"]
    await memory_store.add_message(
        conversation_id=conversation_id,
        owner_id=payload["owner_id"],
        role="assistant",
        content=answer,
        client_id=payload.get("client_id"),
        metadata={"request_id": request_id, "selected_model": selected_model},
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
            },
            "fallback": {
                "triggered": fallback_used,
                "reason": "provider_error" if fallback_used else None,
            },
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
