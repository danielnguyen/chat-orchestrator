from __future__ import annotations

from typing import Any

from services.prompt_budget import ProviderAttempt


def choose_fallback(route: dict[str, Any]) -> dict[str, Any] | None:
    fallbacks = route.get("fallbacks", [])
    return fallbacks[0] if fallbacks else None


def resolve_provider_attempt_plan(
    *,
    registry: dict[str, Any],
    route: dict[str, Any],
    selected_model: str,
    selected_provider: str,
    local_only: bool,
    cost_mode: str | None,
    latency_mode: str | None,
    policy_pick_model: Any,
    model_provider: Any,
) -> list[ProviderAttempt]:
    attempts: list[ProviderAttempt] = [
        _attempt_from_model(
            registry=registry,
            model=selected_model,
            provider=selected_provider,
            role="primary",
        )
    ]

    fallback = choose_fallback(route)
    if fallback:
        fallback_model = fallback.get("selected_model")
        fallback_provider = model_provider(fallback_model, registry, fallback.get("provider"))
        if local_only and fallback_provider != "local":
            local_fallback = policy_pick_model(
                registry,
                provider="local",
                cost_mode=cost_mode,
                latency_mode=latency_mode,
            )
            if local_fallback:
                fallback_model = local_fallback
                fallback_provider = "local"
            else:
                fallback_model = None
        if isinstance(fallback_model, str) and fallback_model:
            attempts.append(
                _attempt_from_model(
                    registry=registry,
                    model=fallback_model,
                    provider=fallback_provider,
                    role="fallback",
                )
            )
    return attempts


def _attempt_from_model(
    *,
    registry: dict[str, Any],
    model: str,
    provider: str,
    role: str,
) -> ProviderAttempt:
    info = registry.get(model, {}) if isinstance(registry, dict) else {}
    return ProviderAttempt(
        model=model,
        provider=provider,
        max_context_tokens=info.get("max_context_tokens"),
        role=role,
    )
