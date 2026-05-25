from __future__ import annotations

from typing import Any


ROUTING_PRECEDENCE = [
    "request_sensitivity_local_only",
    "profile_routing_policy_local_only",
    "manual_override_if_allowed",
    "router_rule_selection",
    "profile_cost_latency_policy",
    "provider_fallback",
]


def routing_trace_metadata(
    *,
    sensitivity: str,
    profile_local_only: bool,
    manual_override_requested: str | None,
    manual_override_applied: bool,
    manual_override_rejection_reason: str | None,
    selected_model: str,
    selected_provider: str,
    fallback_used: bool,
) -> dict[str, Any]:
    return {
        "precedence": ROUTING_PRECEDENCE,
        "sensitivity": sensitivity,
        "profile_local_only": profile_local_only,
        "manual_override_requested": manual_override_requested,
        "manual_override_applied": manual_override_applied,
        "manual_override_rejection_reason": manual_override_rejection_reason,
        "selected_model": selected_model,
        "selected_provider": selected_provider,
        "fallback_used": fallback_used,
    }
