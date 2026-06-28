from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

ESTIMATOR_ID = "co-local-utf8-v1"
BUDGET_CONTRACT_VERSION = "prompt-input-budget-v1"
ESTIMATED_GLOBAL_PROMPT_OVERHEAD_TOKENS = 2


class PromptBudgetError(RuntimeError):
    def __init__(self, reason: str, trace: dict[str, Any]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.trace = trace


@dataclass(frozen=True)
class ProviderAttempt:
    model: str
    provider: str
    max_context_tokens: int
    role: str

    def to_trace(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "provider": self.provider,
            "max_context_tokens": self.max_context_tokens,
            "role": self.role,
        }


@dataclass(frozen=True)
class PromptBudgetContract:
    attempts: list[ProviderAttempt]
    output_token_reserve: int = 2048
    context_safety_margin: int = 256
    profile_prompt_budget: Any = None


@dataclass
class PromptBudgetResult:
    messages: list[dict[str, str]]
    layers: list[dict[str, Any]]
    trace: dict[str, Any]
    retained_source_ids: dict[str, list[str]] = field(default_factory=dict)


def estimate_message_tokens(message: dict[str, str]) -> int:
    role = str(message.get("role", ""))
    content = str(message.get("content", ""))
    role_units = len(role.encode("utf-8"))
    content_units = (len(content.encode("utf-8")) + 3) // 4
    return 4 + role_units + content_units


def estimate_prompt_tokens(messages: list[dict[str, str]]) -> int:
    return ESTIMATED_GLOBAL_PROMPT_OVERHEAD_TOKENS + estimate_messages_tokens(messages)


def estimate_messages_tokens(messages: list[dict[str, str]]) -> int:
    return sum(estimate_message_tokens(message) for message in messages)


def validate_budget_contract(contract: PromptBudgetContract) -> tuple[int, dict[str, Any]]:
    attempts = contract.attempts
    if not attempts:
        trace = _base_trace(contract, failure_reason="model_context_limit_unavailable")
        raise PromptBudgetError("model_context_limit_unavailable", trace)

    limits: list[int] = []
    for attempt in attempts:
        limit = attempt.max_context_tokens
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            trace = _base_trace(contract, failure_reason="model_context_limit_unavailable")
            raise PromptBudgetError("model_context_limit_unavailable", trace)
        limits.append(limit)

    reserve = contract.output_token_reserve
    margin = contract.context_safety_margin
    if (
        not isinstance(reserve, int)
        or isinstance(reserve, bool)
        or reserve < 0
        or not isinstance(margin, int)
        or isinstance(margin, bool)
        or margin < 0
    ):
        trace = _base_trace(contract, failure_reason="effective_prompt_budget_unusable")
        raise PromptBudgetError("effective_prompt_budget_unusable", trace)

    effective_budget = min(limits) - reserve - margin
    profile_trace = _profile_clamp_trace(contract.profile_prompt_budget, effective_budget)
    if profile_trace["valid"] and profile_trace["applied"]:
        effective_budget = profile_trace["supplied_max_input_tokens"]

    if effective_budget <= 0:
        trace = _base_trace(
            contract,
            failure_reason="effective_prompt_budget_unusable",
            profile_trace=profile_trace,
            effective_hard_input_budget=effective_budget,
        )
        raise PromptBudgetError("effective_prompt_budget_unusable", trace)
    return effective_budget, profile_trace


def _profile_clamp_trace(value: Any, model_budget: int) -> dict[str, Any]:
    supplied = None
    valid = False
    applied = False
    warning = None
    if isinstance(value, dict):
        supplied = value.get("max_input_tokens")
    elif value is not None:
        warning = "malformed_profile_prompt_budget"

    if supplied is None:
        return {
            "supplied": value is not None,
            "supplied_max_input_tokens": None,
            "valid": False,
            "applied": False,
            "warning": warning,
        }
    if isinstance(supplied, int) and not isinstance(supplied, bool) and supplied > 0:
        valid = True
        applied = supplied < model_budget
        warning = None if applied or supplied == model_budget else "profile_clamp_not_narrower"
    else:
        warning = "invalid_profile_prompt_budget"
    return {
        "supplied": True,
        "supplied_max_input_tokens": supplied if isinstance(supplied, int) else None,
        "valid": valid,
        "applied": applied,
        "warning": warning,
    }


def _base_trace(
    contract: PromptBudgetContract,
    *,
    failure_reason: str | None = None,
    profile_trace: dict[str, Any] | None = None,
    effective_hard_input_budget: int | None = None,
) -> dict[str, Any]:
    limits = [
        attempt.max_context_tokens
        for attempt in contract.attempts
        if isinstance(attempt.max_context_tokens, int)
        and not isinstance(attempt.max_context_tokens, bool)
        and attempt.max_context_tokens > 0
    ]
    profile_trace = profile_trace or _profile_clamp_trace(
        contract.profile_prompt_budget,
        min(limits) - contract.output_token_reserve - contract.context_safety_margin
        if limits
        else 0,
    )
    return {
        "budget_contract_version": BUDGET_CONTRACT_VERSION,
        "estimator": {"id": ESTIMATOR_ID, "exact_provider_tokenizer": False},
        "attempts": [attempt.to_trace() for attempt in contract.attempts],
        "effective_min_context_limit": min(limits) if limits else None,
        "output_token_reserve": contract.output_token_reserve,
        "context_safety_margin": contract.context_safety_margin,
        "profile_clamp": profile_trace,
        "effective_hard_input_budget": effective_hard_input_budget,
        "estimated_global_prompt_overhead_tokens": ESTIMATED_GLOBAL_PROMPT_OVERHEAD_TOKENS,
        "failure_reason": failure_reason,
    }


def prompt_budget_failure_trace(
    *,
    contract: PromptBudgetContract,
    failure_reason: str,
) -> dict[str, Any]:
    return _base_trace(contract, failure_reason=failure_reason)


def prompt_budget_trace(
    *,
    contract: PromptBudgetContract,
    effective_budget: int,
    profile_trace: dict[str, Any],
    before_messages: list[dict[str, str]],
    after_messages: list[dict[str, str]],
    before_layers: list[dict[str, Any]],
    after_layers: list[dict[str, Any]],
    dropped: list[dict[str, Any]],
    reason: str,
    required_preserved: bool,
    current_turn_preserved: bool,
) -> dict[str, Any]:
    before_estimate = estimate_prompt_tokens(before_messages)
    after_estimate = estimate_prompt_tokens(after_messages)
    reasons: dict[str, int] = {}
    layers: dict[str, int] = {}
    for item in dropped:
        reason_code = str(item.get("reason") or "unknown")
        reasons[reason_code] = reasons.get(reason_code, 0) + 1
        layer = str(item.get("layer") or "unknown")
        layers[layer] = layers.get(layer, 0) + 1
    trace = _base_trace(
        contract,
        profile_trace=profile_trace,
        effective_hard_input_budget=effective_budget,
    )
    trace.update(
        {
            "estimated_tokens_before_budgeting": before_estimate,
            "estimated_tokens_after_budgeting": after_estimate,
            "estimated_global_prompt_overhead_tokens": ESTIMATED_GLOBAL_PROMPT_OVERHEAD_TOKENS,
            "final_within_budget": after_estimate <= effective_budget,
            "omission_or_truncation_occurred": bool(dropped),
            "required_content_preserved": required_preserved,
            "current_turn_preserved": current_turn_preserved,
            "status": reason,
            "per_layer": _layer_budget_summary(before_layers, after_layers),
            "dropped_context": {
                "total_count": len(dropped),
                "by_reason": reasons,
                "by_layer": layers,
            },
        }
    )
    return trace


def _layer_budget_summary(
    before_layers: list[dict[str, Any]],
    after_layers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    after_by_name = {layer.get("name"): layer for layer in after_layers}
    rows: list[dict[str, Any]] = []
    for before in before_layers:
        name = before.get("name")
        after = after_by_name.get(name, {})
        before_messages = before.get("_messages", [])
        after_messages = after.get("_messages", [])
        before_estimated_tokens = estimate_messages_tokens(before_messages)
        after_estimated_tokens = estimate_messages_tokens(after_messages)
        rows.append(
            {
                "name": name,
                "before_estimated_tokens": before_estimated_tokens,
                "after_estimated_tokens": after_estimated_tokens,
                "before_message_count": len(before_messages),
                "after_message_count": len(after_messages),
            }
        )
    rows.append(
        {
            "name": "global_prompt_framing",
            "before_estimated_tokens": ESTIMATED_GLOBAL_PROMPT_OVERHEAD_TOKENS,
            "after_estimated_tokens": ESTIMATED_GLOBAL_PROMPT_OVERHEAD_TOKENS,
            "before_message_count": 0,
            "after_message_count": 0,
        }
    )
    return rows
