from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def _read_yaml(path: str) -> dict[str, Any]:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def evaluate_route(
    *,
    rules_path: str,
    model_registry_path: str,
    signals: dict[str, Any],
    model_override: str | None,
) -> dict[str, Any]:
    rules_doc = _read_yaml(rules_path)
    _ = _read_yaml(model_registry_path)

    for rule in rules_doc.get("rules", []):
        predicate = rule.get("when", {})
        matched = True
        for k, v in predicate.items():
            if signals.get(k) != v:
                matched = False
                break
        if not matched:
            continue

        action = dict(rule.get("then", {}))
        if action.get("selected_model_from") == "model_override":
            if not model_override:
                continue
            action["selected_model"] = model_override
        action["rule_id"] = rule["id"]
        return action

    return {
        "selected_model": "gpt-4o-mini",
        "provider": "cloud",
        "rationale": "default fallback",
        "fallbacks": [{"selected_model": "local-llm", "provider": "local"}],
        "rule_id": "implicit-default",
    }
