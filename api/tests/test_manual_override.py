from pathlib import Path

from router.engine import evaluate_route

ROOT = Path(__file__).resolve().parents[1]


def test_manual_override_rule_uses_override_model():
    out = evaluate_route(
        rules_path=str(ROOT / "router" / "rules.yaml"),
        model_registry_path=str(ROOT / "router" / "model_registry.yaml"),
        signals={"sensitivity": "private", "has_code": False, "model_override_present": True},
        model_override="gpt-4o-mini",
    )
    assert out["rule_id"] == "override"
    assert out["selected_model"] == "gpt-4o-mini"
