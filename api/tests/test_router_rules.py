from pathlib import Path

from router.engine import evaluate_route

ROOT = Path(__file__).resolve().parents[1]


def test_router_selects_local_for_local_only():
    out = evaluate_route(
        rules_path=str(ROOT / "router" / "rules.yaml"),
        model_registry_path=str(ROOT / "router" / "model_registry.yaml"),
        signals={"sensitivity": "local_only", "has_code": False, "model_override_present": False},
        model_override=None,
    )
    assert out["rule_id"] == "local-only"
    assert out["selected_model"] == "chat_local_fast"
