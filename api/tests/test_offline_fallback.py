from services.fallback import choose_fallback


def test_choose_fallback_returns_first_option():
    route = {"fallbacks": [{"selected_model": "local-llm", "provider": "local"}]}
    out = choose_fallback(route)
    assert out and out["selected_model"] == "local-llm"
