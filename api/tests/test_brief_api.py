import importlib

from fastapi.testclient import TestClient


def _load_main(monkeypatch):
    monkeypatch.setenv("ORCH_API_KEY", "orch-test")
    monkeypatch.setenv("MEMORY_STORE_BASE_URL", "http://memory")
    monkeypatch.setenv("MEMORY_STORE_API_KEY", "memory")
    monkeypatch.setenv("LITELLM_BASE_URL", "http://litellm")

    import settings

    settings.get_settings.cache_clear()
    import main

    return importlib.reload(main)


def test_brief_generate_endpoint_with_valid_key_and_no_extra_brief_apis(monkeypatch):
    main = _load_main(monkeypatch)
    client = TestClient(main.app)

    response = client.post(
        "/v1/brief/generate",
        headers={"X-API-Key": "orch-test"},
        json={
            "content": "Risk: output could feel too rigid. Next: keep brief mode opt-in.",
            "structured": {
                "net_assessment": "Ship the deterministic first slice.",
                "primary_recommendation": "Expose generate only.",
            },
            "brief_type": "recommendation",
            "depth_level": 2,
            "surface": "mobile",
            "source_context": {"request_id": "rid-api-test"},
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"rendered", "brief", "debug"}
    assert body["rendered"].startswith("Net: Ship the deterministic first slice.")
    assert body["brief"]["net_assessment"] == "Ship the deterministic first slice."
    assert body["brief"]["top_risk"] == "output could feel too rigid"
    assert body["debug"]["brief_type"] == "recommendation"
    assert body["debug"]["depth_level"] == 2
    assert body["debug"]["surface"] == "telegram"
    assert body["debug"]["formatter"] == "telegram"
    assert body["debug"]["source_context"] == {"request_id": "rid-api-test"}

    unauthorized = client.post("/v1/brief/generate", json={"content": "Net: no auth."})
    assert unauthorized.status_code == 401

    expand = client.post(
        "/v1/brief/expand",
        headers={"X-API-Key": "orch-test"},
        json={},
    )
    templates = client.get(
        "/v1/brief/templates",
        headers={"X-API-Key": "orch-test"},
    )
    assert expand.status_code == 404
    assert templates.status_code == 404
