import importlib

import httpx
import pytest


def _load_main(monkeypatch):
    monkeypatch.setenv("ORCH_API_KEY", "orch-test")
    monkeypatch.setenv("MEMORY_STORE_BASE_URL", "http://memory")
    monkeypatch.setenv("MEMORY_STORE_API_KEY", "memory")
    monkeypatch.setenv("LITELLM_BASE_URL", "http://litellm")
    monkeypatch.setenv("DSA_ENABLED", "true")
    monkeypatch.setenv("DSA_BASE_URL", "http://dsa")

    import settings

    settings.get_settings.cache_clear()
    import main

    return importlib.reload(main)


def _full_chat_payload(**overrides):
    payload = {
        "owner_id": "owner",
        "client_id": "node-red",
        "surface": "node_red",
        "surface_context": {
            "surface_type": "node_red",
            "interaction_mode": "text",
            "spoken_output": False,
            "active_task_mode": False,
            "output_format": "markdown",
        },
        "messages": [
            {
                "role": "user",
                "content": "Do I have any vehicle maintenance records for the battery?",
            }
        ],
        "requested_profile": "default",
        "sensitivity": "private",
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "expected_enabled", "expected_external_context"),
    [
        (_full_chat_payload(), False, None),
        (_full_chat_payload(external_context_enabled=True), True, None),
        (
            _full_chat_payload(
                external_context={
                    "enabled": True,
                    "source_ids": ["example_source"],
                    "max_results": 5,
                }
            ),
            False,
            {
                "allowed_sensitivity": None,
                "domain_tags": None,
                "enabled": True,
                "source_ids": ["example_source"],
                "max_results": 5,
            },
        ),
    ],
)
async def test_chat_endpoint_preserves_request_level_external_context_contract(
    monkeypatch,
    payload,
    expected_enabled,
    expected_external_context,
):
    main = _load_main(monkeypatch)
    captured_payloads = []

    async def fake_orchestrate_chat(**kwargs):
        captured_payloads.append(kwargs["payload"])
        return {
            "request_id": "rid-chat-api",
            "conversation_id": "conv-1",
            "profile_name": "default",
            "selected_model": "gpt-4o-mini",
            "answer": "ok",
            "status": "ok",
            "sources": [],
        }

    monkeypatch.setattr(main, "orchestrate_chat", fake_orchestrate_chat)

    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/v1/chat",
            headers={"X-API-Key": "orch-test"},
            json=payload,
        )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert len(captured_payloads) == 1
    assert captured_payloads[0]["external_context_enabled"] is expected_enabled
    assert captured_payloads[0]["external_context"] == expected_external_context


@pytest.mark.asyncio
async def test_chat_endpoint_does_not_expose_orchestration_exception_text(monkeypatch):
    main = _load_main(monkeypatch)

    async def fake_orchestrate_chat(**kwargs):
        raise RuntimeError("PRIVATE-DIAGNOSTIC-SENTINEL-BMS-EXCEPTION")

    monkeypatch.setattr(main, "orchestrate_chat", fake_orchestrate_chat)

    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/v1/chat",
            headers={"X-API-Key": "orch-test"},
            json=_full_chat_payload(),
        )

    assert response.status_code == 500
    body = response.json()
    assert body["status"] == "failed"
    assert body["error"] == {
        "code": "orchestration_error",
        "message": "The chat request could not be completed.",
    }
    assert "PRIVATE-DIAGNOSTIC-SENTINEL" not in str(body)
