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
@pytest.mark.parametrize("completion_failure", [False, True])
async def test_chat_api_durable_work_keeps_synchronous_contract(
    monkeypatch, tmp_path, completion_failure,
):
    from test_orchestrate_flow import (
        DurableWorkMemoryStore,
        FakeLiteLLM,
        FakeRuntime,
        _write_router_files,
    )

    main = _load_main(monkeypatch)
    rules, models = _write_router_files(tmp_path)
    memory = DurableWorkMemoryStore(fail_at="completed" if completion_failure else None)
    provider = FakeLiteLLM(content="Canonical synchronous answer.")
    monkeypatch.setattr(main, "memory_store", memory)
    monkeypatch.setattr(main, "litellm", provider)
    monkeypatch.setattr(main, "runtime", FakeRuntime())
    monkeypatch.setattr(main.settings, "router_rules_path", str(rules))
    monkeypatch.setattr(main.settings, "model_registry_path", str(models))
    transport = httpx.ASGITransport(app=main.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/v1/chat", headers={"X-API-Key": "orch-test"},
            json=_full_chat_payload(messages=[{"role": "user", "content": "Hello."}]),
        )
    result = response.json()
    assert memory.work["request_id"] == result["request_id"]
    assert len(provider.calls) == 1
    assert [m["role"] for m in memory.added_messages] == ["user", "assistant"]
    assert "work_id" not in result
    if completion_failure:
        assert response.status_code == 500
        assert result["error"]["code"] == "orchestration_error"
        assert memory.work["state"] == "running"
        assert "answer" not in result
    else:
        assert response.status_code == 200
        assert result["answer"] == "Canonical synchronous answer."
        assert set(result) == {
            "request_id", "conversation_id", "profile_name", "selected_model",
            "answer", "status", "sources",
        }
        assert memory.work["state"] == "completed"
        assert memory.work["conversation_id"] == result["conversation_id"]
        assert memory.work["assistant_message_id"] == "00000000-0000-4000-8000-000000000002"


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
    assert set(response.json()) == {
        "request_id", "conversation_id", "profile_name", "selected_model",
        "answer", "status", "sources",
    }
    assert len(captured_payloads) == 1
    assert captured_payloads[0]["external_context_enabled"] is expected_enabled
    assert captured_payloads[0]["external_context"] == expected_external_context


@pytest.mark.asyncio
async def test_chat_endpoint_passes_independent_classifier_timeouts(monkeypatch):
    monkeypatch.setenv("INTENT_CLASSIFIER_TIMEOUT_MS", "1111")
    monkeypatch.setenv("EVIDENCE_INTERPRETER_TIMEOUT_MS", "4321")
    main = _load_main(monkeypatch)
    captured_kwargs = []

    async def fake_orchestrate_chat(**kwargs):
        captured_kwargs.append(kwargs)
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
            json=_full_chat_payload(),
        )

    assert response.status_code == 200
    assert main.settings.intent_classifier_timeout_ms == 1111
    assert main.settings.evidence_interpreter_timeout_ms == 4321
    assert captured_kwargs[0]["intent_classifier_timeout_ms"] == 1111
    assert captured_kwargs[0]["evidence_interpreter_timeout_ms"] == 4321


def test_classifier_timeout_defaults_are_independent(monkeypatch):
    main = _load_main(monkeypatch)

    assert main.settings.intent_classifier_timeout_ms == 3000
    assert main.settings.evidence_interpreter_timeout_ms == 5000


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
