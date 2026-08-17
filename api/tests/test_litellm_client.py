from __future__ import annotations

import httpx
import pytest
from clients.litellm import LiteLLMClient


@pytest.mark.asyncio
async def test_litellm_chat_includes_capability_descriptors_as_tools(monkeypatch):
    client = LiteLLMClient("http://provider.local", "secret")
    captured: dict[str, object] = {}
    tools = [
        {
            "type": "function",
            "function": {"name": "draft_local_message", "parameters": {"type": "object"}},
        }
    ]

    async def fake_post(self, url, *, json=None, headers=None, **kwargs):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "ok"}}]},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    response = await client.chat(
        request_id="rid",
        model="gpt-test",
        messages=[{"role": "user", "content": "hello"}],
        tools=tools,
    )

    assert response["choices"][0]["message"]["content"] == "ok"
    assert captured["url"] == "http://provider.local/v1/chat/completions"
    assert captured["headers"]["Authorization"] == "Bearer secret"
    assert captured["headers"]["X-Request-ID"] == "rid"
    assert captured["json"] == {
        "model": "gpt-test",
        "messages": [{"role": "user", "content": "hello"}],
        "tools": tools,
        "tool_choice": "auto",
    }


@pytest.mark.asyncio
async def test_litellm_chat_ordinary_payload_is_unchanged_without_optional_fields(
    monkeypatch,
):
    client = LiteLLMClient("http://provider.local")
    captured = {}

    async def fake_post(self, url, *, json=None, headers=None, **kwargs):
        captured.update(url=url, json=json, headers=headers)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "ok"}}]},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    await client.chat(
        request_id="ordinary-request",
        model="ordinary-model",
        messages=[{"role": "user", "content": "ordinary turn"}],
    )

    assert captured["json"] == {
        "model": "ordinary-model",
        "messages": [{"role": "user", "content": "ordinary turn"}],
    }


@pytest.mark.asyncio
async def test_litellm_chat_supports_strict_bounded_classifier_request(monkeypatch):
    client = LiteLLMClient("http://provider.local", timeout_ms=30000)
    captured = {}
    response_format = {
        "type": "json_schema",
        "json_schema": {
            "name": "intent",
            "strict": True,
            "schema": {"type": "object", "additionalProperties": False},
        },
    }

    async def fake_post(self, url, *, json=None, headers=None, **kwargs):
        captured.update(json=json, headers=headers, timeout=self.timeout.read)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": "{}"}}]},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    await client.chat(
        request_id="classifier-request",
        model="gpt-5-mini",
        messages=[{"role": "user", "content": "Where did that come from?"}],
        response_format=response_format,
        max_completion_tokens=512,
        reasoning_effort="minimal",
        timeout_ms=3000,
    )

    assert captured == {
        "json": {
            "model": "gpt-5-mini",
            "messages": [
                {"role": "user", "content": "Where did that come from?"}
            ],
            "response_format": response_format,
            "max_completion_tokens": 512,
            "reasoning_effort": "minimal",
        },
        "headers": {
            "Content-Type": "application/json",
            "X-Request-ID": "classifier-request",
        },
        "timeout": 3.0,
    }
