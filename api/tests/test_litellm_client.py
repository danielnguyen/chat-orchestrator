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
            "function": {"name": "draft.local_message", "parameters": {"type": "object"}},
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
