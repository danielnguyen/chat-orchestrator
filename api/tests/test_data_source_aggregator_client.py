from __future__ import annotations

import pytest
import httpx
from clients.data_source_aggregator import DataSourceAggregatorClient


@pytest.mark.asyncio
async def test_context_pack_posts_expected_payload():
    client = DataSourceAggregatorClient("http://dsa.local", timeout_ms=1500)
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append((path, json))
        return {"items": []}

    client._post = fake_post  # type: ignore[method-assign]

    response = await client.context_pack(query="battery replacement")

    assert response == {"items": []}
    assert calls == [
        (
            "/v1/context-pack",
            {
                "query": "battery replacement",
                "source_ids": None,
                "domain_tags": None,
                "retrieval_mode": "targeted",
                "allowed_sensitivity": "medium",
                "budget": {
                    "max_results": 5,
                    "max_bytes": 50000,
                    "max_text_chars": 12000,
                },
            },
        )
    ]


@pytest.mark.asyncio
async def test_context_pack_posts_targeting_and_budget_overrides():
    client = DataSourceAggregatorClient("http://dsa.local", timeout_ms=1500)
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append((path, json))
        return {"items": []}

    client._post = fake_post  # type: ignore[method-assign]

    await client.context_pack(
        query="battery replacement",
        source_ids=["vehicle_log_primary"],
        domain_tags=["vehicle", "maintenance"],
        allowed_sensitivity="low",
        budget={
            "max_results": 2,
            "max_bytes": 50000,
            "max_text_chars": 12000,
        },
    )

    assert calls == [
        (
            "/v1/context-pack",
            {
                "query": "battery replacement",
                "source_ids": ["vehicle_log_primary"],
                "domain_tags": ["vehicle", "maintenance"],
                "retrieval_mode": "targeted",
                "allowed_sensitivity": "low",
                "budget": {
                    "max_results": 2,
                    "max_bytes": 50000,
                    "max_text_chars": 12000,
                },
            },
        )
    ]


@pytest.mark.asyncio
async def test_client_includes_api_key_header_when_configured(monkeypatch):
    client = DataSourceAggregatorClient(
        "http://dsa.local",
        timeout_ms=5000,
        api_key="dsa-secret",
    )
    captured: dict[str, object] = {}

    async def fake_post(self, url, *, json=None, headers=None, **kwargs):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        return httpx.Response(200, json={"items": []}, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    response = await client.context_pack(query="battery replacement")

    assert response == {"items": []}
    assert captured["url"] == "http://dsa.local/v1/context-pack"
    assert captured["headers"] == {"X-API-Key": "dsa-secret"}


@pytest.mark.asyncio
async def test_client_omits_api_key_header_when_not_configured(monkeypatch):
    client = DataSourceAggregatorClient("http://dsa.local", timeout_ms=5000)
    captured: dict[str, object] = {}

    async def fake_post(self, url, *, json=None, headers=None, **kwargs):
        captured["headers"] = headers
        return httpx.Response(200, json={"items": []}, request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    await client.context_pack(query="battery replacement")

    assert captured["headers"] is None
