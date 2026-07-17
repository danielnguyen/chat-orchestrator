from __future__ import annotations

import httpx
import pytest
from clients.data_source_aggregator import DataSourceAggregatorClient


@pytest.mark.asyncio
async def test_list_sources_gets_inventory():
    client = DataSourceAggregatorClient("http://dsa.local", timeout_ms=1500)
    calls = []

    async def fake_get(path):
        calls.append(path)
        return {"sources": []}

    client._get = fake_get  # type: ignore[method-assign]

    assert await client.list_sources() == {"sources": []}
    assert calls == ["/v1/sources"]


@pytest.mark.asyncio
async def test_list_sources_preserves_api_key_header(monkeypatch):
    client = DataSourceAggregatorClient(
        "http://dsa.local",
        timeout_ms=1500,
        api_key="dsa-secret",
    )
    captured = {}

    async def fake_get(self, url, *, headers=None, **kwargs):
        captured["url"] = url
        captured["headers"] = headers
        return httpx.Response(200, json={"sources": []}, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)

    assert await client.list_sources() == {"sources": []}
    assert captured == {
        "url": "http://dsa.local/v1/sources",
        "headers": {"X-API-Key": "dsa-secret"},
    }


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
async def test_fetch_source_posts_exact_bounded_no_raw_payload():
    client = DataSourceAggregatorClient("http://dsa.local", timeout_ms=1500)
    calls: list[tuple[str, dict[str, object]]] = []

    async def fake_post(path: str, *, json: dict[str, object]):
        calls.append((path, json))
        return {"retrieval_mode": "fetch", "results": []}

    client._post = fake_post  # type: ignore[method-assign]

    response = await client.fetch_source(
        source_ref="connector:source-a:item-1",
    )

    assert response == {"retrieval_mode": "fetch", "results": []}
    assert calls == [
        (
            "/v1/sources/fetch",
            {
                "source_ref": "connector:source-a:item-1",
                "include_raw": False,
                "budget": {
                    "max_results": 1,
                    "max_bytes": 50000,
                    "max_text_chars": 12000,
                },
            },
        )
    ]


@pytest.mark.asyncio
async def test_fetch_source_preserves_headers_timeout_and_http_boundary(monkeypatch):
    captured: dict[str, object] = {}

    class FakeAsyncClient:
        def __init__(self, *, timeout):
            captured["timeout"] = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, *, json, headers):
            captured.update(url=url, json=json, headers=headers)
            return httpx.Response(
                503,
                json={"detail": "PRIVATE ERROR"},
                request=httpx.Request("POST", url),
            )

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    client = DataSourceAggregatorClient(
        "http://dsa.local",
        timeout_ms=1750,
        api_key="dsa-secret",
    )

    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_source(source_ref="connector:source-a:item-1")

    assert captured["timeout"] == 1.75
    assert captured["url"] == "http://dsa.local/v1/sources/fetch"
    assert captured["headers"] == {"X-API-Key": "dsa-secret"}
    assert captured["json"]["include_raw"] is False


@pytest.mark.asyncio
async def test_fetch_source_timeout_propagates_without_retry(monkeypatch):
    calls = 0

    class FakeAsyncClient:
        def __init__(self, *, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, *, json, headers):
            nonlocal calls
            calls += 1
            raise httpx.ReadTimeout("timed out", request=httpx.Request("POST", url))

    monkeypatch.setattr(httpx, "AsyncClient", FakeAsyncClient)
    client = DataSourceAggregatorClient("http://dsa.local", timeout_ms=1750)

    with pytest.raises(httpx.ReadTimeout):
        await client.fetch_source(source_ref="connector:source-a:item-1")
    assert calls == 1


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
