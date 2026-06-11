from __future__ import annotations

import pytest
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
