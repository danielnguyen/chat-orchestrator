from __future__ import annotations

from typing import Any

import httpx


class DataSourceAggregatorClient:
    def __init__(self, base_url: str, timeout_ms: int = 1500) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout_ms / 1000

    async def _post(self, path: str, *, json: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(f"{self.base_url}{path}", json=json)
            resp.raise_for_status()
            return resp.json()

    async def context_pack(
        self,
        *,
        query: str,
        source_ids: list[str] | None = None,
        domain_tags: list[str] | None = None,
        retrieval_mode: str = "targeted",
        allowed_sensitivity: str = "medium",
        budget: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/context-pack",
            json={
                "query": query,
                "source_ids": source_ids,
                "domain_tags": domain_tags,
                "retrieval_mode": retrieval_mode,
                "allowed_sensitivity": allowed_sensitivity,
                "budget": budget
                or {
                    "max_results": 5,
                    "max_bytes": 50000,
                    "max_text_chars": 12000,
                },
            },
        )
