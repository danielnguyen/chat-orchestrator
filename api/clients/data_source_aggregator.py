from __future__ import annotations

from typing import Any

import httpx


class DataSourceAggregatorClient:
    def __init__(
        self,
        base_url: str,
        timeout_ms: int = 5000,
        api_key: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout_ms / 1000
        self.api_key = api_key

    def _build_headers(self) -> dict[str, str] | None:
        if not self.api_key:
            return None
        return {"X-API-Key": self.api_key}

    async def _post(self, path: str, *, json: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}{path}",
                json=json,
                headers=self._build_headers(),
            )
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
        normalized_source_ids = source_ids or None
        normalized_domain_tags = domain_tags or None
        return await self._post(
            "/v1/context-pack",
            json={
                "query": query,
                "source_ids": normalized_source_ids,
                "domain_tags": normalized_domain_tags,
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
