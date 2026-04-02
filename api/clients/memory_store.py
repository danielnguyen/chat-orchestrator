from __future__ import annotations

from typing import Any

import httpx


class MemoryStoreClient:
    def __init__(self, base_url: str, api_key: str, timeout_ms: int = 30000) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout_ms / 1000

    async def _post(
        self,
        path: str,
        *,
        request_id: str | None = None,
        json: dict[str, Any],
    ) -> dict[str, Any]:
        headers = {"X-API-Key": self.api_key}
        if request_id:
            headers["X-Request-ID"] = request_id
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(f"{self.base_url}{path}", headers=headers, json=json)
            resp.raise_for_status()
            return resp.json()

    async def _get(self, path: str) -> dict[str, Any]:
        headers = {"X-API-Key": self.api_key}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(f"{self.base_url}{path}", headers=headers)
            resp.raise_for_status()
            return resp.json()

    async def resolve_conversation(
        self,
        *,
        owner_id: str,
        client_id: str | None,
        title: str | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/conversations/resolve",
            json={"owner_id": owner_id, "client_id": client_id, "title": title},
        )

    async def add_message(
        self,
        *,
        conversation_id: str,
        owner_id: str,
        role: str,
        content: str,
        client_id: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            f"/v1/conversations/{conversation_id}/messages",
            json={
                "owner_id": owner_id,
                "role": role,
                "content": content,
                "client_id": client_id,
                "metadata": metadata,
            },
        )

    async def retrieve_bundle(
        self,
        *,
        request_id: str,
        conversation_id: str,
        owner_id: str,
        query: str,
        retrieval: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return await self._post(
            f"/v2/conversations/{conversation_id}/retrieve",
            request_id=request_id,
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "query": query,
                "retrieval": retrieval,
            },
        )

    async def resolve_profile(
        self,
        *,
        owner_id: str,
        surface: str,
        requested_profile: str | None,
        client_id: str | None,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/profiles/resolve",
            json={
                "owner_id": owner_id,
                "surface": surface,
                "requested_profile": requested_profile,
                "client_id": client_id,
            },
        )

    async def create_trace(self, *, request_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("/v1/traces", request_id=request_id, json=payload)

    async def get_trace(self, request_id: str) -> dict[str, Any]:
        return await self._get(f"/v1/traces/{request_id}")
