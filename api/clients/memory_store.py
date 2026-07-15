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

    async def _get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers = {"X-API-Key": self.api_key}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(
                f"{self.base_url}{path}",
                headers=headers,
                params=params,
            )
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
        policy_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "owner_id": owner_id,
            "role": role,
            "content": content,
            "client_id": client_id,
            "metadata": metadata,
        }
        if policy_metadata is not None:
            payload["policy_metadata"] = policy_metadata
        return await self._post(
            f"/v1/conversations/{conversation_id}/messages",
            json=payload,
        )

    async def retrieve_bundle(
        self,
        *,
        request_id: str,
        conversation_id: str,
        owner_id: str,
        query: str,
        retrieval: dict[str, Any] | None,
        include_artifacts: bool | None = None,
        allowed_memory_domains: list[str] | None = None,
        blocked_memory_domains: list[str] | None = None,
        containment_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "query": query,
            "mode": "augmented",
            "retrieval": retrieval,
        }
        if include_artifacts is not None:
            payload["include_artifacts"] = include_artifacts
        if containment_policy is not None:
            payload["containment_policy"] = containment_policy
        elif allowed_memory_domains:
            payload["allowed_memory_domains"] = allowed_memory_domains
        if containment_policy is None and blocked_memory_domains:
            payload["blocked_memory_domains"] = blocked_memory_domains
        response = await self._post(
            f"/v2/conversations/{conversation_id}/retrieve",
            request_id=request_id,
            json=payload,
        )
        response_request_id = response.get("request_id")
        if response_request_id is not None and response_request_id != request_id:
            raise RuntimeError("retrieval_request_id_mismatch")
        return response

    async def select_recall(
        self,
        *,
        request_id: str,
        owner_id: str,
        context: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        response = await self._post(
            "/v1/internal/recall/select",
            request_id=request_id,
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "context": context,
                "candidates": candidates,
            },
        )
        if response.get("request_id") != request_id or response.get("owner_id") != owner_id:
            raise RuntimeError("recall_response_context_mismatch")
        return response

    async def retrieve_episode_callbacks(
        self,
        *,
        request_id: str,
        owner_id: str,
        context: dict[str, Any],
        limit: int = 10,
    ) -> dict[str, Any]:
        response = await self._post(
            "/v1/internal/episodes/retrieve",
            request_id=request_id,
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "context": context,
                "limit": limit,
            },
        )
        if response.get("request_id") != request_id or response.get("owner_id") != owner_id:
            raise RuntimeError("episode_response_context_mismatch")
        return response

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

    async def create_claim_record(
        self,
        *,
        request_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/internal/claim-records",
            request_id=request_id,
            json=payload,
        )

    async def list_claim_records(
        self,
        *,
        owner_id: str,
        conversation_id: str,
        limit: int = 20,
    ) -> dict[str, Any]:
        if not 1 <= limit <= 20:
            raise ValueError("claim_record_limit_out_of_range")
        return await self._get(
            "/v1/internal/claim-records",
            params={
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "limit": limit,
            },
        )

    async def get_trace(self, request_id: str) -> dict[str, Any]:
        return await self._get(f"/v1/traces/{request_id}")
