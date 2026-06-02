from __future__ import annotations

from typing import Any

import httpx


class RuntimeClient:
    def __init__(self, base_url: str, api_key: str | None, timeout_ms: int = 30000) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout_ms / 1000

    async def _post(self, path: str, *, json: dict[str, Any]) -> dict[str, Any]:
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(f"{self.base_url}{path}", headers=headers, json=json)
            resp.raise_for_status()
            return resp.json()

    async def overlay(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/runtime/overlay",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "surface": surface,
            },
        )

    async def compile_companion_policy(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        requested_scene: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if requested_scene is not None:
            payload["requested_scene"] = requested_scene
        return await self._post("/v1/companion/policy/compile", json=payload)

    async def evaluate_interrupt(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        current_user_text: str | None = None,
        recent_messages: list[dict[str, Any]] | None = None,
        requested_scene: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if current_user_text is not None:
            payload["current_user_text"] = current_user_text
        if recent_messages is not None:
            payload["recent_messages"] = recent_messages
        if requested_scene is not None:
            payload["requested_scene"] = requested_scene
        return await self._post("/v1/interrupt/evaluate", json=payload)

    async def reset(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        reason: str,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/runtime/state/reset",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "surface": surface,
                "reason": reason,
            },
        )
