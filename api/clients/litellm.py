from __future__ import annotations

from typing import Any

import httpx


class LiteLLMClient:
    def __init__(self, base_url: str, api_key: str | None = None, timeout_ms: int = 30000) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout_ms / 1000

    async def chat(
        self,
        *,
        request_id: str,
        model: str,
        messages: list[dict[str, str]],
    ) -> dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if request_id:
            headers["X-Request-ID"] = request_id

        payload = {"model": model, "messages": messages}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(
                f"{self.base_url}/v1/chat/completions",
                headers=headers,
                json=payload,
            )
            resp.raise_for_status()
            return resp.json()
