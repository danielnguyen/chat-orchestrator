from __future__ import annotations

import logging
from typing import Any

import httpx

_request_logger = logging.getLogger("chat_orchestrator.dsa")


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

    def _build_headers(self, *, request_id: str | None = None) -> dict[str, str] | None:
        headers: dict[str, str] = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        if request_id is not None:
            headers["X-Request-ID"] = request_id
        return headers or None

    async def _post(
        self,
        path: str,
        *,
        json: dict[str, Any],
        request_id: str | None = None,
    ) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                resp = await client.post(
                    f"{self.base_url}{path}",
                    json=json,
                    headers=self._build_headers(request_id=request_id),
                )
                resp.raise_for_status()
                result = resp.json()
            except httpx.HTTPStatusError as error:
                self._log_request(
                    request_id=request_id,
                    method="POST",
                    path=path,
                    status_code=error.response.status_code,
                    error_category="http_status",
                )
                raise
            except httpx.TimeoutException:
                self._log_request(
                    request_id=request_id,
                    method="POST",
                    path=path,
                    error_category="timeout",
                )
                raise
            except httpx.RequestError:
                self._log_request(
                    request_id=request_id,
                    method="POST",
                    path=path,
                    error_category="transport",
                )
                raise
            except ValueError:
                self._log_request(
                    request_id=request_id,
                    method="POST",
                    path=path,
                    status_code=resp.status_code,
                    error_category="response_decode",
                )
                raise
            self._log_request(
                request_id=request_id,
                method="POST",
                path=path,
                status_code=resp.status_code,
            )
            return result

    async def _get(
        self,
        path: str,
        *,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                resp = await client.get(
                    f"{self.base_url}{path}",
                    headers=self._build_headers(request_id=request_id),
                )
                resp.raise_for_status()
                result = resp.json()
            except httpx.HTTPStatusError as error:
                self._log_request(
                    request_id=request_id,
                    method="GET",
                    path=path,
                    status_code=error.response.status_code,
                    error_category="http_status",
                )
                raise
            except httpx.TimeoutException:
                self._log_request(
                    request_id=request_id,
                    method="GET",
                    path=path,
                    error_category="timeout",
                )
                raise
            except httpx.RequestError:
                self._log_request(
                    request_id=request_id,
                    method="GET",
                    path=path,
                    error_category="transport",
                )
                raise
            except ValueError:
                self._log_request(
                    request_id=request_id,
                    method="GET",
                    path=path,
                    status_code=resp.status_code,
                    error_category="response_decode",
                )
                raise
            self._log_request(
                request_id=request_id,
                method="GET",
                path=path,
                status_code=resp.status_code,
            )
            return result

    @staticmethod
    def _log_request(
        *,
        request_id: str | None,
        method: str,
        path: str,
        status_code: int | None = None,
        error_category: str | None = None,
    ) -> None:
        fields: list[object] = [request_id or "absent", method, path]
        message = (
            "dsa_request_completed component=chat-orchestrator "
            "request_id=%s method=%s path=%s"
        )
        if status_code is not None:
            message += " status=%d"
            fields.append(status_code)
        if error_category is not None:
            message += " error_category=%s"
            fields.append(error_category)
        _request_logger.info(message, *fields)

    async def list_sources(self, *, request_id: str | None = None) -> dict[str, Any]:
        return await self._get("/v1/sources", request_id=request_id)

    async def context_pack(
        self,
        *,
        query: str,
        source_ids: list[str] | None = None,
        domain_tags: list[str] | None = None,
        retrieval_mode: str = "targeted",
        allowed_sensitivity: str = "medium",
        budget: dict[str, int] | None = None,
        request_id: str | None = None,
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
            request_id=request_id,
        )

    async def fetch_source(
        self,
        *,
        source_ref: str,
        include_raw: bool = False,
        budget: dict[str, int] | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/sources/fetch",
            json={
                "source_ref": source_ref,
                "include_raw": include_raw,
                "budget": budget
                or {
                    "max_results": 1,
                    "max_bytes": 50000,
                    "max_text_chars": 12000,
                },
            },
            request_id=request_id,
        )

    async def context_source(
        self,
        *,
        source_ref: str,
        context_mode: str,
        budget: dict[str, int] | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/sources/context",
            json={
                "source_ref": source_ref,
                "context_mode": context_mode,
                "budget": budget
                or {
                    "max_rows": 5,
                    "max_bytes": 50000,
                    "max_text_chars": 12000,
                },
            },
            request_id=request_id,
        )
