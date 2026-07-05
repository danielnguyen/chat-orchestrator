from __future__ import annotations

import os
import re
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response

app = FastAPI(title="Wave 3B BMS recording observer")

_UPSTREAM = os.getenv("BMS_UPSTREAM_URL", "http://bms:8000").rstrip("/")
_RETRIEVE_RE = re.compile(r"^/v2/conversations/([^/]+)/retrieve$")
_records: list[dict[str, Any]] = []
_forwarded_count = 0


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/fixture/reset")
async def fixture_reset() -> dict[str, str]:
    global _forwarded_count
    _records.clear()
    _forwarded_count = 0
    return {"status": "ok"}


@app.get("/fixture/requests")
async def fixture_requests() -> dict[str, Any]:
    return {
        "request_count": len(_records),
        "forwarded_count": _forwarded_count,
        "requests": list(_records),
    }


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def proxy(path: str, request: Request) -> Response:
    global _forwarded_count

    raw_body = await request.body()
    incoming_path = "/" + path
    capture_match = _RETRIEVE_RE.match(incoming_path)
    should_capture = request.method.upper() == "POST" and capture_match is not None
    captured_body: Any | None = None
    if should_capture:
        try:
            captured_body = await request.json()
        except Exception:
            captured_body = None

    headers = {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in {"host", "content-length"}
    }
    url = f"{_UPSTREAM}{incoming_path}"
    if request.url.query:
        url = f"{url}?{request.url.query}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        upstream = await client.request(
            request.method,
            url,
            content=raw_body,
            headers=headers,
        )

    if should_capture:
        _forwarded_count += 1
        _records.append(
            {
                "method": request.method.upper(),
                "path": incoming_path,
                "conversation_id": capture_match.group(1) if capture_match else None,
                "request_id": request.headers.get("x-request-id"),
                "headers": {
                    "content-type": request.headers.get("content-type"),
                    "x-request-id": request.headers.get("x-request-id"),
                },
                "body": captured_body,
                "forward_status": upstream.status_code,
            }
        )

    response_headers = {
        key: value
        for key, value in upstream.headers.items()
        if key.lower() in {"content-type", "x-request-id"}
    }
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=response_headers,
    )
