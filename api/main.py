from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import httpx
from clients.litellm import LiteLLMClient
from clients.memory_store import MemoryStoreClient
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from models import ChatRequest, ChatResponse
from services.orchestrate import orchestrate_chat
from settings import get_settings

settings = get_settings()
app = FastAPI(title="Chat Orchestrator", version="0.1.0")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

memory_store = MemoryStoreClient(
    base_url=settings.memory_store_base_url,
    api_key=settings.memory_store_api_key,
    timeout_ms=settings.request_timeout_ms,
)
litellm = LiteLLMClient(
    base_url=settings.litellm_base_url,
    api_key=settings.litellm_api_key,
    timeout_ms=settings.request_timeout_ms,
)


async def require_api_key(api_key: str | None = Security(api_key_header)) -> None:
    if not api_key or api_key != settings.orch_api_key:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/healthz")
async def healthz() -> dict[str, object]:
    dependency_status = {"memory_store": "unknown"}
    try:
        async with httpx.AsyncClient(timeout=1.5) as client:
            r = await client.get(f"{settings.memory_store_base_url.rstrip('/')}/healthz")
            if r.status_code == 200:
                dependency_status["memory_store"] = "ok"
            else:
                dependency_status["memory_store"] = f"http_{r.status_code}"
    except Exception as e:  # best effort only
        dependency_status["memory_store"] = f"error:{type(e).__name__}"

    return {
        "status": "ok",
        "service": "chat-orchestrator",
        "time": datetime.now(UTC).isoformat(),
        "dependencies": dependency_status,
    }


@app.post(
    "/v1/chat",
    response_model=ChatResponse,
    dependencies=[Depends(require_api_key)],
)
async def chat(body: ChatRequest) -> ChatResponse:
    request_id = str(uuid4())
    try:
        result = await orchestrate_chat(
            payload=body.model_dump(),
            memory_store=memory_store,
            litellm=litellm,
            rules_path=settings.router_rules_path,
            model_registry_path=settings.model_registry_path,
            allow_manual_override=settings.allow_manual_override,
            request_id=request_id,
        )
        return ChatResponse(**result)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "request_id": request_id,
                "status": "failed",
                "error": {
                    "code": "orchestration_error",
                    "message": str(e),
                },
            },
        )
