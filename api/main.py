from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import httpx
from clients.litellm import LiteLLMClient
from clients.memory_store import MemoryStoreClient
from clients.runtime import RuntimeClient
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from models import BriefGenerateRequest, BriefGenerateResponse, ChatRequest, ChatResponse
from services.briefing import generate_brief
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
runtime = (
    RuntimeClient(
        base_url=settings.cognitive_runtime_base_url,
        api_key=settings.cognitive_runtime_api_key,
        timeout_ms=settings.cognitive_runtime_timeout_ms,
    )
    if settings.cognitive_runtime_base_url
    else None
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
    "/v1/brief/generate",
    response_model=BriefGenerateResponse,
    dependencies=[Depends(require_api_key)],
)
async def brief_generate(body: BriefGenerateRequest) -> BriefGenerateResponse:
    structured = body.structured.model_dump() if body.structured else None
    result = generate_brief(
        content=body.content,
        structured=structured,
        brief_type=body.brief_type,
        depth_level=body.depth_level,
        surface=body.surface,
        source="explicit_user_request",
        explicit_request=True,
    )
    debug = {**result.debug, "source_context": body.source_context}
    return BriefGenerateResponse(
        rendered=result.rendered,
        brief=result.brief.to_dict(),
        debug=debug,
    )


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
            runtime=runtime,
            rules_path=settings.router_rules_path,
            model_registry_path=settings.model_registry_path,
            allow_manual_override=settings.allow_manual_override,
            enable_runtime_overlays=settings.enable_runtime_overlays,
            companion_policy_enabled=settings.cognitive_runtime_companion_enabled,
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
