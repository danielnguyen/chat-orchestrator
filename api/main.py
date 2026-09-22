from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
from clients.data_source_aggregator import DataSourceAggregatorClient
from clients.litellm import LiteLLMClient
from clients.memory_store import MemoryStoreClient
from clients.runtime import RuntimeClient
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.responses import JSONResponse
from fastapi.security.api_key import APIKeyHeader
from models import (
    BriefGenerateRequest,
    BriefGenerateResponse,
    ChatRequest,
    ChatResponse,
    DeferredChatResponse,
)
from services.briefing import generate_brief
from services.orchestrate import orchestrate_chat
from settings import get_settings

settings = get_settings()
_chat_logger = logging.getLogger("uvicorn.error.chat_orchestrator.chat")
_owned_chat_tasks: set[asyncio.Task] = set()


def _chat_task_done(task: asyncio.Task) -> None:
    _owned_chat_tasks.discard(task)
    if not task.cancelled() and task.exception() is not None:
        _chat_logger.warning("owned_chat_execution_failed")


async def _await_chat_delivery(
    cognition: Coroutine[Any, Any, dict[str, Any]],
    admitted: asyncio.Future,
    body: ChatRequest,
) -> dict[str, Any] | DeferredChatResponse:
    # One process owns this exact invocation, independently of the HTTP waiter.
    task = asyncio.create_task(cognition)
    _owned_chat_tasks.add(task)
    task.add_done_callback(_chat_task_done)
    # wait() does not propagate waiter cancellation to these independently owned tasks.
    await asyncio.wait({task, admitted}, return_when=asyncio.FIRST_COMPLETED)
    if task.done():
        return task.result()
    work, admitted_at = admitted.result()
    remaining = max(
        0, admitted_at + body.delivery_wait_ms / 1000 - asyncio.get_running_loop().time(),
    )
    # asyncio.wait distinguishes delivery expiry from a TimeoutError in cognition.
    done, _ = await asyncio.wait({task}, timeout=remaining)
    if done:
        return task.result()
    if body.client_id is not None:
        try:
            await memory_store.set_current_work(
                owner_id=work["owner_id"], client_id=body.client_id, work_id=work["work_id"],
            )
        except Exception:
            _chat_logger.warning("deferred_locator_unconfirmed")
            return await asyncio.shield(task)
    return DeferredChatResponse(
        request_id=work["request_id"], conversation_id=work["conversation_id"],
        work_id=work["work_id"],
    )


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    runtime_client = runtime
    try:
        if runtime_client is not None:
            await runtime_client.open()
            await runtime_client.reconcile_interrupted_turns(str(uuid4()))
        await memory_store.reconcile_interrupted_work()
        yield
    finally:
        tasks = tuple(_owned_chat_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if runtime_client is not None:
            await runtime_client.close()


app = FastAPI(title="Chat Orchestrator", version="0.1.0", lifespan=lifespan)
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
dsa = (
    DataSourceAggregatorClient(
        base_url=settings.dsa_base_url,
        timeout_ms=settings.dsa_timeout_ms,
        api_key=settings.dsa_api_key,
    )
    if settings.dsa_enabled
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
    responses={202: {"model": DeferredChatResponse}},
    dependencies=[Depends(require_api_key)],
)
async def chat(body: ChatRequest) -> ChatResponse:
    request_id = str(uuid4())
    _chat_logger.info(
        "chat_request_started component=chat-orchestrator request_id=%s",
        request_id,
    )
    try:
        eligible = (
            body.allow_deferred and body.delivery_wait_ms is not None
            and runtime is not None
            and not settings.cognitive_runtime_capability_registry_enabled
            and body.capability_confirmation is None
        )
        admitted = asyncio.get_running_loop().create_future() if eligible else None

        def on_work_admitted(work: dict[str, Any]) -> None:
            if admitted is not None and not admitted.done():
                admitted.set_result((work, asyncio.get_running_loop().time()))

        cognition = orchestrate_chat(
            payload=body.model_dump(exclude={"allow_deferred", "delivery_wait_ms"}),
            memory_store=memory_store,
            litellm=litellm,
            runtime=runtime,
            rules_path=settings.router_rules_path,
            model_registry_path=settings.model_registry_path,
            allow_manual_override=settings.allow_manual_override,
            enable_runtime_overlays=settings.enable_runtime_overlays,
            companion_policy_enabled=settings.cognitive_runtime_companion_enabled,
            interaction_governance_enabled=(
                settings.cognitive_runtime_interaction_governance_enabled
            ),
            persona_containment_enabled=(
                settings.cognitive_runtime_persona_containment_enabled
            ),
            restraint_enabled=settings.cognitive_runtime_restraint_enabled,
            memory_hygiene_enabled=settings.cognitive_runtime_memory_hygiene_enabled,
            privacy_context_enabled=settings.cognitive_runtime_privacy_context_enabled,
            capability_registry_enabled=settings.cognitive_runtime_capability_registry_enabled,
            claim_record_capture_enabled=settings.claim_record_capture_enabled,
            evidence_acquisition_enabled=settings.evidence_acquisition_enabled,
            history_followup_enabled=settings.history_followup_enabled,
            intent_classifier_timeout_ms=settings.intent_classifier_timeout_ms,
            evidence_interpreter_timeout_ms=(
                settings.evidence_interpreter_timeout_ms
            ),
            response_action_mode=settings.response_action_mode,
            interrupt_policy_mode=body.interrupt_policy_mode,
            dsa=dsa,
            dsa_enabled=settings.dsa_enabled,
            prompt_output_token_reserve=settings.prompt_output_token_reserve,
            prompt_context_safety_margin=settings.prompt_context_safety_margin,
            request_id=request_id,
            on_work_admitted=on_work_admitted if eligible else None,
        )
        result = (
            await _await_chat_delivery(cognition, admitted, body)
            if eligible else await cognition
        )
        if isinstance(result, DeferredChatResponse):
            return JSONResponse(status_code=202, content=result.model_dump())
        response = ChatResponse(**result)
        _chat_logger.info(
            "chat_request_completed component=chat-orchestrator request_id=%s status=%s",
            request_id,
            response.status,
        )
        return response
    except Exception:
        _chat_logger.info(
            "chat_request_failed component=chat-orchestrator request_id=%s "
            "error_category=orchestration_exception",
            request_id,
        )
        return JSONResponse(
            status_code=500,
            content={
                "request_id": request_id,
                "status": "failed",
                "error": {
                    "code": "orchestration_error",
                    "message": "The chat request could not be completed.",
                },
            },
        )
