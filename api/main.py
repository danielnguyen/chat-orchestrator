from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Coroutine
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import httpx
from clients.data_source_aggregator import DataSourceAggregatorClient
from clients.litellm import LiteLLMClient
from clients.memory_store import (
    MemoryStoreClient,
    _validate_conversation_projection,
    _validate_current_work,
    _validate_work_result,
)
from clients.runtime import RuntimeClient
from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security.api_key import APIKeyHeader
from models import (
    CHAT_FEEDBACK_MEDIA_TYPE,
    BriefGenerateRequest,
    BriefGenerateResponse,
    ChatFailureResponse,
    ChatRequest,
    ChatResponse,
    ChatTerminalRecord,
    CurrentWorkStatus,
    DeferredChatResponse,
    PresenceProjection,
    PresenceRuntimeBatch,
    ProcessingCueRecord,
    WorkIdentifier,
    WorkStatus,
    WorkUUID,
)
from services.briefing import generate_brief
from services.orchestrate import orchestrate_chat
from services.surface_presence import publish_surface_presence, stream_surface_presence
from settings import get_settings

settings = get_settings()
_chat_logger = logging.getLogger("uvicorn.error.chat_orchestrator.chat")
_owned_chat_tasks: set[asyncio.Task] = set()
_owned_presence_tasks: set[asyncio.Task] = set()
_presence_stopping = False
_PRESENCE_OBSERVER_LIMIT = 32
_PRESENCE_OBSERVER_SECONDS = 2.1


def _presence_observation_done(task: asyncio.Task) -> None:
    _owned_presence_tasks.discard(task)
    if not task.cancelled() and task.exception() is not None:
        _chat_logger.warning("presence_observation_failed")


def _schedule_presence_observation(trace: dict[str, Any] | None) -> None:
    # No queue or retry: saturation/shutdown can lose an observation. CR readers
    # qualify missing terminal observations as unavailable, never successful idle.
    if _presence_stopping or len(_owned_presence_tasks) >= _PRESENCE_OBSERVER_LIMIT:
        return
    snapshot = deepcopy(trace)
    runtime_client, memory_client = runtime, memory_store
    deadline = asyncio.get_running_loop().time() + _PRESENCE_OBSERVER_SECONDS

    async def observe() -> None:
        try:
            async with asyncio.timeout_at(deadline):
                await publish_surface_presence(
                    runtime=runtime_client, memory_store=memory_client,
                    trace=snapshot, stage="terminal",
                )
        except TimeoutError:
            pass

    task = asyncio.create_task(observe(), name="surface-presence-observation")
    _owned_presence_tasks.add(task)
    task.add_done_callback(_presence_observation_done)


async def _feedback_records(
    delivery: asyncio.Task, cue: asyncio.Future, request_id: str,
) -> AsyncIterator[str]:
    try:
        await asyncio.wait({delivery, cue}, return_when=asyncio.FIRST_COMPLETED)
        # A ready terminal always wins, including simultaneous completion.
        if not delivery.done() and cue.done():
            yield cue.result().model_dump_json() + "\n"
        response = await asyncio.shield(delivery)
        if isinstance(response, JSONResponse):
            code, body = response.status_code, json.loads(response.body)
        else:
            code, body = 200, response
        record = ChatTerminalRecord(http_status=code, body=body)
        if (record.body.request_id != request_id or (
            cue.done() and not cue.cancelled()
            and getattr(record.body, "conversation_id", None) != cue.result().conversation_id
            and code != 500
        )):
            raise ValueError("terminal_request_mismatch")
        yield record.model_dump_json() + "\n"
    except Exception:
        yield ChatTerminalRecord(
            http_status=500, body=ChatFailureResponse(request_id=request_id),
        ).model_dump_json() + "\n"
    finally:
        # This is only the HTTP delivery waiter. Existing deferred cognition is
        # separately owned by _await_chat_delivery and the application lifespan.
        if not delivery.done():
            delivery.cancel()
        await asyncio.gather(delivery, return_exceptions=True)
        cue.cancel()


def _feedback_requested(request: Request | None) -> bool:
    if request is None or not settings.progressive_feedback_enabled:
        return False
    # Require this exact version explicitly; wildcards never opt a client in.
    for item in request.headers.get("accept", "").split(","):
        parts = [part.strip().lower() for part in item.split(";")]
        if parts[0] != CHAT_FEEDBACK_MEDIA_TYPE:
            continue
        try:
            if len(parts) <= 2 and all(part.startswith("q=") for part in parts[1:]) and (
                not parts[1:] or 0 < float(parts[1][2:]) <= 1
            ):
                return True
        except ValueError:
            continue
    return False


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
    global _presence_stopping

    _presence_stopping = False
    runtime_client = runtime
    memory_client = memory_store
    memory_opened = False
    try:
        if runtime_client is not None:
            await runtime_client.open()
            await runtime_client.reconcile_interrupted_turns(str(uuid4()))
        await memory_client.open()
        memory_opened = True
        await memory_client.reconcile_interrupted_work()
        yield
    finally:
        _presence_stopping = True
        try:
            tasks = tuple(_owned_chat_tasks)
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            observations = tuple(_owned_presence_tasks)
            for task in observations:
                task.cancel()
            if observations:
                await asyncio.gather(*observations, return_exceptions=True)
        finally:
            try:
                if memory_opened:
                    await memory_client.close()
            finally:
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


def _public_work_result(value: dict[str, Any], **expected: Any) -> WorkStatus:
    validated = _validate_work_result(value, expected=expected)
    work, result = validated["work"], validated["result"]
    return WorkStatus(
        **{key: work[key] for key in (
            "work_id", "conversation_id", "request_id", "state", "failure_code",
        )},
        result=None if result is None else {
            "assistant_message_id": result["assistant_message_id"], "answer": result["content"],
        },
    )


@app.get(
    "/v1/work-items/{work_id}", response_model=WorkStatus,
    dependencies=[Depends(require_api_key)],
)
async def work_status(
    work_id: WorkUUID, owner_id: WorkIdentifier, conversation_id: WorkUUID,
) -> WorkStatus:
    try:
        expected = dict(work_id=work_id, owner_id=owner_id, conversation_id=conversation_id)
        value = await memory_store.get_work_result(**expected)
        result = None if value is None else _public_work_result(value, **expected)
    except Exception:
        raise HTTPException(status_code=503, detail="work_unavailable") from None
    if result is None:
        raise HTTPException(status_code=404, detail="work_not_found")
    return result


@app.get(
    "/v1/current-work", response_model=CurrentWorkStatus,
    dependencies=[Depends(require_api_key)],
)
async def current_work_status(
    owner_id: WorkIdentifier, client_id: WorkIdentifier,
) -> CurrentWorkStatus:
    try:
        locator = _validate_current_work(
            await memory_store.get_current_work(owner_id=owner_id, client_id=client_id),
            expected={"owner_id": owner_id, "client_id": client_id},
        )
        if locator["status"] == "none":
            return CurrentWorkStatus(status="none", work=None)
        work = locator["work"]
        value = await memory_store.get_work_result(
            work_id=work["work_id"], owner_id=owner_id, conversation_id=work["conversation_id"],
        )
        # Only lifecycle fields may advance between the explicit locator and exact read.
        expected = {key: work[key] for key in (
            "work_id", "owner_id", "conversation_id", "request_id", "client_id", "surface",
            "created_at",
        )}
        return CurrentWorkStatus(status="resolved", work=_public_work_result(value, **expected))
    except Exception:
        raise HTTPException(status_code=503, detail="work_unavailable") from None


_PRESENCE_HEADERS = {"Cache-Control": "no-store", "X-Accel-Buffering": "no"}


class _PresenceStreamingResponse(StreamingResponse):
    # Includes the producer's 15-second polling horizon and a bounded in-flight read.
    max_duration = 17.0

    async def stream_response(self, send):
        try:
            await asyncio.wait_for(super().stream_response(send), timeout=self.max_duration)
        except TimeoutError:
            try:
                await asyncio.wait_for(send({
                    "type": "http.response.body", "body": b"", "more_body": False,
                }), timeout=.1)
            except Exception:
                pass
        finally:
            close = getattr(self.body_iterator, "aclose", None)
            if callable(close):
                try:
                    await asyncio.wait_for(close(), timeout=.1)
                except Exception:
                    pass


async def _presence_access(api_key: str | None = Security(api_key_header)):
    try:
        await require_api_key(api_key)
    except HTTPException:
        raise HTTPException(status_code=401, detail="Unauthorized",
                            headers=_PRESENCE_HEADERS) from None


async def _read_presence(owner_id: str, conversation_id: str, surface: str, after: str | None):
    import re

    from pydantic import TypeAdapter

    try:
        TypeAdapter(WorkIdentifier).validate_python(owner_id)
        TypeAdapter(WorkIdentifier).validate_python(conversation_id)
        if not isinstance(surface, str) or not 1 <= len(surface) <= 64 or re.search(r"\s", surface):
            raise ValueError
        if after is not None and re.fullmatch(r"p[0e]_[a-f0-9]{32}", after) is None:
            raise ValueError
    except Exception:
        raise HTTPException(status_code=400, detail="presence_scope_or_cursor_invalid",
                            headers=_PRESENCE_HEADERS) from None
    if runtime is None:
        raise HTTPException(status_code=503, detail="presence_unavailable",
                            headers=_PRESENCE_HEADERS)
    try:
        conversation = _validate_conversation_projection(
            await memory_store.get_conversation(owner_id=owner_id, conversation_id=conversation_id),
            owner_id=owner_id, conversation_id=conversation_id,
        )
        if conversation["lifecycle_state"] != "open":
            raise RuntimeError("conversation_not_open")
        result = PresenceRuntimeBatch.model_validate(await runtime.read_surface_presence(
            owner_id=owner_id, conversation_id=conversation_id, surface=surface, after=after,
        ))
        if any(getattr(result, key) != value for key, value in (
            ("owner_id", owner_id), ("conversation_id", conversation_id), ("surface", surface),
        )):
            raise RuntimeError("presence_scope_mismatch")
        return result
    except ValueError as exc:
        if str(exc) == "presence_cursor_invalid":
            raise HTTPException(status_code=400, detail="presence_cursor_invalid",
                                headers=_PRESENCE_HEADERS) from None
        raise HTTPException(status_code=503, detail="presence_unavailable",
                            headers=_PRESENCE_HEADERS) from None
    except Exception:
        raise HTTPException(status_code=503, detail="presence_unavailable",
                            headers=_PRESENCE_HEADERS) from None


@app.get("/v1/presence/current", response_model=PresenceProjection,
         dependencies=[Depends(_presence_access)])
async def current_presence(owner_id: str | None = None, conversation_id: str | None = None,
                           surface: str | None = None):
    try:
        batch = await asyncio.wait_for(
            _read_presence(owner_id, conversation_id, surface, None), 2.0,
        )
    except TimeoutError:
        raise HTTPException(status_code=503, detail="presence_unavailable",
                            headers=_PRESENCE_HEADERS) from None
    projection = batch.current or PresenceProjection(state="unavailable", cursor=batch.cursor)
    return JSONResponse(projection.model_dump(), headers=_PRESENCE_HEADERS)


@app.get("/v1/presence/stream", dependencies=[Depends(_presence_access)])
async def presence_stream(request: Request, owner_id: str | None = None,
                          conversation_id: str | None = None, surface: str | None = None,
                          cursor: str | None = None):
    last = request.headers.get("last-event-id")
    if last is not None and cursor is not None and last != cursor:
        raise HTTPException(status_code=400, detail="presence_cursor_invalid",
                            headers=_PRESENCE_HEADERS)
    after = cursor if cursor is not None else last

    async def read(position):
        return await _read_presence(owner_id, conversation_id, surface, position)

    try:
        batch = await asyncio.wait_for(read(after), 2.0)
    except TimeoutError:
        raise HTTPException(status_code=503, detail="presence_unavailable",
                            headers=_PRESENCE_HEADERS) from None
    return _PresenceStreamingResponse(
        stream_surface_presence(request=request, initial=batch, read=read, after=after),
        media_type="text/event-stream", headers=_PRESENCE_HEADERS,
    )


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
    responses={
        200: {"content": {CHAT_FEEDBACK_MEDIA_TYPE: {"schema": {
            "type": "string",
            "description": "Optional processing_cue record, then one terminal NDJSON record.",
        }}}},
        202: {"model": DeferredChatResponse},
    },
    dependencies=[Depends(require_api_key)],
)
async def chat(body: ChatRequest, request: Request = None) -> ChatResponse:
    request_id = str(uuid4())
    _chat_logger.info(
        "chat_request_started component=chat-orchestrator request_id=%s",
        request_id,
    )
    try:
        # Negotiation may only narrow this invocation. Establish the restriction
        # before constructing cognition or admitting independently owned work.
        non_executing_conversation = bool(
            body.allow_deferred and body.delivery_wait_ms is not None
            and runtime is not None and body.capability_confirmation is None
            and _feedback_requested(request)
        )
        eligible = (
            body.allow_deferred and body.delivery_wait_ms is not None
            and runtime is not None
            and (not settings.cognitive_runtime_capability_registry_enabled
                 or non_executing_conversation)
            and body.capability_confirmation is None
        )
        admitted = asyncio.get_running_loop().create_future() if eligible else None
        # Only the existing independently owned, read-only delivery path supports feedback.
        # Action/confirmation and synchronous-only requests retain their JSON lifecycle.
        cue = (asyncio.get_running_loop().create_future()
               if non_executing_conversation else None)
        admitted_work = None

        def on_work_admitted(work: dict[str, Any]) -> None:
            nonlocal admitted_work
            admitted_work = work
            if admitted is not None and not admitted.done():
                admitted.set_result((work, asyncio.get_running_loop().time()))

        def on_processing_eligible(value: dict[str, Any]) -> None:
            if cue is None or cue.done() or not isinstance(admitted_work, dict):
                return
            try:
                record = ProcessingCueRecord.model_validate(value)
                expected = {
                    "request_id": request_id, "owner_id": body.owner_id,
                    "client_id": body.client_id, "surface": body.surface,
                    "conversation_id": record.conversation_id,
                }
                if (record.request_id != request_id or any(
                    key not in admitted_work or admitted_work[key] != item
                    for key, item in expected.items()
                )):
                    return
            except (ValueError, TypeError):
                return
            cue.set_result(record)

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
            strict_capability_information_enabled=getattr(
                settings, "strict_capability_information_enabled", False,
            ),
            strict_relationship_retrieval_enabled=settings.strict_relationship_retrieval_enabled,
            strict_world_state_authority_enabled=settings.strict_world_state_authority_enabled,
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
            on_work_admitted=on_work_admitted if eligible or cue is not None else None,
            on_presence_observation=_schedule_presence_observation,
            **({"on_processing_eligible": on_processing_eligible} if cue is not None else {}),
            **({"non_executing_conversation": True} if non_executing_conversation else {}),
        )

        async def deliver():
            try:
                result = (
                    await _await_chat_delivery(cognition, admitted, body)
                    if eligible else await cognition
                )
                if isinstance(result, DeferredChatResponse):
                    return JSONResponse(status_code=202, content=result.model_dump())
                response = ChatResponse(**result)
                _chat_logger.info(
                    "chat_request_completed component=chat-orchestrator request_id=%s status=%s",
                    request_id, response.status,
                )
                return response
            except Exception:
                _chat_logger.info(
                    "chat_request_failed component=chat-orchestrator request_id=%s "
                    "error_category=orchestration_exception", request_id,
                )
                return JSONResponse(
                    status_code=500,
                    content=ChatFailureResponse(request_id=request_id).model_dump(),
                )

        if cue is not None:
            return StreamingResponse(
                _feedback_records(asyncio.create_task(deliver()), cue, request_id),
                media_type=CHAT_FEEDBACK_MEDIA_TYPE,
                headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
            )
        return await deliver()
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
