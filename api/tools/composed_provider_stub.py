from __future__ import annotations

import asyncio
import hashlib
import json
import re
import unittest
from collections import defaultdict, deque
from typing import Any
from unittest.mock import patch

from fastapi import FastAPI, Header, HTTPException

app = FastAPI(title="Deterministic composed-smoke provider")
_calls: dict[str, list[dict[str, Any]]] = defaultdict(list)
_fail_primary: set[str] = set()
_primary_failed: set[str] = set()
_fail_next_primary = False
_next_primary_delay_ms = 0
_watched_sentinels: dict[str, str] = {}
_next_answers: list[dict[str, Any]] = []
_fixture_generation = 0
_fixture_sequence = 0
_fixture_events: deque[dict[str, Any]] = deque(maxlen=256)
_delayed_fixture_calls: dict[str, dict[str, Any]] = {}
_next_semantic_interpretations: list[dict[str, Any]] = []
_scene_policy_answers: list[dict[str, str]] = []
_pending_scene_calls: dict[str, dict[str, Any]] = {}
_world_state_read_fixture: dict[str, Any] | None = None
_READ_HOLD_SECONDS = 8.0
_WORLD_STATE_READ_ARGUMENTS = {
    "requested_domains": ["active_project"],
    "entity_id": "fixture-project",
    "attribute": "state",
    "output_mode": "summary",
}
_TOKEN_RE = re.compile(r"[A-Za-z0-9_.:-]+")
_FIXTURE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,119}$")
_SEMANTIC_STATUSES = {"resolved", "ambiguous", "no_match"}
_SEMANTIC_OPERATIONS = {
    "lookup",
    "latest",
    "comparison",
    "exhaustive_review",
    "contradiction_review",
    "absence_check",
    "historical_reconstruction",
    "decision_support",
    "aggregate",
    "unknown",
}
_SEMANTIC_FIXTURE_FIELDS = {
    "expected_request_text",
    "expected_source_id",
    "expected_content_fields",
    "interpretation_status",
    "operation_hint",
    "candidate_source_ids",
}
_SEMANTIC_AGGREGATE_FIELDS = {"aggregate_function", "aggregate_field_name"}
_AGGREGATE_FUNCTIONS = {"median", "mean", "count", "sum", "minimum", "maximum"}
_EXTERNAL_EVIDENCE_ITEM = re.compile(
    r"source_ref: (?P<source_ref>[^\n]+)\n(?P<text>[^\n]+)"
)


def _clear_read_fixture(reason: str, fixture: dict[str, Any] | None = None) -> None:
    global _world_state_read_fixture
    current = _world_state_read_fixture
    if current is None or (fixture is not None and current is not fixture):
        return
    _world_state_read_fixture = None
    current["timer"].cancel()
    current["release"].set()
    _fixture_event(reason, request_id=current.get("request_id"),
                   call_ref=current.get("call_ref"))


def _fixture_event(event: str, **details: Any) -> dict[str, Any]:
    global _fixture_sequence
    _fixture_sequence += 1
    if "request_id" in details:
        value = details["request_id"]
        details["request_id"] = value if (
            isinstance(value, str) and _FIXTURE_IDENTIFIER_RE.fullmatch(value)
        ) else "unscoped"
    observation = {
        "sequence": _fixture_sequence, "event": event, "generation": _fixture_generation,
        "queue_depth": len(_next_answers), **details,
    }
    _fixture_events.append(observation)
    return observation


@app.get("/fixture/diagnostics")
async def fixture_diagnostics(request_id: str | None = None) -> dict[str, Any]:
    # Only bounded associations; never prompts, answers, headers, or credentials.
    events = list(_fixture_events)
    return {
        "generation": _fixture_generation, "sequence": _fixture_sequence,
        "queue_depth": len(_next_answers),
        "queued_answer_refs": [item["answer_ref"] for item in _next_answers[:16]],
        "delayed_calls": list(_delayed_fixture_calls.values())[-64:],
        "events": events,
        "request_calls": [item for item in events if item.get("request_id") == request_id],
        "read_fixture": ({key: _world_state_read_fixture.get(key)
                          for key in ("request_id", "call_ref", "armed")}
                         if _world_state_read_fixture is not None else None),
    }


def _bounded_excerpt(value: str) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= 500:
        return normalized
    boundary = normalized.rfind(" ", 0, 501)
    return normalized[: boundary if boundary > 0 else 500]


def _scene_policy_answer(
    messages: list[dict[str, str]],
) -> tuple[str | None, dict[str, str] | None]:
    """Select a fixture answer only from an exact system-policy match."""
    if not _scene_policy_answers:
        return None, None
    matches = [
        policy for policy in _scene_policy_answers
        if policy["content"] and any(
            message["role"] == "system" and message["content"] == policy["content"]
            for message in messages
        )
    ]
    if not matches:
        matches = [policy for policy in _scene_policy_answers if not policy["content"]]
    if len(matches) != 1:
        raise HTTPException(status_code=422, detail="scene policy fixture mismatch")
    policy = matches[0]
    return policy["answer"], {
        "scene_id": policy["scene_id"],
        "policy_sha256": hashlib.sha256(policy["content"].encode()).hexdigest(),
    }


def _governed_evidence_candidate(prompt_text: str) -> str | None:
    if "Governed evidence response contract:" not in prompt_text:
        return None
    matches = list(_EXTERNAL_EVIDENCE_ITEM.finditer(prompt_text))
    if not matches:
        return None
    evidence_excerpts = [
        {
            "source_ref": match.group("source_ref"),
            "excerpt": _bounded_excerpt(match.group("text")),
        }
        for match in matches[:8]
    ]
    return json.dumps(
        {
            "conclusion_disposition": "supports",
            "evidence_excerpts": evidence_excerpts,
        },
        separators=(",", ":"),
    )


def _validate_identifier(value: object) -> str:
    if not isinstance(value, str) or _FIXTURE_IDENTIFIER_RE.fullmatch(value) is None:
        raise HTTPException(status_code=422, detail="invalid semantic fixture identifier")
    return value


def _validate_semantic_fixture(body: dict[str, Any]) -> dict[str, Any]:
    supplied = set(body)
    if not (
        supplied == _SEMANTIC_FIXTURE_FIELDS
        or supplied == _SEMANTIC_FIXTURE_FIELDS | _SEMANTIC_AGGREGATE_FIELDS
    ):
        raise HTTPException(status_code=422, detail="invalid semantic fixture fields")
    request_text = body.get("expected_request_text")
    if (
        not isinstance(request_text, str)
        or not request_text.strip()
        or len(request_text) > 2_000
    ):
        raise HTTPException(status_code=422, detail="invalid semantic fixture request")
    expected_source_id = _validate_identifier(body.get("expected_source_id"))
    content_fields = body.get("expected_content_fields")
    if (
        not isinstance(content_fields, list)
        or len(content_fields) > 24
        or any(
            not isinstance(field, str)
            or not field.strip()
            or len(field) > 120
            or re.search(r"[\x00-\x1f\x7f]", field) is not None
            for field in content_fields
        )
        or len(content_fields) != len(set(content_fields))
    ):
        raise HTTPException(status_code=422, detail="invalid semantic fixture fields")
    interpretation_status = body.get("interpretation_status")
    if (
        not isinstance(interpretation_status, str)
        or interpretation_status not in _SEMANTIC_STATUSES
    ):
        raise HTTPException(status_code=422, detail="invalid semantic fixture status")
    operation_hint = body.get("operation_hint")
    if not isinstance(operation_hint, str) or operation_hint not in _SEMANTIC_OPERATIONS:
        raise HTTPException(status_code=422, detail="invalid semantic fixture operation")
    candidate_source_ids = body.get("candidate_source_ids")
    if (
        not isinstance(candidate_source_ids, list)
        or len(candidate_source_ids) > 3
        or any(
            _FIXTURE_IDENTIFIER_RE.fullmatch(candidate) is None
            if isinstance(candidate, str)
            else True
            for candidate in candidate_source_ids
        )
        or len(candidate_source_ids) != len(set(candidate_source_ids))
    ):
        raise HTTPException(status_code=422, detail="invalid semantic fixture candidates")
    expected_count = {"resolved": 1, "ambiguous": (2, 3), "no_match": 0}
    count = len(candidate_source_ids)
    consistent = (
        count in expected_count[interpretation_status]
        if isinstance(expected_count[interpretation_status], tuple)
        else count == expected_count[interpretation_status]
    )
    if not consistent:
        raise HTTPException(status_code=422, detail="incoherent semantic fixture")
    aggregate_function = body.get("aggregate_function")
    aggregate_field_name = body.get("aggregate_field_name")
    if supplied & _SEMANTIC_AGGREGATE_FIELDS:
        if (
            operation_hint != "aggregate"
            or aggregate_function not in _AGGREGATE_FUNCTIONS
            or not isinstance(aggregate_field_name, str)
            or not aggregate_field_name
            or len(aggregate_field_name) > 120
            or aggregate_field_name != aggregate_field_name.strip()
            or re.search(r"[\x00-\x1f\x7f]", aggregate_field_name) is not None
        ):
            raise HTTPException(status_code=422, detail="invalid aggregate semantic fixture")
    fixture = {
        "expected_request_text": request_text,
        "expected_source_id": expected_source_id,
        "expected_content_fields": list(content_fields),
        "interpretation_status": interpretation_status,
        "operation_hint": operation_hint,
        "candidate_source_ids": list(candidate_source_ids),
    }
    if aggregate_function is not None:
        fixture["aggregate_function"] = aggregate_function
        fixture["aggregate_field_name"] = aggregate_field_name
    return fixture


def _consume_semantic_fixture(messages: list[Any]) -> dict[str, Any] | None:
    if not _next_semantic_interpretations:
        return None
    fixture = _next_semantic_interpretations.pop(0)
    user_content = next(
        (
            message.get("content")
            for message in reversed(messages)
            if isinstance(message, dict)
            and message.get("role") == "user"
            and isinstance(message.get("content"), str)
        ),
        None,
    )
    try:
        classifier_input = json.loads(user_content) if user_content is not None else None
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=422, detail="semantic fixture input is not JSON"
        ) from exc
    if not isinstance(classifier_input, dict):
        raise HTTPException(status_code=422, detail="semantic fixture input is invalid")
    if classifier_input.get("request_text") != fixture["expected_request_text"]:
        raise HTTPException(status_code=422, detail="semantic fixture request mismatch")
    sources = classifier_input.get("sources")
    if not isinstance(sources, list):
        raise HTTPException(status_code=422, detail="semantic fixture inventory missing")
    matches = [
        source
        for source in sources
        if isinstance(source, dict)
        and source.get("source_id") == fixture["expected_source_id"]
    ]
    if len(matches) != 1:
        raise HTTPException(status_code=422, detail="semantic fixture source mismatch")
    if (matches[0].get("content_fields") or []) != fixture["expected_content_fields"]:
        raise HTTPException(status_code=422, detail="semantic fixture content fields mismatch")
    result = {
        "interpretation_status": fixture["interpretation_status"],
        "operation_hint": fixture["operation_hint"],
        "candidate_source_ids": fixture["candidate_source_ids"],
    }
    if "aggregate_function" in fixture:
        result["aggregate_function"] = fixture["aggregate_function"]
        result["aggregate_field_name"] = fixture["aggregate_field_name"]
    return result


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/chat/completions")
async def chat_completions(
    body: dict[str, Any],
    x_request_id: str | None = Header(default=None),
) -> dict[str, Any]:
    global _next_primary_delay_ms
    request_id = x_request_id or "unscoped"
    generation = _fixture_generation
    # Snapshot before any await: an already-started call cannot acquire a new hold.
    read_fixture = _world_state_read_fixture
    call_ref = f"call_{_fixture_sequence + 1}"
    messages = body.get("messages")
    messages = messages if isinstance(messages, list) else []
    model = body.get("model")
    tools = body.get("tools")
    tool_count = len(tools) if isinstance(tools, list) else 0
    tool_names = [
        tool["function"]["name"] for tool in (tools if isinstance(tools, list) else [])
        if isinstance(tool, dict) and isinstance(tool.get("function"), dict)
        and isinstance(tool["function"].get("name"), str)
    ]
    response_format = body.get("response_format")
    json_schema = (
        response_format.get("json_schema")
        if isinstance(response_format, dict)
        else None
    )
    schema = json_schema.get("schema") if isinstance(json_schema, dict) else None
    classifier_diagnostics = {
        "response_format_type": (
            response_format.get("type") if isinstance(response_format, dict) else None
        ),
        "response_schema_name": (
            json_schema.get("name") if isinstance(json_schema, dict) else None
        ),
        "response_schema_strict": (
            json_schema.get("strict") if isinstance(json_schema, dict) else None
        ),
        "response_schema_additional_properties": (
            schema.get("additionalProperties") if isinstance(schema, dict) else None
        ),
        "response_schema_required": (
            schema.get("required") if isinstance(schema, dict) else None
        ),
        "max_completion_tokens": body.get("max_completion_tokens"),
    }
    _fixture_event("request_started", request_id=request_id, call_ref=call_ref,
                   request_generation=generation,
                   call_kind="structured" if response_format is not None else "chat")
    if classifier_diagnostics["response_schema_name"] == (
        "evidence_source_interpretation"
    ):
        semantic_result = _consume_semantic_fixture(messages) or {
            "interpretation_status": "no_match",
            "operation_hint": "unknown",
            "candidate_source_ids": [],
        }
        _calls[request_id].append(
            {
                "kind": "semantic_interpreter",
                "request_id": x_request_id,
                "model": model,
                "tool_count": tool_count,
                "reasoning_effort": body.get("reasoning_effort"),
                **classifier_diagnostics,
                "status": "ok",
            }
        )
        _fixture_event("completed", request_id=request_id, call_ref=call_ref,
                       request_generation=generation, answer_source="semantic_interpreter")
        return {
            "id": "completion-smoke",
            "service_tier": "default",
            "choices": [
                {
                    "index": 0,
                    "provider_specific_fields": {},
                    "message": {
                        "role": "assistant",
                        "content": json.dumps(
                            semantic_result,
                            separators=(",", ":"),
                        ),
                        "annotations": [],
                        "provider_specific_fields": {},
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 8, "completion_tokens": 3, "total_tokens": 11},
        }
    prompt_text = "\n".join(
        message.get("content", "")
        for message in messages
        if isinstance(message, dict) and isinstance(message.get("content"), str)
    )
    user_text = "\n".join(
        message.get("content", "")
        for message in messages
        if (
            isinstance(message, dict)
            and message.get("role") == "user"
            and isinstance(message.get("content"), str)
        )
    )
    normalized_messages = [
        {
            "role": str(message.get("role", "")),
            "content": str(message.get("content", "")),
        }
        for message in messages
        if isinstance(message, dict)
    ]
    scene_answer, scene_match = _scene_policy_answer(normalized_messages)
    delay_ms = _next_primary_delay_ms
    _next_primary_delay_ms = 0
    if delay_ms:
        _delayed_fixture_calls[call_ref] = {
            **_fixture_events[-1], "call_ref": call_ref,
        }
        pending_scene = None
        if scene_match is not None:
            pending_scene = {
                "kind": "chat", "status": "pending", "request_id": request_id,
                "scene_policy_match": scene_match,
            }
            _pending_scene_calls[request_id] = pending_scene
        try:
            await asyncio.sleep(delay_ms / 1000)
        finally:
            if _pending_scene_calls.get(request_id) is pending_scene:
                _pending_scene_calls.pop(request_id, None)
            _delayed_fixture_calls.pop(call_ref, None)
    # There is no await between this check and queue/failure-state consumption.
    # An older request cannot touch configuration installed by any reset.
    if generation != _fixture_generation:
        _fixture_event("rejected_reset", request_id=request_id, call_ref=call_ref,
                       request_generation=generation)
        raise HTTPException(status_code=409, detail="fixture_reset_during_request")
    latest_user_text = next(
        (
            message["content"]
            for message in reversed(normalized_messages)
            if message["role"] == "user"
        ),
        "",
    )
    read_fixture_selected = False
    if (read_fixture is not None and read_fixture is _world_state_read_fixture
            and latest_user_text == read_fixture["expected_user_text"]
            and request_id != "unscoped" and _FIXTURE_IDENTIFIER_RE.fullmatch(request_id)
            and read_fixture.get("call_ref") is None):
        # Holding does not select a proposal. The harness must independently
        # resolve admitted work, then bind both this request ID and this call.
        read_fixture.update(request_id=request_id, call_ref=call_ref)
        _fixture_event("read_request_held", request_id=request_id, call_ref=call_ref)
        try:
            await read_fixture["release"].wait()
        except asyncio.CancelledError:
            _clear_read_fixture("read_cancelled", read_fixture)
            raise
        if (read_fixture is not _world_state_read_fixture
                or generation != _fixture_generation or not read_fixture["armed"]):
            raise HTTPException(status_code=409, detail="read_fixture_expired_or_reset")
        read_fixture_selected = True
    prompt_fingerprint = hashlib.sha256(
        json.dumps(normalized_messages, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    current_memory_messages = [
        message["content"]
        for message in normalized_messages
        if message["role"] == "system"
        and "Current memory evidence:" in message["content"]
    ]
    has_current = bool(current_memory_messages)
    has_historical = "Historical or unverified memory context:" in prompt_text
    beta_in_current = any("Beta" in content for content in current_memory_messages)
    beta_anywhere = "Beta" in prompt_text
    wave2e_private_sentinel = "PRIVATE-WAVE2E-DIAGNOSTIC-SENTINEL" in prompt_text
    raw_diagnostics_marker = (
        "raw_bundle" in prompt_text
        or "augmented_bundle" in prompt_text
        or "comparison" in prompt_text
    )
    sentinel_presence = {
        name: sentinel in prompt_text for name, sentinel in sorted(_watched_sentinels.items())
    }
    sentinel_in_user_messages = {
        name: sentinel in user_text for name, sentinel in sorted(_watched_sentinels.items())
    }
    global _fail_next_primary
    should_fail_primary = request_id in _fail_primary or _fail_next_primary
    if should_fail_primary and request_id not in _primary_failed:
        _fail_next_primary = False
        _primary_failed.add(request_id)
        _calls[request_id].append(
            {
                "kind": "chat",
                "request_id": x_request_id,
                "model": model,
                "tool_count": tool_count,
                **classifier_diagnostics,
                "message_count": len(messages),
                "normalized_messages": normalized_messages,
                "prompt_fingerprint": prompt_fingerprint,
                "has_current_memory_evidence": has_current,
                "has_historical_memory_context": has_historical,
                "has_forbidden_beta_in_current": beta_in_current,
                "has_beta_marker": beta_anywhere,
                "has_wave2e_private_sentinel": wave2e_private_sentinel,
                "has_raw_diagnostics_marker": raw_diagnostics_marker,
                "sentinel_presence": sentinel_presence,
                "sentinel_in_user_messages": sentinel_in_user_messages,
                "status": "failed",
            }
        )
        _fixture_event("failed_primary", request_id=request_id, call_ref=call_ref,
                       request_generation=generation)
        if read_fixture_selected:
            _clear_read_fixture("read_failed_primary", read_fixture)
        raise HTTPException(status_code=503, detail="primary failure fixture")
    governed_candidate = _governed_evidence_candidate(prompt_text)
    answer_source = "derived_fixture"
    tool_calls = []
    if read_fixture_selected:
        # A one-shot proposal, never authority. No caller-supplied tool or args.
        _clear_read_fixture("read_released", read_fixture)
        answer = "No read capability was exposed. No read was proposed."
        answer_source = "world_state_read"
        if tool_names == ["runtime_world_state_read"] and tool_count == 1:
            tool_calls = [{
                "id": "call_world_state_read", "type": "function",
                "function": {"name": "runtime_world_state_read",
                             "arguments": json.dumps(_WORLD_STATE_READ_ARGUMENTS)},
            }]
            answer = ""
        _fixture_event("read_fixture_consumed", request_id=request_id, call_ref=call_ref,
                       proposal_count=len(tool_calls))
    elif scene_answer is not None:
        answer = scene_answer
        answer_source = "scene_policy"
    elif _next_answers:
        queued = _next_answers.pop(0)
        answer = queued["answer"]
        answer_source = "queue"
        _fixture_event("consumed", request_id=request_id, call_ref=call_ref,
                       request_generation=generation, answer_ref=queued["answer_ref"])
    elif governed_candidate is not None:
        answer = governed_candidate
    elif latest_user_text.strip() == "What does the retained file report about the setting?":
        answer = "The retained file reports that the setting is active."
    elif has_current and "Current plan is Alpha." in prompt_text:
        answer = "Current plan is Alpha."
    elif has_historical:
        answer = "I only have historical or unverified memory context."
    else:
        answer = "neutral smoke response"
        answer_source = "default"
    _fixture_event("completed", request_id=request_id, call_ref=call_ref,
                   request_generation=generation, answer_source=answer_source)
    _calls[request_id].append(
        {
            "kind": "chat",
            "request_id": x_request_id,
            "model": model,
            "tool_count": tool_count,
            **classifier_diagnostics,
            "message_count": len(messages),
            "normalized_messages": normalized_messages,
            "prompt_fingerprint": prompt_fingerprint,
            "has_current_memory_evidence": has_current,
            "has_historical_memory_context": has_historical,
            "has_forbidden_beta_in_current": beta_in_current,
            "has_beta_marker": beta_anywhere,
            "has_wave2e_private_sentinel": wave2e_private_sentinel,
            "has_raw_diagnostics_marker": raw_diagnostics_marker,
            "sentinel_presence": sentinel_presence,
            "sentinel_in_user_messages": sentinel_in_user_messages,
            "status": "ok",
            "scene_policy_match": scene_match,
            "tool_names": tool_names,
            "read_fixture_selected": read_fixture_selected,
            "read_proposal_count": len(tool_calls),
        }
    )
    return {
        "id": "completion-smoke",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": answer,
                            **({"tool_calls": tool_calls} if tool_calls else {})},
                "finish_reason": "tool_calls" if tool_calls else "stop",
            }
        ],
        "usage": {"prompt_tokens": 8, "completion_tokens": 3, "total_tokens": 11},
    }


@app.post("/v1/embeddings")
async def embeddings(
    body: dict[str, Any],
    x_request_id: str | None = Header(default=None),
) -> dict[str, Any]:
    inputs = body.get("input")
    inputs = inputs if isinstance(inputs, list) else [inputs]
    request_id = x_request_id or "unscoped"
    _calls[request_id].append(
        {
            "kind": "embedding",
            "request_id": x_request_id,
            "model": body.get("model"),
            "input_count": len(inputs),
        }
    )
    vectors = [_embedding_vector(item) for item in inputs]
    return {
        "data": [
            {"object": "embedding", "index": index, "embedding": vector}
            for index, vector in enumerate(vectors)
        ],
        "model": body.get("model"),
    }


@app.get("/calls/{request_id}")
async def calls(request_id: str) -> dict[str, Any]:
    pending = [_pending_scene_calls[request_id]] if request_id in _pending_scene_calls else []
    return {"request_id": request_id, "calls": [*_calls.get(request_id, []), *pending]}


@app.post("/fixture/reset")
async def fixture_reset(body: dict[str, Any] | None = None) -> dict[str, Any]:
    global _fail_next_primary, _next_primary_delay_ms, _fixture_generation
    _fixture_generation += 1
    _next_primary_delay_ms = 0
    _next_semantic_interpretations.clear()
    _scene_policy_answers.clear()
    _clear_read_fixture("read_reset")
    request_id = (body or {}).get("request_id")
    if isinstance(request_id, str) and request_id:
        _calls.pop(request_id, None)
        _fail_primary.discard(request_id)
        _primary_failed.discard(request_id)
    else:
        _calls.clear()
        _fail_primary.clear()
        _primary_failed.clear()
        _watched_sentinels.clear()
        _next_answers.clear()
        _fail_next_primary = False
    _fixture_event("reset", request_id=request_id)
    return {"status": "ok", "generation": _fixture_generation}


@app.post("/fixture/fail-primary/{request_id}")
async def fixture_fail_primary(request_id: str) -> dict[str, str]:
    _fail_primary.add(request_id)
    _primary_failed.discard(request_id)
    return {"status": "ok"}


@app.post("/fixture/fail-next-primary")
async def fixture_fail_next_primary() -> dict[str, str]:
    global _fail_next_primary
    _fail_next_primary = True
    return {"status": "ok"}


@app.post("/fixture/delay-next-primary")
async def fixture_delay_next_primary(body: dict[str, Any]) -> dict[str, Any]:
    global _next_primary_delay_ms
    delay_ms = body.get("delay_ms")
    if isinstance(delay_ms, bool) or not isinstance(delay_ms, int):
        raise HTTPException(status_code=422, detail="invalid fixture delay")
    if not 1 <= delay_ms <= 20_000:
        raise HTTPException(status_code=422, detail="invalid fixture delay")
    _next_primary_delay_ms = delay_ms
    return {"status": "ok", "delay_ms": delay_ms}


@app.post("/fixture/sentinels")
async def fixture_sentinels(body: dict[str, Any]) -> dict[str, Any]:
    _watched_sentinels.clear()
    sentinels = body.get("sentinels")
    if isinstance(sentinels, dict):
        for name, sentinel in sentinels.items():
            if isinstance(name, str) and isinstance(sentinel, str) and name and sentinel:
                _watched_sentinels[name[:80]] = sentinel[:240]
    return {"status": "ok", "count": len(_watched_sentinels)}


@app.post("/fixture/next-answer")
async def fixture_next_answer(body: dict[str, Any]) -> dict[str, Any]:
    global _world_state_read_fixture
    if "world_state_read" in body:
        fixture = body["world_state_read"]
        if (set(body) == {"world_state_read"} and isinstance(fixture, dict)
                and set(fixture) == {"request_id", "call_ref"}):
            current = _world_state_read_fixture
            if (current is None or current["armed"] or current.get("call_ref") is None
                    or fixture != {key: current.get(key) for key in fixture}):
                raise HTTPException(status_code=422, detail="read request binding mismatch")
            current["armed"] = True
            _fixture_event("read_request_bound", **fixture)
            current["release"].set()
            return {"status": "ok", "bound": True, **fixture}
        if (
            set(body) != {"world_state_read"} or not isinstance(fixture, dict)
            or set(fixture) != {"expected_user_text"}
            or not isinstance(fixture.get("expected_user_text"), str)
            or not 1 <= len(fixture["expected_user_text"]) <= 500
            or _world_state_read_fixture or _next_answers or _scene_policy_answers
        ):
            raise HTTPException(status_code=422, detail="invalid world-state read fixture")
        current = {**fixture, "release": asyncio.Event(), "armed": False}
        current["timer"] = asyncio.get_running_loop().call_later(
            _READ_HOLD_SECONDS, _clear_read_fixture, "read_timeout", current,
        )
        _world_state_read_fixture = current
        return {"status": "ok", "held_only": True, "generation": _fixture_generation}
    if "scene_policies" in body:
        policies = body["scene_policies"]
        if (
            set(body) != {"scene_policies"} or not isinstance(policies, list)
            or not 1 <= len(policies) <= 4
            or any(
                not isinstance(policy, dict)
                or set(policy) != {"scene_id", "content", "answer"}
                or any(not isinstance(value, str) for value in policy.values())
                or not policy["scene_id"] or len(policy["scene_id"]) > 64
                or len(policy["content"]) > 2000
                or not policy["answer"] or len(policy["answer"]) > 2000
                for policy in policies
            )
            or len({policy["scene_id"] for policy in policies}) != len(policies)
            or len({policy["content"] for policy in policies}) != len(policies)
            or _next_answers
        ):
            raise HTTPException(status_code=422, detail="invalid scene policy fixture")
        _scene_policy_answers[:] = [dict(policy) for policy in policies]
        return {"status": "ok", "queued": len(policies)}
    answer = body.get("answer")
    if not isinstance(answer, str) or not answer or len(answer) > 2_000:
        raise HTTPException(status_code=422, detail="invalid fixture answer")
    answer_ref = f"answer_{_fixture_sequence + 1}"
    _next_answers.append({"answer": answer, "answer_ref": answer_ref})
    _fixture_event("queued", answer_ref=answer_ref)
    return {"status": "ok", "queued": len(_next_answers),
            "generation": _fixture_generation, "answer_ref": answer_ref}


@app.post("/fixture/next-semantic-interpretation")
async def fixture_next_semantic_interpretation(
    body: dict[str, Any],
) -> dict[str, Any]:
    if _next_semantic_interpretations:
        raise HTTPException(status_code=409, detail="semantic fixture already queued")
    _next_semantic_interpretations.append(_validate_semantic_fixture(body))
    return {"status": "ok", "queued": 1}


def _embedding_vector(value: Any) -> list[float]:
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
    vector = [0.0] * 1536
    vector[0] = 10.0
    for token in _TOKEN_RE.findall(text.lower()):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = 1 + (int.from_bytes(digest[:2], "big") % 1535)
        vector[index] += 1.0
    return vector


class ProviderFixtureTests(unittest.IsolatedAsyncioTestCase):
    """Run explicitly here to keep regressions inside the fixture's approved scope."""

    async def asyncSetUp(self):
        await fixture_reset({})

    async def asyncTearDown(self):
        await fixture_reset({})

    async def complete(self, request_id):
        return await chat_completions(
            {"model": "fixture-model", "messages": [{"role": "user", "content": "PRIVATE-PROMPT"}]},
            x_request_id=request_id,
        )

    def answer(self, response):
        return response["choices"][0]["message"]["content"]

    def read_body(self, names=("runtime_world_state_read",)):
        return {"messages": [{"role": "user", "content": "read fixture"}],
                "tools": [{"function": {"name": name}} for name in names]}

    async def prepare_read(self):
        await fixture_next_answer({"world_state_read": {"expected_user_text": "read fixture"}})

    async def bind_read(self, request_id="intended"):
        pending = (await fixture_diagnostics())["read_fixture"]
        self.assertEqual(pending["request_id"], request_id)
        return await fixture_next_answer({"world_state_read": {
            "request_id": request_id, "call_ref": pending["call_ref"],
        }})

    async def test_read_fixture_requires_exact_request_exposure_and_single_consumption(self):
        from services.capabilities import (
            parse_provider_capability_request,
            validate_and_digest_capability_request,
        )

        await self.prepare_read()
        intended = asyncio.create_task(chat_completions(self.read_body(), "intended"))
        await asyncio.sleep(0)
        other = await chat_completions(self.read_body(), "other")
        self.assertIsNone(parse_provider_capability_request(other))
        pending = dict((await fixture_diagnostics())["read_fixture"])
        for wrong in ({"request_id": "other", "call_ref": pending["call_ref"]},
                      {"request_id": "intended", "call_ref": "wrong-call"}):
            with self.assertRaises(HTTPException):
                await fixture_next_answer({"world_state_read": wrong})
            self.assertEqual((await fixture_diagnostics())["read_fixture"], pending)
        await self.bind_read()
        parsed = parse_provider_capability_request(await intended)
        result = validate_and_digest_capability_request(
            request=parsed, exposed_capability_ids=["runtime.world_state.read"],
        )
        self.assertEqual(result.normalized_arguments, _WORLD_STATE_READ_ARGUMENTS)
        self.assertIsNone(parse_provider_capability_request(
            await chat_completions(self.read_body(), "intended")))
        self.assertIsNone((await fixture_diagnostics())["read_fixture"])
        self.assertEqual([c["read_proposal_count"] for c in (await calls("intended"))["calls"]],
                         [1, 0])

    async def test_read_fixture_ignores_request_started_before_preparation(self):
        await fixture_delay_next_primary({"delay_ms": 40})
        old = asyncio.create_task(chat_completions(self.read_body(), "old"))
        await asyncio.sleep(0)
        self.assertEqual(len(_delayed_fixture_calls), 1)
        await self.prepare_read()
        # Let the older delayed call finish while the new fixture is pending.
        self.assertNotIn("tool_calls", (await old)["choices"][0]["message"])
        self.assertIsNone((await fixture_diagnostics())["read_fixture"]["request_id"])
        intended = asyncio.create_task(chat_completions(self.read_body(), "intended"))
        await asyncio.sleep(0)
        await self.bind_read()
        self.assertIn("tool_calls", (await intended)["choices"][0]["message"])
        self.assertFalse(_delayed_fixture_calls)

    async def test_read_fixture_never_proposes_hidden_or_arbitrary_tools(self):
        for names in ([], ["arbitrary_tool"], ["runtime_world_state_read", "arbitrary_tool"]):
            with self.subTest(names=names):
                await fixture_reset({})
                await self.prepare_read()
                task = asyncio.create_task(chat_completions(self.read_body(names), "intended"))
                await asyncio.sleep(0)
                await self.bind_read()
                response = await task
                self.assertNotIn("tool_calls", response["choices"][0]["message"])
                self.assertIn("No read was proposed", self.answer(response))

    async def test_read_fixture_rejects_arguments_and_cleans_up_every_exit(self):
        for fixture in ({"tool": "arbitrary"}, {"expected_user_text": "read", "arguments": {}},
                        {"expected_user_text": ""}, {"request_id": "unknown", "call_ref": "old"}):
            with self.assertRaises(HTTPException):
                await fixture_next_answer({"world_state_read": fixture})
        for mode in ("cancel", "global_reset", "scoped_reset", "timeout", "unused_timeout"):
            with self.subTest(mode=mode), patch(__name__ + "._READ_HOLD_SECONDS", .03):
                await self.prepare_read()
                fixture = _world_state_read_fixture
                task = None
                if mode != "unused_timeout":
                    task = asyncio.create_task(chat_completions(self.read_body(), "intended"))
                    await asyncio.sleep(0)
                if mode == "cancel":
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                elif mode in {"global_reset", "scoped_reset"}:
                    await fixture_reset(
                        {} if mode == "global_reset" else {"request_id": "intended"},
                    )
                    with self.assertRaises(HTTPException):
                        await task
                elif task is not None:
                    with self.assertRaises(HTTPException):
                        await asyncio.wait_for(task, .2)
                else:
                    await asyncio.sleep(.05)
                self.assertIsNone((await fixture_diagnostics())["read_fixture"])
                self.assertTrue(fixture["timer"].cancelled())
                self.assertFalse(_delayed_fixture_calls)
                self.assertFalse([t for t in asyncio.all_tasks()
                                  if t is not asyncio.current_task() and not t.done()])

    async def test_delayed_request_cannot_consume_after_global_or_scoped_reset(self):
        for reset in ({}, {"request_id": "old"}):
            with self.subTest(reset=reset):
                await fixture_reset({})
                entered, release = asyncio.Event(), asyncio.Event()

                async def hold(delay):
                    self.assertEqual(delay, .05)
                    entered.set()
                    await release.wait()

                await fixture_next_answer({"answer": "old answer"})
                await fixture_delay_next_primary({"delay_ms": 50})
                with patch.object(asyncio, "sleep", hold):
                    pending = asyncio.create_task(self.complete("old"))
                    try:
                        await asyncio.wait_for(entered.wait(), 1)
                        previous = (await fixture_diagnostics())["generation"]
                        acknowledgment = await fixture_reset(reset)
                        self.assertGreater(acknowledgment["generation"], previous)
                        # Scoped resets keep the existing queue, as before.
                        if reset:
                            self.assertEqual(self.answer(await self.complete("retained")),
                                             "old answer")
                        queued = await fixture_next_answer({"answer": "7."})
                    finally:
                        release.set()
                    with self.assertRaises(HTTPException) as error:
                        await pending
                    self.assertEqual(error.exception.status_code, 409)
                self.assertEqual((await fixture_diagnostics())["queued_answer_refs"],
                                 [queued["answer_ref"]])
                self.assertEqual(self.answer(await self.complete("intended")), "7.")
                events = (await fixture_diagnostics())["events"]
                self.assertTrue(any(e["event"] == "rejected_reset" and
                                    e.get("request_id") == "old" for e in events))
                consumer = [e for e in events if e["event"] == "consumed"
                            and e.get("answer_ref") == queued["answer_ref"]]
                self.assertEqual([e["request_id"] for e in consumer], ["intended"])

    async def test_concurrent_old_requests_cannot_cross_multiple_resets(self):
        release = asyncio.Event()
        entered = asyncio.Queue()

        async def hold(delay):
            entered.put_nowait(True)
            await release.wait()

        pending = []
        with patch.object(asyncio, "sleep", hold):
            try:
                for index in range(8):
                    await fixture_delay_next_primary({"delay_ms": 50})
                    pending.append(asyncio.create_task(self.complete(f"old-{index}")))
                    await asyncio.wait_for(entered.get(), 1)
                await fixture_reset({})
                await fixture_next_answer({"answer": "discarded"})
                await fixture_reset({})
                queued = await fixture_next_answer({"answer": "7."})
            finally:
                release.set()
            results = await asyncio.gather(*pending, return_exceptions=True)
        self.assertTrue(all(isinstance(result, HTTPException) and result.status_code == 409
                            for result in results))
        self.assertEqual((await fixture_diagnostics())["queued_answer_refs"],
                         [queued["answer_ref"]])
        self.assertEqual(self.answer(await self.complete("new")), "7.")

    async def test_same_generation_keeps_completion_order_queue_selection(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def hold(delay):
            entered.set()
            await release.wait()

        await fixture_next_answer({"answer": "first"})
        await fixture_next_answer({"answer": "second"})
        await fixture_delay_next_primary({"delay_ms": 50})
        with patch.object(asyncio, "sleep", hold):
            pending = asyncio.create_task(self.complete("delayed"))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                self.assertEqual(self.answer(await self.complete("immediate")), "first")
            finally:
                release.set()
            self.assertEqual(self.answer(await pending), "second")

    async def test_primary_failure_preserves_answer_for_fallback(self):
        queued = await fixture_next_answer({"answer": "7."})
        await fixture_fail_next_primary()
        with self.assertRaises(HTTPException) as error:
            await self.complete("fallback")
        self.assertEqual(error.exception.status_code, 503)
        self.assertEqual((await fixture_diagnostics())["queued_answer_refs"],
                         [queued["answer_ref"]])
        self.assertEqual(self.answer(await self.complete("fallback")), "7.")

    async def test_stale_request_cannot_consume_new_failure_flag(self):
        entered, release = asyncio.Event(), asyncio.Event()

        async def hold(delay):
            entered.set()
            await release.wait()

        await fixture_delay_next_primary({"delay_ms": 50})
        with patch.object(asyncio, "sleep", hold):
            pending = asyncio.create_task(self.complete("old"))
            try:
                await asyncio.wait_for(entered.wait(), 1)
                await fixture_reset({})
                await fixture_fail_next_primary()
                await fixture_next_answer({"answer": "7."})
            finally:
                release.set()
            with self.assertRaises(HTTPException) as error:
                await pending
            self.assertEqual(error.exception.status_code, 409)
        with self.assertRaises(HTTPException) as error:
            await self.complete("new")
        self.assertEqual(error.exception.status_code, 503)
        self.assertEqual(self.answer(await self.complete("new")), "7.")

    async def test_reset_discards_queue_and_diagnostics_are_bounded_and_read_only(self):
        for _ in range(300):
            await fixture_next_answer({"answer": "PRIVATE-ANSWER"})
        before = await fixture_diagnostics()
        self.assertEqual(len(before["events"]), 256)
        self.assertEqual(len(before["queued_answer_refs"]), 16)
        self.assertEqual(await fixture_diagnostics(), before)
        self.assertNotIn("PRIVATE-ANSWER", json.dumps(before))
        reset = await fixture_reset({})
        self.assertEqual((await fixture_diagnostics())["queue_depth"], 0)
        self.assertEqual(reset["generation"], before["generation"] + 1)
        self.assertEqual(self.answer(await self.complete("empty")), "neutral smoke response")
        report = await fixture_diagnostics("empty")
        self.assertNotIn("PRIVATE-PROMPT", json.dumps(report))
        self.assertEqual([e["event"] for e in report["request_calls"]],
                         ["request_started", "completed"])
