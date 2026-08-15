from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, Header, HTTPException

app = FastAPI(title="Deterministic composed-smoke provider")
_calls: dict[str, list[dict[str, Any]]] = defaultdict(list)
_fail_primary: set[str] = set()
_primary_failed: set[str] = set()
_fail_next_primary = False
_next_primary_delay_ms = 0
_watched_sentinels: dict[str, str] = {}
_next_answers: list[str] = []
_next_semantic_interpretations: list[dict[str, Any]] = []
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
_EXTERNAL_EVIDENCE_ITEM = re.compile(
    r"source_ref: (?P<source_ref>[^\n]+)\n(?P<text>[^\n]+)"
)


def _bounded_excerpt(value: str) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= 500:
        return normalized
    boundary = normalized.rfind(" ", 0, 501)
    return normalized[: boundary if boundary > 0 else 500]


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
    if set(body) != _SEMANTIC_FIXTURE_FIELDS:
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
        or not content_fields
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
    return {
        "expected_request_text": request_text,
        "expected_source_id": expected_source_id,
        "expected_content_fields": list(content_fields),
        "interpretation_status": interpretation_status,
        "operation_hint": operation_hint,
        "candidate_source_ids": list(candidate_source_ids),
    }


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
    if matches[0].get("content_fields") != fixture["expected_content_fields"]:
        raise HTTPException(status_code=422, detail="semantic fixture content fields mismatch")
    return {
        "interpretation_status": fixture["interpretation_status"],
        "operation_hint": fixture["operation_hint"],
        "candidate_source_ids": fixture["candidate_source_ids"],
    }


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
    messages = body.get("messages")
    messages = messages if isinstance(messages, list) else []
    model = body.get("model")
    tools = body.get("tools")
    tool_count = len(tools) if isinstance(tools, list) else 0
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
                **classifier_diagnostics,
                "status": "ok",
            }
        )
        return {
            "id": "completion-smoke",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": json.dumps(
                            semantic_result,
                            separators=(",", ":"),
                        ),
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 8, "completion_tokens": 3, "total_tokens": 11},
        }
    delay_ms = _next_primary_delay_ms
    _next_primary_delay_ms = 0
    if delay_ms:
        await asyncio.sleep(delay_ms / 1000)
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
    latest_user_text = next(
        (
            message["content"]
            for message in reversed(normalized_messages)
            if message["role"] == "user"
        ),
        "",
    )
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
        raise HTTPException(status_code=503, detail="primary failure fixture")
    governed_candidate = _governed_evidence_candidate(prompt_text)
    if _next_answers:
        answer = _next_answers.pop(0)
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
        }
    )
    return {
        "id": "completion-smoke",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": answer},
                "finish_reason": "stop",
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
    return {"request_id": request_id, "calls": _calls.get(request_id, [])}


@app.post("/fixture/reset")
async def fixture_reset(body: dict[str, Any] | None = None) -> dict[str, str]:
    global _fail_next_primary, _next_primary_delay_ms
    _next_primary_delay_ms = 0
    _next_semantic_interpretations.clear()
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
    return {"status": "ok"}


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
    if not 1 <= delay_ms <= 5_000:
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
    answer = body.get("answer")
    if not isinstance(answer, str) or not answer or len(answer) > 2_000:
        raise HTTPException(status_code=422, detail="invalid fixture answer")
    _next_answers.append(answer)
    return {"status": "ok", "queued": len(_next_answers)}


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
