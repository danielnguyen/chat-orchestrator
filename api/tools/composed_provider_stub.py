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
_TOKEN_RE = re.compile(r"[A-Za-z0-9_.:-]+")
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
                            {
                                "interpretation_status": "no_match",
                                "operation_hint": "unknown",
                                "candidate_source_ids": [],
                            },
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


def _embedding_vector(value: Any) -> list[float]:
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
    vector = [0.0] * 1536
    vector[0] = 10.0
    for token in _TOKEN_RE.findall(text.lower()):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = 1 + (int.from_bytes(digest[:2], "big") % 1535)
        vector[index] += 1.0
    return vector
