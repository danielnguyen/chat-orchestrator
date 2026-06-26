from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, Header, HTTPException

app = FastAPI(title="Deterministic composed-smoke provider")
_calls: dict[str, list[dict[str, Any]]] = defaultdict(list)
_fail_primary: set[str] = set()
_primary_failed: set[str] = set()
_fail_next_primary = False


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/chat/completions")
async def chat_completions(
    body: dict[str, Any],
    x_request_id: str | None = Header(default=None),
) -> dict[str, Any]:
    request_id = x_request_id or "unscoped"
    messages = body.get("messages")
    messages = messages if isinstance(messages, list) else []
    model = body.get("model")
    prompt_text = "\n".join(
        message.get("content", "")
        for message in messages
        if isinstance(message, dict) and isinstance(message.get("content"), str)
    )
    normalized_messages = [
        {
            "role": str(message.get("role", "")),
            "content": str(message.get("content", "")),
        }
        for message in messages
        if isinstance(message, dict)
    ]
    prompt_fingerprint = hashlib.sha256(
        json.dumps(normalized_messages, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    current_block = prompt_text.split("Historical or unverified memory context:")[0]
    has_current = "Current memory evidence:" in prompt_text
    has_historical = "Historical or unverified memory context:" in prompt_text
    beta_in_current = "Current memory evidence:" in current_block and "Beta" in current_block
    beta_anywhere = "Beta" in prompt_text
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
                "message_count": len(messages),
                "prompt_fingerprint": prompt_fingerprint,
                "has_current_memory_evidence": has_current,
                "has_historical_memory_context": has_historical,
                "has_forbidden_beta_in_current": beta_in_current,
                "has_beta_marker": beta_anywhere,
                "status": "failed",
            }
        )
        raise HTTPException(status_code=503, detail="primary failure fixture")
    if has_current and "Current plan is Alpha." in prompt_text:
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
            "message_count": len(messages),
            "prompt_fingerprint": prompt_fingerprint,
            "has_current_memory_evidence": has_current,
            "has_historical_memory_context": has_historical,
            "has_forbidden_beta_in_current": beta_in_current,
            "has_beta_marker": beta_anywhere,
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
    return {
        "data": [
            {"object": "embedding", "index": index, "embedding": [1.0] + [0.0] * 1535}
            for index, _ in enumerate(inputs)
        ],
        "model": body.get("model"),
    }


@app.get("/calls/{request_id}")
async def calls(request_id: str) -> dict[str, Any]:
    return {"request_id": request_id, "calls": _calls.get(request_id, [])}


@app.post("/fixture/reset")
async def fixture_reset(body: dict[str, Any] | None = None) -> dict[str, str]:
    global _fail_next_primary
    request_id = (body or {}).get("request_id")
    if isinstance(request_id, str) and request_id:
        _calls.pop(request_id, None)
        _fail_primary.discard(request_id)
        _primary_failed.discard(request_id)
    else:
        _calls.clear()
        _fail_primary.clear()
        _primary_failed.clear()
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
