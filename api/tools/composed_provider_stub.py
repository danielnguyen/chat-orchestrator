from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import FastAPI, Header

app = FastAPI(title="Deterministic composed-smoke provider")
_calls: dict[str, list[dict[str, Any]]] = defaultdict(list)


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
    prompt_text = "\n".join(
        message.get("content", "")
        for message in messages
        if isinstance(message, dict) and isinstance(message.get("content"), str)
    )
    has_current = "Current memory evidence:" in prompt_text
    has_historical = "Historical or unverified memory context:" in prompt_text
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
            "model": body.get("model"),
            "message_count": len(messages),
            "has_current_memory_evidence": has_current,
            "has_historical_memory_context": has_historical,
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
            {"object": "embedding", "index": index, "embedding": [0.0] * 1536}
            for index, _ in enumerate(inputs)
        ],
        "model": body.get("model"),
    }


@app.get("/calls/{request_id}")
async def calls(request_id: str) -> dict[str, Any]:
    return {"request_id": request_id, "calls": _calls.get(request_id, [])}
