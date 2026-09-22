from __future__ import annotations

import re
from datetime import datetime
from typing import Any
from uuid import UUID

import httpx

_WORK_ASSOCIATION_FIELDS = {
    "work_id", "owner_id", "conversation_id", "request_id", "client_id", "surface",
}
_WORK_FIELDS = _WORK_ASSOCIATION_FIELDS | {
    "state", "created_at", "started_at", "completed_at", "assistant_message_id", "failure_code",
}
_WORK_FAILURE_CODES = {
    "interrupted", "execution_failed", "dependency_unavailable", "authority_unavailable",
}


def _work_uuid(value: Any) -> None:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise RuntimeError("work_projection_invalid") from None


def _work_identifier(value: Any, limit: int = 120) -> None:
    if (not isinstance(value, str) or not 1 <= len(value) <= limit
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", value) is None):
        raise RuntimeError("work_projection_invalid")


def _validate_work_projection(
    response: Any, *, expected: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate the bounded BMS reference-only contract, never answer content."""
    if not isinstance(response, dict) or set(response) != _WORK_FIELDS:
        raise RuntimeError("work_projection_invalid")
    for key in ("work_id", "conversation_id"):
        _work_uuid(response[key])
    for key in ("owner_id", "request_id"):
        _work_identifier(response[key])
    _work_identifier(response["surface"], 64)
    if response["client_id"] is not None:
        _work_identifier(response["client_id"])
    state = response["state"]
    if not isinstance(state, str) or state not in {"pending", "running", "completed", "failed"}:
        raise RuntimeError("work_projection_invalid")
    assistant = response["assistant_message_id"]
    failure = response["failure_code"]
    if (state == "completed") != (assistant is not None):
        raise RuntimeError("work_projection_invalid")
    if assistant is not None:
        _work_uuid(assistant)
    if (state == "failed") != (failure is not None):
        raise RuntimeError("work_projection_invalid")
    if failure is not None and (
        not isinstance(failure, str) or failure not in _WORK_FAILURE_CODES
    ):
        raise RuntimeError("work_projection_invalid")
    timestamps = {}
    for key in ("created_at", "started_at", "completed_at"):
        value = response[key]
        if value is None and key != "created_at":
            timestamps[key] = None
            continue
        try:
            if not isinstance(value, str) or len(value) > 64:
                raise ValueError
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError
        except (ValueError, TypeError):
            raise RuntimeError("work_projection_invalid") from None
        timestamps[key] = parsed
    created, started, completed = (timestamps[k] for k in (
        "created_at", "started_at", "completed_at",
    ))
    if (
        (state in {"running", "completed"} and started is None)
        or (state == "pending" and started is not None)
        or ((state in {"completed", "failed"}) != (completed is not None))
        or (started is not None and started < created)
        or (completed is not None and completed < (started or created))
    ):
        raise RuntimeError("work_projection_invalid")
    if expected and any(response.get(key) != value for key, value in expected.items()):
        raise RuntimeError("work_projection_context_mismatch")
    return response


def _validate_current_work(
    response: Any, *, expected: dict[str, Any], allow_none: bool = True,
) -> dict[str, Any]:
    if not isinstance(response, dict) or set(response) != {"status", "work"}:
        raise RuntimeError("current_work_response_invalid")
    if response["status"] == "none" and response["work"] is None and allow_none:
        return response
    if response["status"] != "resolved":
        raise RuntimeError("current_work_response_invalid")
    _validate_work_projection(response["work"], expected=expected)
    return response


def _validate_work_result(response: Any, *, expected: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(response, dict) or set(response) != {"work", "result"}:
        raise RuntimeError("work_result_invalid")
    work = _validate_work_projection(response["work"], expected=expected)
    result = response["result"]
    if work["state"] != "completed":
        if result is not None:
            raise RuntimeError("work_result_invalid")
    else:
        if not isinstance(result, dict) or set(result) != {"assistant_message_id", "content"}:
            raise RuntimeError("work_result_invalid")
        _work_uuid(result["assistant_message_id"])
        if (result["assistant_message_id"] != work["assistant_message_id"]
                or not isinstance(result["content"], str)):
            raise RuntimeError("work_result_invalid")
    return response


class MemoryStoreClient:
    def __init__(self, base_url: str, api_key: str, timeout_ms: int = 30000) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout_ms / 1000

    async def reconcile_interrupted_work(self) -> dict[str, int]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/v1/internal/work-items/reconcile-interrupted",
                headers={"X-API-Key": self.api_key},
            )
            response.raise_for_status()
            result = response.json()
        if (
            not isinstance(result, dict)
            or set(result) != {"interrupted_count"}
            or type(result["interrupted_count"]) is not int
            or result["interrupted_count"] < 0
        ):
            raise RuntimeError("work_reconciliation_response_invalid")
        return result

    async def create_work(
        self, *, owner_id: str, conversation_id: str, request_id: str,
        client_id: str | None, surface: str,
    ) -> dict[str, Any]:
        association = dict(owner_id=owner_id, conversation_id=conversation_id,
                           request_id=request_id, client_id=client_id, surface=surface)
        response = await self._post(
            "/v1/internal/work-items", request_id=request_id, json=association,
        )
        return _validate_work_projection(response, expected=association)

    async def get_work(
        self, *, work_id: str, owner_id: str, conversation_id: str,
    ) -> dict[str, Any]:
        _work_uuid(work_id)
        response = await self._get(
            f"/v1/internal/work-items/{work_id}",
            params={"owner_id": owner_id, "conversation_id": conversation_id},
        )
        return _validate_work_projection(response, expected={
            "work_id": work_id, "owner_id": owner_id, "conversation_id": conversation_id,
        })

    async def _work_write(
        self, method: str, path: str, payload: dict[str, Any],
    ) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.request(
                method, f"{self.base_url}{path}",
                headers={"X-API-Key": self.api_key}, json=payload,
            )
            response.raise_for_status()
            return response.json()

    async def get_work_result(
        self, *, work_id: str, owner_id: str, conversation_id: str,
    ) -> dict[str, Any] | None:
        _work_uuid(work_id)
        _work_uuid(conversation_id)
        _work_identifier(owner_id)
        try:
            response = await self._get(
                f"/v1/internal/work-items/{work_id}/result",
                params={"owner_id": owner_id, "conversation_id": conversation_id},
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise
        return _validate_work_result(response, expected={
            "work_id": work_id, "owner_id": owner_id, "conversation_id": conversation_id,
        })

    async def transition_work(
        self, *, work: dict[str, Any], state: str,
        assistant_message_id: str | None = None, failure_code: str | None = None,
    ) -> dict[str, Any]:
        _validate_work_projection(work)
        payload = {
            "owner_id": work["owner_id"], "conversation_id": work["conversation_id"],
            "state": state, "assistant_message_id": assistant_message_id,
            "failure_code": failure_code,
        }
        expected = {key: work[key] for key in _WORK_ASSOCIATION_FIELDS}
        expected.update(payload)
        expected["created_at"] = work["created_at"]
        if work["started_at"] is not None:
            expected["started_at"] = work["started_at"]
        if work["completed_at"] is not None:
            expected["completed_at"] = work["completed_at"]
        response = await self._work_write(
            "PATCH", f"/v1/internal/work-items/{work['work_id']}", payload,
        )
        return _validate_work_projection(response, expected=expected)

    async def set_current_work(
        self, *, owner_id: str, client_id: str, work_id: str,
    ) -> dict[str, Any]:
        _work_identifier(client_id)
        _work_uuid(work_id)
        response = await self._work_write("PUT", "/v1/internal/current-work", {
            "owner_id": owner_id, "client_id": client_id, "work_id": work_id,
        })
        return _validate_current_work(response, expected={
            "owner_id": owner_id, "client_id": client_id, "work_id": work_id,
        }, allow_none=False)

    async def get_current_work(self, *, owner_id: str, client_id: str) -> dict[str, Any]:
        _work_identifier(client_id)
        response = await self._get("/v1/internal/current-work", params={
            "owner_id": owner_id, "client_id": client_id,
        })
        return _validate_current_work(response, expected={
            "owner_id": owner_id, "client_id": client_id,
        })

    async def _post(
        self,
        path: str,
        *,
        request_id: str | None = None,
        json: dict[str, Any],
    ) -> dict[str, Any]:
        headers = {"X-API-Key": self.api_key}
        if request_id:
            headers["X-Request-ID"] = request_id
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(f"{self.base_url}{path}", headers=headers, json=json)
            resp.raise_for_status()
            return resp.json()

    async def _get(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers = {"X-API-Key": self.api_key}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(
                f"{self.base_url}{path}",
                headers=headers,
                params=params,
            )
            resp.raise_for_status()
            return resp.json()

    async def resolve_conversation(
        self,
        *,
        owner_id: str,
        client_id: str | None,
        title: str | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/conversations/resolve",
            json={"owner_id": owner_id, "client_id": client_id, "title": title},
        )

    async def get_conversation(
        self,
        *,
        conversation_id: str,
        owner_id: str,
    ) -> dict[str, Any]:
        response = await self._get(
            f"/v1/conversations/{conversation_id}",
            params={"owner_id": owner_id},
        )
        return _validate_conversation_projection(
            response,
            conversation_id=conversation_id,
            owner_id=owner_id,
        )

    async def list_open_conversations(
        self,
        *,
        owner_id: str,
        updated_since: datetime | None = None,
        updated_before: datetime | None = None,
        limit: int = 9,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "owner_id": owner_id,
            "lifecycle_state": "open",
            "limit": limit,
        }
        if updated_since is not None:
            _require_aware_datetime(updated_since, "updated_since")
            params["updated_since"] = updated_since.isoformat()
        if updated_before is not None:
            _require_aware_datetime(updated_before, "updated_before")
            params["updated_before"] = updated_before.isoformat()
        response = await self._get(
            "/v1/conversations",
            params=params,
        )
        if not isinstance(response, dict) or set(response) != {
            "conversations",
            "next_cursor",
        }:
            raise RuntimeError("conversation_list_response_invalid")
        conversations = response.get("conversations")
        next_cursor = response.get("next_cursor")
        if (
            not isinstance(conversations, list)
            or len(conversations) > limit
            or next_cursor is not None
            and (
                not isinstance(next_cursor, str)
                or not next_cursor
                or len(next_cursor) > 2048
            )
        ):
            raise RuntimeError("conversation_list_response_invalid")

        seen: set[str] = set()
        validated: list[dict[str, Any]] = []
        for row in conversations:
            if not isinstance(row, dict):
                raise RuntimeError("conversation_list_response_invalid")
            conversation_id = row.get("conversation_id")
            try:
                canonical_id = str(UUID(conversation_id))
            except (TypeError, ValueError, AttributeError):
                raise RuntimeError("conversation_list_response_invalid") from None
            if conversation_id != canonical_id:
                raise RuntimeError("conversation_list_response_invalid")
            if canonical_id in seen:
                raise RuntimeError("conversation_list_response_invalid")
            seen.add(canonical_id)
            if row.get("owner_id") != owner_id:
                raise RuntimeError("conversation_list_response_context_mismatch")
            if row.get("lifecycle_state") != "open":
                raise RuntimeError("conversation_list_response_context_mismatch")
            if row.get("superseded_by_conversation_id") is not None:
                raise RuntimeError("conversation_list_response_invalid")
            updated_at = row.get("updated_at")
            if not isinstance(updated_at, str):
                raise RuntimeError("conversation_list_response_invalid")
            try:
                parsed_updated_at = datetime.fromisoformat(updated_at)
            except ValueError:
                raise RuntimeError("conversation_list_response_invalid") from None
            if parsed_updated_at.tzinfo is None or parsed_updated_at.utcoffset() is None:
                raise RuntimeError("conversation_list_response_invalid")
            validated.append(row)
        return {"conversations": validated, "next_cursor": next_cursor}

    async def close_conversation(
        self,
        *,
        conversation_id: str,
        owner_id: str,
        expected_updated_at: datetime,
    ) -> dict[str, Any]:
        _require_aware_datetime(expected_updated_at, "expected_updated_at")
        response = await self._post(
            f"/v1/conversations/{conversation_id}/lifecycle",
            json={
                "owner_id": owner_id,
                "lifecycle_state": "closed",
                "expected_updated_at": expected_updated_at.isoformat(),
            },
        )
        projection = _validate_conversation_projection(
            response,
            conversation_id=conversation_id,
            owner_id=owner_id,
        )
        if (
            projection["lifecycle_state"] != "closed"
            or projection["superseded_by_conversation_id"] is not None
        ):
            raise RuntimeError("conversation_lifecycle_response_invalid")
        return projection

    async def create_conversation(
        self,
        *,
        owner_id: str,
        client_id: str | None,
    ) -> dict[str, Any]:
        response = await self._post(
            "/v1/conversations",
            json={"owner_id": owner_id, "client_id": client_id},
        )
        if not isinstance(response, dict) or set(response) != {"conversation_id"}:
            raise RuntimeError("conversation_create_response_invalid")
        conversation_id = response.get("conversation_id")
        try:
            canonical_id = str(UUID(conversation_id))
        except (TypeError, ValueError, AttributeError):
            raise RuntimeError("conversation_create_response_invalid") from None
        if conversation_id != canonical_id:
            raise RuntimeError("conversation_create_response_invalid")
        return response

    async def add_message(
        self,
        *,
        conversation_id: str,
        owner_id: str,
        role: str,
        content: str,
        client_id: str | None,
        metadata: dict[str, Any] | None = None,
        policy_metadata: dict[str, Any] | None = None,
        history_root_lineage: dict[str, Any] | None = None,
        message_id: str | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "owner_id": owner_id,
            "role": role,
            "content": content,
            "client_id": client_id,
            "metadata": metadata,
        }
        if policy_metadata is not None:
            payload["policy_metadata"] = policy_metadata
        if history_root_lineage is not None:
            payload["history_root_lineage"] = history_root_lineage
        if message_id is not None:
            payload["message_id"] = message_id
        response = await self._post(
            f"/v1/conversations/{conversation_id}/messages",
            request_id=request_id,
            json=payload,
        )
        if not isinstance(response, dict):
            raise RuntimeError("message_append_response_invalid")
        response_message_id = response.get("message_id")
        if (
            not isinstance(response_message_id, str)
            or not response_message_id
            or len(response_message_id) > 120
        ):
            raise RuntimeError("message_append_response_invalid")
        if message_id is not None and not _conversation_ids_equivalent(
            response_message_id, message_id
        ):
            raise RuntimeError("message_append_response_context_mismatch")
        return response

    async def retrieve_bundle(
        self,
        *,
        request_id: str,
        conversation_id: str,
        owner_id: str,
        query: str,
        retrieval: dict[str, Any] | None,
        include_artifacts: bool | None = None,
        allowed_memory_domains: list[str] | None = None,
        blocked_memory_domains: list[str] | None = None,
        containment_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "query": query,
            "mode": "augmented",
            "retrieval": retrieval,
        }
        if include_artifacts is not None:
            payload["include_artifacts"] = include_artifacts
        if containment_policy is not None:
            payload["containment_policy"] = containment_policy
        elif allowed_memory_domains:
            payload["allowed_memory_domains"] = allowed_memory_domains
        if containment_policy is None and blocked_memory_domains:
            payload["blocked_memory_domains"] = blocked_memory_domains
        response = await self._post(
            f"/v2/conversations/{conversation_id}/retrieve",
            request_id=request_id,
            json=payload,
        )
        response_request_id = response.get("request_id")
        if response_request_id is not None and response_request_id != request_id:
            raise RuntimeError("retrieval_request_id_mismatch")
        return response

    async def select_recall(
        self,
        *,
        request_id: str,
        owner_id: str,
        context: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        response = await self._post(
            "/v1/internal/recall/select",
            request_id=request_id,
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "context": context,
                "candidates": candidates,
            },
        )
        if response.get("request_id") != request_id or response.get("owner_id") != owner_id:
            raise RuntimeError("recall_response_context_mismatch")
        return response

    async def retrieve_episode_callbacks(
        self,
        *,
        request_id: str,
        owner_id: str,
        context: dict[str, Any],
        limit: int = 10,
    ) -> dict[str, Any]:
        response = await self._post(
            "/v1/internal/episodes/retrieve",
            request_id=request_id,
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "context": context,
                "limit": limit,
            },
        )
        if response.get("request_id") != request_id or response.get("owner_id") != owner_id:
            raise RuntimeError("episode_response_context_mismatch")
        return response

    async def resolve_profile(
        self,
        *,
        owner_id: str,
        surface: str,
        requested_profile: str | None,
        client_id: str | None,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/profiles/resolve",
            json={
                "owner_id": owner_id,
                "surface": surface,
                "requested_profile": requested_profile,
                "client_id": client_id,
            },
        )

    async def create_trace(self, *, request_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("/v1/traces", request_id=request_id, json=payload)

    async def create_claim_record(
        self,
        *,
        request_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/internal/claim-records",
            request_id=request_id,
            json=payload,
        )

    async def list_claim_records(
        self,
        *,
        owner_id: str,
        conversation_id: str,
        limit: int = 20,
    ) -> dict[str, Any]:
        if not 1 <= limit <= 20:
            raise ValueError("claim_record_limit_out_of_range")
        return await self._get(
            "/v1/internal/claim-records",
            params={
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "limit": limit,
            },
        )

    async def get_trace(self, request_id: str) -> dict[str, Any]:
        return await self._get(f"/v1/traces/{request_id}")

    async def resolve_acquisition_history(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        target_mode: str,
        normalized_first_paragraph: str,
        response_digest: str | None = None,
    ) -> dict[str, Any]:
        if target_mode == "immediate_previous":
            if response_digest is None:
                raise ValueError("acquisition_history_response_digest_required")
        elif target_mode == "quoted_first_paragraph":
            if response_digest is not None:
                raise ValueError("acquisition_history_response_digest_not_allowed")
        else:
            raise ValueError("acquisition_history_target_mode_invalid")
        payload: dict[str, Any] = {
            "schema_version": "acquisition-history-resolution.v1",
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "target_mode": target_mode,
            "normalized_first_paragraph": normalized_first_paragraph,
        }
        if response_digest is not None:
            payload["response_digest"] = response_digest
        response = await self._post(
            "/v1/internal/acquisition-history/resolve",
            request_id=request_id,
            json=payload,
        )
        expected_scope = {
            "schema_version": "acquisition-history-resolution.v1",
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "target_mode": target_mode,
        }
        if any(response.get(key) != value for key, value in expected_scope.items()):
            raise RuntimeError("acquisition_history_response_context_mismatch")
        return response

    async def resolve_immediate_history(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        explanation_kind: str,
    ) -> dict[str, Any]:
        payload = {
            "schema_version": "immediate-history-resolution.v2",
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "explanation_kind": explanation_kind,
        }
        response = await self._post(
            "/v1/internal/immediate-history/resolve",
            request_id=request_id,
            json=payload,
        )
        if not isinstance(response, dict) or any(
            response.get(key) != value for key, value in payload.items()
        ):
            raise RuntimeError("immediate_history_response_context_mismatch")
        return response


def _conversation_ids_equivalent(actual: Any, expected: str) -> bool:
    if not isinstance(actual, str):
        return False
    if actual == expected:
        return True
    try:
        return UUID(actual) == UUID(expected)
    except (TypeError, ValueError):
        return False


def _require_aware_datetime(value: datetime, field: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field}_timezone_required")


def _parse_aware_projection_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise RuntimeError("conversation_projection_invalid")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise RuntimeError("conversation_projection_invalid") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RuntimeError("conversation_projection_invalid")
    return parsed


def _validate_conversation_projection(
    response: Any,
    *,
    conversation_id: str,
    owner_id: str,
) -> dict[str, Any]:
    expected_fields = {
        "conversation_id",
        "owner_id",
        "client_id",
        "title",
        "lifecycle_state",
        "superseded_by_conversation_id",
        "created_at",
        "updated_at",
    }
    if not isinstance(response, dict) or set(response) != expected_fields:
        raise RuntimeError("conversation_projection_invalid")
    response_conversation_id = response.get("conversation_id")
    response_owner_id = response.get("owner_id")
    if not isinstance(response_conversation_id, str) or not isinstance(
        response_owner_id, str
    ):
        raise RuntimeError("conversation_projection_invalid")
    if (
        not _conversation_ids_equivalent(response_conversation_id, conversation_id)
        or response_owner_id != owner_id
    ):
        raise RuntimeError("conversation_projection_context_mismatch")

    lifecycle_state = response.get("lifecycle_state")
    if lifecycle_state not in {"open", "closed", "superseded"}:
        raise RuntimeError("conversation_projection_invalid")
    replacement = response.get("superseded_by_conversation_id")
    if lifecycle_state == "superseded":
        if not isinstance(replacement, str) or not replacement.strip():
            raise RuntimeError("conversation_projection_invalid")
    elif replacement is not None:
        raise RuntimeError("conversation_projection_invalid")

    for field in ("client_id", "title"):
        if response.get(field) is not None and not isinstance(response.get(field), str):
            raise RuntimeError("conversation_projection_invalid")
    _parse_aware_projection_datetime(response.get("created_at"))
    _parse_aware_projection_datetime(response.get("updated_at"))
    return response
