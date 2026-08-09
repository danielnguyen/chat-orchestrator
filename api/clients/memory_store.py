from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import httpx


class MemoryStoreClient:
    def __init__(self, base_url: str, api_key: str, timeout_ms: int = 30000) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout_ms / 1000

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
        if not isinstance(response, dict):
            raise RuntimeError("conversation_projection_invalid")

        response_conversation_id = response.get("conversation_id")
        response_owner_id = response.get("owner_id")
        if not isinstance(response_conversation_id, str) or not isinstance(
            response_owner_id, str
        ):
            raise RuntimeError("conversation_projection_invalid")
        if not _conversation_ids_equivalent(response_conversation_id, conversation_id):
            raise RuntimeError("conversation_projection_context_mismatch")
        if response_owner_id != owner_id:
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
        if not isinstance(response.get("created_at"), str) or not isinstance(
            response.get("updated_at"), str
        ):
            raise RuntimeError("conversation_projection_invalid")
        return response

    async def list_open_conversations(
        self,
        *,
        owner_id: str,
        updated_since: datetime | None = None,
        limit: int = 9,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "owner_id": owner_id,
            "lifecycle_state": "open",
            "limit": limit,
        }
        if updated_since is not None:
            if updated_since.tzinfo is None or updated_since.utcoffset() is None:
                raise ValueError("updated_since_timezone_required")
            params["updated_since"] = updated_since.isoformat()
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
