from __future__ import annotations

from typing import Any

import httpx
from services.privacy_context import validate_privacy_policy_result

_PREFERRED_COMPANION_COMPILE_PATH = "/v1/companion/profile/compile"
_COMPAT_COMPANION_COMPILE_PATH = "/v1/companion/policy/compile"
_COMPANION_ENDPOINT_KEY = "_cognitive_runtime_compile_endpoint"


class RuntimeClient:
    def __init__(self, base_url: str, api_key: str | None, timeout_ms: int = 30000) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout_ms / 1000
        self.last_companion_compile_endpoint: str | None = None

    async def _post(self, path: str, *, json: dict[str, Any]) -> dict[str, Any]:
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(f"{self.base_url}{path}", headers=headers, json=json)
            resp.raise_for_status()
            return resp.json()

    async def overlay(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/runtime/overlay",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "surface": surface,
            },
        )

    async def resolve_session(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        surface_session_id: str | None = None,
        active_mode: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if surface_session_id is not None:
            payload["surface_session_id"] = surface_session_id
        if active_mode is not None:
            payload["active_mode"] = active_mode
        return await self._post("/v1/runtime/sessions/resolve", json=payload)

    async def start_turn(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        input_message_id: str | None = None,
        intent_class: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if input_message_id is not None:
            payload["input_message_id"] = input_message_id
        if intent_class is not None:
            payload["intent_class"] = intent_class
        return await self._post("/v1/runtime/turns/start", json=payload)

    async def update_turn(
        self,
        *,
        request_id: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        turn_status: str,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/runtime/turns/update",
            json={
                "request_id": request_id,
                "runtime_session_id": runtime_session_id,
                "runtime_turn_id": runtime_turn_id,
                "turn_status": turn_status,
            },
        )

    async def complete_turn(
        self,
        *,
        request_id: str,
        runtime_session_id: str,
        runtime_turn_id: str,
        turn_status: str,
        continuation_state: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "runtime_session_id": runtime_session_id,
            "runtime_turn_id": runtime_turn_id,
            "turn_status": turn_status,
        }
        if continuation_state is not None:
            payload["continuation_state"] = continuation_state
        return await self._post("/v1/runtime/turns/complete", json=payload)

    async def resolve_identity(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        return await self._post("/v1/runtime/identity/resolve", json=payload)

    async def world_state_resolve(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        active_persona_id: str | None = None,
        requested_domains: list[str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if active_persona_id is not None:
            payload["active_persona_id"] = active_persona_id
        if requested_domains:
            payload["requested_domains"] = requested_domains
        return await self._post("/v1/world-state/resolve", json=payload)

    async def relationship_select(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        active_persona_id: str | None = None,
        requested_scopes: list[str] | None = None,
        entity_ids: list[str] | None = None,
        relationship_types: list[str] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if active_persona_id is not None:
            payload["active_persona_id"] = active_persona_id
        if requested_scopes:
            payload["requested_scopes"] = requested_scopes
        if entity_ids:
            payload["entity_ids"] = entity_ids
        if relationship_types:
            payload["relationship_types"] = relationship_types
        return await self._post("/v1/relationships/select", json=payload)

    async def compile_companion_policy(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        requested_scene: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if requested_scene is not None:
            payload["requested_scene"] = requested_scene

        self.last_companion_compile_endpoint = _PREFERRED_COMPANION_COMPILE_PATH
        try:
            response = await self._post(_PREFERRED_COMPANION_COMPILE_PATH, json=payload)
            return _with_compile_endpoint(response, _PREFERRED_COMPANION_COMPILE_PATH)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code not in {404, 405}:
                raise

        self.last_companion_compile_endpoint = _COMPAT_COMPANION_COMPILE_PATH
        response = await self._post(_COMPAT_COMPANION_COMPILE_PATH, json=payload)
        return _with_compile_endpoint(response, _COMPAT_COMPANION_COMPILE_PATH)

    async def evaluate_interrupt(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        current_user_text: str | None = None,
        recent_messages: list[dict[str, Any]] | None = None,
        requested_scene: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if current_user_text is not None:
            payload["current_user_text"] = current_user_text
        if recent_messages is not None:
            payload["recent_messages"] = recent_messages
        if requested_scene is not None:
            payload["requested_scene"] = requested_scene
        return await self._post("/v1/interrupt/evaluate", json=payload)

    async def evaluate_interaction_governance(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        surface_session_id: str | None = None,
        active_mode: str | None = None,
        current_user_text: str | None = None,
        recent_messages: list[dict[str, Any]] | None = None,
        surface_metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if surface_session_id is not None:
            payload["surface_session_id"] = surface_session_id
        if active_mode is not None:
            payload["active_mode"] = active_mode
        if current_user_text is not None:
            payload["current_user_text"] = current_user_text
        if recent_messages is not None:
            payload["recent_messages"] = recent_messages
        if surface_metadata_json is not None:
            payload["surface_metadata_json"] = surface_metadata_json
        return await self._post("/v1/runtime/interaction-governance/evaluate", json=payload)

    async def evaluate_persona_containment(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        active_persona_id: str | None = None,
        requested_persona_id: str | None = None,
        persona_scope_hint: str | None = None,
        interaction_kind: str | None = None,
        current_user_text: str | None = None,
        recent_messages: list[dict[str, Any]] | None = None,
        surface_metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if active_persona_id is not None:
            payload["active_persona_id"] = active_persona_id
        if requested_persona_id is not None:
            payload["requested_persona_id"] = requested_persona_id
        if persona_scope_hint is not None:
            payload["persona_scope_hint"] = persona_scope_hint
        if interaction_kind is not None:
            payload["interaction_kind"] = interaction_kind
        if current_user_text is not None:
            payload["current_user_text"] = current_user_text
        if recent_messages is not None:
            payload["recent_messages"] = recent_messages
        if surface_metadata_json is not None:
            payload["surface_metadata_json"] = surface_metadata_json
        return await self._post("/v1/runtime/persona-containment/evaluate", json=payload)

    async def evaluate_restraint(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        interaction_kind: str | None = None,
        response_posture: str | None = None,
        active_persona_id: str | None = None,
        capability_domain: str | None = None,
        current_user_text: str | None = None,
        recent_messages: list[dict[str, Any]] | None = None,
        surface_metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if interaction_kind is not None:
            payload["interaction_kind"] = interaction_kind
        if response_posture is not None:
            payload["response_posture"] = response_posture
        if active_persona_id is not None:
            payload["active_persona_id"] = active_persona_id
        if capability_domain is not None:
            payload["capability_domain"] = capability_domain
        if current_user_text is not None:
            payload["current_user_text"] = current_user_text
        if recent_messages is not None:
            payload["recent_messages"] = recent_messages
        if surface_metadata_json is not None:
            payload["surface_metadata_json"] = surface_metadata_json
        return await self._post("/v1/runtime/restraint/evaluate", json=payload)

    async def evaluate_memory_hygiene(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        items: list[dict[str, Any]],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "items": items,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        return await self._post("/v1/runtime/memory-hygiene/evaluate", json=payload)

    async def evaluate_privacy_context(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        runtime_session_id: str | None = None,
        runtime_turn_id: str | None = None,
        surface_category: str | None = None,
        sensitivity_level: str,
        sensitivity_domains: list[str],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": request_id,
            "owner_id": owner_id,
            "conversation_id": conversation_id,
            "surface": surface,
            "sensitivity_level": sensitivity_level,
            "sensitivity_domains": sensitivity_domains,
        }
        if runtime_session_id is not None:
            payload["runtime_session_id"] = runtime_session_id
        if runtime_turn_id is not None:
            payload["runtime_turn_id"] = runtime_turn_id
        if surface_category is not None:
            payload["surface_category"] = surface_category

        response = await self._post("/v1/runtime/privacy-context/evaluate", json=payload)
        if not isinstance(response, dict):
            raise ValueError("malformed_privacy_context_response")
        result = validate_privacy_policy_result(response.get("result"))
        if result is None:
            raise ValueError("invalid_privacy_context_result")
        validated_response = dict(response)
        validated_response["result"] = result
        return validated_response

    async def reset(
        self,
        *,
        request_id: str,
        owner_id: str,
        conversation_id: str,
        surface: str,
        reason: str,
    ) -> dict[str, Any]:
        return await self._post(
            "/v1/runtime/state/reset",
            json={
                "request_id": request_id,
                "owner_id": owner_id,
                "conversation_id": conversation_id,
                "surface": surface,
                "reason": reason,
            },
        )


def _with_compile_endpoint(response: Any, endpoint: str) -> Any:
    if isinstance(response, dict):
        enriched = dict(response)
        enriched[_COMPANION_ENDPOINT_KEY] = endpoint
        return enriched
    return response
