from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

CAPABILITY_DESCRIPTOR_VERSION = "co.capability-descriptor.v1"
CAPABILITY_ARGUMENT_SCHEMA_VERSION = "co.capability-args.v1"
MAX_ARGUMENT_BYTES = 4096

_SAFE_ID = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
_SAFE_PROVIDER_TOOL_NAME = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SAFE_LABEL = re.compile(r"^[A-Za-z0-9_.:@/-]{1,120}$")
_WORLD_STATE_DOMAINS = {
    "active_project",
    "active_repository",
    "active_artifact",
    "active_tool_session",
    "active_external_system",
    "pending_action",
    "runtime_surface",
}
_WORLD_STATE_OUTPUT_MODES = {"summary", "structured"}
_DRAFT_TONES = {"neutral", "warm", "direct"}
_DRAFT_FORMATS = {"plain_text", "markdown"}


class CapabilityValidationError(ValueError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(frozen=True)
class CapabilityEntry:
    capability_id: str
    provider_tool_name: str
    operation_class: str
    capability_domain: str
    supported_surfaces: tuple[str, ...]
    executor_binding: str
    descriptor_metadata: dict[str, Any]
    privacy_classification: str
    authorization_requirements: dict[str, Any]
    argument_schema: dict[str, Any]


@dataclass(frozen=True)
class ParsedCapabilityRequest:
    capability_id: str
    provider_tool_name: str
    arguments: dict[str, Any]


@dataclass(frozen=True)
class CapabilityValidationResult:
    capability_id: str
    schema_version: str
    normalized_arguments: dict[str, Any]
    argument_digest: str
    trace: dict[str, Any]


PRODUCTION_CAPABILITIES: tuple[CapabilityEntry, ...] = (
    CapabilityEntry(
        capability_id="runtime.world_state.read",
        provider_tool_name="runtime_world_state_read",
        operation_class="read",
        capability_domain="software_architecture",
        supported_surfaces=("dev", "vscode"),
        executor_binding="cr_world_state_read",
        descriptor_metadata={
            "display_name": "Read runtime world state",
            "description": "Read bounded runtime world-state claims as structured context.",
        },
        privacy_classification="runtime_context",
        authorization_requirements={
            "authorization_phases": ["exposure", "selection", "dispatch"],
            "relationship_requirements": [],
            "world_state_requirements": [],
        },
        argument_schema={
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "requested_domains": {
                    "type": "array",
                    "items": {"type": "string", "enum": sorted(_WORLD_STATE_DOMAINS)},
                    "maxItems": 4,
                },
                "entity_id": {"type": "string", "maxLength": 120},
                "attribute": {"type": "string", "maxLength": 64},
                "output_mode": {
                    "type": "string",
                    "enum": sorted(_WORLD_STATE_OUTPUT_MODES),
                },
            },
        },
    ),
    CapabilityEntry(
        capability_id="draft.local_message",
        provider_tool_name="draft_local_message",
        operation_class="draft",
        capability_domain="software_architecture",
        supported_surfaces=("dev", "vscode"),
        executor_binding="local_message_draft",
        descriptor_metadata={
            "display_name": "Draft local message",
            "description": "Create a local unsent message draft with no delivery side effect.",
        },
        privacy_classification="local_unsent_draft",
        authorization_requirements={
            "authorization_phases": ["exposure", "selection", "dispatch"],
            "relationship_requirements": [],
            "world_state_requirements": [],
        },
        argument_schema={
            "type": "object",
            "additionalProperties": False,
            "required": ["body"],
            "properties": {
                "recipient_label": {"type": "string", "maxLength": 80},
                "subject": {"type": "string", "maxLength": 120},
                "body": {"type": "string", "minLength": 1, "maxLength": 2000},
                "tone": {"type": "string", "enum": sorted(_DRAFT_TONES)},
                "format": {"type": "string", "enum": sorted(_DRAFT_FORMATS)},
            },
        },
    ),
)


def production_capability_registry() -> tuple[CapabilityEntry, ...]:
    validate_production_registry(PRODUCTION_CAPABILITIES)
    return PRODUCTION_CAPABILITIES


def validate_production_registry(entries: tuple[CapabilityEntry, ...]) -> None:
    ids = [entry.capability_id for entry in entries]
    if ids != ["runtime.world_state.read", "draft.local_message"]:
        raise RuntimeError("production_capability_registry_unexpected_ids")
    for entry in entries:
        if not entry.executor_binding:
            raise RuntimeError(f"missing_executor_binding:{entry.capability_id}")
        if not _SAFE_ID.fullmatch(entry.capability_id):
            raise RuntimeError(f"invalid_capability_id:{entry.capability_id}")
        if not _SAFE_PROVIDER_TOOL_NAME.fullmatch(entry.provider_tool_name):
            raise RuntimeError(f"invalid_provider_tool_name:{entry.capability_id}")
        if entry.provider_tool_name == entry.capability_id:
            raise RuntimeError(f"provider_tool_name_not_distinct:{entry.capability_id}")
    provider_names = [entry.provider_tool_name for entry in entries]
    if len(provider_names) != len(set(provider_names)):
        raise RuntimeError("duplicate_provider_tool_name")


def capability_by_id(capability_id: str) -> CapabilityEntry | None:
    for entry in production_capability_registry():
        if entry.capability_id == capability_id:
            return entry
    return None


def capability_by_provider_tool_name(provider_tool_name: str) -> CapabilityEntry | None:
    for entry in production_capability_registry():
        if entry.provider_tool_name == provider_tool_name:
            return entry
    return None


def provider_descriptor(entry: CapabilityEntry) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": entry.provider_tool_name,
            "description": entry.descriptor_metadata["description"],
            "parameters": entry.argument_schema,
        },
        "metadata": {
            "capability_id": entry.capability_id,
            "provider_tool_name": entry.provider_tool_name,
            "operation_class": entry.operation_class,
            "capability_domain": entry.capability_domain,
            "privacy_classification": entry.privacy_classification,
            "descriptor_version": CAPABILITY_DESCRIPTOR_VERSION,
            "schema_version": CAPABILITY_ARGUMENT_SCHEMA_VERSION,
            "local_only": entry.capability_id == "draft.local_message",
        },
    }


def provider_descriptors(entries: list[CapabilityEntry]) -> list[dict[str, Any]]:
    return [
        provider_descriptor(entry)
        for entry in sorted(entries, key=lambda item: item.capability_id)
    ]


def descriptor_fingerprint(descriptors: list[dict[str, Any]]) -> str:
    material = _canonical_json(descriptors)
    return f"capdesc_{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"


async def filter_capability_descriptors_for_exposure(
    *,
    runtime: Any | None,
    request_id: str,
    owner_id: str,
    conversation_id: str,
    surface: str,
    runtime_session_id: str | None,
    runtime_turn_id: str | None,
    active_persona_id: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidates = list(production_capability_registry())
    candidate_ids = [entry.capability_id for entry in candidates]
    trace: dict[str, Any] = {
        "status": "failed_closed",
        "schema_version": CAPABILITY_ARGUMENT_SCHEMA_VERSION,
        "descriptor_version": CAPABILITY_DESCRIPTOR_VERSION,
        "candidate_capability_ids": candidate_ids,
        "exposed_capability_ids": [],
        "blocked_capability_ids": candidate_ids,
        "blocked_reasons": {
            capability_id: "exposure_authorization_unavailable"
            for capability_id in candidate_ids
        },
        "descriptor_count": 0,
        "descriptor_fingerprint": descriptor_fingerprint([]),
    }
    if (
        runtime is None
        or not runtime_session_id
        or not runtime_turn_id
        or not active_persona_id
        or not hasattr(runtime, "authorize_capability")
    ):
        trace["status_reason"] = "runtime_context_unavailable"
        return [], trace

    exposed: list[CapabilityEntry] = []
    blocked_reasons: dict[str, str] = {}
    for entry in candidates:
        try:
            response = await runtime.authorize_capability(
                request_id=f"{request_id}:{entry.capability_id}:exposure",
                owner_id=owner_id,
                conversation_id=conversation_id,
                surface=surface,
                runtime_session_id=runtime_session_id,
                runtime_turn_id=runtime_turn_id,
                active_persona_id=active_persona_id,
                authorization_phase="exposure",
                capability_id=entry.capability_id,
                capability_domain=entry.capability_domain,
                operation_class=entry.operation_class,
                argument_digest=None,
                supported_surfaces=list(entry.supported_surfaces),
                relationship_requirements=entry.authorization_requirements.get(
                    "relationship_requirements",
                    [],
                ),
                selected_relationship_ids=[],
                world_state_requirements=entry.authorization_requirements.get(
                    "world_state_requirements",
                    [],
                ),
                selected_world_state_claim_ids=[],
                confirmation_challenge_ref=None,
            )
        except Exception:
            trace["status_reason"] = "exposure_authorization_unavailable"
            return [], trace
        result = response.get("result") if isinstance(response, dict) else None
        if not isinstance(result, dict) or not isinstance(result.get("allowed"), bool):
            trace["status_reason"] = "malformed_exposure_authorization"
            return [], trace
        if result["allowed"]:
            exposed.append(entry)
        else:
            blocked_reasons[entry.capability_id] = _bounded_reason(result)

    descriptors = provider_descriptors(exposed)
    exposed_ids = [entry.capability_id for entry in exposed]
    blocked_ids = [
        entry.capability_id
        for entry in candidates
        if entry.capability_id not in exposed_ids
    ]
    trace.update(
        {
            "status": "ok",
            "status_reason": "exposure_authorization_complete",
            "exposed_capability_ids": exposed_ids,
            "blocked_capability_ids": blocked_ids,
            "blocked_reasons": blocked_reasons,
            "descriptor_count": len(descriptors),
            "descriptor_fingerprint": descriptor_fingerprint(descriptors),
        }
    )
    return descriptors, trace


def parse_provider_capability_request(completion: dict[str, Any]) -> ParsedCapabilityRequest | None:
    message = _completion_message(completion)
    if message is None:
        return None
    requests: list[ParsedCapabilityRequest] = []
    tool_calls = message.get("tool_calls")
    if tool_calls is not None:
        if not isinstance(tool_calls, list):
            raise CapabilityValidationError("malformed_capability_call")
        for call in tool_calls:
            if not isinstance(call, dict):
                raise CapabilityValidationError("malformed_capability_call")
            function = call.get("function")
            if not isinstance(function, dict):
                raise CapabilityValidationError("malformed_capability_call")
            requests.append(
                _request_from_parts(function.get("name"), function.get("arguments"))
            )
    capability_request = message.get("capability_request")
    if capability_request is not None:
        if not isinstance(capability_request, dict):
            raise CapabilityValidationError("malformed_capability_call")
        requests.append(
            _request_from_parts(
                capability_request.get("capability_id"),
                capability_request.get("arguments"),
            )
        )
    if not requests:
        return None
    if len(requests) != 1:
        raise CapabilityValidationError("multiple_capability_calls")
    return requests[0]


def validate_and_digest_capability_request(
    *,
    request: ParsedCapabilityRequest,
    exposed_capability_ids: list[str],
) -> CapabilityValidationResult:
    if request.capability_id not in exposed_capability_ids:
        if capability_by_id(request.capability_id) is None:
            raise CapabilityValidationError("unknown_capability_id")
        raise CapabilityValidationError("capability_not_exposed")
    entry = capability_by_id(request.capability_id)
    if entry is None:
        raise CapabilityValidationError("unknown_capability_id")
    normalized = normalize_arguments(entry, request.arguments)
    digest_material = {
        "capability_id": entry.capability_id,
        "arguments": normalized,
    }
    digest = (
        f"capargs_"
        f"{hashlib.sha256(_canonical_json(digest_material).encode('utf-8')).hexdigest()}"
    )
    return CapabilityValidationResult(
        capability_id=entry.capability_id,
        schema_version=CAPABILITY_ARGUMENT_SCHEMA_VERSION,
        normalized_arguments=normalized,
        argument_digest=digest,
        trace={
            "capability_id": entry.capability_id,
            "provider_tool_name": entry.provider_tool_name,
            "schema_version": CAPABILITY_ARGUMENT_SCHEMA_VERSION,
            "validation_status": "ok",
            "argument_digest": digest,
            "reason_code": "validated",
        },
    )


def capability_validation_failure_trace(
    reason_code: str,
    capability_id: str | None = None,
    provider_tool_name: str | None = None,
) -> dict[str, Any]:
    trace = {
        "schema_version": CAPABILITY_ARGUMENT_SCHEMA_VERSION,
        "validation_status": "rejected",
        "reason_code": reason_code,
    }
    if capability_id:
        trace["capability_id"] = capability_id[:120]
    if provider_tool_name:
        trace["provider_tool_name"] = provider_tool_name[:80]
    return trace


def normalize_arguments(entry: CapabilityEntry, arguments: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(arguments, dict):
        raise CapabilityValidationError("malformed_arguments")
    try:
        encoded = _canonical_json(arguments)
    except TypeError as exc:
        raise CapabilityValidationError("malformed_arguments") from exc
    if len(encoded.encode("utf-8")) > MAX_ARGUMENT_BYTES:
        raise CapabilityValidationError("oversized_arguments")
    if any(key not in entry.argument_schema["properties"] for key in arguments):
        raise CapabilityValidationError("schema_invalid_arguments")
    if entry.capability_id == "runtime.world_state.read":
        return _normalize_world_state_arguments(arguments)
    if entry.capability_id == "draft.local_message":
        return _normalize_draft_arguments(arguments)
    raise CapabilityValidationError("unknown_capability_id")


def provider_text(completion: dict[str, Any]) -> str:
    message = _completion_message(completion)
    if message is None:
        return ""
    content = message.get("content")
    return content if isinstance(content, str) else ""


def _normalize_world_state_arguments(arguments: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    if "requested_domains" in arguments:
        domains = arguments["requested_domains"]
        if not isinstance(domains, list) or len(domains) > 4:
            raise CapabilityValidationError("schema_invalid_arguments")
        cleaned: list[str] = []
        for domain in domains:
            if not isinstance(domain, str) or domain not in _WORLD_STATE_DOMAINS:
                raise CapabilityValidationError("schema_invalid_arguments")
            if domain not in cleaned:
                cleaned.append(domain)
        normalized["requested_domains"] = sorted(cleaned)
    for key in ("entity_id", "attribute"):
        if key in arguments:
            value = arguments[key]
            if not isinstance(value, str) or not value or not _SAFE_LABEL.fullmatch(value):
                raise CapabilityValidationError("schema_invalid_arguments")
            normalized[key] = value
    if "output_mode" in arguments:
        value = arguments["output_mode"]
        if not isinstance(value, str) or value not in _WORLD_STATE_OUTPUT_MODES:
            raise CapabilityValidationError("schema_invalid_arguments")
        normalized["output_mode"] = value
    return {key: normalized[key] for key in sorted(normalized)}


def _normalize_draft_arguments(arguments: dict[str, Any]) -> dict[str, Any]:
    body = arguments.get("body")
    if not isinstance(body, str) or not body.strip() or len(body) > 2000:
        raise CapabilityValidationError("schema_invalid_arguments")
    normalized: dict[str, Any] = {"body": body.strip()}
    for key, max_length in (("recipient_label", 80), ("subject", 120)):
        if key in arguments:
            value = arguments[key]
            if not isinstance(value, str) or not value.strip() or len(value) > max_length:
                raise CapabilityValidationError("schema_invalid_arguments")
            normalized[key] = value.strip()
    for key, allowed in (("tone", _DRAFT_TONES), ("format", _DRAFT_FORMATS)):
        if key in arguments:
            value = arguments[key]
            if not isinstance(value, str) or value not in allowed:
                raise CapabilityValidationError("schema_invalid_arguments")
            normalized[key] = value
    return {key: normalized[key] for key in sorted(normalized)}


def _completion_message(completion: dict[str, Any]) -> dict[str, Any] | None:
    choices = completion.get("choices")
    if not isinstance(choices, list) or not choices:
        return None
    first = choices[0]
    if not isinstance(first, dict):
        return None
    message = first.get("message")
    return message if isinstance(message, dict) else None


def _request_from_parts(provider_tool_name: Any, arguments: Any) -> ParsedCapabilityRequest:
    if not isinstance(provider_tool_name, str) or not _SAFE_PROVIDER_TOOL_NAME.fullmatch(
        provider_tool_name
    ):
        raise CapabilityValidationError("unknown_capability_id")
    entry = capability_by_provider_tool_name(provider_tool_name)
    if entry is None:
        raise CapabilityValidationError("unknown_capability_id")
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments)
        except json.JSONDecodeError as exc:
            raise CapabilityValidationError("malformed_arguments") from exc
    if not isinstance(arguments, dict):
        raise CapabilityValidationError("malformed_arguments")
    return ParsedCapabilityRequest(
        capability_id=entry.capability_id,
        provider_tool_name=provider_tool_name,
        arguments=arguments,
    )


def _bounded_reason(result: dict[str, Any]) -> str:
    reason_codes = result.get("reason_codes")
    if isinstance(reason_codes, list):
        for reason in reason_codes:
            if isinstance(reason, str) and reason:
                return reason[:80]
    decision_code = result.get("decision_code")
    return (
        decision_code[:80]
        if isinstance(decision_code, str) and decision_code
        else "authorization_denied"
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
