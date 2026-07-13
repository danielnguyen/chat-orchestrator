from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Protocol, runtime_checkable

_CAPABILITY_ID = re.compile(r"^[a-z][a-z0-9]*(?:[._][a-z][a-z0-9_]*)+$")
_SAFE_CODE = re.compile(r"^[a-z][a-z0-9_]{0,79}$")
_SAFE_LABEL = re.compile(r"^[A-Za-z0-9_.:@/-]{1,120}$")
_EFFECT_MODES = {"simulated", "live"}
_MAX_PRESENTATION_LENGTH = 500


class ConnectorInputError(ValueError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class ExecutionStatus(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    UNKNOWN = "unknown"


class VerificationStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    UNKNOWN = "unknown"
    NOT_SUPPORTED = "not_supported"


class RevalidationStatus(str, Enum):
    SUCCESSFUL = "successful"
    FAILED = "failed"
    UNAVAILABLE = "unavailable"


class ConnectorOutcome(str, Enum):
    PENDING_CONFIRMATION = "pending_confirmation"
    CONFIRMATION_REJECTED = "confirmation_rejected"
    EXECUTION_FAILED = "execution_failed"
    EXECUTION_UNKNOWN = "execution_unknown"
    EXECUTED = "executed"
    EXECUTED_VERIFIED = "executed_verified"
    EXECUTED_UNVERIFIED = "executed_unverified"


def is_valid_capability_id(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) <= 120
        and _CAPABILITY_ID.fullmatch(value) is not None
    )


def _validate_code(value: Any, field_name: str) -> None:
    if not isinstance(value, str) or _SAFE_CODE.fullmatch(value) is None:
        raise ValueError(f"invalid_{field_name}")


def _validate_label(value: Any, field_name: str) -> None:
    if not isinstance(value, str) or _SAFE_LABEL.fullmatch(value) is None:
        raise ValueError(f"invalid_{field_name}")


def _validate_count(value: Any, field_name: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0 or value > 1:
        raise ValueError(f"invalid_{field_name}")


def _validate_presentation_text(value: Any, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not value
        or len(value) > _MAX_PRESENTATION_LENGTH
        or any(ord(character) < 32 and character not in "\n\t" for character in value)
    ):
        raise ValueError(f"invalid_{field_name}")


@dataclass(frozen=True)
class ConnectorArguments:
    values: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.values, Mapping):
            raise ValueError("invalid_arguments")
        cleaned: dict[str, Any] = {}
        for key, value in self.values.items():
            if not isinstance(key, str) or _SAFE_CODE.fullmatch(key) is None:
                raise ValueError("invalid_argument_key")
            if not isinstance(value, str | int | float | bool) or isinstance(value, complex):
                raise ValueError("invalid_argument_value")
            if isinstance(value, str) and len(value) > 500:
                raise ValueError("invalid_argument_value")
            cleaned[key] = value
        object.__setattr__(self, "values", MappingProxyType(dict(sorted(cleaned.items()))))

    def as_dict(self) -> dict[str, Any]:
        return dict(self.values)


@dataclass(frozen=True)
class ConnectorContinuationDescription:
    target: str
    confirmation_text: str

    def __post_init__(self) -> None:
        _validate_label(self.target, "continuation_target")
        _validate_presentation_text(self.confirmation_text, "confirmation_text")


@dataclass(frozen=True)
class ConnectorPresentation:
    pending_confirmation: str
    confirmation_rejected: str
    execution_failed: str
    execution_unknown: str
    executed: str
    executed_verified: str
    executed_unverified: str

    def __post_init__(self) -> None:
        for field_name in (
            "pending_confirmation",
            "confirmation_rejected",
            "execution_failed",
            "execution_unknown",
            "executed",
            "executed_verified",
            "executed_unverified",
        ):
            _validate_presentation_text(getattr(self, field_name), field_name)

    def text_for(self, outcome: ConnectorOutcome) -> str:
        if not isinstance(outcome, ConnectorOutcome):
            raise ValueError("invalid_connector_outcome")
        return getattr(self, outcome.value)


@dataclass(frozen=True)
class ConnectorClaimRef:
    claim_id: str
    value_digest: str

    def __post_init__(self) -> None:
        _validate_label(self.claim_id, "claim_id")
        _validate_label(self.value_digest, "value_digest")


@dataclass(frozen=True)
class ConnectorAvailabilityRequest:
    selected_claims: tuple[ConnectorClaimRef, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.selected_claims, tuple) or any(
            not isinstance(item, ConnectorClaimRef) for item in self.selected_claims
        ):
            raise ValueError("invalid_selected_claims")


@dataclass(frozen=True)
class ConnectorAvailabilityResult:
    available: bool
    reason_code: str

    def __post_init__(self) -> None:
        if not isinstance(self.available, bool):
            raise ValueError("invalid_availability")
        _validate_code(self.reason_code, "reason_code")


@dataclass(frozen=True)
class ConnectorRevalidationSpec:
    revalidator_id: str
    verifier_id: str
    source_type: str
    source_ref: str

    def __post_init__(self) -> None:
        for field_name in (
            "revalidator_id",
            "verifier_id",
            "source_type",
            "source_ref",
        ):
            _validate_label(getattr(self, field_name), field_name)


@dataclass(frozen=True)
class ConnectorRevalidationRequest:
    request_id: str
    arguments: ConnectorArguments
    selected_claims: tuple[ConnectorClaimRef, ...]
    requested_claim_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _validate_label(self.request_id, "request_id")
        if not isinstance(self.arguments, ConnectorArguments):
            raise ValueError("invalid_arguments")
        if not isinstance(self.selected_claims, tuple) or any(
            not isinstance(item, ConnectorClaimRef) for item in self.selected_claims
        ):
            raise ValueError("invalid_selected_claims")
        if not self.requested_claim_ids:
            raise ValueError("missing_claim_ids")
        if len(set(self.requested_claim_ids)) != len(self.requested_claim_ids):
            raise ValueError("duplicate_claim_ids")
        for claim_id in self.requested_claim_ids:
            _validate_label(claim_id, "claim_id")


@dataclass(frozen=True)
class ConnectorClaimObservation:
    claim_id: str
    expected_value_digest: str
    observed_at: str
    verified_at: str

    def __post_init__(self) -> None:
        _validate_label(self.claim_id, "claim_id")
        _validate_label(self.expected_value_digest, "expected_value_digest")
        if not isinstance(self.observed_at, str) or not 0 < len(self.observed_at) <= 64:
            raise ValueError("invalid_observed_at")
        if not isinstance(self.verified_at, str) or not 0 < len(self.verified_at) <= 64:
            raise ValueError("invalid_verified_at")


@dataclass(frozen=True)
class ConnectorRevalidationResult:
    status: RevalidationStatus
    reason_code: str
    observations: tuple[ConnectorClaimObservation, ...] = ()
    external_call_count: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.status, RevalidationStatus):
            raise ValueError("invalid_revalidation_status")
        _validate_code(self.reason_code, "reason_code")
        _validate_count(self.external_call_count, "external_call_count")
        if not isinstance(self.observations, tuple) or any(
            not isinstance(item, ConnectorClaimObservation) for item in self.observations
        ):
            raise ValueError("invalid_observations")
        if self.status is RevalidationStatus.SUCCESSFUL and not self.observations:
            raise ValueError("missing_observations")
        if self.status is not RevalidationStatus.SUCCESSFUL and self.observations:
            raise ValueError("unexpected_observations")


@dataclass(frozen=True)
class ConnectorExecutionRequest:
    request_id: str
    runtime_session_id: str
    runtime_turn_id: str
    arguments: ConnectorArguments

    def __post_init__(self) -> None:
        _validate_label(self.request_id, "request_id")
        _validate_label(self.runtime_session_id, "runtime_session_id")
        _validate_label(self.runtime_turn_id, "runtime_turn_id")
        if not isinstance(self.arguments, ConnectorArguments):
            raise ValueError("invalid_arguments")


@dataclass(frozen=True)
class ConnectorExecutionResult:
    status: ExecutionStatus
    reason_code: str
    external_reason_code: str
    external_call_count: int
    effect_mode: str | None = None
    target_label: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, ExecutionStatus):
            raise ValueError("invalid_execution_status")
        _validate_code(self.reason_code, "reason_code")
        _validate_label(self.external_reason_code, "external_reason_code")
        _validate_count(self.external_call_count, "external_call_count")
        if self.effect_mode is not None and self.effect_mode not in _EFFECT_MODES:
            raise ValueError("invalid_effect_mode")
        if self.target_label is not None:
            _validate_label(self.target_label, "target_label")


@dataclass(frozen=True)
class ConnectorVerificationRequest:
    request_id: str
    arguments: ConnectorArguments
    execution: ConnectorExecutionResult

    def __post_init__(self) -> None:
        _validate_label(self.request_id, "request_id")
        if not isinstance(self.arguments, ConnectorArguments):
            raise ValueError("invalid_arguments")
        if not isinstance(self.execution, ConnectorExecutionResult):
            raise ValueError("invalid_execution")


@dataclass(frozen=True)
class ConnectorVerificationResult:
    status: VerificationStatus
    reason_code: str
    external_reason_code: str
    external_call_count: int
    effect_mode: str | None = None
    target_label: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, VerificationStatus):
            raise ValueError("invalid_verification_status")
        _validate_code(self.reason_code, "reason_code")
        _validate_label(self.external_reason_code, "external_reason_code")
        _validate_count(self.external_call_count, "external_call_count")
        if self.effect_mode is not None and self.effect_mode not in _EFFECT_MODES:
            raise ValueError("invalid_effect_mode")
        if self.target_label is not None:
            _validate_label(self.target_label, "target_label")


@runtime_checkable
class ActionConnector(Protocol):
    @property
    def capability_id(self) -> str: ...

    @property
    def effect_mode(self) -> str | None: ...

    @property
    def revalidation_spec(self) -> ConnectorRevalidationSpec | None: ...

    @property
    def presentation(self) -> ConnectorPresentation: ...

    def normalize_arguments(self, arguments: Mapping[str, Any]) -> ConnectorArguments: ...

    def describe_continuation(
        self,
        arguments: ConnectorArguments,
    ) -> ConnectorContinuationDescription: ...

    def restore_continuation(
        self,
        description: ConnectorContinuationDescription,
    ) -> ConnectorArguments: ...

    def check_availability(
        self,
        request: ConnectorAvailabilityRequest,
    ) -> ConnectorAvailabilityResult: ...

    async def revalidate(
        self,
        request: ConnectorRevalidationRequest,
    ) -> ConnectorRevalidationResult: ...

    async def execute(
        self,
        request: ConnectorExecutionRequest,
    ) -> ConnectorExecutionResult: ...

    async def verify(
        self,
        request: ConnectorVerificationRequest,
    ) -> ConnectorVerificationResult: ...


class ActionConnectorRegistry:
    def __init__(self, connectors: list[ActionConnector] | tuple[ActionConnector, ...]) -> None:
        indexed: dict[str, ActionConnector] = {}
        for connector in connectors:
            capability_id = getattr(connector, "capability_id", None)
            if not is_valid_capability_id(capability_id):
                raise ValueError("invalid_connector_capability_id")
            if not isinstance(connector, ActionConnector):
                raise TypeError("invalid_action_connector")
            if capability_id in indexed:
                raise ValueError("duplicate_connector_capability_id")
            indexed[capability_id] = connector
        self._connectors = MappingProxyType(dict(sorted(indexed.items())))

    def get(self, capability_id: str) -> ActionConnector | None:
        if not is_valid_capability_id(capability_id):
            return None
        return self._connectors.get(capability_id)

    def capability_ids(self) -> tuple[str, ...]:
        return tuple(self._connectors)
