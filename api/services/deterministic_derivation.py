from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation, localcontext
from hashlib import sha256
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

_IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]*$"
_DECIMAL_PATTERN = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
EXECUTOR_VERSION = "bounded-decimal-v1"


class DerivationOperand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: str | None = Field(default=None, min_length=1, max_length=64)
    derivation_ref: str | None = Field(
        default=None,
        min_length=1,
        max_length=120,
        pattern=_IDENTIFIER_PATTERN,
    )

    @model_validator(mode="after")
    def validate_exactly_one_source(self):
        if (self.value is None) == (self.derivation_ref is None):
            raise ValueError("derivation_operand_source_invalid")
        return self


class DerivationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    derivation_id: str = Field(
        min_length=1,
        max_length=120,
        pattern=_IDENTIFIER_PATTERN,
    )
    operation: Literal["divide", "mean"]
    operands: list[DerivationOperand] = Field(min_length=1, max_length=16)
    supporting_evidence_ref_ids: list[str] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def validate_shape(self):
        if self.operation == "divide" and len(self.operands) != 2:
            raise ValueError("divide_operand_count_invalid")
        if self.operation == "mean" and len(self.operands) < 1:
            raise ValueError("mean_operand_count_invalid")
        if len(self.supporting_evidence_ref_ids) != len(
            set(self.supporting_evidence_ref_ids)
        ):
            raise ValueError("duplicate_derivation_evidence_reference")
        return self


class DerivationExecutionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    derivation_id: str
    operation: Literal["divide", "mean"]
    canonical_inputs: list[str]
    canonical_result: str
    execution_digest: str
    executor_version: Literal["bounded-decimal-v1"]
    supporting_evidence_ref_ids: list[str]
    input_basis: Literal["model_interpreted"]


def _decimal(value: str) -> Decimal:
    if not isinstance(value, str) or not _DECIMAL_PATTERN.fullmatch(value):
        raise ValueError("derivation_operand_invalid")
    digits = sum(character.isdigit() for character in value)
    if digits > 50:
        raise ValueError("derivation_operand_invalid")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("derivation_operand_invalid") from exc
    if not parsed.is_finite():
        raise ValueError("derivation_operand_invalid")
    return parsed


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("derivation_result_invalid")
    if value == 0:
        return "0"
    rendered = format(value.normalize(), "f")
    if len(rendered) > 160:
        raise ValueError("derivation_result_invalid")
    return rendered


def _execution_digest(
    *,
    operation: str,
    canonical_inputs: list[str],
    canonical_result: str,
    evidence_ref_ids: list[str],
) -> str:
    material = json.dumps(
        {
            "operation": operation,
            "canonical_inputs": canonical_inputs,
            "canonical_result": canonical_result,
            "executor_version": EXECUTOR_VERSION,
            "supporting_evidence_ref_ids": sorted(evidence_ref_ids),
            "input_basis": "model_interpreted",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return "sha256:" + sha256(material.encode()).hexdigest()


def execute_derivations(
    requests: list[DerivationRequest | dict[str, Any]],
    *,
    authorized_evidence_ref_ids: set[str],
) -> list[dict[str, Any]]:
    if not isinstance(requests, list) or len(requests) > 16:
        raise ValueError("derivation_request_count_invalid")
    parsed = [
        item if isinstance(item, DerivationRequest) else DerivationRequest.model_validate(item)
        for item in requests
    ]
    by_id = {item.derivation_id: item for item in parsed}
    if len(by_id) != len(parsed):
        raise ValueError("duplicate_derivation_id")
    for request in parsed:
        if not set(request.supporting_evidence_ref_ids) <= authorized_evidence_ref_ids:
            raise ValueError("derivation_evidence_reference_unknown")
        for operand in request.operands:
            if (
                operand.derivation_ref is not None
                and operand.derivation_ref not in by_id
            ):
                raise ValueError("derivation_reference_unknown")

    records: dict[str, DerivationExecutionRecord] = {}
    values: dict[str, Decimal] = {}
    visiting: set[str] = set()

    def execute(derivation_id: str) -> Decimal:
        if derivation_id in values:
            return values[derivation_id]
        if derivation_id in visiting:
            raise ValueError("derivation_cycle")
        visiting.add(derivation_id)
        request = by_id[derivation_id]
        resolved: list[Decimal] = []
        for operand in request.operands:
            resolved.append(
                _decimal(operand.value)
                if operand.value is not None
                else execute(str(operand.derivation_ref))
            )
        with localcontext() as context:
            context.prec = 64
            if request.operation == "divide":
                if resolved[1] == 0:
                    raise ValueError("divide_by_zero")
                result = resolved[0] / resolved[1]
            else:
                result = sum(resolved, Decimal(0)) / Decimal(len(resolved))
        canonical_inputs = [_canonical_decimal(value) for value in resolved]
        if request.operation == "mean":
            canonical_inputs.sort(key=Decimal)
        canonical_result = _canonical_decimal(result)
        evidence_ref_ids = sorted(request.supporting_evidence_ref_ids)
        record = DerivationExecutionRecord(
            derivation_id=request.derivation_id,
            operation=request.operation,
            canonical_inputs=canonical_inputs,
            canonical_result=canonical_result,
            execution_digest=_execution_digest(
                operation=request.operation,
                canonical_inputs=canonical_inputs,
                canonical_result=canonical_result,
                evidence_ref_ids=evidence_ref_ids,
            ),
            executor_version=EXECUTOR_VERSION,
            supporting_evidence_ref_ids=evidence_ref_ids,
            input_basis="model_interpreted",
        )
        records[derivation_id] = record
        values[derivation_id] = result
        visiting.remove(derivation_id)
        return result

    for item in parsed:
        execute(item.derivation_id)
    return [records[item.derivation_id].model_dump(mode="json") for item in parsed]
