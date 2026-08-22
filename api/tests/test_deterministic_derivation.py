from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError
from services.deterministic_derivation import (
    DerivationRequest,
    execute_derivations,
)


def _divide(derivation_id: str, numerator: str, denominator: str) -> dict:
    return {
        "derivation_id": derivation_id,
        "operation": "divide",
        "operands": [{"value": numerator}, {"value": denominator}],
        "supporting_evidence_ref_ids": ["fact-1"],
    }


def test_divide_and_transitive_mean_are_exact_and_interpretation_dependent():
    records = execute_derivations(
        [
            _divide("ratio-1", "1", "2"),
            _divide("ratio-2", "3", "4"),
            {
                "derivation_id": "mean-1",
                "operation": "mean",
                "operands": [
                    {"derivation_ref": "ratio-1"},
                    {"derivation_ref": "ratio-2"},
                ],
                "supporting_evidence_ref_ids": ["fact-1"],
            },
        ],
        authorized_evidence_ref_ids={"fact-1"},
    )

    assert [record["canonical_result"] for record in records] == [
        "0.5",
        "0.75",
        "0.625",
    ]
    assert {record["input_basis"] for record in records} == {"model_interpreted"}
    assert all(record["executor_version"] == "bounded-decimal-v1" for record in records)
    assert all(record["execution_digest"].startswith("sha256:") for record in records)


def test_mean_canonicalization_and_digest_are_reordering_stable():
    first = {
        "derivation_id": "mean-a",
        "operation": "mean",
        "operands": [{"value": "2.00"}, {"value": "1"}],
        "supporting_evidence_ref_ids": ["fact-2", "fact-1"],
    }
    second = deepcopy(first)
    second["derivation_id"] = "mean-b"
    second["operands"].reverse()
    second["supporting_evidence_ref_ids"].reverse()

    left = execute_derivations(
        [first],
        authorized_evidence_ref_ids={"fact-1", "fact-2"},
    )[0]
    right = execute_derivations(
        [second],
        authorized_evidence_ref_ids={"fact-1", "fact-2"},
    )[0]

    assert left["canonical_inputs"] == right["canonical_inputs"] == ["1", "2"]
    assert left["canonical_result"] == right["canonical_result"] == "1.5"
    assert left["execution_digest"] == right["execution_digest"]


@pytest.mark.parametrize(
    ("requests", "reason"),
    [
        ([_divide("zero", "1", "0")], "divide_by_zero"),
        ([_divide("bad", "NaN", "2")], "derivation_operand_invalid"),
        ([_divide("bad", "Infinity", "2")], "derivation_operand_invalid"),
        ([_divide("bad", "01", "2")], "derivation_operand_invalid"),
        (
            [
                {
                    "derivation_id": "other",
                    "operation": "mean",
                    "operands": [{"derivation_ref": "missing"}],
                    "supporting_evidence_ref_ids": ["fact-1"],
                }
            ],
            "derivation_reference_unknown",
        ),
        (
            [
                {
                    "derivation_id": "cycle-a",
                    "operation": "mean",
                    "operands": [{"derivation_ref": "cycle-b"}],
                    "supporting_evidence_ref_ids": ["fact-1"],
                },
                {
                    "derivation_id": "cycle-b",
                    "operation": "mean",
                    "operands": [{"derivation_ref": "cycle-a"}],
                    "supporting_evidence_ref_ids": ["fact-1"],
                },
            ],
            "derivation_cycle",
        ),
    ],
)
def test_executor_fails_closed_for_invalid_mechanical_requests(requests, reason):
    with pytest.raises(ValueError, match=reason):
        execute_derivations(requests, authorized_evidence_ref_ids={"fact-1"})


def test_duplicate_ids_unknown_evidence_and_request_bound_are_rejected():
    duplicate = [_divide("same", "1", "2"), _divide("same", "3", "4")]
    with pytest.raises(ValueError, match="duplicate_derivation_id"):
        execute_derivations(duplicate, authorized_evidence_ref_ids={"fact-1"})
    with pytest.raises(ValueError, match="derivation_evidence_reference_unknown"):
        execute_derivations(
            [_divide("one", "1", "2")],
            authorized_evidence_ref_ids=set(),
        )
    with pytest.raises(ValueError, match="derivation_request_count_invalid"):
        execute_derivations(
            [_divide(f"item-{index}", "1", "2") for index in range(17)],
            authorized_evidence_ref_ids={"fact-1"},
        )


def test_schema_rejects_unknown_operation_extra_fields_and_coercive_operands():
    with pytest.raises(ValidationError):
        DerivationRequest.model_validate(
            {
                **_divide("invalid", "1", "2"),
                "operation": "normalize_fraction",
            }
        )
    with pytest.raises(ValidationError):
        DerivationRequest.model_validate(
            {**_divide("invalid", "1", "2"), "execution_status": "executed"}
        )
    with pytest.raises(ValidationError):
        DerivationRequest.model_validate(
            {
                "derivation_id": "invalid",
                "operation": "divide",
                "operands": [{"value": 1}, {"value": "2"}],
                "supporting_evidence_ref_ids": ["fact-1"],
            }
        )
