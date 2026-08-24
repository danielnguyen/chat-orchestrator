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


def _bound_value(value: str, observation_index: int) -> dict:
    return {
        "value": value,
        "source_observation": {
            "evidence_ref_id": "fact-1",
            "observation_index": observation_index,
        },
    }


def test_structured_terminal_mean_rejects_synthetic_compound_literals():
    request = {
        "derivation_id": "mean-1",
        "operation": "mean",
        "operands": [
            _bound_value(value, index)
            for index, value in enumerate(["58", "916", "38", "14"])
        ],
        "supporting_evidence_ref_ids": ["fact-1"],
    }

    with pytest.raises(ValueError, match="derivation_observation_literal_invalid"):
        execute_derivations(
            [request],
            authorized_evidence_ref_ids={"fact-1"},
            structured_observations_by_evidence_ref={
                "fact-1": ("5/8", "9/16", "3/8", "1/4")
            },
        )

    unbound = deepcopy(request)
    for operand in unbound["operands"]:
        operand.pop("source_observation")
    with pytest.raises(ValueError, match="derivation_observation_binding_required"):
        execute_derivations(
            [unbound],
            authorized_evidence_ref_ids={"fact-1"},
            structured_observations_by_evidence_ref={
                "fact-1": ("5/8", "9/16", "3/8", "1/4")
            },
        )


def test_structured_compound_observations_use_grounded_intermediate_derivations():
    requests = [
        {
            "derivation_id": f"ratio-{index}",
            "operation": "divide",
            "operands": [
                _bound_value(numerator, index - 1),
                _bound_value(denominator, index - 1),
            ],
            "supporting_evidence_ref_ids": ["fact-1"],
        }
        for index, (numerator, denominator) in enumerate(
            [("5", "8"), ("9", "16"), ("3", "8"), ("1", "4")],
            start=1,
        )
    ]
    requests.append(
        {
            "derivation_id": "mean-1",
            "operation": "mean",
            "operands": [
                {"derivation_ref": f"ratio-{index}"}
                for index in range(1, 5)
            ],
            "supporting_evidence_ref_ids": ["fact-1"],
        }
    )

    records = execute_derivations(
        requests,
        authorized_evidence_ref_ids={"fact-1"},
        structured_observations_by_evidence_ref={
            "fact-1": ("5/8", "9/16", "3/8", "1/4")
        },
    )

    assert records[-1]["canonical_inputs"] == ["0.25", "0.375", "0.5625", "0.625"]
    assert records[-1]["canonical_result"] == "0.453125"
    assert {record["input_basis"] for record in records} == {"model_interpreted"}


def test_compound_observation_rule_is_separator_neutral():
    with pytest.raises(
        ValueError,
        match="derivation_observation_transformation_required",
    ):
        execute_derivations(
            [
                {
                    "derivation_id": "mean-1",
                    "operation": "mean",
                    "operands": [_bound_value("10", 0)],
                    "supporting_evidence_ref_ids": ["fact-1"],
                }
            ],
            authorized_evidence_ref_ids={"fact-1"},
            structured_observations_by_evidence_ref={"fact-1": ("10-20",)},
        )


def test_direct_numeric_and_zero_atom_semantic_observations_remain_compatible():
    records = execute_derivations(
        [
            {
                "derivation_id": "mean-1",
                "operation": "mean",
                "operands": [
                    _bound_value("0.5", 0),
                    _bound_value("0.75", 1),
                    _bound_value("0.5", 2),
                ],
                "supporting_evidence_ref_ids": ["fact-1"],
            }
        ],
        authorized_evidence_ref_ids={"fact-1"},
        structured_observations_by_evidence_ref={
            "fact-1": ("0.5", "about 0.75 units", "half")
        },
    )

    assert records[0]["canonical_inputs"] == ["0.5", "0.5", "0.75"]
    assert records[0]["canonical_result"] == "0.5833333333333333333333333333"


def test_structured_transformation_chain_rejects_ungrounded_synthetic_literals():
    synthetic = _divide("synthetic", "58", "1")
    synthetic["supporting_evidence_ref_ids"] = ["fact-2"]
    with pytest.raises(ValueError, match="derivation_observation_binding_required"):
        execute_derivations(
            [
                synthetic,
                {
                    "derivation_id": "mean-1",
                    "operation": "mean",
                    "operands": [{"derivation_ref": "synthetic"}],
                    "supporting_evidence_ref_ids": ["fact-1"],
                },
            ],
            authorized_evidence_ref_ids={"fact-1", "fact-2"},
            structured_observations_by_evidence_ref={"fact-1": ("5/8",)},
        )
