import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "wave3b_composed_smoke.sh"
OVERLAY = ROOT / "docker-compose.wave3b-smoke.yml"


def _script() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def test_wave3b_smoke_harness_has_no_generic_probe_or_prepassed_rows():
    source = _script()
    assert "run_boundary_probe" not in source
    assert "acceptance_json='{}'" in source
    assert 'A1: "passed"' not in source
    assert "mark_acceptance" in source
    assert source.index("acceptance_json='{}'") < source.index("scenario_shared_memory")


def test_wave3b_smoke_harness_assertions_fail_nonzero():
    source = _script()
    assert "assert_jq()" in source
    assert "assert_provider_sentinel()" in source
    assert "assert_provider_chat_calls" in source
    assert "length) > 0" in source[source.index("assert_provider_chat_calls") :]
    assert "exit 1" in source[source.index("assert_jq()") : source.index("acceptance_json=")]
    assert "not all A1-A9 rows were proven" in source
    assert source.index("not all A1-A9 rows were proven") < source.rindex("ok: true")


def test_wave3b_smoke_harness_final_ok_true_is_co3f_gated():
    source = _script()
    scenario_order = [
        "scenario_shared_memory",
        "scenario_relationship_narrowing",
        "scenario_restraint_zero_call",
        "scenario_artifact_policy",
        "scenario_fallback_identity",
        "scenario_privacy_safe_diagnostics",
    ]
    positions = [source.index(f"{name}\n") for name in scenario_order]
    assert positions == sorted(positions)
    final_gate = source.rindex("acceptance_json=\"$(jq -c")
    full_suite_gate = source.index('if [ "$full_suite" = true ]; then', final_gate)
    final_acceptance_gate = source.index("WAVE3B_FINAL_ACCEPTANCE", full_suite_gate)
    assert max(positions) < final_gate
    assert final_gate < full_suite_gate < final_acceptance_gate < source.rindex("ok: true")
    assert "packet_ok: true" in source
    assert "final_acceptance: false" in source


def test_wave3b_smoke_harness_scenario_selection_before_topology():
    source = _script()
    assert "REQUESTED_SCENARIOS=" in source
    assert "unknown scenario" in source
    assert source.index("unknown scenario") < source.index("compose up")
    assert source.index("harness_only") < source.index("compose up")
    assert "focused: $focused" in source


def test_wave3b_smoke_harness_required_fixture_setup_is_not_swallowed():
    source = _script()
    for line in source.splitlines():
        if "|| true" not in line:
            continue
        assert "compose down" in line
    assert "assert_required_fixture_setup_is_strict" in source
    assert "fixture failed: artifact-sql-" in source


def test_wave3b_smoke_harness_public_source_positive_requires_expected_source():
    source = _script()
    helper = source[source.index("assert_public_source_allowlist()") :]
    helper = helper[: helper.index("acceptance_json=")]
    assert '(.sources | type == "array")' in helper
    assert "(.sources | length > 0)" in helper
    assert 'select(.artifact_id == $artifact)' in helper
    assert "owner_id" not in helper
    assert "assert_public_source_allowlist" in source[source.index("scenario_artifact_policy") :]


def test_wave3b_smoke_harness_provider_selftests_cover_empty_missing_and_forbidden():
    source = _script()
    selftest = source[source.index("run_harness_selftest()") :]
    assert "zero-provider-calls" in selftest
    assert "matching-provider-sentinel" in selftest
    assert "missing-required-provider-sentinel" in selftest
    assert "forbidden-provider-sentinel" in selftest
    assert "empty-public-source" in selftest
    assert "internal-public-source-field" in selftest


def test_wave3b_harness_scenario_runs_without_final_acceptance():
    result = subprocess.run(
        [str(SCRIPT), "--scenario", "harness"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["packet_ok"] is True
    assert payload["packet"] == "CO-3A"
    assert payload["focused"] is True
    assert payload["final_acceptance"] is False
    assert "ok" not in payload
    assert payload["acceptance"] == {}


def test_wave3b_unknown_scenario_fails_before_topology_execution():
    result = subprocess.run(
        [str(SCRIPT), "--scenario", "unknown-co3a"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 2
    assert "unknown scenario unknown-co3a" in result.stderr
    assert "compose up" not in result.stderr


def _assert_invalid_selection_fails_before_topology(
    *,
    args: list[str],
    env_value: str | None = None,
    expected_token: str = "harness",
):
    env = os.environ.copy()
    if env_value is not None:
        env["WAVE3B_SCENARIOS"] = env_value
    result = subprocess.run(
        [str(SCRIPT), *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )
    assert result.returncode == 2
    assert "wave3b-composed-smoke usage error:" in result.stderr
    assert expected_token in result.stderr
    assert "compose up" not in result.stderr
    assert "prerequisite" not in result.stderr
    assert '"packet_ok"' not in result.stdout
    assert '"ok"' not in result.stdout


def test_wave3b_harness_artifact_selection_fails_before_topology():
    _assert_invalid_selection_fails_before_topology(args=["--scenario", "harness,artifact"])


def test_wave3b_artifact_harness_selection_fails_before_topology():
    _assert_invalid_selection_fails_before_topology(args=["--scenario", "artifact,harness"])


def test_wave3b_env_artifact_harness_selection_fails_before_topology():
    _assert_invalid_selection_fails_before_topology(args=[], env_value="artifact,harness")


def test_wave3b_duplicate_harness_selection_fails_before_topology():
    _assert_invalid_selection_fails_before_topology(args=["--scenario", "harness,harness"])


def test_wave3b_all_combination_selection_fails_order_independent_before_topology():
    _assert_invalid_selection_fails_before_topology(
        args=["--scenario", "all,artifact"], expected_token="all"
    )
    _assert_invalid_selection_fails_before_topology(
        args=["--scenario", "artifact,all"], expected_token="all"
    )


def test_wave3b_topology_overlay_enables_required_flags_and_indexing():
    overlay = OVERLAY.read_text(encoding="utf-8")
    for flag in (
        'COGNITIVE_RUNTIME_INTERACTION_GOVERNANCE_ENABLED: "true"',
        'COGNITIVE_RUNTIME_PERSONA_CONTAINMENT_ENABLED: "true"',
        'COGNITIVE_RUNTIME_RESTRAINT_ENABLED: "true"',
        'COGNITIVE_RUNTIME_PRIVACY_CONTEXT_ENABLED: "true"',
        'ENABLE_RUNTIME_OVERLAYS: "true"',
        'COGNITIVE_RUNTIME_MEMORY_HYGIENE_ENABLED: "true"',
        'INDEX_USER_QUESTIONS: "true"',
        'INDEX_ASSISTANT_MESSAGES: "true"',
    ):
        assert flag in overlay


def test_wave3b_prerequisites_are_checked_before_docker_startup():
    source = _script()
    assert source.index("status --porcelain") < source.index("compose up")
    assert source.index("branch --show-current") < source.index("compose up")
    assert source.index("merge-base --is-ancestor \"$BMS_REQUIRED_COMMIT\" HEAD") < source.index(
        "compose up"
    )
    assert source.index("merge-base --is-ancestor \"$CR_REQUIRED_COMMIT\" HEAD") < source.index(
        "compose up"
    )
