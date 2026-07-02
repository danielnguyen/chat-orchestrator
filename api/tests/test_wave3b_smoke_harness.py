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
    assert "exit 1" in source[source.index("assert_jq()") : source.index("acceptance_json=")]
    assert "not all A1-A9 rows were proven" in source
    assert source.index("not all A1-A9 rows were proven") < source.index("ok: true")


def test_wave3b_smoke_harness_ok_true_after_all_scenarios_complete():
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
    assert max(positions) < final_gate
    assert final_gate < source.index("ok: true")


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
