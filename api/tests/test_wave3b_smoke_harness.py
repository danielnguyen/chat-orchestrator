import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "wave3b_composed_smoke.sh"
OVERLAY = ROOT / "docker-compose.wave3b-smoke.yml"
OBSERVER = ROOT / "api" / "tools" / "wave3b_bms_observer.py"


def _script() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def _shared_memory_scenario(source: str) -> str:
    return source[
        source.index("scenario_shared_memory()") : source.index("relationship_entity_json()")
    ]


def _focused_packet_labeler(source: str) -> str:
    return source[
        source.index("focused_packet_label()") : source.index(
            "assert_packet_label_for_selection()"
        )
    ]


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


def test_wave3b_smoke_overlay_routes_co_through_bms_observer_only():
    overlay = OVERLAY.read_text(encoding="utf-8")
    observer = OBSERVER.read_text(encoding="utf-8")
    assert "bms-observer:" in overlay
    assert "tools.wave3b_bms_observer:app" in overlay
    assert "MEMORY_STORE_BASE_URL: http://bms-observer:8000" in overlay
    assert "BMS_UPSTREAM_URL: http://bms:8000" in overlay
    assert '"14331:8000"' in overlay
    assert "x-api-key" not in observer[observer.index('_records.append(') :]
    assert "/v2/conversations/([^/]+)/retrieve" in observer
    assert 'content=raw_body' in observer


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


def test_wave3b_harness_selftest_exercises_packet_labels():
    source = _script()
    assert "focused_packet_label()" in source
    assert 'assert_packet_label_for_selection "CO-3A" "harness"' in source
    assert 'assert_packet_label_for_selection "CO-3B" "shared-memory"' in source
    assert 'assert_packet_label_for_selection "CO-3D" "artifact"' in source
    assert 'assert_packet_label_for_selection "CO-3E" "fallback-privacy"' in source
    assert 'assert_packet_label_for_selection "CO-3E" "privacy-fallback"' in source
    assert 'packet: $packet' in source


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


def test_wave3b_shared_memory_uses_three_distinct_personas_and_storage_checks():
    source = _script()
    scenario = _shared_memory_scenario(source)
    assert 'persona_a="general_assistant"' in scenario
    assert 'persona_b="technical_architect"' in scenario
    assert 'persona_c="personal_companion"' in scenario
    assert "assert_distinct_personas" in scenario
    assert "ensure_cr_surface_binding" in scenario
    assert "cr_storage_match_count" in scenario
    assert "bms_persona_overlay_match_count" in scenario
    assert "bms_canonical_message_count" in scenario
    assert "canonical fact duplicated in BMS messages" in scenario


def test_wave3b_shared_memory_message_crowding_has_score_and_boundary_evidence():
    source = _script()
    scenario = _shared_memory_scenario(source)
    assert 'effective_limit=3' in scenario
    assert 'crowd_size=$((crowd_size + 4))' in scenario
    assert 'json_vector_for_score "$query_vector" "0.62"' in scenario
    assert 'json_vector_for_score "$query_vector" "0.98"' in scenario
    assert "assert_score_ordering" in scenario
    assert "canonical_score" in scenario
    assert "min_decoy_score" in scenario
    assert "pre_limit_policy_filter_applied == true" in scenario
    assert "mandatory_policy_filter_applied == true" in scenario
    assert "semantic_candidates" in scenario
    assert 'index("mandatory_containment_applied")' in scenario
    assert 'select(.message_id == $id)' in scenario
    assert 'select(.message_id as $message_id | $decoys | index($message_id))' in scenario
    assert "exact canonical retained from BMS response" in scenario
    assert "no decoy retained from BMS response" in scenario


def test_wave3b_shared_memory_crowd_distinguishes_all_ineligible_groups():
    source = _script()
    scenario = _shared_memory_scenario(source)
    assert "blocked_ids_json" in scenario
    assert "spoof_ids_json" in scenario
    assert "outside_ids_json" in scenario
    assert "untagged_ids_json" in scenario
    assert "seed_untagged_message" in scenario
    assert "qdrant_upsert_message_untagged" in source
    untagged_helper = source[
        source.index("qdrant_upsert_message_untagged()") : source.index("seed_untrusted_message()")
    ]
    assert "retrieval_policy_valid" not in untagged_helper
    assert "memory_domains" not in untagged_helper


def test_wave3b_shared_memory_checks_normal_co_boundary_before_direct_bms_probe():
    source = _script()
    scenario = _shared_memory_scenario(source)
    normal_before = scenario.index('normal_retrieval_before="$(retrieval_log_count)"')
    normal_call = scenario.index('read_response="$(co_chat')
    normal_after = scenario.index('normal_retrieval_after="$(wait_retrieval_log_delta')
    demote_query = scenario.index('demote_current_turn_query_messages "$owner" "$read_query"')
    direct_call = scenario.index('direct_bms_response="$(bms_retrieve_bundle')
    assert normal_before < normal_call < normal_after < demote_query < direct_call
    assert "normal CO request BMS retrieval boundary" in scenario
    assert (
        'read_query="Bring in project context from memory. What is the saved durable fact?"'
        in scenario
    )
    assert "demoted_query_vector" in scenario
    assert "demote_current_turn_query_messages()" in source


def test_wave3b_shared_memory_focused_acceptance_is_partial_not_final():
    source = _script()
    scenario = _shared_memory_scenario(source)
    assert 'mark_acceptance "A1"' in scenario
    assert 'mark_acceptance "A2"' in scenario
    assert 'mark_acceptance "A3"' in scenario
    assert 'mark_acceptance "A7_message"' in scenario
    for row in ("A4", "A5", "A6", "A7_artifact", "A8", "A9"):
        assert f'mark_acceptance "{row}"' not in scenario


def test_wave3b_shared_memory_denial_does_not_reuse_authorized_personas():
    source = _script()
    scenario = _shared_memory_scenario(source)
    assert '"$unauthorized_surface" "$unauthorized_surface" "$conv_unauthorized"' in scenario
    assert "third persona surface" in scenario
    assert "unauthorized persona cannot retain canonical" in scenario
    assert "same saved project fact" in scenario
    assert "persona C containment blocks project memory" in scenario
    assert "allowed_memory_domains" in scenario
    assert "blocked_memory_domains" in scenario
    assert "capability_domain == \"personal\"" in scenario
    assert "persona-c-canonical-sentinel" in scenario
    assert "persona-c-trace-canonical-id" in scenario
    assert '"web" "$conv_unauthorized"' not in scenario


def test_wave3b_artifact_scenario_creates_all_required_fixture_groups():
    source = _script()
    scenario = source[
        source.index("scenario_artifact_policy()") : source.index("scenario_fallback_identity()")
    ]
    for token in (
        "eligible-code",
        "eligible-doc",
        "blocked-domain",
        "outside-domain",
        "too-sensitive",
        "unsupported-class",
        "malformed-policy",
        "incomplete-lifecycle",
        "unavailable-source",
        "irrelevant",
    ):
        assert token in scenario
    assert "(.positive | to_entries | length) == 2" in scenario
    assert "(.negative | to_entries | length) == 8" in scenario
    assert "fixture failed: artifact-sql-" in source
    assert "|| true" not in scenario


def test_wave3b_artifact_scenario_proves_score_crowding_and_direct_bms_filtering():
    source = _script()
    scenario = source[
        source.index("scenario_artifact_policy()") : source.index("scenario_fallback_identity()")
    ]
    assert "artifact_limit=3" in scenario
    assert 'candidate_limit="$(artifact_qdrant_candidate_limit "$artifact_limit")"' in scenario
    assert "artifact_limit * 20" in source
    assert "expanded=100" in source
    assert 'json_vector_for_score "$query_vector" "0.62"' in scenario
    assert 'json_vector_for_score "$query_vector" "0.61"' in scenario
    assert 'json_vector_for_score "$query_vector" "0.98"' in scenario
    assert "high_crowd_count" in scenario
    assert '"$high_crowd_count" -gt "$candidate_limit"' in scenario
    assert "blocked-domain-extra-$index" in scenario
    assert "outside-domain-extra-$index" in scenario
    assert "too-sensitive-extra-$index" in scenario
    assert "unsupported-class-extra-$index" in scenario
    assert "malformed-policy-extra-$index" in scenario
    assert "incomplete-lifecycle-extra-$index" in scenario
    assert "qdrant_artifact_scores" in scenario
    assert "assert_artifact_score_ordering" in scenario
    assert "$expected_crowd > $candidate_limit" in source
    assert "$code_rank > $candidate_limit" in source
    assert "$doc_rank > $candidate_limit" in source
    assert "eligible_code_score" in scenario
    assert "eligible_doc_score" in scenario
    assert "eligible_code_rank" in scenario
    assert "eligible_doc_rank" in scenario
    assert "min_ineligible_score" in scenario
    assert 'direct_bms_response="$(bms_retrieve_bundle' in scenario
    assert "direct_policy=" not in scenario
    assert "bms_observer_reset" in scenario
    assert "observer_capture" in scenario
    assert "captured_policy" in scenario
    assert "direct_bms_payload" in scenario
    assert ".containment_policy == $captured.containment_policy" in scenario
    assert "direct BMS request uses exact captured normal CO policy" in scenario
    assert "observer captured and forwarded exactly one normal CO retrieval" in scenario
    assert "observer does not expose BMS credentials" in scenario
    assert "pre_limit_policy_filter_applied == true" in scenario
    assert "missing_derivative_source_record" in scenario
    assert "source_missing_or_unavailable" in scenario
    assert 'select(.artifact_id == $code)' in scenario
    assert 'select(.artifact_id == $doc)' in scenario
    assert 'select(.artifact_id as $id | $negatives | index($id))' in scenario
    assert "qdrant_candidate_limit=$candidate_limit" in scenario
    assert "high_scoring_crowd_count=$high_crowd_count" in scenario
    assert "eligible_code_raw_rank=$eligible_code_rank" in scenario
    assert "eligible_doc_raw_rank=$eligible_doc_rank" in scenario


def test_wave3b_artifact_scenario_ties_normal_co_provider_and_public_sources():
    source = _script()
    scenario = source[
        source.index("scenario_artifact_policy()") : source.index("scenario_fallback_identity()")
    ]
    normal_before = scenario.index('before="$(retrieval_log_count)"')
    normal_call = scenario.index('response="$(co_chat')
    normal_after = scenario.index('after="$(wait_retrieval_log_delta')
    assert normal_before < normal_call < normal_after
    assert '"true" "0.5"' in scenario
    assert "normal CO artifact request BMS retrieval boundary" in scenario
    assert "artifact_request_status == \"mandatory_policy_forwarded\"" in scenario
    assert "artifact_result_status == \"validated\"" in scenario
    assert ".artifact_access_policy.allowed_content_classes" in scenario
    assert ".artifact_access_policy.allowed_domains" in scenario
    assert ".artifact_access_policy.maximum_sensitivity" in scenario
    assert ".artifact_access_policy.surface_content_capabilities" in scenario
    assert ".artifact_access_policy.reason_codes" in scenario
    assert "eligible code artifact retained by CO" in scenario
    assert "eligible document artifact retained by CO" in scenario
    assert (
        'assert_provider_sentinel "$calls" "$request_id" "eligible_artifact_code" true "1"'
        in scenario
    )
    assert (
        'assert_provider_sentinel "$calls" "$request_id" "eligible_artifact_doc" true "1"'
        in scenario
    )
    assert "artifact-provider-negative-artifact-ids" in scenario
    assert "assert_public_source_allowlist" in scenario
    assert "artifact-public-source-forbidden" in scenario
    assert 'mark_acceptance "A6"' in scenario
    assert 'mark_acceptance "A7_artifact"' in scenario
    for row in ("A1", "A2", "A3", "A4", "A5", "A8", "A9"):
        assert f'mark_acceptance "{row}"' not in scenario


def test_wave3b_artifact_focused_label_is_co3d_only_for_artifact():
    source = _script()
    labeler = _focused_packet_labeler(source)
    assert 'selected_scenarios[0]}" = "artifact"' in labeler
    assert 'echo "CO-3D"' in labeler
    assert 'selected_scenarios=(artifact)' in source


def test_wave3b_fallback_privacy_focused_label_is_co3e_order_independent():
    source = _script()
    labeler = _focused_packet_labeler(source)
    assert "has_fallback=false" in labeler
    assert "has_privacy=false" in labeler
    assert 'echo "CO-3E"' in labeler
    assert 'selected_scenarios=(fallback privacy)' in source
    assert 'selected_scenarios=(privacy fallback)' in source
