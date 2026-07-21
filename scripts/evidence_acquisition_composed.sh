#!/usr/bin/env bash

evidence_prepare_fixture_config() {
  mkdir -p "$COMPOSED_SMOKE_TMP/config/sources" "$COMPOSED_SMOKE_TMP/audit"
  chmod 0777 "$COMPOSED_SMOKE_TMP/audit"

  cat >"$COMPOSED_SMOKE_TMP/config/credentials.yaml" <<'YAML'
credentials:
  fixture_google:
    type: google_application_default
YAML

  cat >"$COMPOSED_SMOKE_TMP/config/sources/records_primary.yaml" <<'YAML'
source_id: records_primary
display_name: Migration Records
description: Bounded migration records.
domain_tags: [records, migration]
connector: google_sheets
enabled: true
authority_role: authoritative
sensitivity: medium
access_mode: read_only
connector_config:
  spreadsheet_id: targeted-sheet
  worksheet: Records
  header_row: 1
  credentials_ref: fixture_google
retrieval:
  default_mode: targeted
  max_results: 8
  max_bytes: 50000
  max_text_chars: 12000
  max_context_rows: 20
  allow_full_fetch: true
result_text:
  title_from: Record
  include_fields: [Record, Status, Notes]
YAML

  cat >"$COMPOSED_SMOKE_TMP/config/sources/records_optional.yaml" <<'YAML'
source_id: records_optional
display_name: Optional Migration Notes
description: Optional supplemental migration notes.
domain_tags: [records, migration]
connector: google_sheets
enabled: false
authority_role: supplemental
sensitivity: medium
access_mode: read_only
connector_config:
  spreadsheet_id: targeted-sheet
  worksheet: Records
  header_row: 1
  credentials_ref: fixture_google
retrieval:
  default_mode: targeted
  max_results: 8
  max_bytes: 50000
  max_text_chars: 12000
  max_context_rows: 20
  allow_full_fetch: true
result_text:
  title_from: Record
  include_fields: [Record, Status, Notes]
YAML

  cat >"$COMPOSED_SMOKE_TMP/config/sources/complete_register.yaml" <<'YAML'
source_id: complete_register
display_name: Configured Review Register
description: Complete configured worksheet for a bounded review.
domain_tags: [register, review]
connector: google_sheets
enabled: true
authority_role: authoritative
sensitivity: medium
access_mode: read_only
connector_config:
  spreadsheet_id: complete-sheet
  worksheet: Register
  header_row: 1
  credentials_ref: fixture_google
retrieval:
  default_mode: targeted
  max_results: 8
  max_bytes: 50000
  max_text_chars: 12000
  max_context_rows: 20
  allow_full_fetch: true
result_text:
  title_from: Entry
  include_fields: [Entry, Required, Status]
YAML

  cat >"$COMPOSED_SMOKE_TMP/config/sources/followup_records.yaml" <<'YAML'
source_id: followup_records
display_name: Follow-up Records
description: Bounded records supporting one exact follow-up.
domain_tags: [followup, records]
connector: google_sheets
enabled: true
authority_role: authoritative
sensitivity: medium
access_mode: read_only
connector_config:
  spreadsheet_id: followup-sheet
  worksheet: Followup
  header_row: 1
  credentials_ref: fixture_google
retrieval:
  default_mode: targeted
  max_results: 8
  max_bytes: 50000
  max_text_chars: 12000
  max_context_rows: 20
  allow_full_fetch: true
result_text:
  title_from: Record
  include_fields: [Record, Status, Notes]
YAML

  cat >"$COMPOSED_SMOKE_TMP/config/sources/calendar_alpha.yaml" <<'YAML'
source_id: calendar_alpha
display_name: Alpha Review Calendar
description: Configured calendar for alpha review events.
domain_tags: [calendar, comparison]
connector: ics_calendar
enabled: true
authority_role: authoritative
sensitivity: low
access_mode: read_only
connector_config:
  url: http://source-fixture:8000/ics/calendar-alpha.ics
  timezone: UTC
retrieval:
  default_mode: targeted
  max_results: 8
  max_bytes: 50000
  max_text_chars: 12000
  lookback_days: 365
  lookahead_days: 365
  max_context_rows: 8
  allow_full_fetch: false
result_text:
  title_from: summary
  include_fields: [summary, start, end, location, description]
YAML

  cat >"$COMPOSED_SMOKE_TMP/config/sources/calendar_beta.yaml" <<'YAML'
source_id: calendar_beta
display_name: Beta Review Calendar
description: Configured calendar for beta review events.
domain_tags: [calendar, comparison]
connector: ics_calendar
enabled: true
authority_role: supplemental
sensitivity: low
access_mode: read_only
connector_config:
  url: http://source-fixture:8000/ics/calendar-beta.ics
  timezone: UTC
retrieval:
  default_mode: targeted
  max_results: 8
  max_bytes: 50000
  max_text_chars: 12000
  lookback_days: 365
  lookahead_days: 365
  max_context_rows: 8
  allow_full_fetch: false
result_text:
  title_from: summary
  include_fields: [summary, start, end, location, description]
YAML
}

run_evidence_chat() {
  local owner="$1" client="$2" conversation_id="$3" question="$4"
  local external_context="$5"
  co_post "$(jq -nc \
    --arg owner "$owner" \
    --arg client "$client" \
    --arg conversation "$conversation_id" \
    --arg question "$question" \
    --argjson external_context "$external_context" \
    '{owner_id:$owner,client_id:$client,conversation_id:$conversation,surface:"chat",messages:[{role:"user",content:$question}],sensitivity:"private",external_context_enabled:true,external_context:$external_context}')"
}

fetch_source_fixture_calls() {
  curl -fsS "http://127.0.0.1:14351/fixture/calls"
}

reset_source_fixture() {
  curl -fsS -X POST "http://127.0.0.1:14351/fixture/reset" >/dev/null
}

configure_source_fixture() {
  local source_name="$1" mode="$2"
  curl -fsS -X POST "http://127.0.0.1:14351/fixture/sources/$source_name" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc --arg mode "$mode" '{mode:$mode}')" >/dev/null
}

queue_provider_answer() {
  local answer="$1"
  provider_post "/fixture/next-answer" \
    "$(jq -nc --arg answer "$answer" '{answer:$answer}')"
}

wait_for_http() {
  local url="$1"
  local attempt
  for attempt in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "service did not become ready: $url" >&2
  return 1
}

restart_orchestrator_with_reserve() {
  COMPOSED_PROMPT_OUTPUT_TOKEN_RESERVE="$1"
  export COMPOSED_PROMPT_OUTPUT_TOKEN_RESERVE
  docker compose -f "$COMPOSE" up -d --force-recreate --no-deps orchestrator >/dev/null
  wait_for_http "http://127.0.0.1:14361/healthz"
}

restart_dsa() {
  docker compose -f "$COMPOSE" up -d --force-recreate --no-deps dsa >/dev/null
  wait_for_http "http://127.0.0.1:14374/health"
}

restrict_dsa_config_to() {
  local retained="$1" path base
  for path in "$COMPOSED_SMOKE_TMP"/config/sources/*.yaml; do
    base="$(basename "$path")"
    if [ "$base" != "$retained" ]; then
      mv "$path" "$path.disabled"
    fi
  done
  restart_dsa
}

restore_dsa_config() {
  local path
  for path in "$COMPOSED_SMOKE_TMP"/config/sources/*.yaml.disabled; do
    if [ -e "$path" ]; then
      mv "$path" "${path%.disabled}"
    fi
  done
  restart_dsa
}

reset_dsa_audit() {
  : >"$COMPOSED_SMOKE_TMP/audit/events.jsonl"
}

fetch_dsa_audit() {
  if [ -s "$COMPOSED_SMOKE_TMP/audit/events.jsonl" ]; then
    jq -s . "$COMPOSED_SMOKE_TMP/audit/events.jsonl"
  else
    echo '[]'
  fi
}

runtime_diagnostics_from_trace() {
  local trace="$1" runtime_session_id
  runtime_session_id="$(jq -r '
    .prompt.runtime_session.runtime_session_id
    // .retrieval.prompt_assembly.runtime_session.runtime_session_id
    // empty
  ' <<<"$trace")"
  test -n "$runtime_session_id"
  fetch_runtime_diagnostics "$runtime_session_id"
}

assert_evidence_runtime_events() {
  local diagnostics="$1" request_id="$2"
  local expected_shape="$3" expected_plan="$4" expected_sufficiency="$5" expected_next="$6"
  jq -e \
    --arg request_id "$request_id" \
    --argjson shape "$expected_shape" \
    --argjson plan "$expected_plan" \
    --argjson sufficiency "$expected_sufficiency" \
    --argjson next "$expected_next" '
      ([.events[] | select(.event_payload_json.request_id == $request_id and .event_type == "evidence_shape_derived")] | length) == $shape
      and ([.events[] | select(.event_payload_json.request_id == $request_id and .event_type == "evidence_plan_compiled")] | length) == $plan
      and ([.events[] | select(.event_payload_json.request_id == $request_id and .event_type == "evidence_sufficiency_evaluated")] | length) == $sufficiency
      and ([.events[] | select(.event_payload_json.request_id == $request_id and .event_type == "evidence_next_step_selected")] | length) == $next
    ' <<<"$diagnostics" >/dev/null
}

run_evidence_targeted_scenario() {
  local owner client conversation_id question external response request_id answer
  local trace provider_calls fixture_calls diagnostics manifest
  owner="owner-evidence-targeted"
  client="client-evidence-targeted"
  question="Verify the migration record."
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium","max_results":5}'

  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-targeted")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  fixture_calls="$(fetch_source_fixture_calls)"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"

  jq -e '
    .status == "ok"
    and (.answer | endswith("This reflects only the targeted sources checked, not a complete search of every possible source."))
  ' <<<"$response" >/dev/null
  jq -e '
    .enabled == true
    and .attempted == true
    and .shape.task_shape == "targeted_lookup"
    and .plan.plan_status == "ready"
    and .plan.selected_strategies == ["targeted_retrieval"]
    and .acquisition.strategy_attempted == "targeted_retrieval"
    and .acquisition.sources_considered == ["records_primary"]
    and .acquisition.sources_selected == ["records_primary"]
    and .acquisition.sources_used == ["records_primary"]
    and .acquisition.item_count == 2
    and .acquisition.prompt_retained_item_count == 2
    and .sufficiency.status == "sufficient_for_declared_scope"
    and .next_steps.selection_count == 1
    and .next_steps.selections[0].selected_next_step == "answer_within_declared_scope"
    and (.assistant_message_id | type == "string")
    and (.response_digest | test("^sha256:[0-9a-f]{64}$"))
  ' <<<"$manifest" >/dev/null
  jq -e '
    ([.calls[] | select(.kind == "chat")] | length) == 1
    and ([.calls[] | select(.kind == "chat") | .normalized_messages[] | select(.content | contains("The migration record confirms the bounded setting."))] | length) == 1
    and ([.calls[] | select(.kind == "chat") | .normalized_messages[] | select(.content | contains("A second retained row prevents count-only proof."))] | length) == 1
  ' <<<"$provider_calls" >/dev/null
  jq -e '
    ([.calls[] | select(.source == "targeted-sheet" and .operation == "google_values")] | length) == 1
  ' <<<"$fixture_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  echo "Evidence targeted: cr_shape=1 cr_plan=1 dsa_context_pack=1 retained_items=2 cr_sufficiency=1 cr_next_step=1 provider_chat=1 assistant_persistence=1 trace_persistence=1"
}

run_evidence_exact_scenario() {
  local owner client conversation_id question external response request_id answer
  local trace provider_calls diagnostics manifest audit
  owner="owner-evidence-exact"
  client="client-evidence-exact"
  question="Verify the exact migration record."
  external='{"enabled":true,"source_ids":["records_primary"],"exact_source_refs":[{"source_id":"records_primary","source_ref":"google_sheets:records_primary:Records!A2:C2"}],"allowed_sensitivity":"medium"}'

  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  queue_provider_answer "The exact migration record confirms the bounded setting."
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-exact")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  audit="$(fetch_dsa_audit)"

  jq -e '
    .status == "ok"
    and (.answer | startswith("The exact migration record confirms"))
    and (.answer | endswith("This reflects only the targeted sources checked, not a complete search of every possible source."))
  ' <<<"$response" >/dev/null
  jq -e '
    .plan.selected_strategies == ["exact_fetch"]
    and .acquisition.strategy_attempted == "exact_fetch"
    and .acquisition.exact_reference_attempt_count == 1
    and .acquisition.exact_reference_successful_count == 1
    and .acquisition.item_count == 1
    and .acquisition.prompt_retained_item_count == 1
    and .sufficiency.status == "sufficient_for_declared_scope"
  ' <<<"$manifest" >/dev/null
  jq -e '([.[] | select(.operation == "fetch" and .status == "success")] | length) == 1' <<<"$audit" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 1' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  echo "Evidence exact: cr_shape=1 cr_plan=1 dsa_fetch=1 cr_sufficiency=1 cr_next_step=1 provider_chat=1"
}

run_evidence_hybrid_scenarios() {
  local owner client conversation_id question external response request_id answer
  local trace provider_calls diagnostics manifest audit fixture_calls
  question="Compare these two review calendars and explain the differences between them."
  external='{"enabled":true,"source_ids":["calendar_alpha","calendar_beta"],"allowed_sensitivity":"medium","max_results":2}'

  owner="owner-evidence-hybrid"
  client="client-evidence-hybrid"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  queue_provider_answer "The selected calendars record review events on different days."
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-hybrid")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  audit="$(fetch_dsa_audit)"
  fixture_calls="$(fetch_source_fixture_calls)"
  jq -e '
    .status == "ok"
    and (.answer | endswith("This comparison is limited to the selected sources and bounded context checked, not every potentially relevant source."))
  ' <<<"$response" >/dev/null
  jq -e '
    .shape.task_shape == "cross_source_comparison"
    and .plan.selected_strategies == ["hybrid"]
    and .acquisition.strategy_attempted == "hybrid"
    and .acquisition.expansion_attempt_count == 2
    and .acquisition.expansion_successful_count == 2
    and .sufficiency.status == "sufficient_for_declared_scope"
  ' <<<"$manifest" >/dev/null
  jq -e '
    ([.[] | select(.operation == "context_pack" and .status == "success")] | length) == 1
    and ([.[] | select(.operation == "context" and .status == "success")] | length) == 2
  ' <<<"$audit" >/dev/null
  jq -e '
    ([.calls[] | select(.source == "calendar-alpha" and .operation == "ics_get")] | length) == 2
    and ([.calls[] | select(.source == "calendar-beta" and .operation == "ics_get")] | length) == 2
  ' <<<"$fixture_calls" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 1' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"

  owner="owner-evidence-hybrid-failure"
  client="client-evidence-hybrid-failure"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  configure_source_fixture "calendar-beta" "unavailable"
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-hybrid-failure")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  fixture_calls="$(fetch_source_fixture_calls)"
  jq -e '
    .status == "degraded"
    and (.answer | contains("requested conclusion") or contains("selected-source comparison"))
  ' <<<"$response" >/dev/null
  jq -e '
    .acquisition.sources_considered == ["calendar_alpha","calendar_beta"]
    and (.sufficiency.status == "insufficient" or .sufficiency.status == "unknown")
    and (.next_steps.selections[0].selected_next_step == "disclose_unexamined_scope" or .next_steps.selections[0].selected_next_step == "withhold_unsupported_conclusion")
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  jq -e '
    ([.calls[] | select(.source == "calendar-beta" and .operation == "ics_get")] | length) == 1
  ' <<<"$fixture_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  configure_source_fixture "calendar-beta" "ready"
  echo "Evidence hybrid: positive_context_pack=1 positive_expansions=2 positive_provider=1 failure_provider=0 failure_retry=0"
}

run_evidence_exhaustive_scenarios() {
  local owner client conversation_id question external response request_id answer
  local trace provider_calls diagnostics manifest audit
  question="Check whether every mandatory entry in the register is reviewed."
  external='{"enabled":true,"source_ids":["complete_register"],"allowed_sensitivity":"medium","max_results":1}'

  owner="owner-evidence-exhaustive"
  client="client-evidence-exhaustive"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  queue_provider_answer "The configured register shows that every mandatory entry was reviewed."
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-exhaustive")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  audit="$(fetch_dsa_audit)"
  jq -e '
    .status == "ok"
    and (.answer | endswith("This conclusion is complete only for the declared source scope that was checked; sources outside that scope were not examined."))
    and (.answer | contains("universal") | not)
  ' <<<"$response" >/dev/null
  jq -e '
    .shape.task_shape == "bounded_exhaustive_review"
    and .plan.selected_strategies == ["hybrid"]
    and .acquisition.expansion_attempt_count == 1
    and .acquisition.expansion_successful_count == 1
    and .acquisition.item_count == 1
    and .acquisition.prompt_retained_item_count == 1
    and .sufficiency.status == "sufficient_for_declared_scope"
  ' <<<"$manifest" >/dev/null
  jq -e '
    ([.[] | select(.operation == "context_pack")] | length) == 1
    and ([.[] | select(.operation == "context")] | length) == 1
  ' <<<"$audit" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 1' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"

  owner="owner-evidence-exhaustive-truncation"
  client="client-evidence-exhaustive-truncation"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  restart_orchestrator_with_reserve 29500
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-exhaustive-truncation")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("reasoning context"))
    and (.answer | contains("withholding a complete-scope conclusion"))
  ' <<<"$response" >/dev/null
  jq -e '
    .acquisition.expansion_successful_count == 1
    and .acquisition.item_count == 1
    and .acquisition.prompt_retained_item_count == 0
    and .sufficiency.status == "insufficient"
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  restart_orchestrator_with_reserve 2048
  echo "Evidence exhaustive: positive_provider=1 configured_range=1 truncation_provider=0 truncation_retained=0"
}

run_evidence_limitation_and_failure_scenarios() {
  local owner client conversation_id question external response request_id trace
  local provider_calls manifest diagnostics

  owner="owner-evidence-limited"
  client="client-evidence-limited"
  question="Verify the migration record."
  external='{"enabled":true,"allowed_sensitivity":"medium","max_results":5}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The available migration record supports the bounded setting."
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-limited")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  jq -e '
    .status == "ok"
    and (.answer | contains("Limitation:"))
    and (.answer | endswith("This reflects only the targeted sources checked, not a complete search of every possible source."))
  ' <<<"$response" >/dev/null
  jq -e '
    .plan.plan_status == "ready_with_limitations"
    and .sufficiency.status == "sufficient_with_limitations"
    and .next_steps.selections[0].selected_next_step == "provide_qualified_partial_answer"
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 1' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1

  owner="owner-evidence-empty"
  client="client-evidence-empty"
  question="Verify the zephyr artifact."
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "PRIVATE PROVIDER SILENCE OVERCLAIM"
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-empty")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("requested targeted evidence"))
    and (.answer | contains("PRIVATE PROVIDER") | not)
    and (.answer | contains("withholding the requested conclusion"))
  ' <<<"$response" >/dev/null
  jq -e '
    .sufficiency.status == "unknown"
    and .next_steps.selections[0].selected_next_step == "withhold_unsupported_conclusion"
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1

  owner="owner-evidence-failure"
  client="client-evidence-failure"
  question="Verify the alpha review calendar record."
  external='{"enabled":true,"source_ids":["calendar_alpha"],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  configure_source_fixture "calendar-alpha" "unavailable"
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-failure")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("acquisition failed"))
  ' <<<"$response" >/dev/null
  jq -e '.sufficiency.status == "insufficient"' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  configure_source_fixture "calendar-alpha" "ready"

  owner="owner-evidence-malformed"
  client="client-evidence-malformed"
  question="Verify the migration record."
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  configure_source_fixture "targeted-sheet" "malformed"
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-malformed")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("PRIVATE MALFORMED CELL SENTINEL") | not)
  ' <<<"$response" >/dev/null
  jq -e '
    (.sufficiency.status == "insufficient" or .sufficiency.status == "unknown")
    and (.acquisition.dsa_error_codes | length) > 0
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  configure_source_fixture "targeted-sheet" "ready"

  local unauthorized_response unauthorized_status
  unauthorized_response="$(mktemp)"
  unauthorized_status="$(curl -sS -o "$unauthorized_response" -w '%{http_code}' http://127.0.0.1:14374/v1/sources)"
  test "$unauthorized_status" = "401"
  jq -e '.error.code == "unauthorized"' "$unauthorized_response" >/dev/null
  rm -f "$unauthorized_response"
  echo "Evidence outcomes: limited_provider=1 unknown_provider=0 failed_provider=0 malformed_provider=0 fallback=0 dsa_unauthorized=401"
}

run_evidence_clarification_scenario() {
  local owner client conversation_id question response request_id trace provider_calls manifest diagnostics
  owner="owner-evidence-clarification"
  client="client-evidence-clarification"
  question="Check whether every mandatory entry in the register is reviewed."
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  configure_source_fixture "complete-sheet" "empty"
  restrict_dsa_config_to "complete_register.yaml"
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-clarification")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" '{"enabled":true,"allowed_sensitivity":"medium"}')"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  jq -e '
    .status == "degraded"
    and .answer == "Which bounded source or source set should I examine?"
  ' <<<"$response" >/dev/null
  jq -e '
    .sufficiency.status == "unknown"
    and .next_steps.selections[0].selected_next_step == "ask_narrow_clarification"
    and .next_steps.selections[0].clarification_target == "source_scope"
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  configure_source_fixture "complete-sheet" "ready"
  restore_dsa_config
  echo "Evidence clarification: cr_selection=ask_narrow_clarification provider_chat=0 dsa_additional=0"
}

run_evidence_changed_premise_scenarios() {
  local owner client conversation_id question external response request_id trace
  local manifest provider_calls diagnostics audit first_request_id
  owner="owner-evidence-followup"
  client="client-evidence-followup"
  question="Verify the follow-up records."
  external='{"enabled":true,"source_ids":["followup_records"],"allowed_sensitivity":"medium","max_results":8}'

  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  restart_orchestrator_with_reserve 29500
  queue_provider_answer "The exact follow-up record confirms the bounded detail."
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-followup")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  first_request_id="$request_id"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  jq -e '
    .status == "ok"
    and (.answer | endswith("This reflects only the targeted sources checked, not a complete search of every possible source."))
  ' <<<"$response" >/dev/null
  jq -e '
    .plan.selected_strategies == ["exact_fetch"]
    and .acquisition.strategy_attempted == "exact_fetch"
    and .acquisition.exact_reference_attempt_count == 1
    and .acquisition.exact_reference_successful_count == 1
    and .acquisition.prompt_retained_item_count == 1
    and .sufficiency.status == "sufficient_for_declared_scope"
    and .next_steps.additional_acquisition_count == 1
    and .next_steps.selection_count == 2
    and .next_steps.initial_attempt.strategy == "targeted_retrieval"
    and (.next_steps.initial_attempt.result_count > 1)
    and .next_steps.initial_attempt.retained_reference_count == 0
    and .next_steps.initial_attempt.changed_premise_exact_fetch_followed == true
    and [.next_steps.selections[].selected_next_step] == ["perform_additional_acquisition","answer_within_declared_scope"]
    and .next_steps.selections[0].reacquisition_guard == "changed_premise_allowed"
    and .next_steps.selections[0].additional_acquisition_executed == true
  ' <<<"$manifest" >/dev/null
  jq -e '
    ([.[] | select(.operation == "context_pack")] | length) == 1
    and ([.[] | select(.operation == "fetch")] | length) == 1
  ' <<<"$audit" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 1' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 2 2 2

  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  jq -e '
    .status == "degraded"
    and (.answer | contains("requested conclusion"))
    and (.answer | contains("exact follow-up record confirms") | not)
  ' <<<"$response" >/dev/null
  jq -e '
    .sufficiency.status == "insufficient"
    and .next_steps.additional_acquisition_count == 0
    and .next_steps.selection_count == 1
    and .next_steps.selections[0].reacquisition_guard == "premise_already_attempted"
    and .next_steps.selections[0].selected_next_step != "perform_additional_acquisition"
    and .next_steps.selections[0].additional_acquisition_executed == false
  ' <<<"$manifest" >/dev/null
  jq -e '
    ([.[] | select(.operation == "context_pack")] | length) == 2
    and ([.[] | select(.operation == "fetch")] | length) == 1
  ' <<<"$audit" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  restart_orchestrator_with_reserve 2048
  echo "Evidence changed premise: initial_request=$first_request_id targeted=1 exact_fetch=1 selections=2 provider=1 repeated_guard=premise_already_attempted repeated_fetch=0 repeated_provider=0"
}

run_evidence_adversarial_provider_scenario() {
  local owner client conversation_id response request_id trace manifest provider_calls
  owner="owner-evidence-adversarial"
  client="client-evidence-adversarial"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "Every possible source was fully examined, and no evidence exists outside this result."
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-adversarial")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." '{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}')"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  jq -e '
    (.answer | contains("Every possible source was fully examined"))
    and (.answer | endswith("This reflects only the targeted sources checked, not a complete search of every possible source."))
  ' <<<"$response" >/dev/null
  jq -e '
    .shape.task_shape == "targeted_lookup"
    and .acquisition.sources_considered == ["records_primary"]
    and .sufficiency.status == "sufficient_for_declared_scope"
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 1' <<<"$provider_calls" >/dev/null
  echo "Evidence adversarial provider: provider_chat=1 scope_boundary=1 retry=0 manifest_scope_unchanged=1"
}

run_evidence_acquisition_composed_suite() {
  run_evidence_targeted_scenario
  run_evidence_exact_scenario
  run_evidence_hybrid_scenarios
  run_evidence_exhaustive_scenarios
  run_evidence_limitation_and_failure_scenarios
  run_evidence_clarification_scenario
  run_evidence_changed_premise_scenarios
  run_evidence_adversarial_provider_scenario
  echo "Evidence acquisition composed smoke passed: scenarios=targeted,exact,hybrid,exhaustive,limited,unknown,failure,clarification,changed-premise,repeated-premise,adversarial-provider"
}
