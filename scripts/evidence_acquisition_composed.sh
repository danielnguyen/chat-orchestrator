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
    and .acquisition.returned_item_count == 2
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

run_evidence_acquisition_composed_suite() {
  run_evidence_targeted_scenario
  echo "Evidence acquisition composed smoke passed: scenarios=targeted"
}
