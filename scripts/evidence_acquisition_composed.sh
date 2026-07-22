#!/usr/bin/env bash

evidence_prepare_fixture_config() {
  mkdir -p "$COMPOSED_SMOKE_TMP/config/sources" "$COMPOSED_SMOKE_TMP/audit"
  chmod 0777 "$COMPOSED_SMOKE_TMP/audit"
  : >"$COMPOSED_SMOKE_TMP/audit/events.jsonl"
  chmod 0666 "$COMPOSED_SMOKE_TMP/audit/events.jsonl"

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
  local external_context
  external_context="$(jq -c '
    . + {
      source_ids: (.source_ids // []),
      domain_tags: (.domain_tags // []),
      exact_source_refs: (.exact_source_refs // [])
    }
  ' <<<"$5")"
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

restart_orchestrator_with_privacy() {
  COMPOSED_PRIVACY_CONTEXT_ENABLED="$1"
  export COMPOSED_PRIVACY_CONTEXT_ENABLED
  docker compose -f "$COMPOSE" up -d --force-recreate --no-deps orchestrator >/dev/null
  wait_for_http "http://127.0.0.1:14361/healthz"
}

run_evidence_chat_with_artifacts() {
  local owner="$1" client="$2" conversation_id="$3" question="$4"
  local external_context
  external_context="$(jq -c '
    . + {
      source_ids: (.source_ids // []),
      domain_tags: (.domain_tags // []),
      exact_source_refs: (.exact_source_refs // [])
    }
  ' <<<"$5")"
  co_post "$(jq -nc \
    --arg owner "$owner" \
    --arg client "$client" \
    --arg conversation "$conversation_id" \
    --arg question "$question" \
    --argjson external_context "$external_context" \
    '{owner_id:$owner,client_id:$client,conversation_id:$conversation,surface:"chat",messages:[{role:"user",content:$question}],sensitivity:"private",external_context_enabled:true,external_context:$external_context,retrieval:{include_artifacts:true,k:8,min_score:0,scope:"conversation",time_window:"all",retrieval_mode:"balanced"}}')"
}

run_evidence_messages() {
  local owner="$1" client="$2" conversation_id="$3" messages="$4"
  local external_context="${5:-null}"
  if [ "$external_context" != "null" ]; then
    external_context="$(jq -c '
      . + {
        source_ids: (.source_ids // []),
        domain_tags: (.domain_tags // []),
        exact_source_refs: (.exact_source_refs // [])
      }
    ' <<<"$external_context")"
  fi
  co_post "$(jq -nc \
    --arg owner "$owner" \
    --arg client "$client" \
    --arg conversation "$conversation_id" \
    --argjson messages "$messages" \
    --argjson external_context "$external_context" '
      {owner_id:$owner,client_id:$client,conversation_id:$conversation,surface:"chat",messages:$messages,sensitivity:"private"}
      + if $external_context == null then {} else {external_context_enabled:true,external_context:$external_context} end
    ')"
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
  question="Compare these two review calendar records and explain the differences between them."
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
  configure_source_fixture "calendar-beta" "unavailable_after_first"
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-hybrid-failure")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  fixture_calls="$(fetch_source_fixture_calls)"
  echo "Evidence hybrid failure observed: response_status=$(jq -r '.status' <<<"$response") considered_count=$(jq -r '.acquisition.sources_considered | length' <<<"$manifest") sufficiency=$(jq -r '.sufficiency.status' <<<"$manifest") next_step=$(jq -r '.next_steps.selections[0].selected_next_step' <<<"$manifest") provider_chat=$(jq '[.calls[] | select(.kind == "chat")] | length' <<<"$provider_calls") failed_source_calls=$(jq '[.calls[] | select(.source == "calendar-beta" and .operation == "ics_get")] | length' <<<"$fixture_calls")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("requested conclusion") or contains("selected-source comparison"))
  ' <<<"$response" >/dev/null
  jq -e '
    .acquisition.sources_considered == ["calendar_alpha","calendar_beta"]
    and (.sufficiency.status == "insufficient" or .sufficiency.status == "unknown")
    and (
      .next_steps.selections[0].selected_next_step == "provide_qualified_partial_answer"
      or .next_steps.selections[0].selected_next_step == "disclose_unexamined_scope"
      or .next_steps.selections[0].selected_next_step == "withhold_unsupported_conclusion"
    )
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  jq -e '
    ([.calls[] | select(.source == "calendar-beta" and .operation == "ics_get")] | length) == 2
  ' <<<"$fixture_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  configure_source_fixture "calendar-beta" "ready"
  echo "Evidence hybrid: positive_context_pack=1 positive_expansions=2 positive_provider=1 failure_provider=0 failure_retry=0"
}

run_evidence_exhaustive_scenarios() {
  local owner client conversation_id question external response request_id answer
  local trace provider_calls diagnostics manifest audit
  question="Check whether every mandatory record in the register is reviewed."
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
  configure_source_fixture "complete-sheet" "large"
  restart_orchestrator_with_reserve 180000
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-exhaustive-truncation")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  echo "Evidence exhaustive truncation stage: chat_response_received"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  echo "Evidence exhaustive truncation stage: trace_resolved"
  provider_calls="$(fetch_provider_calls "$request_id")"
  echo "Evidence exhaustive truncation stage: provider_log_resolved"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  echo "Evidence exhaustive truncation stage: runtime_diagnostics_resolved"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  echo "Evidence exhaustive truncation observed: response_status=$(jq -r '.status' <<<"$response") manifest_status=$(jq -r '.status' <<<"$manifest") expansion_success=$(jq -r '.acquisition.expansion_successful_count' <<<"$manifest") item_count=$(jq -r '.acquisition.item_count' <<<"$manifest") retained_count=$(jq -r '.acquisition.prompt_retained_item_count' <<<"$manifest") sufficiency=$(jq -r '.sufficiency.status' <<<"$manifest") cr_sufficiency_events=$(jq '[.events[] | select(.event_type == "evidence_sufficiency_evaluated")] | length' <<<"$diagnostics") next_step=$(jq -r '.next_steps.selections[0].selected_next_step' <<<"$manifest") provider_chat=$(jq '[.calls[] | select(.kind == "chat")] | length' <<<"$provider_calls") provider_model=$(jq -r '[.calls[] | select(.kind == "chat")][0].model // "not_called"' <<<"$provider_calls")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("reasoning context"))
    and (.answer | contains("withholding a complete-scope conclusion"))
  ' <<<"$response" >/dev/null
  jq -e '
    .acquisition.expansion_successful_count == 1
    and .acquisition.item_count == 1
    and .acquisition.prompt_retained_item_count == 0
    and .sufficiency.status == "unknown"
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  restart_orchestrator_with_reserve 2048
  configure_source_fixture "complete-sheet" "ready"
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
  question="Check whether every mandatory record in the register is reviewed."
  provider_post "/fixture/reset" '{}'
  restrict_dsa_config_to "complete_register.yaml"
  reset_source_fixture
  configure_source_fixture "complete-sheet" "empty_after_first"
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
  local manifest provider_calls diagnostics audit source_calls first_request_id
  local initial_result_count initial_retained_count exact_retained_count
  owner="owner-evidence-followup"
  client="client-evidence-followup"
  question="Verify the follow-up records."
  external='{"enabled":true,"source_ids":["followup_records"],"allowed_sensitivity":"medium","max_results":8}'

  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  configure_source_fixture "followup-sheet" "alternating_large_compact"
  reset_dsa_audit
  restart_orchestrator_with_reserve 8000
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
  source_calls="$(fetch_source_fixture_calls)"
  initial_result_count="$(jq -r '.next_steps.initial_attempt.result_count' <<<"$manifest")"
  initial_retained_count="$(jq -r '.next_steps.initial_attempt.retained_reference_count' <<<"$manifest")"
  exact_retained_count="$(jq -r '.acquisition.prompt_retained_item_count' <<<"$manifest")"
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
  jq -e '
    [.calls[] | select(
      .source == "followup-sheet" and .operation == "google_values"
    )] as $calls
    | ($calls | length) == 2
    and [$calls[].ordinal] == [1, 2]
    and [$calls[].variant] == ["large", "compact"]
    and ([$calls[].mode] | all(. == "alternating_large_compact"))
    and ($calls[0].returned_row_count == $calls[1].returned_row_count)
    and ($calls[0].returned_cell_character_count
      > $calls[1].returned_cell_character_count)
  ' <<<"$source_calls" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 1' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 2 2 2

  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  source_calls="$(fetch_source_fixture_calls)"
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
  jq -e '
    [.calls[] | select(
      .source == "followup-sheet" and .operation == "google_values"
    )] as $calls
    | ($calls | length) == 3
    and [$calls[].ordinal] == [1, 2, 3]
    and [$calls[].variant] == ["large", "compact", "large"]
    and ([$calls[].mode] | all(. == "alternating_large_compact"))
    and ($calls[0].returned_cell_character_count
      > $calls[1].returned_cell_character_count)
    and ($calls[0].returned_cell_character_count
      == $calls[2].returned_cell_character_count)
  ' <<<"$source_calls" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  configure_source_fixture "followup-sheet" "ready"
  restart_orchestrator_with_reserve 2048
  echo "Evidence changed premise: initial_request=$first_request_id targeted_results=$initial_result_count targeted_retained=$initial_retained_count changed_premise_authorizations=1 exact_fetch=1 exact_retained=$exact_retained_count selections=2 provider=1 repeated_targeted=1 repeated_guard=premise_already_attempted repeated_fetch=0 repeated_provider=0"
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

normalized_first_paragraph() {
  awk 'BEGIN { RS = "" } { gsub(/[[:space:]]+/, " "); print; exit }'
}

assert_pure_history() {
  local owner="$1" client="$2" conversation_id="$3" prior_answer="$4"
  local question="$5" expected_fragment="$6" messages response request_id trace
  local provider_calls serialized
  messages="$(jq -nc \
    --arg answer "$prior_answer" \
    --arg question "$question" \
    '[{role:"assistant",content:$answer},{role:"user",content:$question}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  response="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  jq -e \
    --arg fragment "$expected_fragment" '
      (.answer | contains($fragment))
      and (.answer | endswith("I did not perform a new verification for this explanation."))
    ' <<<"$response" >/dev/null
  jq -e '
    .retrieval.status == "not_requested"
    and .model_call.status == "not_called"
    and .prompt.claim_explanation.explanation_kind == "acquisition"
    and .prompt.claim_explanation.storage_call_count == 1
    and .prompt.claim_explanation.provider_call_count == 0
    and .prompt.claim_explanation.manifest_resolution_status == "resolved"
    and (.prompt | has("evidence_acquisition") | not)
  ' <<<"$trace" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  test "$(fetch_dsa_audit | jq 'length')" = "0"
  assert_persisted_answer_matches "$conversation_id" "$request_id" "$(jq -r '.answer' <<<"$response")"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  serialized="$(jq -c . <<<"$response")$(jq -c '.prompt.claim_explanation' <<<"$trace")"
  case "$serialized" in
    *records_primary*|*complete_register*|*calendar_alpha*|*calendar_beta*|*google_sheets:*|*http://*|*PRIVATE*)
      echo "trace-first acquisition history exposed a private identifier or content" >&2
      return 1
      ;;
  esac
}

run_evidence_history_scenarios() {
  local owner client conversation_id external response request_id answer first_paragraph
  local messages history history_request trace provider_calls

  owner="owner-history-targeted"
  client="client-history-targeted"
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The migration record supports the bounded setting."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-targeted")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  assert_pure_history "$owner" "$client" "$conversation_id" "$answer" \
    "What did you check?" "retained record shows a targeted lookup"

  owner="owner-history-exact"
  client="client-history-exact"
  external='{"enabled":true,"source_ids":["records_primary"],"exact_source_refs":[{"source_id":"records_primary","source_ref":"google_sheets:records_primary:Records!A2:C2"}],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The exact migration record supports the bounded setting."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-exact")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the exact migration record." "$external")"
  answer="$(jq -r '.answer' <<<"$response")"
  first_paragraph="$(printf '%s' "$answer" | normalized_first_paragraph)"
  messages="$(jq -nc \
    --arg answer "$answer" \
    --arg target "$first_paragraph" '
    [{role:"assistant",content:$answer},{role:"user",content:"Continue."},{role:"assistant",content:"A newer unrelated answer."},{role:"user",content:("What did you check for the statement \"" + $target + "\"?")}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  history_request="$(jq -r '.request_id' <<<"$history")"
  trace="$(fetch_trace "$history_request")"
  provider_calls="$(fetch_provider_calls "$history_request")"
  jq -e '
    .status == "ok"
    and (.answer | contains("specified references"))
    and (.answer | endswith("I did not perform a new verification for this explanation."))
  ' <<<"$history" >/dev/null
  jq -e '
    .prompt.claim_explanation.target_mode == "quoted_first_paragraph"
    and .prompt.claim_explanation.manifest_resolution_status == "resolved"
    and .prompt.claim_explanation.storage_call_count == 1
  ' <<<"$trace" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  test "$(fetch_dsa_audit | jq 'length')" = "0"

  owner="owner-history-hybrid"
  client="client-history-hybrid"
  external='{"enabled":true,"source_ids":["calendar_alpha","calendar_beta"],"allowed_sensitivity":"medium","max_results":2}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The selected calendars show bounded differences."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-hybrid")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Compare these two review calendars and explain the differences between them." "$external")"
  answer="$(jq -r '.answer' <<<"$response")"
  assert_pure_history "$owner" "$client" "$conversation_id" "$answer" \
    "What did you examine?" "bounded comparison across selected configured sources"

  owner="owner-history-exhaustive"
  client="client-history-exhaustive"
  external='{"enabled":true,"source_ids":["complete_register"],"allowed_sensitivity":"medium","max_results":1}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The configured register shows every mandatory entry was reviewed."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-exhaustive")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Check whether every mandatory entry in the register is reviewed." "$external")"
  answer="$(jq -r '.answer' <<<"$response")"
  assert_pure_history "$owner" "$client" "$conversation_id" "$answer" \
    "Did you look at everything relevant?" "Within the declared bounded scope, yes."

  owner="owner-history-limited"
  client="client-history-limited"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The available migration record supports the bounded setting."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-limited")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." '{"enabled":true,"allowed_sensitivity":"medium"}')"
  answer="$(jq -r '.answer' <<<"$response")"
  assert_pure_history "$owner" "$client" "$conversation_id" "$answer" \
    "What might you have missed?" "sufficient only with recorded limitations"

  owner="owner-history-unknown"
  client="client-history-unknown"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  conversation_id="$(resolve_conversation "$owner" "$client" "history-unknown")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the zephyr artifact." '{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}')"
  answer="$(jq -r '.answer' <<<"$response")"
  assert_pure_history "$owner" "$client" "$conversation_id" "$answer" \
    "What did you not check?" "sufficiency remained unknown"
  echo "Evidence history: targeted=resolved exact_quoted=resolved hybrid=resolved exhaustive=resolved limited=resolved unknown=resolved claim_dependency=0 provider=0 dsa=0 cr_evidence=0"
}

run_evidence_privacy_history_scenario() {
  local owner client conversation_id external response request_id answer trace manifest
  owner="owner-history-private"
  client="client-history-private"
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  restart_orchestrator_with_privacy true
  queue_provider_answer "PRIVATE SOURCE DETAIL from the migration record."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-private")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  jq -e '
    .acquisition.source_identifiers_suppressed == true
    and .acquisition.sources_considered == []
    and .acquisition.sources_considered_count == 1
    and .acquisition.source_references_returned == []
    and .acquisition.source_references_returned_count == 2
  ' <<<"$manifest" >/dev/null
  case "$(jq -c . <<<"$response")$(jq -c . <<<"$manifest")" in
    *records_primary*|*google_sheets:*|*PRIVATE\ SOURCE\ DETAIL*)
      echo "privacy-suppressed evidence response exposed identifiers or content" >&2
      return 1
      ;;
  esac
  assert_pure_history "$owner" "$client" "$conversation_id" "$answer" \
    "What did you check?" "retained record shows a targeted lookup"
  restart_orchestrator_with_privacy false
  echo "Evidence privacy history: suppressed_source_count=1 suppressed_reference_count=2 reconstructed_identifiers=0"
}

run_evidence_claim_subset_scenario() {
  local owner client conversation_id source_message_id derived_id response request_id
  local answer trace manifest claims claim_digest response_digest association_count
  owner="owner-evidence-claim"
  client="client-evidence-claim"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-claim")"
  source_message_id="$(add_message "$conversation_id" "$owner" "$client" "user" "The setting is active in the retained file.")"
  derived_id="$(seed_derived \
    "$conversation_id" "$owner" "$client" "$source_message_id" \
    "The setting is active in the retained file." "active" "evidence-claim" "active")"
  queue_provider_answer "The retained file reports that the setting is active."
  response="$(run_evidence_chat_with_artifacts \
    "$owner" "$client" "$conversation_id" \
    "What do the retained file and migration records report about the setting?" \
    '{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium","max_results":5}')"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  claims="$(list_claim_records "$owner" "$conversation_id")"
  jq -e \
    --arg request_id "$request_id" \
    --arg derived_id "$derived_id" \
    --arg manifest_id "$(jq -r '.manifest_id' <<<"$manifest")" '
      (.records | length) == 1
      and .records[0].request_id == $request_id
      and .records[0].acquisition_manifest_id == $manifest_id
      and (.records[0].validated_evidence_references | length) == 1
      and .records[0].validated_evidence_references[0].ref_type == "derived_text"
      and .records[0].validated_evidence_references[0].ref_id == $derived_id
      and (.records[0].claim_anchor | contains("This reflects only") | not)
    ' <<<"$claims" >/dev/null
  jq -e \
    --arg derived_id "$derived_id" '
      .acquisition.item_count == 2
      and .acquisition.prompt_retained_item_count == 2
      and (.acquisition.source_references_returned | length) == 2
      and all(.acquisition.source_references_returned[]; contains($derived_id) | not)
    ' <<<"$manifest" >/dev/null
  claim_digest="$(jq -r '.records[0].claim_anchor_digest' <<<"$claims")"
  response_digest="$(jq -r '.response_digest' <<<"$manifest")"
  test "$claim_digest" != "$response_digest"
  test "$response_digest" = "sha256:$(printf '%s' "$answer" | sha256sum | cut -d' ' -f1)"
  association_count="$(psql_exec -At -c "
    SELECT count(*)
    FROM claim_records cr
    JOIN messages m ON m.id = cr.assistant_message_id
    JOIN traces t ON t.request_id = cr.request_id
    WHERE cr.request_id = '$request_id'
      AND cr.conversation_id = '$conversation_id'
      AND m.content = \$\$${answer}\$\$
      AND t.prompt_json->'evidence_acquisition'->>'manifest_id' = cr.acquisition_manifest_id;
  ")"
  test "$association_count" = "1"
  assert_request_persistence_counts "$conversation_id" "$request_id" 1
  echo "Evidence claim subset: acquired_external_items=2 validated_claim_support=1 manifest_link=1 claim_digest_distinct_from_response_digest=1 durable_association=1"
}

run_evidence_history_negative_scenarios() {
  local owner client conversation_id external response answer messages history request_id trace
  local provider_calls target sentinel
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}'

  owner="owner-history-mismatch"
  client="client-history-mismatch"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The retained migration record supports the bounded setting."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-mismatch")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  answer="$(jq -r '.answer' <<<"$response")"
  messages='[{"role":"assistant","content":"A mismatched immediate response."},{"role":"user","content":"What did you check?"}]'
  provider_post "/fixture/reset" '{}'
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("no retained acquisition record could be resolved"))
    and (.answer | endswith("I did not perform a new verification for this explanation."))
  ' <<<"$history" >/dev/null
  request_id="$(jq -r '.request_id' <<<"$history")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null

  owner="owner-history-ambiguous"
  client="client-history-ambiguous"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  conversation_id="$(resolve_conversation "$owner" "$client" "history-ambiguous")"
  queue_provider_answer "The duplicate bounded statement is supported."
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  answer="$(jq -r '.answer' <<<"$response")"
  queue_provider_answer "The duplicate bounded statement is supported."
  run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external" >/dev/null
  target="$(printf '%s' "$answer" | normalized_first_paragraph)"
  messages="$(jq -nc --arg target "$target" '[{role:"assistant",content:"A newer answer."},{role:"user",content:("What did you check for the statement \"" + $target + "\"?")}]')"
  provider_post "/fixture/reset" '{}'
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("more than one exact prior response matched"))
    and (.answer | contains("no record was selected"))
  ' <<<"$history" >/dev/null

  owner="owner-history-corrupt"
  client="client-history-corrupt"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The corruptible bounded statement is supported."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-corrupt")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  psql_exec -c "
    UPDATE traces
    SET prompt_json = jsonb_set(
      prompt_json,
      '{evidence_acquisition,assistant_message_id}',
      '\"association-corrupted\"'::jsonb
    )
    WHERE request_id = '$request_id';
  " >/dev/null
  messages="$(jq -nc --arg answer "$answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check?"}]')"
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("failed association or privacy validation"))
    and (.answer | contains("association-corrupted") | not)
  ' <<<"$history" >/dev/null

  owner="owner-history-private-invalid"
  client="client-history-private-invalid"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The private-boundary statement is supported."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-private-invalid")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  sentinel="PRIVATE-CREDENTIAL-SENTINEL"
  psql_exec -c "
    UPDATE traces
    SET prompt_json = jsonb_set(
      prompt_json,
      '{evidence_acquisition,acquisition,api_key}',
      to_jsonb('$sentinel'::text),
      true
    )
    WHERE request_id = '$request_id';
  " >/dev/null
  messages="$(jq -nc --arg answer "$answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check?"}]')"
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  trace="$(fetch_trace "$(jq -r '.request_id' <<<"$history")")"
  jq -e \
    --arg sentinel "$sentinel" '
      .status == "degraded"
      and (.answer | contains("failed association or privacy validation"))
      and (.answer | contains($sentinel) | not)
    ' <<<"$history" >/dev/null
  case "$(jq -c . <<<"$trace")" in
    *PRIVATE-CREDENTIAL-SENTINEL*)
      echo "privacy-invalid history leaked corrupted content" >&2
      return 1
      ;;
  esac

  owner="owner-history-isolated"
  client="client-history-isolated"
  conversation_id="$(resolve_conversation "$owner" "$client" "history-isolated")"
  messages="$(jq -nc --arg answer "$answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check?"}]')"
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("no retained acquisition record could be resolved"))
  ' <<<"$history" >/dev/null
  case "$(jq -c . <<<"$history")" in
    *owner-history-private-invalid*|*PRIVATE-CREDENTIAL-SENTINEL*|*records_primary*)
      echo "owner-isolated history exposed the original record" >&2
      return 1
      ;;
  esac
  echo "Evidence history negatives: immediate_no_backward_scan=1 quoted_ambiguity=1 malformed_association=1 privacy_invalid=1 owner_isolation=1 provider=0"
}

run_evidence_compound_scenarios() {
  local owner client conversation_id external original original_request original_answer
  local messages response request_id answer trace manifest original_manifest provider_calls
  local diagnostics audit replacement
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium","max_results":5}'

  owner="owner-evidence-compound"
  client="client-evidence-compound"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-compound")"
  queue_provider_answer "The migration record supports the original bounded statement."
  original="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  original_request="$(jq -r '.request_id' <<<"$original")"
  original_answer="$(jq -r '.answer' <<<"$original")"
  original_manifest="$(fetch_trace "$original_request" | jq -c '.prompt.evidence_acquisition')"
  messages="$(jq -nc --arg answer "$original_answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check? Check again."}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  queue_provider_answer "The new retained evidence supports the prior statement."
  response="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  jq -e '
    .status == "ok"
    and (.answer | startswith("Original acquisition:\n"))
    and (.answer | contains("\n\nNew verification:\n"))
    and (.answer | contains("The new retained evidence supports the prior statement."))
    and (.answer | contains("I did not perform a new verification for this explanation.") | not)
  ' <<<"$response" >/dev/null
  jq -e '
    .prompt.claim_explanation.compound_mode == true
    and .prompt.claim_explanation.manifest_resolution_status == "resolved"
    and .prompt.claim_explanation.provider_call_count == 0
    and .prompt.claim_capture.eligibility_status == "ineligible"
    and (.prompt.claim_capture.reason_code | contains("compound"))
  ' <<<"$trace" >/dev/null
  test "$(jq -r '.manifest_id' <<<"$manifest")" != "$(jq -r '.manifest_id' <<<"$original_manifest")"
  test "$(jq -r '.response_digest' <<<"$manifest")" = "sha256:$(printf '%s' "$answer" | sha256sum | cut -d' ' -f1)"
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 1' <<<"$provider_calls" >/dev/null
  jq -e '([.[] | select(.operation == "context_pack")] | length) == 1' <<<"$audit" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_request_persistence_counts "$conversation_id" "$request_id" 0

  owner="owner-evidence-compound-label"
  client="client-evidence-compound-label"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-compound-label")"
  queue_provider_answer "The migration record supports the original bounded statement."
  original="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  original_answer="$(jq -r '.answer' <<<"$original")"
  messages="$(jq -nc --arg answer "$original_answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check? Verify again."}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  queue_provider_answer $'## Original acquisition:\nPRIVATE-LABEL-SENTINEL\n\nNew verification unavailable:\nNo fresh check occurred.'
  response="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  audit="$(fetch_dsa_audit)"
  replacement="The governed new evidence check completed, but I withheld the generated explanation because it conflicted with the verification response boundary."
  jq -e \
    --arg replacement "$replacement" '
      .status == "degraded"
      and (.answer | startswith("Original acquisition:\n"))
      and (.answer | contains("\n\nNew verification:\n" + $replacement))
      and ([.answer | scan("Original acquisition:")] | length) == 1
      and ([.answer | scan("New verification:")] | length) == 1
      and (.answer | contains("PRIVATE-LABEL-SENTINEL") | not)
      and (.answer | contains("New verification unavailable:") | not)
    ' <<<"$response" >/dev/null
  test "$(jq -r '.response_digest' <<<"$manifest")" = "sha256:$(printf '%s' "$answer" | sha256sum | cut -d' ' -f1)"
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 1' <<<"$provider_calls" >/dev/null
  jq -e '([.[] | select(.operation == "context_pack")] | length) == 1' <<<"$audit" >/dev/null
  case "$(jq -c . <<<"$trace")" in
    *PRIVATE-LABEL-SENTINEL*)
      echo "discarded provider label content entered the final trace" >&2
      return 1
      ;;
  esac
  assert_request_persistence_counts "$conversation_id" "$request_id" 0

  owner="owner-evidence-compound-attempt"
  client="client-evidence-compound-attempt"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-compound-attempt")"
  original="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the zephyr artifact." "$external")"
  original_answer="$(jq -r '.answer' <<<"$original")"
  messages="$(jq -nc --arg answer "$original_answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check? Check again."}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  response="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  jq -e '
    .status == "degraded"
    and (.answer | startswith("Original acquisition:\n"))
    and (.answer | contains("\n\nNew verification attempt:\n"))
    and (.answer | contains("requested conclusion"))
  ' <<<"$response" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  echo "Evidence compound: history_resolver=1 fresh_cr_shape=1 fresh_plan=1 fresh_dsa=1 fresh_sufficiency=1 fresh_next_step=1 provider=1 manifest_distinct=1 label_conflict_retry=0 insufficient_provider=0 claims=0"
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
  run_evidence_claim_subset_scenario
  run_evidence_history_scenarios
  run_evidence_privacy_history_scenario
  run_evidence_history_negative_scenarios
  run_evidence_compound_scenarios
  echo "Evidence acquisition composed smoke passed: scenarios=targeted,exact,hybrid,exhaustive,limited,unknown,failure,clarification,changed-premise,repeated-premise,adversarial-provider,claim-subset,trace-first-history,privacy-history,history-negatives,compound-verification"
}
