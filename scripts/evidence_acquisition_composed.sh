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
  local external_context model_override
  model_override="${6:-}"
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
    --arg model_override "$model_override" \
    --argjson external_context "$external_context" \
    '{owner_id:$owner,client_id:$client,conversation_id:$conversation,surface:"chat",messages:[{role:"user",content:$question}],sensitivity:"private",external_context_enabled:true,external_context:$external_context}
    + if $model_override == "" then {} else {model_override:$model_override} end')"
}

fetch_source_fixture_calls() {
  curl -fsS "http://127.0.0.1:14351/fixture/calls"
}

reset_source_fixture() {
  curl -fsS -X POST "http://127.0.0.1:14351/fixture/reset" >/dev/null
}

configure_source_fixture() {
  local source_name="$1" mode="$2" response
  response="$(curl -fsS -X POST "http://127.0.0.1:14351/fixture/sources/$source_name" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc --arg mode "$mode" '{mode:$mode}')")"
  jq -e --arg mode "$mode" '
    .status == "ok" and .mode == $mode
  ' <<<"$response" >/dev/null
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
  COMPOSED_ALLOW_MANUAL_OVERRIDE=false
  COMPOSED_PROMPT_OUTPUT_TOKEN_RESERVE="$1"
  export COMPOSED_ALLOW_MANUAL_OVERRIDE COMPOSED_PROMPT_OUTPUT_TOKEN_RESERVE
  docker compose -f "$COMPOSE" up -d --force-recreate --no-deps orchestrator >/dev/null
  wait_for_http "http://127.0.0.1:14361/healthz"
}

restart_orchestrator_for_changed_premise() {
  COMPOSED_ALLOW_MANUAL_OVERRIDE=true
  COMPOSED_PROMPT_OUTPUT_TOKEN_RESERVE=14744
  export COMPOSED_ALLOW_MANUAL_OVERRIDE COMPOSED_PROMPT_OUTPUT_TOKEN_RESERVE
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
  local path source_count disabled_count
  for path in "$COMPOSED_SMOKE_TMP"/config/sources/*.yaml.disabled; do
    if [ -e "$path" ]; then
      mv "$path" "${path%.disabled}"
    fi
  done
  restart_dsa
  source_count="$(find "$COMPOSED_SMOKE_TMP/config/sources" -maxdepth 1 -type f -name '*.yaml' | wc -l)"
  disabled_count="$(find "$COMPOSED_SMOKE_TMP/config/sources" -maxdepth 1 -type f -name '*.yaml.disabled' | wc -l)"
  test "$source_count" = "6"
  test "$disabled_count" = "0"
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

assert_claim_calibration_events() {
  local diagnostics="$1" request_id="$2" expected="$3"
  jq -e \
    --arg request_id "$request_id" \
    --argjson expected "$expected" '
      ([.events[] | select(
        .event_payload_json.request_id == $request_id
        and .event_type == "claim_calibration_evaluated"
      )] | length) == $expected
    ' <<<"$diagnostics" >/dev/null
}

assert_dsa_operation_counts() {
  local audit="$1" context_pack="$2" context="$3" fetch="$4"
  jq -e \
    --argjson context_pack "$context_pack" \
    --argjson context "$context" \
    --argjson fetch "$fetch" '
      ([.[] | select(.operation == "context_pack")] | length) == $context_pack
      and ([.[] | select(.operation == "context")] | length) == $context
      and ([.[] | select(.operation == "fetch")] | length) == $fetch
    ' <<<"$audit" >/dev/null
}

assert_provider_free_trace() {
  local trace="$1"
  jq -e '
    .router_decision.selected_model == "not_called"
    and .router_decision.provider == "none"
    and (
      (.router_decision | has("routing_contract") | not)
      or (
        .router_decision.routing_contract.selected_model == "not_called"
        and .router_decision.routing_contract.selected_provider == "none"
      )
    )
    and .model_call.status == "not_called"
    and .model_calls == []
    and .fallback.triggered == false
  ' <<<"$trace" >/dev/null
}

assert_history_request_boundaries() {
  local conversation_id="$1" response="$2" expected_resolution="$3"
  local request_id trace provider_calls diagnostics audit
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  jq -e --arg resolution "$expected_resolution" '
    .retrieval.status == "not_requested"
    and .prompt.claim_explanation.explanation_kind == "acquisition"
    and .prompt.claim_explanation.storage_call_count == 1
    and .prompt.claim_explanation.provider_call_count == 0
    and .prompt.claim_explanation.manifest_resolution_status == $resolution
    and (.prompt | has("evidence_acquisition") | not)
  ' <<<"$trace" >/dev/null
  assert_provider_free_trace "$trace"
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' \
    <<<"$provider_calls" >/dev/null
  assert_dsa_operation_counts "$audit" 0 0 0
  assert_evidence_runtime_events "$diagnostics" "$request_id" 0 0 0 0
  assert_claim_calibration_events "$diagnostics" "$request_id" 0
  assert_persisted_answer_matches \
    "$conversation_id" "$request_id" "$(jq -r '.answer' <<<"$response")"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  HISTORY_TRACE="$trace"
  HISTORY_REQUEST_ID="$request_id"
  HISTORY_RESPONSE="$response"
}

readonly EVIDENCE_HYBRID_COMPARISON_QUESTION="Compare these two review calendar records and explain the differences between them."
readonly EVIDENCE_EXHAUSTIVE_REVIEW_QUESTION="Check whether every mandatory record in the register is reviewed."
readonly EVIDENCE_HISTORY_NO_RECORD_SENTENCE="I couldn’t resolve a retained acquisition record for the specified response."
readonly EVIDENCE_HISTORY_AMBIGUOUS_SENTENCE="More than one exact prior response matched, so I did not select an acquisition record."
readonly EVIDENCE_HISTORY_NO_NEW_VERIFICATION_SENTENCE="I did not perform a new verification for this explanation."

run_evidence_targeted_scenario() {
  local owner client conversation_id question external response request_id answer
  local trace provider_calls fixture_calls diagnostics manifest audit
  owner="owner-evidence-targeted"
  client="client-evidence-targeted"
  question="Verify the migration record."
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium","max_results":5}'

  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-targeted")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  fixture_calls="$(fetch_source_fixture_calls)"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  audit="$(fetch_dsa_audit)"

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
  assert_jq "targeted.inventory" "$manifest" '
    .inventory.inventory_status == "complete_for_declared_scope"
    and .inventory.inventory_source_count == 6
    and .inventory.declared_source_count == 1
  '
  if ! assert_dsa_operation_counts "$audit" 1 0 0 >/dev/null 2>&1; then
    echo "Assertion failed: targeted.dsa" >&2
    return 1
  fi
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
  question="$EVIDENCE_HYBRID_COMPARISON_QUESTION"
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
  assert_provider_free_trace "$trace"
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
  question="$EVIDENCE_EXHAUSTIVE_REVIEW_QUESTION"
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
    and .sufficiency.status == "unknown"
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  restart_orchestrator_with_reserve 2048
  configure_source_fixture "complete-sheet" "ready"
  echo "Evidence exhaustive: positive_provider=1 configured_expansion=1 truncation_provider=0 truncation_retained=0"
}

run_evidence_limitation_and_failure_scenarios() {
  local owner client conversation_id question external response request_id trace
  local provider_calls manifest diagnostics audit source_calls answer

  owner="owner-evidence-limited"
  client="client-evidence-limited"
  question="Verify the migration record."
  external='{"enabled":true,"allowed_sensitivity":"medium","max_results":5}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  queue_provider_answer "The available migration record supports the bounded setting."
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-limited")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  answer="$(jq -r '.answer' <<<"$response")"
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
  assert_dsa_operation_counts "$audit" 1 0 0
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_claim_calibration_events "$diagnostics" "$request_id" 0
  jq -e '.fallback.triggered == false and (.model_calls | length) == 1' \
    <<<"$trace" >/dev/null
  assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0

  owner="owner-evidence-empty"
  client="client-evidence-empty"
  question="Verify the zephyr artifact."
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  queue_provider_answer "PRIVATE PROVIDER SILENCE OVERCLAIM"
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-empty")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  source_calls="$(fetch_source_fixture_calls)"
  answer="$(jq -r '.answer' <<<"$response")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("requested targeted evidence"))
    and (.answer | contains("PRIVATE PROVIDER") | not)
    and (.answer | contains("withholding the requested conclusion"))
  ' <<<"$response" >/dev/null
  jq -e '
    .sufficiency.status == "unknown"
    and .acquisition.sources_considered == ["records_primary"]
    and .acquisition.sources_selected == ["records_primary"]
    and .acquisition.item_count == 0
    and .next_steps.selections[0].selected_next_step == "withhold_unsupported_conclusion"
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_provider_free_trace "$trace"
  assert_dsa_operation_counts "$audit" 1 0 0
  jq -e '
    ([.calls[] | select(
      .source == "targeted-sheet" and .operation == "google_values"
    )] | length) == 1
  ' <<<"$source_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_claim_calibration_events "$diagnostics" "$request_id" 0
  assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0

  owner="owner-evidence-failure"
  client="client-evidence-failure"
  question="Verify the alpha review calendar record."
  external='{"enabled":true,"source_ids":["calendar_alpha"],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  configure_source_fixture "calendar-alpha" "unavailable"
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-failure")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  source_calls="$(fetch_source_fixture_calls)"
  answer="$(jq -r '.answer' <<<"$response")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("acquisition failed"))
  ' <<<"$response" >/dev/null
  jq -e '.sufficiency.status == "insufficient"' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_provider_free_trace "$trace"
  assert_dsa_operation_counts "$audit" 0 0 0
  jq -e '
    .retrieval.prompt_assembly.dsa.called == true
    and .retrieval.prompt_assembly.dsa.status == "error"
    and .retrieval.prompt_assembly.dsa.error_code == "http_502"
  ' <<<"$trace" >/dev/null
  jq -e '
    ([.calls[] | select(
      .source == "calendar-alpha" and .operation == "ics_get"
    )] | length) == 1
  ' <<<"$source_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_claim_calibration_events "$diagnostics" "$request_id" 0
  assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  case "$(jq -c . <<<"$response")$(jq -c . <<<"$trace")" in
    *PRIVATE*|*fixture-source-failure*|*credentials*|*Traceback*)
      echo "unavailable source diagnostics exposed private dependency data" >&2
      return 1
      ;;
  esac
  configure_source_fixture "calendar-alpha" "ready"

  owner="owner-evidence-malformed"
  client="client-evidence-malformed"
  question="Verify the migration record."
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  configure_source_fixture "targeted-sheet" "malformed"
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-malformed")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  source_calls="$(fetch_source_fixture_calls)"
  answer="$(jq -r '.answer' <<<"$response")"
  jq -e '
    .status == "degraded"
    and (.answer | contains("PRIVATE MALFORMED CELL SENTINEL") | not)
  ' <<<"$response" >/dev/null
  jq -e '
    .sufficiency.status == "insufficient"
    and (.acquisition.dsa_error_codes | length) > 0
  ' <<<"$manifest" >/dev/null
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  assert_provider_free_trace "$trace"
  assert_dsa_operation_counts "$audit" 0 0 0
  jq -e '
    .retrieval.prompt_assembly.dsa.called == true
    and .retrieval.prompt_assembly.dsa.status == "error"
    and .retrieval.prompt_assembly.dsa.error_code == "http_500"
  ' <<<"$trace" >/dev/null
  jq -e '
    ([.calls[] | select(
      .source == "targeted-sheet" and .operation == "google_values"
    )] | length) == 1
  ' <<<"$source_calls" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_claim_calibration_events "$diagnostics" "$request_id" 0
  assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  case "$(jq -c . <<<"$response")$(jq -c . <<<"$trace")" in
    *PRIVATE\ MALFORMED\ CELL\ SENTINEL*|*credentials*|*Traceback*)
      echo "malformed source diagnostics exposed private dependency data" >&2
      return 1
      ;;
  esac
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
  local owner client conversation_id question response request_id trace
  local provider_calls manifest diagnostics audit
  owner="owner-evidence-clarification"
  client="client-evidence-clarification"
  question="$EVIDENCE_EXHAUSTIVE_REVIEW_QUESTION"
  provider_post "/fixture/reset" '{}'
  restrict_dsa_config_to "complete_register.yaml"
  reset_source_fixture
  configure_source_fixture "complete-sheet" "empty_after_first"
  reset_dsa_audit
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-clarification")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" '{"enabled":true,"allowed_sensitivity":"medium"}')"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  jq -e '
    .status == "degraded"
    and .answer == "Which bounded source or source set should I examine?"
  ' <<<"$response" >/dev/null
  jq -e '
    .sufficiency.status == "unknown"
    and .next_steps.selections[0].selected_next_step == "ask_narrow_clarification"
    and .next_steps.selections[0].clarification_target == "source_scope"
  ' <<<"$manifest" >/dev/null
  assert_jq "clarification.additional_acquisition" "$manifest" \
    '.next_steps.additional_acquisition_count == 0'
  assert_jq "clarification.inventory" "$manifest" '
    .inventory.inventory_status == "complete_for_declared_scope"
    and .inventory.inventory_source_count == 1
    and .inventory.declared_source_count == 0
  '
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 0' <<<"$provider_calls" >/dev/null
  if ! assert_dsa_operation_counts "$audit" 1 1 0 >/dev/null 2>&1; then
    echo "Assertion failed: clarification.dsa" >&2
    return 1
  fi
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  configure_source_fixture "complete-sheet" "ready"
  restore_dsa_config
  echo "Evidence clarification: cr_selection=ask_narrow_clarification provider_chat=0 dsa_context_pack=1 dsa_context=1 dsa_fetch=0 additional_acquisition=0"
}

run_evidence_changed_premise_scenarios() {
  local owner client conversation_id question external response request_id trace
  local manifest provider_calls diagnostics audit source_calls
  owner="owner-evidence-followup"
  client="client-evidence-followup"
  question="Verify the follow-up records."
  external='{"enabled":true,"source_ids":["followup_records"],"allowed_sensitivity":"medium","max_results":8}'

  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  configure_source_fixture "followup-sheet" "alternating_large_compact"
  reset_dsa_audit
  restart_orchestrator_for_changed_premise
  queue_provider_answer "The exact follow-up record confirms the bounded detail."
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-followup")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external" "chat_local_fast")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  source_calls="$(fetch_source_fixture_calls)"
  jq -e '
    .router_decision.selected_model == "chat_local_fast"
    and .router_decision.routing_contract.manual_override_requested == "chat_local_fast"
    and .router_decision.routing_contract.manual_override_applied == true
    and .router_decision.routing_contract.manual_override_rejection_reason == null
    and .manual_override.requested_model == "chat_local_fast"
    and .manual_override.applied == true
    and .manual_override.rejection_reason == null
    and .retrieval.prompt_assembly.prompt_budget.effective_min_context_limit == 16000
    and .retrieval.prompt_assembly.prompt_budget.output_token_reserve == 14744
    and .retrieval.prompt_assembly.prompt_budget.context_safety_margin == 256
    and .retrieval.prompt_assembly.prompt_budget.effective_hard_input_budget == 1000
    and .retrieval.prompt_assembly.prompt_budget.profile_clamp.supplied == false
    and .retrieval.prompt_assembly.prompt_budget.profile_clamp.applied == false
  ' <<<"$trace" >/dev/null
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
    and .next_steps.initial_attempt.result_count == 2
    and .next_steps.initial_attempt.retained_reference_count == 0
    and .next_steps.initial_attempt.changed_premise_exact_fetch_followed == true
    and [.next_steps.selections[].selected_next_step] == ["perform_additional_acquisition","answer_within_declared_scope"]
    and .next_steps.selections[0].reacquisition_guard == "changed_premise_allowed"
    and .next_steps.selections[0].additional_acquisition_executed == true
  ' <<<"$manifest" >/dev/null
  if ! assert_dsa_operation_counts "$audit" 1 0 1 >/dev/null 2>&1; then
    echo "Assertion failed: changed_premise.initial.dsa" >&2
    return 1
  fi
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
  jq -e '.fallback.triggered == false and (.model_calls | length) == 1' \
    <<<"$trace" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 2 2 2
  assert_claim_calibration_events "$diagnostics" "$request_id" 0
  assert_persisted_answer_matches \
    "$conversation_id" "$request_id" "$(jq -r '.answer' <<<"$response")"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0

  reset_dsa_audit
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$question" "$external" "chat_local_fast")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  source_calls="$(fetch_source_fixture_calls)"
  jq -e '
    .router_decision.selected_model == "not_called"
    and .router_decision.provider == "none"
    and .router_decision.routing_contract.selected_model == "not_called"
    and .router_decision.routing_contract.selected_provider == "none"
    and .router_decision.routing_contract.manual_override_requested == "chat_local_fast"
    and .router_decision.routing_contract.manual_override_applied == true
    and .router_decision.routing_contract.manual_override_rejection_reason == null
    and .manual_override.requested_model == "chat_local_fast"
    and .manual_override.applied == true
    and .manual_override.rejection_reason == null
    and .retrieval.prompt_assembly.prompt_budget.effective_min_context_limit == 16000
    and .retrieval.prompt_assembly.prompt_budget.output_token_reserve == 14744
    and .retrieval.prompt_assembly.prompt_budget.context_safety_margin == 256
    and .retrieval.prompt_assembly.prompt_budget.effective_hard_input_budget == 1000
    and .retrieval.prompt_assembly.prompt_budget.profile_clamp.supplied == false
    and .retrieval.prompt_assembly.prompt_budget.profile_clamp.applied == false
    and .retrieval.prompt_assembly.prompt_budget.attempts[0].model == "chat_local_fast"
    and .retrieval.prompt_assembly.prompt_budget.attempts[0].provider == "local"
    and .retrieval.prompt_assembly.prompt_budget.attempts[0].max_context_tokens == 16000
    and .retrieval.prompt_assembly.prompt_budget.attempts[0].role == "primary"
    and .model_call.status == "not_called"
    and .model_calls == []
    and .fallback.triggered == false
  ' <<<"$trace" >/dev/null
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
  if ! assert_dsa_operation_counts "$audit" 1 0 0 >/dev/null 2>&1; then
    echo "Assertion failed: changed_premise.repeated.dsa" >&2
    return 1
  fi
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
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 2 1 1
  assert_claim_calibration_events "$diagnostics" "$request_id" 0
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  configure_source_fixture "followup-sheet" "ready"
  restart_orchestrator_with_reserve 2048
  docker compose -f "$COMPOSE" exec -T orchestrator /bin/sh -c '
    test "$ALLOW_MANUAL_OVERRIDE" = "false"
    test "$PROMPT_OUTPUT_TOKEN_RESERVE" = "2048"
  '
  echo "Evidence changed premise: model=chat_local_fast effective_budget=1000 targeted_results=2 targeted_retained=0 changed_premise_authorizations=1 exact_fetch=1 exact_retained=1 selections=2 provider=1 fixture_variants=large,compact,large repeated_targeted=1 repeated_guard=premise_already_attempted repeated_fetch=0 repeated_provider=0"
}

run_evidence_adversarial_provider_scenario() {
  local owner client conversation_id response request_id answer trace manifest
  local provider_calls diagnostics audit
  owner="owner-evidence-adversarial"
  client="client-evidence-adversarial"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  queue_provider_answer "Every possible source was fully examined, and no evidence exists outside this result."
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-adversarial")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." '{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}')"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  assert_jq "adversarial.scope.response_status" "$response" \
    '.status == "ok"'
  assert_jq "adversarial.scope.provider_overclaim_absent" "$response" '
    (.answer | contains("Every possible source was fully examined") | not)
    and (.answer | contains("no evidence exists outside this result") | not)
  '
  assert_jq "adversarial.scope.replacement" "$response" '
    ([.answer | scan("I withheld the generated answer because it claimed evidence coverage beyond the examined scope\\.")] | length) == 1
  '
  assert_jq "adversarial.scope.boundary" "$response" '
    .answer | endswith("This reflects only the targeted sources checked, not a complete search of every possible source.")
  '
  assert_jq "adversarial.scope.manifest" "$manifest" '
    .shape.task_shape == "targeted_lookup"
    and .acquisition.sources_considered == ["records_primary"]
    and .acquisition.sources_selected == ["records_primary"]
    and .sufficiency.status == "sufficient_for_declared_scope"
  '
  assert_jq "adversarial.scope.inventory" "$manifest" '
    .inventory.inventory_status == "complete_for_declared_scope"
    and .inventory.inventory_source_count == 6
    and .inventory.declared_source_count == 1
  '
  assert_jq "adversarial.scope.provider_calls" "$provider_calls" \
    '([.calls[] | select(.kind == "chat")] | length) == 1'
  if ! assert_dsa_operation_counts "$audit" 1 0 0 >/dev/null 2>&1; then
    echo "Assertion failed: adversarial.scope.dsa" >&2
    return 1
  fi
  if ! assert_evidence_runtime_events \
    "$diagnostics" "$request_id" 1 1 1 1 >/dev/null 2>&1; then
    echo "Assertion failed: adversarial.scope.runtime" >&2
    return 1
  fi
  if ! assert_claim_calibration_events \
    "$diagnostics" "$request_id" 0 >/dev/null 2>&1; then
    echo "Assertion failed: adversarial.scope.claim_calibration" >&2
    return 1
  fi
  assert_jq "adversarial.scope.dispatch" "$trace" \
    '.fallback.triggered == false and (.model_calls | length) == 1'
  if ! assert_persisted_answer_matches \
    "$conversation_id" "$request_id" "$answer" >/dev/null 2>&1; then
    echo "Assertion failed: adversarial.scope.persistence" >&2
    return 1
  fi
  if ! assert_request_persistence_counts \
    "$conversation_id" "$request_id" 0 >/dev/null 2>&1; then
    echo "Assertion failed: adversarial.scope.persistence" >&2
    return 1
  fi

  owner="owner-evidence-adversarial-negated"
  client="client-evidence-adversarial-negated"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  queue_provider_answer "Not every possible source was fully examined."
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-adversarial-negated")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." '{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}')"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  assert_jq "adversarial.negated.response_status" "$response" \
    '.status == "ok"'
  assert_jq "adversarial.negated.preserved" "$response" '
    ([.answer | scan("Not every possible source was fully examined\\.")] | length) == 1
  '
  assert_jq "adversarial.negated.no_replacement" "$response" '
    .answer
    | contains("I withheld the generated answer because it claimed evidence coverage beyond the examined scope.")
    | not
  '
  assert_jq "adversarial.negated.boundary" "$response" '
    .answer | endswith("This reflects only the targeted sources checked, not a complete search of every possible source.")
  '
  assert_jq "adversarial.negated.manifest" "$manifest" '
    .shape.task_shape == "targeted_lookup"
    and .acquisition.sources_considered == ["records_primary"]
    and .acquisition.sources_selected == ["records_primary"]
    and .sufficiency.status == "sufficient_for_declared_scope"
  '
  assert_jq "adversarial.negated.inventory" "$manifest" '
    .inventory.inventory_status == "complete_for_declared_scope"
    and .inventory.inventory_source_count == 6
    and .inventory.declared_source_count == 1
  '
  assert_jq "adversarial.negated.provider_calls" "$provider_calls" \
    '([.calls[] | select(.kind == "chat")] | length) == 1'
  if ! assert_dsa_operation_counts "$audit" 1 0 0 >/dev/null 2>&1; then
    echo "Assertion failed: adversarial.negated.dsa" >&2
    return 1
  fi
  if ! assert_evidence_runtime_events \
    "$diagnostics" "$request_id" 1 1 1 1 >/dev/null 2>&1; then
    echo "Assertion failed: adversarial.negated.runtime" >&2
    return 1
  fi
  if ! assert_claim_calibration_events \
    "$diagnostics" "$request_id" 0 >/dev/null 2>&1; then
    echo "Assertion failed: adversarial.negated.claim_calibration" >&2
    return 1
  fi
  assert_jq "adversarial.negated.dispatch" "$trace" \
    '.fallback.triggered == false and (.model_calls | length) == 1'
  if ! assert_persisted_answer_matches \
    "$conversation_id" "$request_id" "$answer" >/dev/null 2>&1; then
    echo "Assertion failed: adversarial.negated.persistence" >&2
    return 1
  fi
  if ! assert_request_persistence_counts \
    "$conversation_id" "$request_id" 0 >/dev/null 2>&1; then
    echo "Assertion failed: adversarial.negated.persistence" >&2
    return 1
  fi
  echo "Evidence adversarial provider: affirmative_replaced=1 negated_preserved=1 affirmative_provider=1 negated_provider=1 retry=0"
}

normalized_first_paragraph() {
  awk 'BEGIN { RS = "" } { gsub(/[[:space:]]+/, " "); print; exit }'
}

print_compound_claim_capture_state() {
  local case_name="$1" trace="$2" fields
  local enabled eligibility reason runtime_calls storage_calls
  fields="$(jq -r '
    def boolean_or_missing:
      if type == "boolean" then tostring else "missing" end;
    def label_or_missing:
      if type == "string"
        and length >= 1
        and length <= 120
        and test("^[A-Za-z0-9_.:-]{1,120}$")
      then . else "missing" end;
    def count_or_missing:
      if type == "number"
        and floor == .
        and . >= 0
        and . <= 4
      then tostring else "missing" end;
    (.prompt.claim_capture // {}) as $capture
    | [
        ($capture.enabled | boolean_or_missing),
        ($capture.eligibility_status | label_or_missing),
        ($capture.reason_code | label_or_missing),
        ($capture.runtime_call_count | count_or_missing),
        ($capture.storage_call_count | count_or_missing)
      ]
    | @tsv
  ' <<<"$trace")"
  IFS=$'\t' read -r \
    enabled eligibility reason runtime_calls storage_calls <<<"$fields"
  printf 'Compound claim-capture state: case=%s enabled=%s eligibility=%s reason=%s runtime_calls=%s storage_calls=%s\n' \
    "$case_name" "$enabled" "$eligibility" "$reason" \
    "$runtime_calls" "$storage_calls"
}

assert_jq() {
  local label="$1" json="$2" predicate="$3"
  shift 3
  if ! jq -e "$@" "$predicate" <<<"$json" >/dev/null 2>&1; then
    echo "Assertion failed: $label" >&2
    return 1
  fi
}

assert_pure_history() {
  local owner="$1" client="$2" conversation_id="$3" prior_answer="$4"
  local question="$5" expected_fragment="$6" scenario_label="$7"
  local messages response request_id trace
  local diagnostic_fields lookup_status resolution_status manifest_resolution_status
  local reason_code projection_status projection_reason selected_source_count serialized
  local generic_budget generic_candidate staged_budget staged_candidate
  local disclosure_fields
  messages="$(jq -nc \
    --arg answer "$prior_answer" \
    --arg question "$question" \
    '[{role:"assistant",content:$answer},{role:"user",content:$question}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  response="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  if [[ "$scenario_label" == "history.hybrid" || "$scenario_label" == "history.exhaustive" || "$scenario_label" == "history.unknown" ]]; then
    diagnostic_fields="$(jq -er '
      .prompt.claim_explanation as $explanation
      | [
          $explanation.lookup_status,
          $explanation.resolution_status,
          $explanation.manifest_resolution_status,
          $explanation.reason_code,
          $explanation.manifest_projection_status,
          $explanation.manifest_projection_reason
        ] as $labels
      | ($labels | map(
          if type == "string" then
            length >= 1
            and length <= 120
            and test("^[A-Za-z0-9_.:-]{1,120}$")
          else
            false
          end
        ) | all) as $labels_are_safe
      | if $labels_are_safe then
          $explanation.aggregate_counts.sources_selected as $count
          | ($labels + [
              if ($count | type) == "number" then
                if $count >= 0 and $count <= 64 and $count == ($count | floor) then
                  ($count | tostring)
                else
                  "missing"
                end
              else
                "missing"
              end
            ])
          | @tsv
        else
          empty
        end
    ' <<<"$trace")"
    IFS=$'\t' read -r lookup_status resolution_status \
      manifest_resolution_status reason_code projection_status projection_reason \
      selected_source_count \
      <<<"$diagnostic_fields"
    case "$scenario_label" in
      history.hybrid)
        if [[ "${EVIDENCE_SCENARIO:-all}" == "history-hybrid" || "$projection_status" != "accepted" ]]; then
          echo "Hybrid history safe state: lookup=$lookup_status resolution=$resolution_status manifest=$manifest_resolution_status reason=$reason_code projection_status=$projection_status projection_reason=$projection_reason selected_sources=$selected_source_count"
        fi
        ;;
      history.exhaustive)
        echo "Exhaustive history safe state: lookup=$lookup_status resolution=$resolution_status manifest=$manifest_resolution_status reason=$reason_code projection_status=$projection_status projection_reason=$projection_reason selected_sources=$selected_source_count"
        disclosure_fields="$(jq -r '[
          (.answer | contains("Acquisition was truncated by the retrieval budget.")),
          (.answer | contains("Candidate selection was truncated.")),
          (.answer | contains("The preliminary seed search was truncated, but the configured-scope expansion completed without truncation.")),
          (.answer | contains("Preliminary seed candidate selection was truncated."))
        ] | @tsv' <<<"$response")"
        IFS=$'\t' read -r generic_budget generic_candidate staged_budget \
          staged_candidate \
          <<<"$disclosure_fields"
        echo "Exhaustive history truncation disclosure: generic_budget=$generic_budget generic_candidate=$generic_candidate staged_budget=$staged_budget staged_candidate=$staged_candidate"
        assert_jq "history.exhaustive.truncation_stage" "$response" '
          (.answer | contains("The preliminary seed search was truncated, but the configured-scope expansion completed without truncation."))
          and (.answer | contains("Preliminary seed candidate selection was truncated."))
          and (.answer | contains("Acquisition was truncated by the retrieval budget.") | not)
          and (.answer | contains("Candidate selection was truncated.") | not)
        '
        ;;
      history.unknown)
        echo "Unknown history safe state: lookup=$lookup_status resolution=$resolution_status manifest=$manifest_resolution_status reason=$reason_code projection_status=$projection_status projection_reason=$projection_reason selected_sources=$selected_source_count"
        ;;
    esac
  fi
  assert_jq "${scenario_label}.response_fragment" "$response" \
    '.answer | contains($fragment)' --arg fragment "$expected_fragment"
  assert_jq "${scenario_label}.response_suffix" "$response" '
    .answer
    | endswith("I did not perform a new verification for this explanation.")
  '
  assert_jq "${scenario_label}.trace_target_mode" "$trace" '
    .prompt.claim_explanation.target_mode == "immediate_previous"
  '
  if ! assert_history_request_boundaries "$conversation_id" "$response" "resolved"; then
    echo "Assertion failed: ${scenario_label}.request_boundaries" >&2
    return 1
  fi
  serialized="$(jq -c . <<<"$response")$(jq -c '.prompt.claim_explanation' <<<"$trace")"
  case "$serialized" in
    *records_primary*|*complete_register*|*calendar_alpha*|*calendar_beta*|*google_sheets:*|*http://*|*PRIVATE*)
      echo "Assertion failed: ${scenario_label}.privacy_boundary" >&2
      return 1
      ;;
  esac
}

run_evidence_history_hybrid_scenario() {
  local owner client conversation_id external response request_id answer trace manifest
  local provider_calls diagnostics audit safe_fields response_status manifest_status
  local shape_status plan_status strategy_status sufficiency_status dependency_status
  local selection_count next_step model_status persistence_counts
  local assistant_count trace_count claim_count
  local detail_fields selected_count selected_expected used_count used_expected
  local expansion_attempts expansion_success item_count retained_count
  local dsa_truncated candidate_truncated
  owner="owner-history-hybrid"
  client="client-history-hybrid"
  external='{"enabled":true,"source_ids":["calendar_alpha","calendar_beta"],"allowed_sensitivity":"medium","max_results":2}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  queue_provider_answer "The selected calendars show bounded differences."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-hybrid")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$EVIDENCE_HYBRID_COMPARISON_QUESTION" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  safe_fields="$(jq -nr \
    --argjson response "$response" \
    --argjson manifest "$manifest" \
    --argjson trace "$trace" '
      def safe_label:
        if type == "string"
          and length >= 1
          and length <= 120
          and test("^[A-Za-z0-9_.:-]{1,120}$")
        then . else "missing" end;
      [
        ($response.status | safe_label),
        ($manifest.status | safe_label),
        ($manifest.shape.task_shape | safe_label),
        ($manifest.plan.plan_status | safe_label),
        ($manifest.acquisition.strategy_attempted | safe_label),
        ($manifest.sufficiency.status | safe_label),
        (if $manifest.next_steps.dependency_status == null
         then "none"
         else ($manifest.next_steps.dependency_status | safe_label)
         end),
        (if ($manifest.next_steps.selection_count | type) == "number"
          and $manifest.next_steps.selection_count >= 0
          and $manifest.next_steps.selection_count <= 2
          and $manifest.next_steps.selection_count
            == ($manifest.next_steps.selection_count | floor)
         then ($manifest.next_steps.selection_count | tostring)
         else "missing"
         end),
        ($manifest.next_steps.selections[-1].selected_next_step | safe_label),
        ($trace.model_call.status | safe_label)
      ] | @tsv
    ')"
  IFS=$'\t' read -r response_status manifest_status shape_status plan_status \
    strategy_status sufficiency_status dependency_status selection_count \
    next_step model_status <<<"$safe_fields"
  echo "Hybrid acquisition safe state: response=$response_status manifest=$manifest_status shape=$shape_status plan=$plan_status strategy=$strategy_status sufficiency=$sufficiency_status dependency=$dependency_status selections=$selection_count next=$next_step model=$model_status"

  assert_jq "history.hybrid.original.response_status" "$response" \
    '.status == "ok"'
  assert_jq "history.hybrid.original.manifest_status" "$manifest" \
    '.status == "sufficient_for_declared_scope"'
  assert_jq "history.hybrid.original.shape" "$manifest" '
    .shape.derivation_status == "derived"
    and .shape.task_shape == "cross_source_comparison"
    and .shape.clarification_required == false
  '
  assert_jq "history.hybrid.original.plan" "$manifest" '
    .plan.plan_status == "ready"
    and .plan.selected_strategies == ["hybrid"]
    and .plan.completeness_expectation == "complete_for_selected_sources"
    and .plan.contradiction_search_required == false
  '
  assert_jq "history.hybrid.original.acquisition" "$manifest" '
    .acquisition.strategy_attempted == "hybrid"
    and .acquisition.expansion_attempt_count == 2
    and .acquisition.expansion_successful_count == 2
    and .acquisition.sources_selected == ["calendar_alpha", "calendar_beta"]
    and .acquisition.sources_used == ["calendar_alpha", "calendar_beta"]
    and .acquisition.prompt_retained_item_count >= 2
  '
  assert_jq "history.hybrid.original.sufficiency" "$manifest" '
    .sufficiency.status == "sufficient_for_declared_scope"
    and .sufficiency.qualification_required == false
    and .sufficiency.additional_acquisition_required == false
  '
  assert_jq "history.hybrid.original.next_step" "$manifest" '
    .next_steps.selection_count == 1
    and .next_steps.additional_acquisition_count == 0
    and .next_steps.dependency_status == null
    and .next_steps.selections[0].selected_next_step
      == "answer_within_declared_scope"
    and .next_steps.selections[0].provider_disposition == "allowed"
    and .next_steps.selections[0].reacquisition_guard == "not_applicable"
    and .next_steps.selections[0].additional_acquisition_executed == false
  '
  assert_jq "history.hybrid.original.provider" "$provider_calls" \
    '([.calls[] | select(.kind == "chat")] | length) == 1'
  assert_jq "history.hybrid.original.model" "$trace" '
    .model_call.status == "ok"
    and (.model_calls | length) == 1
    and .fallback.triggered == false
  '
  assert_jq "history.hybrid.original.runtime" "$diagnostics" '
    ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_shape_derived"
    )] | length) == 1
    and ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_plan_compiled"
    )] | length) == 1
    and ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_sufficiency_evaluated"
    )] | length) == 1
    and ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_next_step_selected"
    )] | length) == 1
  ' --arg request_id "$request_id"
  assert_jq "history.hybrid.original.dsa" "$audit" '
    ([.[] | select(.operation == "context_pack")] | length) == 1
    and ([.[] | select(.operation == "context")] | length) == 2
    and ([.[] | select(.operation == "fetch")] | length) == 0
  '
  assistant_count="$(psql_exec -At -c "SELECT count(*) FROM messages WHERE conversation_id = '$conversation_id' AND role = 'assistant' AND metadata->>'request_id' = '$request_id';")"
  trace_count="$(psql_exec -At -c "SELECT count(*) FROM traces WHERE conversation_id = '$conversation_id' AND request_id = '$request_id';")"
  claim_count="$(psql_exec -At -c "SELECT count(*) FROM claim_records WHERE conversation_id = '$conversation_id' AND request_id = '$request_id';")"
  persistence_counts="$(jq -nc \
    --arg assistant "$assistant_count" \
    --arg trace "$trace_count" \
    --arg claims "$claim_count" \
    '{assistant:$assistant,trace:$trace,claims:$claims}')"
  assert_jq "history.hybrid.original.persistence" "$persistence_counts" '
    .assistant == "1" and .trace == "1" and .claims == "0"
  '
  assert_pure_history "$owner" "$client" "$conversation_id" "$answer" \
    "What did you examine?" "bounded comparison across 2 selected configured sources" \
    "history.hybrid"
}

run_evidence_history_exhaustive_scenario() {
  local owner client conversation_id external response request_id answer trace manifest
  local provider_calls diagnostics audit safe_fields response_status manifest_status
  local shape_status plan_status strategy_status sufficiency_status dependency_status
  local selection_count next_step model_status persistence_counts
  local assistant_count trace_count claim_count
  local detail_fields selected_count selected_expected used_count used_expected
  local expansion_attempts expansion_success expansion_truncated item_count
  local retained_count aggregate_budget_truncated search_budget_truncated
  local expansion_budget_truncated candidate_truncated
  owner="owner-history-exhaustive"
  client="client-history-exhaustive"
  external='{"enabled":true,"source_ids":["complete_register"],"allowed_sensitivity":"medium","max_results":1}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  queue_provider_answer "The configured register shows every mandatory entry was reviewed."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-exhaustive")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "$EVIDENCE_EXHAUSTIVE_REVIEW_QUESTION" "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  safe_fields="$(jq -nr \
    --argjson response "$response" \
    --argjson manifest "$manifest" \
    --argjson trace "$trace" '
      def safe_label:
        if type == "string"
          and length >= 1
          and length <= 120
          and test("^[A-Za-z0-9_.:-]{1,120}$")
        then . else "missing" end;
      [
        ($response.status | safe_label),
        ($manifest.status | safe_label),
        ($manifest.shape.task_shape | safe_label),
        ($manifest.plan.plan_status | safe_label),
        ($manifest.acquisition.strategy_attempted | safe_label),
        ($manifest.sufficiency.status | safe_label),
        (if $manifest.next_steps.dependency_status == null
         then "none"
         else ($manifest.next_steps.dependency_status | safe_label)
         end),
        (if ($manifest.next_steps.selection_count | type) == "number"
          and $manifest.next_steps.selection_count >= 0
          and $manifest.next_steps.selection_count <= 2
          and $manifest.next_steps.selection_count
            == ($manifest.next_steps.selection_count | floor)
         then ($manifest.next_steps.selection_count | tostring)
         else "missing"
         end),
        ($manifest.next_steps.selections[-1].selected_next_step | safe_label),
        ($trace.model_call.status | safe_label)
      ] | @tsv
    ')"
  IFS=$'\t' read -r response_status manifest_status shape_status plan_status \
    strategy_status sufficiency_status dependency_status selection_count \
    next_step model_status <<<"$safe_fields"
  echo "Exhaustive acquisition safe state: response=$response_status manifest=$manifest_status shape=$shape_status plan=$plan_status strategy=$strategy_status sufficiency=$sufficiency_status dependency=$dependency_status selections=$selection_count next=$next_step model=$model_status"
  detail_fields="$(jq -nr \
    --argjson manifest "$manifest" \
    --argjson trace "$trace" '
    def bounded_integer($maximum):
      if type == "number"
        and . >= 0
        and . <= $maximum
        and . == floor
      then tostring else "missing" end;
    def bounded_boolean:
      if type == "boolean" then tostring else "missing" end;
    [
      (if ($manifest.acquisition.sources_selected | type) == "array"
       then ($manifest.acquisition.sources_selected | length | bounded_integer(64))
       else "missing"
       end),
      ($manifest.acquisition.sources_selected == ["complete_register"] | tostring),
      (if ($manifest.acquisition.sources_used | type) == "array"
       then ($manifest.acquisition.sources_used | length | bounded_integer(64))
       else "missing"
       end),
      ($manifest.acquisition.sources_used == ["complete_register"] | tostring),
      ($manifest.acquisition.expansion_attempt_count | bounded_integer(16)),
      ($manifest.acquisition.expansion_successful_count | bounded_integer(16)),
      ($manifest.acquisition.expansion_truncated_count | bounded_integer(16)),
      ($manifest.acquisition.item_count | bounded_integer(10000)),
      ($manifest.acquisition.prompt_retained_item_count | bounded_integer(10000)),
      ($manifest.acquisition.dsa_budget_truncation | bounded_boolean),
      ($trace.retrieval.prompt_assembly.dsa.search_budget_truncated
        | bounded_boolean),
      ($trace.retrieval.prompt_assembly.dsa.expansion_budget_truncated
        | bounded_boolean),
      ($manifest.acquisition.candidate_truncation | bounded_boolean)
    ] | @tsv
  ')"
  IFS=$'\t' read -r selected_count selected_expected used_count used_expected \
    expansion_attempts expansion_success expansion_truncated item_count \
    retained_count aggregate_budget_truncated search_budget_truncated \
    expansion_budget_truncated candidate_truncated <<<"$detail_fields"
  echo "Exhaustive acquisition details: selected_count=$selected_count selected_expected=$selected_expected used_count=$used_count used_expected=$used_expected expansion_attempts=$expansion_attempts expansion_success=$expansion_success expansion_truncated=$expansion_truncated item_count=$item_count retained_count=$retained_count aggregate_budget_truncated=$aggregate_budget_truncated search_budget_truncated=$search_budget_truncated expansion_budget_truncated=$expansion_budget_truncated candidate_truncated=$candidate_truncated"

  assert_jq "history.exhaustive.original.response_status" "$response" \
    '.status == "ok"'
  assert_jq "history.exhaustive.original.manifest_status" "$manifest" \
    '.status == "sufficient_for_declared_scope"'
  assert_jq "history.exhaustive.original.shape" "$manifest" '
    .shape.derivation_status == "derived"
    and .shape.task_shape == "bounded_exhaustive_review"
    and .shape.clarification_required == false
  '
  assert_jq "history.exhaustive.original.plan" "$manifest" '
    .plan.plan_status == "ready"
    and .plan.selected_strategies == ["hybrid"]
    and .plan.completeness_expectation == "complete_for_declared_scope"
    and .plan.contradiction_search_required == true
  '
  assert_jq "history.exhaustive.original.strategy" "$manifest" \
    '.acquisition.strategy_attempted == "hybrid"'
  assert_jq "history.exhaustive.original.selected_sources" "$manifest" \
    '.acquisition.sources_selected == ["complete_register"]'
  assert_jq "history.exhaustive.original.used_sources" "$manifest" \
    '.acquisition.sources_used == ["complete_register"]'
  assert_jq "history.exhaustive.original.expansion_counts" "$manifest" '
    .acquisition.expansion_attempt_count == 1
    and .acquisition.expansion_successful_count == 1
  '
  assert_jq "history.exhaustive.original.item_counts" "$manifest" '
    .acquisition.item_count == 1
    and .acquisition.prompt_retained_item_count == 1
  '
  assert_jq "history.exhaustive.original.aggregate_truncation" "$manifest" '
    .acquisition.dsa_budget_truncation == true
    and .acquisition.candidate_truncation == true
  '
  assert_jq "history.exhaustive.original.search_truncation" "$trace" '
    .retrieval.prompt_assembly.dsa.search_budget_truncated == true
    and .retrieval.prompt_assembly.dsa.candidate_truncated == true
  '
  assert_jq "history.exhaustive.original.expansion_complete" "$trace" '
    .retrieval.prompt_assembly.dsa.expansion_budget_truncated == false
    and .retrieval.prompt_assembly.dsa.context_expansion_call_count == 1
    and .retrieval.prompt_assembly.dsa.final_combined_item_count == 1
  '
  assert_jq "history.exhaustive.original.expansion_outcome" "$manifest" '
    .acquisition.expansion_attempt_count == 1
    and .acquisition.expansion_successful_count == 1
    and .acquisition.expansion_truncated_count == 0
    and .acquisition.prompt_retained_item_count == 1
  '
  assert_jq "history.exhaustive.original.sufficiency" "$manifest" '
    .sufficiency.status == "sufficient_for_declared_scope"
    and .sufficiency.qualification_required == false
    and .sufficiency.additional_acquisition_required == false
  '
  assert_jq "history.exhaustive.original.next_step" "$manifest" '
    .next_steps.selection_count == 1
    and .next_steps.additional_acquisition_count == 0
    and .next_steps.dependency_status == null
    and .next_steps.selections[0].selected_next_step
      == "answer_within_declared_scope"
    and .next_steps.selections[0].provider_disposition == "allowed"
    and .next_steps.selections[0].reacquisition_guard == "not_applicable"
    and .next_steps.selections[0].additional_acquisition_executed == false
  '
  assert_jq "history.exhaustive.original.provider" "$provider_calls" \
    '([.calls[] | select(.kind == "chat")] | length) == 1'
  assert_jq "history.exhaustive.original.model" "$trace" '
    .model_call.status == "ok"
    and (.model_calls | length) == 1
    and .fallback.triggered == false
  '
  assert_jq "history.exhaustive.original.runtime" "$diagnostics" '
    ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_shape_derived"
    )] | length) == 1
    and ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_plan_compiled"
    )] | length) == 1
    and ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_sufficiency_evaluated"
    )] | length) == 1
    and ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_next_step_selected"
    )] | length) == 1
  ' --arg request_id "$request_id"
  assert_jq "history.exhaustive.original.dsa" "$audit" '
    ([.[] | select(.operation == "context_pack")] | length) == 1
    and ([.[] | select(.operation == "context")] | length) == 1
    and ([.[] | select(.operation == "fetch")] | length) == 0
  '
  assistant_count="$(psql_exec -At -c "SELECT count(*) FROM messages WHERE conversation_id = '$conversation_id' AND role = 'assistant' AND metadata->>'request_id' = '$request_id';")"
  trace_count="$(psql_exec -At -c "SELECT count(*) FROM traces WHERE conversation_id = '$conversation_id' AND request_id = '$request_id';")"
  claim_count="$(psql_exec -At -c "SELECT count(*) FROM claim_records WHERE conversation_id = '$conversation_id' AND request_id = '$request_id';")"
  persistence_counts="$(jq -nc \
    --arg assistant "$assistant_count" \
    --arg trace "$trace_count" \
    --arg claims "$claim_count" \
    '{assistant:$assistant,trace:$trace,claims:$claims}')"
  assert_jq "history.exhaustive.original.persistence" "$persistence_counts" '
    .assistant == "1" and .trace == "1" and .claims == "0"
  '
  assert_pure_history "$owner" "$client" "$conversation_id" "$answer" \
    "Did you look at everything relevant?" "Within the declared bounded scope, yes." \
    "history.exhaustive"
}

run_evidence_history_unknown_scenario() {
  local owner client conversation_id external response request_id answer trace manifest
  local provider_calls diagnostics audit safe_fields response_status manifest_status
  local shape_status plan_status strategy_status sufficiency_status dependency_status
  local selection_count next_step model_status persistence_counts
  local assistant_count trace_count claim_count
  local sufficiency_flags qualification_required additional_acquisition_required
  owner="owner-history-unknown"
  client="client-history-unknown"
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  conversation_id="$(resolve_conversation "$owner" "$client" "history-unknown")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the zephyr artifact." "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  safe_fields="$(jq -nr \
    --argjson response "$response" \
    --argjson manifest "$manifest" \
    --argjson trace "$trace" '
      def safe_label:
        if type == "string"
          and length >= 1
          and length <= 120
          and test("^[A-Za-z0-9_.:-]{1,120}$")
        then . else "missing" end;
      [
        ($response.status | safe_label),
        ($manifest.status | safe_label),
        ($manifest.shape.task_shape | safe_label),
        ($manifest.plan.plan_status | safe_label),
        ($manifest.acquisition.strategy_attempted | safe_label),
        ($manifest.sufficiency.status | safe_label),
        (if $manifest.next_steps.dependency_status == null
         then "none"
         else ($manifest.next_steps.dependency_status | safe_label)
         end),
        (if ($manifest.next_steps.selection_count | type) == "number"
          and $manifest.next_steps.selection_count >= 0
          and $manifest.next_steps.selection_count <= 2
          and $manifest.next_steps.selection_count
            == ($manifest.next_steps.selection_count | floor)
         then ($manifest.next_steps.selection_count | tostring)
         else "missing"
         end),
        ($manifest.next_steps.selections[-1].selected_next_step | safe_label),
        ($trace.model_call.status | safe_label)
      ] | @tsv
    ')"
  IFS=$'\t' read -r response_status manifest_status shape_status plan_status \
    strategy_status sufficiency_status dependency_status selection_count \
    next_step model_status <<<"$safe_fields"
  echo "Unknown acquisition safe state: response=$response_status manifest=$manifest_status shape=$shape_status plan=$plan_status strategy=$strategy_status sufficiency=$sufficiency_status dependency=$dependency_status selections=$selection_count next=$next_step model=$model_status"
  sufficiency_flags="$(jq -r '
    def bounded_boolean:
      if type == "boolean" then tostring else "missing" end;
    [
      (.sufficiency.qualification_required | bounded_boolean),
      (.sufficiency.additional_acquisition_required | bounded_boolean)
    ] | @tsv
  ' <<<"$manifest")"
  IFS=$'\t' read -r qualification_required additional_acquisition_required \
    <<<"$sufficiency_flags"
  echo "Unknown sufficiency flags: qualification_required=$qualification_required additional_acquisition_required=$additional_acquisition_required"

  assert_jq "history.unknown.original.response_status" "$response" \
    '.status == "degraded"'
  assert_jq "history.unknown.original.manifest_status" "$manifest" \
    '.status == "unknown"'
  assert_jq "history.unknown.original.shape" "$manifest" '
    .shape.derivation_status == "derived"
    and .shape.task_shape == "targeted_lookup"
    and .shape.clarification_required == false
  '
  assert_jq "history.unknown.original.plan" "$manifest" '
    .plan.plan_status == "ready"
    and .plan.selected_strategies == ["targeted_retrieval"]
    and .plan.completeness_expectation == "targeted_scope"
  '
  assert_jq "history.unknown.original.acquisition" "$manifest" '
    .acquisition.strategy_attempted == "targeted_retrieval"
    and .acquisition.sources_considered == ["records_primary"]
    and .acquisition.sources_selected == ["records_primary"]
    and .acquisition.item_count == 0
    and .acquisition.prompt_retained_item_count == 0
  '
  assert_jq "history.unknown.original.sufficiency_status" "$manifest" \
    '.sufficiency.status == "unknown"'
  assert_jq "history.unknown.original.qualification_required" "$manifest" \
    '.sufficiency.qualification_required == true'
  assert_jq "history.unknown.original.additional_acquisition_required" "$manifest" \
    '.sufficiency.additional_acquisition_required == true'
  assert_jq "history.unknown.original.next_step" "$manifest" '
    .next_steps.selection_count == 1
    and .next_steps.additional_acquisition_count == 0
    and .next_steps.dependency_status == null
    and .next_steps.selections[0].selected_next_step
      == "withhold_unsupported_conclusion"
    and .next_steps.selections[0].provider_disposition == "blocked"
    and .next_steps.selections[0].additional_acquisition_executed == false
  '
  assert_jq "history.unknown.original.provider" "$provider_calls" \
    '([.calls[] | select(.kind == "chat")] | length) == 0'
  assert_jq "history.unknown.original.model" "$trace" '
    .model_call.status == "not_called"
    and .model_calls == []
    and .fallback.triggered == false
  '
  assert_jq "history.unknown.original.runtime" "$diagnostics" '
    ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_shape_derived"
    )] | length) == 1
    and ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_plan_compiled"
    )] | length) == 1
    and ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_sufficiency_evaluated"
    )] | length) == 1
    and ([.events[] | select(
      .event_payload_json.request_id == $request_id
      and .event_type == "evidence_next_step_selected"
    )] | length) == 1
  ' --arg request_id "$request_id"
  assert_jq "history.unknown.original.dsa" "$audit" '
    ([.[] | select(.operation == "context_pack")] | length) == 1
    and ([.[] | select(.operation == "context")] | length) == 0
    and ([.[] | select(.operation == "fetch")] | length) == 0
  '
  assistant_count="$(psql_exec -At -c "SELECT count(*) FROM messages WHERE conversation_id = '$conversation_id' AND role = 'assistant' AND metadata->>'request_id' = '$request_id';")"
  trace_count="$(psql_exec -At -c "SELECT count(*) FROM traces WHERE conversation_id = '$conversation_id' AND request_id = '$request_id';")"
  claim_count="$(psql_exec -At -c "SELECT count(*) FROM claim_records WHERE conversation_id = '$conversation_id' AND request_id = '$request_id';")"
  persistence_counts="$(jq -nc \
    --arg assistant "$assistant_count" \
    --arg trace "$trace_count" \
    --arg claims "$claim_count" \
    '{assistant:$assistant,trace:$trace,claims:$claims}')"
  assert_jq "history.unknown.original.persistence" "$persistence_counts" '
    .assistant == "1" and .trace == "1" and .claims == "0"
  '
  assert_pure_history "$owner" "$client" "$conversation_id" "$answer" \
    "What did you not check?" \
    "The record left evidence sufficiency unknown, so the requested conclusion was not established." \
    "history.unknown"
}

run_evidence_history_scenarios() {
  local owner client conversation_id external response request_id answer first_paragraph
  local messages history history_request trace newer newer_request newer_answer
  local original_manifest_id newer_manifest_id

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
    "What did you check?" "retained record shows a targeted lookup" \
    "history.targeted"

  owner="owner-history-exact"
  client="client-history-exact"
  external='{"enabled":true,"source_ids":["records_primary"],"exact_source_refs":[{"source_id":"records_primary","source_ref":"google_sheets:records_primary:Records!A2:C2"}],"allowed_sensitivity":"medium"}'
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The exact migration record supports the bounded setting."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-exact")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the exact migration record." "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  original_manifest_id="$(fetch_trace "$request_id" | jq -r '.prompt.evidence_acquisition.manifest_id')"
  assert_request_persistence_counts "$conversation_id" "$request_id" 0
  first_paragraph="$(printf '%s' "$answer" | normalized_first_paragraph)"
  queue_provider_answer "A newer bounded migration response is available."
  newer="$(run_evidence_chat \
    "$owner" "$client" "$conversation_id" "Verify the migration record." \
    '{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}')"
  newer_request="$(jq -r '.request_id' <<<"$newer")"
  newer_answer="$(jq -r '.answer' <<<"$newer")"
  newer_manifest_id="$(fetch_trace "$newer_request" | jq -r '.prompt.evidence_acquisition.manifest_id')"
  test "$request_id" != "$newer_request"
  test "$original_manifest_id" != "$newer_manifest_id"
  test "$answer" != "$newer_answer"
  assert_request_persistence_counts "$conversation_id" "$newer_request" 0
  messages="$(jq -nc \
    --arg answer "$newer_answer" \
    --arg target "$first_paragraph" '
    [{role:"assistant",content:$answer},{role:"user",content:("What did you check for the statement \"" + $target + "\"?")}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  history_request="$(jq -r '.request_id' <<<"$history")"
  trace="$(fetch_trace "$history_request")"
  jq -e '
    .status == "ok"
    and (.answer | contains("an exact fetch for 1 specified reference."))
    and (.answer | endswith("I did not perform a new verification for this explanation."))
  ' <<<"$history" >/dev/null
  jq -e '
    .prompt.claim_explanation.target_mode == "quoted_first_paragraph"
    and .prompt.claim_explanation.manifest_resolution_status == "resolved"
    and .prompt.claim_explanation.storage_call_count == 1
  ' <<<"$trace" >/dev/null
  assert_history_request_boundaries "$conversation_id" "$history" "resolved"

  run_evidence_history_hybrid_scenario
  run_evidence_history_exhaustive_scenario

  owner="owner-history-limited"
  client="client-history-limited"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The available migration record supports the bounded setting."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-limited")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." '{"enabled":true,"allowed_sensitivity":"medium"}')"
  answer="$(jq -r '.answer' <<<"$response")"
  assert_pure_history "$owner" "$client" "$conversation_id" "$answer" \
    "What might you have missed?" "sufficient only with recorded limitations" \
    "history.limited"

  run_evidence_history_unknown_scenario
  echo "Evidence history: targeted=resolved exact_quoted=resolved hybrid=resolved exhaustive=resolved limited=resolved unknown=resolved provider=0 dsa=0 cr_evidence=0"
}

run_evidence_privacy_history_scenario() {
  local owner client conversation_id external response request_id answer trace manifest
  local serialized
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
    "What did you check?" "retained record shows a targeted lookup" \
    "history.private"
  serialized="$(jq -c . <<<"$response")$(jq -c . <<<"$manifest")$(jq -c . <<<"$trace")$(jq -c . <<<"$HISTORY_RESPONSE")$(jq -c . <<<"$HISTORY_TRACE")"
  case "$serialized" in
    *records_primary*|*google_sheets:*|*targeted-sheet*|*fixture_google*|*http://source-fixture*|*The\ migration\ record\ confirms*|*PRIVATE\ SOURCE\ DETAIL*)
      echo "privacy-suppressed history or persisted trace exposed source data" >&2
      return 1
      ;;
  esac
  restart_orchestrator_with_privacy false
  docker compose -f "$COMPOSE" exec -T orchestrator /bin/sh -c '
    test "$COGNITIVE_RUNTIME_PRIVACY_CONTEXT_ENABLED" = "false"
  '
  echo "Evidence privacy history: suppressed_source_count=1 suppressed_reference_count=2 reconstructed_identifiers=0"
}

run_evidence_claim_subset_scenario() {
  local owner client conversation_id source_message_id derived_id response request_id
  local answer trace manifest claims claim_digest response_digest association_count
  local provider_calls diagnostics audit manifest_id provider_sentinel
  local rejected_claim_id rejected_manifest_id rejected_body rejected_response
  local rejected_status rejected_count valid_count claims_after
  owner="owner-evidence-claim"
  client="client-evidence-claim"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  reset_dsa_audit
  conversation_id="$(resolve_conversation "$owner" "$client" "evidence-claim")"
  source_message_id="$(add_message "$conversation_id" "$owner" "$client" "user" "The setting is active in the retained file.")"
  derived_id="$(seed_derived \
    "$conversation_id" "$owner" "$client" "$source_message_id" \
    "The setting is active in the retained file." "active" "006" "active")"
  provider_sentinel="provider-manifest-sentinel"
  queue_provider_answer "The retained file reports that the setting is active with $provider_sentinel."
  response="$(run_evidence_chat_with_artifacts \
    "$owner" "$client" "$conversation_id" \
    "What do the retained file and migration records report about the setting?" \
    '{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium","max_results":5}')"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  manifest_id="$(jq -r '.manifest_id' <<<"$manifest")"
  claims="$(list_claim_records "$owner" "$conversation_id")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  jq -e \
    --arg request_id "$request_id" \
    --arg derived_id "$derived_id" \
    --arg manifest_id "$manifest_id" \
    --arg provider_sentinel "$provider_sentinel" '
      (.records | length) == 1
      and .records[0].request_id == $request_id
      and .records[0].acquisition_manifest_id == $manifest_id
      and (.records[0].acquisition_manifest_id | contains($provider_sentinel) | not)
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
  test "$manifest_id" != "$provider_sentinel"
  case "$(jq -c . <<<"$manifest")" in
    *provider-manifest-sentinel*)
      echo "provider text influenced the retained acquisition manifest" >&2
      return 1
      ;;
  esac
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
  assert_dsa_operation_counts "$audit" 1 0 0
  jq -e '([.calls[] | select(.kind == "chat")] | length) == 1' \
    <<<"$provider_calls" >/dev/null
  jq -e '.fallback.triggered == false and (.model_calls | length) == 1' \
    <<<"$trace" >/dev/null
  assert_evidence_runtime_events "$diagnostics" "$request_id" 1 1 1 1
  assert_claim_calibration_events "$diagnostics" "$request_id" 1
  assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
  assert_request_persistence_counts "$conversation_id" "$request_id" 1

  rejected_claim_id="claim_invalid_manifest_association"
  rejected_manifest_id="manifest_invalid_association_0000000000000001"
  rejected_body="$(jq -c \
    --arg claim_id "$rejected_claim_id" \
    --arg manifest_id "$rejected_manifest_id" '
      .records[0] as $record
      | {
          schema_version: $record.schema_version,
          request_id: $record.request_id,
          owner_id: $record.owner_id,
          conversation_id: $record.conversation_id,
          assistant_message_id: $record.assistant_message_id,
          surface: $record.surface,
          runtime_session_id: $record.runtime_session_id,
          runtime_turn_id: $record.runtime_turn_id,
          acquisition_manifest_id: $manifest_id,
          calibration_result: {
            claim_id: $claim_id,
            claim_anchor: $record.claim_anchor,
            claim_anchor_digest: $record.claim_anchor_digest,
            claim_class: $record.claim_class,
            calibration_status: $record.calibration_status,
            evidence_strength: $record.evidence_strength,
            confidence: $record.confidence,
            strongest_authority: $record.strongest_authority,
            freshness_summary: $record.freshness_summary,
            uncertainty_disclosure_required: $record.uncertainty_disclosure_required,
            validated_evidence_references: $record.validated_evidence_references,
            limitation_codes: $record.limitation_codes,
            user_safe_summary: $record.user_safe_summary
          }
        }
    ' <<<"$claims")"
  rejected_response="$(mktemp)"
  rejected_status="$(curl -sS -o "$rejected_response" -w '%{http_code}' \
    -X POST "http://127.0.0.1:14321/v1/internal/claim-records" \
    -H "X-API-Key: smoke-memory-key" \
    -H "X-Request-ID: $request_id" \
    -H "Content-Type: application/json" \
    -d "$rejected_body")"
  test "$rejected_status" = "422"
  jq -e '
    keys == ["detail"]
    and .detail == "acquisition_manifest_association_mismatch"
  ' "$rejected_response" >/dev/null
  case "$(cat "$rejected_response")" in
    *provider-manifest-sentinel*|*The\ retained\ file*|*derived_text*|*prompt*|*credential*|*PRIVATE*)
      echo "invalid claim association response exposed private data" >&2
      return 1
      ;;
  esac
  rm -f "$rejected_response"
  claims_after="$(list_claim_records "$owner" "$conversation_id")"
  jq -e \
    --arg valid_claim_id "$(jq -r '.records[0].claim_id' <<<"$claims")" \
    --arg rejected_claim_id "$rejected_claim_id" '
      (.records | length) == 1
      and .records[0].claim_id == $valid_claim_id
      and ([.records[] | select(.claim_id == $rejected_claim_id)] | length) == 0
    ' <<<"$claims_after" >/dev/null
  rejected_count="$(psql_exec -At -c \
    "SELECT count(*) FROM claim_records WHERE claim_id = '$rejected_claim_id';")"
  valid_count="$(psql_exec -At -c \
    "SELECT count(*) FROM claim_records WHERE request_id = '$request_id';")"
  test "$rejected_count" = "0"
  test "$valid_count" = "1"
  echo "Evidence claim subset: acquired_external_items=2 validated_claim_support=1 manifest_link=1 claim_digest_distinct_from_response_digest=1 durable_association=1"
}

run_evidence_history_negative_scenarios() {
  local owner client conversation_id external response answer messages history request_id trace
  local target sentinel newer newer_answer original_request same_owner_conversation
  local corrupt_count privacy_invalid_count
  external='{"enabled":true,"source_ids":["records_primary"],"allowed_sensitivity":"medium"}'

  owner="owner-history-mismatch"
  client="client-history-mismatch"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The retained migration record supports the bounded setting."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-mismatch")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  original_request="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  queue_provider_answer "A newer persisted bounded response is available."
  newer="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  newer_answer="$(jq -r '.answer' <<<"$newer")"
  if [[ "$answer" == "$newer_answer" ]]; then
    echo "Assertion failed: history.negatives.immediate.answers_differ" >&2
    return 1
  fi
  if ! assert_request_persistence_counts \
    "$conversation_id" "$(jq -r '.request_id' <<<"$newer")" 0; then
    echo "Assertion failed: history.negatives.immediate.newer_persistence" >&2
    return 1
  fi
  messages="$(jq -nc --arg answer "$answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check?"}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  assert_jq "history.negatives.immediate.response_status" "$history" \
    '.status == "degraded"'
  assert_jq "history.negatives.immediate.response_wording" "$history" \
    '.answer | contains($expected)' \
    --arg expected "$EVIDENCE_HISTORY_NO_RECORD_SENTENCE"
  assert_jq "history.negatives.immediate.response_suffix" "$history" \
    '.answer | endswith($suffix)' \
    --arg suffix "$EVIDENCE_HISTORY_NO_NEW_VERIFICATION_SENTENCE"
  if ! assert_history_request_boundaries \
    "$conversation_id" "$history" "no_record"; then
    echo "Assertion failed: history.negatives.immediate.boundaries" >&2
    return 1
  fi
  case "$(jq -c . <<<"$history")$(jq -c . <<<"$HISTORY_TRACE")" in
    *"$original_request"*|*records_primary*|*The\ retained\ migration\ record*)
      echo "Assertion failed: history.negatives.immediate.no_backward_scan" >&2
      return 1
      ;;
  esac
  echo "History negative case passed: immediate"

  target="A quoted acquisition statement that was never persisted."
  messages="$(jq -nc --arg answer "$newer_answer" --arg target "$target" '
    [{role:"assistant",content:$answer},{role:"user",content:("What did you check for the statement \"" + $target + "\"?")}]
  ')"
  reset_dsa_audit
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  assert_jq "history.negatives.quoted_not_found.response_status" "$history" \
    '.status == "degraded"'
  assert_jq "history.negatives.quoted_not_found.response_wording" "$history" \
    '.answer | contains($expected)' \
    --arg expected "$EVIDENCE_HISTORY_NO_RECORD_SENTENCE"
  assert_jq "history.negatives.quoted_not_found.response_suffix" "$history" \
    '.answer | endswith($suffix)' \
    --arg suffix "$EVIDENCE_HISTORY_NO_NEW_VERIFICATION_SENTENCE"
  if ! assert_history_request_boundaries \
    "$conversation_id" "$history" "no_record"; then
    echo "Assertion failed: history.negatives.quoted_not_found.boundaries" >&2
    return 1
  fi
  echo "History negative case passed: quoted_not_found"

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
  reset_dsa_audit
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  assert_jq "history.negatives.ambiguous.response_status" "$history" \
    '.status == "degraded"'
  assert_jq "history.negatives.ambiguous.response_wording" "$history" \
    '.answer | contains($expected)' \
    --arg expected "$EVIDENCE_HISTORY_AMBIGUOUS_SENTENCE"
  assert_jq "history.negatives.ambiguous.response_suffix" "$history" \
    '.answer | endswith($suffix)' \
    --arg suffix "$EVIDENCE_HISTORY_NO_NEW_VERIFICATION_SENTENCE"
  if ! assert_history_request_boundaries \
    "$conversation_id" "$history" "ambiguous"; then
    echo "Assertion failed: history.negatives.ambiguous.boundaries" >&2
    return 1
  fi
  echo "History negative case passed: ambiguous"

  owner="owner-history-corrupt"
  client="client-history-corrupt"
  provider_post "/fixture/reset" '{}'
  reset_source_fixture
  queue_provider_answer "The corruptible bounded statement is supported."
  conversation_id="$(resolve_conversation "$owner" "$client" "history-corrupt")"
  response="$(run_evidence_chat "$owner" "$client" "$conversation_id" "Verify the migration record." "$external")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  answer="$(jq -r '.answer' <<<"$response")"
  if ! psql_exec -c "
    UPDATE traces
    SET prompt_json = jsonb_set(
      prompt_json,
      '{evidence_acquisition,assistant_message_id}',
      '\"association-corrupted\"'::jsonb
    )
    WHERE request_id = '$request_id';
  " >/dev/null; then
    echo "Assertion failed: history.negatives.corrupt.mutation" >&2
    return 1
  fi
  if ! corrupt_count="$(psql_exec -At -c "
    SELECT count(*)
    FROM traces
    WHERE request_id = '$request_id'
      AND prompt_json #>> '{evidence_acquisition,assistant_message_id}'
        = 'association-corrupted';
  ")" || [[ "$corrupt_count" != "1" ]]; then
    echo "Assertion failed: history.negatives.corrupt.mutation" >&2
    return 1
  fi
  messages="$(jq -nc --arg answer "$answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check?"}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  assert_jq "history.negatives.corrupt.response_status" "$history" \
    '.status == "degraded"'
  assert_jq "history.negatives.corrupt.response_wording" "$history" \
    '.answer | contains("failed association or privacy validation")'
  assert_jq "history.negatives.corrupt.response_privacy" "$history" \
    '.answer | contains("association-corrupted") | not'
  assert_jq "history.negatives.corrupt.response_suffix" "$history" \
    '.answer | endswith($suffix)' \
    --arg suffix "$EVIDENCE_HISTORY_NO_NEW_VERIFICATION_SENTENCE"
  if ! assert_history_request_boundaries \
    "$conversation_id" "$history" "invalid"; then
    echo "Assertion failed: history.negatives.corrupt.boundaries" >&2
    return 1
  fi
  echo "History negative case passed: corrupt"

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
  if ! psql_exec -c "
    UPDATE traces
    SET prompt_json = jsonb_set(
      prompt_json,
      '{evidence_acquisition,acquisition,api_key}',
      to_jsonb('$sentinel'::text),
      true
    )
    WHERE request_id = '$request_id';
  " >/dev/null; then
    echo "Assertion failed: history.negatives.privacy_invalid.mutation" >&2
    return 1
  fi
  if ! privacy_invalid_count="$(psql_exec -At -c "
    SELECT count(*)
    FROM traces
    WHERE request_id = '$request_id'
      AND prompt_json #>> '{evidence_acquisition,acquisition,api_key}'
        = '$sentinel';
  ")" || [[ "$privacy_invalid_count" != "1" ]]; then
    echo "Assertion failed: history.negatives.privacy_invalid.mutation" >&2
    return 1
  fi
  messages="$(jq -nc --arg answer "$answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check?"}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  trace="$(fetch_trace "$(jq -r '.request_id' <<<"$history")")"
  assert_jq "history.negatives.privacy_invalid.response_status" "$history" \
    '.status == "degraded"'
  assert_jq "history.negatives.privacy_invalid.response_wording" "$history" \
    '.answer | contains("failed association or privacy validation")'
  assert_jq "history.negatives.privacy_invalid.response_privacy" "$history" \
    '.answer | contains($sentinel) | not' --arg sentinel "$sentinel"
  assert_jq "history.negatives.privacy_invalid.response_suffix" "$history" \
    '.answer | endswith($suffix)' \
    --arg suffix "$EVIDENCE_HISTORY_NO_NEW_VERIFICATION_SENTENCE"
  case "$(jq -c . <<<"$trace")" in
    *PRIVATE-CREDENTIAL-SENTINEL*)
      echo "Assertion failed: history.negatives.privacy_invalid.trace_privacy" >&2
      return 1
      ;;
  esac
  if ! assert_history_request_boundaries \
    "$conversation_id" "$history" "invalid"; then
    echo "Assertion failed: history.negatives.privacy_invalid.boundaries" >&2
    return 1
  fi
  echo "History negative case passed: privacy_invalid"

  owner="owner-history-isolated"
  client="client-history-isolated"
  conversation_id="$(resolve_conversation "$owner" "$client" "history-isolated")"
  messages="$(jq -nc --arg answer "$answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check?"}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  history="$(run_evidence_messages "$owner" "$client" "$conversation_id" "$messages")"
  assert_jq "history.negatives.owner_isolation.response_status" "$history" \
    '.status == "degraded"'
  assert_jq "history.negatives.owner_isolation.response_wording" "$history" \
    '.answer | contains($expected)' \
    --arg expected "$EVIDENCE_HISTORY_NO_RECORD_SENTENCE"
  assert_jq "history.negatives.owner_isolation.response_suffix" "$history" \
    '.answer | endswith($suffix)' \
    --arg suffix "$EVIDENCE_HISTORY_NO_NEW_VERIFICATION_SENTENCE"
  case "$(jq -c . <<<"$history")" in
    *owner-history-private-invalid*|*PRIVATE-CREDENTIAL-SENTINEL*|*records_primary*)
      echo "Assertion failed: history.negatives.owner_isolation.privacy" >&2
      return 1
      ;;
  esac
  if ! assert_history_request_boundaries \
    "$conversation_id" "$history" "no_record"; then
    echo "Assertion failed: history.negatives.owner_isolation.boundaries" >&2
    return 1
  fi
  echo "History negative case passed: owner_isolation"

  owner="owner-history-private-invalid"
  client="client-history-private-invalid"
  same_owner_conversation="$(resolve_conversation "$owner" "$client" "history-wrong-conversation")"
  messages="$(jq -nc --arg answer "$answer" '[{role:"assistant",content:$answer},{role:"user",content:"What did you check?"}]')"
  provider_post "/fixture/reset" '{}'
  reset_dsa_audit
  history="$(run_evidence_messages "$owner" "$client" "$same_owner_conversation" "$messages")"
  assert_jq "history.negatives.conversation_isolation.response_status" "$history" \
    '.status == "degraded"'
  assert_jq "history.negatives.conversation_isolation.response_wording" "$history" \
    '.answer | contains($expected)' \
    --arg expected "$EVIDENCE_HISTORY_NO_RECORD_SENTENCE"
  assert_jq "history.negatives.conversation_isolation.response_suffix" "$history" \
    '.answer | endswith($suffix)' \
    --arg suffix "$EVIDENCE_HISTORY_NO_NEW_VERIFICATION_SENTENCE"
  case "$(jq -c . <<<"$history")" in
    *PRIVATE-CREDENTIAL-SENTINEL*|*records_primary*)
      echo "Assertion failed: history.negatives.conversation_isolation.privacy" >&2
      return 1
      ;;
  esac
  if ! assert_history_request_boundaries \
    "$same_owner_conversation" "$history" "no_record"; then
    echo "Assertion failed: history.negatives.conversation_isolation.boundaries" >&2
    return 1
  fi
  echo "History negative case passed: conversation_isolation"
  echo "Evidence history negatives: immediate_no_backward_scan=1 quoted_not_found=1 quoted_ambiguity=1 malformed_association=1 privacy_invalid=1 owner_isolation=1 conversation_isolation=1 provider=0"
}

run_evidence_compound_scenarios() {
  local owner client conversation_id external original original_request original_answer
  local messages response request_id answer trace manifest original_manifest provider_calls
  local diagnostics audit replacement verification_target expected_task expected_digest
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
  verification_target="$(printf '%s' "$original_answer" | normalized_first_paragraph)"
  expected_task="Verify this prior statement with a new evidence check: \"$verification_target\""
  expected_digest="sha256:$(printf '%s' "$expected_task" | sha256sum | cut -d' ' -f1)"
  assert_jq "compound.verification.response_status" "$response" \
    '.status == "ok"'
  assert_jq "compound.verification.original_section" "$response" \
    '.answer | startswith("Original acquisition:\n")'
  assert_jq "compound.verification.new_verification_section" "$response" \
    '.answer | contains("\n\nNew verification:\n")'
  assert_jq "compound.verification.provider_answer" "$response" \
    '.answer | contains("The new retained evidence supports the prior statement.")'
  assert_jq "compound.verification.no_historical_suffix" "$response" \
    '.answer | contains("I did not perform a new verification for this explanation.") | not'
  assert_jq "compound.verification.original_section_count" "$response" \
    '([.answer | scan("Original acquisition:")] | length) == 1'
  assert_jq "compound.verification.verification_section_count" "$response" \
    '([.answer | scan("New verification:")] | length) == 1'
  assert_jq "compound.verification.trace_compound_mode" "$trace" \
    '.prompt.claim_explanation.compound_mode == true'
  assert_jq "compound.verification.trace_manifest_resolution" "$trace" \
    '.prompt.claim_explanation.manifest_resolution_status == "resolved"'
  assert_jq "compound.verification.trace_storage" "$trace" \
    '.prompt.claim_explanation.storage_call_count == 1'
  assert_jq "compound.verification.trace_history_provider" "$trace" \
    '.prompt.claim_explanation.provider_call_count == 0'
  print_compound_claim_capture_state "verification" "$trace"
  assert_jq "compound.verification.claim_capture_enabled" "$trace" \
    '.prompt.claim_capture.enabled == true'
  assert_jq "compound.verification.claim_capture_status" "$trace" \
    '.prompt.claim_capture.eligibility_status == "ineligible"'
  assert_jq "compound.verification.claim_capture_reason" "$trace" \
    '.prompt.claim_capture.reason_code == "compound_verification_response"'
  assert_jq "compound.verification.claim_capture_calls" "$trace" '
    .prompt.claim_capture.runtime_call_count == 0
    and .prompt.claim_capture.storage_call_count == 0
    and .prompt.claim_capture.calibration_status == "not_attempted"
    and .prompt.claim_capture.persistence_status == "not_attempted"
  '
  assert_jq "compound.verification.trace_fallback" "$trace" \
    '.fallback.triggered == false'
  assert_jq "compound.verification.trace_model_count" "$trace" \
    '(.model_calls | length) == 1'
  if ! test "$(jq -r '.manifest_id' <<<"$manifest")" != \
    "$(jq -r '.manifest_id' <<<"$original_manifest")"; then
    echo "Assertion failed: compound.verification.manifest_distinct" >&2
    return 1
  fi
  if ! test "$(jq -r '.response_digest' <<<"$manifest")" != \
    "$(jq -r '.response_digest' <<<"$original_manifest")"; then
    echo "Assertion failed: compound.verification.response_digest_distinct" >&2
    return 1
  fi
  if ! test "$(jq -r '.response_digest' <<<"$manifest")" = \
    "sha256:$(printf '%s' "$answer" | sha256sum | cut -d' ' -f1)"; then
    echo "Assertion failed: compound.verification.response_digest_matches" >&2
    return 1
  fi
  assert_jq "compound.verification.no_additional_acquisition" "$manifest" \
    '.next_steps.additional_acquisition_count == 0'
  assert_jq "compound.verification.inventory" "$manifest" '
    .inventory.inventory_status == "complete_for_declared_scope"
    and .inventory.inventory_source_count == 6
    and .inventory.declared_source_count == 1
  '
  assert_jq "compound.verification.provider_calls" "$provider_calls" \
    '([.calls[] | select(.kind == "chat")] | length) == 1'
  if ! assert_dsa_operation_counts "$audit" 1 0 0 >/dev/null 2>&1; then
    echo "Assertion failed: compound.verification.dsa" >&2
    return 1
  fi
  if ! assert_evidence_runtime_events \
    "$diagnostics" "$request_id" 1 1 1 1 >/dev/null 2>&1; then
    echo "Assertion failed: compound.verification.runtime" >&2
    return 1
  fi
  if ! assert_claim_calibration_events \
    "$diagnostics" "$request_id" 0 >/dev/null 2>&1; then
    echo "Assertion failed: compound.verification.claim_calibration" >&2
    return 1
  fi
  assert_jq "compound.verification.question_anchor" "$diagnostics" '
    ([.events[] | select(
      .event_type == "evidence_shape_derived"
      and .event_payload_json.request_id == $request_id
      and .event_payload_json.question_anchor_digest == $digest
    )] | length) == 1
  ' --arg request_id "$request_id" --arg digest "$expected_digest"
  if ! assert_persisted_answer_matches \
    "$conversation_id" "$request_id" "$answer" >/dev/null 2>&1; then
    echo "Assertion failed: compound.verification.answer_persistence" >&2
    return 1
  fi
  if ! assert_request_persistence_counts \
    "$conversation_id" "$request_id" 0 >/dev/null 2>&1; then
    echo "Assertion failed: compound.verification.request_persistence" >&2
    return 1
  fi
  echo "Compound case passed: verification"

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
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  replacement="The governed new evidence check completed, but I withheld the generated explanation because it conflicted with the verification response boundary."
  assert_jq "compound.label_conflict.response_status" "$response" \
    '.status == "degraded"'
  assert_jq "compound.label_conflict.original_section" "$response" \
    '.answer | startswith("Original acquisition:\n")'
  assert_jq "compound.label_conflict.replacement" "$response" \
    '.answer | contains("\n\nNew verification:\n" + $replacement)' \
    --arg replacement "$replacement"
  assert_jq "compound.label_conflict.original_section_count" "$response" \
    '([.answer | scan("Original acquisition:")] | length) == 1'
  assert_jq "compound.label_conflict.verification_section_count" "$response" \
    '([.answer | scan("New verification:")] | length) == 1'
  assert_jq "compound.label_conflict.private_content" "$response" \
    '.answer | contains("PRIVATE-LABEL-SENTINEL") | not'
  assert_jq "compound.label_conflict.unavailable_label" "$response" \
    '.answer | contains("New verification unavailable:") | not'
  assert_jq "compound.label_conflict.discarded_text" "$response" \
    '.answer | contains("No fresh check occurred.") | not'
  if ! test "$(jq -r '.response_digest' <<<"$manifest")" = \
    "sha256:$(printf '%s' "$answer" | sha256sum | cut -d' ' -f1)"; then
    echo "Assertion failed: compound.label_conflict.response_digest" >&2
    return 1
  fi
  assert_jq "compound.label_conflict.no_additional_acquisition" "$manifest" \
    '.next_steps.additional_acquisition_count == 0'
  assert_jq "compound.label_conflict.inventory" "$manifest" '
    .inventory.inventory_status == "complete_for_declared_scope"
    and .inventory.inventory_source_count == 6
    and .inventory.declared_source_count == 1
  '
  assert_jq "compound.label_conflict.trace_storage" "$trace" \
    '.prompt.claim_explanation.storage_call_count == 1'
  assert_jq "compound.label_conflict.trace_manifest_resolution" "$trace" \
    '.prompt.claim_explanation.manifest_resolution_status == "resolved"'
  print_compound_claim_capture_state "label_conflict" "$trace"
  assert_jq "compound.label_conflict.claim_capture_enabled" "$trace" \
    '.prompt.claim_capture.enabled == true'
  assert_jq "compound.label_conflict.claim_capture_status" "$trace" \
    '.prompt.claim_capture.eligibility_status == "ineligible"'
  assert_jq "compound.label_conflict.claim_capture_reason" "$trace" \
    '.prompt.claim_capture.reason_code == "compound_verification_response"'
  assert_jq "compound.label_conflict.claim_capture_calls" "$trace" '
    .prompt.claim_capture.runtime_call_count == 0
    and .prompt.claim_capture.storage_call_count == 0
    and .prompt.claim_capture.calibration_status == "not_attempted"
    and .prompt.claim_capture.persistence_status == "not_attempted"
  '
  assert_jq "compound.label_conflict.trace_fallback" "$trace" \
    '.fallback.triggered == false'
  assert_jq "compound.label_conflict.trace_model_count" "$trace" \
    '(.model_calls | length) == 1'
  assert_jq "compound.label_conflict.provider_calls" "$provider_calls" \
    '([.calls[] | select(.kind == "chat")] | length) == 1'
  if ! assert_dsa_operation_counts "$audit" 1 0 0 >/dev/null 2>&1; then
    echo "Assertion failed: compound.label_conflict.dsa" >&2
    return 1
  fi
  case "$(jq -c . <<<"$trace")" in
    *PRIVATE-LABEL-SENTINEL*|*New\ verification\ unavailable:*|*No\ fresh\ check\ occurred.*)
      echo "Assertion failed: compound.label_conflict.trace_privacy" >&2
      return 1
      ;;
  esac
  if ! assert_evidence_runtime_events \
    "$diagnostics" "$request_id" 1 1 1 1 >/dev/null 2>&1; then
    echo "Assertion failed: compound.label_conflict.runtime" >&2
    return 1
  fi
  if ! assert_claim_calibration_events \
    "$diagnostics" "$request_id" 0 >/dev/null 2>&1; then
    echo "Assertion failed: compound.label_conflict.claim_calibration" >&2
    return 1
  fi
  if ! assert_persisted_answer_matches \
    "$conversation_id" "$request_id" "$answer" >/dev/null 2>&1; then
    echo "Assertion failed: compound.label_conflict.answer_persistence" >&2
    return 1
  fi
  if ! assert_request_persistence_counts \
    "$conversation_id" "$request_id" 0 >/dev/null 2>&1; then
    echo "Assertion failed: compound.label_conflict.request_persistence" >&2
    return 1
  fi
  echo "Compound case passed: label_conflict"

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
  answer="$(jq -r '.answer' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  manifest="$(jq -c '.prompt.evidence_acquisition' <<<"$trace")"
  provider_calls="$(fetch_provider_calls "$request_id")"
  diagnostics="$(runtime_diagnostics_from_trace "$trace")"
  audit="$(fetch_dsa_audit)"
  assert_jq "compound.attempt.response_status" "$response" \
    '.status == "degraded"'
  assert_jq "compound.attempt.original_section" "$response" \
    '.answer | startswith("Original acquisition:\n")'
  assert_jq "compound.attempt.attempt_section" "$response" \
    '.answer | contains("\n\nNew verification attempt:\n")'
  assert_jq "compound.attempt.withheld_conclusion" "$response" \
    '.answer | contains("requested conclusion")'
  assert_jq "compound.attempt.original_section_count" "$response" \
    '([.answer | scan("Original acquisition:")] | length) == 1'
  assert_jq "compound.attempt.attempt_section_count" "$response" \
    '([.answer | scan("New verification attempt:")] | length) == 1'
  assert_jq "compound.attempt.no_verification_section" "$response" \
    '.answer | contains("\n\nNew verification:\n") | not'
  assert_jq "compound.attempt.sufficiency" "$manifest" \
    '.sufficiency.status == "unknown"'
  assert_jq "compound.attempt.no_additional_acquisition" "$manifest" \
    '.next_steps.additional_acquisition_count == 0'
  assert_jq "compound.attempt.inventory" "$manifest" '
    .inventory.inventory_status == "complete_for_declared_scope"
    and .inventory.inventory_source_count == 6
    and .inventory.declared_source_count == 1
  '
  assert_jq "compound.attempt.trace_storage" "$trace" \
    '.prompt.claim_explanation.storage_call_count == 1'
  assert_jq "compound.attempt.trace_manifest_resolution" "$trace" \
    '.prompt.claim_explanation.manifest_resolution_status == "resolved"'
  print_compound_claim_capture_state "attempt" "$trace"
  assert_jq "compound.attempt.claim_capture_enabled" "$trace" \
    '.prompt.claim_capture.enabled == true'
  assert_jq "compound.attempt.claim_capture_status" "$trace" \
    '.prompt.claim_capture.eligibility_status == "ineligible"'
  assert_jq "compound.attempt.claim_capture_reason" "$trace" \
    '.prompt.claim_capture.reason_code == "compound_verification_response"'
  assert_jq "compound.attempt.claim_capture_calls" "$trace" '
    .prompt.claim_capture.runtime_call_count == 0
    and .prompt.claim_capture.storage_call_count == 0
    and .prompt.claim_capture.calibration_status == "not_attempted"
    and .prompt.claim_capture.persistence_status == "not_attempted"
  '
  if ! assert_provider_free_trace "$trace" >/dev/null 2>&1; then
    echo "Assertion failed: compound.attempt.provider_free_trace" >&2
    return 1
  fi
  assert_jq "compound.attempt.provider_calls" "$provider_calls" \
    '([.calls[] | select(.kind == "chat")] | length) == 0'
  if ! assert_dsa_operation_counts "$audit" 1 0 0 >/dev/null 2>&1; then
    echo "Assertion failed: compound.attempt.dsa" >&2
    return 1
  fi
  if ! assert_evidence_runtime_events \
    "$diagnostics" "$request_id" 1 1 1 1 >/dev/null 2>&1; then
    echo "Assertion failed: compound.attempt.runtime" >&2
    return 1
  fi
  if ! assert_claim_calibration_events \
    "$diagnostics" "$request_id" 0 >/dev/null 2>&1; then
    echo "Assertion failed: compound.attempt.claim_calibration" >&2
    return 1
  fi
  if ! assert_persisted_answer_matches \
    "$conversation_id" "$request_id" "$answer" >/dev/null 2>&1; then
    echo "Assertion failed: compound.attempt.answer_persistence" >&2
    return 1
  fi
  if ! assert_request_persistence_counts \
    "$conversation_id" "$request_id" 0 >/dev/null 2>&1; then
    echo "Assertion failed: compound.attempt.request_persistence" >&2
    return 1
  fi
  echo "Compound case passed: attempt"
  echo "Evidence compound: history_resolver=1 fresh_cr_shape=1 fresh_plan=1 fresh_dsa=1 fresh_sufficiency=1 fresh_next_step=1 provider=1 manifest_distinct=1 label_conflict_retry=0 insufficient_provider=0 claims=0"
}

run_evidence_acquisition_composed_suite() {
  local scenario="${EVIDENCE_SCENARIO:-all}"
  case "$scenario" in
    ""|all)
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
      ;;
    history-hybrid)
      run_evidence_history_hybrid_scenario
      echo "Evidence acquisition composed smoke passed: scenarios=history-hybrid"
      ;;
    history-exhaustive)
      run_evidence_history_exhaustive_scenario
      echo "Evidence acquisition composed smoke passed: scenarios=history-exhaustive"
      ;;
    history-unknown)
      run_evidence_history_unknown_scenario
      echo "Evidence acquisition composed smoke passed: scenarios=history-unknown"
      ;;
    history-negatives)
      run_evidence_history_negative_scenarios
      echo "Evidence acquisition composed smoke passed: scenarios=history-negatives"
      ;;
    compound)
      run_evidence_compound_scenarios
      echo "Evidence acquisition composed smoke passed: scenarios=compound"
      ;;
    adversarial-provider)
      run_evidence_adversarial_provider_scenario
      echo "Evidence acquisition composed smoke passed: scenarios=adversarial-provider"
      ;;
    *)
      if [[ "$scenario" =~ ^[A-Za-z0-9_.:-]{1,120}$ ]]; then
        echo "Unsupported evidence scenario: $scenario" >&2
      else
        echo "Unsupported evidence scenario: invalid" >&2
      fi
      return 1
      ;;
  esac
}
