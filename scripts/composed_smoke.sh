#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BMS="$ROOT/../basic-memory-store"
CR="$ROOT/../cognitive-runtime"
COMPOSE="$ROOT/docker-compose.composed-smoke.yml"
BMS_COMMIT="3c10de23160822a3da3fec5ab71570ce93ab568c"
CR_COMMIT="1404a77f9c9d1a13df3246f1e98401d9680d653e"

for command in git docker curl jq python3; do
  command -v "$command" >/dev/null || {
    echo "composed-smoke prerequisite missing: $command" >&2
    exit 2
  }
done

for repository in "$BMS" "$CR"; do
  test -d "$repository/.git" || {
    echo "composed-smoke prerequisite missing: sibling repository $repository" >&2
    exit 2
  }
done

git -C "$BMS" merge-base --is-ancestor "$BMS_COMMIT" main || {
  echo "basic-memory-store/main does not contain required merge $BMS_COMMIT" >&2
  exit 2
}
git -C "$CR" merge-base --is-ancestor "$CR_COMMIT" main || {
  echo "cognitive-runtime/main does not contain required merge $CR_COMMIT" >&2
  exit 2
}

cleanup() {
  docker compose -f "$COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker compose -f "$COMPOSE" up -d --build --wait

provider_post() {
  local body
  if [ "$#" -ge 2 ]; then
    body="$2"
  else
    body="{}"
  fi
  curl -fsS -X POST "http://127.0.0.1:14381$1" \
    -H "Content-Type: application/json" \
    -d "$body" >/dev/null
}

bms_post() {
  curl -fsS -X POST "http://127.0.0.1:14321$1" \
    -H "X-API-Key: smoke-memory-key" \
    -H "Content-Type: application/json" \
    -d "$2"
}

co_post() {
  curl -fsS -X POST "http://127.0.0.1:14361/v1/chat" \
    -H "X-API-Key: smoke-orchestrator-key" \
    -H "Content-Type: application/json" \
    -d "$1"
}

psql_exec() {
  docker compose -f "$COMPOSE" exec -T postgres psql -U smoke -d memory "$@"
}

source_hash() {
  python3 - "$1" "$2" <<'PY'
import hashlib, json, sys
refs = [{"ref_type": sys.argv[1], "ref_id": sys.argv[2], "support_kind": "direct"}]
payload = json.dumps(refs, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
print(hashlib.sha256(payload.encode("utf-8")).hexdigest())
PY
}

json_vector() {
  python3 - <<'PY'
import json
print(json.dumps([1.0] + [0.0] * 1535))
PY
}

ensure_qdrant_collection() {
  curl -sS -o /dev/null -X PUT "http://127.0.0.1:14391/collections/messages" \
    -H "Content-Type: application/json" \
    -d '{"vectors":{"size":1536,"distance":"Cosine"}}' || true
}

qdrant_upsert_message() {
  local message_id="$1" owner="$2" conversation_id="$3" client_id="$4" role="$5"
  local vector
  vector="$(json_vector)"
  jq -nc \
    --arg id "$message_id" \
    --arg owner "$owner" \
    --arg conversation "$conversation_id" \
    --arg client "$client_id" \
    --arg role "$role" \
    --argjson vector "$vector" \
    '{points:[{id:$id, vector:$vector, payload:{ref_type:"message", message_id:$id, owner_id:$owner, conversation_id:$conversation, client_id:$client, role:$role}}]}' \
    | curl -fsS -X PUT "http://127.0.0.1:14391/collections/messages/points" \
      -H "Content-Type: application/json" \
      -d @- >/dev/null
}

qdrant_upsert_derived() {
  local derived_id="$1" artifact_id="$2" owner="$3" client_id="$4" file_path="$5"
  local vector
  vector="$(json_vector)"
  jq -nc \
    --arg id "$derived_id" \
    --arg artifact "$artifact_id" \
    --arg owner "$owner" \
    --arg client "$client_id" \
    --arg path "$file_path" \
    --argjson vector "$vector" \
    '{points:[{id:$id, vector:$vector, payload:{ref_type:"derived_text", derived_text_id:$id, artifact_id:$artifact, owner_id:$owner, client_id:$client, file_path:$path, repo_name:"smoke", chunk_index:0}}]}' \
    | curl -fsS -X PUT "http://127.0.0.1:14391/collections/messages/points" \
      -H "Content-Type: application/json" \
      -d @- >/dev/null
}

insert_memory_item() {
  local owner="$1" ref_type="$2" ref_id="$3" status="$4"
  local hash
  hash="$(source_hash "$ref_type" "$ref_id")"
  psql_exec >/dev/null <<SQL
INSERT INTO memory_items (
  owner_id, memory_type, summary, source_refs_json, source_ref_hash,
  scores_json, promotion_state, status, confidence, explanation_json, generation_trace_id
) VALUES (
  '$owner', 'fact', 'neutral smoke fixture',
  '[{"ref_type":"$ref_type","ref_id":"$ref_id","support_kind":"direct"}]'::jsonb,
  '$hash', '{}'::jsonb, 'promoted', '$status', 0.9, '{}'::jsonb, 'smoke-fixture'
);
SQL
}

resolve_conversation() {
  local owner="$1" client="$2" title="$3"
  bms_post "/v1/conversations/resolve" \
    "$(jq -nc --arg owner "$owner" --arg client "$client" --arg title "$title" '{owner_id:$owner, client_id:$client, title:$title, idle_ttl_s:60}')" \
    | jq -r '.conversation_id'
}

add_message() {
  local conversation_id="$1" owner="$2" client="$3" role="$4" content="$5"
  bms_post "/v1/conversations/$conversation_id/messages" \
    "$(jq -nc --arg owner "$owner" --arg client "$client" --arg role "$role" --arg content "$content" '{owner_id:$owner, client_id:$client, role:$role, content:$content}')" \
    | jq -r '.message_id'
}

seed_canonical() {
  local conversation_id="$1" owner="$2" client="$3" content="$4" status="$5"
  local message_id
  message_id="$(add_message "$conversation_id" "$owner" "$client" "assistant" "$content")"
  insert_memory_item "$owner" "message" "$message_id" "$status"
  qdrant_upsert_message "$message_id" "$owner" "$conversation_id" "$client" "assistant"
  echo "$message_id"
}

seed_derived() {
  local conversation_id="$1" owner="$2" client="$3" source_message_id="$4" text="$5" status="$6" suffix="$7"
  local artifact_id="10000000-0000-4000-8000-000000000$suffix"
  local derived_id="20000000-0000-4000-8000-000000000$suffix"
  local file_path="fixture-$suffix.txt"
  psql_exec >/dev/null <<SQL
INSERT INTO artifacts (
  id, owner_id, client_id, conversation_id, filename, mime, size, object_uri,
  source_surface, status, source_kind, repo_name, file_path, completed_at
) VALUES (
  '$artifact_id', '$owner', '$client', '$conversation_id', '$file_path', 'text/plain',
  64, 'memory://smoke/$suffix', 'smoke', 'completed', 'text', 'smoke', '$file_path', now()
);
INSERT INTO derived_text (id, artifact_id, kind, language, text, derivation_params)
VALUES (
  '$derived_id', '$artifact_id', 'derived_text', 'en', '$text',
  '{"source_refs":[{"ref_type":"message","ref_id":"$source_message_id","support_kind":"direct"}],"status":"$status","derivation_version":"v1","confidence":0.9}'::jsonb
);
SQL
  insert_memory_item "$owner" "derived_text" "$derived_id" "$status"
  qdrant_upsert_derived "$derived_id" "$artifact_id" "$owner" "$client" "$file_path"
  echo "$derived_id"
}

seed_missing_source_derivative() {
  local conversation_id="$1" owner="$2" client="$3" text="$4" suffix="$5"
  local artifact_id="10000000-0000-4000-8000-000000000$suffix"
  local derived_id="20000000-0000-4000-8000-000000000$suffix"
  local missing_id="30000000-0000-4000-8000-000000000$suffix"
  local file_path="unsafe-$suffix.txt"
  psql_exec >/dev/null <<SQL
INSERT INTO artifacts (
  id, owner_id, client_id, conversation_id, filename, mime, size, object_uri,
  source_surface, status, source_kind, repo_name, file_path, completed_at
) VALUES (
  '$artifact_id', '$owner', '$client', '$conversation_id', '$file_path', 'text/plain',
  64, 'memory://smoke/unsafe/$suffix', 'smoke', 'completed', 'text', 'smoke', '$file_path', now()
);
INSERT INTO derived_text (id, artifact_id, kind, language, text, derivation_params)
VALUES (
  '$derived_id', '$artifact_id', 'derived_text', 'en', '$text',
  '{"source_refs":[{"ref_type":"message","ref_id":"$missing_id","support_kind":"direct"}],"status":"active","derivation_version":"v1","confidence":0.9}'::jsonb
);
SQL
  qdrant_upsert_derived "$derived_id" "$artifact_id" "$owner" "$client" "$file_path"
  echo "$derived_id"
}

run_chat() {
  local owner="$1" client="$2" conversation_id="$3" question="$4"
  co_post "$(jq -nc --arg owner "$owner" --arg client "$client" --arg conversation "$conversation_id" --arg question "$question" '{owner_id:$owner, client_id:$client, conversation_id:$conversation, surface:"chat", messages:[{role:"user", content:$question}], sensitivity:"private"}')"
}

fetch_trace() {
  local request_id="$1"
  curl -fsS "http://127.0.0.1:14321/v1/traces/$request_id" \
    -H "X-API-Key: smoke-memory-key"
}

fetch_provider_calls() {
  local request_id="$1"
  curl -fsS "http://127.0.0.1:14381/calls/$request_id"
}

assert_persisted_answer_matches() {
  local conversation_id="$1" request_id="$2" expected_answer="$3"
  local row role content
  row="$(
    psql_exec -At -F $'\t' -c "SELECT role, content FROM messages WHERE conversation_id = '$conversation_id' AND metadata->>'request_id' = '$request_id' ORDER BY created_at DESC LIMIT 1;"
  )"
  role="${row%%$'\t'*}"
  content="${row#*$'\t'}"
  test "$role" = "assistant"
  test "$content" = "$expected_answer"
}

assert_runtime_memory_hygiene_count() {
  local trace="$1" request_id="$2" expected_count="$3"
  local runtime_session_id runtime_diagnostics
  runtime_session_id="$(jq -r '.retrieval.prompt_assembly.runtime_session.runtime_session_id // empty' <<<"$trace")"
  test -n "$runtime_session_id"
  runtime_diagnostics="$(
    curl -fsS "http://127.0.0.1:14371/v1/runtime/sessions/$runtime_session_id"
  )"
  jq -e --arg request_id "$request_id" --argjson expected "$expected_count" '
    .latest_turn.turn_status == "completed"
    and ([.events[]
      | select(.event_type == "turn_started"
        or .event_type == "turn_updated"
        or .event_type == "turn_completed")
      | .event_payload_json.request_id] | length >= 3)
    and ([.events[]
      | select(.event_type == "turn_started"
        or .event_type == "turn_updated"
        or .event_type == "turn_completed")
      | .event_payload_json.request_id] | all(. == $request_id))
    and ([.events[]
      | select(.event_type == "memory_hygiene_evaluated")
      | .event_payload_json.evaluated_item_count] | last) == $expected
  ' <<<"$runtime_diagnostics" >/dev/null
}

assert_common_trace() {
  local trace="$1" request_id="$2"
  jq -e --arg request_id "$request_id" '
    .request_id == $request_id
    and (.status == "ok" or .status == "degraded")
    and (.retrieval.bundle | type == "object")
    and (.retrieval.bundle.retrieval_debug.truth_qualification | type == "object")
    and .retrieval.prompt_assembly.memory_hygiene.attempted == true
    and (.prompt.provider_prompt.fingerprint | type == "string")
    and (.prompt.ordered_layer_names | length > 0)
    and .prompt.token_accounting.budget_enforcement == "not_enforced"
  ' <<<"$trace" >/dev/null
}

ensure_qdrant_collection
provider_post "/fixture/reset" '{}'

# Scenario A: active canonical Alpha beats parked derivative Beta.
owner="owner-smoke-a"
client="client-smoke-a"
conversation_id="$(resolve_conversation "$owner" "$client" "smoke-a")"
alpha_id="$(seed_canonical "$conversation_id" "$owner" "$client" "Current plan is Alpha." "active")"
seed_derived "$conversation_id" "$owner" "$client" "$alpha_id" "Old plan was Beta." "parked" "001" >/dev/null
response="$(run_chat "$owner" "$client" "$conversation_id" "What is the current plan?")"
request_id="$(jq -r '.request_id' <<<"$response")"
answer="$(jq -r '.answer' <<<"$response")"
test "$answer" = "Current plan is Alpha."
trace="$(fetch_trace "$request_id")"
provider_calls="$(fetch_provider_calls "$request_id")"
assert_common_trace "$trace" "$request_id"
assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
assert_runtime_memory_hygiene_count "$trace" "$request_id" 2
jq -e '
  .retrieval.prompt_assembly.memory_hygiene.truth_selection.current_canonical_evidence_count >= 1
  and .retrieval.prompt_assembly.memory_hygiene.truth_selection.historical_or_parked_context_count >= 1
  and .retrieval.prompt_assembly.memory_hygiene.truth_selection.no_safe_current_evidence == false
' <<<"$trace" >/dev/null
jq -e '
  (.calls | map(select(.kind == "chat")) | length) == 1
  and (.calls | map(select(.kind == "chat")) | all(.has_current_memory_evidence == true))
  and (.calls | map(select(.kind == "chat")) | all(.has_historical_memory_context == true))
  and (.calls | map(select(.kind == "chat")) | all(.has_forbidden_beta_in_current == false))
' <<<"$provider_calls" >/dev/null

# Scenario B: only stale evidence remains uncertain/historical.
owner="owner-smoke-b"
client="client-smoke-b"
conversation_id="$(resolve_conversation "$owner" "$client" "smoke-b")"
seed_canonical "$conversation_id" "$owner" "$client" "Old plan was Beta." "stale" >/dev/null
response="$(run_chat "$owner" "$client" "$conversation_id" "What is the current plan?")"
request_id="$(jq -r '.request_id' <<<"$response")"
answer="$(jq -r '.answer' <<<"$response")"
test "$answer" = "I only have historical or unverified memory context."
trace="$(fetch_trace "$request_id")"
provider_calls="$(fetch_provider_calls "$request_id")"
assert_common_trace "$trace" "$request_id"
assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
jq -e '
  .retrieval.prompt_assembly.memory_hygiene.truth_selection.no_safe_current_evidence == true
  and .retrieval.prompt_assembly.memory_hygiene.truth_selection.provider_visible_current_count == 0
' <<<"$trace" >/dev/null
jq -e '
  (.calls | map(select(.kind == "chat")) | length) == 1
  and (.calls | map(select(.kind == "chat")) | all(.has_current_memory_evidence == false))
  and (.calls | map(select(.kind == "chat")) | all(.has_historical_memory_context == true))
' <<<"$provider_calls" >/dev/null

# Scenario C: unsafe missing-source derivative Beta is omitted.
owner="owner-smoke-c"
client="client-smoke-c"
conversation_id="$(resolve_conversation "$owner" "$client" "smoke-c")"
seed_canonical "$conversation_id" "$owner" "$client" "Current plan is Alpha." "active" >/dev/null
seed_missing_source_derivative "$conversation_id" "$owner" "$client" "Unsafe derivative says Beta." "002" >/dev/null
response="$(run_chat "$owner" "$client" "$conversation_id" "What is the current plan?")"
request_id="$(jq -r '.request_id' <<<"$response")"
answer="$(jq -r '.answer' <<<"$response")"
test "$answer" = "Current plan is Alpha."
trace="$(fetch_trace "$request_id")"
provider_calls="$(fetch_provider_calls "$request_id")"
assert_common_trace "$trace" "$request_id"
assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
assert_runtime_memory_hygiene_count "$trace" "$request_id" 1
jq -e '
  (.retrieval.bundle.retrieval_debug.truth_qualification.source_missing_count // 0) >= 1
  and (.retrieval.bundle.retrieval_debug.truth_qualification.derivative_omissions_by_reason.missing_derivative_source_record // 0) >= 1
' <<<"$trace" >/dev/null
jq -e '
  (.calls | map(select(.kind == "chat")) | length) == 1
  and (.calls | map(select(.kind == "chat")) | all(.has_beta_marker == false))
' <<<"$provider_calls" >/dev/null

# Scenario D: primary provider fails and fallback reuses the same sanitized prompt.
owner="owner-smoke-d"
client="client-smoke-d"
conversation_id="$(resolve_conversation "$owner" "$client" "smoke-d")"
seed_canonical "$conversation_id" "$owner" "$client" "Current plan is Alpha." "active" >/dev/null
seed_missing_source_derivative "$conversation_id" "$owner" "$client" "Unsafe derivative says Beta." "003" >/dev/null
provider_post "/fixture/fail-next-primary" '{}'
response="$(run_chat "$owner" "$client" "$conversation_id" "What is the current plan?")"
request_id="$(jq -r '.request_id' <<<"$response")"
status="$(jq -r '.status' <<<"$response")"
answer="$(jq -r '.answer' <<<"$response")"
test "$status" = "degraded"
test "$answer" = "Current plan is Alpha."
trace="$(fetch_trace "$request_id")"
provider_calls="$(fetch_provider_calls "$request_id")"
assert_common_trace "$trace" "$request_id"
assert_persisted_answer_matches "$conversation_id" "$request_id" "$answer"
jq -e '
  .prompt.provider_fallback_context.same_sanitized_messages_reused == true
  and .prompt.provider_fallback_context.prompt_fingerprint == .prompt.provider_prompt.fingerprint
  and .fallback.triggered == true
' <<<"$trace" >/dev/null
jq -e '
  (.calls | map(select(.kind == "chat")) | length) == 2
  and (.calls | map(select(.kind == "chat")) | .[0].status == "failed")
  and (.calls | map(select(.kind == "chat")) | .[1].status == "ok")
  and ((.calls | map(select(.kind == "chat")) | .[0].prompt_fingerprint) == (.calls | map(select(.kind == "chat")) | .[1].prompt_fingerprint))
  and (.calls | map(select(.kind == "chat")) | all(.has_beta_marker == false))
  and (.calls | map(select(.kind == "chat")) | all(.has_forbidden_beta_in_current == false))
' <<<"$provider_calls" >/dev/null

echo "Composed smoke passed: scenarios=A-active-canonical, B-stale-only, C-unsafe-derivative, D-provider-fallback"
echo "Topology: CO branch -> deterministic provider stub; BMS main -> PostgreSQL 16 + Qdrant; CR main -> disposable SQLite."
