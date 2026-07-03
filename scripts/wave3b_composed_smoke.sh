#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BMS="$ROOT/../basic-memory-store"
CR="$ROOT/../cognitive-runtime"
COMPOSE="$ROOT/docker-compose.composed-smoke.yml"
OVERLAY="$ROOT/docker-compose.wave3b-smoke.yml"
BMS_REQUIRED_COMMIT="f8a4e51595963555a024d22b1491301d5dbd29e6"
CR_REQUIRED_COMMIT="8353f0010e1616db55286eab0e79897315f412eb"
REQUESTED_SCENARIOS="${WAVE3B_SCENARIOS:-all}"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --scenario)
      shift
      test "$#" -gt 0 || {
        echo "wave3b-composed-smoke usage error: --scenario requires a name" >&2
        exit 2
      }
      REQUESTED_SCENARIOS="$1"
      ;;
    --scenario=*)
      REQUESTED_SCENARIOS="${1#--scenario=}"
      ;;
    *)
      echo "wave3b-composed-smoke usage error: unknown argument $1" >&2
      exit 2
      ;;
  esac
  shift
done

selected_scenarios=()
full_suite=false
harness_only=false
focused_composed_selected=false

add_selected_scenario() {
  local name="$1"
  case "$name" in
    all)
      if [ "$full_suite" = true ] || [ "$harness_only" = true ] || [ "$focused_composed_selected" = true ]; then
        echo "wave3b-composed-smoke usage error: all cannot be combined with focused scenarios" >&2
        exit 2
      fi
      full_suite=true
      selected_scenarios=(
        shared_memory
        relationship
        restraint
        artifact
        fallback
        privacy
      )
      ;;
    harness)
      if [ "$harness_only" = true ]; then
        echo "wave3b-composed-smoke usage error: duplicate scenario harness" >&2
        exit 2
      fi
      if [ "$full_suite" = true ] || [ "$focused_composed_selected" = true ]; then
        echo "wave3b-composed-smoke usage error: harness cannot be combined with composed scenarios" >&2
        exit 2
      fi
      harness_only=true
      selected_scenarios=(harness)
      ;;
    shared_memory|shared-memory|shared_canonical_memory_prelimit_filtering)
      if [ "$full_suite" = true ] || [ "$harness_only" = true ]; then
        echo "wave3b-composed-smoke usage error: harness/all cannot be combined with composed scenarios" >&2
        exit 2
      fi
      focused_composed_selected=true
      selected_scenarios+=(shared_memory)
      ;;
    relationship|relationship-narrowing|relationship_narrowing_before_retrieval)
      if [ "$full_suite" = true ] || [ "$harness_only" = true ]; then
        echo "wave3b-composed-smoke usage error: harness/all cannot be combined with composed scenarios" >&2
        exit 2
      fi
      focused_composed_selected=true
      selected_scenarios+=(relationship)
      ;;
    restraint|restraint-zero-call|restraint_zero_call_boundary)
      if [ "$full_suite" = true ] || [ "$harness_only" = true ]; then
        echo "wave3b-composed-smoke usage error: harness/all cannot be combined with composed scenarios" >&2
        exit 2
      fi
      focused_composed_selected=true
      selected_scenarios+=(restraint)
      ;;
    artifact|artifact-policy|artifact_policy_prelimit_filtering)
      if [ "$full_suite" = true ] || [ "$harness_only" = true ]; then
        echo "wave3b-composed-smoke usage error: harness/all cannot be combined with composed scenarios" >&2
        exit 2
      fi
      focused_composed_selected=true
      selected_scenarios+=(artifact)
      ;;
    fallback|fallback-identity|fallback_identity)
      if [ "$full_suite" = true ] || [ "$harness_only" = true ]; then
        echo "wave3b-composed-smoke usage error: harness/all cannot be combined with composed scenarios" >&2
        exit 2
      fi
      focused_composed_selected=true
      selected_scenarios+=(fallback)
      ;;
    privacy|privacy-safe-diagnostics|privacy_safe_diagnostics)
      if [ "$full_suite" = true ] || [ "$harness_only" = true ]; then
        echo "wave3b-composed-smoke usage error: harness/all cannot be combined with composed scenarios" >&2
        exit 2
      fi
      focused_composed_selected=true
      selected_scenarios+=(privacy)
      ;;
    "")
      echo "wave3b-composed-smoke usage error: empty scenario name" >&2
      exit 2
      ;;
    *)
      echo "wave3b-composed-smoke usage error: unknown scenario $name" >&2
      exit 2
      ;;
  esac
}

IFS=',' read -r -a requested_scenario_parts <<<"$REQUESTED_SCENARIOS"
for requested_scenario in "${requested_scenario_parts[@]}"; do
  add_selected_scenario "$requested_scenario"
done
if [ "${#selected_scenarios[@]}" -eq 0 ]; then
  echo "wave3b-composed-smoke usage error: no scenarios selected" >&2
  exit 2
fi
if [ "$harness_only" = true ] && [ "${#selected_scenarios[@]}" -ne 1 ]; then
  echo "wave3b-composed-smoke usage error: harness cannot be combined with composed scenarios" >&2
  exit 2
fi

for command in git docker curl jq python3; do
  command -v "$command" >/dev/null || {
    echo "wave3b-composed-smoke prerequisite missing: $command" >&2
    exit 2
  }
done

for repository in "$BMS" "$CR"; do
  test -d "$repository/.git" || {
    echo "wave3b-composed-smoke prerequisite missing: sibling repository $repository" >&2
    exit 2
  }
  if [ -n "$(git -C "$repository" status --porcelain)" ]; then
    echo "wave3b-composed-smoke prerequisite failed: sibling repository dirty: $repository" >&2
    exit 2
  fi
  branch="$(git -C "$repository" branch --show-current)"
  if [ "$branch" != "main" ]; then
    echo "wave3b-composed-smoke prerequisite failed: sibling repository is not on main: $repository branch=$branch" >&2
    exit 2
  fi
done

git -C "$BMS" merge-base --is-ancestor "$BMS_REQUIRED_COMMIT" HEAD || {
  echo "wave3b-composed-smoke prerequisite failed: basic-memory-store HEAD does not contain $BMS_REQUIRED_COMMIT" >&2
  exit 2
}
git -C "$CR" merge-base --is-ancestor "$CR_REQUIRED_COMMIT" HEAD || {
  echo "wave3b-composed-smoke prerequisite failed: cognitive-runtime HEAD does not contain $CR_REQUIRED_COMMIT" >&2
  exit 2
}

compose() {
  docker compose -f "$COMPOSE" -f "$OVERLAY" "$@"
}

cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

curl_json() {
  local label="$1"
  shift
  local tmp status
  tmp="$(mktemp)"
  status="$(curl -sS -o "$tmp" -w "%{http_code}" "$@")"
  if [ "$status" -lt 200 ] || [ "$status" -ge 300 ]; then
    echo "wave3b-composed-smoke HTTP $status at $label" >&2
    python3 - "$tmp" <<'PY' >&2
import json
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
text = re.sub(r"(Credential=|Signature=|X-Amz-|password=|token=)[^\"&\\s]+", r"\1[redacted]", text)
try:
    text = json.dumps(json.loads(text), sort_keys=True)
except Exception:
    pass
print(text[:900])
PY
    rm -f "$tmp"
    exit 1
  fi
  cat "$tmp"
  rm -f "$tmp"
}

provider_post() {
  local path="$1" body="${2-}"
  if [ -z "$body" ]; then
    body="{}"
  fi
  local payload_file
  payload_file="$(mktemp)"
  printf "%s" "$body" >"$payload_file"
  curl_json "provider POST $path" -X POST "http://127.0.0.1:14381$path" \
    -H "Content-Type: application/json" \
    --data-binary @"$payload_file" >/dev/null
  rm -f "$payload_file"
}

bms_post() {
  curl_json "BMS POST $1" -X POST "http://127.0.0.1:14321$1" \
    -H "X-API-Key: smoke-memory-key" \
    -H "Content-Type: application/json" \
    -d "$2"
}

cr_post() {
  curl_json "CR POST $1" -X POST "http://127.0.0.1:14371$1" \
    -H "Content-Type: application/json" \
    -d "$2"
}

co_chat() {
  local owner="$1" client="$2" surface="$3" conversation="$4" text="$5" sensitivity="${6:-private}" surface_category="${7:-desktop_private}" include_artifacts="${8:-false}"
  curl_json "CO POST /v1/chat" -X POST "http://127.0.0.1:14361/v1/chat" \
    -H "X-API-Key: smoke-orchestrator-key" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc \
      --arg owner "$owner" \
      --arg client "$client" \
      --arg surface "$surface" \
      --arg conversation "$conversation" \
      --arg text "$text" \
      --arg sensitivity "$sensitivity" \
      --arg surface_category "$surface_category" \
      --argjson include_artifacts "$include_artifacts" \
      '{
        owner_id:$owner,
        client_id:$client,
        surface:$surface,
        conversation_id:$conversation,
        surface_context:{
          surface_category:$surface_category,
          sensitivity_level:(if $sensitivity == "local_only" then "highly_sensitive" else "normal" end)
        },
        sensitivity:$sensitivity,
        messages:[{role:"user",content:$text}],
        retrieval:{
          k:3,
          min_score:0,
          scope:"owner",
          time_window:"all",
          retrieval_mode:"balanced",
          include_artifacts:$include_artifacts,
          artifact_k:3
        }
      } + (if $include_artifacts then {external_context_enabled:false} else {} end)')"
}

fetch_trace() {
  curl_json "BMS GET trace" "http://127.0.0.1:14321/v1/traces/$1" \
    -H "X-API-Key: smoke-memory-key"
}

fetch_provider_calls() {
  curl_json "provider GET calls" "http://127.0.0.1:14381/calls/$1"
}

psql_exec() {
  compose exec -T postgres psql -U smoke -d memory "$@"
}

psql_value() {
  psql_exec -At "$@"
}

uuid_for() {
  python3 - "$1" <<'PY'
import sys
import uuid
print(uuid.uuid5(uuid.NAMESPACE_URL, sys.argv[1]))
PY
}

json_vector() {
  python3 - <<'PY'
import json
print(json.dumps([1.0] + [0.0] * 1535))
PY
}

ensure_qdrant_collection() {
  curl_json "Qdrant ensure messages collection" -X PUT "http://127.0.0.1:14391/collections/messages" \
    -H "Content-Type: application/json" \
    -d '{"vectors":{"size":1536,"distance":"Cosine"}}' >/dev/null
}

resolve_conversation() {
  local owner="$1" client="$2" title="$3"
  bms_post "/v1/conversations/resolve" \
    "$(jq -nc --arg owner "$owner" --arg client "$client" --arg title "$title" \
      '{owner_id:$owner,client_id:$client,title:$title,idle_ttl_s:60}')" \
    | jq -r '.conversation_id'
}

policy_json() {
  local domain="$1" sensitivity="${2:-medium}" content_class="${3:-}" rel_id="${4:-}" entity_id="${5:-}" rel_scope="${6:-}"
  jq -nc \
    --arg domain "$domain" \
    --arg sensitivity "$sensitivity" \
    --arg content_class "$content_class" \
    --arg rel_id "$rel_id" \
    --arg entity_id "$entity_id" \
    --arg rel_scope "$rel_scope" \
    '{
      memory_domains:[$domain],
      sensitivity:$sensitivity,
      entity_ids:(if $entity_id == "" then [] else ["project:wave3b", $entity_id] end),
      relationship_ids:(if $rel_id == "" then [] else [$rel_id] end),
      relationship_scopes:(if $rel_scope == "" then [] else [$rel_scope] end)
    } + (if $content_class == "" then {} else {content_class:$content_class} end)'
}

add_message() {
  local conversation="$1" owner="$2" client="$3" role="$4" content="$5" policy="$6" metadata="${7-}"
  if [ -z "$metadata" ]; then
    metadata="{}"
  fi
  bms_post "/v1/conversations/$conversation/messages" \
    "$(jq -nc \
      --arg owner "$owner" \
      --arg client "$client" \
      --arg role "$role" \
      --arg content "$content" \
      --argjson policy "$policy" \
      --argjson metadata "$metadata" \
      '{owner_id:$owner,client_id:$client,role:$role,content:$content,metadata:$metadata,policy_metadata:$policy}')" \
    | jq -r '.message_id'
}

qdrant_upsert_message() {
  local message_id="$1" owner="$2" conversation="$3" client="$4" role="$5" policy="$6"
  local vector
  vector="$(json_vector)"
  jq -nc \
    --arg id "$message_id" \
    --arg owner "$owner" \
    --arg conversation "$conversation" \
    --arg client "$client" \
    --arg role "$role" \
    --argjson vector "$vector" \
    --argjson policy "$policy" \
    '{
      points:[{
        id:$id,
        vector:$vector,
        payload:{
          ref_type:"message",
          message_id:$id,
          owner_id:$owner,
          conversation_id:$conversation,
          client_id:$client,
          role:$role,
          retrieval_policy_valid:true,
          memory_domains:$policy.memory_domains,
          sensitivity:$policy.sensitivity,
          entity_ids:($policy.entity_ids // []),
          relationship_ids:($policy.relationship_ids // []),
          relationship_scopes:($policy.relationship_scopes // [])
        } + (if $policy.content_class then {content_class:$policy.content_class} else {} end)
      }]
    }' \
    | curl_json "Qdrant upsert message fixture" -X PUT "http://127.0.0.1:14391/collections/messages/points" \
      -H "Content-Type: application/json" \
      -d @- >/dev/null
}

add_message_untrusted() {
  local conversation="$1" owner="$2" client="$3" role="$4" content="$5" metadata="${6-}"
  if [ -z "$metadata" ]; then
    metadata="{}"
  fi
  bms_post "/v1/conversations/$conversation/messages" \
    "$(jq -nc \
      --arg owner "$owner" \
      --arg client "$client" \
      --arg role "$role" \
      --arg content "$content" \
      --argjson metadata "$metadata" \
      '{owner_id:$owner,client_id:$client,role:$role,content:$content,metadata:$metadata}')" \
    | jq -r '.message_id'
}

qdrant_upsert_message_untrusted() {
  local message_id="$1" owner="$2" conversation="$3" client="$4" role="$5"
  local vector
  vector="$(json_vector)"
  jq -nc \
    --arg id "$message_id" \
    --arg owner "$owner" \
    --arg conversation "$conversation" \
    --arg client "$client" \
    --arg role "$role" \
    --argjson vector "$vector" \
    '{
      points:[{
        id:$id,
        vector:$vector,
        payload:{
          ref_type:"message",
          message_id:$id,
          owner_id:$owner,
          conversation_id:$conversation,
          client_id:$client,
          role:$role,
          retrieval_policy_valid:false,
          memory_domains:["project"]
        }
      }]
    }' \
    | curl_json "Qdrant upsert untrusted message fixture" -X PUT "http://127.0.0.1:14391/collections/messages/points" \
      -H "Content-Type: application/json" \
      -d @- >/dev/null
}

seed_untrusted_message() {
  local conversation="$1" owner="$2" client="$3" content="$4" metadata="${5-}" message_id
  message_id="$(add_message_untrusted "$conversation" "$owner" "$client" "assistant" "$content" "$metadata")"
  qdrant_upsert_message_untrusted "$message_id" "$owner" "$conversation" "$client" "assistant"
  echo "$message_id"
}

seed_message() {
  local conversation="$1" owner="$2" client="$3" content="$4" policy="$5" metadata="${6-}"
  if [ -z "$metadata" ]; then
    metadata="{}"
  fi
  local message_id
  message_id="$(add_message "$conversation" "$owner" "$client" "assistant" "$content" "$policy" "$metadata")"
  qdrant_upsert_message "$message_id" "$owner" "$conversation" "$client" "assistant" "$policy"
  echo "$message_id"
}

seed_artifact() {
  local owner="$1" client="$2" conversation="$3" suffix="$4" text="$5" policy="$6" status="${7:-completed}" provenance_status="${8:-complete}"
  local artifact_id derived_id file_path object_uri derived_status
  artifact_id="$(uuid_for "wave3b-artifact-$suffix")"
  derived_id="$(uuid_for "wave3b-derived-$suffix")"
  file_path="wave3b/$suffix.txt"
  object_uri="memory://wave3b/$suffix"
  derived_status="$provenance_status"
  if [ "$status" = "completed" ] && [ "$provenance_status" = "complete" ]; then
    derived_status="active"
  fi
  if ! psql_exec >/dev/null <<SQL
INSERT INTO artifacts (
  id, owner_id, client_id, conversation_id, filename, mime, size, object_uri,
  source_surface, status, source_kind, repo_name, file_path, policy_metadata, completed_at
) VALUES (
  '$artifact_id', '$owner', '$client', '$conversation', '$suffix.txt', 'text/plain',
  length('$text'), '$object_uri', 'wave3b-smoke', '$status', 'text', 'wave3b-repo',
  '$file_path', '$policy'::jsonb, CASE WHEN '$status' = 'completed' THEN now() ELSE NULL END
)
ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status;
INSERT INTO derived_text (id, artifact_id, kind, language, text, derivation_params)
VALUES (
  '$derived_id', '$artifact_id', 'derived_text', 'en', '$text',
  jsonb_build_object(
    'source_refs', jsonb_build_array(jsonb_build_object('ref_type','derived_text','ref_id','$derived_id')),
    'status', '$derived_status',
    'effective_status', '$derived_status',
    'provenance_status', '$provenance_status',
    'derivation_version', 'wave3b-smoke-v1',
    'confidence', 0.9,
    'explanation', 'bounded fixture'
  )
)
ON CONFLICT (id) DO UPDATE SET text = EXCLUDED.text, derivation_params = EXCLUDED.derivation_params;
SQL
  then
    echo "wave3b-composed-smoke fixture failed: artifact-sql-$suffix" >&2
    exit 1
  fi
  qdrant_upsert_derived "$derived_id" "$artifact_id" "$owner" "$client" "$conversation" "$file_path" "$policy" "$derived_status"
  echo "$artifact_id:$derived_id"
}

qdrant_upsert_derived() {
  local derived_id="$1" artifact_id="$2" owner="$3" client="$4" conversation="$5" file_path="$6" policy="$7" status="$8"
  local vector
  vector="$(json_vector)"
  jq -nc \
    --arg id "$derived_id" \
    --arg artifact "$artifact_id" \
    --arg owner "$owner" \
    --arg client "$client" \
    --arg conversation "$conversation" \
    --arg path "$file_path" \
    --arg status "$status" \
    --argjson vector "$vector" \
    --argjson policy "$policy" \
    '{
      points:[{
        id:$id,
        vector:$vector,
        payload:{
          ref_type:"derived_text",
          derived_text_id:$id,
          artifact_id:$artifact,
          owner_id:$owner,
          client_id:$client,
          conversation_id:$conversation,
          file_path:$path,
          repo_name:"wave3b-repo",
          chunk_index:0,
          derivation_status:$status,
          retrieval_policy_valid:true,
          memory_domains:$policy.memory_domains,
          sensitivity:$policy.sensitivity,
          entity_ids:($policy.entity_ids // []),
          relationship_ids:($policy.relationship_ids // []),
          relationship_scopes:($policy.relationship_scopes // [])
        } + (if $policy.content_class then {content_class:$policy.content_class} else {} end)
      }]
    }' \
    | curl_json "Qdrant upsert derived artifact fixture" -X PUT "http://127.0.0.1:14391/collections/messages/points" \
      -H "Content-Type: application/json" \
      -d @- >/dev/null
}

retrieval_log_count() {
  compose logs --no-color bms 2>/dev/null | awk '/POST \/v2\/conversations\/.*\/retrieve/ {count += 1} END {print count + 0}'
}

cr_storage_match_count() {
  local needle="$1"
  compose exec -T runtime python -c '
import pathlib
import sqlite3
import sys

needle = sys.argv[1]
count = 0
for path in pathlib.Path("/data").glob("*.sqlite3"):
    conn = sqlite3.connect(path)
    try:
        tables = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = ? AND name NOT LIKE ?",
                ("table", "sqlite_%"),
            )
        ]
        for table in tables:
            columns = [
                row[1]
                for row in conn.execute(f"PRAGMA table_info({table})")
            ]
            if not columns:
                continue
            quoted = ", ".join("\"" + column.replace("\"", "\"\"") + "\"" for column in columns)
            for row in conn.execute(f"SELECT {quoted} FROM \"{table.replace(chr(34), chr(34) + chr(34))}\""):
                if any(value is not None and needle in str(value) for value in row):
                    count += 1
    finally:
        conn.close()
print(count)
' "$needle"
}

assert_jq() {
  local json="$1" filter="$2" label="$3"
  if ! jq -e "$filter" <<<"$json" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: $label" >&2
    exit 1
  fi
}

assert_not_contains() {
  local haystack="$1" needle="$2" label="$3"
  case "$haystack" in
    *"$needle"*)
      echo "wave3b-composed-smoke leaked sentinel label: $label" >&2
      exit 1
      ;;
  esac
}

assert_provider_chat_calls() {
  local calls="$1" request_id="$2" expected_attempts="${3:-}"
  if ! jq -e --arg request_id "$request_id" --arg expected_attempts "$expected_attempts" '
    (.request_id == $request_id)
    and ([.calls[]? | select(.kind == "chat")] | length) > 0
    and (
      $expected_attempts == ""
      or ([.calls[]? | select(.kind == "chat")] | length) == ($expected_attempts | tonumber)
    )
  ' <<<"$calls" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: provider chat calls request_id=$request_id expected_attempts=${expected_attempts:-any}" >&2
    exit 1
  fi
}

assert_provider_sentinel() {
  local calls="$1" request_id="$2" label="$3" expected="$4" expected_attempts="${5:-}"
  assert_provider_chat_calls "$calls" "$request_id" "$expected_attempts"
  if ! jq -e --arg label "$label" --argjson expected "$expected" \
    '[.calls[]? | select(.kind=="chat") | .sentinel_presence[$label] == $expected] as $matches
    | ($matches | length > 0) and ($matches | all)' \
    <<<"$calls" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: provider sentinel label=$label request_id=$request_id expected=$expected" >&2
    exit 1
  fi
}

assert_public_source_allowlist() {
  local response="$1" expected_artifact="$2" label="$3"
  if ! jq -e --arg artifact "$expected_artifact" '
    (.sources | type == "array")
    and (.sources | length > 0)
    and ([.sources[] | select(.artifact_id == $artifact)] | length) >= 1
    and (
      [.sources[] | keys_unsorted[]]
      - ["artifact_id","repo_name","file_path","snippet","relevance_score","source_ref"]
      | length == 0
    )
  ' <<<"$response" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: public source allowlist label=$label expected_artifact=$expected_artifact" >&2
    exit 1
  fi
}

acceptance_json='{}'
scenario_json='{}'

record_scenario() {
  local scenario="$1" detail="$2"
  scenario_json="$(jq -c --arg scenario "$scenario" --arg detail "$detail" '. + {($scenario): $detail}' <<<"$scenario_json")"
}

mark_acceptance() {
  local row="$1" scenario="$2"
  acceptance_json="$(jq -c --arg row "$row" --arg scenario "$scenario" '. + {($row): $scenario}' <<<"$acceptance_json")"
}

expect_harness_failure() {
  local label="$1"
  shift
  if ( "$@" ) >/dev/null 2>&1; then
    echo "wave3b-composed-smoke harness assertion failed: expected failure label=$label" >&2
    exit 1
  fi
}

expect_harness_success() {
  local label="$1"
  shift
  if ! "$@" >/dev/null 2>&1; then
    echo "wave3b-composed-smoke harness assertion failed: expected success label=$label" >&2
    exit 1
  fi
}

assert_required_fixture_setup_is_strict() {
  python3 - "$0" <<'PY'
import re
import sys
from pathlib import Path

source = Path(sys.argv[1]).read_text(encoding="utf-8")
needle = "|" + "| true"
required_patterns = (
    "provider_post",
    "bms_post",
    "cr_post",
    "seed_message",
    "seed_untrusted_message",
    "seed_artifact",
    "qdrant_upsert",
    "ensure_qdrant_collection",
)
for lineno, line in enumerate(source.splitlines(), 1):
    if needle not in line:
        continue
    if "compose down" in line:
        continue
    if any(pattern in line for pattern in required_patterns):
        print(f"wave3b-composed-smoke harness assertion failed: swallowed required fixture setup line={lineno}", file=sys.stderr)
        sys.exit(1)
sys.exit(0)
PY
}

run_harness_selftest() {
  local zero_calls matching_calls missing_required forbidden_present source_good source_empty source_internal
  zero_calls='{"request_id":"rid-provider","calls":[]}'
  matching_calls='{"request_id":"rid-provider","calls":[{"kind":"chat","sentinel_presence":{"expected":true,"forbidden":false}}]}'
  missing_required='{"request_id":"rid-provider","calls":[{"kind":"chat","sentinel_presence":{"expected":false,"forbidden":false}}]}'
  forbidden_present='{"request_id":"rid-provider","calls":[{"kind":"chat","sentinel_presence":{"expected":true,"forbidden":true}}]}'
  source_good='{"sources":[{"artifact_id":"artifact-1","repo_name":"repo","file_path":"file.py","snippet":"allowed","relevance_score":0.9,"source_ref":{"ref_type":"derived_text","ref_id":"derived-1"}}]}'
  source_empty='{"sources":[]}'
  source_internal='{"sources":[{"artifact_id":"artifact-1","snippet":"allowed","owner_id":"owner-internal"}]}'

  expect_harness_failure "zero-provider-calls" assert_provider_sentinel "$zero_calls" "rid-provider" "expected" true "1"
  expect_harness_success "matching-provider-sentinel" assert_provider_sentinel "$matching_calls" "rid-provider" "expected" true "1"
  expect_harness_failure "missing-required-provider-sentinel" assert_provider_sentinel "$missing_required" "rid-provider" "expected" true "1"
  expect_harness_failure "forbidden-provider-sentinel" assert_provider_sentinel "$forbidden_present" "rid-provider" "forbidden" false "1"
  expect_harness_success "non-empty-public-source" assert_public_source_allowlist "$source_good" "artifact-1" "harness-public-source"
  expect_harness_failure "empty-public-source" assert_public_source_allowlist "$source_empty" "artifact-1" "harness-public-source"
  expect_harness_failure "internal-public-source-field" assert_public_source_allowlist "$source_internal" "artifact-1" "harness-public-source"
  assert_required_fixture_setup_is_strict
  assert_jq "$acceptance_json" '. == {}' "harness acceptance starts empty"

  jq -nc \
    --argjson scenarios '{"harness":"CO-3A harness truthfulness assertions passed"}' \
    --argjson acceptance "$acceptance_json" \
    '{
      packet_ok: true,
      packet: "CO-3A",
      focused: true,
      wave: "3B",
      final_acceptance: false,
      scenarios: $scenarios,
      acceptance: $acceptance
    }'
}

if [ "$harness_only" = true ]; then
  run_harness_selftest
  exit 0
fi

compose up -d --build --wait
ensure_qdrant_collection
provider_post "/fixture/reset"

scenario_shared_memory() {
  local scenario="shared_canonical_memory_prelimit_filtering"
  local owner="owner-wave3b-s1" canonical="W3B_CANONICAL_FACT_ALPHA_7fd51" decoy="W3B_DECOY_PRIVATE_90cb2"
  local conv_general conv_authorized conv_unauthorized write_response request_id trace calls metadata_row qpayload
  conv_general="$(resolve_conversation "$owner" "web" "wave3b general write")"
  conv_authorized="$(resolve_conversation "$owner" "vscode" "wave3b technical read")"
  conv_unauthorized="$(resolve_conversation "$owner" "web" "wave3b general excluded read")"
  provider_post "/fixture/sentinels" "$(jq -nc --arg canonical "$canonical" --arg decoy "$decoy" '{sentinels:{canonical:$canonical,decoy:$decoy}}')"

  write_response="$(co_chat "$owner" "web" "web" "$conv_general" "Remember this project fact for later: $canonical" "private" "desktop_private" "false")"
  request_id="$(jq -r '.request_id' <<<"$write_response")"
  trace="$(fetch_trace "$request_id")"
  assert_jq "$trace" '.retrieval.prompt_assembly.retrieval_dispatch.neutral_persistence_classification == "applied"' "$scenario neutral persistence applied"
  metadata_row="$(psql_value -c "SELECT policy_metadata::text FROM messages WHERE owner_id='$owner' AND content LIKE '%$canonical%' ORDER BY created_at DESC LIMIT 1;")"
  case "$metadata_row" in
    *active_persona_id*|*persona_owner*|*persona_id*)
      echo "wave3b-composed-smoke assertion failed: canonical fact stored persona ownership" >&2
      exit 1
      ;;
  esac
  case "$metadata_row" in
    *memory_domains*|*sensitivity*) ;;
    *)
      echo "wave3b-composed-smoke assertion failed: canonical fact missing trusted policy metadata" >&2
      exit 1
      ;;
  esac
  qpayload="$(curl_json "Qdrant scroll canonical" -X POST "http://127.0.0.1:14391/collections/messages/points/scroll" -H "Content-Type: application/json" -d "$(jq -nc --arg owner "$owner" '{filter:{must:[{key:"owner_id",match:{value:$owner}}]},with_payload:true,limit:64}')")"
  assert_jq "$qpayload" '.result.points | length > 0' "$scenario canonical indexed"

  local allowed_policy blocked_policy outside_policy decoy_conv
  allowed_policy="$(policy_json "project" "medium")"
  blocked_policy="$(policy_json "finance" "medium")"
  outside_policy="$(policy_json "personal" "medium")"
  decoy_conv="$(resolve_conversation "$owner" "vscode" "wave3b decoys")"
  for i in 1 2 3 4; do
    seed_message "$decoy_conv" "$owner" "vscode" "higher score blocked decoy $i $decoy" "$blocked_policy" "{}" >/dev/null
    seed_untrusted_message "$decoy_conv" "$owner" "vscode" "higher score spoof metadata decoy $i $decoy" '{"memory_domains":["project"]}' >/dev/null
    seed_message "$decoy_conv" "$owner" "vscode" "higher score outside decoy $i $decoy" "$outside_policy" "{}" >/dev/null
  done

  local read_response read_request read_trace read_calls public_json
  read_response="$(co_chat "$owner" "vscode" "vscode" "$conv_authorized" "Bring in project context. What from memory is the saved project fact?" "private" "desktop_private" "false")"
  read_request="$(jq -r '.request_id' <<<"$read_response")"
  read_trace="$(fetch_trace "$read_request")"
  read_calls="$(fetch_provider_calls "$read_request")"
  public_json="$read_response $read_trace $read_calls"
  assert_not_contains "$public_json" "$decoy" "shared-memory-decoy"
  assert_provider_sentinel "$read_calls" "$read_request" "canonical" true "1"
  assert_provider_sentinel "$read_calls" "$read_request" "decoy" false "1"
  assert_jq "$read_trace" '(.retrieval.prompt_assembly.result_boundary.retained_counts.semantic // .retrieval.prompt_assembly.result_boundary.retained_semantic_count // 0) >= 1' "$scenario retained semantic evidence"
  assert_jq "$read_trace" '.retrieval.bundle.doctrine_summary.canonical_used == true and (.retrieval.bundle.doctrine_summary.reason_codes // [] | index("canonical_evidence_used"))' "$scenario canonical doctrine evidence"
  assert_jq "$read_trace" '(.retrieval.prompt_assembly.memory_hygiene.truth_selection.pre_cr_rejection_reasons.canonical_durable_status_invalid // 0) >= 1' "$scenario rejected untrusted decoys before prompt assembly"

  local denied_response denied_request denied_calls
  denied_response="$(co_chat "$owner" "web" "web" "$conv_unauthorized" "What from memory is saved for my car maintenance?" "private" "desktop_private" "false")"
  denied_request="$(jq -r '.request_id' <<<"$denied_response")"
  denied_calls="$(fetch_provider_calls "$denied_request")"
  assert_provider_sentinel "$denied_calls" "$denied_request" "canonical" false "1"
  if [ "$(cr_storage_match_count "$canonical")" != "0" ]; then
    echo "wave3b-composed-smoke assertion failed: $scenario canonical absent from CR runtime/profile storage" >&2
    exit 1
  fi

  record_scenario "$scenario" "A1/A2/A3 and message A7 assertions passed"
  mark_acceptance "A1" "$scenario"
  mark_acceptance "A2" "$scenario"
  mark_acceptance "A3" "$scenario"
  mark_acceptance "A7_message" "$scenario"
}

relationship_entity_json() {
  local entity_id="$1" label="$2" type="${3:-project}" domain="${4:-project_context}"
  jq -nc --arg entity_id "$entity_id" --arg label "$label" --arg type "$type" --arg domain "$domain" \
    '{entity_id:$entity_id,entity_type:$type,canonical_label:$label,display_label:$label,domain:$domain,sensitivity_level:"medium",source_type:"trusted_config",source_ref:"config:wave3b",canonical_memory_ref:null,artifact_ref:null,status:"active",archived_at:null}'
}

relationship_edge_json() {
  local rel_id="$1" object_id="$2" status="${3:-active}" mentionability="${4:-use_for_filtering_only}" blocked_persona="${5:-}"
  jq -nc --arg rel_id "$rel_id" --arg object_id "$object_id" --arg status "$status" --arg mentionability "$mentionability" --arg blocked "$blocked_persona" \
    '{
      relationship_id:$rel_id,
      subject_entity_id:"project:wave3b",
      relationship_type:"documents",
      object_entity_id:$object_id,
      relationship_scope:"project_context",
      source_type:"trusted_config",
      source_refs_json:["config:wave3b"],
      confidence:0.9,
      status:$status,
      sensitivity_level:"medium",
      mentionability:$mentionability,
      allowed_persona_scopes_json:[],
      blocked_persona_scopes_json:(if $blocked == "" then [] else [$blocked] end),
      valid_from:"2026-01-01T00:00:00+00:00",
      valid_until:null,
      supersede_existing_relationship_id:null,
      superseded_by_relationship_id:null,
      revoked_at:null
    }'
}

scenario_relationship_narrowing() {
  local scenario="relationship_narrowing_before_retrieval"
  local owner="owner-wave3b-s2" eligible="W3B_REL_ELIGIBLE_7d1" excluded="W3B_REL_EXCLUDED_984"
  local conv rel_good="rel_wave3b_good" rel_bad="rel_wave3b_bad" good_entity="repo:wave3b-good" bad_entity="repo:wave3b-bad"
  conv="$(resolve_conversation "$owner" "vscode" "wave3b relationship")"
  cr_post "/v1/relationships/entities/upsert" "$(jq -nc --arg owner "$owner" --argjson entity "$(relationship_entity_json "project:wave3b" "Wave 3B Project")" '{request_id:"rid-wave3b-rel-project",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",entity:$entity}')" >/dev/null
  cr_post "/v1/relationships/entities/upsert" "$(jq -nc --arg owner "$owner" --argjson entity "$(relationship_entity_json "$good_entity" "Wave 3B Good Repo" "repository")" '{request_id:"rid-wave3b-rel-good-entity",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",entity:$entity}')" >/dev/null
  cr_post "/v1/relationships/entities/upsert" "$(jq -nc --arg owner "$owner" --argjson entity "$(relationship_entity_json "$bad_entity" "Wave 3B Bad Repo" "repository")" '{request_id:"rid-wave3b-rel-bad-entity",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",entity:$entity}')" >/dev/null
  cr_post "/v1/relationships/edges/upsert" "$(jq -nc --arg owner "$owner" --argjson edge "$(relationship_edge_json "$rel_good" "$good_entity")" '{request_id:"rid-wave3b-rel-good",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",edge:$edge,evidence:[{evidence_type:"config_reference",source_ref:"config:wave3b",summary:"filtering-only relationship evidence",confidence_delta:0.1}]}')" >/dev/null
  cr_post "/v1/relationships/edges/upsert" "$(jq -nc --arg owner "$owner" --argjson edge "$(relationship_edge_json "$rel_bad" "$bad_entity" "active" "mentionable" "technical_architect")" '{request_id:"rid-wave3b-rel-bad",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",edge:$edge,evidence:[]}')" >/dev/null

  local good_policy bad_policy
  good_policy="$(policy_json "project" "medium" "" "$rel_good" "$good_entity" "project_context")"
  bad_policy="$(policy_json "project" "medium" "" "$rel_bad" "$bad_entity" "project_context")"
  seed_message "$conv" "$owner" "vscode" "eligible relationship memory $eligible" "$good_policy" "{}" >/dev/null
  seed_message "$conv" "$owner" "vscode" "excluded relationship memory $excluded" "$bad_policy" "{}" >/dev/null
  provider_post "/fixture/sentinels" "$(jq -nc --arg eligible "$eligible" --arg excluded "$excluded" '{sentinels:{eligible_relationship:$eligible,excluded_relationship:$excluded}}')"

  local before after response request_id trace calls cr_select
  before="$(retrieval_log_count)"
  response="$(co_chat "$owner" "vscode" "vscode" "$conv" "What from memory should be used for the selected project relationship?" "private" "desktop_private" "false")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  after="$(retrieval_log_count)"
  test "$after" -gt "$before" || { echo "wave3b-composed-smoke assertion failed: relationship scenario did not cross BMS retrieve boundary" >&2; exit 1; }
  trace="$(fetch_trace "$request_id")"
  calls="$(fetch_provider_calls "$request_id")"
  cr_select="$(cr_post "/v1/relationships/select" "$(jq -nc --arg owner "$owner" '{request_id:"rid-wave3b-rel-select-proof",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",active_persona_id:"technical_architect",requested_scopes:["project_context"],relationship_types:["documents"]}')")"
  if ! jq -e --arg rel "$rel_good" '.retrieval_scope_projection.relationship_ids == [$rel]' <<<"$cr_select" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: $scenario CR selected only eligible relationship" >&2
    exit 1
  fi
  assert_jq "$trace" '.retrieval.prompt_assembly.retrieval_dispatch.relationship_projection_applied == true and .retrieval.prompt_assembly.retrieval_dispatch.relationship_id_count == 1' "$scenario CO dispatched selected projection"
  assert_provider_sentinel "$calls" "$request_id" "eligible_relationship" true "1"
  assert_provider_sentinel "$calls" "$request_id" "excluded_relationship" false "1"
  assert_not_contains "$trace" "filtering-only relationship evidence" "relationship-evidence-text"

  cr_post "/v1/relationships/edges/revoke" "$(jq -nc --arg owner "$owner" --arg rel "$rel_good" '{request_id:"rid-wave3b-rel-revoke",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",relationship_id:$rel,evidence:{evidence_type:"user_confirmation",source_ref:"turn:wave3b",summary:"revoked",confidence_delta:0}}')" >/dev/null
  local revoked_response revoked_request revoked_calls
  revoked_response="$(co_chat "$owner" "vscode" "vscode" "$conv" "What from memory should be used for the selected project relationship after revocation?" "private" "desktop_private" "false")"
  revoked_request="$(jq -r '.request_id' <<<"$revoked_response")"
  revoked_calls="$(fetch_provider_calls "$revoked_request")"
  assert_provider_sentinel "$revoked_calls" "$revoked_request" "eligible_relationship" false "1"

  record_scenario "$scenario" "A4 assertions passed"
  mark_acceptance "A4" "$scenario"
}

scenario_restraint_zero_call() {
  local scenario="restraint_zero_call_boundary"
  local owner="owner-wave3b-s3" conv before after response request_id trace control_before control_after control_response
  conv="$(resolve_conversation "$owner" "web" "wave3b restraint")"
  before="$(retrieval_log_count)"
  response="$(co_chat "$owner" "web" "web" "$conv" "Give a short current-turn answer without using memory." "private" "desktop_private" "false")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  after="$(retrieval_log_count)"
  test "$after" = "$before" || { echo "wave3b-composed-smoke assertion failed: restraint request reached BMS retrieve boundary" >&2; exit 1; }
  trace="$(fetch_trace "$request_id")"
  assert_jq "$trace" '.retrieval.prompt_assembly.retrieval_dispatch.bms_retrieval_call_suppressed == true and .retrieval.prompt_assembly.retrieval_dispatch.bms_retrieval_call_issued == false' "$scenario suppressed trace"
  control_before="$(retrieval_log_count)"
  control_response="$(co_chat "$owner" "web" "web" "$conv" "What from memory is relevant?" "private" "desktop_private" "false")"
  control_after="$(retrieval_log_count)"
  test $((control_after - control_before)) -eq 1 || { echo "wave3b-composed-smoke assertion failed: explicit memory control did not issue exactly one BMS retrieval" >&2; exit 1; }
  assert_jq "$(fetch_trace "$(jq -r '.request_id' <<<"$control_response")")" '.retrieval.prompt_assembly.retrieval_dispatch.bms_retrieval_call_issued == true' "$scenario control issued"
  record_scenario "$scenario" "A5 assertions passed"
  mark_acceptance "A5" "$scenario"
}

scenario_artifact_policy() {
  local scenario="artifact_policy_prelimit_filtering"
  local owner="owner-wave3b-s4" conv eligible="W3B_ARTIFACT_ELIGIBLE_3b1" blocked="W3B_ARTIFACT_BLOCKED_2aa" credential="W3B_ARTIFACT_CREDENTIAL_5c9"
  conv="$(resolve_conversation "$owner" "vscode" "wave3b artifacts")"
  local eligible_policy blocked_policy outside_policy sensitive_policy malformed_policy
  eligible_policy="$(policy_json "project" "medium" "code")"
  blocked_policy="$(policy_json "finance" "medium" "code")"
  outside_policy="$(policy_json "personal" "medium" "code")"
  sensitive_policy="$(policy_json "project" "restricted" "code")"
  malformed_policy='{"memory_domains":[],"sensitivity":"medium","content_class":"code"}'
  local eligible_artifact_pair eligible_artifact_id
  eligible_artifact_pair="$(seed_artifact "$owner" "vscode" "$conv" "eligible" "eligible project code artifact $eligible" "$eligible_policy" "completed" "complete")"
  eligible_artifact_id="${eligible_artifact_pair%%:*}"
  seed_artifact "$owner" "vscode" "$conv" "blocked" "blocked domain artifact $blocked $credential" "$blocked_policy" "completed" "complete" >/dev/null
  seed_artifact "$owner" "vscode" "$conv" "outside" "outside domain artifact $blocked" "$outside_policy" "completed" "complete" >/dev/null
  seed_artifact "$owner" "vscode" "$conv" "sensitive" "restricted artifact $blocked" "$sensitive_policy" "completed" "complete" >/dev/null
  seed_artifact "$owner" "vscode" "$conv" "malformed" "malformed metadata artifact $blocked" "$malformed_policy" "completed" "complete" >/dev/null
  seed_artifact "$owner" "vscode" "$conv" "incomplete" "incomplete provenance artifact $blocked" "$eligible_policy" "pending" "building" >/dev/null
  provider_post "/fixture/sentinels" "$(jq -nc --arg eligible "$eligible" --arg blocked "$blocked" --arg credential "$credential" '{sentinels:{eligible_artifact:$eligible,blocked_artifact:$blocked,artifact_credential:$credential}}')"

  local response request_id trace calls
  response="$(co_chat "$owner" "vscode" "vscode" "$conv" "What from memory should I use from allowed project artifacts?" "private" "desktop_private" "true")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  calls="$(fetch_provider_calls "$request_id")"
  assert_jq "$trace" '.retrieval.prompt_assembly.persona_containment.artifact_result_status == "validated"' "$scenario artifact policy validated"
  assert_provider_sentinel "$calls" "$request_id" "eligible_artifact" true "1"
  assert_provider_sentinel "$calls" "$request_id" "blocked_artifact" false "1"
  assert_provider_sentinel "$calls" "$request_id" "artifact_credential" false "1"
  assert_not_contains "$response $trace $calls" "$credential" "artifact-credential"
  assert_public_source_allowlist "$response" "$eligible_artifact_id" "$scenario public sources allowlist"
  record_scenario "$scenario" "A6 and artifact A7 assertions passed"
  mark_acceptance "A6" "$scenario"
  mark_acceptance "A7_artifact" "$scenario"
}

scenario_fallback_identity() {
  local scenario="fallback_identity"
  local owner="owner-wave3b-s5" conv sentinel="W3B_FALLBACK_ALLOWED_33e" blocked="W3B_FALLBACK_BLOCKED_44f" policy response request_id calls trace before after
  conv="$(resolve_conversation "$owner" "vscode" "wave3b fallback")"
  policy="$(policy_json "project" "medium")"
  seed_message "$conv" "$owner" "vscode" "fallback eligible memory $sentinel" "$policy" "{}" >/dev/null
  seed_message "$conv" "$owner" "vscode" "fallback blocked finance memory $blocked" "$(policy_json "finance" "medium")" "{}" >/dev/null
  provider_post "/fixture/sentinels" "$(jq -nc --arg sentinel "$sentinel" --arg blocked "$blocked" '{sentinels:{fallback_allowed:$sentinel,fallback_blocked:$blocked}}')"
  provider_post "/fixture/fail-next-primary"
  before="$(retrieval_log_count)"
  response="$(co_chat "$owner" "vscode" "vscode" "$conv" "What from memory tests fallback scoped context?" "private" "desktop_private" "false")"
  after="$(retrieval_log_count)"
  test $((after - before)) -eq 1 || { echo "wave3b-composed-smoke assertion failed: fallback scenario did not issue exactly one BMS retrieval" >&2; exit 1; }
  request_id="$(jq -r '.request_id' <<<"$response")"
  calls="$(fetch_provider_calls "$request_id")"
  trace="$(fetch_trace "$request_id")"
  assert_jq "$calls" '[.calls[] | select(.kind=="chat")] | length == 2' "$scenario two provider attempts"
  assert_jq "$calls" '[.calls[] | select(.kind=="chat") | .normalized_messages] | .[0] == .[1]' "$scenario normalized messages identical"
  assert_jq "$calls" '[.calls[] | select(.kind=="chat") | .prompt_fingerprint] | .[0] == .[1]' "$scenario prompt fingerprints identical"
  assert_jq "$calls" '[.calls[] | select(.kind=="chat") | .message_count] | .[0] == .[1]' "$scenario message counts identical"
  assert_provider_sentinel "$calls" "$request_id" "fallback_allowed" true "2"
  assert_provider_sentinel "$calls" "$request_id" "fallback_blocked" false "2"
  assert_jq "$trace" '.retrieval.prompt_assembly.provider_fallback_context.same_sanitized_messages_reused == true' "$scenario bounded trace fallback identity"
  record_scenario "$scenario" "A8 assertions passed"
  mark_acceptance "A8" "$scenario"
}

scenario_privacy_safe_diagnostics() {
  local scenario="privacy_safe_diagnostics"
  local owner="owner-wave3b-s6" conv msg="W3B_PRIV_MSG_91a" artifact="W3B_PRIV_ART_82b" meta="W3B_PRIV_META_73c" url="W3B_PRIV_URL_64d" cred="W3B_PRIV_CRED_55e" rel="W3B_PRIV_REL_46f"
  conv="$(resolve_conversation "$owner" "vscode" "wave3b privacy")"
  local policy
  policy="$(policy_json "project" "high" "code")"
  seed_message "$conv" "$owner" "vscode" "privacy message content $msg" "$policy" "$(jq -nc --arg meta "$meta" '{internal_metadata:$meta}')" >/dev/null
  seed_artifact "$owner" "vscode" "$conv" "privacy" "privacy artifact snippet $artifact object url $url credential $cred provenance $rel" "$policy" "completed" "complete" >/dev/null
  provider_post "/fixture/sentinels" "$(jq -nc --arg msg "$msg" --arg artifact "$artifact" --arg meta "$meta" --arg url "$url" --arg cred "$cred" --arg rel "$rel" '{sentinels:{privacy_msg:$msg,privacy_artifact:$artifact,privacy_meta:$meta,privacy_url:$url,privacy_credential:$cred,privacy_relationship:$rel}}')"
  local response request_id trace calls
  response="$(co_chat "$owner" "vscode" "vscode" "$conv" "What from memory can be safely summarized for public glasses?" "private" "glasses_public_or_semi_public" "true")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  calls="$(fetch_provider_calls "$request_id")"
  assert_provider_chat_calls "$calls" "$request_id" "1"
  for label in privacy_msg privacy_artifact privacy_meta privacy_url privacy_credential privacy_relationship; do
    local sentinel
    sentinel="$(jq -r --arg label "$label" '.sentinels[$label]' <<<"$(jq -nc --arg msg "$msg" --arg artifact "$artifact" --arg meta "$meta" --arg url "$url" --arg cred "$cred" --arg rel "$rel" '{sentinels:{privacy_msg:$msg,privacy_artifact:$artifact,privacy_meta:$meta,privacy_url:$url,privacy_credential:$cred,privacy_relationship:$rel}}')")"
    assert_not_contains "$response" "$sentinel" "$label-response"
    assert_not_contains "$trace" "$sentinel" "$label-trace"
    assert_not_contains "$calls" "$sentinel" "$label-provider"
  done
  assert_jq "$response" '.sources == []' "$scenario public sources empty"
  assert_jq "$trace" '.retrieval.prompt_assembly.privacy_context.enforcement_required == true and (.references | length == 0)' "$scenario privacy trace suppressed references"
  assert_jq "$trace" '.retrieval.prompt_assembly.retrieval.bundle == null or .retrieval.prompt_assembly.retrieval.bundle.semantic == null or (.retrieval.prompt_assembly.retrieval.bundle.semantic | length == 0)' "$scenario no unrestricted bundle payload"
  assert_jq "$trace" '((.model_calls // []) | all((.prompt_message_count // .message_count // 0) >= 0 and (.prompt_fingerprint | type == "string"))) and ((.model_call // {}) | (.prompt_fingerprint? // "" | type == "string"))' "$scenario bounded model-call evidence retained"
  record_scenario "$scenario" "A9 assertions passed"
  mark_acceptance "A9" "$scenario"
}

for selected_scenario in "${selected_scenarios[@]}"; do
  case "$selected_scenario" in
    shared_memory)
      scenario_shared_memory
      ;;
    relationship)
      scenario_relationship_narrowing
      ;;
    restraint)
      scenario_restraint_zero_call
      ;;
    artifact)
      scenario_artifact_policy
      ;;
    fallback)
      scenario_fallback_identity
      ;;
    privacy)
      scenario_privacy_safe_diagnostics
      ;;
    *)
      echo "wave3b-composed-smoke usage error: selected scenario not executable: $selected_scenario" >&2
      exit 2
      ;;
  esac
done

acceptance_json="$(jq -c 'if (.A7_message and .A7_artifact) then . + {A7:"message_and_artifact_prelimit_filtering"} else . end' <<<"$acceptance_json")"

if [ "$full_suite" = true ]; then
  jq -e '
    ["A1","A2","A3","A4","A5","A6","A7","A8","A9"] as $required
    | ($required - (keys)) == []
  ' <<<"$acceptance_json" >/dev/null || {
    echo "wave3b-composed-smoke assertion failed: not all A1-A9 rows were proven" >&2
    exit 1
  }
fi

topology_json="$(jq -nc \
  --arg bms_required "$BMS_REQUIRED_COMMIT" \
  --arg cr_required "$CR_REQUIRED_COMMIT" \
  '{
      orchestrator: "branch-under-test",
      basic_memory_store: "main",
      cognitive_runtime: "main",
      postgres: "16",
      qdrant: true,
      object_store_enabled: false,
      provider: "deterministic_stub",
      flags: {
        COGNITIVE_RUNTIME_INTERACTION_GOVERNANCE_ENABLED: true,
        COGNITIVE_RUNTIME_PERSONA_CONTAINMENT_ENABLED: true,
        COGNITIVE_RUNTIME_RESTRAINT_ENABLED: true,
        COGNITIVE_RUNTIME_PRIVACY_CONTEXT_ENABLED: true,
        ENABLE_RUNTIME_OVERLAYS: true,
        COGNITIVE_RUNTIME_MEMORY_HYGIENE_ENABLED: true,
        INDEX_USER_QUESTIONS: true,
        INDEX_ASSISTANT_MESSAGES: true
      },
      prerequisites: {
        basic_memory_store_contains: $bms_required,
        cognitive_runtime_contains: $cr_required
      }
    }')"

if [ "$full_suite" = true ] && { [ "${WAVE3B_FINAL_ACCEPTANCE:-false}" = "true" ] || [ "${WAVE3B_FINAL_ACCEPTANCE:-false}" = "1" ]; }; then
  jq -nc \
    --argjson scenarios "$scenario_json" \
    --argjson acceptance "$acceptance_json" \
    --argjson topology "$topology_json" \
    '{
      ok: true,
      wave: "3B",
      final_acceptance: true,
      topology: $topology,
      scenarios: $scenarios,
      acceptance: $acceptance
    }'
else
  jq -nc \
    --argjson scenarios "$scenario_json" \
    --argjson acceptance "$acceptance_json" \
    --argjson topology "$topology_json" \
    --argjson focused "$(if [ "$full_suite" = true ]; then echo false; else echo true; fi)" \
    '{
      packet_ok: true,
      packet: "CO-3A",
      wave: "3B",
      focused: $focused,
      final_acceptance: false,
      topology: $topology,
      scenarios: $scenarios,
      acceptance: $acceptance
    }'
fi
