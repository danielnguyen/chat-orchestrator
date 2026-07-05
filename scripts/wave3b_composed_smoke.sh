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

bms_observer_reset() {
  curl_json "BMS observer reset" -X POST "http://127.0.0.1:14331/fixture/reset" \
    -H "Content-Type: application/json" \
    -d "{}" >/dev/null
}

bms_observer_requests() {
  curl_json "BMS observer requests" "http://127.0.0.1:14331/fixture/requests"
}

bms_post() {
  curl_json "BMS POST $1" -X POST "http://127.0.0.1:14321$1" \
    -H "X-API-Key: smoke-memory-key" \
    -H "Content-Type: application/json" \
    -d "$2"
}

bms_retrieve_bundle() {
  local conversation="$1" request_id="$2" body="$3"
  curl_json "BMS POST retrieve bundle" -X POST "http://127.0.0.1:14321/v2/conversations/$conversation/retrieve" \
    -H "X-API-Key: smoke-memory-key" \
    -H "X-Request-ID: $request_id" \
    -H "Content-Type: application/json" \
    -d "$body"
}

cr_post() {
  curl_json "CR POST $1" -X POST "http://127.0.0.1:14371$1" \
    -H "Content-Type: application/json" \
    -d "$2"
}

co_chat() {
  local owner="$1" client="$2" surface="$3" conversation="$4" text="$5" sensitivity="${6:-private}" surface_category="${7:-desktop_private}" include_artifacts="${8:-false}" min_score="${9:-0}"
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
      --argjson min_score "$min_score" \
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
          min_score:$min_score,
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

provider_embedding_vector() {
  local text="$1"
  curl_json "provider embeddings fixture vector" -X POST "http://127.0.0.1:14381/v1/embeddings" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc --arg text "$text" '{input:$text,model:"fixture-embedding"}')" \
    | jq -c '.data[0].embedding'
}

json_vector_for_score() {
  local query_vector="$1" score="$2"
  python3 - "$query_vector" "$score" <<'PY'
import json
import math
import sys

q = [float(value) for value in json.loads(sys.argv[1])]
score = float(sys.argv[2])
norm = math.sqrt(sum(value * value for value in q))
if norm <= 0:
    raise SystemExit("query vector has zero norm")
q = [value / norm for value in q]
basis_index = min(range(len(q)), key=lambda index: abs(q[index]))
basis = [0.0] * len(q)
basis[basis_index] = 1.0
dot = sum(a * b for a, b in zip(q, basis))
u = [b - dot * a for a, b in zip(q, basis)]
u_norm = math.sqrt(sum(value * value for value in u))
if u_norm <= 0:
    raise SystemExit("orthogonal vector unavailable")
u = [value / u_norm for value in u]
orthogonal_weight = math.sqrt(max(0.0, 1.0 - score * score))
vector = [score * a + orthogonal_weight * b for a, b in zip(q, u)]
print(json.dumps(vector, separators=(",", ":")))
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

mandatory_message_policy_json() {
  local domain="$1"
  jq -nc --arg domain "$domain" '{
    enforcement_mode:"mandatory",
    allowed_memory_domains:[$domain],
    blocked_memory_domains:[],
    artifact_access_policy:{
      enforcement_mode:"mandatory",
      allowed_content_classes:["document","code"],
      allowed_domains:[$domain],
      maximum_sensitivity:"medium",
      surface_content_capabilities:["document","code"],
      reason_codes:["artifact_policy_applied"]
    },
    relationship_scope_projection:null
  }'
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
  local message_id="$1" owner="$2" conversation="$3" client="$4" role="$5" policy="$6" vector="${7-}"
  if [ -z "$vector" ]; then
    vector="$(json_vector)"
  fi
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
  local message_id="$1" owner="$2" conversation="$3" client="$4" role="$5" vector="${6-}"
  if [ -z "$vector" ]; then
    vector="$(json_vector)"
  fi
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

demote_current_turn_query_messages() {
  local owner="$1" query="$2" canonical_id="$3" policy="$4" vector="$5"
  local rows
  rows="$(psql_exec -At -F '|' -c "SELECT id, conversation_id, COALESCE(client_id, ''), role FROM messages WHERE owner_id='$owner' AND content='$query' AND id <> '$canonical_id';")"
  while IFS='|' read -r message_id conversation_id client_id role; do
    [ -n "$message_id" ] || continue
    qdrant_upsert_message "$message_id" "$owner" "$conversation_id" "$client_id" "$role" "$policy" "$vector"
  done <<<"$rows"
}

qdrant_upsert_message_untagged() {
  local message_id="$1" owner="$2" conversation="$3" client="$4" role="$5" vector="${6-}"
  if [ -z "$vector" ]; then
    vector="$(json_vector)"
  fi
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
          role:$role
        }
      }]
    }' \
    | curl_json "Qdrant upsert untagged message fixture" -X PUT "http://127.0.0.1:14391/collections/messages/points" \
      -H "Content-Type: application/json" \
      -d @- >/dev/null
}

seed_untrusted_message() {
  local conversation="$1" owner="$2" client="$3" content="$4" metadata="${5-}" vector="${6-}" message_id
  message_id="$(add_message_untrusted "$conversation" "$owner" "$client" "assistant" "$content" "$metadata")"
  qdrant_upsert_message_untrusted "$message_id" "$owner" "$conversation" "$client" "assistant" "$vector"
  echo "$message_id"
}

seed_untagged_message() {
  local conversation="$1" owner="$2" client="$3" content="$4" metadata="${5-}" vector="${6-}" message_id
  message_id="$(add_message_untrusted "$conversation" "$owner" "$client" "assistant" "$content" "$metadata")"
  qdrant_upsert_message_untagged "$message_id" "$owner" "$conversation" "$client" "assistant" "$vector"
  echo "$message_id"
}

seed_message() {
  local conversation="$1" owner="$2" client="$3" content="$4" policy="$5" metadata="${6-}" vector="${7-}"
  if [ -z "$metadata" ]; then
    metadata="{}"
  fi
  local message_id
  message_id="$(add_message "$conversation" "$owner" "$client" "assistant" "$content" "$policy" "$metadata")"
  qdrant_upsert_message "$message_id" "$owner" "$conversation" "$client" "assistant" "$policy" "$vector"
  echo "$message_id"
}

seed_artifact() {
  local owner="$1" client="$2" conversation="$3" suffix="$4" text="$5" policy="$6" status="${7:-completed}" provenance_status="${8:-complete}" vector="${9-}" mime="${10:-text/plain}" filename="${11:-}" source_refs="${12:-}"
  local artifact_id derived_id file_path object_uri derived_status retrieval_policy_valid
  artifact_id="$(uuid_for "wave3b-artifact-$suffix")"
  derived_id="$(uuid_for "wave3b-derived-$suffix")"
  if [ -z "$filename" ]; then
    filename="$suffix.txt"
  fi
  file_path="wave3b/$filename"
  object_uri="memory://wave3b/$suffix"
  derived_status="$provenance_status"
  if [ "$status" = "completed" ] && [ "$provenance_status" = "complete" ]; then
    derived_status="active"
  fi
  if [ -z "$source_refs" ]; then
    source_refs="$(jq -nc --arg id "$derived_id" '[{ref_type:"derived_text",ref_id:$id,support_kind:"direct"}]')"
  fi
  if jq -e '(.memory_domains // []) | length > 0' <<<"$policy" >/dev/null 2>&1; then
    retrieval_policy_valid=true
  else
    retrieval_policy_valid=false
  fi
  if ! psql_exec >/dev/null <<SQL
INSERT INTO artifacts (
  id, owner_id, client_id, conversation_id, filename, mime, size, object_uri,
  source_surface, status, source_kind, repo_name, file_path, policy_metadata, completed_at
) VALUES (
  '$artifact_id', '$owner', '$client', '$conversation', '$filename', '$mime',
  length('$text'), '$object_uri', 'wave3b-smoke', '$status', 'text', 'wave3b-repo',
  '$file_path', '$policy'::jsonb, CASE WHEN '$status' = 'completed' THEN now() ELSE NULL END
)
ON CONFLICT (id) DO UPDATE SET
  filename = EXCLUDED.filename,
  mime = EXCLUDED.mime,
  status = EXCLUDED.status,
  file_path = EXCLUDED.file_path,
  policy_metadata = EXCLUDED.policy_metadata,
  completed_at = EXCLUDED.completed_at;
INSERT INTO derived_text (id, artifact_id, kind, language, text, derivation_params)
VALUES (
  '$derived_id', '$artifact_id', 'derived_text', 'en', '$text',
  jsonb_build_object(
    'source_refs', '$source_refs'::jsonb,
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
  qdrant_upsert_derived "$derived_id" "$artifact_id" "$owner" "$client" "$conversation" "$file_path" "$policy" "$derived_status" "$vector" "$retrieval_policy_valid"
  echo "$artifact_id:$derived_id"
}

seed_active_memory_item_for_source_ref() {
  local owner="$1" ref_type="$2" ref_id="$3" summary="$4" source_hash
  source_hash="$(uuid_for "wave3b-memory-item-$owner-$ref_type-$ref_id")"
  if ! psql_exec >/dev/null <<SQL
INSERT INTO memory_items (
  owner_id, memory_type, summary, source_refs_json, source_ref_hash,
  scores_json, promotion_state, status, confidence, explanation_json,
  last_reinforced_at
) VALUES (
  '$owner', 'derived_artifact', '$summary',
  jsonb_build_array(jsonb_build_object('ref_type', '$ref_type', 'ref_id', '$ref_id')),
  '$source_hash', '{}'::jsonb, 'promoted', 'active', 0.91,
  jsonb_build_object('rationale', 'wave3b active lifecycle fixture'),
  now()
)
ON CONFLICT DO NOTHING;
SQL
  then
    echo "wave3b-composed-smoke fixture failed: active-memory-item-$ref_id" >&2
    exit 1
  fi
}

qdrant_upsert_derived() {
  local derived_id="$1" artifact_id="$2" owner="$3" client="$4" conversation="$5" file_path="$6" policy="$7" status="$8" vector="${9-}" retrieval_policy_valid="${10:-true}"
  if [ -z "$vector" ]; then
    vector="$(json_vector)"
  fi
  jq -nc \
    --arg id "$derived_id" \
    --arg artifact "$artifact_id" \
    --arg owner "$owner" \
    --arg client "$client" \
    --arg conversation "$conversation" \
    --arg path "$file_path" \
    --arg status "$status" \
    --argjson retrieval_policy_valid "$retrieval_policy_valid" \
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
          retrieval_policy_valid:$retrieval_policy_valid,
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

relationship_select_log_count() {
  compose logs --no-color runtime 2>/dev/null | awk '/POST \/v1\/relationships\/select/ {count += 1} END {print count + 0}'
}

wait_retrieval_log_delta() {
  local before="$1" expected="$2" label="$3" current delta
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    current="$(retrieval_log_count)"
    delta=$((current - before))
    if [ "$delta" -ge "$expected" ]; then
      break
    fi
    sleep 0.25
  done
  current="$(retrieval_log_count)"
  delta=$((current - before))
  if [ "$delta" -ne "$expected" ]; then
    echo "wave3b-composed-smoke assertion failed: $label expected_retrieval_delta=$expected actual_retrieval_delta=$delta" >&2
    exit 1
  fi
  echo "$current"
}

wait_relationship_select_log_delta() {
  local before="$1" expected="$2" label="$3" current delta
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    current="$(relationship_select_log_count)"
    delta=$((current - before))
    if [ "$delta" -ge "$expected" ]; then
      break
    fi
    sleep 0.25
  done
  current="$(relationship_select_log_count)"
  delta=$((current - before))
  if [ "$delta" -ne "$expected" ]; then
    echo "wave3b-composed-smoke assertion failed: $label expected_relationship_select_delta=$expected actual_relationship_select_delta=$delta" >&2
    exit 1
  fi
  echo "$current"
}

assert_relationship_select_before_bms_retrieve() {
  local relationship_before="$1" retrieval_before="$2" label="$3" log_file
  log_file="$(mktemp)"
  compose logs --no-color --timestamps runtime bms >"$log_file" 2>/dev/null
  if ! python3 - "$log_file" "$relationship_before" "$retrieval_before" <<'PY'
import re
import sys
from pathlib import Path

lines = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace").splitlines()
relationship_before = int(sys.argv[2])
retrieval_before = int(sys.argv[3])
relationship_seen = 0
retrieval_seen = 0
relationship_index = None
retrieval_index = None
relationship_timestamp = None
retrieval_timestamp = None
timestamp_pattern = re.compile(r"(\d{4}-\d{2}-\d{2}T[0-9:.]+Z)")

for index, line in enumerate(lines):
    if re.search(r"POST /v1/relationships/select", line):
        relationship_seen += 1
        if relationship_seen > relationship_before and relationship_index is None:
            relationship_index = index
            match = timestamp_pattern.search(line)
            relationship_timestamp = match.group(1) if match else None
    if re.search(r"POST /v2/conversations/.*/retrieve", line):
        retrieval_seen += 1
        if retrieval_seen > retrieval_before and retrieval_index is None:
            retrieval_index = index
            match = timestamp_pattern.search(line)
            retrieval_timestamp = match.group(1) if match else None

if relationship_index is None or retrieval_index is None:
    raise SystemExit(1)
if relationship_timestamp and retrieval_timestamp:
    if relationship_timestamp >= retrieval_timestamp:
        raise SystemExit(1)
elif relationship_index >= retrieval_index:
    raise SystemExit(1)
PY
  then
    rm -f "$log_file"
    echo "wave3b-composed-smoke assertion failed: $label relationship selection did not precede BMS retrieve in service logs" >&2
    exit 1
  fi
  rm -f "$log_file"
}

qdrant_message_scores() {
  local owner="$1" query_vector="$2" limit="${3:-32}"
  curl_json "Qdrant message score evidence" -X POST "http://127.0.0.1:14391/collections/messages/points/search" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc \
      --arg owner "$owner" \
      --argjson vector "$query_vector" \
      --argjson limit "$limit" \
      '{
        vector:$vector,
        limit:$limit,
        with_payload:true,
        filter:{must:[{key:"owner_id",match:{value:$owner}}]}
      }')"
}

qdrant_artifact_scores() {
  local owner="$1" query_vector="$2" limit="${3:-32}"
  curl_json "Qdrant artifact score evidence" -X POST "http://127.0.0.1:14391/collections/messages/points/search" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc \
      --arg owner "$owner" \
      --argjson vector "$query_vector" \
      --argjson limit "$limit" \
      '{
        vector:$vector,
        limit:$limit,
        with_payload:true,
        filter:{must:[
          {key:"owner_id",match:{value:$owner}},
          {key:"ref_type",match:{value:"derived_text"}}
        ]}
      }')"
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

bms_persona_overlay_match_count() {
  local owner="$1" needle="$2"
  psql_value -c "SELECT count(*) FROM persona_overlays WHERE owner_id='$owner' AND (persona_json::text LIKE '%$needle%' OR COALESCE(policy_metadata::text, '') LIKE '%$needle%');"
}

bms_canonical_message_count() {
  local owner="$1" needle="$2"
  psql_value -c "SELECT count(*) FROM messages WHERE owner_id='$owner' AND content LIKE '%$needle%';"
}

ensure_cr_surface_binding() {
  local surface="$1" surface_type="$2" display_name="$3" persona_id="$4"
  if ! compose exec -T runtime python - "$surface" "$surface_type" "$display_name" "$persona_id" <<'PY'
from datetime import UTC, datetime
import sys

from services.companion_contracts import companion_contracts_repository

surface, surface_type, display_name, persona_id = sys.argv[1:5]
repo = companion_contracts_repository()
now = datetime.now(UTC).isoformat()
with repo._connect() as conn:
    if repo.persona_profile(persona_id) is None:
        raise SystemExit("unknown persona fixture")
    conn.execute(
        """
        INSERT INTO surface_bindings (
            surface_id, surface_type, surface_display_name, default_persona_id,
            allow_user_persona_override, response_length, default_mode,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)
        ON CONFLICT(surface_id) DO UPDATE SET
            surface_type = excluded.surface_type,
            surface_display_name = excluded.surface_display_name,
            default_persona_id = excluded.default_persona_id,
            allow_user_persona_override = excluded.allow_user_persona_override,
            response_length = excluded.response_length,
            default_mode = excluded.default_mode,
            updated_at = excluded.updated_at;
        """,
        (surface, surface_type, display_name, persona_id, "concise", "general", now, now),
    )
PY
  then
    echo "wave3b-composed-smoke fixture failed: cr-surface-binding-$surface" >&2
    exit 1
  fi
}

assert_jq() {
  local json="$1" filter="$2" label="$3"
  if ! jq -e "$filter" <<<"$json" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: $label" >&2
    exit 1
  fi
}

assert_jq_arg() {
  local json="$1" arg_name="$2" arg_value="$3" filter="$4" label="$5"
  if ! jq -e --arg "$arg_name" "$arg_value" "$filter" <<<"$json" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: $label" >&2
    exit 1
  fi
}

assert_jq_argjson() {
  local json="$1" arg_name="$2" arg_value="$3" filter="$4" label="$5"
  if ! jq -e --argjson "$arg_name" "$arg_value" "$filter" <<<"$json" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: $label" >&2
    exit 1
  fi
}

assert_jq_two_argjson() {
  local json="$1" first_name="$2" first_value="$3" second_name="$4" second_value="$5" filter="$6" label="$7"
  if ! jq -e --argjson "$first_name" "$first_value" --argjson "$second_name" "$second_value" "$filter" <<<"$json" >/dev/null; then
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

assert_distinct_personas() {
  local persona_a="$1" persona_b="$2" persona_c="$3" label="$4"
  if [ "$persona_a" = "$persona_b" ] || [ "$persona_a" = "$persona_c" ] || [ "$persona_b" = "$persona_c" ]; then
    echo "wave3b-composed-smoke assertion failed: personas not distinct label=$label" >&2
    exit 1
  fi
}

assert_score_ordering() {
  local score_json="$1" canonical_id="$2" decoy_ids_json="$3" min_decoys="$4" label="$5"
  if ! jq -e \
    --arg canonical "$canonical_id" \
    --argjson decoys "$decoy_ids_json" \
    --argjson min_decoys "$min_decoys" \
    '
      (.result | type == "array")
      and ([.result[] | select(.payload.message_id == $canonical)] | length == 1)
      and (
        [.result[] | select(.payload.message_id as $id | $decoys | index($id))]
        | length >= $min_decoys
      )
      and (
        ([.result[] | select(.payload.message_id == $canonical)][0].score) as $canonical_score
        | ([.result[] | select(.payload.message_id as $id | $decoys | index($id)) | select(.score > $canonical_score)] | length) >= $min_decoys
      )
    ' <<<"$score_json" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: score ordering label=$label" >&2
    exit 1
  fi
}

artifact_qdrant_candidate_limit() {
  local artifact_limit="$1"
  local expanded=$((artifact_limit * 20))
  if [ "$expanded" -lt "$artifact_limit" ]; then
    expanded="$artifact_limit"
  fi
  if [ "$expanded" -gt 100 ]; then
    expanded=100
  fi
  echo "$expanded"
}

score_for_message_id() {
  local score_json="$1" message_id="$2"
  jq -r --arg id "$message_id" '[.result[]? | select(.payload.message_id == $id) | .score] | first // empty' <<<"$score_json"
}

assert_artifact_score_ordering() {
  local score_json="$1" eligible_code_derived_id="$2" eligible_doc_derived_id="$3" crowd_ids_json="$4" expected_crowd="$5" candidate_limit="$6" label="$7"
  if ! jq -e \
    --arg code "$eligible_code_derived_id" \
    --arg doc "$eligible_doc_derived_id" \
    --argjson crowd "$crowd_ids_json" \
    --argjson expected_crowd "$expected_crowd" \
    --argjson candidate_limit "$candidate_limit" \
    '
      (.result | type == "array")
      and ($expected_crowd > $candidate_limit)
      and ([.result[] | select(.payload.derived_text_id == $code)] | length == 1)
      and ([.result[] | select(.payload.derived_text_id == $doc)] | length == 1)
      and (
        [.result[] | select(.payload.derived_text_id as $id | $crowd | index($id))]
        | length == $expected_crowd
      )
      and (
        ([.result[] | select(.payload.derived_text_id == $code)][0].score) as $code_score
        | ([.result[] | select(.payload.derived_text_id == $doc)][0].score) as $doc_score
        | ($code_score > $doc_score)
        and ([.result[] | select(.payload.derived_text_id as $id | $crowd | index($id)) | select(.score > $code_score and .score > $doc_score)] | length) == $expected_crowd
      )
      and (
        ([.result | to_entries[] | select(.value.payload.derived_text_id == $code) | .key + 1] | first) as $code_rank
        | ([.result | to_entries[] | select(.value.payload.derived_text_id == $doc) | .key + 1] | first) as $doc_rank
        | ($code_rank > $candidate_limit and $doc_rank > $candidate_limit)
      )
    ' <<<"$score_json" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: artifact score ordering label=$label" >&2
    exit 1
  fi
}

score_for_derived_text_id() {
  local score_json="$1" derived_text_id="$2"
  jq -r --arg id "$derived_text_id" '[.result[]? | select(.payload.derived_text_id == $id) | .score] | first // empty' <<<"$score_json"
}

rank_for_derived_text_id() {
  local score_json="$1" derived_text_id="$2"
  jq -r --arg id "$derived_text_id" '[.result | to_entries[]? | select(.value.payload.derived_text_id == $id) | .key + 1] | first // empty' <<<"$score_json"
}

assert_ids_absent_from_json() {
  local json="$1" ids_json="$2" label="$3"
  if ! jq -n -e --argjson ids "$ids_json" --arg text "$json" '
    $ids
    | map(. as $id | select(($text | contains($id)) == true))
    | length == 0
  ' >/dev/null; then
    python3 - "$json" "$ids_json" <<'PY' >&2
import json
import sys

try:
    payload = json.loads(sys.argv[1])
except Exception:
    payload = sys.argv[1]
ids = set(json.loads(sys.argv[2]))
paths: list[str] = []

def walk(value, path):
    if isinstance(value, dict):
        for key, child in value.items():
            walk(child, f"{path}.{key}" if path else str(key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            walk(child, f"{path}[{index}]")
    elif isinstance(value, str) and any(item in value for item in ids):
        paths.append(path or "$")

walk(payload, "")
for path in paths[:20]:
    print(f"leaked excluded id path: {path}")
PY
    echo "wave3b-composed-smoke assertion failed: excluded id leaked label=$label" >&2
    exit 1
  fi
}

assert_id_absent_from_json() {
  local json="$1" id="$2" label="$3"
  assert_ids_absent_from_json "$json" "$(jq -nc --arg id "$id" '[$id]')" "$label"
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

focused_packet_label() {
  if [ "$full_suite" != true ] && [ "${#selected_scenarios[@]}" -eq 2 ]; then
    local has_fallback=false has_privacy=false scenario
    for scenario in "${selected_scenarios[@]}"; do
      case "$scenario" in
        fallback) has_fallback=true ;;
        privacy) has_privacy=true ;;
      esac
    done
    if [ "$has_fallback" = true ] && [ "$has_privacy" = true ]; then
      echo "CO-3E"
      return
    fi
  fi
  if [ "$full_suite" != true ] && [ "${#selected_scenarios[@]}" -eq 2 ]; then
    local has_relationship=false has_restraint=false scenario
    for scenario in "${selected_scenarios[@]}"; do
      case "$scenario" in
        relationship) has_relationship=true ;;
        restraint) has_restraint=true ;;
      esac
    done
    if [ "$has_relationship" = true ] && [ "$has_restraint" = true ]; then
      echo "CO-3C"
      return
    fi
  fi
  if [ "$full_suite" != true ] && [ "${#selected_scenarios[@]}" -eq 1 ] && [ "${selected_scenarios[0]}" = "shared_memory" ]; then
    echo "CO-3B"
  elif [ "$full_suite" != true ] && [ "${#selected_scenarios[@]}" -eq 1 ] && [ "${selected_scenarios[0]}" = "artifact" ]; then
    echo "CO-3D"
  else
    echo "CO-3A"
  fi
}

assert_packet_label_for_selection() {
  local expected="$1" label="$2" actual
  actual="$(focused_packet_label)"
  if [ "$actual" != "$expected" ]; then
    echo "wave3b-composed-smoke harness assertion failed: packet label $label expected=$expected actual=$actual" >&2
    exit 1
  fi
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
  local saved_full_suite saved_harness_only saved_focused_selected
  local -a saved_selected_scenarios
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
  saved_full_suite="$full_suite"
  saved_harness_only="$harness_only"
  saved_focused_selected="$focused_composed_selected"
  saved_selected_scenarios=("${selected_scenarios[@]}")
  selected_scenarios=(harness)
  full_suite=false
  harness_only=true
  focused_composed_selected=false
  assert_packet_label_for_selection "CO-3A" "harness"
  selected_scenarios=(shared_memory)
  full_suite=false
  harness_only=false
  focused_composed_selected=true
  assert_packet_label_for_selection "CO-3B" "shared-memory"
  selected_scenarios=(relationship restraint)
  full_suite=false
  harness_only=false
  focused_composed_selected=true
  assert_packet_label_for_selection "CO-3C" "relationship-restraint"
  selected_scenarios=(artifact)
  full_suite=false
  harness_only=false
  focused_composed_selected=true
  assert_packet_label_for_selection "CO-3D" "artifact"
  selected_scenarios=(fallback privacy)
  full_suite=false
  harness_only=false
  focused_composed_selected=true
  assert_packet_label_for_selection "CO-3E" "fallback-privacy"
  selected_scenarios=(privacy fallback)
  full_suite=false
  harness_only=false
  focused_composed_selected=true
  assert_packet_label_for_selection "CO-3E" "privacy-fallback"
  selected_scenarios=("${saved_selected_scenarios[@]}")
  full_suite="$saved_full_suite"
  harness_only="$saved_harness_only"
  focused_composed_selected="$saved_focused_selected"

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
  local persona_a="general_assistant" persona_b="technical_architect" persona_c="personal_companion"
  local unauthorized_surface="wave3b-personal"
  local conv_general conv_authorized conv_unauthorized write_response request_id trace calls metadata_row qpayload
  local allowed_policy blocked_policy outside_policy decoy_conv read_query persona_c_query query_vector canonical_vector decoy_vector demoted_query_vector
  local canonical_message_id canonical_count decoy_ids_json blocked_ids_json spoof_ids_json outside_ids_json untagged_ids_json
  local score_evidence canonical_score min_decoy_score crowd_size effective_limit normal_retrieval_before normal_retrieval_after
  assert_distinct_personas "$persona_a" "$persona_b" "$persona_c" "$scenario"
  ensure_cr_surface_binding "$unauthorized_surface" "private_app" "Wave 3B Personal Fixture" "$persona_c"
  conv_general="$(resolve_conversation "$owner" "web" "wave3b general write")"
  conv_authorized="$(resolve_conversation "$owner" "vscode" "wave3b technical read")"
  conv_unauthorized="$(resolve_conversation "$owner" "$unauthorized_surface" "wave3b personal excluded read")"
  provider_post "/fixture/sentinels" "$(jq -nc --arg canonical "$canonical" --arg decoy "$decoy" '{sentinels:{canonical:$canonical,decoy:$decoy}}')"

  allowed_policy="$(policy_json "project" "medium")"
  blocked_policy="$(policy_json "finance" "medium")"
  outside_policy="$(policy_json "personal" "medium")"
  read_query="Bring in project context from memory. What is the saved durable fact?"
  persona_c_query="For my personal planning, use memory to find the same saved project fact from earlier."
  query_vector="$(provider_embedding_vector "$read_query")"
  canonical_vector="$(json_vector_for_score "$query_vector" "0.62")"
  decoy_vector="$(json_vector_for_score "$query_vector" "0.98")"
  demoted_query_vector="$(json_vector_for_score "$query_vector" "0.01")"

  write_response="$(co_chat "$owner" "web" "web" "$conv_general" "Remember this project fact for later: $canonical" "private" "desktop_private" "false")"
  request_id="$(jq -r '.request_id' <<<"$write_response")"
  trace="$(fetch_trace "$request_id")"
  assert_jq_arg "$trace" persona "$persona_a" '.retrieval.prompt_assembly.runtime_identity.active_persona_id == $persona and .retrieval.prompt_assembly.persona_containment.active_persona_id == $persona' "$scenario general assistant surface persona"
  assert_jq "$trace" '.retrieval.prompt_assembly.retrieval_dispatch.neutral_persistence_classification == "applied"' "$scenario neutral persistence applied"
  canonical_message_id="$(psql_value -c "SELECT id FROM messages WHERE owner_id='$owner' AND content LIKE '%$canonical%' ORDER BY created_at DESC LIMIT 1;")"
  test -n "$canonical_message_id" || {
    echo "wave3b-composed-smoke assertion failed: $scenario canonical message persisted" >&2
    exit 1
  }
  canonical_count="$(bms_canonical_message_count "$owner" "$canonical")"
  if [ "$canonical_count" != "1" ]; then
    echo "wave3b-composed-smoke assertion failed: $scenario canonical fact duplicated in BMS messages" >&2
    exit 1
  fi
  metadata_row="$(psql_value -c "SELECT policy_metadata::text FROM messages WHERE id='$canonical_message_id';")"
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
  case "$metadata_row" in
    *project*) ;;
    *)
      echo "wave3b-composed-smoke assertion failed: canonical fact missing project-domain policy metadata" >&2
      exit 1
      ;;
  esac
  qdrant_upsert_message "$canonical_message_id" "$owner" "$conv_general" "web" "user" "$allowed_policy" "$canonical_vector"
  qpayload="$(curl_json "Qdrant scroll canonical" -X POST "http://127.0.0.1:14391/collections/messages/points/scroll" -H "Content-Type: application/json" -d "$(jq -nc --arg id "$canonical_message_id" '{filter:{must:[{key:"message_id",match:{value:$id}}]},with_payload:true,limit:8}')")"
  assert_jq_arg "$qpayload" id "$canonical_message_id" '.result.points | map(select(.payload.message_id == $id and .payload.retrieval_policy_valid == true and (.payload.memory_domains | index("project")))) | length == 1' "$scenario canonical indexed with trusted neutral policy"

  decoy_conv="$(resolve_conversation "$owner" "vscode" "wave3b decoys")"
  decoy_ids_json='[]'
  blocked_ids_json='[]'
  spoof_ids_json='[]'
  outside_ids_json='[]'
  untagged_ids_json='[]'
  effective_limit=3
  crowd_size=0
  for i in 1 2 3 4; do
    local blocked_id spoof_id outside_id untagged_id
    blocked_id="$(seed_message "$decoy_conv" "$owner" "vscode" "higher score blocked decoy $i $decoy" "$blocked_policy" "{}" "$decoy_vector")"
    spoof_id="$(seed_untrusted_message "$decoy_conv" "$owner" "vscode" "higher score spoof metadata decoy $i $decoy" '{"memory_domains":["project"],"trusted_policy_metadata":false}' "$decoy_vector")"
    outside_id="$(seed_message "$decoy_conv" "$owner" "vscode" "higher score outside decoy $i $decoy" "$outside_policy" "{}" "$decoy_vector")"
    untagged_id="$(seed_untagged_message "$decoy_conv" "$owner" "vscode" "higher score untagged decoy $i $decoy" "{}" "$decoy_vector")"
    blocked_ids_json="$(jq -c --arg id "$blocked_id" '. + [$id]' <<<"$blocked_ids_json")"
    spoof_ids_json="$(jq -c --arg id "$spoof_id" '. + [$id]' <<<"$spoof_ids_json")"
    outside_ids_json="$(jq -c --arg id "$outside_id" '. + [$id]' <<<"$outside_ids_json")"
    untagged_ids_json="$(jq -c --arg id "$untagged_id" '. + [$id]' <<<"$untagged_ids_json")"
    decoy_ids_json="$(jq -c --arg blocked "$blocked_id" --arg spoof "$spoof_id" --arg outside "$outside_id" --arg untagged "$untagged_id" '. + [$blocked, $spoof, $outside, $untagged]' <<<"$decoy_ids_json")"
    crowd_size=$((crowd_size + 4))
  done
  if [ "$crowd_size" -le "$effective_limit" ]; then
    echo "wave3b-composed-smoke assertion failed: $scenario ineligible crowd does not exceed effective limit" >&2
    exit 1
  fi
  score_evidence="$(qdrant_message_scores "$owner" "$query_vector" 32)"
  assert_score_ordering "$score_evidence" "$canonical_message_id" "$decoy_ids_json" "$crowd_size" "$scenario ineligible candidates score above canonical"
  canonical_score="$(score_for_message_id "$score_evidence" "$canonical_message_id")"
  min_decoy_score="$(jq -r --argjson decoys "$decoy_ids_json" '[.result[]? | select(.payload.message_id as $id | $decoys | index($id)) | .score] | min // empty' <<<"$score_evidence")"

  local read_response read_request read_trace read_calls public_json direct_bms_request direct_bms_response
  normal_retrieval_before="$(retrieval_log_count)"
  read_response="$(co_chat "$owner" "vscode" "vscode" "$conv_authorized" "$read_query" "private" "desktop_private" "false")"
  normal_retrieval_after="$(wait_retrieval_log_delta "$normal_retrieval_before" 1 "$scenario normal CO request BMS retrieval boundary")"
  read_request="$(jq -r '.request_id' <<<"$read_response")"
  read_trace="$(fetch_trace "$read_request")"
  read_calls="$(fetch_provider_calls "$read_request")"
  public_json="$read_response $read_trace $read_calls"
  assert_not_contains "$public_json" "$decoy" "shared-memory-decoy"
  assert_ids_absent_from_json "$read_response" "$decoy_ids_json" "shared-memory-response-decoy-ids"
  assert_ids_absent_from_json "$read_calls" "$decoy_ids_json" "shared-memory-provider-decoy-ids"
  assert_ids_absent_from_json "$read_trace" "$decoy_ids_json" "shared-memory-trace-decoy-ids"
  assert_jq_arg "$read_trace" persona "$persona_b" '.retrieval.prompt_assembly.runtime_identity.active_persona_id == $persona and .retrieval.prompt_assembly.persona_containment.active_persona_id == $persona' "$scenario authorized technical persona"
  assert_provider_sentinel "$read_calls" "$read_request" "canonical" true "1"
  assert_provider_sentinel "$read_calls" "$read_request" "decoy" false "1"
  assert_jq_arg "$read_trace" id "$canonical_message_id" '([.retrieval.bundle.semantic[]? | select(.message_id == $id)] | length) == 1' "$scenario exact canonical retained from BMS response"
  assert_jq_argjson "$read_trace" decoys "$decoy_ids_json" '([.retrieval.bundle.semantic[]? | select(.message_id as $id | $decoys | index($id))] | length) == 0' "$scenario no decoy retained from BMS response"
  assert_jq_arg "$read_trace" id "$canonical_message_id" '([.retrieval.bundle.semantic[]? | select(.message_id == $id) | .score] | first) < 0.9' "$scenario retained eligible is lower scoring"
  assert_jq_argjson "$read_trace" limit "$effective_limit" '
    ([.retrieval.bundle.semantic[]?] | length) <= $limit
  ' "$scenario CO-retained mandatory filtering evidence"
  assert_jq "$read_trace" '(.retrieval.prompt_assembly.result_boundary.retained_counts.semantic // .retrieval.prompt_assembly.result_boundary.retained_semantic_count // 0) >= 1' "$scenario retained semantic evidence"
  assert_jq "$read_trace" '.retrieval.bundle.doctrine_summary.canonical_used == true and (.retrieval.bundle.doctrine_summary.reason_codes // [] | index("canonical_evidence_used"))' "$scenario canonical doctrine evidence"
  assert_jq "$read_trace" '(.retrieval.prompt_assembly.memory_hygiene.truth_selection.pre_cr_rejection_reasons.canonical_durable_status_invalid // 0) >= 1' "$scenario rejected untrusted decoys before prompt assembly"
  demote_current_turn_query_messages "$owner" "$read_query" "$canonical_message_id" "$outside_policy" "$demoted_query_vector"
  direct_bms_request="wave3b-direct-shared-memory"
  direct_bms_response="$(bms_retrieve_bundle "$conv_authorized" "$direct_bms_request" "$(jq -nc \
    --arg request_id "$direct_bms_request" \
    --arg owner "$owner" \
    --arg query "$read_query" \
    --argjson policy "$(mandatory_message_policy_json "project")" \
    '{
      request_id:$request_id,
      owner_id:$owner,
      query:$query,
      retrieval:{k:3,min_score:0,scope:"owner",time_window:"all",retrieval_mode:"balanced"},
      include_artifacts:false,
      containment_policy:$policy
    }')")"
  if ! jq -e --arg id "$canonical_message_id" --argjson decoys "$decoy_ids_json" --argjson limit "$effective_limit" --argjson min_decoy "$min_decoy_score" '
    (
      .bundle.retrieval_debug.containment_policy.pre_limit_policy_filter_applied == true
      or .bundle.retrieval_debug.mandatory_policy_filter_applied == true
    )
    and ((.bundle.retrieval_debug.semantic_candidates // 0) <= $limit)
    and ([.bundle.semantic[]? | select(.message_id == $id)] | length) == 1
    and ([.bundle.semantic[]? | select(.message_id as $message_id | $decoys | index($message_id))] | length) == 0
    and ([.bundle.semantic[]? | select(.message_id == $id) | .score] | first) < $min_decoy
    and ((.diagnostics.reason_codes // [] | index("mandatory_containment_applied")) != null)
  ' <<<"$direct_bms_response" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: $scenario direct BMS pre-limit mandatory filtering before limiting" >&2
    exit 1
  fi
  assert_ids_absent_from_json "$direct_bms_response" "$decoy_ids_json" "shared-memory-direct-bms-decoy-ids"

  local denied_response denied_request denied_calls
  denied_response="$(co_chat "$owner" "$unauthorized_surface" "$unauthorized_surface" "$conv_unauthorized" "$persona_c_query" "private" "desktop_private" "false")"
  denied_request="$(jq -r '.request_id' <<<"$denied_response")"
  local denied_trace
  denied_trace="$(fetch_trace "$denied_request")"
  denied_calls="$(fetch_provider_calls "$denied_request")"
  assert_jq_arg "$denied_trace" persona "$persona_c" '.retrieval.prompt_assembly.runtime_identity.active_persona_id == $persona and .retrieval.prompt_assembly.persona_containment.active_persona_id == $persona' "$scenario third persona surface"
  assert_jq "$denied_trace" '(.retrieval.prompt_assembly.persona_containment.allowed_memory_domains // [] | index("project")) == null and (.retrieval.prompt_assembly.persona_containment.blocked_memory_domains // [] | index("project")) != null and .retrieval.prompt_assembly.persona_containment.cross_scope_access_allowed == false and .retrieval.prompt_assembly.persona_containment.capability_domain == "personal"' "$scenario persona C containment blocks project memory"
  assert_jq "$denied_calls" '[.calls[]? | select(.kind=="chat") | .normalized_messages[]? | select(.role=="user") | .content | ascii_downcase | contains("memory") and contains("saved project fact")] | any' "$scenario persona C explicitly requested same project memory"
  assert_provider_sentinel "$denied_calls" "$denied_request" "canonical" false "1"
  assert_jq_arg "$denied_trace" id "$canonical_message_id" '([.retrieval.bundle.semantic[]? | select(.message_id == $id)] | length) == 0' "$scenario unauthorized persona cannot retain canonical"
  assert_not_contains "$denied_response $denied_calls $denied_trace" "$canonical" "persona-c-canonical-sentinel"
  assert_id_absent_from_json "$denied_response" "$canonical_message_id" "persona-c-response-canonical-id"
  assert_id_absent_from_json "$denied_calls" "$canonical_message_id" "persona-c-provider-canonical-id"
  assert_id_absent_from_json "$denied_trace" "$canonical_message_id" "persona-c-trace-canonical-id"
  if [ "$(cr_storage_match_count "$canonical")" != "0" ]; then
    echo "wave3b-composed-smoke assertion failed: $scenario canonical absent from CR runtime/profile storage" >&2
    exit 1
  fi
  if [ "$(bms_persona_overlay_match_count "$owner" "$canonical")" != "0" ]; then
    echo "wave3b-composed-smoke assertion failed: $scenario canonical absent from BMS persona overlays" >&2
    exit 1
  fi
  if [ "$(bms_canonical_message_count "$owner" "$canonical")" != "1" ]; then
    echo "wave3b-composed-smoke assertion failed: $scenario canonical fact copied in BMS messages" >&2
    exit 1
  fi

  record_scenario "$scenario" "CO-3B A1/A2/A3 and message A7 assertions passed with three distinct personas, blocked/outside/malformed/untagged crowding, BMS/Qdrant score evidence canonical_score=$canonical_score min_decoy_score=$min_decoy_score, and no CR/BMS persona-overlay copy"
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
  local rel_id="$1" object_id="$2" status="${3:-active}" mentionability="${4:-use_for_filtering_only}" blocked_persona="${5:-}" rel_type="${6:-documents}" rel_scope="${7:-project_context}"
  jq -nc --arg rel_id "$rel_id" --arg object_id "$object_id" --arg status "$status" --arg mentionability "$mentionability" --arg blocked "$blocked_persona" --arg rel_type "$rel_type" --arg rel_scope "$rel_scope" \
    '{
      relationship_id:$rel_id,
      subject_entity_id:"project:wave3b",
      relationship_type:$rel_type,
      object_entity_id:$object_id,
      relationship_scope:$rel_scope,
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
  local owner="owner-wave3b-s2" eligible="W3B_REL_ELIGIBLE_7d1" excluded="W3B_REL_EXCLUDED_984" unrelated="W3B_REL_UNRELATED_542"
  local conv rel_good="rel_wave3b_good" rel_bad="rel_wave3b_bad" rel_unrelated="rel_wave3b_unrelated"
  local good_entity="repo:wave3b-good" bad_entity="repo:wave3b-bad" unrelated_entity="repo:wave3b-unrelated"
  conv="$(resolve_conversation "$owner" "vscode" "wave3b relationship")"
  cr_post "/v1/relationships/entities/upsert" "$(jq -nc --arg owner "$owner" --argjson entity "$(relationship_entity_json "project:wave3b" "Wave 3B Project")" '{request_id:"rid-wave3b-rel-project",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",entity:$entity}')" >/dev/null
  cr_post "/v1/relationships/entities/upsert" "$(jq -nc --arg owner "$owner" --argjson entity "$(relationship_entity_json "$good_entity" "Wave 3B Good Repo" "repository")" '{request_id:"rid-wave3b-rel-good-entity",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",entity:$entity}')" >/dev/null
  cr_post "/v1/relationships/entities/upsert" "$(jq -nc --arg owner "$owner" --argjson entity "$(relationship_entity_json "$bad_entity" "Wave 3B Bad Repo" "repository")" '{request_id:"rid-wave3b-rel-bad-entity",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",entity:$entity}')" >/dev/null
  cr_post "/v1/relationships/entities/upsert" "$(jq -nc --arg owner "$owner" --argjson entity "$(relationship_entity_json "$unrelated_entity" "Wave 3B Unrelated Repo" "repository" "personal_context")" '{request_id:"rid-wave3b-rel-unrelated-entity",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",entity:$entity}')" >/dev/null
  cr_post "/v1/relationships/edges/upsert" "$(jq -nc --arg owner "$owner" --argjson edge "$(relationship_edge_json "$rel_good" "$good_entity")" '{request_id:"rid-wave3b-rel-good",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",edge:$edge,evidence:[{evidence_type:"config_reference",source_ref:"config:wave3b",summary:"filtering-only relationship evidence",confidence_delta:0.1}]}')" >/dev/null
  cr_post "/v1/relationships/edges/upsert" "$(jq -nc --arg owner "$owner" --argjson edge "$(relationship_edge_json "$rel_bad" "$bad_entity" "active" "mentionable" "technical_architect")" '{request_id:"rid-wave3b-rel-bad",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",edge:$edge,evidence:[]}')" >/dev/null
  cr_post "/v1/relationships/edges/upsert" "$(jq -nc --arg owner "$owner" --argjson edge "$(relationship_edge_json "$rel_unrelated" "$unrelated_entity" "active" "mentionable" "" "documents" "personal_context")" '{request_id:"rid-wave3b-rel-unrelated",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",edge:$edge,evidence:[]}')" >/dev/null

  local good_policy bad_policy unrelated_policy eligible_message_id excluded_message_id unrelated_message_id excluded_ids_json
  good_policy="$(policy_json "project" "medium" "" "$rel_good" "$good_entity" "project_context")"
  bad_policy="$(policy_json "project" "medium" "" "$rel_bad" "$bad_entity" "project_context")"
  unrelated_policy="$(policy_json "project" "medium" "" "$rel_unrelated" "$unrelated_entity" "personal_context")"
  eligible_message_id="$(seed_message "$conv" "$owner" "vscode" "eligible relationship memory $eligible" "$good_policy" "{}")"
  excluded_message_id="$(seed_message "$conv" "$owner" "vscode" "excluded relationship memory $excluded" "$bad_policy" "{}")"
  unrelated_message_id="$(seed_message "$conv" "$owner" "vscode" "unrelated relationship memory $unrelated" "$unrelated_policy" "{}")"
  excluded_ids_json="$(jq -nc --arg excluded "$excluded_message_id" --arg unrelated "$unrelated_message_id" '[$excluded, $unrelated]')"
  provider_post "/fixture/sentinels" "$(jq -nc --arg eligible "$eligible" --arg excluded "$excluded" --arg unrelated "$unrelated" '{sentinels:{eligible_relationship:$eligible,excluded_relationship:$excluded,unrelated_relationship:$unrelated}}')"

  local relationship_before relationship_after before after response request_id trace calls
  relationship_before="$(relationship_select_log_count)"
  before="$(retrieval_log_count)"
  response="$(co_chat "$owner" "vscode" "vscode" "$conv" "What from memory should be used for the selected project relationship?" "private" "desktop_private" "false")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  relationship_after="$(wait_relationship_select_log_delta "$relationship_before" 1 "$scenario normal CO request CR relationship selection boundary")"
  after="$(wait_retrieval_log_delta "$before" 1 "$scenario normal CO request BMS retrieval boundary")"
  assert_relationship_select_before_bms_retrieve "$relationship_before" "$before" "$scenario normal CO request"
  trace="$(fetch_trace "$request_id")"
  calls="$(fetch_provider_calls "$request_id")"
  assert_jq_arg "$trace" rel "$rel_good" '.retrieval.prompt_assembly.relationship_context.relationship_edges_used == [$rel]' "$scenario CR selected only eligible relationship for CO request"
  assert_jq_arg "$trace" rel "$rel_bad" '(.retrieval.prompt_assembly.relationship_context.relationship_edges_excluded // [] | index($rel)) != null and (.retrieval.prompt_assembly.relationship_context.relationship_exclusion_reasons[$rel] // "") != ""' "$scenario excluded relationship recorded with bounded reason"
  assert_jq_arg "$trace" rel "$rel_unrelated" '(.retrieval.prompt_assembly.relationship_context.relationship_edges_used // [] | index($rel)) == null' "$scenario unrelated relationship not selected"
  assert_jq_arg "$trace" rel "$rel_good" '.retrieval.prompt_assembly.retrieval_dispatch.relationship_projection_applied == true and .retrieval.prompt_assembly.retrieval_dispatch.relationship_id_count == 1 and .retrieval.prompt_assembly.retrieval_dispatch.entity_id_count == 2 and .retrieval.prompt_assembly.retrieval_dispatch.relationship_scope_count == 1 and .retrieval.prompt_assembly.retrieval_dispatch.relationship_scope_projection.relationship_ids == [$rel]' "$scenario BMS request projection contains only eligible relationship"
  assert_jq_arg "$trace" entity "$good_entity" '(.retrieval.prompt_assembly.retrieval_dispatch.relationship_scope_projection.entity_ids // [] | index("project:wave3b")) != null and (.retrieval.prompt_assembly.retrieval_dispatch.relationship_scope_projection.entity_ids // [] | index($entity)) != null and ((.retrieval.prompt_assembly.retrieval_dispatch.relationship_scope_projection.entity_ids // []) | length) == 2' "$scenario BMS request projection contains only eligible entities"
  assert_jq_arg "$trace" scope "project_context" '.retrieval.prompt_assembly.retrieval_dispatch.relationship_scope_projection.relationship_scopes == [$scope]' "$scenario BMS request projection contains only project scope"
  assert_jq_arg "$trace" rel "$rel_bad" '(.retrieval.prompt_assembly.retrieval_dispatch.relationship_scope_projection.relationship_ids // [] | index($rel)) == null' "$scenario BMS request excludes blocked relationship"
  assert_jq_arg "$trace" rel "$rel_unrelated" '(.retrieval.prompt_assembly.retrieval_dispatch.relationship_scope_projection.relationship_ids // [] | index($rel)) == null' "$scenario BMS request excludes unrelated relationship"
  assert_jq_arg "$trace" id "$eligible_message_id" '([.retrieval.bundle.semantic[]? | select(.message_id == $id)] | length) == 1' "$scenario eligible relationship memory retained"
  assert_jq_argjson "$trace" ids "$excluded_ids_json" '([.retrieval.bundle.semantic[]? | select(.message_id as $id | $ids | index($id))] | length) == 0' "$scenario excluded relationship memories absent from retained bundle"
  assert_provider_sentinel "$calls" "$request_id" "eligible_relationship" true "1"
  assert_provider_sentinel "$calls" "$request_id" "excluded_relationship" false "1"
  assert_provider_sentinel "$calls" "$request_id" "unrelated_relationship" false "1"
  assert_not_contains "$response $trace $calls" "$excluded" "excluded-relationship-sentinel"
  assert_not_contains "$response $trace $calls" "$unrelated" "unrelated-relationship-sentinel"
  assert_ids_absent_from_json "$response" "$excluded_ids_json" "relationship-response-excluded-ids"
  assert_ids_absent_from_json "$calls" "$excluded_ids_json" "relationship-provider-excluded-ids"
  assert_ids_absent_from_json "$trace" "$excluded_ids_json" "relationship-trace-excluded-ids"
  assert_not_contains "$calls" "filtering-only relationship evidence" "relationship-evidence-text"

  cr_post "/v1/relationships/edges/revoke" "$(jq -nc --arg owner "$owner" --arg rel "$rel_good" '{request_id:"rid-wave3b-rel-revoke",owner_id:$owner,conversation_id:"conv-rel",surface:"dev",relationship_id:$rel,evidence:{evidence_type:"user_confirmation",source_ref:"turn:wave3b",summary:"revoked",confidence_delta:0}}')" >/dev/null
  local revoked_relationship_before revoked_relationship_after revoked_before revoked_after revoked_response revoked_request revoked_trace revoked_calls revoked_excluded_ids_json
  revoked_excluded_ids_json="$(jq -nc --arg eligible "$eligible_message_id" --arg excluded "$excluded_message_id" --arg unrelated "$unrelated_message_id" '[$eligible, $excluded, $unrelated]')"
  revoked_relationship_before="$(relationship_select_log_count)"
  revoked_before="$(retrieval_log_count)"
  revoked_response="$(co_chat "$owner" "vscode" "vscode" "$conv" "What from memory should be used for the selected project relationship after revocation?" "private" "desktop_private" "false")"
  revoked_request="$(jq -r '.request_id' <<<"$revoked_response")"
  revoked_relationship_after="$(wait_relationship_select_log_delta "$revoked_relationship_before" 1 "$scenario revoked CO request CR relationship selection boundary")"
  revoked_after="$(wait_retrieval_log_delta "$revoked_before" 1 "$scenario revoked CO request BMS retrieval boundary")"
  assert_relationship_select_before_bms_retrieve "$revoked_relationship_before" "$revoked_before" "$scenario revoked CO request"
  revoked_trace="$(fetch_trace "$revoked_request")"
  revoked_calls="$(fetch_provider_calls "$revoked_request")"
  assert_jq_arg "$revoked_trace" rel "$rel_good" '(.retrieval.prompt_assembly.relationship_context.relationship_edges_used // [] | index($rel)) == null and (.retrieval.prompt_assembly.relationship_context.relationship_edges_excluded // [] | index($rel)) != null' "$scenario revoked relationship no longer selected"
  assert_jq "$revoked_trace" '.retrieval.prompt_assembly.retrieval_dispatch.relationship_projection_applied == false and .retrieval.prompt_assembly.retrieval_dispatch.relationship_id_count == 0 and .retrieval.prompt_assembly.retrieval_dispatch.entity_id_count == 0' "$scenario revoked request has no stale relationship projection"
  assert_jq "$revoked_trace" '(.retrieval.prompt_assembly.retrieval_dispatch.relationship_scope_projection.relationship_ids // []) == [] and (.retrieval.prompt_assembly.retrieval_dispatch.relationship_scope_projection.entity_ids // []) == []' "$scenario BMS request has no revoked relationship projection"
  assert_provider_sentinel "$revoked_calls" "$revoked_request" "eligible_relationship" false "1"
  assert_provider_sentinel "$revoked_calls" "$revoked_request" "excluded_relationship" false "1"
  assert_provider_sentinel "$revoked_calls" "$revoked_request" "unrelated_relationship" false "1"
  assert_not_contains "$revoked_response $revoked_trace $revoked_calls" "$eligible" "revoked-eligible-relationship-sentinel"
  assert_not_contains "$revoked_response $revoked_trace $revoked_calls" "$excluded" "revoked-excluded-relationship-sentinel"
  assert_not_contains "$revoked_response $revoked_trace $revoked_calls" "$unrelated" "revoked-unrelated-relationship-sentinel"
  assert_ids_absent_from_json "$revoked_response" "$revoked_excluded_ids_json" "revoked-relationship-response-ids"
  assert_ids_absent_from_json "$revoked_calls" "$revoked_excluded_ids_json" "revoked-relationship-provider-ids"
  assert_ids_absent_from_json "$revoked_trace" "$revoked_excluded_ids_json" "revoked-relationship-trace-ids"

  record_scenario "$scenario" "A4 assertions passed with CR select deltas normal=$((relationship_after - relationship_before)) revoked=$((revoked_relationship_after - revoked_relationship_before)), BMS retrieve deltas normal=$((after - before)) revoked=$((revoked_after - revoked_before)), eligible_relationship=$rel_good, excluded_relationship=$rel_bad, unrelated_relationship=$rel_unrelated"
  mark_acceptance "A4" "$scenario"
}

scenario_restraint_zero_call() {
  local scenario="restraint_zero_call_boundary"
  local owner="owner-wave3b-s3" conv policy before after response request_id trace calls control_before control_after control_response control_request control_trace current_turn suppressed_policy suppressed_domains suppressed_reasons control_policy memory_sentinel="W3B_RESTRAINT_MEMORY_4aa"
  conv="$(resolve_conversation "$owner" "web" "wave3b restraint")"
  policy="$(policy_json "general" "medium")"
  seed_message "$conv" "$owner" "web" "restraint seeded memory $memory_sentinel" "$policy" "{}" >/dev/null
  provider_post "/fixture/sentinels" "$(jq -nc --arg memory "$memory_sentinel" '{sentinels:{restraint_memory:$memory}}')"
  before="$(retrieval_log_count)"
  current_turn="What does this function do?"
  response="$(co_chat "$owner" "web" "web" "$conv" "$current_turn" "private" "desktop_private" "false")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  after="$(wait_retrieval_log_delta "$before" 0 "$scenario suppressed request BMS retrieval boundary")"
  trace="$(fetch_trace "$request_id")"
  calls="$(fetch_provider_calls "$request_id")"
  assert_jq "$trace" '.retrieval.prompt_assembly.restraint.retrieval_suppressed == true and (.retrieval.prompt_assembly.restraint.reason_summary // [] | index("retrieval_not_requested")) != null and (.retrieval.prompt_assembly.restraint.domains // [] | index("retrieval")) != null and .retrieval.prompt_assembly.restraint.restraint_policy != "ask_clarifying_question"' "$scenario actual CR retrieval_suppressed restraint contract enforced"
  assert_jq "$trace" '.retrieval.prompt_assembly.retrieval_dispatch.bms_retrieval_call_suppressed == true and .retrieval.prompt_assembly.retrieval_dispatch.bms_retrieval_call_issued == false and .retrieval.prompt_assembly.retrieval_dispatch.suppression_or_dependency_reason == "retrieval_suppressed_true"' "$scenario suppressed trace"
  assert_jq "$trace" '([.retrieval.bundle.semantic[]?] | length) == 0 and ([.retrieval.bundle.artifact_refs[]?] | length) == 0 and ((.sources // []) | length) == 0' "$scenario suppressed trace has no retained memory or sources"
  assert_provider_chat_calls "$calls" "$request_id" "1"
  assert_jq_arg "$calls" turn "$current_turn" '[.calls[]? | select(.kind == "chat") | .normalized_messages[]? | select(.role == "user") | (.content // "") | contains($turn)] | any' "$scenario provider receives current turn"
  assert_provider_sentinel "$calls" "$request_id" "restraint_memory" false "1"
  control_before="$(retrieval_log_count)"
  control_response="$(co_chat "$owner" "web" "web" "$conv" "What from memory is relevant?" "private" "desktop_private" "false")"
  control_request="$(jq -r '.request_id' <<<"$control_response")"
  control_after="$(wait_retrieval_log_delta "$control_before" 1 "$scenario explicit memory control BMS retrieval boundary")"
  control_trace="$(fetch_trace "$control_request")"
  assert_jq "$control_trace" '.retrieval.prompt_assembly.restraint.restraint_policy != "ask_clarifying_question" and .retrieval.prompt_assembly.restraint.retrieval_suppressed == false' "$scenario control restraint allows retrieval"
  assert_jq "$control_trace" '.retrieval.prompt_assembly.retrieval_dispatch.bms_retrieval_call_issued == true and .retrieval.prompt_assembly.retrieval_dispatch.bms_retrieval_call_suppressed == false' "$scenario control issued"
  suppressed_policy="$(jq -r '.retrieval.prompt_assembly.restraint.restraint_policy' <<<"$trace")"
  suppressed_domains="$(jq -c '.retrieval.prompt_assembly.restraint.domains // []' <<<"$trace")"
  suppressed_reasons="$(jq -c '.retrieval.prompt_assembly.restraint.reason_summary // []' <<<"$trace")"
  control_policy="$(jq -r '.retrieval.prompt_assembly.restraint.restraint_policy' <<<"$control_trace")"
  record_scenario "$scenario" "A5 assertions passed with suppressed_bms_delta=$((after - before)) suppressed_policy=$suppressed_policy suppressed_domains=$suppressed_domains suppressed_reasons=$suppressed_reasons and control_bms_delta=$((control_after - control_before)) control_policy=$control_policy"
  mark_acceptance "A5" "$scenario"
}

scenario_artifact_policy() {
  local scenario="artifact_policy_prelimit_filtering"
  local owner="owner-wave3b-s4" conv query artifact_limit=3 candidate_limit
  local eligible_code="W3B_ARTIFACT_ELIGIBLE_CODE_3b1" eligible_doc="W3B_ARTIFACT_ELIGIBLE_DOC_4c2"
  local blocked="W3B_ARTIFACT_BLOCKED_2aa" outside="W3B_ARTIFACT_OUTSIDE_6bc" sensitive="W3B_ARTIFACT_SENSITIVE_7cd"
  local unsupported="W3B_ARTIFACT_UNSUPPORTED_8de" malformed="W3B_ARTIFACT_MALFORMED_9ef" incomplete="W3B_ARTIFACT_INCOMPLETE_1f0"
  local unavailable="W3B_ARTIFACT_UNAVAILABLE_2f1" irrelevant="W3B_ARTIFACT_IRRELEVANT_3f2" credential="W3B_ARTIFACT_CREDENTIAL_5c9"
  candidate_limit="$(artifact_qdrant_candidate_limit "$artifact_limit")"
  conv="$(resolve_conversation "$owner" "vscode" "wave3b artifacts")"
  query="What from memory should I use from allowed project artifacts for Wave 3B?"
  local query_vector eligible_code_vector eligible_doc_vector crowd_vector irrelevant_vector
  query_vector="$(provider_embedding_vector "$query")"
  eligible_code_vector="$(json_vector_for_score "$query_vector" "0.62")"
  eligible_doc_vector="$(json_vector_for_score "$query_vector" "0.61")"
  crowd_vector="$(json_vector_for_score "$query_vector" "0.98")"
  irrelevant_vector="$(json_vector_for_score "$query_vector" "0.10")"

  local code_policy doc_policy blocked_policy outside_policy sensitive_policy unsupported_policy malformed_policy
  code_policy="$(policy_json "project" "medium" "code")"
  doc_policy="$(policy_json "project" "medium" "document")"
  blocked_policy="$(policy_json "finance" "medium" "code")"
  outside_policy="$(policy_json "personal" "medium" "code")"
  sensitive_policy="$(policy_json "project" "restricted" "code")"
  unsupported_policy="$(policy_json "project" "medium" "image")"
  malformed_policy='{"memory_domains":[],"sensitivity":"medium","content_class":"code"}'

  local eligible_code_pair eligible_doc_pair blocked_pair outside_pair sensitive_pair unsupported_pair malformed_pair incomplete_pair unavailable_pair irrelevant_pair
  local eligible_code_artifact eligible_code_derived eligible_doc_artifact eligible_doc_derived
  local blocked_artifact blocked_derived outside_artifact outside_derived sensitive_artifact sensitive_derived unsupported_artifact unsupported_derived
  local malformed_artifact malformed_derived incomplete_artifact incomplete_derived unavailable_artifact unavailable_derived irrelevant_artifact irrelevant_derived
  local missing_source_ref
  missing_source_ref="$(jq -nc --arg id "$(uuid_for "wave3b-missing-artifact-source")" '[{ref_type:"derived_text",ref_id:$id,support_kind:"direct"}]')"

  eligible_code_pair="$(seed_artifact "$owner" "vscode" "$conv" "eligible-code" "eligible project code artifact $eligible_code" "$code_policy" "completed" "complete" "$eligible_code_vector" "text/plain" "eligible-code.py")"
  eligible_doc_pair="$(seed_artifact "$owner" "vscode" "$conv" "eligible-doc" "eligible project document artifact $eligible_doc" "$doc_policy" "completed" "complete" "$eligible_doc_vector" "text/markdown" "eligible-doc.md")"
  blocked_pair="$(seed_artifact "$owner" "vscode" "$conv" "blocked-domain" "blocked domain artifact $blocked $credential" "$blocked_policy" "completed" "complete" "$crowd_vector" "text/plain" "blocked-domain.py")"
  outside_pair="$(seed_artifact "$owner" "vscode" "$conv" "outside-domain" "outside domain artifact $outside $credential" "$outside_policy" "completed" "complete" "$crowd_vector" "text/plain" "outside-domain.py")"
  sensitive_pair="$(seed_artifact "$owner" "vscode" "$conv" "too-sensitive" "restricted artifact $sensitive $credential" "$sensitive_policy" "completed" "complete" "$crowd_vector" "text/plain" "too-sensitive.py")"
  unsupported_pair="$(seed_artifact "$owner" "vscode" "$conv" "unsupported-class" "unsupported image artifact $unsupported $credential" "$unsupported_policy" "completed" "complete" "$crowd_vector" "image/png" "unsupported.png")"
  malformed_pair="$(seed_artifact "$owner" "vscode" "$conv" "malformed-policy" "malformed metadata artifact $malformed $credential" "$malformed_policy" "completed" "complete" "$crowd_vector" "text/plain" "malformed-policy.py")"
  incomplete_pair="$(seed_artifact "$owner" "vscode" "$conv" "incomplete-lifecycle" "incomplete lifecycle artifact $incomplete $credential" "$code_policy" "pending" "building" "$crowd_vector" "text/plain" "incomplete-lifecycle.py")"
  unavailable_pair="$(seed_artifact "$owner" "vscode" "$conv" "unavailable-source" "unavailable source artifact $unavailable $credential" "$code_policy" "completed" "complete" "$crowd_vector" "text/plain" "unavailable-source.py" "$missing_source_ref")"
  irrelevant_pair="$(seed_artifact "$owner" "vscode" "$conv" "irrelevant" "irrelevant project artifact $irrelevant $credential" "$code_policy" "completed" "complete" "$irrelevant_vector" "text/plain" "irrelevant.py")"

  eligible_code_artifact="${eligible_code_pair%%:*}"; eligible_code_derived="${eligible_code_pair#*:}"
  eligible_doc_artifact="${eligible_doc_pair%%:*}"; eligible_doc_derived="${eligible_doc_pair#*:}"
  blocked_artifact="${blocked_pair%%:*}"; blocked_derived="${blocked_pair#*:}"
  outside_artifact="${outside_pair%%:*}"; outside_derived="${outside_pair#*:}"
  sensitive_artifact="${sensitive_pair%%:*}"; sensitive_derived="${sensitive_pair#*:}"
  unsupported_artifact="${unsupported_pair%%:*}"; unsupported_derived="${unsupported_pair#*:}"
  malformed_artifact="${malformed_pair%%:*}"; malformed_derived="${malformed_pair#*:}"
  incomplete_artifact="${incomplete_pair%%:*}"; incomplete_derived="${incomplete_pair#*:}"
  unavailable_artifact="${unavailable_pair%%:*}"; unavailable_derived="${unavailable_pair#*:}"
  irrelevant_artifact="${irrelevant_pair%%:*}"; irrelevant_derived="${irrelevant_pair#*:}"

  local high_crowd_artifact_ids_json='[]' high_crowd_derived_ids_json='[]' high_crowd_count=0 crowd_pair crowd_artifact crowd_derived index
  for crowd_pair in "$blocked_pair" "$outside_pair" "$sensitive_pair" "$unsupported_pair" "$malformed_pair" "$incomplete_pair" "$unavailable_pair"; do
    crowd_artifact="${crowd_pair%%:*}"
    crowd_derived="${crowd_pair#*:}"
    high_crowd_artifact_ids_json="$(jq -c --arg id "$crowd_artifact" '. + [$id]' <<<"$high_crowd_artifact_ids_json")"
    high_crowd_derived_ids_json="$(jq -c --arg id "$crowd_derived" '. + [$id]' <<<"$high_crowd_derived_ids_json")"
    high_crowd_count=$((high_crowd_count + 1))
  done
  for index in $(seq 1 9); do
    crowd_pair="$(seed_artifact "$owner" "vscode" "$conv" "blocked-domain-extra-$index" "blocked domain crowd artifact $credential" "$blocked_policy" "completed" "complete" "$crowd_vector" "text/plain" "blocked-domain-extra-$index.py")"
    crowd_artifact="${crowd_pair%%:*}"; crowd_derived="${crowd_pair#*:}"
    high_crowd_artifact_ids_json="$(jq -c --arg id "$crowd_artifact" '. + [$id]' <<<"$high_crowd_artifact_ids_json")"
    high_crowd_derived_ids_json="$(jq -c --arg id "$crowd_derived" '. + [$id]' <<<"$high_crowd_derived_ids_json")"
    high_crowd_count=$((high_crowd_count + 1))
    crowd_pair="$(seed_artifact "$owner" "vscode" "$conv" "outside-domain-extra-$index" "outside domain crowd artifact $credential" "$outside_policy" "completed" "complete" "$crowd_vector" "text/plain" "outside-domain-extra-$index.py")"
    crowd_artifact="${crowd_pair%%:*}"; crowd_derived="${crowd_pair#*:}"
    high_crowd_artifact_ids_json="$(jq -c --arg id "$crowd_artifact" '. + [$id]' <<<"$high_crowd_artifact_ids_json")"
    high_crowd_derived_ids_json="$(jq -c --arg id "$crowd_derived" '. + [$id]' <<<"$high_crowd_derived_ids_json")"
    high_crowd_count=$((high_crowd_count + 1))
    crowd_pair="$(seed_artifact "$owner" "vscode" "$conv" "too-sensitive-extra-$index" "restricted crowd artifact $credential" "$sensitive_policy" "completed" "complete" "$crowd_vector" "text/plain" "too-sensitive-extra-$index.py")"
    crowd_artifact="${crowd_pair%%:*}"; crowd_derived="${crowd_pair#*:}"
    high_crowd_artifact_ids_json="$(jq -c --arg id "$crowd_artifact" '. + [$id]' <<<"$high_crowd_artifact_ids_json")"
    high_crowd_derived_ids_json="$(jq -c --arg id "$crowd_derived" '. + [$id]' <<<"$high_crowd_derived_ids_json")"
    high_crowd_count=$((high_crowd_count + 1))
    crowd_pair="$(seed_artifact "$owner" "vscode" "$conv" "unsupported-class-extra-$index" "unsupported image crowd artifact $credential" "$unsupported_policy" "completed" "complete" "$crowd_vector" "image/png" "unsupported-extra-$index.png")"
    crowd_artifact="${crowd_pair%%:*}"; crowd_derived="${crowd_pair#*:}"
    high_crowd_artifact_ids_json="$(jq -c --arg id "$crowd_artifact" '. + [$id]' <<<"$high_crowd_artifact_ids_json")"
    high_crowd_derived_ids_json="$(jq -c --arg id "$crowd_derived" '. + [$id]' <<<"$high_crowd_derived_ids_json")"
    high_crowd_count=$((high_crowd_count + 1))
    crowd_pair="$(seed_artifact "$owner" "vscode" "$conv" "malformed-policy-extra-$index" "malformed metadata crowd artifact $credential" "$malformed_policy" "completed" "complete" "$crowd_vector" "text/plain" "malformed-policy-extra-$index.py")"
    crowd_artifact="${crowd_pair%%:*}"; crowd_derived="${crowd_pair#*:}"
    high_crowd_artifact_ids_json="$(jq -c --arg id "$crowd_artifact" '. + [$id]' <<<"$high_crowd_artifact_ids_json")"
    high_crowd_derived_ids_json="$(jq -c --arg id "$crowd_derived" '. + [$id]' <<<"$high_crowd_derived_ids_json")"
    high_crowd_count=$((high_crowd_count + 1))
    crowd_pair="$(seed_artifact "$owner" "vscode" "$conv" "incomplete-lifecycle-extra-$index" "incomplete lifecycle crowd artifact $credential" "$code_policy" "pending" "building" "$crowd_vector" "text/plain" "incomplete-lifecycle-extra-$index.py")"
    crowd_artifact="${crowd_pair%%:*}"; crowd_derived="${crowd_pair#*:}"
    high_crowd_artifact_ids_json="$(jq -c --arg id "$crowd_artifact" '. + [$id]' <<<"$high_crowd_artifact_ids_json")"
    high_crowd_derived_ids_json="$(jq -c --arg id "$crowd_derived" '. + [$id]' <<<"$high_crowd_derived_ids_json")"
    high_crowd_count=$((high_crowd_count + 1))
  done
  test "$high_crowd_count" -gt "$candidate_limit" || { echo "wave3b-composed-smoke assertion failed: $scenario high-scoring crowd does not exceed candidate limit" >&2; exit 1; }

  local fixture_ids_json negative_artifact_ids_json crowd_derived_ids_json all_negative_sentinels_json
  fixture_ids_json="$(jq -nc \
    --arg code "$eligible_code_artifact" --arg code_derived "$eligible_code_derived" \
    --arg doc "$eligible_doc_artifact" --arg doc_derived "$eligible_doc_derived" \
    --arg blocked "$blocked_artifact" --arg blocked_derived "$blocked_derived" \
    --arg outside "$outside_artifact" --arg outside_derived "$outside_derived" \
    --arg sensitive "$sensitive_artifact" --arg sensitive_derived "$sensitive_derived" \
    --arg unsupported "$unsupported_artifact" --arg unsupported_derived "$unsupported_derived" \
    --arg malformed "$malformed_artifact" --arg malformed_derived "$malformed_derived" \
    --arg incomplete "$incomplete_artifact" --arg incomplete_derived "$incomplete_derived" \
    --arg unavailable "$unavailable_artifact" --arg unavailable_derived "$unavailable_derived" \
    --arg irrelevant "$irrelevant_artifact" --arg irrelevant_derived "$irrelevant_derived" \
    '{
      positive:{eligible_code:{artifact_id:$code,derived_text_id:$code_derived,content_class:"code"},eligible_document:{artifact_id:$doc,derived_text_id:$doc_derived,content_class:"document"}},
      negative:{
        blocked_domain:{artifact_id:$blocked,derived_text_id:$blocked_derived},
        outside_domain:{artifact_id:$outside,derived_text_id:$outside_derived},
        sensitivity_above_ceiling:{artifact_id:$sensitive,derived_text_id:$sensitive_derived},
        unsupported_content_class:{artifact_id:$unsupported,derived_text_id:$unsupported_derived},
        malformed_policy_metadata:{artifact_id:$malformed,derived_text_id:$malformed_derived},
        incomplete_lifecycle:{artifact_id:$incomplete,derived_text_id:$incomplete_derived},
        unavailable_source:{artifact_id:$unavailable,derived_text_id:$unavailable_derived},
        irrelevant:{artifact_id:$irrelevant,derived_text_id:$irrelevant_derived}
      }
    }')"
  assert_jq "$fixture_ids_json" '(.positive | to_entries | length) == 2 and (.negative | to_entries | length) == 8 and ([.. | objects | select(has("artifact_id") and has("derived_text_id")) | select(.artifact_id != "" and .derived_text_id != "")] | length) == 10' "$scenario every artifact fixture group created"
  negative_artifact_ids_json="$(jq -c --arg irrelevant_id "$irrelevant_artifact" '. + [$irrelevant_id]' <<<"$high_crowd_artifact_ids_json")"
  crowd_derived_ids_json="$high_crowd_derived_ids_json"
  all_negative_sentinels_json="$(jq -nc --arg blocked "$blocked" --arg outside "$outside" --arg sensitive "$sensitive" --arg unsupported "$unsupported" --arg malformed "$malformed" --arg incomplete "$incomplete" --arg unavailable "$unavailable" --arg irrelevant "$irrelevant" '[$blocked,$outside,$sensitive,$unsupported,$malformed,$incomplete,$unavailable,$irrelevant]')"

  provider_post "/fixture/sentinels" "$(jq -nc \
    --arg eligible_code "$eligible_code" --arg eligible_doc "$eligible_doc" \
    --arg blocked "$blocked" --arg outside "$outside" --arg sensitive "$sensitive" --arg unsupported "$unsupported" \
    --arg malformed "$malformed" --arg incomplete "$incomplete" --arg unavailable "$unavailable" --arg irrelevant "$irrelevant" \
    --arg credential "$credential" \
    '{sentinels:{eligible_artifact_code:$eligible_code,eligible_artifact_doc:$eligible_doc,blocked_artifact:$blocked,outside_artifact:$outside,sensitive_artifact:$sensitive,unsupported_artifact:$unsupported,malformed_artifact:$malformed,incomplete_artifact:$incomplete,unavailable_artifact:$unavailable,irrelevant_artifact:$irrelevant,artifact_credential:$credential}}')"

  local score_evidence eligible_code_score eligible_doc_score min_ineligible_score eligible_code_rank eligible_doc_rank raw_qdrant_limit
  raw_qdrant_limit=$((high_crowd_count + 4))
  score_evidence="$(qdrant_artifact_scores "$owner" "$query_vector" "$raw_qdrant_limit")"
  assert_artifact_score_ordering "$score_evidence" "$eligible_code_derived" "$eligible_doc_derived" "$crowd_derived_ids_json" "$high_crowd_count" "$candidate_limit" "$scenario ineligible artifact crowd scores above both eligibles beyond candidate limit"
  eligible_code_score="$(score_for_derived_text_id "$score_evidence" "$eligible_code_derived")"
  eligible_doc_score="$(score_for_derived_text_id "$score_evidence" "$eligible_doc_derived")"
  eligible_code_rank="$(rank_for_derived_text_id "$score_evidence" "$eligible_code_derived")"
  eligible_doc_rank="$(rank_for_derived_text_id "$score_evidence" "$eligible_doc_derived")"
  min_ineligible_score="$(jq -r --argjson crowd "$crowd_derived_ids_json" '[.result[]? | select(.payload.derived_text_id as $id | $crowd | index($id)) | .score] | min // empty' <<<"$score_evidence")"
  assert_jq_arg "$score_evidence" id "$irrelevant_derived" '([.result[]? | select(.payload.derived_text_id == $id)] | length) == 1' "$scenario irrelevant artifact score evidence present"

  local before after response request_id trace calls observer_capture captured_body captured_policy
  bms_observer_reset
  before="$(retrieval_log_count)"
  response="$(co_chat "$owner" "vscode" "vscode" "$conv" "$query" "private" "desktop_private" "true" "0.5")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  after="$(wait_retrieval_log_delta "$before" 1 "$scenario normal CO artifact request BMS retrieval boundary")"
  observer_capture="$(bms_observer_requests)"
  assert_jq_arg "$observer_capture" rid "$request_id" '.request_count == 1 and .forwarded_count == 1 and ([.requests[]? | select(.request_id == $rid and .method == "POST" and (.path | test("^/v2/conversations/[^/]+/retrieve$")))] | length) == 1' "$scenario observer captured and forwarded exactly one normal CO retrieval"
  assert_jq "$observer_capture" '([.requests[]?.headers | has("x-api-key")] | any) == false and ([.requests[]?.headers | has("authorization")] | any) == false' "$scenario observer does not expose BMS credentials"
  captured_body="$(jq -c --arg rid "$request_id" '.requests[] | select(.request_id == $rid) | .body' <<<"$observer_capture")"
  captured_policy="$(jq -c '.containment_policy' <<<"$captured_body")"
  assert_jq "$captured_body" '.owner_id != null and .query != null and .include_artifacts == null and .retrieval.k == 3 and .retrieval.min_score == 0.5 and .containment_policy.enforcement_mode == "mandatory" and (.containment_policy.artifact_access_policy | type == "object") and (.containment_policy | has("relationship_scope_projection"))' "$scenario captured normal CO request contains complete mandatory artifact policy"
  assert_jq "$captured_policy" '.artifact_access_policy.enforcement_mode == "mandatory" and (.artifact_access_policy.allowed_content_classes | type == "array" and length > 0) and (.artifact_access_policy.allowed_domains | type == "array" and length > 0) and (.artifact_access_policy.maximum_sensitivity | type == "string") and (.artifact_access_policy.surface_content_capabilities | type == "array" and length > 0) and (.artifact_access_policy.reason_codes | type == "array" and length > 0) and (.allowed_memory_domains | type == "array") and (.blocked_memory_domains | type == "array") and has("relationship_scope_projection")' "$scenario captured policy has complete bounded policy fields"
  trace="$(fetch_trace "$request_id")"
  calls="$(fetch_provider_calls "$request_id")"
  assert_jq "$trace" '.retrieval.prompt_assembly.persona_containment.artifact_request_status == "mandatory_policy_forwarded" and .retrieval.prompt_assembly.persona_containment.artifact_result_status == "validated"' "$scenario artifact policy validated"
  assert_jq "$trace" '.retrieval.prompt_assembly.result_boundary.artifact_policy_applied == true and .retrieval.prompt_assembly.result_boundary.validation_status == "filtered" and (.retrieval.prompt_assembly.result_boundary.retained_counts.artifact_refs // 0) >= 2' "$scenario CO artifact result boundary validated"
  assert_jq_arg "$trace" id "$eligible_code_artifact" '([.retrieval.bundle.artifact_refs[]? | select(.artifact_id == $id)] | length) == 1' "$scenario eligible code artifact retained by CO"
  assert_jq_arg "$trace" id "$eligible_doc_artifact" '([.retrieval.bundle.artifact_refs[]? | select(.artifact_id == $id)] | length) == 1' "$scenario eligible document artifact retained by CO"
  assert_jq_argjson "$trace" negatives "$negative_artifact_ids_json" '([.retrieval.bundle.artifact_refs[]? | select(.artifact_id as $id | $negatives | index($id))] | length) == 0' "$scenario negative artifacts absent from CO retained bundle"

  local direct_bms_request direct_bms_payload direct_bms_response direct_debug_json
  direct_bms_request="wave3b-direct-artifact"
  direct_bms_payload="$(jq -c \
    --arg request_id "$direct_bms_request" \
    '. + {request_id:$request_id}' <<<"$captured_body")"
  assert_jq_two_argjson "$direct_bms_payload" captured "$captured_policy" observed "$(jq -c '.containment_policy' <<<"$direct_bms_payload")" '$captured == $observed' "$scenario direct BMS request uses exact captured normal CO policy"
  assert_jq_argjson "$direct_bms_payload" captured "$captured_body" '.owner_id == $captured.owner_id and .query == $captured.query and .retrieval == $captured.retrieval and .include_artifacts == $captured.include_artifacts and .containment_policy == $captured.containment_policy' "$scenario direct BMS request preserves captured owner query retrieval artifact state and policy"
  direct_bms_response="$(bms_retrieve_bundle "$conv" "$direct_bms_request" "$direct_bms_payload")"
  if ! jq -e \
    --arg code "$eligible_code_artifact" \
    --arg doc "$eligible_doc_artifact" \
    --argjson negatives "$negative_artifact_ids_json" \
    --argjson limit "$artifact_limit" \
    --argjson candidate_limit "$candidate_limit" \
    --argjson crowd_count "$high_crowd_count" \
    --argjson code_rank "$eligible_code_rank" \
    --argjson doc_rank "$eligible_doc_rank" \
    --argjson min_ineligible "$min_ineligible_score" \
    '
      .bundle.retrieval_debug.containment_policy.pre_limit_policy_filter_applied == true
      and $crowd_count > $candidate_limit
      and $code_rank > $candidate_limit
      and $doc_rank > $candidate_limit
      and ((.bundle.retrieval_debug.artifact_ranked // 0) <= $limit)
      and ((.bundle.retrieval_debug.truth_qualification.derivative_omissions_by_reason.missing_derivative_source_record // 0) >= 1)
      and ((.diagnostics.reason_codes // [] | index("mandatory_containment_applied")) != null)
      and ((.diagnostics.reason_codes // [] | index("source_missing_or_unavailable")) != null)
      and ([.bundle.artifact_refs[]? | select(.artifact_id == $code)] | length) == 1
      and ([.bundle.artifact_refs[]? | select(.artifact_id == $doc)] | length) == 1
      and ([.bundle.artifact_refs[]? | select(.artifact_id as $id | $negatives | index($id))] | length) == 0
      and ([.bundle.artifact_refs[]?] | length) <= $limit
      and ([.bundle.artifact_refs[]? | select(.artifact_id == $code) | .relevance_score] | first) < $min_ineligible
      and ([.bundle.artifact_refs[]? | select(.artifact_id == $doc) | .relevance_score] | first) < $min_ineligible
    ' <<<"$direct_bms_response" >/dev/null; then
    echo "wave3b-composed-smoke assertion failed: $scenario direct BMS artifact pre-limit mandatory filtering before limiting" >&2
    exit 1
  fi
  assert_ids_absent_from_json "$direct_bms_response" "$negative_artifact_ids_json" "artifact-direct-bms-negative-artifact-ids"
  direct_debug_json="$(jq -c '.bundle.retrieval_debug' <<<"$direct_bms_response")"
  assert_not_contains "$direct_debug_json" "$credential" "artifact-direct-bms-diagnostics-credential"
  assert_not_contains "$direct_debug_json" "object_uri" "artifact-direct-bms-diagnostics-object-uri"
  assert_not_contains "$direct_debug_json" "signed_url" "artifact-direct-bms-diagnostics-signed-url"
  assert_provider_sentinel "$calls" "$request_id" "eligible_artifact_code" true "1"
  assert_provider_sentinel "$calls" "$request_id" "eligible_artifact_doc" true "1"
  assert_provider_sentinel "$calls" "$request_id" "blocked_artifact" false "1"
  assert_provider_sentinel "$calls" "$request_id" "outside_artifact" false "1"
  assert_provider_sentinel "$calls" "$request_id" "sensitive_artifact" false "1"
  assert_provider_sentinel "$calls" "$request_id" "unsupported_artifact" false "1"
  assert_provider_sentinel "$calls" "$request_id" "malformed_artifact" false "1"
  assert_provider_sentinel "$calls" "$request_id" "incomplete_artifact" false "1"
  assert_provider_sentinel "$calls" "$request_id" "unavailable_artifact" false "1"
  assert_provider_sentinel "$calls" "$request_id" "irrelevant_artifact" false "1"
  assert_provider_sentinel "$calls" "$request_id" "artifact_credential" false "1"
  assert_ids_absent_from_json "$response" "$negative_artifact_ids_json" "artifact-response-negative-artifact-ids"
  assert_ids_absent_from_json "$calls" "$negative_artifact_ids_json" "artifact-provider-negative-artifact-ids"
  assert_ids_absent_from_json "$trace" "$negative_artifact_ids_json" "artifact-trace-negative-artifact-ids"
  assert_not_contains "$response $trace $calls" "$credential" "artifact-credential"
  while IFS= read -r sentinel; do
    [ -n "$sentinel" ] || continue
    assert_not_contains "$response $trace $calls" "$sentinel" "artifact-negative-sentinel"
  done < <(jq -r '.[]' <<<"$all_negative_sentinels_json")
  assert_public_source_allowlist "$response" "$eligible_code_artifact" "$scenario public sources allowlist"
  assert_jq_argjson "$response" negatives "$negative_artifact_ids_json" '(.sources | length > 0) and ([.sources[]? | select(.artifact_id as $id | $negatives | index($id))] | length) == 0' "$scenario public sources exclude negative artifacts"
  for forbidden in "$credential" "memory://wave3b" "object_uri" "download_url" "signed_url" "credentials" "policy_metadata" "provenance" "source_checks" "freshness_state" "durable_status"; do
    assert_not_contains "$response" "$forbidden" "artifact-public-source-forbidden-$forbidden"
  done
  record_scenario "$scenario" "A6 and artifact A7 assertions passed with artifact_limit=$artifact_limit qdrant_candidate_limit=$candidate_limit high_scoring_crowd_count=$high_crowd_count eligible_code_artifact=$eligible_code_artifact eligible_doc_artifact=$eligible_doc_artifact eligible_code_score=$eligible_code_score eligible_doc_score=$eligible_doc_score eligible_code_raw_rank=$eligible_code_rank eligible_doc_raw_rank=$eligible_doc_rank min_ineligible_score=$min_ineligible_score normal_bms_delta=$((after - before))"
  mark_acceptance "A6" "$scenario"
  mark_acceptance "A7_artifact" "$scenario"
}

scenario_fallback_identity() {
  local scenario="fallback_identity"
  local owner="owner-wave3b-s5" conv sentinel="W3B_FALLBACK_ALLOWED_33e" artifact_sentinel="W3B_FALLBACK_ART_23a" blocked="W3B_FALLBACK_BLOCKED_44f" blocked_artifact_sentinel="W3B_FALLBACK_BLOCKED_ART_12c" policy artifact_policy response request_id calls trace before after allowed_message_id blocked_message_id allowed_artifact_pair allowed_artifact_id blocked_artifact_pair blocked_artifact_id query query_vector allowed_artifact_vector blocked_artifact_vector allowed_source_refs blocked_source_refs observer_capture
  conv="$(resolve_conversation "$owner" "vscode" "wave3b fallback")"
  query="What from memory tests fallback scoped context?"
  query_vector="$(provider_embedding_vector "$query")"
  allowed_artifact_vector="$(json_vector_for_score "$query_vector" "0.999")"
  blocked_artifact_vector="$(json_vector_for_score "$query_vector" "0.998")"
  policy="$(policy_json "technical" "medium")"
  artifact_policy="$(policy_json "technical" "medium" "document")"
  allowed_message_id="$(seed_message "$conv" "$owner" "vscode" "fallback eligible memory $sentinel" "$policy" "{}")"
  blocked_message_id="$(seed_message "$conv" "$owner" "vscode" "fallback blocked finance memory $blocked" "$(policy_json "finance" "medium")" "{}")"
  allowed_source_refs="$(jq -nc --arg id "$allowed_message_id" '[{ref_type:"message",ref_id:$id,support_kind:"direct"}]')"
  blocked_source_refs="$(jq -nc --arg id "$blocked_message_id" '[{ref_type:"message",ref_id:$id,support_kind:"direct"}]')"
  allowed_artifact_pair="$(seed_artifact "$owner" "vscode" "$conv" "fallback-allowed" "fallback allowed artifact $artifact_sentinel" "$artifact_policy" "completed" "complete" "$allowed_artifact_vector" "text/markdown" "fallback-allowed.md" "$allowed_source_refs")"
  allowed_artifact_id="${allowed_artifact_pair%%:*}"
  seed_active_memory_item_for_source_ref "$owner" "derived_text" "${allowed_artifact_pair#*:}" "fallback active artifact lifecycle"
  blocked_artifact_pair="$(seed_artifact "$owner" "vscode" "$conv" "fallback-blocked" "fallback blocked artifact $blocked_artifact_sentinel" "$(policy_json "finance" "medium" "code")" "completed" "complete" "$blocked_artifact_vector" "text/plain" "fallback-blocked.py" "$blocked_source_refs")"
  blocked_artifact_id="${blocked_artifact_pair%%:*}"
  provider_post "/fixture/sentinels" "$(jq -nc \
    --arg sentinel "$sentinel" \
    --arg artifact "$artifact_sentinel" \
    --arg blocked "$blocked" \
    --arg blocked_artifact "$blocked_artifact_sentinel" \
    '{sentinels:{fallback_allowed:$sentinel,fallback_artifact:$artifact,fallback_blocked:$blocked,fallback_blocked_artifact:$blocked_artifact}}')"
  provider_post "/fixture/fail-next-primary"
  bms_observer_reset
  before="$(retrieval_log_count)"
  response="$(co_chat "$owner" "vscode" "vscode" "$conv" "$query" "private" "desktop_private" "true" "0")"
  after="$(wait_retrieval_log_delta "$before" 1 "$scenario exactly one normal CO to BMS retrieval")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  calls="$(fetch_provider_calls "$request_id")"
  trace="$(fetch_trace "$request_id")"
  observer_capture="$(bms_observer_requests)"
  assert_jq_arg "$observer_capture" rid "$request_id" '.request_count == 1 and .forwarded_count == 1 and ([.requests[]? | select(.request_id == $rid and .method == "POST" and (.path | test("^/v2/conversations/[^/]+/retrieve$")))] | length) == 1' "$scenario observer captured exactly one normal CO retrieval"
  assert_jq "$calls" '[.calls[] | select(.kind=="chat")] | length == 2' "$scenario exactly two provider attempts"
  assert_jq "$calls" '[.calls[] | select(.kind=="chat") | .status] == ["failed","ok"]' "$scenario first provider attempt fails and fallback succeeds"
  assert_jq "$calls" '[.calls[] | select(.kind=="chat") | .normalized_messages] | .[0] == .[1]' "$scenario normalized messages identical"
  assert_jq "$calls" '[.calls[] | select(.kind=="chat") | .prompt_fingerprint] | .[0] == .[1]' "$scenario prompt fingerprints identical"
  assert_jq "$calls" '[.calls[] | select(.kind=="chat") | .message_count] | .[0] == .[1]' "$scenario message counts identical"
  assert_jq "$calls" '[.calls[] | select(.kind=="chat") | (.normalized_messages | map(.role))] | .[0] == .[1]' "$scenario role sequence identical"
  assert_jq "$trace" '(.model_calls | length) == 2 and [.model_calls[].status] == ["failed","ok"] and [.model_calls[].attempt_ordinal] == [1,2]' "$scenario persisted two provider attempts with failure then success"
  assert_jq "$trace" '.model_calls[0].prompt_fingerprint == .model_calls[1].prompt_fingerprint and .model_calls[0].prompt_message_count == .model_calls[1].prompt_message_count and .model_calls[0].prompt_role_sequence == .model_calls[1].prompt_role_sequence' "$scenario per-attempt prompt identity evidence identical"
  assert_jq "$trace" '.model_calls[0].retained_semantic_message_ids == .model_calls[1].retained_semantic_message_ids and .model_calls[0].retained_artifact_ids == .model_calls[1].retained_artifact_ids and (.model_calls[0].retained_semantic_message_ids | length) > 0 and (.model_calls[0].retained_artifact_ids | length) > 0' "$scenario per-attempt retained IDs identical and non-empty"
  assert_jq_arg "$trace" id "$allowed_message_id" '(.model_calls[0].retained_semantic_message_ids | index($id)) != null and (.model_calls[1].retained_semantic_message_ids | index($id)) != null' "$scenario allowed semantic memory visible to both attempts"
  assert_jq_arg "$trace" id "$allowed_artifact_id" '(.model_calls[0].retained_artifact_ids | index($id)) != null and (.model_calls[1].retained_artifact_ids | index($id)) != null' "$scenario allowed artifact visible to both attempts"
  assert_jq_arg "$trace" id "$blocked_message_id" '([.model_calls[]?.retained_semantic_message_ids[]? | select(. == $id)] | length) == 0' "$scenario blocked semantic memory absent from both attempts"
  assert_jq_arg "$trace" id "$blocked_artifact_id" '([.model_calls[]?.retained_artifact_ids[]? | select(. == $id)] | length) == 0' "$scenario blocked artifact absent from both attempts"
  assert_jq_arg "$trace" id "$allowed_message_id" '([.retrieval.bundle.semantic[]? | select(.message_id == $id)] | length) == 1' "$scenario allowed semantic retained after BMS result validation"
  assert_jq_arg "$trace" id "$allowed_artifact_id" '([.retrieval.bundle.artifact_refs[]? | select(.artifact_id == $id)] | length) == 1' "$scenario allowed artifact retained after BMS result validation"
  assert_jq_arg "$trace" id "$blocked_message_id" '([.retrieval.bundle.semantic[]? | select(.message_id == $id)] | length) == 0' "$scenario omitted semantic record does not reappear"
  assert_jq_arg "$trace" id "$blocked_artifact_id" '([.retrieval.bundle.artifact_refs[]? | select(.artifact_id == $id)] | length) == 0' "$scenario omitted artifact record does not reappear"
  assert_provider_sentinel "$calls" "$request_id" "fallback_allowed" true "2"
  assert_provider_sentinel "$calls" "$request_id" "fallback_artifact" true "2"
  assert_provider_sentinel "$calls" "$request_id" "fallback_blocked" false "2"
  assert_provider_sentinel "$calls" "$request_id" "fallback_blocked_artifact" false "2"
  assert_jq "$trace" '.retrieval.prompt_assembly.provider_fallback_context.same_sanitized_messages_reused == true' "$scenario bounded trace fallback identity"
  record_scenario "$scenario" "A8 assertions passed with normal_bms_delta=$((after - before)) provider_attempts=2 retained_semantic_id=$allowed_message_id retained_artifact_id=$allowed_artifact_id"
  mark_acceptance "A8" "$scenario"
}

scenario_privacy_safe_diagnostics() {
  local scenario="privacy_safe_diagnostics"
  local owner="owner-wave3b-s6" conv msg="W3B_PRIV_MSG_91a" artifact="W3B_PRIV_ART_82b" meta="W3B_PRIV_META_73c" url="W3B_PRIV_URL_64d" cred="W3B_PRIV_CRED_55e" provenance="W3B_PRIV_PROV_38d" rel="W3B_PRIV_REL_46f" query query_vector artifact_vector
  conv="$(resolve_conversation "$owner" "vscode" "wave3b privacy")"
  query="What from memory can be safely summarized for public glasses?"
  query_vector="$(provider_embedding_vector "$query")"
  artifact_vector="$(json_vector_for_score "$query_vector" "0.997")"
  local policy message_id artifact_pair artifact_id derived_id source_refs restricted_entity restricted_rel relationship_fixture
  policy="$(policy_json "project" "medium" "code")"
  message_id="$(seed_message "$conv" "$owner" "vscode" "privacy message content $msg" "$policy" "$(jq -nc --arg meta "$meta" '{credential_bearing_metadata:{api_token:$meta}}')")"
  source_refs="$(jq -nc --arg msg_id "$message_id" --arg cred "$cred" --arg provenance "$provenance" '[{ref_type:"message",ref_id:$msg_id,support_kind:"direct",metadata:{credential_hint:$cred,provenance_marker:$provenance},note:$provenance}]')"
  artifact_pair="$(seed_artifact "$owner" "vscode" "$conv" "privacy" "privacy artifact snippet $artifact" "$policy" "completed" "complete" "$artifact_vector" "text/plain" "privacy.py" "$source_refs")"
  artifact_id="${artifact_pair%%:*}"
  derived_id="${artifact_pair#*:}"
  seed_active_memory_item_for_source_ref "$owner" "derived_text" "$derived_id" "privacy active artifact lifecycle"
  psql_exec >/dev/null -c "UPDATE artifacts SET object_uri = 'https://storage.invalid/private/$url?token=$cred' WHERE id = '$artifact_id';"
  restricted_entity="repo:privacy-restricted"
  restricted_rel="rel_privacy_restricted"
  cr_post "/v1/relationships/entities/upsert" "$(jq -nc --arg owner "$owner" --argjson entity "$(relationship_entity_json "project:wave3b" "Wave 3B Project")" '{request_id:"rid-wave3b-privacy-project",owner_id:$owner,conversation_id:"conv-privacy",surface:"dev",entity:$entity}')" >/dev/null
  cr_post "/v1/relationships/entities/upsert" "$(jq -nc --arg owner "$owner" --argjson entity "$(relationship_entity_json "$restricted_entity" "Restricted privacy repo" "repository" "technical")" '{request_id:"rid-wave3b-privacy-entity",owner_id:$owner,conversation_id:"conv-privacy",surface:"dev",entity:$entity}')" >/dev/null
  relationship_fixture="$(jq -nc \
    --arg owner "$owner" \
    --arg rid "$restricted_rel" \
    --arg entity "$restricted_entity" \
    --arg relsent "$rel" \
    --argjson edge "$(relationship_edge_json "$restricted_rel" "$restricted_entity" "active" "restricted" "" "references" "project_context")" \
    '{request_id:"rid-wave3b-privacy-rel",owner_id:$owner,conversation_id:"conv-privacy",surface:"dev",edge:($edge + {relationship_id:$rid,object_entity_id:$entity,source_type:"explicit_user_confirmation",source_refs_json:[$relsent],sensitivity_level:"restricted"}),evidence:[{evidence_type:"user_confirmation",source_ref:$relsent,summary:$relsent,confidence_delta:0.1}]}')"
  assert_jq_arg "$relationship_fixture" sentinel "$rel" '.edge.source_refs_json == [$sentinel] and .evidence[0].source_ref == $sentinel and .evidence[0].summary == $sentinel' "$scenario restricted relationship fixture contains sentinel in source refs and evidence"
  cr_post "/v1/relationships/edges/upsert" "$relationship_fixture" >/dev/null
  local message_row artifact_row derived_row
  message_row="$(psql_value -c "SELECT content || '|' || metadata::text FROM messages WHERE id = '$message_id';")"
  artifact_row="$(psql_value -c "SELECT object_uri FROM artifacts WHERE id = '$artifact_id';")"
  derived_row="$(psql_value -c "SELECT derivation_params::text FROM derived_text WHERE id = '$derived_id';")"
  assert_not_contains "$message_row" "__missing_privacy_fixture__" "$scenario message fixture inspected"
  for sentinel in "$msg" "$meta"; do assert_not_contains "$sentinel" "__impossible__" "$scenario message sentinel defined"; done
  case "$message_row" in *"$msg"*"$meta"*) ;; *) echo "wave3b-composed-smoke assertion failed: $scenario message content/metadata sentinels missing before CO" >&2; exit 1 ;; esac
  case "$artifact_row" in *"$url"*"$cred"*) ;; *) echo "wave3b-composed-smoke assertion failed: $scenario artifact object_uri URL/credential sentinels missing before CO" >&2; exit 1 ;; esac
  case "$derived_row" in *"$cred"*"$provenance"*) ;; *) echo "wave3b-composed-smoke assertion failed: $scenario provenance metadata sentinels missing before CO" >&2; exit 1 ;; esac
  provider_post "/fixture/sentinels" "$(jq -nc \
    --arg msg "$msg" --arg artifact "$artifact" --arg meta "$meta" --arg url "$url" --arg cred "$cred" --arg provenance "$provenance" --arg rel "$rel" \
    '{sentinels:{privacy_msg:$msg,privacy_artifact:$artifact,privacy_meta:$meta,privacy_url:$url,privacy_credential:$cred,privacy_provenance:$provenance,privacy_relationship:$rel}}')"
  local response request_id trace calls
  response="$(co_chat "$owner" "vscode" "vscode" "$conv" "$query" "private" "glasses_public_or_semi_public" "true")"
  request_id="$(jq -r '.request_id' <<<"$response")"
  trace="$(fetch_trace "$request_id")"
  calls="$(fetch_provider_calls "$request_id")"
  assert_provider_chat_calls "$calls" "$request_id" "1"
  for label in privacy_msg privacy_artifact privacy_meta privacy_url privacy_credential privacy_provenance privacy_relationship; do
    local sentinel
    sentinel="$(jq -r --arg label "$label" '.sentinels[$label]' <<<"$(jq -nc --arg msg "$msg" --arg artifact "$artifact" --arg meta "$meta" --arg url "$url" --arg cred "$cred" --arg provenance "$provenance" --arg rel "$rel" '{sentinels:{privacy_msg:$msg,privacy_artifact:$artifact,privacy_meta:$meta,privacy_url:$url,privacy_credential:$cred,privacy_provenance:$provenance,privacy_relationship:$rel}}')")"
    assert_not_contains "$response" "$sentinel" "$label-response"
    assert_not_contains "$trace" "$sentinel" "$label-trace"
    assert_not_contains "$calls" "$sentinel" "$label-provider"
  done
  assert_provider_sentinel "$calls" "$request_id" "privacy_msg" false "1"
  assert_provider_sentinel "$calls" "$request_id" "privacy_artifact" false "1"
  assert_provider_sentinel "$calls" "$request_id" "privacy_url" false "1"
  assert_provider_sentinel "$calls" "$request_id" "privacy_credential" false "1"
  assert_provider_sentinel "$calls" "$request_id" "privacy_provenance" false "1"
  assert_provider_sentinel "$calls" "$request_id" "privacy_relationship" false "1"
  assert_jq "$response" '.sources == []' "$scenario public sources empty"
  assert_jq "$trace" '.retrieval.prompt_assembly.privacy_context.enforcement_required == true and (.references | length == 0) and .artifacts.reason == "privacy_suppressed" and (.artifacts.included_ids == []) and (.artifacts | has("ids") | not)' "$scenario privacy trace suppressed references and artifact ids"
  assert_jq "$trace" '.retrieval.prompt_assembly.retrieval.bundle == null or .retrieval.prompt_assembly.retrieval.bundle.semantic == null or (.retrieval.prompt_assembly.retrieval.bundle.semantic | length == 0)' "$scenario no unrestricted bundle payload"
  assert_jq "$trace" '((.model_calls // []) | all((.prompt_message_count // .message_count // 0) >= 0 and (.prompt_fingerprint | type == "string") and (has("retained_semantic_message_ids") | not) and (has("retained_artifact_ids") | not))) and ((.model_call // {}) | (.prompt_fingerprint? // "" | type == "string"))' "$scenario bounded model-call evidence retained without private retained IDs"
  assert_jq "$trace" '.retrieval.bundle.privacy_suppressed == true and (.retrieval.bundle.semantic_item_count // 0) >= 1 and (.retrieval.bundle.artifact_count // 0) >= 1' "$scenario bounded counts retained"
  assert_jq "$trace" '.retrieval.prompt_assembly.result_boundary.validation_status == "filtered" and (.retrieval.prompt_assembly.result_boundary.retained_counts.semantic // 0) >= 1 and (.retrieval.prompt_assembly.result_boundary.retained_counts.artifact_refs // 0) >= 1 and (.retrieval.prompt_assembly.result_boundary.omission_counts_by_reason | type == "object")' "$scenario bounded structural summaries retained"
  assert_jq "$calls" '([.calls[] | select(.kind=="chat") | .normalized_messages[]? | select(.content | contains("privacy message content") or contains("privacy artifact snippet"))] | length) == 0' "$scenario privacy suppression before provider prompt assembly"
  assert_not_contains "$response $trace $calls" "$message_id" "privacy-message-id-suppressed"
  assert_not_contains "$response $trace $calls" "$artifact_id" "privacy-artifact-id-suppressed"
  record_scenario "$scenario" "A9 assertions passed with structural sentinels message_id=$message_id artifact_id=$artifact_id derived_id=$derived_id"
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
  focused_packet="$(focused_packet_label)"
  jq -nc \
    --arg packet "$focused_packet" \
    --argjson scenarios "$scenario_json" \
    --argjson acceptance "$acceptance_json" \
    --argjson topology "$topology_json" \
    --argjson focused "$(if [ "$full_suite" = true ]; then echo false; else echo true; fi)" \
    '{
      packet_ok: true,
      packet: $packet,
      wave: "3B",
      focused: $focused,
      final_acceptance: false,
      topology: $topology,
      scenarios: $scenarios,
      acceptance: $acceptance
    }'
fi
