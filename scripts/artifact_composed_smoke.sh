#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BMS="$ROOT/../basic-memory-store"
COMPOSE="$ROOT/docker-compose.artifact-composed-smoke.yml"
BMS_WAVE2F_HEAD="919b670617e6749c0eee45e0192576e562490ac1"

for command in git docker curl jq python3; do
  command -v "$command" >/dev/null || {
    echo "artifact-composed-smoke prerequisite missing: $command" >&2
    exit 2
  }
done

test -d "$BMS/.git" || {
  echo "artifact-composed-smoke prerequisite missing: sibling repository $BMS" >&2
  exit 2
}

git -C "$BMS" merge-base --is-ancestor "$BMS_WAVE2F_HEAD" main || {
  echo "basic-memory-store/main does not contain required Wave 2F merge head $BMS_WAVE2F_HEAD" >&2
  exit 2
}

cleanup() {
  docker compose -f "$COMPOSE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker compose -f "$COMPOSE" up -d --build --wait

provider_post() {
  local path="$1" body
  if [ "$#" -ge 2 ]; then
    body="$2"
  else
    body="{}"
  fi
  curl_json "provider POST $path" -X POST "http://127.0.0.1:14481$path" \
    -H "Content-Type: application/json" \
    -d "$body" >/dev/null
}

curl_json() {
  local label="$1"
  shift
  local tmp status
  tmp="$(mktemp)"
  status="$(curl -sS -o "$tmp" -w "%{http_code}" "$@")"
  if [ "$status" -lt 200 ] || [ "$status" -ge 300 ]; then
    echo "artifact-composed-smoke HTTP $status at $label" >&2
    python3 - "$tmp" <<'PY' >&2
import json
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
for token in ("X-Amz-", "Signature=", "Credential=", "127.0.0.1:14400"):
    text = text.replace(token, "[redacted]")
try:
    parsed = json.loads(text)
    text = json.dumps(parsed, sort_keys=True)[:800]
except Exception:
    text = text[:800]
print(text)
PY
    rm -f "$tmp"
    exit 1
  fi
  cat "$tmp"
  rm -f "$tmp"
}

curl_expect_success() {
  local label="$1"
  shift
  local tmp status
  tmp="$(mktemp)"
  status="$(curl -sS -o "$tmp" -w "%{http_code}" "$@")"
  if [ "$status" -lt 200 ] || [ "$status" -ge 300 ]; then
    echo "artifact-composed-smoke HTTP $status at $label" >&2
    python3 - "$tmp" <<'PY' >&2
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
for token in ("X-Amz-", "Signature=", "Credential=", "127.0.0.1:14400"):
    text = text.replace(token, "[redacted]")
print(text[:800])
PY
    rm -f "$tmp"
    exit 1
  fi
  rm -f "$tmp"
}

bms_post() {
  curl_json "BMS POST $1" -X POST "http://127.0.0.1:14421$1" \
    -H "X-API-Key: smoke-memory-key" \
    -H "Content-Type: application/json" \
    -d "$2"
}

bms_retrieve() {
  local conversation_id="$1" request_id="$2" owner="$3" query="$4"
  curl_json "BMS retrieve conversation" -X POST "http://127.0.0.1:14421/v2/conversations/$conversation_id/retrieve" \
    -H "X-API-Key: smoke-memory-key" \
    -H "X-Request-ID: $request_id" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc \
      --arg request_id "$request_id" \
      --arg owner "$owner" \
      --arg query "$query" \
      '{
        request_id:$request_id,
        owner_id:$owner,
        query:$query,
        include_artifacts:true,
        retrieval:{k:8,min_score:0,scope:"conversation",time_window:"all",retrieval_mode:"balanced"}
      }')"
}

co_chat() {
  local owner="$1" client="$2" conversation_id="$3" question="$4" sensitivity="$5" surface_category="$6"
  curl_json "CO POST /v1/chat" -X POST "http://127.0.0.1:14461/v1/chat" \
    -H "X-API-Key: smoke-orchestrator-key" \
    -H "Content-Type: application/json" \
    -d "$(jq -nc \
      --arg owner "$owner" \
      --arg client "$client" \
      --arg conversation "$conversation_id" \
      --arg question "$question" \
      --arg sensitivity "$sensitivity" \
      --arg surface_category "$surface_category" \
      '{
        owner_id:$owner,
        client_id:$client,
        conversation_id:$conversation,
        surface:"chat",
        surface_context:{surface_category:$surface_category},
        sensitivity:$sensitivity,
        messages:[{role:"user",content:$question}],
        retrieval:{k:8,min_score:0,scope:"conversation",time_window:"all",retrieval_mode:"balanced"}
      }')"
}

fetch_trace() {
  local request_id="$1"
  curl_json "BMS GET trace" "http://127.0.0.1:14421/v1/traces/$request_id" \
    -H "X-API-Key: smoke-memory-key"
}

fetch_provider_calls() {
  local request_id="$1"
  curl_json "provider calls" "http://127.0.0.1:14481/calls/$request_id"
}

resolve_conversation() {
  local owner="$1" client="$2" title="$3"
  bms_post "/v1/conversations/resolve" \
    "$(jq -nc --arg owner "$owner" --arg client "$client" --arg title "$title" \
      '{owner_id:$owner,client_id:$client,title:$title}')" \
    | jq -r '.conversation_id'
}

create_conversation() {
  local owner="$1" client="$2" title="$3"
  bms_post "/v1/conversations" \
    "$(jq -nc --arg owner "$owner" --arg client "$client" --arg title "$title" \
      '{owner_id:$owner,client_id:$client,title:$title}')" \
    | jq -r '.conversation_id'
}

byte_len() {
  python3 - "$1" <<'PY'
import sys
print(len(sys.argv[1].encode("utf-8")))
PY
}

assert_upload_host() {
  python3 - "$1" <<'PY'
import sys
from urllib.parse import urlparse

assert urlparse(sys.argv[1]).netloc == "127.0.0.1:14400"
PY
}

upload_artifact() {
  local owner="$1" client="$2" conversation_id="$3" filename="$4" content="$5"
  local size init upload_url artifact_id complete payload_file
  size="$(byte_len "$content")"
  init="$(bms_post "/v1/artifacts/init" \
    "$(jq -nc \
      --arg owner "$owner" \
      --arg client "$client" \
      --arg conversation "$conversation_id" \
      --arg filename "$filename" \
      --argjson size "$size" \
      '{
        owner_id:$owner,
        client_id:$client,
        conversation_id:$conversation,
        filename:$filename,
        mime:"text/plain",
        size:$size,
        source_surface:"artifact-composed-smoke"
      }')")"
  upload_url="$(jq -r '.upload_url' <<<"$init")"
  assert_upload_host "$upload_url"
  payload_file="$(mktemp)"
  printf "%s" "$content" >"$payload_file"
  curl_expect_success "presigned artifact PUT" -X PUT "$upload_url" \
    -H "Content-Type: text/plain" \
    --data-binary @"$payload_file"
  rm -f "$payload_file"
  artifact_id="$(jq -r '.artifact_id' <<<"$init")"
  complete="$(bms_post "/v1/artifacts/complete" \
    "$(jq -nc --arg artifact "$artifact_id" --arg owner "$owner" \
      '{artifact_id:$artifact,owner_id:$owner,status:"completed"}')")"
  assert_upload_host "$(jq -r '.download_url' <<<"$complete")"
  echo "$artifact_id"
}

init_incomplete_artifact() {
  local owner="$1" client="$2" conversation_id="$3" filename="$4" size="$5"
  bms_post "/v1/artifacts/init" \
    "$(jq -nc \
      --arg owner "$owner" \
      --arg client "$client" \
      --arg conversation "$conversation_id" \
      --arg filename "$filename" \
      --argjson size "$size" \
      '{
        owner_id:$owner,
        client_id:$client,
        conversation_id:$conversation,
        filename:$filename,
        mime:"text/plain",
        size:$size,
        source_surface:"artifact-composed-smoke"
      }')" >/dev/null
}

assert_not_contains() {
  local haystack="$1" needle="$2" label="$3"
  case "$haystack" in
    *"$needle"*)
      echo "artifact-composed-smoke leaked $label" >&2
      exit 1
      ;;
  esac
}

assert_artifact_source() {
  local response="$1" artifact_id="$2" source_ref="$3" sentinel="$4"
  jq -e \
    --arg artifact "$artifact_id" \
    --argjson source_ref "$source_ref" \
    --arg sentinel "$sentinel" '
      (.sources | length) >= 1
      and (.sources[] | select(
        .artifact_id == $artifact
        and .source_ref == $source_ref
        and (.snippet | contains($sentinel))
      ))
    ' <<<"$response" >/dev/null
}

provider_post "/fixture/reset" '{}'

POSITIVE_SENTINEL="W2F-POSITIVE-RETAINED-SENTINEL"
OTHER_OWNER_SENTINEL="W2F-OTHER-OWNER-SENTINEL"
OTHER_CONVERSATION_SENTINEL="W2F-OTHER-CONVERSATION-SENTINEL"
INCOMPLETE_SENTINEL="W2F-INCOMPLETE-UNAVAILABLE-SENTINEL"
BUDGET_RETAINED_SENTINEL="W2F-BUDGET-RETAINED-SENTINEL"
BUDGET_OMITTED_SENTINEL="W2F-BUDGET-OMITTED-SENTINEL"
PRIVACY_SENTINEL="W2F-PRIVACY-SUPPRESSED-SENTINEL"

provider_post "/fixture/sentinels" "$(jq -nc \
  --arg positive "$POSITIVE_SENTINEL" \
  --arg other_owner "$OTHER_OWNER_SENTINEL" \
  --arg other_conversation "$OTHER_CONVERSATION_SENTINEL" \
  --arg incomplete "$INCOMPLETE_SENTINEL" \
  --arg retained "$BUDGET_RETAINED_SENTINEL" \
  --arg omitted "$BUDGET_OMITTED_SENTINEL" \
  --arg privacy "$PRIVACY_SENTINEL" \
  '{sentinels:{
    positive:$positive,
    other_owner:$other_owner,
    other_conversation:$other_conversation,
    incomplete:$incomplete,
    budget_retained:$retained,
    budget_omitted:$omitted,
    privacy:$privacy
  }}')"

owner="owner-wave2f-composed"
client="client-wave2f-composed"
conversation_id="$(resolve_conversation "$owner" "$client" "wave2f-composed-positive")"
other_owner_conversation="$(resolve_conversation "owner-wave2f-other" "$client" "wave2f-other-owner")"
other_conversation_id="$(create_conversation "$owner" "$client" "wave2f-other-conversation")"

positive_content="$POSITIVE_SENTINEL retained artifact alpha provenance same-artifact derivation."
other_owner_content="$OTHER_OWNER_SENTINEL must never reach this owner's prompt or sources."
other_conversation_content="$OTHER_CONVERSATION_SENTINEL must never reach this conversation prompt or sources."

positive_artifact="$(upload_artifact "$owner" "$client" "$conversation_id" "positive-retained.txt" "$positive_content")"
other_owner_artifact="$(upload_artifact "owner-wave2f-other" "$client" "$other_owner_conversation" "other-owner.txt" "$other_owner_content")"
other_conversation_artifact="$(upload_artifact "$owner" "$client" "$other_conversation_id" "other-conversation.txt" "$other_conversation_content")"
init_incomplete_artifact "$owner" "$client" "$conversation_id" "incomplete-$INCOMPLETE_SENTINEL.txt" 77

bms_positive_request_id="bms-wave2f-positive-$(python3 - <<'PY'
from uuid import uuid4
print(uuid4())
PY
)"
bms_positive="$(bms_retrieve "$conversation_id" "$bms_positive_request_id" "$owner" "$POSITIVE_SENTINEL alpha")"
positive_source_ref="$(
  jq -c --arg artifact "$positive_artifact" \
    '.bundle.artifact_refs[] | select(.artifact_id == $artifact) | .source_ref' \
    <<<"$bms_positive"
)"
test -n "$positive_source_ref"
jq -e \
  --arg artifact "$positive_artifact" \
  --arg sentinel "$POSITIVE_SENTINEL" '
    (.bundle.artifact_refs[] | select(
      .artifact_id == $artifact
      and (.snippet | contains($sentinel))
      and (.source_ref.ref_type == "derived_text")
    ))
  ' <<<"$bms_positive" >/dev/null

positive_response="$(co_chat \
  "$owner" \
  "$client" \
  "$conversation_id" \
  "Use retained artifact alpha provenance same-artifact derivation and ignore unavailable or unrelated artifacts." \
  "public" \
  "desktop_private")"
positive_request_id="$(jq -r '.request_id' <<<"$positive_response")"
jq -e '.status == "ok"' <<<"$positive_response" >/dev/null
assert_artifact_source "$positive_response" "$positive_artifact" "$positive_source_ref" "$POSITIVE_SENTINEL"
positive_response_text="$(jq -c . <<<"$positive_response")"
assert_not_contains "$positive_response_text" "$OTHER_OWNER_SENTINEL" "other-owner source content"
assert_not_contains "$positive_response_text" "$OTHER_CONVERSATION_SENTINEL" "other-conversation source content"
assert_not_contains "$positive_response_text" "$INCOMPLETE_SENTINEL" "incomplete artifact content"

positive_trace="$(fetch_trace "$positive_request_id")"
positive_trace_text="$(jq -c . <<<"$positive_trace")"
positive_source_ref_id="$(jq -r '.ref_id' <<<"$positive_source_ref")"
jq -e \
  --arg request_id "$positive_request_id" \
  --arg owner "$owner" \
  --arg conversation "$conversation_id" \
  --arg artifact "$positive_artifact" \
  --arg source_ref_id "$positive_source_ref_id" '
    .request_id == $request_id
    and .owner_id == $owner
    and .conversation_id == $conversation
    and (.retrieval.bundle.artifact_refs[] | select(
      .artifact_id == $artifact
      and .source_ref.ref_type == "derived_text"
      and .source_ref.ref_id == $source_ref_id
    ))
    and (.artifacts.included_ids | index($artifact))
    and (.prompt.provider_prompt.fingerprint | type == "string")
  ' <<<"$positive_trace" >/dev/null
assert_not_contains "$positive_trace_text" "$positive_content" "complete positive object bytes in trace"
assert_not_contains "$positive_trace_text" "$OTHER_OWNER_SENTINEL" "other-owner trace evidence"
assert_not_contains "$positive_trace_text" "$OTHER_CONVERSATION_SENTINEL" "other-conversation trace evidence"
assert_not_contains "$positive_trace_text" "$INCOMPLETE_SENTINEL" "incomplete artifact trace evidence"
assert_not_contains "$positive_trace_text" "$other_owner_artifact" "other-owner artifact identity in trace"
assert_not_contains "$positive_trace_text" "$other_conversation_artifact" "other-conversation artifact identity in trace"
assert_not_contains "$positive_trace_text" "X-Amz-" "signed query values in trace"
assert_not_contains "$positive_trace_text" "minioadmin" "object-store credential in trace"
assert_not_contains "$positive_trace_text" "127.0.0.1:14400" "presigned endpoint in trace"

positive_provider_calls="$(fetch_provider_calls "$positive_request_id")"
jq -e '
  (.calls | map(select(.kind == "chat")) | length) == 1
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_presence.positive == true))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_in_user_messages.positive == false))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_presence.other_owner == false))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_presence.other_conversation == false))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_presence.incomplete == false))
' <<<"$positive_provider_calls" >/dev/null

budget_owner="owner-wave2f-budget"
budget_client="client-wave2f-budget"
budget_conversation="$(resolve_conversation "$budget_owner" "$budget_client" "wave2f-budget")"
budget_retained_content="$BUDGET_RETAINED_SENTINEL retained anchor compact survivor."
budget_omitted_content="$BUDGET_OMITTED_SENTINEL omitted archive filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler filler."
budget_retained_artifact="$(upload_artifact "$budget_owner" "$budget_client" "$budget_conversation" "budget-retained.txt" "$budget_retained_content")"
budget_omitted_artifact="$(upload_artifact "$budget_owner" "$budget_client" "$budget_conversation" "budget-omitted.txt" "$budget_omitted_content")"

bms_budget_request_id="bms-wave2f-budget-$(python3 - <<'PY'
from uuid import uuid4
print(uuid4())
PY
)"
bms_budget="$(bms_retrieve "$budget_conversation" "$bms_budget_request_id" "$budget_owner" "retained anchor compact survivor omitted archive")"
jq -e \
  --arg retained "$budget_retained_artifact" \
  --arg omitted "$budget_omitted_artifact" '
    ([.bundle.artifact_refs[].artifact_id] | index($retained))
    and ([.bundle.artifact_refs[].artifact_id] | index($omitted))
  ' <<<"$bms_budget" >/dev/null

budget_response="$(co_chat \
  "$budget_owner" \
  "$budget_client" \
  "$budget_conversation" \
  "Answer using retained anchor compact survivor." \
  "public" \
  "desktop_private")"
budget_request_id="$(jq -r '.request_id' <<<"$budget_response")"
jq -e --arg retained "$budget_retained_artifact" --arg omitted "$budget_omitted_artifact" '
  .status == "ok"
  and ([.sources[].artifact_id] | index($retained))
  and ([.sources[].artifact_id] | index($omitted) | not)
' <<<"$budget_response" >/dev/null
budget_provider_calls="$(fetch_provider_calls "$budget_request_id")"
jq -e '
  (.calls | map(select(.kind == "chat")) | length) == 1
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_presence.budget_retained == true))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_in_user_messages.budget_retained == false))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_presence.budget_omitted == false))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_in_user_messages.budget_omitted == false))
' <<<"$budget_provider_calls" >/dev/null
budget_trace="$(fetch_trace "$budget_request_id")"
budget_trace_text="$(jq -c . <<<"$budget_trace")"
jq -e --arg retained "$budget_retained_artifact" --arg omitted "$budget_omitted_artifact" --argjson response "$budget_response" '
  .retrieval.prompt_assembly.prompt_budget.status == "optional_context_reduced"
  and (.retrieval.prompt_assembly.retained_source_ids.artifact_ids == [$retained])
  and ([.prompt.prompt_budget.dropped_context.by_reason | keys[]] | length) >= 1
  and (.retrieval.prompt_assembly.retained_source_ids.artifact_ids == ([$response.sources[] | select(.artifact_id != null) | .artifact_id] | unique))
  and (.retrieval.prompt_assembly.retained_source_ids.artifact_ids | index($omitted) | not)
' <<<"$budget_trace" >/dev/null
assert_not_contains "$budget_trace_text" "$budget_omitted_content" "budget-omitted complete object bytes in trace"

provider_post "/fixture/fail-next-primary" '{}'
fallback_response="$(co_chat \
  "$budget_owner" \
  "$budget_client" \
  "$budget_conversation" \
  "Retry with fallback but keep only retained anchor compact survivor." \
  "public" \
  "desktop_private")"
fallback_request_id="$(jq -r '.request_id' <<<"$fallback_response")"
jq -e --arg retained "$budget_retained_artifact" --arg omitted "$budget_omitted_artifact" '
  .status == "degraded"
  and ([.sources[].artifact_id] | index($retained))
  and ([.sources[].artifact_id] | index($omitted) | not)
' <<<"$fallback_response" >/dev/null
fallback_calls="$(fetch_provider_calls "$fallback_request_id")"
jq -e '
  (.calls | map(select(.kind == "chat")) | length) == 2
  and (.calls | map(select(.kind == "chat")) | .[0].status == "failed")
  and (.calls | map(select(.kind == "chat")) | .[1].status == "ok")
  and ((.calls | map(select(.kind == "chat")) | .[0].prompt_fingerprint) == (.calls | map(select(.kind == "chat")) | .[1].prompt_fingerprint))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_presence.budget_retained == true))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_in_user_messages.budget_retained == false))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_presence.budget_omitted == false))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_in_user_messages.budget_omitted == false))
' <<<"$fallback_calls" >/dev/null
fallback_trace="$(fetch_trace "$fallback_request_id")"
jq -e --arg retained "$budget_retained_artifact" --arg omitted "$budget_omitted_artifact" '
  .fallback.triggered == true
  and .prompt.provider_fallback_context.same_sanitized_messages_reused == true
  and .prompt.provider_fallback_context.prompt_fingerprint == .prompt.provider_prompt.fingerprint
  and ([.model_calls[].status] == ["failed","ok"])
  and (.retrieval.prompt_assembly.retained_source_ids.artifact_ids == [$retained])
  and (.retrieval.prompt_assembly.retained_source_ids.artifact_ids | index($omitted) | not)
' <<<"$fallback_trace" >/dev/null

privacy_owner="owner-wave2f-privacy"
privacy_client="client-wave2f-privacy"
privacy_conversation="$(resolve_conversation "$privacy_owner" "$privacy_client" "wave2f-privacy")"
privacy_content="$PRIVACY_SENTINEL privacy anchor notice."
privacy_artifact="$(upload_artifact "$privacy_owner" "$privacy_client" "$privacy_conversation" "privacy-sensitive.txt" "$privacy_content")"
privacy_response="$(co_chat \
  "$privacy_owner" \
  "$privacy_client" \
  "$privacy_conversation" \
  "Summarize privacy anchor notice." \
  "private" \
  "notification_preview")"
privacy_request_id="$(jq -r '.request_id' <<<"$privacy_response")"
jq -e '
  .status == "ok"
  and .answer == "A private update is available. Open a private surface for details."
  and (.sources | length) == 0
' <<<"$privacy_response" >/dev/null
privacy_calls="$(fetch_provider_calls "$privacy_request_id")"
jq -e '
  (.calls | map(select(.kind == "chat")) | length) == 1
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_presence.privacy == true))
  and (.calls | map(select(.kind == "chat")) | all(.sentinel_in_user_messages.privacy == false))
' <<<"$privacy_calls" >/dev/null
privacy_trace="$(fetch_trace "$privacy_request_id")"
privacy_trace_text="$(jq -c . <<<"$privacy_trace")"
jq -e '
  .retrieval.bundle.privacy_suppressed == true
  and .retrieval.prompt_assembly.privacy_context.action_taken == "replaced_with_safe_template"
  and .retrieval.prompt_assembly.privacy_context.sources_suppressed_count == 1
  and .artifacts.status == "omitted"
  and .artifacts.reason == "privacy_suppressed"
  and (.references | length) == 0
' <<<"$privacy_trace" >/dev/null
assert_not_contains "$privacy_trace_text" "$PRIVACY_SENTINEL" "privacy artifact content in restricted trace"
assert_not_contains "$privacy_trace_text" "$privacy_artifact" "privacy artifact id in restricted trace"
assert_not_contains "$privacy_trace_text" "X-Amz-" "signed query values in restricted trace"
assert_not_contains "$privacy_trace_text" "minioadmin" "object-store credential in restricted trace"
assert_not_contains "$privacy_trace_text" "127.0.0.1:14400" "presigned endpoint in restricted trace"

echo "Artifact composed smoke passed: positive lifecycle, owner/conversation isolation, incomplete artifact omission, prompt-budget survivor filtering, provider fallback reuse, privacy source suppression."
echo "Topology: CO branch -> BMS main with PR #33 -> PostgreSQL 16 + Qdrant + MinIO bucket -> deterministic local provider/embedding stub."
