#!/bin/sh
# Register the Event Avro schema under the value subject for each origin
# topic. Subjects follow the TopicNameStrategy: <topic>-value.
set -eu

: "${SR_URL:=http://schema-registry:8081}"

# jq is not in curlimages/curl; build the JSON body with a tiny shell
# transform that escapes embedded quotes for Avro JSON.
SCHEMA_FILE=/schemas/event.avsc
[ -f "$SCHEMA_FILE" ] || { echo "missing $SCHEMA_FILE"; exit 1; }

# Use sed to escape quotes, then wrap in {"schema": "..."}
ESCAPED=$(awk 'BEGIN{ORS=""} {gsub(/\\/,"\\\\"); gsub(/"/,"\\\""); print}' "$SCHEMA_FILE")
BODY="{\"schemaType\":\"AVRO\",\"schema\":\"${ESCAPED}\"}"

for topic in us.events eu.events ap.events; do
  subject="${topic}-value"
  echo "registering $subject"
  printf '%s' "$BODY" | curl -fsS -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data-binary @- \
    "$SR_URL/subjects/$subject/versions"
  echo
done

echo "schemas-init: done"
