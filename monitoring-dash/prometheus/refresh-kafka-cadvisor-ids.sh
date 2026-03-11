#!/usr/bin/env bash
set -euo pipefail

# Refresh cAdvisor Kafka container ID selectors in recording rules.
# Usage:
#   ./refresh-kafka-cadvisor-ids.sh
# Optional env vars:
#   RULES_FILE=/path/to/recording-rules.yml
#   PROM_URL=http://localhost:9090

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RULES_FILE="${RULES_FILE:-${SCRIPT_DIR}/recording-rules.yml}"
PROM_URL="${PROM_URL:-http://localhost:9090}"

id_k1="$(docker inspect -f '{{.Id}}' kafka-1)"
id_k2="$(docker inspect -f '{{.Id}}' kafka-2)"
id_k3="$(docker inspect -f '{{.Id}}' kafka-3)"

if [[ -z "$id_k1" || -z "$id_k2" || -z "$id_k3" ]]; then
  echo "Failed to resolve container IDs for kafka-1/2/3" >&2
  exit 1
fi

ids_regex="${id_k1}|${id_k2}|${id_k3}"

if [[ ! -f "$RULES_FILE" ]]; then
  echo "Rules file not found: $RULES_FILE" >&2
  exit 1
fi

ID_K1="$id_k1" ID_K2="$id_k2" ID_K3="$id_k3" IDS_REGEX="$ids_regex" perl -0777 -i -pe '
  # CPU broker selectors
  s#(rate\(container_cpu_usage_seconds_total\{id=~"/docker/)[a-f0-9]+("\}\[5m\]\) \* 100,\n\s*"broker", "kafka-1")#${1}$ENV{ID_K1}${2}#g;
  s#(rate\(container_cpu_usage_seconds_total\{id=~"/docker/)[a-f0-9]+("\}\[5m\]\) \* 100,\n\s*"broker", "kafka-2")#${1}$ENV{ID_K2}${2}#g;
  s#(rate\(container_cpu_usage_seconds_total\{id=~"/docker/)[a-f0-9]+("\}\[5m\]\) \* 100,\n\s*"broker", "kafka-3")#${1}$ENV{ID_K3}${2}#g;

  # Memory broker selectors
  s#(container_memory_usage_bytes\{id=~"/docker/)[a-f0-9]+("\},\n\s*"broker", "kafka-1")#${1}$ENV{ID_K1}${2}#g;
  s#(container_memory_usage_bytes\{id=~"/docker/)[a-f0-9]+("\},\n\s*"broker", "kafka-2")#${1}$ENV{ID_K2}${2}#g;
  s#(container_memory_usage_bytes\{id=~"/docker/)[a-f0-9]+("\},\n\s*"broker", "kafka-3")#${1}$ENV{ID_K3}${2}#g;

  # Cluster memory combined selector
  s#(container_memory_usage_bytes\{id=~"/docker/\()[a-f0-9|]+(\)"\})#${1}$ENV{IDS_REGEX}${2}#g;
' "$RULES_FILE"

echo "Updated selectors in: $RULES_FILE"
echo "kafka-1: $id_k1"
echo "kafka-2: $id_k2"
echo "kafka-3: $id_k3"

# Trigger Prometheus config/rules reload if available.
if curl -fsS -X POST "${PROM_URL}/-/reload" >/dev/null; then
  echo "Prometheus reloaded via ${PROM_URL}/-/reload"
else
  echo "Warning: failed to reload Prometheus at ${PROM_URL}" >&2
  exit 1
fi
