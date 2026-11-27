#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
TOPIC="gateway.traffic.events"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config retention.bytes=2147483648 \
  --config segment.ms=3600000 \
  --config compression.type=producer

echo "--- Topic details ---"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"