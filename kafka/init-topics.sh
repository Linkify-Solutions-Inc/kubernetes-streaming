#!/bin/sh
set -eu

BOOTSTRAP="kafka:9092"

# Partition count is a placeholder (JIT decision — see SPEC.md).
# 3 gives us something to see partitioning/consumer-groups behavior with
# locally without needing real headroom numbers yet.
PARTITIONS=3
REPLICATION=1

for topic in stream.lifecycle upload.events transcode.jobs transcode.status viewer.analytics; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION"
done

echo "Topics after init:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
