#!/bin/sh
set -eu

BOOTSTRAP="kafka:9092"
REPLICATION=1

# Partition counts, sized for real headroom (previously a flat placeholder
# of 3 everywhere -- see SPEC.md Phase 1 hardening). What actually matters:
# in Phase 2, KEDA's Kafka scaler caps ScaledJob concurrency at the number
# of partitions carrying lag, so a topic needs at least as many partitions
# as the concurrent-Job ceiling it's meant to support.
#
# stream.lifecycle / upload.events drive that ceiling directly. Empirically
# measured this session: one live transcode costs ~2.3 cores / ~1GB RAM,
# putting the dev box's 12-core live-stream ceiling at 3-4 concurrent
# streams (see SPEC.md). 8 partitions gives ~2x headroom above that ceiling
# -- room to grow (a bigger box, or Phase 3's multi-node AWS) without an
# immediate repartition, without going so wide it's meaningless on a single
# 12-core node today. upload.events gets the same count: VOD has no live-
# latency constraint (excess uploads just queue, per SPEC), but the
# partition count still caps how many upload Jobs KEDA can run in parallel,
# so it should scale with the same box capacity.
STREAM_PARTITIONS=8

# transcode.jobs is unused (no dispatcher consumes it -- see SPEC.md), and
# transcode.status/viewer.analytics are only ever consumed by a single
# analytics-worker instance with no horizontal-scaling plan, so none of the
# three benefit from more than a minimal partition count.
OTHER_PARTITIONS=3

for topic in stream.lifecycle upload.events; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$STREAM_PARTITIONS" \
    --replication-factor "$REPLICATION"
done

for topic in transcode.jobs transcode.status viewer.analytics; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$OTHER_PARTITIONS" \
    --replication-factor "$REPLICATION"
done

echo "Topics after init:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
