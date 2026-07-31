#!/bin/sh
# Asserts that the live-stream admission ceiling in the ConfigMap
# (MAX_CONCURRENT_LIVE_STREAMS) matches the transcode-live ScaledJob's
# maxReplicaCount. The two can't be sourced from one value -- maxReplicaCount
# is an integer field inside a CRD, and Kustomize's replacements can't write
# a string ConfigMap literal into an int field and pass CRD validation -- so
# they have to be kept in sync by hand, with this as the backstop.
# See docs/aws/14-keda-scaledjobs.md.
set -eu
RENDERED="${1:?usage: check_ceiling_sync.sh <rendered.yaml>}"
CM=$(yq 'select(.kind=="ConfigMap" and (.metadata.name|test("^streaming-config")))
         | .data.MAX_CONCURRENT_LIVE_STREAMS' "$RENDERED" | grep -v '^null$' | head -1)
SJ=$(yq 'select(.kind=="ScaledJob" and .metadata.name=="transcode-live")
         | .spec.maxReplicaCount' "$RENDERED" | grep -v '^null$' | head -1)
[ "$CM" = "$SJ" ] || { echo "ceiling drift: config=$CM scaledjob=$SJ"; exit 1; }
echo "ceiling in sync at $CM"
