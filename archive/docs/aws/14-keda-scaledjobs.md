# Module 14 — Transcoding that scales

Prerequisite: [Module 13](13-first-stream.md) passed. You have watched a stream end to end. If you haven't, stop — this module changes the component you'd be debugging.

---

## What you're building

Today `transcode-worker` is one long-running container. It has a `while True` Kafka consumer at the bottom of `main.py`, and every stream and every uploaded video is handled by a thread inside that one process, sharing one node's cores. Concurrency is capped by whatever that pod's CPU request happens to be, and if the pod dies, everything it was doing dies with it.

By the end of this module, each unit of work is its own Kubernetes Job. A stream starts, KEDA notices a Kafka message, KEDA creates a Job, the Job is unschedulable, Karpenter provisions a node for it, the pod transcodes exactly that one stream, the stream ends, the pod exits, the Job completes, Karpenter consolidates the node away. Zero pods and zero transcode nodes when nobody is streaming.

You will also do the piece nobody warns you about: **KEDA does not tell the pod what to work on.** Most of this module is designing that handoff so you don't lose work or do it twice.

---

## Why it works this way

### KEDA is a counter, not a dispatcher

This is the single most important sentence in the module, so read it twice.

KEDA's Kafka scaler computes `latest_offset − committed_offset` for a `(consumerGroup, topic)` pair, divides by `lagThreshold`, and creates that many Jobs from a **fixed, static template**. There is no message body in the pod's environment. No key. No partition. No offset. Nothing.

This surprises people because every other queue-worker system they've used hands the message to the worker. Lambda does. SQS-triggered ECS tasks do. KEDA does not, and it can't: the pod is created from YAML you wrote before the message existed.

So the spawned pod has to go and get its own work:

```
ingest-webhook  --produce-->  stream.start.requests   (8 partitions)
                                        |
                    KEDA polls lag for group "transcode-live" every 5s
                                        |
                        lag = 1  ->  create 1 Job  ->  1 pod
                                        |
       pod joins group "transcode-live", polls ONE message, learns stream_id + path
                                        |
            pod atomically claims the stream in Postgres, THEN commits the offset
                                        |
                              lag drops to 0, KEDA goes quiet
```

Two things must line up for that loop to close, and both are easy to get wrong.

**The consumer group name in the pod's config must be byte-identical to the ScaledJob trigger's `consumerGroup`.** If they differ by so much as a hyphen, KEDA measures lag on a group nobody ever commits to. Lag grows monotonically forever. KEDA spawns Jobs at `maxReplicaCount`, continuously, until you notice — and because each of those pods loses its claim and exits 0, nothing looks like it's failing. It is a very expensive, very quiet incident. That's why the value appears as an env var in the pod spec *and* in the trigger metadata in the same file, with a comment on each pointing at the other.

**`offsetResetPolicy` on the trigger must match `auto.offset.reset` in the pod's consumer.** If the trigger says `latest` and the pod says `earliest`, the first pod to ever join replays the entire topic history and starts a transcode for every stream that has ever existed.

### Offset commit timing — the decision that makes or breaks it

There are two candidate points to commit the offset, and one of them will burn money for hours before you spot it.

**Wrong: commit after the work completes.** This is the reflex — it's textbook at-least-once, and for a 200ms HTTP handler it's correct. Here a VOD transcode takes 10–30 minutes and a live stream takes hours. The offset stays uncommitted that whole time, so KEDA sees lag ≥ 1 continuously and creates a fresh Job **every `pollingInterval`** for the entire duration, up to `maxReplicaCount`. Each new pod reads the same message, loses the claim, exits. That's a pod-churn treadmill for hours, with Karpenter provisioning and deprovisioning nodes underneath it.

**Right: commit as soon as the work is durably claimed.** The offset commit's job here is to say *"this message has an owner"*, not *"this message is finished"*. What makes the ownership durable is the Postgres claim, not the commit. So:

```
poll one message
  -> atomic DB claim   (UPDATE ... WHERE <still unclaimed> RETURNING id)
  -> commit the offset (ALWAYS — whether the claim won or lost)
  -> claim won:  do the work
     claim lost: exit 0, someone else has it
```

**Commit even when the claim loses.** This one feels wrong and isn't. If you skip the commit on a lost claim, that message is redelivered forever, KEDA never stops spawning pods for it, and every one of those pods loses the claim too. A lost claim means the message is definitively handled by *somebody* — that's a reason to commit, not a reason to retry.

### What a crash costs you, and how idempotency absorbs it

Committing at claim time trades one failure mode for another, and you should be able to state the trade out loud.

A pod that crashes **before** the commit: Kafka redelivers, a new pod claims it, work proceeds. Fine.

A pod that crashes **after** the commit but before finishing: Kafka will never redeliver that message. The row stays claimed with nothing working on it. The stream is dead, or the video is stuck at `transcoding`, permanently.

That hole already exists in the code you have. `_claim_video` does `UPDATE videos SET status='transcoding' WHERE id=%s AND status='uploaded'`, and its own comment admits a crash leaves the row stuck forever because a redelivered event won't match that `WHERE` clause. On the dev box that was theoretical — the container ran for weeks. In the pod-per-job model it becomes routine: spot reclaim, node consolidation, OOMKill, an ephemeral-storage eviction. Every one of those ends a pod mid-work with no warning.

Note what the redelivery semantics buy you and what they don't. At-least-once redelivery plus the atomic claim gives you exactly-once *starting*: two pods can read the same message and only one will ever begin ffmpeg, because the claim is a single-row conditional `UPDATE` and Postgres serializes it. What it does not give you is exactly-once *completion*. That gap is what the sweeper fills.

### Why `active_streams` stops working

`transcode-worker` keeps a module-level dict guarded by a lock:

```python
active_streams: dict[str, dict | None] = {}
active_streams_lock = threading.Lock()
```

It does two different jobs, and they have different answers now.

| What it does today | Why it stops working | What replaces it |
|---|---|---|
| Dedupes a redelivered `started` event (`if path in active_streams: return`) | The dict is per-process. Every stream is now its own pod with its own empty dict. Two pods for the same stream would each see an empty dict, both start ffmpeg against the same RTMP path, MediaMTX would serve both, and you'd get two encoders fighting over one source and two writers racing on the same S3 prefix. | A claim column on `streams` in Postgres, mirroring what `_claim_video` already does for videos — plus a heartbeat, so a dead claim is detectable. |
| Routes an `ended` event to the right ffmpeg (`active_streams[path]["stop_event"].set()`) | — | **Nothing.** It disappears. Each pod owns exactly one stream, so "which ffmpeg does this event belong to?" collapses to `if data["path"] == MY_PATH`. |

That second row is the good news buried in this design: the routing table you had to maintain in memory, with a lock, with a `None` placeholder to close a race — all of it deletes. One pod, one stream, no table.

The first row is the work. The dict was doing something a dict genuinely can't do across processes, and the replacement has to be a shared, atomic, durable thing. Postgres already is one.

**Why a heartbeat and not just a status column.** A status column tells you a claim exists. It cannot tell you whether the claimer is still alive. A live pod writes `transcode_heartbeat = now()` every 15 seconds; a claim whose heartbeat is two minutes stale is a claim whose owner is gone. That's a lease, and it's the minimum you need to safely recover work you can't re-derive from Kafka.

**Why a sweeper CronJob and not in-pod recovery.** The pod that needs recovering is, by definition, not running. Something outside it has to notice. A CronJob every two minutes is the cheapest possible watcher and it costs about $0.

### How the live pod knows when its stream ended

`stream.lifecycle` carries both `started` and `ended` events. The KEDA trigger can't point at it — the scaler has no idea what's in a message, so an `ended` event would spawn a transcode Job just as readily as a `started` one. That's why `stream.start.requests` exists as a separate, `started`-only topic.

But the pod still needs the `ended` event, so it consumes `stream.lifecycle` itself and filters for its own path. When it matches, it sets a stop event, ffmpeg gets SIGTERM, finalizes the playlist, and the process exits. **Job completion is the teardown.** Nothing external tracks or kills the pod.

The watcher uses `assign()`, not `subscribe()`, and that distinction matters. With `subscribe()` and a shared group, partitions get split across pods — a pod might be assigned partitions that never carry its own stream's event, and it would wait forever. It would also trigger a consumer-group rebalance every single time a live Job starts or stops. `assign()` bypasses group coordination entirely: this pod reads every partition of `stream.lifecycle`, filters in Python, commits nothing, and disturbs no other consumer.

Teardown is layered, and you should know the order because you'll debug it:

1. **ffmpeg exits on its own.** OBS disconnects, MediaMTX tears down the path, ffmpeg's RTMP read hits EOF. This already works today and is the normal case.
2. **The `ended` event sets `stop_event`.** Its real job is not killing ffmpeg — case 1 does that — but stopping the *retry loop* from restarting ffmpeg into a path that no longer exists. Look at `start_live_transcode`: on a detected stall it loops and starts a new ffmpeg. Without the `ended` signal, a stream that ended during a stall retries until `activeDeadlineSeconds`.
3. **A DB poll every 15 seconds** on the heartbeat thread (`SELECT status FROM streams WHERE id = %s`). Covers a missed Kafka event — a broker restart mid-stream, an `offsets_for_times` edge case.
4. **`activeDeadlineSeconds: 21600`.** A pod that survives all three is wedged. The Job controller kills it at six hours, which also puts a hard ceiling on what one bad pod can cost you.

---

## Do it

### 1. Install KEDA

For now, by hand. Module 15 moves it into ArgoCD and you'll delete nothing — Argo adopts the existing release.

```sh
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm upgrade --install keda kedacore/keda \
  --namespace keda --create-namespace \
  --version 2.19.0 \
  --set operator.replicaCount=1 \
  --set metricsServer.replicaCount=1
```

```sh
kubectl get pods -n keda
```

```
NAME                                      READY   STATUS    RESTARTS   AGE
keda-admission-webhooks-6c9d8f7b4-2xk7p   1/1     Running   0          58s
keda-operator-7d5b9c8f66-lq4wn            1/1     Running   0          58s
keda-operator-metrics-apiserver-...-vt8ck 1/1     Running   0          58s
```

The Argo Application version is already written for you at `k8s/platform/keda.yaml`; Module 15 wires it in.

**Which Kafka listener KEDA authenticates against: the plain internal listener on port 9092, no TLS, no SCRAM.** Strimzi's convention is a `plain` internal listener on 9092 and a `tls` one on 9093. The trigger points at `streaming-kafka-bootstrap.kafka.svc.cluster.local:9092` and needs no `TriggerAuthentication` at all.

Somebody will call that insecure, so have the reasoning ready:

- **TLS costs work in five places.** Every Python service would need `security.protocol=SSL` plus the cluster CA mounted from Strimzi's auto-generated `streaming-cluster-ca-cert` Secret. KEDA would need a `TriggerAuthentication` with a `ca` parameter pointing at a copy of that Secret **in the `keda` namespace** — Secrets don't cross namespaces, so that's a second ExternalSecret or a replicator existing purely for this. Strimzi rotates that CA on a schedule, and a stale copy breaks KEDA silently.
- **SCRAM costs more.** `KafkaUser` CRs per client, generated password Secrets, `sasl.mechanism` config in five services, and an `authorization: type: simple` block with per-topic ACLs or nothing is actually protected.
- **It buys close to nothing here.** The traffic is pod-to-pod inside one VPC, one cluster, one team. The threat TLS addresses — a passive observer on the wire between broker and client — is not present. If your worry is a compromised pod in another namespace, the NetworkPolicy in `k8s/infra/kafka/` is the control that helps, and it's one file.

Write the upgrade path down so it's a decision and not an omission: add a second listener `{name: tls, port: 9093, type: internal, tls: true}` alongside the plain one — they coexist — migrate clients one at a time, then delete the plain listener. Do that when a second team shows up or Kafka gets exposed outside the cluster. Not before.

**No IRSA or Pod Identity for KEDA.** The Kafka scaler opens a plain TCP connection to an in-cluster Service and makes zero AWS API calls. KEDA needs an AWS identity only for AWS-flavoured scalers — SQS, CloudWatch, DynamoDB, or MSK with IAM auth. This is a real, concrete argument for having kept Kafka in-cluster on Strimzi.

The transcode **Job pods** do need AWS credentials, for S3. They keep using the `transcode-worker` ServiceAccount that [Module 11](11-workloads.md) already created at `k8s/apps/base/transcode-worker/serviceaccount.yaml`, bound to an IAM role by the EKS Pod Identity association from [Module 7](07-secrets.md). **Do not create a new ServiceAccount for this** — reusing the existing one means the Pod Identity association and any imagePullSecret carry over untouched. You delete the Deployment next to it, not the ServiceAccount.

### 2. Confirm the trigger topic exists

`stream.start.requests` needs 8 partitions. [Module 10](10-kafka-strimzi.md) should already have created it:

```sh
kubectl get kafkatopic -n kafka
```

```
NAME                    CLUSTER     PARTITIONS   REPLICATION FACTOR   READY
stream.lifecycle        streaming   8            1                    True
stream.start.requests   streaming   8            1                    True
transcode.status        streaming   3            1                    True
upload.events           streaming   8            1                    True
viewer.analytics        streaming   3            1                    True
```

If it isn't there, add it to `k8s/infra/kafka/topics.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: stream.start.requests
  namespace: kafka
  labels:
    strimzi.io/cluster: streaming
spec:
  # 8 partitions is the hard ceiling on live transcode concurrency —
  # allowIdleConsumers: false caps KEDA replicas at the partition count.
  # Partitions can only ever go up, so this is headroom, not a target.
  partitions: 8
  replicas: 1
  config:
    retention.ms: 604800000 # 7 days
```

### 3. The schema change

`postgres/migrations/002_transcode_claim.sql`:

```sql
ALTER TABLE streams
  ADD COLUMN IF NOT EXISTS transcode_status TEXT NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS transcode_heartbeat TIMESTAMPTZ;

ALTER TABLE videos
  ADD COLUMN IF NOT EXISTS transcode_heartbeat TIMESTAMPTZ;

-- ingest-webhook runs COUNT(*) WHERE status='live' on every publish attempt.
-- Without this it's a sequential scan over a table that grows one row per
-- broadcast forever. With it, a sub-millisecond index-only scan.
CREATE INDEX IF NOT EXISTS streams_live_idx ON streams (status) WHERE status = 'live';
```

Apply it with the migration Job from [Module 8](08-rds-postgres.md). If you haven't built that yet, `psql` it in once and come back — but don't let that become the habit.

### 4. The code changes in `transcode-worker`

You are rewriting the *process model*, not the encoding. `build_ffmpeg_command`, `RENDITIONS`, `_make_rendition_dirs`, `upload_loop`, `_upload_pass`, `_s3_extra_args`, `_watch_for_stall`, `_claim_video`, `set_video_status` and `emit_status` all stay exactly as they are. What goes is `main()`'s `while True`, `active_streams`, `active_streams_lock`, `handle_stream_lifecycle`, `handle_upload_event` and `stop_live_transcode`.

**Configuration.** Same image, both modes, `TRANSCODE_MODE` selects:

```python
BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
MODE      = os.environ["TRANSCODE_MODE"]              # "live" | "vod"
GROUP     = os.environ["TRANSCODE_CONSUMER_GROUP"]    # MUST match the ScaledJob trigger
TOPIC     = os.environ["TRANSCODE_TRIGGER_TOPIC"]
POD_NAME  = os.environ["POD_NAME"]
CLAIM_DEADLINE_S = int(os.environ.get("CLAIM_DEADLINE_SECONDS", "120"))
HEARTBEAT_S      = 15
```

**Getting the work.** This is the function that implements the commit rule:

```python
def claim_one(topic: str, group: str, try_claim) -> dict | None:
    """Consume until one message is successfully claimed, or the deadline expires.

    Returns the claimed payload, or None meaning "there is nothing here for me"
    — which is a normal, successful outcome, not an error.
    """
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        # MUST match the ScaledJob trigger's offsetResetPolicy. If this says
        # "earliest" and the group has no committed offset, the first pod ever
        # to run replays the whole topic and starts a transcode for every
        # stream that has ever existed.
        "auto.offset.reset": "latest",
        # Auto-commit would fire on a 5s timer regardless of whether we claimed
        # anything, silently dropping work when the pod exits before claiming.
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 45000,
    })
    c.subscribe([topic])
    deadline = time.monotonic() + CLAIM_DEADLINE_S
    try:
        while time.monotonic() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                log.error("unparseable message on %s: %r", topic, msg.value())
                c.commit(message=msg, asynchronous=False)  # poison pill: skip it
                continue

            won = try_claim(data)
            # Commit EITHER WAY. A lost claim still means the message has an
            # owner. Not committing means infinite redelivery and infinite
            # Job creation.
            c.commit(message=msg, asynchronous=False)
            if won:
                return data
            log.info("lost claim for %s, looking for other work", data)
        return None
    finally:
        c.close()
```

Two things to be deliberate about here.

**`subscribe()` rather than manual `assign()`.** With `subscribe()` the group coordinator assigns partitions. With 8 partitions and 4 concurrent pods, a pod can be assigned partitions that hold nothing pending while the message sits on a partition owned by a pod that already took other work. That pod polls, finds nothing, hits the deadline and exits 0. KEDA still sees lag, spawns another, and it converges within a poll cycle or two. Churny but correct — and the alternative (`AdminClient.list_consumer_group_offsets()` plus `get_watermark_offsets()` to find a lagging partition, then `assign()` and `seek()`) still lets two pods pick the same partition, so you'd still need the DB claim as the arbiter. Since the claim is doing the real work either way, take the simpler consumer.

The model to internalize: **the Kafka read is best-effort dispatch; the Postgres claim is the correctness boundary.**

**Exit code discipline.** "No message for me" is `exit 0`. With `backoffLimit: 0`, a non-zero exit marks the Job Failed, fills `failedJobsHistoryLimit` with non-failures, and makes your Grafana "failed transcodes" panel meaningless. Reserve non-zero for "I claimed work and could not do it."

**Claiming a stream.** The live twin of `_claim_video`:

```python
def _claim_stream(stream_id: str) -> str | None:
    """Atomically claim a live stream. Returns its path, or None if lost."""
    with pg_connect() as conn:
        row = conn.execute(
            """
            UPDATE streams
               SET transcode_status = 'claimed', transcode_heartbeat = now()
             WHERE id = %s AND transcode_status = 'pending' AND status = 'live'
            RETURNING path
            """,
            (stream_id,),
        ).fetchone()
        conn.commit()
    return row[0] if row else None
```

`AND status = 'live'` is not decoration. It fixes the Karpenter cold-start case for free: if the node took 90 seconds to provision and the streamer gave up in the meantime, the stream is already `ended` by the time this pod claims, the claim returns nothing, the pod exits 0, and no ffmpeg is launched against a dead path. Drop that clause and you get a pod starting an encoder for a stream that no longer exists, retrying into "connection refused" until `activeDeadlineSeconds`.

`_claim_video` needs **no change at all**. Its `WHERE id = %s AND status = 'uploaded'` was already exactly this pattern. The comment in the current code calling the claim "backed by Postgres rather than the in-memory `active_streams`-style tracking" was incidentally right and is now load-bearing.

**The heartbeat and the DB-poll fallback,** one thread doing both:

```python
def heartbeat_thread(table: str, row_id: str, stop_event: threading.Event) -> None:
    """Renew the claim lease every 15s, and notice if the stream ended.

    The lease is what the sweeper reads. If this thread stops (pod killed,
    node reclaimed), the heartbeat goes stale and the sweeper re-queues the
    work. That is the entire recovery mechanism for a crash after commit.
    """
    while not stop_event.wait(HEARTBEAT_S):
        try:
            with pg_connect() as conn:
                conn.execute(
                    f"UPDATE {table} SET transcode_heartbeat = now() WHERE id = %s",
                    (row_id,),
                )
                if table == "streams":
                    # Tertiary teardown path: covers an 'ended' event we never
                    # saw on Kafka (broker restart mid-stream).
                    status = conn.execute(
                        "SELECT status FROM streams WHERE id = %s", (row_id,)
                    ).fetchone()
                    if status and status[0] != "live":
                        log.info("stream %s is no longer live in the DB, stopping", row_id)
                        stop_event.set()
                conn.commit()
        except Exception:
            # A transient RDS blip must not kill the transcode. Missing one
            # beat is fine — the sweeper's threshold is eight beats wide.
            log.exception("heartbeat failed for %s=%s", table, row_id)
```

`f"UPDATE {table}"` is safe here only because `table` is one of two literals chosen by this module, never by input. Keep it that way.

**Watching for the end of your own stream:**

```python
def watch_for_end(path: str, stop_event: threading.Event, started_at: float) -> None:
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        # Throwaway and unique per pod. confluent-kafka requires group.id to be
        # set, but we never commit and never join a group — see assign() below.
        "group.id": f"live-watch-{POD_NAME}",
        "enable.auto.commit": False,
    })
    md = c.list_topics("stream.lifecycle", timeout=10)
    parts = list(md.topics["stream.lifecycle"].partitions)

    # Start ~60s before this Job began, not at the tail. For a very short
    # stream the 'ended' event can land while this pod is still pulling its
    # image, and OFFSET_END would miss it forever.
    since_ms = int((started_at - 60) * 1000)
    tps = c.offsets_for_times(
        [TopicPartition("stream.lifecycle", p, since_ms) for p in parts], timeout=10
    )
    for tp in tps:
        if tp.offset < 0:            # no message at or after that timestamp
            tp.offset = OFFSET_END
    c.assign(tps)                    # NOT subscribe() — see below

    try:
        while not stop_event.is_set():
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                d = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                continue
            if d.get("path") == path and d.get("event") == "ended":
                log.info("received 'ended' for path=%s, stopping", path)
                stop_event.set()
                return
    finally:
        c.close()
```

`assign()` and not `subscribe()`, for three reasons: this pod must see *every* partition so it can't miss its own stream's event; it must not cause a rebalance every time a live Job starts or stops; and it must not interfere with `analytics-worker`'s consumer group on the same topic. `assign()` stores no offsets and joins no group.

**The entrypoint.** `main()` becomes a dispatcher with no loop:

```python
def main() -> None:
    started_at = time.time()

    if MODE == "live":
        claimed = {}
        def try_claim(data: dict) -> bool:
            stream_id = data.get("stream_id")
            if not stream_id:
                log.error("no stream_id in %s, skipping", data)
                return False
            path = _claim_stream(stream_id)
            if path is None:
                return False
            claimed.update(stream_id=stream_id, path=path)
            return True

        if claim_one(TOPIC, GROUP, try_claim) is None:
            log.info("no live work available within the deadline, exiting cleanly")
            return                                    # exit 0, NOT an error

        stop_event = threading.Event()
        threading.Thread(target=watch_for_end,
                         args=(claimed["path"], stop_event, started_at),
                         daemon=True).start()
        threading.Thread(target=heartbeat_thread,
                         args=("streams", claimed["stream_id"], stop_event),
                         daemon=True).start()
        # start_live_transcode keeps its retry/stall/upload logic verbatim;
        # it now takes stop_event from the caller instead of creating it, and
        # it no longer touches active_streams.
        start_live_transcode(claimed["path"], claimed["stream_id"], stop_event)

    elif MODE == "vod":
        claimed = {}
        def try_claim(data: dict) -> bool:
            if data.get("event") != "uploaded":
                return False                          # committed, then skipped
            if not _claim_video(data["video_id"]):
                return False
            claimed.update(video_id=data["video_id"], object_key=data["object_key"])
            return True

        if claim_one(TOPIC, GROUP, try_claim) is None:
            log.info("no VOD work available within the deadline, exiting cleanly")
            return

        stop_event = threading.Event()
        threading.Thread(target=heartbeat_thread,
                         args=("videos", claimed["video_id"], stop_event),
                         daemon=True).start()
        try:
            transcode_video(claimed["video_id"], claimed["object_key"])
        finally:
            stop_event.set()

    else:
        raise SystemExit(f"unknown TRANSCODE_MODE {MODE!r}")
```

Note the VOD `try_claim` returning `False` for a non-`uploaded` event. That still commits — which is correct, because `upload.events` may carry other event types later and none of them should be redelivered forever.

**The sweeper**, `services/transcode-worker/sweeper.py`, about 50 lines reusing the existing producer:

```python
def sweep() -> None:
    with pg_connect() as conn:
        # Live: claimed, still live, but nothing has heartbeated in 2 minutes.
        stuck_live = conn.execute(
            """
            UPDATE streams SET transcode_status = 'pending'
             WHERE status = 'live'
               AND transcode_status = 'claimed'
               AND transcode_heartbeat < now() - %s::interval
            RETURNING id, path
            """,
            (os.environ["LIVE_STALE_AFTER"],),
        ).fetchall()

        # VOD: stuck at 'transcoding' with a stale heartbeat.
        stuck_vod = conn.execute(
            """
            UPDATE videos SET status = 'uploaded'
             WHERE status = 'transcoding'
               AND transcode_heartbeat < now() - %s::interval
            RETURNING id, raw_object_key
            """,
            (os.environ["VOD_STALE_AFTER"],),
        ).fetchall()
        conn.commit()

    for stream_id, path in stuck_live:
        log.warning("re-queueing abandoned live stream %s (path=%s)", stream_id, path)
        producer.produce("stream.start.requests", key=path, value=json.dumps(
            {"event": "started", "path": path,
             "stream_id": str(stream_id), "ts": time.time()}))
    for video_id, object_key in stuck_vod:
        log.warning("re-queueing abandoned VOD transcode %s", video_id)
        producer.produce("upload.events", key=str(video_id), value=json.dumps(
            {"event": "uploaded", "video_id": str(video_id),
             "object_key": object_key, "ts": time.time()}))
    producer.flush()
```

The `UPDATE ... RETURNING` runs before the produce on purpose. If the produce fails, the row is back to `pending` and the *next* sweep two minutes later re-emits it. The other order — produce first, then un-claim — can emit the same work twice.

### 5. The two ScaledJobs

Written for you at `k8s/apps/base/transcode/scaledjob-live.yaml` and `scaledjob-vod.yaml`. Read them; the comments carry the reasoning. Here's the live one:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledJob
metadata:
  name: transcode-live
  namespace: streaming
spec:
  pollingInterval: 5
  minReplicaCount: 0
  maxReplicaCount: 8              # KEEP IN SYNC with MAX_CONCURRENT_LIVE_STREAMS
  successfulJobsHistoryLimit: 5
  failedJobsHistoryLimit: 20
  rollout:
    strategy: gradual
  scalingStrategy:
    strategy: accurate
  jobTargetRef:
    parallelism: 1
    completions: 1
    backoffLimit: 0
    activeDeadlineSeconds: 21600
    ttlSecondsAfterFinished: 3600
    template:
      metadata:
        labels: {app: transcode, mode: live}
        annotations:
          karpenter.sh/do-not-disrupt: "true"
      spec:
        restartPolicy: Never
        serviceAccountName: transcode-worker   # the SA from Module 11, reused
        terminationGracePeriodSeconds: 60
        nodeSelector: {workload: transcode, transcode-mode: live}
        tolerations:
          - {key: workload, operator: Equal, value: transcode, effect: NoSchedule}
        containers:
          - name: transcode
            image: ghcr.io/linkify-solutions-inc/kubernetes-streaming-transcode-worker:latest
            env:
              - {name: TRANSCODE_MODE, value: "live"}
              - {name: TRANSCODE_CONSUMER_GROUP, value: "transcode-live"}
              - {name: TRANSCODE_TRIGGER_TOPIC, value: "stream.start.requests"}
              - {name: TMPDIR, value: /scratch}
              # ...POD_NAME via fieldRef, POSTGRES_DSN via secretKeyRef,
              #    KAFKA_BOOTSTRAP_SERVERS / MEDIAMTX_RTMP_URL / S3_BUCKET
              #    via configMapKeyRef
            resources:
              requests:
                cpu: "2500m"
                memory: 2Gi
                ephemeral-storage: 5Gi
              limits:
                memory: 2Gi              # == request  -> Guaranteed QoS
                ephemeral-storage: 8Gi
                # deliberately NO cpu limit
            volumeMounts: [{name: scratch, mountPath: /scratch}]
        volumes:
          - name: scratch
            emptyDir: {sizeLimit: 8Gi}
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: streaming-kafka-bootstrap.kafka.svc.cluster.local:9092
        consumerGroup: transcode-live        # == TRANSCODE_CONSUMER_GROUP above
        topic: stream.start.requests
        lagThreshold: "1"
        activationLagThreshold: "0"
        offsetResetPolicy: latest            # == auto.offset.reset in main.py
        allowIdleConsumers: "false"
        excludePersistentLag: "false"
        scaleToZeroOnInvalidOffset: "true"
```

Field by field, the ones that bite:

- **`rollout.strategy: gradual`** is the most important line in the file. The default strategy **terminates every running Job when the ScaledJob spec changes** — and from Module 15 onward, CI changes this spec on every `transcode-worker` push. The default therefore means *every deploy kills every live stream on the platform*. `gradual` leaves running Jobs alone; only new Jobs use the new spec.
- **`scalingStrategy.strategy: accurate`.** The `default` strategy computes `maxScale − runningJobCount`. Because you commit at claim time, there's a 60–100 second window on a cold Karpenter node where the Job is Pending, the offset is not yet committed, and lag is still ≥ 1 — `default` counts nothing running and over-spawns through that whole window. `accurate` counts pending Jobs. The extra pods aren't wrong (they lose the claim, exit 0) but each one may provision an EC2 instance.
- **`allowIdleConsumers: "false"` caps replicas at the partition count.** `maxReplicaCount: 8` only works because `stream.start.requests` has 8 partitions. Set it to 12 and KEDA silently gives you 8, and someone loses a day to it. Partition count and concurrency ceiling are one conversation.
- **No CPU limit, on purpose.** A CPU limit means CFS throttling: the cgroup burns its quota inside the 100 ms period and is hard-stopped until the next one. For x264 that's dropped frames and a visibly stuttering stream on a node with idle cores. `requests: 2500m` guarantees the floor and is what the scheduler and Karpenter size against; no limit lets it burst into whatever's spare. Memory `request == limit` makes the pod Guaranteed so it won't be evicted under node pressure.
- **Resources come from the benchmark, plus headroom.** SPEC.md measures one live transcode at **~2.3 cores / ~1 GB** for a 3-rendition ABR encode at `veryfast`. Requesting `2500m`/`2Gi` leaves room for the boto3 upload threads and the stall watchdog, and it's the number [Module 6](06-karpenter.md)'s NodePool sizing assumes. Don't request exactly the measured value — a Guaranteed pod whose limit equals its measured peak gets OOMKilled by the first allocation spike.
- **`ephemeral-storage` is mandatory, and VOD's number is large.** Python's `tempfile` writes to `TMPDIR`, which without a request lands on the node's root volume via the container's writable layer. Exceed the *limit* and the kubelet evicts the pod with **no grace period** — mid-encode. Live keeps a 30-segment sliding window across 3 renditions, roughly 140 MB steady state, so `5Gi` request / `8Gi` limit is generous. VOD downloads the entire source (up to `MAX_UPLOAD_BYTES`, which every `config.env` sets to 2 GiB) *and* writes the full ladder into the same scratch dir. The output tracks duration, not source size — about 4 GB per hour of encode — and 2 GiB of source can be three hours of video, so the worst case is roughly 2 GiB in and 12 GB out. Hence `40Gi` request / `50Gi` limit, with the `emptyDir` `sizeLimit` matching the limit in both cases. Those five numbers are what is in the manifests; if you change one, change its `sizeLimit` with it.

  Then confirm the node can actually hold it. [Module 6](06-karpenter.md) already sized `k8s/infra/karpenter/ec2nodeclass.yaml` at `volumeSize: 200Gi` for exactly this — two 50Gi VOD pods plus AL2023 (~3 GiB) and the ffmpeg image (~1.5 GiB). Nothing to change here; just check it is still 200Gi, because the EKS AMI's own default is 20 GiB and the scheduler will happily place a 40Gi-ephemeral pod on a 20 GiB node.
- **`backoffLimit: 0` on both.** With `restartPolicy: Never` and `backoffLimit > 0`, a failed pod gets a replacement pod — which loses the claim (the row is already `claimed`/`transcoding`) and exits, burning a retry for nothing. Retries are the sweeper's job.
- **`ttlSecondsAfterFinished: 3600`.** `successfulJobsHistoryLimit` is KEDA's own cleanup and mostly overlaps, but the TTL controller is what actually reaps Job objects out of etcd if KEDA is down.

VOD differs on: `pollingInterval: 15` (nobody watches a VOD transcode in real time), `maxReplicaCount: 4` (a pure cost cap — excess uploads queue in Kafka, which is fine), `activeDeadlineSeconds: 7200`, no `karpenter.sh/do-not-disrupt` annotation (it's interruptible, which is why it can run on spot), `nodeSelector: transcode-mode: vod`, `cpu: 4000m` (the `medium` preset eats more than `veryfast`, and requesting 4 cores caps a `c6i.2xlarge` at two VOD jobs, which is what keeps 2 × 50Gi of scratch inside the node's root volume), and `ephemeral-storage: 40Gi/50Gi`.

### 6. Bootstrap the consumer groups — or your first stream won't trigger

With `offsetResetPolicy: latest`, if consumer group `transcode-live` has *never committed an offset* for a partition, KEDA has nothing to subtract and treats the lag as 0. **The very first stream after a fresh deploy never creates a Job.** Somebody will lose an afternoon to this.

Create the groups at the end of the topics before anything runs. `k8s/infra/kafka/consumer-group-bootstrap-job.yaml`:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: kafka-consumer-group-bootstrap
  namespace: kafka
  annotations:
    argocd.argoproj.io/hook: PostSync
    argocd.argoproj.io/hook-delete-policy: HookSucceeded
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: bootstrap
          image: quay.io/strimzi/kafka:0.49.0-kafka-3.9.0
          command: [/bin/sh, -c]
          args:
            - |
              set -eu
              B=streaming-kafka-bootstrap.kafka.svc.cluster.local:9092
              for pair in "transcode-live stream.start.requests" "transcode-vod upload.events"; do
                set -- $pair
                bin/kafka-consumer-groups.sh --bootstrap-server "$B" \
                  --group "$1" --topic "$2" --reset-offsets --to-latest --execute
              done
```

The alternative is `scaleToZeroOnInvalidOffset: "false"`, which keeps one replica alive on an invalid offset so that pod self-bootstraps the group. It works, and it also spawns a pointless pod at every idle moment something goes wrong with offsets. Prefer the explicit Job.

### 7. `ingest-webhook`: produce the trigger, and gate admission

**Produce to `stream.start.requests`.** In `on_publish`, after the `streams` INSERT, alongside the existing `stream.lifecycle` event:

```python
producer.produce(
    "stream.start.requests",
    key=path,
    value=json.dumps({"event": "started", "path": path,
                      "stream_id": str(stream_id), "ts": time.time()}),
)
producer.flush()
```

Keep emitting `started` on `stream.lifecycle` too. That topic is the event log, `analytics-worker` reads it, and the live pod's `ended` watcher needs it populated. `stream.start.requests` is a *KEDA trigger topic*, not a source of truth. Same key (`path`) so per-stream ordering holds.

Also add producer timeouts while you're in this file — `message.timeout.ms: 10000`. Without it, a Kafka outage makes `producer.flush()` block for the 300-second default, holding a uvicorn worker and hanging MediaMTX's webhook call, which stalls the publish. That's a stream failure caused by a *monitoring* dependency being down.

**Admission control in `/hooks/auth`:**

```python
MAX_CONCURRENT_LIVE = int(os.environ["MAX_CONCURRENT_LIVE_STREAMS"])


@app.post("/hooks/auth")
def on_auth(req: MediaMTXAuthRequest, response: Response):
    if req.action != "publish":
        return {"ok": True}
    with pg_connect() as conn:
        row = conn.execute(
            "SELECT id FROM streamers WHERE stream_key = %s", (req.path,)
        ).fetchone()
        if row is None:
            log.info("rejected publish: unknown stream key")
            response.status_code = 401
            return {"ok": False}
        live = conn.execute(
            "SELECT COUNT(*) FROM streams WHERE status = 'live'"
        ).fetchone()[0]
    if live >= MAX_CONCURRENT_LIVE:
        log.warning("rejected publish: at capacity (%d/%d live)", live, MAX_CONCURRENT_LIVE)
        response.status_code = 503
        return {"ok": False}
    return {"ok": True}
```

Why this exists at all: if `maxReplicaCount` is hit, KEDA does not reject the excess `started` events — it leaves them lagging until a slot frees. For live that means OBS is accepted and pushing bytes into MediaMTX with **nothing consuming them**, which is the "connected but stuck" symptom, not a clean failure. The check has to happen at `/hooks/auth`, before MediaMTX ever accepts the publish, so the streamer gets an immediate connection failure instead of a silent hang.

Three notes:

- **503, not 401.** MediaMTX rejects on any non-2xx so OBS can't tell the difference, but *you* can: "at capacity" becomes trivially distinguishable from "bad key" in logs and in a Grafana panel. That's the alert you actually want.
- **The race is real and can't be closed here.** `/hooks/auth` fires at connection time; the `streams` row is only INSERTed at `runOnReady` (`/hooks/publish`), which is later. Two OBS clients connecting inside that window both see the same count and both pass. `maxReplicaCount` is the backstop for exactly that window. Put that in a comment so nobody "fixes" it with a lock.
- **The `COUNT(*)` runs on every publish attempt.** With `streams_live_idx` from step 3 it's a sub-millisecond index-only scan. Without it, a sequential scan over a table that grows one row per broadcast forever.

### 8. Where the ceiling lives, and what it should be

**Put it in the ConfigMap** — `k8s/apps/base/config.env` — not a code constant and not a default baked into the Deployment.

The justification is specific: this is *the* value an operator will want to change without touching code or rebuilding an image ("there's an event tonight, take it to 12"). In the ConfigMap it's a one-line git diff that Argo syncs in under a minute and that shows up in `git log`. Read it with `os.environ["MAX_CONCURRENT_LIVE_STREAMS"]` and no default, so a missing value is a loud `CreateContainerConfigError` rather than a silent fallback to a number nobody chose.

The ugly part, stated honestly: `maxReplicaCount` is an integer field inside a CRD, so Kustomize can't source it from the same ConfigMap literal (a `replacements:` block writes a string into an int field and fails CRD validation). The two values have to be kept in sync by hand. Mitigate with a comment on each pointing at the other, and a CI assertion — `scripts/check_ceiling_sync.sh`:

```sh
#!/bin/sh
set -eu
RENDERED="${1:?usage: check_ceiling_sync.sh <rendered.yaml>}"
CM=$(yq 'select(.kind=="ConfigMap" and (.metadata.name|test("^streaming-config")))
         | .data.MAX_CONCURRENT_LIVE_STREAMS' "$RENDERED" | grep -v '^null$' | head -1)
SJ=$(yq 'select(.kind=="ScaledJob" and .metadata.name=="transcode-live")
         | .spec.maxReplicaCount' "$RENDERED" | grep -v '^null$' | head -1)
[ "$CM" = "$SJ" ] || { echo "ceiling drift: config=$CM scaledjob=$SJ"; exit 1; }
echo "ceiling in sync at $CM"
```

**What the number should be now that Karpenter exists. Set it to 8.**

This is the part worth thinking about, because the ceiling has changed *category*. On the kubeadm box it was a physics limit: 12 cores ÷ 2.3 ≈ 4 concurrent streams, and exceeding it meant oversubscription and visible stutter for everyone already streaming. Karpenter deletes that constraint entirely — it will provision a `c6i.2xlarge` for the fifth stream and a third node for the ninth.

So the ceiling is no longer protecting node capacity. What it protects now:

1. **Your bill.** 8 × 2.5 vCPU ≈ 20 vCPU ≈ three `c6i.2xlarge` on-demand ≈ **$1.02/hour, about $735/month if pinned at capacity**. That is a sentence you can say to whoever pays. If that's too much, lower the ConfigMap value — not the partition count, because partitions only go up and you'd be permanently capping your own headroom.
2. **Blast radius.** A bug that emits duplicate `started` events, or a compromised stream key being shared around, can now spend money at Karpenter's speed rather than being naturally throttled by a fixed box. The ceiling is the circuit breaker.
3. **The gap between "accepted" and "encoding".** Admission control is what turns "queued behind capacity" into an immediate, unambiguous rejection.

And 8 specifically because `allowIdleConsumers: false` makes the partition count the real hard ceiling anyway. Configuring 12 would be a lie: the system silently caps at 8, and someone spends a day discovering that.

**Flag the thing Karpenter doesn't fix: cold start.** A node takes 40–60 seconds to provision, and the transcode image (python:3.12-slim plus ffmpeg, ~500 MB) pulls in another 20–40. So the first stream after an idle period sits for **60–100 seconds with OBS connected and pushing bytes into MediaMTX while nothing pulls them.** Admission control does not prevent this — admission passed, capacity is coming, it's just slow. Options, most valuable first:

1. **An over-provisioning placeholder.** A 1-replica Deployment running `registry.k8s.io/pause` with `requests: {cpu: 2500m, memory: 2Gi}`, the transcode nodeSelector and toleration, and a `PriorityClass` with `value: -1` and `globalDefault: false`. It holds one transcode-sized node warm; when a real Job (priority 0) needs the space the scheduler preempts the placeholder and the real pod starts immediately on a running node with the image already pulled. Costs one `c6i.2xlarge` running 24/7, roughly $250/month. Worth it if people actually watch live streams; skip it for a demo.
2. Slim the image. Halving 500 MB saves about 20 seconds.
3. Accept 60–100 seconds and say so in the UI.

### 9. Every file that changes

| File | Change |
|---|---|
| `services/transcode-worker/main.py` | The rewrite. Delete `main()`'s `while True`, `active_streams`, `active_streams_lock`, `handle_stream_lifecycle`, `handle_upload_event`, `stop_live_transcode`. Add `TRANSCODE_MODE` dispatch, `claim_one`, `_claim_stream`, `watch_for_end`, `heartbeat_thread`. `start_live_transcode` now takes `stop_event` from its caller. Keep `build_ffmpeg_command`, `RENDITIONS`, `_make_rendition_dirs`, `upload_loop`, `_upload_pass`, `_s3_extra_args`, `_watch_for_stall`, `_claim_video`, `set_video_status`, `emit_status` verbatim. Replace `BUCKET = "media"` with `os.environ["S3_BUCKET"]`. |
| `services/transcode-worker/sweeper.py` | New, ~50 lines. Un-claims stale leases and re-emits their trigger messages. |
| `services/transcode-worker/test_main.py` | Add: `claim_one` commits when the claim *loses*; `claim_one` returns `None` at deadline; `watch_for_end` matches only its own `path`; `_claim_stream` is a no-op on a second call. Add the four new env vars to the module's env stubs. |
| `services/transcode-worker/Dockerfile` | No functional change. Add `USER 10001` to satisfy `runAsNonRoot`. |
| `services/ingest-webhook/main.py` | Produce to `stream.start.requests` in `on_publish`. Add the `MAX_CONCURRENT_LIVE_STREAMS` check to `/hooks/auth` returning 503. Add `message.timeout.ms: 10000` to the producer. |
| `services/ingest-webhook/test_main.py` | Add: accepted at `live == ceiling − 1`; 503 at `live == ceiling`; unknown key still 401 and short-circuits before the count query; `/hooks/publish` produces to both topics. |
| `postgres/migrations/002_transcode_claim.sql` | New. `transcode_status` and `transcode_heartbeat` columns, plus the partial index on `streams (status)`. |
| `k8s/platform/keda.yaml` | New. KEDA 2.19.0 as an Argo Application, sync-wave 0. |
| `k8s/apps/base/transcode/scaledjob-live.yaml` | New. Live ScaledJob off `stream.start.requests`. |
| `k8s/apps/base/transcode/scaledjob-vod.yaml` | New. VOD ScaledJob off `upload.events`. |
| `k8s/apps/base/transcode/sweeper-cronjob.yaml` | New. Every 2 minutes, `concurrencyPolicy: Forbid`. |
| `k8s/apps/base/transcode/kustomization.yaml` | New. Ties the three together. Note the absence of a Deployment, and that it does not redefine the ServiceAccount. |
| `k8s/apps/base/transcode-worker/deployment.yaml` | **Deleted.** This is the long-running Deployment that ScaledJobs replace. |
| `k8s/apps/base/transcode-worker/serviceaccount.yaml` | **Unchanged and kept.** The ScaledJob pods reuse it, so the Pod Identity association survives. |
| `k8s/apps/base/kustomization.yaml` | Drop `transcode-worker/deployment.yaml` from `resources`, keep `transcode-worker/serviceaccount.yaml`, add `transcode/`. |
| `k8s/apps/base/config.env` | `MAX_CONCURRENT_LIVE_STREAMS=8` — already present from Module 11, now actually read by `ingest-webhook`. |
| `k8s/infra/kafka/topics.yaml` | Confirm `stream.start.requests` exists at 8 partitions (Module 10 should have created it). |
| `k8s/infra/kafka/consumer-group-bootstrap-job.yaml` | New. Seeds both consumer-group offsets so the first stream triggers. |
| `k8s/infra/karpenter/ec2nodeclass.yaml` | Confirm `blockDeviceMappings[0].ebs.volumeSize` is 200Gi so two 50Gi-ephemeral VOD pods fit alongside the OS and image. |
| `scripts/check_ceiling_sync.sh` | New. Asserts ConfigMap ceiling == ScaledJob `maxReplicaCount`. |
| `kafka/init-topics.sh` | Add `stream.start.requests` at 8 partitions; drop `transcode.jobs`. Compose/dev only. |
| `docker-compose.yml` | Replace the single `transcode-worker` with `transcode-worker-live` and `transcode-worker-vod`, each with its `TRANSCODE_MODE` and `restart: always`. A one-shot process that exits and gets restarted is a passable poor-man's ScaledJob locally, and it keeps dev exercising the exact same code path. |
| `SPEC.md` | Mark the Phase 3 transcode-scaling decision as implemented. |

---

## Verify

Apply everything and watch one stream go through the whole loop.

```sh
kubectl apply -k k8s/apps/base/transcode
kubectl get scaledjob -n streaming
```

```
NAME             MIN   MAX   TRIGGERS   AUTHENTICATION   READY   ACTIVE   AGE
transcode-live   0     8     kafka                       True    False    12s
transcode-vod    0     4     kafka                       True    False    12s
```

`READY=True` means KEDA connected to Kafka and read the group's offsets. If it's `False`, `kubectl describe scaledjob transcode-live -n streaming` puts the Kafka connection error in the condition message verbatim.

Confirm KEDA is measuring the group you think it is:

```sh
kubectl -n kafka run kt --rm -it --restart=Never \
  --image=quay.io/strimzi/kafka:0.49.0-kafka-3.9.0 -- \
  bin/kafka-consumer-groups.sh \
    --bootstrap-server streaming-kafka-bootstrap.kafka.svc.cluster.local:9092 \
    --describe --group transcode-live
```

```
GROUP           TOPIC                  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
transcode-live  stream.start.requests  0          0               0               0
transcode-live  stream.start.requests  1          0               0               0
...
```

Eight rows, `CURRENT-OFFSET` not `-`. A `-` means the bootstrap Job didn't run and your first stream won't trigger.

Now open four terminals and run the checkpoint.

**Terminal 1 — Jobs:**
```sh
kubectl get jobs -n streaming -w
```

**Terminal 2 — nodes:**
```sh
kubectl get nodes -L karpenter.sh/nodepool,karpenter.sh/capacity-type -w
```

**Terminal 3 — Karpenter:**
```sh
kubectl logs -n karpenter deploy/karpenter -f
```

**Terminal 4 — start streaming.** Point OBS at `rtmp://rtmp.k8s.linkifysolutions.com/<your-stream-key>` and hit Start Streaming.

Within about 5 seconds, terminal 1:

```
NAME                        STATUS    COMPLETIONS   DURATION   AGE
transcode-live-vx8q2        Running   0/1                      0s
```

Terminal 3, within another 15:

```
{"level":"INFO","message":"found provisionable pod(s)","Pods":"streaming/transcode-live-vx8q2-hd4kf","duration":"32ms"}
{"level":"INFO","message":"computed new nodeclaim(s) to fit pod(s)","nodeclaims":1,"pods":1}
{"level":"INFO","message":"created nodeclaim","NodePool":{"name":"transcode-live"},"instance-types":"c6i.xlarge, c6i.2xlarge, c6a.xlarge, ..."}
{"level":"INFO","message":"launched nodeclaim","provider-id":"aws:///us-east-1a/i-0a1b2c3d4e5f6a7b8","instance-type":"c6i.xlarge","capacity-type":"on-demand"}
```

Terminal 2, at roughly 45–60 seconds:

```
NAME                          STATUS   ROLES    AGE   VERSION   NODEPOOL         CAPACITY-TYPE
ip-10-0-1-83.ec2.internal     Ready    <none>   3d    v1.33.x
ip-10-0-2-19.ec2.internal     Ready    <none>   3d    v1.33.x
ip-10-0-1-204.ec2.internal    Ready    <none>   8s    v1.33.x   transcode-live   on-demand
```

Confirm the pod actually claimed something:

```sh
kubectl logs -n streaming -l app=transcode,mode=live --tail=20
```

```
INFO:transcode-worker:live transcode starting (attempt 1): path=... stream_id=8f2c... cmd=ffmpeg -y -i rtmp://mediamtx:1935/...
```

And that the lease is being renewed:

```sh
psql "$POSTGRES_DSN" -c \
  "SELECT id, transcode_status, now() - transcode_heartbeat AS beat_age
     FROM streams WHERE status = 'live'"
```

```
                  id                  | transcode_status |    beat_age
--------------------------------------+------------------+-----------------
 8f2c1d5a-4e6b-47a9-b2f1-9c0d3e7a1b45 | claimed          | 00:00:07.412009
```

`beat_age` under 15 seconds. If it climbs past two minutes, the sweeper will re-queue the stream and you'll see a second Job appear.

**Now stop the stream.** Hit Stop Streaming in OBS.

Terminal 4:
```sh
kubectl logs -n streaming -l app=transcode,mode=live --tail=5
```
```
INFO:transcode-worker:received 'ended' for path=..., stopping
INFO:transcode-worker:live_transcode_stopped
```

Terminal 1:
```
transcode-live-vx8q2        Complete   1/1           4m12s      4m12s
```

Terminal 3, two minutes later (`consolidateAfter: 2m` on the live pool):
```
{"level":"INFO","message":"disrupting nodeclaim(s) via delete, terminating 1 nodes","reason":"emptiness"}
```

Terminal 2:
```
ip-10-0-1-204.ec2.internal    NotReady   <none>   6m    v1.33.x   transcode-live   on-demand
ip-10-0-1-204.ec2.internal    deleted
```

Back to two nodes. **That's the checkpoint: a Job appeared from a Kafka message, a node appeared for the Job, the Job completed when the stream ended, and the node went away.** Zero transcode cost while idle.

---

## What breaks

Ordered by how often it actually happens.

**Jobs spawn continuously and never stop, even with nothing streaming.**
The consumer group in the pod env doesn't match the trigger's `consumerGroup`, so nothing ever commits to the group KEDA is watching. Diagnose:
```sh
kubectl get scaledjob transcode-live -n streaming \
  -o jsonpath='{.spec.triggers[0].metadata.consumerGroup}{"\n"}'
kubectl get scaledjob transcode-live -n streaming \
  -o jsonpath='{range .spec.jobTargetRef.template.spec.containers[0].env[?(@.name=="TRANSCODE_CONSUMER_GROUP")]}{.value}{"\n"}{end}'
```
Those two strings must be identical. Scale the damage down immediately with `kubectl patch scaledjob transcode-live -n streaming --type merge -p '{"spec":{"maxReplicaCount":0}}'` before you fix it.

**The first stream after a fresh deploy never creates a Job.**
The consumer group has no committed offset, so KEDA computes lag 0. Run the bootstrap Job from step 6. Confirm with the `--describe` command above: `CURRENT-OFFSET` must not be `-`.

**A Job is created but stays Pending forever.**
```sh
kubectl describe pod -n streaming -l app=transcode | tail -30
```
`0/2 nodes are available: 2 node(s) had untolerated taint` and no Karpenter activity means the pod's `nodeSelector`/`tolerations` don't match any NodePool's labels and taints. Compare against [Module 6](06-karpenter.md) — the labels are `workload: transcode` plus `transcode-mode: live|vod`, and the taint is `workload=transcode:NoSchedule`. `did not have enough resource: ephemeral-storage` means the NodePool's instance types are too small or the `EC2NodeClass` root volume is smaller than the pod's request.

**The pod starts, logs "lost claim", and exits 0 — every time.**
The row is stuck in a claimed state from an earlier crashed pod and the sweeper isn't running or isn't finding it. Check:
```sh
kubectl get cronjob transcode-sweeper -n streaming
kubectl logs -n streaming -l app=transcode-sweeper --tail=20
psql "$POSTGRES_DSN" -c \
  "SELECT id, status, transcode_status, transcode_heartbeat FROM streams
    WHERE transcode_status = 'claimed' ORDER BY transcode_heartbeat"
```
A `transcode_heartbeat` of `NULL` on a claimed row is the giveaway that a pod claimed and died before its first beat — `_claim_stream` sets `transcode_heartbeat = now()` in the same statement precisely so this can't happen; if you see NULL, that's the bug.

**Every live stream drops the moment you deploy.**
`rollout.strategy` is missing or set to `default` on the ScaledJob. Add `rollout: {strategy: gradual}`. This will not show up until the first deploy that happens while someone is streaming, which is why it's worth checking now rather than discovering later:
```sh
kubectl get scaledjob -n streaming -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.rollout.strategy}{"\n"}{end}'
```

**The stream plays for a minute and then freezes; the pod is still Running.**
This is the pre-existing wedged-ffmpeg case (MediaMTX drops the RTMP read for "reader is too slow", ffmpeg goes idle at ~0% CPU and never recovers). `_watch_for_stall` should catch it and restart within 20 seconds — look for `ffmpeg wedged (no new segment for 20s)` in the pod logs. If it *doesn't* restart, `stop_event` was set by the `ended` watcher first, meaning the pod thinks the stream ended. Check what's actually on `stream.lifecycle`.

**A VOD video sits at `transcoding` forever.**
Its pod was killed on a spot reclaim. Confirm, then wait for the sweeper:
```sh
kubectl get events -n streaming --field-selector reason=Killing --sort-by=.lastTimestamp | tail
psql "$POSTGRES_DSN" -c \
  "SELECT id, status, now() - transcode_heartbeat FROM videos WHERE status = 'transcoding'"
```
If the heartbeat is older than `VOD_STALE_AFTER` and nothing has happened, the sweeper is down.

**Pods get OOMKilled or evicted mid-encode.**
`kubectl get pod <name> -n streaming -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason}'`. `OOMKilled` means raise the memory request *and* limit together — keep them equal so QoS stays Guaranteed. `Evicted` with a message about ephemeral storage means the encode exceeded the `ephemeral-storage` limit; the kubelet gives no grace period for that, so the fix is a bigger limit and a bigger `emptyDir` `sizeLimit`, not a retry.

**A publish is rejected with 503 when nothing is streaming.**
Stale `streams` rows stuck at `status='live'` — `/hooks/unpublish` never fired, usually because MediaMTX was restarted. `SELECT count(*) FROM streams WHERE status='live'` will show the phantom rows. Longer term that wants a reaper of its own; short term, close them by hand.

---

Next: [Module 15 — GitOps with ArgoCD](15-argocd-gitops.md), which is where the ScaledJob image tag starts updating itself.
