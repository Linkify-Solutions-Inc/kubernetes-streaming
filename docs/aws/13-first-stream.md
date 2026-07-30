# Module 13 — Your first stream

Prerequisites: [Module 11](11-workloads.md) (all pods Running), [Module 12](12-ingress-and-rtmp.md) (HTTPS and RTMP both answering).

---

## What you're building

Nothing. This module builds no infrastructure at all — it runs the whole path end to end and checks every hop, in order, so that when something fails you know exactly which one hop it was.

This is the milestone the course exists for. By the end you will have pushed RTMP from your laptop into EKS, watched it become three HLS renditions in S3, and played it back through CloudFront in a browser — and separately walked a file upload through the VOD path.

It's a lab rather than a normal module: every step is a command, the output you should see, and a **if this failed** block naming the likely cause and the one diagnostic that distinguishes it. Work through it in order. Do not skip a step because the previous one looked fine.

> **Reminder about `transcode-worker`:** in this module it is still a single long-running Deployment consuming `stream.lifecycle` and `upload.events`, exactly as it does under Docker Compose. You will read its logs with `kubectl logs deploy/transcode-worker`. [Module 14](14-keda-scaledjobs.md) replaces it with KEDA `ScaledJob`s — one Job per stream, one per upload — at which point the same evidence lives in per-Job pod logs and `kubectl get jobs`. Everything below is true until then.

---

## Setup: two shortcuts you'll use throughout

**A psql you can actually run.** RDS is in private subnets, so query it from inside the VPC. Pull the DSN out of the Secret once, then run throwaway pods against it:

```sh
export PGURL=$(kubectl get secret -n streaming streaming-db \
  -o jsonpath='{.data.POSTGRES_DSN}' | base64 -d)

pg() {
  kubectl run "pg-$RANDOM" -n default --rm -i --restart=Never --quiet \
    --image=postgres:16-alpine -- psql "$PGURL" -c "$1"
}
```

The pod runs in `default`, not `streaming`: the `streaming` namespace enforces the `restricted` Pod Security profile, and a bare `kubectl run` doesn't set the fields that profile demands.

```sh
pg "SELECT count(*) FROM streamers"
```

```
 count
-------
     0
(1 row)
```

**Your bucket name.**

```sh
export BUCKET=linkify-streaming-media-$(aws sts get-caller-identity --query Account --output text)
echo "$BUCKET"
```

---

## Step 1 — Create a streamer and get a stream key

Do it through the web UI, because that also proves `web` → `upload-api` → RDS works from the public internet:

```sh
curl -sS -X POST https://stream.k8s.linkifysolutions.com/streamers/new \
  -d 'display_name=fatima-test' | grep -o 'rtmp://[^<"]*'
```

```
rtmp://rtmp.k8s.linkifysolutions.com:1935/8f3a1c9d2e7b4a5f6c0d8e1a3b5c7d9e
```

That whole URL is what OBS wants. The 32 hex characters at the end are the stream key. Keep it:

```sh
export STREAM_KEY=8f3a1c9d2e7b4a5f6c0d8e1a3b5c7d9e
```

Confirm it landed in RDS rather than only in the response:

```sh
pg "SELECT display_name, left(stream_key, 8) || '...' AS key, created_at FROM streamers ORDER BY created_at DESC LIMIT 1"
```

```
 display_name  |     key     |          created_at
---------------+-------------+-------------------------------
 fatima-test   | 8f3a1c9d... | 2026-07-29 14:02:11.481293+00
(1 row)
```

**If this failed:**

- **`grep` found nothing, and the body is an HTML error page.** `kubectl logs -n streaming deploy/web --tail=30`. An httpx `ConnectError` to `upload-api` means `upload-api` has no Ready endpoints — check `kubectl get endpointslice -n streaming -l kubernetes.io/service-name=upload-api`, then `/readyz` on the pod.
- **500, and `upload-api` logs `UndefinedTable: relation "streamers" does not exist`.** The Module 8 schema job never ran against this database. `kubectl get job -n streaming db-migrate` and read its logs.
- **500, and the log shows a connection timeout to RDS.** The RDS security group must allow 5432 from the *node* security group. This is the single most common Module 8 leftover.
- **`curl` itself fails on TLS or DNS.** You're back in Module 12; don't debug it here.

---

## Step 2 — Push RTMP

Copy-pasteable, generates a test pattern with a 440 Hz tone, no camera or capture card required:

```sh
ffmpeg -re \
  -f lavfi -i testsrc2=size=1920x1080:rate=30 \
  -f lavfi -i sine=frequency=440 \
  -c:v libx264 -preset veryfast -b:v 4000k -pix_fmt yuv420p -g 60 \
  -c:a aac -b:a 128k \
  -f flv "rtmp://rtmp.k8s.linkifysolutions.com:1935/${STREAM_KEY}"
```

**`-re` is not optional.** Without it, ffmpeg pushes the synthetic source as fast as it can encode rather than at real-time speed, which is not what a live publisher does. SPEC.md documents an afternoon lost to exactly this: the output appeared to "cut off early", and the cause was the test harness, not the pipeline.

Leave it running. You should see a steadily advancing counter:

```
Output #0, flv, to 'rtmp://rtmp.k8s.linkifysolutions.com:1935/8f3a1c9d...':
  Stream #0:0: Video: h264 ([7][0][0][0] / 0x0007), yuv420p(tv, progressive), 1920x1080, q=2-31, 4000 kb/s, 30 fps
  Stream #0:1: Audio: aac (LC) ([10][0][0][0] / 0x000A), 44100 Hz, mono, fltp, 128 kb/s
frame=  312 fps= 30 q=27.0 size=    1792KiB time=00:00:10.36 bitrate=1416.4kbits/s speed=   1x
```

`speed=1x` is the number that matters — that's `-re` doing its job.

### The OBS equivalent, and the footgun

In OBS: **Settings → Stream → Service: Custom**.

| Field | Value |
|---|---|
| **Server** | `rtmp://rtmp.k8s.linkifysolutions.com:1935/8f3a1c9d2e7b4a5f6c0d8e1a3b5c7d9e` |
| **Stream Key** | *(leave completely blank)* |

**This is a known footgun and it is worth reading twice.** `web`'s `/streamers/new` handler builds the publish URL as `{RTMP_PUBLIC_URL}/{stream_key}` — the key is part of the URL path, not a separate credential. OBS presents Server and Stream Key as two fields, which invites pasting the key into both. Do that and OBS publishes to `rtmp://host/<key>/<key>`. MediaMTX passes `<key>/<key>` to `/hooks/auth` as the path, it matches no row in `streamers`, and the publish is rejected with an unhelpful "Failed to connect to server". SPEC.md flags this too. Server field gets everything; Stream Key stays empty.

**If this failed:**

- **`Connection refused` or `Connection timed out` immediately.** You never reached MediaMTX. `dig +short rtmp.k8s.linkifysolutions.com`, then `aws elbv2 describe-target-health --target-group-arn <arn>`. An empty or unhealthy target group is a Module 12 problem.
- **Connects, then disconnects within a second.** `/hooks/auth` rejected you. This is the interesting failure — go straight to Step 3's logs, which distinguish the causes.
- **`Broken pipe` after a few seconds of apparently working.** MediaMTX accepted the publish and then something killed the flow. Check `kubectl get pods -n streaming -l app=mediamtx` for a restart, and remember the NLB's 350-second idle timeout if the gap was around that long.
- **Connects and hangs with no output counter at all.** ffmpeg is waiting on the RTMP handshake. If you enabled PROXY protocol on the NLB, this is exactly what it looks like — see Module 12, and turn it off.

---

## Step 3 — Confirm `ingest-webhook` authorised it and wrote the row

In a second terminal, with ffmpeg still running:

```sh
kubectl logs -n streaming -l app=ingest-webhook --tail=20 --prefix
```

```
[pod/ingest-webhook-7d4c8b95f6-h9wqr/ingest-webhook] INFO:     10.0.13.204:51224 - "POST /hooks/auth HTTP/1.1" 200 OK
[pod/ingest-webhook-7d4c8b95f6-h9wqr/ingest-webhook] INFO:ingest-webhook:stream started: path=8f3a1c9d2e7b4a5f6c0d8e1a3b5c7d9e stream_id=3f6b2c8a-1d4e-4a7b-9c0f-2e5d8a1b3c6f
[pod/ingest-webhook-7d4c8b95f6-h9wqr/ingest-webhook] INFO:     10.0.13.204:51226 - "POST /hooks/publish HTTP/1.1" 200 OK
```

Two separate hooks, in that order, and they mean different things. `/hooks/auth` is the **gate** — MediaMTX calls it before accepting the publish, and a non-2xx rejects the stream. `/hooks/publish` is `runOnReady`, which fires *after* the stream is already accepted and cannot reject anything; it's what creates the `streams` row and emits the `started` event.

Note the source IP: `10.0.13.204` is the MediaMTX pod, not your laptop. Every call to `ingest-webhook` comes from inside the cluster. That's the invariant the NetworkPolicy encodes.

Now the row:

```sh
pg "SELECT id, left(path,8)||'...' AS path, status, started_at FROM streams ORDER BY started_at DESC LIMIT 1"
```

```
                  id                  |    path     | status |          started_at
--------------------------------------+-------------+--------+-------------------------------
 3f6b2c8a-1d4e-4a7b-9c0f-2e5d8a1b3c6f | 8f3a1c9d... | live   | 2026-07-29 14:06:44.203881+00
(1 row)
```

Save the id — every remaining step uses it, and it's the only identifier that's safe to put in a URL:

```sh
export STREAM_ID=3f6b2c8a-1d4e-4a7b-9c0f-2e5d8a1b3c6f
```

(`path` is the same value as the secret stream key. It never leaves the server side. HLS output is keyed by `stream_id` precisely so that the key never appears in anything a browser sees.)

And confirm MediaMTX agrees it has a live path:

```sh
kubectl exec -n streaming deploy/mediamtx -- \
  wget -qO- http://localhost:9997/v3/paths/list | python3 -m json.tool
```

```json
{
    "pageCount": 1,
    "itemCount": 1,
    "items": [
        {
            "name": "8f3a1c9d2e7b4a5f6c0d8e1a3b5c7d9e",
            "confName": "all_others",
            "source": { "type": "rtmpConn", "id": "..." },
            "ready": true,
            "tracks": ["H264", "MPEG-4 Audio"],
            "bytesReceived": 18443127,
            "readers": [ { "type": "rtmpConn", "id": "..." } ]
        }
    ]
}
```

`"ready": true`, two tracks, `bytesReceived` climbing, and exactly one reader — that reader is `transcode-worker`'s ffmpeg. If `readers` is empty, Step 4 is where the problem is.

**If this failed:**

- **`"POST /hooks/auth HTTP/1.1" 401` and `rejected publish: unknown stream key '...'`.** Look at the path in that log line. If it contains the key twice, you filled in the OBS Stream Key field. If it's a key you don't recognise, you're using a stale one. If it looks right, confirm the row exists: `pg "SELECT count(*) FROM streamers WHERE stream_key = '$STREAM_KEY'"`.
- **No `/hooks/auth` line at all, but ffmpeg connected.** MediaMTX could not reach `ingest-webhook` and defaulted to rejecting. Check DNS and reachability from inside the MediaMTX pod: `kubectl exec -n streaming deploy/mediamtx -- wget -qO- http://ingest-webhook.streaming.svc.cluster.local:8000/livez`. A hang here means the NetworkPolicy is selecting the wrong pods; a DNS failure means CoreDNS or a typo in the ConfigMap.
- **`/hooks/auth` 200 but no `stream started` line.** `runOnReady` didn't fire or didn't land. `kubectl logs -n streaming deploy/mediamtx` — this hook is a `wget` in a shell, so it only works on the `-ffmpeg` image variant (Alpine, has `wget`). If someone changed the image to plain `bluenviron/mediamtx`, auth works and the publish hook silently does nothing.
- **`duplicate publish hook for path=..., stream_id=... already live`.** MediaMTX retried `runOnReady`. The guard did its job; nothing is wrong. But if you see it on a *first* attempt, there's a stale `live` row from an earlier test: `pg "UPDATE streams SET status='ended', ended_at=now() WHERE status='live'"`.
- **`ingest-webhook` returns 500 with a psycopg error.** RDS. Same causes as Step 1.

---

## Step 4 — Confirm the transcode workload picked it up

```sh
kubectl logs -n streaming deploy/transcode-worker --tail=5
```

```
INFO:transcode-worker:live transcode starting (attempt 1): path=8f3a1c9d2e7b4a5f6c0d8e1a3b5c7d9e stream_id=3f6b2c8a-1d4e-4a7b-9c0f-2e5d8a1b3c6f cmd=ffmpeg -y -i rtmp://mediamtx.streaming.svc.cluster.local:1935/8f3a1c9d2e7b4a5f6c0d8e1a3b5c7d9e -filter_complex [0:v]split=3[v0][v1][v2];[v0]scale=w=1920:h=1080[v0out];[v1]scale=w=1280:h=720[v1out];[v2]scale=w=854:h=480[v2out] -map [v0out] -c:v:0 libx264 -preset:v:0 veryfast -b:v:0 5000k ... -f hls -hls_time 4 -hls_flags independent_segments+delete_segments -hls_list_size 30 -master_pl_name master.m3u8 -var_stream_map v:0,a:0,name:1080p v:1,a:1,name:720p v:2,a:2,name:480p ...
```

Read that command, because it encodes several decisions:

- **One ffmpeg process, one decode, `split=3`** — not three processes. Three independent encoders would place keyframes independently, and HLS ABR switching requires the renditions to be keyframe-aligned. `-g 60 -keyint_min 60 -sc_threshold 0` forces that alignment (2-second GOP at 30fps, so exactly two keyframes per 4-second segment).
- **`-preset veryfast`** for live. The encoder must keep up with real time; quality is the thing that gives.
- **`-hls_flags +delete_segments` and `-hls_list_size 30`** — a sliding 30-segment window, about two minutes of DVR range.
- **The input is `rtmp://mediamtx.streaming.svc.cluster.local:1935/...`** — the ClusterIP Service, not the public NLB hostname. That is deliberate; see Module 12 on the `preserve_client_ip` hairpin.

Also confirm the reverse direction — that the pipeline is producing status events:

```sh
kubectl logs -n streaming deploy/analytics-worker --tail=5
```

```
INFO:analytics-worker:[transcode.status] {'event': 'live_transcode_started', 'ts': 1785405000.12, 'path': '8f3a1c9d2e7b4a5f6c0d8e1a3b5c7d9e', 'stream_id': '3f6b2c8a-1d4e-4a7b-9c0f-2e5d8a1b3c6f'}
```

`analytics-worker` consumes `transcode.status` and logs it without aggregating, which makes it a convenient window onto Kafka without running a console consumer.

**If this failed:**

- **No log line at all.** `transcode-worker` isn't consuming. Check the pod is Running and check the consumer group's lag:
  ```sh
  kubectl -n kafka run kt --rm -it --restart=Never --quiet \
    --image=quay.io/strimzi/kafka:0.49.0-kafka-3.9.0 -- \
    bin/kafka-consumer-groups.sh \
      --bootstrap-server streaming-kafka-bootstrap.kafka.svc.cluster.local:9092 \
      --describe --group transcode-worker
  ```
  Non-zero `LAG` means it's connected but stuck. `Consumer group has no active members` means it never connected — usually a wrong `KAFKA_BOOTSTRAP_SERVERS`.
- **`no stream_id for path=..., refusing to start`.** The `started` event arrived without a `stream_id`. That means `ingest-webhook` emitted it from the duplicate-guard branch, or an older image is running.
- **`ffmpeg exited 1 for path=...` with `Connection refused` in the stderr tail.** The transcode pod can't reach MediaMTX. `kubectl exec -n streaming deploy/transcode-worker -- python -c "import socket;socket.create_connection(('mediamtx.streaming.svc.cluster.local',1935),5)"` — silence is success. If it connects but ffmpeg still fails, check MediaMTX's replica count: with more than one replica the ClusterIP round-robins and half of all connections land on a pod that has never heard of that path. That is why `replicas: 1` is a correctness constraint.
- **`ffmpeg wedged (no new segment for 20s) for path=..., restarting`.** The stall watchdog fired. Confirm the running config actually has the large write queue:
  ```sh
  kubectl exec -n streaming deploy/mediamtx -- \
    wget -qO- http://localhost:9997/v3/config/global/get | grep -o '"writeQueueSize":[0-9]*'
  ```
  ```
  "writeQueueSize":65536
  ```
  If it says 512, the ConfigMap didn't mount where you think it did and MediaMTX fell back to its defaults.
- **Pod `OOMKilled`.** The 1 GiB request was measured against a 1080p source. A higher-resolution input needs more; raise request and limit together.
- **Pod evicted with `The node was low on resource: ephemeral-storage`.** The `emptyDir` `sizeLimit`, or the node's root volume. Karpenter nodes need a root volume comfortably above the scratch requirement.

---

## Step 5 — Confirm HLS lands in S3

Give it about thirty seconds after the transcode starts, then:

```sh
aws s3 ls "s3://${BUCKET}/hls/live/${STREAM_ID}/" --recursive | head -20
```

```
2026-07-29 14:07:12       1043 hls/live/3f6b2c8a-.../1080p/playlist.m3u8
2026-07-29 14:07:08    2094336 hls/live/3f6b2c8a-.../1080p/segment_000.ts
2026-07-29 14:07:12    2081792 hls/live/3f6b2c8a-.../1080p/segment_001.ts
2026-07-29 14:07:12        997 hls/live/3f6b2c8a-.../480p/playlist.m3u8
2026-07-29 14:07:08     612352 hls/live/3f6b2c8a-.../480p/segment_000.ts
2026-07-29 14:07:12        998 hls/live/3f6b2c8a-.../720p/playlist.m3u8
2026-07-29 14:07:08    1180672 hls/live/3f6b2c8a-.../720p/segment_000.ts
2026-07-29 14:07:12        512 hls/live/3f6b2c8a-.../master.m3u8
```

Three rendition directories, each with a `playlist.m3u8` and segments, plus one `master.m3u8` at the top. Run it again twenty seconds later — **the segment numbers must be increasing.** A static listing means ffmpeg wrote a few segments and stopped.

Now the headers, which are the part people skip and then spend a day on:

```sh
aws s3api head-object --bucket "$BUCKET" \
  --key "hls/live/${STREAM_ID}/master.m3u8"
```

```json
{
    "LastModified": "2026-07-29T14:07:12+00:00",
    "ContentLength": 512,
    "ETag": "\"a1b2c3d4e5f60718293a4b5c6d7e8f90\"",
    "CacheControl": "no-cache, must-revalidate",
    "ContentType": "application/vnd.apple.mpegurl",
    "Metadata": {}
}
```

```sh
aws s3api head-object --bucket "$BUCKET" \
  --key "hls/live/${STREAM_ID}/1080p/segment_001.ts" \
  --query '[ContentType,CacheControl]'
```

```json
[
    "video/mp2t",
    "public, max-age=31536000, immutable"
]
```

**Why those two headers matter, precisely.**

`ContentType: application/vnd.apple.mpegurl` is what makes a playlist a playlist. Neither S3 nor boto3 guesses MIME types for `.m3u8` or `.ts`; without an explicit `ContentType` they'd be served as `binary/octet-stream`. Safari's native HLS player refuses to touch that, and hls.js's behaviour depends on the browser. `transcode-worker`'s `_s3_extra_args` sets it on every upload.

`CacheControl: no-cache, must-revalidate` on a **live** manifest is load-bearing in a way the segment header is not. A live `playlist.m3u8` is rewritten every few seconds: new segments are appended and old ones drop out of the sliding window, and `+delete_segments` removes the corresponding files. A cached copy therefore references segments that no longer exist. The player fetches the stale manifest, requests a segment that has been deleted, gets a 404, and freezes — while the stream is perfectly healthy. This exact bug already happened once in the browser's own HTTP cache ("works in a fresh tab, breaks on refresh"), which is why the header exists at all. Behind CloudFront it comes back as a CDN-cache version of the same bug, which is why Module 9's cache behaviour for `*.m3u8` has a near-zero TTL and honours the origin's `Cache-Control` rather than overriding it.

Segments get the opposite treatment — `max-age=31536000, immutable` — because a segment file, once written under a given key, is never rewritten. A VOD manifest gets the long treatment too: it's written once, finalised with `EXT-X-ENDLIST`, and never touched again.

**If this failed:**

- **Nothing in the bucket, and the transcode log looks fine.** Credentials. `_upload_pass` logs and swallows exceptions rather than crashing, so this fails *quietly*: `kubectl logs -n streaming deploy/transcode-worker | grep -i "failed to upload"`. Then check the identity the pod actually has:
  ```sh
  kubectl exec -n streaming deploy/transcode-worker -- \
    python -c "import boto3;print(boto3.client('sts').get_caller_identity()['Arn'])"
  ```
  ```
  arn:aws:sts::123456789012:assumed-role/streaming-transcode-worker/eks-streaming-...
  ```
  If that shows the *node* role instead, the Pod Identity association is missing, the ServiceAccount name doesn't match, or the pod started before the association existed — `kubectl rollout restart deploy/transcode-worker`.
- **`InvalidAccessKeyId` or `SignatureDoesNotMatch`.** `S3_ACCESS_KEY`/`S3_SECRET_KEY` are still set in the environment, and explicit credentials win over the credential chain. `kubectl exec -n streaming deploy/transcode-worker -- env | grep -c S3_ACCESS_KEY` must print `0`.
- **`Could not connect to the endpoint URL`.** `S3_ENDPOINT` is still pointing at something MinIO-shaped. It must be *unset* on AWS.
- **`AccessDenied` on `PutObject`.** The IAM role from Module 7 grants `s3:PutObject` on `hls/*` only. Check the key prefix in the error message — writing outside `hls/` means the code is using a different prefix than you think.
- **`ContentType` is `binary/octet-stream`.** An older image without `_s3_extra_args`. Check the tag.
- **Segments stop advancing.** Back to Step 4's watchdog block.

---

## Step 6 — Confirm playback through CloudFront

```sh
curl -sSI "https://cdn.k8s.linkifysolutions.com/hls/live/${STREAM_ID}/master.m3u8"
```

```
HTTP/2 200
content-type: application/vnd.apple.mpegurl
content-length: 512
cache-control: no-cache, must-revalidate
x-cache: Miss from cloudfront
via: 1.1 8f2c1a9d3e4b5f6a.cloudfront.net (CloudFront)
```

Note `cache-control` survived the trip — CloudFront is passing the origin's header through, not substituting its own. That's the Module 9 cache policy working.

Now the content:

```sh
curl -sS "https://cdn.k8s.linkifysolutions.com/hls/live/${STREAM_ID}/master.m3u8"
```

```
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-STREAM-INF:BANDWIDTH=5128000,RESOLUTION=1920x1080,CODECS="avc1.64002a,mp4a.40.2"
1080p/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2928000,RESOLUTION=1280x720,CODECS="avc1.64001f,mp4a.40.2"
720p/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=1528000,RESOLUTION=854x480,CODECS="avc1.64001e,mp4a.40.2"
480p/playlist.m3u8
```

Count them:

```sh
curl -sS "https://cdn.k8s.linkifysolutions.com/hls/live/${STREAM_ID}/master.m3u8" \
  | grep -c EXT-X-STREAM-INF
```

```
3
```

Three variants is the ABR ladder. Fewer means a rendition directory is missing from S3.

**The CORS preflight.** The watch page is served from `stream.k8s...` and hls.js fetches manifests from `cdn.k8s...` — a cross-origin request. This is the check that catches the single most confusing failure in the whole migration:

```sh
curl -sS -o /dev/null -D - -X OPTIONS \
  -H 'Origin: https://stream.k8s.linkifysolutions.com' \
  -H 'Access-Control-Request-Method: GET' \
  "https://cdn.k8s.linkifysolutions.com/hls/live/${STREAM_ID}/master.m3u8" \
  | grep -i 'access-control'
```

```
access-control-allow-origin: https://stream.k8s.linkifysolutions.com
access-control-allow-methods: GET, HEAD, OPTIONS
access-control-expose-headers: Content-Length, Content-Range, ETag
access-control-max-age: 3000
```

Why this deserves its own step: MinIO reflected the `Origin` header for free, so nothing in the Compose setup ever configured CORS. S3 does not. Without these headers the browser blocks the fetch while the network tab shows a perfectly good **200 OK** — the object is there, the URL is right, the response is fine, and the player says "Playback error". Worse, Safari uses native HLS (`video.src = ...`), which is not subject to CORS for media elements, so **it works on your Mac in Safari and fails in Chrome.** Do not let that mislead you.

Sanity-check the actual media before involving a browser:

```sh
ffplay "https://cdn.k8s.linkifysolutions.com/hls/live/${STREAM_ID}/master.m3u8"
```

A window with the test pattern and a 440 Hz tone. `ffplay` doesn't do CORS, which is exactly why it is a *separate* check from the one above and not a substitute for it.

**Now the real thing.** Open in a browser:

```
https://stream.k8s.linkifysolutions.com/watch/stream/3f6b2c8a-1d4e-4a7b-9c0f-2e5d8a1b3c6f
```

You should see the test pattern playing, with the streamer's display name as the heading. The homepage at `https://stream.k8s.linkifysolutions.com` should now list the stream as live with a view count.

Open the browser devtools network tab and watch it for twenty seconds: `master.m3u8` fetched once, then `playlist.m3u8` re-fetched every few seconds (each a 200, not a 304 — that's `no-cache, must-revalidate`), and `segment_NNN.ts` files arriving in sequence.

**If this failed:**

- **403 from CloudFront.** Origin Access Control. The bucket is fully private now — no anonymous public-read prefix, unlike the MinIO setup — so every read must come through the distribution. The bucket policy needs `s3:GetObject` granted to `cloudfront.amazonaws.com` with an `AWS:SourceArn` condition naming the distribution.
- **404, and the path contains `/media/`.** The Module 0 change to `web/main.py` didn't land. `/media` was MinIO's *bucket name* in a path-style URL; behind CloudFront the origin is one bucket and there is no bucket segment in the path. `kubectl exec -n streaming deploy/web -- env | grep HLS_PUBLIC_BASE_URL` — it must have no `/media` suffix.
- **Player loads, then freezes after about 20 seconds.** This is the likeliest CloudFront problem and it is the stale-manifest bug from Step 5, moved to the CDN. Confirm with `curl -sSI` twice, twenty seconds apart, and compare `x-cache` and `last-modified`. A `Hit from cloudfront` on a live `.m3u8` is the smoking gun. Fix in Module 9's cache behaviours: near-zero TTL for `*.m3u8`, `CachingOptimized` for `*.ts`.
- **Browser console shows a CORS error, but `ffplay` works.** The preflight check above already told you this. Note the sneaky variant: if CORS is configured only on the S3 bucket and not as a CloudFront response headers policy, the first request to reach a given edge — possibly your own `curl` with no `Origin` header — populates the cache with a header-less response, and every browser after that gets the cached copy. That produces intermittent, edge-dependent CORS failures that appear to fix themselves. The response headers policy makes CloudFront synthesise the header on every response regardless of what's cached.
- **Plays, but only one quality.** `master.m3u8` is there and the variant playlists 404. Check the `%v` subdirectories exist in S3; `_make_rendition_dirs` creates them because ffmpeg's HLS muxer does not.
- **View count stays 0.** `kubectl logs -n streaming deploy/analytics-worker` — the `viewer.analytics` event goes `web` → `upload-api` → Kafka → `analytics-worker` → Postgres, so a missing count means one of those four hops, and the log tells you which.

---

## Step 7 — Stop the stream and confirm teardown

Ctrl-C the ffmpeg (or Stop Streaming in OBS).

```sh
kubectl logs -n streaming -l app=ingest-webhook --tail=10 --prefix
```

```
[pod/ingest-webhook-7d4c8b95f6-h9wqr/ingest-webhook] INFO:ingest-webhook:stream ended: path=8f3a1c9d2e7b4a5f6c0d8e1a3b5c7d9e stream_id=3f6b2c8a-1d4e-4a7b-9c0f-2e5d8a1b3c6f
[pod/ingest-webhook-7d4c8b95f6-h9wqr/ingest-webhook] INFO:     10.0.13.204:51402 - "POST /hooks/unpublish HTTP/1.1" 200 OK
```

The DB row closes:

```sh
pg "SELECT status, started_at, ended_at, ended_at - started_at AS duration FROM streams WHERE id = '$STREAM_ID'"
```

```
 status |          started_at           |           ended_at            |    duration
--------+-------------------------------+-------------------------------+-----------------
 ended  | 2026-07-29 14:06:44.203881+00 | 2026-07-29 14:11:57.882014+00 | 00:05:13.678133
(1 row)
```

The transcode winds down. There is no "stopping" log line today — the signal is the `live_transcode_stopped` event, which `analytics-worker` prints:

```sh
kubectl logs -n streaming deploy/analytics-worker --tail=5
```

```
INFO:analytics-worker:[transcode.status] {'event': 'live_transcode_stopped', 'ts': 1785405117.44, 'path': '8f3a1c9d2e7b4a5f6c0d8e1a3b5c7d9e', 'stream_id': '3f6b2c8a-1d4e-4a7b-9c0f-2e5d8a1b3c6f'}
```

Confirm the ffmpeg process is actually gone rather than orphaned:

```sh
kubectl exec -n streaming deploy/transcode-worker -- python -c \
  "import glob;print(sum('ffmpeg' in open(f,'rb').read().decode(errors='ignore') for f in glob.glob('/proc/*/cmdline')))"
```

```
0
```

(`ps` is not in the `python:3.12-slim` base image, hence reading `/proc` directly.)

And MediaMTX has no live path:

```sh
kubectl exec -n streaming deploy/mediamtx -- \
  wget -qO- http://localhost:9997/v3/paths/list | grep -o '"itemCount":[0-9]*'
```

```
"itemCount":0
```

The HLS objects stay in S3 — nothing deletes them at teardown, by design. Module 9's lifecycle rule on `hls/live/` is what reclaims them, and it is the only thing that does. If you skipped that rule, this is where the bill starts growing quietly.

**If this failed:**

- **`stream ended` logged but ffmpeg still running.** The `ended` event didn't reach `transcode-worker`, or it reached a different `path` than the one it claimed. Check the topic directly:
  ```sh
  kubectl -n kafka run kt --rm -it --restart=Never --quiet \
    --image=quay.io/strimzi/kafka:0.49.0-kafka-3.9.0 -- \
    bin/kafka-console-consumer.sh \
      --bootstrap-server streaming-kafka-bootstrap.kafka.svc.cluster.local:9092 \
      --topic stream.lifecycle --from-beginning --max-messages 10
  ```
  You want a `{"event": "ended", "path": "...", "stream_id": "..."}` matching the `started` you saw earlier.
- **`no active live transcode for path=... (already stopped, or never started)`.** `transcode-worker` restarted at some point between `started` and `ended`. `active_streams` is in-process memory, so a restart loses the mapping — and the ffmpeg child dies with the container, so nothing is orphaned. Harmless here; it is one of the reasons Module 14 moves this state out of memory.
- **`streams` row still `live`.** `/hooks/unpublish` never fired. Same MediaMTX-side causes as `runOnReady` in Step 3. A stale `live` row matters more than it looks: once the Module 14 admission check lands, stale rows count toward the concurrency ceiling and will eventually block all new streams. Reset with `pg "UPDATE streams SET status='ended', ended_at=now() WHERE status='live'"`.

---

## Step 8 — The VOD path

Different entry point, same back half. Make a short test file:

```sh
ffmpeg -f lavfi -i testsrc2=size=1920x1080:rate=30:duration=30 \
       -f lavfi -i sine=frequency=440:duration=30 \
       -c:v libx264 -preset veryfast -pix_fmt yuv420p -c:a aac /tmp/test.mp4
```

Upload it through the public UI:

```sh
curl -sS -X POST https://stream.k8s.linkifysolutions.com/upload \
  -F "stream_key=${STREAM_KEY}" \
  -F "title=verify-vod" \
  -F "file=@/tmp/test.mp4" | grep -o '"id":"[^"]*"'
```

```
"id":"7c1e9d4b-3a8f-4d2c-b6e0-5f9a1c3d7e2b"
```

```sh
export VIDEO_ID=7c1e9d4b-3a8f-4d2c-b6e0-5f9a1c3d7e2b
```

Watch the status walk through its three values. Run this a few times over the next minute or two:

```sh
pg "SELECT title, status, raw_object_key FROM videos WHERE id = '$VIDEO_ID'"
```

```
   title    |   status    |               raw_object_key
------------+-------------+---------------------------------------------
 verify-vod | uploaded    | raw/7c1e9d4b-3a8f-4d2c-b6e0-5f9a1c3d7e2b.mp4
```

then

```
 verify-vod | transcoding | raw/7c1e9d4b-...mp4
```

then

```
 verify-vod | ready       | raw/7c1e9d4b-...mp4
```

`uploaded` is written by `upload-api` after the raw file lands in S3. `transcoding` is `transcode-worker` claiming the job with an atomic `UPDATE videos SET status='transcoding' WHERE id=%s AND status='uploaded'` — that `WHERE` clause is the whole dedup mechanism, and unlike the live path's in-memory guard it survives a restart. `ready` is written after the last rendition is uploaded.

The raw object and the output:

```sh
aws s3 ls "s3://${BUCKET}/raw/${VIDEO_ID}.mp4"
aws s3 ls "s3://${BUCKET}/hls/vod/${VIDEO_ID}/" --recursive | wc -l
```

```
2026-07-29 14:15:03   14238721 raw/7c1e9d4b-3a8f-4d2c-b6e0-5f9a1c3d7e2b.mp4
      13
```

A VOD manifest is finalised, unlike a live one:

```sh
curl -sS "https://cdn.k8s.linkifysolutions.com/hls/vod/${VIDEO_ID}/1080p/playlist.m3u8" | tail -3
```

```
#EXTINF:2.000000,
segment_007.ts
#EXT-X-ENDLIST
```

`#EXT-X-ENDLIST` is what `-hls_playlist_type vod` produces, and it's what tells the player this is a finite thing it can seek around in. Its `Cache-Control` differs accordingly:

```sh
aws s3api head-object --bucket "$BUCKET" \
  --key "hls/vod/${VIDEO_ID}/master.m3u8" --query 'CacheControl'
```

```
"public, max-age=31536000, immutable"
```

Then play it:

```
https://stream.k8s.linkifysolutions.com/watch/video/7c1e9d4b-3a8f-4d2c-b6e0-5f9a1c3d7e2b
```

**If this failed:**

- **413.** `MAX_UPLOAD_BYTES`. Module 12 lowered it to 2 GiB in `overlays/prod/config.env`.
- **429.** The per-streamer rate limit — ten uploads per hour, counted from the `videos` table. You hit it while testing. Create a fresh streamer.
- **The connection resets and `web` restarts.** `kubectl describe pod -n streaming <web-pod> | grep -i -A2 'last state'` showing `OOMKilled` is the `await file.read()` bug from Module 12. It presents as a browser reset, not an error page.
- **Stuck at `uploaded` forever.** The `upload.events` message was produced but nothing consumed it. Check `transcode-worker`'s lag on the `transcode-worker` group.
- **Stuck at `transcoding` forever.** The worker claimed the job and then died. Because the claim lives in Postgres, a redelivered event will *not* retry it — that's the deliberate trade-off documented in the code. Read the logs for the ffmpeg failure, then reset by hand: `pg "UPDATE videos SET status='uploaded' WHERE id='$VIDEO_ID'"` and re-produce the event. Module 14 adds a sweeper for this.
- **`failed`.** ffmpeg rejected the input. `kubectl logs -n streaming deploy/transcode-worker | grep "ffmpeg failed"` prints the last 2000 characters of its stderr.

---

## What you just proved

Each step closed the loop on a module you built earlier. If everything above passed, all of this is now demonstrated rather than assumed:

| Step | What it proved | Built in |
|---|---|---|
| 1 | Public HTTPS reaches `web`; `web` reaches `upload-api`; `upload-api` writes to RDS | [5](05-dns-and-certificates.md), [8](08-rds-postgres.md), [12](12-ingress-and-rtmp.md) |
| 2 | The NLB passes raw TCP to MediaMTX with the handshake intact, from the public internet | [12](12-ingress-and-rtmp.md) |
| 3 | MediaMTX resolves in-cluster DNS, the auth gate rejects and accepts correctly, and the lifecycle event reaches Kafka | [10](10-kafka-strimzi.md), [11](11-workloads.md) |
| 4 | A consumer picked the event off Kafka and started the right ffmpeg against the right in-cluster source | [10](10-kafka-strimzi.md), [11](11-workloads.md) |
| 5 | The pod's IAM identity is its own role, not the node's, and it can write only where it should | [7](07-secrets.md), [9](09-s3-and-cloudfront.md) |
| 6 | CloudFront serves a private bucket through OAC, with cache behaviours and CORS that a real player accepts | [9](09-s3-and-cloudfront.md) |
| 7 | Teardown is event-driven end to end — no polling, no orphaned processes, no stuck rows | [10](10-kafka-strimzi.md), [11](11-workloads.md) |
| 8 | The second entry point shares the same back half, with idempotency that survives a restart | [8](08-rds-postgres.md), [9](09-s3-and-cloudfront.md), [11](11-workloads.md) |

What you have **not** proved, and shouldn't claim: that any of it scales. One stream ran on a `transcode-worker` Deployment that handles every stream in one process, on nodes you provisioned by hand. Start a second stream now and you'll see the ceiling — which is precisely what [Module 14](14-keda-scaledjobs.md) is for.

Before you move on, stop paying for an idle cluster if you're taking a break: [Module 16](16-monitoring-cost-teardown.md)'s cost section has the scale-to-zero commands.

---

Next: [Module 14 — Transcoding that scales](14-keda-scaledjobs.md).
