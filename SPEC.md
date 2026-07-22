# Streaming Platform — Preliminary Spec

Learning-focused project. Priority is depth of understanding of **ffmpeg, Kafka/event-driven systems, and Kubernetes deployment** — not shipping the leanest possible product. Where those goals conflict with "simplest solution," we favor the one that teaches more, within reason.

## What it does

Anyone can either go live via OBS (RTMP) or upload a video file. Either way, the result is watchable by others. No accounts beyond a per-streamer stream key (Twitch-style); viewers are anonymous.

## Two-phase build

1. **Pipeline first** — everything runs as Docker Compose on a dev laptop. This is where nearly all the ffmpeg/Kafka learning happens.
2. **Kubernetes second** — same stack redeployed on a self-managed kubeadm cluster on an Ubuntu box (dev), then AWS/EKS (prod). Phase 1 choices are made so this lift doesn't require a rework (e.g. MinIO in dev mirrors the S3 API).

## Shape of the system

```
OBS ──RTMP──▶ MediaMTX ──webhook──▶ ingest-webhook (validates stream key) ──▶ Kafka: stream.lifecycle
                                                                                        │
Browser ──upload──▶ upload-api ──▶ MinIO (raw) ──▶ Kafka: upload.events              │
                                                                                        ▼
                                                                          transcode-worker (ffmpeg)
                                                                                        │
                                                                    HLS renditions ──▶ MinIO/S3
                                                                                        │
                                                        Kafka: transcode.status ◀───────┘
Viewer ──HTTP──▶ web app ──reads manifests/segments from MinIO/S3, metadata from Postgres
                       │
                       └──▶ Kafka: viewer.analytics
```

## Settled decisions

These came out of earlier planning + a research pass validating them against real systems (Twitch, Mux, Owncast, SRS) — treat as fixed unless something concrete forces a change:

- **Language**: Python for all custom services.
- **Ingest**: MediaMTX (RTMP in, webhooks on publish/unpublish).
- **Kafka**: the event backbone — stream lifecycle, transcode job dispatch, transcode status, viewer analytics all flow through it. Topics partitioned by stream ID so per-stream ordering holds.
- **Transcoding**: one ffmpeg process per stream (`-filter_complex split`), not one process per rendition — needed for GOP/keyframe alignment across the ABR ladder, which HLS requires. Ladder: 1080p (5000k)/720p (2800k)/480p (1400k), 4s segments, 2s GOP, `veryfast` preset for live / `medium` for VOD. `transcode-worker` consumes `stream.lifecycle`/`upload.events` directly — no separate dispatcher onto `transcode.jobs` (that topic still exists in Kafka, unused for now).
- **Storage**: MinIO (dev) / S3 (prod), same API.
- **Metadata**: Postgres (users/streams/videos/stream-keys).
- **Auth**: stream keys only, no login system.
- **Dev orchestration**: Docker Compose, then kubeadm (not k3s/microk8s) on the Ubuntu box, then AWS.
- **Frontend**: a small real web app (browse/watch/upload) — not just a bare player page, not a full SPA-scale build either.

## Services (Phase 1)

| Service | Role | Status |
|---|---|---|
| `ingest-webhook` | MediaMTX auth (`/hooks/auth`, validates stream key before publish is accepted) + publish/unpublish hooks → tracks `streams` rows → emits `stream.lifecycle` | real |
| `upload-api` | Stream key issuance (`POST /streamers`), browse (`GET /streams`, `GET /videos`), upload (`POST /videos` → MinIO + `upload.events`) | real |
| `transcode-worker` | Real ffmpeg ABR transcode (single process, 1080p/720p/480p, GOP-aligned) for both live (RTMP, real-time) and VOD (uploaded file); writes HLS to MinIO under `hls/live/{stream_id}/` or `hls/vod/{video_id}/`; emits `transcode.status` | real |
| `analytics-worker` | Consumes `viewer.analytics`, writes `view_events` rows to Postgres; consumes `transcode.status` (logs only, no aggregation needed there yet) | real |
| `web` | Homepage (live streams + videos, with view counts), get-a-stream-key form, upload form, `/watch/{type}/{id}` with a real `hls.js` player | real |

Infra containers: MediaMTX, Kafka, Postgres, MinIO — all up, topics/bucket/schema created. MinIO's `media/hls` prefix is public-read (dev-only convenience, see SPEC caveat below) so a video's `master.m3u8` is directly fetchable/playable (e.g. in VLC) once transcoded.

Postgres schema (`postgres/init.sql`): `streamers(id, display_name, stream_key)`, `streams(id, streamer_id, path, status, started_at, ended_at)`, `videos(id, streamer_id, title, raw_object_key, status)`, `view_events(id, content_type, content_id, created_at)` (raw log; counts derived via `GROUP BY`, not a running counter column).

**Design smell fixed**: live HLS output in MinIO is now keyed by `stream_id` (the safe, public `streams.id` UUID), not by MediaMTX's `path` (which is still the same value as the stream's secret auth key — that part is unchanged, `path` is only ever used server-side to read the RTMP source, never exposed to a browser). `transcode-worker` writes to `hls/live/{stream_id}/...` instead of `hls/live/{path}/...`.

`web` now has a real `hls.js` player at `/watch/{type}/{id}` (falls back to native HLS on Safari) for both live and VOD content, pointed at MinIO's public-read `hls/` URLs. Verified working: manifest fetches 200 with correct CORS headers (MinIO reflects `Access-Control-Allow-Origin` by default), full ABR ladder present for both a VOD upload and a live stream.

## Deliberately left for just-in-time decision

Not deciding these now — they'll get resolved when we're actually building that piece, so the decision is informed by what exists at that point rather than guessed in advance:

- Kafka topic partition counts (currently 3 per topic as a placeholder — need real headroom numbers for KEDA later)
- Exactly-once/idempotent transcode-job dedup mechanism
- Strimzi vs. Bitnami Kafka chart, and other Phase 2 k8s manifest specifics
- Observability (metrics/logs/dashboards) and CI/CD — not addressed at all yet, revisit once the core pipeline works

## Out of scope for now

Chat, multi-tenant accounts beyond stream keys, DASH (HLS only), transcoding GPU acceleration, CDN.

## Where it currently runs

Docker Compose stack, deployed on a remote Ubuntu box (Tailscale hostname `aayush-internserver.hs.aayushpathak.com`, reachable at `100.64.0.9`), not on any contributor's laptop. Deploy flow (no CI yet): sync files over with `tar czf - <paths> | ssh kubernetes-server "cd ~/streaming-project && tar xzf -"`, then `docker compose up -d --build <service>` on the box. Schema changes to the already-running Postgres are applied by hand via `docker compose exec postgres psql` (`postgres/init.sql` only auto-runs on a fresh volume). Confirmed: Kubernetes is **not** installed on that box yet, despite the name — Phase 2 hasn't started.

## Next steps

Roughly in order:

1. **Phase 1 hardening** (cheap, do before or alongside Phase 2):
   - Exactly-once/idempotent transcode-job handling — right now a redelivered `stream.lifecycle`/`upload.events` message would kick off a duplicate transcode.
   - Size Kafka topic partitions for real (currently 3 everywhere, a placeholder) — matters once KEDA autoscaling enters the picture.
   - Set correct `Content-Type` on MinIO uploads (`.m3u8`/`.ts` currently upload as `binary/octet-stream` since boto3's MIME guessing doesn't know HLS extensions — works with `hls.js` today but worth fixing before relying on anything stricter).
   - Basic auth/rate-limiting on `upload-api` — right now anyone with a stream key can upload arbitrarily large/many files.
2. **Phase 2: Kubernetes (dev)** — install kubeadm on the Ubuntu box (`kubernetes-server`; confirmed not installed as of 2026-07-21 despite the hostname), then port the Compose stack to k8s manifests: Kafka via Strimzi (leaning this way for the operator-pattern learning value, not yet decided — see SPEC's settled-decisions section), Postgres/MinIO as StatefulSets or Bitnami Helm charts, MediaMTX's RTMP port exposed via a plain NodePort (validated against SRS's own reference k8s deployment in the research pass), transcode-worker as a Deployment with resource requests sized from real ffmpeg CPU benchmarks (not yet measured — do this empirically, see research memory).
3. **CI/CD** — not started. At minimum: build+push images on merge, maybe lint/basic tests for the Python services.
4. **Observability** — not started. Prometheus/Grafana would be a natural fit once things are on k8s; even basic structured logging would help before that.
5. **Phase 3: AWS (prod)** — EKS, real S3, MSK or self-hosted Kafka, RDS, NLB with TCP passthrough for RTMP (watch for the long-lived-TCP-through-managed-LB failure class documented in the research pass before assuming it "just works").

See the `research_validated_architecture` conversation history / memory for the research and empirical findings backing these decisions (not reproduced here to keep this doc short).
