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
- **Kafka**: the event backbone — stream lifecycle, transcode job dispatch, transcode status, viewer analytics all flow through it. Topics partitioned by stream ID so per-stream ordering holds. **Phase 2 runtime: Strimzi operator** (not Bitnami) — CRD-based (`Kafka`/`KafkaTopic`/`KafkaUser`) declarative management fits the project's k8s-learning goal better than a plain Helm chart, and as of Broadcom's 2025 Bitnami restructuring, Bitnami's free images are unpinned-`latest`-only (pinned versions require a paid Bitnami Secure Images subscription) — a real operational risk, not just a style preference. Strimzi also ships a built-in Prometheus JMX exporter + ready-made Grafana dashboards, which feeds directly into the Observability decision below.
- **Secrets (Phase 2)**: SOPS + `age`. Encrypted values committed to git under `k8s/secrets/*.enc.yaml`, decrypted at apply time (`sops -d ... | kubectl apply -f -` for now — no CI yet, see "Where it currently runs"). The `age` private key itself is never committed; lives only on the deploying machine/box. Chosen over External Secrets Operator for Phase 2: ESO's payoff (same manifests, repoint at AWS Secrets Manager in Phase 3) wasn't worth running an extra in-cluster operator before Phase 2 even has its first workload up — revisit ESO specifically when Phase 3 (AWS) starts, since that's exactly the case it's built for.
- **Observability (Phase 2)**: `kube-prometheus-stack` (Prometheus Operator + Grafana + Alertmanager + node-exporter + kube-state-metrics), one Helm chart. Gives infra-level dashboards (node/pod CPU & memory, Job/Deployment status, restarts) for free, plus Kafka consumer-lag-per-topic and broker health for free once Strimzi's metrics are scraped in — no extra work on our side for either layer. App-level metrics (active live-stream count, HTTP latency for `web`/`upload-api`) need our own services instrumented with a Prometheus client and a `/metrics` endpoint — real but modest follow-up work, not blocking the infra rollout.
- **CI/CD**: GitHub Actions, registry is **GitHub Container Registry (`ghcr.io`)** — free, uses the automatic `GITHUB_TOKEN`, already where the repo lives. CI (every PR + push to `master`): `ruff` lint + `hadolint` on the five Dockerfiles, a small starting test set (`build_ffmpeg_command` unit tests — pure function, and exactly where this session found/fixed real bugs; `ingest-webhook`'s `/hooks/auth` accept/reject logic; a `web` `/health` smoke test), then a `dorny/paths-filter`-driven matrix build so a change to one service doesn't rebuild all five — tag images `sha-<short-sha>` + `latest` on `master`. CD is deliberately **not** the same mechanism across phases:
  - **Phase 1 (now, Compose)**: a **self-hosted GitHub Actions runner installed on the Ubuntu box itself** (systemd service, dedicated non-root user, `docker` group only — not sudo) runs `docker compose pull/up` locally on push to `master`. No SSH key, no Tailscale-join step, no network hop at all — deliberately chosen over a Tailscale-joined hosted runner. Since the box already has real CPU contention (Kafka's bursty log-segment work, live-transcode spikes — both observed this session), the runner service gets a `Nice=`/`CPUWeight=` systemd constraint so a CI build doesn't steal cycles from a live stream mid-encode. Also means shifting the box's compose files from `build:` (rebuild-from-source-on-the-box, today's model) to `image: ghcr.io/...:sha` (run the exact artifact CI built).
  - **Phase 2 (k8s)**: **ArgoCD**, pull-based — it runs inside the cluster and watches a git repo of manifests, so CI never needs network access to the box/cluster at all in this phase either; CI's job shrinks to "build, push, bump the image tag in a manifest path." Chosen over Flux for the UI/dashboard as a distinct learning artifact, at a real cost: ArgoCD has **no built-in SOPS decryption** (unlike Flux). Needs **KSOPS** (a Kustomize exec plugin) — a custom `repo-server` image with `sops`/`ksops` baked in, plus the `age` private key mounted into that pod as a k8s Secret. Extra infra to stand up, not just a config flag.
- **Transcoding**: one ffmpeg process per stream (`-filter_complex split`), not one process per rendition — needed for GOP/keyframe alignment across the ABR ladder, which HLS requires. Ladder: 1080p (5000k)/720p (2800k)/480p (1400k), 4s segments, 2s GOP, `veryfast` preset for live / `medium` for VOD. `transcode-worker` consumes `stream.lifecycle`/`upload.events` directly — no separate dispatcher onto `transcode.jobs` (that topic still exists in Kafka, unused for now).
- **Storage**: MinIO (dev) / S3 (prod), same API.
- **Metadata**: Postgres (users/streams/videos/stream-keys).
- **Auth**: stream keys only, no login system.
- **Dev orchestration**: Docker Compose, then kubeadm (not k3s/microk8s) on the Ubuntu box, then AWS.
- **Frontend**: a small real web app (browse/watch/upload) — not just a bare player page, not a full SPA-scale build either.
- **Live + VOD transcode scaling (Phase 2)**: one Kubernetes **Job** per unit of work (one live stream, one video upload) — not a shared pool of `transcode-worker` replicas, and not a hand-rolled dispatcher service either. **KEDA `ScaledJob`** creates these directly off Kafka, replacing the custom-dispatcher idea from earlier planning with an off-the-shelf primitive. Real code implication, not just a deployment-topology change: today's `transcode-worker/main.py` is a persistent process with its own `while True` Kafka consumer loop handling every stream/upload sequentially for the container's whole lifetime — Job pods run-to-completion, so this needs a one-shot entrypoint instead (read `stream_id`/`path` or `video_id`/`object_key` from pod env vars the `ScaledJob` trigger metadata injects, do exactly that one job, exit). Not a rewrite of the ffmpeg/upload logic itself, just how it's invoked.
  - **VOD**: `ScaledJob` triggers off `upload.events` consumer-group lag (that topic already carries exactly one event type, `uploaded` — no change needed there). `maxReplicaCount` acts as a natural concurrency cap; excess uploads simply queue in Kafka, which is fine since nobody's watching a VOD transcode in real time.
  - **Live**: needs one small new piece of plumbing. `stream.lifecycle` carries *both* `started` and `ended` events, but KEDA's Kafka scaler triggers off raw consumer-group lag against a topic — it can't filter by message content, so pointing it at `stream.lifecycle` directly would spawn a Job for `ended` events too. Fix: `ingest-webhook`'s `on_publish` also emits to a new topic, `stream.start.requests`, carrying only `started` events; the `ScaledJob` watches *that*. Each spawned pod still consumes `stream.lifecycle` itself (unchanged from earlier planning), filtering for its own `stream_id`'s `ended` event to SIGTERM ffmpeg and exit — Job completion *is* the teardown, nothing external has to track or kill it.
  - **Why live can't just queue past capacity like VOD does**: if `ScaledJob`'s `maxReplicaCount` is hit, KEDA doesn't reject the excess `started` events, it leaves them lagging until a slot frees up — for live that means OBS gets accepted and starts pushing bytes into MediaMTX with *nothing pulling them yet*, which looks exactly like this session's "connected but stuck" symptom, not a clean failure. So live needs a synchronous admission check *before* MediaMTX ever accepts the publish: `ingest-webhook`'s existing `/hooks/auth` gate adds a concurrent-live-stream count check (`SELECT COUNT(*) FROM streams WHERE status='live'`) against a configured ceiling and rejects (401) over it — the streamer sees an immediate, unambiguous connection failure instead of a silent hang. `ScaledJob`'s own `maxReplicaCount` stays set to the same ceiling as a backstop for the race window, not the primary control.
  - **The ceiling itself, and the "proactive" framing**: empirically measured this session, one live transcode (3-rendition ABR, `veryfast`) costs **~2.3 cores / ~1GB RAM**. Against the kubeadm box's 12 cores minus baseline-service overhead (Kafka's bursty log-segment CPU included), that's roughly **3-4 concurrent live streams** before the single node oversubscribes. KEDA itself already reacts to a new `started` event in near-real-time (sub-second-to-few-second polling) — the trigger layer isn't what's slow. What can't happen on a *single-node* Phase 2 cluster is manufacturing more CPU cores ahead of demand — there's no second node to burst onto. So "autocreate before the ceiling, not after" concretely means: **reject early and clearly at admission time**, watched live in the Grafana dashboard (Observability, above) as concurrent-Job count approaches the configured cap. Genuine elastic capacity — a node autoscaler (Karpenter/Cluster Autoscaler) provisioning new nodes *ahead of* the existing ones filling up, with KEDA's Job-creation logic unchanged underneath — is a Phase 3 (AWS) capability, not something a single physical box can do. Revisit the fixed ceiling number then; it likely becomes a dynamic function of current node-pool size instead of a constant.
  - This pod is a single **writer** only (ingest + transcode) — it has no relationship to viewer count. Viewers read finished HLS segments straight from MinIO/S3 (or a CDN later) over plain HTTP, a completely separate and independently-scaling path; one viewer or ten thousand is identical load on the transcode pod, since it never serves them directly.
- **Networking / load balancing (Phase 2)**: two different traffic shapes need two different mechanisms. HTTP services (`web`, `upload-api`, `ingest-webhook`) sit behind a single Ingress controller doing L7 routing (host/path rules, TLS termination) across each service's replicas, replacing today's separate host ports. RTMP (MediaMTX, port 1935) isn't HTTP, so it needs L4 TCP load balancing instead: a `LoadBalancer`/`NodePort` Service, not an Ingress. Per-stream transcode pods need no inbound load balancing at all since they only pull from MediaMTX and push to S3, never receiving inbound traffic themselves. (There's no separate dispatcher service to reason about HA/leader-election for — KEDA's own controller creates the Jobs directly off Kafka lag, see the transcode-scaling decision above.)

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

- Kafka topic partition counts (currently 3 per topic as a placeholder — need real headroom numbers now that KEDA `ScaledJob` concurrency depends on it, see settled decisions above)
- Exactly-once/idempotent transcode-job dedup mechanism
- The live-stream concurrency ceiling's exact configured value, and where it lives (env var / ConfigMap) — 3-4 is the current empirical estimate, not yet a chosen production constant
- CI/CD — not addressed at all yet, revisit once the core pipeline works

## Out of scope for now

Chat, multi-tenant accounts beyond stream keys, DASH (HLS only), transcoding GPU acceleration, CDN.

## Where it currently runs

Docker Compose stack, deployed on a remote Ubuntu box (Tailscale hostname `aayush-internserver.hs.aayushpathak.com`, reachable at `100.64.0.9`), not on any contributor's laptop. **As of 2026-07-23 the whole stack is stopped** (`docker compose down`, named volumes `postgres-data`/`minio-data`/`kafka-data` preserved) — deliberately, to free the box for the CI/CD and kubeadm work below rather than running two things at once. Schema changes to Postgres are applied by hand via `docker compose exec postgres psql` (`postgres/init.sql` only auto-runs on a fresh volume). **As of 2026-07-28 a Kubernetes cluster is live on that box** alongside the (stopped) Compose stack — see "Phase 2 cluster" below.

**CI is live** (`.github/workflows/ci.yml`, first green run 2026-07-23): `ruff` lint, `hadolint` on all 5 Dockerfiles, `pytest` for the 3 services with a `test_main.py`, then a `dorny/paths-filter`-driven matrix that builds and pushes only the changed services to `ghcr.io/linkify-solutions-inc/kubernetes-streaming-<service>` tagged `sha-<short>` + `latest` (push-to-`ghcr.io` only happens on push to `master`; PRs still build to validate the Dockerfile).

**CD is half-done**: `docker-compose.yml`'s 5 app services now read `image: ghcr.io/.../kubernetes-streaming-<service>:${IMAGE_TAG:-latest}` instead of `build: ./services/<service>` — the box no longer needs to rebuild-from-source at all, it just runs the exact artifact CI built. `ci.yml` has a `deploy` job that runs on a self-hosted runner (`runs-on: [self-hosted]`), symlinks in the box's persistent `.env` (a fresh `actions/checkout` per run has no gitignored secrets), sets `IMAGE_TAG=sha-<short>` for that commit, and runs `docker compose pull/up -d` scoped to only the services `paths-filter` flagged as changed. The self-hosted runner **is now installed** (dedicated `gha-runner` user, `docker` group only, systemd unit `actions.runner.Linkify-Solutions-Inc-kubernetes-streaming.kubernetes-server.service`, active as of 2026-07-28) — this was hand-walked rather than automated, per the user's request to learn the server-side/systemd pieces directly.

Old deploy flow (superseded once the runner is up): sync files with `tar czf - <paths> | ssh kubernetes-server "cd ~/streaming-project && tar xzf -"`, then `docker compose up -d --build <service>` on the box.

### Phase 2 cluster (built 2026-07-28)

Single-node kubeadm control plane on the same box, manifests under `k8s/cluster/`. No workloads on it yet — Phase 2's checklist below starts from here.

| Piece | Choice | Why |
|---|---|---|
| Kubernetes | **v1.35.7** (pinned, `apt-mark hold`) | v1.36 is latest stable, but v1.35 sits inside Calico v3.32's tested set (1.34–1.36) with a minor of slack for MetalLB/KEDA/Strimzi. |
| Runtime | existing **containerd 2.2.5** from the Docker install | Docker ships it with `disabled_plugins = ["cri"]`; enabling CRI reuses one runtime instead of running a second alongside Docker. `SystemdCgroup = true`. Original config kept at `/etc/containerd/config.toml.docker-orig`. |
| CNI | **Calico v3.32.1** via Tigera operator | NetworkPolicy support (Flannel has none) and the closest match to what Phase 3's EKS will look like. Note: `operator-crds.yaml` must be applied *before* `tigera-operator.yaml`'s CRs in 3.3x — they're separate manifests now. |
| Pod CIDR | `192.168.0.0/16` | Deliberately not `10.x` or `172.16-31.x`: the box's LAN is `10.50.0.0/24` and Docker holds `172.17–172.20.0.0/16`. |
| L4 / LoadBalancer | **MetalLB v0.16.0**, L2 mode, pool `10.50.0.240-254` on `ens18` | Makes `type: LoadBalancer` real on bare metal, so MediaMTX's RTMP:1935 Service and the manifests around it carry over to EKS unchanged. A full `/24` ping sweep confirmed only `.1` (gateway) and `.2` (the box) are in use, so the top `/28` is free. |
| L7 / Ingress | **ingress-nginx v1.15.1**, `cloud` variant | The `cloud` variant asks for a `type: LoadBalancer` Service, which is exactly what MetalLB is there to satisfy; the `baremetal` variant's NodePort would bypass it. Holds `10.50.0.240`. |

**Reachability caveat, and it's the load-bearing one.** MetalLB IPs live on `10.50.0.0/24`, but every human and every OBS client reaches this box over Tailscale (`100.64.0.9/32`), which by default advertises no subnet routes — so LoadBalancer IPs are invisible off-box. Fixed by making the box a Tailscale subnet router for just the pool: `sudo tailscale set --advertise-routes=10.50.0.240/28`. **This route must then be approved in the Tailscale admin console**; until it is, `10.50.0.240` answers only from the box itself. (The route works despite MetalLB being L2/ARP-based: kube-proxy's iptables rules match on destination IP in `PREROUTING` regardless of ingress interface, so subnet-routed packets get DNAT'd without any ARP resolution involved.)

The API server advertises on `10.50.0.2` but carries `100.64.0.9` and `aayush-internserver.hs.aayushpathak.com` as cert SANs, so `kubectl` works over Tailscale from a laptop with no tunnel or proxy — context `streaming-k8s`.

**RTMP URL config (done)**: the `web` service reads `RTMP_PUBLIC_URL` (added alongside `MINIO_PUBLIC_URL`'s existing pattern, to fix the streamer-facing RTMP URL that was previously hardcoded to `localhost`, which only ever resolves to whatever machine OBS itself is running on). `.env` on the box (untracked/gitignored) has this set:
```
RTMP_PUBLIC_URL=rtmp://aayush-internserver.hs.aayushpathak.com:1935
```
Note for anyone setting up OBS: `services/web/main.py`'s `/streamers/new` handler builds the full publish URL as `{RTMP_PUBLIC_URL}/{stream_key}` — the stream key is baked into the URL path, not a separate credential. That whole string goes in OBS's **Server** field; the **Stream Key** field must be left **blank**. Filling in both duplicates the key in the path (`.../<key>/<key>`), which won't match any `streamers.stream_key` row in `ingest-webhook`'s `/hooks/auth` check and gets the publish rejected — this is a known footgun, since OBS's separate Server/Stream Key fields invite pasting the key into both.

## Next steps

Roughly in order:

1. **Phase 1 hardening** (cheap, do before or alongside Phase 2) — **done, not yet deployed to the box**:
   - Idempotent transcode-job handling: `ingest-webhook`'s `/hooks/publish` now guards against a duplicate MediaMTX hook call inserting a second `streams` row / emitting a second `started` event; `transcode-worker` claims a `path` in `active_streams` synchronously before spawning a live transcode (guards a redelivered `stream.lifecycle` "started"), and claims a VOD job via an atomic `UPDATE videos ... WHERE status = 'uploaded'` (guards a redelivered `upload.events`, and survives a worker restart since the claim lives in Postgres, not memory — trade-off: a crash mid-transcode leaves a video stuck at `status='transcoding'` forever; a stale-job sweep is a separate, not-yet-needed piece of work). All three Kafka producers (`ingest-webhook`, `upload-api`, `transcode-worker`) now set `enable.idempotence: True` too, closing the smaller produce-retry-duplicate risk.
   - Kafka topic partitions sized for real: `stream.lifecycle`/`upload.events` (the two KEDA cares about) now create at 8 partitions — ~2x headroom over the empirical 3-4 concurrent-live-stream ceiling — in `kafka/init-topics.sh`; `transcode.jobs`/`transcode.status`/`viewer.analytics` stay at 3 (no KEDA/horizontal-scaling need). **Deploy note**: the script uses `--if-not-exists`, so the already-running box's topics (created at 3 partitions) won't pick this up automatically — needs either a `kafka-topics.sh --alter --partitions 8` against the live topics or a topic recreation before this takes effect there.
   - `Content-Type`/`Cache-Control` on MinIO uploads — done (see `_s3_extra_args` in `transcode-worker/main.py`, already in the working tree from this session's live debugging).
   - `upload-api`: `MAX_UPLOAD_BYTES` (default 5 GiB, env-configurable) enforced via a size-limited reader wrapping the upload stream (confirmed empirically that boto3's `upload_fileobj` propagates a read-time exception unwrapped, both below and above the multipart threshold), plus a per-streamer rate limit (`UPLOAD_RATE_LIMIT_COUNT`/`UPLOAD_RATE_LIMIT_WINDOW_MINUTES`, default 10/60min) backed by a `videos` count query rather than in-memory state, so it holds even if `upload-api` is ever scaled to multiple replicas.
2. **Phase 2: Kubernetes (dev)** — ~~install kubeadm on the Ubuntu box~~ **done 2026-07-28** (cluster + Calico + MetalLB + ingress-nginx, see "Phase 2 cluster" above). Then, roughly in dependency order:
   1. **Kafka via Strimzi** — operator + `Kafka`/`KafkaTopic` CRs, real partition counts (see JIT list above, now blocking since `ScaledJob` concurrency depends on it), KRaft mode (matching Phase 1's `apache/kafka` setup, no ZooKeeper).
   2. **Postgres/MinIO as StatefulSets** (or their upstream — not Bitnami — Helm charts).
   3. **Stateless services** (`web`, `upload-api`, `ingest-webhook`) as Deployments behind a single Ingress (L7); **MediaMTX**'s RTMP port via a plain `NodePort`/`LoadBalancer` Service (L4 TCP, validated against SRS's own reference k8s deployment in the research pass) — replacing today's separate Compose host ports.
   4. **Secrets via SOPS** — encrypt today's `.env` values into `k8s/secrets/*.enc.yaml`, decrypt at apply time. (Today's `.env` drift already caused one real outage this session — a value silently missing from the file crashed `web` on redeploy — so this isn't optional polish.)
   5. **`transcode-worker` becomes the container image run by two `KEDA ScaledJob`s** (live off a new `stream.start.requests` topic, VOD off existing `upload.events`) instead of a long-running Deployment — see the "Live + VOD transcode scaling" settled decision above for the full design, including the live-specific admission-control cap in `ingest-webhook`. Resource `requests`/`limits` sized from this session's empirical benchmark (~2.3 cores / ~1GB RAM per live job).
   6. **`kube-prometheus-stack`** — infra + Kafka-lag dashboards essentially for free once Strimzi's metrics are wired in; app-level `/metrics` endpoints on our own services are a follow-up, not blocking.
3. **CI/CD** — see "Where it currently runs" above for the current state. Can run in parallel with Phase 2, not blocked by it:
   1. **CI**: **done** — `ruff` + `hadolint`, the 3 starting tests, `paths-filter`-driven matrix build, push to `ghcr.io`.
   2. **Phase 1 CD**: **workflow + compose file done**; the one remaining piece is installing the self-hosted runner itself on the box (systemd service, restricted user, `Nice=`/`CPUWeight=` constrained) — hand-walked rather than automated, see below.
   3. **Phase 2 CD** (once Phase 2's kubeadm/ArgoCD exist): ArgoCD + KSOPS (custom `repo-server` image, `age` key as a k8s Secret) watching a manifests path; CI's job becomes build/push/bump-tag only.
4. **Phase 3: AWS (prod)** — EKS, real S3, MSK or self-hosted Kafka, RDS, NLB with TCP passthrough for RTMP (watch for the long-lived-TCP-through-managed-LB failure class documented in the research pass before assuming it "just works"). Also where a node autoscaler (Karpenter/Cluster Autoscaler) turns the Phase 2 live-stream admission ceiling into genuine proactive capacity scaling, and where External Secrets Operator becomes worth adopting (SOPS → ESO backed by AWS Secrets Manager).

See the `research_validated_architecture` conversation history / memory for the research and empirical findings backing these decisions (not reproduced here to keep this doc short).
