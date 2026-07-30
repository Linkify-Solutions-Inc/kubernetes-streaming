# Module 11 — Running the services

Prerequisites: [Module 8](08-rds-postgres.md) (RDS, schema applied), [Module 9](09-s3-and-cloudfront.md) (bucket, CloudFront), [Module 10](10-kafka-strimzi.md) (Kafka, five topics), [Module 7](07-secrets.md) (`streaming-db` syncing).

---

## What you're building

All five Python services plus MediaMTX, running in the `streaming` namespace as Deployments, reading their configuration from a ConfigMap and a Secret, with health probes that report the truth. Nothing is reachable from the internet yet — that's Module 12. At the end of this module every pod is `Running`, and a `kubectl port-forward` to `web` renders the homepage in your browser.

You also establish the manifest layout that every module from here to the end uses.

---

## Why it works this way

### Kustomize base plus overlays

Every manifest in this course lives under `k8s/`, organised as a Kustomize **base** with two **overlays**. Four reasons, in the order they'll matter to you:

**ArgoCD consumes it natively.** In [Module 15](15-argocd-gitops.md) you point an ArgoCD `Application` at a directory containing a `kustomization.yaml` and it renders it. No plugin, no `repo-server` custom image, no chart repository to host. If you used Helm for your own services you'd be teaching ArgoCD how to template before you could deploy anything.

**There is no templating language to learn.** `kubectl kustomize k8s/apps/overlays/prod` prints the exact YAML the cluster will receive. You can pipe it to a file and diff it against `kubectl get -o yaml`. When something is wrong you are reading Kubernetes objects, not chasing a value through three levels of `{{ .Values.x | default .Values.y }}`. An intern debugging a Helm chart learns Helm; an intern debugging Kustomize output learns Kubernetes.

**The dev overlay keeps the bare-metal box working.** The kubeadm box from Phase 2 runs Postgres and MinIO in-cluster behind ingress-nginx. EKS runs RDS and S3 behind an ALB. Those two environments share about 95% of their manifests. That is the textbook base-plus-overlay case, and it has a second benefit: the dev overlay is a tripwire. The moment `base/` starts assuming an ALB or an RDS hostname, the dev overlay stops making sense, and you notice.

**`configMapGenerator` gives you free rolling restarts.** It appends a hash of the content to the ConfigMap's name — `streaming-config-g96ttgh296`. Change a value, the name changes, every Deployment that references it has a new pod template, and Kubernetes rolls it. Without this you hand-roll a checksum annotation and forget to update it exactly once, then spend an afternoon wondering why your config change did nothing.

The honest cost: Kustomize has no conditionals, so five near-identical Deployments are five near-identical files. Accept it. Patch files are the price, and they're greppable, which a Helm `if` is not.

Helm is still used in this course — but only for other people's software (Strimzi, KEDA, the load balancer controller, ArgoCD), wrapped in ArgoCD `Application`s. Never for our five services.

### The tree

```
k8s/
├── cluster/                       # UNCHANGED — kubeadm bring-up for the bare-metal box
├── infra/                         # custom resources we own (StorageClass, Kafka, secrets)
└── apps/
    ├── base/                      # the truth
    │   ├── kustomization.yaml
    │   ├── namespace.yaml
    │   ├── config.env             # non-secret config -> configMapGenerator
    │   ├── web/{deployment,service}.yaml
    │   ├── upload-api/{serviceaccount,deployment,service}.yaml
    │   ├── ingest-webhook/{deployment,service,networkpolicy}.yaml
    │   ├── analytics-worker/deployment.yaml
    │   └── transcode-worker/{serviceaccount,deployment}.yaml
    ├── mediamtx/                  # its own root — NOT part of apps/base
    │   ├── base/                  # cloud-neutral; dev uses this directly
    │   │   ├── kustomization.yaml
    │   │   └── {deployment,service,pdb}.yaml + mediamtx.yml
    │   └── overlays/aws/          # EKS only
    │       ├── kustomization.yaml
    │       ├── service-rtmp-nlb.yaml      # Module 12
    │       └── patches/mediamtx-placement.yaml
    └── overlays/
        ├── dev/                   # kubeadm box: in-cluster Postgres + MinIO
        │   ├── kustomization.yaml #     ../../base + ../../mediamtx/base
        │   ├── config.env
        │   └── patches/{web,upload-api,ingest-webhook,transcode-worker}.yaml
        └── prod/                  # EKS
            ├── kustomization.yaml # <-- CI writes the image tags HERE (Module 15)
            ├── config.env
            └── ingress-alb.yaml       # Module 12
```

One line each, for the README you'll write later:

- `apps/base` — our five services. This is the truth.
- `apps/mediamtx` — the RTMP server, on its own. It is a separate kustomize root because [Module 15](15-argocd-gitops.md) gives it a separate, manually-synced ArgoCD Application: restarting MediaMTX ends every live broadcast, so it must not ride along with a routine deploy of the five services. Two Applications cannot own the same objects, so being outside `apps/base/` is what makes that gate real. It splits base/overlay like everything else — `mediamtx/base` is cloud-neutral and is what the dev overlay pulls in, `mediamtx/overlays/aws` adds the public RTMP NLB Service (which has to be owned by the same Application as the pods it targets) and the node placement.
- `apps/overlays` — the deltas. If it contains an ARN or an account id, it belongs here and not in base.
- `infra/` — custom resources we author that need an operator's CRDs.
- `cluster/` — the old kubeadm box. Do not edit it in this course.

### Probes done properly

This is the part of the module that is actually about Kubernetes rather than about YAML.

A **liveness** probe answers exactly one question: *is this process wedged such that only a restart can fix it?* A **readiness** probe answers a different one: *should this pod receive traffic right now?* Conflating them is the single most common way to turn a brief dependency outage into a long self-inflicted one.

Look at what `upload-api` ships today (`services/upload-api/main.py`):

```python
@app.get("/health")
def health():
    checks = {}
    try:
        with pg_connect() as conn:
            conn.execute("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {exc}"
    try:
        s3_client().head_bucket(Bucket="media")
        checks["minio"] = "ok"
    except Exception as exc:
        checks["minio"] = f"error: {exc}"
    return checks
```

Two independent problems.

**It is wrong as a liveness probe.** It does a real `SELECT 1` against Postgres and a real `head_bucket` against S3. Wire that to liveness and an RDS hiccup — a failover, a maintenance window, a connection storm — fails the probe on *every* `upload-api` replica at once. The kubelet restarts them all. They come up, fail again, and enter `CrashLoopBackOff`, where the backoff grows to five minutes. Your outage now outlives the RDS blip by minutes, and the restart storm hammers RDS while it is trying to recover. A dead database is not a wedged process.

**It also returns 200 unconditionally, which is a live bug.** It builds `checks = {"postgres": "error: ...", "minio": "error: ..."}` and returns the dict. FastAPI serialises that with status 200. So it can never fail. Wire it to readiness and you will watch it stay Ready straight through a total database outage and conclude that probes don't work.

The fix is a small code change, and here it is exactly. Three endpoints instead of one:

| Endpoint | Body | Status | Used by |
|---|---|---|---|
| `GET /livez` | `{"status":"ok"}` | always 200 | liveness + startup |
| `GET /readyz` | per-dependency checks | **503 if any fail** | readiness |
| `GET /health` | unchanged (verbose, always 200) | 200 | humans, `curl` from a shell |

```python
@app.get("/livez")
def livez():
    return {"status": "ok"}


@app.get("/readyz")
def readyz(response: Response):
    checks, ok = {}, True
    try:
        with pg_connect() as conn:
            conn.execute("SELECT 1")
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["postgres"], ok = f"error: {exc}", False
    try:
        s3_client().head_bucket(Bucket=S3_BUCKET)
        checks["s3"] = "ok"
    except Exception as exc:  # noqa: BLE001
        checks["s3"], ok = f"error: {exc}", False
    if not ok:
        response.status_code = 503
    return checks
```

`Response` comes from `fastapi`. This is the change [Module 0](00-preflight-code-changes.md) already landed — if you skipped it, land it now, because the manifests below reference both paths.

Two things to know about `/readyz` before you set the period:

- It opens a **new Postgres connection every 10 seconds per pod**. The code has no pooling; `pg_connect()` builds a fresh connection per call. At two replicas that's twelve TLS handshakes a minute against RDS, forever. Acceptable at this scale, and the strongest single argument for adding `psycopg_pool.ConnectionPool` later.
- `head_bucket` is a real S3 request. The cost is negligible; the latency is not always. `timeoutSeconds: 5` keeps a slow S3 from holding the probe open past its own period.

`web` and `ingest-webhook` already have a trivial `/health` that returns `{"status":"ok"}`. Module 0 adds `/livez` as an alias and leaves `/health` in place. Both use `/livez` for all three probes.

`ingest-webhook`'s readiness is trivial **on purpose**, and this deserves saying out loud because it looks like an oversight: it has database dependencies and does not check them. MediaMTX's `authHTTPAddress` is a hard gate. If zero `ingest-webhook` pods are Ready, nobody on the platform can go live at all. A DB-dependent readiness check turns a thirty-second RDS blip into a total ingest outage, when the honest signal — a 500 from the handler on the one request that hit the bad moment — would have been enough.

`analytics-worker` has no HTTP server at all today. Module 0 adds a twelve-line health thread, because a Kafka consumer that quietly stops polling — rebalance storm, broker gone, an exception swallowed in a loop — is a real failure mode that a probe-less Deployment will never notice:

```python
LAST_POLL = time.time()


class _H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if time.time() - LAST_POLL < 60 else 503)
        self.end_headers()

    def log_message(self, *a):
        pass


threading.Thread(
    target=lambda: http.server.HTTPServer(("", 8000), _H).serve_forever(),
    daemon=True,
).start()
```

Set `LAST_POLL = time.time()` on each loop iteration and point liveness at it.

`transcode-worker` gets no probes in this module. It has no HTTP surface, and an exec probe against a busy encoder buys nothing. Module 14 makes the question moot: a Job either completes or it doesn't.

### Config wiring, and why `envFrom` is banned here

Every service reads its configuration as `os.environ["KEY"]` at import time. A missing key is a `KeyError` traceback inside `CrashLoopBackOff`, discovered several minutes after a deploy, naming exactly one of possibly several missing variables.

Three layers fix this, cheapest first.

**Never use bare `envFrom`.** `envFrom: {secretRef: {name: streaming-db}}` projects whatever keys happen to be in the Secret and silently omits the ones that aren't. The container starts, the app dies at import. Enumerate every variable explicitly instead:

```yaml
env:
  - name: POSTGRES_DSN
    valueFrom:
      secretKeyRef:
        name: streaming-db
        key: POSTGRES_DSN        # optional defaults to false
  - name: KAFKA_BOOTSTRAP_SERVERS
    valueFrom:
      configMapKeyRef:
        name: streaming-config
        key: KAFKA_BOOTSTRAP_SERVERS
```

With `optional` at its default of `false`, a missing key means the kubelet **refuses to create the container**. The pod sits in `CreateContainerConfigError` with the message `couldn't find key POSTGRES_DSN in Secret streaming/streaming-db`. That is a named, greppable failure carrying the exact key — and critically, it is not a crash-loop, so the old ReplicaSet keeps serving. It is verbose. It is correct. Write it out.

**Fail at sync time.** The `ExternalSecret` from Module 7 lists each key with an explicit `remoteRef`. If a key is absent in Secrets Manager, the `ExternalSecret` goes `SecretSyncError`, the target Secret is never created, and (once Module 15 lands) ArgoCD blocks the app rollout entirely because secrets are an earlier sync wave. You see it in the UI before a single pod is touched.

**Fail in the process, all at once.** Module 0 added a `config.py` next to each service:

```python
_MISSING: list[str] = []


def require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        _MISSING.append(name)
        return ""
    return value


def seal() -> None:
    if _MISSING:
        raise SystemExit("missing required config: " + ", ".join(sorted(_MISSING)))
```

Every module-level `os.environ["X"]` becomes `require("X")`, and `seal()` runs once after the last one. The difference is concrete: instead of

```
KeyError: 'POSTGRES_DSN'
```

and a redeploy, then

```
KeyError: 'S3_BUCKET'
```

and another redeploy, you get one line:

```
missing required config: POSTGRES_DSN, S3_BUCKET
```

Three deploys saved per incident, for nine lines of code.

**The split itself.** Non-secret, environment-shaped values go in `base/config.env` and are overridden per-overlay. Exactly one value is secret: `POSTGRES_DSN`, which carries the RDS password. There are **no S3 credentials anywhere** — Module 7 attached IAM roles to the `upload-api` and `transcode-worker` ServiceAccounts via EKS Pod Identity, and Module 0 made `S3_ENDPOINT`/`S3_ACCESS_KEY`/`S3_SECRET_KEY` optional so boto3 falls through to the credential chain when they're unset. Setting a static key on EKS would *win* over that chain and silently disable the role you configured.

### MediaMTX

`mediamtx/mediamtx.yml` is a bind mount under Docker Compose. In Kubernetes it becomes a ConfigMap, generated from `k8s/apps/mediamtx/base/mediamtx.yml` and mounted with `subPath: mediamtx.yml`. Note the path: MediaMTX is not under `base/`. It is its own kustomize root, for the reason above, and the generator that builds this ConfigMap lives in `k8s/apps/mediamtx/base/kustomization.yaml` — the hashed name is what restarts the pod, so it has to be produced by the Application that is allowed to restart it.

Four things about that file.

**`writeQueueSize: 65536` is do-not-touch.** It looks like an arbitrary large number somebody pasted in. It is the fix for a reproduced production stall: at the default of 512 outgoing packets, MediaMTX logged `reader is too slow, discarding N frames` and then `closed: i/o timeout` against `transcode-worker`'s ffmpeg client, on a box with CPU and network to spare (ffmpeg at ~230% of twelve available cores during the stall). A brief scheduling or IO hiccup on the reader side overflows a 512-packet queue. SPEC.md documents the whole investigation. On EKS this matters *more* than it did on the single box, because ffmpeg and MediaMTX now sit on different nodes with a real network between them. The manifest carries a comment saying so; leave it there.

**The hook URLs already work — write the FQDN anyway.** The Compose file points at `http://ingest-webhook:8000/hooks/auth`. That short name happens to also be valid Kubernetes DNS: a same-namespace Service resolves through the pod's `search streaming.svc.cluster.local svc.cluster.local cluster.local` list. So the file would work unchanged, provided the Service is named exactly `ingest-webhook` and exposes port `8000`. Write `http://ingest-webhook.streaming.svc.cluster.local:8000/hooks/auth` regardless, for two reasons: the short name working is a coincidence rather than a design and breaks the moment anyone moves MediaMTX to another namespace, and with the default `ndots:5` every short name costs four failed DNS lookups against CoreDNS before the right suffix is tried.

**Enable the API.** Add `api: yes` and `apiAddress: :9997`. This gives you a health check target that returns 200 only when MediaMTX is fully up, which Module 12's NLB needs — a bare TCP connect on 1935 passes the instant the socket is listening, before MediaMTX has read its config, and keeps passing when MediaMTX is wedged. It also gives you `kubectl exec deploy/mediamtx -- wget -qO- localhost:9997/v3/paths/list`, which is the fastest way to answer "is this stream actually arriving" in Module 13.

**Never `kubectl edit` the generated ConfigMap.** MediaMTX watches its own config file and reloads on change. The kubelet updates a projected ConfigMap within about a minute. Put those together and an in-place edit restarts the RTMP listener and drops every live stream — with no rollout, no `kubectl rollout status` to watch, and no event to explain it. Change the file in git, let the generator's name hash roll the pod, and do it in a quiet window.

### Why a MediaMTX rolling update drops every viewer

`replicas: 1` on the MediaMTX Deployment is a **correctness constraint, not a cost decision**, and "scale it up for HA" is the obvious wrong instinct.

MediaMTX has no clustering. A stream published to pod A exists only in pod A's memory. `transcode-worker` dials the `mediamtx` ClusterIP Service, which round-robins across endpoints — with two replicas, roughly half of all transcodes connect to the replica that has never heard of that path and get an empty stream or an immediate exit. Meanwhile the NLB is independently round-robining OBS clients across both. The failure presents as "ffmpeg exited immediately", not as a load balancing bug.

Now the rollout question. RTMP is one long-lived TCP connection per publisher, opened when OBS hits Start Streaming and held for the whole broadcast. There is no reconnect protocol in MediaMTX's RTMP server. When the pod goes away, OBS's own auto-reconnect kicks in, drops several seconds of video, and restarts the path — which fires `runOnNotReady`, then `runOnReady`. Follow that through your own code:

1. `runOnNotReady` → `ingest-webhook` closes the `streams` row and emits `ended`.
2. `transcode-worker` sees `ended`, stops the ffmpeg for that stream.
3. `runOnReady` → a **new** `streams` row with a **new** `stream_id`, and a fresh `started` event.
4. A new transcode starts, writing to `hls/live/<new-stream-id>/`.
5. Every viewer's player is still polling `hls/live/<old-stream-id>/master.m3u8`, which stops updating and then 404s as the lifecycle rule cleans it up.

One pod restart cascades into "everyone watching gets a dead player, and a duplicate stream appears in the list."

The grace period does not save you: MediaMTX exits promptly on SIGTERM, it does not drain. `terminationGracePeriodSeconds: 1800` is there to stop the kubelet SIGKILLing during the flush and to make an accidental `kubectl rollout restart` obviously slow rather than quietly destructive. The real mitigations are structural, and they're in the manifests:

1. **`strategy: Recreate`.** Not because it's better — it guarantees downtime — but because `RollingUpdate` on a single replica pretends there's a safe path and there isn't. It also closes the window where two pods are briefly alive and `transcode-worker` can dial the wrong one.
2. **A PodDisruptionBudget with `minAvailable: 1`.** With one replica this blocks *every* voluntary eviction: `kubectl drain` waits, Karpenter refuses to consolidate, the node upgrade stalls. That is the intent — no automated process gets to end a live broadcast without a human deciding to. Write the cost on a sticky note for whoever does the next EKS upgrade: `kubectl drain` will print `Cannot evict pod as it would violate the pod's disruption budget` forever until someone passes `--disable-eviction` or scales MediaMTX to zero deliberately.
3. **`karpenter.sh/do-not-disrupt: "true"` plus `nodeSelector: {workload: system}`.** Karpenter nodes are disposable by design; the one pod on the platform that must not move belongs on the stable managed node group from [Module 3](03-eks-cluster.md). Without this, consolidation evicts MediaMTX to save four cents an hour.
4. **In Module 15, a separate ArgoCD `Application` with manual sync.** A MediaMTX config change shows as `OutOfSync` until a human presses Sync during a quiet window. Everything else auto-syncs.

### Resources, security context, replicas, spread

**Resource requests and limits.** Requests are what the scheduler reserves; limits are what the kubelet enforces. Every container here sets a memory limit and a memory request. Only `transcode-worker` deliberately omits a **CPU limit**, because CFS throttling a real-time encoder makes it fall behind the source, which is indistinguishable from "the stream froze" — you want it to burst.

`upload-api` also requests `ephemeral-storage`, and that is not optional. Starlette's `UploadFile` is a `SpooledTemporaryFile` that rolls over to disk past about 1 MB, so a 2 GiB upload writes 2 GiB into the pod's filesystem before boto3 streams it out. Without an `ephemeral-storage` request the scheduler cannot reason about it, the node's disk fills, and the kubelet evicts **arbitrary pods on that node** under `DiskPressure` — a failure that looks nothing like "an upload was too big". `TMPDIR=/tmp` plus an `emptyDir` with a `sizeLimit` makes the whole thing accounted for.

**Security context.** The namespace is labelled `pod-security.kubernetes.io/enforce: restricted`, so every pod must be non-root, drop all capabilities, disallow privilege escalation, and set a `RuntimeDefault` seccomp profile. `readOnlyRootFilesystem: true` is on everywhere, which means uvicorn and Jinja2 need a writable `/tmp` mounted as an `emptyDir`.

`runAsNonRoot: true` alone is not enough for these images. The `python:3.12-slim` Dockerfiles have no `USER` line, so the kubelet rejects the pod with `container has runAsNonRoot and image will run as root`. Module 0 added `USER 10001` to each Dockerfile; the manifests set `runAsUser: 10001` to match. Same story for `bluenviron/mediamtx`, which also ships no `USER` — both of its listeners are above 1024, so non-root is fine, it just has to be stated.

**Replicas and spread.** `web`, `upload-api` and `ingest-webhook` run two replicas with `topologySpreadConstraints` on both `topology.kubernetes.io/zone` and `kubernetes.io/hostname`, so a single node or a single AZ going away leaves you serving. `whenUnsatisfiable: ScheduleAnyway` rather than `DoNotSchedule` — a soft preference. `DoNotSchedule` on a two-node cluster means the second replica sits `Pending` forever the moment one node is cordoned, which trades an availability improvement for an availability *problem*.

`analytics-worker` runs one replica with `strategy: Recreate` — it's a Kafka consumer group of one and there is nothing to gain from a rebalance mid-rollout. `mediamtx` runs one for the reasons above. `transcode-worker` runs one because its live-stream dedup guard (`active_streams`) is in-process memory; a second replica would happily start a second ffmpeg for the same stream.

> **Note for later:** through Modules 11–13, `transcode-worker` is a long-running Deployment — exactly what it is under Docker Compose, one process with a `while True` Kafka consumer loop handling every stream and every upload for the container's lifetime. [Module 14](14-keda-scaledjobs.md) deletes this Deployment and replaces it with two KEDA `ScaledJob`s that create one Job per stream and per upload. Everything you read about `transcode-worker` in these three modules is true until then and false afterwards. The manifest carries the same warning at the top of the file.

---

## Do it

### 1. Confirm the platform is ready

```sh
kubectl get externalsecret -n streaming
kubectl get kafkatopic -n kafka
```

```
NAME           STORETYPE            STORE                REFRESH INTERVAL   STATUS         READY
streaming-db   ClusterSecretStore   aws-secretsmanager   1h                 SecretSynced   True

NAME                    CLUSTER     PARTITIONS   REPLICATION FACTOR   READY
stream.lifecycle        streaming   8            1                    True
upload.events           streaming   8            1                    True
stream.start.requests   streaming   8            1                    True
transcode.status        streaming   3            1                    True
viewer.analytics        streaming   3            1                    True
```

Those are the five from [Module 10](10-kafka-strimzi.md) — note `stream.start.requests` rather than the `transcode.jobs` that `kafka/init-topics.sh` used to create. Nothing produces to `stream.start.requests` until [Module 14](14-keda-scaledjobs.md); it exists now because KEDA's trigger needs a topic carrying `started` events and nothing else.

If `streaming-db` is not `SecretSynced`, stop and fix Module 7. Every Deployment below references `POSTGRES_DSN` out of it, and they will all sit in `CreateContainerConfigError`.

### 2. Read the manifests

They're in the repo. Read them before you apply them — the comments are half the module:

```sh
ls -R k8s/apps/base
```

Start with `k8s/apps/base/kustomization.yaml`, then `upload-api/deployment.yaml` (probes, ephemeral storage), then `mediamtx/deployment.yaml` (the replica-count comment).

### 3. Fill in the two placeholders

`k8s/apps/overlays/prod/config.env` has one:

```sh
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
sed -i.bak "s/<accountid>/${ACCOUNT_ID}/" k8s/apps/overlays/prod/config.env
rm k8s/apps/overlays/prod/config.env.bak
```

`k8s/apps/overlays/prod/ingress-alb.yaml` has the other (`<acm-cert-arn>`), but that file belongs to Module 12.

### 4. Take the Module 12 resources out, for now

`k8s/apps/overlays/prod/kustomization.yaml` in the repo is the finished article and lists one resource Module 12 explains:

```yaml
resources:
  - ../../base
  - ingress-alb.yaml         # <- comment out for this module
```

Comment it out. Uncommenting it is the first step of the next module. Applying it now provisions a public ALB before you've decided what it should do, and it bills by the hour.

The public RTMP NLB is the same story but it lives elsewhere — `k8s/apps/mediamtx/overlays/aws/service-rtmp-nlb.yaml`, with the Deployment it targets. Comment that entry out of `k8s/apps/mediamtx/overlays/aws/kustomization.yaml` for now, for the same reason.

### 5. Render both roots and read the output

```sh
kubectl kustomize k8s/apps/overlays/prod | less
kubectl kustomize k8s/apps/mediamtx/overlays/aws | less
```

Two commands, because there are two kustomize roots. This is the literal YAML the API server will receive. Check three things in the first: the namespace is `streaming` everywhere, the ConfigMap name has a hash suffix (`streaming-config-<hash>`), and every `configMapKeyRef` points at that same hashed name. In the second, check that `mediamtx-config-<hash>` is what the Deployment's volume references.

Check one thing across both: **no object appears in both outputs.** That is the invariant the split exists to hold, and Module 15 turns it into a scripted check.

### 6. Apply

```sh
kubectl apply -k k8s/apps/overlays/prod
```

```
namespace/streaming configured
serviceaccount/transcode-worker created
serviceaccount/upload-api created
configmap/streaming-config-g96ttgh296 created
service/ingest-webhook created
service/upload-api created
service/web created
deployment.apps/analytics-worker created
deployment.apps/ingest-webhook created
deployment.apps/transcode-worker created
deployment.apps/upload-api created
deployment.apps/web created
networkpolicy.networking.k8s.io/ingest-webhook-from-mediamtx created
```

Then MediaMTX, separately — and get used to that, because from Module 15 onward it is a separate ArgoCD Application you sync by hand:

```sh
kubectl apply -k k8s/apps/mediamtx/overlays/aws
```

```
configmap/mediamtx-config-bb424mt4ct created
service/mediamtx created
deployment.apps/mediamtx created
poddisruptionbudget.policy/mediamtx created
```

### 7. Restart anything that predates its Pod Identity association

A Pod Identity association only takes effect on pod start. If you created the associations in Module 7 and the pods came up before that, they're running with the node role:

```sh
kubectl rollout restart -n streaming deploy/upload-api deploy/transcode-worker
```

---

## Verify

```sh
kubectl get pods -n streaming
```

Every pod `Running`, every container ready, zero restarts:

```
NAME                                READY   STATUS    RESTARTS   AGE
analytics-worker-6c9b7d4f88-2xk4t   1/1     Running   0          92s
ingest-webhook-7d4c8b95f6-h9wqr     1/1     Running   0          92s
ingest-webhook-7d4c8b95f6-vk2ml     1/1     Running   0          92s
mediamtx-5f9b6c7d84-t4nzp           1/1     Running   0          92s
transcode-worker-84b7f9d6c5-jr8xw   1/1     Running   0          92s
upload-api-6b8f7c9d54-4kp2n         1/1     Running   0          92s
upload-api-6b8f7c9d54-9wtqz         1/1     Running   0          92s
web-7c5d9f8b64-mn6vx                1/1     Running   0          92s
web-7c5d9f8b64-x2rlk                1/1     Running   0          92s
```

Confirm the two-replica services actually spread across both AZs:

```sh
kubectl get pods -n streaming -l app=web \
  -o custom-columns=NAME:.metadata.name,NODE:.spec.nodeName
kubectl get nodes -L topology.kubernetes.io/zone
```

The two `web` pods should be on different nodes, and those nodes in different zones.

Confirm readiness is telling the truth rather than defaulting to yes:

```sh
kubectl exec -n streaming deploy/upload-api -- \
  python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8000/readyz').read())"
```

```
b'{"postgres":"ok","s3":"ok"}'
```

Confirm MediaMTX loaded the config you think it did:

```sh
kubectl exec -n streaming deploy/mediamtx -- wget -qO- http://localhost:9997/v3/config/global/get \
  | python3 -m json.tool | grep -i writequeue
```

```
    "writeQueueSize": 65536,
```

**The checkpoint.** Port-forward to `web` and open the homepage:

```sh
kubectl port-forward -n streaming svc/web 8080:8000
```

```
Forwarding from 127.0.0.1:8080 -> 8000
Forwarding from [::1]:8080 -> 8000
```

Open <http://localhost:8080>. You should get the homepage — the heading, an empty live-streams section, an empty videos section, and working links to "Get a stream key" and "Upload". Empty lists are the correct answer; nothing has streamed yet. A rendered page proves `web` started, reached `upload-api` over ClusterIP DNS, and `upload-api` reached RDS. That is three of the five services and both of the new dependencies, in one HTTP request.

Do not move on until that page renders.

---

## What breaks

Ordered by how often it actually happens.

**`CreateContainerConfigError`.** Read the message; it names the key:

```sh
kubectl describe pod -n streaming <pod> | tail -20
```

```
  Warning  Failed  12s (x4 over 45s)  kubelet
    Error: couldn't find key HLS_PUBLIC_BASE_URL in ConfigMap streaming/streaming-config-g96ttgh296
```

Either the key is missing from `overlays/prod/config.env`, or you edited `config.env` and applied a Deployment that still references the *old* hash. `kubectl apply -k` regenerates both together; a hand-edited `kubectl apply -f` on one file does not.

**`CrashLoopBackOff` with a `KeyError` or `SystemExit`.** The config reached the container but the app rejected it.

```sh
kubectl logs -n streaming <pod> --previous
```

```
missing required config: S3_BUCKET
```

That single line listing everything missing is `config.py` doing its job. If you instead see a raw `KeyError`, the Module 0 change didn't land in the image you're running — check the image tag.

**Pod rejected before it starts, with a security context message.**

```
Error: container has runAsNonRoot and image will run as root
```

The image has no `USER` and the manifest didn't set `runAsUser`. Or:

```
Error creating: pods "web-..." is forbidden: violates PodSecurity "restricted:latest":
  allowPrivilegeEscalation != false, unrestricted capabilities, seccompProfile
```

You dropped part of the `securityContext` block. The namespace label enforces the whole profile, not parts of it.

**`upload-api` Ready flaps.** `/readyz` is doing its job and something underneath is genuinely unhealthy:

```sh
kubectl exec -n streaming deploy/upload-api -- \
  python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8000/readyz').read())"
```

`postgres: error: connection timeout` → the RDS security group doesn't allow 5432 from the node security group, or `sslmode` is missing and the parameter group sets `rds.force_ssl`. `s3: error: An error occurred (403)` → the Pod Identity role is missing `s3:ListBucket` on the **bucket** ARN, which is what `head_bucket` needs. That's a classic: every real operation works and only the health check fails.

**`ImagePullBackOff`.**

```sh
kubectl describe pod -n streaming <pod> | grep -A3 Events
```

The ghcr.io package is still private (Module 15 covers the pull secret), or the tag doesn't exist because the CI build for that commit didn't finish.

**MediaMTX `CrashLoopBackOff` immediately after start.**

```sh
kubectl logs -n streaming deploy/mediamtx
```

A YAML parse error names the line. Remember the mount is `subPath: mediamtx.yml` — if you got the `subPath` wrong, MediaMTX starts with its *built-in defaults* rather than failing, which is worse: it comes up healthy, `writeQueueSize` is 512, and you find out during your first live stream. The `writeQueueSize` check in the Verify section above is there specifically to catch this.

**Pods `Pending`.**

```sh
kubectl describe pod -n streaming <pod> | tail -15
```

`0/2 nodes are available: 2 Insufficient cpu` — the two `t3.medium` nodes are full. `transcode-worker` requests a whole core and `mediamtx` requests half of one. Either scale the managed node group, or let [Module 6](06-karpenter.md)'s Karpenter provision capacity. `1 node(s) didn't match Pod's node affinity/selector` on `mediamtx` means the `workload: system` label isn't on your managed node group nodes.

**`web` renders but every list is a 500.** `kubectl logs -n streaming deploy/web` will show an httpx connection error to `upload-api`. Check the Service exists and has endpoints:

```sh
kubectl get endpointslice -n streaming -l kubernetes.io/service-name=upload-api
```

No endpoints means no `upload-api` pod is *Ready* — which sends you back to `/readyz`, which is exactly where the answer is.

---

Next: [Module 12 — Exposing it to the world](12-ingress-and-rtmp.md).
