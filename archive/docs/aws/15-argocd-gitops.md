# Module 15 — GitOps with ArgoCD

Prerequisite: [Module 14](14-keda-scaledjobs.md). Everything in the cluster is currently there because you ran `kubectl apply` at some point. This module makes that untrue.

---

## What you're building

A cluster whose state is a function of a git branch. You install ArgoCD, apply exactly one manifest by hand, and from then on every change to the platform — a new operator, a config value, a new image — is a commit. `kubectl` becomes a read-only debugging tool.

You'll also delete the `deploy` job in `.github/workflows/ci.yml` and the self-hosted runner it runs on. Right now, deploying means a GitHub Actions runner installed on the dev box runs `docker compose pull && docker compose up -d` against the Compose stack on that same box. That box is not in the picture anymore. What replaces it is: CI builds the image, CI commits the new tag into the prod overlay, ArgoCD notices the commit and syncs it. CI never touches the cluster, never holds a kubeconfig, and never needs network access to EKS.

---

## Why it works this way

### Pull, not push

The `deploy` job you have today is push-based: CI has credentials and reaches into the environment to change it. That model has three properties you stop wanting the moment the environment is an EKS cluster in a private VPC.

It needs **inbound access** — CI must be able to reach the API server, which means either a public endpoint or a runner inside the VPC. It needs **long-lived credentials** in GitHub that can do anything to your cluster. And it has **no reconciliation**: if someone `kubectl edit`s a Deployment at 2am, nothing ever notices or puts it back.

Pull-based inverts all three. A controller inside the cluster watches git and reconciles toward it. There is no inbound path, no credential outside the cluster, and drift is corrected automatically because reconciling is the only thing the controller does.

### App-of-apps, and why not just apply seven Applications

The naive version is seven `Application` manifests, applied by hand. It works, and it decays: the first time someone adds a component with `kubectl apply` "just for now", the cluster and git have diverged and nobody knows it.

App-of-apps means one `Application` whose source is a *directory of Applications*. Adding a component becomes a PR that adds a file. That's the whole point of GitOps and it's the part that erodes first if you skip it.

There's a second, more mechanical benefit. Argo has a built-in health assessment for the `Application` kind: an Application is Healthy when it is Synced **and** all of its resources are Healthy. Since sync waves apply to Applications too, wave N+1's Application does not begin syncing until wave N's Application reports Healthy. That is how you get "Strimzi's CRDs exist before the Kafka CR" without hand-sequencing anything.

The cost is one layer of indirection and eight rows in `kubectl get app` instead of seven.

### Sync waves, and what goes wrong without them

A sync wave is an integer annotation. Argo groups resources by wave, applies the lowest wave, waits for everything in it to be Healthy, then moves on.

Without waves, Argo applies everything at once and you get failures that look like bugs but are ordering problems:

- **ESO before ExternalSecrets.** Apply an `ExternalSecret` before the External Secrets Operator's CRDs exist and you get `no matches for kind "ExternalSecret"`. Retry and it eventually succeeds — but in the meantime the pods that mount `streaming-db` are in `CreateContainerConfigError` because the Secret doesn't exist, and a Deployment that has been failing to start for five minutes has already backed off its restarts.
- **Strimzi before Kafka CRs.** Same shape, worse consequence. `no matches for kind "Kafka"`, and once the operator does arrive the `Kafka` CR is applied but the `KafkaTopic`s were rejected in the same failed sync, so you have a broker with no topics and every service crash-looping on an unknown topic.
- **KEDA before ScaledJobs.** `no matches for kind "ScaledJob"`, and — this is the one that hurts — your transcode pipeline is silently absent. Nothing crash-loops. Nothing is red. Streams just never transcode, and you go looking at ffmpeg.

Now the part people miss: **sync waves alone do not solve this.** Within a single Application, Argo dry-runs *every* manifest before applying *any* of them. A `ScaledJob` sitting in the same Application as the KEDA Helm chart fails the whole sync at dry-run, before wave ordering ever runs. There are two fixes, and you want both:

1. **Put the operators in a separate, earlier Application.** That's what `00-platform.yaml` is. Application-level separation works because Argo evaluates each Application independently, and wave 10 doesn't start until wave 0 is Healthy.
2. **Annotate any CR whose CRD might not exist yet:**
   ```yaml
   argocd.argoproj.io/sync-options: SkipDryRunOnMissingResource=true
   ```
   This is the escape hatch for the case where a CRD and its CR genuinely have to share an Application.

The wave layout:

| Wave | Application | Contents | Waits for |
|---|---|---|---|
| 0 | `platform` | ESO, Strimzi operator, KEDA, AWS Load Balancer Controller, Karpenter, kube-prometheus-stack — each a Helm chart | — |
| 10 | `secrets` | `ClusterSecretStore`, `ExternalSecret`s | ESO CRDs |
| 10 | `karpenter-pools` | `EC2NodeClass`, `NodePool`s, warm-pool placeholder | Karpenter CRDs |
| 20 | `kafka` | Namespace, `KafkaNodePool`, `Kafka`, `KafkaTopic`s, NetworkPolicy, consumer-group bootstrap Job | Strimzi CRDs, gp3 StorageClass |
| 25 | `db-migrate` | The schema Job and its generated SQL ConfigMap | `streaming-db` exists |
| 30 | `streaming` | The five services, ScaledJobs, sweeper, Ingress | Schema applied, Kafka Ready, KEDA CRDs |
| 40 | `mediamtx` | `k8s/apps/mediamtx/overlays/aws`: Deployment, ClusterIP Service, `mediamtx.yml` ConfigMap, PDB, and the public RTMP NLB Service — **manual sync** | `streaming` |

**The waves go up in tens, and the annotation matches the filename.** `20-kafka.yaml` carries `sync-wave: "20"`. Argo only cares that the numbers sort; the gaps are so you can insert a stage between two existing ones without renumbering everything downstream — which is exactly what wave 25 is. Both wave-10 Applications run concurrently; they don't depend on each other.

**Why the migration is its own Application rather than a `PreSync` hook.** The obvious design is a hook inside `streaming`, and it is wrong here for a mechanical reason. `k8s/apps/base/db-migrate/` builds its ConfigMap from `postgres/migrations/*.sql` — outside its own kustomization root, so that the schema has one copy shared with the Compose path. Kustomize needs `--load-restrictor LoadRestrictionsNone` for that ([Module 8](08-rds-postgres.md), "What breaks" #1). Put that directory inside `apps/base` or `overlays/prod` and the requirement spreads to those roots, so every plain `kubectl apply -k` in Modules 8 and 11 starts failing with `security; file ... is not in or below ...`. A separate Application keeps the relaxed restrictor confined to the one directory that needs it — ArgoCD's `kustomize.buildOptions` is global, so the repo-server can build it either way, but nothing else has to live with the consequence. The wave does what the hook would have done, and the Job stays a normal tracked resource you can `kubectl logs` afterwards instead of one that is deleted and recreated on every sync.

Inside the `streaming` Application there's a second, resource-level ordering: Deployments and Services are wave 0; ScaledJobs and the sweeper are wave 1 (so KEDA doesn't spawn a transcode pod before mediamtx and RDS are reachable); the Ingress is wave 2.

**Why MediaMTX is manual.** Rolling the mediamtx pod tears down every in-flight RTMP connection on the platform. Every live stream drops, and OBS does not silently reconnect. Automated sync means a config typo committed at 4pm ends everyone's stream at 4pm. Argo still shows it OutOfSync so you always know a change is waiting; you choose when.

### ESO and the "Argo sees drift and reverts it" problem

This problem is entirely self-inflicted and entirely avoidable. Two controllers both believe they own the contents of a `Secret`: ESO syncs it from AWS Secrets Manager, Argo reconciles it toward git. If both actually manage it, they fight — Argo writes git's version, ESO writes AWS's version, Argo calls that drift and writes git's version again. With `selfHeal: true` this is a loop that flaps the Secret and restarts every pod mounting it.

The fix is one rule: **commit the `ExternalSecret`, never commit a `Secret`.** Not even an empty placeholder, not even with a comment saying it gets overwritten. The instant a `Secret` is in git, Argo tracks it and the fight starts.

With that rule and `creationPolicy: Owner`, the mechanics work out cleanly:

- ESO creates the Secret with an `ownerReference` pointing at the `ExternalSecret`, and **without** Argo's tracking label (`app.kubernetes.io/instance`).
- Argo therefore sees an object that is neither part of desired state nor tracked by it. Not drift. And crucially **not pruned** by `automated.prune: true` — untracked resources are left alone.
- Deleting the `ExternalSecret` cascades to the Secret through the owner reference, so cleanup still works.

One residual annoyance: the `ExternalSecret`'s own `.status` block is rewritten on every refresh, which Argo can read as drift on the `ExternalSecret` itself. That's what the `ignoreDifferences` block in `k8s/argocd/apps/10-secrets.yaml` handles.

And one behaviour to know rather than be surprised by: `refreshInterval: 1h` means a rotated secret reaches the cluster within an hour but **does not restart pods**. Env vars are read once at container start. If you rotate `POSTGRES_DSN`, you must also `kubectl rollout restart` the Deployments. Accept the manual roll for now, or add Reloader later — just don't assume rotation is end-to-end automatic.

### Image tags: CI writes them into kustomize, not Argo Image Updater

Argo Image Updater is the tool people reach for. Don't. The reasoning:

- **Git should be the deployment audit log.** `git log -p k8s/apps/overlays/prod/kustomization.yaml` is a complete, timestamped, attributed history of every version that was ever deployed, and rollback is `git revert`. Image Updater's default write-back mode stores the running version in a cluster annotation, so git stops describing the cluster — which is the one property you installed ArgoCD to get.
- **Image Updater's git write-back mode does exactly what CI would do,** but from inside the cluster, needing a git write credential *and* a registry credential in the cluster. Same work, two more secrets, one more thing that can be down.
- **CI already has the information.** It computes `sha-<short>` and already knows which services changed via `paths-filter`. The bump is three lines of shell.
- **Semver/regex tracking is for upstream releases.** That's not what this is. This is "deploy the artifact I just built", and the tag is already known at build time.

The one thing you give up is auto-deploying an image built somewhere other than CI. You don't want that.

---

## Do it

### 1. Install ArgoCD

ArgoCD is the one thing installed by hand, because it can't bootstrap itself.

```sh
helm repo add argo https://argoproj.github.io/argo-helm
helm repo update
helm upgrade --install argocd argo/argo-cd \
  --namespace argocd --create-namespace \
  --version 8.5.8 \
  --set configs.params."server\.insecure"=true
```

`server.insecure=true` because the ALB from [Module 12](12-ingress-and-rtmp.md) terminates TLS. Without it, the ALB speaks HTTP to a pod expecting HTTPS and you get a redirect loop that looks like a broken ingress.

**Then one setting, before you point Argo at the repo.** The repo-server runs `kustomize build` itself, with the default load restrictor, and exactly one directory in this repo needs it relaxed: `k8s/apps/base/db-migrate/` reads the SQL from `postgres/migrations/`, outside its own kustomization root, which [Module 8](08-rds-postgres.md) covers under "What breaks". Every other root builds fine either way. Skip this and the wave-25 `db-migrate` Application never syncs — it reports a repo error rather than anything mentioning SQL, wave 30 never starts because Argo will not advance past an unhealthy wave, and the symptom you actually see is "the whole platform stopped deploying":

```sh
kubectl -n argocd patch cm argocd-cm --type merge \
  -p '{"data":{"kustomize.buildOptions":"--load-restrictor LoadRestrictionsNone"}}'

# The repo-server reads this at build time, but restart it so you are not
# debugging a stale cache later.
kubectl -n argocd rollout restart deploy/argocd-repo-server
kubectl -n argocd rollout status  deploy/argocd-repo-server
```

It is a global setting, which is the honest cost: every kustomization Argo builds may now read outside its root, not just the one that needs to. The alternative is to give up on `postgres/migrations/` being the single copy shared with the Compose path and keep a duplicate under `k8s/`. One relaxed flag beats two copies of the schema that will drift.

```sh
kubectl get pods -n argocd
```

```
NAME                                               READY   STATUS    RESTARTS   AGE
argocd-application-controller-0                    1/1     Running   0          92s
argocd-applicationset-controller-6b7c4d9f8-x2mrt   1/1     Running   0          92s
argocd-dex-server-7f8b95c6d-9nq4l                  1/1     Running   0          92s
argocd-notifications-controller-5d8f7b64c-tk8vz    1/1     Running   0          92s
argocd-redis-6c9b5d8f74-hs2wl                      1/1     Running   0          92s
argocd-repo-server-79d6c854b-4pxjn                 1/1     Running   0          92s
argocd-server-5b8f9c6d4-qw7rz                      1/1     Running   0          92s
```

Get the admin password:

```sh
kubectl -n argocd get secret argocd-initial-admin-secret \
  -o jsonpath='{.data.password}' | base64 -d; echo
```

Reach the UI. Port-forward is fine for now:

```sh
kubectl port-forward -n argocd svc/argocd-server 8080:80
# http://localhost:8080  — user: admin
```

Exposing it through the ALB is a Module 12 concern — a path rule for `/argocd` on the existing Ingress. Do that once you're tired of port-forwarding; there's nothing new in it.

### 2. Apply the root Application — the last `kubectl apply`

```sh
kubectl apply -f k8s/bootstrap/root-app.yaml
```

That's it. That is the only manifest a human applies from here on.

```sh
kubectl get app -n argocd
```

```
NAME              SYNC STATUS   HEALTH STATUS
root              Synced        Healthy
platform          Synced        Healthy
secrets           Synced        Healthy
karpenter-pools   Synced        Healthy
kafka             Synced        Healthy
db-migrate        Synced        Healthy
streaming         Synced        Healthy
mediamtx          OutOfSync     Healthy
```

`mediamtx` sitting OutOfSync is correct and expected — it's the manual-sync Application. Everything else should converge within a few minutes as the waves progress.

**Argo adopts what's already there.** Every one of these components is already installed from earlier modules. Argo doesn't reinstall or duplicate them; it compares the live objects against git and, if they match, reports Synced. If they *don't* match, it overwrites the live object with git's version — which is the point, and also why you want to read `kubectl get app -n argocd` output carefully the first time rather than walking away.

### 3. Replace the `deploy` job in CI

Delete the entire `deploy` job from `.github/workflows/ci.yml`. It looks like this today:

```yaml
  deploy:
    needs: [changes, build]
    if: needs.changes.outputs.services != '[]' && ...
    runs-on: [self-hosted]
    steps:
      - uses: actions/checkout@v4
      - name: Deploy changed services via docker compose
        run: |
          ln -sf /home/gha-runner/streaming-project.env .env
          export IMAGE_TAG=sha-$(git rev-parse --short=7 HEAD)
          docker compose pull $SERVICE_LIST
          docker compose up -d $SERVICE_LIST
```

Everything about that job is now wrong. It runs on `[self-hosted]`, meaning the runner installed on the dev box. It symlinks a real secrets file from that box's filesystem. It runs `docker compose`, which has nothing to do with the cluster. **The runner and this job both go away entirely.** Stop and disable the runner's systemd unit, or keep it if you still want the Compose stack for local dev — but it is out of the deploy path either way, and leaving it registered while it no longer deploys anything is how you end up with a job silently succeeding against the wrong environment.

Replace it with:

```yaml
  bump-manifests:
    needs: [changes, build]
    if: needs.changes.outputs.services != '[]' && github.event_name == 'push' && github.ref == 'refs/heads/master'
    runs-on: ubuntu-latest        # no longer self-hosted; the dev box is out of the loop
    permissions:
      contents: write             # to push the manifest commit
    steps:
      - uses: actions/checkout@v4

      - name: Set image tags in the prod overlay
        run: |
          set -eu
          SHA_SHORT=$(git rev-parse --short=7 HEAD)
          REPO_LC=$(echo '${{ github.repository }}' | tr '[:upper:]' '[:lower:]')
          cd k8s/apps/overlays/prod
          for svc in $(echo '${{ needs.changes.outputs.services }}' | jq -r '.[]'); do
            IMG="ghcr.io/${REPO_LC}-${svc}"
            kustomize edit set image "${IMG}=${IMG}:sha-${SHA_SHORT}"
          done

      - name: Commit and push
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add k8s/apps/overlays/prod/kustomization.yaml
          git diff --staged --quiet || git commit -m "deploy: sha-$(git rev-parse --short=7 HEAD)"
          git push
```

`kustomize edit set image` writes into the overlay's `images:` block:

```yaml
images:
  - name: ghcr.io/linkify-solutions-inc/kubernetes-streaming-transcode-worker
    newTag: sha-a3f91c2
  - name: ghcr.io/linkify-solutions-inc/kubernetes-streaming-web
    newTag: sha-9d4e0b1
```

That single field rewrites the image on every resource in the overlay that references it — Deployments *and* both ScaledJob templates. Nothing enumerates the ScaledJobs by hand.

**The infinite-loop gotcha, and why the fix isn't what you'd guess.** Pushing to `master` from a workflow triggered by a push to `master` would normally re-trigger the workflow forever. The fix is that **commits pushed using the default `GITHUB_TOKEN` do not trigger new workflow runs.** That's deliberate GitHub behaviour and exactly the right mechanism here. So: do *not* swap in a PAT for this step — a PAT-authored push *does* re-trigger, and you get the loop. If you ever need a PAT for some other reason, fall back to `[skip ci]` in the commit message plus an `if: !contains(github.event.head_commit.message, '[skip ci]')` guard on the workflow.

While you're in this file, add the manifest validation job. It runs on every PR and catches the class of problem that otherwise only shows up as a failed sync:

```yaml
  manifest-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: kustomize build k8s/apps/overlays/prod > /tmp/prod.yaml
      - run: kustomize build k8s/apps/mediamtx/overlays/aws > /tmp/mediamtx.yaml
      # The db-migrate root reads SQL from postgres/migrations/, outside its own
      # tree, so it needs the same relaxed restrictor ArgoCD is given below.
      - run: kustomize build --load-restrictor LoadRestrictionsNone k8s/apps/base/db-migrate > /tmp/migrate.yaml
      - run: |
          kubeconform -strict -summary -schema-location default \
            -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
            /tmp/prod.yaml /tmp/mediamtx.yaml /tmp/migrate.yaml
      - run: scripts/check_ceiling_sync.sh /tmp/prod.yaml   # Module 14
```

The second `-schema-location` is what lets kubeconform validate `ScaledJob` and the other CRDs. Without it, `-strict` skips anything it has no schema for, which is precisely the resources most likely to have a typo.

Build all three roots, not just the prod overlay. `k8s/apps/mediamtx/overlays/aws/` is a separate Application (step 4 below) and would otherwise never be validated, and `k8s/apps/base/db-migrate/` is the one directory in the repo that needs `--load-restrictor LoadRestrictionsNone` — a CI job that never exercises it will not catch the failure that setting exists to prevent.

### 4. Understand why MediaMTX is a separate Application, then prove the split holds

There is nothing to move here — the repo already has this layout, and [Module 11](11-workloads.md) has had you applying two roots since you first deployed. This step is about knowing *why*, because it is the one piece of the directory structure that exists for an operational reason rather than a tidiness one.

**MediaMTX holds long-lived connections.** Every other pod on the platform serves short requests: restart `web` and a viewer's next page load lands on the new pod and nobody notices. MediaMTX holds an open RTMP session for the entire duration of a broadcast — hours, sometimes. Restarting it does not drop a request, it drops *every live stream on the platform at once*, and OBS does not silently reconnect; the streamer sees the disconnect and has to press Start Streaming again. That is why `40-mediamtx.yaml` has `syncPolicy: {}` and no `automated` block: with auto-sync, a config typo merged at 4pm ends everyone's broadcast at 4pm. Manual sync means you pick the moment.

**A manual gate is worthless if something else can sync the same objects.** Argo Applications own objects, and two of them cannot own the same one. If the MediaMTX Deployment were still listed in `k8s/apps/base/kustomization.yaml`, the auto-synced wave-3 `streaming` Application would render it too — and would push the held-back change within 30 seconds anyway. You would have a manual-sync Application that gates nothing, which is worse than not having one, because you would believe you were protected.

So MediaMTX is its own kustomize root at `k8s/apps/mediamtx/`, split the ordinary way into a portable half and an AWS half:

```
k8s/apps/mediamtx/
├── base/                             # cloud-neutral
│   ├── kustomization.yaml            # its own configMapGenerator for mediamtx.yml
│   ├── deployment.yaml
│   ├── service.yaml                  # the in-cluster ClusterIP
│   ├── pdb.yaml
│   └── mediamtx.yml
└── overlays/
    └── aws/                          # <- 40-mediamtx.yaml points HERE
        ├── kustomization.yaml
        ├── service-rtmp-nlb.yaml     # the public NLB — Module 12
        └── patches/mediamtx-placement.yaml
```

Three of those placements are worth naming.

**The NLB Service is in the overlay, not in `apps/overlays/prod/`,** because its target group is populated from the endpoints of the pods its selector matches — the load balancer and the Deployment behind it have to move as one unit, under one gate.

**The `mediamtx.yml` `configMapGenerator` is duplicated here rather than shared with `apps/base`,** because the generated ConfigMap's name hash is precisely what rolls the pod; if `apps/base` produced it, editing the config would restart MediaMTX through the auto-synced Application and walk straight around the gate.

**`base/` is cloud-neutral so the dev overlay can use it.** `k8s/apps/overlays/dev/` lists `../../mediamtx/base` alongside `../../base`, which is how the kubeadm box still gets MediaMTX — it has no NLB to create and no node labelled `workload: system`, so it takes the portable half and none of the AWS half. Note also that `overlays/aws` is a *sibling* of `base`, not a child of the root it extends: kustomize rejects a root that references its own parent with `cycle detected`, so `mediamtx/overlays/aws` could not have pointed at `mediamtx/` itself.

Confirm the split is clean — no object rendered by both roots:

```sh
ids() { kubectl kustomize "$1" | awk '/^kind:/{k=$2} /^  name:/{if(!n){n=$2; print k"/"n}} /^---$/{n=""}' | sort -u; }
comm -12 <(ids k8s/apps/overlays/prod) <(ids k8s/apps/mediamtx/overlays/aws)   # expect NO output
kubectl kustomize k8s/apps/mediamtx/overlays/aws | grep -c "kind: Deployment"  # expect 1
```

A plain `grep -c mediamtx` on the prod build is *not* the check, and it is the mistake to avoid: `MEDIAMTX_RTMP_URL` is a key in `streaming-config`, so the string legitimately appears in the prod output forever. Compare object identities, not text.

If you'd rather not run MediaMTX under a separate Application, the honest alternative is to delete `k8s/argocd/apps/40-mediamtx.yaml`, remove it from that directory's `kustomization.yaml`, and fold `k8s/apps/mediamtx/base` back into the prod overlay's `resources`. MediaMTX then syncs with everything else — which works, and means a bad MediaMTX commit drops every live stream the moment it merges. Make that a decision, not an accident.

### 5. ghcr.io image pull

ghcr packages inherit repository visibility, so `kubernetes-streaming-*` are **private** unless you change that. A private package with no credential in the cluster produces `ImagePullBackOff: unauthorized`, which looks identical to a wrong tag and wastes an hour.

**Option A — make the five packages public. This is the recommendation.**

Repository → Packages → each package → Package settings → Change visibility → Public.

Zero cluster configuration, no credential to rotate, nothing to replicate into namespaces. The images contain Python source that already lives in a repo you could make public and an apt-installed ffmpeg. Verify before you do it — nothing in the Dockerfiles copies `.env`, and `COPY . .` copies the service directory, which has no secrets in it. For a learning project the operational simplicity is worth more than the obscurity, and obscurity is all you're giving up.

**Option B — an imagePullSecret.** Fully specified, because you may not have the authority to flip visibility.

Create a PAT with `read:packages` and nothing else, store it in Secrets Manager alongside your other values, and let ESO template a `dockerconfigjson`:

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: ghcr-pull
  namespace: streaming
spec:
  secretStoreRef: {name: aws-secretsmanager, kind: ClusterSecretStore}
  refreshInterval: 1h
  target:
    name: ghcr-pull
    creationPolicy: Owner
    template:
      type: kubernetes.io/dockerconfigjson
      data:
        .dockerconfigjson: |
          {"auths":{"ghcr.io":{"username":"{{ .username }}","password":"{{ .token }}","auth":"{{ printf "%s:%s" .username .token | b64enc }}"}}}
  data:
    - {secretKey: username, remoteRef: {key: streaming/prod, property: GHCR_USER}}
    - {secretKey: token,    remoteRef: {key: streaming/prod, property: GHCR_TOKEN}}
```

Attach it on the **ServiceAccount**, not per-pod:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: transcode
  namespace: streaming
imagePullSecrets:
  - name: ghcr-pull
```

Doing it on the ServiceAccount is what makes KEDA-spawned Job pods inherit it without the ScaledJob template listing it — and the ScaledJob template is generated YAML you'd rather not have to remember to edit.

**Gotcha either way:** `imagePullSecrets` are namespace-scoped. Every namespace that pulls these images needs its own copy, which means its own `ExternalSecret`. That's a second reason to prefer Option A.

---

## Verify

The checkpoint is a full round trip with no `kubectl` from you at any point.

Make a trivial, visible change to one service — a log line is ideal:

```sh
cd /path/to/kubernetes-streaming
git checkout -b gitops-checkpoint
# edit services/web/main.py, e.g. add:
#   log.info("gitops checkpoint build")
git commit -am "web: gitops checkpoint log line"
git push -u origin gitops-checkpoint
# open the PR, watch manifest-check pass, merge to master
```

**Watch CI.** On master, the `build` job pushes `ghcr.io/linkify-solutions-inc/kubernetes-streaming-web:sha-<short>`, then `bump-manifests` runs:

```
Run kustomize edit set image ...
[master a3f91c2] deploy: sha-a3f91c2
 1 file changed, 1 insertion(+), 1 deletion(-)
To https://github.com/Linkify-Solutions-Inc/kubernetes-streaming.git
   9d4e0b1..a3f91c2  master -> master
```

Confirm the bot commit exists and did **not** start another run:

```sh
git fetch origin master && git log --oneline -3 origin/master
```
```
a3f91c2 deploy: sha-a3f91c2
9d4e0b1 web: gitops checkpoint log line
7c2b8e4 Add self-hosted runner setup documentation
```

**Watch ArgoCD pick it up.** Default polling is every 3 minutes.

```sh
kubectl get app streaming -n argocd -w
```
```
NAME        SYNC STATUS   HEALTH STATUS
streaming   Synced        Healthy
streaming   OutOfSync     Healthy
streaming   OutOfSync     Progressing
streaming   Synced        Progressing
streaming   Synced        Healthy
```

**Confirm the rollout.** Note that you are *reading* here, not applying:

```sh
kubectl get deploy web -n streaming \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```
```
ghcr.io/linkify-solutions-inc/kubernetes-streaming-web:sha-a3f91c2
```

```sh
kubectl logs -n streaming deploy/web --tail=5 | grep checkpoint
```
```
INFO:web:gitops checkpoint build
```

**That's the checkpoint: a code change reached production and the only commands you ran were `git` commands.**

If three minutes is too slow, add a GitHub webhook pointing at `https://stream.k8s.linkifysolutions.com/argocd/api/webhook` and sync happens in seconds. The polling interval is a floor on how fast a deploy can be, not a limit on correctness.

---

## What breaks

**An Application sits `OutOfSync` forever and re-syncing does nothing.**
Almost always a controller writing to a field git doesn't manage. Find the exact field:
```sh
kubectl get app streaming -n argocd -o jsonpath='{.status.conditions}' | jq
argocd app diff streaming
```
The fix is an `ignoreDifferences` entry for that group/kind/path, not turning off `selfHeal`.

**A Secret flaps and pods restart in a loop.**
You committed a `Secret`. `git grep -l "kind: Secret" k8s/` should return nothing except `ExternalSecret` definitions. Delete the committed Secret from git; Argo prunes it, ESO recreates it untracked, the loop stops.

**`no matches for kind "ScaledJob"` / `"Kafka"` / `"ExternalSecret"`.**
A CR is being applied before its CRD exists. Either the operator's Application is in the same wave (it must be earlier), or the CR is in the *same Application* as the chart that provides its CRD (split them, or annotate the CR with `argocd.argoproj.io/sync-options: SkipDryRunOnMissingResource=true`).

**`bump-manifests` fails with `Permission to ... denied`.**
The job is missing `permissions: contents: write`, or the repository has "Read repository contents permission" set as the default for `GITHUB_TOKEN` under Settings → Actions → General → Workflow permissions.

**CI runs forever in a loop.**
Someone replaced `GITHUB_TOKEN` with a PAT on the push step. Put it back.

**Pods stuck at `ImagePullBackOff`.**
```sh
kubectl describe pod <name> -n streaming | grep -A3 Failed
```
`unauthorized` means the package is private and there is no pull secret in *that namespace*. `manifest unknown` means the tag genuinely does not exist — check that `build` actually pushed before `bump-manifests` wrote the tag (`needs: [changes, build]` guarantees the ordering; if you edited that, you may have broken it).

**Argo overwrote something you'd hand-edited in the cluster.**
Working as designed. That's `selfHeal`. Commit it, or accept it's gone. If you need a scratch space Argo won't touch, use a namespace no Application targets.

**Deleting the root Application took down the entire cluster.**
Also working as designed — the `resources-finalizer.argocd.argoproj.io` finalizer cascades. To detach without deleting, remove the finalizer first and use `--cascade=orphan`; the exact commands are in `k8s/bootstrap/README.md`.

---

Next: [Module 16 — Monitoring, cost, and teardown](16-monitoring-cost-teardown.md).
