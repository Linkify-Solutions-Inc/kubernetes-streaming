# Module 7 — Secrets

*Part II — Platform. Previous: [Module 6](06-karpenter.md). Next: [Module 8](08-rds-postgres.md).*

---

## What you're building

The end of `.env`. AWS Secrets Manager holds the credentials, the External Secrets Operator pulls them into the cluster on a schedule, and the applications read ordinary Kubernetes Secrets without knowing any of that happened. Non-secret configuration — the public RTMP and CDN URLs, the bucket name, the Kafka bootstrap address — moves to a ConfigMap in git where you can read it in a diff.

You will also retire several `.env` keys permanently, and deal with the fact that the file currently contains a live SSH password.

---

## Why it works this way

### What replaces `.env`, and why not the obvious alternatives

Compose loads `.env` from disk and interpolates it into container environments. There is no equivalent on Kubernetes, and the naive translations are all worse than they look:

- **`kubectl create secret` by hand.** Works, once. Then nobody knows which cluster has which value, the secret is nowhere in version control, and recreating the cluster means recreating secrets from someone's terminal history.
- **Secrets committed to git.** Kubernetes Secrets are base64, not encryption. Committing one is publishing it.
- **Sealed Secrets / SOPS.** Encrypt the value into git, decrypt in-cluster. Legitimate, but the ciphertext still lives in the repo, rotation means a commit, and you now own a key-management problem.
- **AWS Secrets Manager + External Secrets Operator.** The value lives in one place, with IAM controlling who can read it, CloudTrail recording who did, and rotation possible without a deploy. Git holds a *reference* — the secret's name — which is not sensitive.

The last one is what you are building. ESO is a controller that reads `ExternalSecret` resources, fetches from a provider, and writes ordinary Kubernetes Secrets. Your pods use `envFrom: secretRef` exactly as they would have anyway; nothing in the application knows AWS is involved.

Secrets Manager costs **$0.40 per secret per month** plus $0.05 per 10,000 API calls. Three secrets is $1.20/month, and `refreshInterval: 1h` across three secrets is about 2,200 calls a month — a cent. It is not a line item worth optimizing.

### Secret or config? Decide per key, not per file

`.env` mixed the two because Compose gave no reason not to. Kubernetes distinguishes them, and it is worth honouring the distinction:

**Secrets** are things that grant access: database passwords, registry tokens, API credentials. They belong in Secrets Manager, gated by IAM, audited.

**Config** is everything else: hostnames, bucket names, ports, feature toggles. `RTMP_PUBLIC_URL` is literally printed on a web page for streamers to copy into OBS. `HLS_PUBLIC_BASE_URL` is in the `<video>` element of every viewer's browser. Treating them as secrets buys nothing and costs plenty — you cannot see the current value in a diff, you cannot review a change to it, and you pay $0.40/month for the privilege of hiding a URL that is on the internet.

So: **`RTMP_PUBLIC_URL`, `HLS_PUBLIC_BASE_URL`, `S3_BUCKET`, `UPLOAD_API_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `AWS_REGION` go in a ConfigMap in git.** Only the database credentials, the GHCR token and the MediaMTX API password go in Secrets Manager.

### Every key in the current `.env`, and where it goes

| `.env` key today | Destination | Notes |
|---|---|---|
| `POSTGRES_USER` | **Secrets Manager** (RDS-managed) | Folded into `POSTGRES_DSN` — see below |
| `POSTGRES_PASSWORD` | **Secrets Manager** (RDS-managed) | RDS creates and owns it; you never see it |
| `POSTGRES_DB` | **Secrets Manager** (`streaming/db-endpoint`) | Not a secret and *not* in the RDS-managed secret — see below. Created in [Module 8](08-rds-postgres.md) alongside the host and port |
| `MINIO_PUBLIC_URL` | **ConfigMap**, renamed `HLS_PUBLIC_BASE_URL` | The `/media` path segment was MinIO's bucket name; CloudFront has no bucket segment. Code change in [Module 0](00-preflight-code-changes.md). |
| `RTMP_PUBLIC_URL` | **ConfigMap** | **Currently missing from `.env` entirely.** `web/main.py` does `os.environ["RTMP_PUBLIC_URL"]` at import, so the pod crashes on startup without it. |
| `MINIO_ROOT_USER` | **Retired** | No MinIO. Replaced by Pod Identity. |
| `MINIO_ROOT_PASSWORD` | **Retired** | Same |
| `S3_ACCESS_KEY` | **Retired** | Same |
| `S3_SECRET_KEY` | **Retired** | Same |
| `S3_ENDPOINT` | **Retired** | Real S3 needs no endpoint override |
| `KAFKA_CLUSTER_ID` | **Retired** | Strimzi generates and owns the KRaft cluster ID ([Module 10](10-kafka-strimzi.md)) |
| `SERVER_IP` | **Retired** | The dev box is not part of this stack |
| `SERVER_USERNAME` | **Retired** | Same |
| `SERVER_PASSWORD` | **Retired — and rotate it** | See below |

**On the retirements that Pod Identity causes:** `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` and `S3_ACCESS_KEY`/`S3_SECRET_KEY` are the same two values wearing different names — Compose passed the MinIO root credentials straight through as the S3 keys. On AWS there is nothing to pass. The pod's ServiceAccount is associated with an IAM role, the SDK picks up temporary credentials from the Pod Identity agent, and they rotate themselves every few hours. **The best thing you can do with a long-lived access key is not have one.** Note this depends on the `s3_client()` change from [Module 0](00-preflight-code-changes.md): boto3 checks explicit arguments *first*, so as long as `aws_access_key_id=` is passed, Pod Identity credentials are fetched and then ignored.

**`KAFKA_CLUSTER_ID`** was a KRaft cluster identifier someone generated once and pasted into `.env`. Strimzi owns that now — it generates the ID when the `Kafka` resource is created and stores it in the cluster's own state. Keeping the old value around is worse than useless: if anyone ever set it, they would be asserting an identity that does not match the storage.

### `SERVER_PASSWORD` — read this part

The first three lines of `.env` are:

```
SERVER_IP=100.64.0.9
SERVER_USERNAME=fatima
SERVER_PASSWORD=<a real password>
```

Three things are true about this and all of them matter.

**It is a live SSH credential for a real machine, sitting in plaintext on disk.** Not an application secret — a login. Anyone who has ever had a copy of that file, or a backup of a laptop containing it, has shell access to the dev box as that user.

**It does not belong in the same file as application config.** Application config is copied around casually: pasted into chats, mounted into containers, included in support bundles, echoed by a debugging script that dumps the environment. That is fine for a bucket name. It is not fine for a login, and mixing the two guarantees the login gets the careless handling.

**It has no Kubernetes equivalent, so it does not migrate.** No pod SSHes to the dev box. There is no secret to create, no `ExternalSecret` to write. It is deleted, not moved.

**Rotate it as part of this migration.** Not because you know it leaked, but because you cannot know it did not — its exposure history includes every copy of the repo working tree and every shell that ever ran `env`. Change the password on the box, remove the three lines from `.env`, confirm `.env` is in `.gitignore` (it is), and — if the file was ever committed — remember that `git rm` does not remove it from history, so the rotation is the only thing that actually helps.

### Secrets Manager layout

Everything under the `streaming/` prefix, which is what the IAM policy scopes to. Values are JSON objects, so one secret can hold several related keys and ESO can flatten them.

| Secret | Contents | Created by |
|---|---|---|
| `rds!db-xxxxxxxx` | `{username, password}` — and see the warning below | **RDS**, automatically ([Module 8](08-rds-postgres.md)) |
| `streaming/db-endpoint` | `{"host": "...", "port": "5432", "dbname": "streaming"}` | You, in [Module 8](08-rds-postgres.md), once the instance has an endpoint |
| `streaming/ghcr` | `{"username": "...", "token": "..."}` for the image pull secret | You |
| `streaming/mediamtx` | `{"apiUser": "...", "apiPassword": "..."}` | You |

**Let RDS own the database credential.** `aws rds create-db-instance --manage-master-user-password` makes RDS generate the password, store it in its own secret, and optionally rotate it. You never see it, never paste it, and it exists in exactly one place. That secret's name is not under `streaming/`, which is why the IAM policy below allows `rds!db-*` as a second resource.

**Do not assume what else is in that secret.** The only fields AWS documents as being in an RDS-managed master user secret are `username` and `password`. AWS documents the secret's ARN and its status — it does not document the JSON shape, and it is free to change it. Plenty of writing online says the blob also carries `engine`, `host`, `port` and `dbname`; that shape comes from the *hand-created* "credentials for an RDS database" secret template, which is a different thing.

This matters more than it sounds, because the failure is silent. ESO's `extract` pulls whatever keys are there. Reference `{{ .host }}` when no `host` key exists and the template renders an empty string — the `ExternalSecret` reports `SecretSynced`, the Secret is created, and `POSTGRES_DSN` comes out as `postgresql://user:pw@:5432/?sslmode=require`. Everything says green and nothing connects.

So this course takes the endpoint from somewhere it controls: **`streaming/db-endpoint`**, a second, plain Secrets Manager entry holding the host, port and database name. None of those three is a credential — they are stable, non-secret facts about the instance, sitting in a private subnet — but putting them in Secrets Manager alongside the password gives the ESO template one uniform source instead of a mix of fetched values and hand-edited literals. It is already covered by the `streaming/*` resource pattern in the IAM policy below, so it needs no extra permission. Rotation still flows through untouched, because the password still comes from the secret RDS owns.

### The derived-value problem: composing `POSTGRES_DSN`

This is the one part of the migration where a straight key-to-key copy does not work, so it is worth understanding properly.

`docker-compose.yml` builds the DSN with shell interpolation at deploy time:

```yaml
POSTGRES_DSN: postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
```

Four `.env` values become one string, and every service gets the composed result. All five Python services then do `psycopg.connect(os.environ["POSTGRES_DSN"])`.

**Kubernetes has no equivalent of that interpolation.** Specifically:

- `envFrom: secretRef` projects keys as literals. No templating.
- `valueFrom: secretKeyRef` reads one key into one variable. No concatenation.
- `$(VAR)` substitution in a container's `env` list *does* work — but only for variables defined earlier in that same container's `env`. To build the DSN that way you would have to expose `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` and the host as separate environment variables first, which puts the raw password in the pod spec's environment anyway, and means `kubectl describe pod` prints the variable names beside a value assembled from them.

The alternative — writing the composed DSN into Secrets Manager by hand — throws away the thing that made RDS-managed credentials worth having. Rotate the password and your hand-written DSN is stale, silently, until every service fails to connect at once.

**ESO's `template` block solves it exactly.** The template runs over the fetched data *before* the Kubernetes Secret is written, so composition happens once, in the operator, and the app sees a single literal value that stays correct across rotations:

This is `k8s/infra/secrets/externalsecret-db.yaml`, abridged — read the file itself, the comments in it are the argument:

```yaml
  target:
    name: streaming-db
    creationPolicy: Owner
    template:
      engineVersion: v2
      data:
        POSTGRES_DSN: >-
          postgresql://{{ .username }}:{{ .password | urlquery }}@{{ .host }}:{{ .port }}/{{ .dbname }}?sslmode=require
        host:     "{{ .host }}"
        port:     "{{ .port }}"
        username: "{{ .username }}"
        password: "{{ .password }}"
        dbname:   "{{ .dbname }}"
  dataFrom:
    # username + password. The name is generated, not chosen — Module 8 shows
    # you how to look it up.
    - extract:
        key: rds!db-REPLACE-ME
    # host + port + dbname. Created in Module 8. See the layout section above
    # for why these do not come out of the secret RDS manages.
    - extract:
        key: streaming/db-endpoint
```

**Two `extract` entries, one flat namespace.** ESO merges everything `dataFrom` produces into a single map before the template runs, so `.username` comes from the first secret and `.host` from the second and the template does not know or care which. A key present in both would be won by the later entry; there is no overlap here by design.

Three more details earn their place:

**`| urlquery` is not decoration.** RDS-generated passwords contain punctuation. A password containing `@` produces `postgresql://user:pa@ss@host:5432/db`, and the URL parser splits on the *first* `@` — so it reads the host as `ss@host` and psycopg reports what looks like a DNS failure. A `/` truncates the database name instead. This will happen eventually, the symptom points at the wrong thing entirely, and one filter prevents it.

**`sslmode=require`** is appended because [Module 8](08-rds-postgres.md) sets `rds.force_ssl=1` on the parameter group. Without it every connection is refused at the server, which presents as `connection closed` rather than anything mentioning TLS.

**The individual fields are kept as well** because the `db-migrate` Job in [Module 8](08-rds-postgres.md) runs `psql`, which wants `PGHOST`/`PGUSER`/`PGPASSWORD`, not a DSN. Emitting both shapes from one `ExternalSecret` costs nothing.

Note that `engineVersion: v2` is required for these Go template functions. `v1` is the deprecated templating engine and silently behaves differently.

### The thing `refreshInterval` does not do

`refreshInterval: 1h` re-fetches from Secrets Manager and rewrites the Kubernetes Secret. **It does not restart your pods.** A pod that read the Secret via `envFrom` at startup holds those values in its process environment until it dies. Rotate the RDS password and the Kubernetes Secret updates within the hour while every running pod continues using the old one — and keeps working, because the old password is still valid, right up until it is not.

Two honest options:

1. **Accept it and write it down.** After any rotation, `kubectl rollout restart deployment -n streaming --all`. This project has no rotation schedule, so this is the right answer for now — but it has to be in the runbook, not in someone's memory.
2. **Run [Reloader](https://github.com/stakater/Reloader)** — one small Deployment that watches Secrets and ConfigMaps and restarts the workloads that reference them. Worth it once rotation is automatic; premature before then.

The same applies to the ConfigMap: editing `streaming-config` does not restart anything either.

---

## Do it

`AWS_PROFILE=linkify-streaming` and `AWS_REGION=us-east-1` exported.

### 7.1 — Create the secrets you own

The GHCR pull token first. `ghcr.io/linkify-solutions-inc/*` images are private and cluster nodes have no GitHub credentials, so without this every pod fails with `ImagePullBackOff: denied`.

Generate a **fine-grained personal access token with `read:packages` and nothing else** at `https://github.com/settings/tokens`. Not a classic token with `repo` scope — that grants write access to source code to anything that can read this secret.

```sh
aws secretsmanager create-secret \
  --name streaming/ghcr \
  --description "GHCR pull token for the streaming platform" \
  --secret-string '{"username":"<github-username>","token":"<ghp_...>"}'
```

Then the MediaMTX API credentials. [Module 12](12-ingress-and-rtmp.md) enables MediaMTX's control API on `:9997` for the NLB health check, and it should not be open:

```sh
aws secretsmanager create-secret \
  --name streaming/mediamtx \
  --secret-string "{\"apiUser\":\"mediamtx\",\"apiPassword\":\"$(openssl rand -base64 24)\"}"
```

Generating it inline means the password never appears in your shell history or on your screen. Confirm both exist:

```sh
aws secretsmanager list-secrets \
  --query 'SecretList[?starts_with(Name, `streaming/`)].[Name,CreatedDate]' --output table
```

The RDS secret is not created here — `--manage-master-user-password` in [Module 8](08-rds-postgres.md) creates it.

### 7.2 — IAM role for ESO

Same Pod Identity trust policy as [Modules 4](04-addons-and-storage.md) and [5](05-dns-and-certificates.md):

```sh
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

cat > /tmp/pod-identity-trust.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "pods.eks.amazonaws.com" },
      "Action": ["sts:AssumeRole", "sts:TagSession"]
    }
  ]
}
EOF

aws iam create-role \
  --role-name streaming-external-secrets \
  --assume-role-policy-document file:///tmp/pod-identity-trust.json

cat > /tmp/eso-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
      "Resource": [
        "arn:aws:secretsmanager:us-east-1:${ACCOUNT_ID}:secret:streaming/*",
        "arn:aws:secretsmanager:us-east-1:${ACCOUNT_ID}:secret:rds!db-*"
      ]
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name streaming-external-secrets \
  --policy-name read-streaming-secrets \
  --policy-document file:///tmp/eso-policy.json
```

Read-only, and scoped to two name patterns. ESO never needs to write, and this role cannot read a secret belonging to anything else in the account. The `rds!db-*` entry is separate because RDS names its managed secrets itself and will not put them under your prefix.

### 7.3 — Install ESO

```sh
helm repo add external-secrets https://charts.external-secrets.io
helm repo update
helm search repo external-secrets/external-secrets --versions | head -3
```

Pin what that reports:

```sh
export ESO_VERSION="<version from helm search>"

helm install external-secrets external-secrets/external-secrets \
  -n external-secrets --create-namespace \
  --version "$ESO_VERSION" \
  --set installCRDs=true

aws eks create-pod-identity-association \
  --cluster-name streaming \
  --namespace external-secrets \
  --service-account external-secrets \
  --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/streaming-external-secrets"

# Pod Identity credentials resolve at pod start, so the pod that is already
# running has none. Restart it.
kubectl -n external-secrets rollout restart deploy/external-secrets
kubectl -n external-secrets rollout status deploy/external-secrets
```

Three Deployments come up: `external-secrets` (the controller), `external-secrets-webhook` (validates the CRs) and `external-secrets-cert-controller` (issues the webhook's certificate). All three must be Ready before any `ExternalSecret` will be accepted — applying one against a not-yet-ready webhook fails with a connection-refused error mentioning `validate.externalsecret.external-secrets.io`.

Check which API version your install serves, because the manifests below use `v1`:

```sh
kubectl api-resources | grep externalsecret
```

```
externalsecrets    es    external-secrets.io/v1    true    ExternalSecret
```

If yours only offers `v1beta1`, either upgrade the chart or change `apiVersion` in the four manifests to match. The field layout is otherwise the same.

### 7.4 — Create the namespace and the store

```sh
kubectl create namespace streaming
kubectl apply -f k8s/infra/secrets/clustersecretstore.yaml
```

```yaml
apiVersion: external-secrets.io/v1
kind: ClusterSecretStore
metadata:
  name: aws-secretsmanager
spec:
  provider:
    aws:
      service: SecretsManager
      region: us-east-1
```

A **`ClusterSecretStore`**, not a namespaced `SecretStore`, because the same store serves `streaming` today and will serve other namespaces later without duplication.

There is deliberately **no `auth:` block**. With Pod Identity, the AWS SDK's default credential chain resolves through the agent automatically. Do **not** add `auth.jwt.serviceAccountRef` — ESO cannot impersonate a ServiceAccount whose role is bound by Pod Identity, and adding it produces `unable to assume role`, which reads like an IAM trust-policy problem and is actually a client-configuration problem. That error costs people an hour.

Confirm the store is valid:

```sh
kubectl get clustersecretstore aws-secretsmanager
```

```
NAME                 AGE   STATUS   CAPABILITIES   READY
aws-secretsmanager   10s   Valid    ReadWrite      True
```

`Valid`/`True` means ESO authenticated to Secrets Manager successfully. If it says `Invalid`, stop here — no `ExternalSecret` will work.

### 7.5 — The ExternalSecrets and the ConfigMap

```sh
kubectl apply -f k8s/infra/secrets/externalsecret-ghcr.yaml
kubectl apply -f k8s/infra/secrets/externalsecret-mediamtx.yaml
kubectl apply -f k8s/infra/secrets/configmap-app.yaml
```

The GHCR one needs a `template` for a different reason than the database one — the shape, not the composition. The kubelet only honours a pull secret of type `kubernetes.io/dockerconfigjson` containing a single `.dockerconfigjson` key with a very specific JSON structure:

```yaml
  target:
    name: ghcr
    template:
      engineVersion: v2
      type: kubernetes.io/dockerconfigjson
      data:
        .dockerconfigjson: |
          {"auths":{"ghcr.io":{"username":"{{ .username }}","password":"{{ .token }}","auth":"{{ printf "%s:%s" .username .token | b64enc }}"}}}
  dataFrom:
    - extract:
        key: streaming/ghcr
```

The `auth` field is `username:token` base64-encoded — legacy, redundant with the two fields beside it, and required anyway by some registry clients. `b64enc` produces it.

Reference it from every pod spec with `imagePullSecrets: [{name: ghcr}]`, or once for the whole namespace by patching the default ServiceAccount:

```sh
kubectl -n streaming patch serviceaccount default \
  -p '{"imagePullSecrets":[{"name":"ghcr"}]}'
```

The ConfigMap holds the non-secret half. Edit `S3_BUCKET` to your real account ID before applying:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: streaming-config
  namespace: streaming
data:
  RTMP_PUBLIC_URL: "rtmp://rtmp.k8s.linkifysolutions.com:1935"
  HLS_PUBLIC_BASE_URL: "https://cdn.k8s.linkifysolutions.com"
  UPLOAD_API_URL: "http://upload-api.streaming.svc.cluster.local:8000"
  MEDIAMTX_RTMP_URL: "rtmp://mediamtx.streaming.svc.cluster.local:1935"
  KAFKA_BOOTSTRAP_SERVERS: "streaming-kafka-bootstrap.kafka.svc.cluster.local:9092"
  S3_BUCKET: "linkify-streaming-media-<accountid>"
  AWS_REGION: "us-east-1"
  S3_ADDRESSING_STYLE: "virtual"
```

`RTMP_PUBLIC_URL` carries the explicit `:1935`. `web/main.py` builds `f"{RTMP_PUBLIC_URL}/{stream_key}"` and hands the result to a human to paste into OBS; OBS would default to 1935 anyway, but the explicit port makes the copy-pasted value self-documenting. This key **is not in `.env` today** — if you skip it, `web` crashes at import on `os.environ["RTMP_PUBLIC_URL"]`.

`KAFKA_BOOTSTRAP_SERVERS` uses the fully-qualified name because Kafka lives in the `kafka` namespace and the apps live in `streaming`. The short name resolves only within a namespace.

Consumed in [Module 11](11-workloads.md) like this:

```yaml
envFrom:
  - configMapRef: { name: streaming-config }
  - secretRef:    { name: streaming-db }      # POSTGRES_DSN and the PG* fields
```

### 7.6 — The database ExternalSecret

`k8s/infra/secrets/externalsecret-db.yaml` is written and ready, but **it cannot sync yet** — neither of the two secrets it reads exists until [Module 8](08-rds-postgres.md): RDS creates `rds!db-…` with the instance, and you create `streaming/db-endpoint` once the instance has an endpoint to record. Module 8 does both, then substitutes the one placeholder and applies this file. There is no second copy of this manifest anywhere in the course — Module 8 applies *this* one.

```sh
# In Module 8, once RDS exists:
aws rds describe-db-instances --db-instance-identifier streaming-db \
  --query 'DBInstances[0].MasterUserSecret.SecretArn' --output text
# then replace `rds!db-REPLACE-ME` in externalsecret-db.yaml with that secret's
# name and apply it. `streaming/db-endpoint` is already correct — you create it
# under exactly that name.
```

Applying it now is harmless but produces a permanently failing resource, which makes the checkpoint below ambiguous. Leave it.

---

## Verify

**1. The store authenticated.**

```sh
kubectl get clustersecretstore aws-secretsmanager
```

`STATUS=Valid`, `READY=True`.

**2. The ExternalSecrets synced.**

```sh
kubectl get externalsecret -n streaming
```

```
NAME                 STORE                REFRESH INTERVAL   STATUS         READY
ghcr                 aws-secretsmanager   24h                SecretSynced   True
streaming-mediamtx   aws-secretsmanager   1h                 SecretSynced   True
```

`SecretSynced` / `True` on both. Anything else — `SecretSyncedError`, `ready=False` — means read the events:

```sh
kubectl describe externalsecret ghcr -n streaming | tail -20
```

**3. The generated Secrets exist and have the right shape.**

```sh
kubectl get secret -n streaming
```

```
NAME                 TYPE                             DATA   AGE
ghcr                 kubernetes.io/dockerconfigjson   1      45s
streaming-mediamtx   Opaque                           2      45s
```

The `TYPE` column on `ghcr` must read `kubernetes.io/dockerconfigjson`. If it says `Opaque`, the `template.type` line is missing and image pulls will fail with an error that never mentions the secret's type.

Prove the pull credential actually works, rather than assuming:

```sh
kubectl -n streaming get secret ghcr -o jsonpath='{.data.\.dockerconfigjson}' \
  | base64 -d | python3 -m json.tool
```

```json
{
    "auths": {
        "ghcr.io": {
            "username": "linkify-bot",
            "password": "ghp_xxxxxxxxxxxxxxxxxxxx",
            "auth": "bGlua2lmeS1ib3Q6Z2hwX3h4eHh4eHh4"
        }
    }
}
```

**4. The template engine works — including `urlquery`.** The database secret cannot sync until [Module 8](08-rds-postgres.md), but you can validate the template today with a throwaway secret in the same shape, deliberately containing a password with the punctuation that breaks naive composition:

```sh
aws secretsmanager create-secret --name streaming/db-template-test \
  --secret-string '{"username":"streaming","password":"p@ss/w0rd","host":"db.example.internal","port":"5432","dbname":"streaming"}'

# Both `extract` keys are pointed at the one throwaway secret, because neither
# real source exists yet. Extracting the same secret twice is harmless — the
# merged map is the same either way.
sed -e 's|rds!db-REPLACE-ME|streaming/db-template-test|' \
    -e 's|streaming/db-endpoint|streaming/db-template-test|' \
    -e 's|name: streaming-db|name: streaming-db-test|' \
    k8s/infra/secrets/externalsecret-db.yaml | kubectl apply -f -

sleep 5
kubectl -n streaming get secret streaming-db-test -o jsonpath='{.data.POSTGRES_DSN}' | base64 -d; echo
```

```
postgresql://streaming:p%40ss%2Fw0rd@db.example.internal:5432/streaming?sslmode=require
```

**That is the checkpoint.** The `@` became `%40` and the `/` became `%2F`, so the URL has exactly one `@` separating credentials from host and the database name is intact. Without `| urlquery` you would see `postgresql://streaming:p@ss/w0rd@db.example.internal:5432/...`, which psycopg parses as host `ss` and reports as a name-resolution failure.

Clean up the test:

```sh
kubectl -n streaming delete externalsecret streaming-db-test
kubectl -n streaming delete secret streaming-db-test
aws secretsmanager delete-secret --secret-id streaming/db-template-test \
  --force-delete-without-recovery
```

Run the same decode against the real `streaming-db` Secret at the end of [Module 8](08-rds-postgres.md):

```sh
kubectl -n streaming get secret streaming-db -o jsonpath='{.data.POSTGRES_DSN}' | base64 -d
```

**5. `.env` has been dealt with.** Not a command — a checklist:

- [ ] `SERVER_IP`, `SERVER_USERNAME`, `SERVER_PASSWORD` removed from `.env`
- [ ] That SSH password **rotated on the box**
- [ ] `MINIO_ROOT_*`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_ENDPOINT`, `KAFKA_CLUSTER_ID` removed
- [ ] `.env` confirmed in `.gitignore` (`git check-ignore -v .env`)
- [ ] `git log --all --full-history -- .env` returns nothing — if it returns commits, the values in them are compromised regardless of what the file says now

---

## What breaks

Ordered by how often each actually happens.

### 1. `ExternalSecret` shows `SecretSyncedError`

```sh
kubectl describe externalsecret <name> -n streaming | tail -25
kubectl -n external-secrets logs deploy/external-secrets --tail=50
```

The message names the cause:

| Message | Cause |
|---|---|
| `AccessDeniedException` | IAM. Check the association, and that the secret name matches the policy's `streaming/*` or `rds!db-*` pattern. |
| `ResourceNotFoundException` | Secret name typo, or it is in a different region. `aws secretsmanager list-secrets --query 'SecretList[].Name'`. |
| `unable to assume role` | An `auth:` block in the `ClusterSecretStore`. Delete it — with Pod Identity there must be none. |
| `key not found in secret` | The JSON in Secrets Manager lacks a field the template references. Compare `aws secretsmanager get-secret-value --secret-id <name> --query SecretString` with the template. |

### 2. `ClusterSecretStore` shows `Invalid`

ESO cannot get credentials at all — usually because the pod started before the Pod Identity association existed.

```sh
kubectl describe clustersecretstore aws-secretsmanager | tail -20
aws eks list-pod-identity-associations --cluster-name streaming \
  --query 'associations[?namespace==`external-secrets`]'
kubectl -n external-secrets rollout restart deploy/external-secrets
```

Verify the ServiceAccount name is exactly `external-secrets`:

```sh
kubectl -n external-secrets get sa
```

### 3. Pods `ImagePullBackOff` with `denied` even though the `ghcr` Secret exists

```sh
kubectl describe pod <name> -n streaming | grep -A10 Events
kubectl -n streaming get secret ghcr -o jsonpath='{.type}'
kubectl -n streaming get sa default -o yaml | grep -A3 imagePullSecrets
```

Three usual causes: the Secret type is `Opaque` rather than `kubernetes.io/dockerconfigjson`; the pod spec has no `imagePullSecrets` and the ServiceAccount was never patched; or the PAT has expired or lacks `read:packages`. Test the credential outside Kubernetes to isolate it:

```sh
echo "<token>" | docker login ghcr.io -u "<username>" --password-stdin
```

### 4. `POSTGRES_DSN` present but the app cannot connect

Decode it and look at it before assuming the database is at fault:

```sh
kubectl -n streaming get secret streaming-db -o jsonpath='{.data.POSTGRES_DSN}' | base64 -d; echo
```

- More than one `@` before the host → `| urlquery` is missing from the template.
- No `?sslmode=require` → RDS with `rds.force_ssl=1` refuses the connection and reports it as a closed connection.
- Right-looking DSN, still failing → it is the security group, not this module. [Module 8](08-rds-postgres.md).

### 5. Rotating a secret changes nothing

Working as designed, and covered above. ESO updated the Kubernetes Secret; the running pods still hold the old environment.

```sh
kubectl -n streaming get secret streaming-db -o jsonpath='{.metadata.annotations}' | jq
kubectl rollout restart deployment -n streaming --all
```

To force ESO to re-fetch immediately rather than waiting out `refreshInterval`:

```sh
kubectl -n streaming annotate externalsecret streaming-db force-sync="$(date +%s)" --overwrite
```

### 6. Applying an `ExternalSecret` fails with a webhook error

```sh
kubectl -n external-secrets get pods
kubectl -n external-secrets logs deploy/external-secrets-webhook --tail=30
```

`connection refused` on `validate.externalsecret.external-secrets.io` means the webhook Deployment is not Ready yet — wait for all three ESO Deployments and retry. If it persists, the cert-controller has not issued the webhook's certificate; restart it.

### 7. `no matches for kind "ExternalSecret" in version "external-secrets.io/v1"`

Your chart version serves `v1beta1` only.

```sh
kubectl api-resources | grep external-secrets
helm list -n external-secrets
```

Either upgrade the chart or change `apiVersion` in the four manifests under `k8s/infra/secrets/`. Do not mix versions across files.

---

**Next:** [Module 8 — RDS Postgres](08-rds-postgres.md). That is where the RDS-managed secret appears and `externalsecret-db.yaml` finally gets applied.
