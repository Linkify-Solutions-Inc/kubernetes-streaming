# Module 0 — Pre-flight: the code must change first

## What you're building

A version of the five services that *can* run on AWS. No AWS resources yet, no account, no cluster — this module is entirely edits to `services/`, `postgres/`, `docker-compose.yml` and the test env stubs, verified by `docker compose up` and the existing pytest suite still passing. At the end of it, nothing about the application behaves differently on your laptop, and every hard blocker to running it on EKS is gone.

## Why it works this way

The instinct with a migration like this is lift-and-shift: containers are containers, Kubernetes runs containers, so point it at a cluster and go. That instinct is right about 80% of this codebase and wrong about the 20% that stops you dead.

Three things in the code make "just deploy it" impossible, and they are impossible in different ways:

1. **The S3 bucket is hardcoded to `media`.** You cannot create an S3 bucket called `media`. Not "you shouldn't" — you *can't*. Someone took the name a decade ago and S3 bucket names are globally unique across every AWS account on earth. There is no infrastructure workaround: no alias, no rename, no per-account namespace. The bucket name has to come out of the code.
2. **`web` builds HLS URLs with a `/media` path segment** that only made sense because MinIO puts the bucket in the URL path. Behind CloudFront the bucket *is* the origin, so that segment points at an S3 key that does not exist. Every player 403s, and the failure looks exactly like a CORS problem, which is the wrong thing to spend two days on.
3. **`s3_client()` passes explicit credentials to boto3.** boto3 checks explicit arguments first in its credential chain, so passing `aws_access_key_id` means the pod's IAM role is attached, present, valid — and silently ignored. And the vars are read with `os.environ[...]`, so you cannot fix that by not setting them: the process dies at import with a `KeyError`.

None of these produce a helpful error message on AWS. Blocker 1 fails at bucket-creation time with `BucketAlreadyExists` (which reads like a mistake you made). Blocker 2 fails in the browser. Blocker 3 fails as `AccessDenied` on a role that visibly has the permission.

There are two more changes in this module that are not blockers in the same absolute sense but that you will regret skipping: the database schema has to become re-runnable because RDS has no init hook, and every service should report *all* its missing config at once instead of dying on the first one.

Do this work before you have an AWS account. It's free, it's testable locally, and it means that when something breaks in Module 11, it's an AWS problem and not this.

---

## Do it

### 0.1 — The bucket name (`upload-api`, `transcode-worker`)

`services/upload-api/main.py` has the literal `"media"` in two places.

```diff
+BUCKET = os.environ.get("S3_BUCKET", "media")
+
+
 def s3_client():
```

```diff
     try:
-        s3_client().head_bucket(Bucket="media")
+        s3_client().head_bucket(Bucket=BUCKET)
         checks["minio"] = "ok"
     except Exception as exc:  # noqa: BLE001
         checks["minio"] = f"error: {exc}"
```

```diff
         try:
             s3_client().upload_fileobj(
-                _SizeLimitedReader(file.file, MAX_UPLOAD_BYTES), "media", object_key
+                _SizeLimitedReader(file.file, MAX_UPLOAD_BYTES), BUCKET, object_key
             )
```

`services/transcode-worker/main.py` has it once, as a module constant, and the three call sites (`upload_file` in `_upload_pass`, `download_file` and `upload_file` in `transcode_video`) already read that constant — so it is a one-line change:

```diff
 MEDIAMTX_RTMP_BASE = os.environ.get("MEDIAMTX_RTMP_URL", "rtmp://mediamtx:1935")
-BUCKET = "media"
+BUCKET = os.environ.get("S3_BUCKET", "media")
```

The default of `"media"` is deliberate: Compose sets nothing and keeps working, and MinIO's `minio-init` service still creates `local/media`. On EKS you set `S3_BUCKET=linkify-streaming-media-<accountid>` and the same image does the right thing.

Why the bucket ends up named `linkify-streaming-media-<accountid>`: bucket names must be globally unique, and appending your AWS account ID is the standard way to guarantee that without inventing a random suffix you then have to remember. That name is fixed in the [conventions table](README.md#conventions-used-throughout) and you'll create the bucket in [Module 9](09-s3-and-cloudfront.md).

### 0.2 — The `/media` path segment in `web`

Today:

```python
manifest_url = f"{MINIO_PUBLIC_URL}/media/hls/{prefix}/{content_id}/master.m3u8"
```

With MinIO, `MINIO_PUBLIC_URL` was a host (`http://box:9000`) and `/media/` was the **bucket name** in a path-style S3 URL: `http://box:9000/media/hls/live/<id>/master.m3u8`. Path-style URLs put the bucket in the path.

With CloudFront in front of one S3 bucket, the distribution's origin *is* that bucket. There is no bucket segment. `https://cdn.k8s.linkifysolutions.com/media/hls/live/<id>/master.m3u8` asks CloudFront for the S3 key `media/hls/live/<id>/master.m3u8`, and the key that exists is `hls/live/<id>/master.m3u8`. S3 returns 403 (not 404 — with a bucket policy scoped to `hls/*`, a request for a key outside that prefix is denied before the existence check), CloudFront passes it through, and `hls.js` reports a network error in the console.

That failure mode is worth naming precisely, because you will otherwise misdiagnose it: the browser shows a failed cross-origin request to your CDN, so it looks like the CORS configuration from [Module 9](09-s3-and-cloudfront.md) is wrong. It isn't. The URL is wrong. The tell is that `curl -I` on the same URL — no `Origin` header, no CORS involved — also returns 403.

Rename the variable and drop the segment:

```diff
-MINIO_PUBLIC_URL = os.environ["MINIO_PUBLIC_URL"]
+# On AWS this is the CloudFront distribution and HLS keys start at "hls/".
+# Under docker compose it's MinIO, where "/media" is the bucket segment of a
+# path-style URL — hence the fallback, so compose keeps working unchanged.
+HLS_PUBLIC_BASE_URL = os.environ.get("HLS_PUBLIC_BASE_URL") or (
+    os.environ["MINIO_PUBLIC_URL"] + "/media"
+)
```

```diff
     prefix = "live" if content_type == "stream" else "vod"
-    manifest_url = f"{MINIO_PUBLIC_URL}/media/hls/{prefix}/{content_id}/master.m3u8"
+    manifest_url = f"{HLS_PUBLIC_BASE_URL}/hls/{prefix}/{content_id}/master.m3u8"
```

(Section 0.5 replaces the `os.environ[...]` reads here with `require()`. Write it this way first if you want to keep the steps separate, or skip straight to the 0.5 version.)

On EKS you set `HLS_PUBLIC_BASE_URL=https://cdn.k8s.linkifysolutions.com` and `MINIO_PUBLIC_URL` never gets set at all.

The alternative designs, so you can defend the choice: you could keep a literal `media/` prefix inside the bucket, which means editing key construction in *two* files instead of one and carrying a meaningless path segment in every object forever. Or you could deploy a CloudFront Function that rewrites `/media/x` to `/x`, which costs ~$0.10 per million invocations plus a piece of JavaScript to version and debug at 2am when playback breaks. One line of Python is better than either.

### 0.3 — `s3_client()` in both S3 services

Both `upload-api/main.py` and `transcode-worker/main.py` contain the identical function. Three things about it are incompatible with EKS:

- **Explicit credentials win.** boto3's credential resolution order checks the arguments you passed to `boto3.client()` before it checks anything else. Pass `aws_access_key_id` and the EKS Pod Identity credentials are never consulted. The role is attached, the token is mounted, and boto3 doesn't look.
- **`os.environ[...]` raises on absent.** So "stop setting them" is not a fix by itself — the module fails to import and the pod crash-loops with a `KeyError: 'S3_ENDPOINT'` that `kubectl logs` will show you, if you think to look at the previous container.
- **`endpoint_url` plus `addressing_style: "path"` is MinIO's shape.** Real S3 still accepts path-style today, but AWS has deprecated it for new buckets and virtual-hosted style is the supported direction. Path-style also breaks TLS hostname matching for bucket names containing dots — and the bucket name is about to become an env var that somebody will eventually set to something dotted.

Replace the function, identically, in both files:

```python
def s3_client():
    # Endpoint and static credentials are set for MinIO under docker compose
    # and unset on AWS, where boto3 resolves the region from AWS_REGION and
    # picks up credentials from the EKS Pod Identity agent. Passing explicit
    # keys would make boto3 ignore that role entirely.
    kwargs = {
        "config": Config(
            s3={"addressing_style": os.environ.get("S3_ADDRESSING_STYLE", "path")},
            retries={"max_attempts": 5, "mode": "adaptive"},
        )
    }
    endpoint = os.environ.get("S3_ENDPOINT")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")
    if access_key and secret_key:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return boto3.client("s3", **kwargs)
```

Behaviour, spelled out:

| | `S3_ENDPOINT` | `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `S3_ADDRESSING_STYLE` | Result |
|---|---|---|---|---|
| docker compose | `http://minio:9000` | set | unset → `path` | Identical to today. Zero regression. |
| EKS | unset | unset | `virtual` | `https://<bucket>.s3.us-east-1.amazonaws.com`, credentials from the pod's role |

The `retries` block is new and is not strictly required. It's there because S3 across a NAT-free VPC endpoint under a transcoder writing a segment every two seconds will occasionally return `SlowDown`, and adaptive mode backs off correctly instead of failing the upload.

One thing to carry forward to [Module 11](11-workloads.md): **set `AWS_REGION` explicitly** in the Deployment spec for both services. Pod Identity injects it, but that's an implementation detail that a chart upgrade can change, and boto3's failure when it's missing is `NoRegionError: You must specify a region` raised at client-construction time — inside a request handler, surfacing as a 500 with a stack trace nobody was expecting.

### 0.4 — `postgres/init.sql` becomes a re-runnable migration

Compose mounts `./postgres/init.sql` into `/docker-entrypoint-initdb.d/`, a hook the official Postgres image provides: on first boot with an empty data directory, it runs everything in that directory.

**RDS has no such hook.** There is no `/docker-entrypoint-initdb.d`, there is no way to hand a SQL file to the instance at creation time, and the instance lives in a private subnet where you can't reach it from your laptop without a bastion. The mechanism you'll use in [Module 8](08-rds-postgres.md) is a Kubernetes `Job` that mounts the SQL as a ConfigMap and runs `psql` from inside the cluster.

That Job runs on every deploy. Which means the SQL has to be safe to apply twice — today the second run fails with `relation "streamers" already exists`, the Job exhausts its `backoffLimit`, and it reports `Failed` in a way that is indistinguishable from a real schema error.

Move the file and make every statement idempotent:

```sh
mkdir -p postgres/migrations
git mv postgres/init.sql postgres/migrations/001_baseline.sql
```

```diff
-CREATE TABLE streamers (
+CREATE TABLE IF NOT EXISTS streamers (
```

```diff
-CREATE TABLE streams (
+CREATE TABLE IF NOT EXISTS streams (
```

```diff
-CREATE TABLE videos (
+CREATE TABLE IF NOT EXISTS videos (
```

```diff
-CREATE TABLE view_events (
+CREATE TABLE IF NOT EXISTS view_events (
```

Four mechanical edits. Then update the Compose mount so local dev still gets its schema (see 0.6).

The numbered-file convention matters for what comes next. Every future schema change is a new file — `002_add_thumbnails.sql` — that is itself idempotent (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`). The Job's loop applies `/migrations/*.sql` in lexical order every time, and re-applying is a no-op.

Be honest with yourself about what this is: **it is not a migration tool.** There's no version table, so it can't detect out-of-order application, can't roll back, and can't express a change that isn't naturally idempotent — a data backfill, a column rename. For four tables and a two-person team that is the right amount of machinery. When you need a destructive change, adopt Alembic or Atlas; the numbered-file layout is already the shape those tools expect, so the switch is cheap.

### 0.5 — A shared `config.py` per service

From `SPEC.md`, in the list of reasons the current `.env` approach has to go:

> Today's `.env` drift already caused one real outage this session — a value silently missing from the file crashed `web` on redeploy — so this isn't optional polish.

That is the failure mode this change addresses, and moving to Secrets Manager in [Module 7](07-secrets.md) does not fix it. The problem isn't where the value is stored; it's that `os.environ["X"]` fails on the *first* missing variable, tells you about that one, and says nothing about the other three that are also missing. You fix it, redeploy, wait for the rollout, and find the next one. On Compose that loop is 20 seconds. On EKS with an image pull and a readiness probe it's several minutes each time, and you do it in the middle of a broken deploy.

Add this file to each service directory that needs it — `upload-api`, `transcode-worker`, `web`, `ingest-webhook`, `analytics-worker`:

```python
# services/<service>/config.py
"""Read required env vars, then report every missing one at once.

os.environ["X"] fails on the first missing var and hides the rest, which
turns a bad deploy into a serial guessing game (see SPEC.md).
"""
import os
import sys

_missing: list[str] = []


def require(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        _missing.append(name)
        return ""
    return value


def seal() -> None:
    if _missing:
        sys.exit("missing required environment variables: " + ", ".join(sorted(_missing)))
```

It has to be copied per service rather than shared from the repo root: each `Dockerfile` is `COPY . .` with the service directory as the build context, so nothing above `services/<service>/` exists inside the image. Fifteen duplicated lines across five services is the cheaper trade against restructuring every build context and Dockerfile.

`seal()` uses `sys.exit` with a string, which exits non-zero and prints the message to stderr — so it shows up in `kubectl logs` as a plain sentence rather than a traceback, and the container goes to `CrashLoopBackOff` immediately rather than serving traffic in a broken state.

Here is `services/web/main.py`'s header using it:

```python
import httpx
from config import require, seal
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

UPLOAD_API_URL = require("UPLOAD_API_URL")
RTMP_PUBLIC_URL = require("RTMP_PUBLIC_URL")
# AWS: the CloudFront domain, HLS keys start at "hls/".
# Compose: MinIO, where "/media" is the bucket segment of a path-style URL.
HLS_PUBLIC_BASE_URL = os.environ.get("HLS_PUBLIC_BASE_URL") or (
    require("MINIO_PUBLIC_URL") + "/media"
)
seal()
```

Note the ordering: every `require()` runs before `seal()`, so a boot with none of the three set prints

```
missing required environment variables: MINIO_PUBLIC_URL, RTMP_PUBLIC_URL, UPLOAD_API_URL
```

and you fix all three in one pass. `os` is still imported for the `HLS_PUBLIC_BASE_URL` fallback — `HLS_PUBLIC_BASE_URL` is genuinely optional, so it stays an `os.environ.get`, and only the MinIO fallback path registers `MINIO_PUBLIC_URL` as required.

Apply the same pattern to the others:

| Service | `require()` calls |
|---|---|
| `upload-api` | `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_DSN` |
| `transcode-worker` | `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_DSN` |
| `ingest-webhook` | `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_DSN` |
| `analytics-worker` | `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_DSN` |
| `web` | `UPLOAD_API_URL`, `RTMP_PUBLIC_URL`, and `MINIO_PUBLIC_URL` only when `HLS_PUBLIC_BASE_URL` is unset |

`POSTGRES_DSN` in `upload-api` and the workers is read inside `pg_connect()` rather than at module level today. Hoist it to a module-level `POSTGRES_DSN = require("POSTGRES_DSN")` and have `pg_connect()` use the constant — otherwise a missing DSN still fails at first-request time instead of at boot, which is the whole point of the change. `S3_BUCKET`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `S3_ADDRESSING_STYLE` all stay as `os.environ.get` with defaults: they're genuinely optional in one environment or the other, and that is exactly the distinction `require()` is meant to encode.

### 0.6 — Compose and the test stubs, in the same commit

If you do 0.1–0.5 and stop, local dev breaks (the init.sql mount points at a moved file) and CI goes red (the tests import modules whose env expectations changed). Both are one-liners; do them now.

**`docker-compose.yml`** — repoint the schema mount:

```diff
     volumes:
       - postgres-data:/var/lib/postgresql/data
-      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
+      - ./postgres/migrations:/docker-entrypoint-initdb.d
```

The Postgres image runs every `.sql` in that directory in lexical order, so mounting the directory gets you the same behaviour as before *and* picks up `002_*.sql` when it exists, with no further Compose edits.

Nothing else in `docker-compose.yml` needs to change. `S3_BUCKET` is unset there and defaults to `media`; `S3_ENDPOINT`/`S3_ACCESS_KEY`/`S3_SECRET_KEY` are already set on `upload-api` and `transcode-worker`; `MINIO_PUBLIC_URL` is already set on `web` and the fallback handles it. If you'd rather be explicit than rely on defaults, add `S3_BUCKET: media` to `upload-api` and `transcode-worker` — it documents the contract and costs nothing.

**`services/web/test_main.py`** — the stubs work as-is, because the `MINIO_PUBLIC_URL` fallback covers the renamed variable. Add one line so the AWS path is exercised too if you want coverage of it; otherwise leave it alone.

**`.github/workflows/ci.yml`** — the dummy env block feeds all three test services. It already sets `KAFKA_BOOTSTRAP_SERVERS`, `POSTGRES_DSN`, `UPLOAD_API_URL`, `MINIO_PUBLIC_URL` and `RTMP_PUBLIC_URL`, which covers every `require()` in the table above. No change needed — but re-read that block after you edit any service's header, because a `require()` you add without a matching stub turns into a `sys.exit` at import and pytest reports it as a collection error, not a test failure.

**`services/transcode-worker/test_main.py`** sets only `KAFKA_BOOTSTRAP_SERVERS`. Once `POSTGRES_DSN` moves to a module-level `require()`, add it:

```diff
 os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "test:9092")
+os.environ.setdefault("POSTGRES_DSN", "postgresql://test")
```

`services/ingest-webhook/test_main.py` already sets both.

---

## Verify

This is the checkpoint. Do not start [Module 1](01-aws-account-setup.md) until both halves pass.

**1. The application still works locally.**

```sh
docker compose down -v          # -v so postgres re-initialises from the new path
docker compose up -d
docker compose ps
```

Every service `running`, `minio-init` and `kafka-init` `exited (0)`. Then:

```sh
curl -s localhost:8001/health
```

```json
{"postgres":"ok","minio":"ok"}
```

`"minio":"ok"` is the meaningful half — it's `head_bucket(Bucket=BUCKET)` going through the rewritten `s3_client()` against the env-var bucket name. If that returns `ok`, 0.1 and 0.3 are both correct.

Confirm the schema landed from the new location:

```sh
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\dt'
```

```
             List of relations
 Schema |    Name     | Type  |  Owner
--------+-------------+-------+----------
 public | streamers   | table | streaming
 public | streams     | table | streaming
 public | videos      | table | streaming
 public | view_events | table | streaming
(4 rows)
```

Then open `http://localhost:8080`, create a streamer, upload a short MP4, and watch it. The watch page's manifest URL should be unchanged from before this module — `http://<your-minio-host>/media/hls/vod/<id>/master.m3u8`. That's the fallback in 0.2 doing its job: on Compose, nothing moved.

**2. The tests still pass.**

```sh
for s in transcode-worker ingest-webhook web; do
  (cd "services/$s" && pip install -q -r requirements-dev.txt && pytest -q) || echo "FAILED: $s"
done
```

```
....................                                                     [100%]
20 passed in 0.84s
```

Exact counts will differ; what matters is zero failures and zero collection errors. A collection error that says `SystemExit: missing required environment variables: ...` means you added a `require()` without a matching stub in that service's `test_main.py` — go back to 0.6.

**3. The idempotency check that the whole of Module 8 depends on.**

```sh
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  < postgres/migrations/001_baseline.sql
```

```
NOTICE:  relation "streamers" already exists, skipping
NOTICE:  relation "streams" already exists, skipping
NOTICE:  relation "videos" already exists, skipping
NOTICE:  relation "view_events" already exists, skipping
```

`NOTICE` lines and exit code 0. If you get `ERROR: relation "streamers" already exists` you missed an `IF NOT EXISTS`, and the `db-migrate` Job in Module 8 will fail on its second run.

---

## What breaks

**`web` renders a watch page but the video never loads, console shows a failed request to the CDN.** The single most common outcome of getting 0.2 half-right — usually `HLS_PUBLIC_BASE_URL` set *with* a trailing slash, producing `//hls/...`, or set to the CloudFront domain without the `https://` scheme. Read the actual URL out of the page rather than guessing:

```sh
curl -s "http://localhost:8080/watch/video/<id>" | grep -o 'https\?://[^"]*master.m3u8'
```

Then `curl -I` that exact URL. A 403 with no `Origin` header involved means the path is wrong, not CORS.

**`upload-api` returns `{"minio":"error: An error occurred (403) when calling the HeadBucket operation"}` on AWS while uploads work fine.** `head_bucket` requires `s3:ListBucket` on the *bucket* ARN, which is a different resource from the `arn:.../raw/*` that `PutObject` needs. The IAM policy in [Module 9](09-s3-and-cloudfront.md) includes it for exactly this reason. On Compose you won't see this at all — MinIO's root credentials can do everything.

**A pod crash-loops with `KeyError: 'S3_ENDPOINT'` (or any other var).** You applied 0.3 to one of the two files and not the other. They are byte-identical functions and it is easy to fix `upload-api`, feel done, and forget `transcode-worker`.

```sh
grep -n 'os.environ\[' services/*/main.py
```

After this module the only `os.environ[...]` reads left should be none — everything required goes through `require()`, everything optional through `os.environ.get`.

**The `db-migrate` Job fails on its second run in Module 8.** `IF NOT EXISTS` missed on one statement. Diagnostic is verification step 3 above, run twice.

**boto3 on EKS raises `NoRegionError` at client construction.** `AWS_REGION` is not set on the pod. It surfaces as a 500 from `/health` or an unhandled exception in the transcoder's upload thread, not as a startup failure, because `s3_client()` is called per-request.

```sh
kubectl -n streaming exec deploy/upload-api -- env | grep -E 'AWS_REGION|AWS_DEFAULT_REGION'
```

**CI goes red with `SystemExit` during collection.** A `require()` without a stub. The CI env block is in `.github/workflows/ci.yml` under the `test` job; the per-file stubs are the `os.environ.setdefault` lines at the top of each `test_main.py`. Both have to cover every `require()` in that service.

---

Next: [Module 1 — AWS account, access, and not getting a surprise bill](01-aws-account-setup.md).
