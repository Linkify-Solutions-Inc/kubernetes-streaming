# Module 9 — S3 and CloudFront

← [Module 8: RDS Postgres](08-rds-postgres.md) · [Index](README.md) · [Module 10: Kafka with Strimzi](10-kafka-strimzi.md) →

---

## What you're building

The object storage that replaces the MinIO container: one private S3 bucket holding source uploads and HLS output, fronted by a CloudFront distribution on `cdn.k8s.linkifysolutions.com` that is the *only* thing allowed to read it. Plus the two IAM roles that let `upload-api` and `transcode-worker` reach it without credentials, and the bucket CORS policy without which the player breaks in Chrome and works in Safari.

At the end, `curl -I https://cdn.k8s.linkifysolutions.com/hls/...` returns 200 with the right content type and an `access-control-allow-origin` header.

Cost: pennies while you are learning. CloudFront's first 1 TB/month of egress is free on the AWS Free Tier's always-free allowance; S3 storage for a few GB of HLS is under a dollar. The line item that can surprise you is covered under lifecycle rules below.

---

## Why it works this way

### Why the bucket is private, when the MinIO one was not

`docker-compose.yml` runs this on startup:

```sh
mc mb --ignore-existing local/media &&
mc anonymous set download local/media/hls
```

That makes the entire `hls/` prefix world-readable with no authentication. On a dev box that is a reasonable trade: MinIO is bound to a Docker network, the only person who can reach it is you, and it removes an entire class of "why is the player 403ing" debugging from your first week.

The same command against S3 exposes a bucket to the whole internet, with per-request and per-GB billing attached to it, no rate limiting, and no way to know it happened until the bill arrives. The failure mode is not "someone reads my test video"; it is "someone points a downloader at it and you pay for the egress." Block Public Access stays fully on, and CloudFront gets read access through an Origin Access Control — a signed request from CloudFront's service principal, restricted to one distribution.

Note where the bucket policy is scoped: `hls/*` only. `raw/` — the original uploads — is not readable by CloudFront at all, so a request for `/raw/anything` returns 403 from S3 with nothing extra to configure. That is structural rather than procedural: you cannot accidentally expose source uploads later by adding a cache behaviour, because the permission does not exist.

### The key layout, and where it comes from

```
s3://linkify-streaming-media-<accountid>/
  raw/{video_id}{ext}                              # source uploads, never public
  hls/live/{stream_id}/master.m3u8
  hls/live/{stream_id}/{1080p,720p,480p}/playlist.m3u8
  hls/live/{stream_id}/{1080p,720p,480p}/segment_%03d.ts
  hls/vod/{video_id}/...                           # same shape
```

You are not choosing this layout — the code already writes it. `services/upload-api/main.py` builds `object_key = f"raw/{video_id}{ext}"`, and `services/transcode-worker/main.py` sets `s3_prefix = f"hls/live/{stream_id}"` and `f"hls/vod/{video_id}"`. What you are choosing is that the bucket, its lifecycle rules and its CloudFront behaviours line up with those three prefixes.

The bucket name gets the account ID appended because S3 names are globally unique across all AWS customers. `linkify-streaming-media` is almost certainly taken; `linkify-streaming-media-123456789012` is yours and is reproducible if you tear down and rebuild.

### Lifecycle rules on day one, especially the invisible one

Three rules, and the third is the one nobody sets.

**`hls/live/` expires after 7 days.** Live HLS is a rotating window that nothing reads once the stream ends. The VOD copy is the durable artifact.

**`raw/` expires after 30 days.** This assumes the transcoded VOD output is what you keep and the source is disposable. If you ever want to re-transcode from source at a new bitrate, extend it — that is a real trade-off, not a default to accept blindly.

**Abort incomplete multipart uploads after 1 day.** This is the invisible one. `upload_fileobj` switches to multipart for anything over 8 MB. When an upload dies partway — the client disconnects, or `_SizeLimitedReader` raises `_UploadTooLarge` because someone exceeded the 2 GiB cap — the parts already uploaded stay in the bucket. They **do not appear in `aws s3 ls`**, they **do not appear in the console's object listing**, and they **bill as standard storage forever**. The only way to see them is `list-multipart-uploads`. Given that this application has a size limit and a rate limiter, i.e. it expects people to hit failure paths, this will happen. One rule, set once, and it never does.

### Why the cache behaviours have to be split

`_s3_extra_args` in `services/transcode-worker/main.py` already sets exactly the right headers per object:

```python
def _s3_extra_args(fname: str, *, live: bool) -> dict:
    if fname.endswith(".m3u8"):
        cache_control = "no-cache, must-revalidate" if live else "public, max-age=31536000, immutable"
        return {"ContentType": "application/vnd.apple.mpegurl", "CacheControl": cache_control}
    return {"ContentType": "video/mp2t", "CacheControl": "public, max-age=31536000, immutable"}
```

So: live manifests are `no-cache, must-revalidate`; VOD manifests and *all* `.ts` segments are `immutable` for a year. That split is correct, and it is correct because of what HLS is. A live `.m3u8` is rewritten every couple of seconds with a new segment appended and an old one dropped. A `.ts` segment, once written, never changes — its filename identifies its contents.

The trap: **CloudFront's `MinTTL` overrides the origin's `Cache-Control`.** The managed `CachingOptimized` policy has `MinTTL = 1`, which is survivable for a live manifest (one second of staleness). Anything larger is not, and the failure is spectacular in a confusing direction:

- Viewers receive a playlist that lists segments which ffmpeg's `+delete_segments` has already removed.
- The player requests those segments, gets 404s, and hls.js shows "Connection hiccup — reconnecting…" in a loop.
- Meanwhile the stream is fine. The transcoder is running, S3 has current segments, and if you `curl` the manifest with a cache-buster it looks perfect.

**If CloudFront caches live manifests, the stream freezes for viewers and nothing you look at on the server side will show it.** That is the single most confusing failure in this module. The fix is a dedicated cache behaviour for `hls/live/*.m3u8` with `MinTTL 0, DefaultTTL 1, MaxTTL 2` — one second of caching, which is still enough to absorb a thundering herd of viewers hitting the same manifest, and short enough that no viewer ever sees a segment list that has expired.

Everything else falls through to the default behaviour with `CachingOptimized`, which honours the origin's `max-age=31536000, immutable` and serves segments from the edge essentially forever. That is where all the bandwidth is, and it is the entire reason for putting a CDN here.

CloudFront path patterns treat `*` as matching across `/`, so `hls/live/*.m3u8` correctly matches `hls/live/<uuid>/720p/playlist.m3u8`. Do not take that on faith — the Verify section checks it with `x-cache` and `age` headers.

### CORS: the delta that silently breaks the player

Nothing in the compose stack configures CORS, and nothing needed to: **MinIO reflects the `Origin` header by default. S3 does not.**

The browser loads the page from `https://stream.k8s.linkifysolutions.com`. hls.js then fetches `https://cdn.k8s.linkifysolutions.com/hls/.../master.m3u8` **via XHR/fetch**, which is a cross-origin request. Without `Access-Control-Allow-Origin` on the response the browser discards it, and `watch.html` shows "Playback error". Meanwhile the network tab shows **200 OK**, with the correct body. The object is there, the URL is right, the response is fine, and the player is broken.

There is an asymmetry that makes this worse. The player does:

```js
if (video.canPlayType("application/vnd.apple.mpegurl")) { video.src = src; }
else if (Hls.isSupported()) { /* hls.js */ }
```

Safari takes the first branch — native HLS through a `<video>` element, which is not subject to CORS. So **it works perfectly in Safari on your Mac and fails in Chrome**, which is a great way to spend an afternoon blaming the network.

Fix it in two places, because each alone has a hole:

**(a) Bucket CORS**, which is correct at the origin. This is what answers a preflight `OPTIONS`.

**(b) A CloudFront `ResponseHeadersPolicy`**, which is what actually saves you. With bucket CORS alone, `Access-Control-Allow-Origin` comes *from the origin* and is therefore **cached by CloudFront**. Unless `Origin` is part of the cache key, whichever request populates the cache first decides what every subsequent viewer gets — and the first request may well have no `Origin` header at all (Safari, a `curl`, a monitoring probe). You would get CORS failures that depend on which edge location the viewer hit and that "fix themselves" after an invalidation. A ResponseHeadersPolicy makes CloudFront synthesise the header on every response regardless of what is cached, which removes the whole class of bug.

You still want (a), because preflight `OPTIONS` requests are forwarded to S3 and S3 has to answer them.

### The `/media` path segment is gone

With MinIO, `MINIO_PUBLIC_URL` was a host and `/media/` in the URL was the **bucket name** in a path-style URL:

```
http://localhost:9000/media/hls/live/<id>/master.m3u8
       └── host ──┘  └bucket┘└──── key ─────┘
```

With CloudFront the origin *is* one bucket, so there is no bucket segment in the path. `https://cdn.k8s.../media/hls/...` maps to the S3 key `media/hls/...`, which does not exist, and every player 403s.

[Module 0](00-preflight-code-changes.md) already handled this by introducing `HLS_PUBLIC_BASE_URL`:

```python
HLS_PUBLIC_BASE_URL = os.environ.get("HLS_PUBLIC_BASE_URL") or (
    os.environ["MINIO_PUBLIC_URL"] + "/media"   # back-compat for docker compose
)
manifest_url = f"{HLS_PUBLIC_BASE_URL}/hls/{prefix}/{content_id}/master.m3u8"
```

On EKS you set `HLS_PUBLIC_BASE_URL=https://cdn.k8s.linkifysolutions.com` and the `/media` segment disappears. Compose keeps working unchanged. That value gets wired into the ConfigMap in [Module 11](11-workloads.md); note it now so it is not a surprise there.

### Why two IAM roles, not one

`upload-api` writes to `raw/` and never touches HLS. `transcode-worker` reads `raw/` and writes `hls/`, and never needs to delete anything (ffmpeg's `+delete_segments` deletes from the local scratch directory; `_upload_pass` never removes from S3 — which is exactly why the `hls/live/` lifecycle rule exists).

Giving each its own role costs one extra `create-role` and buys you a real boundary: a bug in the upload path cannot overwrite HLS output, and a bug in the transcoder cannot destroy someone's source file. On a two-service system that is a small win; the habit is the point.

One non-obvious permission: `upload-api`'s `health()` calls `head_bucket(Bucket=...)`, and `HeadBucket` is authorised by **`s3:ListBucket` on the bucket ARN**, not by any object permission. Omit it and you get a health check that returns `An error occurred (403)` while every real upload works fine — a readiness probe failing for a reason that has nothing to do with readiness.

Pod Identity, rather than IRSA, because it is the newer mechanism and does not require an OIDC-provider round trip or annotating the ServiceAccount with a role ARN. [Module 7](07-secrets.md) covers the distinction. The one thing to remember: **an association only takes effect when the pod starts.** Create the association after the pods are running and you will debug a permissions problem that is actually a stale pod.

---

## Do it

```sh
export AWS_PROFILE=linkify-streaming
export AWS_REGION=us-east-1

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET="linkify-streaming-media-${ACCOUNT_ID}"
echo "$BUCKET"
```

### 1. The bucket

```sh
# us-east-1 is the one region where create-bucket takes no LocationConstraint.
aws s3api create-bucket --bucket "$BUCKET" --region us-east-1

aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-encryption --bucket "$BUCKET" \
  --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}'
```

SSE-S3 (`AES256`) rather than SSE-KMS: KMS charges per request, and HLS is a very high request-count workload — a live stream writes a segment per rendition every few seconds and rewrites three manifests every two. SSE-KMS here would be a real line item for no threat-model gain, since the bucket is already private.

Confirm Block Public Access took, because everything downstream depends on it:

```sh
aws s3api get-public-access-block --bucket "$BUCKET" \
  --query PublicAccessBlockConfiguration
```

All four values must be `true`.

### 2. Lifecycle rules

```sh
cat > /tmp/lifecycle.json <<'JSON'
{"Rules": [
  {"ID": "expire-live-hls", "Status": "Enabled",
   "Filter": {"Prefix": "hls/live/"},
   "Expiration": {"Days": 7}},

  {"ID": "expire-raw-uploads", "Status": "Enabled",
   "Filter": {"Prefix": "raw/"},
   "Expiration": {"Days": 30}},

  {"ID": "abort-incomplete-mpu", "Status": "Enabled",
   "Filter": {"Prefix": ""},
   "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}
]}
JSON

aws s3api put-bucket-lifecycle-configuration \
  --bucket "$BUCKET" --lifecycle-configuration file:///tmp/lifecycle.json

aws s3api get-bucket-lifecycle-configuration --bucket "$BUCKET"
```

The command that shows you what the third rule is for — run it any time you suspect the bucket is bigger than the object listing says:

```sh
aws s3api list-multipart-uploads --bucket "$BUCKET"
```

Empty output today. Come back after Module 13 and after someone has cancelled an upload.

### 3. Origin Access Control

```sh
OAC_ID=$(aws cloudfront create-origin-access-control \
  --origin-access-control-config '{
    "Name": "streaming-media-oac",
    "Description": "S3 origin for the streaming HLS distribution",
    "OriginAccessControlOriginType": "s3",
    "SigningBehavior": "always",
    "SigningProtocol": "sigv4"
  }' --query 'OriginAccessControl.Id' --output text)

echo "oac=$OAC_ID"
```

Origin Access *Identity* (OAI) is the legacy mechanism and is still what most tutorials show. Use OAC: it signs with SigV4, works in every region, and supports SSE-KMS if you ever change your mind about encryption.

### 4. Cache and response-headers policies

```sh
# 1s cache for live manifests. MinTTL 0 is the part that matters — it is what
# stops CloudFront overriding the origin's no-cache.
LIVE_CACHE_POLICY_ID=$(aws cloudfront create-cache-policy --cache-policy-config '{
  "Name": "streaming-hls-live-manifest",
  "Comment": "Near-live HLS playlists",
  "MinTTL": 0,
  "DefaultTTL": 1,
  "MaxTTL": 2,
  "ParametersInCacheKeyAndForwardedToOrigin": {
    "EnableAcceptEncodingGzip": true,
    "EnableAcceptEncodingBrotli": true,
    "HeadersConfig": {"HeaderBehavior": "none"},
    "CookiesConfig": {"CookieBehavior": "none"},
    "QueryStringsConfig": {"QueryStringBehavior": "none"}
  }
}' --query 'CachePolicy.Id' --output text)

RHP_ID=$(aws cloudfront create-response-headers-policy --response-headers-policy-config '{
  "Name": "streaming-hls-cors",
  "Comment": "Synthesise CORS headers regardless of what is cached",
  "CorsConfig": {
    "AccessControlAllowOrigins": {"Quantity": 1, "Items": ["https://stream.k8s.linkifysolutions.com"]},
    "AccessControlAllowMethods": {"Quantity": 3, "Items": ["GET", "HEAD", "OPTIONS"]},
    "AccessControlAllowHeaders": {"Quantity": 1, "Items": ["*"]},
    "AccessControlAllowCredentials": false,
    "AccessControlExposeHeaders": {"Quantity": 4, "Items": ["Content-Length", "Content-Range", "ETag", "Accept-Ranges"]},
    "AccessControlMaxAgeSec": 3000,
    "OriginOverride": true
  }
}' --query 'ResponseHeadersPolicy.Id' --output text)

echo "live cache policy=$LIVE_CACHE_POLICY_ID"
echo "response headers policy=$RHP_ID"
```

`OriginOverride: true` is deliberate: CloudFront replaces whatever the origin said about CORS with this, on every response, cached or not.

You also need two AWS-managed policy IDs. They are stable, but look them up rather than pasting IDs from a blog post:

```sh
aws cloudfront list-cache-policies --type managed \
  --query "CachePolicyList.Items[?CachePolicy.CachePolicyConfig.Name=='Managed-CachingOptimized'].CachePolicy.Id" \
  --output text
# expect: 658327ea-f89d-4fab-a63d-7e88639e58f6

aws cloudfront list-origin-request-policies --type managed \
  --query "OriginRequestPolicyList.Items[?OriginRequestPolicy.OriginRequestPolicyConfig.Name=='Managed-CORS-S3Origin'].OriginRequestPolicy.Id" \
  --output text
# expect: 88a5eaf4-2fd4-4709-b370-b4c650ea3fcf
```

`Managed-CORS-S3Origin` forwards `Origin`, `Access-Control-Request-Method` and `Access-Control-Request-Headers` to S3. Without it, a preflight `OPTIONS` reaches S3 with no `Origin` header and S3 answers 403 — the bucket CORS config never gets a chance to apply.

### 5. The distribution

You need the wildcard ACM certificate from [Module 5](05-dns-and-certificates.md). CloudFront only accepts certificates from **us-east-1**, regardless of where anything else lives — which for this project is free, since you are in us-east-1 anyway.

```sh
CERT_ARN=$(aws acm list-certificates --region us-east-1 \
  --query "CertificateSummaryList[?DomainName=='*.k8s.linkifysolutions.com'].CertificateArn" \
  --output text)

CACHING_OPTIMIZED=658327ea-f89d-4fab-a63d-7e88639e58f6
CORS_S3_ORIGIN=88a5eaf4-2fd4-4709-b370-b4c650ea3fcf

cat > /tmp/dist.json <<JSON
{
  "CallerReference": "streaming-cdn-$(date +%s)",
  "Comment": "streaming HLS",
  "Enabled": true,
  "HttpVersion": "http2and3",
  "PriceClass": "PriceClass_100",
  "Aliases": {"Quantity": 1, "Items": ["cdn.k8s.linkifysolutions.com"]},
  "Origins": {"Quantity": 1, "Items": [{
    "Id": "s3-media",
    "DomainName": "${BUCKET}.s3.us-east-1.amazonaws.com",
    "OriginAccessControlId": "${OAC_ID}",
    "S3OriginConfig": {"OriginAccessIdentity": ""},
    "CustomHeaders": {"Quantity": 0},
    "ConnectionAttempts": 3,
    "ConnectionTimeout": 10
  }]},
  "DefaultCacheBehavior": {
    "TargetOriginId": "s3-media",
    "ViewerProtocolPolicy": "redirect-to-https",
    "AllowedMethods": {"Quantity": 3, "Items": ["GET", "HEAD", "OPTIONS"],
      "CachedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]}},
    "Compress": true,
    "CachePolicyId": "${CACHING_OPTIMIZED}",
    "OriginRequestPolicyId": "${CORS_S3_ORIGIN}",
    "ResponseHeadersPolicyId": "${RHP_ID}"
  },
  "CacheBehaviors": {"Quantity": 1, "Items": [{
    "PathPattern": "hls/live/*.m3u8",
    "TargetOriginId": "s3-media",
    "ViewerProtocolPolicy": "redirect-to-https",
    "AllowedMethods": {"Quantity": 3, "Items": ["GET", "HEAD", "OPTIONS"],
      "CachedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]}},
    "Compress": true,
    "CachePolicyId": "${LIVE_CACHE_POLICY_ID}",
    "OriginRequestPolicyId": "${CORS_S3_ORIGIN}",
    "ResponseHeadersPolicyId": "${RHP_ID}"
  }]},
  "ViewerCertificate": {
    "ACMCertificateArn": "${CERT_ARN}",
    "SSLSupportMethod": "sni-only",
    "MinimumProtocolVersion": "TLSv1.2_2021"
  },
  "Restrictions": {"GeoRestriction": {"RestrictionType": "none", "Quantity": 0}}
}
JSON

DIST_ID=$(aws cloudfront create-distribution --distribution-config file:///tmp/dist.json \
  --query 'Distribution.Id' --output text)

DIST_DOMAIN=$(aws cloudfront get-distribution --id "$DIST_ID" \
  --query 'Distribution.DomainName' --output text)

echo "distribution=$DIST_ID  domain=$DIST_DOMAIN"
```

`PriceClass_100` is North America and Europe. It roughly halves per-GB delivery cost and this project has no viewers elsewhere. `Compress: true` matters for `.m3u8`, which is text and compresses around 5:1; CloudFront skips compression on `.ts` automatically because of its content type.

Deployment takes a few minutes:

```sh
aws cloudfront wait distribution-deployed --id "$DIST_ID"
```

### 6. The bucket policy that lets only this distribution in

```sh
cat > /tmp/bucket-policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "AllowCloudFrontHLSOnly",
    "Effect": "Allow",
    "Principal": {"Service": "cloudfront.amazonaws.com"},
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::${BUCKET}/hls/*",
    "Condition": {"StringEquals": {
      "AWS:SourceArn": "arn:aws:cloudfront::${ACCOUNT_ID}:distribution/${DIST_ID}"
    }}
  }]
}
JSON

aws s3api put-bucket-policy --bucket "$BUCKET" --policy file:///tmp/bucket-policy.json
```

Read that `Resource` line again: `hls/*`, not `*`. A request through CloudFront for `/raw/anything` gets 403 from S3, permanently, because the permission to read it does not exist.

### 7. CORS on the bucket

```sh
aws s3api put-bucket-cors --bucket "$BUCKET" --cors-configuration '{
  "CORSRules": [{
    "AllowedOrigins": ["https://stream.k8s.linkifysolutions.com"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedHeaders": ["Origin", "Range", "Access-Control-Request-Method", "Access-Control-Request-Headers"],
    "ExposeHeaders": ["Content-Length", "Content-Range", "ETag", "Accept-Ranges"],
    "MaxAgeSeconds": 3000
  }]
}'

aws s3api get-bucket-cors --bucket "$BUCKET"
```

`Range` in `AllowedHeaders` and `Content-Range` / `Accept-Ranges` in `ExposeHeaders` are load-bearing: hls.js issues range requests for segments, and without the exposed response headers seeking in a VOD breaks while normal playback looks fine.

If you later serve the page from another origin (a local dev server on `http://localhost:8080`, say), add it to **both** this list and the ResponseHeadersPolicy. Missing it in one of the two produces the intermittent-by-edge-location behaviour described above.

### 8. DNS

CloudFront is not a Kubernetes object, so ExternalDNS will not manage this record. Create it once:

```sh
ZONE_ID=$(aws route53 list-hosted-zones-by-name \
  --dns-name k8s.linkifysolutions.com \
  --query 'HostedZones[0].Id' --output text | cut -d/ -f3)

cat > /tmp/cdn-record.json <<JSON
{"Changes": [{
  "Action": "UPSERT",
  "ResourceRecordSet": {
    "Name": "cdn.k8s.linkifysolutions.com",
    "Type": "A",
    "AliasTarget": {
      "HostedZoneId": "Z2FDTNDATAQYW2",
      "DNSName": "${DIST_DOMAIN}",
      "EvaluateTargetHealth": false
    }
  }
}]}
JSON

aws route53 change-resource-record-sets --hosted-zone-id "$ZONE_ID" \
  --change-batch file:///tmp/cdn-record.json
```

`Z2FDTNDATAQYW2` is a constant — it is CloudFront's hosted zone ID for alias records, the same for every distribution in every account. It looks like a placeholder and is not.

### 9. IAM roles and Pod Identity

First check the Pod Identity Agent addon is installed (Module 4):

```sh
aws eks list-addons --cluster-name streaming --query addons
# expect eks-pod-identity-agent in the list
```

Trust policy, identical for both roles:

```sh
cat > /tmp/pod-identity-trust.json <<'JSON'
{"Version": "2012-10-17", "Statement": [{
  "Effect": "Allow",
  "Principal": {"Service": "pods.eks.amazonaws.com"},
  "Action": ["sts:AssumeRole", "sts:TagSession"]
}]}
JSON
```

`sts:TagSession` is required, not optional — EKS tags the session with the cluster and namespace, and omitting it produces an `AccessDenied` at pod start that names neither.

`upload-api` — writes `raw/`, and reads nothing:

```sh
aws iam create-role --role-name streaming-upload-api \
  --assume-role-policy-document file:///tmp/pod-identity-trust.json

cat > /tmp/upload-api-s3.json <<JSON
{"Version":"2012-10-17","Statement":[
  {"Sid":"HeadBucketForHealthCheck",
   "Effect":"Allow","Action":"s3:ListBucket",
   "Resource":"arn:aws:s3:::${BUCKET}"},
  {"Effect":"Allow",
   "Action":["s3:PutObject","s3:AbortMultipartUpload"],
   "Resource":"arn:aws:s3:::${BUCKET}/raw/*"}
]}
JSON

aws iam put-role-policy --role-name streaming-upload-api \
  --policy-name s3-raw-write --policy-document file:///tmp/upload-api-s3.json
```

`transcode-worker` — reads `raw/`, writes `hls/`:

```sh
aws iam create-role --role-name streaming-transcode-worker \
  --assume-role-policy-document file:///tmp/pod-identity-trust.json

cat > /tmp/transcode-s3.json <<JSON
{"Version":"2012-10-17","Statement":[
  {"Effect":"Allow","Action":"s3:ListBucket",
   "Resource":"arn:aws:s3:::${BUCKET}"},
  {"Effect":"Allow","Action":"s3:GetObject",
   "Resource":"arn:aws:s3:::${BUCKET}/raw/*"},
  {"Effect":"Allow",
   "Action":["s3:PutObject","s3:AbortMultipartUpload"],
   "Resource":"arn:aws:s3:::${BUCKET}/hls/*"}
]}
JSON

aws iam put-role-policy --role-name streaming-transcode-worker \
  --policy-name s3-raw-read-hls-write --policy-document file:///tmp/transcode-s3.json
```

Associate each with its ServiceAccount. The ServiceAccounts themselves are created in [Module 11](11-workloads.md); the association can be made first and will apply to pods created later.

```sh
for SA in upload-api transcode-worker; do
  aws eks create-pod-identity-association \
    --cluster-name streaming \
    --namespace streaming \
    --service-account "$SA" \
    --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/streaming-${SA}"
done

aws eks list-pod-identity-associations --cluster-name streaming \
  --query 'associations[].{ns:namespace,sa:serviceAccount}' --output table
```

If those pods are already running, restart them — an association is only picked up at pod start:

```sh
kubectl -n streaming rollout restart deploy/upload-api 2>/dev/null || true
```

Record the values Module 11 will need:

```sh
cat <<EOF
S3_BUCKET=${BUCKET}
S3_ADDRESSING_STYLE=virtual
HLS_PUBLIC_BASE_URL=https://cdn.k8s.linkifysolutions.com
AWS_REGION=us-east-1
# and S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY must be UNSET on AWS
EOF
```

Those last three matter. boto3 checks explicit credentials *first*, so leaving `S3_ACCESS_KEY` set anywhere means Pod Identity is attached, working, and completely ignored.

---

## Verify

Upload a test object that mimics what the transcoder writes, then fetch it through CloudFront.

```sh
printf '#EXTM3U\n#EXT-X-VERSION:3\n' > /tmp/test.m3u8

aws s3 cp /tmp/test.m3u8 "s3://${BUCKET}/hls/vod/checkpoint/master.m3u8" \
  --content-type application/vnd.apple.mpegurl \
  --cache-control "public, max-age=31536000, immutable"
```

**Checkpoint 1 — it is served, with the right type and CORS:**

```sh
curl -sI -H "Origin: https://stream.k8s.linkifysolutions.com" \
  https://cdn.k8s.linkifysolutions.com/hls/vod/checkpoint/master.m3u8
```

```
HTTP/2 200
content-type: application/vnd.apple.mpegurl
content-length: 25
cache-control: public, max-age=31536000, immutable
access-control-allow-origin: https://stream.k8s.linkifysolutions.com
access-control-expose-headers: Content-Length, Content-Range, ETag, Accept-Ranges
x-cache: Miss from cloudfront
```

Four things must be true: `HTTP/2 200`; `content-type: application/vnd.apple.mpegurl` (not `binary/octet-stream`); `cache-control` echoing what you set; and `access-control-allow-origin` present.

**Checkpoint 2 — the preflight is answered:**

```sh
curl -s -o /dev/null -D- -X OPTIONS \
  -H "Origin: https://stream.k8s.linkifysolutions.com" \
  -H "Access-Control-Request-Method: GET" \
  -H "Access-Control-Request-Headers: range" \
  https://cdn.k8s.linkifysolutions.com/hls/vod/checkpoint/master.m3u8
```

```
HTTP/2 200
access-control-allow-origin: https://stream.k8s.linkifysolutions.com
access-control-allow-methods: GET, HEAD, OPTIONS
access-control-max-age: 3000
```

**Checkpoint 3 — `raw/` is not reachable:**

```sh
aws s3 cp /tmp/test.m3u8 "s3://${BUCKET}/raw/checkpoint.txt"
curl -s -o /dev/null -w '%{http_code}\n' https://cdn.k8s.linkifysolutions.com/raw/checkpoint.txt
```

```
403
```

A `200` here means the bucket policy is scoped to `*` instead of `hls/*`. Fix it before going further; this is the difference between a private bucket and a public one.

**Checkpoint 4 — the live behaviour is actually matching.** Put an object where a live manifest would go and check that it is barely cached:

```sh
aws s3 cp /tmp/test.m3u8 "s3://${BUCKET}/hls/live/checkpoint/720p/playlist.m3u8" \
  --content-type application/vnd.apple.mpegurl \
  --cache-control "no-cache, must-revalidate"

for i in 1 2 3; do
  curl -sI https://cdn.k8s.linkifysolutions.com/hls/live/checkpoint/720p/playlist.m3u8 \
    | grep -Ei '^(x-cache|age|cache-control)'
  sleep 2
done
```

```
cache-control: no-cache, must-revalidate
x-cache: Miss from cloudfront
cache-control: no-cache, must-revalidate
x-cache: Miss from cloudfront
...
```

Repeated `Miss` (or a `Hit` with `age: 0`/`age: 1`) is what you want — it proves `hls/live/*.m3u8` matched the 1-second behaviour and that the `*` wildcard does cross `/`. If you instead see `x-cache: Hit from cloudfront` with `age` climbing past 2, the path pattern did not match and the default `CachingOptimized` behaviour is serving it. **Do not proceed to Module 13 in that state** — it is the frozen-stream failure, and it will look like a transcoder bug.

Clean up:

```sh
aws s3 rm "s3://${BUCKET}/hls/vod/checkpoint/master.m3u8"
aws s3 rm "s3://${BUCKET}/hls/live/checkpoint/720p/playlist.m3u8"
aws s3 rm "s3://${BUCKET}/raw/checkpoint.txt"
```

---

## What breaks

### 1. The player shows an error, the network tab shows 200

CORS. Confirm it from the browser console first — Chrome names it explicitly:

```
Access to XMLHttpRequest at 'https://cdn.k8s.../master.m3u8' from origin
'https://stream.k8s...' has been blocked by CORS policy: No
'Access-Control-Allow-Origin' header is present on the requested resource.
```

Then reproduce it with `curl`, which is the fast loop:

```sh
curl -sI -H "Origin: https://stream.k8s.linkifysolutions.com" \
  https://cdn.k8s.linkifysolutions.com/hls/live/<stream-id>/master.m3u8 \
  | grep -i access-control
```

No output means the ResponseHeadersPolicy is not attached to the behaviour that served this path. Check both behaviours have it:

```sh
aws cloudfront get-distribution-config --id "$DIST_ID" \
  --query 'DistributionConfig.[DefaultCacheBehavior.ResponseHeadersPolicyId, CacheBehaviors.Items[].ResponseHeadersPolicyId]'
```

**If it works in Safari, that is not evidence it works.** Safari plays HLS natively through the `<video>` element, which bypasses CORS entirely. Always test in Chrome.

### 2. Viewers see "Connection hiccup — reconnecting…" while the stream is fine

CloudFront is caching live manifests. Check the age on a real live manifest during a stream:

```sh
curl -sI https://cdn.k8s.linkifysolutions.com/hls/live/<stream-id>/720p/playlist.m3u8 \
  | grep -Ei '^(x-cache|age)'
```

`age` above 2 means the request is hitting the default behaviour. Either the path pattern is wrong, or the behaviours are in the wrong order (CloudFront evaluates `CacheBehaviors` in order and the default is always last — with one custom behaviour that cannot be wrong, but it can be if someone adds a second).

While debugging, `aws cloudfront create-invalidation --distribution-id "$DIST_ID" --paths '/hls/live/*'` clears it — but that is a symptom fix. Invalidations are also billed after the first 1,000 paths per month, so do not put one in a loop.

### 3. Every object 403s through CloudFront but `aws s3 ls` works fine

The OAC is not authorised. Three things to check in order:

```sh
# a) Is the OAC actually attached to the origin?
aws cloudfront get-distribution-config --id "$DIST_ID" \
  --query 'DistributionConfig.Origins.Items[0].OriginAccessControlId'

# b) Does the bucket policy name THIS distribution?
aws s3api get-bucket-policy --bucket "$BUCKET" --query Policy --output text | python3 -m json.tool

# c) Is the origin domain the REST endpoint, not a website endpoint?
aws cloudfront get-distribution-config --id "$DIST_ID" \
  --query 'DistributionConfig.Origins.Items[0].DomainName'
```

(c) catches a common one: the origin must be `<bucket>.s3.us-east-1.amazonaws.com`. If it is `<bucket>.s3-website-us-east-1.amazonaws.com`, CloudFront treats it as a custom origin, does not sign requests, and OAC does nothing.

### 4. `NoCredentialsError` or `AccessDenied` from a pod

Check the association exists and that the ServiceAccount name matches exactly:

```sh
aws eks list-pod-identity-associations --cluster-name streaming \
  --query 'associations[].{ns:namespace,sa:serviceAccount,role:roleArn}' --output table

kubectl -n streaming get pod <pod> -o jsonpath='{.spec.serviceAccountName}{"\n"}'
```

Then check what identity the pod actually has:

```sh
kubectl -n streaming exec <pod> -- env | grep -i aws
```

You should see `AWS_CONTAINER_CREDENTIALS_FULL_URI` and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`. If those are absent, the association was created after the pod started — `kubectl rollout restart` the Deployment. If `S3_ACCESS_KEY` is present, that is your problem instead: boto3 prefers explicit credentials and never consults Pod Identity.

### 5. `upload-api`'s readiness probe fails but uploads work

`head_bucket` needs `s3:ListBucket` on the **bucket** ARN. Confirm:

```sh
aws iam get-role-policy --role-name streaming-upload-api --policy-name s3-raw-write \
  --query 'PolicyDocument.Statement[?Action==`s3:ListBucket`]'
```

An empty result is the bug. The resource must be `arn:aws:s3:::<bucket>` with no `/*`.

### 6. The bucket is larger than the sum of its objects

Incomplete multipart uploads.

```sh
aws s3api list-multipart-uploads --bucket "$BUCKET"
```

The lifecycle rule from step 2 cleans these up within a day. If the rule is missing, they accumulate silently and forever. Verify the rule is present rather than assuming:

```sh
aws s3api get-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --query "Rules[?ID=='abort-incomplete-mpu']"
```

### 7. `cdn.k8s.linkifysolutions.com` does not resolve, or serves the wrong certificate

```sh
dig +short cdn.k8s.linkifysolutions.com
curl -sv https://cdn.k8s.linkifysolutions.com/ 2>&1 | grep -E 'subject|issuer|SSL'
```

A `SSL: no alternative certificate subject name matches` means the distribution's `Aliases` does not include `cdn.k8s.linkifysolutions.com`, or the ACM certificate does not cover it. Both are visible in `get-distribution-config`. Remember the certificate must be in **us-east-1**; a certificate in another region cannot be attached to a distribution at all, and the API error for that is unhelpfully generic.

---

Next: [Module 10 — Kafka with Strimzi](10-kafka-strimzi.md).
