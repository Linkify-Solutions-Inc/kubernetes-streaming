# Module 12 — Exposing it to the world

Prerequisites: [Module 5](05-dns-and-certificates.md) (Route 53 zone, ACM certificate, AWS Load Balancer Controller, ExternalDNS), [Module 11](11-workloads.md) (all pods Running).

---

## What you're building

Two public entry points, built with two different mechanisms:

- `https://stream.k8s.linkifysolutions.com` — the web UI, on an Application Load Balancer, TLS terminated with the ACM certificate from Module 5, HTTP redirected to HTTPS, DNS created automatically by ExternalDNS.
- `rtmp://rtmp.k8s.linkifysolutions.com:1935` — RTMP ingest, on a Network Load Balancer doing raw TCP passthrough to MediaMTX.

Nothing else becomes public. `upload-api`, `ingest-webhook` and `analytics-worker` stay on ClusterIP, deliberately.

---

## Why it works this way

### Two traffic shapes, two mechanisms

An Ingress is an HTTP object. Its whole vocabulary — hosts, paths, TLS SNI, header-based routing — is L7. That's what makes it powerful: because the load balancer parses every request, many hostnames and many paths can share **one** load balancer, and you pay for one. `stream.k8s...` and any future `admin.k8s...` land on the same ALB with different rules.

RTMP is not HTTP. It's a raw TCP stream carrying a binary handshake and then an endless flow of FLV-tagged media, opened once when OBS hits Start Streaming and held for the entire broadcast. There is no request boundary to route on, no `Host` header, no path. An Ingress controller has nothing to work with, and there is no annotation that makes it work — this is a shape mismatch, not a missing feature.

So RTMP gets a `type: LoadBalancer` Service, which is the L4 primitive: the load balancer forwards bytes on a port and does not look inside. On AWS the Load Balancer Controller renders that as a Network Load Balancer.

That's two load balancers and roughly $33/month. It is not avoidable at reasonable cost or risk — see the ingress-nginx discussion below for the one alternative and why it's rejected.

The per-stream transcode workload needs no inbound load balancing at all. It pulls from MediaMTX and pushes to S3; nothing ever connects *to* it. (That stays true in [Module 14](14-keda-scaledjobs.md) when it becomes one Job per stream.)

### ALB, not ingress-nginx

This one is settled by an external fact before you even get to the technical comparison: **upstream Ingress NGINX was retired in March 2026.** No further releases, no bug fixes, no security patches. The EKS 1.35 release notes call it out and tell customers to migrate off. Standing up a retired, unpatched, internet-facing proxy on a brand-new cluster is not defensible, whatever its merits used to be.

Even setting that aside, the ALB wins on this specific application:

- **ACM integration is one annotation.** TLS terminates at the ALB. There is no certificate in the cluster, no cert-manager, no renewal, no reload.
- **There are no controller pods in the data path.** The AWS Load Balancer Controller is a control-plane component: it watches Kubernetes objects and reconciles AWS resources. The data plane is AWS-managed and scales without you. On a 4 GiB node, not running two nginx replicas is real memory.
- **The ALB has no request body size limit.** ingress-nginx defaults `proxy-body-size` to **1m** and would reject a video upload with a 413 that looks exactly like the application's own `MAX_UPLOAD_BYTES` check — a genuinely nasty half-hour of debugging.

The honest cost of choosing the ALB: about $16.43/month base, and you give up the option of collapsing HTTP and RTMP onto one load balancer. ingress-nginx can proxy raw TCP on 1935 through its `tcp-services` ConfigMap, which would save the NLB's ~$16/month. Reject that saving: it puts a retired proxy in the RTMP path and couples MediaMTX's availability to nginx rollouts, so every nginx restart drops every live stream.

### `group.name` — set it now or pay later

```yaml
alb.ingress.kubernetes.io/group.name: streaming
```

Without this annotation, **every** Ingress object provisions its own ALB, at ~$16/month each. With it, all Ingresses sharing the group name land on one. It costs nothing today with a single Ingress, and it is not retrofittable without a DNS change, because adding it later moves the rules to a different ALB with a different hostname.

### `ingest-webhook` must never be in the Ingress

Read what those handlers actually do (`services/ingest-webhook/main.py`). `/hooks/publish` and `/hooks/unpublish` take a single `path` form field and perform **zero authentication**. That is deliberate and the code says so: by the time they run, `/hooks/auth` has already accepted the stream key, so there's nothing to re-validate. The assumption baked into that comment is that only MediaMTX can reach them.

Break that assumption and: anyone who can POST to `/hooks/publish` can forge stream-start events, create phantom `streams` rows, and trigger transcode work — that is, spend your money and fill your bucket. And `/hooks/auth` is worse in a quieter way: it's an oracle. POST a candidate `path` with `action: "publish"` and the status code tells you whether that stream key exists. 401 means no, 200 means yes. Exposed publicly, that turns a 32-hex-character secret into something enumerable by anyone patient enough.

Three layers, use all of them:

1. **No Ingress rule.** ClusterIP only. The Ingress in this module has exactly one backend and it is `web`.
2. **A NetworkPolicy** restricting ingress on port 8000 to pods labelled `app: mediamtx`. It's already in `k8s/apps/base/ingest-webhook/networkpolicy.yaml`. **The gotcha:** the AWS VPC CNI does not enforce NetworkPolicy unless you enabled it (`enableNetworkPolicy: "true"` on the vpc-cni add-on, Module 4). Without that, the manifest applies cleanly, reports no error, and does nothing whatsoever. Verify it with a real request from a pod outside the selector; do not assume.
3. **Never add a path rule "just for debugging."** With `group.name` set, it lands on the same public ALB. Use `kubectl port-forward`.

The same reasoning applies to `upload-api`, which mints stream keys via `POST /streamers` with no authentication at all. `web` proxies to it server-side over ClusterIP. Keep it that way.

### The 5 GiB upload path has three real collisions

`MAX_UPLOAD_BYTES` defaults to 5 GiB in the code. Putting the current upload path behind an ALB collides with that in three separate places, and only one of them is fixed by the load balancer annotation people reach for first.

**Collision 1 — the ALB's default idle timeout is 60 seconds.** Exceed it and the ALB closes the connection mid-upload; the browser shows a connection reset. Fixed with:

```yaml
alb.ingress.kubernetes.io/load-balancer-attributes: >-
  idle_timeout.timeout_seconds=1800
```

Understand what that timeout actually measures, though: **idle** time, meaning a period with no bytes in either direction. A steadily progressing upload never goes idle, so a healthy 5 GiB upload over a fast link may not have tripped this at all. Raise it anyway — a stalled client on hotel wifi will trip it — but do not stop here, because it is not the binding constraint.

**Collision 2 — `web`'s httpx timeout is a total timeout, not an idle one.** This is the one that actually bites:

```python
with httpx.Client(base_url=UPLOAD_API_URL, timeout=60) as client:
```

`timeout=60` in httpx sets connect, read, write and pool timeouts to 60 seconds each. The write timeout is per-chunk rather than total, but the read timeout applies to waiting for `upload-api`'s response — and `upload-api` doesn't respond until it has finished streaming the whole file into S3. So `web` gives up 60 seconds after it finishes sending, regardless of how much progress everything made. **The ALB annotation alone does not fix the upload**, and if you only change the annotation you will conclude the annotation didn't work.

The fix is explicit per-operation timeouts:

```python
timeout = httpx.Timeout(connect=10.0, read=1800.0, write=1800.0, pool=10.0)
with httpx.Client(base_url=UPLOAD_API_URL, timeout=timeout) as client:
```

**Collision 3 — `web` buffers the entire file in memory.**

```python
files={"file": (file.filename, await file.read(), file.content_type)}
```

`await file.read()` materialises the whole body as a single `bytes` object in the `web` pod. A 5 GiB upload wants 5 GiB of RSS on a `t3.medium` with 4 GiB total. The pod is OOMKilled, and it presents to the user as a browser connection reset with no error page and nothing obviously wrong in the logs — the process is gone. This is not tunable. There is no memory limit you can afford that makes a 5 GiB in-memory buffer safe, and two concurrent uploads double it.

Stream the forward instead of reading it, and pass the file object through:

```python
files={"file": (file.filename, file.file, file.content_type)}
```

Then note that `upload-api` still spools to disk — Starlette's `UploadFile` rolls over to a temporary file past ~1 MB, so the full upload lands in that pod's `/tmp` before boto3 reads it back out. That's why Module 11 gave it `ephemeral-storage` requests and an `emptyDir` at `/tmp` with `TMPDIR` set.

**What to actually do in this module:**

1. Set `idle_timeout.timeout_seconds=1800` on the ALB. (In the manifest already.)
2. Land the two `web` changes from Module 0: real `httpx.Timeout`, and stream instead of `await file.read()`.
3. **Lower `MAX_UPLOAD_BYTES` to 2 GiB** in `overlays/prod/config.env`. 5 GiB was a placeholder chosen on a twelve-core box with 64 GiB of RAM. On a `t3.medium` node pool, 2 GiB is what the ephemeral storage requests and the transcode scratch space are sized for. Raise it deliberately later, along with those numbers.

**And the right long-term fix, which is out of scope here:** have the browser upload directly to S3 with a presigned PUT. `upload-api` grows a `POST /videos/presign` returning a presigned URL and an object key; the browser PUTs straight to S3; a second small call records the row and emits the Kafka event. That removes 5 GiB from *both* pods, removes the ALB from the data path entirely, removes the ephemeral-storage requirement, and makes every timeout question above disappear. It costs a bucket CORS rule allowing PUT from `stream.k8s.linkifysolutions.com` and maybe sixty lines of Python and JavaScript. It is what the architecture wants. It is not in this course because it changes the upload flow rather than deploying it, and this module is about deploying what exists — but write it on the backlog before you leave.

### RTMP via NLB, and the long-lived TCP failure class

SPEC.md flags this as the piece to be careful with: *"watch for the long-lived-TCP-through-managed-LB failure class documented in the research pass before assuming it just works."* Here is that class, concretely. Every one of these is a setting whose default was chosen for HTTP, where connections last milliseconds.

**`target-type: ip`.** Traffic goes NLB → pod IP directly. The alternative, `instance` mode, lands on a NodePort and gets kube-proxy DNAT'd — possibly to a pod on a *different* node, adding a cross-AZ hop and its data charge. With the VPC CNI giving pods real VPC addresses, `ip` is free and strictly better.

**Cross-zone load balancing: on.** It costs $0.01/GB for traffic crossing an AZ boundary — at roughly 4 GB/hour per stream that's about four cents an hour. The alternative is worse than it sounds: with one MediaMTX pod in one AZ and cross-zone off, the NLB's nodes in the other AZs have zero healthy targets and get pulled from its DNS. That works until a TTL boundary or a pod reschedule hands OBS an address with no path to a target, producing an intermittent "connection refused" that fixes itself in sixty seconds and cannot be reproduced. Pay the pennies.

**The TCP idle timeout is 350 seconds.** This is the number to memorise. It used to be entirely fixed; AWS shipped `tcp.idle_timeout.seconds` as a **listener** attribute (not a target-group attribute) in September 2024, settable from 60 to 6000 seconds for TCP listeners. TLS listeners are still pinned at 350 and cannot be changed; you're using a plain TCP listener, so you can raise it.

The Load Balancer Controller does not expose it as an annotation, so you set it out-of-band after the NLB exists (commands in "Do it" below).

How much does it matter? Less than it first appears, and knowing why is the point. An *actively publishing* stream is never idle for 350 seconds — OBS sends video continuously. The exposure is three specific cases: a connected-but-silent scene (still frame, no audio) where MediaMTX's large `writeQueueSize` means very low packet rates; a streamer who pauses; and a network partition where neither side sends anything. The failure is what makes it worth fixing: **the NLB silently drops the flow without sending a TCP RST.** OBS keeps a half-open socket, believes it is still streaming, and displays no error at all. One CLI command removes the entire scenario.

Belt and braces, the prod overlay also enables kernel TCP keepalives on the MediaMTX pod (`net.ipv4.tcp_keepalive_time=120` and friends), so the kernel sends probes well inside any timeout. Those three sysctls are in the safe set — no kubelet flag, no node configuration.

**Deregistration delay is what kills live streams during a rollout.** When a MediaMTX pod is deregistered — a rollout, a node drain, a Karpenter action — the NLB stops sending *new* connections and starts a drain timer, 300 seconds by default. Two attributes decide whether existing streams survive it:

- **`deregistration_delay.connection_termination.enabled=false`.** For TCP target groups this is already the default, meaning existing connections survive past the drain timeout. If it were `true`, every in-flight RTMP publish would be forcibly RST'd the moment the timer expires. Set it **explicitly** in the annotation so a future controller default change cannot silently break you.
- **`deregistration_delay.timeout_seconds=900`.** With connection termination off, this only bounds how long the target lingers in `draining` — but a longer value means the target group isn't churning while a stream finishes.

**`target_health_state.unhealthy.connection_termination.enabled=false`** is the third one and the sneaky one. By default, when a target goes **unhealthy**, the NLB terminates its existing connections. So a transient health check blip — MediaMTX's API taking longer than the check timeout because a transcode saturated the node — would kill every live stream on a pod that is, in fact, fine. Set it to `false` so an unhealthy verdict only stops *new* connections.

**Health check the API, not the port.** Do not use a bare TCP health check on 1935. A TCP-connect check passes the instant the socket is listening — which for MediaMTX is before it has read its config — and keeps passing when MediaMTX is wedged. Point the check at the HTTP API you enabled in Module 11:

```
protocol: HTTP, port: 9997, path: /v3/config/global/get, interval: 10
healthy-threshold: 2, unhealthy-threshold: 3
```

`interval: 10` is the NLB minimum. `unhealthy-threshold: 3` (30 seconds) rather than the default 2 avoids flapping under transcode-induced CPU pressure.

**Do not enable PROXY protocol.** Take this as a firm position, not a preference. **MediaMTX does not parse PROXY protocol on its RTMP listener.** Enable it (`service.beta.kubernetes.io/aws-load-balancer-proxy-protocol: "*"`) and the NLB prepends a 28-plus-byte binary header to every connection; MediaMTX reads those bytes as the beginning of the RTMP handshake; the handshake fails; **every single publish is rejected** with an opaque error. That is a total outage, not a degradation, and it is easy to reach for because "preserve the client IP" is a reasonable thing to want.

Use **`preserve_client_ip.enabled=true`** instead. It rewrites the source IP at the network layer, so MediaMTX sees the real client with no protocol change. `/hooks/auth` already receives an `ip` field it currently ignores (`MediaMTXAuthRequest.ip`), so nothing depends on this today — but it's what makes per-IP rate limiting possible later, and it makes the logs useful now.

One consequence comes with it, and it is worth writing on the manifest: **with `preserve_client_ip` on IP targets, a pod cannot reach the NLB from inside the VPC.** The hairpin fails because source and destination are both in-VPC and the NLB cannot NAT it. Check whether that affects you: `transcode-worker` dials `MEDIAMTX_RTMP_URL`, which is the `mediamtx` ClusterIP Service. No hairpin. But if anyone ever "simplifies" that variable to point at `rtmp.k8s.linkifysolutions.com` — which looks tidier and reads like the obvious thing — every transcode fails to connect and nothing in the logs explains why. The comment is in `k8s/apps/base/config.env`; leave it there.

**Never let ArgoCD prune this Service.** Deleting and recreating it creates a **brand new NLB with a brand new DNS name**. ExternalDNS updates the Route 53 record, and then you wait out the TTL while every OBS client in the world has the old hostname cached in a config file. The `argocd.argoproj.io/sync-options: Prune=false` annotation is already on the manifest.

---

## Do it

### 1. Put the two resources back

Module 11 had you comment these out. They are in two different kustomize roots, so uncomment them in two files.

The ALB Ingress, in the prod overlay:

```yaml
# k8s/apps/overlays/prod/kustomization.yaml
resources:
  - ../../base
  - ingress-alb.yaml
```

The RTMP NLB, in the MediaMTX root:

```yaml
# k8s/apps/mediamtx/overlays/aws/kustomization.yaml
resources:
  - deployment.yaml
  - service.yaml
  - pdb.yaml
  - service-rtmp-nlb.yaml
```

**That is where the NLB Service belongs, and it is not filing.** A `type: LoadBalancer` Service's target group is populated from the endpoints of the pods its selector matches — so the Service and the Deployment it selects have to be owned by the same thing. [Module 15](15-argocd-gitops.md) gives MediaMTX its own manually-synced ArgoCD Application precisely so a routine deploy cannot restart it mid-broadcast; if the NLB Service sat in the auto-synced prod overlay instead, the fastest Application would still be free to delete and recreate the load balancer under a live stream, and you would get a new NLB hostname out of it.

### 2. Fill in the certificate ARN

```sh
CERT_ARN=$(aws acm list-certificates \
  --query "CertificateSummaryList[?DomainName=='*.k8s.linkifysolutions.com'].CertificateArn" \
  --output text)
echo "$CERT_ARN"
sed -i.bak "s|<acm-cert-arn>|${CERT_ARN}|" k8s/apps/overlays/prod/ingress-alb.yaml
rm k8s/apps/overlays/prod/ingress-alb.yaml.bak
```

If `$CERT_ARN` is empty, the Module 5 certificate isn't `ISSUED` yet — `aws acm describe-certificate --certificate-arn <arn>` and check the DNS validation record landed in the zone.

### 3. Confirm the controller is running

```sh
kubectl get deploy -n kube-system aws-load-balancer-controller
```

```
NAME                           READY   UP-TO-DATE   AVAILABLE   AGE
aws-load-balancer-controller   2/2     2            2           3d
```

Nothing below produces an AWS resource without it. If a load balancer never appears, this controller's logs are where the answer is, essentially always.

### 4. Apply

Two roots, two applies:

```sh
kubectl apply -k k8s/apps/overlays/prod
kubectl apply -k k8s/apps/mediamtx/overlays/aws
```

```
ingress.networking.k8s.io/streaming created
...
service/mediamtx-rtmp created
...
```

### 5. Watch the load balancers appear

```sh
kubectl get ingress,svc -n streaming -w
```

Give it 2–4 minutes. The ALB takes longer than the NLB to become active:

```
NAME                                CLASS   HOSTS                             ADDRESS                                                    PORTS     AGE
ingress.networking.k8s.io/streaming alb     stream.k8s.linkifysolutions.com   k8s-streaming-1a2b3c4d5e-123456789.us-east-1.elb.amazonaws.com   80, 443   3m

NAME                    TYPE           CLUSTER-IP      EXTERNAL-IP                                                              PORT(S)
service/mediamtx-rtmp   LoadBalancer   172.20.84.191   k8s-streaming-mediamtx-abcdef0123-9876543210abcdef.elb.us-east-1.amazonaws.com   1935:31842/TCP
```

`EXTERNAL-IP` stuck on `<pending>` for more than five minutes means the controller could not create it — go read its logs (`kubectl logs -n kube-system deploy/aws-load-balancer-controller`), and check the subnet tags from Module 2. Public subnets need `kubernetes.io/role/elb=1`.

### 6. Raise the NLB listener's idle timeout

This is the out-of-band step the controller can't do for you:

```sh
LB_ARN=$(aws elbv2 describe-load-balancers \
  --query "LoadBalancers[?contains(DNSName,'streaming-mediamtx')].LoadBalancerArn" \
  --output text)

LISTENER_ARN=$(aws elbv2 describe-listeners --load-balancer-arn "$LB_ARN" \
  --query 'Listeners[?Port==`1935`].ListenerArn' --output text)

aws elbv2 modify-listener-attributes --listener-arn "$LISTENER_ARN" \
  --attributes Key=tcp.idle_timeout.seconds,Value=6000
```

```json
{
    "Attributes": [
        {
            "Key": "tcp.idle_timeout.seconds",
            "Value": "6000"
        }
    ]
}
```

Write this in the runbook. It does not live in git, it does not survive the NLB being recreated, and nothing will tell you it's missing until a stream silently half-opens.

### 7. Confirm ExternalDNS wrote the records

```sh
ZONE_ID=$(aws route53 list-hosted-zones-by-name \
  --dns-name k8s.linkifysolutions.com --query 'HostedZones[0].Id' --output text)

aws route53 list-resource-record-sets --hosted-zone-id "$ZONE_ID" \
  --query "ResourceRecordSets[?Type=='A'].[Name,AliasTarget.DNSName]" --output table
```

```
------------------------------------------------------------------------------------------------
|  stream.k8s.linkifysolutions.com.  |  k8s-streaming-1a2b3c4d5e-123456789.us-east-1.elb.amazonaws.com.  |
|  rtmp.k8s.linkifysolutions.com.    |  k8s-streaming-mediamtx-abcdef0123-...elb.us-east-1.amazonaws.com. |
------------------------------------------------------------------------------------------------
```

Missing records: `kubectl logs -n kube-system deploy/external-dns`. The usual cause is the IAM policy scoping `route53:ChangeResourceRecordSets` to the wrong hosted zone id.

---

## Verify

### HTTPS, with a valid certificate

```sh
curl -I https://stream.k8s.linkifysolutions.com
```

```
HTTP/2 200
content-type: text/html; charset=utf-8
cache-control: no-store
server: awselb/2.0
```

`curl` validating the chain without `-k` *is* the certificate check — it fails loudly otherwise. To see it explicitly:

```sh
curl -sv https://stream.k8s.linkifysolutions.com -o /dev/null 2>&1 | grep -E 'subject:|issuer:|expire'
```

```
*  subject: CN=*.k8s.linkifysolutions.com
*  start date: Jul 12 00:00:00 2026 GMT
*  expire date: Aug 10 23:59:59 2027 GMT
*  issuer: C=US; O=Amazon; CN=Amazon RSA 2048 M02
```

And the redirect:

```sh
curl -sI http://stream.k8s.linkifysolutions.com | head -3
```

```
HTTP/1.1 301 Moved Permanently
Location: https://stream.k8s.linkifysolutions.com:443/
```

### RTMP port reachable

```sh
nc -vz rtmp.k8s.linkifysolutions.com 1935
```

```
Connection to rtmp.k8s.linkifysolutions.com port 1935 [tcp/macromedia-fcs] succeeded!
```

That proves DNS resolves, the NLB has at least one healthy target, and the security groups allow 1935. It does **not** prove RTMP works — a TCP connect says nothing about the handshake. That's Module 13's job.

Check the target group agrees:

```sh
TG_ARN=$(aws elbv2 describe-target-groups --load-balancer-arn "$LB_ARN" \
  --query 'TargetGroups[0].TargetGroupArn' --output text)
aws elbv2 describe-target-health --target-group-arn "$TG_ARN" \
  --query 'TargetHealthDescriptions[].[Target.Id,Target.Port,TargetHealth.State]' --output table
```

```
--------------------------------
|  10.0.13.204  |  1935  |  healthy  |
--------------------------------
```

One target, healthy. One, because MediaMTX runs one replica by design.

### The negative check — the private services are private

This matters as much as the positive ones:

```sh
curl -s -o /dev/null -w '%{http_code}\n' \
  https://stream.k8s.linkifysolutions.com/hooks/auth -X POST
curl -s -o /dev/null -w '%{http_code}\n' \
  https://stream.k8s.linkifysolutions.com/streamers -X POST
```

```
404
404
```

404 from `web`, which has no such routes. If either returns something else, an Ingress rule is routing to a service that should not be public.

If you enabled VPC CNI network policy support, prove the NetworkPolicy too:

```sh
# Run it in `default`, not `streaming`: the streaming namespace enforces the
# `restricted` Pod Security profile, and a bare `kubectl run` does not set
# the securityContext fields that profile demands.
kubectl run probe -n default --rm -it --restart=Never \
  --image=curlimages/curl:8.11.1 --command -- \
  curl -s -m 5 -o /dev/null -w '%{http_code}\n' \
  http://ingest-webhook.streaming.svc.cluster.local:8000/livez
```

```
command terminated with exit code 28
```

A timeout is the pass. A `200` means the policy is not being enforced — go back to the vpc-cni add-on configuration, because right now that manifest is decoration.

---

## What breaks

**`EXTERNAL-IP: <pending>` forever, or no ALB address.**

```sh
kubectl logs -n kube-system deploy/aws-load-balancer-controller --tail=50
```

Nine times in ten it's subnet tags or IAM. Public subnets need `kubernetes.io/role/elb=1`; every subnet needs `kubernetes.io/cluster/streaming` (`shared` or `owned`). `AccessDenied` in the log means the controller's IAM policy is missing an action — reapply the policy document from Module 5 rather than adding permissions one at a time.

**`curl` returns 503 from the ALB.**

```
HTTP/2 503
server: awselb/2.0
```

The ALB is up and the target group is empty or unhealthy. `aws elbv2 describe-target-health` will say `unhealthy` with a reason. The health check path is `/livez`, so this usually means Module 0's `/livez` alias didn't land in the running `web` image and the ALB is getting a 404. `kubectl exec deploy/web -- python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8000/livez').read())"` settles it in one command.

**Certificate error in the browser, `curl` says `SSL: no alternative certificate subject name matches`.** The ACM certificate covers `*.k8s.linkifysolutions.com` but the ALB is serving its default certificate — the `certificate-arn` annotation is missing, still contains the literal `<acm-cert-arn>`, or points at a certificate in a different region. ACM certificates for an ALB must be in the ALB's region (`us-east-1` here); the CloudFront one from Module 9 also happens to be in `us-east-1`, which is why one certificate serves both.

**`nc` says `Connection refused`.** DNS resolved but nothing accepted. Check `dig +short rtmp.k8s.linkifysolutions.com` returns the NLB name, then check target health. `unhealthy` with `Health checks failed` means the health check can't reach 9997 — either `api: yes` isn't in the running ConfigMap (`kubectl exec deploy/mediamtx -- wget -qO- localhost:9997/v3/paths/list` from inside proves it either way) or the node security group doesn't allow 9997 from the NLB.

**`nc` hangs with no answer at all.** That's a security group dropping the packet rather than rejecting it. The Load Balancer Controller manages the node security group rules for `target-type: ip` automatically; if you're also managing security groups in Terraform, you're fighting it and Terraform is winning.

**Uploads fail at exactly 60 seconds.** The `httpx` timeout, not the ALB. Confirm by checking whether the ALB annotation actually made it to AWS:

```sh
ALB_ARN=$(aws elbv2 describe-load-balancers \
  --query "LoadBalancers[?Type=='application'].LoadBalancerArn" --output text)
aws elbv2 describe-load-balancer-attributes --load-balancer-arn "$ALB_ARN" \
  --query "Attributes[?Key=='idle_timeout.timeout_seconds']"
```

If that says 1800 and uploads still die at 60 seconds, it is Collision 2 and the fix is in `web/main.py`.

**Uploads kill the `web` pod.**

```sh
kubectl get pods -n streaming -l app=web
kubectl describe pod -n streaming <pod> | grep -i -A2 'last state'
```

```
    Last State:     Terminated
      Reason:       OOMKilled
      Exit Code:    137
```

That is Collision 3, `await file.read()`. Do not raise the memory limit — there is no limit you can afford.

**A live stream dies every time you deploy.** Check `deregistration_delay.connection_termination.enabled` on the target group. And remember that in Modules 11–13 `transcode-worker` is still a single long-running Deployment, so a rollout of *it* also kills every in-flight transcode regardless of what the NLB does. [Module 14](14-keda-scaledjobs.md) is what fixes that half.

---

Next: [Module 13 — Your first stream](13-first-stream.md). This is the milestone.
