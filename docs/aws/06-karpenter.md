# Module 6 — Karpenter

*Part II — Platform. Previous: [Module 5](05-dns-and-certificates.md). Next: [Module 7](07-secrets.md).*

---

## What you're building

Node autoscaling. By the end of this module, a pod that cannot be scheduled causes a correctly-sized EC2 instance to appear within about a minute, and when that pod goes away the instance is terminated. Two node pools: one on **on-demand** capacity for live transcoding, one on **spot** for VOD.

The last thing you do is a deliberately fake test — a dummy Deployment that asks for 3 CPUs and does nothing with them. Read the next section to understand why that is the right way to finish this module rather than a cop-out.

---

## Why it works this way

### What Karpenter actually does

Karpenter is a controller with one job. It watches for **pods that the scheduler could not place** — pods sitting in `Pending` with `Insufficient cpu` or `Insufficient memory` — reads what those pods asked for, and launches an EC2 instance shaped to fit them. When the node later has no non-DaemonSet pods left, it terminates it.

That is the whole loop:

```
pod Pending  ->  Karpenter reads its requests, affinities, tolerations
             ->  picks an instance type from the allowed set
             ->  launches it, node joins, kubelet reports Ready
             ->  scheduler places the pod
             ...
             ->  pod finishes, node is empty
             ->  consolidateAfter elapses, node terminated
```

The important word is **requests**. Karpenter does not measure CPU usage; it reads `resources.requests` off the pod spec and does bin-packing arithmetic. A pod that requests `100m` and burns four cores gets no help from Karpenter at all. A pod that requests `2500m` and sleeps gets a node. **Your resource requests are the input to this system — if they are wrong, everything downstream is wrong.**

If you have met the **Cluster Autoscaler** before, the difference is worth naming. Cluster Autoscaler works on Auto Scaling Groups: you predefine node groups with fixed instance types, and it increments the desired count. If your groups are `m5.large` and a pod wants 8 CPUs, it cannot help you. Karpenter has no groups — it evaluates the actual pending pod against the whole EC2 catalogue (filtered by your constraints) and launches whatever fits best. That is why this module defines *shapes* (`c` or `m` family, 4 or 8 vCPU, generation > 5) rather than instance types.

### Karpenter scales nodes. It does not scale pods.

State this plainly, because it is the thing that confuses everyone at this point in the course:

**Karpenter creates nodes for pods that already exist. Something else has to create the pods.**

Today, `transcode-worker` is a long-running Deployment with `replicas: 1` that consumes Kafka in a `while True` loop. One replica, forever, no matter how many streams are running. So after installing Karpenter, the honest description of what happens when three people start streaming is: **nothing**. One pod exists, one pod is scheduled, no new pod becomes `Pending`, Karpenter has nothing to react to.

The pod half of the answer is **KEDA**, in [Module 14](14-keda-scaledjobs.md). KEDA watches Kafka topic lag and creates one `Job` per message — one pod per stream. Each of those pods is `Pending` until Karpenter provisions a node. That is when this module's work starts paying for itself.

```
Kafka message  ->  KEDA creates a Job  ->  pod is Pending
                                       ->  Karpenter launches a node
                                       ->  pod runs, stream transcodes
                                       ->  Job completes, node empties, node dies
```

**Karpenter is the node half. KEDA is the pod half. Neither does anything useful alone.**

Which is why this module ends with a dummy Deployment. There is no real workload that can exercise the node pools yet, and pretending otherwise would mean either skipping verification or waiting eight modules to find out whether the IAM was right. A fake pod with a real resource request exercises exactly the same code path as a real transcode job — pending pod in, node out — and tells you now, while the configuration is fresh, whether it works.

If you would like the interim behaviour before KEDA lands: run `transcode-worker` as a `Deployment` with `replicas: 1`, `requests.cpu: 2500m`, and the live pool's toleration. Karpenter provisions exactly one node and concurrency is bounded by that node's cores — the same constraint the dev box had, on rented hardware. That is a workable stopgap, but it is not elasticity.

### Instance selection for a ~2.3-core ffmpeg job

[SPEC.md](../../SPEC.md) records the measurement: one live transcode — three renditions, `veryfast` preset — costs **~2.3 cores and ~1 GB of RAM**. Request `2500m` CPU and `2Gi` memory, leaving headroom for the boto3 upload threads and the watchdog.

Against that number, two node shapes make sense:

| Node | vCPU | Live jobs at 2500m | Trade-off |
|---|---|---|---|
| `c6i.xlarge` | 4 | 1 (≈1.2 vCPU left for kubelet + DaemonSets) | Perfect isolation, some waste |
| `c6i.2xlarge` | 8 | 3 (7.5 vCPU requested) | Much better $/stream, three streams share one node's memory bandwidth and EBS throughput |

Rather than choosing, constrain the *shape* and let Karpenter pick per launch:

- **Compute-optimized (`c`) families.** x264 is CPU-bound with a small working set — it wants clock speed, not memory. The `c` families give the best GHz per dollar. `m` families are included as a fallback so a thin `c` pool never leaves a job unschedulable.
- **Generation > 5.** Excludes `c4`/`m4` and older, which are slower per core, on worse networking, and frequently no cheaper.
- **4 or 8 vCPU** for live (16 also allowed on VOD, where jobs are larger). Anything smaller cannot fit one job; anything larger for live means one interruption takes out three streams instead of one.
- **`amd64` only.** Graviton is genuinely better value for ffmpeg, but CI publishes `linux/amd64` images only. Allowing `arm64` here would produce nodes your pods cannot run on and an `exec format error` that looks like a corrupt image. If you later add a multi-arch build, revisit this line — not before.

### Spot for VOD, on-demand for live

Spot instances are roughly 65% cheaper and can be reclaimed with **two minutes' notice**. Whether that is acceptable depends entirely on what the pod is doing.

**For live, two minutes is not enough, and the failure is not graceful.** Trace it through:

1. Karpenter gets the interruption notice, cordons the node, drains it. The transcode pod receives SIGTERM.
2. A replacement node must be provisioned: EC2 launch + AL2023 boot + a ~1.5 GB image pull through the NAT gateway. **90–150 seconds**, best case.
3. Meanwhile ffmpeg has stopped writing segments. The live playlist stops advancing. Every viewer's hls.js exhausts its retries — roughly 22 seconds of tolerance — and shows "Lost connection to the stream".
4. Worse: `transcode-worker` holds `active_streams` in memory. The `stream.lifecycle` "started" event was consumed and its offset committed long ago. Nothing re-emits it. **The stream stays dead until the streamer stops and restarts OBS.**

The cost of avoiding that: a `c6i.xlarge` is about $0.17/hr on-demand against roughly $0.06/hr spot. **$0.11/hr, billed only while someone is actually streaming.** At twenty hours of streaming a month, $2.20. Paying $2.20/month to eliminate a class of unrecoverable viewer-visible blackouts is not a close call.

**For VOD, spot is exactly right.** `transcode_video` is a batch job. Nobody is waiting on a live playlist; an interruption costs a retry.

**But VOD-on-spot exposes a bug you should fix first.** `_claim_video` runs `UPDATE videos SET status='transcoding' WHERE id=%s AND status='uploaded'`. If the pod dies mid-transcode, the row stays at `transcoding` forever — the redelivered Kafka message no longer matches the `WHERE` clause, so it is never retried. The code already acknowledges this ("a stuck-job sweep is a separate concern"). Under on-demand it is theoretical. **Under spot it becomes routine.** Either add a claim timestamp plus a reaper, or set the VOD pool to on-demand until that exists. The manifest carries this as a comment so the next person finds it.

### Taints and tolerations — you need both

Both pools taint their nodes:

```yaml
taints:
  - key: workload
    value: transcode
    effect: NoSchedule
```

and the transcode pod carries both a matching toleration and a `nodeSelector`:

```yaml
nodeSelector:
  workload: transcode
  transcode-mode: live      # or vod
tolerations:
  - key: workload
    operator: Equal
    value: transcode
    effect: NoSchedule
```

Neither alone is sufficient, and the reason is that they do opposite jobs:

- **The taint repels.** It stops CoreDNS, the LBC, ExternalDNS and anything else from drifting onto a $0.17/hr compute node. Without it, Karpenter launches a node for your transcode pod and the scheduler helpfully fills the spare capacity with system pods — which then block consolidation when the transcode finishes, and the node never goes away.
- **The `nodeSelector` attracts.** It stops the transcode pod from being scheduled onto the general-purpose managed node group, where it would run at whatever CPU it could scrounge and never trigger a scale-up at all.

A tainted node with no selector: your pod might land on a system node. A selected node with no taint: system pods land on your expensive node. Use both.

### Ephemeral scratch disk — the `tempfile` gotcha

`transcode-worker` uses `tempfile.mkdtemp()` and `tempfile.TemporaryDirectory()`. In a container those resolve to `/tmp`, which is the container's **writable layer**, which lives on the node's root EBS volume under `/var/lib/containerd`. Kubernetes accounts for that as **`ephemeral-storage`**, and if a pod exceeds its ephemeral-storage *limit* the kubelet **evicts it immediately, with no grace period** — mid-encode, mid-stream.

Under Compose this was invisible: the dev box had a big disk and nobody was counting. On a Karpenter node, the disk is whatever you put in `blockDeviceMappings`, and the accounting is enforced.

Do the arithmetic.

**Live.** `LIVE_LIST_SIZE = 30` segments of 4 seconds, three renditions at 5000k/2800k/1400k. Per 4-second interval that is 2.5 + 1.4 + 0.7 ≈ 4.6 MB, so 30 segments ≈ **140 MB steady state**. `-hls_flags delete_segments` only removes files as they rotate out, and a stall (the watchdog case) lets them accumulate. Request **5 Gi**, limit **8 Gi** — generous, and effectively free.

**VOD.** `transcode_video` downloads the full source — up to `MAX_UPLOAD_BYTES`, which every `config.env` in this repo sets to **2 GiB** — *and* writes the full HLS output alongside it, in the same scratch dir, before either is cleaned up.

The output is the part that does not have a fixed ceiling. Its size tracks *duration*, not source size: a one-hour encode yields roughly 2.25 GB at 1080p + 1.26 GB at 720p + 0.63 GB at 480p ≈ **4.1 GB per hour**. And 2 GiB of source can be a long video — at a modest 1.5 Mbps that is about three hours, so ≈ 12 GB of output. Peak concurrent usage on that worst case is 2 GiB in + 12 GB out ≈ **14 GiB**.

Request **40 Gi**, limit **50 Gi**. That is deliberate headroom over the 14 GiB worst case rather than a tight fit, for two reasons: the duration ceiling is a bitrate assumption rather than an enforced limit, and an ephemeral-storage eviction is not a retry — it kills the encode with no grace period. Disk on a node that lives twenty minutes is the cheapest insurance on this platform.

Two consequences.

**Pods must declare `ephemeral-storage`.** If they do not, Karpenter has no idea disk is needed and will happily bin-pack four VOD jobs onto a node with a 20 GiB root volume:

```yaml
# live transcode pod — this is what k8s/apps/base/transcode/scaledjob-live.yaml
# ends up carrying in Module 14
resources:
  requests: { cpu: "2500m", memory: "2Gi", ephemeral-storage: "5Gi" }
  limits:   { memory: "2Gi", ephemeral-storage: "8Gi" }
env:
  - { name: TMPDIR, value: /scratch }
volumes:
  - name: scratch
    emptyDir: { sizeLimit: 8Gi }
volumeMounts:
  - name: scratch
    mountPath: /scratch
```

Memory `limit == request`, deliberately: that makes the pod Guaranteed QoS, so the kubelet will not evict it under node memory pressure mid-encode. The `ephemeral-storage` limit is higher than the request on purpose — it is a ceiling, not a reservation, and the `emptyDir`'s `sizeLimit` matches it.

Mount the `emptyDir` at `/scratch` and point `TMPDIR` at it. Python's `tempfile` honours `TMPDIR`, so `mkdtemp()` lands on the volume with no code change. A named mount rather than overmounting `/tmp` because (a) it makes the requirement visible in the manifest, (b) it gives `sizeLimit` enforcement with a clear eviction message instead of a mysterious kill, (c) it keeps scratch off the container overlay filesystem, which is measurably slower for the many small writes ffmpeg's HLS muxer produces, and (d) the container runs with `readOnlyRootFilesystem: true`, so a writable path has to be an explicit volume anyway.

**The node's root volume must hold all of it.** Per node: AL2023 (~3 GiB) + the transcode image with ffmpeg (~1.5 GiB) + the sum of every scheduled pod's ephemeral-storage request. A `c6i.2xlarge` running two VOD jobs at 40 Gi needs 3 + 1.5 + 80 ≈ 85 GiB, and three would need 125 GiB. The `EC2NodeClass` in this module uses **200 GiB** and stops the arithmetic from being a recurring worry. That volume would cost $16/month if it ran all month — but a node that lives twenty minutes costs about half a cent. **Node root volumes are effectively free; size them generously.**

gp3's free baseline of 125 MB/s is plenty: ffmpeg writing three renditions while the uploader reads them back runs at about 5 MB/s. Do not pay for provisioned throughput.

### Consolidation: the one line that must differ between the pools

```yaml
# live
disruption:
  consolidationPolicy: WhenEmpty
  consolidateAfter: 2m

# vod
disruption:
  consolidationPolicy: WhenEmptyOrUnderutilized
  consolidateAfter: 1m
```

`WhenEmptyOrUnderutilized` — the default — lets Karpenter **evict running pods** to repack them onto fewer or cheaper nodes. That is excellent for stateless web workloads and catastrophic for a live transcode: it is a self-inflicted spot interruption, with all the consequences described above, on a node you deliberately paid on-demand prices for.

`WhenEmpty` restricts Karpenter to removing nodes with zero non-DaemonSet pods. Scale-to-zero still works perfectly — when the stream ends, the pod exits, the node is empty, and two minutes later it is gone. It simply never touches a node that is doing work.

`consolidateAfter` is the settling delay. Without it, a node would be terminated the instant a job finished, and the next job 30 seconds later would pay the full 90-second launch penalty again. Two minutes for live (streams cluster together), one for VOD.

Belt and braces on the live pod itself, so drift and expiry cannot touch it either:

```yaml
metadata:
  annotations:
    karpenter.sh/do-not-disrupt: "true"
```

---

## Do it

`AWS_PROFILE=linkify-streaming` and `AWS_REGION=us-east-1` exported.

### 6.1 — Pick a version

```sh
helm show chart oci://public.ecr.aws/karpenter/karpenter | grep '^version:'
```

```
version: 1.13.0
```

Pin whatever that reports and use it consistently — the CloudFormation template, the CRDs and the chart must all come from the same release:

```sh
export KARPENTER_VERSION="1.13.0"
export KARPENTER_DOCS_VERSION="v1.13"     # major.minor of the above, with the 'v'
export CLUSTER_NAME=streaming
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
```

### 6.2 — Tag the subnets and security group

Karpenter finds where to launch by **tag lookup**, not by hard-coded IDs. The `EC2NodeClass` selects on `karpenter.sh/discovery: streaming`, so those tags must exist first. [Module 2](02-vpc-and-networking.md) may already have set them; this is idempotent either way.

```sh
PRIVATE_SUBNETS=$(aws ec2 describe-subnets \
  --filters "Name=tag:kubernetes.io/role/internal-elb,Values=1" \
            "Name=tag:kubernetes.io/cluster/${CLUSTER_NAME},Values=owned,shared" \
  --query 'Subnets[].SubnetId' --output text)
echo "$PRIVATE_SUBNETS"

aws ec2 create-tags --resources $PRIVATE_SUBNETS \
  --tags "Key=karpenter.sh/discovery,Value=${CLUSTER_NAME}"

CLUSTER_SG=$(aws eks describe-cluster --name "$CLUSTER_NAME" \
  --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' --output text)

aws ec2 create-tags --resources "$CLUSTER_SG" \
  --tags "Key=karpenter.sh/discovery,Value=${CLUSTER_NAME}"
```

**Private subnets only.** A transcode node has no reason to hold a public IP; it reaches S3 through the gateway endpoint and everything else through the NAT gateway.

Note what Karpenter nodes must *not* be: Karpenter has to run on nodes it does not manage, otherwise it can terminate the node it is running on and nothing is left to bring anything back. That is what the managed node group from [Module 3](03-eks-cluster.md) is for. Its own instances must never match this discovery tag.

### 6.3 — The CloudFormation stack

One template creates four things you would otherwise assemble by hand:

- **`KarpenterNodeRole-streaming`** — the IAM role assumed by the instances Karpenter launches (worker node policy, ECR read, CNI, SSM).
- **The instance profile** wrapping that role.
- **`KarpenterControllerPolicy-streaming`** — what the controller itself needs (`ec2:RunInstances`, `ec2:TerminateInstances`, `iam:PassRole`, pricing APIs, and so on).
- **An SQS interruption queue plus the EventBridge rules** that route spot interruption notices, rebalance recommendations and scheduled-maintenance events into it.

```sh
curl -fsSL -o /tmp/karpenter-cfn.yaml \
  "https://raw.githubusercontent.com/aws/karpenter-provider-aws/v${KARPENTER_VERSION}/website/content/en/${KARPENTER_DOCS_VERSION}/getting-started/getting-started-with-karpenter/cloudformation.yaml"

aws cloudformation deploy \
  --stack-name "Karpenter-${CLUSTER_NAME}" \
  --template-file /tmp/karpenter-cfn.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides "ClusterName=${CLUSTER_NAME}"
```

If that URL 404s, the docs directory for your version is named differently — browse `https://github.com/aws/karpenter-provider-aws/tree/v${KARPENTER_VERSION}/website/content/en` and use the folder you find.

**The interruption queue is not optional for this workload.** It is what delivers the two-minute spot warning so Karpenter can cordon and drain rather than having the instance vanish underneath a running pod. Without it, spot VOD jobs die with no notice at all.

### 6.4 — IRSA for the controller

Karpenter's controller uses **IRSA**, not Pod Identity — it is the second of the two exceptions from [Module 4](04-addons-and-storage.md)'s table, and the official getting-started path wires it this way.

```sh
eksctl create iamserviceaccount \
  --cluster "$CLUSTER_NAME" \
  --region us-east-1 \
  --namespace kube-system \
  --name karpenter \
  --role-name "${CLUSTER_NAME}-karpenter" \
  --attach-policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/KarpenterControllerPolicy-${CLUSTER_NAME}" \
  --approve
```

### 6.5 — Let Karpenter's nodes join the cluster

```sh
aws eks create-access-entry \
  --cluster-name "$CLUSTER_NAME" \
  --principal-arn "arn:aws:iam::${ACCOUNT_ID}:role/KarpenterNodeRole-${CLUSTER_NAME}" \
  --type EC2_LINUX
```

**This is the step people forget, and its failure mode is the worst in the module.** Without it, Karpenter launches instances that boot perfectly, run the bootstrap script, present a certificate the API server does not recognise, and are rejected. You see instances appearing and disappearing in the EC2 console while `kubectl get nodes` shows nothing and **not one Kubernetes event mentions the problem**. The pod stays `Pending`, Karpenter keeps trying, and your bill keeps moving.

The cluster uses `authenticationMode: API` (access entries, no `aws-auth` ConfigMap), which is why this is an `aws eks` call rather than a ConfigMap edit. If a tutorial tells you to edit `aws-auth`, it predates access entries.

### 6.6 — Install the controller

```sh
helm upgrade --install karpenter \
  oci://public.ecr.aws/karpenter/karpenter \
  --version "$KARPENTER_VERSION" \
  -n kube-system \
  --set "settings.clusterName=${CLUSTER_NAME}" \
  --set "settings.interruptionQueue=Karpenter-${CLUSTER_NAME}" \
  --set serviceAccount.create=false \
  --set serviceAccount.name=karpenter \
  --set controller.resources.requests.cpu=200m \
  --set controller.resources.requests.memory=512Mi \
  --set controller.resources.limits.memory=512Mi \
  --wait

kubectl -n kube-system rollout status deploy/karpenter
```

`serviceAccount.create=false` for the same reason as the LBC in [Module 5](05-dns-and-certificates.md): `eksctl` already made one carrying the IRSA annotation, and letting Helm replace it produces a controller with no AWS permissions.

The chart installs the `NodePool`, `EC2NodeClass` and `NodeClaim` CRDs. Confirm:

```sh
kubectl get crd | grep karpenter
```

```
ec2nodeclasses.karpenter.k8s.aws     2026-07-29T10:14:02Z
nodeclaims.karpenter.sh              2026-07-29T10:14:02Z
nodepools.karpenter.sh               2026-07-29T10:14:02Z
```

### 6.7 — Apply the node class and node pools

The three manifests live in `k8s/infra/karpenter/`. The `EC2NodeClass` describes *how* to build a node (AMI, IAM role, where, what disk); each `NodePool` describes *when and what shape* it is allowed to be.

```sh
kubectl apply -f k8s/infra/karpenter/ec2nodeclass.yaml
kubectl apply -f k8s/infra/karpenter/nodepool-live.yaml
kubectl apply -f k8s/infra/karpenter/nodepool-vod.yaml
```

`ec2nodeclass.yaml`, in full:

```yaml
apiVersion: karpenter.k8s.aws/v1
kind: EC2NodeClass
metadata:
  name: transcode
spec:
  amiSelectorTerms:
    - alias: al2023@latest
  role: KarpenterNodeRole-streaming
  subnetSelectorTerms:
    - tags: { karpenter.sh/discovery: streaming }
  securityGroupSelectorTerms:
    - tags: { karpenter.sh/discovery: streaming }
  blockDeviceMappings:
    - deviceName: /dev/xvda          # AL2023 root device
      ebs:
        volumeSize: 200Gi            # scratch for ffmpeg — see sizing above
        volumeType: gp3
        iops: 3000                   # free gp3 baseline
        throughput: 125              # free gp3 baseline, MB/s
        encrypted: true
        deleteOnTermination: true
  metadataOptions:
    httpEndpoint: enabled
    httpTokens: required
    httpPutResponseHopLimit: 2
  tags:
    Project: streaming
    Workload: transcode
    ManagedBy: karpenter
```

The `alias: al2023@latest` form pins the family and tracks the current EKS-optimized AL2023 release for your cluster version. Do **not** also set `spec.amiFamily` — with an alias it is derived, and setting both is rejected by the validating webhook.

`nodepool-live.yaml` — note the capacity type and the consolidation policy, which are the two lines that make it the live pool:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: transcode-live
spec:
  template:
    metadata:
      labels: { workload: transcode, transcode-mode: live }
    spec:
      nodeClassRef: { group: karpenter.k8s.aws, kind: EC2NodeClass, name: transcode }
      taints:
        - key: workload
          value: transcode
          effect: NoSchedule
      requirements:
        - { key: karpenter.sh/capacity-type,             operator: In, values: ["on-demand"] }
        - { key: karpenter.k8s.aws/instance-category,    operator: In, values: ["c", "m"] }
        - { key: karpenter.k8s.aws/instance-generation,  operator: Gt, values: ["5"] }
        - { key: karpenter.k8s.aws/instance-cpu,         operator: In, values: ["4", "8"] }
        - { key: kubernetes.io/arch,                     operator: In, values: ["amd64"] }
      expireAfter: 24h
      terminationGracePeriod: 30m
  limits:
    cpu: "32"                        # hard ceiling ≈ 12 concurrent streams
  disruption:
    consolidationPolicy: WhenEmpty   # never repack a running live transcode
    consolidateAfter: 2m
```

`limits.cpu: "32"` is a **hard ceiling on total CPU this pool may provision**, and it is your protection against a runaway loop turning into a four-figure bill. When the limit is reached, Karpenter stops launching and leaves pods `Pending` — which is a visible, cheap failure. Set it deliberately; do not remove it.

`expireAfter: 24h` recycles nodes daily so AMI security patches land without anyone remembering to do it. A pod annotated `karpenter.sh/do-not-disrupt: "true"` blocks expiry until it finishes, and `terminationGracePeriod: 30m` is the outer bound — long enough for a stream to end naturally, short enough that a wedged pod does not pin a node forever.

`nodepool-vod.yaml` differs in four places: `capacity-type` allows `["spot", "on-demand"]`, `instance-cpu` allows `16`, `limits.cpu` is `64`, and `consolidationPolicy` is `WhenEmptyOrUnderutilized` with `consolidateAfter: 1m`. Everything else is identical.

---

## Verify

The checkpoint is a dummy Deployment: one pod, 3 CPUs requested, the transcode toleration and node selector, and an image that does nothing. It asks for more CPU than any existing node has free, so it is guaranteed to be `Pending`, which is exactly the signal Karpenter reacts to.

Open a second terminal and leave this running:

```sh
kubectl get nodes -L karpenter.sh/nodepool,node.kubernetes.io/instance-type,karpenter.sh/capacity-type -w
```

In the first terminal:

```sh
kubectl apply -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: karpenter-smoke-test
spec:
  replicas: 1
  selector:
    matchLabels: { app: karpenter-smoke-test }
  template:
    metadata:
      labels: { app: karpenter-smoke-test }
    spec:
      nodeSelector:
        workload: transcode
        transcode-mode: live
      tolerations:
        - key: workload
          operator: Equal
          value: transcode
          effect: NoSchedule
      containers:
        - name: pause
          image: public.ecr.aws/eks-distro/kubernetes/pause:3.9
          resources:
            requests:
              cpu: "3"
              memory: "2Gi"
              ephemeral-storage: "5Gi"
EOF
```

**Within a second or two**, Karpenter should decide:

```sh
kubectl -n kube-system logs deploy/karpenter --tail=20
```

```
{"level":"INFO","message":"found provisionable pod(s)","Pods":"default/karpenter-smoke-test-6d9f8c4b7-x2klm","duration":"18.4ms"}
{"level":"INFO","message":"computed new nodeclaim(s) to fit pod(s)","nodeclaims":1,"pods":1}
{"level":"INFO","message":"created nodeclaim","NodePool":{"name":"transcode-live"},"NodeClaim":{"name":"transcode-live-vz8qd"},"requests":{"cpu":"3155m","memory":"2Gi","ephemeral-storage":"5Gi","pods":"6"},"instance-types":"c6i.xlarge, c6in.xlarge, c7i.xlarge, m6i.xlarge, m7i.xlarge and 4 other(s)"}
{"level":"INFO","message":"launched nodeclaim","provider-id":"aws:///us-east-1a/i-0f3ab29c7d51e8a44","instance-type":"c6i.xlarge","zone":"us-east-1a","capacity-type":"on-demand"}
```

Read that `requests` line: `3155m`, not `3000m` — Karpenter added the DaemonSet overhead (CNI, kube-proxy, Pod Identity agent) before choosing. That is the arithmetic that makes it pick the right size.

**Within about 60 seconds** the node appears in the watch terminal, and it is Ready shortly after:

```
NAME                          STATUS     ROLES    AGE   VERSION   NODEPOOL         INSTANCE-TYPE   CAPACITY-TYPE
ip-10-0-33-14.ec2.internal    Ready      <none>   62m   v1.35.0   <none>           t3.medium       <none>
ip-10-0-66-201.ec2.internal   Ready      <none>   62m   v1.35.0   <none>           t3.medium       <none>
ip-10-0-41-88.ec2.internal    NotReady   <none>   8s    v1.35.0   transcode-live   c6i.xlarge      on-demand
ip-10-0-41-88.ec2.internal    Ready      <none>   34s   v1.35.0   transcode-live   c6i.xlarge      on-demand
```

`NODEPOOL=transcode-live`, `CAPACITY-TYPE=on-demand`, a `c` family with 4 vCPU. All three columns matter — if capacity type says `spot`, the pod landed on the wrong pool.

Confirm the pod is actually running on it:

```sh
kubectl get pod -l app=karpenter-smoke-test -o wide
```

Now delete it and watch the other half of the loop:

```sh
kubectl delete deploy karpenter-smoke-test
```

The node is empty immediately; `consolidateAfter: 2m` means Karpenter waits two minutes before acting, then drains and terminates:

```
{"level":"INFO","message":"disrupting nodeclaim(s) via delete, terminating 1 nodes","command-id":"...","reason":"empty"}
```

```
ip-10-0-41-88.ec2.internal   Ready,SchedulingDisabled   <none>   3m    v1.35.0   transcode-live   c6i.xlarge   on-demand
ip-10-0-41-88.ec2.internal   NotReady                   <none>   3m    ...
```

and then it is gone from `kubectl get nodes`. **Do not skip this half.** A cluster that scales up and never scales down is worse than no autoscaling at all — it just costs money more slowly. Verify the node disappears, and if it does not, find out why before moving on.

Finally, confirm you are back to where you started:

```sh
kubectl get nodes
kubectl get nodeclaims
```

Two `t3.medium` managed nodes, no NodeClaims.

---

## What breaks

Ordered by how often each actually happens.

### 1. Pod stays `Pending`, and Karpenter's logs say nothing at all

Karpenter has not noticed the pod, which almost always means the pod does not match any NodePool.

```sh
kubectl describe pod <name> | grep -A10 Events
kubectl -n kube-system logs deploy/karpenter --tail=50
```

The mismatch is usually one of:

- The toleration does not exactly match the taint. `key`, `value` and `effect` must all agree; `operator: Equal` with no `value` matches nothing.
- The `nodeSelector` labels are not on the NodePool's `template.metadata.labels`. `workload: transcode` and `transcode-mode: live` must both be there.
- The pod requests more than any allowed instance type provides — a 16 vCPU request against a pool restricted to `["4", "8"]` is unsatisfiable and Karpenter correctly does nothing.

`kubectl get nodepool transcode-live -o yaml` and compare against the pod spec line by line.

### 2. Instances launch and terminate repeatedly; `kubectl get nodes` never shows them

The missing access entry. This is the one with no Kubernetes-side signal at all.

```sh
aws eks list-access-entries --cluster-name streaming
aws ec2 describe-instances \
  --filters "Name=tag:karpenter.sh/nodepool,Values=transcode-live" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name,LaunchTime]' --output table
```

If `KarpenterNodeRole-streaming` is not in the access entries list, run the `create-access-entry` command from step 6.5. To see the node's own view, connect with SSM and read the bootstrap log:

```sh
aws ssm start-session --target <instance-id>
sudo journalctl -u kubelet -n 100
```

`Unauthorized` from the API server confirms it.

### 3. `NodeClaim` created but stuck, never launching an instance

```sh
kubectl get nodeclaims
kubectl describe nodeclaim <name>
```

Read the conditions and events. The usual causes:

- **`no subnets matched selector`** or **`no security groups matched selector`** — the discovery tags from step 6.2 are missing. Re-run the tagging commands and check with `aws ec2 describe-subnets --filters "Name=tag:karpenter.sh/discovery,Values=streaming" --query 'Subnets[].SubnetId'`.
- **`InsufficientInstanceCapacity`** — AWS has none of that shape in that AZ right now. Genuinely happens with spot. Widening `instance-cpu` or adding a family gives Karpenter more options.
- **`UnauthorizedOperation` on `ec2:RunInstances`** — the controller's IRSA is wrong. Check `kubectl -n kube-system get sa karpenter -o yaml` for the `eks.amazonaws.com/role-arn` annotation.
- **`limits exceeded`** — the pool hit `limits.cpu`. Working as designed; either something is looping, or you need a bigger ceiling.

### 4. Node appears but the pod still will not schedule onto it

Usually an `ephemeral-storage` request larger than the node's allocatable disk, or a second label mismatch.

```sh
kubectl describe node <new-node> | grep -A8 Allocatable
kubectl describe pod <name> | grep -A10 Events
```

`0/3 nodes are available: 1 Insufficient ephemeral-storage` against a 200 GiB root volume means the pod is asking for more than the kubelet reserves as allocatable — kubelet keeps back a slice for images and system use, so allocatable is meaningfully less than 200 GiB.

### 5. Pod evicted mid-run with `Pod ephemeral local storage usage exceeds the total limit of containers`

The `tempfile` problem, in production. The transcode wrote more scratch than its limit allowed.

```sh
kubectl get events --sort-by=.lastTimestamp | grep -i evict
kubectl describe pod <name> | grep -A5 "Last State"
```

Raise the pod's `ephemeral-storage` limit (and `emptyDir.sizeLimit` to match), and check the numbers in the sizing section above against the actual source file. A 2 GiB VOD source with an 8 Gi limit will always lose — 2 GiB in plus the ladder out does not fit, which is why VOD gets 40Gi/50Gi and live gets 5Gi/8Gi.

### 6. Nodes never go away after pods finish

```sh
kubectl get nodes -L karpenter.sh/nodepool
kubectl -n kube-system logs deploy/karpenter --tail=100 | grep -i -e disrupt -e consolidat
kubectl get pods --all-namespaces --field-selector spec.nodeName=<node> -o wide
```

`WhenEmpty` means literally empty. If any non-DaemonSet pod is still there, the node stays. The usual culprit is a system pod that got scheduled before the taint existed, or a completed Job's pod that was never cleaned up. Also check the node for a `karpenter.sh/do-not-disrupt` annotation left over from testing.

### 7. `exec format error` when the transcode image starts

An `arm64` node ran an `amd64` image, which means the `kubernetes.io/arch` requirement was widened or dropped.

```sh
kubectl get node <node> -o jsonpath='{.status.nodeInfo.architecture}'
kubectl get nodepool transcode-live -o yaml | grep -A3 arch
```

Restore `values: ["amd64"]` until CI publishes multi-arch images.

---

**Next:** [Module 7 — Secrets](07-secrets.md).
