# Module 16 — Monitoring, cost, and teardown

The last module. Three things: see what the system is doing, decide what you're willing to pay for it, and be able to make the bill go to zero.

The teardown section is the one that matters most. Several AWS resources keep billing after the cluster is gone, and they are easy to leave running for months without noticing.

---

## What you're building

A Prometheus and Grafana stack that gives you Kafka consumer lag, transcode Job outcomes, node count and RDS connections on one screen; an itemized understanding of the ~$238/month you're spending and which levers actually move it; and a teardown procedure that leaves nothing behind.

---

# Part 1 — Monitoring

## Why it works this way

### Consumer lag is *the* metric for this system

Most systems don't have a single number that tells you whether they're healthy. This one does.

Every unit of work in the platform enters through Kafka. A stream starting is a message on `stream.start.requests`. An upload finishing is a message on `upload.events`. And since [Module 14](14-keda-scaledjobs.md), the thing that decides whether work gets done is **consumer-group lag** — KEDA reads it, and creates a pod per unit of lag.

So lag is simultaneously:

- **the queue depth** — how much work is waiting,
- **the input to the autoscaler** — literally the number KEDA divides by `lagThreshold`,
- **the health signal** — lag that isn't draining means pods aren't claiming, which means something between Kafka and Postgres is broken.

That's an unusual amount of signal in one series. Three distinct failure modes each have a distinct lag shape:

| Lag shape | What it means |
|---|---|
| Spikes to 1, drops to 0 within ~30s | Normal. A stream started, a pod claimed it. |
| Sits at a constant N for minutes | Pods are being created but not claiming — DB unreachable, or the pods are Pending because Karpenter can't provision. |
| Climbs monotonically forever while Jobs churn | The consumer group name in the pod doesn't match the trigger's. The expensive one from Module 14. |

Watch that one panel and you'll catch most of what goes wrong.

### Why kube-prometheus-stack, and why Kafka dashboards are nearly free

`kube-prometheus-stack` is one Helm chart containing the Prometheus operator, Prometheus, Alertmanager, Grafana, node-exporter and kube-state-metrics, pre-wired. It gives you `ServiceMonitor` and `PodMonitor` CRDs, which turn "scrape this thing" into a small YAML file rather than editing a Prometheus config and reloading it.

The Kafka half comes almost for free because of a decision made back in [Module 10](10-kafka-strimzi.md). Strimzi ships a **built-in JMX Prometheus exporter**: you add a `metricsConfig` block to the `Kafka` CR referencing a ConfigMap of JMX rules, and the broker exposes Prometheus metrics on port 9404. Strimzi also publishes ready-made Grafana dashboards that consume exactly those metrics, including a consumer-lag dashboard. You import a JSON file and you're done.

That is a concrete payoff from choosing an operator over a plain Helm chart, and it's worth noticing: you didn't build the Kafka observability, you inherited it.

## Do it

### 1. Install the stack

```sh
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace \
  --version 78.1.0 \
  --set grafana.adminPassword='<pick-one>' \
  --set prometheus.prometheusSpec.retention=7d \
  --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.storageClassName=gp3 \
  --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage=20Gi \
  --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
  --set alertmanager.enabled=false
```

Three of those flags matter:

- **`serviceMonitorSelectorNilUsesHelmValues=false`** (and the PodMonitor twin). By default the chart makes Prometheus scrape only ServiceMonitors carrying its own release label. Every `ServiceMonitor` you write yourself, and every one Strimzi generates, would be silently ignored. This is the single most common "I installed Prometheus and it scrapes nothing" cause.
- **`retention=7d` on a 20Gi gp3 PVC.** Seven days is enough to answer "what happened yesterday" and costs $1.60/month. Longer retention on a learning cluster is paying to store data you'll never read.
- **`alertmanager.enabled=false`.** You have nowhere to route alerts and nobody on call. Turning it on later is one flag; running an Alertmanager with no receivers just burns memory.

Once Module 15 is in place, move this into `k8s/platform/kube-prometheus-stack.yaml` as an Argo Application in sync-wave 0 and delete the manual Helm release from your notes. Argo will adopt the existing release.

### 2. Turn on Strimzi's JMX exporter

Add to the `Kafka` CR in `k8s/infra/kafka/kafka.yaml`, under `spec.kafka`:

```yaml
    metricsConfig:
      type: jmxPrometheusExporter
      valueFrom:
        configMapKeyRef:
          name: kafka-metrics
          key: kafka-metrics-config.yml
```

The ConfigMap contents come straight from Strimzi's `examples/metrics/kafka-metrics.yaml` in the release you pinned — don't hand-write JMX rules. Then tell Prometheus to scrape it:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: kafka-resources
  namespace: monitoring
spec:
  selector:
    matchLabels:
      strimzi.io/kind: Kafka
  namespaceSelector:
    matchNames: [kafka]
  podMetricsEndpoints:
    - path: /metrics
      port: tcp-prometheus
```

Restarting the broker to pick up `metricsConfig` is a rolling restart Strimzi handles itself, but it does briefly interrupt Kafka. Do it when nobody is streaming.

### 3. Reach Grafana

```sh
kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80
# http://localhost:3000  — admin / <the password you set>
```

Port-forward is genuinely fine here. Grafana behind the public ALB means a public login page for a dashboard only you use — the tradeoff is bad. If you do want it on the ALB, put it behind the same path-based rule as ArgoCD and set `grafana.grafana\.ini.server.root_url` so the asset paths resolve under the subpath.

Import Strimzi's dashboards: Dashboards → Import → Upload JSON, using `examples/metrics/grafana-dashboards/strimzi-kafka.json` and `strimzi-kafka-exporter.json` from the Strimzi release. The kube-prometheus-stack dashboards (node, pod, workload) are already there.

### 4. What to actually watch

Four panels. Not forty.

**1. Kafka consumer lag on the two KEDA topics.** The one that matters.

```promql
sum by (consumergroup, topic) (
  kafka_consumergroup_lag{consumergroup=~"transcode-live|transcode-vod"}
)
```

Read it with the table above. If you add one alert to this entire cluster, make it "lag > 0 for more than 5 minutes" — that condition means work is waiting and not being picked up, and it fires for all three failure modes.

**2. Transcode Job success and failure.** From kube-state-metrics, which the stack installs:

```promql
sum(kube_job_status_succeeded{namespace="streaming", job_name=~"transcode-.*"})
sum(kube_job_status_failed{namespace="streaming", job_name=~"transcode-.*"})
```

Two things to know when reading this. `failedJobsHistoryLimit: 20` versus `successfulJobsHistoryLimit: 5` means failures stay visible about four times longer than successes — the asymmetry is deliberate, but it will skew a naive ratio. And because of the exit-code discipline from Module 14, a pod that found no work exits 0 and shows as *succeeded*. A rising success count with no streams is not a bug; it's KEDA converging.

**3. Node count from Karpenter.** This is your live spend, in a number:

```promql
count(kube_node_info)
count by (nodepool) (karpenter_nodes_allocatable{resource="cpu"})
```

The shape you want is a flat 2 with occasional bumps to 3 or 4 that come back down within a few minutes of a stream ending. A step up that never comes back down means consolidation isn't happening — usually a pod without a controller, or `karpenter.sh/do-not-disrupt` left on something that finished.

**4. RDS connections.** Every handler opens its own `psycopg.connect()` with no pool, and a `db.t4g.micro` allows roughly 85. Eight transcode Jobs plus readiness probes plus the sweeper gets closer to that than you'd expect. There's no in-cluster exporter for this — read it from CloudWatch:

```sh
aws cloudwatch get-metric-statistics \
  --namespace AWS/RDS --metric-name DatabaseConnections \
  --dimensions Name=DBInstanceIdentifier,Value=streaming \
  --start-time "$(date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --period 300 --statistics Maximum
```

If you want it in Grafana, add the CloudWatch datasource — but check the API call cost first; polling CloudWatch aggressively is a line item people are surprised by.

**Not done here: application-level metrics.** None of the five services expose a `/metrics` endpoint. There's real value waiting there — transcode duration by rendition, admission-control 503 rate, upload sizes, viewer counts from `analytics-worker` — and `prometheus-client` plus a `PodMonitor` per service is maybe an hour of work. It's a follow-up, not part of this module. What you have now is infrastructure-level: Kubernetes knows a Job failed, but only the app can tell you *why* ffmpeg exited 1.

## Verify

```sh
kubectl get pods -n monitoring
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090
```

In Prometheus → Status → Targets, you want the Kafka target `UP`:

```
podMonitor/monitoring/kafka-resources/0 (1/1 up)
  http://10.0.2.117:9404/metrics    UP    2.1s ago    14.3ms
```

Then start a stream and watch panel 1 spike to 1 and return to 0 within about 30 seconds. If the lag panel is empty, the JMX exporter isn't wired; if it's populated but flat at 0 while a stream is running, you're querying the wrong consumer group name.

## What breaks

- **Prometheus scrapes nothing you added.** The `serviceMonitorSelectorNilUsesHelmValues=false` flag is missing. Check `kubectl get prometheus -n monitoring -o yaml | grep -A5 serviceMonitorSelector` — an empty `{}` selector is what you want.
- **Prometheus pod Pending.** Its PVC has no StorageClass. `kubectl get pvc -n monitoring`; the fix is [Module 4](04-addons-and-storage.md)'s default gp3 class.
- **Grafana dashboards show "No data" everywhere.** Almost always the datasource UID in an imported JSON doesn't match the one the chart created. Re-import and pick the datasource explicitly in the import dialog.
- **Prometheus OOMKilled.** 7 days of retention with default scrape intervals on a t3.medium is tight. Raise `scrapeInterval` to 60s before you raise the memory limit — you don't need 30-second resolution here.

---

# Part 2 — Cost

## The line items

Monthly, `us-east-1`, 730 hours, everything running 24/7.

| # | Resource | Spec | Monthly |
|---|---|---|---|
| 1 | **EKS control plane** | $0.10/hr | **$73.00** |
| 2 | Control-plane logs → CloudWatch | api/audit/authenticator, 7d retention | $1.50 |
| 3 | **Managed node group** | 2 × t3.medium on-demand | **$60.74** |
| 4 | Node root EBS | 2 × 40 GiB gp3 | $6.40 |
| 5 | **NAT Gateway** | 1 × $0.045/hr + ~20 GB processed | **$33.75** |
| 6 | **ALB** | $0.0225/hr + ~1 LCU | **$17.43** |
| 7 | **NLB** | $0.0225/hr + ~2 NLCU | **$18.43** |
| 8 | **RDS instance** | db.t4g.micro, single-AZ | **$11.68** |
| 9 | RDS storage | 20 GiB gp3 | $2.30 |
| 10 | RDS backups | 7 days, ≤100% of storage | $0.00 |
| 11 | Kafka PVC | 20 GiB gp3 | $1.60 |
| 12 | S3 storage | ~100 GB Standard | $2.30 |
| 13 | S3 requests | PUT-heavy — live playlists re-uploaded every 2s per stream | ~$1.50 |
| 14 | **CloudFront** | <1 TB out, <10M requests — perpetual free tier | **$0.00** |
| 15 | S3 → CloudFront origin transfer | Free by design (OAC, same region) | $0.00 |
| 16 | Route 53 | 1 hosted zone + queries | $0.60 |
| 17 | ACM | DNS-validated public certs | $0.00 |
| 18 | Secrets Manager | 4 secrets + API calls | $1.65 |
| 19 | S3 Gateway VPC endpoint | Gateway type is free | $0.00 |
| 20 | **Karpenter transcode nodes** | ~20h live on-demand + ~10h VOD spot | **$4.60** |
| 21 | Karpenter node EBS | transient, ~120 GiB × ~30h | $0.40 |
| 22 | CloudWatch metrics | default only, Container Insights **off** | $0.00 |
| | **Total** | | **≈ $238/mo** |

Two things to sit with.

**The EKS control plane is $73 — roughly half the budget — and it is fixed.** There is no smaller tier, no scaling it down, no turning it off overnight. You pay $73/month for an empty cluster with zero nodes. That single number shapes every other decision in this course: it's why there's one NAT gateway instead of three, why Kafka runs in-cluster instead of on MSK (which has a ~$100/month floor of its own), and why the node group is two t3.mediums rather than something comfortable.

**CloudFront is genuinely $0 here,** and that's not a rounding error. The perpetual free tier covers 1 TB out and 10M requests per month, and a learning project with a handful of viewers is nowhere near it. Serving HLS from CloudFront instead of straight from an S3 origin is free *and* faster.

## The levers

| Lever | Saving | Verdict |
|---|---|---|
| **A. Node group on spot capacity** | −$42.50 | **Take it.** |
| **B. NAT instance instead of NAT Gateway** | −$30 | **Defer** until everything works. |
| **C. Scheduled scale-to-zero overnight** | −$25 | **Take it.** |
| **D. Stop RDS when idle** | −$8 | Only if you'll remember to restart it. |
| E. Collapse ALB + NLB onto ingress-nginx | −$17 | **Reject.** |
| F. EKS Auto Mode | +12% | **Reject.** |

### A. Node group on spot — take it

`t3.medium` on-demand is $0.0416/hr; spot is around $0.0125. Two nodes, 730 hours: **$60.74 → $18.25**.

```sh
eksctl create nodegroup --cluster streaming --name system-spot \
  --instance-types t3.medium,t3a.medium,t2.medium \
  --spot --nodes 2 --nodes-min 2 --nodes-max 3 \
  --node-volume-size 40 --node-volume-type gp3
eksctl delete nodegroup --cluster streaming --name system   # after the new one is Ready
```

Multiple instance types is the important part — a spot pool for one type in one AZ can be genuinely unavailable, and three types across two AZs essentially never are.

What's actually at risk: a spot reclaim gives 2 minutes' notice, the pod gets drained, and it comes back on another node. CoreDNS, the AWS Load Balancer Controller, Karpenter, ArgoCD and the web tier all tolerate that fine. The one to think about is the **Kafka broker** — its PVC is single-AZ, so the replacement pod must land in the same AZ as its EBS volume, and if there's no spot capacity in that AZ at that moment the broker sits Pending. That means a few minutes with no event bus. On a learning cluster that's an acceptable, instructive outage. In production you'd keep the broker on on-demand.

Note that this does **not** touch the transcode NodePools. Module 6 already put live transcode on on-demand deliberately — a spot reclaim mid-stream is an unrecoverable blackout for viewers — and VOD on spot. That split stays.

### C. Scheduled scale-to-zero overnight — take it, and here's how

The single highest-leverage habit in this whole course. Twelve hours a day at 0 nodes saves half your EC2 bill: on top of lever A, roughly **−$25/month**.

The manual version, which you should do the first few times so you can see what happens:

```sh
# End of the day
eksctl scale nodegroup --cluster streaming --name system-spot --nodes 0 --nodes-min 0

# Next morning
eksctl scale nodegroup --cluster streaming --name system-spot --nodes 2 --nodes-min 2
```

Automate it with EventBridge Scheduler calling the EKS API directly — no Lambda needed, since Scheduler's universal targets can invoke any AWS API:

```sh
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)

# A role Scheduler can assume, allowed to resize exactly this node group.
cat > /tmp/trust.json <<'JSON'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
  "Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}
JSON
aws iam create-role --role-name streaming-nodegroup-scheduler \
  --assume-role-policy-document file:///tmp/trust.json

cat > /tmp/policy.json <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
  "Action":["eks:UpdateNodegroupConfig","eks:DescribeNodegroup"],
  "Resource":"arn:aws:eks:us-east-1:${ACCOUNT}:nodegroup/streaming/system-spot/*"}]}
JSON
aws iam put-role-policy --role-name streaming-nodegroup-scheduler \
  --policy-name resize --policy-document file:///tmp/policy.json

ROLE_ARN="arn:aws:iam::${ACCOUNT}:role/streaming-nodegroup-scheduler"

# Down at 20:00 local, up at 08:00 local, weekdays only.
aws scheduler create-schedule --name streaming-nodes-down \
  --schedule-expression 'cron(0 20 ? * MON-FRI *)' \
  --schedule-expression-timezone 'America/Toronto' \
  --flexible-time-window '{"Mode":"OFF"}' \
  --target "{\"Arn\":\"arn:aws:scheduler:::aws-sdk:eks:updateNodegroupConfig\",
             \"RoleArn\":\"${ROLE_ARN}\",
             \"Input\":\"{\\\"ClusterName\\\":\\\"streaming\\\",\\\"NodegroupName\\\":\\\"system-spot\\\",\\\"ScalingConfig\\\":{\\\"MinSize\\\":0,\\\"DesiredSize\\\":0,\\\"MaxSize\\\":3}}\"}"

aws scheduler create-schedule --name streaming-nodes-up \
  --schedule-expression 'cron(0 8 ? * MON-FRI *)' \
  --schedule-expression-timezone 'America/Toronto' \
  --flexible-time-window '{"Mode":"OFF"}' \
  --target "{\"Arn\":\"arn:aws:scheduler:::aws-sdk:eks:updateNodegroupConfig\",
             \"RoleArn\":\"${ROLE_ARN}\",
             \"Input\":\"{\\\"ClusterName\\\":\\\"streaming\\\",\\\"NodegroupName\\\":\\\"system-spot\\\",\\\"ScalingConfig\\\":{\\\"MinSize\\\":2,\\\"DesiredSize\\\":2,\\\"MaxSize\\\":3}}\"}"
```

Know what you're signing up for:

- **The control plane, RDS, the load balancers and every EBS volume keep billing.** This saves EC2 hours, nothing else. It's about a third of the total bill, not all of it.
- **At 0 nodes, Karpenter is also down**, because Karpenter runs on those nodes. Nothing can provision transcode capacity overnight. That's the intent.
- **Morning bring-up takes 5–10 minutes** for nodes to join, Kafka's broker to reattach its PVC and reach Ready, and Argo to reconcile. Don't schedule the wake-up for the moment you start work; schedule it an hour earlier.
- **The Kafka broker's PVC pins it to one AZ.** If the node group brings both nodes up in the other AZ, the broker stays Pending. Keeping `--nodes 2` across two AZs makes this a non-issue, but it's the thing to check first if Kafka doesn't come back.

### B. NAT instance instead of NAT Gateway — defer

A `t4g.nano` running the fck-nat AMI does the same job for about $3/month versus $33.75. It's a real saving and a genuinely good thing to learn.

**Do it after everything else works, not before.** The reason isn't technical difficulty, it's diagnostic cost. A NAT instance is a single EC2 instance doing packet forwarding, with a source/dest check you have to disable, a route table entry you have to point at an ENI, and no redundancy. When something in the cluster can't reach the internet — an image pull, an AWS API call, a Helm repo — you now have an extra suspect, and "is it my NAT instance?" is a question you'll ask on every network problem for the rest of the project. Managed NAT costs $30/month to never have to ask it.

Swap it in when the platform is boring. Then, if something breaks, you have exactly one recent change to blame.

### D. Stop RDS when idle — conditional

`aws rds stop-db-instance` pauses instance-hour billing (storage bills either way), saving about $8/month.

The catch that makes this a conditional recommendation: **RDS auto-restarts a stopped instance after 7 days.** So it's not "stop it and forget it" — it's a habit you have to keep, and if you forget, you get a partial saving and an instance you thought was off. Take it only if you're already in the habit of scale-to-zero. If lever C is automated and this isn't, this one will rot.

### E. Collapse the ALB and NLB onto ingress-nginx — reject

Superficially attractive: run ingress-nginx behind one NLB, use its `tcp-services` ConfigMap to also proxy RTMP on 1935, and delete the ALB. Saves $17/month.

Reject it for one decisive reason: **ingress-nginx is retired upstream.** The maintainers announced its retirement in favour of Gateway API, so you'd be adopting a component with no future, on a project whose explicit goal is learning things that transfer.

The secondary reasons are real too. You'd trade AWS-native, controller-managed load balancing for a pod you now operate, on a cluster where scale-to-zero means that pod is regularly gone. You'd lose the per-listener tuning from [Module 12](12-ingress-and-rtmp.md) — the NLB's idle timeout and deregistration delay were configured specifically because RTMP connections are long-lived, and generic `tcp-services` proxying gives you neither knob. And $17/month is not worth operating your own L7 proxy.

### F. EKS Auto Mode — reject

Auto Mode has AWS manage compute, storage and load balancing for you, adding roughly 12% on top of EC2 costs.

Reject it on both counts. **It costs more, not less** — the lever is supposed to save money and this one adds to the bill. And **it removes exactly what you came here to learn.** Karpenter's NodePools, the EBS CSI driver and StorageClass, the AWS Load Balancer Controller and its annotations — Auto Mode hides all of it behind a managed abstraction. Modules 4, 6 and 12 would collapse into a checkbox. That's a reasonable trade for a team shipping a product and a bad one for a course whose entire point is understanding how the pieces fit.

## Where you land

| Configuration | Monthly |
|---|---|
| As designed | ~$238 |
| + A (spot node group) | ~$195 |
| + A + C (spot and overnight scale-to-zero) | ~$170 |
| + A + B + C (and a NAT instance) | ~$140 |

In budget, with the parts worth keeping intact.

## Verify your spend

Set the budget alarm in [Module 1](01-aws-account-setup.md) if you somehow haven't. Then check the shape of the bill weekly:

```sh
aws ce get-cost-and-usage \
  --time-period Start="$(date -u -v-7d +%Y-%m-%d)",End="$(date -u +%Y-%m-%d)" \
  --granularity DAILY --metrics UnblendedCost \
  --group-by Type=DIMENSION,Key=SERVICE \
  --query 'ResultsByTime[-1].Groups[?Metrics.UnblendedCost.Amount>`0.1`].[Keys[0],Metrics.UnblendedCost.Amount]' \
  --output table
```

A daily total in the $6–8 range is on track for the as-designed number. A day that jumps to $20 is almost always EC2, and almost always a transcode node that never consolidated or a Job loop from a mismatched consumer group.

---

# Part 3 — Teardown

This is the part that matters most, and the part people get wrong in one specific way.

## Why the order matters

**Delete the Kubernetes objects that own AWS resources first.**

The ALB and the NLB were not created by eksctl or CloudFormation. They were created by the AWS Load Balancer Controller, in response to an Ingress and a Service of type LoadBalancer. eksctl has no idea they exist. Delete the cluster first and those load balancers survive it — running, billing **$16–18/month each**, with no Kubernetes object left anywhere that points at them. They'll sit in your account until you happen to look at the EC2 console's load balancer page.

That's the number one forgotten cost in EKS teardown, and it's a direct consequence of a good design: controllers create real infrastructure from Kubernetes objects, so the Kubernetes objects have to go first, while the controllers are still running to clean up after them.

Same logic for Karpenter's NodePools (Karpenter terminates the instances it launched) and for PVCs (the EBS CSI driver deletes the volumes).

## Do it

### Step 1 — Kubernetes objects that own AWS resources

```sh
# Stop Argo re-creating everything you are about to delete.
kubectl patch app root -n argocd --type merge \
  -p '{"metadata":{"finalizers":null}}'
kubectl delete app --all -n argocd --cascade=orphan

# The ALB and the NLB.
kubectl delete ingress --all --all-namespaces
kubectl delete svc --all-namespaces --field-selector spec.type=LoadBalancer

# Karpenter drains and terminates every node it launched.
kubectl delete nodepool --all
kubectl delete ec2nodeclass --all

# Releases the Kafka and Prometheus EBS volumes.
kubectl delete pvc --all --all-namespaces

sleep 180   # let the controllers finish; watch the logs if you are impatient
```

Confirm before moving on — this is the checkpoint that saves you money:

```sh
aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerName' --output text
aws ec2 describe-instances \
  --filters "Name=tag:karpenter.sh/nodepool,Values=*" "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].InstanceId' --output text
```

Both must come back empty. If a load balancer is still listed, the controller was already gone when you deleted the Ingress — delete it by hand with `aws elbv2 delete-load-balancer --load-balancer-arn <arn>`.

### Step 2 — The cluster

```sh
eksctl delete cluster -f infra/eks/cluster.yaml --disable-nodegroup-eviction
```

This takes 15–20 minutes and also removes the managed node group, the VPC, the NAT gateway, the subnets and the route tables.

### Step 3 — Karpenter's CloudFormation stack

Separate from the cluster, because it was created separately: the node IAM role, the instance profile, the interruption SQS queue and the EventBridge rules.

```sh
aws cloudformation delete-stack --stack-name Karpenter-streaming
aws cloudformation wait stack-delete-complete --stack-name Karpenter-streaming
```

### Step 4 — RDS

Deletion protection has to come off first, and it is a separate call:

```sh
aws rds modify-db-instance --db-instance-identifier streaming \
  --no-deletion-protection --apply-immediately

aws rds delete-db-instance --db-instance-identifier streaming \
  --skip-final-snapshot --delete-automated-backups
```

Both flags on the delete matter. Without `--skip-final-snapshot` RDS takes one by default and it bills at $0.095/GiB-month forever. Without `--delete-automated-backups` the retained backups survive the instance and bill too. If you actually want the data, take a snapshot deliberately and know you're paying for it.

### Step 5 — CloudFront

CloudFront cannot be deleted while enabled, and disabling it is not instant.

```sh
DIST_ID=E1XXXXXXXXXXXX
aws cloudfront get-distribution-config --id "$DIST_ID" > /tmp/dist.json
ETAG=$(jq -r '.ETag' /tmp/dist.json)
jq '.DistributionConfig | .Enabled = false' /tmp/dist.json > /tmp/disabled.json

aws cloudfront update-distribution --id "$DIST_ID" \
  --if-match "$ETAG" --distribution-config file:///tmp/disabled.json

# Poll until Status is Deployed. This takes about 15 minutes. Make tea.
aws cloudfront get-distribution --id "$DIST_ID" --query 'Distribution.Status' --output text

NEW_ETAG=$(aws cloudfront get-distribution-config --id "$DIST_ID" --query ETag --output text)
aws cloudfront delete-distribution --id "$DIST_ID" --if-match "$NEW_ETAG"
```

A disabled distribution costs nothing, so if you get interrupted after the disable, you're not bleeding money — just leaving clutter.

### Step 6 — S3

A bucket must be empty to delete, and "empty" means more than the object list shows.

```sh
BUCKET=linkify-streaming-media-<accountid>

# Current objects.
aws s3 rm "s3://$BUCKET" --recursive

# Object VERSIONS and delete markers. `aws s3 rm` does not touch these, and if
# versioning was ever enabled they are invisible in the console's object list
# while still billing.
aws s3api list-object-versions --bucket "$BUCKET" \
  --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' > /tmp/versions.json
[ "$(jq '.Objects | length' /tmp/versions.json)" -gt 0 ] && \
  aws s3api delete-objects --bucket "$BUCKET" --delete file:///tmp/versions.json

aws s3api list-object-versions --bucket "$BUCKET" \
  --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' > /tmp/markers.json
[ "$(jq '.Objects | length' /tmp/markers.json)" -gt 0 ] && \
  aws s3api delete-objects --bucket "$BUCKET" --delete file:///tmp/markers.json

# Incomplete multipart uploads. Invisible in the console's object list, and
# very much billable. The 2 GiB upload path creates these whenever a browser
# upload is abandoned partway.
aws s3api list-multipart-uploads --bucket "$BUCKET" \
  --query 'Uploads[].[Key,UploadId]' --output text | while read -r key uid; do
    aws s3api abort-multipart-upload --bucket "$BUCKET" --key "$key" --upload-id "$uid"
  done

aws s3api delete-bucket --bucket "$BUCKET"
```

If `delete-bucket` returns `BucketNotEmpty` after all that, re-run the versions block — deleting objects in a versioned bucket creates *new* delete markers, so it can take two passes.

### Step 7 — Secrets Manager

Deleted secrets sit in a recovery window, and **they keep billing during it** — 7 days by default, at $0.40 each.

```sh
for s in streaming/ghcr streaming/mediamtx streaming/db-endpoint; do
  aws secretsmanager delete-secret --secret-id "$s" --force-delete-without-recovery
done
```

That is the complete list of secrets *you* created — [Module 7](07-secrets.md) made the first two, [Module 8](08-rds-postgres.md) the third. There is no `streaming/app-config`: the non-secret configuration lives in `k8s/infra/secrets/configmap-app.yaml`, in git, and disappears with the cluster. Confirm nothing is left behind:

```sh
aws secretsmanager list-secrets \
  --query 'SecretList[?starts_with(Name, `streaming/`)].Name' --output text
```

The RDS-managed `rds!db-…` secret is not in that list and must not be deleted by hand — it belongs to the instance and goes away with it in step 4. Deleting it first leaves the instance holding a dangling `MasterUserSecret` reference.

`--force-delete-without-recovery` is irreversible and that is the point. It is also incompatible with `--recovery-window-in-days`; pass one or the other.

### Step 8 — Route 53

A hosted zone can't be deleted while it holds records other than its own NS and SOA.

```sh
ZONE_ID=ZXXXXXXXXXXXXX

# Show what's left. Anything that isn't NS or SOA at the apex has to go.
aws route53 list-resource-record-sets --hosted-zone-id "$ZONE_ID" \
  --query 'ResourceRecordSets[?Type!=`NS` && Type!=`SOA`].[Name,Type]' --output table

# ExternalDNS should have removed its own records when you deleted the
# Ingress/Services in step 1. Delete anything remaining by hand, then:
aws route53 delete-hosted-zone --id "$ZONE_ID"
```

Also remove the `k8s` NS delegation record from the parent `linkifysolutions.com` zone at your registrar. Leaving a delegation pointing at a zone that no longer exists means anyone querying `k8s.linkifysolutions.com` gets a lame delegation — harmless, but untidy, and it's the kind of thing that confuses someone a year from now.

### Step 9 — The sweep for things nothing owns

```sh
# Orphaned load balancers — check again, seriously.
aws elbv2 describe-load-balancers --query 'LoadBalancers[].[LoadBalancerName,Type]' --output table

# Unassociated Elastic IPs. Since Feb 2024, IDLE public IPv4 addresses bill
# $3.65/mo each. The NAT gateway's EIP is free while attached and starts
# billing the moment the NAT is deleted without releasing it.
aws ec2 describe-addresses \
  --query 'Addresses[?AssociationId==null].[PublicIp,AllocationId]' --output table
# aws ec2 release-address --allocation-id <id>

# Available (detached) EBS volumes — the Kafka PVC if its StorageClass or
# `deleteClaim: false` retained it.
aws ec2 describe-volumes --filters Name=status,Values=available \
  --query 'Volumes[].[VolumeId,Size,CreateTime]' --output table
# aws ec2 delete-volume --volume-id <id>

# Snapshots survive everything that created them.
aws ec2 describe-snapshots --owner-ids self \
  --query 'Snapshots[].[SnapshotId,VolumeSize,StartTime]' --output table
aws rds describe-db-snapshots --snapshot-type manual \
  --query 'DBSnapshots[].[DBSnapshotIdentifier,AllocatedStorage]' --output table

# CloudWatch log groups outlive the cluster, forever, at whatever retention
# they had. /aws/eks/streaming/cluster is the big one.
aws logs describe-log-groups --query 'logGroups[].[logGroupName,storedBytes]' --output table
# aws logs delete-log-group --log-group-name /aws/eks/streaming/cluster

# ECR/Karpenter leftovers: the interruption queue and its EventBridge rules.
aws sqs list-queues --queue-name-prefix Karpenter
```

## Verify the bill actually went to zero

The only source of truth is Cost Explorer, and it lags about 24 hours. So: tear down, wait a day, then look.

```sh
aws ce get-cost-and-usage \
  --time-period Start="$(date -u -v-2d +%Y-%m-%d)",End="$(date -u +%Y-%m-%d)" \
  --granularity DAILY --metrics UnblendedCost \
  --group-by Type=DIMENSION,Key=SERVICE \
  --output table
```

What you want to see is the previous day at a few cents, and the services list containing only Route 53 (if you kept the zone) and possibly S3.

**What legitimately still costs a little,** so you don't chase zeros that aren't there:

| Thing | Cost | Why it's fine |
|---|---|---|
| Route 53 hosted zone | $0.50/mo | Only if you deliberately kept the zone to avoid re-delegating later. Reasonable choice. |
| S3 buckets you kept | ~$0.023/GB-mo | Anything you saved on purpose. |
| CloudWatch log group storage | $0.03/GB-mo | Until you delete the log groups. A few hundred MB is cents. |
| Deleted Secrets Manager secrets | $0.40/secret | Only if you used a recovery window instead of `--force-delete-without-recovery`. Ends when the window does. |
| The current month's already-accrued charges | varies | Deleting a resource doesn't refund the hours you already used. Two more days of billing after teardown is normal. |

Anything else showing up 48 hours after teardown is something you missed. Go back to the step 9 sweep — in practice it is a load balancer, an unassociated Elastic IP, or an EBS volume, in that order of likelihood.

---

## That's the course

You've built a streaming platform on EKS that ingests RTMP through an NLB, transcodes on nodes that don't exist until they're needed, serves HLS from CloudFront, keeps no credentials in git, deploys itself from a git commit, and can be deleted completely in nine steps.

The thing worth carrying forward isn't any particular command. It's the shape of the reasoning: every piece of this cost something, and for each one there's a defensible answer to "why this and not the cheaper thing" — one NAT gateway, Kafka in-cluster, on-demand for live and spot for VOD, Kustomize over Helm, an ALB *and* an NLB. Being able to give those answers is the actual deliverable.

Back to the [course index](README.md).
