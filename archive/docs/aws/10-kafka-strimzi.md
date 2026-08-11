# Module 10 — Kafka with Strimzi

← [Module 9: S3 and CloudFront](09-s3-and-cloudfront.md) · [Index](README.md) · [Module 11: Running the services](11-workloads.md) →

---

## Read this before you copy any YAML

Kafka on Kubernetes changed shape twice in the last two years, and the internet has not caught up. Two specific things will send you down a wrong path:

1. **Strimzi ≥ 0.46 removed ZooKeeper entirely.** KRaft is the only mode, and broker sizing/storage/replica count moved out of the `Kafka` resource into a separate `KafkaNodePool` resource. Any guide that shows `spec.zookeeper` or `spec.kafka.replicas` predates this.
2. **Strimzi 1.0 dropped the `kafka.strimzi.io/v1beta2` API in favour of `kafka.strimzi.io/v1`.** Almost every blog post, Stack Overflow answer, and a fair number of doc snippets still say `v1beta2`.

So: **do not guess the API version.** After installing the operator, ask the cluster what it actually serves:

```sh
kubectl get crd kafkanodepools.kafka.strimzi.io \
  -o jsonpath='{range .spec.versions[*]}{.name}{" served="}{.served}{" storage="}{.storage}{"\n"}{end}'
```

```
v1beta2 served=true storage=true
```

Use the version with `storage=true`. The manifests in `k8s/infra/kafka/` are written for `v1beta2`, matching the operator version pinned below. If your check prints `v1 served=true storage=true`, it is a find-and-replace across those five files, plus one structural change noted in "What breaks".

---

## What you're building

The Strimzi operator in the `kafka` namespace, a single-node Kafka cluster in KRaft mode with a 20 GiB gp3 volume, and five topics declared as Kubernetes resources — which is what replaces `kafka/init-topics.sh`.

At the end, `kubectl -n kafka get kafka` shows Ready, five `KafkaTopic`s are Ready, and a throwaway `kcat` pod can list the topics with the right partition counts.

Cost: this runs on the existing node group. The only new AWS resource is a 20 GiB gp3 EBS volume, about $1.60/month. That is the whole reason Kafka is here rather than on MSK — MSK Serverless has roughly a $100/month floor even when completely idle.

---

## Why it works this way

### Why an operator instead of a StatefulSet

You could write a StatefulSet for Kafka. People do. What you would then own: rolling restarts that respect partition leadership, storage class and PVC lifecycle, KRaft controller quorum formation, certificate rotation if you ever enable TLS, and topic creation as an imperative script you have to remember to run.

Strimzi turns all of that into resources you declare and a controller that reconciles. Concretely: a topic becomes a `KafkaTopic` object that git owns, so `kafka/init-topics.sh` — a shell script somebody has to run at the right moment against the right broker — stops existing. That is the same trade you made for the schema in [Module 8](08-rds-postgres.md), and it is worth making twice.

The cost is a CRD API surface to learn and an operator to keep pinned. Pin it. Strimzi releases roughly monthly and does not hesitate to remove APIs.

### Why one node that is both controller and broker

`roles: [controller, broker]` on a single replica. In KRaft, controllers hold the cluster metadata log (what ZooKeeper used to do) and brokers hold the data. Splitting them is the right answer at three-plus nodes; at one node it would just mean two pods on the same machine pretending to be a quorum.

Storage is a 20 GiB gp3 persistent claim with `deleteClaim: false`. Two things to understand about that number:

- It is not about capacity. Every message on every topic in this system is a few hundred bytes of JSON. At 10,000 events a day with seven-day retention you are under 100 MB. 20 GiB is "you will never think about this again."
- gp3's baseline 3,000 IOPS and 125 MB/s come free at any size, so a bigger volume buys nothing here.

**Do not use `type: ephemeral`.** With one replica, a pod reschedule would lose every offset — including `__consumer_offsets`. That means every consumer restarts from its `auto.offset.reset` position, and in [Module 14](14-keda-scaledjobs.md) KEDA's lag measurement, which is computed from committed offsets, goes invalid and the ScaledJobs misbehave in ways that look random.

The pod carries `karpenter.sh/do-not-disrupt: "true"`. Karpenter consolidates under-utilised nodes, and a single-broker Kafka has no failover — a helpful consolidation mid-stream is a full outage.

### Replication factor 1, and what that honestly means

Every topic is `replicas: 1`, and the `Kafka` CR sets `default.replication.factor`, `offsets.topic.replication.factor`, `transaction.state.log.replication.factor` and `min.insync.replicas` all to 1.

Those overrides are **mandatory**, not stylistic. Strimzi's defaults are 3. With one broker, a topic that wants three replicas can never reach in-sync state, `__consumer_offsets` never initialises, and every consumer in the application blocks trying to join a group — which presents as five services that start cleanly, log nothing unusual, and process nothing.

`min.insync.replicas: 1` deserves its own note. All three producers in this codebase set `enable.idempotence=True`, which forces `acks=all`. With `min.insync.replicas` above the replica count, every produce fails `NOT_ENOUGH_REPLICAS`, and in `ingest-webhook` that surfaces as a blocked `flush()` on the RTMP auth path — MediaMTX rejects the publish and it looks like an OBS problem.

The honest caveat: **replication factor 1 means the broker is a single point of data loss.** If that EBS volume is lost, every message and every consumer offset is gone. Not delayed — gone. The system recovers in the sense that new events flow once the broker returns, but anything in flight is not replayable. That is acceptable here because the events are operational signals with a seven-day retention, not a system of record — the system of record is Postgres. It would not be acceptable for anything you bill on.

Related, and worth budgeting for: with one broker, **every Strimzi upgrade, Kafka version bump, or node roll is a total Kafka outage** for a minute or two. Do not do any of them during a stream.

### Plain listener, not TLS, and who connects

One listener: `name: plain`, port 9092, `type: internal`, `tls: false`.

Everything that talks to Kafka is inside the cluster — the five services in the `streaming` namespace and, importantly, the KEDA operator in `keda`. There is no path from outside the VPC to port 9092 and no `type: loadbalancer` listener to create one. Adding TLS would mean distributing Strimzi's CA into every client, configuring `security.protocol=SSL` and a truststore in `confluent-kafka`, and debugging certificate rotation — real work, for encryption between two pods on a private subnet in a single-tenant cluster.

Two things make that decision defensible rather than lazy. First, the NetworkPolicy in `k8s/infra/kafka/networkpolicy.yaml` restricts who may open 9092 at all. Second, ports **9090 and 9091 are reserved by Strimzi** for the control plane and inter-broker replication, and those are TLS-encrypted with Strimzi's own CA whether you ask or not. A listener declared on either port is rejected outright.

So: **the apps use 9092, and KEDA uses 9092.** There is no second answer.

### The bootstrap DNS name, and why the compose value stops working

Strimzi creates two Services:

```
streaming-kafka-bootstrap.kafka.svc:9092    # what every client uses
streaming-kafka-brokers.kafka.svc           # headless, per-pod, for the operator
```

The name is `<cluster-name>-kafka-bootstrap`, where the cluster name is the `Kafka` CR's `metadata.name`. Ours is `streaming`.

Today, `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`. That short name resolved because compose put every container on one Docker network. On EKS, the app pods are in namespace `streaming` and Kafka is in `kafka`, so the short name resolves to nothing. It has to become `streaming-kafka-bootstrap.kafka.svc:9092` (or the fully-qualified `streaming-kafka-bootstrap.kafka.svc.cluster.local:9092` — identical result, one fewer DNS lookup).

This failure is quiet in an unhelpful way. `confluent-kafka` logs `Failed to resolve 'kafka:9092': Name or service not known` at whatever log level the client is configured for, then retries forever while the producer's `flush()` blocks. Nothing crashes.

The KEDA half is easy to miss: **the KEDA operator pod, not your application pods, is what connects to Kafka to measure consumer lag.** So the `ScaledJob` trigger's `bootstrapServers` must be reachable from the `keda` namespace, and any NetworkPolicy has to allow it. Forget that and KEDA reports `error getting kafka client` and never spawns a Job — with the application working perfectly, because the application's connectivity is a different question.

### Why `stream.start.requests` exists, when nothing produces to it yet

`kafka/init-topics.sh` creates five topics. You are creating five, but not the same five: `transcode.jobs` is dropped and `stream.start.requests` is added.

`transcode.jobs` goes because nothing produces to it and nothing consumes it. It is a leftover from an earlier design that had a dispatcher stage, which KEDA replaces. It has never held a message. Delete it from `kafka/init-topics.sh` too, so dev and prod agree about what exists.

`stream.start.requests` is new, and the reason is a real constraint in [Module 14](14-keda-scaledjobs.md) that is easier to absorb now than to discover then. **KEDA's Kafka scaler triggers on raw consumer-group lag. It cannot read messages and it cannot filter by content.** All it knows is "this consumer group is N messages behind on this topic."

`stream.lifecycle` carries both `started` and `ended` events. If you pointed KEDA's live trigger at it, every `ended` event would also count as lag, and KEDA would spawn a transcode Job for a stream that has just finished — which would start ffmpeg against an RTMP path with no publisher, fail, retry, and count against your concurrency ceiling the whole time.

So the trigger needs a topic that contains *only* the thing that should cause a Job. `stream.start.requests` carries `started` events and nothing else. `stream.lifecycle` keeps carrying both, because the live transcode pod tails it to learn that its own stream has ended.

Eight partitions, matching `stream.lifecycle`, because the partition count is the hard ceiling on how many Jobs KEDA can run concurrently — a topic needs at least as many partitions as the concurrency it must support.

### Partition counts, inherited not invented

Straight from the reasoning already in `kafka/init-topics.sh`:

| Topic | Partitions | Why |
|---|---|---|
| `stream.lifecycle` | 8 | Keyed by MediaMTX path. Also the topic each live transcode pod tails for its own `ended` event. |
| `upload.events` | 8 | KEDA's VOD trigger. Partition count is the hard cap on concurrent VOD Jobs. |
| `stream.start.requests` | 8 | KEDA's live trigger. Same ceiling as `stream.lifecycle`. |
| `transcode.status` | 3 | One `analytics-worker` consumer, log-only, no horizontal-scaling plan. |
| `viewer.analytics` | 3 | Same. |
| ~~`transcode.jobs`~~ | — | Dropped. Unused. |

One live transcode was measured at roughly 2.3 cores and 1 GB, so eight is about double any concurrency this cluster will reach — headroom without being meaningless.

**Partitions can only go up.** The Topic Operator will increase them to match the CR but cannot decrease. Editing `8` → `4` leaves the CR permanently `NotReady` with a `PartitionDecreaseException`, and in Module 15 that means ArgoCD is permanently `Degraded`. Choose now.

And the destructive one, worth internalising before Module 15: **deleting a `KafkaTopic` CR deletes the Kafka topic and all its data.** With ArgoCD's `automated.prune: true`, renaming `topics.yaml` or moving one topic into another file is a prune-then-recreate — which reads as a no-op in the diff and is a data wipe in practice. Every topic in `topics.yaml` carries `argocd.argoproj.io/sync-options: Prune=false` for exactly this reason.

---

## Do it

### 1. Install the operator, pinned

```sh
helm repo add strimzi https://strimzi.io/charts/
helm repo update

# See what exists before pinning to something from a blog post.
helm search repo strimzi/strimzi-kafka-operator --versions | head -8
```

```
NAME                             CHART VERSION  APP VERSION
strimzi/strimzi-kafka-operator   0.49.0         0.49.0
strimzi/strimzi-kafka-operator   0.48.0         0.48.0
...
```

Pin **0.49.x** and write `v1beta2`. That is a deliberate choice: the whole ecosystem's examples still match `v1beta2`, so you get one less unknown while you are learning the rest of this stack. Moving to Strimzi 1.x and `kafka.strimzi.io/v1` is a separate, deliberate task, not something to fold into a migration.

```sh
kubectl create namespace kafka --dry-run=client -o yaml | kubectl apply -f -

helm upgrade --install strimzi strimzi/strimzi-kafka-operator \
  --namespace kafka \
  --version 0.49.0 \
  --set watchAnyNamespace=false \
  --set resources.requests.cpu=200m \
  --set resources.requests.memory=384Mi \
  --set resources.limits.memory=384Mi \
  --wait
```

`watchAnyNamespace=false` means the operator only reconciles resources in `kafka`, which is where all of them live. Widening it later is one `helm upgrade`.

```sh
kubectl -n kafka rollout status deploy/strimzi-cluster-operator
```

### 2. Check the API version — actually run this

```sh
kubectl get crd kafkanodepools.kafka.strimzi.io \
  -o jsonpath='{range .spec.versions[*]}{.name}{" served="}{.served}{" storage="}{.storage}{"\n"}{end}'
```

If it prints `v1beta2 served=true storage=true`, the manifests match and you continue. If it prints anything else, stop and read "What breaks" #1.

While you are here, note which Kafka versions this operator can actually run, so `spec.kafka.version` is not a guess:

```sh
kubectl -n kafka get deploy strimzi-cluster-operator \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="STRIMZI_KAFKA_IMAGES")].value}{"\n"}'
```

```
3.8.0=quay.io/strimzi/kafka:0.49.0-kafka-3.8.0
3.9.0=quay.io/strimzi/kafka:0.49.0-kafka-3.9.0
...
```

The manifests use `3.9.0`. If it is not in that list, pick the highest version that is and update `k8s/infra/kafka/kafka.yaml` to match.

### 3. Confirm the gp3 StorageClass exists

The `KafkaNodePool` asks for `class: gp3`. EKS ships `gp2` as default and no gp3 at all, which [Module 4](04-addons-and-storage.md) fixed.

```sh
kubectl get storageclass
```

```
NAME            PROVISIONER             DEFAULT   VOLUMEBINDINGMODE
gp3 (default)   ebs.csi.aws.com         true      WaitForFirstConsumer
gp2             kubernetes.io/aws-ebs             WaitForFirstConsumer
```

No `gp3` line means the PVC will sit `Pending` forever and you will spend an hour blaming Strimzi. Go back to Module 4.

### 4. Apply the cluster

The manifests are at `k8s/infra/kafka/`:

| File | What it is |
|---|---|
| `namespace.yaml` | the `kafka` namespace |
| `nodepool-dual.yaml` | `KafkaNodePool` — one dual-role node, 20 GiB gp3 |
| `kafka.yaml` | the `Kafka` CR, KRaft mode, one plain listener |
| `topics.yaml` | five `KafkaTopic` CRs |
| `networkpolicy.yaml` | who may reach 9092 |

Read them before applying — the comments carry the reasoning that would otherwise only exist in this document.

```sh
kubectl apply -k k8s/infra/kafka
```

```
namespace/kafka configured
kafkanodepool.kafka.strimzi.io/dual created
kafka.kafka.strimzi.io/streaming created
kafkatopic.kafka.strimzi.io/stream.lifecycle created
kafkatopic.kafka.strimzi.io/upload.events created
kafkatopic.kafka.strimzi.io/stream.start.requests created
kafkatopic.kafka.strimzi.io/transcode.status created
kafkatopic.kafka.strimzi.io/viewer.analytics created
networkpolicy.networking.k8s.io/kafka-clients created
```

Then wait. The first reconcile pulls a Kafka image, provisions an EBS volume and formats the KRaft metadata log; two to four minutes is normal.

```sh
kubectl -n kafka wait kafka/streaming --for=condition=Ready --timeout=600s
```

Watch it while it goes if you want to see the ordering — the `Kafka` CR stays `NotReady` until the node pool's pod is up, and the `KafkaTopic`s stay `NotReady` until the `Kafka` CR is Ready. Both are expected, not errors:

```sh
kubectl -n kafka get pods -w
```

---

## Verify

Three checkpoints. All three must pass.

### 1. The cluster is Ready

```sh
kubectl -n kafka get kafka
```

```
NAME        DESIRED KAFKA REPLICAS   DESIRED ZK REPLICAS   READY   METADATA STATE   WARNINGS
streaming                                                  True    KRaft
```

`READY True` and `METADATA STATE KRaft`. Empty replica columns are correct — those are ZooKeeper-era fields, and with node pools the replica count lives on the `KafkaNodePool`:

```sh
kubectl -n kafka get kafkanodepool
```

```
NAME   DESIRED REPLICAS   ROLES                    NODEIDS
dual   1                  ["controller","broker"]  [0]
```

### 2. Five topics, all Ready

```sh
kubectl -n kafka get kafkatopic
```

```
NAME                    CLUSTER     PARTITIONS   REPLICATION FACTOR   READY
stream.lifecycle        streaming   8            1                    True
stream.start.requests   streaming   8            1                    True
transcode.status        streaming   3            1                    True
upload.events           streaming   8            1                    True
viewer.analytics        streaming   3            1                    True
```

Five rows, `READY True` on every one, and `transcode.jobs` absent.

A `READY` column that is blank or `False` means the Topic Operator has not reconciled it. `kubectl -n kafka describe kafkatopic <name>` puts the reason in `.status.conditions`.

### 3. A client can see them, with the right partition counts

`kubectl get` is asking the operator. This asks Kafka.

```sh
kubectl -n kafka run kcat --rm -it --restart=Never \
  --image=edenhill/kcat:1.7.1 -- \
  -b streaming-kafka-bootstrap.kafka.svc:9092 -L
```

```
Metadata for all topics (from broker 0: streaming-kafka-bootstrap.kafka.svc:9092/0):
 1 brokers:
  broker 0 at streaming-dual-0.streaming-kafka-brokers.kafka.svc:9092 (controller)
 5 topics:
  topic "stream.lifecycle" with 8 partitions:
    partition 0, leader 0, replicas: 0, isrs: 0
    ...
  topic "upload.events" with 8 partitions:
    ...
  topic "stream.start.requests" with 8 partitions:
    ...
  topic "transcode.status" with 3 partitions:
    ...
  topic "viewer.analytics" with 3 partitions:
    ...
```

What to actually read in that output:

- **`1 brokers`** and the broker is `(controller)` — the dual-role node is doing both jobs.
- **`5 topics`**, not 6. `__consumer_offsets` will appear here once a consumer group exists; it does not yet.
- **8 / 8 / 8 / 3 / 3 partitions** in that order.
- **`replicas: 0, isrs: 0`** on every partition — one replica, on broker 0, and it is in-sync. Read as "replica list is [broker 0]", not as "zero replicas".

Prove a round trip while you are here:

```sh
kubectl -n kafka run kcat-p --rm -i --restart=Never --image=edenhill/kcat:1.7.1 -- \
  -b streaming-kafka-bootstrap.kafka.svc:9092 -P -t transcode.status <<< '{"event":"checkpoint"}'

kubectl -n kafka run kcat-c --rm -it --restart=Never --image=edenhill/kcat:1.7.1 -- \
  -b streaming-kafka-bootstrap.kafka.svc:9092 -C -t transcode.status -o beginning -e
```

```
{"event":"checkpoint"}
% Reached end of topic transcode.status [1] at offset 1: exiting
```

Record the value [Module 11](11-workloads.md) needs:

```
KAFKA_BOOTSTRAP_SERVERS=streaming-kafka-bootstrap.kafka.svc:9092
```

And note what is retired: **`KAFKA_CLUSTER_ID` from `.env` is gone.** Strimzi generates and manages the KRaft cluster ID itself. Setting it does nothing; leaving it in a ConfigMap is a lie waiting to confuse someone.

---

## What breaks

### 1. `no matches for kind "KafkaNodePool" in version "kafka.strimzi.io/v1beta2"`

The operator you installed serves a different API version. Confirm:

```sh
kubectl get crd kafkanodepools.kafka.strimzi.io \
  -o jsonpath='{range .spec.versions[*]}{.name}{" served="}{.served}{" storage="}{.storage}{"\n"}{end}'
```

If it says `v1`, you are on Strimzi 1.x. Two changes:

```sh
sed -i '' 's#kafka.strimzi.io/v1beta2#kafka.strimzi.io/v1#' k8s/infra/kafka/*.yaml
```

and then check `kafka.yaml` for anything under `spec.kafka` that belongs to the node pool. In `v1beta2` a stray `spec.kafka.replicas` or `spec.kafka.storage` is ignored; in `v1` it is **rejected**, and the error names the field. The manifests here already have none, but a copied snippet will.

The reverse error — `v1beta2` CRDs and manifests written for `v1` — is the same fix in the other direction.

### 2. The Kafka pod is `Pending` forever

Almost always storage.

```sh
kubectl -n kafka get pvc
kubectl -n kafka describe pvc data-0-streaming-dual-0
```

Look at the Events. Two dominate:

- `storageclass.storage.k8s.io "gp3" not found` — Module 4's StorageClass is missing. This is the single most common cause and the error is at the *PVC*, not at anything named Kafka, which is why people blame the operator.
- `failed to provision volume ... could not create volume ... UnauthorizedOperation` — the EBS CSI driver addon is installed but its IAM role is not attached. `aws eks describe-addon --cluster-name streaming --addon-name aws-ebs-csi-driver --query 'addon.serviceAccountRoleArn'` should return a role ARN, not `null`.

If the PVC is `Bound` and the pod is still `Pending`, it is scheduling instead:

```sh
kubectl -n kafka describe pod streaming-dual-0 | sed -n '/Events:/,$p'
```

`Insufficient memory` means the node group cannot fit a 2 GiB request. The node group is two `t3.medium` (4 GiB each); Kafka at 2 GiB plus the DaemonSets is tight but fits. If it does not, that is a Module 6 (Karpenter) conversation, not a reason to shrink the broker below 2 GiB — a 1 GiB heap with no page cache headroom is worse than no Kafka.

### 3. Topics stay `NotReady`

```sh
kubectl -n kafka describe kafkatopic stream.lifecycle | sed -n '/Status:/,$p'
```

- **`PartitionDecreaseException`** — someone lowered the partition count in the CR. Partitions only go up. Put the original number back; the CR reconciles and the topic was never actually changed.
- **The Topic Operator is not running.** `kubectl -n kafka get pods | grep entity-operator` should show one Running pod with two containers. If `entityOperator.topicOperator` is missing from `kafka.yaml`, the `KafkaTopic` CRs are inert YAML and nothing tells you so.
- **`label strimzi.io/cluster does not match any Kafka cluster`** — a typo in the label. It must equal the `Kafka` CR's `metadata.name`, which is `streaming`.

One behaviour that surprises people: the Topic Operator is **unidirectional**. It reconciles CR → Kafka, not the other way. A topic created directly with `kafka-topics.sh` never gets a CR, and a topic altered directly gets silently reverted on the next reconcile. Manage topics through git or not at all.

### 4. An app logs `Failed to resolve 'kafka:9092'` and then hangs

`KAFKA_BOOTSTRAP_SERVERS` is still the compose value.

```sh
kubectl -n streaming exec deploy/ingest-webhook -- env | grep KAFKA
```

It must be `streaming-kafka-bootstrap.kafka.svc:9092`. Verify the name resolves from the app's namespace, which also proves the Service exists under the name you think it does:

```sh
kubectl -n streaming run dns --rm -it --restart=Never --image=busybox:1.36 -- \
  nslookup streaming-kafka-bootstrap.kafka.svc.cluster.local
```

The failure is quiet: `confluent-kafka` retries name resolution forever and `flush()` blocks. Nothing crashes and nothing logs an error at default level, so the symptom you see is "events never arrive" rather than "cannot connect."

### 5. KEDA reports `error getting kafka client` while the apps work fine

Different client, different namespace. The KEDA operator pod in `keda` is what connects to measure lag.

```sh
kubectl -n keda logs deploy/keda-operator | grep -i kafka
```

Check the NetworkPolicy includes `keda`:

```sh
kubectl -n kafka get networkpolicy kafka-clients -o yaml | grep -A2 namespaceSelector
```

Then check whether the policy is even enforced, because on EKS with the VPC CNI it may not be:

```sh
kubectl -n kube-system get ds aws-node \
  -o jsonpath='{.spec.template.spec.containers[0].args}{"\n"}'
```

If `--enable-network-policy=true` is absent, NetworkPolicy is not enforced at all and `networkpolicy.yaml` is documentation. Know which it is before you spend time debugging a policy that is not running.

The other half of this trap: if anyone sets `spec.kafka.listeners[].networkPolicyPeers` in `kafka.yaml`, Strimzi generates its *own* deny-everything-else policy. Forget `keda` in that list and you get this exact symptom with no policy of yours to blame. That field is deliberately unset here.

### 6. Producers fail with `NOT_ENOUGH_REPLICAS`

`min.insync.replicas` is above the replica count.

```sh
kubectl -n kafka get kafka streaming -o jsonpath='{.spec.kafka.config}{"\n"}' | python3 -m json.tool
```

All four replication settings must be `1`. This also fires if a `KafkaTopic` overrides `min.insync.replicas` in its own `config` block — check the topic as well as the cluster.

The application-side symptom is worth recognising: the producers set `enable.idempotence=True`, which forces `acks=all`, so this presents as `ingest-webhook` blocking on `flush()` during RTMP auth, and MediaMTX refusing the publish. It looks like a streaming problem.

### 7. Everything was fine, then the broker restarted and consumers reset

Check whether the PVC survived:

```sh
kubectl -n kafka get pvc
```

If the PVC is new (recent `AGE`), the volume was recreated and `__consumer_offsets` is empty. That happens if the node pool was deleted with `deleteClaim: true`, or if someone switched storage to `ephemeral`. The manifests set `deleteClaim: false` precisely to make this hard to do by accident.

There is no recovery — offsets are gone. Consumers resume from `auto.offset.reset`, and KEDA's lag numbers are meaningless until each group commits again. Recognising it quickly is the whole win.

---

That completes Part III. You now have a database with a schema, object storage with a CDN in front of it, and a message bus with topics — every stateful dependency the application has. [Module 11](11-workloads.md) puts the services on top of them.
