# Module 4 — Add-ons and the storage blocker

*Part II — Platform. Previous: [Module 3](03-eks-cluster.md). Next: [Module 5](05-dns-and-certificates.md).*

---

## What you're building

A cluster that can actually run things. Right now you have nodes and an API server. By the end of this module the four core add-ons are confirmed healthy, the EBS CSI driver has an IAM role that lets it create disks, `metrics-server` is answering `kubectl top`, and — the part that will otherwise stop you dead in [Module 10](10-kafka-strimzi.md) — a **gp3 StorageClass marked default, with gp2's default flag removed**.

Nothing in this module is visible from the outside. It is entirely plumbing. Do it properly now and the next twelve modules work; skip it and you will debug a `Pending` Kafka pod for an afternoon.

---

## Why it works this way

### The add-ons are a dependency chain, not a checklist

EKS gives you a control plane. It does not give you a working pod network, in-cluster DNS, Service routing, or the ability to attach a disk. Those are *add-ons*, and they layer strictly:

| Order | Add-on | What it does | Depends on |
|---|---|---|---|
| 1 | **VPC CNI** (`vpc-cni`) | Gives every pod a real IP address from your VPC subnets | Nothing. Nothing else works without it. |
| 2 | **CoreDNS** (`coredns`) | Resolves `upload-api.streaming.svc.cluster.local` | Runs as pods, so it needs the CNI first |
| 3 | **kube-proxy** (`kube-proxy`) | Programs iptables/nftables so Service ClusterIPs route | CNI |
| 4 | **Pod Identity Agent** (`eks-pod-identity-agent`) | Hands AWS credentials to pods that ask | CNI |
| 5 | **EBS CSI driver** (`aws-ebs-csi-driver`) | Creates and attaches EBS volumes for PVCs | Pod Identity (or IRSA) for its IAM permissions |

That order is why a broken CNI presents as "CoreDNS is CrashLooping" and a broken CoreDNS presents as "every service in the cluster can't reach the database". When something is wrong, check the chain from the top.

**The VPC CNI is the one that shapes your network design.** Unlike Calico on the kubeadm box in `k8s/cluster/`, which gave pods addresses from an overlay network invisible to the rest of the LAN, the VPC CNI assigns each pod a **secondary IP on the node's ENI**, taken from the same subnet the node sits in. Pods are first-class VPC citizens: a security group rule can target one, an ALB can send traffic straight to one, and a `t3.medium` can hold at most 17 pods because that is how many secondary IPs three ENIs give you. This is also why [Module 2](02-vpc-and-networking.md) made the private subnets `/19` instead of something tidy — every pod eats a real VPC address.

`eksctl create cluster` in [Module 3](03-eks-cluster.md) installed all five. Your job here is to confirm that, not to install them, and then to fix the one thing EKS does not do for you.

### Pod Identity vs IRSA — you will use both, deliberately

Two mechanisms exist for giving a pod AWS credentials.

**IRSA** (IAM Roles for Service Accounts) is the older one: the cluster gets an OIDC identity provider, each IAM role's *trust policy* names that provider plus a specific `namespace:serviceaccount`, and the pod exchanges a projected token for credentials. It works everywhere but the trust policy is per-cluster, so roles are not reusable and the JSON is fiddly.

**EKS Pod Identity** is the current answer: no OIDC provider, no trust-policy editing. The role trusts the service principal `pods.eks.amazonaws.com`, and you bind role-to-service-account with a plain AWS API call (`aws eks create-pod-identity-association`). A DaemonSet on each node serves the credentials.

Use Pod Identity everywhere you can. Two things force IRSA anyway:

| Component | Mechanism | Reason |
|---|---|---|
| EBS CSI driver | **Pod Identity** | Natively supported by the managed add-on |
| External Secrets Operator ([Module 7](07-secrets.md)) | **Pod Identity** | Supported, and it lets you delete the `auth:` block entirely |
| ExternalDNS ([Module 5](05-dns-and-certificates.md)) | **Pod Identity** | Supported |
| AWS Load Balancer Controller ([Module 5](05-dns-and-certificates.md)) | **IRSA** | Does not support Pod Identity |
| Karpenter ([Module 6](06-karpenter.md)) | **IRSA** | The official CloudFormation getting-started path wires IRSA |
| `upload-api`, `transcode-worker` ([Module 11](11-workloads.md)) | **Pod Identity** | Your own workloads. Simplest thing that works. |

That mixture is a compatibility fact, not a preference, and it is why `iam.withOIDC: true` is in the cluster config even though Pod Identity is the modern default. If you find yourself wondering "which one does this thing use", this table is the answer.

### The storage blocker

**A fresh EKS cluster's default StorageClass is gp2. Nothing in `k8s/cluster/` provisions storage at all.** Compose used named Docker volumes — `postgres-data`, `minio-data`, `kafka-data` — which are just directories on the host, created implicitly, never sized, never thought about. The kubeadm cluster never got as far as needing a PVC. So this is the first time anyone on this project has had to answer the question "where does a stateful pod's disk come from".

Two things go wrong if you leave the default alone.

**First, gp2 is the wrong volume type.** It costs $0.10/GiB-month against gp3's $0.08 — 25% more — and its IOPS are a function of size: 3 IOPS per GiB, so the 20 GiB volume Kafka gets in [Module 10](10-kafka-strimzi.md) is capped at **100 IOPS**. gp3 gives you 3000 IOPS and 125 MB/s at *any* size, included in the price. Kafka does small synchronous writes to its log segments; 100 IOPS is a number you will feel, as producer latency spikes that look like a network problem.

**Second, and worse, `volumeBindingMode`.** The gp2 class EKS ships uses `Immediate`, which means the EBS volume is created the moment the PVC is created — before the scheduler has picked a node. **EBS volumes live in exactly one Availability Zone and cannot be attached across AZs.** Your cluster spans three. So the sequence is:

1. Strimzi creates a PVC for the Kafka broker.
2. The provisioner immediately creates a 20 GiB volume in, say, `us-east-1c`.
3. The scheduler looks for a node for the Kafka pod and finds a good one in `us-east-1a`.
4. The volume cannot follow. The pod is unschedulable **forever**, with `node(s) had volume node affinity conflict`.

`WaitForFirstConsumer` inverts the order: the PVC stays `Pending` until a pod that uses it is scheduled, *then* the volume is created in the AZ the scheduler chose. One line, and the entire failure mode disappears. This is the single most common EKS storage failure and it is fully preventable.

### What a `Pending` PVC actually looks like

You will see this at least once, so learn to read it now rather than at 11pm in Module 10.

```
$ kubectl get pvc -n kafka
NAME                          STATUS    VOLUME   CAPACITY   ACCESS MODES   STORAGECLASS   AGE
data-0-streaming-dual-0       Pending                                                     4m
```

An empty `STORAGECLASS` column is the tell: no class was requested and there is no default, so nothing is even trying to provision it. The pod above it says `pod has unbound immediate PersistentVolumeClaims` and Strimzi gives you no useful signal at all — the operator is happy, the Kafka CR looks fine, and one pod sits there.

`kubectl describe` is where the truth lives. The **Events** block at the bottom is the part you read:

```
$ kubectl describe pvc data-0-streaming-dual-0 -n kafka
...
Events:
  Type     Reason              Age   From                         Message
  ----     ------              ----  ----                         -------
  Warning  ProvisioningFailed  30s   persistentvolume-controller  storageclass.storage.k8s.io "" not found
```

Three messages, three different problems:

| Event message | Means |
|---|---|
| `storageclass.storage.k8s.io "" not found` | No default StorageClass. This module fixes it. |
| `waiting for first consumer to be created before binding` | **Normal.** `WaitForFirstConsumer` doing its job. Look at the *pod*, not the PVC. |
| `could not create volume in EC2: UnauthorizedOperation` | EBS CSI driver has no IAM permissions. See the Pod Identity step below. |

The second one catches people constantly. A `Pending` PVC with that message is not broken — it is waiting for you to fix whatever is stopping the pod from being scheduled, which is usually something else entirely.

---

## Do it

Everything below assumes `AWS_PROFILE=linkify-streaming` and `AWS_REGION=us-east-1` are exported, and `kubectl` points at the `streaming` cluster.

### 4.1 — Confirm the core add-ons

```sh
aws eks list-addons --cluster-name streaming
```

```
{
    "addons": [
        "aws-ebs-csi-driver",
        "coredns",
        "eks-pod-identity-agent",
        "kube-proxy",
        "vpc-cni"
    ]
}
```

If any of those five is missing, `eksctl create cluster` did not do what Module 3 said it would — go back and re-read the `addons:` block in `cluster.yaml` before continuing.

Now check they are actually *running*, not just registered:

```sh
kubectl get pods -n kube-system
```

Every pod `Running`, every `READY` column showing all containers up. `aws-node` (the CNI) and `kube-proxy` and `eks-pod-identity-agent` are DaemonSets, so you get one of each per node; `coredns` is a 2-replica Deployment; `ebs-csi-node` is a DaemonSet and `ebs-csi-controller` a Deployment.

Add-ons have versions, and the right version depends on your Kubernetes version. Rather than pasting a version here that will be stale by the time you read it, ask:

```sh
aws eks describe-addon-versions \
  --addon-name aws-ebs-csi-driver \
  --kubernetes-version 1.35 \
  --query 'addons[0].addonVersions[?compatibilities[0].defaultVersion==`true`].addonVersion' \
  --output text
```

Compare against what you have (`aws eks describe-addon --cluster-name streaming --addon-name aws-ebs-csi-driver --query 'addon.addonVersion'`). If yours is older, that is fine for now — the default version is chosen by AWS as a safe one, not the newest. Do not chase versions in the middle of a build.

### 4.2 — Give the EBS CSI driver an IAM role

The driver's controller pod calls `ec2:CreateVolume`, `ec2:AttachVolume`, `ec2:DescribeVolumes` and friends. Out of the box it has no credentials at all. Everything below silently works right up until the first PVC, at which point you get `UnauthorizedOperation`.

Create the role with a **Pod Identity** trust policy — note the principal is `pods.eks.amazonaws.com`, not an OIDC provider:

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
  --role-name streaming-ebs-csi-driver \
  --assume-role-policy-document file:///tmp/pod-identity-trust.json

aws iam attach-role-policy \
  --role-name streaming-ebs-csi-driver \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy
```

`sts:TagSession` in the trust policy is required — Pod Identity tags the session with the cluster and namespace. Leave it out and the association is created successfully but credential vending fails with an unhelpful `AccessDenied`.

Keep that trust policy file. Modules [5](05-dns-and-certificates.md) and [7](07-secrets.md) reuse it verbatim.

Bind the role to the driver's ServiceAccount:

```sh
aws eks create-pod-identity-association \
  --cluster-name streaming \
  --namespace kube-system \
  --service-account ebs-csi-controller-sa \
  --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/streaming-ebs-csi-driver"
```

The service account name is `ebs-csi-controller-sa`. It is created by the add-on; you do not make it. Getting the name wrong produces no error — the association is created, it just never matches a pod.

Existing pods do not pick up a new association. Restart the controller:

```sh
kubectl -n kube-system rollout restart deploy/ebs-csi-controller
kubectl -n kube-system rollout status deploy/ebs-csi-controller
```

Confirm the association exists and points where you think:

```sh
aws eks list-pod-identity-associations --cluster-name streaming \
  --query 'associations[].[namespace,serviceAccount,roleArn]' --output table
```

### 4.3 — gp3 as default, gp2 demoted

The manifest is in the repo at `k8s/infra/storageclass-gp3.yaml`:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: ebs.csi.aws.com
# Do not create the EBS volume until a pod that wants it has been scheduled.
# EBS volumes are single-AZ; binding early pins the pod to whichever AZ the
# volume happened to land in.
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: Delete
parameters:
  type: gp3
  iops: "3000"          # free gp3 baseline
  throughput: "125"     # free gp3 baseline, MB/s
  encrypted: "true"
  fsType: ext4
```

Line by line, the choices that matter:

- **`volumeBindingMode: WaitForFirstConsumer`** — the AZ fix described above. Not optional.
- **`iops: 3000` / `throughput: 125`** — the *free* gp3 baseline. You pay $0.08/GiB-month for capacity and nothing for these. Raising either bills separately, so do not raise them without a measured reason.
- **`allowVolumeExpansion: true`** — lets you grow a PVC later by editing it. Costs nothing to enable, impossible to add retroactively to a bound volume's class without recreating the class.
- **`reclaimPolicy: Delete`** — deleting the PVC deletes the EBS volume. Correct for a learning cluster; it is also how you avoid paying for orphaned volumes forever. Note that Strimzi's `deleteClaim: false` keeps the *PVC* around when the Kafka cluster is deleted, so this policy does not silently eat your Kafka data.
- **`encrypted: "true"`** — EBS encryption with the account's default KMS key. Free, and there is no reason to say no.

Apply it, and take gp2's crown away:

```sh
kubectl patch storageclass gp2 \
  -p '{"metadata":{"annotations":{"storageclass.kubernetes.io/is-default-class":"false"}}}'

kubectl apply -f k8s/infra/storageclass-gp3.yaml
```

Order matters slightly: patch gp2 first. If both classes claim the default at once, Kubernetes picks one arbitrarily and a PVC created in that window can land on gp2.

### 4.4 — metrics-server

`kubectl top` needs it, and so does any future HorizontalPodAutoscaler. It is a single small Deployment.

```sh
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
kubectl -n kube-system rollout status deploy/metrics-server
```

If you would rather pin a version than track `latest` — reasonable, since this is the one URL in this module that moves under you — check what the current release is and substitute it:

```sh
curl -fsSL https://api.github.com/repos/kubernetes-sigs/metrics-server/releases/latest \
  | grep '"tag_name"'
```

On EKS this works with no arguments. You will find a great deal of internet advice telling you to add `--kubelet-insecure-tls`; that is for kubeadm clusters like the one in `k8s/cluster/`, whose kubelets have self-signed serving certificates. The EKS-optimized AMI's kubelet has a properly signed cert. **If you find yourself adding `--kubelet-insecure-tls` on EKS, you are papering over a different problem** — almost always a security group that is blocking port 10250 from the control plane to the nodes.

Metrics take about 30 seconds to populate after the pod is Ready.

---

## Verify

Four checks. All four must pass before [Module 5](05-dns-and-certificates.md).

**1. Exactly one default StorageClass, and it is gp3.**

```sh
kubectl get storageclass
```

```
NAME            PROVISIONER             RECLAIMPOLICY   VOLUMEBINDINGMODE      ALLOWVOLUMEEXPANSION   AGE
gp2             kubernetes.io/aws-ebs   Delete          WaitForFirstConsumer   false                  41m
gp3 (default)   ebs.csi.aws.com         Delete          WaitForFirstConsumer   true                   30s
```

`(default)` appears on gp3 and **nowhere else**. If it appears twice, or not at all, stop here — everything downstream that needs a disk will fail in a way that does not mention StorageClasses.

**2. A PVC actually provisions.** The StorageClass existing does not prove the driver works. Prove it:

```sh
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: storage-smoke-test
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: v1
kind: Pod
metadata:
  name: storage-smoke-test
spec:
  containers:
    - name: writer
      image: public.ecr.aws/docker/library/busybox:1.36
      command: ["sh", "-c", "echo ok > /data/proof && sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: storage-smoke-test
EOF

kubectl wait --for=condition=Ready pod/storage-smoke-test --timeout=180s
kubectl get pvc storage-smoke-test
```

```
NAME                 STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
storage-smoke-test   Bound    pvc-3f1c9a02-6d7e-4f8a-9b21-0c5d4e7a1b33   1Gi        RWO            gp3            48s
```

`Bound`, capacity `1Gi`, class `gp3`. Note that it stayed `Pending` until the pod appeared — that is `WaitForFirstConsumer` working, not a fault.

Clean up, or you pay $0.08/month forever for a 1 GiB volume you forgot:

```sh
kubectl delete pod storage-smoke-test
kubectl delete pvc storage-smoke-test
```

**3. `kubectl top` returns numbers.**

```sh
kubectl top nodes
```

```
NAME                          CPU(cores)   CPU%   MEMORY(bytes)   MEMORY%
ip-10-0-33-14.ec2.internal    58m          2%     742Mi           19%
ip-10-0-66-201.ec2.internal   71m          3%     801Mi           21%
```

**4. Pod Identity association is bound.**

```sh
aws eks list-pod-identity-associations --cluster-name streaming \
  --query 'associations[?serviceAccount==`ebs-csi-controller-sa`]' --output table
```

One row. If check 2 passed, this is already proven — the driver could not have created a volume without it.

---

## What breaks

Ordered by how often each actually happens.

### 1. PVC stuck `Pending`, `STORAGECLASS` column empty

No default class, or two of them. By far the most common.

```sh
kubectl get storageclass
kubectl describe pvc <name> -n <namespace> | tail -20
```

If `(default)` is on neither class, re-apply `k8s/infra/storageclass-gp3.yaml`. If it is on both, patch gp2 again — an EKS add-on update can restore it. Note that an already-created PVC does **not** retroactively pick up a newly created default class; delete and recreate the PVC (and for Strimzi, delete the pod so the operator recreates its claim).

### 2. `node(s) had volume node affinity conflict`

The AZ mismatch. It means a volume was bound before its pod was scheduled, which means something used a class with `volumeBindingMode: Immediate`.

```sh
kubectl describe pod <name> -n <namespace> | grep -A5 Events
kubectl get pv <pv-name> -o jsonpath='{.spec.nodeAffinity}' | jq
```

The `nodeAffinity` block names one AZ. Compare with `kubectl get nodes -L topology.kubernetes.io/zone`. There is no fix for the existing volume — delete the PVC and the pod, confirm the class the workload requests is `gp3`, and let it re-provision. If a chart hardcodes `gp2`, override it; do not "fix" gp2 by editing its binding mode, because a StorageClass's `volumeBindingMode` is immutable and the patch will be rejected.

### 3. `could not create volume in EC2: UnauthorizedOperation`

The EBS CSI driver has no IAM permissions, or has them but did not restart to pick them up.

```sh
kubectl -n kube-system logs deploy/ebs-csi-controller -c csi-provisioner --tail=50
aws eks list-pod-identity-associations --cluster-name streaming
```

Check the association's `serviceAccount` is exactly `ebs-csi-controller-sa` and the namespace is `kube-system`. Then `kubectl -n kube-system rollout restart deploy/ebs-csi-controller` — an association created after the pod started does nothing until the pod is recreated.

### 4. `kubectl top` says `Metrics API not available`

```sh
kubectl -n kube-system get pods -l k8s-app=metrics-server
kubectl -n kube-system logs deploy/metrics-server --tail=30
kubectl get apiservice v1beta1.metrics.k8s.io -o yaml | grep -A5 status
```

If the log shows TLS or timeout errors reaching port 10250 on the nodes, that is a security group problem between the control plane and the node group, not a metrics-server problem. Wait 60 seconds after the pod becomes Ready before deciding it is broken — the first scrape has not happened yet.

### 5. CoreDNS pods `Pending`

CoreDNS has a hard anti-affinity that prevents two replicas on the same node, so a single-node cluster leaves one replica `Pending` permanently. That is cosmetic. If **both** are Pending, you have no schedulable nodes at all:

```sh
kubectl get nodes
kubectl -n kube-system describe pod -l k8s-app=kube-dns | grep -A10 Events
```

### 6. Pods stuck `ContainerCreating`, events mention `failed to assign an IP address`

VPC CNI IP exhaustion: the private subnet has run out of addresses, or the instance type's ENI limit is reached.

```sh
kubectl describe pod <name> -n <namespace> | grep -A10 Events
kubectl -n kube-system logs -l k8s-app=aws-node --tail=50
aws ec2 describe-subnets --filters "Name=tag:kubernetes.io/role/internal-elb,Values=1" \
  --query 'Subnets[].[SubnetId,AvailableIpAddressCount]' --output table
```

If `AvailableIpAddressCount` is healthy in the hundreds, it is the per-instance ENI limit instead — a `t3.medium` tops out around 17 pods including DaemonSets. That is a node-size problem, and [Module 6](06-karpenter.md) is where you get more nodes.

---

**Next:** [Module 5 — DNS, certificates, load balancer controller](05-dns-and-certificates.md).
