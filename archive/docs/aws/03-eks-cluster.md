# Module 3 — The EKS cluster

## What you're building

An EKS cluster named `streaming`, running Kubernetes 1.35, in a VPC that matches the design from [Module 2](02-vpc-and-networking.md), with two `t3.medium` nodes in a managed node group. One file — `infra/eks/cluster.yaml` — and one command produce all of it: VPC, subnets with the right tags, IAM roles, the OIDC provider, the control plane, the node group, access entries, and the core add-ons.

It takes 15–20 minutes and it is the first thing in this course that costs money. The EKS control plane starts billing at $0.10/hour ($73/month) the moment the API server is up, whether or not you ever run a pod on it.

## Why it works this way

### eksctl, not Terraform

Terraform is the better tool in a working environment with more than one engineer and a state backend that somebody maintains. This is not that, and here's the specific reasoning for choosing `eksctl` anyway:

**One file, one command, and the failure mode is readable.** `eksctl` drives CloudFormation. When something fails, it fails as a stack event with a message — `AccessDenied` on a specific action, `LimitExceeded` on a specific quota — that you can read in the CloudFormation console and act on. Terraform's failure mode on EKS is frequently a partially-applied plan and a state file that no longer describes reality, and recovering from that requires understanding Terraform's state model before you have finished understanding EKS.

**The comparison is not 1 file vs 1 file.** The `terraform-aws-modules/eks` module needs roughly 150 lines across four files, plus a VPC module, plus an S3 backend with a DynamoDB lock table that you have to create first — and you need `for_each` and module composition to be second nature before any of it reads clearly. That's a real amount of Terraform to learn in order to learn EKS.

**eksctl has first-class support for the two modern features this design depends on** — `accessConfig.authenticationMode: API` and `iam.podIdentityAssociations` — at about three lines each.

The honest cost, which you should say out loud rather than discover: **eksctl has no state file, so drift is unmanaged.** If you click something in the console, eksctl will neither know nor reconcile it. Three habits contain that:

1. `infra/eks/cluster.yaml` lives in git and is treated as the truth. If you change the cluster, change the file.
2. Cluster-level changes go through `eksctl upgrade cluster` / `eksctl create nodegroup`, not the console.
3. Everything *above* the cluster is declarative by other means — Helm with values files in git for add-ons ([Module 4](04-addons-and-storage.md) onward), Kustomize for workloads ([Module 11](11-workloads.md)), ArgoCD reconciling both ([Module 15](15-argocd-gitops.md)).

Which tool owns what:

| Layer | Tool | Why |
|---|---|---|
| VPC, cluster, node group, OIDC, access entries, EKS add-ons | `eksctl` + `cluster.yaml` | One declarative file |
| Karpenter, LB Controller, ExternalDNS, ESO, Strimzi, metrics-server | `helm` + values files in git | Versioned, upgradeable, `helm diff` works |
| Workloads, StorageClass, NodePools, Secrets | `kubectl apply -k` (Kustomize) | Plain YAML in git |
| RDS, S3, CloudFront, Route 53, ACM, Secrets Manager | `aws` CLI, scripted under `infra/aws/` | ~8 one-time resources; a Terraform module for these is more machinery than the resources are worth |

### Kubernetes 1.35

EKS currently offers 1.33 (standard support ending now), 1.34, 1.35, and 1.36.

Take **1.35**, for a reason specific to this project rather than a general one: `k8s/cluster/kubeadm-config.yaml` pins `kubernetesVersion: v1.35.7`. Matching it means every manifest, every API version, and every `kubectl` behaviour you already learned on the kubeadm box transfers to EKS unchanged. Migration debugging is hard enough without simultaneously chasing API deprecations. 1.35 has roughly eight months of standard support left, which outlives this project; upgrading later is `eksctl upgrade cluster --version=1.36`.

Two 1.35-specific notes:

- **containerd 1.x support ends with 1.35.** The EKS-optimised AL2023 AMIs already ship containerd 2.x, so it is a non-issue for you — but it is why you shouldn't sit on 1.35 past 1.37.
- **Upstream Ingress NGINX was retired in March 2026** — no further releases, bug fixes or security patches. That's why [Module 12](12-ingress-and-rtmp.md) uses the AWS Load Balancer Controller and an ALB rather than the ingress-nginx everyone's blog posts assume.

Do not reach for 1.36 on the theory that newer is better. It enables `StrictIPCIDRValidation` by default (non-canonical CIDR notation in manifests is now rejected — the kind of thing that was fine in your `metallb-pool.yaml` and now isn't) and it removes kube-proxy's IPVS mode.

### `authenticationMode: API` — and why every old blog post is wrong here

This is the single most important stanza in the file, and it is where existing documentation on the internet will actively mislead you.

For most of EKS's life, mapping an IAM principal to a Kubernetes identity was done through a ConfigMap called `aws-auth` in `kube-system`. You edited YAML that mapped role ARNs to Kubernetes usernames and groups. Every EKS tutorial written before 2024 describes this, and a great many written since still do.

It has one catastrophic property: **it is a ConfigMap, and it is the thing that authorises access to the cluster.** Corrupt it — a typo in an ARN, a botched `kubectl edit`, a bad `apply` — and you have removed your own access to the cluster. You can no longer edit the ConfigMap, because editing it requires the access it grants. There is no AWS-side recovery path. The remediation is to delete the cluster and build a new one.

**EKS Access Entries** replace it with a real AWS API. `aws eks create-access-entry` and `aws eks associate-access-policy` are IAM-authorised calls, recorded in CloudTrail, and fixable by any principal holding `eks:*` — which is an entirely separate axis from whatever is inside the cluster. You cannot lock yourself out with a typo.

Three modes exist:

| Mode | Behaviour |
|---|---|
| `CONFIG_MAP` | Legacy. `aws-auth` only. |
| `API_AND_CONFIG_MAP` | Both consulted. Exists for migrating an existing cluster. |
| `API` | Access entries only. `aws-auth` is not read. |

Use **`API`**. The hybrid mode's whole purpose is to let a running cluster migrate gradually; a green-field cluster has nothing to migrate, and a dual source of truth for "who can access this" is exactly the confusion you are trying to avoid. When you later read a blog post telling you to `kubectl edit configmap aws-auth`, the correct response on this cluster is that the ConfigMap does not do anything.

`bootstrapClusterCreatorAdminPermissions: true` gives the principal that runs `eksctl create cluster` — you — cluster-admin automatically. The extra `accessEntries` block adds your SSO role a second time, explicitly. That is not redundant: the bootstrap entry is tied to the exact principal that created the cluster, and if that principal is ever deleted or its SSO role is recreated with a new suffix, the explicit entry is what keeps you in.

This is where you need the role ARN you saved in [Module 1](01-aws-account-setup.md) — the `arn:aws:iam::...:role/aws-reserved/sso.amazonaws.com/us-east-1/AWSReservedSSO_...` form, **not** the `arn:aws:sts::...:assumed-role/.../intern` form that `get-caller-identity` prints. Access entries reject the STS session ARN, and the error does not spell out why.

### `iam.withOIDC: true` — still needed, even with Pod Identity

The cluster gets two mechanisms for giving pods AWS permissions, and it uses both deliberately.

**EKS Pod Identity** is the modern one: an agent runs as a DaemonSet (`eks-pod-identity-agent`, in the add-ons list), and you associate a ServiceAccount with an IAM role through a plain AWS API call. No OIDC provider, no trust-policy editing, roles are reusable across clusters. It's what `upload-api` and `transcode-worker` will use in [Module 9](09-s3-and-cloudfront.md), and what makes the `s3_client()` rewrite from [Module 0](00-preflight-code-changes.md) necessary.

**IRSA** (IAM Roles for Service Accounts) is the older mechanism: the cluster publishes an OIDC identity document, you register it with IAM as an identity provider, and each role's trust policy names the cluster's OIDC issuer plus a specific ServiceAccount. More moving parts, and the trust policy is fiddly.

You need OIDC anyway, because the **AWS Load Balancer Controller** and **Karpenter** still expect IRSA. `iam: withOIDC: true` creates and registers the provider at cluster-creation time. Adding it later is possible but means a separate `eksctl utils associate-iam-oidc-provider` run, so turn it on now.

### Control-plane logging, and the retention note

`cloudWatch.clusterLogging` turns on control-plane log types. Three are worth having:

- **`api`** — API server logs. Requests, errors, admission failures.
- **`audit`** — who changed what, when. The only way to answer "what deleted my Deployment".
- **`authenticator`** — authentication decisions. When an access entry doesn't work, this is where the reason is.

`controllerManager` and `scheduler` are omitted; they're high-volume and rarely what you need on a cluster this size.

**Set `logRetentionInDays`.** CloudWatch log groups default to "Never expire". At $0.03/GB/month for storage, audit logs on a cluster you leave running will quietly accumulate a bill line that nobody attributes to EKS. Seven days is enough to debug something that happened while you were asleep.

### Two `t3.medium` nodes, and why it's tight on purpose

Two nodes, one in each of `us-east-1a` and `us-east-1b`, 2 vCPU and 4 GiB each. Here's what has to fit on them by the end of the course:

| Component | Pods | Memory request each |
|---|---|---|
| CoreDNS | 2 | 70 Mi |
| AWS Load Balancer Controller | 2 | 100 Mi |
| External Secrets (controller, webhook, cert-controller) | 3 | 64 Mi |
| ExternalDNS | 1 | 64 Mi |
| Karpenter | 2 | 512 Mi |
| metrics-server | 1 | 200 Mi |
| Strimzi cluster operator | 1 | 384 Mi |
| Kafka broker (KRaft, 1 replica) | 1 | 1.5 Gi |
| Strimzi entity operator | 1 | 256 Mi |
| MediaMTX | 1 | 256 Mi |
| `web`, `upload-api`, `ingest-webhook` | 2 each | 256 Mi |
| `analytics-worker` | 1 | 128 Mi |

That is roughly **6.5 GiB of requests against about 7 GiB allocatable** (8 GiB total minus what the kubelet and system reserve). It fits, barely, and you should design around that rather than be surprised by it:

- If you hit memory pressure, run **one** replica of the LB Controller, the ESO webhook and Karpenter. A brief control-plane gap during a rolling update is a better trade than not fitting.
- `maxSize: 4` is set, but there is **no Cluster Autoscaler in this design** — Karpenter handles burst capacity for transcoding on its own NodePools, and this group is fixed. When it's genuinely full you bump it by hand with `eksctl scale nodegroup --cluster streaming --name system --nodes 3`. That's honest and it's cheap.
- Recall from [Module 2](02-vpc-and-networking.md) that a `t3.medium` tops out at **17 pods** for IP reasons, well above the ~9 per node here. Memory is the binding constraint, not addresses.

The node group is deliberately **not tainted**. Karpenter, CoreDNS, the LB controller, ESO, Strimzi and every stateless app pod live here, so a taint would mean adding a matching toleration to every chart and every manifest — churn with no benefit at this size. Taints start earning their keep in [Module 6](06-karpenter.md), on the transcode NodePools.

**Graviton is a ~30% saving you cannot take yet.** A `t4g.medium` is $0.0336/hour against `t3.medium` at $0.0416. But `.github/workflows/ci.yml` uses `docker/build-push-action@v6` with no `platforms:` key, so it produces **amd64-only images**, and an arm64 node would fail every pod with `exec format error`. To take the saving you'd add `platforms: linux/amd64,linux/arm64` to the build step — `confluent-kafka` has arm64 wheels and ffmpeg installs from Debian arm64, so it should work. Follow-up, not a day-one task.

---

## Do it

### 3.1 — Write the config file

`infra/eks/cluster.yaml` is in this repo. Read it end to end before you apply it — every stanza is explained above, and applying a cluster config you haven't read is how you end up with a cluster you can't reason about.

```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
  name: streaming
  region: us-east-1
  version: "1.35"
  tags:
    Project: streaming

# EKS Access Entries, not the legacy aws-auth ConfigMap.
accessConfig:
  authenticationMode: API
  bootstrapClusterCreatorAdminPermissions: true
  accessEntries:
    - principalARN: arn:aws:iam::<accountid>:role/aws-reserved/sso.amazonaws.com/us-east-1/AWSReservedSSO_<SSO_ROLE_SUFFIX>
      accessPolicies:
        - policyARN: arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy
          accessScope:
            type: cluster

vpc:
  cidr: 10.42.0.0/16
  nat:
    gateway: Single
  clusterEndpoints:
    publicAccess: true
    privateAccess: true
  publicAccessCIDRs:
    - 0.0.0.0/0

iam:
  withOIDC: true

managedNodeGroups:
  - name: system
    instanceType: t3.medium
    amiFamily: AmazonLinux2023
    desiredCapacity: 2
    minSize: 2
    maxSize: 4
    volumeSize: 40
    volumeType: gp3
    volumeEncrypted: true
    privateNetworking: true
    availabilityZones: [us-east-1a, us-east-1b]
    labels:
      workload: system
    tags:
      Project: streaming
      karpenter.sh/discovery: ""

cloudWatch:
  clusterLogging:
    enableTypes: ["api", "audit", "authenticator"]
    logRetentionInDays: 7

addons:
  - name: vpc-cni
    version: latest
    configurationValues: |-
      {"env":{"ENABLE_PREFIX_DELEGATION":"false","WARM_ENI_TARGET":"1"}}
  - name: coredns
    version: latest
  - name: kube-proxy
    version: latest
  - name: eks-pod-identity-agent
    version: latest
  - name: aws-ebs-csi-driver
    version: latest
    podIdentityAssociations:
      - namespace: kube-system
        serviceAccountName: ebs-csi-controller-sa
        permissionPolicyARNs: ["arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"]
```

Two things to substitute before you apply:

```sh
export AWS_PROFILE=linkify-streaming
export AWS_REGION=us-east-1

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
SSO_ROLE_ARN=$(aws iam list-roles --path-prefix /aws-reserved/sso.amazonaws.com/ \
  --query 'Roles[?starts_with(RoleName, `AWSReservedSSO_AdministratorAccess`)].Arn' --output text)

echo "$ACCOUNT_ID"
echo "$SSO_ROLE_ARN"
```

Put `$SSO_ROLE_ARN` into `principalARN` verbatim and `$ACCOUNT_ID` wherever `<accountid>` appears. Confirm the file parses before you spend 20 minutes on it:

```sh
eksctl create cluster -f infra/eks/cluster.yaml --dry-run > /dev/null && echo "config OK"
```

A note on `addons[].podIdentityAssociations`: this field needs a recent eksctl (the ≥0.200 pinned in [Module 1](01-aws-account-setup.md)). If `--dry-run` rejects it, check `eksctl version` before you assume the field is wrong — and check the eksctl schema docs for the current spelling rather than guessing at an alternative.

### 3.2 — Create the cluster

```sh
eksctl create cluster -f infra/eks/cluster.yaml
```

**This takes 15–20 minutes.** Leave it running; do not interrupt it. What it is doing, in order:

1. **CloudFormation stack `eksctl-streaming-cluster`** — the VPC, an internet gateway, six subnets across three AZs, one NAT gateway, route tables, the cluster security group, and the IAM role the control plane assumes. Roughly 4 minutes.
2. **The EKS control plane itself** — AWS provisions API server and etcd instances across multiple AZs in their own account. This is the slow part, 8–10 minutes, and there is nothing to watch.
3. **The OIDC provider** — registered with IAM from the cluster's issuer URL.
4. **CloudFormation stack `eksctl-streaming-nodegroup-system`** — the node IAM role, launch template and an EC2 Auto Scaling group. The two instances boot the AL2023 EKS-optimised AMI, whose bootstrap script joins them to the cluster. 3–5 minutes.
5. **Add-ons** — `vpc-cni`, `coredns`, `kube-proxy`, `eks-pod-identity-agent`, `aws-ebs-csi-driver`, plus the EBS CSI driver's Pod Identity association.
6. **Kubeconfig** — eksctl writes a context into `~/.kube/config` and makes it current.

The output is verbose and mostly reassuring. The lines worth reading are anything containing `waiting for CloudFormation stack` (normal), and anything containing `error` (not).

If you want to watch progress rather than stare at scrolling text, the CloudFormation console shows stack events live, and that is also where a failure will explain itself.

### 3.3 — Point kubectl at it

eksctl does this for you, but do it explicitly so you know the command:

```sh
aws eks update-kubeconfig --name streaming --region us-east-1
kubectl config current-context
```

```
arn:aws:eks:us-east-1:123456789012:cluster/streaming
```

### 3.4 — Immediately: the Module 2 post-creation steps

The S3 Gateway endpoint and the `karpenter.sh/discovery` tags belong to the VPC design and are documented in [Module 2](02-vpc-and-networking.md#do-it). Run them now, before anything writes to S3. The endpoint is the difference between $0 and $0.19/hour per live stream.

---

## Verify

Both checks must pass before [Module 4](04-addons-and-storage.md).

**1. Two nodes, both `Ready`.**

```sh
kubectl get nodes -o wide
```

```
NAME                          STATUS   ROLES    AGE   VERSION               INTERNAL-IP    EXTERNAL-IP   OS-IMAGE                       KERNEL-VERSION                    CONTAINER-RUNTIME
ip-10-42-45-183.ec2.internal  Ready    <none>   3m    v1.35.7-eks-a1b2c3d   10.42.45.183   <none>        Amazon Linux 2023.9.20260715   6.1.134-152.225.amzn2023.x86_64   containerd://2.0.5
ip-10-42-77-9.ec2.internal    Ready    <none>   3m    v1.35.7-eks-a1b2c3d   10.42.77.9     <none>        Amazon Linux 2023.9.20260715   6.1.134-152.225.amzn2023.x86_64   containerd://2.0.5
```

Four things in that output, not just `Ready`:

- **Two nodes.** One means the second AZ's instance failed to join.
- **`EXTERNAL-IP` is `<none>`** on both. `privateNetworking: true` worked; the nodes are not internet-addressable.
- **`INTERNAL-IP` in `10.42.x.x`**, in two different /19 ranges — one node per AZ.
- **`VERSION` is `v1.35.x`**, and within one minor of your `kubectl version --client`.

Confirm the AZ spread explicitly:

```sh
kubectl get nodes -L topology.kubernetes.io/zone
```

```
NAME                           STATUS   ROLES    AGE   VERSION               ZONE
ip-10-42-45-183.ec2.internal   Ready    <none>   3m    v1.35.7-eks-a1b2c3d   us-east-1a
ip-10-42-77-9.ec2.internal     Ready    <none>   3m    v1.35.7-eks-a1b2c3d   us-east-1b
```

**2. The core system pods are running.**

```sh
kubectl get pods -A
```

```
NAMESPACE     NAME                                       READY   STATUS    RESTARTS   AGE
kube-system   aws-node-6kx4t                             2/2     Running   0          3m
kube-system   aws-node-p9wzs                             2/2     Running   0          3m
kube-system   coredns-7c9d8f6b5-2xqvn                    1/1     Running   0          6m
kube-system   coredns-7c9d8f6b5-h4mkd                    1/1     Running   0          6m
kube-system   ebs-csi-controller-6b7f9c4d8-fk2wl         5/5     Running   0          2m
kube-system   ebs-csi-controller-6b7f9c4d8-tn8qz         5/5     Running   0          2m
kube-system   ebs-csi-node-4vzjl                         3/3     Running   0          2m
kube-system   ebs-csi-node-9dnrx                         3/3     Running   0          2m
kube-system   eks-pod-identity-agent-lp2mn               1/1     Running   0          3m
kube-system   eks-pod-identity-agent-wq7dv               1/1     Running   0          3m
kube-system   kube-proxy-nb8rk                           1/1     Running   0          3m
kube-system   kube-proxy-x5c2f                           1/1     Running   0          3m
```

What matters: **`aws-node`** (the VPC CNI — without it nothing gets an IP), **`coredns`** (2/2 Running; if they're `Pending` the nodes joined after CoreDNS was scheduled and it will resolve itself, but check), and **`kube-proxy`**. Two of each DaemonSet pod, one per node. Names and hashes will differ; counts and `Running` should not.

Nothing outside `kube-system`. Everything else in this course you install yourself.

**3. The add-ons and the OIDC provider registered.**

```sh
eksctl get addon --cluster streaming
aws eks describe-cluster --name streaming --query cluster.identity.oidc.issuer --output text
```

```
https://oidc.eks.us-east-1.amazonaws.com/id/A1B2C3D4E5F67890ABCDEF1234567890
```

An empty result for the OIDC issuer means `withOIDC` did not take effect, and the AWS Load Balancer Controller in [Module 5](05-dns-and-certificates.md) will fail to get credentials.

**4. Access entries, not aws-auth.**

```sh
aws eks list-access-entries --cluster-name streaming --output table
aws eks describe-cluster --name streaming --query cluster.accessConfig.authenticationMode --output text
```

```
API
```

You should see two principal ARNs — the bootstrap entry for the creating principal, and the explicit SSO role entry from the config. And confirm the ConfigMap that every old tutorial talks about genuinely is not there:

```sh
kubectl -n kube-system get configmap aws-auth
```

```
Error from server (NotFound): configmaps "aws-auth" not found
```

That `NotFound` is the correct answer.

---

## What breaks

**`kubectl` returns `error: You must be logged in to the server (Unauthorized)`.** Overwhelmingly the most common failure after cluster creation, and it has two distinct causes.

First, the boring one — your SSO session expired:

```sh
aws sts get-caller-identity      # if this fails, it's the session
aws sso login
```

Second, the interesting one — your current principal has no access entry. This happens when you create the cluster as one principal and come back as another (a different SSO role, or a colleague).

```sh
aws eks list-access-entries --cluster-name streaming
aws sts get-caller-identity --query Arn --output text
```

Compare them. Remember the ARN shapes differ: `get-caller-identity` returns the `sts::...:assumed-role/...` session form, and the access entry holds the `iam::...:role/aws-reserved/sso.amazonaws.com/...` form. Adding yourself:

```sh
aws eks create-access-entry --cluster-name streaming --principal-arn "$SSO_ROLE_ARN" --type STANDARD
aws eks associate-access-policy --cluster-name streaming --principal-arn "$SSO_ROLE_ARN" \
  --policy-arn arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy \
  --access-scope type=cluster
```

That command working from outside the cluster, when you have no access *inside* the cluster, is the entire argument for `authenticationMode: API`.

**`eksctl create cluster` fails partway and rolls back.** Read the CloudFormation stack events; the message is specific.

```sh
aws cloudformation describe-stack-events --stack-name eksctl-streaming-cluster \
  --query 'StackEvents[?ResourceStatus==`CREATE_FAILED`].[LogicalResourceId,ResourceStatusReason]' \
  --output table
```

The two you're most likely to hit: an **EIP limit** (`The maximum number of addresses has been reached`) — NAT gateways need an Elastic IP, and new accounts have a low regional quota, so release unused EIPs or request an increase — and **`AccessDenied`** on an IAM action, which means the permission set is narrower than `AdministratorAccess`.

Clean up before retrying, or the next run collides with half-created resources:

```sh
eksctl delete cluster -f infra/eks/cluster.yaml --disable-nodegroup-eviction
```

**Nodes never appear, or appear as `NotReady`.** The control plane came up but the node group did not join.

```sh
eksctl get nodegroup --cluster streaming
aws cloudformation describe-stack-events --stack-name eksctl-streaming-nodegroup-system \
  --query 'StackEvents[?ResourceStatus==`CREATE_FAILED`].[LogicalResourceId,ResourceStatusReason]' --output table
kubectl describe node <name>       # Conditions, then Events
```

`NotReady` with `container runtime network not ready` means the CNI hasn't initialised — check `kubectl logs -n kube-system ds/aws-node`. Nodes that never appear at all usually failed to reach the EKS API endpoint or the EC2 instances failed to launch (capacity, or a missing route to the NAT gateway).

**`coredns` pods stuck `Pending`.** CoreDNS is scheduled before the node group exists during creation, so it sits `Pending` until nodes join and then schedules on its own. If it's still `Pending` several minutes after the nodes are `Ready`:

```sh
kubectl -n kube-system describe pod -l k8s-app=kube-dns    # Events explain the refusal
kubectl describe nodes | grep -A5 'Allocated resources'
```

Usually insufficient memory, which on a 2 × `t3.medium` cluster is a real possibility — see the sizing table above.

**`--dry-run` rejects a field in `cluster.yaml`.** Almost always an eksctl version older than the schema you've written against — `podIdentityAssociations` under an add-on is the most likely offender.

```sh
eksctl version    # want >= 0.200
```

Check the field name against the eksctl schema documentation for your version rather than substituting a guess; a silently-ignored field is worse than a rejected one.

**The bill starts and you weren't ready.** The control plane bills from the moment it's `ACTIVE`, at $73/month, and it does not stop when you scale nodes to zero. If you're pausing for more than a few days, [Module 16](16-monitoring-cost-teardown.md) covers teardown properly. If you're pausing overnight, scaling the node group to zero is the cheap habit:

```sh
eksctl scale nodegroup --cluster streaming --name system --nodes 0 --nodes-min 0
eksctl scale nodegroup --cluster streaming --name system --nodes 2 --nodes-min 2
```

---

Next: [Module 4 — Add-ons and the storage blocker](04-addons-and-storage.md).
