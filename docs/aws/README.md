# Deploying the streaming platform on AWS — a course

This is a hands-on course, not a reference manual. You work through it in order, one module at a time, and at the end you have the whole streaming platform running on AWS: someone pushes RTMP from OBS, and someone else watches it in a browser over HTTPS.

Every module has the same shape:

1. **What you're building** — the one-paragraph goal.
2. **Why it works this way** — the reasoning. Read this part. The commands are the easy bit; understanding *why* EKS wants this shape is the thing you're actually here for.
3. **Do it** — commands and YAML you can copy.
4. **Verify** — a checkpoint with the exact output you should see. Do not move to the next module until this passes.
5. **What breaks** — the failure modes for this step, and the command that tells you which one you hit.

You will break things. That's fine and it's budgeted for. Module 16 covers teardown, and Module 1 sets a billing alarm before you create anything that costs money.

---

## What you already have

The platform runs today as Docker Compose (`docker-compose.yml`) — five Python services plus MediaMTX, Postgres, MinIO and Kafka. There is also a single-node Kubernetes cluster on a bare-metal box under `k8s/cluster/`, built with kubeadm. That box is *not* what this course uses. It was a rehearsal; this is the real thing.

Read `SPEC.md` first if you haven't. In particular the section on how a stream flows through the system, because you'll be debugging that flow in Module 13.

## What you're building

```
                                  Route 53  (k8s.linkifysolutions.com)
                                        |
        +-------------------------------+--------------------------------+
        |                               |                                |
   stream.k8s...                   cdn.k8s...                       rtmp.k8s...
        |                               |                                |
      [ALB]  HTTPS, ACM cert       [CloudFront]  <--- OAC ---        [NLB]  TCP :1935
        |                               |                                |
        |                          [S3 bucket]                           |
        |                          hls/  raw/                            |
        |                               ^                                |
        |                               |  writes HLS                    |
  +-----+---------------------------------------------------------------+---------+
  |     |                          EKS cluster                           |         |
  |     v                               |                                v         |
  |  [web]  [upload-api]          [transcode Jobs]                  [mediamtx]     |
  |     |        |                  KEDA + Karpenter                     |         |
  |     |        |                       ^                               |         |
  |     |        |                       |                        [ingest-webhook] |
  |     |        +---------+-------------+-------------+------------------+        |
  |     |                  |                           |                           |
  |     |             [Strimzi Kafka]           [analytics-worker]                 |
  |     |                                              |                           |
  +-----+----------------------------------------------+---------------------------+
        |                                              |
        +---------------------- [RDS Postgres] --------+
```

The pieces that change from Compose:

| Compose today | On AWS | Module |
|---|---|---|
| `postgres` container | RDS Postgres (managed) | 8 |
| `minio` container | S3 bucket + CloudFront | 9 |
| `kafka` container | Strimzi operator, in-cluster | 10 |
| `mediamtx` host port 1935 | NLB, TCP passthrough | 12 |
| `web` / `upload-api` host ports | one ALB Ingress, HTTPS | 12 |
| `transcode-worker` long-running container | KEDA `ScaledJob`, one Job per stream | 14 |
| `.env` file | AWS Secrets Manager + External Secrets Operator | 7 |
| `docker compose up` on a self-hosted runner | ArgoCD, pull-based | 15 |

Kafka stays inside the cluster rather than moving to MSK. That's deliberate: MSK Serverless has a ~$100/month floor even when idle, and running an operator is a genuinely useful thing to learn. Postgres and object storage move to managed services because nobody should be running their own database on a learning cluster.

---

## The modules

### Part I — Foundations

| # | Module | You'll have |
|---|---|---|
| 0 | [Pre-flight: the code must change first](00-preflight-code-changes.md) | Code that *can* run on AWS |
| 1 | [AWS account, access, and not getting a surprise bill](01-aws-account-setup.md) | SSO login, MFA, a budget alarm |
| 2 | [The VPC](02-vpc-and-networking.md) | Network design you can defend |
| 3 | [The EKS cluster](03-eks-cluster.md) | `kubectl get nodes` returning nodes |

### Part II — Platform

| # | Module | You'll have |
|---|---|---|
| 4 | [Add-ons and the storage blocker](04-addons-and-storage.md) | A default gp3 StorageClass |
| 5 | [DNS, certificates, load balancer controller](05-dns-and-certificates.md) | A real HTTPS hostname |
| 6 | [Karpenter](06-karpenter.md) | Nodes that appear when needed |
| 7 | [Secrets](07-secrets.md) | No credentials in git |

### Part III — Data layer

| # | Module | You'll have |
|---|---|---|
| 8 | [RDS Postgres](08-rds-postgres.md) | The four tables, on RDS |
| 9 | [S3 and CloudFront](09-s3-and-cloudfront.md) | HLS served from a CDN |
| 10 | [Kafka with Strimzi](10-kafka-strimzi.md) | Five topics, operator-managed |

### Part IV — The application

| # | Module | You'll have |
|---|---|---|
| 11 | [Running the services](11-workloads.md) | All five services green |
| 12 | [Exposing it to the world](12-ingress-and-rtmp.md) | Public HTTPS + public RTMP |
| 13 | [Your first stream](13-first-stream.md) | **A working stream, end to end** |

### Part V — Elastic and automated

| # | Module | You'll have |
|---|---|---|
| 14 | [Transcoding that scales](14-keda-scaledjobs.md) | One Job per stream, nodes on demand |
| 15 | [GitOps with ArgoCD](15-argocd-gitops.md) | Deploys without `kubectl` |
| 16 | [Monitoring, cost, and teardown](16-monitoring-cost-teardown.md) | Dashboards, and a way to stop paying |

Module 13 is the milestone. Everything before it is setup; everything after it is making the setup good. If you have limited time, get to 13.

---

## Before you start

You need:

- An AWS account you can create IAM Identity Center users in (Module 1 covers this).
- Access to DNS for `linkifysolutions.com`, to delegate the `k8s.` subdomain. Ask before you touch the parent zone.
- A machine with a terminal. macOS or Linux. On Windows use WSL2.
- The repo cloned, and `docker compose up` working locally at least once, so you know what "working" looks like before you try to reproduce it on AWS.

Tool versions are pinned in Module 1. Install them there, not now.

---

## Conventions used throughout

These are fixed across every module. If a command in one module disagrees with this table, this table wins — tell whoever wrote the module.

| Thing | Value |
|---|---|
| AWS region | `us-east-1` |
| AWS CLI profile | `linkify-streaming` |
| EKS cluster name | `streaming` |
| Kubernetes namespaces | `streaming` (apps), `kafka`, `keda`, `karpenter`, `argocd`, `external-secrets` |
| DNS zone | `k8s.linkifysolutions.com` |
| Web UI | `stream.k8s.linkifysolutions.com` |
| HLS / CDN | `cdn.k8s.linkifysolutions.com` |
| RTMP ingest | `rtmp.k8s.linkifysolutions.com` |
| S3 bucket | `linkify-streaming-media-<accountid>` |
| Secrets Manager prefix | `streaming/` |
| Manifests live in | `k8s/` (Kustomize base + overlays) |
| Container images | `ghcr.io/linkify-solutions-inc/kubernetes-streaming-<service>` |

Shell blocks assume you have exported `AWS_PROFILE=linkify-streaming` and `AWS_REGION=us-east-1`. Module 1 sets these up.

Placeholders look like `<accountid>` or `<your-value>`. Anything in angle brackets, you substitute. Anything not in angle brackets, type literally.

---

## What this costs

Roughly **$240/month** if you leave everything running 24/7. Module 16 has the line-by-line table and two changes that bring it to about **$170** — putting the node group on spot capacity, and scaling the nodes to zero overnight.

The single biggest line item is the EKS control plane at **$73/month, fixed**. There is no way to make that cheaper; it's the price of a managed control plane, and it's roughly half your budget before you've run a single pod. Knowing that shapes every other decision in this course — it's why there's one NAT gateway instead of three, and why Kafka isn't on MSK.

Two habits that will save you real money:

1. **Set the budget alarm in Module 1 before you create anything.** Not after.
2. **Scale the node group to zero when you stop for the day.** The control plane, RDS and the load balancers still bill, but the EC2 instances are the part that adds up fastest.

Module 16's teardown section is not optional reading. Several AWS resources keep billing after you delete the cluster — load balancers created by Kubernetes objects, CloudFront distributions, S3 object versions — and they're easy to leave running for months without noticing.

---

## When you get stuck

Work the "What breaks" section of the module you're on first — it's ordered by how often each failure actually happens.

Past that, the general method: **find the thing that isn't Ready, then ask it why.**

```sh
kubectl get pods -A | grep -v Running | grep -v Completed
kubectl describe pod <name> -n <namespace>       # Events at the bottom
kubectl logs <name> -n <namespace> --previous    # --previous if it's crash-looping
```

For AWS-side resources, the console's own error messages are usually accurate; the CLI equivalent is `aws <service> describe-<thing>`. For anything that involves a load balancer not appearing, the answer is almost always in the AWS Load Balancer Controller's logs:

```sh
kubectl logs -n kube-system deploy/aws-load-balancer-controller
```

`docs/kubernetes-explained.html` in this repo covers the Kubernetes concepts themselves — reconciliation loops, why a Pending pod is Pending, how Services actually route. If a module assumes something about Kubernetes you don't have yet, that's where it is.
