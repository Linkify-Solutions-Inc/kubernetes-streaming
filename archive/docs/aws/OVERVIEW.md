# Getting this platform onto AWS — the map

This document is deliberately code-free. It tells you *what* to build, *in what order*, *why each piece is shaped the way it is*, and *what to go read* — and leaves the commands to you. If at any point you want the worked answer, `docs/aws/README.md` and Modules 00–16 next to this file have it. Use those as the answer key, not as the first thing you reach for.

Read this once end to end before you create a single AWS resource. The single most expensive mistake on a project like this is building in the wrong order and discovering a dependency three layers down.

---

## 1. The shape of what you're building

Today the platform is `docker compose up`: five Python services, MediaMTX, Postgres, MinIO, Kafka — one host, one network, one file of config.

On AWS the same system splits across three planes that you build in order:

| Plane | What lives here | Who owns it |
|---|---|---|
| **Account & network** | AWS account, identities, VPC, subnets, routing, DNS zone, budget alarms | You, once. Rarely changes. |
| **Cluster platform** | EKS control plane, nodes, storage driver, ingress controller, cert automation, secrets sync, autoscaler | You, via add-ons. Changes a few times a year. |
| **Application** | The five services, MediaMTX, Kafka, the transcode jobs, the migrations | Changes daily. This is the part GitOps should be driving. |

Everything below is organised by those three planes. The reason to keep them mentally separate is that they fail differently and they're debugged with different tools. A pod that won't start is a plane-3 problem; a pod that starts but can't reach the database is almost always plane-1.

The end state you're aiming at:

```
        DNS (a subdomain you control)
                  |
    +-------------+--------------+
    |             |              |
  web UI       HLS video      RTMP in
    |             |              |
  [ALB]      [CloudFront]     [NLB]
    |             |              |
    |          [S3 bucket]       |
    |             ^              |
    |             | HLS segments |
  +-+-------------+--------------+-+
  |            EKS cluster         |
  |  web  upload-api  ingest-hook  |
  |  mediamtx  analytics  transcode|
  |  Kafka (in-cluster operator)   |
  +--------------+-----------------+
                 |
          [RDS Postgres]
```

Three public entry points, one cluster, two managed data services outside it.

---

## 2. What changes from Compose, and why

You are not lifting-and-shifting. Five things genuinely change their nature, and each one is a concept worth learning properly rather than copying:

**Postgres → RDS.** Managed backups, managed failover, managed patching. The learning is not "how to click Create Database"; it's *networking and identity*: RDS lives in your VPC but not in your cluster, so you have to reason about subnet groups and security groups for the first time. This is where most people's first "it just hangs" moment happens.

**MinIO → S3 + CloudFront.** MinIO was an S3 impersonator; now you use the real thing. The interesting part is the CDN in front: HLS is thousands of small segment requests, and serving those from your cluster means your bandwidth bill and your pod CPU both scale with viewers. Putting CloudFront in front decouples viewer count from cluster size — that's the whole point. Learn how CloudFront reaches a private bucket without the bucket being public.

**Kafka stays in-cluster, via an operator.** The managed option (MSK) has a large fixed monthly floor whether you use it or not. Running the Strimzi operator instead teaches you what a Kubernetes operator actually *is*: a controller that watches custom resources and reconciles real infrastructure toward them. That concept generalises to almost every serious thing running in Kubernetes.

**MediaMTX's port 1935 → a network load balancer.** RTMP is raw TCP, not HTTP. Your web traffic goes through an application load balancer that understands paths and hostnames and terminates TLS; RTMP can't use any of that and needs a plain TCP passthrough. Understanding *why you need two different load balancers* is the single most useful networking insight in this project.

**The long-running transcode worker → one job per stream.** In Compose, a worker sits idle burning CPU-shaped money. On AWS the target is: a stream starts, a job is created, a node appears to run it, the stream ends, the job finishes, the node goes away. This is the most architecturally interesting change and the one to do last, because it only makes sense once everything else works.

Plus one that isn't a service at all: **the `.env` file → a real secrets store**. Nothing secret goes in git. The mechanism is a controller inside the cluster that reads from AWS Secrets Manager and materialises Kubernetes Secrets — so your manifests reference secrets by name and never contain values.

---

## 3. Plane 1 — Account and network

### 3.1 Account, identity, and a spending guardrail

Do this before anything else, and treat it as non-negotiable: **set a budget alarm before you create your first billable resource.** Not after the first bill.

What to set up, in order: a proper identity setup (IAM Identity Center / SSO users rather than long-lived root or IAM user keys), MFA, a named CLI profile so you can never accidentally point a command at the wrong account, and a billing alarm with a threshold you'd actually be unhappy to reach.

What to learn here: the difference between *authentication* (who you are) and *authorization* (what you may do), and why IAM roles beat access keys. You'll need this again in §4.3, because the way pods get AWS permissions is the same idea wearing a different hat.

### 3.2 The VPC

The network is the layer everything else sits on and the one you can't casually change later, so understand it rather than accepting a default.

The design questions you should be able to answer before you build it:

- **Public vs private subnets.** Which things need to be reachable from the internet (load balancers) and which absolutely must not be (nodes, database)? The answer determines where each resource goes.
- **How do private things reach the internet outbound?** Pulling container images, calling AWS APIs. The answer is a NAT gateway — and NAT gateways cost real money per hour *and* per GB. This is the first place where a "best practice" (one NAT per availability zone, for resilience) collides with a learning budget. Pick one, and know what you traded away.
- **How many availability zones?** More AZs means more resilience and more cross-AZ data transfer charges. For a learning cluster, two is a reasonable answer; know why you chose it.
- **How does the cluster tell AWS where to put load balancers?** Subnets carry tags that the load balancer controller reads. Untagged subnets are the classic cause of "my Ingress exists but no load balancer ever appeared."

Learn what a route table actually does before you move on. Almost every "I can't reach X" problem for the rest of the project resolves to a route table, a security group, or DNS.

### 3.3 DNS

You need a subdomain you control, with three names under it: the web UI, the CDN for video, and the RTMP ingest endpoint. Getting the parent domain to delegate that subdomain to AWS is a coordination step with whoever owns the parent zone — start it early, because delegation propagation is the kind of thing that eats an afternoon while you wait.

Learn: what delegation means (NS records), and the difference between a record that points at an IP and one that points at an AWS-managed endpoint whose IPs change.

---

## 4. Plane 2 — The cluster platform

### 4.1 The cluster itself

An EKS cluster is two halves: a **control plane** AWS runs for you at a fixed hourly price, and **nodes** you provide. Creating it is genuinely easy. The things worth thinking about:

- **How do you want to create infrastructure?** A CLI tool that creates clusters from a small config file is the fastest way to learn and the easiest to throw away. Infrastructure-as-code (Terraform, or AWS's own CDK/CloudFormation) is what you'd use for something you intend to keep, and it makes teardown reliable. There's a genuine tradeoff: IaC is slower to start and much better at not leaking resources. This repo has an `infra/eks/` directory — look at what's there before deciding.
- **The cluster's own identity model.** Two separate permission systems meet here: AWS IAM decides who can call the EKS API, and Kubernetes RBAC decides what you can do once you're talking to the cluster. New users routinely get authenticated and then denied, and can't tell which system said no. Learn to tell them apart.
- **Node groups vs. on-demand provisioning.** Start with a small managed node group — a fixed set of instances. It's simpler, and you want a stable floor to debug against before you introduce anything dynamic.

Your checkpoint for this whole section is trivial to state: you can list the cluster's nodes from your laptop and they're all Ready.

### 4.2 The add-ons that make it usable

A bare EKS cluster can't do several things you'll assume it can. Each of these is a controller you install, and each one is worth understanding as "a program that watches Kubernetes objects and makes AWS do something":

- **Storage.** A fresh cluster has no default way to satisfy a persistent volume claim. Until you install the EBS CSI driver *and* mark a storage class as default, anything wanting a disk — Kafka, most notably — sits Pending forever with a confusing message. This blocks §5.3, so do it early.
- **Load balancer controller.** Watches Ingress and Service objects and creates real AWS load balancers. Without it, an Ingress is an inert record.
- **Certificates and DNS automation.** ACM issues the TLS certificate; a DNS controller can create records automatically from your Kubernetes objects. Learn how certificate validation works — it's a DNS record you must publish to prove you own the domain, and it's a common place to get stuck waiting.
- **Secrets sync.** The controller that turns AWS Secrets Manager entries into Kubernetes Secrets.
- **Dynamic node provisioning (Karpenter).** Watches for pods that can't be scheduled and buys the right-shaped instance for them, then removes it when idle. This is what makes §6.3 economical. Install it *after* your fixed node group works, so you have a known-good baseline.

### 4.3 The single most important concept in this section

**How a pod gets AWS permissions.** Your services need to write to S3 and read from Secrets Manager. The wrong answer is baking AWS access keys into the container. The right answer is a federation mechanism where a Kubernetes service account is trusted by an IAM role, and the pod gets short-lived credentials automatically.

If you learn one thing from this whole AWS exercise, make it this one. It's the concept that separates "I got it working" from "I know what I'm doing," it shows up in every AWS-hosted Kubernetes system, and it's the source of an enormous share of confusing `AccessDenied` errors. Look up **IRSA** (IAM Roles for Service Accounts) and its newer successor, **EKS Pod Identity**, and understand what problem each solves.

---

## 5. Plane 3 — Data

Build the data layer before the application, because every service needs at least one part of it at startup and a service that can't reach its database is a much harder thing to debug than one that hasn't been deployed yet.

### 5.1 Postgres

A managed instance in your private subnets, reachable only from the cluster's security group. The work is: create it, restrict who can reach it, get the credentials into Secrets Manager (not into git, not into your shell history), and run your schema migrations against it.

Think about *how migrations run* on AWS. In Compose, something probably runs them at startup. In Kubernetes the idiom is a Job that runs once before the app rolls out. This repo already has a `db-migrate` component — read how it's wired.

The failure to expect: connectivity. Security groups are stateful allow-lists, and "the database is in the same VPC" does not mean "the cluster can reach it."

### 5.2 Object storage and the CDN

An S3 bucket holding raw uploads and generated HLS output, with a CloudFront distribution in front of the HLS prefix.

Concepts to get right:

- **The bucket should not be public.** CloudFront can be granted private access to it; learn that mechanism (Origin Access Control) rather than opening the bucket.
- **Cache behaviour matters for HLS.** The playlist file changes constantly; the video segments never change once written. Those two need different cache lifetimes, and getting it wrong produces either a stream that never advances or a CDN that does nothing for you.
- **Lifecycle rules.** Video is the thing that will quietly grow your bill. Decide up front how long recordings live.

### 5.3 Kafka

Install the operator, then declare a cluster and your topics as Kubernetes resources. It needs working persistent storage, so §4.2's storage step must be done.

The design point already made elsewhere in this project and worth re-reading: **partition by stream ID**, so all events for one stream land in order on one partition. Ordering guarantees in Kafka are per-partition, not per-topic — this is the detail that makes or breaks event-driven systems and it's worth being able to explain out loud.

---

## 6. Plane 3 — The application

### 6.1 Getting the services running

The five services plus MediaMTX, deployed from the manifests in `k8s/`. There is a base definition and per-environment overlays — understand that layering (Kustomize) before you edit anything, or you'll fix a value in a place that gets overridden.

At this stage nothing is exposed publicly. The goal is only: every pod Running, and readiness probes passing. Port-forward to check things by hand.

Expect to spend your time on: image pull permissions (private registry credentials), config that pointed at Compose service names and now needs to point at Kubernetes services or AWS endpoints, and probes with timeouts that were fine on a laptop and aren't on a cold-starting pod.

### 6.2 Exposing it

Three doors, and they are deliberately different:

- **Web UI and upload API** — one HTTP load balancer, TLS terminated at the edge with your ACM certificate, routing by hostname and path to different services.
- **HLS playback** — not through the cluster at all. Viewers hit CloudFront, which reads from S3. Your cluster's job is only to *write* segments there. This is the decoupling that lets viewer count grow without cluster growth.
- **RTMP ingest** — a TCP load balancer straight through to MediaMTX. No TLS termination, no path routing, because RTMP has neither.

Then the milestone: **push a stream from OBS to your RTMP hostname and watch it in a browser via the CDN.** Everything before this is setup; everything after is making the setup good. If you run out of time or budget, get to here.

When it doesn't work first time — it won't — debug in the direction of the data: is the stream arriving at MediaMTX? Is the webhook firing? Is an event on Kafka? Did a transcode run? Did segments land in S3? Is CloudFront serving them? Each of those is a separate, checkable question, and the discipline of asking them in order is worth more than any individual command.

### 6.3 Making transcoding elastic

Now replace the always-on worker with per-stream jobs: an autoscaler watches Kafka's consumer lag and creates one Job per pending stream; the node autoscaler creates capacity for those Jobs and removes it afterwards.

Two things to think about carefully, both already noted in this project's research: **cap the scaler at the partition count** — more consumers than partitions is pure waste, since the extra ones get no work — and be deliberate about what happens to a job that fails halfway through a stream.

This is also where spot capacity becomes attractive: transcode jobs are interruptible and re-runnable, which is exactly the workload spot pricing is for.

### 6.4 GitOps

The last step is removing yourself from the deploy path: a controller in the cluster watches the git repo and makes the cluster match it. You stop running deploy commands; you merge a pull request.

The concept to internalise is **pull vs push**. A push pipeline needs credentials to your cluster stored in your CI system. A pull agent needs no inbound access at all — it reaches out. That's both a security improvement and an operational one, because the cluster self-heals toward the repo if someone changes something by hand.

The awkward part, and it's worth thinking about before you start: some things (the cluster itself, IAM roles, RDS) can't be managed this way, so you'll have a boundary between "GitOps manages this" and "I manage this another way." Know where you're drawing that line.

---

## 7. Order and dependencies

Strict prerequisites — you cannot usefully skip ahead past these:

```
Budget alarm ──► everything (do this first, always)

VPC ──► EKS ──► add-ons ──┬──► RDS access
                          ├──► storage class ──► Kafka
                          ├──► load balancer controller ──► public access
                          └──► pod identity ──► S3 access from pods

DNS delegation ──► certificate ──► HTTPS

RDS + S3 + Kafka ──► services actually start

everything above ──► first stream ──► elastic transcoding ──► GitOps
```

Things you can safely do in parallel or defer: CloudFront (S3 direct works for testing), Karpenter (a fixed node group is fine to start), GitOps (deploy by hand until the shape stops changing), monitoring.

Things people wrongly defer: the budget alarm, the storage class, and DNS delegation — the first costs money, the second silently blocks Kafka, the third has a waiting period you can't compress.

---

## 8. Cost, and the habits that control it

Roughly $240/month if everything runs continuously; the biggest single line is the EKS control plane at about $73/month, fixed, before you run a single pod. That number should shape your decisions — it's why one NAT gateway instead of three, and why Kafka isn't on the managed service.

The four levers, in rough order of effect:

1. **Scale nodes to zero when you stop for the day.** The control plane, RDS and load balancers keep billing, but EC2 is what adds up fastest.
2. **Spot capacity** for anything interruptible — transcode jobs especially.
3. **NAT gateway count**, and being aware that it bills per GB processed as well as per hour.
4. **S3 lifecycle rules**, so old video doesn't accumulate silently.

And the thing that catches everyone: **several resources keep billing after you delete the cluster.** Load balancers created by Kubernetes objects rather than by you, CloudFront distributions, S3 object versions, orphaned EBS volumes, unattached elastic IPs. Before you tear anything down, know how you'll verify that it's actually gone — the billing console's cost-by-service view a day later is the honest check.

Plan the teardown before you build. If you built with infrastructure-as-code, this is one command and a manual sweep; if you built by hand, it's an archaeology exercise.

---

## 9. What to actually go read

Roughly in the order you'll need it:

- **AWS VPC fundamentals** — subnets, route tables, security groups vs. network ACLs, NAT.
- **EKS networking** — how pods get IP addresses from your VPC, and why that constrains how many pods fit on an instance. This surprises people.
- **IRSA / EKS Pod Identity** — as said in §4.3, the highest-value concept here.
- **Kubernetes Services, Ingress, and the Gateway API** — what each layer does, and where the AWS controller fits.
- **The Kubernetes operator pattern** — custom resources and controllers. Explains Strimzi, Karpenter, the secrets operator, and ArgoCD all at once.
- **HLS and CDN caching** — why playlists and segments need different cache rules.
- **Kafka partitioning and consumer groups** — ordering, lag, and why consumer count above partition count is wasted.

For the Kubernetes concepts themselves, `docs/kubernetes-explained.html` in this repo is the local reference. For the platform's own data flow, `SPEC.md`.

---

## 10. How to use this alongside the modules

This document is the map; Modules 00–16 are the route. A reasonable way to work:

1. Read the module's "What you're building" and "Why it works this way," then close it.
2. Try to do the step from the AWS docs and your own understanding.
3. Open the module's commands when you're stuck or to check your answer.
4. Always read "What breaks" — those are failure modes ordered by how often they actually happen, and reading them in advance is much cheaper than discovering them.

Module 00 (pre-flight code changes) is already done in this repo as of commit `8775b6c`. Your real starting point is Module 01: the account, and the budget alarm.
