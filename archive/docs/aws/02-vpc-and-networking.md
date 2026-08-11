# Module 2 — The VPC

## What you're building

Nothing, yet. This module is the network design: the reasoning behind the VPC that `eksctl` will create for you in [Module 3](03-eks-cluster.md), and the two things you must add to it by hand afterwards — subnet tags and an S3 Gateway endpoint — because their absence produces silent failures rather than errors.

It's short on commands and long on why, deliberately. `eksctl create cluster` builds a VPC in about four minutes. Understanding what it built, and being able to say why each choice is right, is the part that takes longer and matters more. If a load balancer never appears in [Module 12](12-ingress-and-rtmp.md), or pods get stuck at `ContainerCreating`, or the bill has a $60 line item called "EC2-Other", the answer is in this module.

## Why it works this way

### The pod-CIDR reasoning from the kubeadm box inverts completely

On the bare-metal cluster under `k8s/cluster/`, `kubeadm-config.yaml` says:

```yaml
networking:
  # Calico's default pod CIDR. Deliberately not 10.x or 172.16-31.x: the box's
  # LAN is 10.50.0.0/24 and Docker's bridges hold 172.17-172.20.0.0/16.
  podSubnet: 192.168.0.0/16
```

That comment is correct reasoning for that cluster. Calico ran an **overlay**: pod IPs existed only inside the cluster, encapsulated when they crossed between nodes, invisible to anything on the LAN. Because they were invisible, the only requirement on the pod CIDR was that it not *collide* with a range the node also had to route to. A /16 was free — 65,536 addresses that cost nothing because they weren't real.

**EKS with the AWS VPC CNI has no overlay and no pod CIDR at all.** Every pod gets a real, routable, secondary IPv4 address on an elastic network interface attached to its node, drawn from **the subnet that node sits in**. A pod's IP is a VPC IP. You can reach it from an EC2 instance in the same VPC. Security groups apply to it.

Delete the `podSubnet` mental model entirely. Three consequences follow, and each one bites in a different module:

**1. Subnet sizing is driven by pods, not nodes.** A 20-node cluster running 30 pods each consumes 600 subnet addresses, not 20. Run out and pods stick in `ContainerCreating` with `failed to assign an IP address to container` — an error that reads like a CNI bug and is actually an exhausted subnet.

**2. There is a warm IP pool on top of that.** The CNI pre-allocates addresses so pod startup doesn't have to wait for an EC2 API call. With the default `WARM_ENI_TARGET=1`, each node keeps one entire spare ENI's worth of addresses allocated the moment it joins — roughly 12 subnet IPs on a `t3.medium`, before a single application pod is scheduled. A Karpenter node that lives for eight minutes still holds a full ENI's worth of addresses for those eight minutes.

**3. Pod-to-pod traffic across AZs is billable inter-AZ data transfer,** at $0.01/GB in each direction. On the kubeadm box, pod-to-pod was free because it was one box. Here, `transcode-worker` pulling RTMP from MediaMTX is a real network flow: a 1080p ingest is about 9 Mbps, ≈ 4 GB/hour. If those two pods land in different availability zones, that's roughly **$0.08/hour per stream**, and it appears on your bill as "EC2-Other" with no further explanation. [Module 14](14-keda-scaledjobs.md) deals with keeping transcode pods near MediaMTX.

### Work the numbers: how many pods fit on a t3.medium

This is the calculation that turns "the CNI uses VPC IPs" into a subnet size.

An EC2 instance type has a fixed maximum number of ENIs and a fixed number of IPv4 addresses per ENI. For a `t3.medium`: **3 ENIs, 6 IPv4 addresses each.**

The first address on every ENI is that interface's own primary address — it belongs to the node's networking, not to a pod. So the assignable count is:

```
max pods = ENIs × (IPv4 per ENI − 1) + 2
         = 3    × (6 − 1)            + 2
         = 17
```

The `+ 2` is for pods that use **host networking** and therefore consume no ENI address at all: `aws-node` (the CNI itself) and `kube-proxy` run in the host network namespace on every node. `ebs-csi-node` from [Module 4](04-addons-and-storage.md) does too.

So: **17 pods per `t3.medium`**, and the node group in [Module 3](03-eks-cluster.md) is 2 of them, giving you 34 pod slots for a workload that needs about 18. That headroom is intentional and it is not large.

Now the subnet. A **/19** gives 8,192 addresses; AWS reserves 5 in every subnet (network address, VPC router, DNS, one reserved for future use, broadcast), leaving **8,187 usable**.

```
8,187 usable addresses ÷ ~18 addresses per fully-packed t3.medium ≈ 450 nodes
```

You will never run 450 nodes. That's the point — the /19 removes the entire failure class, and it is completely free. IPv4 addresses inside your own VPC cost nothing; you are not buying them.

Compare a /24, which is what most tutorials use and what looks "reasonable":

```
251 usable addresses ÷ ~18 ≈ 13 nodes
```

Thirteen nodes sounds like plenty for a learning cluster right up until Karpenter spins up six transcode nodes for a burst of VOD jobs while the system node group is at four, and the fourteenth node's pods fail to get addresses. The failure is intermittent, load-dependent, and reads like a CNI problem. Size the subnet at /19 and never think about it again.

One knob you should *not* turn: `ENABLE_PREFIX_DELEGATION`. It raises a `t3.medium`'s pod ceiling from 17 to 110 by assigning /28 prefixes (16 addresses at a time) instead of individual addresses. You don't need 110 pods on a 4 GiB node — you'd run out of memory at roughly 20 — and prefix delegation would allocate addresses in blocks of 16 whether you use them or not. It's the right setting for large nodes with high pod density; it's pure waste here.

### Two AZs of workloads, in a VPC that spans three

EKS requires the control plane to have subnets in **at least two availability zones** — that's a hard API requirement, not a recommendation, and cluster creation fails without it.

`eksctl` builds the VPC with a public and a private subnet in each of three AZs, because three is its default and the extra subnets cost nothing. Your **nodes** live in two of them (`us-east-1a` and `us-east-1b`), pinned by the node group's `availabilityZones` in [Module 3](03-eks-cluster.md). The third AZ's subnets sit empty for now and give Karpenter a third spot capacity pool to choose from in [Module 6](06-karpenter.md), which materially improves spot availability for VOD transcoding.

Concentrating the running workload in two AZs is a cost decision, not an availability one: every byte between two pods in different AZs is billable, and this platform's hot path is video moving between pods.

The intended layout:

| Subnet | AZ | Size | Purpose |
|---|---|---|---|
| public-1a | us-east-1a | /24 | NAT gateway, ALB, NLB |
| public-1b | us-east-1b | /24 | ALB, NLB |
| public-1c | us-east-1c | /24 | ALB, NLB |
| private-1a | us-east-1a | /19 | Nodes and pods |
| private-1b | us-east-1b | /19 | Nodes and pods |
| private-1c | us-east-1c | /19 | Spare — Karpenter spot pool |

The VPC CIDR is `10.42.0.0/16`. Not `10.0.0.0/16`, which is every tutorial's default and therefore the first range that collides the day you peer anything to anything. Not `192.168.0.0/16`, because you want the kubeadm dev box reachable over Tailscale without a route conflict.

`eksctl` chooses the exact subnet CIDRs inside that /16 itself. Rather than assume, confirm what it actually built once the cluster exists:

```sh
aws ec2 describe-subnets \
  --filters "Name=tag:alpha.eksctl.io/cluster-name,Values=streaming" \
  --query 'Subnets[].{Name:Tags[?Key==`Name`]|[0].Value,AZ:AvailabilityZone,CIDR:CidrBlock}' \
  --output table
```

If a later requirement means you need the CIDRs pinned exactly rather than eksctl-chosen, `cluster.yaml` accepts an explicit `vpc.subnets` block listing each AZ's CIDR — check the eksctl schema for the current field names before writing one.

**Public versus private, and why nodes are private.** Public subnets have a route to an internet gateway; anything with a public IP in them is reachable from the internet. Load balancers go there, because being reachable is their job. Nodes go in private subnets with no public IPs at all — the only things that reach your pods are the load balancers you explicitly create. Outbound traffic from private subnets goes through a NAT gateway.

### One NAT gateway, and the honest tradeoff

A NAT gateway costs about **$0.045/hour** — $32.85/month — plus $0.045 per GB processed. The textbook design is one per AZ, so that an AZ failure doesn't take out egress for the others. That's three of them: **$98.55/month in hourly charges alone**, before a single byte moves.

Against a total budget of roughly $240/month, of which the EKS control plane is already an unavoidable $73, three NAT gateways would be the second-largest line item on the bill. One NAT gateway in `public-1a`, with all three private route tables pointing at it, is the right call.

State the tradeoff plainly, because the point is being able to defend it rather than to pretend it doesn't exist: **if `us-east-1a` has a zone-level failure, nodes in `1b` and `1c` lose all outbound internet.** No image pulls from ghcr.io, no Secrets Manager API calls, no reaching anything external. Pods already running keep running — their traffic to RDS, to Kafka, to each other, stays inside the VPC. Nothing new starts.

That's a real availability hit, and it is not this design's weakest link. You have one RDS instance in one AZ ([Module 8](08-rds-postgres.md)) and one MediaMTX replica ([Module 12](12-ingress-and-rtmp.md)). Both are single points of failure that are cheaper to fix than $65/month. Spend the money there first if you ever spend it.

### The S3 Gateway VPC Endpoint — the most important cost decision in the design

This is the one to get right, and it costs nothing.

Without it, **every HLS segment the transcoder writes to S3 leaves the VPC through the NAT gateway**, and NAT charges $0.045 per GB processed. S3 is a public AWS endpoint; from a private subnet, "public" means "via NAT".

Work out what that means for one hour of one live stream. From `services/transcode-worker/main.py`, the ABR ladder is three renditions:

```
1080p  5000 kbps
 720p  2800 kbps
 480p  1400 kbps
      ─────────
       9200 kbps  =  9.2 Mbps  of video
```

(Plus three AAC audio tracks at 128 kbps each, which adds about 4%. Ignore them; the number is big enough without.)

```
9.2 Mbit/s × 3600 s = 33,120 Mbit
33,120 Mbit ÷ 8     = 4,140 MB  ≈  4.1 GB per hour
4.1 GB × $0.045/GB  =  $0.19 per hour
```

**Nineteen cents an hour, per live stream, to move bytes you have already paid to produce.** For comparison, the `c6i.xlarge` that Karpenter would launch to do the actual encoding is about $0.17/hour on demand — so the NAT charge slightly *exceeds* the cost of the compute doing the work. You are paying more to ship the output than to create it.

Scale it out. Leave one test stream running around the clock for a month:

```
730 hours × $0.19 = $139/month
```

On a $200 budget where $73 is already spoken for by the control plane. That single line would appear as **"EC2-Other"** on your bill with no further breakdown, which is why [Module 1](01-aws-account-setup.md) has you activate cost allocation tags.

An **S3 Gateway endpoint** removes all of it. It is not a proxy or an appliance — it's an entry in your route tables that says "traffic destined for S3's address ranges in this region goes straight out of the VPC, not to the NAT gateway." Gateway endpoints support S3 and DynamoDB, and they are **free**. (Do not confuse them with *Interface* endpoints, which are ENIs backed by PrivateLink and cost about $7.30/month each plus per-GB charges. For S3 you want Gateway.)

```sh
# Run this once the cluster's VPC exists — see "Do it" below.
aws ec2 create-vpc-endpoint \
  --vpc-id "$VPC_ID" \
  --service-name com.amazonaws.us-east-1.s3 \
  --vpc-endpoint-type Gateway \
  --route-table-ids $PRIVATE_RTBS
```

What still goes through NAT afterwards, so you know what the remaining $32.85 buys: container image pulls from ghcr.io (roughly 400 MB per service image, once per new node — which is why Karpenter node churn has a real, if small, cost), Secrets Manager API calls (kilobytes), and ACM/EKS control-plane chatter. All small. The video does not.

### Subnet tags: absence produces silence, not errors

Three separate controllers discover AWS resources by **tag**, not by configuration. If a tag is missing, the controller does not error — it finds nothing, and reports that it found nothing in a place you are not looking.

**Public subnets**, all three:

```
kubernetes.io/role/elb            = 1
kubernetes.io/cluster/streaming   = shared
Project                           = streaming
```

`kubernetes.io/role/elb` is how the AWS Load Balancer Controller decides where an **internet-facing** ALB or NLB can go. Without it, an Ingress you create in [Module 12](12-ingress-and-rtmp.md) sits with `ADDRESS` empty forever, and the reason is one event on the Ingress object reading `couldn't auto-discover subnets`. Nothing else tells you.

**Private subnets**, all three:

```
kubernetes.io/role/internal-elb   = 1
kubernetes.io/cluster/streaming   = shared
karpenter.sh/discovery            = streaming
Project                           = streaming
```

`kubernetes.io/role/internal-elb` is the same mechanism for internal load balancers. `karpenter.sh/discovery` is what Karpenter's `EC2NodeClass` matches on in [Module 6](06-karpenter.md) — its `subnetSelectorTerms` is a tag query, and if nothing matches, Karpenter logs `no subnets matched selector` and quietly never launches an instance while your pods sit `Pending`.

`eksctl` applies the `kubernetes.io/role/*` and `kubernetes.io/cluster/*` tags itself when it creates the VPC. **`karpenter.sh/discovery` it does not** — that one is yours to add, along with the matching tag on the cluster security group.

---

## Do it

There is nothing to create in this module. These are the commands you run **immediately after `eksctl create cluster` finishes** in [Module 3](03-eks-cluster.md) — before any pod writes to S3 and before you install Karpenter. They're here because the reasoning for them is here.

```sh
export AWS_PROFILE=linkify-streaming
export AWS_REGION=us-east-1

# The VPC eksctl created
VPC_ID=$(aws eks describe-cluster --name streaming \
  --query cluster.resourcesVpcConfig.vpcId --output text)
echo "$VPC_ID"
```

**Add the S3 Gateway endpoint to every private route table:**

```sh
PRIVATE_RTBS=$(aws ec2 describe-route-tables \
  --filters "Name=vpc-id,Values=$VPC_ID" \
            "Name=tag:Name,Values=*Private*" \
  --query 'RouteTables[].RouteTableId' --output text)
echo "$PRIVATE_RTBS"

aws ec2 create-vpc-endpoint \
  --vpc-id "$VPC_ID" \
  --service-name com.amazonaws.us-east-1.s3 \
  --vpc-endpoint-type Gateway \
  --route-table-ids $PRIVATE_RTBS \
  --tag-specifications 'ResourceType=vpc-endpoint,Tags=[{Key=Project,Value=streaming}]'
```

(`$PRIVATE_RTBS` is intentionally unquoted — the command takes a list and the shell needs to split it.)

**Tag the private subnets for Karpenter:**

```sh
PRIVATE_SUBNETS=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=$VPC_ID" "Name=tag:Name,Values=*Private*" \
  --query 'Subnets[].SubnetId' --output text)

aws ec2 create-tags --resources $PRIVATE_SUBNETS \
  --tags Key=karpenter.sh/discovery,Value=streaming Key=Project,Value=streaming
```

**Tag the cluster security group for Karpenter** (it matches on this too):

```sh
CLUSTER_SG=$(aws eks describe-cluster --name streaming \
  --query cluster.resourcesVpcConfig.clusterSecurityGroupId --output text)

aws ec2 create-tags --resources "$CLUSTER_SG" \
  --tags Key=karpenter.sh/discovery,Value=streaming
```

---

## Verify

Run these after the commands above. All three must pass before [Module 6](06-karpenter.md), and the first must pass before anything writes to S3.

**1. The S3 route exists in every private route table.**

```sh
aws ec2 describe-route-tables --route-table-ids $PRIVATE_RTBS \
  --query 'RouteTables[].Routes[?GatewayId!=null && starts_with(GatewayId, `vpce-`)].[DestinationPrefixListId,GatewayId]' \
  --output text
```

```
pl-63a5400a	vpce-0a1b2c3d4e5f67890
pl-63a5400a	vpce-0a1b2c3d4e5f67890
pl-63a5400a	vpce-0a1b2c3d4e5f67890
```

One line per private route table. `pl-63a5400a` is the managed prefix list for S3 in us-east-1 — the set of address ranges S3 serves from. If a route table is missing from that output, its subnet's pods will pay NAT charges for every byte to S3, silently.

**2. Subnet tags are present.**

```sh
aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" \
  --query 'Subnets[].{Name:Tags[?Key==`Name`]|[0].Value,
                      elb:Tags[?Key==`kubernetes.io/role/elb`]|[0].Value,
                      internal:Tags[?Key==`kubernetes.io/role/internal-elb`]|[0].Value,
                      karpenter:Tags[?Key==`karpenter.sh/discovery`]|[0].Value}' \
  --output table
```

Every **public** subnet must show `elb = 1`. Every **private** subnet must show `internal = 1` and `karpenter = streaming`. A `None` in a column that should have a value is the failure — and it is the failure you will not notice until a controller silently does nothing three modules later.

**3. Nodes are in private subnets with no public IP.**

```sh
aws ec2 describe-instances \
  --filters "Name=tag:eks:cluster-name,Values=streaming" "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].{Id:InstanceId,Private:PrivateIpAddress,Public:PublicIpAddress,Subnet:SubnetId}' \
  --output table
```

`Public` must be `None` for every node. A public IP on a node means it landed in a public subnet — the node group's `privateNetworking: true` didn't take effect — and your nodes are directly addressable from the internet.

**4. Questions you should be able to answer without looking.** These are the checkpoint as much as the commands are:

- How many pods fit on a `t3.medium`, and where does the number come from? *(17: 3 ENIs × 5 assignable + 2 host-network pods.)*
- Why is `podSubnet` meaningless on EKS? *(No overlay. Pods get real VPC addresses from the node's subnet.)*
- What does one hour of one live stream cost in NAT charges without the S3 endpoint? *(≈ $0.19 — 4.1 GB at $0.045/GB.)*
- What breaks if `us-east-1a` fails? *(Everything loses outbound internet; running pods keep running.)*
- What happens if `karpenter.sh/discovery` is missing from the private subnets? *(Karpenter launches nothing and logs `no subnets matched selector`. Pods stay `Pending`.)*

---

## What breaks

**An Ingress or Service of type LoadBalancer never gets an address.** By far the most common networking failure, and always a tag. The controller's logs say so explicitly:

```sh
kubectl logs -n kube-system deploy/aws-load-balancer-controller | grep -i subnet
kubectl describe ingress -n streaming <name>     # Events at the bottom
```

`couldn't auto-discover subnets` means `kubernetes.io/role/elb` (internet-facing) or `kubernetes.io/role/internal-elb` (internal) is missing. Re-run verification step 2.

**Karpenter never launches a node; pods stay `Pending` indefinitely.** Same class of problem, different tag.

```sh
kubectl logs -n karpenter deploy/karpenter | grep -i 'selector\|matched'
```

`no subnets matched selector` or `no instance profile / security groups matched` means `karpenter.sh/discovery` is missing on the subnets or on the cluster security group. Both are in the "Do it" section above.

**A bill line item called "EC2-Other" that you cannot explain.** It's some combination of EBS volumes, NAT gateway data processing, and inter-AZ transfer — the three things AWS lumps together under that label. Check the S3 endpoint first (verification step 1); it's the largest of the three by a wide margin on this workload. Then use Cost Explorer grouped by the `Project` tag you activated in [Module 1](01-aws-account-setup.md), and by usage type.

**Pods stuck in `ContainerCreating` with `failed to assign an IP address to container`.** The subnet is out of addresses.

```sh
kubectl describe pod -n streaming <name>          # the message is in Events
aws ec2 describe-subnets --subnet-ids <subnet> \
  --query 'Subnets[].AvailableIpAddressCount'
```

With /19 subnets you should never see this. If you do, either the subnets came out smaller than intended (verification in "Why it works this way" — check the actual CIDRs) or something is leaking ENIs. `kubectl logs -n kube-system ds/aws-node` is the CNI's own view.

**Nodes cannot pull images; pods sit in `ImagePullBackOff` with a timeout.** Something is wrong on the path to the NAT gateway — a missing default route in a private route table, or a NAT gateway that failed to create. This is distinct from an authentication failure, which says `unauthorized` rather than timing out.

```sh
aws ec2 describe-nat-gateways --filter "Name=vpc-id,Values=$VPC_ID" \
  --query 'NatGateways[].{Id:NatGatewayId,State:State,Subnet:SubnetId}' --output table
aws ec2 describe-route-tables --route-table-ids $PRIVATE_RTBS \
  --query 'RouteTables[].Routes[?DestinationCidrBlock==`0.0.0.0/0`]'
```

**Cross-AZ data transfer charges you did not expect.** Two pods that talk a lot landed in different zones. Find out where things actually are:

```sh
kubectl get pods -n streaming -o wide
kubectl get nodes -L topology.kubernetes.io/zone
```

The pair that matters most is `transcode-worker` and `mediamtx` — see [Module 14](14-keda-scaledjobs.md) for keeping them together.

---

Next: [Module 3 — The EKS cluster](03-eks-cluster.md).
