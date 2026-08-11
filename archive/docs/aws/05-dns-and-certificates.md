# Module 5 — DNS, certificates, load balancer controller

*Part II — Platform. Previous: [Module 4](04-addons-and-storage.md). Next: [Module 6](06-karpenter.md).*

---

## What you're building

A real hostname with a real certificate, and the two controllers that will use them. Concretely: a Route 53 hosted zone for `k8s.linkifysolutions.com` delegated from the parent domain, one ACM wildcard certificate for `*.k8s.linkifysolutions.com` in `us-east-1`, the AWS Load Balancer Controller (so a Kubernetes `Ingress` turns into an ALB), and ExternalDNS (so that ALB gets a DNS record without anyone opening the console).

At the end of this module nothing is served yet — there is no Ingress until [Module 12](12-ingress-and-rtmp.md). What you have is the machinery that makes Module 12 a five-line YAML file instead of an afternoon of clicking.

One step here needs a human who is not you: adding the NS records at the parent zone. Read the "Do it" section, find out who that is, and ask them early.

---

## Why it works this way

### Delegate a subdomain; do not migrate the zone

Linkify owns `linkifysolutions.com`, and its DNS lives with the existing provider, serving production records for things that have nothing to do with this project. You need AWS-side DNS so that ExternalDNS and ACM can create records automatically. There are two ways to get it:

**Move the whole zone to Route 53.** Every existing record has to be recreated correctly, and a mistake takes down the company's website. No.

**Delegate one subdomain.** You create a Route 53 hosted zone for `k8s.linkifysolutions.com`. Route 53 gives you four nameservers. Someone adds a single `NS` record set at the parent zone saying "for anything under `k8s`, go ask those four". From then on, Route 53 is authoritative for the subtree and you can do whatever you want inside it without ever touching the parent again.

That is one record, added once, permanently. It is the smallest possible blast radius, and it is why every hostname in this course sits under `.k8s.`.

**Delegation is a one-time change that someone with access to `linkifysolutions.com`'s DNS must make.** You probably do not have that access. Ask before you need it, not after — and note that when it does happen, propagation is on the order of **minutes, not hours**. The parent zone's NS record set has a TTL (often 300s–3600s), and resolvers that have never looked up `k8s.linkifysolutions.com` before have nothing stale to cache. If it is not resolving after 15 minutes, something is wrong with the record — do not sit and wait for a day.

### One certificate, two consumers, one region

You need TLS in two places: the ALB in front of `stream.k8s.linkifysolutions.com`, and CloudFront in front of `cdn.k8s.linkifysolutions.com` ([Module 9](09-s3-and-cloudfront.md)).

A single wildcard certificate for `*.k8s.linkifysolutions.com` covers both names, plus `rtmp.` and anything else you add later. Request it **in `us-east-1`** and you are done.

The region matters, and it is the fact people get wrong. **ACM certificates are regional, and CloudFront can only use certificates from `us-east-1`.** CloudFront is a global service whose control plane lives in `us-east-1`; a certificate in `eu-west-1` is simply not visible to it. An ALB, by contrast, needs a certificate in *its own* region.

This course puts everything in `us-east-1`, which means the two requirements collapse into one: **one `us-east-1` wildcard certificate serves both the ALB and CloudFront.** Had the cluster been in, say, `ca-central-1`, you would need two certificates — one in `ca-central-1` for the ALB and a second in `us-east-1` for CloudFront — each with its own validation records. That is a genuine reason `us-east-1` was chosen back in [Module 1](01-aws-account-setup.md).

**DNS validation, not email.** ACM gives you a CNAME to publish; once it sees it, the certificate issues. Crucially, it keeps checking, which is how **renewal is automatic and permanent** — as long as that CNAME stays in the zone, ACM renews the certificate every year with zero human involvement. Email validation requires a human to click a link every time and is a scheduled outage waiting to happen.

### The explicit decision: ACM, not cert-manager

If you have used Kubernetes before, your instinct will be to install cert-manager. Do not. This is a considered decision, not an omission.

**Use ACM because:**

- ACM certificates are **free**. Not "free tier" — free, permanently, for use with AWS services.
- They **renew themselves**. There is no controller to keep alive, no `Certificate` resource to go stale, no 3am alert about an expiring secret.
- TLS terminates **at the ALB and at CloudFront**, which is where the traffic already arrives. The pods never see TLS, never mount a certificate, never need reloading when one rotates. `web` and `upload-api` keep speaking plain HTTP on port 8000 exactly as they do under Compose.
- The integration is a single annotation on the Ingress. That is the whole thing.

**cert-manager would mean running an operator to solve a problem AWS has already solved.** It issues Let's Encrypt certificates *into the cluster* as Kubernetes Secrets, which is the right answer when TLS terminates at a pod — an ingress-nginx setup, or mTLS between services. Neither is true here. You would be running three extra pods (controller, webhook, cainjector), a set of CRDs, and an ACME HTTP-01 or DNS-01 solver that needs its own Route 53 permissions, in order to produce a certificate that then has to be handed to the ALB anyway.

There is one thing cert-manager is genuinely required for on some EKS setups: the AWS Load Balancer Controller's own admission webhook needs a certificate. The Helm chart generates a self-signed one itself by default, so that need is already met. If you find a blog post saying "install cert-manager before the LBC", it is describing the chart's `enableCertManager=true` path, which you are not using.

### Why the AWS Load Balancer Controller, and not ingress-nginx

The controller watches `Ingress` and `Service type=LoadBalancer` objects and creates real AWS load balancers. An `Ingress` becomes an **ALB** (layer 7, HTTPS, ACM certificate attached, path routing); a `Service` with the right annotations becomes an **NLB** (layer 4 — which is what [Module 12](12-ingress-and-rtmp.md) uses for RTMP on TCP 1935, because RTMP is not HTTP and an ALB cannot carry it).

ingress-nginx would put a pod in the path: NLB → nginx pods → your pods. That is one more hop, one more thing to run, one more TLS termination point, and it forfeits the ALB's native ACM and WAF integration. On EKS with the traffic shape this app has, the ALB is the smaller, cheaper answer.

**The controller needs IRSA, not Pod Identity.** It is one of the two components in this whole course that does — see the table in [Module 4](04-addons-and-storage.md). This is why the cluster has an OIDC provider.

### ExternalDNS, and why `policy=sync` is safe here

ExternalDNS watches Ingress and Service objects and writes Route 53 records to match. Without it, every time an ALB is recreated (a different DNS name each time) somebody has to update a record by hand.

Two settings do the real work:

- **`domainFilters`** restricts it to `k8s.linkifysolutions.com`. Combined with an IAM policy scoped to that one hosted zone, ExternalDNS is *incapable* of touching the parent domain even if misconfigured. Belt and braces, and worth it when the parent zone is a production domain.
- **`policy=sync` with `txtOwnerId`**. The default `upsert-only` never deletes, so records accumulate as garbage pointing at load balancers that no longer exist. `sync` deletes a record when its Ingress goes away — which is what you want, but on its own it is dangerous, because ExternalDNS would happily delete records it did not create. `txtOwnerId` fixes that: alongside every record it creates, it writes a TXT record claiming ownership, and it will not touch anything without its own marker. **`policy=sync` without `txtOwnerId` is how people delete production DNS.**

---

## Do it

`AWS_PROFILE=linkify-streaming` and `AWS_REGION=us-east-1` exported, as always.

### 5.1 — Create the hosted zone

```sh
aws route53 create-hosted-zone \
  --name k8s.linkifysolutions.com \
  --caller-reference "streaming-$(date +%s)"
```

`--caller-reference` is an idempotency token; it just has to be unique per request, hence the timestamp.

Pull out the two things you need — the zone ID and the four nameservers:

```sh
HOSTED_ZONE_ID=$(aws route53 list-hosted-zones-by-name \
  --dns-name k8s.linkifysolutions.com \
  --query 'HostedZones[0].Id' --output text | sed 's|/hostedzone/||')
echo "$HOSTED_ZONE_ID"

aws route53 get-hosted-zone --id "$HOSTED_ZONE_ID" \
  --query 'DelegationSet.NameServers' --output text
```

```
ns-1234.awsdns-26.org  ns-567.awsdns-07.net  ns-890.awsdns-38.com  ns-1512.awsdns-60.co.uk
```

Save that zone ID somewhere. You need it in this module twice and again in [Module 9](09-s3-and-cloudfront.md).

### 5.2 — The one-time change at the parent zone

**This is the step you cannot do alone.** Whoever controls DNS for `linkifysolutions.com` must add one record set at the parent:

| Field | Value |
|---|---|
| Name | `k8s` (i.e. `k8s.linkifysolutions.com`) |
| Type | `NS` |
| TTL | `300` |
| Value | the four nameservers from the previous step, one per line |

That is the entire change. It creates nothing, deletes nothing, and affects no existing record. It says "the `k8s` branch is delegated to AWS".

Send them the four nameservers exactly as printed, including the trailing dots if their interface wants them, and note that `.org`, `.net`, `.com` and `.co.uk` all appearing in one delegation set is normal — AWS deliberately spreads nameservers across TLDs.

Then wait, and check:

```sh
dig +short NS k8s.linkifysolutions.com
```

Nothing for the first few minutes is expected. **Minutes, not hours.** If you get nothing after 15, query the parent's nameservers directly to see whether the record was actually added:

```sh
dig +short NS linkifysolutions.com                        # parent's nameservers
dig @<one-of-those> NS k8s.linkifysolutions.com           # ask one directly
```

If the direct query returns the record but `dig +short` does not, it is caching and it will resolve. If the direct query returns nothing, the record was not added — go back to whoever added it.

### 5.3 — Request the ACM certificate

```sh
CERT_ARN=$(aws acm request-certificate \
  --domain-name "*.k8s.linkifysolutions.com" \
  --subject-alternative-names "k8s.linkifysolutions.com" \
  --validation-method DNS \
  --region us-east-1 \
  --query CertificateArn --output text)
echo "$CERT_ARN"
```

The SAN for the bare `k8s.linkifysolutions.com` is included because a wildcard covers `stream.k8s...` but **not** `k8s...` itself. It costs nothing and saves a second certificate later.

Get the validation CNAME. It takes a few seconds to appear after the request — if the query returns `None`, wait and retry:

```sh
aws acm describe-certificate --certificate-arn "$CERT_ARN" --region us-east-1 \
  --query 'Certificate.DomainValidationOptions[0].ResourceRecord'
```

```json
{
    "Name": "_a79865eb4cd1a6ab990a45779b53961e.k8s.linkifysolutions.com.",
    "Type": "CNAME",
    "Value": "_424c463e9b51d4e5e8a1d1e0d8dbd6f2.acm-validations.aws."
}
```

Both entries of the wildcard and the SAN validate through the **same** record, so you only publish one. Write it into the hosted zone:

```sh
VAL_NAME=$(aws acm describe-certificate --certificate-arn "$CERT_ARN" --region us-east-1 \
  --query 'Certificate.DomainValidationOptions[0].ResourceRecord.Name' --output text)
VAL_VALUE=$(aws acm describe-certificate --certificate-arn "$CERT_ARN" --region us-east-1 \
  --query 'Certificate.DomainValidationOptions[0].ResourceRecord.Value' --output text)

cat > /tmp/acm-validation.json <<EOF
{
  "Changes": [{
    "Action": "UPSERT",
    "ResourceRecordSet": {
      "Name": "${VAL_NAME}",
      "Type": "CNAME",
      "TTL": 300,
      "ResourceRecords": [{ "Value": "${VAL_VALUE}" }]
    }
  }]
}
EOF

aws route53 change-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///tmp/acm-validation.json
```

Because that record lives inside the delegated zone that you control, ACM will keep finding it forever — which is what makes renewal automatic. **Never delete it**, not even after the certificate reaches `ISSUED`. Removing it breaks the renewal a year later, at which point nobody will remember this step.

Wait for issuance (typically under five minutes once the NS delegation is live):

```sh
aws acm wait certificate-validated --certificate-arn "$CERT_ARN" --region us-east-1
aws acm describe-certificate --certificate-arn "$CERT_ARN" --region us-east-1 \
  --query 'Certificate.Status' --output text
```

Record `$CERT_ARN` in your notes. [Module 12](12-ingress-and-rtmp.md) puts it in an Ingress annotation and [Module 9](09-s3-and-cloudfront.md) puts it in the CloudFront distribution.

### 5.4 — AWS Load Balancer Controller (IRSA)

Download the IAM policy that matches the chart version you are installing. Check what that is rather than trusting a number in a document:

```sh
helm repo add eks https://aws.github.io/eks-charts
helm repo update
helm search repo eks/aws-load-balancer-controller --versions | head -5
```

The `APP VERSION` column (e.g. `v2.13.0`) is the controller version. Use it below:

```sh
export LBC_VERSION="v2.13.0"     # substitute what helm search reported
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

curl -fsSL -o /tmp/lbc-iam-policy.json \
  "https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/${LBC_VERSION}/docs/install/iam_policy.json"

aws iam create-policy \
  --policy-name AWSLoadBalancerControllerIAMPolicy \
  --policy-document file:///tmp/lbc-iam-policy.json
```

That policy is long and grants a lot of EC2 and ELB permissions. It is the upstream-published one; do not hand-edit it, and do not substitute `AdministratorAccess` because it fails to create.

Create the IRSA-backed ServiceAccount. `eksctl` writes the trust policy against the cluster's OIDC provider and creates the Kubernetes ServiceAccount in one step:

```sh
eksctl create iamserviceaccount \
  --cluster=streaming \
  --region=us-east-1 \
  --namespace=kube-system \
  --name=aws-load-balancer-controller \
  --role-name=AmazonEKSLoadBalancerControllerRole \
  --attach-policy-arn="arn:aws:iam::${ACCOUNT_ID}:policy/AWSLoadBalancerControllerIAMPolicy" \
  --approve
```

Install the chart. You need the VPC ID from [Module 2](02-vpc-and-networking.md):

```sh
VPC_ID=$(aws eks describe-cluster --name streaming \
  --query 'cluster.resourcesVpcConfig.vpcId' --output text)

helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
  -n kube-system \
  --set clusterName=streaming \
  --set serviceAccount.create=false \
  --set serviceAccount.name=aws-load-balancer-controller \
  --set region=us-east-1 \
  --set vpcId="$VPC_ID" \
  --set replicaCount=2

kubectl -n kube-system rollout status deploy/aws-load-balancer-controller
```

Two of those flags deserve explanation:

- **`serviceAccount.create=false`** — `eksctl` already made it, with the IRSA annotation attached. Letting Helm create its own produces a ServiceAccount with no role annotation and a controller that cannot call any AWS API.
- **`region` and `vpcId` set explicitly** — not cosmetic. The controller can auto-discover both from the instance metadata service, but that discovery fails intermittently on nodes with `httpPutResponseHopLimit=1` (the secure default). The resulting error, `failed to introspect vpcID`, does not mention IMDS, hop limits, or anything you could search for productively. Set them and the problem cannot occur.

### 5.5 — ExternalDNS (Pod Identity)

Reuse the Pod Identity trust policy from [Module 4](04-addons-and-storage.md):

```sh
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
  --role-name streaming-external-dns \
  --assume-role-policy-document file:///tmp/pod-identity-trust.json
```

The inline policy is where the scoping happens. `ChangeResourceRecordSets` is restricted to **this one hosted zone**; the `List*` actions have to be `*` because ExternalDNS enumerates zones at startup to find the one matching its `domainFilters`, and listing is read-only:

```sh
cat > /tmp/external-dns-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["route53:ChangeResourceRecordSets"],
      "Resource": ["arn:aws:route53:::hostedzone/${HOSTED_ZONE_ID}"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "route53:ListHostedZones",
        "route53:ListHostedZonesByName",
        "route53:ListResourceRecordSets",
        "route53:ListTagsForResource"
      ],
      "Resource": ["*"]
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name streaming-external-dns \
  --policy-name route53-delegated-zone \
  --policy-document file:///tmp/external-dns-policy.json
```

**That `Resource` restriction is the real safety mechanism.** `domainFilters` is a controller-side setting that a future `helm upgrade` could drop; the IAM policy is enforced by AWS regardless of what the pod believes it is allowed to do. If ExternalDNS is ever pointed at the parent zone by accident, AWS refuses.

Create the namespace, bind the role, install the chart:

```sh
kubectl create namespace external-dns

aws eks create-pod-identity-association \
  --cluster-name streaming \
  --namespace external-dns \
  --service-account external-dns \
  --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/streaming-external-dns"

helm repo add external-dns https://kubernetes-sigs.github.io/external-dns/
helm repo update

helm install external-dns external-dns/external-dns \
  -n external-dns \
  --set provider.name=aws \
  --set "domainFilters[0]=k8s.linkifysolutions.com" \
  --set policy=sync \
  --set registry=txt \
  --set txtOwnerId=streaming-eks \
  --set "extraArgs[0]=--aws-zone-type=public"

kubectl -n external-dns rollout status deploy/external-dns
```

The chart creates a ServiceAccount named `external-dns` in that namespace, which is what the association above targets. **The association must exist before the pod starts, or the pod must be restarted after** — Pod Identity credentials are resolved at pod startup. If you created the association after installing, run `kubectl -n external-dns rollout restart deploy/external-dns`.

---

## Verify

**1. The delegation resolves.**

```sh
dig +short NS k8s.linkifysolutions.com
```

```
ns-1234.awsdns-26.org.
ns-567.awsdns-07.net.
ns-890.awsdns-38.com.
ns-1512.awsdns-60.co.uk.
```

Four nameservers, matching what Route 53 gave you. Empty output means the parent record is missing or has not propagated.

**2. The certificate is `ISSUED`.**

```sh
aws acm describe-certificate --certificate-arn "$CERT_ARN" --region us-east-1 \
  --query 'Certificate.{Status:Status,Domain:DomainName,Region:CertificateArn}' --output table
```

```
------------------------------------------------------------------------------------
|                               DescribeCertificate                                |
+----------+-----------------------------------+-----------------------------------+
|  Domain  |  *.k8s.linkifysolutions.com        |                                  |
|  Status  |  ISSUED                            |                                  |
+----------+-----------------------------------+-----------------------------------+
```

`PENDING_VALIDATION` means ACM cannot see the CNAME yet. Confirm it is actually published:

```sh
dig +short CNAME "$VAL_NAME"
```

**3. Both controllers are Ready.**

```sh
kubectl -n kube-system get deploy aws-load-balancer-controller
kubectl -n external-dns get deploy external-dns
```

```
NAME                           READY   UP-TO-DATE   AVAILABLE   AGE
aws-load-balancer-controller   2/2     2            2           3m

NAME           READY   UP-TO-DATE   AVAILABLE   AGE
external-dns   1/1     1            1           90s
```

**4. ExternalDNS can see the zone.** Ready is not the same as working — the pod starts fine with no permissions at all. Read the logs:

```sh
kubectl -n external-dns logs deploy/external-dns --tail=20
```

```
time="..." level=info msg="Instantiating new Kubernetes client"
time="..." level=info msg="Applying provider record filter for domains [k8s.linkifysolutions.com. .k8s.linkifysolutions.com.]"
time="..." level=info msg="All records are already up to date"
```

`All records are already up to date` with no `AccessDenied` above it is the pass condition. There are no Ingresses yet, so having nothing to do is correct.

---

## What breaks

Ordered by how often each actually happens.

### 1. Certificate stuck in `PENDING_VALIDATION`

Almost always the validation CNAME: either the NS delegation was not live when the record was written, or the record name is wrong.

```sh
aws acm describe-certificate --certificate-arn "$CERT_ARN" --region us-east-1 \
  --query 'Certificate.DomainValidationOptions'
dig +short CNAME "$VAL_NAME"
aws route53 list-resource-record-sets --hosted-zone-id "$HOSTED_ZONE_ID" \
  --query "ResourceRecordSets[?Type=='CNAME']"
```

The record name ACM gives you already includes the full domain — if you pasted it into a console field that appends the zone name, you have created `_abc.k8s.linkifysolutions.com.k8s.linkifysolutions.com`. That is the usual cause.

### 2. `dig NS` returns nothing

The parent zone record. Query the parent's nameservers directly rather than guessing at cache behaviour:

```sh
dig +short NS linkifysolutions.com
dig @<parent-ns> NS k8s.linkifysolutions.com
dig +trace k8s.linkifysolutions.com | tail -20
```

`+trace` walks the delegation from the root and shows exactly where the chain stops.

### 3. LBC pod running, but no ALB appears for an Ingress (Module 12)

The controller's own log is where the answer always is:

```sh
kubectl -n kube-system logs deploy/aws-load-balancer-controller --tail=100
```

The three recurring causes:

- `failed to introspect vpcID` — the IMDS discovery problem. Confirm `vpcId` and `region` are set: `helm get values aws-load-balancer-controller -n kube-system`.
- `AccessDenied` on an `elasticloadbalancing:*` call — the ServiceAccount is missing its IRSA annotation. Check `kubectl -n kube-system get sa aws-load-balancer-controller -o yaml` for `eks.amazonaws.com/role-arn`. If absent, Helm created its own ServiceAccount; delete it and re-run the `eksctl create iamserviceaccount` command.
- `couldn't auto-discover subnets` — the subnet tags from [Module 2](02-vpc-and-networking.md) are missing. Public subnets need `kubernetes.io/role/elb=1`, private need `kubernetes.io/role/internal-elb=1`, and both need `kubernetes.io/cluster/streaming` set to `owned` or `shared`.

### 4. ExternalDNS logs `AccessDenied`

```sh
kubectl -n external-dns logs deploy/external-dns --tail=50 | grep -i denied
aws eks list-pod-identity-associations --cluster-name streaming \
  --query 'associations[?namespace==`external-dns`]'
```

If the association exists and looks right, the pod started before it did — `kubectl -n external-dns rollout restart deploy/external-dns`. If the denial is specifically on `ChangeResourceRecordSets`, the hosted zone ID baked into the IAM policy is wrong; compare it with `$HOSTED_ZONE_ID`.

### 5. ExternalDNS creates records that immediately disappear (or vice versa)

Two ExternalDNS instances with different `txtOwnerId` values fighting, or a record created by hand that ExternalDNS now considers orphaned.

```sh
aws route53 list-resource-record-sets --hosted-zone-id "$HOSTED_ZONE_ID" \
  --query "ResourceRecordSets[?Type=='TXT']"
```

Every record ExternalDNS owns has a matching TXT registry record. Records without one it will not touch — so if a hand-made record is being deleted, it is not ExternalDNS doing it.

### 6. Certificate exists but the ALB rejects it

You requested it in the wrong region. `aws acm list-certificates` with no `--region` uses the profile default, so this is easy to miss:

```sh
aws acm list-certificates --region us-east-1 \
  --query 'CertificateSummaryList[].[DomainName,CertificateArn]' --output table
```

The ARN must contain `:us-east-1:`. If it does not, delete it and request again — there is no way to move a certificate between regions.

---

**Next:** [Module 6 — Karpenter](06-karpenter.md).
