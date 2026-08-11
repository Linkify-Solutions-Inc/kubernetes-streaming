# BUILD LOG — verified commands, in execution order

Raw material for the two MIL-STD-38784 documents. Only commands that actually
worked are recorded; failures appear as GOTCHA entries with their fix.
Companion file: TEARDOWN.md (delete command for everything created here).

Account 242626138899 · region us-east-1 · date 2026-08-10

## Phase 0 — Local machine setup (macOS; Fatima's doc needs Ubuntu equivalents)

```
brew install awscli
brew install eksctl kubectl helm
# versions verified: awscli current, eksctl 0.229.0, kubectl 1.36.3, helm 4.2.3
```

## Phase 1 — Identity rebuild (root, in CloudShell + console)

Console (root): deleted old Identity Center instance (us-east-2) — Settings →
Management → Delete configuration. Re-enabled Identity Center with region
picker on **us-east-1 first** (it installs into the region you're standing in).

CloudShell (root) — full script in git history / chat log, summary:
```
aws sso-admin create-permission-set --name AdminAccess --session-duration PT8H ...
aws sso-admin attach-managed-policy-to-permission-set ... AdministratorAccess
aws sso-admin create-permission-set --name StreamingDeploy --session-duration PT8H ...
aws sso-admin attach-managed-policy-to-permission-set ... PowerUserAccess
aws sso-admin put-inline-policy-to-permission-set ... file:///tmp/eksctl-iam.json   # iam:CreateRole/PassRole/etc for eksctl
aws identitystore create-user --user-name aayush ... aayush@linkifysolutions.com
aws identitystore create-user --user-name fatima ... fatima@linkifysolutions.com
aws sso-admin create-account-assignment ... AdminAccess -> aayush
aws sso-admin create-account-assignment ... StreamingDeploy -> fatima
```
Result: instance ssoins-7223ef3085110868, store d-90667b4413,
start URL https://d-90667b4413.awsapps.com/start
GOTCHA: SSO role ARNs get NO region path segment when Identity Center is in
us-east-1 (us-east-2 instances had /us-east-2/ in the path).
Console (root): Users → Reset password (email) per user; Settings →
Authentication → MFA every sign-in. Billing: Account → activate
"IAM user and role access to Billing information" (Cost Explorer 400s without it).

## Phase 2 — CLI auth on the workstation

`~/.aws/config`:
```
[profile streaming-admin]
sso_session = linkify
sso_account_id = 242626138899
sso_role_name = AdminAccess          # Fatima: StreamingDeploy
region = us-east-1
output = json

[sso-session linkify]
sso_start_url = https://d-90667b4413.awsapps.com/start
sso_region = us-east-1
sso_registration_scopes = sso:account:access
```
```
aws sso login --profile streaming-admin
aws sts get-caller-identity --profile streaming-admin   # checkpoint: assumed-role/AWSReservedSSO_AdminAccess_*/aayush
```

## Phase 3 — Account hygiene (root CloudShell, one-off)

Deleted empty default VPCs in us-east-1 + us-east-2 (verified 0 ENIs first):
detach+delete IGW → delete subnets → delete VPC. Reversible:
`aws ec2 create-default-vpc`.

## Phase 4 — EKS cluster

```
AWS_PROFILE=streaming-admin eksctl create cluster -f infra/eks/cluster.yaml    # run from repo root
```
GOTCHA 1: config had no cluster-level availabilityZones -> eksctl picked
us-east-1f/1a at random, nodegroup wanted 1a/1b -> "could not find private
subnets for zones". FIX: pin `availabilityZones: [us-east-1a, us-east-1f]`
(cluster level) and match the nodegroup. Control plane/VPC/access-entry/4
add-ons survived; resume with `eksctl create nodegroup`, no teardown needed.

GOTCHA 2: nodegroup tag `karpenter.sh/discovery: ""` (empty value) ->
CloudFormation CreateStack rejects empty tag values. FIX: tag removed
(pointless anyway: Karpenter only manages nodes it provisioned).

GOTCHA 3: `-f` paths are relative — commands run from the repo root.

```
AWS_PROFILE=streaming-admin eksctl create nodegroup -f infra/eks/cluster.yaml   # 2 nodes Ready in ~3 min
AWS_PROFILE=streaming-admin eksctl create addon -f infra/eks/cluster.yaml       # metrics-server + aws-ebs-csi-driver (missed by failed run)
aws eks update-kubeconfig --name streaming --region us-east-1 --profile streaming-admin
```
Checkpoints:
```
kubectl get nodes -o wide     # 2 nodes Ready, no EXTERNAL-IP (private networking)
kubectl get pods -A           # aws-node, coredns, kube-proxy, pod-identity, metrics-server, ebs-csi all Running
kubectl top nodes             # proves metrics-server works
```

## Phase 5 — VPC hardening

Discovery (values differ per build — rediscover, never copy IDs):
```
export AWS_PROFILE=streaming-admin AWS_REGION=us-east-1
VPC=$(aws ec2 describe-vpcs --filters Name=tag:alpha.eksctl.io/cluster-name,Values=streaming --query 'Vpcs[0].VpcId' --output text)
aws ec2 describe-route-tables --filters Name=vpc-id,Values=$VPC --query 'RouteTables[].[RouteTableId,Tags[?Key==`Name`].Value|[0]]' --output text
aws ec2 describe-subnets --filters Name=vpc-id,Values=$VPC --query 'Subnets[].[SubnetId,AvailabilityZone,Tags[?Key==`Name`].Value|[0]]' --output text
CSG=$(aws eks describe-cluster --name streaming --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' --output text)
```
Actions (substitute the *Private* route tables / *Private* subnets found above):
```
aws ec2 create-vpc-endpoint --vpc-id $VPC --service-name com.amazonaws.us-east-1.s3 \
  --vpc-endpoint-type Gateway --route-table-ids <rtb-privA> <rtb-privB> \
  --tag-specifications 'ResourceType=vpc-endpoint,Tags=[{Key=Name,Value=streaming-s3-gateway},{Key=Project,Value=streaming}]'
aws ec2 create-tags --resources <subnet-privA> <subnet-privB> $CSG --tags Key=karpenter.sh/discovery,Value=streaming
```
Why: gateway endpoint (free) routes all S3 traffic from private subnets past
the NAT gateway ($0.045/GB) — the transcoder writes every HLS segment to S3,
so this is the single biggest cost lever in the design. The discovery tags are
how Karpenter finds which subnets/SG to launch nodes into; without them node
provisioning fails silently later.
Checkpoints:
```
aws ec2 describe-route-tables --route-table-ids <rtb-privA> <rtb-privB> \
  --query 'RouteTables[].[RouteTableId,Routes[?starts_with(GatewayId,`vpce-`)].GatewayId|[0]]' --output text   # both rows show the vpce id
aws ec2 describe-tags --filters "Name=key,Values=karpenter.sh/discovery" --query 'Tags[].[ResourceId,ResourceType,Value]' --output text  # 2 subnets + 1 SG
```
GOTCHA: the endpoint is NOT part of eksctl's CloudFormation stack — delete it
before `eksctl delete cluster` or the VPC deletion fails (see TEARDOWN.md).

## Phase 6 — Platform layer (in progress)

Chart versions pinned in git 2026-08-10 (re-check with `helm search repo` on
rebuild): LBC 3.5.0, ESO 2.9.0, kube-prometheus-stack 88.2.0; KEDA 2.19.0 and
Strimzi 0.49.0 were already pinned. Placeholders filled: vpcId (LBC app —
REBUILD-SENSITIVE, discovery command in the file), S3 bucket name (account id).

LBC prerequisites (done):
```
curl -sL -o /tmp/lbc-iam-policy.json https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/v3.5.0/docs/install/iam_policy.json
aws iam create-policy --policy-name AWSLoadBalancerControllerIAMPolicy --policy-document file:///tmp/lbc-iam-policy.json
eksctl create iamserviceaccount --cluster streaming --region us-east-1 \
  --namespace kube-system --name aws-load-balancer-controller \
  --attach-policy-arn arn:aws:iam::242626138899:policy/AWSLoadBalancerControllerIAMPolicy --approve
# checkpoint: kubectl get sa aws-load-balancer-controller -n kube-system -o jsonpath='{.metadata.annotations.eks\.amazonaws\.com/role-arn}'  -> role ARN
```

Still to do in this phase:
- Karpenter prerequisites: controller IRSA (needs Karpenter controller policy),
  node IAM role/instance profile (see k8s/infra/karpenter/ec2nodeclass.yaml),
  SQS interruption queue `Karpenter-streaming` + EventBridge rules
- Install ArgoCD; register OCI repo public.ecr.aws/karpenter (--enable-oci)
- Apply k8s/bootstrap/root-app.yaml and let sync waves converge
