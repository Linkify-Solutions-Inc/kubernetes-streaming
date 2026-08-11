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

Karpenter prerequisites (done):
```
curl -fsSL -o /tmp/karpenter-cfn.yaml https://raw.githubusercontent.com/aws/karpenter-provider-aws/v1.13.0/website/content/en/docs/getting-started/getting-started-with-karpenter/cloudformation.yaml
aws cloudformation deploy --stack-name Karpenter-streaming --template-file /tmp/karpenter-cfn.yaml \
  --capabilities CAPABILITY_NAMED_IAM --parameter-overrides ClusterName=streaming --region us-east-1
# v1.13 template creates KarpenterNodeRole-streaming + SIX controller policies; list them:
aws iam list-policies --scope Local --query 'Policies[?contains(PolicyName,`Karpenter`)].Arn' --output text
eksctl create iamserviceaccount --cluster streaming --region us-east-1 --namespace kube-system \
  --name karpenter --attach-policy-arn <all six ARNs, repeated flag> --approve
aws eks create-access-entry --cluster-name streaming --region us-east-1 \
  --principal-arn arn:aws:iam::242626138899:role/KarpenterNodeRole-streaming --type EC2_LINUX
```

ArgoCD (done):
```
helm repo add argo https://argoproj.github.io/argo-helm
helm install argocd argo/argo-cd --version 10.3.2 --namespace argocd --create-namespace \
  --set configs.params."server\.insecure"=true --wait
# OCI repo registration, declarative (no argocd CLI needed):
kubectl apply -f - <<'YAML'
apiVersion: v1
kind: Secret
metadata:
  name: karpenter-oci-repo
  namespace: argocd
  labels: {argocd.argoproj.io/secret-type: repository}
stringData: {name: karpenter, url: public.ecr.aws/karpenter, type: helm, enableOCI: "true"}
YAML
```

## Phase 7 — Data layer

```
# RDS (~10 min): subnet group over the two PRIVATE subnets, SG open only to cluster SG
aws rds create-db-subnet-group --db-subnet-group-name streaming-db-subnets \
  --db-subnet-group-description "streaming: private subnets" --subnet-ids <priv-a> <priv-f>
DBSG=$(aws ec2 create-security-group --group-name streaming-db --description "..." --vpc-id $VPC --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id $DBSG --protocol tcp --port 5432 --source-group <cluster-SG>
aws rds create-db-instance --db-instance-identifier streaming-db --db-instance-class db.t4g.micro \
  --engine postgres --engine-version 16.11 --allocated-storage 20 --storage-type gp3 --storage-encrypted \
  --master-username streaming_admin --manage-master-user-password --db-name streaming \
  --vpc-security-group-ids $DBSG --db-subnet-group-name streaming-db-subnets --no-publicly-accessible \
  --backup-retention-period 1 --no-multi-az
# REBUILD-SENSITIVE: managed master secret name -> k8s/infra/secrets/externalsecret-db.yaml
aws rds describe-db-instances --db-instance-identifier streaming-db --query 'DBInstances[0].MasterUserSecret.SecretArn' --output text

# S3
aws s3api create-bucket --bucket linkify-streaming-media-242626138899
aws s3api put-public-access-block --bucket ... (all four blocks true)
aws s3api put-bucket-lifecycle-configuration ... (abort-incomplete-multipart 7d)
aws s3api put-bucket-cors ... (GET/HEAD from *)

# Route53 + ACM (user step: add the 4 NS records at Cloudflare, name "k8s", DNS-only)
aws route53 create-hosted-zone --name k8s.linkifysolutions.com --caller-reference <unique>
aws acm request-certificate --domain-name "*.k8s.linkifysolutions.com" \
  --subject-alternative-names k8s.linkifysolutions.com --validation-method DNS
aws acm describe-certificate ... DomainValidationOptions[0].ResourceRecord  # -> UPSERT CNAME into the zone

# Pod Identity: 3 policies (streaming-eso, streaming-upload-api, streaming-transcode-worker), then
eksctl create podidentityassociation --cluster streaming --namespace external-secrets \
  --service-account-name external-secrets --permission-policy-arns .../streaming-eso
# ... same for streaming/upload-api and streaming/transcode-worker

# Secrets Manager
aws secretsmanager create-secret --name streaming/mediamtx --secret-string '{"apiUser":"mediamtx-api","apiPassword":"<gen>"}'
aws secretsmanager create-secret --name streaming/grafana  --secret-string '{"username":"admin","password":"<gen>"}'
aws secretsmanager create-secret --name streaming/db-endpoint --secret-string '{"host":"<rds-endpoint>","port":"5432","dbname":"streaming"}'  # after RDS available
aws secretsmanager create-secret --name streaming/ghcr --secret-string '{"username":"<gh-user>","token":"<PAT read:packages>"}'

# CloudFront: OAC + distribution (segments CachingOptimized, *.m3u8 CachingDisabled), then
# bucket policy: cloudfront.amazonaws.com may GetObject hls/* when SourceArn = this distribution
```

GOTCHA 4 (hit at first full sync): app pods Pending with "0/2 nodes are
available: 2 Too many pods". t3.medium = 17 pods max in the CNI's default
mode; the platform layer fills 2 nodes by itself. Karpenter cannot help — its
pools are transcode-tainted on purpose. FIX:
```
eksctl scale nodegroup --cluster streaming --region us-east-1 --name system --nodes 3
```
and cluster.yaml desiredCapacity updated 2 -> 3 to keep file == reality.

GOTCHA 5: k8s/infra/storageclass-gp3.yaml sat at a path NO Application synced
-> the gp3 class (also the default class) never existed; Prometheus and Kafka
PVCs would hang Pending forever. FIX: moved to k8s/infra/storage/ + new
k8s/argocd/apps/05-storage.yaml Application. Lesson for the docs: under
GitOps, a manifest outside every Application's path simply does not exist.

## Phase 8 — GitOps bootstrap

```
git push   # root app reads GitHub master, not the local tree
kubectl apply -f k8s/bootstrap/root-app.yaml   # the ONLY by-hand kubectl apply
kubectl get applications -n argocd             # watch waves 0..40 converge
```
