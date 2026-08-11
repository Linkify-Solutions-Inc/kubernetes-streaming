# TEARDOWN LEDGER — everything we created, and how to delete it

Live operational ledger, updated every time a resource is created.
Rule: nothing gets created without its delete command being recorded here first.
Run deletions BOTTOM-UP within the "billable" section (reverse creation order).

Account: 242626138899 · Region: us-east-1 · CLI profile: `streaming-admin`

## BILLABLE — delete these to stop all costs

| # | Created | Resource | Cost | Delete command |
|---|---------|----------|------|----------------|
| 1 | 2026-08-10 23:23–23:47 | EKS cluster `streaming` COMPLETE: control plane + VPC (10.42.0.0/16, us-east-1a/1f) + single NAT gateway + access entry (StreamingDeploy) + nodegroup `system` (2× t3.medium, Ready) + add-ons (vpc-cni, coredns, kube-proxy, pod-identity-agent, metrics-server, aws-ebs-csi-driver incl. its pod-identity IAM role stack) | ~$0.25/hr (~$180/mo): control plane $0.10 + NAT ~$0.05 + 2 nodes ~$0.08 + EBS ~$0.01 | `AWS_PROFILE=streaming-admin eksctl delete cluster -f infra/eks/cluster.yaml --disable-nodegroup-eviction --wait` (equivalent: `eksctl delete cluster --region=us-east-1 --name=streaming`; removes all eksctl-* CFN stacks incl. addon/pod-identity roles) |

| 2 | 2026-08-10 23:52 | S3 gateway VPC endpoint `vpce-0ac14676c318b7158` (free — exists to avoid NAT $0.045/GB on S3 traffic) + `karpenter.sh/discovery=streaming` tags on private subnets and cluster SG | $0 | **Delete BEFORE cluster teardown** or the VPC's CloudFormation delete fails on the non-stack resource: `AWS_PROFILE=streaming-admin aws ec2 delete-vpc-endpoints --region us-east-1 --vpc-endpoint-ids vpce-0ac14676c318b7158` (tags die with their resources) |

| 3 | 2026-08-11 00:03 | RDS Postgres `streaming-db` (db.t4g.micro, 20GB gp3, encrypted, managed master secret) + subnet group `streaming-db-subnets` + SG `streaming-db` (sg-04a84d83eab3a156b) | ~$0.017/hr + storage ≈ $15/mo | `aws rds delete-db-instance --db-instance-identifier streaming-db --skip-final-snapshot --delete-automated-backups` then (after gone) `aws rds delete-db-subnet-group --db-subnet-group-name streaming-db-subnets` and `aws ec2 delete-security-group --group-id sg-04a84d83eab3a156b`. Deleting the instance auto-deletes the rds!db-* managed secret |
| 4 | 2026-08-11 00:05 | S3 bucket `linkify-streaming-media-242626138899` (private, OAC-only policy, lifecycle, CORS) | pennies until media lands | `aws s3 rm s3://linkify-streaming-media-242626138899 --recursive && aws s3api delete-bucket --bucket linkify-streaming-media-242626138899` |
| 5 | 2026-08-11 00:07 | CloudFront distribution `E25LWLOR00Y4JY` (dpcgfvftw8dto.cloudfront.net) + OAC `E25BGVO5UVC11Y` | $0 idle, pay-per-GB served | disable first: `aws cloudfront get-distribution-config` → set Enabled=false → `update-distribution`; wait Deployed; then `aws cloudfront delete-distribution --id E25LWLOR00Y4JY --if-match <etag>`; then `aws cloudfront delete-origin-access-control --id E25BGVO5UVC11Y` |
| 6 | 2026-08-11 00:06 | Route53 hosted zone `k8s.linkifysolutions.com` (Z00393033J9419WFTFEKG) + ACM cert `*.k8s.linkifysolutions.com` (8f796564-8090-43fd-bdf8-7f94284ab5b1) | $0.50/mo zone | delete all non-NS/SOA records, `aws route53 delete-hosted-zone --id Z00393033J9419WFTFEKG`; `aws acm delete-certificate --certificate-arn arn:aws:acm:us-east-1:242626138899:certificate/8f796564-8090-43fd-bdf8-7f94284ab5b1` (after CloudFront/ALB detach). REMIND: remove the 4 NS records from Cloudflare |
| 7 | 2026-08-11 00:04–00:14 | Secrets Manager: `streaming/mediamtx`, `streaming/grafana`, `streaming/db-endpoint` (+ `streaming/ghcr` when created) | ~$0.40/mo each | `aws secretsmanager delete-secret --secret-id streaming/<name> --force-delete-without-recovery` per secret |
| 8 | 2026-08-11 00:03–00:05 | IAM policies `streaming-eso`, `streaming-upload-api`, `streaming-transcode-worker` + pod identity associations (external-secrets/external-secrets, streaming/upload-api, streaming/transcode-worker; eksctl podidentityrole CFN stacks) | $0 | associations + role stacks die with `eksctl delete cluster`; then `aws iam delete-policy --policy-arn arn:aws:iam::242626138899:policy/streaming-{eso,upload-api,transcode-worker}` |
| 9 | 2026-08-11 00:01+ | In-cluster: ArgoCD (helm release `argocd`, ns argocd) + everything the root app deploys — incl. any ALB/NLB the LBC creates and any PVC-backed EBS (Kafka, Prometheus) | LBs ~$16/mo each once created; Karpenter nodes per-use | `kubectl delete -f k8s/bootstrap/root-app.yaml` (finalizer cascades children; deletes LBs/PVCs) → wait → `helm uninstall argocd -n argocd`. Do this BEFORE eksctl delete cluster |

The billable meter: cluster ~$0.25/hr + RDS ~$0.02/hr; LBs/Karpenter nodes add when workloads sync. When cluster deletion finishes, verify with:
`AWS_PROFILE=streaming-admin aws cloudformation list-stacks --query 'StackSummaries[?starts_with(StackName,`eksctl-streaming`)&&StackStatus!=`DELETE_COMPLETE`].[StackName,StackStatus]' --output table`
(should be empty) and check for stray load balancers / EBS volumes:
`AWS_PROFILE=streaming-admin aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerName'`
`AWS_PROFILE=streaming-admin aws ec2 describe-volumes --filters Name=status,Values=available --query 'Volumes[].VolumeId'`
(Kubernetes-created ALBs/NLBs and PVC volumes are NOT deleted by eksctl — delete the
k8s Service/Ingress/PVC objects first, or delete these by hand after.)

## FREE — no cost, delete only if unwinding everything

| Created | Resource | Delete |
|---------|----------|--------|
| 2026-08-10 | IAM Identity Center instance (us-east-1, `ssoins-7223ef3085110868`, store `d-90667b4413`, start URL https://d-90667b4413.awsapps.com/start) | Console (root): us-east-1 → Identity Center → Settings → Management → Delete configuration |
| 2026-08-10 | Permission set `AdminAccess` (`ps-7223c10310e9c1a4`) = AdministratorAccess, assigned to user `aayush` | dies with instance; or `aws sso-admin delete-permission-set` |
| 2026-08-10 | Permission set `StreamingDeploy` (`ps-722337c324741b45`) = PowerUserAccess + inline eksctl-IAM policy, assigned to user `fatima` | dies with instance; or `aws sso-admin delete-permission-set` |
| 2026-08-10 | Identity Center users `aayush` (04e89428-3021-703d-158a-8eeb19e9ca4d), `fatima` (246814d8-9021-70dd-4b0d-0f4373bd99b7) | die with instance; or `aws identitystore delete-user` |
| 2026-08-10 | SSO-provisioned IAM roles `AWSReservedSSO_AdminAccess_8d36146d2e13acf4`, `AWSReservedSSO_StreamingDeploy_c76ab22657ecbba5` | auto-removed when assignments/instance deleted |
| 2026-08-10 | Karpenter CFN stack `Karpenter-streaming` (KarpenterNodeRole-streaming, 6 controller policies, SQS interruption queue + EventBridge rules; ~$0 idle) | NOT part of eksctl: `aws cloudformation delete-stack --stack-name Karpenter-streaming --region us-east-1` (after cluster is gone) |
| 2026-08-10 | Karpenter IRSA role + SA `kube-system/karpenter` (CFN `eksctl-streaming-addon-iamserviceaccount-kube-system-karpenter`) | removed by `eksctl delete cluster` |
| 2026-08-10 | EKS access entry for `KarpenterNodeRole-streaming` (EC2_LINUX) | dies with cluster |
| 2026-08-10 | IAM policy `AWSLoadBalancerControllerIAMPolicy` (`arn:aws:iam::242626138899:policy/AWSLoadBalancerControllerIAMPolicy`) | survives cluster deletion: `aws iam delete-policy --policy-arn arn:aws:iam::242626138899:policy/AWSLoadBalancerControllerIAMPolicy` (after the IRSA role is gone) |
| 2026-08-10 | IRSA role + ServiceAccount `kube-system/aws-load-balancer-controller` (CFN stack `eksctl-streaming-addon-iamserviceaccount-kube-system-aws-load-balancer-controller`) | removed by `eksctl delete cluster`; standalone: `eksctl delete iamserviceaccount --cluster streaming --namespace kube-system --name aws-load-balancer-controller` |
| 2026-08-10 | Billing setting: "IAM user and role access to Billing information" activated | account setting, free, leave on |

## DELETED / no longer exists

| When | What |
|------|------|
| 2026-08-10 | Old Identity Center instance in us-east-2 (`ssoins-6684f81e73af561b`, store `d-9a675a28f5`) incl. old `fatima` user and PowerUserAccess permission set — deleted by root via console |

## LOCAL MACHINE (this Mac) — free, listed for completeness

- Homebrew: `awscli`, `eksctl`, `kubernetes-cli`, `helm` (`brew uninstall` to remove)
- `~/.aws/config` — profile `streaming-admin` + sso-session `linkify`
- `~/.kube/config` — will gain the `streaming` cluster context when eksctl runs
