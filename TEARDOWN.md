# TEARDOWN LEDGER — everything we created, and how to delete it

Live operational ledger, updated every time a resource is created.
Rule: nothing gets created without its delete command being recorded here first.
Run deletions BOTTOM-UP within the "billable" section (reverse creation order).

Account: 242626138899 · Region: us-east-1 · CLI profile: `streaming-admin`

## BILLABLE — delete these to stop all costs

| # | Created | Resource | Cost | Delete command |
|---|---------|----------|------|----------------|
| 1 | 2026-08-10 23:23–23:47 | EKS cluster `streaming` COMPLETE: control plane + VPC (10.42.0.0/16, us-east-1a/1f) + single NAT gateway + access entry (StreamingDeploy) + nodegroup `system` (2× t3.medium, Ready) + add-ons (vpc-cni, coredns, kube-proxy, pod-identity-agent, metrics-server, aws-ebs-csi-driver incl. its pod-identity IAM role stack) | ~$0.25/hr (~$180/mo): control plane $0.10 + NAT ~$0.05 + 2 nodes ~$0.08 + EBS ~$0.01 | `AWS_PROFILE=streaming-admin eksctl delete cluster -f infra/eks/cluster.yaml --disable-nodegroup-eviction --wait` (equivalent: `eksctl delete cluster --region=us-east-1 --name=streaming`; removes all eksctl-* CFN stacks incl. addon/pod-identity roles) |

Nothing else billable exists yet. When cluster deletion finishes, verify with:
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
| 2026-08-10 | Billing setting: "IAM user and role access to Billing information" activated | account setting, free, leave on |

## DELETED / no longer exists

| When | What |
|------|------|
| 2026-08-10 | Old Identity Center instance in us-east-2 (`ssoins-6684f81e73af561b`, store `d-9a675a28f5`) incl. old `fatima` user and PowerUserAccess permission set — deleted by root via console |

## LOCAL MACHINE (this Mac) — free, listed for completeness

- Homebrew: `awscli`, `eksctl`, `kubernetes-cli`, `helm` (`brew uninstall` to remove)
- `~/.aws/config` — profile `streaming-admin` + sso-session `linkify`
- `~/.kube/config` — will gain the `streaming` cluster context when eksctl runs
