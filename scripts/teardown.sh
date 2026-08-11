#!/bin/bash
# FULL TEARDOWN of the streaming platform. Discovery-based: finds every
# resource by name/tag, so it works for ANY rebuild, not just the original.
# Companion: TEARDOWN.md (the ledger this script implements).
#
# TWO PHASES, BOTH EXECUTED BY THIS SCRIPT, IN ORDER:
#   Phase A (in-cluster): runs automatically when kubectl can reach the
#     cluster; skipped cleanly when it cannot (already-deleted cluster).
#     Ordering rules it encodes (each learned the hard way — see BUILDLOG
#     gotchas 18a-d): disarm root selfHeal; LB-owning apps die while the LB
#     controller lives; CR consumers before operators; explicit child
#     deletion; wedge-breakers for orphaned finalizers and dead APIServices.
#   Phase B (AWS-side, ~25-35 min): everything CloudFormation and the CLI own,
#     with self-healing retries for the known blockers (gotchas 19-20).
#
# Requires: aws CLI with an admin profile, eksctl. Region us-east-1.
set -uo pipefail
export AWS_PROFILE="${AWS_PROFILE:-streaming-admin}" AWS_REGION=us-east-1
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
B="linkify-streaming-media-${ACCOUNT}"

wait_empty(){ # wait_empty "desc" "command producing output when NOT done" timeout_s
  local n=0 t=$(( ${3:-300} / 10 ))
  while [ -n "$(eval "$2" 2>/dev/null)" ]; do
    n=$((n+1)); [ "$n" -gt "$t" ] && { echo "TIMEOUT waiting: $1"; return 1; }
    sleep 10
  done; echo "ok: $1"
}

echo "== [A] In-cluster teardown"
if kubectl cluster-info >/dev/null 2>&1 && kubectl get ns argocd >/dev/null 2>&1; then
  echo "-- A1: disarm root selfHeal (else it resurrects deleted children)"
  kubectl patch application root -n argocd --type merge \
    -p '{"spec":{"syncPolicy":{"automated":null}}}' 2>/dev/null || true

  echo "-- A2: LB-owning apps first, while the LB controller still lives"
  kubectl delete application streaming mediamtx -n argocd --ignore-not-found --wait=false
  wait_empty "AWS load balancers released" \
    "aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerName' --output text | tr -d '[:space:]'" 600

  echo "-- A3: CR consumers before their operators"
  kubectl delete application kafka db-migrate secrets karpenter-pools -n argocd --ignore-not-found --wait=false
  sleep 30
  # wedge-breaker: KafkaTopics whose operator died carry unserviceable finalizers
  for t in $(kubectl get kafkatopics -n kafka --no-headers 2>/dev/null | awk '{print $1}'); do
    kubectl patch kafkatopic "$t" -n kafka --type merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null
  done

  echo "-- A4: everything else, explicitly (root cascade does not reliably reach grandchildren)"
  kubectl delete application root -n argocd --ignore-not-found --wait=false
  kubectl delete applications --all -n argocd --wait=false 2>/dev/null
  wait_empty "ArgoCD applications gone" \
    "kubectl get applications -n argocd --no-headers 2>/dev/null" 600 || {
      # second wedge pass for anything still stuck on kafka finalizers
      for t in $(kubectl get kafkatopics -A --no-headers 2>/dev/null | awk '{print $2}'); do
        kubectl patch kafkatopic "$t" -n kafka --type merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null
      done
      wait_empty "ArgoCD applications gone (retry)" \
        "kubectl get applications -n argocd --no-headers 2>/dev/null" 300
    }

  echo "-- A5: ArgoCD itself, then leftover namespaces"
  helm uninstall argocd -n argocd --wait --timeout 5m 2>/dev/null || true
  kubectl delete ns argocd external-secrets karpenter keda monitoring kafka streaming \
    --ignore-not-found --wait=false 2>/dev/null
  # wedge-breaker: dead aggregated APIServices poison discovery cluster-wide
  for a in $(kubectl get apiservices --no-headers 2>/dev/null | awk '$3=="False"{print $1}'); do
    kubectl delete apiservice "$a" 2>/dev/null && echo "deleted dead APIService $a"
  done
  wait_empty "PVCs gone (EBS released)" "kubectl get pvc -A --no-headers 2>/dev/null" 600
  wait_empty "extra namespaces gone" \
    "kubectl get ns --no-headers 2>/dev/null | grep -vE '^(default|kube-system|kube-public|kube-node-lease) '" 600
  echo "-- Phase A complete: cluster is bare"
else
  echo "-- cluster unreachable or already bare: skipping Phase A"
fi


echo "== [1/9] S3 gateway endpoint (BEFORE cluster delete or the VPC's CFN stack delete fails)"
VPCE=$(aws ec2 describe-vpc-endpoints --filters Name=tag:Name,Values=streaming-s3-gateway \
  --query 'VpcEndpoints[0].VpcEndpointId' --output text)
[ "$VPCE" != "None" ] && aws ec2 delete-vpc-endpoints --vpc-endpoint-ids "$VPCE" >/dev/null && echo "deleted $VPCE"

echo "== [2/9] RDS delete (async; continues during eksctl)"
aws rds delete-db-instance --db-instance-identifier streaming-db \
  --skip-final-snapshot --delete-automated-backups \
  --query 'DBInstance.DBInstanceStatus' --output text 2>/dev/null || echo "already gone"

echo "== [3/9] CloudFront disable (async; delete comes later)"
DIST=$(aws cloudfront list-distributions \
  --query "DistributionList.Items[?Origins.Items[0].DomainName=='${B}.s3.us-east-1.amazonaws.com'].Id | [0]" --output text)
if [ "$DIST" != "None" ] && [ -n "$DIST" ]; then
  ETAG=$(aws cloudfront get-distribution-config --id "$DIST" --query ETag --output text)
  aws cloudfront get-distribution-config --id "$DIST" --query DistributionConfig > /tmp/cf.json
  python3 -c "import json;d=json.load(open('/tmp/cf.json'));d['Enabled']=False;json.dump(d,open('/tmp/cf.json','w'))"
  aws cloudfront update-distribution --id "$DIST" --if-match "$ETAG" \
    --distribution-config file:///tmp/cf.json --query 'Distribution.Status' --output text
fi

echo "== [4/9] EKS cluster (~10-15 min; removes nodegroup, VPC, IRSA/pod-identity/addon stacks)"
eksctl delete cluster --region us-east-1 --name streaming --disable-nodegroup-eviction --wait

echo "== [4b/9] cluster stack retry if the VPC was blocked by the RDS security group"
# GOTCHA 19 (hit on the reference teardown): the streaming-db SG lives in
# eksctl's VPC and references the cluster SG, so the VPC delete inside the
# eksctl stack can fail with "has dependencies" -- its blocker is removed in
# step 5 below, after which one retry succeeds. Handled automatically here.
if aws cloudformation describe-stacks --stack-name eksctl-streaming-cluster      --query 'Stacks[0].StackStatus' --output text 2>/dev/null | grep -q DELETE_FAILED; then
  echo "cluster stack DELETE_FAILED -- removing RDS leftovers first, then retrying"
  aws rds wait db-instance-deleted --db-instance-identifier streaming-db 2>/dev/null
  aws rds delete-db-subnet-group --db-subnet-group-name streaming-db-subnets 2>/dev/null
  # remove EVERY non-default SG still in the eksctl VPC: the streaming-db SG
  # AND the orphaned eks-cluster-sg-* that EKS leaves behind when something
  # referenced it at cluster-delete time (both blocked the VPC on 2026-08-11)
  EVPC=$(aws cloudformation describe-stack-resources --stack-name eksctl-streaming-cluster     --logical-resource-id VPC --query 'StackResources[0].PhysicalResourceId' --output text 2>/dev/null)
  if [ -n "$EVPC" ] && [ "$EVPC" != "None" ]; then
    for G in $(aws ec2 describe-security-groups --filters Name=vpc-id,Values=$EVPC         --query 'SecurityGroups[?GroupName!=`default`].GroupId' --output text); do
      aws ec2 delete-security-group --group-id "$G" && echo "deleted blocking SG $G"
    done
  fi
  aws cloudformation delete-stack --stack-name eksctl-streaming-cluster
  aws cloudformation wait stack-delete-complete --stack-name eksctl-streaming-cluster && echo "cluster stack retry ok"
fi

echo "== [5/9] RDS leftovers"
aws rds wait db-instance-deleted --db-instance-identifier streaming-db 2>/dev/null
aws rds delete-db-subnet-group --db-subnet-group-name streaming-db-subnets 2>/dev/null && echo subnet-group-ok
DBSG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=streaming-db \
  --query 'SecurityGroups[0].GroupId' --output text)
[ "$DBSG" != "None" ] && aws ec2 delete-security-group --group-id "$DBSG" && echo "sg $DBSG ok"

echo "== [6/9] S3 bucket (empties first — HLS output from test streams lives here)"
aws s3 rm "s3://$B" --recursive --quiet 2>/dev/null
aws s3api delete-bucket --bucket "$B" 2>/dev/null && echo bucket-ok

echo "== [7/9] CloudFront delete (waits for Deployed) + OAC"
if [ "$DIST" != "None" ] && [ -n "$DIST" ]; then
  aws cloudfront wait distribution-deployed --id "$DIST"
  ETAG=$(aws cloudfront get-distribution --id "$DIST" --query ETag --output text)
  aws cloudfront delete-distribution --id "$DIST" --if-match "$ETAG" && echo dist-ok
fi
OAC=$(aws cloudfront list-origin-access-controls \
  --query "OriginAccessControlList.Items[?Name=='streaming-s3-oac'].Id | [0]" --output text)
if [ "$OAC" != "None" ] && [ -n "$OAC" ]; then
  OETAG=$(aws cloudfront get-origin-access-control --id "$OAC" --query ETag --output text)
  aws cloudfront delete-origin-access-control --id "$OAC" --if-match "$OETAG" && echo oac-ok
fi

echo "== [8/9] Route53 zone, ACM cert, secrets, Karpenter stack, IAM policies"
ZID=$(aws route53 list-hosted-zones-by-name --dns-name k8s.linkifysolutions.com \
  --query 'HostedZones[0].Id' --output text)
if [ "$ZID" != "None" ] && [ -n "$ZID" ]; then
  # delete every record the zone owns except its own NS/SOA pair
  aws route53 list-resource-record-sets --hosted-zone-id "$ZID" \
    --query 'ResourceRecordSets[?Type!=`NS` && Type!=`SOA`]' > /tmp/rrs.json
  python3 - "$ZID" <<'EOF'
import json,subprocess,sys
zid=sys.argv[1]; rrs=json.load(open('/tmp/rrs.json'))
for r in rrs:
    batch={"Changes":[{"Action":"DELETE","ResourceRecordSet":r}]}
    subprocess.run(["aws","route53","change-resource-record-sets","--hosted-zone-id",zid,
                    "--change-batch",json.dumps(batch)],check=True,capture_output=True)
print(f"deleted {len(rrs)} records")
EOF
  aws route53 delete-hosted-zone --id "$ZID" --query 'ChangeInfo.Status' --output text
fi
CERT=$(aws acm list-certificates --query "CertificateSummaryList[?DomainName=='*.k8s.linkifysolutions.com'].CertificateArn | [0]" --output text)
[ "$CERT" != "None" ] && [ -n "$CERT" ] && aws acm delete-certificate --certificate-arn "$CERT" && echo cert-ok
for s in streaming/mediamtx streaming/grafana streaming/db-endpoint streaming/ghcr; do
  aws secretsmanager delete-secret --secret-id "$s" --force-delete-without-recovery \
    --query Name --output text 2>/dev/null
done
aws cloudformation delete-stack --stack-name Karpenter-streaming
aws cloudformation wait stack-delete-complete --stack-name Karpenter-streaming && echo karpenter-stack-ok
for p in AWSLoadBalancerControllerIAMPolicy streaming-eso streaming-upload-api streaming-transcode-worker; do
  ARN="arn:aws:iam::${ACCOUNT}:policy/$p"
  for v in $(aws iam list-policy-versions --policy-arn "$ARN" \
      --query 'Versions[?!IsDefaultVersion].VersionId' --output text 2>/dev/null); do
    aws iam delete-policy-version --policy-arn "$ARN" --version-id "$v"
  done
  aws iam delete-policy --policy-arn "$ARN" 2>/dev/null && echo "policy $p ok"
done

echo "== [8b/9] orphaned Elastic IPs (GOTCHA 20: the NAT EIP can survive its NAT gateway; unassociated EIPs bill ~\$3.60/mo)"
for A in $(aws ec2 describe-addresses --query 'Addresses[?AssociationId==null].AllocationId' --output text); do
  aws ec2 release-address --allocation-id "$A" && echo "released $A"
done

echo "== [9/9] VERIFY ZERO-COST STATE (every line below should be empty)"
echo "--- CFN stacks:";   aws cloudformation list-stacks --query 'StackSummaries[?StackStatus!=`DELETE_COMPLETE`].[StackName,StackStatus]' --output text
echo "--- EC2 instances:"; aws ec2 describe-instances --filters Name=instance-state-name,Values=running,pending,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text
echo "--- Load balancers:"; aws elbv2 describe-load-balancers --query 'LoadBalancers[].LoadBalancerName' --output text
echo "--- EBS volumes:";  aws ec2 describe-volumes --query 'Volumes[].VolumeId' --output text
echo "--- VPCs:";         aws ec2 describe-vpcs --query 'Vpcs[].VpcId' --output text
echo "--- NAT gateways:"; aws ec2 describe-nat-gateways --filter Name=state,Values=available,pending --query 'NatGateways[].NatGatewayId' --output text
echo "--- Elastic IPs:";  aws ec2 describe-addresses --query 'Addresses[].AllocationId' --output text
echo "--- RDS:";          aws rds describe-db-instances --query 'DBInstances[].DBInstanceIdentifier' --output text
echo "== DONE. Manual leftover: remove the 4 'k8s' NS records from Cloudflare."
