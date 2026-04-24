#!/usr/bin/env zsh
#
# fast_destroy.sh — tear down the stack without the 20-minute Lambda ENI wait.
#
# Why this is necessary:
#   AWS Lambda-in-VPC creates Hyperplane ENIs on first invocation. When you
#   `terraform destroy`, Lambda holds onto those ENIs for ~20 min before
#   releasing them. Subnets and security groups can't delete until the ENIs
#   are gone — so destroy stalls for the full 20 min per VPC-attached Lambda.
#
#   Detaching each Lambda from the VPC (`--vpc-config {}`) tells Lambda to
#   release its ENIs within seconds. We then delete the now-available ENIs
#   and run `terraform destroy`. ~10 min instead of ~30.
#
# Note: after this script, Terraform state is out of sync with AWS for the
# Lambda VPC config — that's fine, we're about to destroy everything anyway.
#
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Reading VPC and Lambda SG from Terraform state..."
VPC_ID=$(cd terraform && terraform output -raw vpc_id)
LAMBDA_SG=$(cd terraform && terraform output -raw lambda_security_group_id)

echo "VPC:       $VPC_ID"
echo "Lambda SG: $LAMBDA_SG"
echo

# ── 1. Detach every VPC-attached Lambda from the VPC ────────────
echo "Detaching VPC-attached Lambdas (releases their Hyperplane ENIs)..."
VPC_LAMBDAS=$(aws lambda list-functions \
  --query "Functions[?VpcConfig.VpcId=='$VPC_ID'].FunctionName" \
  --output text)

if [[ -z "$VPC_LAMBDAS" ]]; then
  echo "  No VPC-attached Lambdas found."
else
  for fn in ${=VPC_LAMBDAS}; do
    echo "  $fn"
    aws lambda update-function-configuration \
      --function-name "$fn" \
      --vpc-config '{"SubnetIds":[],"SecurityGroupIds":[]}' \
      >/dev/null
  done
fi

# ── 2. Wait for each detached Lambda to return to Active state ──
# A Lambda holds its ENIs until it goes Pending → Active after the
# vpc-config update. Polling the state is more reliable than a fixed sleep.
if [[ -n "$VPC_LAMBDAS" ]]; then
  echo
  echo "Waiting for Lambdas to return to Active (ENIs release on transition)..."
  for fn in ${=VPC_LAMBDAS}; do
    aws lambda wait function-updated-v2 --function-name "$fn" 2>/dev/null \
      || aws lambda wait function-updated --function-name "$fn" 2>/dev/null \
      || true
    echo "  $fn → Active"
  done
fi

# ── 3. Delete any orphaned ENIs in the Lambda SG ────────────────
echo
echo "Deleting orphaned ENIs in $LAMBDA_SG..."
ENI_IDS=$(aws ec2 describe-network-interfaces \
  --filters "Name=vpc-id,Values=$VPC_ID" \
            "Name=group-id,Values=$LAMBDA_SG" \
            "Name=status,Values=available" \
  --query 'NetworkInterfaces[].NetworkInterfaceId' \
  --output text)

if [[ -z "$ENI_IDS" ]]; then
  echo "  None found (Lambda may need another 30–60s — terraform will catch up)."
else
  for eni in ${=ENI_IDS}; do
    echo "  $eni"
    aws ec2 delete-network-interface --network-interface-id "$eni" 2>/dev/null || true
  done
fi

# ── 4. Destroy ──────────────────────────────────────────────────
echo
echo "Running terraform destroy..."
cd terraform
terraform destroy -auto-approve
