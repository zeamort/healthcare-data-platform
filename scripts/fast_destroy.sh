#!/usr/bin/env zsh
#
# fast_destroy.sh — Force-delete Lambda Hyperplane ENIs, then terraform destroy.
#
# Why: AWS Lambda-in-VPC creates elastic network interfaces on first invocation.
# When a VPC-attached Lambda is deleted, AWS takes up to ~20 minutes to release
# those ENIs on its own. Subnets and security groups can't be deleted until the
# ENIs are gone, so `terraform destroy` stalls.
#
# This script finds any "available" (unattached) ENIs in the VPC tagged with
# the Lambda SG and deletes them directly, turning a ~30 min destroy into ~10 min.
#
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Reading VPC and Lambda SG from Terraform state..."
VPC_ID=$(cd terraform && terraform output -raw vpc_id)
LAMBDA_SG=$(cd terraform && terraform output -raw lambda_security_group_id)

echo "VPC:       $VPC_ID"
echo "Lambda SG: $LAMBDA_SG"
echo

echo "Finding orphaned Lambda ENIs..."
ENI_IDS=$(aws ec2 describe-network-interfaces \
  --filters "Name=vpc-id,Values=$VPC_ID" \
            "Name=group-id,Values=$LAMBDA_SG" \
            "Name=status,Values=available" \
  --query 'NetworkInterfaces[].NetworkInterfaceId' \
  --output text)

if [[ -z "$ENI_IDS" ]]; then
  echo "No orphaned ENIs found."
else
  echo "Deleting ENIs: $ENI_IDS"
  for eni in $ENI_IDS; do
    echo "  $eni"
    aws ec2 delete-network-interface --network-interface-id "$eni" || true
  done
fi

echo
echo "Running terraform destroy..."
cd terraform
terraform destroy -auto-approve
