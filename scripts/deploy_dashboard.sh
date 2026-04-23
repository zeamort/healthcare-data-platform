#!/usr/bin/env zsh
# Build the dashboard Docker image, push it to ECR, and restart the ECS service.
#
# Run after `terraform apply` creates the ECR repo but before hitting the
# dashboard URL — without the image, the ECS task fails to pull and the
# ALB returns 503.
#
# Prerequisites:
#   - Docker Desktop running
#   - AWS credentials loaded
#   - Terraform state readable (for the ECR URL)
set -euo pipefail

cd "$(dirname "$0")/.."
REGION="${AWS_REGION:-us-east-1}"

echo "Reading ECR URL from Terraform..."
ECR_URL=$(cd terraform && terraform output -raw ecr_repository_url)
REGISTRY="${ECR_URL%/*}"
REPO_NAME="${ECR_URL##*/}"

echo "  ECR:   $ECR_URL"
echo "  Repo:  $REPO_NAME"
echo

echo "Logging in to ECR..."
aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

echo
echo "Building dashboard image (linux/amd64 for Fargate)..."
docker build --platform linux/amd64 -t "${REPO_NAME}:latest" dashboard/

echo
echo "Tagging and pushing to $ECR_URL:latest..."
docker tag "${REPO_NAME}:latest" "${ECR_URL}:latest"
docker push "${ECR_URL}:latest"

echo
echo "Forcing ECS to pull the new image..."
aws ecs update-service \
  --region "$REGION" \
  --cluster healthcare-dev-dashboard-cluster \
  --service healthcare-dev-dashboard-svc \
  --force-new-deployment \
  --query 'service.{desired:desiredCount,status:status}' \
  --output json

echo
echo "Waiting for the service to stabilise (can take 2–4 minutes)..."
aws ecs wait services-stable \
  --region "$REGION" \
  --cluster healthcare-dev-dashboard-cluster \
  --services healthcare-dev-dashboard-svc

echo
echo "Dashboard should now be live at:"
cd terraform && terraform output -raw dashboard_url
