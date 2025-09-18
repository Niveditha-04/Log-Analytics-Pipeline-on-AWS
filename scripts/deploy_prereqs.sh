#!/usr/bin/env bash
set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:?Set S3_BUCKET or export in env}"

echo "Region: $AWS_REGION"
echo "Bucket: $S3_BUCKET"

# Create bucket 
if ! aws s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
  aws s3api create-bucket --bucket "$S3_BUCKET" --region "$AWS_REGION"
fi

# Folders
aws s3api put-object --bucket "$S3_BUCKET" --key "logs/raw/"
aws s3api put-object --bucket "$S3_BUCKET" --key "logs/clean/"
aws s3api put-object --bucket "$S3_BUCKET" --key "logs/rejected/"
aws s3api put-object --bucket "$S3_BUCKET" --key "logs/agg/"
aws s3api put-object --bucket "$S3_BUCKET" --key "dq/parsing_accuracy/"
aws s3api put-object --bucket "$S3_BUCKET" --key "athena-results/"

echo "Done. Now upload sample logs with date partitions:"
echo 'DT=$(date -u +"%Y-%m-%d"); HR=$(date -u +"%H"); aws s3 cp sample_logs/web.log s3://'"$S3_BUCKET"'/logs/raw/source=webapp/dt='"$DT"'/hr='"$HR"'/web.log'
