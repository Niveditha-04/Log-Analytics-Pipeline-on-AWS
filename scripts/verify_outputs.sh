#!/usr/bin/env bash
set -euo pipefail
S3_BUCKET="${S3_BUCKET:?export S3_BUCKET}"

aws s3 ls s3://$S3_BUCKET/logs/clean/ --recursive | head
aws s3 ls s3://$S3_BUCKET/logs/rejected/ --recursive | head
aws s3 ls s3://$S3_BUCKET/dq/parsing_accuracy/ --recursive | head
aws s3 ls s3://$S3_BUCKET/logs/agg/ --recursive | head
