#!/bin/sh
set -eu

target="crawler/${OBJECT_STORAGE_BUCKET}"

mc alias set crawler "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" \
  --api S3v4 --path on
mc mb --ignore-existing "$target"
mc anonymous set private "$target"

if mc ilm rule list "$target" >/dev/null 2>&1; then
  mc ilm rule remove --all --force "$target"
fi

mc ilm rule add --expire-days "$RAW_CONTENT_RETENTION_DAYS" "$target"
