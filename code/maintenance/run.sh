#!/usr/bin/env bash
# Iceberg 유지보수 절차를 단계별로 실행.
#
# 사용법:
#   ./code/maintenance/run.sh                       # 전체 4단계
#   ./code/maintenance/run.sh 01_rewrite_data_files # 단일 단계
#
# 권장 주기:
#   01 rewrite_data_files   : 일 1회  (작은 파일 비율 > 30% 시)
#   02 rewrite_manifests    : 주 1회
#   03 expire_snapshots     : 주 1회
#   04 remove_orphan_files  : 월 1회
#
# 순서: rewrite_data_files → rewrite_manifests → (검증) → expire_snapshots → remove_orphan_files
set -euo pipefail

CONTAINER="${SPARK_CONTAINER:-phm-spark}"
DIR_IN_CONTAINER="/workspace/code/maintenance"

ALL=(
  "01_rewrite_data_files.sql"
  "02_rewrite_manifests.sql"
  "03_expire_snapshots.sql"
  "04_remove_orphan_files.sql"
)

if [[ $# -eq 0 ]]; then
  FILES=("${ALL[@]}")
else
  FILES=("${1%.sql}.sql")
fi

for f in "${FILES[@]}"; do
  echo "▶ Running $f"
  docker exec -i "$CONTAINER" /opt/spark/bin/spark-sql \
    --conf spark.sql.defaultCatalog=phm \
    -f "$DIR_IN_CONTAINER/$f"
done

echo "✓ Maintenance done."
