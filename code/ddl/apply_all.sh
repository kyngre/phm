#!/usr/bin/env bash
# Bronze/Silver/Gold DDL을 Spark에서 일괄 실행
#
# 사용법:
#   ./code/ddl/apply_all.sh
#
# 사전 조건: docker compose up -d 로 phm-spark 컨테이너 기동
set -euo pipefail

CONTAINER="${SPARK_CONTAINER:-phm-spark}"
DDL_DIR_IN_CONTAINER="/workspace/code/ddl"

FILES=(
  "01_namespaces.sql"
  "02_bronze_engine_sensor_raw.sql"
  "03_silver_engine_health.sql"
  "03b_silver_feat_stats.sql"
  "03c_silver_pipeline_state.sql"
  "04_gold_rul_prediction.sql"
  "05_gold_fleet_kpi_daily.sql"
  "06_gold_model_metrics.sql"
)

for f in "${FILES[@]}"; do
  echo "▶ Applying $f"
  docker exec -i "$CONTAINER" /opt/spark/bin/spark-sql \
    --conf spark.sql.defaultCatalog=phm \
    -f "$DDL_DIR_IN_CONTAINER/$f"
done

echo "✓ All DDL applied. Verifying..."
docker exec -i "$CONTAINER" /opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=phm \
  -e "SHOW TABLES IN phm.bronze;
      SHOW TABLES IN phm.silver;
      SHOW TABLES IN phm.gold;"
