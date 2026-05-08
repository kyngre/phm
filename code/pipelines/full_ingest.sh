#!/usr/bin/env bash
# FD001~FD004 전체 데이터 적재 일괄 실행.
#
# 전제:
#   - `docker compose down -v` 후 `docker compose up -d` 로 클린 기동된 상태.
#   - data/raw/ 에 train_FD001.txt ~ train_FD004.txt, RUL_FD0xx.txt 존재.
#
# 단계: DDL → 토픽 → Bronze streaming → Producer → Silver → Gold(RUL+KPI)
set -euo pipefail

SPARK="phm-spark"
KAFKA="phm-kafka"
LOG_DIR="/workspace/data/_logs"
mkdir -p "$(dirname "$0")/../../data/_logs"

echo "▶ 0) 컨테이너 헬스 확인 (healthcheck 있는 서비스는 healthy 까지 대기)"
for c in phm-minio phm-iceberg-rest phm-kafka phm-spark phm-trino; do
  for i in $(seq 1 30); do
    state=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo missing)
    health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$c" 2>/dev/null || echo missing)
    if [[ "$state" == "running" && ( "$health" == "healthy" || "$health" == "none" ) ]]; then
      echo "  ✓ $c ($state, health=$health)"
      break
    fi
    [[ "$i" == "30" ]] && { echo "  × $c state=$state health=$health"; exit 1; }
    sleep 2
  done
done

echo "▶ 1) DDL 적용 (멱등 — DROP+CREATE)"
"$(dirname "$0")/../ddl/apply_all.sh"

echo "▶ 2) Kafka 토픽 생성 (이미 있으면 스킵)"
docker exec -i "$KAFKA" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic phm.engine.sensor --partitions 4 --replication-factor 1

echo "▶ 3) Python 의존성 확인 (이미지에 numpy/kafka-python 사전 포함; 누락 시만 설치)"
docker exec "$SPARK" python3 -c "import numpy, kafka" 2>/dev/null \
  || docker exec -u root "$SPARK" pip install --quiet kafka-python numpy

# Bronze checkpoint wipe — host bind mount 이라 'docker compose down -v' 로 안 지워짐.
# 이전 세션 checkpoint 가 새 Kafka 와 우연히 align 되면 streaming 이 "이미 처리됨" 으로
# 판단해 새 메시지를 모두 skip → Bronze 누락. DDL 재적용 (Bronze drop+create) 직후엔
# checkpoint 도 비우는 게 정합성에 맞다.
CKPT_DIR="$(dirname "$0")/../../data/_checkpoints/bronze_engine_sensor_raw"
if [ -d "$CKPT_DIR" ]; then
  echo "▶ 3.5) Bronze checkpoint 초기화 ($CKPT_DIR)"
  rm -rf "$CKPT_DIR"
fi

echo "▶ 4) Bronze streaming job 백그라운드 기동"
# 자기 자신 매칭 회피 위해 pgrep 은 호스트에서 docker exec 로 호출 (컨테이너 ps 에 host bash 미포함).
PGREP="docker exec $SPARK pgrep -f bronze_ingest.py"
if $PGREP >/dev/null 2>&1; then
  echo "  ✓ already running"
else
  docker exec -d "$SPARK" bash -lc "
    mkdir -p $LOG_DIR;
    nohup setsid /opt/spark/bin/spark-submit --master 'local[*]' \
      /workspace/code/pipelines/bronze_ingest.py \
      </dev/null >$LOG_DIR/bronze_ingest.log 2>&1 &
    disown || true
  "
  for i in $(seq 1 25); do
    sleep 2
    if $PGREP >/dev/null 2>&1; then
      echo "  ✓ bronze_ingest running (after ${i}x2s)"
      break
    fi
  done
  $PGREP >/dev/null 2>&1 \
    || { echo "  × 기동 실패 — log:"; docker exec "$SPARK" tail -40 "$LOG_DIR/bronze_ingest.log" 2>&1; exit 1; }
fi

echo "▶ 5) Kafka producer — FD001~FD004 전체 (Time-Travel: base=2025-08-01, 1 cycle = 1h)"
docker exec -i "$SPARK" python3 /workspace/code/pipelines/cmaps_to_kafka.py \
  --datasets FD001,FD002,FD003,FD004 \
  --base-date 2025-08-01 \
  --interval 1h \
  --unit-jitter 1h \
  --cycle-interval 1 \
  --speedup 1000

echo "▶ 6) Streaming 소화 대기 (60s)"
sleep 60

echo "▶ 7) Bronze 행수 + event_ts 분산 확인"
docker exec -i "$SPARK" /opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=phm \
  -e "SELECT dataset_id, COUNT(*) AS rows, COUNT(DISTINCT unit_id) AS units,
             MAX(cycle) AS max_cycle,
             MIN(event_ts) AS first_event, MAX(event_ts) AS last_event,
             COUNT(DISTINCT DATE(event_ts)) AS days_span
      FROM phm.bronze.engine_sensor_raw GROUP BY dataset_id ORDER BY dataset_id;"

echo "▶ 8) Silver MERGE"
docker exec -i "$SPARK" /opt/spark/bin/spark-submit --master 'local[*]' \
  /workspace/code/pipelines/silver_transform.py

echo "▶ 9) Gold RUL 예측 (gbt-v0)"
docker exec -i "$SPARK" /opt/spark/bin/spark-submit --master 'local[*]' \
  /workspace/code/pipelines/gold_rul_predict.py \
  --model-version gbt-v0 --rul-cap 130

echo "▶ 10) Gold KPI 일배치"
docker exec -i "$SPARK" /opt/spark/bin/spark-submit --master 'local[*]' \
  /workspace/code/pipelines/gold_kpi_aggregate.py

echo "▶ 11) 최종 검증"
docker exec -i "$SPARK" /opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=phm \
  -e "SELECT 'bronze' AS layer, COUNT(*) FROM phm.bronze.engine_sensor_raw
      UNION ALL SELECT 'silver', COUNT(*) FROM phm.silver.engine_health
      UNION ALL SELECT 'gold.rul', COUNT(*) FROM phm.gold.rul_prediction
      UNION ALL SELECT 'gold.kpi', COUNT(*) FROM phm.gold.fleet_kpi_daily;"

echo "✓ 전체 적재 완료. Bronze streaming 은 계속 실행 중."
echo "  중지: docker exec $SPARK pkill -f bronze_ingest.py"
