# Code

SQL DDL, 파이프라인, 운영 헬스 쿼리, Iceberg 유지보수 스크립트 모음.

```
데이터 흐름 관점:

  [설계] ddl/             ← 테이블 구조 정의 (스키마)
     ↓
  [실행] pipelines/       ← 실제 데이터 이동 · 변환
     ↓
  [감시] health-queries/  ← 이상 감지
     ↓
  [정비] maintenance/     ← 파일 정리 · 최적화
```

---

## `ddl/` — 테이블 설계도

테이블 구조(컬럼·파티션·Iceberg 속성)를 정의. 최초 환경 세팅 시, 또는 스키마 변경 시 실행.

| 파일 | 내용 |
|---|---|
| `01_namespaces.sql` | `phm.bronze` / `phm.silver` / `phm.gold` 네임스페이스 생성 |
| `02_bronze_engine_sensor_raw.sql` | 원본 센서 스트림 테이블 |
| `03_silver_engine_health.sql` | 정제 + Health Index + 운영조건 클러스터 테이블 |
| `04_gold_rul_prediction.sql` | 엔진별 RUL 예측 테이블 |
| `05_gold_fleet_kpi_daily.sql` | Fleet 단위 일배치 KPI 테이블 |
| `06_gold_model_metrics.sql` | 모델 버전별 성능 지표 테이블 |
| `apply_all.sh` | 위 6개 SQL 파일 일괄 실행 (DROP+CREATE 멱등) |

```bash
./code/ddl/apply_all.sh
```

---

## `pipelines/` — 데이터 이동 · 변환 엔진

데이터를 어디서 어디로 어떻게 옮기고 가공하는지를 구현. 매일 배치 실행 또는 실시간 시연 시 사용.

| 파일 | 역할 | 입력 → 출력 |
|---|---|---|
| `cmaps_to_kafka.py` | C-MAPSS `.txt`를 읽어 Kafka로 발행 (실시간 시뮬레이터) | `data/raw/` → Kafka `phm.engine.sensor` |
| `bronze_ingest.py` | Spark Structured Streaming, 상시 실행 | Kafka → `phm.bronze.engine_sensor_raw` |
| `silver_transform.py` | KMeans · 정규화 · Health Index · RUL 레이블 계산 후 MERGE | Bronze → `phm.silver.engine_health` |
| `gold_rul_predict.py` | GBT 모델 학습 + 추론, 신뢰구간 · risk_tier 계산 | Silver → `phm.gold.rul_prediction`, `phm.gold.model_metrics` |
| `gold_kpi_aggregate.py` | Fleet 위험도 · 열화율 일배치 집계 | Silver + Gold → `phm.gold.fleet_kpi_daily` |
| `full_ingest.sh` | 위 5개를 순서대로 한 번에 실행 | — |

```bash
# 전체 파이프라인 한 번에
./code/pipelines/full_ingest.sh
```

자세한 실행 옵션은 [pipelines/README.md](pipelines/README.md) 참고.

---

## `health-queries/` — 운영 이상 감지 (매일 5분 점검)

Trino에서 실행하는 운영 헬스 쿼리 8개. 각 파일이 하나의 점검 항목을 담당.

| 파일 | 점검 항목 | 임계 / 액션 |
|---|---|---|
| `01_sensor_dropout.sql` | 엔진별 마지막 데이터 도착 시각 | gap > 30분 → Producer 점검 |
| `02_daily_volume.sql` | 일자별 행 수 추이 | 어제 대비 ±50% → 알림 |
| `03_small_files_ratio.sql` | Iceberg 작은 파일 비율 | > 30% → `rewrite_data_files` 트리거 |
| `04_snapshot_growth.sql` | snapshot 누적 개수 | 90일 초과 → `expire_snapshots` 트리거 |
| `05_rul_mae_drift.sql` | RUL 예측 MAE 추이 | 7일 이동평균 +20% → 모델 재학습 |
| `06_silver_merge_conflicts.sql` | Silver MERGE 충돌 · 재시도 빈도 | replace 비율 급증 → 점검 |
| `07_op_condition_drift.sql` | 운영조건 클러스터 분포 변화 | \|Δpct\| > 0.05 → data drift |
| `08_model_version_consistency.sql` | 모델 버전 간 예측 불일치 | mean_abs_diff > 15 cycle → 회귀 가능성 |

```bash
# 단일 쿼리
docker exec -i phm-trino trino --catalog iceberg < code/health-queries/01_sensor_dropout.sql

# 전체 일괄 실행
for f in code/health-queries/0*.sql; do
  echo "=== $f ==="
  docker exec -i phm-trino trino --catalog iceberg < "$f"
done
```

Airflow `health_check_dag`에서 매일 자동 실행. 자세한 내용은 [health-queries/README.md](health-queries/README.md) 참고.

---

## `maintenance/` — Iceberg 파일 정리 (주기적 실행)

스트리밍 MERGE가 만드는 작은 파일 폭증과 오래된 snapshot을 주기적으로 정리. **실행 순서가 중요하며 `run.sh`가 올바른 순서를 보장한다.**

| 파일 | 절차 | 주기 | 트리거 |
|---|---|---|---|
| `01_rewrite_data_files.sql` | 작은 파일 → 128MB로 합침 (compaction) | 일 1회 | `health-queries/03` > 30% |
| `02_rewrite_manifests.sql` | 메타데이터 매니페스트 정리 | 주 1회 | metadata read 지연 시 |
| `03_expire_snapshots.sql` | 90일 지난 snapshot 삭제 | 주 1회 | snapshot 수 누적 |
| `04_remove_orphan_files.sql` | 실패한 write가 남긴 고아 파일 삭제 | 월 1회 | 실패한 write/컴팩션 후 |

```bash
# 전체 단계 순서대로 실행
./code/maintenance/run.sh

# 단일 단계만 실행
./code/maintenance/run.sh 01_rewrite_data_files
```

> `rewrite_data_files` 직후 즉시 `expire_snapshots`하면 컴팩션 전 파일까지 삭제되어 롤백 불가. 반드시 ETL 검증을 사이에 두고 실행할 것.

Airflow `iceberg_compaction_dag`에서 스케줄 자동 실행. 자세한 내용은 [maintenance/README.md](maintenance/README.md) 참고.
