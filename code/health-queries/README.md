# Health Queries — 매일 5분 운영 점검 (Trino 기준)

| # | 파일 | 점검 항목 | 임계 / 액션 |
|---|---|---|---|
| 1 | `01_sensor_dropout.sql` | 엔진별 마지막 ingest 시각 | gap > 30분 → 시뮬레이터/Producer 점검 |
| 2 | `02_daily_volume.sql` | dataset×cluster 일자 행수 | 어제 대비 ±50% → 알림 |
| 3 | `03_small_files_ratio.sql` | <128MB 파일 비율 | > 30% → `rewrite_data_files` 트리거 |
| 4 | `04_snapshot_growth.sql` | snapshot 개수·기간 | 90일 초과 → `expire_snapshots` |
| 5 | `05_rul_mae_drift.sql` | 모델 일자 MAE/RMSE/PHM08 | MAE 7일 이동평균 +20% → 재학습 |
| 6 | `06_silver_merge_conflicts.sql` | Silver commit 패턴 | replace 비율 ↑ 또는 빈 commit → 점검 |
| 7 | `07_op_condition_drift.sql` | cluster 분포 7일 변화 | \|Δpct\| > 0.05 → data drift |
| 8 | `08_model_version_consistency.sql` | 모델 버전 간 예측 차이 | mean_abs_diff > 15 cycle → 회귀 가능성 |

## 실행

Trino CLI:
```bash
docker exec -i phm-trino trino --catalog iceberg < code/health-queries/01_sensor_dropout.sql
```

일괄 실행:
```bash
for f in code/health-queries/0*.sql; do
  echo "=== $f ==="
  docker exec -i phm-trino trino --catalog iceberg < "$f"
done
```

> Iceberg 메타테이블 (`<table>$snapshots`, `<table>$files`) 은 Trino 455 에서 지원.
