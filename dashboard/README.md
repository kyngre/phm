# Dashboard — Superset

Superset 4.1.1 컨테이너(`phm-superset`, http://localhost:8088, admin/admin)에 Trino 를 연결해 비즈니스/운영 2개 탭을 구성한다.

## 디렉토리

```
dashboard/
├── setup_superset.sh        # Trino DB connection 등록 자동화
├── sql/                     # 차트별 SQL (가상 데이터셋 또는 SQL Lab 시작점)
│   ├── biz_01_rul_distribution.sql
│   ├── biz_02_top_risk_engines.sql
│   ├── biz_03_degradation_by_cluster.sql
│   ├── biz_04_maintenance_queue.sql
│   ├── ops_01_freshness.sql
│   ├── ops_02_volume_files.sql
│   ├── ops_03_iceberg_files.sql
│   ├── ops_04_snapshot_growth.sql
│   ├── ops_05_merge_ops.sql
│   └── ops_06_mae_drift.sql
└── screenshots/             # 완성된 대시보드 이미지 (논문 figure 용)
```

## 셋업

```bash
docker compose -f infra/docker-compose.yml up -d superset
# 첫 기동 시 init 까지 ~1분 — 로그가 'Running on ...' 출력 후
./dashboard/setup_superset.sh
# → "Trino-Iceberg" DB connection 생성
```

수동 등록 시 SQLAlchemy URI:
```
trino://admin@trino:8080/iceberg
```

## 시간 필터 — 중요

시뮬레이션 데이터는 **`event_ts` 가 과거 (default 2025-08-01 시작)** 에 분산. Superset 의 글로벌 시간 필터를 `event_ts` 기반으로 두지 않으면 "Last 7 days" 같은 디폴트가 0 행을 반환.

권장 설정:
- **대시보드 → Filters → + Add filter → Time range**
  - Column: `event_ts` (또는 차트별 `kpi_date`, `predict_ts`)
  - Default value: `Custom — 2025-08-01 to 2025-09-30`
- 차트별 시간 컬럼:
  - biz_03 / KPI → `kpi_date`
  - biz_01 / RUL 분포, ops_06 / MAE drift → `predict_date`
  - ops_02 / 일자별 행수 → `d` (DATE(ingest_ts))

`Last 7 days` 같은 상대 필터는 시뮬 데이터에선 항상 빈 결과 — **절대 시각 필터** 필수.

## 차트 정의

### 비즈니스 탭

| 차트 | SQL | 차트 타입 | X / Y / Series | 필터 |
|---|---|---|---|---|
| Fleet RUL 분포 | `biz_01_rul_distribution.sql` | Histogram | rul_pred (bin=10) / count | predict_date, dataset_id, model_version |
| 위험 엔진 Top 10 | `biz_02_top_risk_engines.sql` | Table | — | dataset_id |
| 운영조건별 열화율 추이 | `biz_03_degradation_by_cluster.sql` | Line | kpi_date / avg_degradation_rate (또는 health_index_p50) / op_condition_cluster | dataset_id |
| 정비 권고 큐 | `biz_04_maintenance_queue.sql` | Table (color: risk_tier) | — | dataset_id, risk_tier |

### 운영 탭

| 차트 | SQL | 차트 타입 | 메모 |
|---|---|---|---|
| 데이터 신선도 | `ops_01_freshness.sql` | Big Number | dataset 별 카드 4장. gap_minutes > 30 → 빨강 |
| 일자별 행 수 | `ops_02_volume_files.sql` | Bar (stacked by dataset) | 파일 수·크기는 ops_03 |
| Iceberg 파일 분포 | `ops_03_iceberg_files.sql` | Table | small_ratio > 0.3 → 빨강 (conditional formatting) |
| Snapshot 추이 | `ops_04_snapshot_growth.sql` | Time-series Line | series = tbl, cumulative count |
| MERGE 작업 패턴 | `ops_05_merge_ops.sql` | Time-series Bar | X = committed_at, series = operation, Y = added_rows |
| MAE drift | `ops_06_mae_drift.sql` | Time-series Line + rolling mean(7) | series = model_version |

## 차트 만드는 절차 (UI)

1. **SQL Lab → Trino-Iceberg 선택 → SQL 붙여넣기 → Run** 으로 결과 확인.
2. **Save → Save as Dataset** (가상 dataset 으로 저장) 또는 **Explore** 바로 진입.
3. 차트 빌더에서 위 표대로 X/Y/Series/Filter 설정 → Save.
4. **Dashboards → + Dashboard** → 비즈니스/운영 탭 분리 → 차트 드래그.

## Export & 버전관리

대시보드는 비즈니스/운영 탭을 분리해 각각 export → 이 디렉토리에 보관:

| 파일 | 대시보드 |
|---|---|
| `dashboard_export_biz.zip` | 비즈니스 탭 (RUL 분포, 위험 엔진, 열화율, 정비 큐) |
| `dashboard_export_ops.zip` | 운영 탭 (신선도, 행수/파일, snapshot, MERGE, MAE drift) |

```
Dashboards → … → Export → dashboard_export_<biz|ops>.zip
```

다른 환경에서는:
```
Dashboards → Import → 위 zip 업로드 → DB connection 을 'Trino-Iceberg' 로 매핑
```

스크린샷은 `dashboard/screenshots/biz.png`, `ops.png` 로 저장.

## 임계값 일관성

대시보드의 색상 임계는 health-queries 와 동일하게 유지:
- gap_minutes > 30 → 빨강  ([health-queries/01_sensor_dropout.sql](../code/health-queries/01_sensor_dropout.sql))
- small_ratio > 0.3 → 빨강 ([03_small_files_ratio.sql](../code/health-queries/03_small_files_ratio.sql))
- MAE 7일 이동평균 +20% → 알림 ([05_rul_mae_drift.sql](../code/health-queries/05_rul_mae_drift.sql))
- risk_tier CRITICAL ≤ 10 cycle, HIGH ≤ 30 cycle (`code/pipelines/gold_rul_predict.py`)
