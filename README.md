# 최종 프로젝트 — 항공기 엔진 PHM 데이터 레이크하우스

> NASA C-MAPSS 터보팬 엔진 데이터셋(FD001~FD004)을 운항 fleet의 실시간 센서 스트림으로 가정한
> Iceberg 기반 메달리온 데이터 레이크하우스 + RUL(Remaining Useful Life) 예측 파이프라인.
> 데이터 엔지니어링 운영 가시성과 PHM(Prognostics & Health Management) 학술 기여를 동시에 목표로 합니다.

---

## 0. 데이터 — 원본 vs 시뮬레이션

### 0-1. 원본 — NASA C-MAPSS Turbofan Degradation Dataset
[Saxena et al., 2008] — 항공 터보팬 엔진의 cycle 단위 degradation simulation.
| 파일 | 용도 | 행 수 | 형식 |
|---|---|---|---|
| `train_FD00x.txt` | run-to-failure (학습) | 20K~61K | 26 cols × N cycles |
| `test_FD00x.txt` | partial trajectory (평가) | 13K~41K | 동일 |
| `RUL_FD00x.txt` | test 의 마지막 cycle 시점 정답 RUL | 100~249 | 1 col |

> **파이프라인 vs 실험 파일 분리**: `full_ingest.sh` 및 Kafka 시뮬레이터는 **train 파일만** 사용 (run-to-failure 궤적 전체가 있어야 `rul_label = max_cycle − cycle` 계산 가능). `test_FD00x.txt`·`RUL_FD00x.txt` 는 `experiments/lstm_baseline.py` 에서 hold-out 평가용으로 별도 소비함.

데이터는 [NASA Prognostics CoE Data Repository](https://www.nasa.gov/intelligent-systems-division/discovery-and-systems-health/pcoe/pcoe-data-set-repository/) 의 *Turbofan Engine Degradation Simulation Data Set* 에서 받아 `data/raw/` 에 풀어 넣는다 (`train_FD001.txt` ~ `RUL_FD004.txt` 12개 파일). `data/raw/` 는 [.gitignore](.gitignore) 대상.

26 컬럼 = `unit_id, cycle, op_setting_1~3, sensor_1~21`. 1 row = 1 비행 cycle. 4개 dataset(FD001~FD004) 은 운영 조건 / fault mode 조합으로 난이도 차이.

| dataset | op condition | fault mode | train rows |
|---|---|---|---|
| FD001 | 1 (sea level) | HPC degradation | 20,631 |
| FD002 | 6 (다양한 고도/스로틀) | HPC | 53,759 |
| FD003 | 1 | HPC + Fan | 24,720 |
| FD004 | 6 | HPC + Fan | 61,249 |

### 0-2. 시뮬레이션 — `cmaps_to_kafka.py` (Time-Travel Simulation)

원본 txt 는 정적 file. 운영 lakehouse 검증을 위해 Kafka 스트림으로 변환하면서 **시간 의미를 합성**해 발행.

**event_ts 합성 공식**:
```
event_ts = base_date  +  unit_jitter × unit_id  +  interval × (cycle − 1)
```
- `--base-date 2025-08-01` (UTC) — 시뮬 기준 시각, 동일 base 면 데이터 재현 가능
- `--interval 1h` — 1 cycle = 1 비행 ≈ 1 시간 (PHM 추상화)
- `--unit-jitter 1h` — 엔진별 출발 시점 분산 (real fleet 흉내)

**예시** (FD001, unit=5, cycle=10): `2025-08-01T00:00 + 1h×5 + 1h×9 = 2025-08-01T14:00`.

| 인자 | 역할 | 기본값 |
|---|---|---|
| `--base-date` | event_ts 기준 시각 | `2025-08-01` |
| `--interval` | 1 cycle 의 시간 폭 | `1h` |
| `--unit-jitter` | unit 별 출발 시점 분산 | `1h` |
| `--cycle-interval` / `--speedup` | **발행 페이싱** (sleep만, event_ts 와 무관) | `60s / 600` |
| `--max-units` | dataset 당 unit 제한 (스모크) | (전체) |
| `--dry-run` | Kafka 미발행, 파싱·event_ts 합성만 검증 | off |

**왜 분리했나** — `event_ts` 는 **데이터 시간** (시뮬 도메인), `ingest_ts` / 발행 페이싱은 **메타 시간** (실시간 적재). Iceberg time-travel 의 두 시간 축 (`AS OF snapshot` vs `WHERE event_ts BETWEEN`) 을 모두 시연 가능.

**산출 분포** (default 인자, FD001~FD004 전체):
- event_ts span: 2025-08-01 ~ 2025-09-03 (약 33일, FD004 max unit≈249 × 1h + max cycle≈543 × 1h = 791h)
- 일자 분산: 약 33 × 4 dataset = ~130 day 파티션 (dataset 별 span 상이: FD001≈19일, FD002≈27일, FD003≈26일, FD004≈33일)
- 멱등: 같은 인자로 재시뮬 → bit-identical Silver/Gold

**대안 시나리오**:
| 의도 | 인자 |
|---|---|
| 90일 백필 시연 | `--base-date 2025-02-01 --interval 1d` |
| 100x 고밀도 (partition 폭증 스트레스) | `--interval 5m` |
| Fleet 동시성 (출발점 동기화) | `--unit-jitter 0s` |

---

## 1. 도메인 정의 + 핵심 KPI 3개

**도메인**: 항공사/MRO 사업자의 터보팬 엔진 fleet 상태 모니터링 및 잔여수명 예측.

**핵심 KPI**
1. **Fleet 위험도 지표** — RUL ≤ 30 cycle 엔진 비율 (조기 경보 커버리지)
2. **예측 정확도** — RUL MAE / PHM08 Score (학술 표준 지표)
3. **데이터 신선도 & 운영 안정성** — 센서 dropout 시간, MERGE commit 패턴(replace 비율·추가 행수; OCC 충돌은 Spark log에서 확인), 컴팩션 후 평균 파일 크기

---

## 2. 전체 아키텍처

```
[C-MAPSS txt] → [Kafka 시뮬레이터] → [Spark Structured Streaming]
                                            │
                                            ▼
                                  ┌─────────────────────┐
                                  │ Iceberg REST Catalog │
                                  │   (MinIO warehouse)  │
                                  └──────────┬──────────┘
                          ┌─────────────────┼──────────────────┐
                          ▼                 ▼                  ▼
                    Bronze (Iceberg)  Silver (Iceberg)   Gold (Iceberg)
                       raw 센서        정제·HI·피처       RUL 예측·KPI
                          │                 │                  │
                          └─────── Airflow ─┴──────────────────┘
                                   compaction / expire / orphan
                                            │
                                  Trino → Superset 대시보드
```

- **환경 선택**: 로컬 Docker (Spark + MinIO + **Iceberg REST Catalog** + Trino + Kafka + Airflow + Superset) — 상세는 [infra/README.md](infra/README.md).
- **빠른 시작** (선행 3단계 필수):
  ```bash
  # 1) NASA C-MAPSS 데이터 배치 — train_FD001.txt ~ RUL_FD004.txt 12개를 data/raw/ 에
  # 2) 커스텀 이미지 빌드 (최초 1회)
  docker compose -f infra/docker-compose.yml build
  # 3) 자격증명 파일 준비 (기본값으로도 동작)
  cp infra/.env.example infra/.env
  # 4) 전체 파이프라인 실행
  docker compose -f infra/docker-compose.yml up -d
  ./code/pipelines/full_ingest.sh
  ```
  DDL → 토픽 → Bronze streaming → Producer(FD001~FD004, **base_date=2025-08-01, 1 cycle = 1h**) → Silver → Gold(RUL+KPI). 단계별 동작은 [code/pipelines/README.md](code/pipelines/README.md#한-번에--full_ingestsh). 시뮬레이션 모델은 [§0-2](#0-2-시뮬레이션--cmaps_to_kafkapy-time-travel-simulation).
- **AWS 매핑**: MinIO ↔ S3, Iceberg REST ↔ Glue Iceberg REST/Tabular, Trino ↔ Athena, Spark ↔ EMR/EKS, Kafka ↔ MSK.
- **카탈로그 설계 결정**: Hive Metastore 대신 REST Catalog 채택 — arm64 네이티브, 단일 컨테이너, Spark·Trino 동일 인터페이스. 자세한 근거는 [infra/README.md](infra/README.md).

---

## 3. 메달리온 3계층 의사결정

### 3-1. Bronze (raw)
- **테이블**: `phm.bronze.engine_sensor_raw`
- **컬럼**: `dataset_id, unit_id, cycle, op_setting_1~3, sensor_1~21, event_ts, ingest_ts, source_file, line_no`
- **시간 컬럼 분리**:
  - `event_ts` — 시뮬레이션상의 비행 발생 시각 (producer 가 `base_date + unit_jitter × unit_id + interval × (cycle−1)` 로 합성)
  - `ingest_ts` — Bronze 적재 시각 (Spark `current_timestamp()`)
- **파티션**: `dataset_id, days(ingest_ts)` — Silver는 `event_ts`(비행 발생 시각) 기준이지만, Bronze는 "언제 적재됐는가"(감사·장애 추적)가 목적이므로 `ingest_ts` 사용. 실시간 적재 흐름 추적에 최적.
- **목적**: 원본 보존 → 백필·재처리·감사 가능.

### 3-2. Silver (processed)
- **테이블**: `phm.silver.engine_health`
- **변환**:
  1. 분산 0인 무정보 센서(1,5,6,10,16,18,19) 제외 → 잔여 14개(`s2,s3,s4,s7~s9,s11~s15,s17,s20,s21`) cluster별 z-score 정규화
  2. `op_setting_1~3` 기반 KMeans(k=6, `seed=42`) → `op_condition_cluster` (FD002/004 6 운영조건 대응)
  3. 5-cycle rolling window → `s_avg_w5`, `s_std_w5`, `s_trend_w5`
  4. Health Index = `1 − min(|s_avg_w5|, 3) / 3` (0=열화, 1=정상; cluster 평균 대비 편차 기반)
  5. `rul_label = max(cycle) − cycle` (train 가정: 최종 cycle = 고장 시점)
- **파티션**: `dataset_id, days(event_ts)` — **시계열 쿼리 가속** (drift, KPI 추이). op_condition_cluster는 데이터 컬럼 (partition으로 두면 6 × days 수만큼 파티션 폭증).
- **MERGE INTO**로 멱등 백필 (`dataset_id, unit_id, cycle` 키) — `WHEN MATCHED UPDATE + WHEN NOT MATCHED INSERT`.
- **재현성 조건**: KMeans `seed=42` + 센터 오름차순 재정렬(`stable_cluster_ids`)로 매 실행 cluster 번호를 안정화. 이 조건이 충족될 때만 동일 입력 → 동일 Silver 행 보장.

### 3-3. Gold (summary)
- **테이블 · 파티션 · MERGE 키**:

  | 테이블 | 내용 | 파티션 | MERGE 키 |
  |---|---|---|---|
  | `phm.gold.rul_prediction` | 엔진별 RUL 예측 + 신뢰구간 + `risk_tier`. `rul_actual`은 train 데이터에서만 유효 (test 시점에는 NULL) | `(model_version, dataset_id, days(predict_ts))` | `(model_version, dataset_id, unit_id, cycle)` |
  | `phm.gold.fleet_kpi_daily` | fleet 위험도·운영조건별 열화율 일배치 | `(months(kpi_date), dataset_id)` | `(kpi_date, dataset_id, op_condition_cluster)` |
  | `phm.gold.model_metrics` | 모델 버전 × split (train / holdout / operational) × dataset 별 MAE / RMSE / PHM08 | `(model_version)` | `(model_version, dataset_id, eval_window_end, eval_split)` |
  | `phm.gold.dq_results` | 데이터 품질 검증 결과 (NULL/finite/dup/cycle/rul/cluster/freshness/count/NaN; §5-2) | `(run_date)` | `(rule_name, layer, dataset_id, run_date)` |
  | `phm.gold.pipeline_state` | 추론 watermark + active 모델 경로 (학습/추론 분리) | — | `(pipeline_name)` |

- **모델 — 학습/추론 분리** (B 프레이밍, 운영 시스템 패턴):
  - `gold_train_dag` (월 07:00, 주 1회): unit-level **80/20 holdout split** 으로 GBT 학습 → MinIO `s3a://warehouse/models/<model_version>/<ts>/` 에 PipelineModel 저장 → `model_metrics` 에 `train` / `holdout` 분리 기록 (정직한 일반화 성능) → `pipeline_state.active_model_path` 갱신.
  - `gold_rul_predict_dag` (매시 :15): `pipeline_state.active_model_path` 에서 PipelineModel 로드 → silver 변경분 + 영향 unit 의 모든 cycle 추론 → `rul_prediction` MERGE. **재학습 X**.
  - 이전 설계 (매 run train+predict 동시) 는 *데이터 누수* (train 데이터에서 추론) 였음 — `experiments/lstm_baseline.py` 가 보여주는 정직한 80/20 split 패턴을 운영 GBT 도 따르도록 정렬.
- **모델 비교**: 학술 비교용 LSTM/CNN 은 `experiments/lstm_baseline.py` (별도 실행). 같은 `model_metrics` 테이블에 다른 `model_version` 으로 적재되어 SQL 한 줄 비교.
- **멱등**: 세 테이블 모두 MERGE INTO — 같은 키 재실행 시 UPDATE, 신규는 INSERT.
- **활용**: 대시보드 직접 쿼리, 시계열 일관성을 위해 time-travel 사용.

---

## 4. 왜 그냥 Parquet + Glue 가 아니라 Iceberg 인가?

> **요지**: 이 프로젝트가 *Iceberg 없이* 같은 요구사항(증분 ETL, 모델 데이터 재현, streaming 정합성, 멀티 엔진 read, DQ 시계열 추적, 운영 자동화)을 만족시키려면 **별도 시스템 / 보조 테이블 / 직접 구현 코드** 가 여러 군데로 분기된다. Iceberg 가 주는 실질 이득은 "신기능" 이 아니라 *분기됐을 책임을 한 카탈로그가 흡수해 운영 표면적이 줄어든다* 는 것. 8개 개별 기능을 아래 **3축** 으로 묶어, 각 축이 비-Iceberg 대비 무엇을 *없애는지* 만 정리한다.

### 4-1. Snapshot 하나로 — 증분 watermark + ML lineage + DQ 시계열

snapshot id 가 이 프로젝트의 세 군데에서 동시에 사용된다. 별도 메타 시스템 없이 *Iceberg 카탈로그 자체* 가 시계열 축 역할.

| 용도 | 어디 | 효과 |
|---|---|---|
| 증분 ETL watermark | `phm.silver.pipeline_state.last_snapshot_id` + Iceberg `start-snapshot-id` 옵션 | Silver 가 Bronze 누적 N 과 무관한 비용으로 매시간 incremental |
| ML 학습 데이터 lineage | `gold.model_metrics.silver_snapshot_id`, `gold.rul_prediction.silver_snapshot_id` | "이 모델이 어떤 Silver 상태로 학습됐나" 가 `AS OF snapshot` 으로 재현 |
| DQ 결과 시계열 추적 | `phm.gold.dq_results` (Iceberg) | NaN 비율 추세 같은 시계열 추적이 `WHERE run_date BETWEEN ...` 한 줄 |

- 코드: [silver_transform.py](code/pipelines/silver_transform.py) `run_incremental`, [gold_rul_predict.py](code/pipelines/gold_rul_predict.py), [DDL 07](code/ddl/07_gold_dq_results.sql)
- 검증: [test_silver_incremental.py](tests/integration/test_silver_incremental.py) `test_incremental_matches_full` — incremental 결과가 full 과 비트 동일

**Parquet + Glue 라면 추가로 들었어야 할 것**: ① watermark 용 별도 컬럼 (`max(ingest_ts)`) + 디렉토리 스캔, ② 학습 데이터 archive (파일 복사 또는 lineage 보조 테이블), ③ Prometheus / 별도 metric store. 게다가 동시 streaming/batch write 시 *commit boundary* 부재로 race window 가 남는다.

### 4-2. ACID write — at-least-once stream 을 exactly-once 로 마무리

Spark Structured Streaming 은 본질적으로 at-least-once. 이 프로젝트는 두 단계로 정합성을 잡고, 그 자체를 자동 검증한다.

- **Bronze**: `writeTo(...).append()` 로 단순 append (batch 별 read 비용 0) — dup 가능성 인정
- **Silver/Gold**: `MERGE INTO ... ON (멱등키)` 로 dedup — Silver `(dataset_id, unit_id, cycle)`, Gold rul `(model_version, dataset_id, unit_id, cycle)`, model_metrics `(model_version, dataset_id, eval_window_end)`, dq_results `(rule_name, layer, dataset_id, run_date)`
- **자동 회귀 감시**: 매일 `silver_count_matches_bronze` DQ rule

코드: [bronze_ingest.py](code/pipelines/bronze_ingest.py) `append_batch`, [silver_transform.py](code/pipelines/silver_transform.py) `dedup_bronze`, [dq_check.py](code/pipelines/dq_check.py)

**Parquet + Glue 라면 추가로 들었어야 할 것**: ACID upsert 미지원 → staging table + INSERT OVERWRITE PARTITION 패턴 직접 구현. partial write 가 reader 에 노출되는 race window 존재. 재시도 안전성을 매 단계 직접 확보.

### 4-3. Open table + 빌트인 운영 — 멀티 엔진 / hidden partition / 유지보수가 한 카탈로그 안

세 가지 운영 부담이 *추가 시스템 없이* 흡수된다.

1. **멀티 엔진 read 일관성**: Spark MLlib (학습) ↔ Trino (BI/health-queries) ↔ Superset (대시보드) 이 *복제 없이* 같은 `phm.silver.engine_health` 를 read. ACID + time-travel 보장. → [infra/](infra/)
2. **Hidden partitioning**: 사용자는 `WHERE ingest_ts > '2025-09-01'` 만 작성. partition 컬럼이 contract 에 노출되지 않아 `days(ingest_ts) → hours(ingest_ts)` 변경에도 다운스트림 쿼리 무수정. → [02_bronze DDL](code/ddl/02_bronze_engine_sensor_raw.sql)
3. **유지보수 빌트인**: 작은 파일 폭증 / snapshot 누적 / orphan file 을 `rewrite_data_files` / `expire_snapshots` / `remove_orphan_files` 프로시저로 흡수, 일/주/월 DAG 로 자동화. → [code/maintenance/](code/maintenance/), [iceberg_compaction_dag](orchestration/dags/iceberg_compaction_dag.py), [iceberg_expire_dag](orchestration/dags/iceberg_expire_dag.py), [iceberg_orphan_cleanup_dag](orchestration/dags/iceberg_orphan_cleanup_dag.py)

**Parquet + Glue 라면 추가로 들었어야 할 것**: ① Glue 는 *읽기 호환* 만 제공, transactional consistency 부재 → 엔진 간 일관성을 직접 확보. ② partition 컬럼이 사용자 contract 의 일부 → 전략 변경 시 모든 다운스트림 쿼리 수정 + 백필. ③ 작은 파일 압축 / snapshot GC / orphan 추적을 모두 직접 구현 (배치 작업 3종 + 모니터링).

### 한 줄 요약

> "Iceberg 의 모든 기능을 다 썼다" 가 가치 주장이 아니다. 가치 주장은 **"Parquet + Glue 였다면 4–5개 별도 시스템 / 보조 테이블 / 직접 구현 코드로 분기됐을 능력이 한 카탈로그 안에 있어 운영 표면적이 줄었다"** 다.

### Parquet + Glue 였다면 추가로 들었어야 할 것 — 항목별 매핑

| 능력 | Parquet + Glue 의 부담 | Iceberg 가 흡수 | 본 프로젝트 근거 |
|---|---|---|---|
| 증분 ETL watermark | watermark 컬럼 + directory scan, race window | snapshot id + `start-snapshot-id` | §4-1 |
| ML 학습 데이터 재현 | 파일 archive 또는 별도 lineage 테이블 | snapshot id 컬럼 + `AS OF` | §4-1 |
| DQ 시계열 추적 | 별도 metric store (Prometheus 등) | 같은 카탈로그의 Iceberg 테이블 | §4-1 |
| Streaming 정합성 | staging + INSERT OVERWRITE 직접 구현 | MERGE INTO 멱등키 | §4-2 |
| 멀티 엔진 read | 읽기 호환만 (ACID 부재) | ACID + time-travel | §4-3 |
| Partition 전략 변경 | 사용자 contract 변경 + 백필 | hidden partitioning | §4-3 |
| 작은 파일 / snapshot / orphan | 직접 구현 (배치 3종) | 빌트인 procedure | §4-3 |
| 스키마 진화 | partition 별 호환 직접 관리 | `ALTER TABLE` | §4-3 |

---

## 5. 운영 헬스 체크 + 데이터 품질 검증

두 축으로 분리:

### 5-1. 운영 헬스 체크 (Trino, 매시간)

`code/health-queries/` 참고. 매일 5분 안에 헬스체크 가능한 8개 쿼리:

1. 엔진별 마지막 cycle 도착 시각 (센서 dropout 감지)
2. `dataset_id × condition` 별 일자 행 수 추이
3. 작은 파일(<128MB) 비율 (Bronze·Silver·Gold 3개 테이블, Iceberg `$files` 메타)
4. snapshot 증가율 (`$snapshots` 메타테이블)
5. RUL 예측 MAE drift (Gold)
6. Silver MERGE commit 패턴 (replace 비율·추가/삭제 행수; OCC 실제 충돌은 Spark log에서 확인)
7. 운영조건 클러스터 분포 변화 (data drift)
8. `model_version` 별 예측 일관성 (동일 테이블 내 버전 간 self JOIN — time-travel 아님)

DAG: `health_check_dag` — 매시 :30. **운영 메트릭 (인프라/snapshot/drift)** 위주.

### 5-2. 데이터 품질 검증 (Spark, 일배치)

`code/pipelines/dq_check.py` — *데이터 자체* 의 품질을 9개 rule 로 측정해
`phm.gold.dq_results (rule_name, layer, dataset_id, run_date)` 에 멱등 MERGE.

| rule | layer | 임계 | 의미 |
|---|---|---|---|
| `bronze_null_key_columns` | bronze | 0 행 | unit_id/cycle 가 NULL 인 행 수 |
| `bronze_finite_sensors` | bronze | 0 행 | sensor_1~21 중 NaN/Inf/NULL |
| `bronze_dup_ratio` | bronze | < 1% | (source_file, line_no) dup 비율 (Silver dedup 이 흡수, 폭주 시 streaming 재시도 의심) |
| `bronze_cycle_monotonic` | bronze | 0 unit | cycle 1..N 연속이 아닌 unit 수 (dedup 후) |
| `silver_rul_label_nonneg` | silver | 0 행 | rul_label NULL/음수 |
| `silver_cluster_in_range` | silver | 0 행 | op_condition_cluster 가 [0, 5] 외부 |
| `silver_freshness_minutes` | silver | ≤ 120분 | max(silver_ts) 와 현재 시각 간격 |
| `silver_count_matches_bronze` | silver | 차이 0 | silver 행 수 == bronze dedup 행 수 |
| `silver_nan_feature_ratio` | silver | ≤ 5% | s*_norm + rolling 의 NaN 비율 (rolling 시작부 정상치 5% 한계) |

각 결과: PASS / WARN / FAIL (warn_factor=1.5, 임계 초과지만 1.5× 이내면 WARN).

DAG: `dq_check_dag` — 매일 01:00 (gold_kpi 30분 후, compaction 2시간 전). 실패 시 다음 단계 차단은 미적용 (관측 우선); FAIL 발생 시 외부 alerting 권장.

**5-1 vs 5-2 의 분리 이유**: 운영 메트릭은 *인프라 시그널*, DQ 는 *데이터 시그널*. 같은 대시보드에 섞으면 알람 노이즈와 우선순위가 망가짐.

---

## 6. 대시보드

`dashboard/` — Superset export zip(`dashboard_export_biz.zip`, `dashboard_export_ops.zip`) 포함. 스크린샷은 미첨부.

- **비즈니스 탭**: fleet RUL 히스토그램, 위험 엔진 Top 10, 운영조건별 열화율, 정비 권고 큐
- **운영 탭**: 데이터 신선도, 행 수/파일 수/평균 크기, snapshot 추이, 컴팩션 전후 비교, MERGE commit 패턴(replace 비율)

---

## 7. 100x 스케일 아웃 시나리오 (설계만)

| 깨지는 지점 | 100x 대응 |
|---|---|
| Single Spark Streaming Job | Kafka 파티션 수 ↑ + Spark executor 수평 확장, dataset_id 별 job 분리 |
| 작은 파일 폭증 | 컴팩션 빈도 ↑ + `write.target-file-size-bytes` 튜닝, partial-progress 활성화 |
| 메타 카탈로그 부하 | 현재 단일 REST Catalog 컨테이너 → Nessie 등 분산·HA 카탈로그로 전환 검토 |
| 백필 비용 | Silver `days(event_ts)` 파티션 활용 + month 단위로 더 잘게 쪼개기 (`months(event_ts)`) |
| 모델 서빙 | 배치 → 온라인 서빙(SageMaker/Triton), Gold에 `predict_ts` 컬럼 추가·`(model_version, dataset_id, unit_id, cycle)` 멱등 키 유지 |

---

## 8. 장애·운영 시나리오

1. **Streaming OOM**: `full_ingest.sh`가 DDL 재적용 직후 Bronze checkpoint를 자동 삭제(step 3.5)하므로 스크립트 재실행으로 복구 가능. 수동 복구 시 `data/_checkpoints/bronze_engine_sensor_raw/` 삭제 후 재기동. Bronze 는 append-only 라 재시도 시 dup 이 발생할 수 있으나 Silver 진입의 `dedup_bronze()` 가 흡수 → 시스템 행 수는 동일.
2. **3개월 백필**: producer를 아래 커맨드로 재실행 → Silver MERGE → Gold 재집계. 같은 자연키(dataset/unit/cycle)면 UPDATE, event_ts 만 바뀜. **expire 정책 = 100일 (학습 윈도우 90d + 안전 마진 10d)** — Bronze/Silver DDL `history.expire.max-snapshot-age-ms = 8_640_000_000` 와 `iceberg_expire_dag.OLDER_THAN_DAYS = 100` 두 곳에 동기화. `tests/dags/test_backfill_safety.py` 가 두 값의 일치와 마진 ≥ 7d 를 강제.
   ```bash
   # Linux/WSL
   --base-date $(date -d '90 days ago' +%F) --interval 1d
   # macOS
   --base-date $(date -v-90d +%F) --interval 1d
   # 크로스 플랫폼
   --base-date $(python3 -c "from datetime import date, timedelta; print(date.today()-timedelta(90))") --interval 1d
   ```
3. **컴팩션 vs MERGE 충돌**: OCC 재시도 + `partial-progress.enabled=true`, 컴팩션은 streaming 저부하 시간대(03~05시)로 분리.
4. **재시뮬레이션 함정**: 다른 `--base-date` 로 재시뮬하면 자연키 동일 → MERGE 가 UPDATE 로 event_ts 만 덮어씀. 시계열 데이터를 보존하려면 시뮬 전 snapshot 태그 (`ALTER TABLE ... CREATE TAG`) 또는 `down -v` 로 전체 초기화.

---

## 9. 멱등성 / 재처리 가능성 설계

- **Bronze**: append-only (`writeTo(...).append()`). batch 마다 read 비용 0. 시스템 레벨 멱등성은 **Silver 진입의 `dedup_bronze()`** 가 `(source_file, line_no)` row_number=1 (first-write-wins) 으로 흡수. Spark Streaming foreachBatch 의 at-least-once 로 발생하는 dup 을 한 단계 늦게 dedup 함으로써 Bronze 의 write 비용을 데이터 누적과 무관한 *일정 비용* 으로 유지.
- **Silver**: `MERGE INTO ... ON (dataset_id, unit_id, cycle)` — matched UPDATE + not matched INSERT. KMeans `seed=42` + `stable_cluster_ids` 조건 하에 동일 입력 N회 → 동일 결과. 매시간 `silver_merge_dag` 가 `--mode incremental` (last_snapshot_id 이후 변경분만 + 영향 unit 의 모든 cycle 재변환). 주 1회 `silver_fit_stats_dag` 가 KMeans/통계 재학습.
- **Gold**: `(model_version, dataset_id, unit_id, cycle)` 키 — matched UPDATE + not matched INSERT. `predict_ts` + `silver_snapshot_id` 기록으로 time-travel 재현.
- **DQ**: `(rule_name, layer, dataset_id, run_date)` 키 — 같은 날 dq_check 재실행 시 UPDATE.
- **백필 절차**:
  ```sql
  -- ① 태그 생성
  ALTER TABLE phm.silver.engine_health CREATE TAG snap_before_backfill;
  -- ② 백필 실행 (silver_transform.py)
  -- ③ 실패 시 태그에서 snapshot_id 조회 후 롤백
  SELECT snapshot_id FROM phm.silver.engine_health.refs WHERE name = 'snap_before_backfill';
  CALL phm.system.rollback_to_snapshot(table => 'silver.engine_health', snapshot_id => <위 snapshot_id>);
  ```

---

## 10. PHM Korea 학회 기여 포인트

`paper/outline.md` 참조 (초안 목차 단계).

1. **재현 가능한 PHM MLOps 레퍼런스 아키텍처** — 데이터 레이크하우스 ↔ 모델 서빙 ↔ 운영 가시성 통합 사례.
2. **모델 vs 데이터 드리프트 분리 모니터링** — Iceberg snapshot 메타 활용 (§5).
3. **운영조건 이질성을 활용한 파티션 전략** — FD002/004 6 condition × 2 fault mode 쿼리 비용 비교.
4. **백필 프로토콜** — time-travel + MERGE 기반 일관성 보장.
5. **Iceberg 를 *파이프라인 런타임* 으로 활용한 8개 패턴** — §4-1~4-8 각각이 별도 인프라 없이 *같은 카탈로그* 안에서 시연됨. 운영 시스템에서 흔히 *부분적으로만* 갖춰지는 8개 능력 (snapshot watermark, ML lineage, append+dedup, exactly-once, schema evo, hidden partition, multi-engine, auto-maintenance, DQ time-travel) 을 단일 테이블 위에서 동시 시연.

---

## 디렉토리 구조

```
final_project/
├── README.md                  # 본 문서
├── project_guide.md           # 원본 과제 가이드
├── infra/                     # docker-compose (Spark, MinIO, Iceberg REST, Trino, Kafka, Airflow, Superset)
├── code/
│   ├── ddl/                   # Bronze/Silver/Gold Iceberg DDL
│   ├── pipelines/             # 적재·정제·집계·MERGE·RUL 예측
│   ├── health-queries/        # 운영 헬스 쿼리 8개
│   └── maintenance/           # Iceberg 유지보수 (compaction/expire/orphan)
├── orchestration/             # Airflow DAG (적재/컴팩션/expire/orphan/예측)
├── dashboard/                 # Superset export zip (비즈니스·운영 탭, 스크린샷 미첨부)
├── experiments/               # RUL baseline 학습 스크립트 (lstm_baseline.py)
├── paper/                     # PHM Korea 논문 초안
└── data/raw/                  # C-MAPSS 원본 (gitignore 대상)
```
