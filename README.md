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
- **빠른 시작**: `docker compose -f infra/docker-compose.yml up -d && ./code/pipelines/full_ingest.sh` — DDL → 토픽 → Bronze streaming → Producer(FD001~FD004, **base_date=2025-08-01, 1 cycle = 1h**) → Silver → Gold(RUL+KPI). 단계별 동작은 [code/pipelines/README.md](code/pipelines/README.md#한-번에--full_ingestsh). 시뮬레이션 모델은 [§0-2](#0-2-시뮬레이션--cmaps_to_kafkapy-time-travel-simulation).
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
- **파티션**: `dataset_id, days(ingest_ts)` — 운영 감사 추적 (실시간 적재 흐름)
- **목적**: 원본 보존 → 백필·재처리·감사 가능.

### 3-2. Silver (processed)
- **테이블**: `phm.silver.engine_health`
- **변환**: 결측·이상치 제거, 정규화, 운영조건 클러스터링(FD002/004의 6 condition), Health Index 계산, rolling window 피처(평균/표준편차/추세).
- **파티션**: `dataset_id, days(event_ts)` — **시계열 쿼리 가속** (drift, KPI 추이). op_condition_cluster 는 데이터 컬럼.
- **MERGE INTO**로 멱등 백필 (`dataset_id, unit_id, cycle` 키).

### 3-3. Gold (summary)
- **테이블**:
  - `phm.gold.rul_prediction` — 엔진별 예측 RUL + 신뢰구간 + `model_version`
  - `phm.gold.fleet_kpi_daily` — fleet 위험도, 운영조건별 열화율
  - `phm.gold.model_metrics` — 모델 버전별 MAE / PHM08 Score
- **활용**: 대시보드 직접 쿼리, 시계열 일관성을 위해 time-travel 사용.

---

## 4. 이 도메인에서 Iceberg가 가장 가치 있는 지점

1. **모델 재학습 시 학습 데이터 재현성** — `AS OF` time-travel로 "모델 v3 학습 시점 Silver" 그대로 복원.
2. **3개월 백필 멱등성** — HI 산출 로직 변경 시 MERGE INTO 로 안전 재처리, snapshot 롤백 가능.
3. **스키마 진화** — 신규 센서 추가 / 단위 변경에 ALTER TABLE만으로 대응(Parquet+Glue는 호환성 직접 관리).
4. **파일 관리 자동화** — 1Hz 스트리밍이 만드는 작은 파일 폭증을 `rewrite_data_files`로 정리.
5. **OCC** — 컴팩션과 streaming MERGE가 같은 파티션을 건드릴 때 충돌 감지·재시도.

> 단순 Parquet+Glue로는 (1)(2)(5)가 사실상 불가능. 이것이 PHM 운영의 핵심 가치.

---

## 5. 운영 헬스 체크 쿼리 모음

`code/health-queries/` 참고. 매일 5분 안에 헬스체크 가능한 8개 쿼리:

1. 엔진별 마지막 cycle 도착 시각 (센서 dropout 감지)
2. `dataset_id × condition` 별 일자 행 수 추이
3. 작은 파일(<128MB) 비율 (Iceberg `files` 메타)
4. snapshot 증가율 (`$snapshots` 메타테이블)
5. RUL 예측 MAE drift (Gold)
6. Silver MERGE 충돌·재시도 카운트
7. 운영조건 클러스터 분포 변화 (data drift)
8. `model_version` 별 예측 일관성 (time-travel diff)

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

1. **Streaming OOM**: checkpointLocation 복구 + `(source_file, line_no)` Bronze 멱등키.
2. **3개월 백필**: producer 를 `--base-date $(date -d '90 days ago' +%F) --interval 1d` 로 재실행 → Silver MERGE → Gold 재집계. 같은 자연키(dataset/unit/cycle)면 UPDATE, event_ts 만 바뀜. expire 정책이 학습 윈도우(예: 90일)를 침범하지 않도록 보호.
3. **컴팩션 vs MERGE 충돌**: OCC 재시도 + `partial-progress.enabled=true`, 컴팩션은 streaming 저부하 시간대(03~05시)로 분리.
4. **재시뮬레이션 함정**: 다른 `--base-date` 로 재시뮬하면 자연키 동일 → MERGE 가 UPDATE 로 event_ts 만 덮어씀. 시계열 데이터를 보존하려면 시뮬 전 snapshot 태그 (`ALTER TABLE ... CREATE TAG`) 또는 `down -v` 로 전체 초기화.

---

## 9. 멱등성 / 재처리 가능성 설계

- **Bronze**: `(source_file, line_no)` 유니크 → 재적재 안전.
- **Silver**: `MERGE INTO ... ON (dataset_id, unit_id, cycle)` — 같은 입력 N회 적용해도 동일 결과.
- **Gold**: `(model_version, dataset_id, unit_id, cycle)` 키, `predict_ts` + `silver_snapshot_id` 기록으로 time-travel 재현.
- **백필 절차**: ① Silver snapshot 태그 → ② 백필 실행 → ③ 실패 시 `ROLLBACK TO TAG`.

---

## 10. PHM Korea 학회 기여 포인트

`paper/outline.md` 참조 (초안 목차 단계).

1. **재현 가능한 PHM MLOps 레퍼런스 아키텍처** — 데이터 레이크하우스 ↔ 모델 서빙 ↔ 운영 가시성 통합 사례.
2. **모델 vs 데이터 드리프트 분리 모니터링** — Iceberg snapshot 메타 활용.
3. **운영조건 이질성을 활용한 파티션 전략** — FD002/004 6 condition × 2 fault mode 쿼리 비용 비교.
4. **백필 프로토콜** — time-travel + MERGE 기반 일관성 보장.

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
├── dashboard/                 # Superset 정의 + 스크린샷
├── experiments/               # RUL baseline 모델 노트북 (LSTM/CNN)
├── paper/                     # PHM Korea 논문 초안
└── data/raw/                  # C-MAPSS 원본 (gitignore 대상)
```
