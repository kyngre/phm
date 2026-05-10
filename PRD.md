# PRD — 항공기 엔진 PHM 데이터 레이크하우스

> 처음 이 레포를 여는 사람이 위에서부터 한 번 읽으면 "왜 만들었고, 무엇을 만들고, 어디까지가 끝인지"를 알 수 있도록 작성한 제품 요구 문서입니다.
> 코드/실행 절차는 [README.md](README.md), 인프라는 [infra/README.md](infra/README.md), 파이프라인은 [code/pipelines/README.md](code/pipelines/README.md) 를 참고하세요.

---

## 0. 한 줄 요약 (TL;DR)

항공기 터보팬 엔진의 **잔여수명(RUL, Remaining Useful Life)** 을 예측하는 데이터 파이프라인을, **Apache Iceberg 기반 메달리온 레이크하우스**(Bronze→Silver→Gold) 위에 만든다. NASA가 공개한 정적 시뮬레이션 데이터(C-MAPSS)를 **실시간 운항 fleet 의 센서 스트림처럼** Kafka 로 흘려 넣고, Spark 가 받아 정제·집계하고, Trino+Superset 으로 시각화한다. 운영자가 매일 5분 안에 파이프라인 헬스를 점검하고, 데이터 엔지니어가 3개월 백필을 안전하게 돌릴 수 있는 것이 핵심 목표다.

---

## 1. 배경 (왜 만드는가)

### 1-1. 도메인 문제
항공사·MRO(정비 사업자)는 보유 엔진 fleet 의 상태를 모니터링하고, **고장 전에** 정비를 계획하고 싶어한다.
- 너무 일찍 정비하면 → 정비비·기회비용 낭비
- 너무 늦게 정비하면 → 비행 중 고장(안전·법적 리스크)

이 trade-off 를 풀려면 "이 엔진은 앞으로 몇 사이클 더 비행 가능한가"를 데이터로 답해야 한다. 이것이 **PHM (Prognostics & Health Management)** 분야이고, 그중 핵심 출력이 **RUL (Remaining Useful Life)** 이다.

### 1-2. 데이터 엔지니어링 문제
PHM 모델이 좋아도, 그 모델을 **운영**하는 인프라가 약하면 가치가 0 이다. 운영 인프라가 답해야 하는 질문은 다음과 같다.
- 모델 v3 가 학습 시점 본 데이터를 **그대로** 다시 볼 수 있는가? (재현성)
- 정제 로직 버그를 발견했다, 3개월 치를 다시 돌려도 다른 결과·중복이 안 나오는가? (멱등 백필)
- 컴팩션과 streaming MERGE 가 같은 파티션을 동시에 건드리면? (동시성)
- 새벽에 streaming 잡이 OOM 으로 죽었다, 무손실/무중복으로 살아나는가? (재시작 안정성)

기존 "Parquet + Hive Metastore" 조합은 위 4개를 직접 풀기 어렵다. **Iceberg** 가 snapshot · OCC(낙관적 동시성) · MERGE INTO · time-travel 로 이 4개를 한 번에 풀어주기 때문에 채택한다.

### 1-3. 학술 기여
한국 PHM 학회(PHM Korea) 발표용으로, **재현 가능한 PHM MLOps 레퍼런스 아키텍처** 를 논문 형태로 정리하는 것이 부수 목표다. (`paper/outline.md` 초안)

---

## 2. 처음 보는 사람을 위한 용어 사전

| 용어 | 풀이 | 이 프로젝트에서의 의미 |
|---|---|---|
| **C-MAPSS** | NASA Commercial Modular Aero-Propulsion System Simulation | 터보팬 엔진의 시뮬레이션 데이터셋. `train_FD001.txt` ~ `RUL_FD004.txt` 12개 텍스트 파일 |
| **cycle** | 비행 1회 (이착륙 한 번) | 1 row = 1 cycle. 본 시뮬레이터는 1 cycle = 1시간으로 매핑 |
| **unit_id** | 엔진 일련번호 | dataset 별 100~249대 |
| **RUL** | Remaining Useful Life | 정답: `max_cycle − current_cycle`. 모델이 예측해야 할 값 |
| **PHM08 Score** | RUL 예측 표준 평가 지표 | 늦은 예측(과대 RUL) 에 더 큰 페널티 |
| **HI** | Health Index, 0~1 | 1=정상, 0=열화. Silver 레이어에서 계산 |
| **메달리온 (Medallion)** | Bronze→Silver→Gold 3계층 레이크하우스 패턴 | Bronze=원본, Silver=정제·피처, Gold=집계·예측 |
| **Iceberg** | Apache Iceberg 테이블 포맷 | 파일 위 메타데이터 레이어. snapshot·MERGE·time-travel 제공 |
| **MERGE INTO** | "있으면 업데이트, 없으면 인서트" SQL | 같은 데이터 N번 돌려도 결과 동일 (멱등성) |
| **OCC** | Optimistic Concurrency Control | 충돌 시 한쪽이 재시도. Iceberg 가 자동 처리 |
| **time-travel** | `AS OF snapshot_id` 로 과거 시점 테이블 조회 | 모델 학습 데이터 재현에 필수 |
| **REST Catalog** | Iceberg 메타데이터를 HTTP 로 서비스 | 본 프로젝트는 Hive Metastore 대신 이걸 사용 |
| **fleet** | 보유 엔진 전체 | "fleet 위험도" = 전체 엔진 중 곧 고장날 비율 |

---

## 3. 사용자 페르소나

이 시스템을 매일 쓰는 사람은 다음 세 명이다. 각자의 "월요일 오전 9시" 동작을 그려보면 요구사항이 또렷해진다.

### P1. 데이터 엔지니어 — Daniel
- **목표**: 어제 밤 streaming · 컴팩션이 무사히 돌았는지 확인하고, 작은 파일 비율·snapshot 증가율이 임계 안인지 본다.
- **사용 화면**: Superset "운영 탭", `code/health-queries/` 8개 쿼리.
- **고통점**: 새벽 OOM, 컴팩션-MERGE 충돌, 백필 시 expire snapshot 가 학습 윈도우를 잘라버리는 사고.

### P2. 데이터 사이언티스트 / PHM 연구원 — Suji
- **목표**: 모델 v3 를 v4 로 올리고 싶다. v3 가 학습할 때 본 Silver 데이터를 그대로 가져와 비교 실험하고 싶다.
- **사용 화면**: Trino SQL, `experiments/lstm_baseline.py`, Gold `model_metrics` 테이블.
- **고통점**: "그때 그 데이터" 가 계속 바뀌면 fair comparison 이 불가능. → time-travel 필수.

### P3. 정비 운영자 / 항공사 PM — Jaehyun
- **목표**: 오늘 위험한 엔진 Top 10, 운영조건별 열화율, 정비 권고 큐를 본다.
- **사용 화면**: Superset "비즈니스 탭".
- **고통점**: KPI 가 어제 본 값과 다르면 의사결정 신뢰가 깨진다. → Gold 멱등성·시계열 일관성 필수.

---

## 4. 목표 / 비목표

### 4-1. 목표 (In Scope)

1. **End-to-end 동작**: `docker compose up` + `full_ingest.sh` 로 Kafka→Bronze→Silver→Gold 까지 단일 명령으로 재현.
2. **메달리온 3계층** Iceberg 테이블 (Bronze/Silver/Gold) 과 각 레이어의 멱등 MERGE.
3. **Iceberg 매니지먼트 자동화**: Compaction · Expire Snapshots · Orphan Cleanup 을 Airflow DAG 로 스케줄.
4. **운영 헬스 8개 쿼리** + Superset 운영 탭 (신선도, 행 수, 파일 수·평균 크기, snapshot 증가율).
5. **비즈니스 대시보드** (Superset 비즈니스 탭): fleet RUL 히스토그램, 위험 엔진 Top 10, 운영조건별 열화율, 정비 권고 큐.
6. **장애 시나리오 재현**: streaming OOM 복구, 3개월 백필, 컴팩션-MERGE 동시성 — 각각 재현 절차 문서화.
7. **재현 가능 학습 파이프라인**: Spark MLlib `GBTRegressor` baseline + 학술용 `experiments/lstm_baseline.py`.
8. **PHM Korea 발표 자료**: 본 PRD + README + paper outline.

### 4-2. 비목표 (Out of Scope)

- **클라우드 배포**: 로컬 Docker 만. AWS 매핑은 README §2 에 "설계만" 명시.
- **온라인/실시간 모델 서빙**: 배치 예측만. 온라인은 100x 시나리오에서 "이렇게 확장한다" 수준.
- **GPU 학습**: CPU 단일 컨테이너 재현성을 우선. LSTM 도 epoch 적게 돌려 시연 위주.
- **다중 노드 Spark 클러스터 튜닝**: single-container Spark 로 충분.
- **인증·권한**: MinIO/Trino 기본 자격. 보안은 본 프로젝트 평가 항목 아님.
- **새 데이터셋 수집**: NASA 공개 데이터만. 실제 항공사 운영 데이터 연결 X.

---

## 5. 핵심 사용자 스토리 (User Story)

> *형식: "역할로서 / 무엇을 / 왜"*

### US-1. 데일리 헬스체크 (P1, Daniel)
**As** 데이터 엔지니어, **I want** Superset 운영 탭과 8개 헬스 쿼리를 5분 안에 훑고 **so that** 어제 밤 파이프라인이 정상이었는지, 누적 작은 파일/snapshot 가 임계 안인지 즉시 판단한다.

수용 기준:
- 운영 탭에 4개 차트 (신선도 / 행 수 / 작은 파일 비율 / snapshot 증가) 가 1초 이내 로드된다.
- 헬스 쿼리 8개가 Trino 에서 각각 5초 이내 응답한다.

### US-2. 모델 학습 데이터 재현 (P2, Suji)
**As** 연구원, **I want** "모델 v3 학습 시점의 Silver 테이블" 을 `AS OF` 한 줄로 복원하고 **so that** v4 와 동일 데이터로 fair benchmark 한다.

수용 기준:
- Gold `model_metrics` 에 `model_version`, `silver_snapshot_id`, `eval_window_end` 가 기록된다.
- `SELECT ... FROM phm.silver.engine_health FOR VERSION AS OF <snapshot_id>` 가 Trino 에서 동작한다.

### US-3. 3개월 백필 (P1, Daniel)
**As** 데이터 엔지니어, HI 산출 로직 버그 픽스 후 **I want** 3개월 치를 안전하게 다시 돌리고 **so that** 새 결과로 Silver/Gold 가 일관되게 갱신되되, 백필 도중 expire 가 학습 윈도우를 잘라먹지 않는다.

수용 기준:
- producer 의 `--base-date` 를 90일 전으로 돌려 재발행 → Silver MERGE → Gold 재집계 절차가 README §8 에 있다.
- expire 정책 = **100일** (학습 윈도우 90d + 안전 마진 10d). DDL 과 `iceberg_expire_dag.OLDER_THAN_DAYS` 두 곳이 동기화됨을 `tests/dags/test_backfill_safety.py` 가 강제한다.
- 백필 전 `CREATE TAG snap_before_backfill` 으로 롤백 포인트 확보.

### US-4. 위험 엔진 식별 (P3, Jaehyun)
**As** 정비 운영자, **I want** 오늘 RUL ≤ 30 cycle 인 엔진 목록과 운영조건별 열화율을 보고 **so that** 다음 주 정비 슬롯에 어떤 엔진을 넣을지 결정한다.

수용 기준:
- Superset 비즈니스 탭에 "위험 엔진 Top 10" 표 + "운영조건별 평균 RUL" 막대 차트.
- Gold `rul_prediction` 의 `risk_tier` 컬럼이 `(safe / watch / critical)` 셋 중 하나.

### US-5. Streaming OOM 복구 (P1, Daniel)
**As** 데이터 엔지니어, 새벽에 Spark Streaming 이 OOM 으로 죽으면 **I want** `full_ingest.sh` 재실행 한 번으로 살아나고 **so that** 중복·누락 없이 Bronze 가 이어 적재된다.

수용 기준:
- `full_ingest.sh` step 3.5 가 Bronze checkpoint 를 자동 정리.
- Bronze 멱등키 `(source_file, line_no)` 로 재발행해도 같은 행이 두 번 들어가지 않는다.

---

## 6. 기능 요구사항 (Functional Requirements)

> 우선순위: **M** = Must-have(MVP), **S** = Should-have, **C** = Could-have(시간 되면).

### 6-1. 데이터 수집 (Ingestion)
| ID | 요구 | 우선 |
|---|---|---|
| FR-I-1 | C-MAPSS `train_FD00x.txt` 4개 dataset 을 Kafka 토픽 `engine.sensor.raw` 로 발행 | M |
| FR-I-2 | `event_ts` 합성: `base_date + unit_jitter × unit_id + interval × (cycle−1)` | M |
| FR-I-3 | `--dry-run` (Kafka 미발행, 파싱·event_ts 검증) | M |
| FR-I-4 | `--max-units` (스모크 테스트용 unit 제한) | S |
| FR-I-5 | `--speedup` 페이싱 (1초에 N cycle 발행) | S |

### 6-2. Bronze (raw 보존)
| ID | 요구 | 우선 |
|---|---|---|
| FR-B-1 | `phm.bronze.engine_sensor_raw` 테이블, 26 센서 컬럼 + `event_ts, ingest_ts, source_file, line_no` | M |
| FR-B-2 | 파티션 = `dataset_id, days(ingest_ts)` (적재 시각 기준 — 감사·장애 추적) | M |
| FR-B-3 | 멱등키 `(source_file, line_no)` 로 INSERT-only MERGE | M |
| FR-B-4 | `full_ingest.sh` 가 checkpoint 정리 자동화 | M |

### 6-3. Silver (정제·피처)
| ID | 요구 | 우선 |
|---|---|---|
| FR-S-1 | 무정보 센서(1,5,6,10,16,18,19) 제외, 14개 센서 cluster 별 z-score 정규화 | M |
| FR-S-2 | `op_setting_1~3` KMeans(k=6, seed=42) → `op_condition_cluster`, 센터 오름차순 재정렬로 cluster id 안정화 | M |
| FR-S-3 | 5-cycle rolling: `s_avg_w5, s_std_w5, s_trend_w5` | M |
| FR-S-4 | `health_index = 1 − min(|s_avg_w5|, 3) / 3` | M |
| FR-S-5 | `rul_label = max(cycle) − cycle` | M |
| FR-S-6 | MERGE INTO 키 `(dataset_id, unit_id, cycle)` | M |
| FR-S-7 | 파티션 = `dataset_id, days(event_ts)` (시계열 쿼리 가속) | M |

### 6-4. Gold (예측·KPI)
| ID | 테이블 | 요구 | 우선 |
|---|---|---|---|
| FR-G-1 | `phm.gold.rul_prediction` | unit·cycle 별 RUL 예측 + 신뢰구간 + `risk_tier` + `silver_snapshot_id` | M |
| FR-G-2 | `phm.gold.fleet_kpi_daily` | 일배치 fleet 위험도, 운영조건별 열화율 | M |
| FR-G-3 | `phm.gold.model_metrics` | `model_version` 별 MAE / RMSE / PHM08 Score | M |
| FR-G-4 | 세 테이블 모두 MERGE INTO 멱등 | M |
| FR-G-5 | 운영 모델 = Spark MLlib `GBTRegressor` (단일 컨테이너 재현 가능) | M |
| FR-G-6 | 학술용 LSTM/CNN baseline = `experiments/lstm_baseline.py` (별도 실행) | S |

### 6-5. Iceberg 매니지먼트 자동화
| ID | DAG | 요구 | 우선 |
|---|---|---|---|
| FR-M-1 | `iceberg_compaction_dag` | `rewrite_data_files` 일배치 (저부하 시간대 03~05시) | M |
| FR-M-2 | `iceberg_expire_dag` | snapshot expire, **OLDER_THAN_DAYS = 100** (학습 90d + 마진 10d) | M |
| FR-M-3 | `iceberg_orphan_cleanup_dag` | `remove_orphan_files` 주배치 | S |
| FR-M-4 | `health_check_dag` | 8개 헬스 쿼리 결과를 로그/슬랙 (구현은 로그까지) | S |

### 6-6. 운영 가시성
| ID | 요구 | 우선 |
|---|---|---|
| FR-O-1 | 헬스 쿼리 8개 (`code/health-queries/`) — 신선도, 일자별 행 수, 작은 파일 비율, snapshot 증가, MAE drift, MERGE commit 패턴, op_condition drift, model 버전 일관성 | M |
| FR-O-2 | Superset 운영 탭 (zip export 포함) | M |
| FR-O-3 | Superset 비즈니스 탭 (RUL 히스토그램·위험 Top 10·열화율·정비 큐) | M |

### 6-7. 테스트
| ID | 요구 | 우선 |
|---|---|---|
| FR-T-1 | `tests/dags/test_backfill_safety.py` — expire 정책과 학습 윈도우 일치 검증 | M |
| FR-T-2 | producer 단위 테스트 (event_ts 합성 공식, dry-run) | M |
| FR-T-3 | Silver 변환 단위 테스트 (z-score, KMeans 안정화) | S |

---

## 7. 비기능 요구사항 (Non-Functional Requirements)

| 영역 | 기준 |
|---|---|
| **재현성** | 동일 인자 (`--base-date`, `--interval`) → bit-identical Silver/Gold (KMeans `seed=42` 가정 하). |
| **재시작 안정성** | streaming 잡이 죽어도 `full_ingest.sh` 한 번으로 무손실/무중복 복구. |
| **멱등성** | 같은 키 N회 실행 → 같은 결과. 모든 레이어가 MERGE INTO. |
| **시연 시간** | `docker compose up` 부터 Gold KPI 까지 노트북에서 기본 인자 (speedup=600) 로 **30분 내** 완료. |
| **헬스 응답** | 8개 헬스 쿼리 각 5초 이내 (Trino, single-node). |
| **이식성** | macOS (Apple Silicon arm64) + Linux/x86 모두 동작. arm64 네이티브 이미지 우선. |
| **백필 안전 마진** | expire 임계 ≥ 학습 윈도우 + 7일 (테스트로 강제). |
| **저장 비용** | 로컬 시연 기준 디스크 점유 < 5GB (default 시뮬 인자). |

---

## 8. 성공 지표 (Success Metrics)

세 그룹으로 나눈다.

### 8-1. 학습 성공 (모델 품질)
- **RUL MAE** ≤ 25 cycle (FD001 기준 baseline; 학술 비교 기준선)
- **PHM08 Score** 가 model_metrics 테이블에 기록되고, 두 모델 버전 간 비교 쿼리 1줄로 가능
- 동일 코드/인자로 학습 → 동일 metric 재현 (재현성)

### 8-2. 운영 성공 (인프라 안정)
- **데일리 5분 헬스체크** 가 Superset + 8 쿼리로 가능 (US-1 수용 기준)
- **3개월 백필** 가 expire 와 충돌 없이 1회 명령으로 완료 (US-3)
- **streaming OOM 복구** = 1회 재실행으로 무손실/무중복 (US-5)

### 8-3. 협업 성공 (지속 가능성)
- 신규 팀원이 README + 본 PRD 만 보고 30분 안에 로컬 시연까지 도달
- DDL · 파이프라인 · DAG · 헬스쿼리가 디렉토리별로 분리되어 있고 각 README 가 입출력을 명시

---

## 9. 데이터 모델 한 장 요약

```
[NASA C-MAPSS train_FD00x.txt]
        │
        ▼  (cmaps_to_kafka.py — event_ts 합성)
   Kafka: engine.sensor.raw
        │
        ▼  (Spark Structured Streaming)
┌────────────────────────────────────────────────┐
│ Bronze: phm.bronze.engine_sensor_raw            │
│   PK: (source_file, line_no) / part: ingest_ts  │
└────────────────────────────────────────────────┘
        │ (silver_transform.py — KMeans, rolling, HI, RUL label)
        ▼
┌────────────────────────────────────────────────┐
│ Silver: phm.silver.engine_health                │
│   MERGE: (dataset_id, unit_id, cycle)           │
│   part: dataset_id, days(event_ts)              │
└────────────────────────────────────────────────┘
        │ (gold_rul_predict.py — GBTRegressor)
        │ (gold_kpi_aggregate.py — daily KPI)
        ▼
┌────────────────────────────────────────────────┐
│ Gold:                                            │
│  • rul_prediction  (model_version, ds, unit, cy)│
│  • fleet_kpi_daily (kpi_date, ds, op_cluster)   │
│  • model_metrics   (model_version, ds, eval_end)│
└────────────────────────────────────────────────┘
        │
        ▼  Trino → Superset (비즈니스 / 운영 탭)
```

---

## 10. 마일스톤

| # | 산출물 | 상태 |
|---|---|---|
| M1 | 인프라 부트스트랩 (docker-compose: Spark/MinIO/Iceberg REST/Trino/Kafka/Airflow/Superset) | ✅ 완료 |
| M2 | DDL + Bronze streaming + Silver MERGE | ✅ 완료 |
| M3 | Gold (GBTRegressor 예측 + KPI 집계 + model_metrics) | ✅ 완료 |
| M4 | Iceberg 매니지먼트 DAG 3종 | ✅ 완료 |
| M5 | 헬스 쿼리 8개 + Superset 대시보드 export | ✅ 완료 |
| M6 | 백필 안전 마진 테스트 (`tests/dags/test_backfill_safety.py`) | ✅ 완료 (`a730af3`, `0398729`) |
| M7 | LSTM baseline 학술 비교 | 🟡 진행 (`experiments/lstm_baseline.py`) |
| M8 | PHM Korea 논문 초안 | 🟡 진행 (`paper/outline.md`) |
| M9 | 발표 슬라이드 | ⬜ 미작성 |

---

## 11. 리스크 & 가정

| 카테고리 | 내용 | 완화 방법 |
|---|---|---|
| **데이터 가정** | C-MAPSS 는 시뮬레이션 데이터. 실제 운영 fleet 분포와 다를 수 있음. | 학회 발표에서 "레퍼런스 아키텍처" 로 한정. 실데이터는 future work. |
| **단일 노드 한계** | Spark · Kafka 모두 single broker/executor. 100x 트래픽엔 부족. | README §7 에 100x 스케일아웃 시나리오 별도 정리. |
| **재시뮬레이션 함정** | 다른 `--base-date` 로 재시뮬 → 자연키 동일 → MERGE 가 event_ts 만 덮어씀. 시계열 보존이 깨짐. | 시뮬 전 snapshot 태그 필수, 또는 `down -v` 로 전체 초기화. README §8-4. |
| **Iceberg expire vs 백필** | expire 가 학습 윈도우 안의 snapshot 을 지우면 time-travel 깨짐. | expire 임계 = 학습 윈도우 + 안전 마진 10d. 두 곳 동기화를 테스트가 강제. |
| **KMeans seed** | KMeans 가 비결정적이면 cluster id 가 매번 바뀌어 Silver 재현성 깨짐. | `seed=42` + 센터 오름차순 재정렬(`stable_cluster_ids`). |
| **arm64/x86 차이** | 일부 Spark 이미지가 arm64 미지원. | Iceberg REST Catalog · 커스텀 Spark 이미지로 arm64 네이티브 보장. |

---

## 12. 의존성

### 12-1. 외부
- NASA Prognostics CoE Data Repository — Turbofan Engine Degradation Simulation Data Set (`train_FD001.txt` ~ `RUL_FD004.txt`)
- Apache Iceberg 1.x · Spark 3.5 · Trino 4xx · Kafka · MinIO · Airflow 2.x · Superset

### 12-2. 내부 (선행 조건)
1. `data/raw/` 에 C-MAPSS 12개 파일 배치 (gitignore 대상)
2. `infra/.env` 작성 (`infra/.env.example` 복사)
3. 커스텀 이미지 빌드 (`docker compose build`)

---

## 13. 향후 작업 (Future Work)

본 프로젝트의 비목표였지만, 다음 단계로 자연스러운 확장:

1. **AWS 이식**: MinIO→S3, REST Catalog→Glue/Tabular, Trino→Athena, Spark→EMR/EKS, Kafka→MSK. Terraform IaC.
2. **온라인 모델 서빙**: 배치 예측 → SageMaker/Triton 으로 분단위 RUL 갱신.
3. **실데이터 PoC**: 항공사 협업으로 실제 ACMS 센서 → 본 파이프라인 어댑터.
4. **다중 모델 ensemble**: GBT + LSTM + CNN 가중 평균. Gold 에 `ensemble_id` 추가.
5. **데이터 품질 게이트**: Great Expectations 로 Silver 진입 전 contract test.
6. **실시간 알림**: `risk_tier=critical` 진입 시 Slack/PagerDuty 통지 DAG.

---

## 부록 A. 참고 문서

- 실행/구조: [README.md](README.md)
- 인프라 결정: [infra/README.md](infra/README.md)
- 파이프라인 단계별 설명: [code/pipelines/README.md](code/pipelines/README.md)
- DDL: [code/ddl/](code/ddl/)
- 헬스 쿼리: [code/health-queries/](code/health-queries/)
- 매니지먼트 SQL: [code/maintenance/](code/maintenance/)
- 오케스트레이션: [orchestration/dags/](orchestration/dags/)
- 학술 baseline: [experiments/lstm_baseline.py](experiments/lstm_baseline.py)
- 논문 초안: [paper/outline.md](paper/outline.md)
- 원본 과제 가이드: [project_guide.md](project_guide.md)
