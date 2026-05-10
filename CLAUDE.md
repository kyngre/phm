# CLAUDE.md

> Claude Code 세션이 시작될 때 자동 로드. 프로젝트 빠른 오리엔테이션 + 반복되는 함정 + 컨벤션 정리.
> 상세 도메인/아키텍처는 [README.md](README.md), 단계별 실행은 [code/pipelines/README.md](code/pipelines/README.md).

## 프로젝트 한 줄

NASA C-MAPSS 터보팬 데이터를 Kafka 스트림으로 변환해 Iceberg 메달리온 lakehouse 에 적재 → RUL 예측 + 대시보드 + Airflow 오케스트레이션. PHM Korea 학회 + 데이터 엔지니어링 과제 동시 목표.

## 주요 디렉토리

```
infra/                  docker-compose, Spark 커스텀 이미지, Trino/Spark conf
code/
  ddl/                  Iceberg 테이블 DDL (apply_all.sh 로 일괄)
  pipelines/            cmaps_to_kafka, bronze_ingest, silver_transform, gold_*, full_ingest.sh
  health-queries/       Trino 운영 헬스 쿼리 8종
  maintenance/          Iceberg rewrite/expire/orphan SQL
orchestration/dags/     Airflow DAGs 8개 (BashOperator + docker exec 패턴)
dashboard/              Superset 차트 SQL + setup_superset.sh
experiments/            lstm_baseline.py (모델 비교용)
data/raw/               NASA C-MAPSS txt 12개 (gitignore)
data/_checkpoints/      Spark streaming checkpoint (호스트 bind mount, down -v 로 안 지워짐)
```

## 자주 쓰는 명령

| 의도 | 명령 |
|---|---|
| 클린 + 전체 적재 | `cd infra && docker compose down -v && docker compose up -d && cd .. && ./code/pipelines/full_ingest.sh` |
| Spark 재빌드 (Dockerfile/conf 변경 시) | `cd infra && docker compose build spark && docker compose up -d spark` |
| DAG 단일 테스트 | `docker exec phm-airflow airflow dags test <dag_id>` |
| Spark SQL 즉석 | `docker exec -i phm-spark /opt/spark/bin/spark-sql --conf spark.sql.defaultCatalog=phm -e "..."` |
| Trino SQL | `docker exec -i phm-trino trino --catalog iceberg < query.sql` |
| Superset Trino DB 등록 | `./dashboard/setup_superset.sh` |
| Bronze streaming 재기동 | `docker exec phm-spark pkill -f bronze_ingest.py; full_ingest.sh 재실행` |

## 반복 함정 (실수 방지)

1. **Bronze checkpoint 가 down -v 로 안 지워짐** — `data/_checkpoints/` 는 host bind mount. 새 시뮬 시작 시 이전 checkpoint 와 새 Kafka offset 이 align 되면 streaming 이 "이미 처리됨" 으로 판단해 메시지 skip → Bronze 누락. `full_ingest.sh` 가 자동으로 wipe 하지만 수동 시뮬 시 주의.
2. **`pgrep -f bronze_ingest.py` 자기 매칭** — bash 스크립트의 cmdline 에 패턴 문자열이 들어 있으면 pgrep 이 부모 bash 를 매칭. 회피책: pgrep 을 컨테이너 내부가 아니라 **호스트에서 `docker exec phm-spark pgrep ...`** 으로 호출.
3. **AWS SDK v1+v2 공존** — Iceberg S3FileIO 는 v2, hadoop-aws 3.3.4 의 S3AFileSystem 은 v1. 둘 다 jar 에 있음 (`aws-java-sdk-bundle-1.12.262.jar` + `aws-sdk-bundle-2.24.6.jar`). 패키지 namespace 가 달라 충돌 없음. `remove_orphan_files` 절차가 v1 의 S3AFileSystem 을 요구 — 빠지면 `No FileSystem for scheme "s3"` 에러.
4. **Airflow SequentialExecutor + SQLite** — 동시성 1. Long-running spark-submit DAG 가 다른 DAG 를 큐잉. `LocalExecutor` 로 바꾸면 SQLite 와 충돌 (Postgres 필수).
5. **Iceberg REST 500 (대량 commit)** — 카탈로그 부하. 완화: producer 분할 발행, `bronze_ingest` trigger 간격 늘리기, 또는 `phm-iceberg-rest` 재기동. Bronze append-only + Silver dedup 정책 덕에 재시도가 dup 을 만들어도 silver 단에서 흡수 → 재시도 안전.
6. **`docker compose down -v` 가 Superset/Airflow metadata DB wipe** — 차트·DAG state 사라짐. DAG 정의는 파일이라 OK, Superset 차트는 export zip 으로 보관 권장.
7. **Spark 컨테이너 recreate 시 pip 사라짐** — numpy/kafka-python 은 Dockerfile 에 사전 설치됨. torch (LSTM 학습용) 는 무거워 Dockerfile 에 없고 첫 실행 시 `pip install` 필요.
8. **`spark compose up -d <subset>` 후 minio-init 누락** — minio volume 이 wipe 됐을 때 `minio-init` 도 같이 띄워야 warehouse 버킷 생성.

## 컨벤션

- **답변 언어**: 한국어. 코드 주석도 한국어 위주.
- **응답 톤**: 짧고 구체. 결과 표 우선. 필요할 때만 설명.
- **CLI 우선**: GUI 작업은 사용자 몫 (Superset 차트 빌드, dashboard export 등).
- **멱등성 원칙**: Bronze 는 append-only (write 비용 일정), Silver 진입에서 `(source_file, line_no)` 단위로 dedup. Silver/Gold 는 MERGE INTO 키로 멱등. 재실행 자유.
- **시간 컬럼 분리**: `event_ts` (시뮬 도메인 시각) ↔ `ingest_ts` (실시간 적재) ↔ `predict_ts` (예측 실행 시각). 절대 혼용 금지.
- **시뮬 default 인자**: `--base-date 2025-08-01 --interval 1h --unit-jitter 1h` (root README §0-2 참조).

## 모델 / 평가 현황

- `gold_rul_predict.py` — Spark MLlib `GBTRegressor` (`gbt-v0`). train 으로 학습+추론 (누수). `phm.gold.rul_prediction` + `phm.gold.model_metrics`.
- `experiments/lstm_baseline.py` — PyTorch LSTM (`lstm-v1`). unit-level 80/20 split.
- **NASA test set + RUL_FD00x.txt 평가는 미적용** (학회용 표준 평가). 추가하려면 producer 의 `--include-test` + Silver 의 `is_test` 플래그 + RUL_FD00x.txt join 필요.
- 결과 표 (전체 silver 기준):
  | dataset | GBT v0 MAE | LSTM v1 MAE |
  |---|---|---|
  | FD001 | 31.07 | 27.72 |
  | FD002 | 31.95 | 29.01 |
  | FD003 | 52.79 | 49.30 |
  | FD004 | 50.05 | 46.05 |

## 주요 의사결정 기록

- **REST Catalog vs HMS**: arm64 / 단일 컨테이너 / Spark·Trino 동일 인터페이스 → REST.
- **Bronze partition `days(ingest_ts)`** vs Silver `days(event_ts)`: Bronze 는 운영 감사 추적 (실시간 적재 흐름) 이 목적, Silver 는 시계열 쿼리 가속이 목적 → 다른 시간 축으로 파티션.
- **Time-Travel Simulation**: producer 가 event_ts 를 합성. `event_ts ≠ ingest_ts` 분리로 Iceberg time-travel 의 두 축(`AS OF snapshot` vs `WHERE event_ts BETWEEN`) 모두 시연.
- **Airflow 패턴**: BashOperator + `docker exec phm-spark|phm-trino` (SparkSubmitOperator/connection 불필요). docker.sock 마운트 + Airflow 컨테이너 내 docker CLI.
- **모델 멱등키 정책**: `(model_version, dataset_id, unit_id, cycle)` — 같은 모델 재실행 시 UPDATE, 새 모델은 새 행. version 으로 모델 비교 자동화.
- **Bronze append-only + Silver dedup**: Bronze 는 `MERGE INTO` 미사용 — `writeTo(...).append()` 로 batch 마다 read 비용 0. Spark Streaming at-least-once 의 dup 은 Silver 진입의 `dedup_bronze()` (`row_number()=1` over `(source_file, line_no)`, first-write-wins) 가 흡수. 운영 시스템에서 트래픽 증가 시 Bronze write 비용이 데이터 누적과 무관해지는 패턴 시연.
- **Silver 증분 처리**: 매시간 `silver_merge_dag` 가 `--mode incremental` 호출. `phm.silver.pipeline_state.last_snapshot_id` 이후 bronze 변경분만 Iceberg `start-snapshot-id` 옵션으로 스캔, 영향 `(dataset, unit)` 의 모든 cycle 을 재변환 (rolling window + rul_label 정확성). KMeans/cluster 통계는 주 1회 `silver_fit_stats_dag` 가 `phm.silver.feat_stats` 에 갱신.

## 작업 시 우선 확인할 것

새 작업 들어왔을 때 점검 순서:
1. `docker ps` 로 7개 컨테이너 (minio, iceberg-rest, spark, trino, kafka, airflow, superset) 상태.
2. Bronze streaming 살아있는지: `docker exec phm-spark pgrep -f bronze_ingest.py`.
3. Iceberg 테이블 카운트: 위 "자주 쓰는 명령" 의 Spark SQL.
4. checkpoint 상태가 의심되면 `data/_checkpoints/bronze_engine_sensor_raw/offsets/` 의 latest 파일 vs Kafka 의 `kafka-get-offsets.sh` 비교.
