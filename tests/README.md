# 테스트

README §9 의 멱등성·재현성 약속, §8-2 의 백필 안전성, §2/§5 의 스케줄 약속을 코드로 보증.

총 **121 케이스 / ~22초** (캐시 미스 1회).

## 레이어

| 폴더 | 의존성 | 무엇을 검증 | 케이스 |
|---|---|---|---|
| `tests/unit/` | PySpark (로컬) | `cmaps_to_kafka` 합성 공식, `silver_transform` 변환 함수 (KMeans seed 안정성, rolling, HI, RUL) | 38 |
| `tests/integration/` | Iceberg 활성 Spark (phm-spark) | Bronze/Silver MERGE 멱등성, 백필 시 UPDATE 동작 | 9 |
| `tests/dags/` | Airflow 2.10 (phm-airflow) | DAG 파싱·구조·스케줄 정합성, expire/orphan vs 학습 윈도우 | 74 |

## 실행

```bash
# 전체 121개 (phm-spark + phm-airflow 컨테이너 자동 감지)
./tests/run_tests.sh all

# 모드별
./tests/run_tests.sh unit         # 38개, ~10s, phm-spark
./tests/run_tests.sh integration  # 9개,  ~18s, phm-spark
./tests/run_tests.sh dags         # 74개, ~1s,  phm-airflow
```

`run_tests.sh` 가 자동으로 처리하는 것:

1. 필요한 컨테이너(`phm-spark` 또는 `phm-airflow`) 떠 있는지 감지
2. `tests/` 와 `pytest.ini` 를 컨테이너에 `docker cp`
   (docker-compose 는 `code/`, `data/`, `experiments/`, `dags/` 만 마운트 — `tests/` 는 마운트 대상 외)
3. 컨테이너에 `pytest` 미설치면 설치
4. PySpark 의 경우 `PYTHONPATH` 에 `$SPARK_HOME/python` + `py4j` zip 등록 후 실행

직접 호출:

```bash
# Iceberg 통합
docker exec -w /workspace phm-spark bash -lc '
  export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
  pytest tests/integration -v
'

# DAG 검증
docker exec --user airflow -w /tmp phm-airflow pytest tests-dags -v
```

호스트 직접 실행은 pyspark 3.5.3 (Python 3.7~3.11) + Java 17 + Airflow 2.10 필요.

## 매핑 — 어떤 README 약속을 어떤 테스트가 보증하나

| README 항목 | 테스트 |
|---|---|
| §0-2 `event_ts = base + jitter*unit + interval*(cycle-1)` | `test_cmaps_to_kafka.py::TestSynthEventTs` |
| §3-2 KMeans seed=42 + stable_cluster_ids → 재현 가능 | `test_silver_transform.py::TestClusterOpConditions::test_seed_fixed_run_twice_same_assignment` |
| §3-2 5-cycle rolling 피처 정의 | `test_silver_transform.py::TestAddRollingFeatures` |
| §3-2 HI = 1 − min(\|s\|, 3)/3 | `test_silver_transform.py::TestAddHealthIndex` |
| §3-2 rul_label = max(cycle) − cycle | `test_silver_transform.py::TestAddRulLabel` |
| §9 Bronze `(source_file, line_no)` INSERT-only 멱등 | `test_bronze_idempotency.py` |
| §9 Silver `(dataset_id, unit_id, cycle)` MERGE 멱등 | `test_silver_idempotency.py::test_same_input_twice_same_health_index` |
| §8-4 재시뮬 → event_ts 만 UPDATE (의도적 함정) | `test_silver_idempotency.py::test_backfill_updates_event_ts_in_place` |
| §2/§5 DAG cron — silver(:00)→gold_rul(:15), compaction(03:00)→expire(04:00) | `test_dag_schedules.py` |
| §8-2 백필 90일 vs expire 100일 — 안전 마진 ≥ 7일 강제 + DDL 동기화 | `test_backfill_safety.py::TestExpirePolicy` |
| `iceberg_orphan_cleanup_dag` 7일 마진, dry_run=false 운영 약속 | `test_backfill_safety.py::TestOrphanCleanupPolicy` |
| 모든 DAG: retries=2, depends_on_past=False, catchup=False, owner=phm | `test_dag_loading.py::TestDagStructure`/`TestDagOwnership` |

## 새 테스트 추가 가이드

- 변환 로직을 만지면 → `tests/unit/test_silver_transform.py` 에 손계산 케이스 추가
- 새 멱등키나 MERGE 추가하면 → `tests/integration/` 에 같은 입력 두 번 적용 + diff 0 케이스 추가
- 픽스처 추가는 `tests/conftest.py`. SparkSession 은 session-scope 라 빠름.
