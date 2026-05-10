# Orchestration — Airflow DAGs

Airflow standalone (SQLite + SequentialExecutor) on `phm-airflow`. UI: http://localhost:8085 (admin/admin — `airflow standalone` 첫 기동 시 콘솔에서 발급되는 password 우선).

| DAG | 스케줄 (UTC) | 설명 |
|---|---|---|
| `ingest_streaming_dag` | `*/5 * * * *` | Bronze streaming process 헬스체크·재시작 |
| `silver_merge_dag` | `0 * * * *` | Bronze → Silver 증분 MERGE (watermark 이후 변경분만) |
| `silver_fit_stats_dag` | `0 6 * * 1` | KMeans + cluster별 sensor mean/std 재학습 (주 1회) |
| `gold_rul_predict_dag` | `15 * * * *` | Silver → RUL 예측 (gbt-v0) |
| `gold_kpi_dag` | `30 0 * * *` | fleet KPI 일배치 |
| `iceberg_compaction_dag` | `0 3 * * *` | rewrite_data_files + rewrite_manifests |
| `iceberg_expire_dag` | `0 4 * * *` | expire_snapshots (older_than = NOW − 90d) |
| `iceberg_orphan_cleanup_dag` | `0 5 * * 0` | remove_orphan_files (주배치) |
| `health_check_dag` | `30 * * * *` | health-queries 8종 Trino 실행 (운영 메트릭) |
| `dq_check_dag` | `0 1 * * *` | 데이터 품질 검증 (NULL/finite/dup/cycle/rul/cluster/freshness/count/NaN) → `phm.gold.dq_results` |

## 실행 패턴

모든 DAG 는 **BashOperator + `docker exec`** 패턴. Airflow 컨테이너에 `docker.sock` 와 `../code` 를 마운트해 호스트 docker 데몬을 통해 `phm-spark`/`phm-trino` 에 명령을 보낸다. 별도 SparkSubmitOperator/connection 설정 불필요.

```
phm-airflow ── docker.sock ──▶ phm-spark / phm-trino
       └── /opt/airflow/code ─── (DAG 가 내부 SQL 경로 참조)
```

## 기동

```bash
docker compose -f infra/docker-compose.yml up -d airflow
docker logs -f phm-airflow                 # 초기 admin password 확인
open http://localhost:8085                 # UI
```

DAG 변경 시 컨테이너 재시작 불필요 (`/opt/airflow/dags` 는 read-only mount).

## 멱등·재시도 정책

- 모든 파이프라인은 멱등 키로 MERGE → 자유로운 재실행.
- `default_args.retries = 2`, `retry_delay = 5min`.
- Iceberg 유지보수 DAG 는 `partial-progress.enabled = true` 로 일부 파일 그룹 실패 허용.
- 백필: `airflow dags backfill <dag> -s <date> -e <date>`. snapshot 기반 time-travel 로 재처리 가능.

## 다음 확장

- `on_failure_callback` 으로 Slack/email 알림.
- KEDA + Celery Executor 로 streaming/batch 분리.
- `expire_snapshots` 의 SQL 파일 jinja-render (현재는 DAG 안에서 동적 timestamp 생성).
- AWS 매핑 시 `BashOperator + docker exec` → `SparkSubmitOperator` (EMR/EKS) 로 교체. `docker.sock` 마운트 surface 제거 + Airflow connection 으로 자격증명 일원화.
