"""Snapshot 만료 — 매일 04:00 (compaction 1시간 후).

03_expire_snapshots.sql 의 older_than 은 SQL 안에서 절대 시각으로 박혀있다.
운영에서는 SQL 을 jinja-render 하거나, 절차 호출을 BashOperator 안에서 동적으로 조합하는 게
정석이지만 — 여기서는 단순화 위해 SQL 파일을 그대로 호출하고, 주기적으로 SQL 의
older_than 을 갱신하는 방침으로 둔다(주석 형태로 명시).

OLDER_THAN_DAYS = 100 = 학습 윈도우(README §8-2: 90일 백필) + 안전 마진 10일.
DDL `history.expire.max-snapshot-age-ms` (= 8_640_000_000 ms = 100일) 와 동기화 필요 —
어긋나면 tests/dags/test_backfill_safety.py::test_table_ddl_max_snapshot_age_matches 가 fail.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, SPARK_CONTAINER

OLDER_THAN_DAYS = 100
RETAIN_LAST = 20

# 동적 expire — SQL 파일 대신 인라인으로 매 실행마다 절대 시각 계산.
EXPIRE_SQL = (
    "CALL phm.system.expire_snapshots("
    "table => '{tbl}', "
    "older_than => TIMESTAMP '{ts}', "
    "retain_last => {retain});"
)

TABLES = [
    ("bronze.engine_sensor_raw", 20),
    ("silver.engine_health", 20),
    ("gold.rul_prediction", 20),
    ("gold.fleet_kpi_daily", 10),
    ("gold.model_metrics", 10),
]


def build_cmd() -> str:
    cutoff = (datetime.utcnow() - timedelta(days=OLDER_THAN_DAYS)).strftime("%Y-%m-%d 00:00:00")
    stmts = " ".join(EXPIRE_SQL.format(tbl=t, ts=cutoff, retain=r) for t, r in TABLES)
    # 한 번의 spark-sql 호출로 5개 테이블 처리.
    return (
        f"docker exec -i {SPARK_CONTAINER} /opt/spark/bin/spark-sql "
        f"--conf spark.sql.defaultCatalog=phm -e \"{stmts}\""
    )


with DAG(
    dag_id="iceberg_expire_dag",
    description="expire_snapshots (older_than = NOW - 90d, retain_last 20/10)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * *",
    catchup=False,
    tags=["maintenance", "iceberg"],
) as dag:
    BashOperator(task_id="expire_snapshots", bash_command=build_cmd())
