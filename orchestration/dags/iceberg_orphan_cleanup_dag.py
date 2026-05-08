"""고아 파일 GC — 매주 일요일 05:00.

remove_orphan_files 는 list 비용이 커서 주배치. older_than = NOW - 7d 로 진행 중 write 보호.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, SPARK_CONTAINER

OLDER_THAN_DAYS = 7

ORPHAN_SQL = (
    "CALL phm.system.remove_orphan_files("
    "table => '{tbl}', "
    "older_than => TIMESTAMP '{ts}', "
    "dry_run => false);"
)

TABLES = [
    "bronze.engine_sensor_raw",
    "silver.engine_health",
    "gold.rul_prediction",
    "gold.fleet_kpi_daily",
    "gold.model_metrics",
]


def build_cmd() -> str:
    cutoff = (datetime.utcnow() - timedelta(days=OLDER_THAN_DAYS)).strftime("%Y-%m-%d 00:00:00")
    stmts = " ".join(ORPHAN_SQL.format(tbl=t, ts=cutoff) for t in TABLES)
    return (
        f"docker exec -i {SPARK_CONTAINER} /opt/spark/bin/spark-sql "
        f"--conf spark.sql.defaultCatalog=phm -e \"{stmts}\""
    )


with DAG(
    dag_id="iceberg_orphan_cleanup_dag",
    description="remove_orphan_files (older_than = NOW - 7d), 주배치",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 5 * * 0",
    catchup=False,
    tags=["maintenance", "iceberg"],
) as dag:
    BashOperator(task_id="remove_orphan_files", bash_command=build_cmd())
