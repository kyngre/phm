"""Health-queries 매시 실행 — 8개 SQL 을 Trino 로 돌려 결과를 로그에 남긴다.

알림 연동(Slack/email)은 추후 on_failure_callback 으로 확장. 여기서는 결과를 stdout 으로
흘리고 task 가 비-zero exit 시 Airflow 가 실패 처리하도록 한다.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, TRINO_CONTAINER

QUERIES = [
    "01_sensor_dropout.sql",
    "02_daily_volume.sql",
    "03_small_files_ratio.sql",
    "04_snapshot_growth.sql",
    "05_rul_mae_drift.sql",
    "06_silver_merge_conflicts.sql",
    "07_op_condition_drift.sql",
    "08_model_version_consistency.sql",
]


def build_cmd() -> str:
    parts = []
    for q in QUERIES:
        parts.append(
            f"echo '=== {q} ==='; "
            f"docker exec -i {TRINO_CONTAINER} trino --catalog iceberg "
            f"< /opt/airflow/code/health-queries/{q}"
        )
    return " && ".join(parts)


with DAG(
    dag_id="health_check_dag",
    description="health-queries 8종 매시 실행 (sensor dropout, drift, MAE, snapshot 등)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="30 * * * *",
    catchup=False,
    tags=["health"],
) as dag:
    BashOperator(task_id="run_health_queries", bash_command=build_cmd())
