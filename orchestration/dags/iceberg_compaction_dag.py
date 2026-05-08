"""Iceberg 컴팩션 — 매일 03:00.

01_rewrite_data_files.sql + 02_rewrite_manifests.sql 을 순차 실행.
small_files_ratio > 30% 면 즉시 효과, 아니면 idempotent.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, spark_sql_file

with DAG(
    dag_id="iceberg_compaction_dag",
    description="rewrite_data_files + rewrite_manifests (Bronze/Silver/Gold)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    tags=["maintenance", "iceberg"],
) as dag:
    rewrite_data = BashOperator(
        task_id="rewrite_data_files",
        bash_command=spark_sql_file("/workspace/code/maintenance/01_rewrite_data_files.sql"),
    )
    rewrite_manifests = BashOperator(
        task_id="rewrite_manifests",
        bash_command=spark_sql_file("/workspace/code/maintenance/02_rewrite_manifests.sql"),
    )
    rewrite_data >> rewrite_manifests
