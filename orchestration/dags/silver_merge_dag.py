"""Bronze → Silver 증분 MERGE — 매시 정각.

silver_transform.py --mode incremental 호출.
phm.silver.pipeline_state.last_snapshot_id 이후 bronze 변경분만 처리하고,
영향받은 (dataset, unit) 의 모든 cycle 을 다시 변환해 rolling/rul_label 정확성 유지.
KMeans 통계는 silver_fit_stats_dag 가 주 1회 갱신.
멱등키: (dataset_id, unit_id, cycle).
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, spark_submit

with DAG(
    dag_id="silver_merge_dag",
    description="Bronze→Silver 증분 MERGE (watermark 이후 변경분만)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["silver"],
) as dag:
    BashOperator(
        task_id="silver_incremental",
        bash_command=spark_submit(
            "/workspace/code/pipelines/silver_transform.py",
            extra_args="--mode incremental",
        ),
    )
