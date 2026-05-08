"""Bronze → Silver MERGE — 매시 정각.

silver_transform.py 는 Bronze 전체를 읽어 KMeans/정규화/롤링/HI/RUL 라벨을 계산하고
(dataset_id, unit_id, cycle) 기준 MERGE INTO. 멱등 → 재실행 안전.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, spark_submit

with DAG(
    dag_id="silver_merge_dag",
    description="Bronze→Silver MERGE INTO (KMeans·정규화·롤링·HI·RUL 라벨)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    tags=["silver"],
) as dag:
    BashOperator(
        task_id="silver_transform",
        bash_command=spark_submit("/workspace/code/pipelines/silver_transform.py"),
    )
