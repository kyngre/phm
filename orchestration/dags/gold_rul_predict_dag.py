"""Silver → Gold RUL 예측 — 매시 :15.

silver_merge_dag(:00) 후 15분 여유. (model_version, dataset, unit, cycle) MERGE.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, spark_submit

with DAG(
    dag_id="gold_rul_predict_dag",
    description="Silver→Gold RUL 예측 (GBT v0)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="15 * * * *",
    catchup=False,
    tags=["gold", "ml"],
) as dag:
    BashOperator(
        task_id="gold_rul_predict",
        bash_command=spark_submit(
            "/workspace/code/pipelines/gold_rul_predict.py",
            extra_args="--model-version gbt-v0 --rul-cap 130",
        ),
    )
