"""Gold fleet KPI 일배치 — 매일 00:30 (UTC).

전일 Silver + rul_prediction 을 join 해 (kpi_date, dataset, cluster) 로 집계.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, spark_submit

with DAG(
    dag_id="gold_kpi_dag",
    description="Gold fleet_kpi_daily 집계 (HI 분포·degradation rate·RUL 백분위)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="30 0 * * *",
    catchup=False,
    tags=["gold", "kpi"],
) as dag:
    BashOperator(
        task_id="gold_kpi_aggregate",
        bash_command=spark_submit("/workspace/code/pipelines/gold_kpi_aggregate.py"),
    )
