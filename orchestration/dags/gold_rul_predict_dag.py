"""Silver → Gold RUL 추론 — 매시 :15.

silver_merge_dag(:00) 후 15분 여유. `--mode predict` — gold_train_dag 가 학습한
PipelineModel 을 phm.gold.pipeline_state.active_model_path 에서 로드해 추론만.
silver 변경분 (last_snapshot_id 이후) + 영향 unit 의 모든 cycle MERGE.
멱등키: (model_version, dataset_id, unit_id, cycle).
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, spark_submit

with DAG(
    dag_id="gold_rul_predict_dag",
    description="Silver→Gold RUL 추론 (저장 모델 로드, 변경분만)",
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
            extra_args="--mode predict",
        ),
    )
