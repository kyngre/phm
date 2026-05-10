"""Gold RUL 모델 재학습 — 매주 월요일 07:00.

gold_rul_predict.py --mode train. unit-level 80/20 split → GBT fit → MinIO 저장
→ phm.gold.model_metrics 에 train/holdout MAE 분리 기록 → phm.gold.pipeline_state
의 active_model_path 갱신.

매시 :15 의 gold_rul_predict_dag 가 다음 실행부터 새 모델을 자동 사용.

스케줄: 월 07:00 — silver_fit_stats(06:00) 직후. holdout split 평가가 매주 자동
갱신되어 모델 drift 추적.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, spark_submit

with DAG(
    dag_id="gold_train_dag",
    description="GBT RUL 모델 재학습 (80/20 unit-level holdout)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 7 * * 1",   # 매주 월 07:00 (silver_fit_stats 06:00 직후)
    catchup=False,
    tags=["gold", "ml", "weekly"],
) as dag:
    BashOperator(
        task_id="gold_train",
        bash_command=spark_submit(
            "/workspace/code/pipelines/gold_rul_predict.py",
            extra_args="--mode train --model-version gbt-v0 --rul-cap 130",
        ),
    )
