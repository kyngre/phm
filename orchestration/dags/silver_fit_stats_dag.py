"""Silver feat_stats 재학습 — 매주 월요일 06:00.

silver_transform.py --mode fit-stats 호출.
KMeans(k=6) 를 다시 fit 하고 (dataset_id, cluster_id) 별 sensor mean/std 를
phm.silver.feat_stats 에 새 stats_version 으로 저장. silver 행은 변경 X.

silver_merge_dag (incremental) 가 다음 실행 시 새 stats_version 을 자동 선택
(load_feat_stats 가 fit_ts 기준 최신 사용).

운영 조건이 크게 변하면(07_op_condition_drift 임계 초과) 수동 트리거 권장.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, spark_submit

with DAG(
    dag_id="silver_fit_stats_dag",
    description="phm.silver.feat_stats 재학습 (KMeans + cluster별 sensor mean/std)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * 1",   # 매주 월 06:00 (compaction/expire 03~04 시 이후)
    catchup=False,
    tags=["silver", "weekly"],
) as dag:
    BashOperator(
        task_id="silver_fit_stats",
        bash_command=spark_submit(
            "/workspace/code/pipelines/silver_transform.py",
            extra_args="--mode fit-stats",
        ),
    )
