"""데이터 품질 검증 — 매일 01:00.

dq_check.py 가 9개 rule (NULL/finite/dup/cycle/rul/cluster/freshness/count/NaN feat) 을
phm.gold.dq_results 에 매일 1회 평가·기록. health-queries 의 운영 메트릭과 분리된
*데이터 자체* 검증이라 분리 DAG.

스케줄: 01:00 — gold_kpi (00:30) 직후, compaction (03:00) 이전.
실패 정책: --fail-on-fail 비활성 (관측만; FAIL 발생 시 다음 단계 차단은 별도 sensor 도입).
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS, spark_submit

with DAG(
    dag_id="dq_check_dag",
    description="phm.gold.dq_results 갱신 (NULL/finite/dup/cycle/rul/cluster/freshness/count/NaN)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * *",   # 매일 01:00 (gold_kpi 30분 후)
    catchup=False,
    tags=["health", "dq"],
) as dag:
    BashOperator(
        task_id="dq_check",
        bash_command=spark_submit(
            "/workspace/code/pipelines/dq_check.py",
        ),
    )
