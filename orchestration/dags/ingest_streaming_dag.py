"""Bronze Streaming job 헬스체크 — 5분마다 컨테이너에 streaming process 가 살아있는지 확인.

Streaming job 자체는 phm-spark 안에서 백그라운드로 돌고, DAG 는 감시·재시작만 담당.

self-match 회피: pgrep 은 호스트(=Airflow 컨테이너) 에서 `docker exec phm-spark pgrep ...`
형태로 호출하고, 재기동 명령은 별도의 `docker exec -d` 로 분리. 두 명령의 cmdline 에 패턴이
같이 들어가지 않으므로 컨테이너 ps 안에서 자기 매칭이 발생하지 않는다.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import DEFAULT_ARGS

CHECK_CMD = r"""
set -e
if docker exec phm-spark pgrep -f bronze_ingest.py >/dev/null 2>&1; then
  echo 'bronze_ingest already running'; exit 0
fi
docker exec -d phm-spark bash -lc '
  mkdir -p /workspace/data/_logs;
  nohup setsid /opt/spark/bin/spark-submit --master "local[*]" \
    /workspace/code/pipelines/bronze_ingest.py \
    </dev/null >/workspace/data/_logs/bronze_ingest.log 2>&1 &
  disown || true
'
for i in $(seq 1 25); do
  sleep 2
  if docker exec phm-spark pgrep -f bronze_ingest.py >/dev/null 2>&1; then
    echo "bronze_ingest started"; exit 0
  fi
done
echo "bronze_ingest failed to start" >&2
docker exec phm-spark tail -40 /workspace/data/_logs/bronze_ingest.log >&2
exit 1
"""

with DAG(
    dag_id="ingest_streaming_dag",
    description="Kafka→Bronze streaming job 헬스체크·재시작",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["bronze", "streaming"],
) as dag:
    BashOperator(task_id="ensure_streaming_alive", bash_command=CHECK_CMD)
