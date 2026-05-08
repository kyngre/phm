"""DAG 공통 유틸 — `docker exec phm-spark ...` 호출 패턴을 한 곳에서 관리."""
from __future__ import annotations

from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "phm",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SPARK_CONTAINER = "phm-spark"
TRINO_CONTAINER = "phm-trino"

SPARK_DEFAULTS = (
    "--conf spark.sql.defaultCatalog=phm "
)


def spark_submit(script: str, extra_args: str = "") -> str:
    """phm-spark 컨테이너에서 spark-submit 호출. script 는 컨테이너 내 경로(/workspace/...)."""
    return (
        f"docker exec -i {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
        f"--master 'local[*]' {script} {extra_args}"
    ).strip()


def spark_sql_file(path_in_container: str) -> str:
    """phm-spark 컨테이너에서 spark-sql -f <file>."""
    return (
        f"docker exec -i {SPARK_CONTAINER} /opt/spark/bin/spark-sql "
        f"{SPARK_DEFAULTS} -f {path_in_container}"
    ).strip()


def trino_sql_file(host_path: str) -> str:
    """phm-trino 컨테이너에 SQL 파일을 stdin 으로 전달."""
    return f"docker exec -i {TRINO_CONTAINER} trino --catalog iceberg < {host_path}"
