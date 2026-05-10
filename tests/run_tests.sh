#!/usr/bin/env bash
# 테스트 러너 — 컨테이너 안에서 실행하는 게 가장 안정적.
#
# 사용법:
#   ./tests/run_tests.sh unit          # 빠른 유닛 테스트만 (38개, ~10s)
#   ./tests/run_tests.sh integration   # Iceberg 통합 (9개, phm-spark 필요, ~18s)
#   ./tests/run_tests.sh dags          # Airflow DAG 검증 (74개, phm-airflow 필요, ~1s)
#   ./tests/run_tests.sh all           # 위 셋 다 (121개)
#
# 각 모드별 컨테이너:
#   unit/integration → phm-spark   (PySpark + Iceberg jar)
#   dags             → phm-airflow (Airflow 2.10.3)
set -euo pipefail

MODE="${1:-all}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"


run_in_spark() {
  local targets="$1"
  if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^phm-spark$'; then
    echo "[run_tests] phm-spark 미기동 — 호스트 pytest 로 실행 시도"
    cd "$ROOT"; python -m pytest $targets -v
    return
  fi
  echo "[run_tests:spark] $targets"
  docker exec -u root phm-spark mkdir -p /workspace/tests
  docker cp "$ROOT/tests/." phm-spark:/workspace/tests/
  docker cp "$ROOT/pytest.ini" phm-spark:/workspace/pytest.ini
  docker exec -u root phm-spark chown -R spark:spark /workspace/tests /workspace/pytest.ini
  docker exec -u root phm-spark bash -lc 'command -v pytest >/dev/null || pip install --quiet pytest'
  docker exec -w /workspace phm-spark bash -lc "
    export PYTHONPATH=\$SPARK_HOME/python:\$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
    pytest $targets -v -p no:cacheprovider
  "
}

run_in_airflow() {
  local targets="$1"
  if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^phm-airflow$'; then
    echo "[run_tests] phm-airflow 미기동 — DAG 테스트 스킵"
    return 1
  fi
  echo "[run_tests:airflow] $targets"
  # 기존 폴더가 있으면 docker cp 가 그 안에 중첩 — 매번 root 권한으로 삭제 후 복사
  docker exec --user root phm-airflow rm -rf /tmp/tests-dags /tmp/pytest.ini
  docker cp "$ROOT/tests/dags" phm-airflow:/tmp/tests-dags
  docker cp "$ROOT/pytest.ini" phm-airflow:/tmp/pytest.ini
  docker exec --user root phm-airflow chown -R airflow:0 /tmp/tests-dags /tmp/pytest.ini
  docker exec --user airflow phm-airflow bash -lc '
    command -v pytest >/dev/null || pip install --quiet pytest
  '
  # /tmp/tests-dags 를 /tmp 기준으로 실행 (DAGS_FOLDER 는 /opt/airflow/dags 기본값 사용)
  docker exec --user airflow -w /tmp phm-airflow bash -lc "
    pytest $targets --rootdir=/tmp -v -p no:cacheprovider
  "
}

case "$MODE" in
  unit)        run_in_spark "tests/unit -m 'not integration'" ;;
  integration) run_in_spark "tests/integration" ;;
  dags)        run_in_airflow "tests-dags" ;;
  all)
    run_in_spark "tests/unit -m 'not integration'"
    run_in_spark "tests/integration"
    run_in_airflow "tests-dags"
    ;;
  *) echo "usage: $0 {unit|integration|dags|all}"; exit 2 ;;
esac
