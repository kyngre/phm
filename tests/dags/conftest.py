"""DAG 테스트 공통 픽스처.

Airflow DagBag 으로 dags 폴더를 한 번 파싱해서 fixture 로 공유.
phm-airflow 컨테이너 안에서 실행되는 게 기본 (`/opt/airflow/dags`).
호스트 실행 시에는 환경변수 DAGS_FOLDER 로 override 가능.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

DAGS_FOLDER = os.environ.get(
    "DAGS_FOLDER",
    "/opt/airflow/dags",  # 컨테이너 기본
)

# _common.py 가 dags 폴더 안에 있어 sys.path 추가 (DagBag 도 자동으로 추가하지만 안전망).
# 또한 expire/orphan DAG 모듈을 import 하는 backfill 테스트에서도 사용.
sys.path.insert(0, DAGS_FOLDER)


# README §2 / 각 DAG docstring 에 박힌 운영 약속.
# 변경 시 README 와 동기화 필요 — 테스트가 그 동기화를 강제한다.
EXPECTED_DAGS = {
    "ingest_streaming_dag":      {"schedule": "*/5 * * * *",  "tags": ["bronze", "streaming"]},
    "silver_merge_dag":           {"schedule": "0 * * * *",    "tags": ["silver"]},
    "silver_fit_stats_dag":       {"schedule": "0 6 * * 1",    "tags": ["silver", "weekly"]},
    "gold_rul_predict_dag":      {"schedule": "15 * * * *",   "tags": ["gold", "ml"]},
    "gold_train_dag":             {"schedule": "0 7 * * 1",    "tags": ["gold", "ml", "weekly"]},
    "gold_kpi_dag":              {"schedule": "30 0 * * *",   "tags": ["gold", "kpi"]},
    "iceberg_compaction_dag":    {"schedule": "0 3 * * *",    "tags": ["maintenance", "iceberg"]},
    "iceberg_expire_dag":        {"schedule": "0 4 * * *",    "tags": ["maintenance", "iceberg"]},
    "iceberg_orphan_cleanup_dag":{"schedule": "0 5 * * 0",    "tags": ["maintenance", "iceberg"]},
    "health_check_dag":          {"schedule": "30 * * * *",   "tags": ["health"]},
    "dq_check_dag":               {"schedule": "0 1 * * *",    "tags": ["health", "dq"]},
}


@pytest.fixture(scope="session")
def expected_dags():
    return EXPECTED_DAGS


@pytest.fixture(scope="session")
def dag_bag():
    pytest.importorskip("airflow", reason="DAG tests require apache-airflow")
    from airflow.models import DagBag

    folder = Path(DAGS_FOLDER)
    if not folder.is_dir():
        pytest.skip(f"DAGS_FOLDER not found: {folder}")
    bag = DagBag(dag_folder=str(folder), include_examples=False)
    return bag
