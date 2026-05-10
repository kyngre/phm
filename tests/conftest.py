"""
공통 pytest 픽스처.

- `code/pipelines/` 를 sys.path 에 등록 → 테스트에서 모듈 직접 import 가능
- `spark` (function-scope): Iceberg 없이 빠르게 도는 로컬 SparkSession (unit/)
- `iceberg_spark` (session-scope): Hadoop catalog 기반 Iceberg SparkSession (integration/)
  · 컨테이너 안에서 실행 시 빌드된 jar 자동 인식, 호스트에서는 maven coords 로 다운로드
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PIPELINES_DIR = PROJECT_ROOT / "code" / "pipelines"
sys.path.insert(0, str(PIPELINES_DIR))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName("phm-unit-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def iceberg_warehouse(tmp_path_factory):
    path = tmp_path_factory.mktemp("iceberg_warehouse")
    yield str(path)
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def iceberg_spark(iceberg_warehouse):
    """Iceberg + Hadoop catalog 로 격리된 임시 warehouse 사용.

    컨테이너 내부 (`/opt/spark/jars` 에 iceberg jar 존재) 면 maven 다운로드 생략,
    호스트에서 실행 시 spark.jars.packages 로 자동 다운로드.
    """
    from pyspark.sql import SparkSession

    in_container = Path("/opt/spark/jars").exists() and any(
        p.name.startswith("iceberg-spark-runtime") for p in Path("/opt/spark/jars").iterdir()
    )

    builder = (
        SparkSession.builder
        .appName("phm-iceberg-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.test", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.test.type", "hadoop")
        .config("spark.sql.catalog.test.warehouse", iceberg_warehouse)
        .config("spark.sql.defaultCatalog", "test")
    )
    if not in_container:
        # 호스트 실행 시 fallback — Spark 3.5 / Iceberg 1.6.1 (infra 와 동일)
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
        )

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session.sql("CREATE NAMESPACE IF NOT EXISTS test.bronze")
    session.sql("CREATE NAMESPACE IF NOT EXISTS test.silver")
    yield session
    session.stop()
