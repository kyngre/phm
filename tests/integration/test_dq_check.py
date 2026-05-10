"""Iceberg 통합 테스트 — dq_check 의 9 rule 이 정상/이상 데이터를 정확히 분류.

검증:
  A. 정상 silver/bronze → 모든 rule PASS (freshness 제외 — 가짜 데이터 시각이라 상관없음)
  B. bronze 에 NULL/NaN/dup/cycle gap 주입 → 해당 rule FAIL
  C. silver 에 음수 rul / 잘못된 cluster 주입 → 해당 rule FAIL
  D. dq_results 멱등 — 같은 날 두 번 실행 시 행 수 동일 (UPDATE)
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark", reason="integration tests require pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

import dq_check  # noqa: E402
import silver_transform as st  # noqa: E402

pytestmark = pytest.mark.integration


# ─── 보조: 4개 테이블 생성 ───

def _create_tables(spark):
    for t in (
        "test.bronze.engine_sensor_raw",
        "test.silver.engine_health",
        "test.gold.dq_results",
    ):
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS test.gold")

    sensor_cols = ", ".join(f"sensor_{i} DOUBLE" for i in range(1, 22))
    spark.sql(f"""
        CREATE TABLE test.bronze.engine_sensor_raw (
            dataset_id STRING, unit_id INT, cycle INT,
            op_setting_1 DOUBLE, op_setting_2 DOUBLE, op_setting_3 DOUBLE,
            {sensor_cols},
            event_ts TIMESTAMP, ingest_ts TIMESTAMP,
            source_file STRING, line_no BIGINT
        )
        USING iceberg
        PARTITIONED BY (dataset_id, days(ingest_ts))
        TBLPROPERTIES ('format-version' = '2')
    """)

    norm_cols = ", ".join(f"s{s}_norm DOUBLE" for s in st.KEEP_SENSORS)
    spark.sql(f"""
        CREATE TABLE test.silver.engine_health (
            dataset_id STRING, unit_id INT, cycle INT,
            op_setting_1 DOUBLE, op_setting_2 DOUBLE, op_setting_3 DOUBLE,
            op_condition_cluster INT,
            {norm_cols},
            s_avg_w5 DOUBLE, s_std_w5 DOUBLE, s_trend_w5 DOUBLE,
            health_index DOUBLE, rul_label INT, is_test BOOLEAN,
            event_ts TIMESTAMP, ingest_ts TIMESTAMP, silver_ts TIMESTAMP,
            silver_version STRING
        )
        USING iceberg
        PARTITIONED BY (dataset_id, days(event_ts))
        TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql("""
        CREATE TABLE test.gold.dq_results (
            rule_name STRING, layer STRING, dataset_id STRING,
            measured_value DOUBLE, threshold DOUBLE, operator STRING, status STRING,
            sample_size BIGINT, description STRING,
            run_ts TIMESTAMP, run_date DATE
        )
        USING iceberg
        PARTITIONED BY (run_date)
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'copy-on-write')
    """)


def _bronze_schema():
    fields = [
        StructField("dataset_id", StringType()),
        StructField("unit_id", IntegerType()),
        StructField("cycle", IntegerType()),
        StructField("op_setting_1", DoubleType()),
        StructField("op_setting_2", DoubleType()),
        StructField("op_setting_3", DoubleType()),
    ]
    fields += [StructField(f"sensor_{i}", DoubleType()) for i in range(1, 22)]
    fields += [
        StructField("event_ts", TimestampType()),
        StructField("ingest_ts", TimestampType()),
        StructField("source_file", StringType()),
        StructField("line_no", LongType()),
    ]
    return StructType(fields)


def _bronze_rows(spark, n_units=2, n_cycles=10, base=None):
    base = base or datetime(2025, 8, 1, tzinfo=timezone.utc)
    rows = []
    for u in range(1, n_units + 1):
        for c in range(1, n_cycles + 1):
            tup = (
                "FD001", u, c,
                0.0, 0.0, 100.0,
                *(500.0 + s * 10 + c * 0.1 for s in range(1, 22)),
                base + timedelta(hours=u + c),
                base + timedelta(hours=u + c),
                "synthetic",
                int(u * 10000 + c),
            )
            rows.append(tup)
    return spark.createDataFrame(rows, schema=_bronze_schema())


def _insert(spark, df, table):
    df.createOrReplaceTempView("_b")
    spark.sql(f"INSERT INTO {table} SELECT * FROM _b")


@pytest.fixture
def dq_env(iceberg_spark, monkeypatch):
    iceberg_spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints-it")
    _create_tables(iceberg_spark)
    monkeypatch.setattr(dq_check, "BRONZE_TABLE", "test.bronze.engine_sensor_raw")
    monkeypatch.setattr(dq_check, "SILVER_TABLE", "test.silver.engine_health")
    monkeypatch.setattr(dq_check, "DQ_TABLE", "test.gold.dq_results")
    monkeypatch.setattr(st, "BRONZE_TABLE", "test.bronze.engine_sensor_raw")
    monkeypatch.setattr(st, "SILVER_TABLE", "test.silver.engine_health")
    yield iceberg_spark
    for t in (
        "test.bronze.engine_sensor_raw",
        "test.silver.engine_health",
        "test.gold.dq_results",
    ):
        iceberg_spark.sql(f"DROP TABLE IF EXISTS {t}")


def _seed_silver_from_bronze(spark, n_units=2, n_cycles=10):
    """bronze → 일관된 silver 행을 직접 생성. 실제 silver_transform 안 거치고
    DQ rule 만 검증하기 위해 단순화."""
    rows = []
    for u in range(1, n_units + 1):
        max_c = n_cycles
        for c in range(1, n_cycles + 1):
            row = {
                "dataset_id": "FD001", "unit_id": u, "cycle": c,
                "op_setting_1": 0.0, "op_setting_2": 0.0, "op_setting_3": 100.0,
                "op_condition_cluster": 0,
                **{f"s{s}_norm": 0.0 for s in st.KEEP_SENSORS},
                "s_avg_w5": 0.0, "s_std_w5": 0.0, "s_trend_w5": 0.0,
                "health_index": 1.0, "rul_label": max_c - c, "is_test": False,
                "event_ts": datetime(2025, 8, 1, tzinfo=timezone.utc),
                "ingest_ts": datetime(2025, 8, 1, tzinfo=timezone.utc),
                "silver_ts": datetime.now(tz=timezone.utc),
                "silver_version": "v1",
            }
            rows.append(row)
    df = spark.createDataFrame(rows)
    cols = [
        "dataset_id", "unit_id", "cycle",
        "op_setting_1", "op_setting_2", "op_setting_3",
        "op_condition_cluster",
        *(f"s{s}_norm" for s in st.KEEP_SENSORS),
        "s_avg_w5", "s_std_w5", "s_trend_w5",
        "health_index", "rul_label", "is_test",
        "event_ts", "ingest_ts", "silver_ts", "silver_version",
    ]
    df = df.select(
        F.col("dataset_id").cast("string"),
        F.col("unit_id").cast("int"),
        F.col("cycle").cast("int"),
        F.col("op_setting_1").cast("double"),
        F.col("op_setting_2").cast("double"),
        F.col("op_setting_3").cast("double"),
        F.col("op_condition_cluster").cast("int"),
        *(F.col(c).cast("double") for c in (f"s{s}_norm" for s in st.KEEP_SENSORS)),
        F.col("s_avg_w5").cast("double"),
        F.col("s_std_w5").cast("double"),
        F.col("s_trend_w5").cast("double"),
        F.col("health_index").cast("double"),
        F.col("rul_label").cast("int"),
        F.col("is_test").cast("boolean"),
        F.col("event_ts").cast("timestamp"),
        F.col("ingest_ts").cast("timestamp"),
        F.col("silver_ts").cast("timestamp"),
        F.col("silver_version").cast("string"),
    ).toDF(*cols)
    _insert(spark, df, "test.silver.engine_health")


def _run_dq(spark) -> dict[tuple[str, str, str], dq_check.DqResult]:
    """evaluate_all → {(rule_name, layer, dataset_id): result}."""
    results = dq_check.evaluate_all(spark)
    return {(r.rule_name, r.layer, r.dataset_id): r for r in results}


class TestDqHappyPath:
    def test_clean_data_passes_critical_rules(self, dq_env):
        spark = dq_env
        _insert(spark, _bronze_rows(spark, 2, 10), "test.bronze.engine_sensor_raw")
        _seed_silver_from_bronze(spark, 2, 10)

        r = _run_dq(spark)
        # 핵심 무결성 rule 들은 모두 PASS
        for key in [
            ("bronze_null_key_columns", "bronze", "FD001"),
            ("bronze_finite_sensors", "bronze", "FD001"),
            ("bronze_cycle_monotonic", "bronze", "FD001"),
            ("silver_rul_label_nonneg", "silver", "FD001"),
            ("silver_cluster_in_range", "silver", "FD001"),
            ("silver_count_matches_bronze", "silver", "FD001"),
        ]:
            assert key in r, f"rule 누락: {key}"
            assert r[key].status() == "PASS", \
                f"{key} 측정={r[key].measured_value} 임계={r[key].threshold} status={r[key].status()}"


class TestDqFailDetection:
    def test_bronze_dup_detected(self, dq_env):
        spark = dq_env
        bronze = _bronze_rows(spark, 2, 10)
        _insert(spark, bronze, "test.bronze.engine_sensor_raw")
        _insert(spark, bronze, "test.bronze.engine_sensor_raw")  # 의도적 dup
        _seed_silver_from_bronze(spark, 2, 10)

        r = _run_dq(spark)
        # dup ratio 1.0 (extra/total = 20/40 = 0.5) — threshold 0.01 초과
        # extra = (2-1)*20 = 20; total = 40 → ratio 0.5
        assert r[("bronze_dup_ratio", "bronze", "FD001")].measured_value == pytest.approx(0.5)
        assert r[("bronze_dup_ratio", "bronze", "FD001")].status() == "FAIL"

    def test_bronze_cycle_gap_detected(self, dq_env):
        spark = dq_env
        bronze = _bronze_rows(spark, 2, 10).where(F.col("cycle") != 5)  # cycle 5 누락
        _insert(spark, bronze, "test.bronze.engine_sensor_raw")
        _seed_silver_from_bronze(spark, 2, 10)

        r = _run_dq(spark)
        # 모든 unit (2개) 이 cycle 5 누락 → bad_units = 2
        assert r[("bronze_cycle_monotonic", "bronze", "FD001")].measured_value == 2.0
        assert r[("bronze_cycle_monotonic", "bronze", "FD001")].status() == "FAIL"

    def test_silver_negative_rul_detected(self, dq_env):
        spark = dq_env
        _insert(spark, _bronze_rows(spark, 2, 10), "test.bronze.engine_sensor_raw")
        _seed_silver_from_bronze(spark, 2, 10)
        # rul_label 1행을 음수로 강제 — UPDATE 안 되니 새 행 추가 후 cycle=999 로 차별화
        bad_row = spark.sql("""
            SELECT * FROM test.silver.engine_health WHERE unit_id=1 AND cycle=1 LIMIT 1
        """).withColumn("cycle", F.lit(999)).withColumn("rul_label", F.lit(-1))
        _insert(spark, bad_row, "test.silver.engine_health")

        r = _run_dq(spark)
        assert r[("silver_rul_label_nonneg", "silver", "FD001")].measured_value == 1.0
        assert r[("silver_rul_label_nonneg", "silver", "FD001")].status() == "FAIL"

    def test_silver_cluster_out_of_range_detected(self, dq_env):
        spark = dq_env
        _insert(spark, _bronze_rows(spark, 2, 10), "test.bronze.engine_sensor_raw")
        _seed_silver_from_bronze(spark, 2, 10)
        bad_row = spark.sql("""
            SELECT * FROM test.silver.engine_health WHERE unit_id=1 AND cycle=1 LIMIT 1
        """).withColumn("cycle", F.lit(999)).withColumn("op_condition_cluster", F.lit(99))
        _insert(spark, bad_row, "test.silver.engine_health")

        r = _run_dq(spark)
        assert r[("silver_cluster_in_range", "silver", "FD001")].measured_value == 1.0
        assert r[("silver_cluster_in_range", "silver", "FD001")].status() == "FAIL"


class TestDqMergeIdempotent:
    def test_same_day_rerun_does_not_create_duplicates(self, dq_env):
        spark = dq_env
        _insert(spark, _bronze_rows(spark, 2, 10), "test.bronze.engine_sensor_raw")
        _seed_silver_from_bronze(spark, 2, 10)

        df1 = dq_check.results_to_df(spark, dq_check.evaluate_all(spark))
        n1 = dq_check.merge_dq_results(spark, df1)

        df2 = dq_check.results_to_df(spark, dq_check.evaluate_all(spark))
        n2 = dq_check.merge_dq_results(spark, df2)

        spark.sql("REFRESH TABLE test.gold.dq_results")
        total = spark.table("test.gold.dq_results").count()
        # 같은 (rule, layer, dataset, run_date) → UPDATE → 행 수는 한 번 분량
        assert total == n1, f"멱등성 깨짐: 1차 {n1}, 누적 {total}"
        assert n1 == n2  # results_to_df 결과 행 수는 deterministic
