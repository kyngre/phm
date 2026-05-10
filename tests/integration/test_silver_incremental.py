"""Iceberg 통합 테스트 — Silver incremental 모드의 정확성.

검증:
  A. fit-stats → incremental 두 단계로 나눠 처리한 silver 가
     full 모드 결과와 동일한 행/값 (op_condition_cluster, s*_norm, health_index, rul_label).
  B. 두 번째 incremental run 이 새 bronze 행만 처리하면서도
     영향받은 unit 의 rul_label 을 올바르게 재계산.
  C. pipeline_state.last_snapshot_id 가 매 incremental run 마다 단조증가.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark", reason="integration tests require pyspark")

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

import silver_transform as st  # noqa: E402

pytestmark = pytest.mark.integration


# ─── 보조: bronze + silver + feat_stats + pipeline_state 테이블 생성 ───

NORM_COLS = ", ".join(f"s{s}_norm DOUBLE" for s in st.KEEP_SENSORS)

SENSOR_STATS_COLS = ", ".join(
    f"sensor_{s}_mean DOUBLE, sensor_{s}_std DOUBLE" for s in st.KEEP_SENSORS
)


def _create_tables(spark):
    spark.sql("DROP TABLE IF EXISTS test.bronze.engine_sensor_raw")
    spark.sql("DROP TABLE IF EXISTS test.silver.engine_health")
    spark.sql("DROP TABLE IF EXISTS test.silver.feat_stats")
    spark.sql("DROP TABLE IF EXISTS test.silver.pipeline_state")

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

    spark.sql(f"""
        CREATE TABLE test.silver.engine_health (
            dataset_id STRING, unit_id INT, cycle INT,
            op_setting_1 DOUBLE, op_setting_2 DOUBLE, op_setting_3 DOUBLE,
            op_condition_cluster INT,
            {NORM_COLS},
            s_avg_w5 DOUBLE, s_std_w5 DOUBLE, s_trend_w5 DOUBLE,
            health_index DOUBLE, rul_label INT, is_test BOOLEAN,
            event_ts TIMESTAMP, ingest_ts TIMESTAMP, silver_ts TIMESTAMP,
            silver_version STRING
        )
        USING iceberg
        PARTITIONED BY (dataset_id, days(event_ts))
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'copy-on-write')
    """)

    spark.sql(f"""
        CREATE TABLE test.silver.feat_stats (
            stats_version STRING, fit_snapshot_id BIGINT,
            cluster_id INT,
            center_op_setting_1 DOUBLE, center_op_setting_2 DOUBLE, center_op_setting_3 DOUBLE,
            dataset_id STRING, n_samples BIGINT,
            {SENSOR_STATS_COLS},
            fit_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (stats_version)
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'copy-on-write')
    """)

    spark.sql("""
        CREATE TABLE test.silver.pipeline_state (
            pipeline_name STRING,
            last_snapshot_id BIGINT,
            last_run_ts TIMESTAMP,
            rows_processed BIGINT,
            active_stats_version STRING
        )
        USING iceberg
        TBLPROPERTIES ('format-version' = '2', 'write.merge.mode' = 'copy-on-write')
    """)


def _bronze_schema():
    """bronze 테이블과 정확히 같은 컬럼 순서/타입 — INSERT 시 안전 캐스팅 보장."""
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


def _bronze_rows(spark, n_units=3, n_cycles=20, base=None):
    base = base or datetime(2025, 8, 1, tzinfo=timezone.utc)
    centers = [
        (0.0, 0.0, 100.0), (10.0, 0.25, 100.0), (20.0, 0.5, 100.0),
        (25.0, 0.62, 60.0), (35.0, 0.84, 60.0), (42.0, 0.84, 40.0),
    ]
    rows = []
    for u in range(1, n_units + 1):
        for c in range(1, n_cycles + 1):
            cluster = (u + c) % 6
            op1, op2, op3 = centers[cluster]
            tup = (
                "FD001", u, c,
                op1 + (c % 3 - 1) * 0.01,
                op2 + (c % 3 - 1) * 0.005,
                op3,
                *(500.0 + s * 10 + (c * 0.1) + (cluster * 5) for s in range(1, 22)),
                base + timedelta(hours=u + c),
                base + timedelta(hours=u + c),
                "synthetic",
                int(u * 10000 + c),
            )
            rows.append(tup)
    return spark.createDataFrame(rows, schema=_bronze_schema())


def _insert_bronze(spark, df):
    df.createOrReplaceTempView("_b")
    spark.sql("INSERT INTO test.bronze.engine_sensor_raw SELECT * FROM _b")


def _silver_rows_dict(spark, table="test.silver.engine_health"):
    """(dataset_id, unit_id, cycle) → 비교 가능한 핵심 컬럼 dict."""
    norm_cols = [f"s{s}_norm" for s in st.KEEP_SENSORS]
    out = {}
    for r in spark.table(table).collect():
        key = (r["dataset_id"], r["unit_id"], r["cycle"])
        out[key] = {
            "op_condition_cluster": r["op_condition_cluster"],
            "rul_label": r["rul_label"],
            "health_index": r["health_index"],
            "s_avg_w5": r["s_avg_w5"],
            **{c: r[c] for c in norm_cols},
        }
    return out


@pytest.fixture
def silver_env(iceberg_spark, monkeypatch):
    """test.* 카탈로그 + silver_transform 모듈 상수 패치 (테이블 prefix 변경)."""
    iceberg_spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints-it")
    _create_tables(iceberg_spark)

    # silver_transform.py 의 모듈 상수가 phm.* 로 박혀 있어 monkeypatch 로 test.* 로 변경.
    monkeypatch.setattr(st, "BRONZE_TABLE", "test.bronze.engine_sensor_raw")
    monkeypatch.setattr(st, "SILVER_TABLE", "test.silver.engine_health")
    monkeypatch.setattr(st, "FEAT_STATS_TABLE", "test.silver.feat_stats")
    monkeypatch.setattr(st, "PIPELINE_STATE_TABLE", "test.silver.pipeline_state")

    yield iceberg_spark

    iceberg_spark.sql("DROP TABLE IF EXISTS test.bronze.engine_sensor_raw")
    iceberg_spark.sql("DROP TABLE IF EXISTS test.silver.engine_health")
    iceberg_spark.sql("DROP TABLE IF EXISTS test.silver.feat_stats")
    iceberg_spark.sql("DROP TABLE IF EXISTS test.silver.pipeline_state")


def _make_args(mode, stats_version=None, silver_version="v1"):
    """silver_transform.run_* 가 받는 argparse Namespace 흉내."""
    class _A:
        pass
    a = _A()
    a.mode = mode
    a.silver_version = silver_version
    a.stats_version = stats_version
    a.kmeans_k = 6
    a.rolling_window = 5
    return a


class TestSilverIncremental:
    def test_incremental_matches_full(self, silver_env):
        """fit-stats → incremental 가 full 과 동일 silver 를 만든다 (핵심 동치성)."""
        spark = silver_env
        _insert_bronze(spark, _bronze_rows(spark, n_units=3, n_cycles=20))

        # Path A: full 한 번에
        st.run_full(spark, _make_args("full", stats_version="fs-A"))
        full_rows = _silver_rows_dict(spark)

        # silver 재초기화 (full 결과 지우고 incremental 검증)
        spark.sql("DELETE FROM test.silver.engine_health")
        spark.sql("DELETE FROM test.silver.pipeline_state")
        # feat_stats 는 그대로 두고 incremental 이 cold start 분기 실행 (last_snap=None)
        st.run_incremental(spark, _make_args("incremental", stats_version="fs-A"))
        incr_rows = _silver_rows_dict(spark)

        assert full_rows.keys() == incr_rows.keys(), \
            f"full vs incremental 행 집합 다름 — full={len(full_rows)}, incr={len(incr_rows)}"
        for k, v_full in full_rows.items():
            v_incr = incr_rows[k]
            assert v_full["op_condition_cluster"] == v_incr["op_condition_cluster"], \
                f"{k}: cluster 불일치 full={v_full['op_condition_cluster']} incr={v_incr['op_condition_cluster']}"
            assert v_full["rul_label"] == v_incr["rul_label"], f"{k}: rul_label 불일치"
            assert v_full["health_index"] == pytest.approx(v_incr["health_index"], abs=1e-9)
            for s in st.KEEP_SENSORS:
                col = f"s{s}_norm"
                assert v_full[col] == pytest.approx(v_incr[col], abs=1e-9), \
                    f"{k}: {col} 불일치 full={v_full[col]} incr={v_incr[col]}"

    def test_two_incremental_runs_extend_correctly(self, silver_env):
        """1회차 후 신규 cycle 추가 → 2회차 incremental 이 영향 unit 의 rul_label 을 재계산."""
        spark = silver_env

        # ── 1회차: cycle 1~10 ──
        first = _bronze_rows(spark, n_units=2, n_cycles=10)
        _insert_bronze(spark, first)
        st.run_full(spark, _make_args("full", stats_version="fs-A"))
        rul_after_first = {
            (r["unit_id"], r["cycle"]): r["rul_label"]
            for r in spark.table("test.silver.engine_health").collect()
        }
        # 1회차 시점: max cycle=10, unit=1 cycle=1 의 rul_label = 9
        assert rul_after_first[(1, 1)] == 9
        assert rul_after_first[(2, 10)] == 0

        spark.sql("REFRESH TABLE test.silver.pipeline_state")
        snap1 = spark.sql(
            "SELECT last_snapshot_id FROM test.silver.pipeline_state"
        ).collect()[0]["last_snapshot_id"]

        # ── 2회차: cycle 11~15 추가 (unit 1, 2 모두) ──
        second = _bronze_rows(spark, n_units=2, n_cycles=15).where(F.col("cycle") > 10)
        _insert_bronze(spark, second)
        st.run_incremental(spark, _make_args("incremental"))

        rul_after_second = {
            (r["unit_id"], r["cycle"]): r["rul_label"]
            for r in spark.table("test.silver.engine_health").collect()
        }
        # 2회차 시점: max cycle=15. unit=1 cycle=1 의 rul_label = 14 (재계산되어야 함)
        assert rul_after_second[(1, 1)] == 14, \
            "기존 행의 rul_label 이 새 max(cycle) 로 재계산 안 됨"
        assert rul_after_second[(1, 15)] == 0
        assert rul_after_second[(2, 15)] == 0
        # 행 수 확인
        assert len(rul_after_second) == 2 * 15

        spark.sql("REFRESH TABLE test.silver.pipeline_state")
        snap2 = spark.sql(
            "SELECT last_snapshot_id FROM test.silver.pipeline_state"
        ).collect()[0]["last_snapshot_id"]
        # Iceberg snapshot ID 는 random 64-bit — 순서가 아니라 식별자.
        # "전진했다" 의 검증은 (a) ID 가 변했고 (b) bronze 의 현재 ID 와 일치한다.
        assert snap2 != snap1, "pipeline_state.last_snapshot_id 가 변하지 않음"
        spark.sql("REFRESH TABLE test.bronze.engine_sensor_raw")
        bronze_now = spark.sql(
            "SELECT snapshot_id FROM test.bronze.engine_sensor_raw.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).collect()[0]["snapshot_id"]
        assert snap2 == bronze_now, (
            f"pipeline_state.last_snapshot_id={snap2} 가 bronze 의 현재 snapshot "
            f"({bronze_now}) 와 다름 — watermark 동기화 깨짐"
        )

    def test_incremental_without_feat_stats_fails(self, silver_env):
        """feat_stats 비어 있으면 incremental 은 즉시 종료 (SystemExit)."""
        spark = silver_env
        _insert_bronze(spark, _bronze_rows(spark, n_units=2, n_cycles=5))
        # feat_stats 비어 있는 상태로 incremental 시도
        with pytest.raises(SystemExit):
            st.run_incremental(spark, _make_args("incremental"))

    def test_silver_dedups_bronze_duplicates(self, silver_env):
        """Bronze 가 같은 (source_file, line_no) 를 두 번 받아도 Silver 는 unique.

        (a) 정책의 *시스템 레벨 멱등성* 보장 — Bronze append-only + Silver dedup.
        Bronze 단독 idempotency 는 test_bronze_idempotency.py 가 깨졌다고 검증하고,
        여기서는 그게 Silver 진입에서 흡수되는지를 검증한다.
        """
        spark = silver_env
        bronze = _bronze_rows(spark, n_units=2, n_cycles=10)

        # 같은 batch 두 번 → bronze 에 의도적 dup 발생
        _insert_bronze(spark, bronze)
        _insert_bronze(spark, bronze)
        assert spark.table("test.bronze.engine_sensor_raw").count() == 40, \
            "테스트 가정 깨짐 — bronze 가 append-only 가 아님"

        st.run_full(spark, _make_args("full", stats_version="fs-A"))

        silver_n = spark.table("test.silver.engine_health").count()
        assert silver_n == 20, (
            f"Silver 에 dup 이 흘러감. expected=20 (2 units × 10 cycles), got={silver_n}"
        )
        # (dataset_id, unit_id, cycle) 자연키도 unique 한지 재확인
        keys = (
            spark.table("test.silver.engine_health")
                 .select("dataset_id", "unit_id", "cycle")
                 .distinct().count()
        )
        assert keys == silver_n

    def test_no_new_snapshots_is_noop(self, silver_env):
        """동일 bronze snapshot 에 대해 incremental 재실행 → no-op."""
        spark = silver_env
        _insert_bronze(spark, _bronze_rows(spark, n_units=2, n_cycles=10))
        st.run_full(spark, _make_args("full", stats_version="fs-A"))
        n_before = spark.table("test.silver.engine_health").count()

        # 추가 데이터 없이 incremental — 처리 0행
        st.run_incremental(spark, _make_args("incremental"))
        n_after = spark.table("test.silver.engine_health").count()
        assert n_after == n_before
        rows_processed = spark.sql(
            "SELECT rows_processed FROM test.silver.pipeline_state"
        ).collect()[0]["rows_processed"]
        assert rows_processed == 0
