"""Iceberg 통합 테스트 — Silver MERGE 멱등성 + 백필 시나리오.

검증 (README §3-2, §8-4, §9):
  A. 같은 입력 두 번 변환 + MERGE → 행 수 불변, health_index/op_condition_cluster 불변
     (KMeans seed=42 + stable_cluster_ids 약속)
  B. 백필 — 같은 자연키지만 event_ts 만 다른 입력 → UPDATE only,
     event_ts 가 새 값으로 갱신됨 (README §8-4 의 "재시뮬레이션 함정" 동작 검증)
  C. 신규 unit 추가된 입력 → 기존 행은 유지, 신규만 INSERT
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark", reason="integration tests require pyspark")

from pyspark.sql import Window
from pyspark.sql import functions as F

from silver_transform import (
    KEEP_SENSORS,
    TARGET_COLS,
    add_health_index,
    add_is_test_flag,
    add_rolling_features,
    add_rul_label,
    cluster_op_conditions,
)

pytestmark = pytest.mark.integration


def _create_silver_table(spark):
    spark.sql("DROP TABLE IF EXISTS test.silver.engine_health")
    norm_cols = ", ".join(f"s{s}_norm DOUBLE" for s in KEEP_SENSORS)
    spark.sql(f"""
        CREATE TABLE test.silver.engine_health (
            dataset_id           STRING,
            unit_id              INT,
            cycle                INT,
            op_setting_1         DOUBLE,
            op_setting_2         DOUBLE,
            op_setting_3         DOUBLE,
            op_condition_cluster INT,
            {norm_cols},
            s_avg_w5             DOUBLE,
            s_std_w5             DOUBLE,
            s_trend_w5           DOUBLE,
            health_index         DOUBLE,
            rul_label            INT,
            is_test              BOOLEAN,
            event_ts             TIMESTAMP,
            ingest_ts            TIMESTAMP,
            silver_ts            TIMESTAMP,
            silver_version       STRING
        )
        USING iceberg
        PARTITIONED BY (dataset_id, days(event_ts))
        TBLPROPERTIES (
            'format-version' = '2',
            'write.merge.mode' = 'copy-on-write'
        )
    """)


def _bronze_rows(spark, n_units: int = 3, n_cycles: int = 20,
                 base: datetime | None = None):
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
            row = {
                "dataset_id": "FD001",
                "unit_id": u, "cycle": c,
                "op_setting_1": op1 + (c % 3 - 1) * 0.01,
                "op_setting_2": op2 + (c % 3 - 1) * 0.005,
                "op_setting_3": op3,
                "event_ts": base + timedelta(hours=u + c),
                "ingest_ts": base,
            }
            for s in range(1, 22):
                row[f"sensor_{s}"] = 500.0 + s * 10 + (c * 0.1) + (cluster * 5)
            rows.append(row)
    return spark.createDataFrame(rows)


def _silver_transform(spark, bronze):
    """main() 의 핵심 변환 (MERGE 직전까지). 테스트에서 두 번 호출해 비교."""
    df, _ = cluster_op_conditions(bronze, k=6, seed=42)
    cw = Window.partitionBy("dataset_id", "op_condition_cluster")
    for s in KEEP_SENSORS:
        col = f"sensor_{s}"
        m = F.avg(col).over(cw)
        sd = F.stddev_pop(col).over(cw)
        df = df.withColumn(
            f"s{s}_norm",
            F.when(sd > 0, (F.col(col) - m) / sd).otherwise(F.lit(0.0)),
        )
    df = add_rolling_features(df, [f"s{s}_norm" for s in KEEP_SENSORS], window=5)
    df = add_health_index(df)
    df = add_is_test_flag(df)
    df = add_rul_label(df)
    df = (
        df.withColumn("silver_ts", F.current_timestamp())
          .withColumn("silver_version", F.lit("v1"))
          .select(*TARGET_COLS)
    )
    return df.localCheckpoint(eager=True)


def _merge(spark, df, table: str = "test.silver.engine_health"):
    df.createOrReplaceTempView("_silver_batch")
    update_assigns = ", ".join(f"t.{c} = s.{c}"
                               for c in TARGET_COLS
                               if c not in ("dataset_id", "unit_id", "cycle"))
    insert_cols = ", ".join(TARGET_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in TARGET_COLS)
    spark.sql(f"""
        MERGE INTO {table} t
        USING (SELECT * FROM _silver_batch) s
          ON  t.dataset_id = s.dataset_id
          AND t.unit_id    = s.unit_id
          AND t.cycle      = s.cycle
        WHEN MATCHED THEN UPDATE SET {update_assigns}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)


@pytest.fixture
def silver_table(iceberg_spark):
    iceberg_spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints-it")
    _create_silver_table(iceberg_spark)
    yield "test.silver.engine_health"
    iceberg_spark.sql("DROP TABLE IF EXISTS test.silver.engine_health")


class TestSilverIdempotency:
    def test_same_input_twice_same_health_index(self, iceberg_spark, silver_table):
        bronze = _bronze_rows(iceberg_spark)

        first = _silver_transform(iceberg_spark, bronze)
        _merge(iceberg_spark, first, silver_table)
        rows1 = {(r["unit_id"], r["cycle"]):
                 (r["op_condition_cluster"], r["health_index"])
                 for r in iceberg_spark.table(silver_table).collect()}

        second = _silver_transform(iceberg_spark, bronze)
        _merge(iceberg_spark, second, silver_table)
        rows2 = {(r["unit_id"], r["cycle"]):
                 (r["op_condition_cluster"], r["health_index"])
                 for r in iceberg_spark.table(silver_table).collect()}

        assert rows1.keys() == rows2.keys(), "행 수가 변함 — MERGE 멱등성 깨짐"
        for k in rows1:
            c1, hi1 = rows1[k]
            c2, hi2 = rows2[k]
            assert c1 == c2, f"unit/cycle={k} cluster id 가 변함 ({c1}→{c2})"
            assert hi1 == pytest.approx(hi2), \
                f"unit/cycle={k} health_index 가 변함 ({hi1}→{hi2})"

    def test_backfill_updates_event_ts_in_place(self, iceberg_spark, silver_table):
        """README §8-4: 다른 base_date 로 재시뮬 → MERGE 가 UPDATE 로 event_ts 만 덮음.

        이 동작은 의도적이며 테스트로 못박는다. 시계열을 보존하려면 시뮬 전에
        snapshot 태그가 필요하다는 게 README 의 안내.
        """
        bronze1 = _bronze_rows(iceberg_spark, base=datetime(2025, 8, 1, tzinfo=timezone.utc))
        _merge(iceberg_spark, _silver_transform(iceberg_spark, bronze1), silver_table)
        n_before = iceberg_spark.table(silver_table).count()
        ts_before = (
            iceberg_spark.table(silver_table)
            .agg(F.min("event_ts").alias("mn"), F.max("event_ts").alias("mx"))
            .collect()[0]
        )

        bronze2 = _bronze_rows(iceberg_spark, base=datetime(2025, 9, 1, tzinfo=timezone.utc))
        _merge(iceberg_spark, _silver_transform(iceberg_spark, bronze2), silver_table)

        n_after = iceberg_spark.table(silver_table).count()
        ts_after = (
            iceberg_spark.table(silver_table)
            .agg(F.min("event_ts").alias("mn"), F.max("event_ts").alias("mx"))
            .collect()[0]
        )

        assert n_after == n_before, \
            "동일 자연키 백필인데 행 수가 늘었음 — MERGE ON 절이 깨졌을 가능성"
        assert ts_after["mn"] > ts_before["mx"], \
            "event_ts 가 새 base_date 로 갱신되지 않음 — UPDATE 가 발화 안 함"

    def test_new_units_only_inserted(self, iceberg_spark, silver_table):
        bronze1 = _bronze_rows(iceberg_spark, n_units=2, n_cycles=10)
        _merge(iceberg_spark, _silver_transform(iceberg_spark, bronze1), silver_table)
        n1 = iceberg_spark.table(silver_table).count()
        assert n1 == 2 * 10

        bronze2 = _bronze_rows(iceberg_spark, n_units=3, n_cycles=10)  # +unit 3
        _merge(iceberg_spark, _silver_transform(iceberg_spark, bronze2), silver_table)
        n2 = iceberg_spark.table(silver_table).count()
        assert n2 == 3 * 10, "신규 unit 의 행이 insert 안 됨"
