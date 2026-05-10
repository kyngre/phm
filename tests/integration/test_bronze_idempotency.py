"""Iceberg 통합 테스트 — Bronze MERGE 멱등성.

검증 시나리오 (README §9 — `(source_file, line_no)` 멱등키):
  A. 같은 배치 두 번 적재 → 행 수 불변, 새 snapshot 만 추가
  B. 동일 자연키지만 값이 다른 행 적재 → INSERT 안 됨 (`WHEN NOT MATCHED THEN INSERT *`)
     ※ Bronze 의 의도: 원본 보존 — 첫 라인이 정답 (값 변경은 Silver 가 함)
  C. 신규 line_no 가 섞인 배치 → 신규 행만 추가
  D. 빈 배치 → no-op (커밋도 없음)
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark", reason="integration tests require pyspark")

from pyspark.sql import functions as F

from bronze_ingest import upsert_batch

pytestmark = pytest.mark.integration


def _create_bronze_table(spark):
    spark.sql("DROP TABLE IF EXISTS test.bronze.engine_sensor_raw")
    sensor_cols = ",\n".join(f"sensor_{i} DOUBLE" for i in range(1, 22))
    spark.sql(f"""
        CREATE TABLE test.bronze.engine_sensor_raw (
            dataset_id   STRING,
            unit_id      INT,
            cycle        INT,
            op_setting_1 DOUBLE,
            op_setting_2 DOUBLE,
            op_setting_3 DOUBLE,
            {sensor_cols},
            event_ts     TIMESTAMP,
            ingest_ts    TIMESTAMP,
            source_file  STRING,
            line_no      BIGINT
        )
        USING iceberg
        PARTITIONED BY (dataset_id, days(ingest_ts))
        TBLPROPERTIES ('format-version' = '2')
    """)


def _row(line_no: int, unit: int = 1, cycle: int = 1, sensor_1: float = 100.0,
         source_file: str = "train_FD001.txt"):
    base = datetime(2025, 8, 1, tzinfo=timezone.utc)
    rec = {
        "dataset_id": "FD001",
        "unit_id": unit,
        "cycle": cycle,
        "op_setting_1": 0.5, "op_setting_2": 0.5, "op_setting_3": 100.0,
        "event_ts": base, "ingest_ts": base,
        "source_file": source_file, "line_no": line_no,
    }
    for s in range(1, 22):
        rec[f"sensor_{s}"] = sensor_1 if s == 1 else 10.0
    return rec


@pytest.fixture
def bronze_table(iceberg_spark, monkeypatch):
    # upsert_batch 가 하드코딩된 phm.bronze 대신 test.bronze 를 가리키도록 패치
    import bronze_ingest
    monkeypatch.setattr(bronze_ingest, "TARGET_TABLE", "test.bronze.engine_sensor_raw")
    _create_bronze_table(iceberg_spark)
    yield "test.bronze.engine_sensor_raw"
    iceberg_spark.sql("DROP TABLE IF EXISTS test.bronze.engine_sensor_raw")


class TestBronzeIdempotency:
    def test_same_batch_twice_no_duplicates(self, iceberg_spark, bronze_table):
        batch = iceberg_spark.createDataFrame([_row(i) for i in range(1, 11)])
        upsert_batch(batch, batch_id=0)
        first = iceberg_spark.table(bronze_table).count()
        assert first == 10

        upsert_batch(batch, batch_id=1)
        second = iceberg_spark.table(bronze_table).count()
        assert second == 10, "같은 (source_file, line_no) 재적재 — 행 수 변하면 안 됨"

    def test_value_change_on_same_key_does_not_overwrite(self, iceberg_spark, bronze_table):
        """Bronze 정책: WHEN NOT MATCHED THEN INSERT *  (UPDATE 없음)
        같은 line_no 로 다른 sensor_1 값을 보내도 첫 적재 값이 유지돼야 함."""
        original = iceberg_spark.createDataFrame([_row(line_no=1, sensor_1=100.0)])
        upsert_batch(original, 0)

        mutated = iceberg_spark.createDataFrame([_row(line_no=1, sensor_1=999.0)])
        upsert_batch(mutated, 1)

        rows = iceberg_spark.table(bronze_table).collect()
        assert len(rows) == 1
        assert rows[0]["sensor_1"] == 100.0, \
            "Bronze 는 INSERT-only — 같은 키의 값이 덮어써지면 원본 보존 약속 깨짐"

    def test_overlapping_batch_inserts_only_new(self, iceberg_spark, bronze_table):
        first = iceberg_spark.createDataFrame([_row(i) for i in range(1, 6)])
        upsert_batch(first, 0)
        assert iceberg_spark.table(bronze_table).count() == 5

        # 3,4,5 중복 + 6,7,8 신규
        overlap = iceberg_spark.createDataFrame([_row(i) for i in range(3, 9)])
        upsert_batch(overlap, 1)
        assert iceberg_spark.table(bronze_table).count() == 8

    def test_different_source_file_same_line_no_both_inserted(
        self, iceberg_spark, bronze_table,
    ):
        """멱등키는 (source_file, line_no) — source_file 이 다르면 별개 row."""
        a = iceberg_spark.createDataFrame([_row(line_no=1, source_file="train_FD001.txt")])
        b = iceberg_spark.createDataFrame([_row(line_no=1, source_file="train_FD002.txt")])
        upsert_batch(a, 0)
        upsert_batch(b, 1)
        assert iceberg_spark.table(bronze_table).count() == 2

    def test_empty_batch_no_op(self, iceberg_spark, bronze_table):
        empty = iceberg_spark.createDataFrame(
            [_row(1)]
        ).where(F.lit(False))  # 스키마는 유지, 행은 0
        upsert_batch(empty, 0)
        assert iceberg_spark.table(bronze_table).count() == 0

    def test_three_runs_create_three_snapshots(self, iceberg_spark, bronze_table):
        """Iceberg snapshot 누적 — 멱등 재실행이라도 snapshot 자체는 늘어남.

        README §5 #4 (snapshot 증가율) 의 "재실행 → snapshot 폭증" 시나리오 근거.
        """
        batch = iceberg_spark.createDataFrame([_row(1)])
        for i in range(3):
            upsert_batch(batch, i)
        snaps = iceberg_spark.sql(
            f"SELECT COUNT(*) AS n FROM {bronze_table}.snapshots"
        ).collect()
        assert snaps[0]["n"] >= 3
