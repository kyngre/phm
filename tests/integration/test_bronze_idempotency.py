"""Iceberg 통합 테스트 — Bronze append-only 시맨틱.

Bronze 정책 (B 프레이밍 — 실시간 운영 추론):
  - foreachBatch 마다 writeTo(...).append() — 읽기 비용 0
  - 멱등성은 Silver 진입의 dedup_bronze() 가 흡수 → test_silver_incremental.py
  - Bronze 자체는 *시스템 audit log* (raw, dup 가능)

이 파일은 *Bronze 단의 행동* 만 검증:
  A. 같은 batch 두 번 → 두 행 (append-only 약속)
  B. 다른 source_file + 같은 line_no → 둘 다 (별개 키)
  C. 빈 배치 → no-op (snapshot 생성 안 함)
  D. 매 batch 마다 새 snapshot 발생
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark", reason="integration tests require pyspark")

from pyspark.sql import functions as F  # noqa: E402

from bronze_ingest import append_batch  # noqa: E402

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
    # append_batch 가 하드코딩된 phm.bronze 대신 test.bronze 를 가리키도록 패치
    import bronze_ingest
    monkeypatch.setattr(bronze_ingest, "TARGET_TABLE", "test.bronze.engine_sensor_raw")
    _create_bronze_table(iceberg_spark)
    yield "test.bronze.engine_sensor_raw"
    iceberg_spark.sql("DROP TABLE IF EXISTS test.bronze.engine_sensor_raw")


class TestBronzeAppend:
    """Bronze 는 append-only — 멱등성 보장은 Silver 가 한다."""

    def test_same_batch_twice_creates_duplicates(self, iceberg_spark, bronze_table):
        """같은 (source_file, line_no) 가 두 번 적재되면 두 행이 남는다.
        이전 MERGE 시맨틱과 *의도적으로 다른* 동작 — 시스템 멱등성은 Silver 가 흡수.
        """
        batch = iceberg_spark.createDataFrame([_row(i) for i in range(1, 11)])
        append_batch(batch, batch_id=0)
        assert iceberg_spark.table(bronze_table).count() == 10

        append_batch(batch, batch_id=1)
        assert iceberg_spark.table(bronze_table).count() == 20, (
            "Bronze append-only 인데 두 번째 batch 가 dedup 됨 — 정책 깨짐"
        )

        # (source_file, line_no) 단위 dup 카운트 확인
        dups = (
            iceberg_spark.table(bronze_table)
            .groupBy("source_file", "line_no").count()
            .where(F.col("count") > 1)
            .count()
        )
        assert dups == 10, f"기대: 10개 키 모두 dup. 실제 dup 키: {dups}"

    def test_different_source_file_same_line_no_both_inserted(
        self, iceberg_spark, bronze_table,
    ):
        """다른 source_file + 같은 line_no — 별개 row (키 자체가 다름)."""
        a = iceberg_spark.createDataFrame([_row(line_no=1, source_file="train_FD001.txt")])
        b = iceberg_spark.createDataFrame([_row(line_no=1, source_file="train_FD002.txt")])
        append_batch(a, 0)
        append_batch(b, 1)
        assert iceberg_spark.table(bronze_table).count() == 2

    def test_empty_batch_no_op(self, iceberg_spark, bronze_table):
        """빈 배치는 snapshot 도 만들지 않음 (rewrite/expire 비용 절감)."""
        empty = iceberg_spark.createDataFrame(
            [_row(1)]
        ).where(F.lit(False))  # 스키마는 유지, 행은 0
        snaps_before = iceberg_spark.sql(
            f"SELECT COUNT(*) AS n FROM {bronze_table}.snapshots"
        ).collect()[0]["n"]

        append_batch(empty, 0)

        assert iceberg_spark.table(bronze_table).count() == 0
        snaps_after = iceberg_spark.sql(
            f"SELECT COUNT(*) AS n FROM {bronze_table}.snapshots"
        ).collect()[0]["n"]
        assert snaps_after == snaps_before, "빈 배치인데 snapshot 이 늘었음"

    def test_each_batch_creates_new_snapshot(self, iceberg_spark, bronze_table):
        """매 batch = 1 snapshot. incremental scan watermark 의 입력이 됨.

        README §5 #4 의 "snapshot 증가율" 모니터링이 의미 있는 이유 + Silver
        incremental 의 start-snapshot-id 가 매 batch 단위로 전진할 수 있는 근거.
        """
        for i in range(3):
            batch = iceberg_spark.createDataFrame([_row(line_no=10 + i)])
            append_batch(batch, i)
        snaps = iceberg_spark.sql(
            f"SELECT COUNT(*) AS n FROM {bronze_table}.snapshots"
        ).collect()
        assert snaps[0]["n"] >= 3
        assert iceberg_spark.table(bronze_table).count() == 3
