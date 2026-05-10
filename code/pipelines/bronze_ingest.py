"""
Spark Structured Streaming: Kafka → Bronze Iceberg (phm.bronze.engine_sensor_raw).

정책 — Bronze 는 append-only:
  - foreachBatch 마다 writeTo(...).append() 로 *읽지 않고 쓰기만 한다*.
  - MERGE INTO 를 쓰지 않음으로써 batch 마다의 read amplification 을 0 으로 만든다
    (운영 시스템 패턴: 트래픽 증가 시 Bronze write 비용이 데이터 누적과 무관).
  - 멱등성은 Silver 진입의 dedup_bronze() 에서 (source_file, line_no) 단위로 보장.
    Spark Structured Streaming foreachBatch 의 at-least-once 로 발생하는 dup 을 흡수.

- event_ts: 메시지에 없으면 ingest_ts 와 동일 (실 운영에서는 producer가 채움).
- partition: dataset_id, days(ingest_ts).
- 작은 파일 누적: maintenance/01_rewrite_data_files.sql (일배치) 가 흡수.

실행:
  docker exec -it phm-spark /opt/spark/bin/spark-submit \
      --master local[2] \
      /workspace/code/pipelines/bronze_ingest.py
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "phm.engine.sensor")
CHECKPOINT = os.environ.get(
    "BRONZE_CHECKPOINT",
    "file:///workspace/data/_checkpoints/bronze_engine_sensor_raw",
)
TARGET_TABLE = "phm.bronze.engine_sensor_raw"
TRIGGER_INTERVAL = os.environ.get("BRONZE_TRIGGER", "10 seconds")
STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "earliest")


def message_schema() -> StructType:
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
        StructField("source_file", StringType()),
        StructField("line_no", LongType()),
    ]
    return StructType(fields)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("bronze_ingest")
        .getOrCreate()
    )


def append_batch(batch_df, batch_id: int):
    """배치를 Bronze 에 append. 읽기 비용 0.

    중복 (source_file, line_no) 는 Silver 진입에서 dedup — 모듈 docstring 참조.
    빈 배치는 빈 snapshot 생성 회피 위해 early return.
    """
    if batch_df.rdd.isEmpty():
        return
    batch_df.writeTo(TARGET_TABLE).append()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    # producer 가 ISO with 'Z' (UTC) 발행 — timestampFormat 명시로 파싱 안정화.
    parsed = (
        raw.select(F.from_json(
            F.col("value").cast("string"),
            message_schema(),
            {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"},
        ).alias("m"))
        .select("m.*")
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn(
            "event_ts",
            F.coalesce(F.col("event_ts"), F.col("ingest_ts")),
        )
    )

    # Bronze 테이블 컬럼 순서로 정렬
    target_cols = [
        "dataset_id", "unit_id", "cycle",
        "op_setting_1", "op_setting_2", "op_setting_3",
    ] + [f"sensor_{i}" for i in range(1, 22)] + [
        "event_ts", "ingest_ts", "source_file", "line_no",
    ]
    parsed = parsed.select(*target_cols)

    query = (
        parsed.writeStream
        .foreachBatch(append_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
