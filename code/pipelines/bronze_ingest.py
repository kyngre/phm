"""
Spark Structured Streaming: Kafka → Bronze Iceberg (phm.bronze.engine_sensor_raw).

- 멱등키: (source_file, line_no) — Kafka 재처리 시 중복 차단을 위해 MERGE INTO 사용.
- event_ts: 메시지에 없으면 ingest_ts 와 동일 (실 운영에서는 producer가 채움).
- partition: dataset_id, days(ingest_ts).

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


def upsert_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        return
    batch_df.createOrReplaceTempView("_bronze_batch")
    spark = batch_df.sparkSession
    # (source_file, line_no) 멱등 — 중복 라인은 무시.
    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} t
        USING (
            SELECT * FROM _bronze_batch
        ) s
        ON  t.source_file = s.source_file
        AND t.line_no     = s.line_no
        WHEN NOT MATCHED THEN INSERT *
    """)


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
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
