"""
Gold KPI 일배치: phm.silver.engine_health + phm.gold.rul_prediction
                  → phm.gold.fleet_kpi_daily

집계 키: (kpi_date, dataset_id, op_condition_cluster)
KPI:
  - engine_count           : 누적 unit 수
  - active_engine_count    : 당일 cycle 발생한 unit 수
  - critical/high/medium   : RUL ≤ 10/30/80 인 (unit, cycle) 수
  - risk_ratio_critical    : critical_count / active_cycles
  - HI 백분위 (p10/p50/p90), 평균
  - avg_degradation_rate   : HI 의 cycle 차분 일자 평균

실행:
  docker exec -it phm-spark /opt/spark/bin/spark-submit --master "local[2]" \
      /workspace/code/pipelines/gold_kpi_aggregate.py \
      --model-version gbt-v0
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

TARGET_COLS = [
    "kpi_date", "dataset_id", "op_condition_cluster",
    "engine_count", "active_engine_count",
    "critical_count", "high_count", "medium_count", "risk_ratio_critical",
    "avg_health_index", "health_index_p10", "health_index_p50", "health_index_p90",
    "avg_degradation_rate",
    "kpi_ts", "source_model_version",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-version", default="gbt-v0")
    ap.add_argument("--ts-col", default="event_ts",
                    help="kpi_date 산출에 사용할 Silver 시각 컬럼")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("gold_kpi_aggregate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

    silver = spark.table("phm.silver.engine_health") \
        .withColumn("kpi_date", F.to_date(F.col(args.ts_col)))

    # HI cycle 차분 (degradation rate)
    eng_w = Window.partitionBy("dataset_id", "unit_id").orderBy("cycle")
    silver = silver.withColumn(
        "hi_diff",
        F.col("health_index") - F.lag("health_index").over(eng_w),
    )

    pred = (
        spark.table("phm.gold.rul_prediction")
        .where(F.col("model_version") == args.model_version)
        .select("dataset_id", "unit_id", "cycle", "rul_pred", "risk_tier")
    )

    joined = silver.join(pred, ["dataset_id", "unit_id", "cycle"], "left")

    grp = joined.groupBy("kpi_date", "dataset_id", "op_condition_cluster").agg(
        F.countDistinct("unit_id").alias("active_engine_count"),
        F.sum(F.when(F.col("risk_tier") == "CRITICAL", 1).otherwise(0)).alias("critical_count"),
        F.sum(F.when(F.col("risk_tier").isin("CRITICAL", "HIGH"), 1).otherwise(0)).alias("high_count"),
        F.sum(F.when(F.col("risk_tier").isin("CRITICAL", "HIGH", "MED"), 1).otherwise(0)).alias("medium_count"),
        F.count(F.lit(1)).alias("active_cycles"),
        F.avg("health_index").alias("avg_health_index"),
        F.percentile_approx("health_index", 0.1).alias("health_index_p10"),
        F.percentile_approx("health_index", 0.5).alias("health_index_p50"),
        F.percentile_approx("health_index", 0.9).alias("health_index_p90"),
        F.avg("hi_diff").alias("avg_degradation_rate"),
    )

    # engine_count: dataset_id × cluster 누적 unique unit
    cum = (
        silver.groupBy("dataset_id", "op_condition_cluster")
        .agg(F.countDistinct("unit_id").alias("engine_count"))
    )

    out = (
        grp.join(cum, ["dataset_id", "op_condition_cluster"], "left")
        .withColumn(
            "risk_ratio_critical",
            F.when(F.col("active_cycles") > 0,
                   F.col("critical_count") / F.col("active_cycles")).otherwise(F.lit(0.0)),
        )
        .withColumn("kpi_ts", F.current_timestamp())
        .withColumn("source_model_version", F.lit(args.model_version))
        .select(*TARGET_COLS)
    )

    out = out.localCheckpoint(eager=True)
    out.createOrReplaceTempView("_kpi_batch")

    update_assigns = ", ".join(
        f"t.{c} = s.{c}" for c in TARGET_COLS
        if c not in ("kpi_date", "dataset_id", "op_condition_cluster")
    )
    insert_cols = ", ".join(TARGET_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in TARGET_COLS)

    spark.sql(f"""
        MERGE INTO phm.gold.fleet_kpi_daily t
        USING (SELECT * FROM _kpi_batch) s
          ON  t.kpi_date             = s.kpi_date
          AND t.dataset_id           = s.dataset_id
          AND t.op_condition_cluster = s.op_condition_cluster
        WHEN MATCHED THEN UPDATE SET {update_assigns}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    print("[gold_kpi_aggregate] sample rows:")
    spark.sql("""
        SELECT kpi_date, dataset_id, op_condition_cluster,
               active_engine_count, critical_count, high_count,
               ROUND(avg_health_index, 3) AS hi
          FROM phm.gold.fleet_kpi_daily
         ORDER BY kpi_date, dataset_id, op_condition_cluster
         LIMIT 20
    """).show(truncate=False)


if __name__ == "__main__":
    main()
