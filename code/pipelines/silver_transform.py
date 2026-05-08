"""
Bronze → Silver MERGE INTO 변환 (배치).

처리 단계:
  1) op_setting 1~3 으로 KMeans(k=6) → op_condition_cluster (FD002/004 의 6 condition)
  2) 분산 0 센서(1,5,6,10,16,18,19) 제외, 나머지 14개를 cluster별 z-score 정규화
  3) (dataset_id, unit_id) 시간순 5-cycle rolling: 평균/표준편차/추세
  4) Health Index — cluster 평균 대비 이탈 정도(0=열화 / 1=정상)
  5) rul_label = max(cycle) per engine - cycle  (train 가정: 최종 cycle = 고장)
  6) MERGE INTO phm.silver.engine_health  ON (dataset_id, unit_id, cycle)

실행:
  docker exec -it phm-spark /opt/spark/bin/spark-submit --master "local[2]" \
      /workspace/code/pipelines/silver_transform.py --silver-version v1
"""
from __future__ import annotations

import argparse

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

KEEP_SENSORS = [2, 3, 4, 7, 8, 9, 11, 12, 13, 14, 15, 17, 20, 21]
DROP_SENSORS = [1, 5, 6, 10, 16, 18, 19]  # C-MAPSS 분산 0 / 무정보 센서

TARGET_COLS = (
    ["dataset_id", "unit_id", "cycle",
     "op_setting_1", "op_setting_2", "op_setting_3",
     "op_condition_cluster"]
    + [f"s{s}_norm" for s in KEEP_SENSORS]
    + ["s_avg_w5", "s_std_w5", "s_trend_w5",
       "health_index", "rul_label",
       "event_ts", "ingest_ts", "silver_ts", "silver_version"]
)


def stable_cluster_ids(model_centers, prediction_col_df, prediction_col):
    """KMeans 클러스터 ID 가 매 실행 swap 되는 문제를 완화.
    op_setting_1 오름차순으로 0..k-1 재할당."""
    order = sorted(range(len(model_centers)),
                   key=lambda i: (float(model_centers[i][0]),
                                  float(model_centers[i][1]),
                                  float(model_centers[i][2])))
    remap = {old: new for new, old in enumerate(order)}
    mapping_expr = F.create_map([F.lit(x) for kv in remap.items() for x in kv])
    return prediction_col_df.withColumn(
        prediction_col, mapping_expr[F.col(prediction_col)]
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver-version", default="v1")
    ap.add_argument("--kmeans-k", type=int, default=6)
    ap.add_argument("--rolling-window", type=int, default=5)
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("silver_transform").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze = spark.table("phm.bronze.engine_sensor_raw")

    # 1) op_condition_cluster
    assembler = VectorAssembler(
        inputCols=["op_setting_1", "op_setting_2", "op_setting_3"],
        outputCol="op_vec",
    )
    bronze_v = assembler.transform(bronze)
    kmeans = KMeans(
        k=args.kmeans_k, seed=42,
        featuresCol="op_vec", predictionCol="op_condition_cluster",
    )
    model = kmeans.fit(bronze_v)
    df = model.transform(bronze_v).drop("op_vec")
    df = stable_cluster_ids(model.clusterCenters(), df, "op_condition_cluster")

    # 2) 센서 z-score (cluster 별)
    cluster_w = Window.partitionBy("dataset_id", "op_condition_cluster")
    for s in KEEP_SENSORS:
        col = f"sensor_{s}"
        m = F.avg(col).over(cluster_w)
        sd = F.stddev_pop(col).over(cluster_w)
        df = df.withColumn(
            f"s{s}_norm",
            F.when(sd > 0, (F.col(col) - m) / sd).otherwise(F.lit(0.0)),
        )

    # 3) rolling 피처 (engine timeline)
    w = args.rolling_window
    engine_w = (
        Window.partitionBy("dataset_id", "unit_id")
        .orderBy("cycle")
        .rowsBetween(-(w - 1), 0)
    )
    norm_cols = [F.col(f"s{s}_norm") for s in KEEP_SENSORS]
    s_row_mean = sum(norm_cols) / F.lit(len(norm_cols))
    df = df.withColumn("_s_row_mean", s_row_mean)
    df = df.withColumn("s_avg_w5", F.avg("_s_row_mean").over(engine_w))
    df = df.withColumn("s_std_w5", F.stddev_samp("_s_row_mean").over(engine_w))
    # 추세: window 내 첫 행 대비 현재 (∝ 기울기 × (w-1))
    first_in_w = F.first("_s_row_mean").over(engine_w)
    df = df.withColumn(
        "s_trend_w5",
        (F.col("_s_row_mean") - first_in_w) / F.lit(max(w - 1, 1)),
    )

    # 4) Health Index — cluster 평균(z=0) 에서 멀수록 열화. 정규화 [0,1].
    df = df.withColumn(
        "health_index",
        F.lit(1.0) - F.least(F.abs(F.col("s_avg_w5")), F.lit(3.0)) / F.lit(3.0),
    )

    # 5) rul_label
    engine_all_w = Window.partitionBy("dataset_id", "unit_id")
    df = df.withColumn("rul_label",
                       F.max("cycle").over(engine_all_w) - F.col("cycle"))

    # 6) silver_ts / version + 컬럼 정렬
    df = (
        df.withColumn("silver_ts", F.current_timestamp())
          .withColumn("silver_version", F.lit(args.silver_version))
          .select(*TARGET_COLS)
    )

    # MERGE source 가 window/KMeans 체인이면 Spark 가 non-deterministic 판정.
    # localCheckpoint 로 lineage 를 끊고 materialize.
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    df = df.localCheckpoint(eager=True)

    df.createOrReplaceTempView("_silver_batch")

    update_assigns = ", ".join(f"t.{c} = s.{c}"
                               for c in TARGET_COLS
                               if c not in ("dataset_id", "unit_id", "cycle"))
    insert_cols = ", ".join(TARGET_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in TARGET_COLS)

    spark.sql(f"""
        MERGE INTO phm.silver.engine_health t
        USING (SELECT * FROM _silver_batch) s
          ON  t.dataset_id = s.dataset_id
          AND t.unit_id    = s.unit_id
          AND t.cycle      = s.cycle
        WHEN MATCHED THEN UPDATE SET {update_assigns}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    rows = spark.table("phm.silver.engine_health").count()
    print(f"[silver_transform] silver row count = {rows}")


if __name__ == "__main__":
    main()
