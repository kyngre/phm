"""
Silver → Gold RUL 예측 (배치, v0 베이스라인).

모델: Spark MLlib GBTRegressor (가볍고 단일 컨테이너에서 동작).
  - feature: op_condition_cluster (numeric), 14 normalized sensors,
             rolling (avg/std/trend), health_index, cycle
  - target:  min(rul_label, --rul-cap)   ← PHM 관례: 초기 cycle은 RUL을 130 등으로 cap
  - 신뢰구간: 학습 잔차 표준편차 σ → [pred − 1.645σ, pred + 1.645σ]  (정규분포 90% CI)

PHM08 score = sum( exp(d/13) − 1  if d>0 else  exp(−d/10) − 1 ),  d = pred − actual

실행:
  docker exec -it phm-spark /opt/spark/bin/spark-submit --master "local[2]" \
      /workspace/code/pipelines/gold_rul_predict.py \
      --model-version gbt-v0 --rul-cap 130
"""
from __future__ import annotations

import argparse

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KEEP_SENSORS = [2, 3, 4, 7, 8, 9, 11, 12, 13, 14, 15, 17, 20, 21]

FEATURES = (
    ["op_condition_cluster", "cycle"]
    + [f"s{s}_norm" for s in KEEP_SENSORS]
    + ["s_avg_w5", "s_std_w5", "s_trend_w5", "health_index"]
)

TARGET_COLS = [
    "model_version", "dataset_id", "unit_id", "cycle",
    "rul_pred", "rul_pred_lower", "rul_pred_upper",
    "rul_actual", "abs_error", "phm08_score",
    "risk_tier", "predict_ts", "silver_snapshot_id",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-version", default="gbt-v0")
    ap.add_argument("--rul-cap", type=int, default=130,
                    help="학습 라벨 상한 (PHM 관례)")
    ap.add_argument("--max-iter", type=int, default=50)
    ap.add_argument("--max-depth", type=int, default=5)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("gold_rul_predict").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

    silver = spark.table("phm.silver.engine_health")

    # 사용 silver snapshot id (재현성)
    snap_row = spark.sql(
        "SELECT snapshot_id FROM phm.silver.engine_health.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    silver_snapshot_id = int(snap_row[0]["snapshot_id"]) if snap_row else 0

    # rolling NaN(window 시작부) → 0 채움, 라벨 cap
    df = silver.na.fill(0.0, subset=["s_std_w5", "s_trend_w5"])
    df = df.withColumn(
        "rul_capped",
        F.least(F.col("rul_label").cast("double"), F.lit(float(args.rul_cap))),
    )

    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features",
                                handleInvalid="skip")
    gbt = GBTRegressor(featuresCol="features", labelCol="rul_capped",
                       maxIter=args.max_iter, maxDepth=args.max_depth, seed=42)
    pipe = Pipeline(stages=[assembler, gbt])

    print("[gold_rul_predict] training GBT...")
    model = pipe.fit(df)

    pred = model.transform(df)
    # 잔차 σ → 신뢰구간
    resid_stats = (
        pred.select((F.col("rul_capped") - F.col("prediction")).alias("r"))
        .select(F.stddev_samp("r").alias("sigma")).collect()
    )
    sigma = float(resid_stats[0]["sigma"]) if resid_stats and resid_stats[0]["sigma"] else 0.0
    z90 = 1.6449
    print(f"[gold_rul_predict] residual sigma = {sigma:.3f}, 90%CI half-width = {z90*sigma:.2f}")

    out = (
        pred
        .withColumn("model_version", F.lit(args.model_version))
        .withColumn("rul_pred", F.col("prediction"))
        .withColumn("rul_pred_lower", F.greatest(F.col("prediction") - F.lit(z90 * sigma), F.lit(0.0)))
        .withColumn("rul_pred_upper", F.col("prediction") + F.lit(z90 * sigma))
        .withColumn("rul_actual", F.col("rul_label"))
        .withColumn("abs_error", F.abs(F.col("prediction") - F.col("rul_label")))
    )
    # PHM08 score (단위 행 단위)
    d = F.col("rul_pred") - F.col("rul_actual")
    out = out.withColumn(
        "phm08_score",
        F.when(d >= 0, F.exp(d / F.lit(13.0)) - 1.0)
         .otherwise(F.exp(-d / F.lit(10.0)) - 1.0),
    )
    out = out.withColumn(
        "risk_tier",
        F.when(F.col("rul_pred") <= 10, F.lit("CRITICAL"))
         .when(F.col("rul_pred") <= 30, F.lit("HIGH"))
         .when(F.col("rul_pred") <= 80, F.lit("MED"))
         .otherwise(F.lit("LOW")),
    )
    out = (
        out
        .withColumn("predict_ts", F.current_timestamp())
        .withColumn("silver_snapshot_id", F.lit(silver_snapshot_id))
        .select(*TARGET_COLS)
    )

    out = out.localCheckpoint(eager=True)
    out.createOrReplaceTempView("_gold_pred")

    update_assigns = ", ".join(
        f"t.{c} = s.{c}" for c in TARGET_COLS
        if c not in ("model_version", "dataset_id", "unit_id", "cycle")
    )
    insert_cols = ", ".join(TARGET_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in TARGET_COLS)

    spark.sql(f"""
        MERGE INTO phm.gold.rul_prediction t
        USING (SELECT * FROM _gold_pred) s
          ON  t.model_version = s.model_version
          AND t.dataset_id    = s.dataset_id
          AND t.unit_id       = s.unit_id
          AND t.cycle         = s.cycle
        WHEN MATCHED THEN UPDATE SET {update_assigns}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    summary = spark.sql(f"""
        SELECT model_version,
               COUNT(*) AS n,
               ROUND(AVG(abs_error), 2) AS mae,
               ROUND(SQRT(AVG(POWER(abs_error, 2))), 2) AS rmse,
               ROUND(SUM(phm08_score), 2) AS phm08
          FROM phm.gold.rul_prediction
         WHERE model_version = '{args.model_version}'
         GROUP BY 1
    """).collect()
    for r in summary:
        print(f"[gold_rul_predict] {dict(r.asDict())}")

    # ─── model_metrics: dataset 별 한 행씩 기록 (모델 비교·drift 추적) ───
    silver_row_count = silver.count()
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW _model_metrics AS
        SELECT
            model_version,
            dataset_id,
            CURRENT_DATE                          AS eval_window_start,
            CURRENT_DATE                          AS eval_window_end,
            COUNT(*)                              AS sample_count,
            AVG(abs_error)                        AS mae,
            SQRT(AVG(POWER(abs_error, 2)))        AS rmse,
            SUM(phm08_score)                      AS phm08_score,
            AVG(rul_pred)                         AS pred_mean,
            STDDEV_SAMP(rul_pred)                 AS pred_std,
            PERCENTILE_APPROX(rul_pred, 0.5)      AS pred_p50,
            CAST({silver_snapshot_id} AS BIGINT)  AS silver_snapshot_id,
            CAST({silver_row_count}    AS BIGINT) AS silver_row_count,
            CURRENT_TIMESTAMP                     AS computed_ts
          FROM phm.gold.rul_prediction
         WHERE model_version = '{args.model_version}'
         GROUP BY model_version, dataset_id
    """)

    # 멱등키: (model_version, dataset_id, eval_window_end). 같은 날 재실행 시 UPDATE.
    spark.sql("""
        MERGE INTO phm.gold.model_metrics t
        USING (SELECT * FROM _model_metrics) s
          ON  t.model_version   = s.model_version
          AND t.dataset_id      = s.dataset_id
          AND t.eval_window_end = s.eval_window_end
        WHEN MATCHED THEN UPDATE SET
            sample_count       = s.sample_count,
            mae                = s.mae,
            rmse               = s.rmse,
            phm08_score        = s.phm08_score,
            pred_mean          = s.pred_mean,
            pred_std           = s.pred_std,
            pred_p50           = s.pred_p50,
            silver_snapshot_id = s.silver_snapshot_id,
            silver_row_count   = s.silver_row_count,
            computed_ts        = s.computed_ts,
            eval_window_start  = s.eval_window_start
        WHEN NOT MATCHED THEN INSERT *
    """)

    metrics_rows = spark.sql(f"""
        SELECT model_version, dataset_id, sample_count,
               ROUND(mae, 2)         AS mae,
               ROUND(rmse, 2)        AS rmse,
               ROUND(phm08_score, 2) AS phm08
          FROM phm.gold.model_metrics
         WHERE model_version = '{args.model_version}'
           AND eval_window_end = CURRENT_DATE
         ORDER BY dataset_id
    """).collect()
    for r in metrics_rows:
        print(f"[gold_rul_predict] metrics → {dict(r.asDict())}")


if __name__ == "__main__":
    main()
