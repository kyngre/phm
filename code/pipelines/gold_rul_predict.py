"""
Silver → Gold RUL 예측 (3-mode: train / predict / full).

학습/추론 분리 (B 프레이밍 — 운영 시스템 패턴):
  - train  : unit-level 80/20 holdout split → fit GBT → 모델 저장 (MinIO/local fs).
             *정직한* 일반화 성능을 model_metrics 에 'train' / 'holdout' 으로 분리 기록.
             (이전: 매 run 마다 fit + 자기 자신에게 추론 → 데이터 누수)
  - predict: 저장된 PipelineModel 로드 → silver 변경분만 추론 → MERGE rul_prediction.
             pipeline_state.last_snapshot_id 이후의 silver snapshot 만 처리해 비용 누적 누르기.
             영향 unit 의 모든 cycle 을 다시 추론 → silver.rul_label 갱신과 abs_error 동기화.
  - full   : cold start = train + predict (backward compat).

모델: Spark MLlib GBTRegressor (`gbt-v0`).
  feature: cycle, op_condition_cluster, 14 normalized sensors, rolling avg/std/trend, health_index.
  target : min(rul_label, --rul-cap)   ← PHM 관례 RUL cap.

신뢰구간: holdout 잔차 σ → [pred − 1.645σ, pred + 1.645σ] (정규분포 90% CI).
PHM08 score = sum(exp(d/13)−1 if d>0 else exp(−d/10)−1), d = pred − actual.

실행:
  # 콜드 스타트 / 백필
  spark-submit gold_rul_predict.py --mode full --model-version gbt-v0 --rul-cap 130
  # 주 1회 학습
  spark-submit gold_rul_predict.py --mode train --model-version gbt-v0 --rul-cap 130
  # 매시간 추론
  spark-submit gold_rul_predict.py --mode predict
"""
from __future__ import annotations

import argparse
import os
import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, SparkSession
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

SILVER_TABLE = "phm.silver.engine_health"
RUL_TABLE = "phm.gold.rul_prediction"
METRICS_TABLE = "phm.gold.model_metrics"
PIPELINE_STATE_TABLE = "phm.gold.pipeline_state"
PIPELINE_NAME = "gold_rul_predict"

MODEL_BASE_PATH = os.environ.get("MODEL_BASE_PATH", "s3a://warehouse/models")


# ─────────────────────── feature/target prep ───────────────────────

def prepare_features(silver_df: DataFrame, rul_cap: int) -> DataFrame:
    """rolling NaN(window 시작부) → 0 채움, 라벨 cap."""
    df = silver_df.na.fill(0.0, subset=["s_std_w5", "s_trend_w5"])
    df = df.withColumn(
        "rul_capped",
        F.least(F.col("rul_label").cast("double"), F.lit(float(rul_cap))),
    )
    return df


def unit_level_split(df: DataFrame, train_ratio: float = 0.8, seed: int = 42):
    """unit-level (dataset_id, unit_id) 80/20 split — 같은 엔진의 cycle 들이 같은
    split 으로 묶여 누수 방지. dataset 별 stratify (각 dataset 의 80% unit).

    Returns: (train_df, holdout_df, n_train_units, n_holdout_units).
    """
    units = df.select("dataset_id", "unit_id").distinct()
    units_with_split = units.withColumn(
        "_h", F.hash(F.col("dataset_id"), F.col("unit_id"), F.lit(seed))
    ).withColumn(
        # dataset 별 stratify: hash mod 100 으로 0..99 분포 → < 80 이면 train
        "_split",
        F.when((F.abs(F.col("_h")) % F.lit(100)) < F.lit(int(train_ratio * 100)),
               F.lit("train")).otherwise(F.lit("holdout")),
    ).select("dataset_id", "unit_id", "_split")

    joined = df.join(F.broadcast(units_with_split), ["dataset_id", "unit_id"])
    train_df = joined.where(F.col("_split") == "train").drop("_split")
    holdout_df = joined.where(F.col("_split") == "holdout").drop("_split")
    n_train = units_with_split.where(F.col("_split") == "train").count()
    n_holdout = units_with_split.where(F.col("_split") == "holdout").count()
    return train_df, holdout_df, n_train, n_holdout


def fit_pipeline(train_df: DataFrame, max_iter: int, max_depth: int,
                 seed: int = 42) -> PipelineModel:
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features",
                                handleInvalid="skip")
    gbt = GBTRegressor(featuresCol="features", labelCol="rul_capped",
                       maxIter=max_iter, maxDepth=max_depth, seed=seed)
    pipe = Pipeline(stages=[assembler, gbt])
    return pipe.fit(train_df)


def residual_sigma(model: PipelineModel, df: DataFrame) -> float:
    pred = model.transform(df)
    row = (
        pred.select((F.col("rul_capped") - F.col("prediction")).alias("r"))
            .select(F.stddev_samp("r").alias("sigma")).collect()
    )
    return float(row[0]["sigma"]) if row and row[0]["sigma"] else 0.0


# ─────────────────────── 추론 결과 → MERGE 형태 ───────────────────────

def predictions_to_target(pred_df: DataFrame, model_version: str, sigma: float,
                          silver_snapshot_id: int) -> DataFrame:
    z90 = 1.6449
    out = (
        pred_df
        .withColumn("model_version", F.lit(model_version))
        .withColumn("rul_pred", F.col("prediction"))
        .withColumn("rul_pred_lower",
                    F.greatest(F.col("prediction") - F.lit(z90 * sigma), F.lit(0.0)))
        .withColumn("rul_pred_upper", F.col("prediction") + F.lit(z90 * sigma))
        .withColumn("rul_actual", F.col("rul_label"))
        .withColumn("abs_error", F.abs(F.col("prediction") - F.col("rul_label")))
    )
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
    return (
        out.withColumn("predict_ts", F.current_timestamp())
           .withColumn("silver_snapshot_id", F.lit(silver_snapshot_id).cast("bigint"))
           .select(*TARGET_COLS)
    )


def merge_rul_prediction(spark: SparkSession, df: DataFrame) -> int:
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    df = df.localCheckpoint(eager=True)
    n = df.count()
    df.createOrReplaceTempView("_gold_pred")
    update_assigns = ", ".join(
        f"t.{c} = s.{c}" for c in TARGET_COLS
        if c not in ("model_version", "dataset_id", "unit_id", "cycle")
    )
    insert_cols = ", ".join(TARGET_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in TARGET_COLS)
    spark.sql(f"""
        MERGE INTO {RUL_TABLE} t
        USING (SELECT * FROM _gold_pred) s
          ON  t.model_version = s.model_version
          AND t.dataset_id    = s.dataset_id
          AND t.unit_id       = s.unit_id
          AND t.cycle         = s.cycle
        WHEN MATCHED THEN UPDATE SET {update_assigns}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
    return n


# ─────────────────────── 평가 지표 → model_metrics ───────────────────────

def compute_metrics_row(df: DataFrame, model_version: str, eval_split: str,
                        silver_snapshot_id: int, silver_row_count: int):
    """단일 (model_version, dataset, eval_window_end, eval_split) 행 dict 리스트.
    df 는 'prediction'/'rul_capped'/'rul_label' 컬럼 포함."""
    summary = (
        df.withColumn("ae", F.abs(F.col("prediction") - F.col("rul_capped")))
          .withColumn("d", F.col("prediction") - F.col("rul_capped"))
          .withColumn(
              "phm08",
              F.when(F.col("d") >= 0, F.exp(F.col("d") / F.lit(13.0)) - 1.0)
               .otherwise(F.exp(-F.col("d") / F.lit(10.0)) - 1.0),
          )
          .groupBy("dataset_id")
          .agg(
              F.count(F.lit(1)).alias("sample_count"),
              F.avg("ae").alias("mae"),
              F.sqrt(F.avg(F.pow("ae", 2))).alias("rmse"),
              F.sum("phm08").alias("phm08_score"),
              F.avg("prediction").alias("pred_mean"),
              F.stddev_samp("prediction").alias("pred_std"),
              F.percentile_approx("prediction", F.lit(0.5)).alias("pred_p50"),
          )
          .collect()
    )
    rows = []
    for r in summary:
        rows.append((
            model_version,
            r["dataset_id"],
            None,  # eval_window_start
            None,  # eval_window_end (CURRENT_DATE 로 SQL 에서 채울 수도 있음 — 단순하게 None)
            eval_split,
            int(r["sample_count"]),
            float(r["mae"]) if r["mae"] is not None else None,
            float(r["rmse"]) if r["rmse"] is not None else None,
            float(r["phm08_score"]) if r["phm08_score"] is not None else None,
            float(r["pred_mean"]) if r["pred_mean"] is not None else None,
            float(r["pred_std"]) if r["pred_std"] is not None else None,
            float(r["pred_p50"]) if r["pred_p50"] is not None else None,
            int(silver_snapshot_id),
            int(silver_row_count),
        ))
    return rows


def merge_model_metrics(spark: SparkSession, rows: list):
    if not rows:
        return
    schema = (
        "model_version STRING, dataset_id STRING, "
        "eval_window_start DATE, eval_window_end DATE, "
        "eval_split STRING, "
        "sample_count BIGINT, mae DOUBLE, rmse DOUBLE, phm08_score DOUBLE, "
        "pred_mean DOUBLE, pred_std DOUBLE, pred_p50 DOUBLE, "
        "silver_snapshot_id BIGINT, silver_row_count BIGINT"
    )
    df = (
        spark.createDataFrame(rows, schema=schema)
             .withColumn("eval_window_start", F.coalesce(F.col("eval_window_start"),
                                                         F.current_date()))
             .withColumn("eval_window_end", F.coalesce(F.col("eval_window_end"),
                                                       F.current_date()))
             .withColumn("computed_ts", F.current_timestamp())
    )
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    df = df.localCheckpoint(eager=True)
    df.createOrReplaceTempView("_metrics_batch")
    spark.sql(f"""
        MERGE INTO {METRICS_TABLE} t
        USING (SELECT * FROM _metrics_batch) s
          ON  t.model_version    = s.model_version
          AND t.dataset_id       = s.dataset_id
          AND t.eval_window_end  = s.eval_window_end
          AND t.eval_split       = s.eval_split
        WHEN MATCHED THEN UPDATE SET
            sample_count = s.sample_count,
            mae          = s.mae,
            rmse         = s.rmse,
            phm08_score  = s.phm08_score,
            pred_mean    = s.pred_mean,
            pred_std     = s.pred_std,
            pred_p50     = s.pred_p50,
            silver_snapshot_id = s.silver_snapshot_id,
            silver_row_count   = s.silver_row_count,
            computed_ts        = s.computed_ts,
            eval_window_start  = s.eval_window_start
        WHEN NOT MATCHED THEN INSERT *
    """)


# ─────────────────────── pipeline_state I/O ───────────────────────

def current_silver_snapshot_id(spark: SparkSession) -> int:
    spark.sql(f"REFRESH TABLE {SILVER_TABLE}")
    row = spark.sql(
        f"SELECT snapshot_id FROM {SILVER_TABLE}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return int(row[0]["snapshot_id"]) if row else 0


def read_pipeline_state(spark: SparkSession):
    spark.sql(f"REFRESH TABLE {PIPELINE_STATE_TABLE}")
    rows = spark.sql(f"""
        SELECT * FROM {PIPELINE_STATE_TABLE}
         WHERE pipeline_name = '{PIPELINE_NAME}'
    """).collect()
    return rows[0] if rows else None


def write_pipeline_state(spark: SparkSession, *,
                         last_snapshot_id: int, rows_processed: int,
                         active_model_version: str | None,
                         active_model_path: str | None,
                         active_model_trained_ts: bool = False):
    """active_model_trained_ts=True 면 현재 시각으로 갱신 (train 직후), False 면 기존 값 유지."""
    existing = read_pipeline_state(spark)
    keep_trained_ts = existing["active_model_trained_ts"] if existing else None

    def _sql_lit(v):
        return "NULL" if v is None else f"'{v}'"

    trained_ts_expr = (
        "CURRENT_TIMESTAMP" if active_model_trained_ts
        else (f"CAST('{keep_trained_ts}' AS TIMESTAMP)" if keep_trained_ts is not None
              else "CAST(NULL AS TIMESTAMP)")
    )

    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW _gpstate AS
        SELECT
            CAST('{PIPELINE_NAME}' AS STRING)     AS pipeline_name,
            CAST({last_snapshot_id} AS BIGINT)    AS last_snapshot_id,
            CURRENT_TIMESTAMP                     AS last_run_ts,
            CAST({rows_processed} AS BIGINT)      AS rows_processed,
            CAST({_sql_lit(active_model_version)} AS STRING) AS active_model_version,
            CAST({_sql_lit(active_model_path)} AS STRING)    AS active_model_path,
            {trained_ts_expr}                                 AS active_model_trained_ts
    """)
    spark.sql(f"""
        MERGE INTO {PIPELINE_STATE_TABLE} t
        USING (SELECT * FROM _gpstate) s
          ON t.pipeline_name = s.pipeline_name
        WHEN MATCHED THEN UPDATE SET
            last_snapshot_id        = s.last_snapshot_id,
            last_run_ts             = s.last_run_ts,
            rows_processed          = s.rows_processed,
            active_model_version    = s.active_model_version,
            active_model_path       = s.active_model_path,
            active_model_trained_ts = s.active_model_trained_ts
        WHEN NOT MATCHED THEN INSERT *
    """)


# ─────────────────────── 모드별 실행 ───────────────────────

def run_train(spark: SparkSession, args) -> tuple[str, float]:
    """unit-level 80/20 → fit → save → metrics. Returns (model_path, holdout_sigma)."""
    silver = spark.table(SILVER_TABLE)
    df = prepare_features(silver, args.rul_cap)
    train_df, holdout_df, n_train, n_holdout = unit_level_split(df, seed=42)
    print(f"[gold_rul_predict.train] split: train_units={n_train}, holdout_units={n_holdout}")

    print("[gold_rul_predict.train] fitting GBT on train split (no leakage)...")
    model = fit_pipeline(train_df, max_iter=args.max_iter, max_depth=args.max_depth)

    # holdout 기반 잔차 σ → 신뢰구간 (정직한 추정)
    holdout_sigma = residual_sigma(model, holdout_df)
    print(f"[gold_rul_predict.train] holdout sigma = {holdout_sigma:.3f}")

    # 저장 — model_version + epoch 단위
    ts = int(time.time())
    model_path = f"{MODEL_BASE_PATH}/{args.model_version}/{ts}"
    print(f"[gold_rul_predict.train] saving model to {model_path}")
    model.write().overwrite().save(model_path)

    # metrics: train + holdout 모두 기록
    silver_snap = current_silver_snapshot_id(spark)
    silver_n = silver.count()

    train_pred = model.transform(train_df)
    holdout_pred = model.transform(holdout_df)

    rows = []
    rows += compute_metrics_row(train_pred, args.model_version, "train",
                                silver_snap, silver_n)
    rows += compute_metrics_row(holdout_pred, args.model_version, "holdout",
                                silver_snap, silver_n)
    merge_model_metrics(spark, rows)
    print(f"[gold_rul_predict.train] wrote {len(rows)} model_metrics rows")

    # pipeline_state: 모델 활성화 — last_snapshot_id 는 유지 (predict 가 갱신)
    existing = read_pipeline_state(spark)
    last_snap = int(existing["last_snapshot_id"]) if existing else 0
    rows_proc = int(existing["rows_processed"]) if existing else 0
    write_pipeline_state(
        spark,
        last_snapshot_id=last_snap, rows_processed=rows_proc,
        active_model_version=args.model_version,
        active_model_path=model_path,
        active_model_trained_ts=True,
    )
    return model_path, holdout_sigma


def run_predict(spark: SparkSession, args) -> int:
    """active 모델 로드 → silver 변경분 + 영향 unit 의 모든 cycle 추론 → MERGE."""
    state = read_pipeline_state(spark)
    if state is None or not state["active_model_path"]:
        raise SystemExit(
            f"[gold_rul_predict.predict] active 모델 없음 — --mode train 또는 full 선행 필요."
        )
    model_path = state["active_model_path"]
    model_version = state["active_model_version"]
    last_snap = int(state["last_snapshot_id"]) if state["last_snapshot_id"] else 0
    silver_snap = current_silver_snapshot_id(spark)
    print(f"[gold_rul_predict.predict] active model={model_version} path={model_path}")

    if silver_snap == 0:
        print("[gold_rul_predict.predict] silver 비어 있음 — skip")
        return 0
    if silver_snap == last_snap:
        print(f"[gold_rul_predict.predict] no new silver snapshots since {last_snap} — skip")
        write_pipeline_state(
            spark, last_snapshot_id=silver_snap, rows_processed=0,
            active_model_version=model_version, active_model_path=model_path,
        )
        return 0

    silver = spark.table(SILVER_TABLE)
    if last_snap == 0:
        new_rows = silver
        print("[gold_rul_predict.predict] cold start — predicting on entire silver")
    else:
        new_rows = (
            spark.read.format("iceberg")
                 .option("start-snapshot-id", last_snap)
                 .option("end-snapshot-id", silver_snap)
                 .load(SILVER_TABLE)
        )
        n_new = new_rows.count()
        print(f"[gold_rul_predict.predict] new silver rows since snapshot {last_snap} = {n_new}")
        if n_new == 0:
            write_pipeline_state(
                spark, last_snapshot_id=silver_snap, rows_processed=0,
                active_model_version=model_version, active_model_path=model_path,
            )
            return 0

    # 영향 unit 의 모든 cycle — silver_transform.run_incremental 가 rul_label 을
    # 재계산했을 수 있으므로 abs_error 동기화 위해 전체 history 재추론.
    affected = new_rows.select("dataset_id", "unit_id").distinct()
    target = silver.join(F.broadcast(affected), ["dataset_id", "unit_id"], "inner")
    target = prepare_features(target, args.rul_cap)

    print(f"[gold_rul_predict.predict] loading PipelineModel from {model_path}")
    model = PipelineModel.load(model_path)
    pred = model.transform(target)

    # 신뢰구간용 σ — model_metrics 의 직전 holdout MAE 에서 가져옴 (정직한 σ)
    sigma_row = spark.sql(f"""
        SELECT rmse FROM {METRICS_TABLE}
         WHERE model_version='{model_version}' AND eval_split='holdout'
         ORDER BY computed_ts DESC LIMIT 1
    """).collect()
    sigma = float(sigma_row[0]["rmse"]) if sigma_row and sigma_row[0]["rmse"] else 0.0

    out = predictions_to_target(pred, model_version, sigma, silver_snap)
    n = merge_rul_prediction(spark, out)

    write_pipeline_state(
        spark, last_snapshot_id=silver_snap, rows_processed=n,
        active_model_version=model_version, active_model_path=model_path,
    )
    print(f"[gold_rul_predict.predict] merged {n} rul_prediction rows, "
          f"advanced to silver snapshot {silver_snap}")
    return n


def run_full(spark: SparkSession, args) -> None:
    """cold start: train + predict. backward compat / full_ingest.sh 용."""
    run_train(spark, args)
    run_predict(spark, args)


# ─────────────────────── entrypoint ───────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["train", "predict", "full"], default="full",
                    help="train: 학습+저장+metrics, predict: 추론만, full: 둘 다")
    ap.add_argument("--model-version", default="gbt-v0")
    ap.add_argument("--rul-cap", type=int, default=130,
                    help="학습 라벨 상한 (PHM 관례)")
    ap.add_argument("--max-iter", type=int, default=50)
    ap.add_argument("--max-depth", type=int, default=5)
    args = ap.parse_args()

    spark = SparkSession.builder.appName(f"gold_rul_predict_{args.mode}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

    if args.mode == "train":
        run_train(spark, args)
    elif args.mode == "predict":
        run_predict(spark, args)
    elif args.mode == "full":
        run_full(spark, args)


if __name__ == "__main__":
    main()
