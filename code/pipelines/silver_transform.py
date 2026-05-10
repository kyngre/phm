"""
Bronze → Silver MERGE INTO 변환 (3-mode: fit-stats / incremental / full).

처리 단계 (full / incremental 공통):
  1) op_setting 1~3 → KMeans(k=6) cluster (FD002/004 의 6 condition)
     · full      : 새로 fit
     · incremental: feat_stats 의 centroids 를 이용해 argmin 으로 assign (재학습 X)
  2) (dataset_id, cluster_id) 별 z-score 정규화
     · full      : 현재 배치에서 mean/std 직접 계산
     · incremental: feat_stats 의 mean/std 를 broadcast join 하여 적용
  3) (dataset_id, unit_id) 시간순 5-cycle rolling (avg/std/trend)
  4) Health Index = 1 − min(|s_avg_w5|, 3) / 3
  5) rul_label = max(cycle) per engine - cycle  (train 가정)
  6) MERGE INTO phm.silver.engine_health  ON (dataset_id, unit_id, cycle)

모드 차이:
  --mode fit-stats   : KMeans + 통계만 phm.silver.feat_stats 에 저장. silver 행 변경 X.
  --mode incremental : phm.silver.pipeline_state.last_snapshot_id 이후 bronze 변경분만 처리.
                       영향받은 (dataset, unit) 의 모든 cycle 을 다시 변환 (rolling/rul_label 정확성).
                       feat_stats 가 없으면 즉시 종료 (fit-stats 선행 필요).
  --mode full        : 전량 fit + transform + merge silver. cold start / backfill 용.
                       feat_stats / pipeline_state 도 함께 갱신.

실행 예시:
  # 콜드 스타트 / 백필
  spark-submit silver_transform.py --mode full
  # 주 1회 또는 drift 임계 초과 시
  spark-submit silver_transform.py --mode fit-stats --stats-version fs-2026-05-10
  # 매시간
  spark-submit silver_transform.py --mode incremental
"""
from __future__ import annotations

import argparse
import time

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame, SparkSession, Window
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

BRONZE_TABLE = "phm.bronze.engine_sensor_raw"
SILVER_TABLE = "phm.silver.engine_health"
FEAT_STATS_TABLE = "phm.silver.feat_stats"
PIPELINE_STATE_TABLE = "phm.silver.pipeline_state"
PIPELINE_NAME = "silver_incremental"

# feat_stats 컬럼 (DDL 03b 와 동기화)
FEAT_STATS_COLS = (
    ["stats_version", "fit_snapshot_id",
     "cluster_id", "center_op_setting_1", "center_op_setting_2", "center_op_setting_3",
     "dataset_id", "n_samples"]
    + [f"sensor_{s}_{m}" for s in KEEP_SENSORS for m in ("mean", "std")]
    + ["fit_ts"]
)


# ─────────────────────── 변환 헬퍼 ───────────────────────

def dedup_bronze(bronze_df: DataFrame) -> DataFrame:
    """Bronze 의 (source_file, line_no) 중복 제거 — 시스템 레벨 멱등성 보장.

    Bronze 는 append-only (write amplification 회피 + 운영 시스템 패턴). 다만 Spark
    Structured Streaming foreachBatch 가 at-least-once 라 batch 실패→재시도 시 같은
    row 가 두 번 적재될 수 있다. Silver 진입 시 행 단위로 dedup 해 정합성 확보.

    동률 시 ingest_ts 가 작은 행 선택 (first-write-wins) — 이전 Bronze MERGE 의
    'WHEN NOT MATCHED THEN INSERT' 시맨틱과 일치. 데이터는 동일하므로 어느 쪽을
    선택해도 무관하지만 결정성 위해 명시.
    """
    w = (
        Window.partitionBy("source_file", "line_no")
        .orderBy(F.col("ingest_ts").asc())
    )
    return (
        bronze_df.withColumn("_rn", F.row_number().over(w))
                 .where(F.col("_rn") == 1)
                 .drop("_rn")
    )


def stable_cluster_ids(model_centers, prediction_col_df, prediction_col):
    """KMeans 클러스터 ID swap 방지 — center 의 (op1, op2, op3) 사전식 정렬로 0..k-1 재할당."""
    mapping_expr = F.create_map([F.lit(x) for kv in _stable_remap(model_centers).items() for x in kv])
    return prediction_col_df.withColumn(
        prediction_col, mapping_expr[F.col(prediction_col)]
    )


def _stable_remap(model_centers) -> dict[int, int]:
    """원본 cluster id → stable id (op1,op2,op3 사전순 0..k-1)."""
    order = sorted(range(len(model_centers)),
                   key=lambda i: (float(model_centers[i][0]),
                                  float(model_centers[i][1]),
                                  float(model_centers[i][2])))
    return {old: new for new, old in enumerate(order)}


def _sorted_centers(model_centers) -> list[tuple[float, float, float]]:
    """stable id 0..k-1 순으로 정렬된 (op1, op2, op3) 리스트."""
    order = sorted(range(len(model_centers)),
                   key=lambda i: (float(model_centers[i][0]),
                                  float(model_centers[i][1]),
                                  float(model_centers[i][2])))
    return [(float(model_centers[i][0]),
             float(model_centers[i][1]),
             float(model_centers[i][2])) for i in order]


def cluster_op_conditions(bronze_df, k: int = 6, seed: int = 42):
    """KMeans fit + stable_cluster_ids 적용. (df, sorted_centers) 반환.

    sorted_centers[i] 는 cluster_id=i 의 (op1, op2, op3) — incremental 모드의
    assign_cluster_from_centroids 입력으로 그대로 사용 가능.
    """
    assembler = VectorAssembler(
        inputCols=["op_setting_1", "op_setting_2", "op_setting_3"],
        outputCol="op_vec",
    )
    bronze_v = assembler.transform(bronze_df)
    kmeans = KMeans(
        k=k, seed=seed,
        featuresCol="op_vec", predictionCol="op_condition_cluster",
    )
    model = kmeans.fit(bronze_v)
    df = model.transform(bronze_v).drop("op_vec")
    df = stable_cluster_ids(model.clusterCenters(), df, "op_condition_cluster")
    return df, _sorted_centers(model.clusterCenters())


def assign_cluster_from_centroids(df: DataFrame, sorted_centers) -> DataFrame:
    """fit 없이 기존 centroids 로 op_condition_cluster 부여 — 증분 모드용.

    sorted_centers: [(c1, c2, c3), ...] (cluster_id 순; stable_cluster_ids 출력).
    각 행에 대해 argmin_{i} ||(op1,op2,op3) - center_i||^2.
    동률 시 작은 cluster_id 우선.
    """
    k = len(sorted_centers)
    # 각 cluster 까지의 squared distance 컬럼
    out = df
    for i, c in enumerate(sorted_centers):
        c1, c2, c3 = float(c[0]), float(c[1]), float(c[2])
        out = out.withColumn(
            f"_d{i}",
            (F.col("op_setting_1") - F.lit(c1)) ** 2
            + (F.col("op_setting_2") - F.lit(c2)) ** 2
            + (F.col("op_setting_3") - F.lit(c3)) ** 2,
        )
    min_d = F.least(*[F.col(f"_d{i}") for i in range(k)])
    # cluster_id = 첫 번째 (가장 작은 i) 로 d_i == min_d 인 i
    cluster_expr = F.when(F.col("_d0") == min_d, F.lit(0))
    for i in range(1, k):
        cluster_expr = cluster_expr.when(F.col(f"_d{i}") == min_d, F.lit(i))
    out = out.withColumn("op_condition_cluster", cluster_expr)
    return out.drop(*[f"_d{i}" for i in range(k)])


def add_rolling_features(df, sensor_cols, window: int = 5):
    """engine timeline 5-cycle rolling: s_avg_w5 / s_std_w5 / s_trend_w5."""
    engine_w = (
        Window.partitionBy("dataset_id", "unit_id")
        .orderBy("cycle")
        .rowsBetween(-(window - 1), 0)
    )
    norm_cols = [F.col(c) for c in sensor_cols]
    s_row_mean = sum(norm_cols) / F.lit(len(norm_cols))
    df = df.withColumn("_s_row_mean", s_row_mean)
    df = df.withColumn("s_avg_w5", F.avg("_s_row_mean").over(engine_w))
    df = df.withColumn("s_std_w5", F.stddev_samp("_s_row_mean").over(engine_w))
    first_in_w = F.first("_s_row_mean").over(engine_w)
    df = df.withColumn(
        "s_trend_w5",
        (F.col("_s_row_mean") - first_in_w) / F.lit(max(window - 1, 1)),
    )
    return df


def add_health_index(df, src_col: str = "s_avg_w5"):
    return df.withColumn(
        "health_index",
        F.lit(1.0) - F.least(F.abs(F.col(src_col)), F.lit(3.0)) / F.lit(3.0),
    )


def add_rul_label(df):
    """train 가정: 마지막 cycle = 고장. rul_label = max(cycle) - cycle (per engine)."""
    engine_all_w = Window.partitionBy("dataset_id", "unit_id")
    return df.withColumn(
        "rul_label",
        F.max("cycle").over(engine_all_w) - F.col("cycle"),
    )


def apply_zscore_inline(df: DataFrame) -> DataFrame:
    """현재 배치 안에서 (dataset_id, op_condition_cluster) window mean/std 로 z-score.
    full 모드에서 사용 (자기 자신의 통계로 정규화)."""
    cluster_w = Window.partitionBy("dataset_id", "op_condition_cluster")
    for s in KEEP_SENSORS:
        col = f"sensor_{s}"
        m = F.avg(col).over(cluster_w)
        sd = F.stddev_pop(col).over(cluster_w)
        df = df.withColumn(
            f"s{s}_norm",
            F.when(sd > 0, (F.col(col) - m) / sd).otherwise(F.lit(0.0)),
        )
    return df


def apply_zscore_from_stats(df: DataFrame, stats_df: DataFrame) -> DataFrame:
    """feat_stats 의 mean/std 를 broadcast join 으로 적용. incremental 모드용.

    stats_df: phm.silver.feat_stats (이미 stats_version 으로 필터됨).
    """
    join_cols = ["dataset_id", "cluster_id"]
    keep_stat_cols = [f"sensor_{s}_{m}" for s in KEEP_SENSORS for m in ("mean", "std")]
    stats_slim = (
        stats_df.select("dataset_id", F.col("cluster_id").alias("cluster_id"),
                        *keep_stat_cols)
    )
    out = df.withColumnRenamed("op_condition_cluster", "cluster_id") \
            .join(F.broadcast(stats_slim), on=join_cols, how="left") \
            .withColumnRenamed("cluster_id", "op_condition_cluster")
    for s in KEEP_SENSORS:
        col = f"sensor_{s}"
        m = F.col(f"sensor_{s}_mean")
        sd = F.col(f"sensor_{s}_std")
        out = out.withColumn(
            f"s{s}_norm",
            F.when(sd > 0, (F.col(col) - m) / sd).otherwise(F.lit(0.0)),
        )
    drop_cols = [f"sensor_{s}_{m}" for s in KEEP_SENSORS for m in ("mean", "std")]
    return out.drop(*drop_cols)


# ─────────────────────── feat_stats 계산/저장 ───────────────────────

def compute_feat_stats(clustered_df, sorted_centers, stats_version: str,
                       fit_snapshot_id: int) -> DataFrame:
    """(dataset_id, cluster_id) 별 sensor mean/std + centroids 를 long → wide 로 묶어 반환.

    clustered_df: op_condition_cluster 컬럼이 이미 부여된 df (KMeans 또는 argmin 결과).
    """
    spark = clustered_df.sparkSession

    agg_exprs = [F.count(F.lit(1)).alias("n_samples")]
    for s in KEEP_SENSORS:
        col = f"sensor_{s}"
        agg_exprs += [
            F.avg(col).alias(f"sensor_{s}_mean"),
            F.stddev_pop(col).alias(f"sensor_{s}_std"),
        ]
    grouped = (
        clustered_df.groupBy("dataset_id", "op_condition_cluster")
                    .agg(*agg_exprs)
                    .withColumnRenamed("op_condition_cluster", "cluster_id")
    )

    # centroids 를 dataframe 으로 만들어 join (cluster_id 별 1행)
    centroid_rows = [
        (i, float(c[0]), float(c[1]), float(c[2]))
        for i, c in enumerate(sorted_centers)
    ]
    centroid_df = spark.createDataFrame(
        centroid_rows,
        ["cluster_id", "center_op_setting_1", "center_op_setting_2", "center_op_setting_3"],
    )

    out = (
        grouped.join(centroid_df, on="cluster_id", how="inner")
               .withColumn("stats_version", F.lit(stats_version))
               .withColumn("fit_snapshot_id", F.lit(fit_snapshot_id).cast("bigint"))
               .withColumn("fit_ts", F.current_timestamp())
               .select(*FEAT_STATS_COLS)
    )
    return out


def merge_feat_stats(spark: SparkSession, stats_df: DataFrame):
    # MERGE source 가 current_timestamp() 를 포함하면 Spark 가 non-deterministic 판정.
    # localCheckpoint 로 lineage 끊고 materialize.
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    stats_df = stats_df.localCheckpoint(eager=True)
    stats_df.createOrReplaceTempView("_feat_stats_batch")
    update_assigns = ", ".join(
        f"t.{c} = s.{c}" for c in FEAT_STATS_COLS
        if c not in ("stats_version", "dataset_id", "cluster_id")
    )
    insert_cols = ", ".join(FEAT_STATS_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in FEAT_STATS_COLS)
    spark.sql(f"""
        MERGE INTO {FEAT_STATS_TABLE} t
        USING (SELECT * FROM _feat_stats_batch) s
          ON  t.stats_version = s.stats_version
          AND t.dataset_id    = s.dataset_id
          AND t.cluster_id    = s.cluster_id
        WHEN MATCHED THEN UPDATE SET {update_assigns}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)


def load_feat_stats(spark: SparkSession, stats_version: str | None):
    """stats_version 이 None 이면 가장 최신 fit_ts 의 stats_version 을 자동 선택.

    Returns: (stats_df_filtered, stats_version_used, sorted_centers).
    feat_stats 가 비어 있으면 (None, None, None).
    """
    table = spark.table(FEAT_STATS_TABLE)
    if table.limit(1).count() == 0:
        return None, None, None

    if stats_version is None:
        latest = (
            table.groupBy("stats_version")
                 .agg(F.max("fit_ts").alias("ts"))
                 .orderBy(F.col("ts").desc())
                 .limit(1)
                 .collect()
        )
        stats_version = latest[0]["stats_version"]

    stats_df = table.where(F.col("stats_version") == stats_version)
    if stats_df.limit(1).count() == 0:
        return None, None, None

    # centroids 추출 (cluster_id 0..k-1 순 정렬)
    centroids_rows = (
        stats_df.select("cluster_id",
                        "center_op_setting_1", "center_op_setting_2", "center_op_setting_3")
                .distinct()
                .orderBy("cluster_id")
                .collect()
    )
    sorted_centers = [
        (r["center_op_setting_1"], r["center_op_setting_2"], r["center_op_setting_3"])
        for r in centroids_rows
    ]
    return stats_df, stats_version, sorted_centers


# ─────────────────────── pipeline_state I/O ───────────────────────

def read_last_snapshot(spark: SparkSession) -> int | None:
    # 같은 세션 내에서 직전 write_pipeline_state 의 commit 이 즉시 보이도록 메타 캐시 무효화.
    spark.sql(f"REFRESH TABLE {PIPELINE_STATE_TABLE}")
    rows = spark.sql(f"""
        SELECT last_snapshot_id FROM {PIPELINE_STATE_TABLE}
         WHERE pipeline_name = '{PIPELINE_NAME}'
    """).collect()
    return int(rows[0]["last_snapshot_id"]) if rows else None


def write_pipeline_state(spark: SparkSession, last_snapshot_id: int,
                         rows_processed: int, active_stats_version: str | None):
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW _pstate AS
        SELECT
            CAST('{PIPELINE_NAME}' AS STRING)                                AS pipeline_name,
            CAST({last_snapshot_id} AS BIGINT)                               AS last_snapshot_id,
            CURRENT_TIMESTAMP                                                AS last_run_ts,
            CAST({rows_processed} AS BIGINT)                                 AS rows_processed,
            CAST({'NULL' if active_stats_version is None else f"'{active_stats_version}'"} AS STRING)
                                                                             AS active_stats_version
    """)
    spark.sql(f"""
        MERGE INTO {PIPELINE_STATE_TABLE} t
        USING (SELECT * FROM _pstate) s
          ON t.pipeline_name = s.pipeline_name
        WHEN MATCHED THEN UPDATE SET
            last_snapshot_id     = s.last_snapshot_id,
            last_run_ts          = s.last_run_ts,
            rows_processed       = s.rows_processed,
            active_stats_version = s.active_stats_version
        WHEN NOT MATCHED THEN INSERT *
    """)


def current_bronze_snapshot_id(spark: SparkSession) -> int:
    # Iceberg metadata 캐시 회피 — 같은 세션 내에서 직전 commit 이 즉시 보이도록.
    spark.sql(f"REFRESH TABLE {BRONZE_TABLE}")
    row = spark.sql(
        f"SELECT snapshot_id FROM {BRONZE_TABLE}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return int(row[0]["snapshot_id"]) if row else 0


# ─────────────────────── Silver MERGE ───────────────────────

def finalize_and_merge_silver(df: DataFrame, silver_version: str) -> int:
    """공통 마무리: silver_ts/version 부여 → 컬럼 정렬 → checkpoint → MERGE.
    반환: 처리 행 수."""
    spark = df.sparkSession
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    df = (
        df.withColumn("silver_ts", F.current_timestamp())
          .withColumn("silver_version", F.lit(silver_version))
          .select(*TARGET_COLS)
          .localCheckpoint(eager=True)
    )
    n = df.count()
    df.createOrReplaceTempView("_silver_batch")

    update_assigns = ", ".join(f"t.{c} = s.{c}"
                               for c in TARGET_COLS
                               if c not in ("dataset_id", "unit_id", "cycle"))
    insert_cols = ", ".join(TARGET_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in TARGET_COLS)
    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} t
        USING (SELECT * FROM _silver_batch) s
          ON  t.dataset_id = s.dataset_id
          AND t.unit_id    = s.unit_id
          AND t.cycle      = s.cycle
        WHEN MATCHED THEN UPDATE SET {update_assigns}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
    return n


# ─────────────────────── 모드별 실행 ───────────────────────

def run_full(spark: SparkSession, args) -> None:
    """전량 fit + transform + MERGE silver. cold start / backfill 용.
    feat_stats / pipeline_state 도 함께 갱신해 이후 incremental 진입 가능."""
    bronze = dedup_bronze(spark.table(BRONZE_TABLE))
    bronze_snap = current_bronze_snapshot_id(spark)
    stats_version = args.stats_version or f"fs-{int(time.time())}"

    # 1) cluster (fit)
    clustered, sorted_centers = cluster_op_conditions(bronze, k=args.kmeans_k, seed=42)

    # 2) z-score (현재 배치 통계로 — fit 결과와 동치)
    df = apply_zscore_inline(clustered)

    # 3-5) rolling / HI / rul_label
    df = add_rolling_features(df, [f"s{s}_norm" for s in KEEP_SENSORS],
                              window=args.rolling_window)
    df = add_health_index(df)
    df = add_rul_label(df)

    # 6) silver MERGE
    n = finalize_and_merge_silver(df, args.silver_version)
    print(f"[silver_transform.full] silver merged rows = {n}")

    # 7) feat_stats / pipeline_state 갱신 — 이후 incremental 가 사용
    #    KMeans 가 부여한 cluster 를 그대로 사용해 silver 와 통계 일치 보장
    stats_df = compute_feat_stats(clustered, sorted_centers,
                                  stats_version=stats_version,
                                  fit_snapshot_id=bronze_snap)
    merge_feat_stats(spark, stats_df)
    print(f"[silver_transform.full] feat_stats version = {stats_version}")

    write_pipeline_state(spark, last_snapshot_id=bronze_snap,
                         rows_processed=n, active_stats_version=stats_version)
    print(f"[silver_transform.full] pipeline_state advanced to bronze snapshot {bronze_snap}")

    rows = spark.table(SILVER_TABLE).count()
    print(f"[silver_transform.full] silver row count = {rows}")


def run_fit_stats(spark: SparkSession, args) -> None:
    """KMeans + 통계만 갱신. silver 행은 변경 X."""
    bronze = dedup_bronze(spark.table(BRONZE_TABLE))
    bronze_snap = current_bronze_snapshot_id(spark)
    stats_version = args.stats_version or f"fs-{int(time.time())}"

    clustered, sorted_centers = cluster_op_conditions(bronze, k=args.kmeans_k, seed=42)
    stats_df = compute_feat_stats(clustered, sorted_centers,
                                  stats_version=stats_version,
                                  fit_snapshot_id=bronze_snap)
    merge_feat_stats(spark, stats_df)
    print(f"[silver_transform.fit-stats] wrote feat_stats version={stats_version} "
          f"(bronze snapshot={bronze_snap}, rows used={clustered.count()})")


def run_incremental(spark: SparkSession, args) -> None:
    """phm.silver.pipeline_state.last_snapshot_id 이후 bronze 변경분만 처리.

    rolling window + rul_label 정확성 위해, 변경분에 등장한 (dataset, unit) 의
    *모든 cycle* 을 다시 변환. 영향 unit 수가 적을수록 비용 작음.
    """
    stats_df, stats_version, sorted_centers = load_feat_stats(spark, args.stats_version)
    if stats_df is None:
        raise SystemExit(
            f"[silver_transform.incremental] {FEAT_STATS_TABLE} 가 비어 있음. "
            f"먼저 --mode fit-stats 또는 --mode full 로 통계를 채우세요."
        )

    last_snap = read_last_snapshot(spark)
    bronze_snap = current_bronze_snapshot_id(spark)
    if bronze_snap == 0:
        print("[silver_transform.incremental] bronze 가 비어 있음 — skip")
        return
    if last_snap == bronze_snap:
        print(f"[silver_transform.incremental] no new bronze snapshots since {last_snap} — skip")
        # 그래도 stats_version 만 바뀌었으면 갱신
        write_pipeline_state(spark, last_snapshot_id=bronze_snap, rows_processed=0,
                             active_stats_version=stats_version)
        return

    bronze = spark.table(BRONZE_TABLE)
    if last_snap is None:
        # cold start: pipeline_state 없음 → 모두 처리
        new_rows = bronze
        print(f"[silver_transform.incremental] cold start (no pipeline_state) — "
              f"processing entire bronze, then advancing to snapshot {bronze_snap}")
    else:
        # last_snap 이후 추가된 행만 (Iceberg incremental scan).
        # append-only Bronze 라 모든 새 snapshot 이 INSERT — incremental scan 안전.
        new_rows = (
            spark.read.format("iceberg")
                 .option("start-snapshot-id", last_snap)
                 .option("end-snapshot-id", bronze_snap)
                 .load(BRONZE_TABLE)
        )
        n_new = new_rows.count()
        print(f"[silver_transform.incremental] new bronze rows since snapshot "
              f"{last_snap} = {n_new}")
        if n_new == 0:
            write_pipeline_state(spark, last_snapshot_id=bronze_snap, rows_processed=0,
                                 active_stats_version=stats_version)
            return

    # rolling/rul 정확성: 영향받은 (dataset, unit) 의 모든 cycle 을 다시 가져옴.
    # join 후 dedup — Bronze append-only 정책 짝.
    affected = new_rows.select("dataset_id", "unit_id").distinct()
    target = dedup_bronze(
        bronze.join(F.broadcast(affected), ["dataset_id", "unit_id"], "inner")
    )

    # 1) cluster (centroids 재사용)
    df = assign_cluster_from_centroids(target, sorted_centers)
    # 2) z-score (feat_stats 의 mean/std 사용)
    df = apply_zscore_from_stats(df, stats_df)
    # 3-5) rolling / HI / rul_label
    df = add_rolling_features(df, [f"s{s}_norm" for s in KEEP_SENSORS],
                              window=args.rolling_window)
    df = add_health_index(df)
    df = add_rul_label(df)
    # 6) MERGE
    n = finalize_and_merge_silver(df, args.silver_version)

    write_pipeline_state(spark, last_snapshot_id=bronze_snap, rows_processed=n,
                         active_stats_version=stats_version)
    print(f"[silver_transform.incremental] silver merged rows = {n}, "
          f"advanced to bronze snapshot {bronze_snap}")


# ─────────────────────── entrypoint ───────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["full", "fit-stats", "incremental"],
                    default="full",
                    help="full: 전량 (cold start), fit-stats: 통계만 갱신, "
                         "incremental: watermark 이후 변경분만")
    ap.add_argument("--silver-version", default="v1")
    ap.add_argument("--stats-version", default=None,
                    help="fit-stats/full: 새 stats_version (생략 시 fs-<epoch>). "
                         "incremental: 사용할 stats_version (생략 시 최신 fit_ts)")
    ap.add_argument("--kmeans-k", type=int, default=6)
    ap.add_argument("--rolling-window", type=int, default=5)
    args = ap.parse_args()

    spark = SparkSession.builder.appName(f"silver_transform_{args.mode}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if args.mode == "full":
        run_full(spark, args)
    elif args.mode == "fit-stats":
        run_fit_stats(spark, args)
    elif args.mode == "incremental":
        run_incremental(spark, args)


if __name__ == "__main__":
    main()
