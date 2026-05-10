"""
데이터 품질 검증 (DQ) — Bronze/Silver 의 *데이터 자체* 품질 측정.

code/health-queries/ 가 다루지 않는 영역:
  · NULL 값 / 핵심 컬럼 무결성
  · 센서 finite 값 (NaN / Inf)
  · cycle 단조성 per (dataset, unit)
  · rul_label / cluster 범위 sanity
  · Silver freshness (마지막 적재 시각 기준)
  · Bronze ↔ Silver count 정합성 (dedup-aware)

각 rule 은:
  - rule_name: 식별자
  - layer: bronze | silver | gold
  - dataset_id: FD00x 또는 None (cross-dataset 집계)
  - measured_value, threshold, operator: 임계치 평가
  - status: PASS | WARN | FAIL
  - sample_size: 검사 모집단

결과는 phm.gold.dq_results 에 (rule_name, layer, dataset_id, run_date) 멱등 MERGE.

실행:
  docker exec -it phm-spark /opt/spark/bin/spark-submit --master local[2] \
      /workspace/code/pipelines/dq_check.py
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from silver_transform import KEEP_SENSORS, dedup_bronze

BRONZE_TABLE = "phm.bronze.engine_sensor_raw"
SILVER_TABLE = "phm.silver.engine_health"
DQ_TABLE = "phm.gold.dq_results"

DQ_COLS = [
    "rule_name", "layer", "dataset_id",
    "measured_value", "threshold", "operator", "status",
    "sample_size", "description",
    "run_ts", "run_date",
]


@dataclass
class DqResult:
    """단일 rule × dataset 의 평가 결과."""
    rule_name: str
    layer: str
    dataset_id: str          # 'FD001'~'FD004' 또는 '__ALL__'
    measured_value: float
    threshold: float
    operator: str            # '<=' | '<' | '=='
    sample_size: int
    description: str

    def status(self, warn_factor: float = 1.5) -> str:
        """PASS: pass. WARN: 임계 초과지만 warn_factor 배 이내. FAIL: warn_factor 초과."""
        m, t, op = self.measured_value, self.threshold, self.operator
        if op == "<=":
            if m <= t:
                return "PASS"
            return "WARN" if m <= t * warn_factor + 1e-9 else "FAIL"
        if op == "<":
            if m < t:
                return "PASS"
            return "WARN" if m < t * warn_factor + 1e-9 else "FAIL"
        if op == "==":
            return "PASS" if abs(m - t) < 1e-9 else "FAIL"
        raise ValueError(f"unsupported operator: {op}")


# ─────────────────────── rules ───────────────────────

def rule_bronze_null_key_columns(spark: SparkSession) -> list[DqResult]:
    """bronze 의 (dataset_id, unit_id, cycle) 가 NULL 인 행 수. 임계: 0."""
    rows = spark.sql(f"""
        SELECT
            COALESCE(dataset_id, '__ALL__') AS ds,
            COUNT(*) AS total,
            SUM(CASE WHEN unit_id IS NULL OR cycle IS NULL THEN 1 ELSE 0 END) AS bad
        FROM {BRONZE_TABLE}
        GROUP BY GROUPING SETS ((dataset_id), ())
    """).collect()
    return [
        DqResult(
            rule_name="bronze_null_key_columns",
            layer="bronze",
            dataset_id=r["ds"] if r["ds"] is not None else "__ALL__",
            measured_value=float(r["bad"] or 0),
            threshold=0.0, operator="<=",
            sample_size=int(r["total"]),
            description="bronze unit_id/cycle 이 NULL 인 행 수 (dataset_id NULL 은 GROUPING SETS 의 cross-dataset 합)",
        )
        for r in rows
    ]


def rule_bronze_finite_sensors(spark: SparkSession) -> list[DqResult]:
    """sensor_1..21 중 NaN 또는 Inf 가 있는 행 수. 임계: 0."""
    bad_expr = " OR ".join(
        f"isnan(sensor_{i}) OR sensor_{i} IS NULL OR sensor_{i} = double('inf') "
        f"OR sensor_{i} = double('-inf')"
        for i in range(1, 22)
    )
    rows = spark.sql(f"""
        SELECT
            COALESCE(dataset_id, '__ALL__') AS ds,
            COUNT(*) AS total,
            SUM(CASE WHEN {bad_expr} THEN 1 ELSE 0 END) AS bad
        FROM {BRONZE_TABLE}
        GROUP BY GROUPING SETS ((dataset_id), ())
    """).collect()
    return [
        DqResult(
            rule_name="bronze_finite_sensors",
            layer="bronze",
            dataset_id=r["ds"] if r["ds"] is not None else "__ALL__",
            measured_value=float(r["bad"] or 0),
            threshold=0.0, operator="<=",
            sample_size=int(r["total"]),
            description="sensor_1~21 중 NaN/Inf/NULL 인 행 수",
        )
        for r in rows
    ]


def rule_bronze_dup_ratio(spark: SparkSession) -> list[DqResult]:
    """(source_file, line_no) dup 비율. Bronze append-only 정책에서 streaming
    재시도로 발생 가능. 임계: < 1% (informational; FAIL 시 streaming 재시도 폭주 의심)."""
    bronze = spark.table(BRONZE_TABLE)
    counts = (
        bronze.groupBy("dataset_id", "source_file", "line_no")
              .agg(F.count(F.lit(1)).alias("c"))
              .withColumn("extra", F.col("c") - F.lit(1))
    )
    agg = (
        counts.groupBy("dataset_id")
              .agg(F.sum("extra").alias("dup"),
                   F.sum("c").alias("total"))
              .orderBy("dataset_id")
              .collect()
    )
    results = []
    total_dup = total_total = 0
    for r in agg:
        ds = r["dataset_id"] or "__ALL__"
        dup = int(r["dup"] or 0)
        total = int(r["total"] or 0)
        total_dup += dup
        total_total += total
        ratio = (dup / total) if total > 0 else 0.0
        results.append(DqResult(
            rule_name="bronze_dup_ratio",
            layer="bronze", dataset_id=ds,
            measured_value=ratio,
            threshold=0.01, operator="<=",
            sample_size=total,
            description="bronze (source_file, line_no) dup 비율 (Silver dedup 이 흡수)",
        ))
    ratio_all = (total_dup / total_total) if total_total > 0 else 0.0
    results.append(DqResult(
        rule_name="bronze_dup_ratio",
        layer="bronze", dataset_id="__ALL__",
        measured_value=ratio_all,
        threshold=0.01, operator="<=",
        sample_size=total_total,
        description="bronze (source_file, line_no) dup 비율 (Silver dedup 이 흡수) - 전체",
    ))
    return results


def rule_bronze_cycle_monotonic(spark: SparkSession) -> list[DqResult]:
    """unit 별 cycle 이 1..N 연속이어야. 누락된 cycle 이 있는 unit 수. 임계: 0."""
    bronze = dedup_bronze(spark.table(BRONZE_TABLE))
    rows = (
        bronze.groupBy("dataset_id", "unit_id")
              .agg(F.countDistinct("cycle").alias("n_distinct"),
                   (F.max("cycle") - F.min("cycle") + 1).alias("expected"))
              .withColumn("bad", (F.col("n_distinct") != F.col("expected")).cast("int"))
              .groupBy("dataset_id")
              .agg(F.sum("bad").alias("bad_units"),
                   F.count(F.lit(1)).alias("total_units"))
              .orderBy("dataset_id")
              .collect()
    )
    results = []
    bad_total = total_units = 0
    for r in rows:
        ds = r["dataset_id"] or "__ALL__"
        bad = int(r["bad_units"] or 0)
        total = int(r["total_units"] or 0)
        bad_total += bad
        total_units += total
        results.append(DqResult(
            rule_name="bronze_cycle_monotonic",
            layer="bronze", dataset_id=ds,
            measured_value=float(bad),
            threshold=0.0, operator="<=",
            sample_size=total,
            description="cycle 이 1..N 연속이 아닌 unit 수 (dedup 후)",
        ))
    results.append(DqResult(
        rule_name="bronze_cycle_monotonic",
        layer="bronze", dataset_id="__ALL__",
        measured_value=float(bad_total),
        threshold=0.0, operator="<=",
        sample_size=total_units,
        description="cycle 이 1..N 연속이 아닌 unit 수 (dedup 후) - 전체",
    ))
    return results


def rule_silver_rul_label_nonneg(spark: SparkSession) -> list[DqResult]:
    """rul_label < 0 또는 NULL 인 행 수. 임계: 0."""
    rows = spark.sql(f"""
        SELECT
            COALESCE(dataset_id, '__ALL__') AS ds,
            COUNT(*) AS total,
            SUM(CASE WHEN rul_label IS NULL OR rul_label < 0 THEN 1 ELSE 0 END) AS bad
        FROM {SILVER_TABLE}
        GROUP BY GROUPING SETS ((dataset_id), ())
    """).collect()
    return [
        DqResult(
            rule_name="silver_rul_label_nonneg",
            layer="silver",
            dataset_id=r["ds"] if r["ds"] is not None else "__ALL__",
            measured_value=float(r["bad"] or 0),
            threshold=0.0, operator="<=",
            sample_size=int(r["total"]),
            description="silver rul_label 이 NULL 또는 음수인 행 수",
        )
        for r in rows
    ]


def rule_silver_cluster_in_range(spark: SparkSession) -> list[DqResult]:
    """op_condition_cluster 가 [0, 5] 외부인 행 수. 임계: 0."""
    rows = spark.sql(f"""
        SELECT
            COALESCE(dataset_id, '__ALL__') AS ds,
            COUNT(*) AS total,
            SUM(CASE WHEN op_condition_cluster IS NULL
                       OR op_condition_cluster < 0
                       OR op_condition_cluster > 5
                     THEN 1 ELSE 0 END) AS bad
        FROM {SILVER_TABLE}
        GROUP BY GROUPING SETS ((dataset_id), ())
    """).collect()
    return [
        DqResult(
            rule_name="silver_cluster_in_range",
            layer="silver",
            dataset_id=r["ds"] if r["ds"] is not None else "__ALL__",
            measured_value=float(r["bad"] or 0),
            threshold=0.0, operator="<=",
            sample_size=int(r["total"]),
            description="silver op_condition_cluster 가 [0, 5] 외부인 행 수",
        )
        for r in rows
    ]


def rule_silver_freshness(spark: SparkSession,
                          warn_minutes: int = 120) -> list[DqResult]:
    """max(silver_ts) 가 현재로부터 warn_minutes 분 이내. cross-dataset 집계만."""
    row = spark.sql(f"""
        SELECT
            MAX(silver_ts) AS last_ts,
            COUNT(*) AS total,
            CAST(unix_timestamp(CURRENT_TIMESTAMP) - unix_timestamp(MAX(silver_ts))
                 AS DOUBLE) AS gap_seconds
        FROM {SILVER_TABLE}
    """).collect()[0]
    gap = float(row["gap_seconds"] or 1e9)
    total = int(row["total"] or 0)
    return [DqResult(
        rule_name="silver_freshness_minutes",
        layer="silver", dataset_id="__ALL__",
        measured_value=gap / 60.0,
        threshold=float(warn_minutes), operator="<=",
        sample_size=total,
        description=f"max(silver_ts) 와 현재 시각의 분 단위 간격 (임계 {warn_minutes}분)",
    )]


def rule_silver_count_matches_bronze(spark: SparkSession) -> list[DqResult]:
    """silver 행 수 == bronze dedup 행 수 (per dataset). 임계: 차이 0."""
    bronze = dedup_bronze(spark.table(BRONZE_TABLE))
    silver = spark.table(SILVER_TABLE)
    bronze_counts = (
        bronze.groupBy("dataset_id").count()
              .withColumnRenamed("count", "bronze_n")
    )
    silver_counts = (
        silver.groupBy("dataset_id").count()
              .withColumnRenamed("count", "silver_n")
    )
    joined = (
        bronze_counts.join(silver_counts, on="dataset_id", how="full_outer")
                     .na.fill(0, ["bronze_n", "silver_n"])
                     .withColumn("diff", F.abs(F.col("bronze_n") - F.col("silver_n")))
                     .orderBy("dataset_id")
                     .collect()
    )
    results = []
    diff_all = total_all = 0
    for r in joined:
        ds = r["dataset_id"] or "__ALL__"
        diff = int(r["diff"] or 0)
        total = int(r["bronze_n"] or 0)
        diff_all += diff
        total_all += total
        results.append(DqResult(
            rule_name="silver_count_matches_bronze",
            layer="silver", dataset_id=ds,
            measured_value=float(diff),
            threshold=0.0, operator="<=",
            sample_size=total,
            description="silver 행 수 vs bronze dedup 행 수 차이",
        ))
    results.append(DqResult(
        rule_name="silver_count_matches_bronze",
        layer="silver", dataset_id="__ALL__",
        measured_value=float(diff_all),
        threshold=0.0, operator="<=",
        sample_size=total_all,
        description="silver vs bronze dedup 행 수 차이 - 전체",
    ))
    return results


def rule_silver_no_nan_features(spark: SparkSession) -> list[DqResult]:
    """rolling 시작부 NaN 은 정상 (window=5 의 첫 4 cycle). NaN 비율이 5% 초과면 이상.

    s_avg_w5 / s_std_w5 / s_trend_w5 와 s*_norm 중 NaN 비율.
    """
    norm_cols = [f"s{s}_norm" for s in KEEP_SENSORS]
    feat_cols = norm_cols + ["s_avg_w5", "s_std_w5", "s_trend_w5"]
    bad_expr = " OR ".join(f"isnan({c}) OR {c} IS NULL" for c in feat_cols)
    rows = spark.sql(f"""
        SELECT
            COALESCE(dataset_id, '__ALL__') AS ds,
            COUNT(*) AS total,
            SUM(CASE WHEN {bad_expr} THEN 1 ELSE 0 END) AS bad
        FROM {SILVER_TABLE}
        GROUP BY GROUPING SETS ((dataset_id), ())
    """).collect()
    results = []
    for r in rows:
        ds = r["ds"] if r["ds"] is not None else "__ALL__"
        total = int(r["total"])
        bad = int(r["bad"] or 0)
        ratio = (bad / total) if total > 0 else 0.0
        # window 시작부의 s_std_w5/s_trend_w5 는 NaN 정상이라 임계 5%
        results.append(DqResult(
            rule_name="silver_nan_feature_ratio",
            layer="silver", dataset_id=ds,
            measured_value=ratio,
            threshold=0.05, operator="<=",
            sample_size=total,
            description="silver feature (s*_norm + rolling) NaN/NULL 비율",
        ))
    return results


# rule 함수 목록 — 추가 시 여기에만 등록
ALL_RULES = [
    rule_bronze_null_key_columns,
    rule_bronze_finite_sensors,
    rule_bronze_dup_ratio,
    rule_bronze_cycle_monotonic,
    rule_silver_rul_label_nonneg,
    rule_silver_cluster_in_range,
    rule_silver_freshness,
    rule_silver_count_matches_bronze,
    rule_silver_no_nan_features,
]


# ─────────────────────── MERGE ───────────────────────

def results_to_df(spark: SparkSession, results: list[DqResult]) -> DataFrame:
    rows = []
    for r in results:
        rows.append((
            r.rule_name, r.layer, r.dataset_id,
            float(r.measured_value), float(r.threshold), r.operator, r.status(),
            int(r.sample_size), r.description,
        ))
    schema = (
        "rule_name STRING, layer STRING, dataset_id STRING, "
        "measured_value DOUBLE, threshold DOUBLE, operator STRING, status STRING, "
        "sample_size BIGINT, description STRING"
    )
    return (
        spark.createDataFrame(rows, schema=schema)
             .withColumn("run_ts", F.current_timestamp())
             .withColumn("run_date", F.current_date())
             .select(*DQ_COLS)
    )


def merge_dq_results(spark: SparkSession, df: DataFrame) -> int:
    # MERGE source 가 current_timestamp() 포함 — non-deterministic 이라 localCheckpoint.
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    df = df.localCheckpoint(eager=True)
    n = df.count()
    df.createOrReplaceTempView("_dq_batch")

    update_assigns = ", ".join(
        f"t.{c} = s.{c}" for c in DQ_COLS
        if c not in ("rule_name", "layer", "dataset_id", "run_date")
    )
    insert_cols = ", ".join(DQ_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in DQ_COLS)
    spark.sql(f"""
        MERGE INTO {DQ_TABLE} t
        USING (SELECT * FROM _dq_batch) s
          ON  t.rule_name  = s.rule_name
          AND t.layer      = s.layer
          AND t.dataset_id = s.dataset_id
          AND t.run_date   = s.run_date
        WHEN MATCHED THEN UPDATE SET {update_assigns}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
    return n


def evaluate_all(spark: SparkSession) -> list[DqResult]:
    out: list[DqResult] = []
    for rule in ALL_RULES:
        out.extend(rule(spark))
    return out


# ─────────────────────── entrypoint ───────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fail-on-fail", action="store_true",
                    help="FAIL status 가 하나라도 있으면 종료 코드 1")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("dq_check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    results = evaluate_all(spark)
    df = results_to_df(spark, results)
    n = merge_dq_results(spark, df)
    print(f"[dq_check] wrote {n} dq_results rows")

    # 요약 출력
    summary = spark.sql(f"""
        SELECT status, COUNT(*) AS n
          FROM {DQ_TABLE}
         WHERE run_date = CURRENT_DATE
         GROUP BY status
         ORDER BY status
    """).collect()
    for r in summary:
        print(f"[dq_check] {r['status']}: {r['n']}")

    fails = [r for r in results if r.status() == "FAIL"]
    if fails:
        print(f"[dq_check] {len(fails)} FAIL rules:")
        for r in fails:
            print(f"  - {r.rule_name} [{r.layer}/{r.dataset_id}] "
                  f"measured={r.measured_value} {r.operator} {r.threshold}")
        if args.fail_on_fail:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
