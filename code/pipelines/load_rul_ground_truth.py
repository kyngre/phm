"""
RUL_FDxxx.txt → phm.silver.rul_ground_truth 로더 (일회성, 멱등).

NASA C-MAPSS 의 정답 RUL 은 정적 텍스트 파일이라 Kafka streaming 경로 불필요.
파일 라인 번호 = unit_id, 라인 값 = test 마지막 cycle 시점의 RUL (정수).

사용:
  docker exec -it phm-spark /opt/spark/bin/spark-submit --master "local[2]" \
      /workspace/code/pipelines/load_rul_ground_truth.py \
      --datasets FD001,FD002,FD003,FD004
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = Path(os.environ.get("CMAPS_DATA_DIR", "/workspace/data/raw"))
TARGET_TABLE = "phm.silver.rul_ground_truth"


def parse_rul_file(path: Path, dataset_id: str) -> list[tuple[str, int, int]]:
    """RUL_FDxxx.txt → [(dataset_id, unit_id, true_rul), ...]

    파일은 행 단위. 행 N (1-base) = unit N 의 정답 RUL. 빈 라인 무시.
    """
    rows: list[tuple[str, int, int]] = []
    with path.open() as f:
        for line_no, line in enumerate(f, start=1):
            v = line.strip()
            if not v:
                continue
            try:
                rul = int(float(v))
            except ValueError:
                raise ValueError(f"{path}:{line_no} 비정수: '{line!r}'")
            rows.append((dataset_id, line_no, rul))
    return rows


def load_dataset(spark: SparkSession, dataset_id: str, target_table: str = TARGET_TABLE) -> int:
    src = DATA_DIR / f"RUL_{dataset_id}.txt"
    if not src.exists():
        raise FileNotFoundError(src)
    rows = parse_rul_file(src, dataset_id)
    if not rows:
        print(f"[load_rul_ground_truth] {src}: 빈 파일 — skip")
        return 0

    df = (
        spark.createDataFrame(rows, schema="dataset_id STRING, unit_id INT, true_rul INT")
             .withColumn("loaded_ts", F.current_timestamp())
    )
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")
    df = df.localCheckpoint(eager=True)
    df.createOrReplaceTempView("_rul_gt")
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING (SELECT * FROM _rul_gt) s
          ON t.dataset_id = s.dataset_id AND t.unit_id = s.unit_id
        WHEN MATCHED THEN UPDATE SET
            true_rul  = s.true_rul,
            loaded_ts = s.loaded_ts
        WHEN NOT MATCHED THEN INSERT *
    """)
    n = len(rows)
    print(f"[load_rul_ground_truth] {dataset_id}: merged {n} rows from {src.name}")
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", default="FD001,FD002,FD003,FD004")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("load_rul_ground_truth").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    total = 0
    for ds in (d.strip() for d in args.datasets.split(",") if d.strip()):
        total += load_dataset(spark, ds)

    summary = spark.sql(f"""
        SELECT dataset_id, COUNT(*) AS n_units, MIN(true_rul) AS min_rul,
               MAX(true_rul) AS max_rul, AVG(true_rul) AS avg_rul
          FROM {TARGET_TABLE}
         GROUP BY dataset_id ORDER BY dataset_id
    """).collect()
    for r in summary:
        print(f"[load_rul_ground_truth] {r.asDict()}")
    print(f"[load_rul_ground_truth] total merged = {total}")


if __name__ == "__main__":
    main()
