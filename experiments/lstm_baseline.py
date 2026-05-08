"""
LSTM RUL baseline (lstm-v1).

데이터: phm.silver.engine_health (Spark Iceberg).
분할:    unit-level 80/20 (dataset 별 stratify).
시퀀스:  최근 `--window` cycle (default 30) 의 19 차원 피처 → 마지막 cycle 의 RUL.
모델:    2-layer LSTM (hidden=64) + Linear head.
출력:    phm.gold.rul_prediction 에 model_version='lstm-v1' 로 MERGE,
         phm.gold.model_metrics 에 (val MAE/RMSE/PHM08, dataset 별) 기록.

실행:
  docker exec -it phm-spark /opt/spark/bin/spark-submit --master "local[*]" \
      /workspace/code/../experiments/lstm_baseline.py \
      --window 30 --epochs 20 --batch-size 256
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from pyspark.sql import SparkSession
from torch.utils.data import DataLoader, TensorDataset

KEEP_SENSORS = [2, 3, 4, 7, 8, 9, 11, 12, 13, 14, 15, 17, 20, 21]
FEATURE_COLS = (
    [f"s{s}_norm" for s in KEEP_SENSORS]
    + ["s_avg_w5", "s_std_w5", "s_trend_w5", "health_index", "cycle_norm"]
)  # 19 dims

TARGET_COLS = [
    "model_version", "dataset_id", "unit_id", "cycle",
    "rul_pred", "rul_pred_lower", "rul_pred_upper",
    "rul_actual", "abs_error", "phm08_score",
    "risk_tier", "predict_ts", "silver_snapshot_id",
]


# ──────────────────────────────────────────────────────────────────────
# Sequence construction
# ──────────────────────────────────────────────────────────────────────
def build_sequences(df: pd.DataFrame, window: int, rul_cap: int):
    """unit 별 시간순 정렬 후 길이 window 의 sliding window 만든다.
    cycle < window 이면 처음 행으로 left-pad (반복 복제) — 짧은 trajectory 도 포함."""
    df = df.sort_values(["dataset_id", "unit_id", "cycle"]).reset_index(drop=True)
    df["cycle_norm"] = df["cycle"] / 300.0  # 대략적 정규화 (max ~360)

    feats = df[FEATURE_COLS].to_numpy(dtype=np.float32)
    rul = np.minimum(df["rul_label"].to_numpy(dtype=np.float32), rul_cap)
    keys = df[["dataset_id", "unit_id", "cycle"]].to_records(index=False)

    seq_X, seq_y, seq_keys = [], [], []
    # unit 경계 빠르게 찾기
    starts = df.groupby(["dataset_id", "unit_id"]).indices
    for (_, _), idxs in starts.items():
        idxs = np.array(sorted(idxs))
        for i, end in enumerate(idxs):
            s = end - window + 1
            if s < idxs[0]:  # left-pad
                pad_n = idxs[0] - s
                head = feats[idxs[0]:idxs[0] + 1].repeat(pad_n, axis=0)
                tail = feats[idxs[0]:end + 1]
                window_feats = np.concatenate([head, tail], axis=0)
            else:
                window_feats = feats[s:end + 1]
            seq_X.append(window_feats)
            seq_y.append(rul[end])
            seq_keys.append(tuple(keys[end]))
    return np.stack(seq_X), np.array(seq_y, dtype=np.float32), seq_keys


# ──────────────────────────────────────────────────────────────────────
# Model
# ──────────────────────────────────────────────────────────────────────
class LSTMRegressor(nn.Module):
    def __init__(self, input_dim: int, hidden: int = 64, layers: int = 2, dropout: float = 0.2):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden, num_layers=layers,
                            batch_first=True, dropout=dropout if layers > 1 else 0.0)
        self.head = nn.Sequential(nn.Linear(hidden, 32), nn.ReLU(), nn.Linear(32, 1))

    def forward(self, x):  # x: [B, T, F]
        out, _ = self.lstm(x)
        return self.head(out[:, -1, :]).squeeze(-1)


# ──────────────────────────────────────────────────────────────────────
# Train / eval
# ──────────────────────────────────────────────────────────────────────
def train_model(model, train_loader, val_loader, epochs: int, lr: float):
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()
    best_val = float("inf")
    best_state = None
    for ep in range(epochs):
        model.train()
        train_loss = 0.0
        n = 0
        for xb, yb in train_loader:
            opt.zero_grad()
            pred = model(xb)
            loss = loss_fn(pred, yb)
            loss.backward()
            opt.step()
            train_loss += loss.item() * len(xb)
            n += len(xb)
        train_loss /= n

        model.eval()
        val_loss = 0.0
        n = 0
        with torch.no_grad():
            for xb, yb in val_loader:
                pred = model(xb)
                val_loss += loss_fn(pred, yb).item() * len(xb)
                n += len(xb)
        val_loss /= n
        print(f"  epoch {ep+1:2d}/{epochs}  train={train_loss:7.2f}  val={val_loss:7.2f}")
        if val_loss < best_val:
            best_val = val_loss
            best_state = {k: v.detach().clone() for k, v in model.state_dict().items()}
    model.load_state_dict(best_state)
    return model, best_val


def predict(model, X: np.ndarray, batch: int = 512) -> np.ndarray:
    model.eval()
    preds = []
    with torch.no_grad():
        for i in range(0, len(X), batch):
            xb = torch.from_numpy(X[i:i + batch])
            preds.append(model(xb).cpu().numpy())
    return np.concatenate(preds)


def phm08(d: np.ndarray) -> np.ndarray:
    return np.where(d >= 0, np.exp(d / 13.0) - 1.0, np.exp(-d / 10.0) - 1.0)


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model-version", default="lstm-v1")
    ap.add_argument("--window", type=int, default=30)
    ap.add_argument("--rul-cap", type=int, default=130)
    ap.add_argument("--epochs", type=int, default=20)
    ap.add_argument("--batch-size", type=int, default=256)
    ap.add_argument("--lr", type=float, default=1e-3)
    ap.add_argument("--val-frac", type=float, default=0.2)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    torch.manual_seed(args.seed)
    np.random.seed(args.seed)

    spark = SparkSession.builder.appName("lstm_baseline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[lstm] reading silver…")
    silver_sdf = spark.table("phm.silver.engine_health").na.fill(0.0, subset=["s_std_w5", "s_trend_w5"])
    silver_pdf = silver_sdf.toPandas()
    print(f"[lstm] silver rows = {len(silver_pdf):,}")

    snap_row = spark.sql(
        "SELECT snapshot_id FROM phm.silver.engine_health.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    silver_snapshot_id = int(snap_row[0]["snapshot_id"]) if snap_row else 0

    # ── unit-level train/val split (dataset stratify) ──
    units = silver_pdf[["dataset_id", "unit_id"]].drop_duplicates().reset_index(drop=True)
    rng = np.random.RandomState(args.seed)
    val_units = []
    for ds in units["dataset_id"].unique():
        u = units[units["dataset_id"] == ds]
        n_val = max(1, int(round(len(u) * args.val_frac)))
        val_units.append(u.sample(n=n_val, random_state=args.seed))
    val_units = pd.concat(val_units)
    val_key = set(map(tuple, val_units.to_numpy()))
    is_val = silver_pdf.apply(lambda r: (r["dataset_id"], r["unit_id"]) in val_key, axis=1)
    train_pdf = silver_pdf[~is_val].copy()
    val_pdf = silver_pdf[is_val].copy()
    print(f"[lstm] train units = {len(units) - len(val_units)}, val units = {len(val_units)}")

    # ── build sequences ──
    print("[lstm] building sequences…")
    X_tr, y_tr, _ = build_sequences(train_pdf, args.window, args.rul_cap)
    X_va, y_va, _ = build_sequences(val_pdf, args.window, args.rul_cap)
    X_all, y_all, keys_all = build_sequences(silver_pdf, args.window, args.rul_cap)
    print(f"[lstm] X_train={X_tr.shape}, X_val={X_va.shape}, X_all={X_all.shape}")

    train_ds = TensorDataset(torch.from_numpy(X_tr), torch.from_numpy(y_tr))
    val_ds = TensorDataset(torch.from_numpy(X_va), torch.from_numpy(y_va))
    train_loader = DataLoader(train_ds, batch_size=args.batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=args.batch_size)

    # ── train ──
    print("[lstm] training…")
    model = LSTMRegressor(input_dim=len(FEATURE_COLS))
    model, best_val_mse = train_model(model, train_loader, val_loader, args.epochs, args.lr)
    sigma = float(np.sqrt(best_val_mse))
    print(f"[lstm] best val MSE = {best_val_mse:.2f}, residual sigma = {sigma:.2f}")

    # ── predict on full silver ──
    print("[lstm] predicting full silver…")
    preds = predict(model, X_all, batch=512)
    preds = np.clip(preds, 0.0, args.rul_cap * 1.2)

    keys_df = pd.DataFrame(keys_all, columns=["dataset_id", "unit_id", "cycle"])
    keys_df["unit_id"] = keys_df["unit_id"].astype(int)
    keys_df["cycle"] = keys_df["cycle"].astype(int)
    out = keys_df.copy()
    out["rul_pred"] = preds.astype(float)
    z90 = 1.6449
    out["rul_pred_lower"] = np.clip(preds - z90 * sigma, 0.0, None).astype(float)
    out["rul_pred_upper"] = (preds + z90 * sigma).astype(float)

    # rul_actual = uncapped rul_label
    actual = (
        silver_pdf[["dataset_id", "unit_id", "cycle", "rul_label"]]
        .rename(columns={"rul_label": "rul_actual"})
    )
    out = out.merge(actual, on=["dataset_id", "unit_id", "cycle"], how="left")
    out["abs_error"] = (out["rul_pred"] - out["rul_actual"]).abs()
    out["phm08_score"] = phm08(out["rul_pred"].to_numpy() - out["rul_actual"].to_numpy())
    out["risk_tier"] = pd.cut(
        out["rul_pred"], bins=[-1, 10, 30, 80, 1e9],
        labels=["CRITICAL", "HIGH", "MED", "LOW"],
    ).astype(str)
    out["model_version"] = args.model_version
    out["silver_snapshot_id"] = silver_snapshot_id

    spark_out = spark.createDataFrame(out)
    spark_out = spark_out.selectExpr(
        "model_version", "dataset_id", "CAST(unit_id AS INT) AS unit_id",
        "CAST(cycle AS INT) AS cycle",
        "CAST(rul_pred AS DOUBLE) AS rul_pred",
        "CAST(rul_pred_lower AS DOUBLE) AS rul_pred_lower",
        "CAST(rul_pred_upper AS DOUBLE) AS rul_pred_upper",
        "CAST(rul_actual AS INT) AS rul_actual",
        "CAST(abs_error AS DOUBLE) AS abs_error",
        "CAST(phm08_score AS DOUBLE) AS phm08_score",
        "risk_tier",
        "current_timestamp() AS predict_ts",
        f"CAST({silver_snapshot_id} AS BIGINT) AS silver_snapshot_id",
    )
    spark_out.createOrReplaceTempView("_lstm_pred")

    update_assigns = ", ".join(
        f"t.{c} = s.{c}" for c in TARGET_COLS
        if c not in ("model_version", "dataset_id", "unit_id", "cycle")
    )
    insert_cols = ", ".join(TARGET_COLS)
    insert_vals = ", ".join(f"s.{c}" for c in TARGET_COLS)

    print("[lstm] MERGE INTO gold.rul_prediction …")
    spark.sql(f"""
        MERGE INTO phm.gold.rul_prediction t
        USING (SELECT * FROM _lstm_pred) s
          ON  t.model_version = s.model_version
          AND t.dataset_id    = s.dataset_id
          AND t.unit_id       = s.unit_id
          AND t.cycle         = s.cycle
        WHEN MATCHED THEN UPDATE SET {update_assigns}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    # ── model_metrics ──
    print("[lstm] writing model_metrics…")
    silver_row_count = silver_sdf.count()
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW _lstm_metrics AS
        SELECT
            model_version, dataset_id,
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
    spark.sql("""
        MERGE INTO phm.gold.model_metrics t
        USING (SELECT * FROM _lstm_metrics) s
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

    # ── 결과 요약 (val unit 한정) ──
    val_keys_set = set((r["dataset_id"], int(r["unit_id"])) for _, r in val_units.iterrows())
    out["is_val"] = out.apply(lambda r: (r["dataset_id"], int(r["unit_id"])) in val_keys_set, axis=1)
    val_summary = (
        out[out["is_val"]]
        .groupby("dataset_id")
        .agg(n=("rul_pred", "size"),
             mae=("abs_error", "mean"),
             rmse=("abs_error", lambda x: float(np.sqrt((x ** 2).mean()))),
             phm08=("phm08_score", "sum"))
        .round(2)
    )
    print("[lstm] hold-out val metrics by dataset:")
    print(val_summary.to_string())


if __name__ == "__main__":
    main()
