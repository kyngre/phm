# Experiments — RUL 예측 baseline

- `01_eda.ipynb` — C-MAPSS 4개 데이터셋 EDA
- `02_feature_engineering.ipynb` — HI, rolling window, op-condition 클러스터링
- `03_baseline_lstm.ipynb` — LSTM RUL 예측
- `04_baseline_cnn.ipynb` — 1D-CNN RUL 예측
- `05_iceberg_time_travel_repro.ipynb` — time-travel 기반 학습 데이터 재현 검증

## 평가지표
- MAE
- PHM08 Score (under-prediction에 페널티)
- RMSE

## 데이터 재현성

LSTM 학습은 `phm.silver.engine_health` 의 특정 snapshot 을 stamp 함 (`silver_snapshot_id` 컬럼). 동일 snapshot 으로 `VERSION AS OF <id>` 시계열 재현 가능. `cmaps_to_kafka.py` 가 동일 `--base-date` 면 Silver 도 bit-identical (메타시각만 다름) — 학습 데이터 재현은 시뮬 인자 + snapshot_id 의 조합으로 강하게 보장.
