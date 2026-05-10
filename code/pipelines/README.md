# Pipelines

| 파일 | 역할 | 입력 | 출력 |
|---|---|---|---|
| `cmaps_to_kafka.py` | C-MAPSS txt 시뮬레이터 | `data/raw/train_FD00x.txt` | Kafka `phm.engine.sensor` |
| `bronze_ingest.py` | Spark Structured Streaming (append-only, dedup 은 Silver) | Kafka | `phm.bronze.engine_sensor_raw` |
| `silver_transform.py` | Bronze → Silver 변환 (`--mode full|fit-stats|incremental`) | Bronze + feat_stats + pipeline_state | `phm.silver.engine_health` (+ feat_stats / pipeline_state) |
| `gold_rul_predict.py` | RUL 학습/추론 (`--mode train|predict|full`) — 80/20 unit holdout, MinIO 모델 저장, 변경분만 추론 | Silver + (PipelineModel + pipeline_state) | `phm.gold.rul_prediction`, `phm.gold.model_metrics` (train/holdout 분리), `phm.gold.pipeline_state` |
| `gold_kpi_aggregate.py` | 일배치 KPI | Silver + Gold | `phm.gold.fleet_kpi_daily` |
| `dq_check.py` | 데이터 품질 검증 (NULL/finite/dup/cycle/rul/cluster/freshness/count/NaN) | Bronze + Silver | `phm.gold.dq_results` |
| `load_rul_ground_truth.py` | RUL_FDxxx.txt → silver 정답 RUL 적재 (NASA 표준 평가용) | `data/raw/RUL_FDxxx.txt` | `phm.silver.rul_ground_truth` |

---

## 한 번에 — `full_ingest.sh`

수동 단계를 모두 자동화한 일괄 적재 스크립트.

```bash
./code/pipelines/full_ingest.sh
```

수행하는 일 (11단계):

| # | 단계 | 내용 |
|---|---|---|
| 0 | 컨테이너 헬스 | minio / iceberg-rest / kafka / spark / trino 가 `running` 인지 확인 |
| 1 | DDL 적용 | `code/ddl/apply_all.sh` — Bronze/Silver/Gold 테이블 DROP+CREATE |
| 2 | Kafka 토픽 | `phm.engine.sensor` (4 partitions) `--if-not-exists` |
| 3 | Python 의존성 | `phm-spark` 컨테이너에 `kafka-python`, `numpy` 설치 (root 1회) |
| 4 | Bronze streaming 기동 | `bronze_ingest.py` 를 백그라운드로 띄움 (자세한 동작은 아래) |
| 5 | Producer | `cmaps_to_kafka.py --datasets FD001~FD004 --include-test --cycle-interval 1 --speedup 1000` — train + test trajectory 모두 Kafka 발행 |
| 5b | RUL ground truth 적재 | `load_rul_ground_truth.py` — RUL_FDxxx.txt → `phm.silver.rul_ground_truth` (NASA 표준 평가 입력) |
| 6 | 소화 대기 | 60s — streaming micro-batch 가 Bronze 까지 commit 하도록 |
| 7 | Bronze 검증 | dataset 별 행수·unit 수·max cycle |
| 8 | Silver `--mode full` | `silver_transform.py` — KMeans fit + 전량 변환 + feat_stats / pipeline_state 초기화 |
| 9 | Gold RUL `--mode full` | `gold_rul_predict.py --mode full` — 80/20 holdout 학습 + 모델 저장 + 추론 |
| 10 | Gold KPI | `gold_kpi_aggregate.py` |
| 11 | 최종 검증 | bronze / silver / gold.rul / gold.kpi 레이어별 카운트 |

### Step 4 의 streaming 기동 메커니즘

이 부분이 가장 까다로워서 별도 설명:

```bash
PGREP="docker exec phm-spark pgrep -f bronze_ingest.py"
if $PGREP >/dev/null 2>&1; then
  echo already; exit 0
fi
docker exec -d phm-spark bash -lc '
  nohup setsid /opt/spark/bin/spark-submit --master "local[*]" \
    /workspace/code/pipelines/bronze_ingest.py \
    </dev/null >/workspace/data/_logs/bronze_ingest.log 2>&1 &
  disown
'
```

핵심 포인트:

1. **pgrep 자기 매칭 회피** — pgrep 을 컨테이너 내부 bash 안에서 호출하면, pgrep 패턴 문자열(`bronze_ingest.py`) 이 부모 bash 의 cmdline 에도 들어있어 자기 자신을 매칭. 그래서 pgrep 은 **호스트에서 `docker exec phm-spark pgrep ...`** 형태로 호출하고, 재기동 명령은 **별도의 `docker exec -d`** 로 분리. 두 명령의 cmdline 이 겹치지 않으므로 컨테이너 ps 안에서 자기 매칭이 발생하지 않음.

2. **SIGHUP 차단** — `docker exec -d` 가 끝나면 컨테이너 안의 detach 된 bash 가 SIGHUP 을 받을 수 있어 spark-submit 도 같이 죽음. 이를 막으려면:
   - `nohup` — HUP 시그널 무시
   - `setsid` — 새 세션 리더로 분리해 컨트롤 터미널 끊음
   - `</dev/null` + `>log 2>&1` — stdin/out/err 모두 닫고 파일로 리다이렉트
   - `disown` — bash 의 job table 에서 제거

3. **시작 폴링** — Spark JVM 기동에 ~10s. 25 × 2s 폴링하며 spark-submit 이 visible 해질 때까지 대기. 끝나도 안 보이면 로그 40줄 출력 후 `exit 1`.

4. **멱등** — 이미 떠있으면 재기동 안 함. 스크립트를 여러 번 호출해도 안전.

### 멈추기

```bash
docker exec phm-spark pkill -f bronze_ingest.py
```

### 데이터 클린 시작

```bash
docker compose -f infra/docker-compose.yml down -v
docker compose -f infra/docker-compose.yml up -d
sleep 30
./code/pipelines/full_ingest.sh
```

이미 들어간 데이터는 Bronze (`source_file, line_no`), Silver (`dataset_id, unit_id, cycle`), Gold (`model_version, dataset_id, unit_id, cycle`) 멱등키로 보호되므로 `down -v` 없이 재실행해도 안전.

---

## 0. 사전 준비 (1회)

`numpy` / `kafka-python` 은 [infra/spark/Dockerfile](../../infra/spark/Dockerfile) 빌드 단계에서 사전 설치되므로 별도 `pip install` 불필요. Kafka 토픽만 만든다 (full_ingest.sh 가 동일 작업을 멱등하게 수행하므로 이 스니펫은 수동 실행 시 참고용).

```bash
docker exec phm-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --topic phm.engine.sensor \
  --partitions 4 --replication-factor 1 \
  --if-not-exists
```

---

> **Checkpoint 위치**: `/workspace/data/_checkpoints/` (호스트의 `data/` 에 마운트).
> S3A checkpoint는 hadoop-aws의 AWS SDK v1을 요구하지만 우리는 v2만 갖고 있어
> 로컬 dev는 file:// 경로를 사용. AWS 운영 시 SDK v1 bundle 추가하면 `s3a://` 가능.

## 1. Bronze 적재 실행

**터미널 A** — Spark Streaming Job 기동 (백그라운드 권장):
```bash
docker exec -it phm-spark /opt/spark/bin/spark-submit \
  --master "local[2]" \
  /workspace/code/pipelines/bronze_ingest.py
```

**터미널 B** — 시뮬레이터로 Kafka 발행 (Time-Travel Simulation):
```bash
docker exec -it phm-spark python3 /workspace/code/pipelines/cmaps_to_kafka.py \
  --datasets FD001,FD002,FD003,FD004 \
  --base-date 2025-08-01 --interval 1h --unit-jitter 1h \
  --speedup 1000
```

| 인자 | 역할 | 기본값 |
|---|---|---|
| `--base-date YYYY-MM-DD` | event_ts 기준 시각 (UTC) | `2025-08-01` |
| `--interval (1h\|30m\|1d\|...)` | 1 cycle 의 시간 폭 | `1h` |
| `--unit-jitter (1h\|...)` | unit_id 별 출발 시점 분산 | `1h` |
| `--cycle-interval` / `--speedup` | 발행 페이싱 (sleep 만, event_ts 와 무관) | `60 / 600` |
| `--max-units` | dataset 당 unit 제한 (스모크) | (전체) |
| `--dry-run` | Kafka 미발행, 파싱만 검증 | off |

상세 모델은 루트 [README §0-2](../../README.md#0-2-시뮬레이션--cmaps_to_kafkapy-time-travel-simulation).

---

## 2. 검증

```bash
docker exec phm-spark /opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=phm \
  -e "SELECT dataset_id, COUNT(*) AS rows, COUNT(DISTINCT unit_id) AS units
       FROM phm.bronze.engine_sensor_raw GROUP BY 1 ORDER BY 1;"
```

기대값 (FD001~FD004 train 전체):
| dataset_id | rows | units |
|---|---|---|
| FD001 | 20,631 | 100 |
| FD002 | 53,759 | 260 |
| FD003 | 24,720 | 100 |
| FD004 | 61,249 | 249 |

---

## 3. Silver 변환 (배치)

```bash
docker exec -it phm-spark /opt/spark/bin/spark-submit --master "local[2]" \
  /workspace/code/pipelines/silver_transform.py --silver-version v1
```

처리 단계:
1. `op_setting_1~3` 으로 KMeans(k=6) → `op_condition_cluster` (FD002/004 의 6 condition)
2. 분산 0 센서(1,5,6,10,16,18,19) 제외, 14개 센서 cluster별 z-score 정규화
3. (dataset_id, unit_id) 시간순 5-cycle rolling: 평균/표준편차/추세
4. Health Index = `1 - min(|s_avg_w5|, 3)/3` (cluster 평균에서 멀수록 열화)
5. `rul_label = max(cycle) - cycle` (train 가정: 최종 cycle = 고장)
6. `MERGE INTO phm.silver.engine_health ON (dataset_id, unit_id, cycle)`

검증 (event_ts 분산 + cluster 분포):
```bash
docker exec phm-spark /opt/spark/bin/spark-sql -e "
  SELECT dataset_id,
         MIN(event_ts) AS first_event, MAX(event_ts) AS last_event,
         COUNT(DISTINCT DATE(event_ts)) AS days_span,
         COUNT(*) AS rows,
         COUNT(DISTINCT op_condition_cluster) AS clusters,
         ROUND(AVG(health_index), 3) AS hi_avg
    FROM phm.silver.engine_health
   GROUP BY dataset_id ORDER BY dataset_id;"
```

`silver_version` 을 `v2` 로 바꾸고 같은 DDL 변경분을 다시 돌리면 백필 시뮬레이션 가능.

---

## 4. Gold RUL 예측 (배치, v0 베이스라인)

```bash
docker exec -it phm-spark /opt/spark/bin/spark-submit --master "local[2]" \
  /workspace/code/pipelines/gold_rul_predict.py \
  --model-version gbt-v0 --rul-cap 130
```

내용:
- Spark MLlib `GBTRegressor` 학습 → 동일 데이터 추론 (v0 베이스라인)
- feature: `op_condition_cluster, cycle, s{...}_norm × 14, s_avg_w5, s_std_w5, s_trend_w5, health_index`
- target: `min(rul_label, --rul-cap)` — PHM 관례 (초기 cycle을 130 cycle로 cap)
- 신뢰구간: 학습 잔차 σ 기반 90% CI
- `risk_tier`: CRITICAL(≤10) / HIGH(≤30) / MED(≤80) / LOW
- `silver_snapshot_id` 기록 → 추후 `phm.silver.engine_health VERSION AS OF <snapshot>` 으로 학습 데이터 재현

검증:
```bash
docker exec phm-spark /opt/spark/bin/spark-sql -e "
  SELECT model_version, risk_tier, COUNT(*) AS n
  FROM phm.gold.rul_prediction
  GROUP BY 1, 2 ORDER BY 1, 2;"
```

⚠️ v0 는 train 데이터 자체에서 학습+추론하는 데모. 정식 평가는 `experiments/` 의 hold-out + LSTM/CNN 으로 진행.

---

## 5. Gold KPI 일배치

```bash
docker exec -it phm-spark /opt/spark/bin/spark-submit --master "local[2]" \
  /workspace/code/pipelines/gold_kpi_aggregate.py --model-version gbt-v0
```

집계 키: `(kpi_date, dataset_id, op_condition_cluster)`
- 위험도: `critical/high/medium_count`, `risk_ratio_critical`
- HI 분포: `avg / p10 / p50 / p90`
- `avg_degradation_rate` = HI 의 cycle 차분 일자 평균
- `engine_count` = dataset×cluster 누적 unique unit, `active_engine_count` = 당일 cycle 발생 unit
- `source_model_version` 으로 모델별 KPI 비교 가능

`kpi_date = DATE(event_ts)` — 시뮬레이션 시각 기준이라, time-travel 시뮬을 활성화하면 자연스럽게 일자별 분산. 단일 시뮬에서는 producer `--interval 1h` 가정 시 25일 분량 KPI 행 생성.

---

## 6. 멱등성 확인

같은 시뮬레이터를 **동일 인자**로 재실행해도 행수가 늘지 않아야 함 (`source_file, line_no` Bronze MERGE 키 + `dataset_id, unit_id, cycle` Silver MERGE 키).

⚠️ **재시뮬 함정** — `--base-date` 를 바꿔서 재실행하면 자연키 동일 → MERGE 가 UPDATE 로 `event_ts` 만 덮어쓴다. 시계열 데이터를 다중 보존하려면 ① `ALTER TABLE phm.silver.engine_health CREATE TAG snap_aug` 로 사전 태그 후 시뮬 ② 또는 `down -v` 로 전체 초기화.

```bash
# 재발행
docker exec -it phm-spark python /workspace/code/pipelines/cmaps_to_kafka.py \
  --datasets FD001 --max-units 3

# 행 수 변화 없음 확인
docker exec phm-spark /opt/spark/bin/spark-sql \
  -e "SELECT COUNT(*) FROM phm.bronze.engine_sensor_raw WHERE dataset_id='FD001';"
```
