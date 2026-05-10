# DDL

| 파일 | 내용 |
|---|---|
| `01_namespaces.sql` | `phm.bronze` / `phm.silver` / `phm.gold` 네임스페이스 생성 |
| `02_bronze_engine_sensor_raw.sql` | 원본 센서 스트림 |
| `03_silver_engine_health.sql` | 정제 + HI + 운영조건 클러스터 |
| `03b_silver_feat_stats.sql` | KMeans centroids + 클러스터별 센서 mean/std (증분 변환 입력) |
| `03c_silver_pipeline_state.sql` | 증분 변환 watermark (마지막 처리 bronze snapshot) |
| `04_gold_rul_prediction.sql` | 엔진별 RUL 예측 |
| `05_gold_fleet_kpi_daily.sql` | fleet 단위 KPI |
| `06_gold_model_metrics.sql` | 모델 버전별 성능 지표 |
| `07_gold_dq_results.sql` | 데이터 품질 검증 결과 (NULL/range/uniqueness/freshness; dq_check.py 출력) |
| `apply_all.sh` | 위 SQL 파일을 phm-spark 컨테이너에서 일괄 실행 |

## 적용

```bash
./code/ddl/apply_all.sh
```

`apply_all.sh` 는 `DROP TABLE IF EXISTS ... → CREATE TABLE` 멱등 형태이며, `full_ingest.sh` 의 1단계로도 호출된다.
