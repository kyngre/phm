# Iceberg Maintenance

Iceberg 테이블 유지보수 절차. 모두 **Spark + Iceberg extensions** 환경에서 `CALL phm.system.<procedure>(...)` 형태로 실행 (Trino 미지원).

| # | 파일 | 절차 | 주기 | 트리거 |
|---|---|---|---|---|
| 1 | `01_rewrite_data_files.sql` | `rewrite_data_files` (binpack) | 일 1회 | `health-queries/03_small_files_ratio.sql` > 30% |
| 2 | `02_rewrite_manifests.sql` | `rewrite_manifests` | 주 1회 | metadata read 지연 시 |
| 3 | `03_expire_snapshots.sql` | `expire_snapshots` (100일 = 학습 90d + 마진 10d) | 주 1회 | snapshot 수 누적 |
| 4 | `04_remove_orphan_files.sql` | `remove_orphan_files` (7일) | 월 1회 | 실패한 write/컴팩션 후 |

## 실행 순서

```
rewrite_data_files → rewrite_manifests → (검증) → expire_snapshots → remove_orphan_files
```

`rewrite_data_files` 직후 바로 `expire_snapshots` 하면 컴팩션 전 파일까지 GC 되어 롤백 불가 → ETL 검증을 사이에 둔다.

## 사용법

전체 단계:
```bash
./code/maintenance/run.sh
```

단일 단계:
```bash
./code/maintenance/run.sh 01_rewrite_data_files
```

수동:
```bash
docker exec -i phm-spark /opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=phm \
  -f /workspace/code/maintenance/03_expire_snapshots.sql
```

## 파라미터 메모

- `target-file-size-bytes = 134217728` (128MB) — DDL `TBLPROPERTIES` 와 동일.
- `min-file-size-bytes = 67108864` (64MB) — 이 미만 파일을 컴팩션 대상으로.
- `partial-progress.enabled = true` — 일부 파일그룹 실패해도 성공한 그룹은 commit.
- `expire_snapshots.older_than` — DDL `history.expire.max-snapshot-age-ms` (100일) + `iceberg_expire_dag.OLDER_THAN_DAYS` 와 정합. 백필 90일 + 마진 10일.
- `remove_orphan_files.older_than` — 진행 중 write 보호 위해 최소 3일, 여기서는 7일.

## Airflow 연계

이 SQL 들은 향후 Airflow `compaction_dag`, `expire_snapshots_dag`, `orphan_files_dag` 의 SparkSubmitOperator 에서 동일 파일을 그대로 호출한다.
