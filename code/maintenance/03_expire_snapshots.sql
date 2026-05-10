-- 3) 오래된 스냅샷 만료 — time-travel 기간 외 스냅샷·매니페스트·데이터 파일 GC.
--    TBLPROPERTIES.history.expire.* 와 정합:
--      max-snapshot-age-ms  = 8640000000 (100일 = 학습 윈도우 90d + 마진 10d)
--      min-snapshots-to-keep = 20
--    older_than 는 절대 시각, retain_last 는 안전장치(최소 보존 개수).
--
--    주의: rewrite_data_files 직후 expire 하면 컴팩션 전 파일까지 삭제되므로
--          rewrite → (ETL 검증) → expire 순서를 권장.
--
--    ⚠️  이 파일의 older_than 은 절대 시각이므로 수동 실행 시 날짜를 직접 갱신할 것.
--       Airflow iceberg_expire_dag 는 inline SQL 로 동적 계산하므로 이 파일을 쓰지 않음.
--       run.sh 경유 수동 실행: older_than = TODAY - 100d 를 계산해 아래 값을 교체한 뒤 실행.

CALL phm.system.expire_snapshots(
    table => 'bronze.engine_sensor_raw',
    older_than => TIMESTAMP '2026-01-30 00:00:00',  -- 갱신 기준: 2026-05-10 - 100일
    retain_last => 20
);

CALL phm.system.expire_snapshots(
    table => 'silver.engine_health',
    older_than => TIMESTAMP '2026-01-30 00:00:00',
    retain_last => 20
);

CALL phm.system.expire_snapshots(
    table => 'gold.rul_prediction',
    older_than => TIMESTAMP '2026-01-30 00:00:00',
    retain_last => 20
);

CALL phm.system.expire_snapshots(
    table => 'gold.fleet_kpi_daily',
    older_than => TIMESTAMP '2026-01-30 00:00:00',
    retain_last => 10
);

CALL phm.system.expire_snapshots(
    table => 'gold.model_metrics',
    older_than => TIMESTAMP '2026-01-30 00:00:00',
    retain_last => 10
);
