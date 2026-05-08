-- 3) 오래된 스냅샷 만료 — time-travel 기간 외 스냅샷·매니페스트·데이터 파일 GC.
--    TBLPROPERTIES.history.expire.* 와 정합:
--      max-snapshot-age-ms  = 7776000000 (90일)
--      min-snapshots-to-keep = 20
--    older_than 는 절대 시각, retain_last 는 안전장치(최소 보존 개수).
--
--    주의: rewrite_data_files 직후 expire 하면 컴팩션 전 파일까지 삭제되므로
--          rewrite → (ETL 검증) → expire 순서를 권장.

CALL phm.system.expire_snapshots(
    table => 'bronze.engine_sensor_raw',
    older_than => TIMESTAMP '2026-02-05 00:00:00',  -- 오늘(2026-05-06) - 90일
    retain_last => 20
);

CALL phm.system.expire_snapshots(
    table => 'silver.engine_health',
    older_than => TIMESTAMP '2026-02-05 00:00:00',
    retain_last => 20
);

CALL phm.system.expire_snapshots(
    table => 'gold.rul_prediction',
    older_than => TIMESTAMP '2026-02-05 00:00:00',
    retain_last => 20
);

CALL phm.system.expire_snapshots(
    table => 'gold.fleet_kpi_daily',
    older_than => TIMESTAMP '2026-02-05 00:00:00',
    retain_last => 10
);

CALL phm.system.expire_snapshots(
    table => 'gold.model_metrics',
    older_than => TIMESTAMP '2026-02-05 00:00:00',
    retain_last => 10
);
