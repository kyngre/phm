-- 4) 고아 파일 제거 — 어떤 스냅샷에도 참조되지 않는 데이터/메타 파일 GC.
--    실패한 write, 중단된 컴팩션 등에서 발생.
--
--    older_than 기본값은 3일 — 진행 중인 write 를 GC 하지 않기 위함.
--    여기서는 명시적으로 7일로 지정 (ETL 재시도/디버깅 여유).
--
--    주의: 분산 파일시스템에서 list 비용이 크다. 월 1회 정도 권장.

CALL phm.system.remove_orphan_files(
    table => 'bronze.engine_sensor_raw',
    older_than => TIMESTAMP '2026-04-29 00:00:00',  -- 오늘(2026-05-06) - 7일
    dry_run => false
);

CALL phm.system.remove_orphan_files(
    table => 'silver.engine_health',
    older_than => TIMESTAMP '2026-04-29 00:00:00',
    dry_run => false
);

CALL phm.system.remove_orphan_files(
    table => 'gold.rul_prediction',
    older_than => TIMESTAMP '2026-04-29 00:00:00',
    dry_run => false
);

CALL phm.system.remove_orphan_files(
    table => 'gold.fleet_kpi_daily',
    older_than => TIMESTAMP '2026-04-29 00:00:00',
    dry_run => false
);

CALL phm.system.remove_orphan_files(
    table => 'gold.model_metrics',
    older_than => TIMESTAMP '2026-04-29 00:00:00',
    dry_run => false
);
