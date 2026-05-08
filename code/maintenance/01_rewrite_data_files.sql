-- 1) 작은 파일 컴팩션 — bin-packing.
--    실행 환경: Spark + Iceberg extensions (Trino 미지원).
--    docker exec -i phm-spark spark-sql --conf ... < 01_rewrite_data_files.sql
--
--    target-file-size-bytes(128MB)에 맞춰 작은 파일을 묶고, 큰 파일은 손대지 않는다.
--    rewrite-job-order = bytes-asc → 가장 작은 파일부터 처리.

-- Bronze
CALL phm.system.rewrite_data_files(
    table => 'bronze.engine_sensor_raw',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '134217728',
        'min-file-size-bytes',    '67108864',
        'max-file-group-size-bytes', '10737418240',
        'rewrite-job-order',      'bytes-asc',
        'partial-progress.enabled', 'true'
    )
);

-- Silver
CALL phm.system.rewrite_data_files(
    table => 'silver.engine_health',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '134217728',
        'min-file-size-bytes',    '67108864',
        'partial-progress.enabled', 'true'
    )
);

-- Gold
CALL phm.system.rewrite_data_files(
    table => 'gold.rul_prediction',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '134217728',
        'partial-progress.enabled', 'true'
    )
);

CALL phm.system.rewrite_data_files(
    table => 'gold.fleet_kpi_daily',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '134217728')
);

CALL phm.system.rewrite_data_files(
    table => 'gold.model_metrics',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '134217728')
);
