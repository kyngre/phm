-- ─────────────────────────────────────────────────────────────
-- Gold: Fleet 단위 KPI 일배치 (대시보드 비즈니스 탭 핵심 소스)
-- ─────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS phm.gold.fleet_kpi_daily;

CREATE TABLE phm.gold.fleet_kpi_daily (
    kpi_date                DATE,
    dataset_id              STRING,
    op_condition_cluster    INT,

    -- Fleet 규모
    engine_count            INT,
    active_engine_count     INT  COMMENT '당일 cycle 발생 엔진 수',

    -- 위험도
    critical_count          INT  COMMENT 'RUL <= 10',
    high_count              INT  COMMENT 'RUL <= 30',
    medium_count            INT  COMMENT 'RUL <= 80',
    risk_ratio_critical     DOUBLE,

    -- 열화율
    avg_health_index        DOUBLE,
    health_index_p10        DOUBLE,
    health_index_p50        DOUBLE,
    health_index_p90        DOUBLE,
    avg_degradation_rate    DOUBLE COMMENT 'HI 일자별 변화량 평균',

    -- 메타
    kpi_ts                  TIMESTAMP,
    source_model_version    STRING
)
USING iceberg
PARTITIONED BY (months(kpi_date), dataset_id)
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728',
    'write.parquet.compression-codec' = 'zstd',
    'write.merge.mode' = 'copy-on-write'
);
