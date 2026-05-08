-- 비즈니스 탭 차트 3: 운영조건별 열화율 추이
-- 차트 타입: Line (multi-series)
-- X: kpi_date, Y: avg_degradation_rate, series: op_condition_cluster
-- 필터: dataset_id
-- 주의: fleet_kpi_daily 에는 HI 백분위(p10/p50/p90) 만 있고 RUL 백분위는 없음.
SELECT
    kpi_date,
    dataset_id,
    op_condition_cluster,
    avg_health_index,
    health_index_p10,
    health_index_p50,
    health_index_p90,
    avg_degradation_rate,
    critical_count,
    high_count,
    risk_ratio_critical
FROM iceberg.gold.fleet_kpi_daily
ORDER BY kpi_date, dataset_id, op_condition_cluster;
