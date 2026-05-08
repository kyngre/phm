-- 2) 매니페스트 재작성 — 매니페스트 파일 분포를 정리해 plan 시간 단축.
--    데이터 파일이 많아지면 매니페스트 파일이 잘게 쪼개져 metadata read 가 느려진다.
--    rewrite_data_files 후 또는 주기적(주 1회)으로 실행 권장.

CALL phm.system.rewrite_manifests('bronze.engine_sensor_raw');
CALL phm.system.rewrite_manifests('silver.engine_health');
CALL phm.system.rewrite_manifests('gold.rul_prediction');
CALL phm.system.rewrite_manifests('gold.fleet_kpi_daily');
CALL phm.system.rewrite_manifests('gold.model_metrics');
