-- warehouse/marts/daily_aqi_summary.sql
-- 1. Xóa bảng cũ (nếu cần re-run)
DROP TABLE IF EXISTS marts.daily_aqi_summary;

-- 2. Tạo bảng và nạp dữ liệu
CREATE TABLE marts.daily_aqi_summary AS
SELECT
    city_key,
    date_key,
    AVG(pm2_5) AS avg_pm2_5,
    MAX(pm2_5) AS max_pm2_5,
    AVG(us_aqi) AS avg_us_aqi,
    COUNT(*) AS measurement_count
FROM warehouse.fact_air_quality_hourly
GROUP BY city_key, date_key;