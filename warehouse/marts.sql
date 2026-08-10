-- File: warehouse/marts.sql

-- Schema cho lớp dữ liệu thành phẩm
CREATE SCHEMA IF NOT EXISTS marts;

-- Data Mart: Thống kê AQI theo ngày
CREATE OR REPLACE TABLE marts.daily_aqi_summary AS
SELECT
    city_key,
    date_key,
    AVG(pm2_5) AS avg_pm2_5,
    MAX(pm2_5) AS max_pm2_5,
    AVG(pm10) AS avg_pm10,
    MAX(pm10) AS max_pm10,
    COUNT(*) AS measurement_count
FROM warehouse.fact_air_quality_hourly
GROUP BY city_key, date_key;