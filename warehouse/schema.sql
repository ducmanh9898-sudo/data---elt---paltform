CREATE SCHEMA IF NOT EXISTS warehouse;


-- ============================================================
-- FULL REFRESH
--
-- Xóa Fact trước vì Fact đang tham chiếu các Dimension.
-- ============================================================
DROP TABLE IF EXISTS warehouse.fact_weather_hourly;
DROP TABLE IF EXISTS warehouse.fact_air_quality_hourly;

DROP TABLE IF EXISTS warehouse.dim_source;
DROP TABLE IF EXISTS warehouse.dim_hour;
DROP TABLE IF EXISTS warehouse.dim_date;
DROP TABLE IF EXISTS warehouse.dim_city;


-- ============================================================
-- DIMENSION: CITY
--
-- Grain:
-- Một dòng đại diện cho một thành phố trong một hệ thống nguồn.
--
-- source_system:
-- Hệ thống cung cấp master data của thành phố.
-- Trong project hiện tại là backend_postgres.
--
-- source_city_id:
-- city_id lấy từ bảng cities trong PostgreSQL backend.
-- ============================================================

CREATE TABLE warehouse.dim_city (
    city_key BIGINT PRIMARY KEY,

    source_system VARCHAR NOT NULL,
    source_city_id BIGINT NOT NULL,

    city_name VARCHAR NOT NULL,
    country_code VARCHAR NOT NULL,
    country_name VARCHAR NOT NULL,

    latitude DOUBLE,
    longitude DOUBLE,

    -- Ví dụ:
    -- Europe/Berlin
    -- Asia/Tokyo
    -- America/New_York
    timezone VARCHAR NOT NULL,

    UNIQUE (
        source_system,
        source_city_id
    )
);


-- ============================================================
-- DIMENSION: DATE
--
-- Grain:
-- Một dòng đại diện cho một ngày địa phương.
--
-- date_key được tạo từ measurement_date_local.
--
-- Ví dụ:
-- full_date = 2026-07-03
-- date_key  = 20260703
-- ============================================================

CREATE TABLE warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,

    day_of_week SMALLINT NOT NULL,
    day_name VARCHAR NOT NULL,
    day_of_month SMALLINT NOT NULL,
    day_of_year SMALLINT NOT NULL,

    week_of_year SMALLINT NOT NULL,

    month_number SMALLINT NOT NULL,
    month_name VARCHAR NOT NULL,

    quarter_number SMALLINT NOT NULL,
    year_number INTEGER NOT NULL,

    is_weekend BOOLEAN NOT NULL,

    CHECK (
        day_of_week BETWEEN 1 AND 7
    ),

    CHECK (
        day_of_month BETWEEN 1 AND 31
    ),

    CHECK (
        month_number BETWEEN 1 AND 12
    ),

    CHECK (
        quarter_number BETWEEN 1 AND 4
    )
);


-- ============================================================
-- DIMENSION: HOUR
--
-- Grain:
-- Một dòng đại diện cho một giờ địa phương.
--
-- hour_key được tạo từ measured_at_local.
--
-- Có đúng 24 dòng, từ 0 đến 23.
-- ============================================================

CREATE TABLE warehouse.dim_hour (
    hour_key SMALLINT PRIMARY KEY,
    hour_of_day SMALLINT NOT NULL UNIQUE,

    hour_label VARCHAR NOT NULL,
    part_of_day VARCHAR NOT NULL,

    CHECK (
        hour_of_day BETWEEN 0 AND 23
    ),

    CHECK (
        part_of_day IN (
            'Night',
            'Morning',
            'Afternoon',
            'Evening'
        )
    )
);


-- ============================================================
-- DIMENSION: SOURCE
--
-- Grain:
-- Một dòng đại diện cho một dataset từ một source system.
--
-- Ví dụ:
-- source_system = open_meteo
-- source_name   = Open-Meteo Air Quality API
-- dataset_name  = air_quality_hourly
-- dataset_kind  = air_quality_model
-- source_type   = api
-- ============================================================

CREATE TABLE warehouse.dim_source (
    source_key SMALLINT PRIMARY KEY,

    source_system VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,

    dataset_name VARCHAR NOT NULL,
    dataset_kind VARCHAR NOT NULL,

    source_type VARCHAR NOT NULL,

    UNIQUE (
        source_system,
        dataset_name,
        dataset_kind
    ),

    CHECK (
        source_type IN (
            'api',
            'database',
            'file',
            'stream'
        )
    )
);


-- ============================================================
-- FACT: HOURLY AIR QUALITY
--
-- Grain:
-- Một thành phố
-- + một thời điểm UTC theo giờ
-- + một nguồn dữ liệu.
--
-- Business key:
-- city_key + measured_at_utc + source_key
--
-- measured_at_local:
-- Thời gian địa phương của thành phố.
-- Dùng để phân tích theo ngày, giờ và part_of_day địa phương.
--
-- measured_at_utc:
-- Thời điểm chuẩn hóa UTC.
-- Dùng để deduplicate và so sánh nhiều thành phố toàn cầu.
--
-- Fact này hiện chỉ chứa historical model data.
-- Forecast sau này nên được lưu trong Fact riêng.
-- ============================================================
CREATE TABLE warehouse.fact_weather_hourly (
    weather_key BIGINT PRIMARY KEY,

    city_key BIGINT NOT NULL,
    date_key INTEGER NOT NULL,
    hour_key SMALLINT NOT NULL,
    source_key SMALLINT NOT NULL,

    measured_at_local TIMESTAMP NOT NULL,
    measured_at_utc TIMESTAMP NOT NULL,
    crawled_at_utc TIMESTAMP NOT NULL,
    utc_offset_seconds INTEGER,

    record_type VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,

    temperature_2m DOUBLE,
    relative_humidity_2m DOUBLE,
    precipitation DOUBLE,
    rain DOUBLE,
    surface_pressure DOUBLE,
    cloud_cover DOUBLE,
    wind_speed_10m DOUBLE,
    wind_direction_10m DOUBLE,
    visibility DOUBLE,
    weather_code INTEGER,

    FOREIGN KEY (city_key)
        REFERENCES warehouse.dim_city(city_key),

    FOREIGN KEY (date_key)
        REFERENCES warehouse.dim_date(date_key),

    FOREIGN KEY (hour_key)
        REFERENCES warehouse.dim_hour(hour_key),

    FOREIGN KEY (source_key)
        REFERENCES warehouse.dim_source(source_key),

    UNIQUE (
        city_key,
        measured_at_utc,
        source_key
    )
);
CREATE TABLE warehouse.fact_air_quality_hourly (
    air_quality_key BIGINT PRIMARY KEY,

    city_key BIGINT NOT NULL,
    date_key INTEGER NOT NULL,
    hour_key SMALLINT NOT NULL,
    source_key SMALLINT NOT NULL,

    measured_at_local TIMESTAMP NOT NULL,
    measured_at_utc TIMESTAMP NOT NULL,

    crawled_at_utc TIMESTAMP NOT NULL,
    utc_offset_seconds INTEGER,

    record_type VARCHAR NOT NULL,

    source_file VARCHAR NOT NULL,

    pm10 DOUBLE,
    pm2_5 DOUBLE,
    carbon_monoxide DOUBLE,
    nitrogen_dioxide DOUBLE,
    sulphur_dioxide DOUBLE,
    ozone DOUBLE,
    us_aqi INTEGER,

    loaded_at TIMESTAMP NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (city_key)
        REFERENCES warehouse.dim_city(city_key),

    FOREIGN KEY (date_key)
        REFERENCES warehouse.dim_date(date_key),

    FOREIGN KEY (hour_key)
        REFERENCES warehouse.dim_hour(hour_key),

    FOREIGN KEY (source_key)
        REFERENCES warehouse.dim_source(source_key),

    -- Fact này chỉ chứa dữ liệu lịch sử đã được lọc
    -- ở Spark Clean Zone.
    CHECK (
        record_type = 'historical'
    ),

    -- Các chỉ số ô nhiễm không được là số âm.
    -- NULL vẫn được chấp nhận vì NULL nghĩa là nguồn thiếu dữ liệu.
    CHECK (
        pm10 IS NULL
        OR pm10 >= 0
    ),

    CHECK (
        pm2_5 IS NULL
        OR pm2_5 >= 0
    ),

    CHECK (
        carbon_monoxide IS NULL
        OR carbon_monoxide >= 0
    ),

    CHECK (
        nitrogen_dioxide IS NULL
        OR nitrogen_dioxide >= 0
    ),

    CHECK (
        sulphur_dioxide IS NULL
        OR sulphur_dioxide >= 0
    ),

    CHECK (
        ozone IS NULL
        OR ozone >= 0
    ),

    CHECK (
        us_aqi IS NULL
        OR us_aqi >= 0
    ),

    UNIQUE (
        city_key,
        measured_at_utc,
        source_key
    )
);