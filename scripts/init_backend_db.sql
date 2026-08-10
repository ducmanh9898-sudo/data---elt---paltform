BEGIN;

-- ============================================================
-- 1. COUNTRIES
-- ============================================================

CREATE TABLE IF NOT EXISTS countries (
    country_id SERIAL PRIMARY KEY,
    country_code VARCHAR(10) UNIQUE NOT NULL,
    country_name VARCHAR(100) NOT NULL
);


-- ============================================================
-- 2. CITIES
-- ============================================================

CREATE TABLE IF NOT EXISTS cities (
    city_id SERIAL PRIMARY KEY,
    country_id INT NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),

    CONSTRAINT fk_cities_country
        FOREIGN KEY (country_id)
        REFERENCES countries(country_id),

    CONSTRAINT uq_cities_country_name
        UNIQUE (country_id, city_name)
);


-- ============================================================
-- 3. DATA PROVIDERS
-- ============================================================

CREATE TABLE IF NOT EXISTS data_providers (
    provider_id SERIAL PRIMARY KEY,
    provider_name VARCHAR(100) UNIQUE NOT NULL,
    provider_type VARCHAR(50),
    base_url TEXT,
    update_frequency VARCHAR(50),
    description TEXT
);


-- ============================================================
-- 4. STATIONS
-- ============================================================

CREATE TABLE IF NOT EXISTS stations (
    station_id SERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    provider_id INT NOT NULL,
    station_name VARCHAR(255) NOT NULL,
    source_station_id VARCHAR(100),
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    is_active BOOLEAN DEFAULT TRUE,

    CONSTRAINT fk_stations_city
        FOREIGN KEY (city_id)
        REFERENCES cities(city_id),

    CONSTRAINT fk_stations_provider
        FOREIGN KEY (provider_id)
        REFERENCES data_providers(provider_id),

    CONSTRAINT uq_stations_provider_source
        UNIQUE (provider_id, source_station_id)
);


-- ============================================================
-- 5. POLLUTANTS
-- ============================================================

CREATE TABLE IF NOT EXISTS pollutants (
    pollutant_id SERIAL PRIMARY KEY,
    pollutant_code VARCHAR(20) UNIQUE NOT NULL,
    pollutant_name VARCHAR(100) NOT NULL,
    unit VARCHAR(20) NOT NULL,
    description TEXT
);


-- ============================================================
-- 6. AQI STANDARDS
-- ============================================================

CREATE TABLE IF NOT EXISTS aqi_standards (
    standard_id SERIAL PRIMARY KEY,
    pollutant_id INT NOT NULL,
    standard_name VARCHAR(100) NOT NULL,
    min_value DECIMAL(10, 2) NOT NULL,
    max_value DECIMAL(10, 2) NOT NULL,
    aqi_level VARCHAR(50) NOT NULL,
    health_impact TEXT,

    CONSTRAINT fk_aqi_pollutant
        FOREIGN KEY (pollutant_id)
        REFERENCES pollutants(pollutant_id),

    CONSTRAINT uq_aqi_standard_range
        UNIQUE (
            pollutant_id,
            standard_name,
            min_value,
            max_value
        )
);


-- ============================================================
-- 7. SEED COUNTRIES
-- ============================================================

INSERT INTO countries (
    country_code,
    country_name
)
VALUES
    ('US', 'United States'),
    ('GB', 'United Kingdom'),
    ('DE', 'Germany'),
    ('JP', 'Japan'),
    ('KR', 'South Korea'),
    ('SG', 'Singapore')
ON CONFLICT (country_code)
DO UPDATE SET
    country_name = EXCLUDED.country_name;


-- ============================================================
-- 8. SEED DATA PROVIDERS
-- ============================================================

INSERT INTO data_providers (
    provider_name,
    provider_type,
    base_url,
    update_frequency,
    description
)
VALUES (
    'Open-Meteo',
    'API',
    'https://open-meteo.com',
    'Hourly',
    'Weather and air quality API provider'
)
ON CONFLICT (provider_name)
DO UPDATE SET
    provider_type = EXCLUDED.provider_type,
    base_url = EXCLUDED.base_url,
    update_frequency = EXCLUDED.update_frequency,
    description = EXCLUDED.description;


-- ============================================================
-- 9. SEED POLLUTANTS
-- ============================================================

INSERT INTO pollutants (
    pollutant_code,
    pollutant_name,
    unit,
    description
)
VALUES
    (
        'PM2.5',
        'Fine particulate matter',
        'µg/m³',
        'Particles with diameter 2.5 micrometers or smaller'
    ),
    (
        'PM10',
        'Coarse particulate matter',
        'µg/m³',
        'Particles with diameter 10 micrometers or smaller'
    ),
    (
        'CO',
        'Carbon monoxide',
        'µg/m³',
        'Carbon monoxide concentration'
    ),
    (
        'NO2',
        'Nitrogen dioxide',
        'µg/m³',
        'Nitrogen dioxide concentration'
    ),
    (
        'SO2',
        'Sulfur dioxide',
        'µg/m³',
        'Sulfur dioxide concentration'
    ),
    (
        'O3',
        'Ozone',
        'µg/m³',
        'Ground-level ozone concentration'
    )
ON CONFLICT (pollutant_code)
DO UPDATE SET
    pollutant_name = EXCLUDED.pollutant_name,
    unit = EXCLUDED.unit,
    description = EXCLUDED.description;


-- ============================================================
-- 10. SEED CITIES
-- ============================================================

INSERT INTO cities (
    country_id,
    city_name,
    latitude,
    longitude
)
SELECT
    country.country_id,
    seed.city_name,
    seed.latitude,
    seed.longitude
FROM (
    VALUES
        ('US', 'New York', 40.7128, -74.0060),
        ('US', 'Los Angeles', 34.0522, -118.2437),
        ('US', 'Chicago', 41.8781, -87.6298),

        ('GB', 'London', 51.5074, -0.1278),
        ('GB', 'Manchester', 53.4808, -2.2426),

        ('DE', 'Berlin', 52.5200, 13.4050),
        ('DE', 'Munich', 48.1351, 11.5820),

        ('JP', 'Tokyo', 35.6762, 139.6503),
        ('JP', 'Osaka', 34.6937, 135.5023),

        ('KR', 'Seoul', 37.5665, 126.9780),
        ('KR', 'Busan', 35.1796, 129.0756),

        ('SG', 'Singapore', 1.3521, 103.8198)
) AS seed (
    country_code,
    city_name,
    latitude,
    longitude
)
JOIN countries AS country
    ON country.country_code = seed.country_code
ON CONFLICT (country_id, city_name)
DO UPDATE SET
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude;


-- ============================================================
-- 11. SEED STATIONS
-- Một station logic cho mỗi thành phố của Open-Meteo.
-- Đây không phải trạm đo vật lý thực tế.
-- ============================================================

INSERT INTO stations (
    city_id,
    provider_id,
    station_name,
    source_station_id,
    latitude,
    longitude,
    is_active
)
SELECT
    city.city_id,
    provider.provider_id,
    city.city_name || ' Monitoring Station',
    regexp_replace(
        lower(city.city_name),
        '[^a-z0-9]+',
        '_',
        'g'
    ) || '_station',
    city.latitude,
    city.longitude,
    TRUE
FROM cities AS city
CROSS JOIN data_providers AS provider
WHERE provider.provider_name = 'Open-Meteo'
ON CONFLICT (
    provider_id,
    source_station_id
)
DO UPDATE SET
    city_id = EXCLUDED.city_id,
    station_name = EXCLUDED.station_name,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    is_active = EXCLUDED.is_active;


-- ============================================================
-- 12. SEED PM2.5 AQI STANDARDS
-- ============================================================

INSERT INTO aqi_standards (
    pollutant_id,
    standard_name,
    min_value,
    max_value,
    aqi_level,
    health_impact
)
SELECT
    pollutant.pollutant_id,
    seed.standard_name,
    seed.min_value,
    seed.max_value,
    seed.aqi_level,
    seed.health_impact
FROM (
    VALUES
        (
            'PM2.5',
            'US EPA',
            0.0,
            12.0,
            'Good',
            'Air quality is satisfactory'
        ),
        (
            'PM2.5',
            'US EPA',
            12.1,
            35.4,
            'Moderate',
            'Acceptable air quality'
        ),
        (
            'PM2.5',
            'US EPA',
            35.5,
            55.4,
            'Unhealthy for Sensitive Groups',
            'Sensitive groups may be affected'
        ),
        (
            'PM2.5',
            'US EPA',
            55.5,
            150.4,
            'Unhealthy',
            'Everyone may begin to experience health effects'
        ),
        (
            'PM2.5',
            'US EPA',
            150.5,
            250.4,
            'Very Unhealthy',
            'Health alert'
        ),
        (
            'PM2.5',
            'US EPA',
            250.5,
            500.4,
            'Hazardous',
            'Health warning of emergency conditions'
        )
) AS seed (
    pollutant_code,
    standard_name,
    min_value,
    max_value,
    aqi_level,
    health_impact
)
JOIN pollutants AS pollutant
    ON pollutant.pollutant_code = seed.pollutant_code
ON CONFLICT (
    pollutant_id,
    standard_name,
    min_value,
    max_value
)
DO UPDATE SET
    aqi_level = EXCLUDED.aqi_level,
    health_impact = EXCLUDED.health_impact;


COMMIT;