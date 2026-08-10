with hourly as (

    select *
    from {{ ref('gold_city_environment_hourly') }}

),

correlation_by_city as (

    select
        -- Grain: one row per city
        city_id,

        -- Latest descriptive values
        max_by(city_name, measured_at_utc) as city_name,
        max_by(country_code, measured_at_utc) as country_code,
        max_by(country_name, measured_at_utc) as country_name,
        max_by(latitude, measured_at_utc) as latitude,
        max_by(longitude, measured_at_utc) as longitude,
        max_by(timezone, measured_at_utc) as timezone,

        -- Data coverage
        count(*) as observed_hour_count,
        min(measured_at_utc) as min_measured_at_utc,
        max(measured_at_utc) as max_measured_at_utc,

        -- Average measurements for analytical context
        avg(pm2_5) as avg_pm2_5,
        avg(pm10) as avg_pm10,
        avg(us_aqi) as avg_us_aqi,
        avg(temperature_2m) as avg_temperature_2m,
        avg(relative_humidity_2m) as avg_relative_humidity_2m,
        avg(precipitation) as avg_precipitation,
        avg(wind_speed_10m) as avg_wind_speed_10m,

        -- Number of usable PM2.5 pairs
        count_if(
            pm2_5 is not null
            and temperature_2m is not null
        ) as pm2_5_temperature_pair_count,

        count_if(
            pm2_5 is not null
            and relative_humidity_2m is not null
        ) as pm2_5_humidity_pair_count,

        count_if(
            pm2_5 is not null
            and precipitation is not null
        ) as pm2_5_precipitation_pair_count,

        count_if(
            pm2_5 is not null
            and wind_speed_10m is not null
        ) as pm2_5_wind_speed_pair_count,

        -- PM2.5 correlations
        corr(
            pm2_5,
            temperature_2m
        ) as corr_pm2_5_temperature,

        corr(
            pm2_5,
            relative_humidity_2m
        ) as corr_pm2_5_humidity,

        corr(
            pm2_5,
            precipitation
        ) as corr_pm2_5_precipitation,

        corr(
            pm2_5,
            wind_speed_10m
        ) as corr_pm2_5_wind_speed,

        -- Number of usable PM10 pairs
        count_if(
            pm10 is not null
            and temperature_2m is not null
        ) as pm10_temperature_pair_count,

        count_if(
            pm10 is not null
            and relative_humidity_2m is not null
        ) as pm10_humidity_pair_count,

        count_if(
            pm10 is not null
            and precipitation is not null
        ) as pm10_precipitation_pair_count,

        count_if(
            pm10 is not null
            and wind_speed_10m is not null
        ) as pm10_wind_speed_pair_count,

        -- PM10 correlations
        corr(
            pm10,
            temperature_2m
        ) as corr_pm10_temperature,

        corr(
            pm10,
            relative_humidity_2m
        ) as corr_pm10_humidity,

        corr(
            pm10,
            precipitation
        ) as corr_pm10_precipitation,

        corr(
            pm10,
            wind_speed_10m
        ) as corr_pm10_wind_speed,

        -- Lineage
        max(data_updated_at_utc) as data_updated_at_utc

    from hourly

    group by city_id

)

select *
from correlation_by_city
