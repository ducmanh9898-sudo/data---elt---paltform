select
    city_hour_key

from {{ ref('int_city_environment_hourly') }}

where
    (
        sensor_avg_pm2_5 is not null
        and pm2_5 is not null
        and abs(
            pm2_5_sensor_minus_batch
            - (sensor_avg_pm2_5 - pm2_5)
        ) > 0.000001
    )

    or

    (
        sensor_avg_pm10 is not null
        and pm10 is not null
        and abs(
            pm10_sensor_minus_batch
            - (sensor_avg_pm10 - pm10)
        ) > 0.000001
    )

    or

    (
        sensor_avg_temperature_2m is not null
        and temperature_2m is not null
        and abs(
            temperature_2m_sensor_minus_batch
            - (
                sensor_avg_temperature_2m
                - temperature_2m
            )
        ) > 0.000001
    )

    or

    (
        sensor_avg_relative_humidity_2m is not null
        and relative_humidity_2m is not null
        and abs(
            relative_humidity_2m_sensor_minus_batch
            - (
                sensor_avg_relative_humidity_2m
                - relative_humidity_2m
            )
        ) > 0.000001
    )