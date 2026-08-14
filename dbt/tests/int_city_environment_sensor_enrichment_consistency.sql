select
    b.city_hour_key

from {{ ref('int_city_environment_hourly') }} b

join {{ ref('int_sensor_environment_hourly') }} s
  on b.city_id = s.city_id
 and b.measured_at_utc = s.measured_at_utc

where
       b.has_sensor_data is distinct from true

    or b.sensor_event_count
        is distinct from s.sensor_event_count

    or b.sensor_device_count
        is distinct from s.sensor_device_count

    or b.sensor_avg_pm2_5
        is distinct from s.sensor_avg_pm2_5

    or b.sensor_avg_pm10
        is distinct from s.sensor_avg_pm10

    or b.sensor_avg_temperature_2m
        is distinct from s.sensor_avg_temperature_2m

    or b.sensor_avg_relative_humidity_2m
        is distinct from s.sensor_avg_relative_humidity_2m

    or b.sensor_data_updated_at
        is distinct from s.sensor_data_updated_at