# Streaming Event Contracts

## `sensor_reading_v1`

Kafka topic:

```text
environment.sensor-readings.raw MARKDOWN'
```

Kafka message key:

```text
city_id
```

The payload is UTF-8 JSON.

## Time semantics

- `event_time_utc`: thời điểm cảm biến ghi nhận dữ liệu.
- `produced_at_utc`: thời điểm simulator gửi dữ liệu vào Kafka.
- Flink sử dụng `event_time_utc` để xử lý event time.
- `event_time_utc` không được lớn hơn `produced_at_utc`.

## Identity and ordering

- `event_id` xác định duy nhất một sự kiện.
- `sequence_number` tăng dần theo từng `device_id`.
- Kafka partition key là `city_id`.
- Các event cùng thành phố được gửi vào cùng một partition.

## Routing

- Event hợp lệ: `environment.sensor-readings.raw`
- Sai JSON hoặc sai schema: `environment.sensor-readings.dlq`
- Event đến quá muộn: `environment.sensor-readings.late`

## Measurement units

- `temperature_2m`: độ C
- `relative_humidity_2m`: phần trăm
- `pm2_5`: microgram trên mét khối
- `pm10`: microgram trên mét khối
- `carbon_monoxide`: microgram trên mét khối
- `nitrogen_dioxide`: microgram trên mét khối
