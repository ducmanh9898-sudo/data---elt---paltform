


import time
import unicodedata
from datetime import datetime, timezone

from typing import Any

import psycopg
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from environment_platform.config import get_settings
from uuid import uuid4

from minio import Minio

from environment_platform.minio_storage import (
    build_raw_object_name,
    create_minio_client,
    upload_json_object,
    validate_minio_bucket,
)


# ============================================================
# CONFIGURATION
# ============================================================



settings = get_settings()

# ============================================================
# CONFIG VALIDATION
# ============================================================

OPEN_METEO_WEATHER_URL = (
    settings.open_meteo_weather_url
)



WEATHER_PAST_DAYS = (
    settings.weather_past_days
)

WEATHER_FORECAST_DAYS = (
    settings.weather_forecast_days
)

REQUEST_TIMEOUT_SECONDS = (
    settings.weather_timeout_seconds
)

REQUEST_DELAY_SECONDS = (
    settings.weather_request_delay_seconds
)


# ============================================================
# POSTGRESQL
# ============================================================

def get_cities() -> list[dict[str, Any]]:
    """
    Đọc danh sách thành phố từ PostgreSQL Backend Database.
    """

    query = """
        SELECT
            c.city_id,
            c.city_name,
            co.country_code,
            co.country_name,
            c.latitude,
            c.longitude
        FROM cities AS c
        INNER JOIN countries AS co
            ON c.country_id = co.country_id
        ORDER BY
            co.country_code,
            c.city_name;
    """

    cities: list[dict[str, Any]] = []

    with psycopg.connect(
        **settings.postgres_config
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)

            rows = cursor.fetchall()

            for row in rows:
                cities.append(
                    {
                        "city_id": int(row[0]),
                        "city_name": row[1],
                        "country_code": row[2],
                        "country_name": row[3],
                        "latitude": float(row[4]),
                        "longitude": float(row[5]),
                    }
                )

    if not cities:
        raise ValueError(
            "No cities found in PostgreSQL"
        )

    return cities


# ============================================================
# HTTP SESSION
# ============================================================

def create_http_session() -> requests.Session:
    """
    Tạo HTTP session có retry khi gặp lỗi tạm thời.
    """

    retry_policy = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1,
        status_forcelist=[
            429,
            500,
            502,
            503,
            504,
        ],
        allowed_methods=frozenset(
            ["GET"]
        ),
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry_policy
    )

    session = requests.Session()

    session.mount(
        "https://",
        adapter,
    )

    session.mount(
        "http://",
        adapter,
    )

    session.headers.update(
        {
            "User-Agent": (
                "environment-data-platform/"
                "1.0-weather-crawler"
            )
        }
    )

    return session

HOURLY_WEATHER_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "surface_pressure",
    "cloud_cover",
    "wind_speed_10m",
    "wind_direction_10m",
    "visibility",
    "weather_code",
]

# ============================================================
# WEATHER API
# ============================================================

def validate_weather_payload(
    city: dict[str, Any],
    payload: dict[str, Any],
) -> None:
    """
    Kiểm tra cấu trúc JSON trả về từ API.
    """

    hourly = payload.get("hourly")

    if not isinstance(hourly, dict):
        raise ValueError(
            f"Missing hourly object for "
            f"{city['city_name']}"
        )

    hourly_times = hourly.get("time")

    if not isinstance(
        hourly_times,
        list,
    ):
        raise ValueError(
            f"Missing hourly.time array for "
            f"{city['city_name']}"
        )

    if not hourly_times:
        raise ValueError(
            f"Empty hourly.time array for "
            f"{city['city_name']}"
        )

    missing_variables = [
        variable
        for variable in HOURLY_WEATHER_VARIABLES
        if variable not in hourly
    ]

    if missing_variables:
        raise ValueError(
            f"Missing weather variables for "
            f"{city['city_name']}: "
            f"{missing_variables}"
        )

    expected_length = len(
        hourly_times
    )

    different_length_variables = []

    for variable in HOURLY_WEATHER_VARIABLES:
        values = hourly.get(variable)

        if (
            isinstance(values, list)
            and len(values) != expected_length
        ):
            different_length_variables.append(
                {
                    "variable": variable,
                    "expected": expected_length,
                    "actual": len(values),
                }
            )

    if different_length_variables:
        print(
            "  Warning: weather arrays have "
            "different lengths:"
        )

        for item in different_length_variables:
            print(
                f"  - {item['variable']}: "
                f"expected={item['expected']}, "
                f"actual={item['actual']}"
            )

    print(
        f"  API hourly records: "
        f"{expected_length}"
    )


def crawl_weather(
    session: requests.Session,
    city: dict[str, Any],
) -> dict[str, Any]:
    """
    Gọi Open-Meteo Weather API cho một thành phố.
    """

    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],

        "hourly": ",".join(
            HOURLY_WEATHER_VARIABLES
        ),

        # API trả timestamp theo múi giờ địa phương.
        "timezone": "auto",

        "past_days": WEATHER_PAST_DAYS,
        "forecast_days": WEATHER_FORECAST_DAYS,

        "temperature_unit": "celsius",
        "wind_speed_unit": "ms",
        "precipitation_unit": "mm",
    }

    response = session.get(
        OPEN_METEO_WEATHER_URL,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    response.raise_for_status()

    payload = response.json()

    validate_weather_payload(
        city=city,
        payload=payload,
    )

    return payload


# ============================================================
# RAW JSON
# ============================================================



def build_raw_payload(
    city: dict[str, Any],
    weather_data: dict[str, Any],
    run_id: str,
    crawled_at_utc: datetime,
) -> dict[str, Any]:
    return {
        "metadata": {
            "run_id": run_id,
            "schema_version": 1,

            "city_id": city["city_id"],
            "city_name": city["city_name"],
            "country_code": city["country_code"],
            "country_name": city["country_name"],
            "latitude": city["latitude"],
            "longitude": city["longitude"],

            "source": "open_meteo_weather",
            "source_system": "open_meteo",
            "dataset_name": "weather_hourly",
            "dataset_kind": "weather_model",

            "requested_past_days": (
                WEATHER_PAST_DAYS
            ),
            "requested_forecast_days": (
                WEATHER_FORECAST_DAYS
            ),

            "source_endpoint": (
                OPEN_METEO_WEATHER_URL
            ),

            "crawled_at_utc": (
                crawled_at_utc
                .isoformat()
                .replace("+00:00", "Z")
            ),
        },
        "data": weather_data,
    }

# ============================================================
# MAIN
# ============================================================
def upload_weather_raw(
    minio_client: Minio,
    city: dict[str, Any],
    weather_data: dict[str, Any],
    run_id: str,
) -> str:
    crawled_at_utc = datetime.now(
        timezone.utc
    )

    object_name = build_raw_object_name(
        dataset="weather",
        country_code=city["country_code"],
        city_name=city["city_name"],
        crawled_at_utc=crawled_at_utc,
        run_id=run_id,
    )

    payload = build_raw_payload(
        city=city,
        weather_data=weather_data,
        run_id=run_id,
        crawled_at_utc=crawled_at_utc,
    )

    upload_json_object(
        client=minio_client,
        bucket_name=settings.minio_bucket,
        object_name=object_name,
        payload=payload,
        object_metadata={
            "run-id": run_id,
            "schema-version": "1",
            "source": "open_meteo_weather",
            "city-id": str(city["city_id"]),
            "country-code": city["country_code"],
            "crawled-at-utc": (
                crawled_at_utc
                .isoformat()
                .replace("+00:00", "Z")
            ),
        },
    )

    return object_name

def main() -> None:
   

    run_id = uuid4().hex

    minio_client = create_minio_client(
        settings
    )

    validate_minio_bucket(
        client=minio_client,
        bucket_name=settings.minio_bucket,
    )

    print("=" * 72)
    print("Open-Meteo Weather Raw Crawler")
    print("=" * 72)
    print(f"Run ID: {run_id}")
    print(
        "MinIO destination: "
        f"{settings.minio_bucket}/raw/weather/"
    )
    print(f"Past days: {WEATHER_PAST_DAYS}")
    print(
        f"Forecast days: "
        f"{WEATHER_FORECAST_DAYS}"
    )

    cities = get_cities()

    print(
        "Cities found in PostgreSQL: "
        f"{len(cities)}"
    )

    successful_objects: list[str] = []

    failed_cities: list[
        tuple[str, str]
    ] = []

    with create_http_session() as session:
        for index, city in enumerate(
            cities,
            start=1,
        ):
            city_label = (
                f"{city['city_name']}, "
                f"{city['country_name']}"
            )

            print(
                f"\n[{index}/{len(cities)}] "
                f"Crawling weather: "
                f"{city_label}"
            )

            try:
                weather_data = crawl_weather(
                    session=session,
                    city=city,
                )

                object_name = upload_weather_raw(
                    minio_client=minio_client,
                    city=city,
                    weather_data=weather_data,
                    run_id=run_id,
                )

                successful_objects.append(
                    object_name
                )

                print(
                    "  Uploaded: "
                    f"{settings.minio_bucket}/"
                    f"{object_name}"
                )

            except Exception as error:
                failed_cities.append(
                    (
                        city_label,
                        str(error),
                    )
                )

                print(
                    "  FAILED: "
                    f"{type(error).__name__}: "
                    f"{error}"
                )

            if (
                REQUEST_DELAY_SECONDS > 0
                and index < len(cities)
            ):
                time.sleep(
                    REQUEST_DELAY_SECONDS
                )

    print("\n" + "=" * 72)
    print("Weather crawl summary")
    print("=" * 72)

    print(
        f"Total cities:      "
        f"{len(cities)}"
    )
    print(
        f"Successful cities: "
        f"{len(successful_objects)}"
    )
    print(
        f"Failed cities:     "
        f"{len(failed_cities)}"
    )
    print(
        f"MinIO raw objects: "
        f"{len(successful_objects)}"
    )

    if failed_cities:
        print("\nFailed city details:")

        for (
            city_label,
            error_message,
        ) in failed_cities:
            print(
                f"- {city_label}: "
                f"{error_message}"
            )

        raise RuntimeError(
            "Weather extraction completed with "
            f"{len(failed_cities)} failed cities"
        )

    print(
        "\nWeather Raw extraction "
        "completed successfully."
    )


if __name__ == "__main__":
    main()