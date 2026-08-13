from minio.error import S3Error

from datetime import datetime, timezone

from typing import Any
from environment_platform.config import get_settings
import psycopg
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from uuid import uuid4

from minio import Minio

from environment_platform.minio_storage import (
    build_raw_object_name,
    create_minio_client,
    upload_json_object,
    validate_minio_bucket,
)

# Đọc các biến môi trường từ file .env.



settings = get_settings()

OPEN_METEO_AIR_QUALITY_URL = (
    settings.open_meteo_air_quality_url
)



PAST_DAYS = settings.air_quality_past_days

FORECAST_DAYS = (
    settings.air_quality_forecast_days
)

REQUEST_TIMEOUT_SECONDS = (
    settings.air_quality_timeout_seconds
)
AIR_QUALITY_VARIABLES = [
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "us_aqi",
]

def get_cities() -> list[dict[str, Any]]:
    with psycopg.connect(
        **settings.postgres_config
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT
                    c.city_id,
                    c.city_name,
                    co.country_code,
                    co.country_name,
                    c.latitude,
                    c.longitude
                FROM cities AS c
                JOIN countries AS co
                    ON c.country_id = co.country_id
                ORDER BY
                    co.country_code,
                    c.city_name;
            """)

            rows = cursor.fetchall()

    cities = [
        {
            "city_id": int(row[0]),
            "city_name": row[1],
            "country_code": row[2],
            "country_name": row[3],
            "latitude": float(row[4]),
            "longitude": float(row[5]),
        }
        for row in rows
    ]

    if not cities:
        raise ValueError(
            "No cities found in PostgreSQL"
        )

    return cities

def create_http_session() -> requests.Session:
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
                "1.0-air-quality-crawler"
            )
        }
    )

    return session


def validate_air_quality_payload(
    city: dict[str, Any],
    payload: dict[str, Any],
) -> None:
    hourly = payload.get("hourly")

    if not isinstance(hourly, dict):
        raise ValueError(
            "Missing hourly object for "
            f"{city['city_name']}"
        )

    hourly_times = hourly.get("time")

    if not isinstance(
        hourly_times,
        list,
    ):
        raise ValueError(
            "Missing hourly.time array for "
            f"{city['city_name']}"
        )

    if not hourly_times:
        raise ValueError(
            "Empty hourly.time array for "
            f"{city['city_name']}"
        )

    expected_length = len(
        hourly_times
    )

    for variable in AIR_QUALITY_VARIABLES:
        values = hourly.get(
            variable
        )

        if not isinstance(
            values,
            list,
        ):
            raise ValueError(
                "Missing or invalid "
                f"{variable} array for "
                f"{city['city_name']}"
            )

        if len(values) != expected_length:
            raise ValueError(
                "Air-quality array length mismatch: "
                f"city={city['city_name']}, "
                f"variable={variable}, "
                f"expected={expected_length}, "
                f"actual={len(values)}"
            )

def crawl_air_quality(
    session: requests.Session,
    city: dict[str, Any],
) -> dict[str, Any]:
    """
    Gọi Open-Meteo Air Quality API cho một thành phố.

    timezone=auto:
        API trả các timestamp hourly theo múi giờ địa phương
        của thành phố.

    forecast_days=0:
        Không lấy các ngày dự báo tương lai vào pipeline
        historical hiện tại.
    """
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "hourly": ",".join(
    AIR_QUALITY_VARIABLES
)
        ,
        "timezone": "auto",
        "past_days": PAST_DAYS,
        "forecast_days": FORECAST_DAYS,
    }

    response =session.get(
    OPEN_METEO_AIR_QUALITY_URL,
    params=params,
    timeout=settings.air_quality_timeout_seconds,
)

    response.raise_for_status()

    payload = response.json()
    validate_air_quality_payload(
    city=city,
    payload=payload,
)
    # Kiểm tra response có cấu trúc tối thiểu cần thiết.
    hourly = payload.get("hourly")

    if not isinstance(hourly, dict):
        raise ValueError(
            "API response does not contain a valid hourly object"
        )

    hourly_times = hourly.get("time")

    if not isinstance(hourly_times, list):
        raise ValueError(
            "API response does not contain hourly.time"
        )

    if not hourly_times:
        raise ValueError(
            "API response contains no hourly records"
        )

    return payload




def build_raw_payload(
    city: dict[str, Any],
    api_data: dict[str, Any],
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

            "source": "open_meteo_air_quality",
            "source_system": "open_meteo",
            "dataset_name": "air_quality_hourly",
            "dataset_kind": "air_quality_model",

            "requested_past_days": PAST_DAYS,
            "requested_forecast_days": FORECAST_DAYS,

            "crawled_at_utc": (
                crawled_at_utc
                .isoformat()
                .replace("+00:00", "Z")
            ),
        },
        "data": api_data,
    }

def upload_air_quality_raw(
    minio_client: Minio,
    city: dict[str, Any],
    api_data: dict[str, Any],
    run_id: str,
) -> str:
    crawled_at_utc = datetime.now(
        timezone.utc
    )

    object_name = build_raw_object_name(
        dataset="air_quality",
        country_code=city["country_code"],
        city_name=city["city_name"],
        crawled_at_utc=crawled_at_utc,
        run_id=run_id,
    )

    payload = build_raw_payload(
        city=city,
        api_data=api_data,
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
            "source": "open_meteo_air_quality",
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

    cities = get_cities()

    print(f"Run ID: {run_id}")
    print(
        f"MinIO destination: "
        f"{settings.minio_bucket}/raw/air_quality/"
    )

    print(
        f"Found {len(cities)} cities "
        "from backend database."
    )

    successful_crawls = 0
    failed_crawls = 0

    # Session tái sử dụng HTTP connection giữa các request.
    with create_http_session() as session:
        for city in cities:
            print(
                "\nCrawling air quality: "
                f"{city['city_name']}, "
                f"{city['country_name']}"
            )

            try:
                data = crawl_air_quality(
                    session=session,
                    city=city,
                )

                object_name = upload_air_quality_raw(
                    minio_client=minio_client,
                    city=city,
                    api_data=data,
                    run_id=run_id,
                )

                hourly_record_count = len(
                    data["hourly"]["time"]
                )

                successful_crawls += 1

                print(
                    f"Hourly records: "
                    f"{hourly_record_count}"
                )
                print(
    f"Uploaded: "
    f"{settings.minio_bucket}/{object_name}"
)

            except (
    requests.RequestException,
    S3Error,
    RuntimeError,
    ValueError,
    OSError,
) as error:
                failed_crawls += 1

                print(
                    f"Failed: {city['city_name']} "
                    f"- {error}"
                )

    print("\n" + "=" * 70)
    print(f"Total cities:       {len(cities)}")
    print(f"Successful crawls:  {successful_crawls}")
    print(f"Failed crawls:      {failed_crawls}")
    print("=" * 70)

    if failed_crawls > 0:
        raise RuntimeError(
            f"{failed_crawls} city crawl(s) failed"
        )


if __name__ == "__main__":
    main()