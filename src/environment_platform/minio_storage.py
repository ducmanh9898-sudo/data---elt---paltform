import json
import re
import unicodedata
from datetime import datetime
from io import BytesIO
from typing import Any

from minio import Minio

from environment_platform.config import Settings


def create_minio_client(
    settings: Settings,
) -> Minio:
    """
    Tạo MinIO client từ cấu hình dùng chung.
    """
    return Minio(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def validate_minio_bucket(
    client: Minio,
    bucket_name: str,
) -> None:
    """
    Dừng crawler nếu bucket chưa tồn tại.

    Không tự tạo bucket để tránh âm thầm ghi dữ liệu
    vào một môi trường cấu hình sai.
    """
    if not client.bucket_exists(bucket_name):
        raise RuntimeError(
            f"MinIO bucket does not exist: {bucket_name}"
        )


def slugify(value: str) -> str:
    """
    Chuyển tên thành phố thành chuỗi an toàn cho object key.
    """
    normalized = unicodedata.normalize(
        "NFKD",
        value,
    )

    ascii_value = normalized.encode(
        "ascii",
        "ignore",
    ).decode("ascii")

    slug = re.sub(
        r"[^a-z0-9]+",
        "_",
        ascii_value.lower().strip(),
    ).strip("_")

    return slug or "unknown"


def build_raw_object_name(
    dataset: str,
    country_code: str,
    city_name: str,
    crawled_at_utc: datetime,
    run_id: str,
) -> str:
    """
    Tạo object key dạng:

    raw/air_quality/
      country=US/
      city=new_york/
      crawl_date=2026-07-24/
      air_quality_<timestamp>_<run_id>.json
    """
    crawl_date = crawled_at_utc.strftime(
        "%Y-%m-%d"
    )

    timestamp = crawled_at_utc.strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    city_slug = slugify(city_name)

    return (
        f"raw/{dataset}/"
        f"country={country_code}/"
        f"city={city_slug}/"
        f"crawl_date={crawl_date}/"
        f"{dataset}_{timestamp}_{run_id}.json"
    )


def upload_json_object(
    client: Minio,
    bucket_name: str,
    object_name: str,
    payload: dict[str, Any],
    object_metadata: dict[str, str],
) -> str:
    """
    Serialize JSON thành bytes và upload trực tiếp lên MinIO.

    Trả về ETag khi upload và xác nhận object thành công.
    """
    json_bytes = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    result = client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=BytesIO(json_bytes),
        length=len(json_bytes),
        content_type="application/json",
        metadata=object_metadata,
    )

    # Xác nhận object thực sự tồn tại và đúng kích thước.
    object_stat = client.stat_object(
        bucket_name=bucket_name,
        object_name=object_name,
    )

    if object_stat.size != len(json_bytes):
        raise RuntimeError(
            "Uploaded object size does not match: "
            f"expected={len(json_bytes)}, "
            f"actual={object_stat.size}, "
            f"object={object_name}"
        )

    return result.etag