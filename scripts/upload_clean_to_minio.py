import io
import os
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from minio.deleteobjects import DeleteObject
from minio.error import S3Error


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parents[1]

load_dotenv(
    dotenv_path=PROJECT_DIR / ".env",
    override=False,
)


# Danh sách các Clean dataset cần upload.
#
# Không cần truyền tham số khi chạy.
# Script sẽ lần lượt xử lý tất cả dataset bên dưới.
CLEAN_DATASETS = {
    "air_quality": {
        "local_directory": (
            PROJECT_DIR
            / "data"
            / "clean"
            / "air_quality"
        ),
        "object_prefix": "clean/air_quality",
    },
    "weather": {
        "local_directory": (
            PROJECT_DIR
            / "data"
            / "clean"
            / "weather"
        ),
        "object_prefix": "clean/weather",
    },
}


# ============================================================
# ENVIRONMENT HELPERS
# ============================================================

def get_boolean_env(
    variable_name: str,
    default: bool = False,
) -> bool:
    """
    Chuyển biến môi trường dạng chuỗi thành boolean.
    """

    value = os.getenv(
        variable_name,
        str(default),
    )

    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }


# ============================================================
# MINIO CLIENT
# ============================================================

def create_minio_client() -> tuple[Minio, str]:
    """
    Tạo MinIO client từ các biến môi trường.
    """

    endpoint = os.getenv(
        "MINIO_ENDPOINT",
        "localhost:9000",
    )

    # MinIO Python client chỉ nhận host:port,
    # không nhận http:// hoặc https://.
    endpoint = (
        endpoint
        .replace("http://", "")
        .replace("https://", "")
        .rstrip("/")
    )

    access_key = (
        os.getenv("MINIO_ACCESS_KEY")
        or os.getenv("MINIO_ROOT_USER")
    )

    secret_key = (
        os.getenv("MINIO_SECRET_KEY")
        or os.getenv("MINIO_ROOT_PASSWORD")
    )

    bucket_name = os.getenv(
        "MINIO_BUCKET",
        "environment-data",
    )

    secure = get_boolean_env(
        "MINIO_SECURE",
        default=False,
    )

    if not access_key:
        raise ValueError(
            "Missing MINIO_ACCESS_KEY or "
            "MINIO_ROOT_USER in environment."
        )

    if not secret_key:
        raise ValueError(
            "Missing MINIO_SECRET_KEY or "
            "MINIO_ROOT_PASSWORD in environment."
        )

    print(
        f"Connecting to MinIO: {endpoint}"
    )

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

    return client, bucket_name


def validate_bucket(
    client: Minio,
    bucket_name: str,
) -> None:
    """
    Kiểm tra bucket đã tồn tại.
    Script không tự tạo bucket mới.
    """

    if not client.bucket_exists(
        bucket_name
    ):
        raise RuntimeError(
            f"Bucket does not exist: "
            f"{bucket_name}"
        )

    print(
        f"Bucket found: {bucket_name}"
    )


# ============================================================
# LOCAL CLEAN VALIDATION
# ============================================================

def validate_local_clean_zone(
    dataset_name: str,
    local_clean_directory: Path,
) -> list[Path]:
    """
    Kiểm tra Clean output của một dataset.

    Yêu cầu:
        thư mục Clean tồn tại
        có Spark _SUCCESS marker
        có ít nhất một file Parquet
    """

    if not local_clean_directory.exists():
        raise FileNotFoundError(
            f"Clean directory does not exist "
            f"for dataset {dataset_name}: "
            f"{local_clean_directory}"
        )

    success_file = (
        local_clean_directory
        / "_SUCCESS"
    )

    if not success_file.exists():
        raise FileNotFoundError(
            f"Spark _SUCCESS marker was not found "
            f"for dataset {dataset_name}. "
            "The Clean output may be incomplete."
        )

    parquet_files = sorted(
        local_clean_directory.rglob(
            "*.parquet"
        )
    )

    if not parquet_files:
        raise FileNotFoundError(
            f"No Parquet files found for "
            f"dataset {dataset_name} under: "
            f"{local_clean_directory}"
        )

    return parquet_files


# ============================================================
# CLEAN PREFIX REMOVAL
# ============================================================

def clear_existing_clean_prefix(
    client: Minio,
    bucket_name: str,
    object_prefix: str,
) -> None:
    """
    Xóa toàn bộ object cũ dưới một Clean prefix.

    Ví dụ:
        clean/air_quality/
        clean/weather/

    Không ảnh hưởng tới Raw Zone hoặc dataset khác.
    """

    full_prefix = (
        f"{object_prefix.rstrip('/')}/"
    )

    existing_objects = list(
        client.list_objects(
            bucket_name=bucket_name,
            prefix=full_prefix,
            recursive=True,
        )
    )

    if not existing_objects:
        print(
            f"No existing objects under "
            f"{bucket_name}/{full_prefix}"
        )

        return

    print(
        f"Removing {len(existing_objects)} "
        f"old objects under "
        f"{bucket_name}/{full_prefix}"
    )

    delete_list = (
        DeleteObject(
            obj.object_name
        )
        for obj in existing_objects
    )

    delete_errors = list(
        client.remove_objects(
            bucket_name=bucket_name,
            delete_object_list=delete_list,
        )
    )

    if delete_errors:
        error_messages = "\n".join(
            (
                f"{error.object_name}: "
                f"{error.code} - "
                f"{error.message}"
            )
            for error in delete_errors
        )

        raise RuntimeError(
            "Failed to remove some old "
            "Clean objects:\n"
            f"{error_messages}"
        )

    print(
        f"Old objects removed: "
        f"{full_prefix}"
    )


# ============================================================
# OBJECT NAME
# ============================================================

def build_object_name(
    file_path: Path,
    local_clean_directory: Path,
    object_prefix: str,
) -> str:
    """
    Giữ nguyên cấu trúc partition khi upload.

    Ví dụ local:

        data/clean/weather/
        country_code=DE/
        measurement_date_local=2026-07-11/
        part-....parquet

    Object MinIO:

        clean/weather/
        country_code=DE/
        measurement_date_local=2026-07-11/
        part-....parquet
    """

    relative_path = (
        file_path
        .relative_to(
            local_clean_directory
        )
        .as_posix()
    )

    return (
        f"{object_prefix.rstrip('/')}/"
        f"{relative_path}"
    )


# ============================================================
# PARQUET UPLOAD
# ============================================================

def upload_parquet_files(
    client: Minio,
    bucket_name: str,
    parquet_files: list[Path],
    local_clean_directory: Path,
    object_prefix: str,
) -> int:
    """
    Upload toàn bộ Parquet của một dataset.

    Trả về tổng số byte đã upload.
    """

    total_files = len(
        parquet_files
    )

    total_bytes = 0

    for index, file_path in enumerate(
        parquet_files,
        start=1,
    ):
        object_name = build_object_name(
            file_path=file_path,
            local_clean_directory=(
                local_clean_directory
            ),
            object_prefix=object_prefix,
        )

        file_size = (
            file_path.stat().st_size
        )

        total_bytes += file_size

        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=str(file_path),
            content_type=(
                "application/octet-stream"
            ),
        )

        print(
            f"[{index}/{total_files}] "
            f"Uploaded: {object_name}"
        )

    print(
        f"Uploaded {total_files} "
        f"Parquet files "
        f"({total_bytes:,} bytes)."
    )

    return total_bytes


# ============================================================
# SUCCESS MARKER
# ============================================================

def upload_success_marker(
    client: Minio,
    bucket_name: str,
    object_prefix: str,
) -> None:
    """
    Upload _SUCCESS sau khi tất cả Parquet
    của dataset đã được upload thành công.

    Nếu quá trình upload lỗi giữa chừng,
    marker sẽ không được tạo.
    """

    object_name = (
        f"{object_prefix.rstrip('/')}/"
        "_SUCCESS"
    )

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=io.BytesIO(b""),
        length=0,
        content_type=(
            "application/octet-stream"
        ),
    )

    print(
        f"Uploaded marker: "
        f"{object_name}"
    )


# ============================================================
# UPLOAD VERIFICATION
# ============================================================

def verify_upload(
    client: Minio,
    bucket_name: str,
    object_prefix: str,
    expected_parquet_count: int,
) -> None:
    """
    Kiểm tra lại số file Parquet và _SUCCESS
    trên MinIO.
    """

    full_prefix = (
        f"{object_prefix.rstrip('/')}/"
    )

    uploaded_objects = list(
        client.list_objects(
            bucket_name=bucket_name,
            prefix=full_prefix,
            recursive=True,
        )
    )

    uploaded_parquet = [
        obj
        for obj in uploaded_objects
        if obj.object_name.endswith(
            ".parquet"
        )
    ]

    success_marker_name = (
        f"{full_prefix}_SUCCESS"
    )

    success_marker_exists = any(
        obj.object_name
        == success_marker_name
        for obj in uploaded_objects
    )

    print("=" * 72)

    print(
        f"MinIO prefix: "
        f"{bucket_name}/{full_prefix}"
    )

    print(
        f"Expected Parquet files: "
        f"{expected_parquet_count}"
    )

    print(
        f"Uploaded Parquet files: "
        f"{len(uploaded_parquet)}"
    )

    print(
        f"_SUCCESS marker exists: "
        f"{success_marker_exists}"
    )

    print("=" * 72)

    if (
        len(uploaded_parquet)
        != expected_parquet_count
    ):
        raise RuntimeError(
            "Uploaded Parquet count does "
            "not match the local file count "
            f"for prefix {object_prefix}."
        )

    if not success_marker_exists:
        raise RuntimeError(
            "MinIO _SUCCESS marker was "
            "not found for prefix "
            f"{object_prefix}."
        )


# ============================================================
# ONE DATASET UPLOAD
# ============================================================

def upload_clean_dataset(
    client: Minio,
    bucket_name: str,
    dataset_name: str,
    local_clean_directory: Path,
    object_prefix: str,
) -> dict[str, int]:
    """
    Thực hiện toàn bộ quá trình upload
    cho một Clean dataset.
    """

    print("\n" + "=" * 72)

    print(
        f"Uploading Clean dataset: "
        f"{dataset_name}"
    )

    print("=" * 72)

    print(
        f"Local directory: "
        f"{local_clean_directory}"
    )

    print(
        f"MinIO destination: "
        f"{bucket_name}/{object_prefix}/"
    )

    parquet_files = (
        validate_local_clean_zone(
            dataset_name=dataset_name,
            local_clean_directory=(
                local_clean_directory
            ),
        )
    )

    print(
        f"Local Parquet files: "
        f"{len(parquet_files)}"
    )

    clear_existing_clean_prefix(
        client=client,
        bucket_name=bucket_name,
        object_prefix=object_prefix,
    )

    total_bytes = upload_parquet_files(
        client=client,
        bucket_name=bucket_name,
        parquet_files=parquet_files,
        local_clean_directory=(
            local_clean_directory
        ),
        object_prefix=object_prefix,
    )

    upload_success_marker(
        client=client,
        bucket_name=bucket_name,
        object_prefix=object_prefix,
    )

    verify_upload(
        client=client,
        bucket_name=bucket_name,
        object_prefix=object_prefix,
        expected_parquet_count=(
            len(parquet_files)
        ),
    )

    print(
        f"Clean dataset uploaded "
        f"successfully: {dataset_name}"
    )

    return {
        "file_count": len(
            parquet_files
        ),
        "total_bytes": total_bytes,
    }


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print("=" * 72)

    print(
        "Upload all Clean Parquet "
        "datasets to MinIO"
    )

    print("=" * 72)

    client, bucket_name = (
        create_minio_client()
    )

    try:
        validate_bucket(
            client=client,
            bucket_name=bucket_name,
        )

        results: dict[
            str,
            dict[str, int],
        ] = {}

        for (
            dataset_name,
            dataset_config,
        ) in CLEAN_DATASETS.items():
            results[dataset_name] = (
                upload_clean_dataset(
                    client=client,
                    bucket_name=bucket_name,
                    dataset_name=dataset_name,
                    local_clean_directory=(
                        dataset_config[
                            "local_directory"
                        ]
                    ),
                    object_prefix=(
                        dataset_config[
                            "object_prefix"
                        ]
                    ),
                )
            )

        print("\n" + "=" * 72)

        print(
            "All Clean datasets upload summary"
        )

        print("=" * 72)

        total_files = 0
        total_bytes = 0

        for (
            dataset_name,
            result,
        ) in results.items():
            file_count = result[
                "file_count"
            ]

            dataset_bytes = result[
                "total_bytes"
            ]

            total_files += file_count
            total_bytes += dataset_bytes

            print(
                f"- {dataset_name}: "
                f"{file_count} files, "
                f"{dataset_bytes:,} bytes"
            )

        print("-" * 72)

        print(
            f"Total uploaded files: "
            f"{total_files}"
        )

        print(
            f"Total uploaded bytes: "
            f"{total_bytes:,}"
        )

        print(
            "\nAll Clean Parquet datasets "
            "were uploaded successfully."
        )

    except S3Error as error:
        raise RuntimeError(
            "MinIO operation failed: "
            f"{error.code} - "
            f"{error.message}"
        ) from error


if __name__ == "__main__":
    main()