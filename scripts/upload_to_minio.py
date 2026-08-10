import os
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error


# ============================================================
# PROJECT PATH
# ============================================================

# File hiện tại:
# project/scripts/upload_to_minio.py
#
# PROJECT_ROOT:
# project/
PROJECT_ROOT = Path(__file__).resolve().parents[1]

ENV_FILE = PROJECT_ROOT / ".env"

# Chạy trong WSL: đọc cấu hình từ .env.
# Chạy trong Airflow: không ghi đè biến Docker Compose đã truyền.
load_dotenv(
    dotenv_path=ENV_FILE,
    override=False,
)


# ============================================================
# MINIO CONFIGURATION
# ============================================================

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "localhost:9000",
)

# Hỗ trợ cả hai cách đặt tên biến:
# MINIO_ACCESS_KEY hoặc MINIO_ROOT_USER.
MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    os.getenv(
        "MINIO_ROOT_USER",
        "minioadmin",
    ),
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    os.getenv(
        "MINIO_ROOT_PASSWORD",
        "minioadmin",
    ),
)

BUCKET_NAME = os.getenv(
    "MINIO_BUCKET",
    "environment-data",
)

MINIO_SECURE = (
    os.getenv(
        "MINIO_SECURE",
        "false",
    )
    .strip()
    .lower()
    == "true"
)


# ============================================================
# LOCAL RAW DIRECTORY
# ============================================================

def resolve_local_raw_directory() -> Path:
    """
    Xác định thư mục Raw local.

    Mặc định:
        <project_root>/data/raw

    Có thể ghi đè bằng:
        LOCAL_RAW_DIR
    """

    configured_path = os.getenv(
        "LOCAL_RAW_DIR",
        "data/raw",
    )

    raw_path = Path(
        configured_path
    )

    # Nếu LOCAL_RAW_DIR là đường dẫn tương đối,
    # tính từ thư mục gốc project.
    if not raw_path.is_absolute():
        raw_path = (
            PROJECT_ROOT
            / raw_path
        )

    return raw_path.resolve()


LOCAL_RAW_DIR = resolve_local_raw_directory()


# ============================================================
# MINIO CLIENT
# ============================================================

def create_minio_client() -> Minio:
    """
    Tạo MinIO client.
    """

    print(
        f"Connecting to MinIO endpoint: "
        f"{MINIO_ENDPOINT}"
    )

    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def ensure_bucket_exists(
    client: Minio,
) -> None:
    """
    Tạo bucket nếu chưa tồn tại.
    """

    if client.bucket_exists(
        BUCKET_NAME
    ):
        print(
            f"Bucket already exists: "
            f"{BUCKET_NAME}"
        )

        return

    client.make_bucket(
        BUCKET_NAME
    )

    print(
        f"Created bucket: "
        f"{BUCKET_NAME}"
    )


# ============================================================
# RAW FILE DISCOVERY
# ============================================================

def get_raw_json_files() -> list[Path]:
    """
    Tìm toàn bộ JSON trong data/raw.

    Bao gồm:
        data/raw/air_quality/**/*.json
        data/raw/weather/**/*.json
        và các dataset Raw khác sau này.
    """

    if not LOCAL_RAW_DIR.exists():
        raise FileNotFoundError(
            "Local Raw directory does not exist: "
            f"{LOCAL_RAW_DIR}"
        )

    json_files = sorted(
        LOCAL_RAW_DIR.rglob(
            "*.json"
        )
    )

    if not json_files:
        raise FileNotFoundError(
            "No Raw JSON files found under: "
            f"{LOCAL_RAW_DIR}"
        )

    return json_files


def get_dataset_name(
    file_path: Path,
) -> str:
    """
    Lấy tên dataset từ thư mục đầu tiên dưới data/raw.

    Ví dụ:

        data/raw/air_quality/country=DE/file.json
        → air_quality

        data/raw/weather/country=DE/file.json
        → weather
    """

    relative_path = file_path.relative_to(
        LOCAL_RAW_DIR
    )

    if len(relative_path.parts) < 2:
        return "unknown"

    return relative_path.parts[0]


def print_dataset_summary(
    json_files: list[Path],
) -> None:
    """
    In số lượng file theo từng dataset.
    """

    dataset_counts = Counter(
        get_dataset_name(file_path)
        for file_path in json_files
    )

    print("\nRaw datasets found:")

    for dataset_name in sorted(
        dataset_counts
    ):
        print(
            f"- {dataset_name}: "
            f"{dataset_counts[dataset_name]} files"
        )


# ============================================================
# UPLOAD
# ============================================================

def build_object_name(
    file_path: Path,
) -> str:
    """
    Giữ nguyên cấu trúc thư mục dưới data/raw.

    Ví dụ local:

        data/raw/weather/
        country=DE/city=berlin/date=.../weather.json

    Object MinIO:

        raw/weather/
        country=DE/city=berlin/date=.../weather.json
    """

    relative_path = (
        file_path
        .relative_to(
            LOCAL_RAW_DIR
        )
        .as_posix()
    )

    return f"raw/{relative_path}"


def upload_file(
    client: Minio,
    file_path: Path,
) -> str:
    """
    Upload một Raw JSON lên MinIO.
    """

    object_name = build_object_name(
        file_path
    )

    client.fput_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        file_path=str(file_path),
        content_type="application/json",
    )

    print(
        f"Uploaded: {object_name}"
    )

    return object_name


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print("=" * 72)
    print("Upload all Raw JSON datasets to MinIO")
    print("=" * 72)

    print(
        f"Project root: "
        f"{PROJECT_ROOT}"
    )

    print(
        f"Local Raw directory: "
        f"{LOCAL_RAW_DIR}"
    )

    print(
        f"MinIO destination: "
        f"{BUCKET_NAME}/raw/"
    )

    json_files = get_raw_json_files()

    print(
        f"Total Raw JSON files found: "
        f"{len(json_files)}"
    )

    print_dataset_summary(
        json_files
    )

    print("=" * 72)

    client = create_minio_client()

    ensure_bucket_exists(
        client
    )

    success_count = 0

    failed_files: list[
        tuple[Path, str]
    ] = []

    uploaded_by_dataset: Counter[str] = (
        Counter()
    )

    for file_path in json_files:
        dataset_name = get_dataset_name(
            file_path
        )

        try:
            upload_file(
                client=client,
                file_path=file_path,
            )

            success_count += 1

            uploaded_by_dataset[
                dataset_name
            ] += 1

        except S3Error as error:
            failed_files.append(
                (
                    file_path,
                    str(error),
                )
            )

            print(
                "MinIO upload failed: "
                f"{file_path} - {error}"
            )

        except Exception as error:
            failed_files.append(
                (
                    file_path,
                    str(error),
                )
            )

            print(
                "Unexpected upload error: "
                f"{file_path} - {error}"
            )

    print("\n" + "=" * 72)
    print("Raw upload summary")
    print("=" * 72)

    print(
        f"Total files:   "
        f"{len(json_files)}"
    )

    print(
        f"Success files: "
        f"{success_count}"
    )

    print(
        f"Failed files:  "
        f"{len(failed_files)}"
    )

    print("\nUploaded by dataset:")

    for dataset_name in sorted(
        uploaded_by_dataset
    ):
        print(
            f"- {dataset_name}: "
            f"{uploaded_by_dataset[dataset_name]} files"
        )

    if failed_files:
        print("\nFailed file details:")

        for (
            file_path,
            error_message,
        ) in failed_files:
            print(
                f"- {file_path}: "
                f"{error_message}"
            )

        # Airflow phải đánh dấu task Failed
        # nếu có bất kỳ file nào upload lỗi.
        raise RuntimeError(
            f"{len(failed_files)} "
            "Raw JSON files failed to upload."
        )

    if success_count != len(json_files):
        raise RuntimeError(
            "Uploaded file count does not match "
            "the discovered Raw file count."
        )

    print(
        "\nAll Raw JSON datasets were "
        "uploaded successfully."
    )


if __name__ == "__main__":
    main()