import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


def load_environment() -> None:
    """Đọc cấu hình project từ file .env."""

    if not ENV_FILE.is_file():
        raise FileNotFoundError(
            f"Environment file not found: {ENV_FILE}"
        )

    load_dotenv(
        dotenv_path=ENV_FILE,
        override=False,
    )


def get_required_env(name: str) -> str:
    """Lấy biến môi trường bắt buộc."""

    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}"
        )

    return value


def get_minio_credentials() -> tuple[str, str]:
    """Hỗ trợ cả tên biến access key và root user."""

    access_key = (
        os.getenv("MINIO_ACCESS_KEY")
        or os.getenv("MINIO_ROOT_USER")
    )

    secret_key = (
        os.getenv("MINIO_SECRET_KEY")
        or os.getenv("MINIO_ROOT_PASSWORD")
    )

    if not access_key:
        raise RuntimeError(
            "Missing MINIO_ACCESS_KEY or MINIO_ROOT_USER"
        )

    if not secret_key:
        raise RuntimeError(
            "Missing MINIO_SECRET_KEY or "
            "MINIO_ROOT_PASSWORD"
        )

    return access_key, secret_key


def normalize_endpoint(endpoint: str) -> str:
    """Bổ sung scheme nếu endpoint chỉ có host:port."""

    if endpoint.startswith(("http://", "https://")):
        return endpoint

    return f"http://{endpoint}"


def create_spark_session() -> SparkSession:
    """Tạo SparkSession local để kiểm tra S3A."""

    return (
        SparkSession.builder
        .appName("EnvironmentSparkMinIOConnectivityTest")
        .master("local[*]")
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .getOrCreate()
    )


def configure_s3a(
    spark: SparkSession,
    endpoint: str,
    access_key: str,
    secret_key: str,
    secure: bool,
) -> None:
    """Cấu hình Hadoop S3A để truy cập MinIO."""

    hadoop_conf = (
        spark.sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )

    hadoop_conf.set(
        "fs.s3a.endpoint",
        endpoint,
    )

    hadoop_conf.set(
        "fs.s3a.endpoint.region",
        "us-east-1",
    )

    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true",
    )

    hadoop_conf.set(
        "fs.s3a.access.key",
        access_key,
    )

    hadoop_conf.set(
        "fs.s3a.secret.key",
        secret_key,
    )

    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        str(secure).lower(),
    )


def main() -> None:
    load_environment()

    raw_endpoint = get_required_env(
        "MINIO_ENDPOINT"
    )

    bucket = os.getenv(
        "MINIO_BUCKET",
        "environment-data",
    )

    secure = (
        os.getenv(
            "MINIO_SECURE",
            "false",
        ).lower()
        == "true"
    )

    endpoint = normalize_endpoint(
        raw_endpoint
    )

    access_key, secret_key = (
        get_minio_credentials()
    )

    raw_path = (
        f"s3a://{bucket}/raw/air_quality/"
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        configure_s3a(
            spark=spark,
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

        print("=" * 70)
        print(f"Spark version: {spark.version}")
        print(
            "Hadoop version: "
            + spark.sparkContext._jvm
                .org.apache.hadoop.util.VersionInfo
                .getVersion()
        )
        print(f"MinIO endpoint: {endpoint}")
        print(f"Raw path: {raw_path}")
        print("=" * 70)

        # Mỗi crawler object là một JSON nhiều dòng,
        # không phải JSON Lines.
        raw_df = (
            spark.read
            .option("multiLine", True)
            .option(
                "recursiveFileLookup",
                True,
            )
            .json(raw_path)
        )

        raw_object_count = raw_df.count()

        print(
            f"Raw JSON objects read by Spark: "
            f"{raw_object_count}"
        )

        if raw_object_count == 0:
            raise RuntimeError(
                "Spark connected to MinIO but no "
                "Raw JSON object was read."
            )

        print("\nRaw schema:")
        raw_df.printSchema()

        print("\nSample metadata:")
        (
            raw_df
            .select(
                "metadata.city_id",
                "metadata.city_name",
                "metadata.country_code",
                "metadata.run_id",
                "metadata.schema_version",
                "metadata.crawled_at_utc",
            )
            .show(
                10,
                truncate=False,
            )
        )

        print("=" * 70)
        print("PASS: Spark read Raw JSON directly from MinIO")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()