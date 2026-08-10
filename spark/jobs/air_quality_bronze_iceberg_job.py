import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    arrays_zip,
    coalesce,
    col,
    explode_outer,
    input_file_name,
    lit,
    to_timestamp,
)


RAW_DIR = "s3a://environment-data/raw/air_quality/"

BRONZE_TABLE = (
    "polaris.bronze.air_quality_hourly_raw"
)

OUTPUT_PARTITIONS = 24


def parse_bool(
    value: str | None,
    default: bool = False,
) -> bool:
    if value is None:
        return default

    normalized = value.strip().lower()

    if normalized in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }:
        return True

    if normalized in {
        "0",
        "false",
        "no",
        "n",
        "off",
    }:
        return False

    raise ValueError(
        f"Invalid boolean value: {value!r}"
    )


def require_environment_variable(
    name: str,
) -> str:
    value = os.getenv(name)

    if (
        value is None
        or not value.strip()
        or value.strip() == "change_me"
    ):
        raise ValueError(
            f"Environment variable {name} is missing "
            "or invalid"
        )

    return value.strip()


def normalize_endpoint(
    endpoint: str,
    secure: bool,
) -> str:
    normalized = endpoint.strip().rstrip("/")

    if normalized.startswith(
        ("http://", "https://")
    ):
        return normalized

    scheme = "https" if secure else "http"

    return f"{scheme}://{normalized}"


def create_spark_session() -> SparkSession:
    minio_secure = parse_bool(
        os.getenv("MINIO_SECURE"),
        default=False,
    )

    minio_endpoint = normalize_endpoint(
        os.getenv(
            "MINIO_ENDPOINT_INTERNAL",
            os.getenv(
                "MINIO_ENDPOINT",
                "minio:9000",
            ),
        ),
        secure=minio_secure,
    )

    minio_access_key = (
        require_environment_variable(
            "MINIO_ACCESS_KEY"
        )
    )

    minio_secret_key = (
        require_environment_variable(
            "MINIO_SECRET_KEY"
        )
    )

    polaris_uri = (
        require_environment_variable(
            "POLARIS_URI"
        )
        .rstrip("/")
    )

    polaris_catalog_name = (
        require_environment_variable(
            "POLARIS_CATALOG_NAME"
        )
    )

    polaris_client_id = (
        require_environment_variable(
            "POLARIS_SPARK_CLIENT_ID"
        )
    )

    polaris_client_secret = (
        require_environment_variable(
            "POLARIS_SPARK_CLIENT_SECRET"
        )
    )

    polaris_scope = os.getenv(
        "POLARIS_SCOPE",
        "PRINCIPAL_ROLE:ALL",
    )

    aws_region = os.getenv(
        "AWS_REGION",
        "us-east-1",
    )

    oauth2_uri = (
        f"{polaris_uri}/v1/oauth/tokens"
    )

    return (
        SparkSession.builder
        .appName(
            "AirQualityBronzeIcebergJob"
        )

        # Iceberg REST Catalog qua Polaris.
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.polaris",
            "org.apache.iceberg.spark."
            "SparkCatalog",
        )
        .config(
            "spark.sql.catalog.polaris.type",
            "rest",
        )
        .config(
            "spark.sql.catalog.polaris.uri",
            polaris_uri,
        )
        .config(
            "spark.sql.catalog.polaris."
            "warehouse",
            polaris_catalog_name,
        )
        .config(
            "spark.sql.catalog.polaris."
            "credential",
            (
                f"{polaris_client_id}:"
                f"{polaris_client_secret}"
            ),
        )
        .config(
            "spark.sql.catalog.polaris."
            "oauth2-server-uri",
            oauth2_uri,
        )
        .config(
            "spark.sql.catalog.polaris.scope",
            polaris_scope,
        )
        .config(
            "spark.sql.catalog.polaris."
            "token-refresh-enabled",
            "false",
        )
        .config(
            "spark.sql.catalog.polaris."
            "client.region",
            aws_region,
        )

        # Iceberg S3FileIO ghi trực tiếp MinIO.
        .config(
            "spark.sql.catalog.polaris."
            "io-impl",
            "org.apache.iceberg.aws.s3."
            "S3FileIO",
        )
        .config(
            "spark.sql.catalog.polaris."
            "s3.endpoint",
            minio_endpoint,
        )
        .config(
            "spark.sql.catalog.polaris."
            "s3.path-style-access",
            "true",
        )
        .config(
            "spark.sql.catalog.polaris."
            "s3.access-key-id",
            minio_access_key,
        )
        .config(
            "spark.sql.catalog.polaris."
            "s3.secret-access-key",
            minio_secret_key,
        )

        # Hadoop S3A đọc Raw JSON từ MinIO.
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a."
            "S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            minio_endpoint,
        )
        .config(
            "spark.hadoop.fs.s3a."
            "path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a."
            "connection.ssl.enabled",
            str(minio_secure).lower(),
        )
        .config(
            "spark.hadoop.fs.s3a."
            "input.stream.type",
            "classic",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            minio_access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            minio_secret_key,
        )
        .config(
            "spark.hadoop.fs.s3a."
            "aws.credentials.provider",
            "org.apache.hadoop.fs.s3a."
            "SimpleAWSCredentialsProvider",
        )

        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(OUTPUT_PARTITIONS),
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
        .config(
            "spark.sql.adaptive."
            "coalescePartitions.enabled",
            "true",
        )
        .getOrCreate()
    )


def read_and_flatten_raw_data(
    spark: SparkSession,
) -> DataFrame:
    print("\nReading Air Quality Raw JSON:")
    print(RAW_DIR)

    raw_df = (
        spark.read
        .option("multiLine", "true")
        .option(
            "recursiveFileLookup",
            "true",
        )
        .option(
            "pathGlobFilter",
            "*.json",
        )
        .option("mode", "FAILFAST")
        .json(RAW_DIR)
    )

    zipped_df = raw_df.withColumn(
        "hourly_zipped",
        arrays_zip(
            col("data.hourly.time")
            .alias("time"),

            col("data.hourly.pm10")
            .alias("pm10"),

            col("data.hourly.pm2_5")
            .alias("pm2_5"),

            col(
                "data.hourly.carbon_monoxide"
            ).alias("carbon_monoxide"),

            col(
                "data.hourly.nitrogen_dioxide"
            ).alias("nitrogen_dioxide"),

            col(
                "data.hourly.sulphur_dioxide"
            ).alias("sulphur_dioxide"),

            col("data.hourly.ozone")
            .alias("ozone"),

            col("data.hourly.us_aqi")
            .alias("us_aqi"),
        ),
    )

    exploded_df = zipped_df.withColumn(
        "hourly_record",
        explode_outer(
            col("hourly_zipped")
        ),
    )

    return exploded_df.select(
        col("metadata.city_id")
        .cast("long")
        .alias("city_id"),

        col("metadata.city_name")
        .cast("string")
        .alias("city_name"),

        col("metadata.country_code")
        .cast("string")
        .alias("country_code"),

        col("metadata.country_name")
        .cast("string")
        .alias("country_name"),

        col("metadata.latitude")
        .cast("double")
        .alias("latitude"),

        col("metadata.longitude")
        .cast("double")
        .alias("longitude"),

        coalesce(
            col("metadata.source")
            .cast("string"),
            lit(
                "Open-Meteo Air Quality API"
            ),
        ).alias("source"),

        coalesce(
            col("metadata.source_system")
            .cast("string"),
            lit("open_meteo"),
        ).alias("source_system"),

        coalesce(
            col("metadata.dataset_name")
            .cast("string"),
            lit("air_quality_hourly"),
        ).alias("dataset_name"),

        coalesce(
            col("metadata.dataset_kind")
            .cast("string"),
            lit("air_quality_model"),
        ).alias("dataset_kind"),

        col(
            "metadata.requested_past_days"
        )
        .cast("integer")
        .alias("requested_past_days"),

        col(
            "metadata."
            "requested_forecast_days"
        )
        .cast("integer")
        .alias(
            "requested_forecast_days"
        ),

        input_file_name()
        .cast("string")
        .alias("source_file"),

        col("metadata.crawled_at_utc")
        .cast("string")
        .alias("crawled_at_utc_raw"),

        to_timestamp(
            col("metadata.crawled_at_utc")
        ).alias("crawled_at_utc"),

        col("data.timezone")
        .cast("string")
        .alias("timezone"),

        col("data.utc_offset_seconds")
        .cast("integer")
        .alias("utc_offset_seconds"),

        col("hourly_record.time")
        .cast("string")
        .alias("measured_at_local_raw"),

        to_timestamp(
            col("hourly_record.time")
        ).alias("measured_at_local"),

        col("hourly_record.pm10")
        .cast("double")
        .alias("pm10"),

        col("hourly_record.pm2_5")
        .cast("double")
        .alias("pm2_5"),

        col(
            "hourly_record.carbon_monoxide"
        )
        .cast("double")
        .alias("carbon_monoxide"),

        col(
            "hourly_record.nitrogen_dioxide"
        )
        .cast("double")
        .alias("nitrogen_dioxide"),

        col(
            "hourly_record.sulphur_dioxide"
        )
        .cast("double")
        .alias("sulphur_dioxide"),

        col("hourly_record.ozone")
        .cast("double")
        .alias("ozone"),

        col("hourly_record.us_aqi")
        .cast("integer")
        .alias("us_aqi"),
    )


def create_bronze_table(
    spark: SparkSession,
) -> None:
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS "
        "polaris.bronze"
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            city_id BIGINT,
            city_name STRING,
            country_code STRING,
            country_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,

            source STRING,
            source_system STRING,
            dataset_name STRING,
            dataset_kind STRING,

            requested_past_days INT,
            requested_forecast_days INT,

            source_file STRING,
            crawled_at_utc_raw STRING,
            crawled_at_utc TIMESTAMP,

            timezone STRING,
            utc_offset_seconds INT,

            measured_at_local_raw STRING,
            measured_at_local TIMESTAMP,

            pm10 DOUBLE,
            pm2_5 DOUBLE,
            carbon_monoxide DOUBLE,
            nitrogen_dioxide DOUBLE,
            sulphur_dioxide DOUBLE,
            ozone DOUBLE,
            us_aqi INT
        )
        USING iceberg
        PARTITIONED BY (
            country_code,
            days(crawled_at_utc)
        )
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.distribution-mode' = 'hash'
        )
        """
    )


def validate_bronze_data(
    bronze_df: DataFrame,
) -> None:
    total_records = bronze_df.count()

    source_files = (
        bronze_df
        .select("source_file")
        .distinct()
        .count()
    )

    invalid_crawl_timestamps = (
        bronze_df
        .filter(
            col("crawled_at_utc").isNull()
        )
        .count()
    )

    invalid_local_timestamps = (
        bronze_df
        .filter(
            col("measured_at_local").isNull()
        )
        .count()
    )

    duplicate_business_keys = (
        bronze_df
        .groupBy(
            "city_id",
            "measured_at_local",
            "source_system",
            "dataset_name",
        )
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print("\nBronze validation:")
    print("=" * 70)
    print(
        f"Bronze records:             "
        f"{total_records}"
    )
    print(
        f"Raw source files:           "
        f"{source_files}"
    )
    print(
        f"Invalid crawl timestamps:   "
        f"{invalid_crawl_timestamps}"
    )
    print(
        f"Invalid local timestamps:   "
        f"{invalid_local_timestamps}"
    )
    print(
        f"Duplicate business keys:    "
        f"{duplicate_business_keys}"
    )
    print("=" * 70)

    if total_records == 0:
        raise ValueError(
            "Bronze dataset is empty"
        )

    if source_files == 0:
        raise ValueError(
            "Bronze dataset has no source files"
        )


def write_bronze_table(
    bronze_df: DataFrame,
) -> None:
    print(
        "\nWriting Bronze Iceberg table:"
    )
    print(BRONZE_TABLE)

    (
        bronze_df
        .repartition(
            OUTPUT_PARTITIONS,
            "country_code",
            "crawled_at_utc",
        )
        .writeTo(BRONZE_TABLE)
        .overwritePartitions()
    )


def validate_written_table(
    spark: SparkSession,
) -> None:
    written_count = (
        spark.table(BRONZE_TABLE)
        .count()
    )

    snapshot_count = (
        spark.sql(
            f"""
            SELECT COUNT(*) AS snapshot_count
            FROM {BRONZE_TABLE}.snapshots
            """
        )
        .first()["snapshot_count"]
    )

    data_file_count = (
        spark.sql(
            f"""
            SELECT COUNT(*) AS data_file_count
            FROM {BRONZE_TABLE}.files
            """
        )
        .first()["data_file_count"]
    )

    print("\nWritten Iceberg validation:")
    print("=" * 70)
    print(
        f"Table records:              "
        f"{written_count}"
    )
    print(
        f"Snapshot count:             "
        f"{snapshot_count}"
    )
    print(
        f"Data file count:            "
        f"{data_file_count}"
    )
    print("=" * 70)

    if written_count == 0:
        raise ValueError(
            "Written Bronze table is empty"
        )

    if snapshot_count < 1:
        raise ValueError(
            "No Iceberg snapshot exists"
        )

    if data_file_count < 1:
        raise ValueError(
            "No Iceberg data file exists"
        )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = None

    try:
        hadoop_version = (
            spark.sparkContext
            ._jvm
            .org.apache.hadoop.util
            .VersionInfo
            .getVersion()
        )

        print(
            f"Spark master:   "
            f"{spark.sparkContext.master}"
        )
        print(
            f"Spark version:  "
            f"{spark.version}"
        )
        print(
            f"Hadoop version: "
            f"{hadoop_version}"
        )

        bronze_df = (
            read_and_flatten_raw_data(
                spark
            )
            .cache()
        )

        print("\nBronze schema:")
        bronze_df.printSchema()

        print("\nSample Bronze records:")
        (
            bronze_df
            .orderBy(
                "country_code",
                "city_name",
                "measured_at_local",
            )
            .show(
                10,
                truncate=False,
            )
        )

        validate_bronze_data(
            bronze_df
        )

        create_bronze_table(
            spark
        )

        write_bronze_table(
            bronze_df
        )

        validate_written_table(
            spark
        )

        print(
            "\nAIR QUALITY BRONZE "
            "ICEBERG JOB: PASS"
        )

    finally:
        if bronze_df is not None:
            bronze_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
