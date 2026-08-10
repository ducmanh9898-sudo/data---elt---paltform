import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    arrays_zip,
    coalesce,
    col,
    date_trunc,
    explode_outer,
    input_file_name,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    to_date,
    to_timestamp,
    to_utc_timestamp,
)
from pyspark.sql.window import Window


RAW_DIR = "s3a://environment-data/raw/air_quality/"
CLEAN_DIR = "s3a://environment-data/clean_v2/air_quality/"

MAX_PM25_NULL_RATE = 0.05
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
            "or still uses change_me"
        )

    return value.strip()


def normalize_minio_endpoint(
    endpoint: str,
    secure: bool,
) -> str:
    endpoint = endpoint.strip().rstrip("/")

    if endpoint.startswith(
        ("http://", "https://")
    ):
        return endpoint

    scheme = "https" if secure else "http"

    return f"{scheme}://{endpoint}"


def create_spark_session() -> SparkSession:
    minio_secure = parse_bool(
        os.getenv("MINIO_SECURE"),
        default=False,
    )

    minio_endpoint = normalize_minio_endpoint(
        os.getenv(
            "MINIO_ENDPOINT",
            "localhost:9000",
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

    

    return (
        SparkSession.builder
        .appName(
            "EnvironmentAirQualityTransform"
        )
        

        # hadoop-aws sẽ kéo AWS SDK tương thích
        # thông qua dependency của nó.
        

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
    "spark.hadoop.fs.s3a.input.stream.type",
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
            "spark.hadoop.fs.s3a.fast.upload",
            "true",
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
    """
    Đọc Raw JSON trực tiếp từ MinIO và flatten
    hoàn toàn bằng Spark DataFrame API.
    """
    print(
        "\nReading Raw JSON directly from:"
    )
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
            ).alias(
                "carbon_monoxide"
            ),

            col(
                "data.hourly.nitrogen_dioxide"
            ).alias(
                "nitrogen_dioxide"
            ),

            col(
                "data.hourly.sulphur_dioxide"
            ).alias(
                "sulphur_dioxide"
            ),

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
        .alias("source_file"),

        col("metadata.crawled_at_utc")
        .cast("string")
        .alias("crawled_at_utc_raw"),

        col("data.timezone")
        .cast("string")
        .alias("timezone"),

        col("data.utc_offset_seconds")
        .cast("integer")
        .alias("utc_offset_seconds"),

        col("hourly_record.time")
        .cast("string")
        .alias(
            "measured_at_local_raw"
        ),

        col("hourly_record.pm10")
        .cast("double")
        .alias("pm10"),

        col("hourly_record.pm2_5")
        .cast("double")
        .alias("pm2_5"),

        col(
            "hourly_record."
            "carbon_monoxide"
        )
        .cast("double")
        .alias("carbon_monoxide"),

        col(
            "hourly_record."
            "nitrogen_dioxide"
        )
        .cast("double")
        .alias("nitrogen_dioxide"),

        col(
            "hourly_record."
            "sulphur_dioxide"
        )
        .cast("double")
        .alias("sulphur_dioxide"),

        col("hourly_record.ozone")
        .cast("double")
        .alias("ozone"),

        col("hourly_record.us_aqi")
        .cast("double")
        .alias("us_aqi_raw"),
    )



def prepare_data(
    source_df: DataFrame,
) -> DataFrame:
    """
    Chuẩn hóa timestamp và tạo UTC/local timestamp.

    measured_at_local:
        Timestamp theo múi giờ địa phương của thành phố.

    measured_at_utc:
        Cùng thời điểm đó sau khi chuyển về UTC.

    historical_cutoff_utc:
        Đầu giờ hiện tại tại thời điểm crawl.

    Record chỉ được xem là historical khi:
        measured_at_utc < historical_cutoff_utc

    Nhờ dùng dấu <, giờ hiện tại đang diễn ra cũng không
    được xem là một giờ lịch sử hoàn chỉnh.
    """
    return (
        source_df
        .withColumn(
            "measured_at_local",
            to_timestamp(
                col("measured_at_local_raw")
            ),
        )
        .withColumn(
            "crawled_at_utc",
            to_timestamp(
                col("crawled_at_utc_raw")
            ),
        )
        .withColumn(
            "measured_at_utc",
            to_utc_timestamp(
                col("measured_at_local"),
                col("timezone"),
            ),
        )
        .withColumn(
            "historical_cutoff_utc",
            date_trunc(
                "hour",
                col("crawled_at_utc"),
            ),
        )
        .withColumn(
            "measurement_date_local",
            to_date(
                col("measured_at_local")
            ),
        )
        .withColumn(
            "measurement_date_utc",
            to_date(
                col("measured_at_utc")
            ),
        )
        .withColumn(
            "us_aqi",
            col("us_aqi_raw").cast("integer"),
        )
        .drop(
            "measured_at_local_raw",
            "crawled_at_utc_raw",
            "us_aqi_raw",
        )
    )


def validate_prepared_data(
    prepared_df: DataFrame,
) -> None:
    """
    Kiểm tra dữ liệu trước khi lọc historical.

    Các trường khóa và timezone không được thiếu.
    """
    missing_city_id = (
        prepared_df
        .filter(
            col("city_id").isNull()
        )
        .count()
    )

    missing_country_code = (
        prepared_df
        .filter(
            col("country_code").isNull()
        )
        .count()
    )

    missing_timezone = (
        prepared_df
        .filter(
            col("timezone").isNull()
        )
        .count()
    )

    invalid_local_timestamp = (
        prepared_df
        .filter(
            col("measured_at_local").isNull()
        )
        .count()
    )

    invalid_utc_timestamp = (
        prepared_df
        .filter(
            col("measured_at_utc").isNull()
        )
        .count()
    )

    invalid_crawl_timestamp = (
        prepared_df
        .filter(
            col("crawled_at_utc").isNull()
        )
        .count()
    )

    future_or_current_rows = (
        prepared_df
        .filter(
            col("measured_at_utc")
            >= col("historical_cutoff_utc")
        )
        .count()
    )

    print("\nPrepared data validation:")
    print("=" * 70)
    print(
        f"Missing city_id:             "
        f"{missing_city_id}"
    )
    print(
        f"Missing country_code:        "
        f"{missing_country_code}"
    )
    print(
        f"Missing timezone:            "
        f"{missing_timezone}"
    )
    print(
        f"Invalid local timestamps:    "
        f"{invalid_local_timestamp}"
    )
    print(
        f"Invalid UTC timestamps:      "
        f"{invalid_utc_timestamp}"
    )
    print(
        f"Invalid crawl timestamps:    "
        f"{invalid_crawl_timestamp}"
    )
    print(
        f"Future/current-hour rows:    "
        f"{future_or_current_rows}"
    )
    print("=" * 70)

    required_field_errors = (
        missing_city_id
        + missing_country_code
        + missing_timezone
        + invalid_local_timestamp
        + invalid_utc_timestamp
        + invalid_crawl_timestamp
    )

    if required_field_errors > 0:
        raise ValueError(
            "Prepared data contains missing or invalid "
            "required values"
        )


def build_clean_data(
    prepared_df: DataFrame,
) -> DataFrame:
    """
    Chỉ giữ dữ liệu historical hoàn chỉnh và loại trùng.

    Nhiều lần crawl có thể chứa cùng một giờ lịch sử.
    Với cùng city_id + measured_at_utc, giữ record từ lần
    crawl mới nhất.
    """
    historical_df = (
        prepared_df
        .filter(
            col("measured_at_utc")
            < col("historical_cutoff_utc")
        )
        .withColumn(
            "record_type",
            lit("historical"),
        )
    )

    latest_record_window = (
        Window
        .partitionBy(
            "city_id",
            "measured_at_utc",
            "source_system",
            "dataset_name",
        )
        .orderBy(
            col("crawled_at_utc")
            .desc_nulls_last(),

            col("source_file")
            .desc(),
        )
    )

    return (
        historical_df
        .withColumn(
            "_row_number",
            row_number().over(
                latest_record_window
            ),
        )
        .filter(
            col("_row_number") == 1
        )
        .drop(
            "_row_number",
            "historical_cutoff_utc",
        )
        .select(
            "city_id",
            "city_name",
            "country_name",
            "latitude",
            "longitude",

            "timezone",
            "utc_offset_seconds",

            "source",
            "source_system",
            "dataset_name",
            "dataset_kind",
            "record_type",

            "requested_past_days",
            "requested_forecast_days",

            "source_file",
            "crawled_at_utc",

            "measured_at_local",
            "measured_at_utc",

            "pm10",
            "pm2_5",
            "carbon_monoxide",
            "nitrogen_dioxide",
            "sulphur_dioxide",
            "ozone",
            "us_aqi",

            # Partition columns đặt cuối để schema dễ đọc.
            "country_code",
            "measurement_date_local",
            "measurement_date_utc",
        )
    )


def validate_clean_data(
    clean_df: DataFrame,
) -> None:
    """
    Kiểm tra dữ liệu Clean sau khi loại forecast và deduplicate.
    """
    total_records = clean_df.count()

    total_cities = (
        clean_df
        .select("city_id")
        .distinct()
        .count()
    )

    total_countries = (
        clean_df
        .select("country_code")
        .distinct()
        .count()
    )

    duplicate_keys = (
        clean_df
        .groupBy(
            "city_id",
            "measured_at_utc",
            "source_system",
            "dataset_name",
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    future_records = (
        clean_df
        .filter(
            col("measured_at_utc")
            >= date_trunc(
                "hour",
                col("crawled_at_utc"),
            )
        )
        .count()
    )

    missing_pm25 = (
        clean_df
        .filter(
            col("pm2_5").isNull()
        )
        .count()
    )

    negative_pollutant_records = (
        clean_df
        .filter(
            (col("pm10") < 0)
            | (col("pm2_5") < 0)
            | (col("carbon_monoxide") < 0)
            | (col("nitrogen_dioxide") < 0)
            | (col("sulphur_dioxide") < 0)
            | (col("ozone") < 0)
            | (col("us_aqi") < 0)
        )
        .count()
    )

    pm25_null_rate = (
        missing_pm25 / total_records
        if total_records > 0
        else 0.0
    )

    print("\nClean data validation:")
    print("=" * 70)
    print(
        f"Clean records:              "
        f"{total_records}"
    )
    print(
        f"Total cities:               "
        f"{total_cities}"
    )
    print(
        f"Total countries:            "
        f"{total_countries}"
    )
    print(
        f"Duplicate business keys:    "
        f"{duplicate_keys}"
    )
    print(
        f"Future/current-hour rows:   "
        f"{future_records}"
    )
    print(
        f"Missing PM2.5:              "
        f"{missing_pm25}"
    )
    print(
        f"PM2.5 null rate:            "
        f"{pm25_null_rate:.2%}"
    )
    print(
        f"Negative pollutant rows:    "
        f"{negative_pollutant_records}"
    )
    print("=" * 70)

    if total_records == 0:
        raise ValueError(
            "Data quality failed: Clean dataset is empty"
        )

    if duplicate_keys != 0:
        raise ValueError(
            "Data quality failed: duplicate business "
            "keys exist"
        )

    if future_records != 0:
        raise ValueError(
            "Data quality failed: future or current-hour "
            "records exist"
        )

    if negative_pollutant_records != 0:
        raise ValueError(
            "Data quality failed: negative pollutant "
            "values exist"
        )

    if pm25_null_rate > MAX_PM25_NULL_RATE:
        raise ValueError(
            "Data quality failed: PM2.5 null rate "
            f"{pm25_null_rate:.2%} exceeds "
            f"{MAX_PM25_NULL_RATE:.0%}"
        )

        null_business_keys = (
        clean_df
        .filter(
            col("city_id").isNull()
            | col("measured_at_utc").isNull()
            | col("source_system").isNull()
            | col("dataset_name").isNull()
        )
        .count()
)

        timestamp_stats = (
            clean_df
            .agg(
                spark_min(
                    "measured_at_utc"
                ).alias(
                    "min_measured_at_utc"
                ),
                spark_max(
                    "measured_at_utc"
                ).alias(
                    "max_measured_at_utc"
                ),
            )
            .first()
)


def write_clean_parquet(
    clean_df: DataFrame,
) -> None:
    """
    Ghi Clean Zone dưới dạng partitioned Parquet.

    Trước khi ghi, dữ liệu được repartition theo đúng:
        country_code
        measurement_date_local

    Nhờ đó, tất cả record thuộc cùng một quốc gia và ngày
    được đưa về cùng một Spark partition, hạn chế tạo
    hàng nghìn file Parquet nhỏ.
    """
    output_df = (
        clean_df
        .repartition(
            OUTPUT_PARTITIONS,
            "country_code",
            "measurement_date_local",
        )
        .sortWithinPartitions(
            "city_id",
            "measured_at_utc",
        )
    )

    print("\nParquet output information:")
    print(
        f"Spark partitions before repartition: "
        f"{clean_df.rdd.getNumPartitions()}"
    )
    print(
        f"Spark partitions before write: "
        f"{output_df.rdd.getNumPartitions()}"
    )

    (
        output_df
        .write
        .mode("overwrite")
        .partitionBy(
            "country_code",
            "measurement_date_local",
        )
        .parquet(
            str(CLEAN_DIR)
        )
    )
def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel(
        "WARN"
    )
    print(
    f"Spark master:   "
    f"{spark.sparkContext.master}"
)

    prepared_df = None
    clean_df = None

    try:
        hadoop_version = (
            spark.sparkContext
            ._jvm
            .org.apache.hadoop.util
            .VersionInfo
            .getVersion()
        )

        print(
            f"Spark version:  "
            f"{spark.version}"
        )
        print(
            f"Hadoop version: "
            f"{hadoop_version}"
        )

        source_df = (
            read_and_flatten_raw_data(
                spark
            )
        )

        prepared_df = (
            prepare_data(source_df)
            .cache()
        )

        prepared_count = (
            prepared_df.count()
        )

        if prepared_count == 0:
            raise ValueError(
                "No usable hourly records "
                f"found at {RAW_DIR}"
            )

        print(
            "\nRaw flattened records "
            f"count: {prepared_count}"
        )

        validate_prepared_data(
            prepared_df
        )

        clean_df = (
            build_clean_data(
                prepared_df
            )
            .cache()
        )

        print("\nClean schema:")
        clean_df.printSchema()

        print("\nSample Clean records:")

        (
            clean_df
            .orderBy(
                "country_code",
                "city_name",
                "measured_at_utc",
            )
            .show(
                10,
                truncate=False,
            )
        )

        validate_clean_data(
            clean_df
        )

        write_clean_parquet(
            clean_df
        )

        print(
            "\nClean Parquet saved "
            "successfully:"
        )
        print(CLEAN_DIR)

    finally:
        if clean_df is not None:
            clean_df.unpersist()

        if prepared_df is not None:
            prepared_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()

