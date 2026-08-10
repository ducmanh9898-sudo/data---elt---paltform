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


# ============================================================
# MINIO PATHS
# ============================================================

RAW_DIR = "s3a://environment-data/raw/weather/"
CLEAN_DIR = "s3a://environment-data/clean_v2/weather/"

MAX_TEMPERATURE_NULL_RATE = 0.05
OUTPUT_PARTITIONS = 24


# ============================================================
# ENVIRONMENT CONFIGURATION
# ============================================================

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


# ============================================================
# SPARK SESSION + S3A
# ============================================================

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
            "EnvironmentWeatherTransform"
        )
        
        # Cung cấp S3AFileSystem cho Spark.
        

        # Kết nối Spark với MinIO qua S3A.
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
    "spark.hadoop.fs.s3a.input.stream.type",
    "classic",
)
        .config(
            "spark.hadoop.fs.s3a."
            "connection.ssl.enabled",
            str(minio_secure).lower(),
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

        # Spark SQL.
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


# ============================================================
# SPARK-NATIVE RAW JSON READ + FLATTEN
# ============================================================

def read_and_flatten_raw_data(
    spark: SparkSession,
) -> DataFrame:
    """
    Đọc Weather Raw JSON trực tiếp từ MinIO.

    Luồng xử lý:

        spark.read.json
        -> arrays_zip
        -> explode_outer
        -> select
        -> cast

    Không sử dụng:
        json.load
        Python for-loop
        list[dict]
        spark.createDataFrame(records)
    """

    print(
        "\nReading Weather Raw JSON "
        "directly from MinIO:"
    )
    print(RAW_DIR)

    raw_df = (
        spark.read
        .option(
            "multiLine",
            "true",
        )
        .option(
            "recursiveFileLookup",
            "true",
        )
        .option(
            "pathGlobFilter",
            "*.json",
        )
        .option(
            "mode",
            "FAILFAST",
        )
        .json(RAW_DIR)
    )

    # Ghép các hourly array theo cùng index.
    zipped_df = raw_df.withColumn(
        "hourly_zipped",
        arrays_zip(
            col("data.hourly.time")
            .alias("time"),

            col(
                "data.hourly.temperature_2m"
            )
            .alias("temperature_2m"),

            col(
                "data.hourly."
                "relative_humidity_2m"
            )
            .alias(
                "relative_humidity_2m"
            ),

            col(
                "data.hourly.precipitation"
            )
            .alias("precipitation"),

            col("data.hourly.rain")
            .alias("rain"),

            col(
                "data.hourly.surface_pressure"
            )
            .alias("surface_pressure"),

            col(
                "data.hourly.cloud_cover"
            )
            .alias("cloud_cover"),

            col(
                "data.hourly.wind_speed_10m"
            )
            .alias("wind_speed_10m"),

            col(
                "data.hourly."
                "wind_direction_10m"
            )
            .alias(
                "wind_direction_10m"
            ),

            col(
                "data.hourly.visibility"
            )
            .alias("visibility"),

            col(
                "data.hourly.weather_code"
            )
            .alias("weather_code"),
        ),
    )

    # Mỗi hourly struct trở thành một dòng.
    exploded_df = zipped_df.withColumn(
        "hourly_record",
        explode_outer(
            col("hourly_zipped")
        ),
    )

    return exploded_df.select(
        # City metadata.
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

        # Dataset metadata.
        coalesce(
            col("metadata.source")
            .cast("string"),
            lit(
                "Open-Meteo Weather API"
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
            lit("weather_hourly"),
        ).alias("dataset_name"),

        coalesce(
            col("metadata.dataset_kind")
            .cast("string"),
            lit("weather_model"),
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

        # Data lineage.
        input_file_name()
        .alias("source_file"),

        col("metadata.crawled_at_utc")
        .cast("string")
        .alias("crawled_at_utc_raw"),

        # API timezone.
        col("data.timezone")
        .cast("string")
        .alias("timezone"),

        col("data.utc_offset_seconds")
        .cast("integer")
        .alias("utc_offset_seconds"),

        # Hourly measurements.
        col("hourly_record.time")
        .cast("string")
        .alias(
            "measured_at_local_raw"
        ),

        col(
            "hourly_record.temperature_2m"
        )
        .cast("double")
        .alias("temperature_2m"),

        col(
            "hourly_record."
            "relative_humidity_2m"
        )
        .cast("double")
        .alias(
            "relative_humidity_2m"
        ),

        col(
            "hourly_record.precipitation"
        )
        .cast("double")
        .alias("precipitation"),

        col("hourly_record.rain")
        .cast("double")
        .alias("rain"),

        col(
            "hourly_record.surface_pressure"
        )
        .cast("double")
        .alias("surface_pressure"),

        col(
            "hourly_record.cloud_cover"
        )
        .cast("double")
        .alias("cloud_cover"),

        col(
            "hourly_record.wind_speed_10m"
        )
        .cast("double")
        .alias("wind_speed_10m"),

        col(
            "hourly_record."
            "wind_direction_10m"
        )
        .cast("double")
        .alias("wind_direction_10m"),

        col(
            "hourly_record.visibility"
        )
        .cast("double")
        .alias("visibility"),

        # Đọc tạm bằng double.
        col(
            "hourly_record.weather_code"
        )
        .cast("double")
        .alias("weather_code_raw"),
    )


# ============================================================
# TIMESTAMP NORMALIZATION
# ============================================================

def prepare_data(
    source_df: DataFrame,
) -> DataFrame:
    """
    Chuẩn hóa timestamp local và UTC.

    Chỉ các record có:

        measured_at_utc < historical_cutoff_utc

    mới được xem là historical hoàn chỉnh.
    """

    return (
        source_df
        .withColumn(
            "measured_at_local",
            to_timestamp(
                col(
                    "measured_at_local_raw"
                )
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
            "weather_code",
            col(
                "weather_code_raw"
            ).cast("integer"),
        )
        .drop(
            "measured_at_local_raw",
            "crawled_at_utc_raw",
            "weather_code_raw",
        )
    )


# ============================================================
# PREPARED DATA VALIDATION
# ============================================================

def validate_prepared_data(
    prepared_df: DataFrame,
) -> None:
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
            col(
                "measured_at_local"
            ).isNull()
        )
        .count()
    )

    invalid_utc_timestamp = (
        prepared_df
        .filter(
            col(
                "measured_at_utc"
            ).isNull()
        )
        .count()
    )

    invalid_crawl_timestamp = (
        prepared_df
        .filter(
            col(
                "crawled_at_utc"
            ).isNull()
        )
        .count()
    )

    future_or_current_rows = (
        prepared_df
        .filter(
            col("measured_at_utc")
            >= col(
                "historical_cutoff_utc"
            )
        )
        .count()
    )

    print(
        "\nPrepared Weather validation:"
    )
    print("=" * 72)

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
        f"Forecast/current-hour rows:  "
        f"{future_or_current_rows}"
    )

    print("=" * 72)

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
            "Prepared Weather data "
            "contains missing or invalid "
            "required values"
        )


# ============================================================
# HISTORICAL FILTER + DEDUPLICATION
# ============================================================

def build_clean_data(
    prepared_df: DataFrame,
) -> DataFrame:
    """
    Loại forecast/current hour và deduplicate.

    Business key:

        city_id
        measured_at_utc
        source_system
        dataset_name

    Với cùng business key, giữ record từ
    lần crawl mới nhất.
    """

    historical_df = (
        prepared_df
        .filter(
            col("measured_at_utc")
            < col(
                "historical_cutoff_utc"
            )
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

            # Partition columns.
            "country_code",
            "measurement_date_local",
            "measurement_date_utc",
        )
    )


# ============================================================
# CLEAN DATA VALIDATION
# ============================================================

def validate_clean_data(
    clean_df: DataFrame,
) -> None:
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

    null_business_keys = (
        clean_df
        .filter(
            col("city_id").isNull()
            | col(
                "measured_at_utc"
            ).isNull()
            | col(
                "source_system"
            ).isNull()
            | col(
                "dataset_name"
            ).isNull()
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

    missing_temperature = (
        clean_df
        .filter(
            col(
                "temperature_2m"
            ).isNull()
        )
        .count()
    )

    missing_weather_code = (
        clean_df
        .filter(
            col(
                "weather_code"
            ).isNull()
        )
        .count()
    )

    invalid_humidity_rows = (
        clean_df
        .filter(
            (
                col(
                    "relative_humidity_2m"
                ) < 0
            )
            | (
                col(
                    "relative_humidity_2m"
                ) > 100
            )
        )
        .count()
    )

    invalid_cloud_cover_rows = (
        clean_df
        .filter(
            (col("cloud_cover") < 0)
            | (col("cloud_cover") > 100)
        )
        .count()
    )

    negative_precipitation_rows = (
        clean_df
        .filter(
            col("precipitation") < 0
        )
        .count()
    )

    negative_rain_rows = (
        clean_df
        .filter(
            col("rain") < 0
        )
        .count()
    )

    invalid_surface_pressure_rows = (
        clean_df
        .filter(
            col("surface_pressure") <= 0
        )
        .count()
    )

    negative_wind_speed_rows = (
        clean_df
        .filter(
            col("wind_speed_10m") < 0
        )
        .count()
    )

    invalid_wind_direction_rows = (
        clean_df
        .filter(
            (
                col(
                    "wind_direction_10m"
                ) < 0
            )
            | (
                col(
                    "wind_direction_10m"
                ) > 360
            )
        )
        .count()
    )

    negative_visibility_rows = (
        clean_df
        .filter(
            col("visibility") < 0
        )
        .count()
    )

    invalid_weather_code_rows = (
        clean_df
        .filter(
            (col("weather_code") < 0)
            | (col("weather_code") > 99)
        )
        .count()
    )

    temperature_null_rate = (
        missing_temperature / total_records
        if total_records > 0
        else 0.0
    )

    print(
        "\nClean Weather validation:"
    )
    print("=" * 72)

    print(
        f"Clean records:                 "
        f"{total_records}"
    )
    print(
        f"Total cities:                  "
        f"{total_cities}"
    )
    print(
        f"Total countries:               "
        f"{total_countries}"
    )
    print(
        f"MIN measured_at_utc:           "
        f"{timestamp_stats['min_measured_at_utc']}"
    )
    print(
        f"MAX measured_at_utc:           "
        f"{timestamp_stats['max_measured_at_utc']}"
    )
    print(
        f"Duplicate business keys:       "
        f"{duplicate_keys}"
    )
    print(
        f"NULL business keys:            "
        f"{null_business_keys}"
    )
    print(
        f"Future/current-hour rows:      "
        f"{future_records}"
    )
    print(
        f"Missing temperature:           "
        f"{missing_temperature}"
    )
    print(
        f"Temperature null rate:         "
        f"{temperature_null_rate:.2%}"
    )
    print(
        f"Missing weather code:          "
        f"{missing_weather_code}"
    )
    print(
        f"Invalid humidity rows:         "
        f"{invalid_humidity_rows}"
    )
    print(
        f"Invalid cloud cover rows:      "
        f"{invalid_cloud_cover_rows}"
    )
    print(
        f"Negative precipitation rows:   "
        f"{negative_precipitation_rows}"
    )
    print(
        f"Negative rain rows:            "
        f"{negative_rain_rows}"
    )
    print(
        f"Invalid surface pressure rows: "
        f"{invalid_surface_pressure_rows}"
    )
    print(
        f"Negative wind speed rows:      "
        f"{negative_wind_speed_rows}"
    )
    print(
        f"Invalid wind direction rows:   "
        f"{invalid_wind_direction_rows}"
    )
    print(
        f"Negative visibility rows:      "
        f"{negative_visibility_rows}"
    )
    print(
        f"Invalid weather code rows:     "
        f"{invalid_weather_code_rows}"
    )

    print("=" * 72)

    if total_records == 0:
        raise ValueError(
            "Data quality failed: "
            "Weather Clean dataset is empty"
        )

    if duplicate_keys != 0:
        raise ValueError(
            "Data quality failed: "
            "duplicate Weather business keys exist"
        )

    if null_business_keys != 0:
        raise ValueError(
            "Data quality failed: "
            "NULL Weather business keys exist"
        )

    if future_records != 0:
        raise ValueError(
            "Data quality failed: future or "
            "current-hour Weather records exist"
        )

    if (
        temperature_null_rate
        > MAX_TEMPERATURE_NULL_RATE
    ):
        raise ValueError(
            "Data quality failed: "
            "temperature_2m null rate "
            f"{temperature_null_rate:.2%} exceeds "
            f"{MAX_TEMPERATURE_NULL_RATE:.0%}"
        )

    if invalid_humidity_rows != 0:
        raise ValueError(
            "Data quality failed: "
            "relative humidity must be "
            "between 0 and 100"
        )

    if invalid_cloud_cover_rows != 0:
        raise ValueError(
            "Data quality failed: "
            "cloud cover must be "
            "between 0 and 100"
        )

    if negative_precipitation_rows != 0:
        raise ValueError(
            "Data quality failed: negative "
            "precipitation values exist"
        )

    if negative_rain_rows != 0:
        raise ValueError(
            "Data quality failed: "
            "negative rain values exist"
        )

    if invalid_surface_pressure_rows != 0:
        raise ValueError(
            "Data quality failed: invalid "
            "surface pressure values exist"
        )

    if negative_wind_speed_rows != 0:
        raise ValueError(
            "Data quality failed: negative "
            "wind speed values exist"
        )

    if invalid_wind_direction_rows != 0:
        raise ValueError(
            "Data quality failed: "
            "wind direction must be "
            "between 0 and 360"
        )

    if negative_visibility_rows != 0:
        raise ValueError(
            "Data quality failed: negative "
            "visibility values exist"
        )

    if invalid_weather_code_rows != 0:
        raise ValueError(
            "Data quality failed: invalid "
            "weather code values exist"
        )


# ============================================================
# WRITE PARQUET DIRECTLY TO MINIO
# ============================================================

def write_clean_parquet(
    clean_df: DataFrame,
) -> None:
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

    print(
        "\nWeather Parquet output information:"
    )
    print(
        "Partitions before repartition: "
        f"{clean_df.rdd.getNumPartitions()}"
    )
    print(
        "Partitions before write: "
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
        .parquet(CLEAN_DIR)
    )


# ============================================================
# MAIN
# ============================================================

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

        print("=" * 72)
        print(
            "Open-Meteo Weather "
            "Spark-native MinIO Transform"
        )
        print("=" * 72)

        print(
            f"Spark version:  "
            f"{spark.version}"
        )
        print(
            f"Hadoop version: "
            f"{hadoop_version}"
        )
        print(
            f"Raw input:      "
            f"{RAW_DIR}"
        )
        print(
            f"Clean output:   "
            f"{CLEAN_DIR}"
        )

        print("=" * 72)

        source_df = (
            read_and_flatten_raw_data(
                spark
            )
        )

        prepared_df = (
            prepare_data(
                source_df
            )
            .cache()
        )

        prepared_count = (
            prepared_df.count()
        )

        if prepared_count == 0:
            raise ValueError(
                "No usable Weather hourly "
                f"records found at {RAW_DIR}"
            )

        print(
            "\nRaw flattened Weather "
            f"records: {prepared_count}"
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

        print(
            "\nClean Weather schema:"
        )
        clean_df.printSchema()

        print(
            "\nSample Clean Weather records:"
        )

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

        print("=" * 72)
        print(
            "PASS: Spark read Weather Raw "
            "directly from MinIO, performed "
            "Spark-native transformations, "
            "and wrote Clean V2 Parquet "
            "directly to MinIO."
        )
        print("=" * 72)

    finally:
        if clean_df is not None:
            clean_df.unpersist()

        if prepared_df is not None:
            prepared_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()