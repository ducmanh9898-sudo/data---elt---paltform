from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_trunc,
    lit,
     countDistinct,
    row_number,
    to_date,
    to_utc_timestamp,
)
from pyspark.sql.window import Window
MAX_TEMPERATURE_NULL_RATE = 0.05
def canonicalize_city_ids(
    historical_df: DataFrame,
) -> DataFrame:
    """
    Chuẩn hóa city_id lịch sử theo khóa tự nhiên:

        country_code + city_name

    Bronze giữ nguyên city_id từ Raw để phục vụ audit.
    Silver sử dụng city_id mới nhất của từng thành phố.
    """

    city_mapping_window = (
        Window
        .partitionBy(
            "country_code",
            "city_name",
        )
        .orderBy(
            col("crawled_at_utc")
            .desc_nulls_last(),

            col("source_file")
            .desc_nulls_last(),
        )
    )

    canonical_mapping_df = (
        historical_df
        .filter(
            col("country_code").isNotNull()
            & col("city_name").isNotNull()
            & col("city_id").isNotNull()
            & col("crawled_at_utc").isNotNull()
        )
        .select(
            "country_code",
            "city_name",
            "city_id",
            "crawled_at_utc",
            "source_file",
        )
        .withColumn(
            "_city_mapping_rank",
            row_number().over(
                city_mapping_window
            ),
        )
        .filter(
            col("_city_mapping_rank") == 1
        )
        .select(
            "country_code",
            "city_name",
            col("city_id").alias(
                "_canonical_city_id"
            ),
        )
    )

    return (
        historical_df
        .join(
            canonical_mapping_df,
            on=[
                "country_code",
                "city_name",
            ],
            how="left",
        )
        .withColumn(
            "city_id",
            col("_canonical_city_id")
            .cast("long"),
        )
        .drop(
            "_canonical_city_id",
        )
    )

from weather_bronze_iceberg_job import (
    BRONZE_TABLE,
    create_spark_session,
)

SILVER_TABLE = (
    "polaris.silver.weather_hourly"
)



MAX_TEMPERATURE_NULL_RATE = 0.05


def build_silver_data(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Chuyển Weather Bronze sang Silver.

    Silver thực hiện:
    1. Chuẩn hóa local timestamp sang UTC.
    2. Chỉ giữ historical hour đã hoàn chỉnh.
    3. Loại record thiếu business key.
    4. Deduplicate và giữ lần crawl mới nhất.
    """

    prepared_df = (
        bronze_df
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
            "record_type",
            lit("historical"),
        )
    )

    valid_historical_df = (
        prepared_df
        .filter(
            col("city_id").isNotNull()
            & col("city_name").isNotNull()
            & col("country_code").isNotNull()
            & col("source_system").isNotNull()
            & col("dataset_name").isNotNull()
            & col("timezone").isNotNull()
            & col("crawled_at_utc").isNotNull()
            & col("measured_at_local").isNotNull()
            & col("measured_at_utc").isNotNull()
            & (
                col("measured_at_utc")
                < col("historical_cutoff_utc")
            )
        )
    )
    canonical_historical_df = (
        canonicalize_city_ids(
            valid_historical_df
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
        .desc_nulls_last(),
    )
)
    return (
         canonical_historical_df
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
            "crawled_at_utc_raw",
            "measured_at_local_raw",
        )
        .select(
            "city_id",
            "city_name",
            "country_code",
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
            "measurement_date_local",
            "measurement_date_utc",

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
        )
    )
def validate_silver_data(
    silver_df: DataFrame,
) -> None:
    total_records = silver_df.count()

    total_cities = (
        silver_df
        .select("city_id")
        .distinct()
        .count()
    )

    total_countries = (
        silver_df
        .select("country_code")
        .distinct()
        .count()
    )

    null_business_keys = (
        silver_df
        .filter(
            col("city_id").isNull()
            | col("measured_at_utc").isNull()
            | col("source_system").isNull()
            | col("dataset_name").isNull()
        )
        .count()
    )

    duplicate_business_keys = (
        silver_df
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

    future_or_current_rows = (
        silver_df
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
        silver_df
        .filter(
            col("temperature_2m").isNull()
        )
        .count()
    )

    missing_weather_code = (
        silver_df
        .filter(
            col("weather_code").isNull()
        )
        .count()
    )

    invalid_humidity_rows = (
        silver_df
        .filter(
            (col("relative_humidity_2m") < 0)
            | (col("relative_humidity_2m") > 100)
        )
        .count()
    )

    invalid_cloud_cover_rows = (
        silver_df
        .filter(
            (col("cloud_cover") < 0)
            | (col("cloud_cover") > 100)
        )
        .count()
    )

    negative_precipitation_rows = (
        silver_df
        .filter(
            col("precipitation") < 0
        )
        .count()
    )

    negative_rain_rows = (
        silver_df
        .filter(
            col("rain") < 0
        )
        .count()
    )

    invalid_surface_pressure_rows = (
        silver_df
        .filter(
            col("surface_pressure") <= 0
        )
        .count()
    )

    negative_wind_speed_rows = (
        silver_df
        .filter(
            col("wind_speed_10m") < 0
        )
        .count()
    )

    invalid_wind_direction_rows = (
        silver_df
        .filter(
            (col("wind_direction_10m") < 0)
            | (col("wind_direction_10m") > 360)
        )
        .count()
    )

    negative_visibility_rows = (
        silver_df
        .filter(
            col("visibility") < 0
        )
        .count()
    )

    invalid_weather_code_rows = (
        silver_df
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

    city_id_mapping_conflicts = (
        silver_df
        .select(
            "city_id",
            "country_code",
            "city_name",
        )
        .distinct()
        .groupBy(
            "city_id",
        )
        .agg(
            countDistinct(
                "country_code",
                "city_name",
            ).alias(
                "city_mapping_count"
            )
        )
        .filter(
            col("city_mapping_count") > 1
        )
        .count()
    )

    city_name_mapping_conflicts = (
        silver_df
        .select(
            "country_code",
            "city_name",
            "city_id",
        )
        .distinct()
        .groupBy(
            "country_code",
            "city_name",
        )
        .agg(
            countDistinct(
                "city_id"
            ).alias(
                "city_id_count"
            )
        )
        .filter(
            col("city_id_count") > 1
        )
        .count()
    )

    print("\nSilver Weather validation:")
    print("=" * 70)
    print(
        f"Silver records:              "
        f"{total_records}"
    )
    print(
        f"Total cities:                "
        f"{total_cities}"
    )
    print(
        f"Total countries:             "
        f"{total_countries}"
    )
    print(
        f"NULL business keys:          "
        f"{null_business_keys}"
    )
    print(
        f"Duplicate business keys:     "
        f"{duplicate_business_keys}"
    )
    print(
        f"Future/current-hour rows:    "
        f"{future_or_current_rows}"
    )
    print(
        f"Missing temperature:         "
        f"{missing_temperature}"
    )
    print(
        f"Temperature null rate:       "
        f"{temperature_null_rate:.2%}"
    )
    print(
        f"Missing weather code:        "
        f"{missing_weather_code}"
    )
    print(
        f"Invalid humidity rows:       "
        f"{invalid_humidity_rows}"
    )
    print(
        f"Invalid cloud cover rows:    "
        f"{invalid_cloud_cover_rows}"
    )
    print(
        f"Negative precipitation:      "
        f"{negative_precipitation_rows}"
    )
    print(
        f"Negative rain:               "
        f"{negative_rain_rows}"
    )
    print(
        f"Invalid surface pressure:    "
        f"{invalid_surface_pressure_rows}"
    )
    print(
        f"Negative wind speed:         "
        f"{negative_wind_speed_rows}"
    )
    print(
        f"Invalid wind direction:      "
        f"{invalid_wind_direction_rows}"
    )
    print(
        f"Negative visibility:         "
        f"{negative_visibility_rows}"
    )
    print(
        f"Invalid weather code:        "
        f"{invalid_weather_code_rows}"
    )
    print(
        f"City ID mapping conflicts:  "
        f"{city_id_mapping_conflicts}"
    )
    print(
        f"City name ID conflicts:     "
        f"{city_name_mapping_conflicts}"
    )
    print("=" * 70)

    if total_records == 0:
        raise ValueError(
            "Weather Silver dataset is empty"
        )

    if null_business_keys != 0:
        raise ValueError(
            "Weather Silver contains "
            "NULL business keys"
        )

    if duplicate_business_keys != 0:
        raise ValueError(
            "Weather Silver contains "
            "duplicate business keys"
        )

    if future_or_current_rows != 0:
        raise ValueError(
            "Weather Silver contains future "
            "or current-hour records"
        )

    if (
        temperature_null_rate
        > MAX_TEMPERATURE_NULL_RATE
    ):
        raise ValueError(
            "Temperature null rate "
            f"{temperature_null_rate:.2%} exceeds "
            f"{MAX_TEMPERATURE_NULL_RATE:.0%}"
        )

    if invalid_humidity_rows != 0:
        raise ValueError(
            "Relative humidity must be "
            "between 0 and 100"
        )

    if invalid_cloud_cover_rows != 0:
        raise ValueError(
            "Cloud cover must be "
            "between 0 and 100"
        )

    if negative_precipitation_rows != 0:
        raise ValueError(
            "Negative precipitation values exist"
        )

    if negative_rain_rows != 0:
        raise ValueError(
            "Negative rain values exist"
        )

    if invalid_surface_pressure_rows != 0:
        raise ValueError(
            "Invalid surface pressure values exist"
        )

    if negative_wind_speed_rows != 0:
        raise ValueError(
            "Negative wind speed values exist"
        )

    if invalid_wind_direction_rows != 0:
        raise ValueError(
            "Wind direction must be "
            "between 0 and 360"
        )

    if negative_visibility_rows != 0:
        raise ValueError(
            "Negative visibility values exist"
        )

    if invalid_weather_code_rows != 0:
        raise ValueError(
            "Invalid weather code values exist"
        )
    if city_id_mapping_conflicts != 0:
        raise ValueError(
            "A city_id maps to multiple cities"
        )

    if city_name_mapping_conflicts != 0:
        raise ValueError(
            "A city maps to multiple city_id values"
        )

def create_silver_table(
    spark: SparkSession,
) -> None:
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS "
        "polaris.silver"
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            city_id BIGINT,
            city_name STRING,
            country_code STRING,
            country_name STRING,
            latitude DOUBLE,
            longitude DOUBLE,

            timezone STRING,
            utc_offset_seconds INT,

            source STRING,
            source_system STRING,
            dataset_name STRING,
            dataset_kind STRING,
            record_type STRING,

            requested_past_days INT,
            requested_forecast_days INT,

            source_file STRING,
            crawled_at_utc TIMESTAMP,

            measured_at_local TIMESTAMP,
            measured_at_utc TIMESTAMP,
            measurement_date_local DATE,
            measurement_date_utc DATE,

            temperature_2m DOUBLE,
            relative_humidity_2m DOUBLE,
            precipitation DOUBLE,
            rain DOUBLE,
            surface_pressure DOUBLE,
            cloud_cover DOUBLE,
            wind_speed_10m DOUBLE,
            wind_direction_10m DOUBLE,
            visibility DOUBLE,
            weather_code INT
        )
        USING iceberg
        PARTITIONED BY (
            country_code,
            days(measured_at_utc)
        )
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet',
            'write.distribution-mode' = 'hash'
        )
        """
    )
def overwrite_silver_table(
    silver_df: DataFrame,
) -> None:
    """
    Ghi lại toàn bộ trạng thái Silver đã chuẩn hóa.

    silver_df được tính từ toàn bộ Bronze và đã:
    - lọc historical
    - loại record không hợp lệ
    - deduplicate theo lần crawl mới nhất

    Overwrite toàn bảng giúp job idempotent và tránh bug
    MERGE INTO của Iceberg 1.11.0 trên Spark 4.1.
    """
    print("\nOverwriting Silver Iceberg table:")
    print(SILVER_TABLE)

    (
        silver_df
        .writeTo(SILVER_TABLE)
        .overwrite(lit(True))
    )

def validate_written_table(
    spark: SparkSession,
) -> None:
    written_df = (
        spark.table(SILVER_TABLE)
        .cache()
    )

    try:
        written_count = written_df.count()

        duplicate_business_keys = (
            written_df
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

        snapshot_count = (
            spark.sql(
                f"""
                SELECT COUNT(*) AS snapshot_count
                FROM {SILVER_TABLE}.snapshots
                """
            )
            .first()["snapshot_count"]
        )

        data_file_count = (
            spark.sql(
                f"""
                SELECT COUNT(*) AS data_file_count
                FROM {SILVER_TABLE}.files
                """
            )
            .first()["data_file_count"]
        )

        print("\nWritten Silver validation:")
        print("=" * 70)
        print(
            f"Table records:              "
            f"{written_count}"
        )
        print(
            f"Duplicate business keys:    "
            f"{duplicate_business_keys}"
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
                "Written Silver table is empty"
            )

        if duplicate_business_keys != 0:
            raise ValueError(
                "Written Silver table contains "
                "duplicate business keys"
            )

        if snapshot_count < 1:
            raise ValueError(
                "No Silver Iceberg snapshot exists"
            )

        if data_file_count < 1:
            raise ValueError(
                "No Silver Iceberg data file exists"
            )

    finally:
        written_df.unpersist()


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = None
    silver_df = None

    try:
        print(
            f"Spark master:   "
            f"{spark.sparkContext.master}"
        )
        print(
            f"Spark version:  "
            f"{spark.version}"
        )
        print(
            f"Bronze table:   "
            f"{BRONZE_TABLE}"
        )
        print(
            f"Silver table:   "
            f"{SILVER_TABLE}"
        )

        bronze_df = (
            spark.table(BRONZE_TABLE)
            .cache()
        )

        bronze_count = bronze_df.count()

        print(
            "\nBronze input records: "
            f"{bronze_count}"
        )

        if bronze_count == 0:
            raise ValueError(
                "Bronze waether table is empty"
            )

        silver_df = (
            build_silver_data(
                bronze_df
            )
            .cache()
        )

        print("\nSilver schema:")
        silver_df.printSchema()

        print("\nSample Silver records:")
        (
            silver_df
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

        validate_silver_data(
            silver_df
        )

        create_silver_table(
            spark
        )

        overwrite_silver_table(
    silver_df
)

        validate_written_table(
            spark
        )

        print(
            "\nweather  SILVER "
            "ICEBERG JOB: PASS"
        )

    finally:
        if silver_df is not None:
            silver_df.unpersist()

        if bronze_df is not None:
            bronze_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
