from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_trunc,
    countDistinct,
    lit,
    row_number,
    to_date,
    to_utc_timestamp,
)
from pyspark.sql.window import Window

from air_quality_bronze_iceberg_job import (
    BRONZE_TABLE,
    create_spark_session,
)


SILVER_TABLE = (
    "polaris.silver.air_quality_hourly"
)


MAX_PM25_NULL_RATE = 0.05
def canonicalize_city_ids(
    historical_df: DataFrame,
) -> DataFrame:
    """
    Chuẩn hóa city_id lịch sử theo khóa tự nhiên:

        country_code + city_name

    Bronze vẫn giữ nguyên city_id từ Raw để phục vụ audit.
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


def build_silver_data(
    bronze_df: DataFrame,
) -> DataFrame:
    """
    Chuyển dữ liệu Bronze sang Silver.

    Các bước:
    1. Chuẩn hóa timestamp địa phương sang UTC.
    2. Chỉ giữ giờ historical đã hoàn chỉnh.
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
            & col("country_code").isNotNull()
            & col("city_name").isNotNull()
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

            "pm10",
            "pm2_5",
            "carbon_monoxide",
            "nitrogen_dioxide",
            "sulphur_dioxide",
            "ozone",
            "us_aqi",
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

    missing_pm25 = (
        silver_df
        .filter(
            col("pm2_5").isNull()
        )
        .count()
    )

    negative_pollutant_rows = (
        silver_df
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

    print("\nSilver data validation:")
    print("=" * 70)
    print(
        f"Silver records:             "
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
        f"NULL business keys:         "
        f"{null_business_keys}"
    )
    print(
        f"Duplicate business keys:    "
        f"{duplicate_business_keys}"
    )
    print(
        f"Future/current-hour rows:   "
        f"{future_or_current_rows}"
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
        f"{negative_pollutant_rows}"
    )
    print("=" * 70)
    print(
    f"City ID mapping conflicts:  "
    f"{city_id_mapping_conflicts}"
)
    print(
    f"City name ID conflicts:     "
    f"{city_name_mapping_conflicts}"
)

    if total_records == 0:
        raise ValueError(
            "Silver dataset is empty"
        )

    if null_business_keys != 0:
        raise ValueError(
            "Silver contains NULL business keys"
        )

    if duplicate_business_keys != 0:
        raise ValueError(
            "Silver contains duplicate business keys"
        )

    if future_or_current_rows != 0:
        raise ValueError(
            "Silver contains future or "
            "current-hour records"
        )

    if negative_pollutant_rows != 0:
        raise ValueError(
            "Silver contains negative "
            "pollutant values"
        )

    if pm25_null_rate > MAX_PM25_NULL_RATE:
        raise ValueError(
            "PM2.5 null rate "
            f"{pm25_null_rate:.2%} exceeds "
            f"{MAX_PM25_NULL_RATE:.0%}"
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
                "Bronze Air Quality table is empty"
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
            "\nAIR QUALITY SILVER "
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
