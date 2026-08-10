import json
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    date_trunc,
    lit,
    row_number,
    to_date,
    to_timestamp,
    to_utc_timestamp,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


# ============================================================
# PATH CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "weather"
)

CLEAN_DIR = (
    PROJECT_ROOT
    / "data"
    / "clean"
    / "weather"
)


# Cho phép tối đa 5% record thiếu temperature_2m.
MAX_TEMPERATURE_NULL_RATE = 0.05
OUTPUT_PARTITIONS = 24

# ============================================================
# FLATTENED RECORD SCHEMA
# ============================================================

# Schema trung gian sau khi Python flatten Raw JSON.
#
# Khai báo schema rõ ràng giúp Spark không phải tự suy luận
# kiểu dữ liệu và tránh lỗi khi một cột có nhiều giá trị NULL.
FLATTENED_RECORD_SCHEMA = StructType([
    StructField(
        "city_id",
        LongType(),
        True,
    ),
    StructField(
        "city_name",
        StringType(),
        True,
    ),
    StructField(
        "country_code",
        StringType(),
        True,
    ),
    StructField(
        "country_name",
        StringType(),
        True,
    ),
    StructField(
        "latitude",
        DoubleType(),
        True,
    ),
    StructField(
        "longitude",
        DoubleType(),
        True,
    ),

    StructField(
        "source",
        StringType(),
        True,
    ),
    StructField(
        "source_system",
        StringType(),
        True,
    ),
    StructField(
        "dataset_name",
        StringType(),
        True,
    ),
    StructField(
        "dataset_kind",
        StringType(),
        True,
    ),

    StructField(
        "requested_past_days",
        IntegerType(),
        True,
    ),
    StructField(
        "requested_forecast_days",
        IntegerType(),
        True,
    ),

    StructField(
        "source_file",
        StringType(),
        True,
    ),
    StructField(
        "crawled_at_utc_raw",
        StringType(),
        True,
    ),

    StructField(
        "timezone",
        StringType(),
        True,
    ),
    StructField(
        "utc_offset_seconds",
        IntegerType(),
        True,
    ),

    StructField(
        "measured_at_local_raw",
        StringType(),
        True,
    ),

    StructField(
        "temperature_2m",
        DoubleType(),
        True,
    ),
    StructField(
        "relative_humidity_2m",
        DoubleType(),
        True,
    ),
    StructField(
        "precipitation",
        DoubleType(),
        True,
    ),
    StructField(
        "rain",
        DoubleType(),
        True,
    ),
    StructField(
        "surface_pressure",
        DoubleType(),
        True,
    ),
    StructField(
        "cloud_cover",
        DoubleType(),
        True,
    ),
    StructField(
        "wind_speed_10m",
        DoubleType(),
        True,
    ),
    StructField(
        "wind_direction_10m",
        DoubleType(),
        True,
    ),
    StructField(
        "visibility",
        DoubleType(),
        True,
    ),

    # Đọc tạm bằng DoubleType để tương thích
    # nếu API trả 3 hoặc 3.0.
    StructField(
        "weather_code_raw",
        DoubleType(),
        True,
    ),
])


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session() -> SparkSession:
    """
    Tạo SparkSession chạy local.

    local[4]:
        Chỉ cho phép tối đa 4 Spark task chạy đồng thời,
        tránh quá nhiều writer cùng tranh RAM.

    spark.sql.shuffle.partitions:
        Giảm số shuffle partition mặc định từ 200 xuống 24.

    Adaptive Query Execution:
        Cho phép Spark tự gộp các partition nhỏ khi phù hợp.
    """
    return (
        SparkSession.builder
        .appName(
            "EnvironmentWeatherTransform"
        )
        .master("local[4]")
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
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true",
        )
        .getOrCreate()
    )

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_array_value(
    values: Any,
    index: int,
) -> Any:
    """
    Lấy phần tử tại vị trí index trong một array.

    Nếu array không tồn tại hoặc ngắn hơn hourly.time,
    trả về None thay vì gây IndexError.
    """

    if not isinstance(values, list):
        return None

    if index >= len(values):
        return None

    return values[index]


def to_optional_float(
    value: Any,
    field_name: str,
) -> float | None:
    """
    Chuẩn hóa một giá trị thành float.

    Chấp nhận:
        25
        25.5
        "25.5"

    None được giữ nguyên.
    """

    if value is None:
        return None

    if isinstance(value, bool):
        raise ValueError(
            f"{field_name} cannot be boolean: "
            f"{value}"
        )

    try:
        return float(value)

    except (
        TypeError,
        ValueError,
    ) as error:
        raise ValueError(
            f"Invalid numeric value for "
            f"{field_name}: {value!r}"
        ) from error


# ============================================================
# RAW JSON FLATTEN
# ============================================================

def flatten_json_file(
    file_path: Path,
) -> list[dict[str, Any]]:
    """
    Đọc một Weather Raw JSON và flatten hourly arrays.

    Một phần tử hourly.time sẽ trở thành một record.

    Python thực hiện:
        JSON parsing
        flatten hourly arrays

    Spark thực hiện:
        timestamp conversion
        timezone conversion
        historical filtering
        deduplication
        data-quality validation
        Parquet writing
    """

    with file_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        payload = json.load(file)

    metadata = payload.get(
        "metadata",
        {},
    )

    api_data = payload.get(
        "data",
        {},
    )

    hourly = api_data.get(
        "hourly",
        {},
    )

    if not isinstance(
        metadata,
        dict,
    ):
        raise ValueError(
            f"Invalid metadata object: "
            f"{file_path}"
        )

    if not isinstance(
        api_data,
        dict,
    ):
        raise ValueError(
            f"Invalid data object: "
            f"{file_path}"
        )

    if not isinstance(
        hourly,
        dict,
    ):
        raise ValueError(
            f"Invalid hourly object: "
            f"{file_path}"
        )

    times = hourly.get(
        "time",
        [],
    )

    if not isinstance(
        times,
        list,
    ):
        raise ValueError(
            f"Invalid hourly.time array: "
            f"{file_path}"
        )

    timezone_name = api_data.get(
        "timezone"
    )

    utc_offset_seconds = api_data.get(
        "utc_offset_seconds"
    )

    records: list[
        dict[str, Any]
    ] = []

    for (
        index,
        measured_at_local_raw,
    ) in enumerate(times):
        record = {
            "city_id": metadata.get(
                "city_id"
            ),
            "city_name": metadata.get(
                "city_name"
            ),
            "country_code": metadata.get(
                "country_code"
            ),
            "country_name": metadata.get(
                "country_name"
            ),

            "latitude": to_optional_float(
                metadata.get(
                    "latitude"
                ),
                "latitude",
            ),

            "longitude": to_optional_float(
                metadata.get(
                    "longitude"
                ),
                "longitude",
            ),

            "source": metadata.get(
                "source",
                "Open-Meteo Weather API",
            ),

            "source_system": metadata.get(
                "source_system",
                "open_meteo",
            ),

            "dataset_name": metadata.get(
                "dataset_name",
                "weather_hourly",
            ),

            "dataset_kind": metadata.get(
                "dataset_kind",
                "weather_model",
            ),

            "requested_past_days": (
                metadata.get(
                    "requested_past_days"
                )
            ),

            "requested_forecast_days": (
                metadata.get(
                    "requested_forecast_days"
                )
            ),

            # Lưu relative path phục vụ data lineage.
            "source_file": (
                file_path
                .relative_to(PROJECT_ROOT)
                .as_posix()
            ),

            "crawled_at_utc_raw": (
                metadata.get(
                    "crawled_at_utc"
                )
            ),

            "timezone": timezone_name,

            "utc_offset_seconds": (
                utc_offset_seconds
            ),

            "measured_at_local_raw": (
                measured_at_local_raw
            ),

            "temperature_2m": (
                to_optional_float(
                    get_array_value(
                        hourly.get(
                            "temperature_2m"
                        ),
                        index,
                    ),
                    "temperature_2m",
                )
            ),

            "relative_humidity_2m": (
                to_optional_float(
                    get_array_value(
                        hourly.get(
                            "relative_humidity_2m"
                        ),
                        index,
                    ),
                    "relative_humidity_2m",
                )
            ),

            "precipitation": (
                to_optional_float(
                    get_array_value(
                        hourly.get(
                            "precipitation"
                        ),
                        index,
                    ),
                    "precipitation",
                )
            ),

            "rain": (
                to_optional_float(
                    get_array_value(
                        hourly.get(
                            "rain"
                        ),
                        index,
                    ),
                    "rain",
                )
            ),

            "surface_pressure": (
                to_optional_float(
                    get_array_value(
                        hourly.get(
                            "surface_pressure"
                        ),
                        index,
                    ),
                    "surface_pressure",
                )
            ),

            "cloud_cover": (
                to_optional_float(
                    get_array_value(
                        hourly.get(
                            "cloud_cover"
                        ),
                        index,
                    ),
                    "cloud_cover",
                )
            ),

            "wind_speed_10m": (
                to_optional_float(
                    get_array_value(
                        hourly.get(
                            "wind_speed_10m"
                        ),
                        index,
                    ),
                    "wind_speed_10m",
                )
            ),

            "wind_direction_10m": (
                to_optional_float(
                    get_array_value(
                        hourly.get(
                            "wind_direction_10m"
                        ),
                        index,
                    ),
                    "wind_direction_10m",
                )
            ),

            "visibility": (
                to_optional_float(
                    get_array_value(
                        hourly.get(
                            "visibility"
                        ),
                        index,
                    ),
                    "visibility",
                )
            ),

            "weather_code_raw": (
                to_optional_float(
                    get_array_value(
                        hourly.get(
                            "weather_code"
                        ),
                        index,
                    ),
                    "weather_code",
                )
            ),
        }

        records.append(
            record
        )

    return records


def read_all_raw_records() -> list[
    dict[str, Any]
]:
    """
    Đọc và flatten toàn bộ Weather Raw JSON.

    File có forecast vẫn được đọc.
    Spark sẽ lọc forecast ở bước build_clean_data().
    """

    json_files = sorted(
        RAW_DIR.rglob(
            "*.json"
        )
    )

    if not json_files:
        raise FileNotFoundError(
            "No Weather Raw JSON files "
            f"found under: {RAW_DIR}"
        )

    records: list[
        dict[str, Any]
    ] = []

    failed_files: list[
        tuple[Path, str]
    ] = []

    for file_path in json_files:
        print(
            f"Reading: {file_path}"
        )

        try:
            file_records = (
                flatten_json_file(
                    file_path
                )
            )

            records.extend(
                file_records
            )

        except (
            json.JSONDecodeError,
            KeyError,
            TypeError,
            ValueError,
            OSError,
        ) as error:
            failed_files.append(
                (
                    file_path,
                    str(error),
                )
            )

    if failed_files:
        print(
            "\nInvalid Weather Raw files:"
        )

        for (
            file_path,
            error_message,
        ) in failed_files:
            print(
                f"- {file_path}: "
                f"{error_message}"
            )

        raise RuntimeError(
            f"{len(failed_files)} Weather "
            "Raw JSON file(s) could not "
            "be processed"
        )

    if not records:
        raise ValueError(
            "Weather Raw JSON files "
            "contain no hourly records"
        )

    print(
        f"\nWeather Raw JSON files: "
        f"{len(json_files)}"
    )

    print(
        f"Raw flattened records: "
        f"{len(records)}"
    )

    return records


# ============================================================
# DATAFRAME CREATION
# ============================================================

def create_source_dataframe(
    spark: SparkSession,
    records: list[dict[str, Any]],
) -> DataFrame:
    """
    Tạo Spark DataFrame bằng schema đã khai báo.
    """

    return spark.createDataFrame(
        records,
        schema=FLATTENED_RECORD_SCHEMA,
    )


# ============================================================
# PREPARE DATA
# ============================================================

def prepare_data(
    source_df: DataFrame,
) -> DataFrame:
    """
    Chuẩn hóa timestamp và timezone.

    measured_at_local:
        Timestamp địa phương của thành phố.

    measured_at_utc:
        Timestamp được chuyển về UTC.

    historical_cutoff_utc:
        Đầu giờ tại thời điểm crawler chạy.

    Chỉ record có:

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
                col(
                    "crawled_at_utc_raw"
                )
            ),
        )
        .withColumn(
            "measured_at_utc",
            to_utc_timestamp(
                col(
                    "measured_at_local"
                ),
                col(
                    "timezone"
                ),
            ),
        )
        .withColumn(
            "historical_cutoff_utc",
            date_trunc(
                "hour",
                col(
                    "crawled_at_utc"
                ),
            ),
        )
        .withColumn(
            "measurement_date_local",
            to_date(
                col(
                    "measured_at_local"
                )
            ),
        )
        .withColumn(
            "measurement_date_utc",
            to_date(
                col(
                    "measured_at_utc"
                )
            ),
        )
        .withColumn(
            "weather_code",
            col(
                "weather_code_raw"
            ).cast(
                "integer"
            ),
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
    """
    Kiểm tra khóa, timezone và timestamp
    trước khi lọc historical.
    """

    missing_city_id = (
        prepared_df
        .filter(
            col(
                "city_id"
            ).isNull()
        )
        .count()
    )

    missing_country_code = (
        prepared_df
        .filter(
            col(
                "country_code"
            ).isNull()
        )
        .count()
    )

    missing_timezone = (
        prepared_df
        .filter(
            col(
                "timezone"
            ).isNull()
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
            col(
                "measured_at_utc"
            )
            >= col(
                "historical_cutoff_utc"
            )
        )
        .count()
    )

    print(
        "\nPrepared Weather validation:"
    )

    print(
        "=" * 72
    )

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

    print(
        "=" * 72
    )

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
            "Prepared Weather data contains "
            "missing or invalid required values"
        )


# ============================================================
# BUILD CLEAN DATA
# ============================================================

def build_clean_data(
    prepared_df: DataFrame,
) -> DataFrame:
    """
    Chỉ giữ dữ liệu historical hoàn chỉnh
    và loại record trùng.

    Business key:

        city_id
        measured_at_utc
        source_system
        dataset_name

    Nếu nhiều lần crawl chứa cùng một giờ,
    giữ record từ lần crawl mới nhất.
    """

    historical_df = (
        prepared_df
        .filter(
            col(
                "measured_at_utc"
            )
            < col(
                "historical_cutoff_utc"
            )
        )
        .withColumn(
            "record_type",
            lit(
                "historical"
            ),
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
            col(
                "crawled_at_utc"
            ).desc_nulls_last(),

            col(
                "source_file"
            ).desc(),
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
            col(
                "_row_number"
            ) == 1
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

            # Partition columns đặt cuối.
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
    """
    Kiểm tra dữ liệu Weather Clean sau khi:
        lọc forecast
        loại current hour
        deduplicate
    """

    total_records = (
        clean_df.count()
    )

    total_cities = (
        clean_df
        .select(
            "city_id"
        )
        .distinct()
        .count()
    )

    total_countries = (
        clean_df
        .select(
            "country_code"
        )
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
            col(
                "count"
            ) > 1
        )
        .count()
    )

    future_records = (
        clean_df
        .filter(
            col(
                "measured_at_utc"
            )
            >= date_trunc(
                "hour",
                col(
                    "crawled_at_utc"
                ),
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
            |
            (
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
            (
                col(
                    "cloud_cover"
                ) < 0
            )
            |
            (
                col(
                    "cloud_cover"
                ) > 100
            )
        )
        .count()
    )

    negative_precipitation_rows = (
        clean_df
        .filter(
            col(
                "precipitation"
            ) < 0
        )
        .count()
    )

    negative_rain_rows = (
        clean_df
        .filter(
            col(
                "rain"
            ) < 0
        )
        .count()
    )

    invalid_surface_pressure_rows = (
        clean_df
        .filter(
            col(
                "surface_pressure"
            ) <= 0
        )
        .count()
    )

    negative_wind_speed_rows = (
        clean_df
        .filter(
            col(
                "wind_speed_10m"
            ) < 0
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
            |
            (
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
            col(
                "visibility"
            ) < 0
        )
        .count()
    )

    invalid_weather_code_rows = (
        clean_df
        .filter(
            (
                col(
                    "weather_code"
                ) < 0
            )
            |
            (
                col(
                    "weather_code"
                ) > 99
            )
        )
        .count()
    )

    temperature_null_rate = (
        missing_temperature
        / total_records
        if total_records > 0
        else 0.0
    )

    print(
        "\nClean Weather validation:"
    )

    print(
        "=" * 72
    )

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
        f"Duplicate business keys:       "
        f"{duplicate_keys}"
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

    print(
        "=" * 72
    )

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
            "Data quality failed: relative humidity "
            "must be between 0 and 100"
        )

    if invalid_cloud_cover_rows != 0:
        raise ValueError(
            "Data quality failed: cloud cover "
            "must be between 0 and 100"
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
            "Data quality failed: wind direction "
            "must be between 0 and 360"
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
# WRITE CLEAN PARQUET
# ============================================================

def write_clean_parquet(
    clean_df: DataFrame,
) -> None:
    """
    Ghi Weather Clean Zone dưới dạng Parquet.

    Trước khi ghi, dữ liệu được repartition theo:
        country_code
        measurement_date_local

    Việc này giúp tất cả record thuộc cùng một quốc gia
    và cùng một ngày được đưa về cùng Spark partition,
    hạn chế tạo nhiều file nhỏ trong một thư mục ngày.
    """
    CLEAN_DIR.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

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
        .mode(
            "overwrite"
        )
        .partitionBy(
            "country_code",
            "measurement_date_local",
        )
        .parquet(
            str(CLEAN_DIR)
        )
    )

# ============================================================
# MAIN
# ============================================================

def main() -> None:
    print(
        "=" * 72
    )

    print(
        "Open-Meteo Weather Spark Transform"
    )

    print(
        "=" * 72
    )

    print(
        f"Raw directory:   {RAW_DIR}"
    )

    print(
        f"Clean directory: {CLEAN_DIR}"
    )

    print(
        "=" * 72
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    prepared_df = None
    clean_df = None

    try:
        records = read_all_raw_records()

        source_df = create_source_dataframe(
            spark=spark,
            records=records,
        )

        print(
            "\nSource Weather schema:"
        )

        source_df.printSchema()

        prepared_df = (
            prepare_data(
                source_df
            )
            .cache()
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

        print(
            "\nWeather Clean Parquet "
            "saved successfully:"
        )

        print(
            CLEAN_DIR
        )

    finally:
        if clean_df is not None:
            clean_df.unpersist()

        if prepared_df is not None:
            prepared_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()