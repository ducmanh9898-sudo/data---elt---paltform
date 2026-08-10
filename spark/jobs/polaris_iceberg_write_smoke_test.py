import os

from pyspark.sql import SparkSession


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}"
        )

    return value


polaris_uri = required_env("POLARIS_URI").rstrip("/")
catalog_name = required_env("POLARIS_CATALOG_NAME")
client_id = required_env("POLARIS_SPARK_CLIENT_ID")
client_secret = required_env("POLARIS_SPARK_CLIENT_SECRET")
scope = os.getenv("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL")
aws_region = os.getenv("AWS_REGION", "us-east-1")
minio_endpoint = required_env("MINIO_ENDPOINT_INTERNAL")
minio_access_key = required_env("MINIO_ACCESS_KEY")
minio_secret_key = required_env("MINIO_SECRET_KEY")
oauth2_uri = f"{polaris_uri}/v1/oauth/tokens"
table_name = "polaris.bronze.iceberg_write_smoke_test"

spark = (
    SparkSession.builder
    .appName("PolarisIcebergWriteSmokeTest")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions."
        "IcebergSparkSessionExtensions",
    )
    .config(
        "spark.sql.catalog.polaris",
        "org.apache.iceberg.spark.SparkCatalog",
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
        "spark.sql.catalog.polaris.warehouse",
        catalog_name,
    )
    .config(
        "spark.sql.catalog.polaris.credential",
        f"{client_id}:{client_secret}",
    )
    .config(
        "spark.sql.catalog.polaris.oauth2-server-uri",
        oauth2_uri,
    )
    .config(
        "spark.sql.catalog.polaris.scope",
        scope,
    )
    .config(
        "spark.sql.catalog.polaris.token-refresh-enabled",
        "false",
    )
    
    .config(
        "spark.sql.catalog.polaris.client.region",
        aws_region,)
    .config(
    "spark.sql.catalog.polaris.io-impl",
    "org.apache.iceberg.aws.s3.S3FileIO",
)
.config(
    "spark.sql.catalog.polaris.s3.endpoint",
    minio_endpoint,
)
.config(
    "spark.sql.catalog.polaris.s3.path-style-access",
    "true",
)
.config(
    "spark.sql.catalog.polaris.s3.access-key-id",
    minio_access_key,
)
.config(
    "spark.sql.catalog.polaris.s3.secret-access-key",
    minio_secret_key,
)
    .config(
        "spark.sql.session.timeZone",
        "UTC",
    )
    .getOrCreate()
)

try:
    print(f"Spark version: {spark.version}")
    print(f"Polaris URI: {polaris_uri}")
    print(f"OAuth2 URI: {oauth2_uri}")
    print(f"Polaris catalog: {catalog_name}")
    print(f"Target table: {table_name}")

    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS polaris.bronze"
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT,
            component STRING,
            written_at_utc TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
        """
    )

    # INSERT OVERWRITE giúp chạy lại không tạo business duplicates.
    spark.sql(
        f"""
        INSERT OVERWRITE {table_name}
        SELECT
            1 AS id,
            'polaris' AS component,
            current_timestamp() AS written_at_utc

        UNION ALL

        SELECT
            2 AS id,
            'minio' AS component,
            current_timestamp() AS written_at_utc
        """
    )

    print("Table data:")
    spark.sql(
        f"""
        SELECT *
        FROM {table_name}
        ORDER BY id
        """
    ).show(truncate=False)

    row_count = spark.sql(
        f"""
        SELECT COUNT(*) AS row_count
        FROM {table_name}
        """
    ).first()["row_count"]

    duplicate_count = spark.sql(
        f"""
        SELECT COUNT(*) AS duplicate_count
        FROM (
            SELECT id
            FROM {table_name}
            GROUP BY id
            HAVING COUNT(*) > 1
        )
        """
    ).first()["duplicate_count"]

    snapshots = spark.sql(
        f"""
        SELECT
            committed_at,
            snapshot_id,
            parent_id,
            operation
        FROM {table_name}.snapshots
        ORDER BY committed_at DESC
        """
    )

    files = spark.sql(
        f"""
        SELECT
            file_path,
            file_format,
            record_count,
            file_size_in_bytes
        FROM {table_name}.files
        """
    )

    snapshot_count = snapshots.count()
    data_file_count = files.count()

    print("Iceberg snapshots:")
    snapshots.show(truncate=False)

    print("Iceberg data files:")
    files.show(truncate=False)

    print(f"Row count: {row_count}")
    print(f"Duplicate business keys: {duplicate_count}")
    print(f"Snapshot count: {snapshot_count}")
    print(f"Data file count: {data_file_count}")

    if row_count != 2:
        raise RuntimeError(
            f"Expected 2 rows, received {row_count}"
        )

    if duplicate_count != 0:
        raise RuntimeError(
            f"Expected 0 duplicate IDs, received {duplicate_count}"
        )

    if snapshot_count < 1:
        raise RuntimeError("No Iceberg snapshot was created")

    if data_file_count < 1:
        raise RuntimeError("No Iceberg data file was created")

    print("POLARIS ICEBERG WRITE SMOKE TEST: PASS")

finally:
    spark.stop()
