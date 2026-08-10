import os

from pyspark.sql import SparkSession


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")

    return value


polaris_uri = required_env("POLARIS_URI")
catalog_name = required_env("POLARIS_CATALOG_NAME")
client_id = required_env("POLARIS_SPARK_CLIENT_ID")
client_secret = required_env("POLARIS_SPARK_CLIENT_SECRET")
scope = os.getenv("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL")
aws_region = os.getenv("AWS_REGION", "us-east-1")

spark = (
    SparkSession.builder
    .appName("PolarisNamespaceSmokeTest")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
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
        "spark.sql.catalog.polaris.scope",
        scope,
    )
    .config(
        "spark.sql.catalog.polaris.token-refresh-enabled",
        "false",
    )
    .config(
        "spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation",
        "vended-credentials",
    )
    .config(
        "spark.sql.catalog.polaris.client.region",
        aws_region,
    )
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

try:
    print(f"Spark version: {spark.version}")
    print(f"Polaris URI: {polaris_uri}")
    print(f"Polaris catalog: {catalog_name}")
    print(f"Spark principal configured: {bool(client_id)}")

    print("Namespaces before:")
    spark.sql("SHOW NAMESPACES IN polaris").show(
        truncate=False
    )

    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS polaris.bronze"
    )

    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS polaris.silver"
    )

    namespaces = {
        row.namespace
        for row in spark.sql(
            "SHOW NAMESPACES IN polaris"
        ).collect()
    }

    print(f"Namespaces after: {sorted(namespaces)}")

    required_namespaces = {"bronze", "silver"}
    missing = required_namespaces - namespaces

    if missing:
        raise RuntimeError(
            f"Missing namespaces after creation: {sorted(missing)}"
        )

    print("POLARIS NAMESPACE SMOKE TEST: PASS")

finally:
    spark.stop()
