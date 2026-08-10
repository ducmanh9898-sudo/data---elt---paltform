from datetime import timedelta

import pendulum

from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


PROJECT_DIR = "/opt/airflow"
SPARK_JOB_DIR = "/opt/airflow/spark/jobs"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

SPARK_MASTER_URL = "spark://spark-master:7077"
PYTHON_ENV = {
    "PYTHONPATH": "/opt/airflow/src",
}

default_args = {
    "owner": "environment-data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
SPARK_JARS = ",".join(
    [
        "/opt/airflow/jars/hadoop-aws.jar",
        "/opt/airflow/jars/aws-sdk-v2-bundle.jar",
        "/opt/airflow/jars/iceberg-spark-runtime.jar",
        "/opt/airflow/jars/iceberg-aws-bundle.jar",
    ]
)

def build_spark_submit_command(
    job_file: str,
) -> str:
    return (
        "set -euo pipefail; "
        "spark-submit "
        f"--master {SPARK_MASTER_URL} "
        "--deploy-mode client "
        f"--jars {SPARK_JARS} "
        "--conf spark.driver.host=airflow-scheduler "
        "--conf spark.driver.bindAddress=0.0.0.0 "
        f"{SPARK_JOB_DIR}/{job_file}"
    )

with DAG(
    dag_id="environment_platform_elt",
    description=(
        "Crawl environmental APIs, build Bronze and Silver "
        "Iceberg tables, build dbt Gold models, and validate "
        "the Lakehouse through Trino"
    ),
    start_date=pendulum.datetime(
        2026,
        7,
        1,
        tz="UTC",
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[
        "environment",
        "lakehouse",
        "iceberg",
        "elt",
    ],
) as dag:

    # ========================================================
    # 1. RAW INGESTION
    # ========================================================

    crawl_air_quality = BashOperator(
        task_id="crawl_air_quality",
        bash_command=(
            "set -euo pipefail; "
            "python "
            "/opt/airflow/scripts/crawl_air_quality.py"
        ),
        cwd=PROJECT_DIR,
        
        execution_timeout=timedelta(minutes=15),
    )

    crawl_weather = BashOperator(
        task_id="crawl_weather",
        bash_command=(
            "set -euo pipefail; "
            "python "
            "/opt/airflow/scripts/crawl_weather.py"
        ),
        cwd=PROJECT_DIR,
        

        execution_timeout=timedelta(minutes=15),
    )

    # ========================================================
    # 2. AIR QUALITY BRONZE / SILVER
    # ========================================================

    build_air_quality_bronze = BashOperator(
        task_id="build_air_quality_bronze",
        bash_command=build_spark_submit_command(
            "air_quality_bronze_iceberg_job.py"
        ),
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=45),
    )

    build_air_quality_silver = BashOperator(
        task_id="build_air_quality_silver",
        bash_command=build_spark_submit_command(
            "air_quality_silver_iceberg_job.py"
        ),
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=45),
    )

    # ========================================================
    # 3. WEATHER BRONZE / SILVER
    # ========================================================

    build_weather_bronze = BashOperator(
        task_id="build_weather_bronze",
        bash_command=build_spark_submit_command(
            "weather_bronze_iceberg_job.py"
        ),
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=45),
    )

    build_weather_silver = BashOperator(
        task_id="build_weather_silver",
        bash_command=build_spark_submit_command(
            "weather_silver_iceberg_job.py"
        ),
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=45),
    )

    # ========================================================
    # 4. DBT INTERMEDIATE / GOLD
    # ========================================================

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "set -euo pipefail; "
            f"dbt build "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
        cwd=DBT_PROJECT_DIR,
        execution_timeout=timedelta(minutes=30),
    )

    # ========================================================
    # 5. TRINO SMOKE CHECK
    # ========================================================

    trino_smoke_check = SQLCheckOperator(
        task_id="trino_smoke_check",
        conn_id="trino_default",
        sql="""
            WITH table_counts AS (
                SELECT
                    (
                        SELECT COUNT(*)
                        FROM iceberg.silver.air_quality_hourly
                    ) AS air_quality_silver_rows,

                    (
                        SELECT COUNT(*)
                        FROM iceberg.silver.weather_hourly
                    ) AS weather_silver_rows,

                    (
                        SELECT COUNT(*)
                        FROM iceberg.gold.gold_city_environment_hourly
                    ) AS gold_hourly_rows,

                    (
                        SELECT COUNT(*)
                        FROM iceberg.gold.gold_city_environment_daily
                    ) AS gold_daily_rows,

                    (
                        SELECT COUNT(*)
                        FROM iceberg.gold.gold_environmental_alerts
                    ) AS gold_alert_rows,

                    (
                        SELECT COUNT(*)
                        FROM iceberg.gold.gold_weather_air_quality_correlation
                    ) AS gold_correlation_rows
            )
            SELECT
                air_quality_silver_rows > 0
                AND weather_silver_rows > 0
                AND gold_hourly_rows > 0
                AND gold_daily_rows > 0
                AND gold_alert_rows >= 0
                AND gold_correlation_rows >= 0
            FROM table_counts
        """,
        execution_timeout=timedelta(minutes=10),
    )

    # ========================================================
    # 6. DEPENDENCIES
    # ========================================================

    (
        crawl_air_quality
        >> build_air_quality_bronze
        >> build_air_quality_silver
    )

    (
        crawl_weather
        >> build_weather_bronze
        >> build_weather_silver
    )

    [
        build_air_quality_silver,
        build_weather_silver,
    ] >> dbt_build >> trino_smoke_check