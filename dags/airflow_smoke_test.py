from datetime import datetime, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="airflow_smoke_test",
    description="Kiểm tra Airflow có thể parse và chạy task",
    start_date=datetime(
        2026,
        7,
        1,
        tzinfo=timezone.utc,
    ),
    schedule=None,
    catchup=False,
    tags=["environment-data-platform", "test"],
)
def airflow_smoke_test():

    @task
    def check_airflow() -> str:
        message = "Airflow is running successfully."
        print(message)
        return message

    check_airflow()


airflow_smoke_test()