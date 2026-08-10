import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"


def require_env(name: str) -> str:
    value = os.getenv(name)

    if value is None or not value.strip():
        raise RuntimeError(
            f"Missing required environment variable: {name}"
        )

    return value.strip()


def get_bool_env(
    name: str,
    default: bool = False,
) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    normalized = value.strip().lower()

    if normalized in {"true", "1", "yes", "y"}:
        return True

    if normalized in {"false", "0", "no", "n"}:
        return False

    raise ValueError(
        f"{name} must be true or false"
    )


def get_project_path(
    name: str,
    default: str,
) -> Path:
    path = Path(
        os.getenv(name, default)
    )

    if path.is_absolute():
        return path

    return PROJECT_ROOT / path


@dataclass(frozen=True)
class Settings:
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    minio_bucket: str

    kafka_bootstrap_servers: str
    kafka_raw_topic: str
    kafka_dlq_topic: str
    kafka_late_topic: str

    open_meteo_air_quality_url: str
    open_meteo_weather_url: str

    air_quality_past_days: int
    air_quality_forecast_days: int
    air_quality_timeout_seconds: int

    weather_past_days: int
    weather_forecast_days: int
    weather_timeout_seconds: int
    weather_request_delay_seconds: float

    raw_air_quality_dir: Path
    raw_weather_dir: Path

    @property
    def postgres_config(self) -> dict[str, object]:
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "dbname": self.postgres_db,
            "user": self.postgres_user,
            "password": self.postgres_password,
        }


@lru_cache
def get_settings() -> Settings:
    # Docker environment vẫn được ưu tiên hơn .env.
    load_dotenv(
        ENV_FILE,
        override=False,
    )

    return Settings(
        postgres_host=require_env(
            "POSTGRES_HOST"
        ),
        postgres_port=int(
            require_env("POSTGRES_PORT")
        ),
        postgres_db=require_env(
            "POSTGRES_DB"
        ),
        postgres_user=require_env(
            "POSTGRES_USER"
        ),
        postgres_password=require_env(
            "POSTGRES_PASSWORD"
        ),

        minio_endpoint=require_env(
            "MINIO_ENDPOINT"
        ),
        minio_access_key=require_env(
            "MINIO_ACCESS_KEY"
        ),
        minio_secret_key=require_env(
            "MINIO_SECRET_KEY"
        ),
        minio_secure=get_bool_env(
            "MINIO_SECURE",
            default=False,
        ),
        minio_bucket=require_env(
            "MINIO_BUCKET"
        ),
        kafka_bootstrap_servers=require_env(
            "KAFKA_BOOTSTRAP_SERVERS"
        ),
        kafka_raw_topic=require_env(
            "KAFKA_RAW_TOPIC"
        ),
        kafka_dlq_topic=require_env(
            "KAFKA_DLQ_TOPIC"
        ),
        kafka_late_topic=require_env(
            "KAFKA_LATE_TOPIC"
        ),

        open_meteo_air_quality_url=os.getenv(
            "OPEN_METEO_AIR_QUALITY_URL",
            (
                "https://air-quality-api."
                "open-meteo.com/v1/air-quality"
            ),
        ),
        open_meteo_weather_url=os.getenv(
            "OPEN_METEO_WEATHER_URL",
            "https://api.open-meteo.com/v1/forecast",
        ),

        air_quality_past_days=int(
            os.getenv(
                "AIR_QUALITY_PAST_DAYS",
                "7",
            )
        ),
        air_quality_forecast_days=int(
            os.getenv(
                "AIR_QUALITY_FORECAST_DAYS",
                "0",
            )
        ),
        air_quality_timeout_seconds=int(
            os.getenv(
                "AIR_QUALITY_TIMEOUT_SECONDS",
                "30",
            )
        ),

        weather_past_days=int(
            os.getenv(
                "WEATHER_PAST_DAYS",
                "7",
            )
        ),
        weather_forecast_days=int(
            os.getenv(
                "WEATHER_FORECAST_DAYS",
                "7",
            )
        ),
        weather_timeout_seconds=int(
            os.getenv(
                "WEATHER_TIMEOUT_SECONDS",
                "60",
            )
        ),
        weather_request_delay_seconds=float(
            os.getenv(
                "WEATHER_REQUEST_DELAY_SECONDS",
                "0.2",
            )
        ),

        raw_air_quality_dir=get_project_path(
            "RAW_AIR_QUALITY_DIR",
            "data/raw/air_quality",
        ),
        raw_weather_dir=get_project_path(
            "RAW_WEATHER_DIR",
            "data/raw/weather",
        ),
    )