import os
from urllib.parse import quote_plus


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


db_user = quote_plus(required_env("SUPERSET_DB_USER"))
db_password = quote_plus(required_env("SUPERSET_DB_PASSWORD"))
db_name = quote_plus(required_env("SUPERSET_DB"))

SECRET_KEY = required_env("SUPERSET_SECRET_KEY")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{db_user}:{db_password}"
    f"@superset-postgres:5432/{db_name}"
)

SQLALCHEMY_TRACK_MODIFICATIONS = False

# This project runs locally through Docker Compose.
WTF_CSRF_ENABLED = True
