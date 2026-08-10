FROM apache/superset:6.1.0

USER root

RUN . /app/.venv/bin/activate && \
    uv pip install --no-cache-dir \
        psycopg2-binary \
        "apache-superset[trino]==6.1.0"

USER superset
