FROM python:3.12-slim

ARG DBT_TRINO_VERSION=1.10.3

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    DBT_PROFILES_DIR=/usr/app/dbt

WORKDIR /usr/app/dbt

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && python -m pip install --upgrade pip \
    && python -m pip install \
        "dbt-trino==${DBT_TRINO_VERSION}"

ENTRYPOINT ["dbt"]