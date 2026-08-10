from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from dotenv import load_dotenv

from streaming.contracts.validate_sensor_reading import (
    CANONICAL_CITIES,
    load_json,
    validate_schema,
    validate_semantics,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SCHEMA_PATH = (
    PROJECT_ROOT
    / "streaming"
    / "contracts"
    / "sensor_reading_v1.schema.json"
)

DEFAULT_TOPIC = "environment.sensor-readings.raw"

RUNNING = True


CITY_PROFILES: dict[int, dict[str, float]] = {
    1: {
        "temperature": 24.0,
        "humidity": 58.0,
        "pm2_5": 14.0,
    },
    2: {
        "temperature": 25.0,
        "humidity": 52.0,
        "pm2_5": 17.0,
    },
    3: {
        "temperature": 23.0,
        "humidity": 61.0,
        "pm2_5": 13.0,
    },
    4: {
        "temperature": 17.0,
        "humidity": 72.0,
        "pm2_5": 10.0,
    },
    5: {
        "temperature": 19.0,
        "humidity": 70.0,
        "pm2_5": 11.0,
    },
    6: {
        "temperature": 21.0,
        "humidity": 63.0,
        "pm2_5": 12.0,
    },
    7: {
        "temperature": 22.0,
        "humidity": 61.0,
        "pm2_5": 13.0,
    },
    8: {
        "temperature": 28.0,
        "humidity": 69.0,
        "pm2_5": 15.0,
    },
    9: {
        "temperature": 29.0,
        "humidity": 67.0,
        "pm2_5": 14.0,
    },
    10: {
        "temperature": 27.0,
        "humidity": 72.0,
        "pm2_5": 18.0,
    },
    11: {
        "temperature": 28.0,
        "humidity": 70.0,
        "pm2_5": 17.0,
    },
    12: {
        "temperature": 31.0,
        "humidity": 76.0,
        "pm2_5": 19.0,
    },
}


def stop_gracefully(
    signum: int,
    frame: object,
) -> None:
    del signum, frame

    global RUNNING
    RUNNING = False


def clamp(
    value: float,
    minimum: float,
    maximum: float,
) -> float:
    return max(
        minimum,
        min(value, maximum),
    )


def format_utc_timestamp(
    timestamp: datetime,
) -> str:
    return (
        timestamp
        .astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def build_device_id(
    city_id: int,
    country_code: str,
) -> str:
    return (
        f"sensor-{country_code.lower()}-"
        f"{city_id:02d}-001"
    )


def generate_event(
    city_id: int,
    sequence_number: int,
    rng: random.Random,
) -> dict[str, Any]:
    city_name, country_code = (
        CANONICAL_CITIES[city_id]
    )

    profile = CITY_PROFILES[city_id]

    produced_at = datetime.now(timezone.utc)

    event_delay_ms = rng.randint(
        50,
        2500,
    )

    event_time = (
        produced_at
        - timedelta(milliseconds=event_delay_ms)
    )

    temperature = clamp(
        rng.gauss(
            profile["temperature"],
            2.5,
        ),
        -80.0,
        70.0,
    )

    humidity = clamp(
        rng.gauss(
            profile["humidity"],
            8.0,
        ),
        0.0,
        100.0,
    )

    pm2_5 = clamp(
        rng.gauss(
            profile["pm2_5"],
            5.0,
        ),
        0.0,
        2000.0,
    )

    pm10 = clamp(
        pm2_5
        + abs(
            rng.gauss(
                8.0,
                4.0,
            )
        ),
        0.0,
        3000.0,
    )

    carbon_monoxide = clamp(
        rng.gauss(
            260.0,
            65.0,
        ),
        0.0,
        100000.0,
    )

    nitrogen_dioxide = clamp(
        rng.gauss(
            18.0,
            7.0,
        ),
        0.0,
        2000.0,
    )

    return {
        "schema_version": "1.0",
        "event_type": (
            "environment.sensor_reading"
        ),
        "event_id": str(uuid.uuid4()),
        "source_system": "iot_simulator",
        "dataset_name": "sensor_readings",
        "device_id": build_device_id(
            city_id,
            country_code,
        ),
        "sequence_number": sequence_number,
        "city_id": city_id,
        "city_name": city_name,
        "country_code": country_code,
        "event_time_utc": format_utc_timestamp(
            event_time
        ),
        "produced_at_utc": format_utc_timestamp(
            produced_at
        ),
        "temperature_2m": round(
            temperature,
            2,
        ),
        "relative_humidity_2m": round(
            humidity,
            2,
        ),
        "pm2_5": round(
            pm2_5,
            2,
        ),
        "pm10": round(
            pm10,
            2,
        ),
        "carbon_monoxide": round(
            carbon_monoxide,
            2,
        ),
        "nitrogen_dioxide": round(
            nitrogen_dioxide,
            2,
        ),
    }


def resolve_environment_value(
    *names: str,
) -> str | None:
    for name in names:
        value = os.getenv(name)

        if value:
            return value.strip()

    return None


def create_producer(
    bootstrap_servers: str,
) -> Producer:
    return Producer(
        {
            "bootstrap.servers": (
                bootstrap_servers
            ),
            "client.id": (
                "environment-iot-simulator"
            ),
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "gzip",
            "linger.ms": 100,
            "delivery.timeout.ms": 30000,
            "request.timeout.ms": 10000,
            "max.in.flight.requests.per.connection": 5,
        }
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate validated IoT environmental "
            "sensor events and publish them to Kafka."
        )
    )

    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help=(
            "Number of events to generate. "
            "Use 0 to run continuously."
        ),
    )

    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=1.0,
        help=(
            "Delay between generated events."
        ),
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help=(
            "Optional random seed for repeatable "
            "measurement values."
        ),
    )

    parser.add_argument(
        "--city-id",
        type=int,
        action="append",
        choices=sorted(CANONICAL_CITIES),
        help=(
            "Restrict generation to one or more "
            "canonical city IDs."
        ),
    )

    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help=(
            "Kafka bootstrap servers. Overrides "
            "KAFKA_BOOTSTRAP_SERVERS."
        ),
    )

    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help=(
            "Kafka destination topic."
        ),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Generate and validate events without "
            "publishing them to Kafka."
        ),
    )

    args = parser.parse_args()

    if args.count < 0:
        parser.error(
            "--count must be greater than "
            "or equal to 0"
        )

    if args.interval_seconds < 0:
        parser.error(
            "--interval-seconds must be greater "
            "than or equal to 0"
        )

    return args


def main() -> int:
    signal.signal(
        signal.SIGINT,
        stop_gracefully,
    )

    signal.signal(
        signal.SIGTERM,
        stop_gracefully,
    )

    load_dotenv(
        PROJECT_ROOT / ".env"
    )

    args = parse_arguments()

    schema = load_json(SCHEMA_PATH)

    city_ids = (
        args.city_id
        if args.city_id
        else sorted(CANONICAL_CITIES)
    )

    topic = (
        args.topic
        or resolve_environment_value(
            "KAFKA_RAW_TOPIC",
            "KAFKA_SENSOR_RAW_TOPIC",
            "KAFKA_SENSOR_READINGS_RAW_TOPIC",
        )
        or DEFAULT_TOPIC
    )

    bootstrap_servers = (
        args.bootstrap_servers
        or resolve_environment_value(
            "KAFKA_BOOTSTRAP_SERVERS"
        )
    )

    if (
        not args.dry_run
        and not bootstrap_servers
    ):
        print(
            "FAIL: KAFKA_BOOTSTRAP_SERVERS "
            "is not configured",
            file=sys.stderr,
        )
        return 1

    rng = random.Random(args.seed)

    sequence_numbers: defaultdict[str, int] = (
        defaultdict(int)
    )

    delivery_stats = {
        "delivered": 0,
        "failed": 0,
    }

    def delivery_callback(
        error: object,
        message: object,
    ) -> None:
        if error is not None:
            delivery_stats["failed"] += 1

            print(
                f"DELIVERY FAILED: {error}",
                file=sys.stderr,
            )
            return

        delivery_stats["delivered"] += 1

        print(
            "DELIVERED: "
            f"topic={message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()} "
            f"key={message.key().decode('utf-8')}",
            file=sys.stderr,
        )

    producer = (
        None
        if args.dry_run
        else create_producer(
            bootstrap_servers
        )
    )

    generated_count = 0

    try:
        while RUNNING:
            if (
                args.count > 0
                and generated_count >= args.count
            ):
                break

            city_id = rng.choice(city_ids)

            city_name, country_code = (
                CANONICAL_CITIES[city_id]
            )

            device_id = build_device_id(
                city_id,
                country_code,
            )

            sequence_numbers[device_id] += 1

            event = generate_event(
                city_id=city_id,
                sequence_number=(
                    sequence_numbers[device_id]
                ),
                rng=rng,
            )

            validate_schema(
                schema,
                event,
            )

            validate_semantics(event)

            payload = json.dumps(
                event,
                ensure_ascii=False,
                separators=(",", ":"),
            )

            kafka_key = str(city_id)

            if args.dry_run:
                print(payload)
            else:
                while True:
                    try:
                        producer.produce(
                            topic=topic,
                            key=kafka_key.encode(
                                "utf-8"
                            ),
                            value=payload.encode(
                                "utf-8"
                            ),
                            on_delivery=(
                                delivery_callback
                            ),
                        )
                        break

                    except BufferError:
                        producer.poll(1.0)

                producer.poll(0)

            generated_count += 1

            if args.interval_seconds > 0:
                time.sleep(
                    args.interval_seconds
                )

    finally:
        if producer is not None:
            outstanding = producer.flush(30)

            if outstanding != 0:
                print(
                    "FAIL: "
                    f"{outstanding} Kafka message(s) "
                    "were not delivered",
                    file=sys.stderr,
                )

                delivery_stats["failed"] += (
                    outstanding
                )

    mode = (
        "dry-run"
        if args.dry_run
        else "Kafka"
    )

    print(
        "SUMMARY: "
        f"mode={mode} "
        f"generated={generated_count} "
        f"delivered={delivery_stats['delivered']} "
        f"failed={delivery_stats['failed']} "
        f"topic={topic}",
        file=sys.stderr,
    )

    if delivery_stats["failed"] != 0:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
