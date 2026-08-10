import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from jsonschema import (
    Draft202012Validator,
    FormatChecker,
)


CANONICAL_CITIES: dict[int, tuple[str, str]] = {
    1: ("Chicago", "US"),
    2: ("Los Angeles", "US"),
    3: ("New York", "US"),
    4: ("Manchester", "GB"),
    5: ("London", "GB"),
    6: ("Munich", "DE"),
    7: ("Berlin", "DE"),
    8: ("Osaka", "JP"),
    9: ("Tokyo", "JP"),
    10: ("Busan", "KR"),
    11: ("Seoul", "KR"),
    12: ("Singapore", "SG"),
}


def load_json(path: Path) -> dict[str, Any]:
    try:
        content = path.read_text(encoding="utf-8")
        data = json.loads(content)
    except FileNotFoundError:
        raise ValueError(
            f"File does not exist: {path}"
        ) from None
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in {path}: "
            f"line {exc.lineno}, column {exc.colno}: "
            f"{exc.msg}"
        ) from exc

    if not isinstance(data, dict):
        raise ValueError(
            f"JSON root must be an object: {path}"
        )

    return data


def parse_timestamp_utc(
    value: str,
    field_name: str,
) -> datetime:
    try:
        timestamp = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError(
            f"{field_name} is not a valid timestamp: "
            f"{value}"
        ) from exc

    if timestamp.tzinfo is None:
        raise ValueError(
            f"{field_name} must include timezone information"
        )

    if timestamp.utcoffset() is None:
        raise ValueError(
            f"{field_name} must have a valid UTC offset"
        )

    if timestamp.utcoffset().total_seconds() != 0:
        raise ValueError(
            f"{field_name} must represent UTC time"
        )

    return timestamp


def validate_schema(
    schema: dict[str, Any],
    event: dict[str, Any],
) -> None:
    validator = Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
    )

    errors = sorted(
        validator.iter_errors(event),
        key=lambda error: list(error.absolute_path),
    )

    if not errors:
        return

    messages: list[str] = []

    for error in errors:
        field_path = ".".join(
            str(part)
            for part in error.absolute_path
        )

        location = field_path or "<root>"
        messages.append(
            f"{location}: {error.message}"
        )

    raise ValueError(
        "Schema validation failed:\n- "
        + "\n- ".join(messages)
    )


def validate_semantics(
    event: dict[str, Any],
) -> None:
    city_id = event["city_id"]
    expected_city = CANONICAL_CITIES.get(city_id)

    if expected_city is None:
        raise ValueError(
            f"Unknown canonical city_id: {city_id}"
        )

    expected_name, expected_country = expected_city

    actual_city = (
        event["city_name"],
        event["country_code"],
    )

    if actual_city != expected_city:
        raise ValueError(
            "Canonical city mapping mismatch: "
            f"city_id={city_id} expects "
            f"city_name={expected_name!r}, "
            f"country_code={expected_country!r}; "
            f"received city_name={event['city_name']!r}, "
            f"country_code={event['country_code']!r}"
        )

    event_time = parse_timestamp_utc(
        event["event_time_utc"],
        "event_time_utc",
    )

    produced_at = parse_timestamp_utc(
        event["produced_at_utc"],
        "produced_at_utc",
    )

    if event_time > produced_at:
        raise ValueError(
            "event_time_utc cannot be later than "
            "produced_at_utc"
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate an environment sensor reading "
            "against sensor_reading_v1."
        )
    )

    parser.add_argument(
        "event_file",
        type=Path,
        help="Path to the event JSON file.",
    )

    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(
            "streaming/contracts/"
            "sensor_reading_v1.schema.json"
        ),
        help="Path to the JSON Schema file.",
    )

    args = parser.parse_args()

    try:
        schema = load_json(args.schema)
        event = load_json(args.event_file)

        validate_schema(schema, event)
        validate_semantics(event)

    except ValueError as exc:
        print(
            f"FAIL: {exc}",
            file=sys.stderr,
        )
        return 1

    print(
        "PASS: Event matches sensor_reading_v1 "
        "schema and semantic rules"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
