from datetime import datetime
import sqlalchemy as sa

from src.constants import EVENT_TYPES


def validate_event_type(event_type: str):
    if event_type not in EVENT_TYPES:
        raise ValueError(f"Invalid event type. Must be one of {EVENT_TYPES}.")


def validate_timestamp(timestamp: str):
    try:
        datetime.fromisoformat(timestamp)
    except ValueError as e:
        raise ValueError(
            "Invalid timestamp format. Must be an isoformat string."
        ) from e


def ingest_data(conn: sa.Connection, timestamp: str, event_type: str) -> None:
    validate_timestamp(timestamp)
    validate_event_type(event_type)
    conn.execute(
        sa.text("INSERT INTO events (time, type) VALUES (:timestamp, :event_type)"),
        {"timestamp": timestamp, "event_type": event_type},
    )
