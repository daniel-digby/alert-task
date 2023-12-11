from datetime import datetime
import sqlalchemy as sa

from src.constants import EVENT_TYPES


def _validate_event_type(event_type: str) -> None:
    """
    Validates the event type against predefined types.

    Args:
        event_type (str): Type of the event to be validated.

    Raises:
        ValueError: If the event_type is not in the predefined EVENT_TYPES.
    """
    if event_type not in EVENT_TYPES:
        raise ValueError(f"Invalid event type. Must be one of {EVENT_TYPES}.")


def _validate_timestamp(timestamp: str) -> None:
    """
    Validates the timestamp format.

    Args:
        timestamp (str): Timestamp string to be validated.

    Raises:
        ValueError: If the timestamp format is invalid.
    """
    try:
        datetime.fromisoformat(timestamp)
    except ValueError as e:
        raise ValueError(
            "Invalid timestamp format. Must be an isoformat string."
        ) from e


def ingest_data(conn: sa.Connection, timestamp: str, event_type: str) -> None:
    """
    Ingests event data into the database after validation.

    Args:
        conn (sa.Connection): SQLAlchemy connection to the database.
        timestamp (str): Timestamp of the event.
        event_type (str): Type of the event.

    Raises:
        ValueError: If the timestamp or event_type fails validation.
        Any other database-related exceptions raised by conn.execute().
    """
    _validate_timestamp(timestamp)
    _validate_event_type(event_type)
    conn.execute(
        sa.text("INSERT INTO events (time, type) VALUES (:timestamp, :event_type)"),
        {"timestamp": timestamp, "event_type": event_type},
    )
