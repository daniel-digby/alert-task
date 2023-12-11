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


def ingest_data(conn: sa.Connection, events: list[tuple]) -> list[dict]:
    """
    Ingests a batch of events into the database while filtering invalid events.

    Args:
        conn (sa.Connection): SQLAlchemy connection to the database.
        events (list[tuple]): List of event data, each item as a tuple with "timestamp"
                              and "event_type" values.

    Returns:
        list[dict]: List of invalid events (with "timestamp" and "event_type" keys) that
                    failed validation and were not inserted into the database.

    Raises:
        Any database-related exceptions raised by conn.execute().
    """
    valid_events = []
    invalid_events = []
    for event in events:
        try:
            _validate_timestamp(event[0])
            _validate_event_type(event[1])
            valid_events.append(dict(timestamp=event[0], event_type=event[1]))
        except ValueError:
            invalid_events.append(dict(timestamp=event[0], event_type=event[1]))

    if valid_events:
        insert_stmt = sa.text(
            """
            INSERT INTO events (time, type)
            VALUES
            (:timestamp, :event_type)
            """
        )

        conn.execute(
            insert_stmt,
            valid_events,
        )

    return invalid_events
