from db import database_connection
import sqlalchemy as sa

from ingest_data import ingest_data


def aggregate_events(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    return {
        "people": [
            ("2023-08-10T10:00:00", "2023-08-10T10:02:00"),
            ("2023-08-10T10:04:00", "2023-08-10T10:05:00"),
        ],
        "vehicles": [
            ("2023-08-10T10:00:00", "2023-08-10T10:02:00"),
            ("2023-08-10T10:05:00", "2023-08-10T10:07:00"),
        ],
    }


def main():
    conn = database_connection()

    # Simulate real-time events every 30 seconds
    events = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]

    ingest_data(conn, events)

    aggregate_results = aggregate_events(conn)
    print(aggregate_results)


if __name__ == "__main__":
    main()
