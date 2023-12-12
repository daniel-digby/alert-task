
from src.aggregate_events import aggregate_events
from src.db import database_connection
from src.ingest_data import ingest_data


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
