import sqlalchemy as sa


def ingest_data(conn: sa.Connection, timestamp: str, event_type: str):
    conn.execute(
        sa.text("INSERT INTO events (time, type) VALUES (:timestamp, :event_type)"),
        {"timestamp": timestamp, "event_type": event_type},
    )
