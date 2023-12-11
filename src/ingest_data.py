import sqlalchemy as sa


def ingest_data(conn: sa.Connection, timestamp: str, event_type: str):
    ...
