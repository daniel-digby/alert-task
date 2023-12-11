import time

import sqlalchemy as sa


def database_connection() -> sa.Connection:
    engine = sa.create_engine("postgresql://postgres:postgres@postgres:5432/postgres")

    for attempt in range(5):
        try:
            conn = engine.connect()
        except sa.exc.OperationalError as e:
            if attempt == 4:
                raise e
            time.sleep(1)

    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS events "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )

    return conn

