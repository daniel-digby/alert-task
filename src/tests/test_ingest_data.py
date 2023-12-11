import sqlalchemy as sa

from .base import BaseTestCase

from src.ingest_data import ingest_data


class TestIngestData(BaseTestCase):
    def test_valid_input(self):
        timestamp = "2023-08-10T18:30:30"
        event_type = "pedestrian"

        ingest_data(self.conn, timestamp, event_type)

        result = self.conn.execute(sa.text("SELECT * FROM events")).fetchall()
        self.assertEqual(len(result), 1)

    def test_multiple_valid_inputs(self):
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

        for timestamp, event_type in events:
            ingest_data(self.conn, timestamp, event_type)

        result = self.conn.execute(sa.text("SELECT * FROM events")).fetchall()
        self.assertEqual(len(result), len(events))