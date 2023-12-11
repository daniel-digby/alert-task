import sqlalchemy as sa

from .base import BaseTestCase

from src.ingest_data import ingest_data


class TestIngestData(BaseTestCase):
    def test_valid_input(self):
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

        ingest_data(self.conn, events)

        result = self.conn.execute(sa.text("SELECT * FROM events")).fetchall()
        self.assertEqual(len(result), len(events))

    def test_invalid_timestamp(self):
        events = [
            ("Invalid Timestamp", "pedestrian"),
        ]
        invalid_events = ingest_data(self.conn, events)
        self.assertTrue(invalid_events[0]["timestamp"] == "Invalid Timestamp")

    def test_invalid_event_type(self):
        events = [
            ("2023-08-10T18:30:30", "Invalid Event Type"),
        ]
        invalid_events = ingest_data(self.conn, events)
        self.assertTrue(invalid_events[0]["event_type"] == "Invalid Event Type")

    def test_multiple_invalid_events(self):
        events = [
            ("Invalid Timestamp", "pedestrian"),
            ("2023-08-10T18:30:30", "Invalid Event Type"),
        ]
        invalid_events = ingest_data(self.conn, events)
        self.assertTrue(invalid_events[0]["timestamp"] == "Invalid Timestamp")
        self.assertTrue(invalid_events[1]["event_type"] == "Invalid Event Type")
