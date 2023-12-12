from src.db import mock_events
from src.main import aggregate_events

from .base import BaseTestCase


class TestAggregateEvents(BaseTestCase):
    def test_example_activity_periods(self):
        events = [
            ("2023-08-10T18:30:30", "car"),
            ("2023-08-10T18:31:00", "car"),
            ("2023-08-10T18:31:30", "car"),
            ("2023-08-10T18:35:00", "car"),
            ("2023-08-10T18:35:30", "car"),
            ("2023-08-10T18:36:00", "car"),
            ("2023-08-10T18:37:30", "car"),
            ("2023-08-10T18:38:00", "car"),
        ]
        mock_events(self.conn, events)

        aggregated = aggregate_events(self.conn)

        expected = {
            "people": [],
            "vehicles": [
                ("2023-08-10T18:30:30", "2023-08-10T18:31:30"),
                ("2023-08-10T18:35:00", "2023-08-10T18:36:00"),
                ("2023-08-10T18:37:30", "2023-08-10T18:38:00"),
            ],
        }
        self.assertEqual(aggregated, expected)

    def test_single_type_close_timestamps(self):
        events = [
            ("2023-08-10T10:00:00", "pedestrian"),
            ("2023-08-10T10:01:00", "pedestrian"),
            ("2023-08-10T10:03:00", "pedestrian"),
        ]
        mock_events(self.conn, events)

        aggregated = aggregate_events(self.conn)

        expected = {
            "people": [
                ("2023-08-10T10:00:00", "2023-08-10T10:01:00"),
                ("2023-08-10T10:03:00", "2023-08-10T10:03:00"),
            ],
            "vehicles": [],
        }
        self.assertEqual(aggregated, expected)

    def test_multiple_types(self):
        events = [
            ("2023-08-10T10:00:00", "pedestrian"),
            ("2023-08-10T10:01:00", "bicycle"),
            ("2023-08-10T10:03:00", "car"),
            ("2023-08-10T10:04:00", "van"),
        ]
        mock_events(self.conn, events)

        aggregated = aggregate_events(self.conn)

        expected = {
            "people": [("2023-08-10T10:00:00", "2023-08-10T10:01:00")],
            "vehicles": [
                ("2023-08-10T10:03:00", "2023-08-10T10:04:00"),
            ],
        }
        self.assertEqual(aggregated, expected)

    def test_far_apart_events(self):
        self.maxDiff = None

        events = [
            ("2023-08-10T10:00:00", "pedestrian"),
            ("2023-08-10T10:10:00", "pedestrian"),
            ("2023-08-10T10:20:00", "bicycle"),
            ("2023-08-10T10:25:00", "bicycle"),
            ("2023-08-10T10:30:00", "car"),
        ]
        mock_events(self.conn, events)

        aggregated = aggregate_events(self.conn)

        expected = {
            "people": [
                ("2023-08-10T10:00:00", "2023-08-10T10:00:00"),
                ("2023-08-10T10:10:00", "2023-08-10T10:10:00"),
                ("2023-08-10T10:20:00", "2023-08-10T10:20:00"),
                ("2023-08-10T10:25:00", "2023-08-10T10:25:00"),
            ],
            "vehicles": [
                ("2023-08-10T10:30:00", "2023-08-10T10:30:00"),
            ],
        }
        self.assertEqual(aggregated, expected)

    def test_mixed_up_events(self):
        self.maxDiff = None

        events = [
            ("2023-08-10T10:00:00", "pedestrian"),
            ("2023-08-10T10:00:30", "car"),
            ("2023-08-10T10:01:00", "pedestrian"),
            ("2023-08-10T10:01:30", "car"),
        ]
        mock_events(self.conn, events)

        aggregated = aggregate_events(self.conn)

        expected = {
            "people": [
                ("2023-08-10T10:00:00", "2023-08-10T10:01:00"),
            ],
            "vehicles": [
                ("2023-08-10T10:00:30", "2023-08-10T10:01:30"),
            ],
        }
        self.assertEqual(aggregated, expected)
