import unittest

import testing.postgresql
from src.db import mock_connection


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.postgresql = testing.postgresql.Postgresql(port=7654)
        self.conn = mock_connection(self.postgresql.url())

    def tearDown(self):
        self.conn.close()
        self.postgresql.stop()
