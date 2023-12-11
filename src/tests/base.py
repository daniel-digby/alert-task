import unittest

from src.db import mock_connection


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = mock_connection()

    def tearDown(self):
        self.conn.close()
