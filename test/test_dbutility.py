import unittest
import sys
import conn_util

class MyTestCase(unittest.TestCase):
    def test_get_database(self):
        self.assertEqual(conn_util.get_database_conf("port"),"3306")


if __name__ == '__main__':
    unittest.main()