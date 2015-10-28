import unittest
import os

from sheila2 import Sheila

class TestBasicInsertion(unittest.TestCase):

    def setUp(self):
        self.sheila = Sheila("testdb", "test.cst")

    def tearDown(self):
        try:
            os.remove("test.cst")
        except OSError:
            pass
        self.sheila.destroy()

    def testInsert(self):
        sheila = self.sheila
        sheila.insert({"a": 1, "b": 2})
        self.assertEqual(len(sheila.cst.tables()), 1)
        sheila.insert({"a": 12})
        self.assertEqual(len(sheila.cst.tables()), 1)
        sheila.insert({"a": 1, "b": 2, "c": 3})
        self.assertEqual(len(sheila.cst.tables()), 1)


if __name__ == '__main__':
    unittest.main()
