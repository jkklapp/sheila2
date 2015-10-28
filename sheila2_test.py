import unittest
import os

from sheila2 import Sheila

class BasicTestCase(unittest.TestCase):
    def setUp(self):
        self.sheila = Sheila("testdb", "test.cst")

    def tearDown(self):
        try:
            os.remove("test.cst")
        except OSError:
            pass
        self.sheila.destroy()

class TestBasicInsertion(BasicTestCase):

    def testTableEntryExpansion(self):
        sheila = self.sheila
        sheila.insert({"a": 1, "b": 2})
        self.assertEqual(len(sheila.cst.tables()), 1)
        sheila.insert({"a": 12})
        self.assertEqual(len(sheila.cst.tables()), 1)
        sheila.insert({"a": 1, "b": 2, "c": 3})
        self.assertEqual(len(sheila.cst.tables()), 1)

    def testTableExpansion(self):
        sheila = self.sheila
        sheila.insert({"a": 1, "b": 2})
        self.assertEqual(len(sheila.cst.tables()), 1)
        sheila.insert({"c": 12})
        self.assertEqual(len(sheila.cst.tables()), 2)
        sheila.insert({"b": 2, "c": 3})
        self.assertEqual(len(sheila.cst.tables()), 2)

class TestBasicQuery(BasicTestCase):

    def testGetData(self):
        sheila = self.sheila
        test_data = {"a": 1, "b": 2}
        sheila.insert(test_data)
        query_data = sheila.query({"a": 1})
        self.assertIn(test_data, query_data)

if __name__ == '__main__':
    unittest.main()
