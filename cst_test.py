import unittest
import os

from cst import CodeTable

class TestBasicCSTFunctionality(unittest.TestCase):

    def setUp(self):
        self.cst = CodeTable("test.cst")

    def tearDown(self):
        try:
            os.remove("test.cst")
        except OSError:
            pass

    def testGettingSetWithMostCommonTags(self):
        cst = self.cst
        cst["a"] = ["k1", "k2", "k3"]
        cst["b"] = ["k3", "k4"]
        cst["b"] = ["k1", "k3", "k5"]
        cst["d"] = ["k1", "k2", "k4", "k5"]
        cst_key = cst.get_set_with_most_common_tags(["k1", "k2", "k4"])
        self.assertEqual(cst_key, "d")

if __name__ == '__main__':
    unittest.main()
