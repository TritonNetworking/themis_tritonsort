#!/usr/bin/env python

import unittest
from utils import NestedDict, flattened_keys

class TestNestedDict(unittest.TestCase):
    def test_silent_dict_construction(self):
        d1 = NestedDict(3, list)

        d1["foo"]["bar"][3] = 100
        d1["baz"] = 50

        try:
            d1["foo"]["baz"]["quuz"][4] = 10
            self.fail("Indexing deeper than 3 levels should have raised "
                      "an IndexError")
        except IndexError, e:
            pass


class TestFlattenedKeys(unittest.TestCase):
    def test_iterate(self):
        test_dict = {1 : {2 : 3,
                          4 : 5},
                     6 : {7 : 8},
                     9 : 10}

        expected_keys = [(1,2), (1,4), (6,7), (9,)]
        self.assertEqual(expected_keys, [x for x in flattened_keys(test_dict)])

    def test_sort_order(self):
        test_dict = {1 : {2 : 3,
                          4 : 5},
                     6 : {7 : 8},
                     9 : 10}

        key_order = [6,9,7,2,4,1]

        def my_sort_function(x):
            return key_order.index(x[0])

        expected_keys = [(6,7), (9,), (1,2), (1,4),]

        self.assertEqual(expected_keys, [x for x in flattened_keys(
                    test_dict, sort_function = my_sort_function)])

if __name__ == "__main__":
    unittest.main()
