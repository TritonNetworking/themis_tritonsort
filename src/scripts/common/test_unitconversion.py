#!/usr/bin/env python

import unittest
import unitconversion as uc

class UnitConversionTest(unittest.TestCase):
    def test_convert(self):
        self.assertAlmostEqual(1.304, uc.convert(1304, "us", "ms"), 2)
        self.assertAlmostEqual(0.001304, uc.convert(1304, "us", "s"), 2)
        self.assertAlmostEqual(95.37, uc.convert(100, "MB", "MiB"), 2)
        self.assertAlmostEqual(2000000, uc.convert(2000, "GB", "MB"), 2)
        self.assertAlmostEqual(1907348.63, uc.convert(2000, "GB", "MiB"), 2)
        self.assertAlmostEqual(0.8, uc.convert(100, "MBps", "Gbps"), 2)
        self.assertAlmostEqual(95.37, uc.convert(100, "MBps", "MiBps"), 2)

    def test_parse(self):
        test_cases = {
            "100GiB" : (100.0, "GiB"),
            "250.6 Gb" : (250.6, "Gb"),
            "100 MBps" : (100, "MBps"),
            "250foos" : None,
            "two hundred and fifty Gb" : None}

        for input_string, expected in test_cases.items():
            self.assertEquals(expected, uc.parse(input_string))

    def test_parse_and_convert(self):
        test_cases = {
            ("100KB", "B") : 100000,
            ("250 Gb", "Mb") : 250000,
            ("250foos", "B") : None,
            ("10 MBps", "GBps") : 0.010
            }

        for (input_string, dest_unit), expected in test_cases.items():
            self.assertEquals(expected, uc.parse_and_convert(
                    input_string, dest_unit))


if __name__ == "__main__":
    unittest.main()
