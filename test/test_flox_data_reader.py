# The MIT License (MIT)
# Copyright (c) 2025 by the xcube team
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
import pkgutil
import unittest

from ingestion.flox_data_reader import DataReader


class DataReaderTest(unittest.TestCase):
    """Test case for DataReader."""

    def test_read_raw(self):
        data = pkgutil.get_data("test.res", "070103.CSV").decode()
        lines = data.split("\n")

        rdr = DataReader()
        gdf = rdr.read(lines)

        self.assertIsNotNone(gdf)

        self.assertEqual(gdf["local_datetime"].size, 428)
        first_datetime: str = gdf["local_datetime"][0]
        self.assertEqual(first_datetime, "2024-11-05 07:01:57")
        last_datetime: str = gdf["local_datetime"].iloc[-1]
        self.assertEqual(last_datetime, "2024-11-05 18:58:48")

        self.assertEqual(gdf["IT_WR[us]"][0], 4000000)
        self.assertEqual(gdf["IT_VEG[us]"][0], 4000000)
        self.assertEqual(gdf["cycle_duration[ms]"][0], 25222)
        self.assertEqual(gdf["Temp1"][0], 26.50)
        self.assertEqual(gdf["veg_CPU"][0], 482112559)
        self.assertEqual(gdf["voltage"][0], 12.34)
        self.assertEqual(gdf["MultiCal"][0], 0)
        self.assertEqual(gdf["GPS_lat"][0], 50.86594)
        self.assertEqual(gdf["GPS_lon"][0], 6.44715)
        self.assertEqual(gdf["utc_datetime"][0], "2080-01-05 05:01:19")

        self.assertEqual(gdf["IT_WR[us]"].iloc[-1], 4000000)
        self.assertEqual(gdf["IT_VEG[us]"].iloc[-1], 4000000)
        self.assertEqual(gdf["cycle_duration[ms]"].iloc[-1], 25221)
        self.assertEqual(gdf["Temp1"].iloc[-1], 26.56)
        self.assertEqual(gdf["veg_CPU"].iloc[-1], 525114072)
        self.assertEqual(gdf["voltage"].iloc[-1], 11.67)
        self.assertEqual(gdf["MultiCal"].iloc[-1], 4)
        self.assertEqual(gdf["GPS_lat"].iloc[-1], 50.86594)
        self.assertEqual(gdf["GPS_lon"].iloc[-1], 6.44715)
        self.assertEqual(gdf["utc_datetime"].iloc[-1], "2080-01-05 16:58:07")

    def test_read_raw_f(self):
        data = pkgutil.get_data("test.res", "F070103.CSV").decode()
        lines = data.split("\n")

        rdr = DataReader()
        gdf = rdr.read(lines)

        self.assertIsNotNone(gdf)

        self.assertEqual(gdf["local_datetime"].size, 428)
        first_datetime: str = gdf["local_datetime"][0]
        self.assertEqual(first_datetime, "2024-11-05 07:01:03")
        last_datetime: str = gdf["local_datetime"].iloc[-1]
        self.assertEqual(last_datetime, "2024-11-05 18:59:22")

        self.assertEqual(gdf["IT_WR[us]"][0], 1000000)
        self.assertEqual(gdf["IT_VEG[us]"][0], 1000000)
        self.assertEqual(gdf["cycle_duration[ms]"][0], 13279)
        self.assertEqual(gdf["veg_CPU"][0], 482067514)
        self.assertEqual(gdf["voltage"][0], 12.60)
        self.assertEqual(gdf["MultiCal"][0], 0)
        self.assertEqual(gdf["GPS_lat"][0], 0.00000)
        self.assertEqual(gdf["GPS_lon"][0], 0.00000)
        self.assertEqual(gdf["utc_datetime"][0], "2080-01-05 05:01:19")

        self.assertEqual(gdf["IT_WR[us]"].iloc[-1], 1000000)
        self.assertEqual(gdf["IT_VEG[us]"].iloc[-1], 1000000)
        self.assertEqual(gdf["cycle_duration[ms]"].iloc[-1], 13279)
        self.assertEqual(gdf["veg_CPU"].iloc[-1], 525156904)
        self.assertEqual(gdf["voltage"].iloc[-1], 11.79)
        self.assertEqual(gdf["MultiCal"].iloc[-1], 0)
        self.assertEqual(gdf["GPS_lat"].iloc[-1], 0.00000)
        self.assertEqual(gdf["GPS_lon"].iloc[-1], 0.00000)
        self.assertEqual(gdf["utc_datetime"].iloc[-1], "2080-01-05 16:59:38")
