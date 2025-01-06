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
import datetime
import pkgutil
import unittest

from ingestion.flox_data_reader import DataReader


class RawDataReaderTest(unittest.TestCase):
    """Test case for RawDataReader."""

    def test_read(self):
        data = pkgutil.get_data("test.res", "070103.CSV").decode()
        rdr = DataReader()
        gdf = rdr.read(data)

        self.assertIsNotNone(gdf)

        self.assertEqual(gdf["datetime"].size, 428)
        first_datetime: datetime.datetime = gdf["datetime"][0]
        self.assertEqual(first_datetime.strftime("%Y%m%d-%H%M%S"), "20241105-070157")
        last_datetime: datetime.datetime = gdf["datetime"].iloc[-1]
        self.assertEqual(last_datetime.strftime("%Y%m%d-%H%M%S"), "20241105-185848")

        self.assertEqual(gdf["IT_WR[us]"][0], 4000000)
        self.assertEqual(gdf["IT_VEG[us]"][0], 4000000)
        self.assertEqual(gdf["cycle_duration[ms]"][0], 25222)
        self.assertEqual(gdf["Temp1"][0], 26.50)
        self.assertEqual(gdf["veg_CPU"][0], 482112559)
        self.assertEqual(gdf["voltage"][0], 12.34)
        self.assertEqual(gdf["MultiCal"][0], 0)
        self.assertEqual(gdf["GPS_TIME_UTC"][0], 50119)
        self.assertEqual(gdf["GPS_date"][0], 50180)
        self.assertEqual(gdf["GPS_lat"][0], 50.86594)
        self.assertEqual(gdf["GPS_lon"][0], 6.44715)
        self.assertEqual(
            gdf["DUE_RTC_DATETIME"][0].strftime("%Y%m%d-%H%M%S"), "20241105-070156"
        )

        self.assertEqual(gdf["IT_WR[us]"].iloc[-1], 4000000)
        self.assertEqual(gdf["IT_VEG[us]"].iloc[-1], 4000000)
        self.assertEqual(gdf["cycle_duration[ms]"].iloc[-1], 25221)
        self.assertEqual(gdf["Temp1"].iloc[-1], 26.56)
        self.assertEqual(gdf["veg_CPU"].iloc[-1], 525114072)
        self.assertEqual(gdf["voltage"].iloc[-1], 11.67)
        self.assertEqual(gdf["MultiCal"].iloc[-1], 4)
        self.assertEqual(gdf["GPS_TIME_UTC"].iloc[-1], 165807)
        self.assertEqual(gdf["GPS_date"].iloc[-1], 50180)
        self.assertEqual(gdf["GPS_lat"].iloc[-1], 50.86594)
        self.assertEqual(gdf["GPS_lon"].iloc[-1], 6.44715)
        self.assertEqual(
            gdf["DUE_RTC_DATETIME"].iloc[-1].strftime("%Y%m%d-%H%M%S"),
            "20241105-185847",
        )
