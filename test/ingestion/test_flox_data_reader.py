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


from deflox.ingestion.flox_data_reader import DataReader


class DataReaderTest(unittest.TestCase):
    """Test case for DataReader."""

    def test_read_raw(self):
        gdf = self._read_raw("070103.CSV")

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
        gdf = self._read_raw("F070103.CSV")

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

    def test_read_processing_incoming_rad_full(self):
        gdf = self._read_processed(
            "Incoming_radiance_FULL_F070003.csv", "incoming_radiance_f"
        )
        self.assertIsNotNone(gdf)

        earliest = 0
        latest = 827
        lowest_wl = 0
        highest_wl = 13

        self.assertEqual("2024-11-05 04:58:36", gdf["local_datetime"].iloc[earliest])
        self.assertEqual("2024-11-05 16:57:43", gdf["local_datetime"].iloc[latest])

        self.assertEqual(828, len(gdf))
        self.assertEqual(14, len(gdf["incoming_radiance_f_wl"].iloc[earliest]))
        self.assertEqual(14, len(gdf["incoming_radiance_f_wl"].iloc[latest]))

        self.assertEqual(
            339.508029007557, gdf["incoming_radiance_f_wl"].iloc[earliest][lowest_wl]
        )
        self.assertEqual(
            339.508029007557, gdf["incoming_radiance_f_wl"].iloc[latest][lowest_wl]
        )
        self.assertEqual(
            1014.45944485292, gdf["incoming_radiance_f_wl"].iloc[earliest][highest_wl]
        )
        self.assertEqual(
            1014.45944485292, gdf["incoming_radiance_f_wl"].iloc[latest][highest_wl]
        )

        self.assertEqual(
            0.000105426381449059, gdf["incoming_radiance_f"].iloc[earliest][lowest_wl]
        )
        self.assertEqual(
            0.0119655355278349, gdf["incoming_radiance_f"].iloc[earliest][highest_wl]
        )

        self.assertEqual(
            -0.000215458904854152, gdf["incoming_radiance_f"].iloc[latest][lowest_wl]
        )
        self.assertEqual(
            0.00797742708134133, gdf["incoming_radiance_f"].iloc[latest][highest_wl]
        )

    def test_read_processing_incoming_rad_fluo(self):
        gdf = self._read_processed(
            "Incoming_radiance_FLUO_070003.csv", "incoming_radiance"
        )
        self.assertIsNotNone(gdf)

        earliest = 0
        latest = 827
        lowest_wl = 0
        highest_wl = 11

        self.assertEqual("2024-11-05 04:58:36", gdf["local_datetime"].iloc[earliest])
        self.assertEqual("2024-11-05 16:57:43", gdf["local_datetime"].iloc[latest])

        self.assertEqual(828, len(gdf))
        self.assertEqual(12, len(gdf["incoming_radiance_wl"].iloc[earliest]))
        self.assertEqual(12, len(gdf["incoming_radiance_wl"].iloc[latest]))

        self.assertEqual(
            649.787675725791, gdf["incoming_radiance_wl"].iloc[earliest][lowest_wl]
        )
        self.assertEqual(
            649.787675725791, gdf["incoming_radiance_wl"].iloc[latest][lowest_wl]
        )
        self.assertEqual(
            812.648178819681, gdf["incoming_radiance_wl"].iloc[earliest][highest_wl]
        )
        self.assertEqual(
            812.648178819681, gdf["incoming_radiance_wl"].iloc[latest][highest_wl]
        )

        self.assertEqual(0, gdf["incoming_radiance"].iloc[earliest][lowest_wl])
        self.assertEqual(
            0.00000180970464051234,
            gdf["incoming_radiance"].iloc[earliest][highest_wl],
        )

        self.assertEqual(0, gdf["incoming_radiance"].iloc[latest][lowest_wl])
        self.assertEqual(
            0.00412447887108089, gdf["incoming_radiance"].iloc[latest][highest_wl]
        )

    def test_read_processing_refl_full(self):
        gdf = self._read_processed("Reflectance_FULL_F070003.csv", "reflectance_f")
        self.assertIsNotNone(gdf)

        earliest = 0
        latest = 827
        lowest_wl = 0
        highest_wl = 14

        self.assertEqual("2024-11-05 04:58:36", gdf["local_datetime"].iloc[earliest])
        self.assertEqual("2024-11-05 16:57:43", gdf["local_datetime"].iloc[latest])

        self.assertEqual(828, len(gdf))
        self.assertEqual(15, len(gdf["reflectance_f_wl"].iloc[earliest]))
        self.assertEqual(15, len(gdf["reflectance_f_wl"].iloc[latest]))

        self.assertEqual(
            339.508029007557, gdf["reflectance_f_wl"].iloc[earliest][lowest_wl]
        )
        self.assertEqual(
            339.508029007557, gdf["reflectance_f_wl"].iloc[latest][lowest_wl]
        )
        self.assertEqual(
            1014.45944485292, gdf["reflectance_f_wl"].iloc[earliest][highest_wl]
        )
        self.assertEqual(
            1014.45944485292, gdf["reflectance_f_wl"].iloc[latest][highest_wl]
        )

        self.assertEqual(
            -0.2619978127435, gdf["reflectance_f"].iloc[earliest][lowest_wl]
        )
        self.assertEqual(
            0.371839598693771,
            gdf["reflectance_f"].iloc[earliest][highest_wl],
        )

        self.assertEqual(
            0.192299542471374, gdf["reflectance_f"].iloc[latest][lowest_wl]
        )
        self.assertEqual(
            0.678966032936192, gdf["reflectance_f"].iloc[latest][highest_wl]
        )
        self.assertEqual(None, gdf["reflectance_f"].iloc[latest - 1][highest_wl])

    def test_read_processing_refl_fluo(self):
        gdf = self._read_processed("Reflectance_FLUO_070003.csv", "reflectance")
        self.assertIsNotNone(gdf)

        earliest = 0
        latest = 827
        lowest_wl = 0
        highest_wl = 15

        self.assertEqual("2024-11-05 04:58:36", gdf["local_datetime"].iloc[earliest])
        self.assertEqual("2024-11-05 16:57:43", gdf["local_datetime"].iloc[latest])

        self.assertEqual(828, len(gdf))
        self.assertEqual(16, len(gdf["reflectance_wl"].iloc[earliest]))
        self.assertEqual(16, len(gdf["reflectance_wl"].iloc[latest]))

        self.assertEqual(
            649.787675725791, gdf["reflectance_wl"].iloc[earliest][lowest_wl]
        )
        self.assertEqual(
            649.787675725791, gdf["reflectance_wl"].iloc[latest][lowest_wl]
        )
        self.assertEqual(
            812.648178819681, gdf["reflectance_wl"].iloc[earliest][highest_wl]
        )
        self.assertEqual(
            812.648178819681, gdf["reflectance_wl"].iloc[latest][highest_wl]
        )

        self.assertEqual(None, gdf["reflectance"].iloc[earliest][lowest_wl])
        self.assertEqual(
            0.213399633577451,
            gdf["reflectance"].iloc[earliest][highest_wl],
        )

        self.assertEqual(None, gdf["reflectance"].iloc[latest][lowest_wl])
        self.assertEqual(0.338137619086993, gdf["reflectance"].iloc[latest][highest_wl])

    def test_read_processing_incoming_refl_rad_full(self):
        gdf = self._read_processed("Reflected_radiance_FULL_F070003.csv", "refl_rad_f")
        self.assertIsNotNone(gdf)

        earliest = 0
        latest = 827
        lowest_wl = 0
        highest_wl = 17

        self.assertEqual("2024-11-05 04:58:36", gdf["local_datetime"].iloc[earliest])
        self.assertEqual("2024-11-05 16:57:43", gdf["local_datetime"].iloc[latest])

        self.assertEqual(828, len(gdf))
        self.assertEqual(18, len(gdf["refl_rad_f_wl"].iloc[earliest]))
        self.assertEqual(18, len(gdf["refl_rad_f_wl"].iloc[latest]))

        self.assertEqual(
            339.508029007557, gdf["refl_rad_f_wl"].iloc[earliest][lowest_wl]
        )
        self.assertEqual(339.508029007557, gdf["refl_rad_f_wl"].iloc[latest][lowest_wl])
        self.assertEqual(
            1014.45944485292, gdf["refl_rad_f_wl"].iloc[earliest][highest_wl]
        )
        self.assertEqual(
            1014.45944485292, gdf["refl_rad_f_wl"].iloc[latest][highest_wl]
        )

        self.assertEqual(
            -0.0000276214813451153, gdf["refl_rad_f"].iloc[earliest][lowest_wl]
        )
        self.assertEqual(
            0.00444925992882619,
            gdf["refl_rad_f"].iloc[earliest][highest_wl],
        )

        self.assertEqual(
            -0.0000414326488248366, gdf["refl_rad_f"].iloc[latest][lowest_wl]
        )
        self.assertEqual(
            0.00541640201845607, gdf["refl_rad_f"].iloc[latest][highest_wl]
        )

    def test_read_processing_incoming_refl_rad_fluo(self):
        gdf = self._read_processed("Reflected_radiance_FLUO_070003.csv", "refl_rad")
        self.assertIsNotNone(gdf)

        earliest = 0
        latest = 827
        lowest_wl = 0
        highest_wl = 16

        self.assertEqual("2024-11-05 04:58:36", gdf["local_datetime"].iloc[earliest])
        self.assertEqual("2024-11-05 16:57:43", gdf["local_datetime"].iloc[latest])

        self.assertEqual(828, len(gdf))
        self.assertEqual(17, len(gdf["refl_rad_wl"].iloc[earliest]))
        self.assertEqual(17, len(gdf["refl_rad_wl"].iloc[latest]))

        self.assertEqual(649.787675725791, gdf["refl_rad_wl"].iloc[earliest][lowest_wl])
        self.assertEqual(649.787675725791, gdf["refl_rad_wl"].iloc[latest][lowest_wl])
        self.assertEqual(
            812.648178819681, gdf["refl_rad_wl"].iloc[earliest][highest_wl]
        )
        self.assertEqual(812.648178819681, gdf["refl_rad_wl"].iloc[latest][highest_wl])

        self.assertEqual(0, gdf["refl_rad"].iloc[earliest][lowest_wl])
        self.assertEqual(
            0.000000386190307168747,
            gdf["refl_rad"].iloc[earliest][highest_wl],
        )

        self.assertEqual(0, gdf["refl_rad"].iloc[latest][lowest_wl])
        self.assertEqual(0.0013946414654419, gdf["refl_rad"].iloc[latest][highest_wl])

    @staticmethod
    def _read_raw(csv):
        data = pkgutil.get_data("test.ingestion.res", csv).decode()
        lines = data.split("\n")
        rdr = DataReader()
        return rdr.read(lines)

    @staticmethod
    def _read_processed(processed, var_name):
        raw_data = pkgutil.get_data("test.ingestion.res", "070103.CSV").decode()
        processed_data = pkgutil.get_data("test.ingestion.res", processed).decode()
        raw_lines = raw_data.split("\n")
        processed_lines = processed_data.split("\n")
        rdr = DataReader()
        return rdr.read(raw_lines, processed_lines, var_name)
