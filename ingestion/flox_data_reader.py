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
from typing import Callable, Optional

import geopandas
import pandas as pd


class Var:

    def __init__(
        self, var_name: str, index: int, converter_func: Optional[Callable] = None
    ):
        self.var_name = var_name
        self.index = index
        if converter_func is None:
            self.converter_func = lambda x: x
        else:
            self.converter_func = converter_func
        self.values = []


class DataReader:
    """
    Reads FLoX data and turns it into a geopandas GeoDataFrame, ready for ingestion into the xcube geoDB.
    """

    def __init__(self):

        self.core_vars = [
            Var("wr", 1),
            Var("veg", 2),
            Var("wr2", 3),
            Var("DC_WR", 4),
            Var("DC_VEG", 5),
        ]

        self.meta_vars = [
            Var("IT_WR[us]", 5, float),
            Var(
                "IT_VEG[us]",
                7,
                float,
            ),
            Var("cycle_duration[ms]", 9, float),
            Var(
                "QEpro_Frame[C]",
                11,
                float,
            ),
            Var("QEpro_CCD[C]", 13, float),
            Var("chamber_temp[C]", 15, float),
            Var("chamber_humidity", 17, float),
            Var("mainboard_temp[C]", 19, float),
            Var("mainboard_humidity", 21, float),
            Var("flox_identifier", 22),
            Var("GPS_TIME_UTC", 24, float),
            Var("GPS_date", 26, int),
            Var("GPS_lat", 28, lambda v: float(v.replace(" N", "").replace(" S", ""))),
            Var("GPS_lon", 30, lambda v: float(v.replace(" E", "").replace(" W", ""))),
            Var("voltage", 32, float),
            Var("gps_CPU", 34, float),
            Var("wr_CPU", 36, float),
            Var("veg_CPU", 38, float),
            Var(
                "wr2_CPU",
                40,
                float,
            ),
            Var("cooling_active", 46),
            Var("heating_active", 48),
            Var("Temp0", 50, float),
            Var("Temp1", 52, float),
            Var("Temp2", 54, float),
            Var("MultiCal", 56, int),
        ]

        self.df = pd.DataFrame()

    def read(self, csv: str) -> geopandas.GeoDataFrame:

        lines = csv.split("\n")

        datetime_values = []
        due_rtc_datetimes = []

        cursor = 0
        while cursor + 6 < len(lines):
            current_msmnt = lines[cursor : cursor + 6]
            cursor += 6

            for core_var in self.core_vars:
                core_var.values.append(
                    [
                        int(m)
                        for m in current_msmnt[core_var.index]
                        .replace("\r", "")
                        .split(";")[1:-1]
                    ]
                )

            meta = current_msmnt[0].split(";")
            datetime_values.append(
                datetime.datetime.strptime(
                    "20" + meta[1] + "-" + meta[2], "%Y%m%d-%H%M%S"
                )
            )

            due_rtc_datetimes.append(
                datetime.datetime.strptime(
                    "20" + meta[42] + "-" + meta[44], "%Y%m%d-%H%M%S"
                )
            )

            for meta_var in self.meta_vars:
                meta_var.values.append(meta_var.converter_func(meta[meta_var.index]))

        for var in self.core_vars:
            self._append_column({var.var_name: var.values})

        for var in self.meta_vars:
            self._append_column({var.var_name: var.values})

        self._append_column({"datetime": datetime_values})
        self._append_column({"DUE_RTC_DATETIME": due_rtc_datetimes})

        gdf = geopandas.GeoDataFrame(
            self.df,
            geometry=geopandas.points_from_xy(self.df.GPS_lon, self.df.GPS_lat),
            crs="EPSG:4326",
        )

        return gdf

    def _append_column(self, append_values):
        for col, values in append_values.items():
            self.df = pd.concat([self.df, pd.DataFrame({col: values})], axis=1)
