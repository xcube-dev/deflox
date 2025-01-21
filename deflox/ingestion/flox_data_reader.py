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
import re
from datetime import datetime
from typing import Callable, Optional, List

import geopandas
import numpy as np
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
        self.df = pd.DataFrame()

    def read(
        self,
        raw_lines: List[str],
        processed_lines: Optional[List[str]] = None,
        var_name: Optional[str] = None,
    ) -> geopandas.GeoDataFrame:
        if processed_lines:
            return self._read_processed(raw_lines[0], processed_lines, var_name)
        else:
            return self._read_raw(raw_lines)

    def _read_raw(self, raw_lines: List[str]) -> geopandas.GeoDataFrame:
        local_datetime_values = []
        utc_datetime_values = []

        first_line = raw_lines[0]
        is_f_prefixed_data = len(first_line.split(";")) == 42
        self._initialize_vars(is_f_prefixed_data)

        cursor = 0
        while cursor + 6 <= len(raw_lines):
            skip_block = False
            current_block = raw_lines[cursor : cursor + 6]
            cursor += 6

            meta = current_block[0].split(";")

            values_to_add = {}
            for core_var in self.core_vars:
                values = [
                    m
                    for m in current_block[core_var.index]
                    .replace("\r", "")
                    .split(";")[1:-1]
                ]
                if not len(values) == 1024:
                    print(
                        f"WARN: line {cursor - 6 + core_var.index + 1} invalid. "
                        f"Skipping respective block of measurements."
                    )
                    for line_index, line in enumerate(
                        raw_lines[cursor - 6 + core_var.index + 1 :]
                    ):
                        if re.match(
                            "^\\d+;\\d\\d\\d\\d\\d\\d;\\d\\d\\d\\d\\d\\d;.*;IT_WR.us.=",
                            line,
                        ):
                            cursor = cursor - 6 + core_var.index + line_index + 1
                            break

                    skip_block = True
                    break

                values_to_add[core_var.var_name] = [int(v) for v in values]

            if skip_block:
                continue

            for core_var in self.core_vars:
                core_var.values.append(values_to_add[core_var.var_name])

            local_datetime_values.append(
                f"20{meta[1][:2]}-{meta[1][2:4]}-{meta[1][4:6]} "
                f"{meta[2][:2]}:{meta[2][2:4]}:{meta[2][4:6]}"
            )
            if is_f_prefixed_data:
                utc_datetime_values.append(
                    f"20{meta[18][4:]}-{meta[18][2:4]}-{meta[18][0:2]} "
                    f"{meta[16][:2]}:{meta[16][2:4]}:{meta[16][4:6]}"
                )
            else:
                utc_datetime_values.append(
                    f"20{meta[26][4:]}-{meta[26][2:4]}-{meta[26][0:2]} "
                    f"{meta[24][:2]}:{meta[24][2:4]}:{meta[24][4:6]}"
                )

            for meta_var in self.meta_vars:
                meta_var.values.append(meta_var.converter_func(meta[meta_var.index]))

        for var in self.core_vars:
            self._append_column(var.var_name, var.values)

        for var in self.meta_vars:
            self._append_column(var.var_name, var.values)

        self._append_column("local_datetime", local_datetime_values)
        self._append_column("utc_datetime", utc_datetime_values)

        gdf = geopandas.GeoDataFrame(
            self.df,
            geometry=geopandas.points_from_xy(self.df.GPS_lon, self.df.GPS_lat),
            crs="EPSG:4326",
        )

        return gdf

    def _append_column(self, column_name: str, new_data: List) -> None:
        if column_name not in self.df.columns:
            self.df[column_name] = pd.Series(new_data)
        else:
            self.df[column_name] = pd.concat(
                [self.df[column_name], pd.Series(new_data)], ignore_index=True
            )

    def _initialize_vars(self, is_f_prefixed_data):
        self.core_vars = [
            Var("wr", 1),
            Var("veg", 2),
            Var("wr2", 3),
            Var("DC_WR", 4),
            Var("DC_VEG", 5),
        ]

        if is_f_prefixed_data:
            self.meta_vars = [
                Var("IT_WR[us]", 5, float),
                Var(
                    "IT_VEG[us]",
                    7,
                    float,
                ),
                Var("cycle_duration[ms]", 9, float),
                Var("mainboard_temp[C]", 11, float),
                Var("mainboard_humidity", 13, float),
                Var("flox_identifier", 14),
                Var(
                    "GPS_lat",
                    20,
                    lambda v: float(v.replace(" N", "").replace(" S", "")),
                ),
                Var(
                    "GPS_lon",
                    22,
                    lambda v: float(v.replace(" E", "").replace(" W", "")),
                ),
                Var("voltage", 24, float),
                Var("gps_CPU", 26, float),
                Var("wr_CPU", 28, float),
                Var("veg_CPU", 30, float),
                Var(
                    "wr2_CPU",
                    32,
                    float,
                ),
                Var("MultiCal", 38, int),
                Var("RSSI", 40, int),
            ]
        else:
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
                Var(
                    "GPS_lat",
                    28,
                    lambda v: float(v.replace(" N", "").replace(" S", "")),
                ),
                Var(
                    "GPS_lon",
                    30,
                    lambda v: float(v.replace(" E", "").replace(" W", "")),
                ),
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

    def _read_processed(
        self, first_header_line: str, processed_lines: List[str], var_name: str
    ) -> geopandas.GeoDataFrame:
        keys = first_header_line.split(";")
        date = keys[1]
        lat = float(keys[28].replace(" N", "").replace(" S", ""))
        lon = float(keys[30].replace(" E", "").replace(" W", ""))

        local_datetime_values = []
        times = [t.replace('"', "") for t in processed_lines[0].split(";")[1:]]
        for time in times:
            local_datetime_values.append(
                datetime.strptime(f"20{date} {time}", "%Y%m%d %H_%M_%S").strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )

        wavelengths = []
        core_var_values = []

        for line in processed_lines[1:]:
            if line:
                values = line.split(";")
                for col_number, value in enumerate(values[1:]):
                    if len(core_var_values) <= col_number:
                        core_var_values.append([])
                        wavelengths.append([])
                    val = float(value) if value != "#N/D" else None
                    core_var_values[col_number].append(val)
                    wavelengths[col_number].append(float(values[0]))

        lat_values = np.repeat(lat, len(local_datetime_values))
        lon_values = np.repeat(lon, len(local_datetime_values))

        self.df["local_datetime"] = local_datetime_values
        self.df["GPS_lon"] = lon_values
        self.df["GPS_lat"] = lat_values
        self.df[f"{var_name}_wl"] = wavelengths
        self.df[var_name] = core_var_values

        gdf = geopandas.GeoDataFrame(
            self.df,
            geometry=geopandas.points_from_xy(self.df.GPS_lon, self.df.GPS_lat),
            crs="EPSG:4326",
        )

        return gdf
