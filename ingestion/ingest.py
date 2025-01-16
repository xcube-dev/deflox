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
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas
from dotenv import load_dotenv
from xcube_geodb.core.geodb import GeoDBClient

from ingestion.fetch_data import DataFetcher
from ingestion.flox_data_reader import DataReader


def ingest():
    """
    Does the complete ingestion process for a single FLoX.
    """
    load_dotenv()
    temp_data_dir = (
        os.environ["TEMP_DATA_DIR"] if "TEMP_DATA_DIR" in os.environ else "."
    )
    max_day_diff = (
        int(os.environ["MAX_DAY_DIFF"]) if "MAX_DAY_DIFF" in os.environ else 2
    )
    data_fetcher = DataFetcher(temp_data_dir)
    data_fetcher.fetch_data(max_day_diff)

    if not data_fetcher.downloaded_files:
        print("No new data to ingest, exiting...")
        sys.exit(0)

    # get time information from geoDB
    raw_collection_name = os.environ["FTP_USER"] + "-raw"
    raw_f_collection_name = f"{os.environ['FTP_USER']}-raw-f"
    geodb = _get_geodb_client()
    latest_time_raw = _get_latest_time(geodb, raw_collection_name)
    latest_time_raw_f = _get_latest_time(geodb, raw_f_collection_name)

    for f in data_fetcher.downloaded_files:
        print(f"reading {f}")
        file_path = os.path.join(temp_data_dir, f)
        with open(file_path, "r") as csvfile:
            gdf = DataReader().read(csvfile.readlines())
        is_f_file = os.path.basename(f)[0] == "F"
        latest_time = latest_time_raw_f if is_f_file else latest_time_raw
        gdf["utc_datetime"] = pandas.to_datetime(
            gdf["utc_datetime"], format="%Y-%m-%d %H:%M:%S"
        )
        gdf = gdf[gdf["utc_datetime"] > latest_time]
        gdf["utc_datetime"] = gdf["utc_datetime"].astype(str)

        collection_name = raw_f_collection_name if is_f_file else raw_collection_name
        if len(gdf) > 0:
            geodb.insert_into_collection(collection_name, gdf, database="deflox")
        else:
            print(f"{f} does not contain any new data")

        os.remove(file_path)
        parent = Path(file_path).parent.absolute()
        files_in_dir = parent.glob("*")
        # weirdly, this does not work with the extra 'len':
        if len(list(files_in_dir)) == 0:
            shutil.rmtree(parent)

    print("ingestion process finished")


def _get_latest_time(geodb, raw_collection_name) -> datetime:
    latest_time_raw_df = geodb.get_collection_pg(
        collection=raw_collection_name,
        select="utc_datetime",
        database="deflox",
        order="utc_datetime DESC",
        limit=1,
    )
    if len(latest_time_raw_df) == 0:
        return datetime.strptime("1900-01-01", "%Y-%m-%d")
    latest_time = datetime.strptime(
        latest_time_raw_df["utc_datetime"][0], "%Y-%m-%dT%H:%M:%S"
    )
    return latest_time


def _get_geodb_client():
    server_url = (
        os.environ["GEODB_SERVER_URL"]
        if "GEODB_SERVER_URL" in os.environ
        else "https://xcube-geodb.brockmann-consult.de"
    )
    server_port = (
        os.environ["GEODB_SERVER_PORT"] if "GEODB_SERVER_PORT" in os.environ else 443
    )
    client_id = os.environ["GEODB_CLIENT_ID"]
    client_secret = os.environ["GEODB_CLIENT_SECRET"]
    auth_audience = (
        os.environ["GEODB_AUTH_AUD"]
        if "GEODB_AUTH_AUD" in os.environ
        else "https://xcube-users.brockmann-consult.de/api/v2"
    )
    geodb = GeoDBClient(
        server_url, server_port, client_id, client_secret, auth_aud=auth_audience
    )
    return geodb


if __name__ == "__main__":
    ingest()
