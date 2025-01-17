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
import os
import re
import time
from ftplib import FTP

from dotenv import load_dotenv


class DataFetcher(object):
    """
    This class fetches data from the source; data must not be older than a configurable number of days.
    """

    def __init__(self, target_dir: str):
        self.max_days = None
        load_dotenv()
        self.ftp = FTP()
        self.ftp.connect(os.getenv("FTP_HOST"), int(os.getenv("FTP_PORT", "21")))
        self.ftp.set_pasv(True)
        self.data_dir = None
        self.target_dir = target_dir
        self.downloaded_files = []

    def fetch_data(self, max_days: int = 2) -> None:
        self.ftp.login(os.getenv("FTP_USER"), os.getenv("FTP_PW"))
        self.max_days = max_days

        data_dirs = []

        for directory in self.ftp.nlst("."):
            print("Fetching data for ", directory)
            if re.search("^\\d\\d\\d\\d\\d\\d$", directory):
                print("using ", directory)
                data_dirs.append(directory)

        for data_dir in data_dirs:
            self.data_dir = data_dir
            self.ftp.dir(f"./{data_dir}", self._download_csv_file)

        self.ftp.quit()

    def _download_csv_file(self, entry: str):
        print(f"Maybe downloading {entry}...")

        entry = entry.split(" ")[-1]
        if entry.lower().endswith(".csv") and not entry.lower() == "log.csv":
            print("69")
            td = f"{self.target_dir}/{self.data_dir}"

            cmd = f"MDTM ./{self.data_dir}/{entry}"
            timestamp = self.ftp.voidcmd(cmd)
            print("timestamp 1: ", timestamp)
            timestamp = timestamp.split(" ")[1]

            print("timestamp 2: ", timestamp)

            if not re.search("\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d\\d", timestamp):
                return

            print("81")

            last_modified_date = datetime.datetime.strptime(timestamp, "%Y%m%d%H%M%S")
            earliest_day = datetime.datetime.today() - datetime.timedelta(
                days=self.max_days
            )

            print(last_modified_date)
            print(earliest_day)

            if last_modified_date <= earliest_day:
                return

            print(f"Downloading {entry} from {self.data_dir} to {td}")

            os.makedirs(td, exist_ok=True)
            with open(f"{td}/{entry}", "wb") as file:
                self.ftp = FTP()
                attempt = 0
                while attempt < 10:
                    try:
                        attempt += 1
                        self.ftp.connect(
                            os.getenv("FTP_HOST"), int(os.getenv("FTP_PORT"))
                        )
                        self.ftp.login(os.getenv("FTP_USER"), os.getenv("FTP_PW"))
                        self.ftp.retrbinary(
                            f"RETR {self.data_dir}/{entry}", file.write, 256 * 1024
                        )
                        self.downloaded_files.append(f"{td}/{entry}")
                        break
                    except Exception as exc:
                        print(exc.args)
                        time.sleep(10)
