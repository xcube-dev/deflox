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
import hashlib
import logging
import os
import shutil
import unittest
from concurrent.futures import ThreadPoolExecutor

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from ingestion.fetch_data import DataFetcher


class DataReaderTest(unittest.TestCase):
    """Test case for DataReader."""

    def setUp(self):

        if os.path.exists("res"):
            self.homedir = "./res"
            self.tmpdir = "./temp"
        else:
            self.homedir = "./test/res"
            self.tmpdir = os.environ["RUNNER_TEMP"]

        print(self.homedir)
        print(self.tmpdir)
        logging.basicConfig(level=logging.ERROR)

        os.environ["FTP_HOST"] = "127.0.0.1"
        os.environ["FTP_PORT"] = "2121"
        os.environ["FTP_USER"] = "username"
        os.environ["FTP_PW"] = "password"

        authorizer = DummyAuthorizer()
        authorizer.add_user(
            os.environ["FTP_USER"],
            os.environ["FTP_PW"],
            self.homedir,
            perm="elradfmwMT",
        )

        handler = FTPHandler
        handler.authorizer = authorizer
        handler.passive_ports = range(60000, 65535)

        self.server = FTPServer(
            (os.getenv("FTP_HOST"), int(os.getenv("FTP_PORT"))), handler
        )

        tpe = ThreadPoolExecutor()
        tpe.submit(
            self.server.serve_forever,
        )

    def tearDown(self):
        self.server.close()

    def test_fetch(self):
        try:
            DataFetcher(self.tmpdir).fetch_data(73000)
            self.assertTrue(os.path.exists(f"{self.tmpdir}"))
            self.assertTrue(os.path.exists(f"{self.tmpdir}/240101"))
            self.assertTrue(os.path.exists(f"{self.tmpdir}/240102"))
            self.assertTrue(os.path.exists(f"{self.tmpdir}/240101/070101.CSV"))
            self.assertTrue(os.path.exists(f"{self.tmpdir}/240102/070102.CSV"))

            file_name = f"{self.tmpdir}/240101/070101.CSV"
            original_file_name = f"{self.homedir}/240101/070101.CSV"
            self._assert_equal_files(file_name, original_file_name)

            file_name = f"{self.tmpdir}/240102/070102.CSV"
            original_file_name = f"{self.homedir}/240102/070102.CSV"
            self._assert_equal_files(file_name, original_file_name)

        finally:
            shutil.rmtree(f"{self.tmpdir}/240101")
            shutil.rmtree(f"{self.tmpdir}/240102")

    def _assert_equal_files(self, file_name, original_file_name):
        with open(file_name, "r") as f:
            expected = [line.rstrip() for line in f]
        with open(original_file_name, "r") as f:
            actual = [line.rstrip() for line in f]
        self.assertEqual(expected, actual)

    @staticmethod
    def md5sum(file_name):
        with open(file_name, "rb") as file_to_check:
            data = file_to_check.read()
            md5_actual = hashlib.md5(data).hexdigest()
        return md5_actual
