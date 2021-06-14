#!/usr/bin/env python3

##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import csv
import io
import json
import operator
import os
import os.path
import subprocess
import sys
import tempfile
import unittest

import avro.datafile
import avro.io
import avro.schema

NUM_RECORDS = 7


SCHEMA = """
{
    "namespace": "test.avro",
        "name": "LooneyTunes",
        "type": "record",
        "fields": [
            {"name": "first", "type": "string"},
            {"name": "last", "type": "string"},
            {"name": "type", "type": "string"}
        ]
}
"""

LOONIES = (
    ("daffy", "duck", "duck"),
    ("bugs", "bunny", "bunny"),
    ("tweety", "", "bird"),
    ("road", "runner", "bird"),
    ("wile", "e", "coyote"),
    ("pepe", "le pew", "skunk"),
    ("foghorn", "leghorn", "rooster"),
)


def looney_records():
    for f, l, t in LOONIES:
        yield {"first": f, "last": l, "type": t}


SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "scripts", "avro")

_JSON_PRETTY = """{
    "first": "daffy",
    "last": "duck",
    "type": "duck"
}"""


def gen_avro(filename):
    schema = avro.schema.parse(SCHEMA)
    fo = open(filename, "wb")
    writer = avro.datafile.DataFileWriter(fo, avro.io.DatumWriter(), schema)
    for record in looney_records():
        writer.append(record)
    writer.close()
    fo.close()


def _tempfile():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        pass
    return f.name


class TestCat(unittest.TestCase):
    def setUp(self):
        self.avro_file = _tempfile()
        gen_avro(self.avro_file)

    def tearDown(self):
        if os.path.isfile(self.avro_file):
            os.unlink(self.avro_file)

    def _run(self, *args, **kw):
        out = subprocess.check_output([sys.executable, SCRIPT, "cat", self.avro_file] + list(args)).decode()
        if kw.get("raw"):
            return out
        return out.splitlines()

    def test_print(self):
        return len(self._run()) == NUM_RECORDS

    def test_filter(self):
        return len(self._run("--filter", "r['type']=='bird'")) == 2

    def test_skip(self):
        skip = 3
        return len(self._run("--skip", str(skip))) == NUM_RECORDS - skip

    def test_csv(self):
        reader = csv.reader(io.StringIO(self._run("-f", "csv", raw=True)))
        assert len(list(reader)) == NUM_RECORDS

    def test_csv_header(self):
        r = {"type": "duck", "last": "duck", "first": "daffy"}
        out = self._run("-f", "csv", "--header", raw=True)
        io_ = io.StringIO(out)
        reader = csv.DictReader(io_)
        assert next(reader) == r

    def test_print_schema(self):
        out = self._run("--print-schema", raw=True)
        assert json.loads(out)["namespace"] == "test.avro"

    def test_help(self):
        # Just see we have these
        self._run("-h")
        self._run("--help")

    def test_json_pretty(self):
        out = self._run("--format", "json-pretty", "-n", "1", raw=1)
        self.assertEqual(out.strip(), _JSON_PRETTY.strip())

    def test_version(self):
        subprocess.check_output([sys.executable, SCRIPT, "cat", "--version"])

    def test_files(self):
        out = self._run(self.avro_file)
        assert len(out) == 2 * NUM_RECORDS

    def test_fields(self):
        # One field selection (no comma)
        out = self._run("--fields", "last")
        assert json.loads(out[0]) == {"last": "duck"}

        # Field selection (with comma and space)
        out = self._run("--fields", "first, last")
        assert json.loads(out[0]) == {"first": "daffy", "last": "duck"}

        # Empty fields should get all
        out = self._run("--fields", "")
        assert json.loads(out[0]) == {"first": "daffy", "last": "duck", "type": "duck"}

        # Non existing fields are ignored
        out = self._run("--fields", "first,last,age")
        assert json.loads(out[0]) == {"first": "daffy", "last": "duck"}


class TestWrite(unittest.TestCase):
    def setUp(self):
        self.json_file = _tempfile() + ".json"
        fo = open(self.json_file, "w")
        for record in looney_records():
            json.dump(record, fo)
            fo.write("\n")
        fo.close()

        self.csv_file = _tempfile() + ".csv"
        fo = open(self.csv_file, "w")
        write = csv.writer(fo).writerow
        get = operator.itemgetter("first", "last", "type")
        for record in looney_records():
            write(get(record))
        fo.close()

        self.schema_file = _tempfile()
        fo = open(self.schema_file, "w")
        fo.write(SCHEMA)
        fo.close()

    def tearDown(self):
        for filename in (self.csv_file, self.json_file, self.schema_file):
            try:
                os.unlink(filename)
            except OSError:  # pragma: no coverage
                continue

    def _run(self, *args, **kw):
        args = [sys.executable, SCRIPT, "write", "--schema", self.schema_file] + list(args)
        subprocess.check_call(args, **kw)

    def load_avro(self, filename):
        out = subprocess.check_output([sys.executable, SCRIPT, "cat", filename]).decode()
        return [json.loads(o) for o in out.splitlines()]

    def test_version(self):
        subprocess.check_call([sys.executable, SCRIPT, "write", "--version"])

    def format_check(self, format, filename):
        tmp = _tempfile()
        with open(tmp, "wb") as fo:
            self._run(filename, "-f", format, stdout=fo)

        records = self.load_avro(tmp)
        assert len(records) == NUM_RECORDS
        assert records[0]["first"] == "daffy"

        os.unlink(tmp)

    def test_write_json(self):
        self.format_check("json", self.json_file)

    def test_write_csv(self):
        self.format_check("csv", self.csv_file)

    def test_outfile(self):
        tmp = _tempfile()
        os.unlink(tmp)
        self._run(self.json_file, "-o", tmp)

        assert len(self.load_avro(tmp)) == NUM_RECORDS
        os.unlink(tmp)

    def test_multi_file(self):
        tmp = _tempfile()
        with open(tmp, "wb") as o:
            self._run(self.json_file, self.json_file, stdout=o)
        assert len(self.load_avro(tmp)) == 2 * NUM_RECORDS
        os.unlink(tmp)

    def test_stdin(self):
        tmp = _tempfile()
        info = open(self.json_file, "rb")
        self._run("--input-type", "json", "-o", tmp, stdin=info)

        assert len(self.load_avro(tmp)) == NUM_RECORDS
        os.unlink(tmp)
