# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest
import csv
from cStringIO import StringIO
try:
    import json
except ImportError:
    import simplejson as json
from tempfile import NamedTemporaryFile
import avro.schema
from avro.io import DatumWriter
from avro.datafile import DataFileWriter
from os.path import dirname, join, isfile
from os import remove
from operator import itemgetter

NUM_RECORDS = 7

try:
    from subprocess import check_output
except ImportError:
    from subprocess import Popen, PIPE

    def check_output(args):
        pipe = Popen(args, stdout=PIPE)
        if pipe.wait() != 0:
            raise ValueError
        return pipe.stdout.read()

try:
    from subprocess import check_call
except ImportError:
    def check_call(args, **kw):
        pipe = Popen(args, **kw)
        assert pipe.wait() == 0

SCHEMA = '''
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
'''

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
        yield {"first": f, "last" : l, "type" : t}

SCRIPT = join(dirname(__file__), "..", "scripts", "avro")

_JSON_PRETTY = '''{
    "type": "duck", 
    "last": "duck", 
    "first": "daffy"
}'''

def gen_avro(filename):
    schema = avro.schema.parse(SCHEMA)
    fo = open(filename, "wb")
    writer = DataFileWriter(fo, DatumWriter(), schema)
    for record in looney_records():
        writer.append(record)
    writer.close()
    fo.close()

def tempfile():
    return NamedTemporaryFile(delete=False).name

class TestCat(unittest.TestCase):
    def setUp(self):
        self.avro_file = tempfile()
        gen_avro(self.avro_file)

    def tearDown(self):
        if isfile(self.avro_file):
            remove(self.avro_file)

    def _run(self, *args, **kw):
        out = check_output([SCRIPT, "cat", self.avro_file] + list(args))
        if kw.get("raw"):
            return out
        else:
            return out.splitlines()

    def test_print(self):
        return len(self._run()) == NUM_RECORDS

    def test_filter(self):
        return len(self._run("--filter", "r['type']=='bird'")) == 2

    def test_skip(self):
        skip = 3
        return len(self._run("--skip", str(skip))) == NUM_RECORDS - skip

    def test_csv(self):
        reader = csv.reader(StringIO(self._run("-f", "csv", raw=True)))
        assert len(list(reader)) == NUM_RECORDS

    def test_csv_header(self):
        io = StringIO(self._run("-f", "csv", "--header", raw=True))
        reader = csv.DictReader(io)
        r = {"type": "duck", "last": "duck", "first": "daffy"}
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
        assert out.strip() == _JSON_PRETTY.strip()

    def test_version(self):
        check_output([SCRIPT, "cat", "--version"])

    def test_files(self):
        out = self._run(self.avro_file)
        assert len(out) == 2 * NUM_RECORDS

    def test_fields(self):
        # One field selection (no comma)
        out = self._run('--fields', 'last')
        assert json.loads(out[0]) == {'last': 'duck'}

        # Field selection (with comma and space)
        out = self._run('--fields', 'first, last')
        assert json.loads(out[0]) == {'first': 'daffy', 'last': 'duck'}

        # Empty fields should get all
        out = self._run('--fields', '')
        assert json.loads(out[0]) == \
                {'first': 'daffy', 'last': 'duck', 'type': 'duck'}

        # Non existing fields are ignored
        out = self._run('--fields', 'first,last,age')
        assert json.loads(out[0]) == {'first': 'daffy', 'last': 'duck'}

class TestWrite(unittest.TestCase):
    def setUp(self):
        self.json_file = tempfile() + ".json"
        fo = open(self.json_file, "w")
        for record in looney_records():
            json.dump(record, fo)
            fo.write("\n")
        fo.close()

        self.csv_file = tempfile() + ".csv"
        fo = open(self.csv_file, "w")
        write = csv.writer(fo).writerow
        get = itemgetter("first", "last", "type")
        for record in looney_records():
            write(get(record))
        fo.close()

        self.schema_file = tempfile()
        fo = open(self.schema_file, "w")
        fo.write(SCHEMA)
        fo.close()

    def tearDown(self):
        for filename in (self.csv_file, self.json_file, self.schema_file):
            try:
                remove(filename)
            except OSError:
                continue

    def _run(self, *args, **kw):
        args = [SCRIPT, "write", "--schema", self.schema_file] + list(args)
        check_call(args, **kw)

    def load_avro(self, filename):
        out = check_output([SCRIPT, "cat", filename])
        return map(json.loads, out.splitlines())

    def test_version(self):
        check_call([SCRIPT, "write", "--version"])

    def format_check(self, format, filename):
        tmp = tempfile()
        fo = open(tmp, "wb")
        self._run(filename, "-f", format, stdout=fo)
        fo.close()

        records = self.load_avro(tmp)
        assert len(records) == NUM_RECORDS
        assert records[0]["first"] == "daffy"

        remove(tmp)

    def test_write_json(self):
        self.format_check("json", self.json_file)

    def test_write_csv(self):
        self.format_check("csv", self.csv_file)

    def test_outfile(self):
        tmp = tempfile()
        remove(tmp)
        self._run(self.json_file, "-o", tmp)

        assert len(self.load_avro(tmp)) == NUM_RECORDS
        remove(tmp)

    def test_multi_file(self):
        tmp = tempfile()
        fo = open(tmp, "wb")
        self._run(self.json_file, self.json_file, stdout=fo)
        fo.close()

        assert len(self.load_avro(tmp)) == 2 * NUM_RECORDS
        remove(tmp)

    def test_stdin(self):
        tmp = tempfile()

        info = open(self.json_file, "rb")
        fo = open(tmp, "wb")
        self._run("--input-type", "json", stdin=info, stdout=fo)
        fo.close()

        assert len(self.load_avro(tmp)) == NUM_RECORDS
        remove(tmp)
