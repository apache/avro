#!/usr/bin/env python

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

import collections
import os
import shutil
import subprocess
import tempfile
import unittest

import avro
import avro.datafile
import avro.io
import avro.schema

_TOPDIR = '@TOPDIR@'
_AVRO_VERSION = '@AVRO_VERSION@'

_JAR_PATH = os.path.abspath("{}/../java/tools/target/avro-tools-{}.jar".format(
  _TOPDIR, _AVRO_VERSION))

_CMD = (
  "java", "-jar", _JAR_PATH, "tether",
  "-protocol=http",
  "-program=python",
  "-exec_args", "-m avro.tether.tether_task_runner word_count_task.WordCountTask",
  "-in=in",
  "-out=out",
  "-outschema=out.avsc",
)

_INPUT_SCHEMA_STRING = '{"type": "string"}'

# The schema for the output of the mapper and reducer
_OUTPUT_SCHEMA_STRING = """{
  "type": "record",
  "name": "Pair",
  "namespace": "org.apache.avro.mapred",
  "fields": [
    {"name": "key", "type": "string"},
    {"name": "value", "type": "long", "order": "ignore"}
  ]
}"""

_LINES = [
  "the quick brown fox jumps over the lazy dog",
  "the cow jumps over the moon",
  "the rain in spain falls mainly on the plains",
]

_EXPECTED_COUNTS = collections.Counter(" ".join(_LINES).split())

_INNER_PYTHON_PATH = os.pathsep.join([
  os.path.dirname(os.path.dirname(avro.__file__)),
  os.path.dirname(__file__)
])

def _run(args, env):
  p = None
  try:
    p = subprocess.Popen(args, env=env)
    return p.wait()
  finally:
    if p is not None and p.returncode is None:
      p.kill()

def _write_lines(lines, fname):
  """
  Write the lines to an avro file named fname

  Parameters
  --------------------------------------------------------
  lines - list of strings to write
  fname - the name of the file to write to.
  """
  datum_writer = avro.io.DatumWriter(_INPUT_SCHEMA_STRING)
  wschema = avro.schema.parse(_INPUT_SCHEMA_STRING)
  with avro.datafile.DataFileWriter(open(fname, 'w'), datum_writer, writers_schema=wschema) as writer:
    for datum in lines:
      writer.append(datum)


class TestTetherWordCount(unittest.TestCase):
  """unittest for a python tethered map-reduce job."""

  def setUp(self):
    """Create the necessary tempfiles to run the tethered job."""
    self._prev_wd = os.getcwd()
    os.chdir(tempfile.mkdtemp(prefix=__name__ + '_'))
    os.mkdir('in')
    _write_lines(_LINES, 'in/lines.avsc')
    with open('out.avsc', 'w') as out_schema:
      out_schema.write(_OUTPUT_SCHEMA_STRING)

  def tearDown(self):
    """Clean up and remove temp files."""
    tmpdir = os.getcwd()
    os.chdir(self._prev_wd)
    # self.assertFalse(os.path.commonpath(self._prev_wd, tmpdir))
    # shutil.rmtree(tmpdir)

  def test_tethered_word_count(self):
    """A tethered map-reduce wordcount job should correctly count words."""
    print("Command:")
    print("\t" + " ".join(_CMD))
    self.assertEqual(0, _run(_CMD, {"PYTHONPATH": _INNER_PYTHON_PATH}))

    # read the output
    with avro.datafile.DataFileReader(open("out/part-00000.avro"), avro.io.DatumReader()) as reader:
      for record in reader:
        self.assertEqual(record["value"], _EXPECTED_COUNTS[record["key"]])

if __name__== "__main__":
  unittest.main()
