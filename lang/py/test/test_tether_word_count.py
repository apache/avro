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


class TestTetherWordCount(unittest.TestCase):
  """unittest for a python tethered map-reduce job."""

  _base_dir = None

  def setUp(self):
    self._base_dir = tempfile.mkdtemp(prefix=__name__ + '_')

  def tearDown(self):
    if self._base_dir is not None:
      shutil.rmtree(self._base_dir)

  def _write_lines(self, lines, fname):
    """
    Write the lines to an avro file named fname

    Parameters
    --------------------------------------------------------
    lines - list of strings to write
    fname - the name of the file to write to.
    """
    os.makedirs(os.path.dirname(fname))
    datum_writer = avro.io.DatumWriter(_INPUT_SCHEMA_STRING)
    wschema = avro.schema.parse(_INPUT_SCHEMA_STRING)
    with open(fname, 'w') as hf, \
        avro.datafile.DataFileWriter(hf, datum_writer, writers_schema=wschema) as writer:
      for datum in lines:
        writer.append(datum)

  def _tether_tool_command_line(self, inpath, outpath, outschema):
    return (
      "java",
      "-jar",
      os.path.abspath("@TOPDIR@/../java/tools/target/avro-tools-@AVRO_VERSION@.jar"),
      "tether",
      "-in", inpath,
      "-out", outpath,
      "-outschema", outschema,
      "-protocol", "http",
      "-program", "python",
      "-exec_args", "-m avro.tether.tether_task_runner word_count_task.WordCountTask",
    )

  def test_tethered_word_count(self):
    """A tethered map-reduce wordcount job should correctly count words."""
    base_dir = self._base_dir
    inpath = os.path.join(base_dir, "in")
    infile = os.path.join(inpath, "lines.avro")
    outpath = os.path.join(base_dir, "out")
    if os.path.exists(base_dir):
      shutil.rmtree(base_dir)
    self._write_lines(_LINES, infile)
    if not(os.path.exists(infile)):
      self.fail("Missing the input file {0}".format(infile))
    # write the schema to a temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix=".avsc", prefix="wordcount", delete=False) as osfile:
      osfile.write(_OUTPUT_SCHEMA_STRING)
    outschema = osfile.name

    if not(os.path.exists(outschema)):
      self.fail("Missing the schema file")

    args = self._tether_tool_command_line(inpath, outpath, outschema)
    print("Command:\n\t{0}".format(" ".join(args)))
    self.assertEqual(0, _run(args, {"PYTHONPATH": _INNER_PYTHON_PATH}))

    # read the output
    with open(os.path.join(outpath, "part-00000.avro")) as hf, \
        avro.datafile.DataFileReader(hf, avro.io.DatumReader()) as reader:
      for record in reader:
        self.assertEqual(record["value"], _EXPECTED_COUNTS[record["key"]])

if __name__== "__main__":
  unittest.main()
