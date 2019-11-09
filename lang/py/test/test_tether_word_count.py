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

from __future__ import absolute_import, division, print_function

import collections
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

import avro
import avro.datafile
import avro.io
import avro.schema
import avro.tether.tether_task_runner
import set_avro_test_path

_TOP_DIR = """@TOPDIR@"""
_AVRO_VERSION = """@AVRO_VERSION@"""
_JAR_PATH = os.path.abspath(os.path.join(_TOP_DIR, "..", "java", "tools", "target", "avro-tools-{}.jar".format(_AVRO_VERSION)))

_LINES = ("the quick brown fox jumps over the lazy dog",
          "the cow jumps over the moon",
          "the rain in spain falls mainly on the plains")
_IN_SCHEMA = '"string"'

# The schema for the output of the mapper and reducer
_OUT_SCHEMA = """{
  "type": "record",
  "name": "Pair",
  "namespace": "org.apache.avro.mapred",
  "fields": [{"name": "key", "type": "string"},
             {"name": "value", "type": "long", "order": "ignore"}]
}"""

# Create a shell script to act as the program we want to execute
# We do this so we can set the python path appropriately
_PYTHON_PATH = os.pathsep.join([os.path.dirname(os.path.dirname(avro.__file__)),
                                os.path.dirname(__file__)])


class TestTetherWordCount(unittest.TestCase):
  """unittest for a python tethered map-reduce job."""

  _base_dir = None
  _script_path = None
  _input_path = None
  _output_path = None
  _output_schema_path = None

  def setUp(self):
    """Create temporary files for testing."""
    prefix, _ = os.path.splitext(os.path.basename(__file__))
    self._base_dir = tempfile.mkdtemp(prefix=prefix)

    # We create the input path...
    self._input_path = os.path.join(self._base_dir, "in")
    if not os.path.exists(self._input_path):
      os.makedirs(self._input_path)
    infile = os.path.join(self._input_path, "lines.avro")
    self._write_lines(_LINES, infile)
    self.assertTrue(os.path.exists(infile), "Missing the input file {}".format(infile))

    # ...and the output schema...
    self._output_schema_path = os.path.join(self._base_dir, "output.avsc")
    with open(self._output_schema_path, 'wb') as output_schema_handle:
      output_schema_handle.write(_OUT_SCHEMA)
    self.assertTrue(os.path.exists(self._output_schema_path), "Missing the schema file")

    # ...but we just name the output path. The tether tool creates it.
    self._output_path = os.path.join(self._base_dir, "out")

  def tearDown(self):
    """Remove temporary files used in testing."""
    if os.path.exists(self._base_dir):
      shutil.rmtree(self._base_dir)
    if self._script_path is not None and os.path.exists(self._script_path):
      os.remove(self._script_path)

  def _write_lines(self, lines, fname):
    """
    Write the lines to an avro file named fname

    Parameters
    --------------------------------------------------------
    lines - list of strings to write
    fname - the name of the file to write to.
    """
    datum_writer = avro.io.DatumWriter(_IN_SCHEMA)
    writers_schema = avro.schema.parse(_IN_SCHEMA)
    with avro.datafile.DataFileWriter(open(fname, 'wb'), datum_writer, writers_schema) as writer:
      for datum in lines:
        writer.append(datum)

  def test_tether_word_count(self):
    """Check that a tethered map-reduce job produces the output expected locally."""
    # Run the job...
    args = ("java", "-jar", _JAR_PATH, "tether",
            "--protocol", "http",
            "--in", self._input_path,
            "--out", self._output_path,
            "--outschema", self._output_schema_path,
            "--program", sys.executable,
            "--exec_args", "-m avro.tether.tether_task_runner word_count_task.WordCountTask")
    print("Command:\n\t{0}".format(" ".join(args)))
    subprocess.check_call(args, env={"PYTHONPATH": _PYTHON_PATH})

    # ...and test the results.
    datum_reader = avro.io.DatumReader()
    outfile = os.path.join(self._output_path, "part-00000.avro")
    expected_counts = collections.Counter(' '.join(_LINES).split())
    with avro.datafile.DataFileReader(open(outfile, 'rb'), datum_reader) as reader:
      actual_counts = {r["key"]: r["value"] for r in reader}
    self.assertDictEqual(actual_counts, expected_counts)

if __name__== "__main__":
  unittest.main()
