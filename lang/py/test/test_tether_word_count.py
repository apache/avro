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


class TestTetherWordCount(unittest.TestCase):
  """unittest for a python tethered map-reduce job."""

  def _write_lines(self,lines,fname):
    """
    Write the lines to an avro file named fname

    Parameters
    --------------------------------------------------------
    lines - list of strings to write
    fname - the name of the file to write to.
    """
    os.makedirs(os.path.dirname(fname))

    inschema = '{"type": "string"}'
    datum_writer = avro.io.DatumWriter(inschema)
    wschema = avro.schema.parse(inschema)
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
    """
    Run a tethered map-reduce job.

    Assumptions: 1) bash is available in /bin/bash
    """
    # The schema for the output of the mapper and reducer
    oschema = """{
      "type": "record",
      "name": "Pair",
      "namespace": "org.apache.avro.mapred",
      "fields": [
        {"name": "key", "type": "string"},
        {"name": "value", "type": "long", "order": "ignore"}
      ]
    }"""
    lines = [
      "the quick brown fox jumps over the lazy dog",
      "the cow jumps over the moon",
      "the rain in spain falls mainly on the plains",
    ]
    # We use the tempfile module to generate random names for the files
    base_dir = "/tmp/test_tether_word_count"
    inpath = os.path.join(base_dir, "in")
    outpath = os.path.join(base_dir, "out")
    infile = os.path.join(inpath, "lines.avro")
    proc = None
    if os.path.exists(base_dir):
      shutil.rmtree(base_dir)
    true_counts = collections.Counter(" ".join(lines).split())
    # We need to make sure avro is on the path
    # getsourcefile(avro) returns .../avro/__init__.py
    apath = os.path.dirname(os.path.dirname(avro.__file__))
    tpath = os.path.dirname(__file__)  # Path to the tests.
    python_path = os.pathsep.join([apath, tpath])
    try:
      self._write_lines(lines, infile)
      if not(os.path.exists(infile)):
        self.fail("Missing the input file {0}".format(infile))

      # write the schema to a temporary file
      with tempfile.NamedTemporaryFile(mode='w', suffix=".avsc", prefix="wordcount", delete=False) as osfile:
        osfile.write(oschema)
      outschema = osfile.name

      if not(os.path.exists(outschema)):
        self.fail("Missing the schema file")

      args = self._tether_tool_command_line(inpath, outpath, outschema)
      print("Command:\n\t{0}".format(" ".join(args)))
      proc = subprocess.Popen(args, env={"PYTHONPATH": python_path})
      self.assertEqual(0, proc.wait())

      # read the output
      with open(os.path.join(outpath, "part-00000.avro")) as hf, \
          avro.datafile.DataFileReader(hf, avro.io.DatumReader()) as reader:
        for record in reader:
          self.assertEqual(record["value"], true_counts[record["key"]])
    finally:
      # close the process
      if proc is not None and proc.returncode is None:
        proc.kill()
      if os.path.exists(base_dir):
        shutil.rmtree(base_dir)

if __name__== "__main__":
  unittest.main()
