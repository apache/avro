#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import csv
import io
import json
import logging
import operator
import os
import subprocess
import tempfile
import unittest

import avro.datafile
import avro.io
import avro.schema

# ------------------------------------------------------------------------------


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
    ('daffy', 'duck', 'duck'),
    ('bugs', 'bunny', 'bunny'),
    ('tweety', '', 'bird'),
    ('road', 'runner', 'bird'),
    ('wile', 'e', 'coyote'),
    ('pepe', 'le pew', 'skunk'),
    ('foghorn', 'leghorn', 'rooster'),
)


def looney_records():
  for f, l, t in LOONIES:
    yield {'first': f, 'last' : l, 'type' : t}


def GetRootDir():
  test_dir = os.path.dirname(os.path.abspath(__file__))
  root_dir = os.path.dirname(os.path.dirname(test_dir))
  return root_dir


def GetScriptPath():
  root_dir = GetRootDir()
  avro_script_path = os.path.join(root_dir, 'scripts', 'avro')
  assert os.path.exists(avro_script_path), \
      ('Avro script not found: %r' % avro_script_path)
  return avro_script_path


# Absolute path of the 'avro' script:
SCRIPT_PATH = GetScriptPath()


def RunScript(*args, stdin=None):
  command = [SCRIPT_PATH]
  command.extend(args)
  env = dict(os.environ)
  env['PYTHONPATH'] = '%s:%s' % (GetRootDir(), env.get('PYTHONPATH', ''))
  logging.debug('Running command:\n%s', ' \\\n\t'.join(command))
  process = subprocess.Popen(
      args=command,
      env=env,
      stdin=stdin,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
  )
  (out, err) = process.communicate()
  assert (process.returncode == os.EX_OK), \
      ('Command %r failed with exit code %r, output %r and error %r'
       % (command, process.returncode, out, err))
  return out


_TEST_JSON_VALUE = {
  'first': 'daffy',
  'last': 'duck',
  'type': 'duck',
}


# ------------------------------------------------------------------------------


class TestCat(unittest.TestCase):

  @staticmethod
  def WriteAvroFile(file_path):
    schema = avro.schema.parse(SCHEMA)
    with open(file_path, 'wb') as writer:
      with avro.datafile.DataFileWriter(
          writer=writer,
          datum_writer=avro.io.DatumWriter(),
          writer_schema=schema,
      ) as writer:
        for record in looney_records():
          writer.append(record)

  def setUp(self):
    # TODO: flag to not delete the files
    delete = True
    self._avro_file = (
        tempfile.NamedTemporaryFile(prefix='test-', suffix='.avro', delete=delete))
    TestCat.WriteAvroFile(self._avro_file.name)

  def tearDown(self):
    self._avro_file.close()

  def _RunCat(self, *args, raw=False):
    """Runs the specified 'avro cat test-file ...' command.

    Args:
      *args: extra parameters to the 'avro cat' command.
      raw: Whether to decode stdout as UTF-8.
    Returns:
      The command stdout (as bytes if raw is set, or else as string).
    """
    out = RunScript('cat', self._avro_file.name, *args)
    if raw:
      return out
    else:
      return out.decode('utf-8')

  def testPrint(self):
    lines = self._RunCat().splitlines()
    return len(lines) == NUM_RECORDS

  def testFilter(self):
    lines = self._RunCat('--filter', "r['type']=='bird'").splitlines()
    return len(lines) == 2

  def testSkip(self):
    skip = 3
    lines = self._RunCat('--skip', str(skip)).splitlines()
    return len(lines) == NUM_RECORDS - skip

  def testCsv(self):
    reader = csv.reader(io.StringIO(self._RunCat('-f', 'csv')))
    self.assertEqual(len(list(reader)), NUM_RECORDS)

  def testCsvHeader(self):
    reader = csv.DictReader(io.StringIO(self._RunCat('-f', 'csv', '--header')))
    expected = {'type': 'duck', 'last': 'duck', 'first': 'daffy'}

    data = next(reader)
    self.assertEqual(expected, data)

  def testPrintSchema(self):
    out = self._RunCat('--print-schema')
    self.assertEqual(json.loads(out)['namespace'], 'test.avro')

  def testHelp(self):
    # Just see we have these
    self._RunCat('-h')
    self._RunCat('--help')

  def testJsonPretty(self):
    out = self._RunCat('--format', 'json-pretty', '-n', '1')
    self.assertEqual(
        json.loads(out),
        _TEST_JSON_VALUE,
        'Output mismatch\n'
        'Expect: %r\n'
        'Actual: %r'
        % (_TEST_JSON_VALUE, out))

  def testVersion(self):
    out = RunScript('cat', '--version').decode('utf-8')

  def testFiles(self):
    lines = self._RunCat(self._avro_file.name).splitlines()
    self.assertEqual(len(lines), 2 * NUM_RECORDS)

  def testFields(self):
    # One field selection (no comma)
    lines = self._RunCat('--fields', 'last').splitlines()
    self.assertEqual(json.loads(lines[0]), {'last': 'duck'})

    # Field selection (with comma and space)
    lines = self._RunCat('--fields', 'first, last').splitlines()
    self.assertEqual(json.loads(lines[0]), {'first': 'daffy', 'last': 'duck'})

    # Empty fields should get all
    lines = self._RunCat('--fields', '').splitlines()
    self.assertEqual(
        json.loads(lines[0]),
        {'first': 'daffy', 'last': 'duck', 'type': 'duck'})

    # Non existing fields are ignored
    lines = self._RunCat('--fields', 'first,last,age').splitlines()
    self.assertEqual(
        json.loads(lines[0]),
        {'first': 'daffy', 'last': 'duck'})


# ------------------------------------------------------------------------------


class TestWrite(unittest.TestCase):

  def setUp(self):
    delete = False

    self._json_file = tempfile.NamedTemporaryFile(
        prefix='test-', suffix='.json', delete=delete)
    with open(self._json_file.name, 'w') as f:
      for record in looney_records():
        json.dump(record, f)
        f.write('\n')

    self._csv_file = tempfile.NamedTemporaryFile(
        prefix='test-', suffix='.csv', delete=delete)
    with open(self._csv_file.name, 'w') as f:
      writer = csv.writer(f)
      get = operator.itemgetter('first', 'last', 'type')
      for record in looney_records():
        writer.writerow(get(record))

    self._schema_file = tempfile.NamedTemporaryFile(
        prefix='test-', suffix='.avsc', delete=delete)
    with open(self._schema_file.name, 'w') as f:
      f.write(SCHEMA)

  def tearDown(self):
    self._csv_file.close()
    self._json_file.close()
    self._schema_file.close()

  def _RunWrite(self, *args, stdin=None):
    """Runs the specified 'avro write ...' command.

    Args:
      *args: extra parameters to the 'avro write' command.
      stdin: Optional string to feed the 'avro write' command stdin with.
    Returns:
      The command stdout as bytes.
    """
    return RunScript(
        'write', '--schema', self._schema_file.name,
        stdin=stdin, *args
    )

  def LoadAvro(self, filename):
    out = RunScript('cat', filename).decode('utf-8')
    return tuple(map(json.loads, out.splitlines()))

  def testVersion(self):
    out = RunScript('write', '--version').decode('utf-8')

  def FormatCheck(self, format, filename):
    with tempfile.NamedTemporaryFile(prefix='test-', suffix='.dat') as temp:
      with open(temp.name, 'wb') as out:
        out.write(self._RunWrite(filename, '-f', format))

      records = self.LoadAvro(temp.name)
      self.assertEqual(len(records), NUM_RECORDS)
      self.assertEqual(records[0]['first'], 'daffy')

  def testWriteJson(self):
    self.FormatCheck('json', self._json_file.name)

  def testWriteCsv(self):
    self.FormatCheck('csv', self._csv_file.name)

  def testOutfile(self):
    with tempfile.NamedTemporaryFile(prefix='test-', suffix='.dat') as temp:
      os.remove(temp.name)
      self._RunWrite(self._json_file.name, '-o', temp.name)
      self.assertEqual(len(self.LoadAvro(temp.name)), NUM_RECORDS)

  def testMultiFile(self):
    with tempfile.NamedTemporaryFile(prefix='test-', suffix='.dat') as temp:
      with open(temp.name, 'wb') as out:
        out.write(self._RunWrite(self._json_file.name, self._json_file.name))

      self.assertEqual(len(self.LoadAvro(temp.name)), 2 * NUM_RECORDS)

  def testStdin(self):
    with tempfile.NamedTemporaryFile(prefix='test-', suffix='.dat') as temp:
      with open(self._json_file.name, 'rb') as input_content:
        with open(temp.name, 'wb') as out:
          out.write(self._RunWrite('--input-type', 'json', stdin=input_content))

      self.assertEqual(len(self.LoadAvro(temp.name)), NUM_RECORDS)


if __name__ == '__main__':
  raise Exception('Use run_tests.py')
