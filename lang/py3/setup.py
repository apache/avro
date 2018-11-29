#!/usr/bin/env python
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
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Provide the code necessary for packaging and installing avro-python3.

The avro-python3 software is designed for Python 3, but this file and the packaging software supports Python 2.7.

https://pypi.org/project/avro-python3/
"""


import os
import shutil
import stat
import sys

from setuptools import setup

VERSION_FILE_NAME = 'VERSION.txt'
LICENSE_FILE_NAME = 'LICENSE'
NOTICE_FILE_NAME = 'NOTICE'

def RunsFromSourceDist():
  """Tests whether setup.py is invoked from a source distribution.

  Returns:
    True if setup.py runs from a source distribution.
    False otherwise, ie. if setup.py runs from the SVN trunk.
  """
  setup_file_path = os.path.abspath(__file__)
  # If a file PKG-INFO exists as a sibling of setup.py,
  # assume we are running as source distribution:
  pkg_info_file_path = \
      os.path.join(os.path.dirname(setup_file_path), 'PKG-INFO')
  return os.path.exists(pkg_info_file_path)


def SetupSources():
  """Prepares the source directory.

  Runs when setup.py is invoked from the Avro SVN/Git source.
  """
  # Avro lang/py3/ source directory:
  py3_dir = os.path.dirname(os.path.abspath(__file__))

  # Avro top-level source directory:
  root_dir = os.path.dirname(os.path.dirname(py3_dir))

  # Read and copy Avro version:
  version_file_path = os.path.join(root_dir, 'share', VERSION_FILE_NAME)
  with open(version_file_path, 'r') as f:
    avro_version = f.read().strip()
  shutil.copy(
      src=version_file_path,
      dst=os.path.join(py3_dir, 'avro', VERSION_FILE_NAME),
  )

  # Copy necessary avsc files:
  avsc_file_path = os.path.join(
      root_dir, 'share', 'schemas',
      'org', 'apache', 'avro', 'ipc', 'HandshakeRequest.avsc')
  shutil.copy(
      src=avsc_file_path,
      dst=os.path.join(py3_dir, 'avro', 'HandshakeRequest.avsc'),
  )

  avsc_file_path = os.path.join(
      root_dir, 'share', 'schemas',
      'org', 'apache', 'avro', 'ipc', 'HandshakeResponse.avsc')
  shutil.copy(
      src=avsc_file_path,
      dst=os.path.join(py3_dir, 'avro', 'HandshakeResponse.avsc'),
  )

  avsc_file_path = os.path.join(
      root_dir, 'share', 'test', 'schemas', 'interop.avsc')
  shutil.copy(
      src=avsc_file_path,
      dst=os.path.join(py3_dir, 'avro', 'tests', 'interop.avsc'),
  )


def ReadVersion():
  """Returns: the content of the Avro version file."""
  setup_file_path = os.path.abspath(__file__)
  install_dir = os.path.dirname(setup_file_path)
  version_file_path = os.path.join(install_dir, 'avro', VERSION_FILE_NAME)
  with open(version_file_path, 'rt') as f:
    avro_version = f.read().strip()
  return avro_version


def Main():
  if not RunsFromSourceDist():
    SetupSources()

  avro_version = ReadVersion()

  setup(
      name = 'avro-python3',
      version = avro_version,
      packages = ['avro'],
      package_dir = {'avro': 'avro'},
      scripts = ['scripts/avro'],

      package_data = {
          'avro': [
              'HandshakeRequest.avsc',
              'HandshakeResponse.avsc',
              VERSION_FILE_NAME,
              LICENSE_FILE_NAME,
              NOTICE_FILE_NAME,
          ],
      },

      test_suite='avro.tests.run_tests',
      tests_require=[],

      # metadata for upload to PyPI
      author = 'Apache Avro',
      author_email = 'dev@avro.apache.org',
      description = 'Avro is a serialization and RPC framework.',
      license = 'Apache License 2.0',
      keywords = 'avro serialization rpc',
      url = 'http://avro.apache.org/',
      classifiers=(
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
      ),
      python_requires='>=3.4',
  )


if __name__ == '__main__':
  Main()
