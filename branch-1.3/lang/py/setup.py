#! /usr/bin/env python

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
try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup

VERSION_FILE='../../share/VERSION.txt'

setup(
  name = 'avro',
  version = file(VERSION_FILE, 'r').read(),
  packages = ['avro',],
  package_dir = {'avro': 'src/avro'},

  # Project uses simplejson, so ensure that it gets installed or upgraded
  # on the target machine
  install_requires = ['simplejson >= 2.0.9'],

  # metadata for upload to PyPI
  author = 'Apache Avro',
  author_email = 'avro-dev@hadoop.apache.org',
  description = 'Avro is a serialization and RPC framework.',
  license = 'Apache License 2.0',
  keywords = 'avro serialization rpc',
  url = 'http://hadoop.apache.org/avro',
)
