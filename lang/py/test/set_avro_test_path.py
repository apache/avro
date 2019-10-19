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

"""
Module adjusts the path PYTHONPATH so the unittests
will work even if an egg for AVRO is already installed.
By default eggs always appear higher on pythons path then
directories set via the environment variable PYTHONPATH.

For reference see:
https://www.velocityreviews.com/forums/t716589-pythonpath-and-eggs.html
https://stackoverflow.com/questions/897792/pythons-sys-path-value.

Unittests would therefore use the installed AVRO and not the AVRO
being built. To work around this the unittests import this module before
importing AVRO. This module in turn adjusts the python path so that the test
build of AVRO is higher on the path then any installed eggs.
"""

from __future__ import absolute_import, division, print_function

import os
import sys

# determine the build directory and then make sure all paths that start with the
# build directory are at the top of the path
builddir=os.path.split(os.path.split(__file__)[0])[0]
bpaths=filter(lambda s:s.startswith(builddir), sys.path)

for p in bpaths:
  sys.path.insert(0,p)
