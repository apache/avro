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

"""Contains Constants for Python Avro"""

from __future__ import absolute_import, division, print_function

DATE = "date"
DECIMAL = "decimal"
TIMESTAMP_MICROS = "timestamp-micros"
TIMESTAMP_MILLIS = "timestamp-millis"
TIME_MICROS = "time-micros"
TIME_MILLIS = "time-millis"

SUPPORTED_LOGICAL_TYPE = [
    DATE,
    DECIMAL,
    TIMESTAMP_MICROS,
    TIMESTAMP_MILLIS,
    TIME_MICROS,
    TIME_MILLIS
]
