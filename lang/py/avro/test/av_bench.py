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
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import, division, print_function

import string
import sys
import time
from random import choice, randint, sample

import avro.datafile
import avro.io
import avro.schema

types = ["A", "CNAME"]


def rand_name():
    return ''.join(sample(string.ascii_lowercase, 15))


def rand_ip():
    return "%s.%s.%s.%s" % (randint(0, 255), randint(0, 255), randint(0, 255), randint(0, 255))


def write(n):
    schema_s = """
    { "type": "record",
      "name": "Query",
    "fields" : [
        {"name": "query", "type": "string"},
        {"name": "response", "type": "string"},
        {"name": "type", "type": "string", "default": "A"}
    ]}"""
    out = open("datafile.avr", 'w')

    schema = avro.schema.parse(schema_s)
    writer = avro.io.DatumWriter(schema)
    dw = avro.datafile.DataFileWriter(out, writer, schema)  # ,codec='deflate')
    for _ in xrange(n):
        response = rand_ip()
        query = rand_name()
        type = choice(types)
        dw.append({'query': query, 'response': response, 'type': type})

    dw.close()


def read():
    f = open("datafile.avr")
    reader = avro.io.DatumReader()
    af = avro.datafile.DataFileReader(f, reader)

    x = 0
    for _ in af:
        pass


def t(f, *args):
    s = time.time()
    f(*args)
    e = time.time()
    return e - s


if __name__ == "__main__":
    n = int(sys.argv[1])
    print("Write %0.4f" % t(write, n))
    print("Read %0.4f" % t(read))
