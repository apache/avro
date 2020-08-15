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

import argparse
import json
import random
import string
import tempfile
import timeit
import unittest
import unittest.mock

import avro.datafile
import avro.io
import avro.schema

TYPES = ("A", "CNAME")
SCHEMA = avro.schema.parse(json.dumps({
    "type": "record",
    "name": "Query",
    "fields": [
        {"name": "query", "type": "string"},
        {"name": "response", "type": "string"},
        {"name": "type", "type": "string", "default": "A"}
    ]
}))
READER = avro.io.DatumReader(SCHEMA)
WRITER = avro.io.DatumWriter(SCHEMA)
NUMBER_OF_TESTS = 10000
MAX_WRITE_SECONDS = 1
MAX_READ_SECONDS = 1


class TestBench(unittest.TestCase):
    def test_minimum_speed(self):
        with tempfile.NamedTemporaryFile(suffix='avr') as temp:
            pass
        self.assertTrue(time_writes(temp.name, NUMBER_OF_TESTS) < MAX_WRITE_SECONDS,
                        'Took longer than {} second(s) to write the test file with {} values.'
                        .format(MAX_WRITE_SECONDS, NUMBER_OF_TESTS))
        self.assertTrue(time_read(temp.name) < MAX_READ_SECONDS,
                        'Took longer than {} second(s) to read the test file with {} values.'
                        .format(MAX_READ_SECONDS, NUMBER_OF_TESTS))


def rand_name():
    return ''.join(random.sample(string.ascii_lowercase, 15))


def rand_ip():
    return '{}.{}.{}.{}'.format(*[random.randint(0, 255) for _ in range(4)])


def picks(n):
    return [{"query": rand_name(), "response": rand_ip(), "type": random.choice(TYPES)}
            for _ in range(n)]


def time_writes(path, number):
    with avro.datafile.DataFileWriter(open(path, 'wb'), WRITER, SCHEMA) as dw:
        globals_ = {"dw": dw, "picks": picks(number)}
        return timeit.timeit('dw.append(next(p))', number=number, setup='p=iter(picks)', globals=globals_)


def time_read(path):
    """
    Time how long it takes to read the file written in the `write` function.
    We only do this once, because the size of the file is defined by the number sent to `write`.
    """
    with avro.datafile.DataFileReader(open(path, 'rb'), READER) as dr:
        return timeit.timeit('tuple(dr)', number=1, globals={"dr": dr})


def parse_args():  # pragma: no cover
    parser = argparse.ArgumentParser(description='Benchmark writing some random avro.')
    parser.add_argument('--number', '-n', type=int, default=timeit.default_number, help='how many times to run')
    return parser.parse_args()


def main():  # pragma: no cover
    args = parse_args()
    with tempfile.NamedTemporaryFile(suffix='.avr') as temp:
        pass
    print("Using file {}".format(temp.name))
    print("Writing: {}".format(time_writes(temp.name, args.number)))
    print("Reading: {}".format(time_read(temp.name)))


if __name__ == '__main__':  # pragma: no cover
    main()
