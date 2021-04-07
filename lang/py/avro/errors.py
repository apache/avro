#!/usr/bin/env python3
# -*- mode: python -*-
# -*- coding: utf-8 -*-

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

import json


class AvroException(Exception):
    """The base class for exceptions in avro."""


class SchemaParseException(AvroException):
    """Raised when a schema failed to parse."""


class InvalidName(SchemaParseException):
    """User attempted to parse a schema with an invalid name."""


class AvroWarning(UserWarning):
    """Base class for warnings."""


class IgnoredLogicalType(AvroWarning):
    """Warnings for unknown or invalid logical types."""


class AvroTypeException(AvroException):
    """Raised when datum is not an example of schema."""
    def __init__(self, **kwargs):
        if any(key not in kwargs.keys() for key in ['expected_schema', 'name', 'datum']):
            raise TypeError('Missing keyword argument to AvroTypeException')

        fail_msg = 'The datum "{datum}" provided for "{name}" is not an example of the schema {pretty_expected}'
        pretty_expected = json.dumps(json.loads(str(kwargs['expected_schema'])), indent=2)

        super(AvroTypeException, self).__init__(
            fail_msg.format(
                datum=kwargs['datum'],
                name=kwargs['name'],
                pretty_expected=pretty_expected))


class SchemaResolutionException(AvroException):
    def __init__(self, fail_msg, writers_schema=None, readers_schema=None):
        pretty_writers = json.dumps(json.loads(str(writers_schema)), indent=2)
        pretty_readers = json.dumps(json.loads(str(readers_schema)), indent=2)
        if writers_schema:
            fail_msg += "\nWriter's Schema: {}".format(pretty_writers)
        if readers_schema:
            fail_msg += "\nReader's Schema: {}".format(pretty_readers)
        super(AvroException, self).__init__(fail_msg)


class DataFileException(AvroException):
    """Raised when there's a problem reading or writing file object containers."""


class AvroRemoteException(AvroException):
    """Raised when an error message is sent by an Avro requestor or responder."""


class ConnectionClosedException(AvroException):
    """Raised when attempting IPC on a closed connection."""


class ProtocolParseException(AvroException):
    """Raised when a protocol failed to parse."""


class UnsupportedCodec(NotImplementedError, AvroException):
    """Raised when the compression named cannot be used."""


class UsageError(RuntimeError, AvroException):
    """An exception raised when incorrect arguments were passed."""


class AvroRuntimeException(RuntimeError, AvroException):
    """Raised when compatibility parsing encounters an unknown type"""
