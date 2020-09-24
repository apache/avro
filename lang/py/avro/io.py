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

"""
Input/Output utilities, including:

 * i/o-specific constants
 * i/o-specific exceptions
 * schema validation
 * leaf value encoding and decoding
 * datum reader/writer stuff (?)

Also includes a generic representation for data, which
uses the following mapping:

  * Schema records are implemented as dict.
  * Schema arrays are implemented as list.
  * Schema maps are implemented as dict.
  * Schema strings are implemented as str.
  * Schema bytes are implemented as bytes.
  * Schema ints are implemented as int.
  * Schema longs are implemented as int.
  * Schema floats are implemented as float.
  * Schema doubles are implemented as float.
  * Schema booleans are implemented as bool.

Validation:

The validation of schema is performed using breadth-first graph
traversal. This allows validation exceptions to pinpoint the exact node
within a complex schema that is problematic, simplifying debugging
considerably. Because it is a traversal, it will also be less
resource-intensive, particularly when validating schema with deep
structures.

Components
==========

Nodes
-----
Avro schemas contain many different schema types. Data about the schema
types is used to validate the data in the corresponding part of a Python
body (the object to be serialized). A node combines a given schema type
with the corresponding Python data, as well as an optional "name" to
identify the specific node. Names are generally the name of a schema
(for named schema) or the name of a field (for child nodes of schema
with named children like maps and records), or None, for schema who's
children are not named (like Arrays).

Iterators
---------
Iterators are generator functions that take a node and return a
generator which will yield a node for each child datum in the data for
the current node. If a node is of a type which has no children, then the
default iterator will immediately exit.

Validators
----------
Validators are used to determine if the datum for a given node is valid
according to the given schema type. Validator functions take a node as
an argument and return a node if the node datum passes validation. If it
does not, the validator must return None.

In most cases, the node returned is identical to the node provided (is
in fact the same object). However, in the case of Union schema, the
returned "valid" node will hold the schema that is represented by the
datum contained. This allows iteration over the child nodes
in that datum, if there are any.
"""

import collections
import datetime
import decimal
import json
import struct

import avro.constants
import avro.errors
import avro.timezones

#
# Constants
#


# TODO(hammer): shouldn't ! be < for little-endian (according to spec?)
STRUCT_FLOAT = struct.Struct('<f')           # big-endian float
STRUCT_DOUBLE = struct.Struct('<d')          # big-endian double
STRUCT_SIGNED_SHORT = struct.Struct('>h')    # big-endian signed short
STRUCT_SIGNED_INT = struct.Struct('>i')      # big-endian signed int
STRUCT_SIGNED_LONG = struct.Struct('>q')     # big-endian signed long


#
# Validate
#


ValidationNode = collections.namedtuple("ValidationNode", ['schema', 'datum', 'name'])


def validate(expected_schema, datum, raise_on_error=False):
    """Return True if the provided datum is valid for the expected schema

    If raise_on_error is passed and True, then raise a validation error
    with specific information about the error encountered in validation.

    :param expected_schema: An avro schema type object representing the schema against
                            which the datum will be validated.
    :param datum: The datum to be validated, A python dictionary or some supported type
    :param raise_on_error: True if a AvroTypeException should be raised immediately when a
                           validation problem is encountered.
    :raises: AvroTypeException if datum is invalid and raise_on_error is True
    :returns: True if datum is valid for expected_schema, False if not.
    """
    # use a FIFO queue to process schema nodes breadth first.
    nodes = collections.deque()
    nodes.append(ValidationNode(expected_schema, datum, getattr(expected_schema, "name", None)))

    while nodes:
        current_node = nodes.popleft()

        # _validate_node returns the node for iteration if it is valid. Or it returns None
        # if current_node.schema.type in {'array', 'map', 'record'}:
        validated_schema = current_node.schema.validate(current_node.datum)
        if validated_schema:
            valid_node = ValidationNode(validated_schema, current_node.datum, current_node.name)
        else:
            valid_node = None
        # else:
        #     valid_node = _validate_node(current_node)

        if valid_node is not None:
            # if there are children of this node to append, do so.
            for child_node in _iterate_node(valid_node):
                nodes.append(child_node)
        else:
            # the current node was not valid.
            if raise_on_error:
                raise avro.errors.AvroTypeException(current_node.schema, current_node.datum)
            else:
                # preserve the prior validation behavior of returning false when there are problems.
                return False

    return True


def _iterate_node(node):
    for item in _ITERATORS.get(node.schema.type, _default_iterator)(node):
        yield ValidationNode(*item)


#############
# Iteration #
#############

def _default_iterator(_):
    """Immediately raise StopIteration.

    This exists to prevent problems with iteration over unsupported container types.

    More efficient approaches are not possible due to support for Python 2.7
    """
    for item in ():
        yield item


def _record_iterator(node):
    """Yield each child node of the provided record node."""
    schema, datum, name = node
    for field in schema.fields:
        yield ValidationNode(field.type, datum.get(field.name), field.name)  # type: ignore


def _array_iterator(node):
    """Yield each child node of the provided array node."""
    schema, datum, name = node
    for item in datum:  # type: ignore
        yield ValidationNode(schema.items, item, name)


def _map_iterator(node):
    """Yield each child node of the provided map node."""
    schema, datum, _ = node
    child_schema = schema.values
    for child_name, child_datum in datum.items():  # type: ignore
        yield ValidationNode(child_schema, child_datum, child_name)


_ITERATORS = {
    'record': _record_iterator,
    'array': _array_iterator,
    'map': _map_iterator,
}
_ITERATORS['error'] = _ITERATORS['request'] = _ITERATORS['record']


#
# Decoder/Encoder
#

class BinaryDecoder:
    """Read leaf values."""

    def __init__(self, reader):
        """
        reader is a Python object on which we can call read, seek, and tell.
        """
        self._reader = reader

    # read-only properties
    reader = property(lambda self: self._reader)

    def read(self, n):
        """
        Read n bytes.
        """
        return self.reader.read(n)

    def read_null(self):
        """
        null is written as zero bytes
        """
        return None

    def read_boolean(self):
        """
        a boolean is written as a single byte
        whose value is either 0 (false) or 1 (true).
        """
        return ord(self.read(1)) == 1

    def read_int(self):
        """
        int and long values are written using variable-length, zig-zag coding.
        """
        return self.read_long()

    def read_long(self):
        """
        int and long values are written using variable-length, zig-zag coding.
        """
        b = ord(self.read(1))
        n = b & 0x7F
        shift = 7
        while (b & 0x80) != 0:
            b = ord(self.read(1))
            n |= (b & 0x7F) << shift
            shift += 7
        datum = (n >> 1) ^ -(n & 1)
        return datum

    def read_float(self):
        """
        A float is written as 4 bytes.
        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        return STRUCT_FLOAT.unpack(self.read(4))[0]

    def read_double(self):
        """
        A double is written as 8 bytes.
        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        return STRUCT_DOUBLE.unpack(self.read(8))[0]

    def read_decimal_from_bytes(self, precision, scale):
        """
        Decimal bytes are decoded as signed short, int or long depending on the
        size of bytes.
        """
        size = self.read_long()
        return self.read_decimal_from_fixed(precision, scale, size)

    def read_decimal_from_fixed(self, precision, scale, size):
        """
        Decimal is encoded as fixed. Fixed instances are encoded using the
        number of bytes declared in the schema.
        """
        datum = self.read(size)
        unscaled_datum = 0
        msb = struct.unpack('!b', datum[0:1])[0]
        leftmost_bit = (msb >> 7) & 1
        if leftmost_bit == 1:
            modified_first_byte = ord(datum[0:1]) ^ (1 << 7)
            datum = bytearray([modified_first_byte]) + datum[1:]
            for offset in range(size):
                unscaled_datum <<= 8
                unscaled_datum += ord(datum[offset:1 + offset])
            unscaled_datum += pow(-2, (size * 8) - 1)
        else:
            for offset in range(size):
                unscaled_datum <<= 8
                unscaled_datum += ord(datum[offset:1 + offset])

        original_prec = decimal.getcontext().prec
        try:
            decimal.getcontext().prec = precision
            scaled_datum = decimal.Decimal(unscaled_datum).scaleb(-scale)
        finally:
            decimal.getcontext().prec = original_prec
        return scaled_datum

    def read_bytes(self):
        """
        Bytes are encoded as a long followed by that many bytes of data.
        """
        return self.read(self.read_long())

    def read_utf8(self):
        """
        A string is encoded as a long followed by
        that many bytes of UTF-8 encoded character data.
        """
        return self.read_bytes().decode("utf-8")

    def read_date_from_int(self):
        """
        int is decoded as python date object.
        int stores the number of days from
        the unix epoch, 1 January 1970 (ISO calendar).
        """
        days_since_epoch = self.read_int()
        return datetime.date(1970, 1, 1) + datetime.timedelta(days_since_epoch)

    def _build_time_object(self, value, scale_to_micro):
        value = value * scale_to_micro
        value, microseconds = value // 1000000, value % 1000000
        value, seconds = value // 60, value % 60
        value, minutes = value // 60, value % 60
        hours = value

        return datetime.time(
            hour=hours,
            minute=minutes,
            second=seconds,
            microsecond=microseconds
        )

    def read_time_millis_from_int(self):
        """
        int is decoded as python time object which represents
        the number of milliseconds after midnight, 00:00:00.000.
        """
        milliseconds = self.read_int()
        return self._build_time_object(milliseconds, 1000)

    def read_time_micros_from_long(self):
        """
        long is decoded as python time object which represents
        the number of microseconds after midnight, 00:00:00.000000.
        """
        microseconds = self.read_long()
        return self._build_time_object(microseconds, 1)

    def read_timestamp_millis_from_long(self):
        """
        long is decoded as python datetime object which represents
        the number of milliseconds from the unix epoch, 1 January 1970.
        """
        timestamp_millis = self.read_long()
        timedelta = datetime.timedelta(microseconds=timestamp_millis * 1000)
        unix_epoch_datetime = datetime.datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=avro.timezones.utc)
        return unix_epoch_datetime + timedelta

    def read_timestamp_micros_from_long(self):
        """
        long is decoded as python datetime object which represents
        the number of microseconds from the unix epoch, 1 January 1970.
        """
        timestamp_micros = self.read_long()
        timedelta = datetime.timedelta(microseconds=timestamp_micros)
        unix_epoch_datetime = datetime.datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=avro.timezones.utc)
        return unix_epoch_datetime + timedelta

    def skip_null(self):
        pass

    def skip_boolean(self):
        self.skip(1)

    def skip_int(self):
        self.skip_long()

    def skip_long(self):
        b = ord(self.read(1))
        while (b & 0x80) != 0:
            b = ord(self.read(1))

    def skip_float(self):
        self.skip(4)

    def skip_double(self):
        self.skip(8)

    def skip_bytes(self):
        self.skip(self.read_long())

    def skip_utf8(self):
        self.skip_bytes()

    def skip(self, n):
        self.reader.seek(self.reader.tell() + n)


class BinaryEncoder:
    """Write leaf values."""

    def __init__(self, writer):
        """
        writer is a Python object on which we can call write.
        """
        self._writer = writer

    # read-only properties
    writer = property(lambda self: self._writer)

    def write(self, datum):
        """Write an arbitrary datum."""
        self.writer.write(datum)

    def write_null(self, datum):
        """
        null is written as zero bytes
        """
        pass

    def write_boolean(self, datum):
        """
        a boolean is written as a single byte
        whose value is either 0 (false) or 1 (true).
        """
        self.write(bytearray([bool(datum)]))

    def write_int(self, datum):
        """
        int and long values are written using variable-length, zig-zag coding.
        """
        self.write_long(datum)

    def write_long(self, datum):
        """
        int and long values are written using variable-length, zig-zag coding.
        """
        datum = (datum << 1) ^ (datum >> 63)
        while (datum & ~0x7F) != 0:
            self.write(bytearray([(datum & 0x7f) | 0x80]))
            datum >>= 7
        self.write(bytearray([datum]))

    def write_float(self, datum):
        """
        A float is written as 4 bytes.
        The float is converted into a 32-bit integer using a method equivalent to
        Java's floatToIntBits and then encoded in little-endian format.
        """
        self.write(STRUCT_FLOAT.pack(datum))

    def write_double(self, datum):
        """
        A double is written as 8 bytes.
        The double is converted into a 64-bit integer using a method equivalent to
        Java's doubleToLongBits and then encoded in little-endian format.
        """
        self.write(STRUCT_DOUBLE.pack(datum))

    def write_decimal_bytes(self, datum, scale):
        """
        Decimal in bytes are encoded as long. Since size of packed value in bytes for
        signed long is 8, 8 bytes are written.
        """
        sign, digits, exp = datum.as_tuple()
        if exp > scale:
            raise avro.errors.AvroTypeException('Scale provided in schema does not match the decimal')

        unscaled_datum = 0
        for digit in digits:
            unscaled_datum = (unscaled_datum * 10) + digit

        bits_req = unscaled_datum.bit_length() + 1
        if sign:
            unscaled_datum = (1 << bits_req) - unscaled_datum

        bytes_req = bits_req // 8
        padding_bits = ~((1 << bits_req) - 1) if sign else 0
        packed_bits = padding_bits | unscaled_datum

        bytes_req += 1 if (bytes_req << 3) < bits_req else 0
        self.write_long(bytes_req)
        for index in range(bytes_req - 1, -1, -1):
            bits_to_write = packed_bits >> (8 * index)
            self.write(bytearray([bits_to_write & 0xff]))

    def write_decimal_fixed(self, datum, scale, size):
        """
        Decimal in fixed are encoded as size of fixed bytes.
        """
        sign, digits, exp = datum.as_tuple()
        if exp > scale:
            raise avro.errors.AvroTypeException('Scale provided in schema does not match the decimal')

        unscaled_datum = 0
        for digit in digits:
            unscaled_datum = (unscaled_datum * 10) + digit

        bits_req = unscaled_datum.bit_length() + 1
        size_in_bits = size * 8
        offset_bits = size_in_bits - bits_req

        mask = 2 ** size_in_bits - 1
        bit = 1
        for i in range(bits_req):
            mask ^= bit
            bit <<= 1

        if bits_req < 8:
            bytes_req = 1
        else:
            bytes_req = bits_req // 8
            if bits_req % 8 != 0:
                bytes_req += 1
        if sign:
            unscaled_datum = (1 << bits_req) - unscaled_datum
            unscaled_datum = mask | unscaled_datum
            for index in range(size - 1, -1, -1):
                bits_to_write = unscaled_datum >> (8 * index)
                self.write(bytearray([bits_to_write & 0xff]))
        else:
            for i in range(offset_bits // 8):
                self.write(b'\x00')
            for index in range(bytes_req - 1, -1, -1):
                bits_to_write = unscaled_datum >> (8 * index)
                self.write(bytearray([bits_to_write & 0xff]))

    def write_bytes(self, datum):
        """
        Bytes are encoded as a long followed by that many bytes of data.
        """
        self.write_long(len(datum))
        self.write(struct.pack('%ds' % len(datum), datum))

    def write_utf8(self, datum):
        """
        A string is encoded as a long followed by
        that many bytes of UTF-8 encoded character data.
        """
        datum = datum.encode("utf-8")
        self.write_bytes(datum)

    def write_date_int(self, datum):
        """
        Encode python date object as int.
        It stores the number of days from
        the unix epoch, 1 January 1970 (ISO calendar).
        """
        delta_date = datum - datetime.date(1970, 1, 1)
        self.write_int(delta_date.days)

    def write_time_millis_int(self, datum):
        """
        Encode python time object as int.
        It stores the number of milliseconds from midnight, 00:00:00.000
        """
        milliseconds = datum.hour * 3600000 + datum.minute * 60000 + datum.second * 1000 + datum.microsecond // 1000
        self.write_int(milliseconds)

    def write_time_micros_long(self, datum):
        """
        Encode python time object as long.
        It stores the number of microseconds from midnight, 00:00:00.000000
        """
        microseconds = datum.hour * 3600000000 + datum.minute * 60000000 + datum.second * 1000000 + datum.microsecond
        self.write_long(microseconds)

    def _timedelta_total_microseconds(self, timedelta):
        return (
            timedelta.microseconds + (timedelta.seconds + timedelta.days * 24 * 3600) * 10 ** 6)

    def write_timestamp_millis_long(self, datum):
        """
        Encode python datetime object as long.
        It stores the number of milliseconds from midnight of unix epoch, 1 January 1970.
        """
        datum = datum.astimezone(tz=avro.timezones.utc)
        timedelta = datum - datetime.datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=avro.timezones.utc)
        milliseconds = self._timedelta_total_microseconds(timedelta) // 1000
        self.write_long(milliseconds)

    def write_timestamp_micros_long(self, datum):
        """
        Encode python datetime object as long.
        It stores the number of microseconds from midnight of unix epoch, 1 January 1970.
        """
        datum = datum.astimezone(tz=avro.timezones.utc)
        timedelta = datum - datetime.datetime(1970, 1, 1, 0, 0, 0, 0, tzinfo=avro.timezones.utc)
        microseconds = self._timedelta_total_microseconds(timedelta)
        self.write_long(microseconds)


#
# DatumReader/Writer
#
class DatumReader:
    """Deserialize Avro-encoded data into a Python data structure."""

    def __init__(self, writers_schema=None, readers_schema=None):
        """
        As defined in the Avro specification, we call the schema encoded
        in the data the "writer's schema", and the schema expected by the
        reader the "reader's schema".
        """
        self._writers_schema = writers_schema
        self._readers_schema = readers_schema

    # read/write properties
    def set_writers_schema(self, writers_schema):
        self._writers_schema = writers_schema
    writers_schema = property(lambda self: self._writers_schema,
                              set_writers_schema)

    def set_readers_schema(self, readers_schema):
        self._readers_schema = readers_schema
    readers_schema = property(lambda self: self._readers_schema,
                              set_readers_schema)

    def read(self, decoder):
        if self.readers_schema is None:
            self.readers_schema = self.writers_schema
        return self.read_data(self.writers_schema, self.readers_schema, decoder)

    def read_data(self, writers_schema, readers_schema, decoder):
        # schema matching
        if not readers_schema.match(writers_schema):
            fail_msg = 'Schemas do not match.'
            raise avro.errors.SchemaResolutionException(fail_msg, writers_schema, readers_schema)

        logical_type = getattr(writers_schema, 'logical_type', None)

        # function dispatch for reading data based on type of writer's schema
        if writers_schema.type in ['union', 'error_union']:
            return self.read_union(writers_schema, readers_schema, decoder)

        if readers_schema.type in ['union', 'error_union']:
            # schema resolution: reader's schema is a union, writer's schema is not
            for s in readers_schema.schemas:
                if s.match(writers_schema):
                    return self.read_data(writers_schema, s, decoder)

            # This shouldn't happen because of the match check at the start of this method.
            fail_msg = 'Schemas do not match.'
            raise avro.errors.SchemaResolutionException(fail_msg, writers_schema, readers_schema)

        if writers_schema.type == 'null':
            return decoder.read_null()
        elif writers_schema.type == 'boolean':
            return decoder.read_boolean()
        elif writers_schema.type == 'string':
            return decoder.read_utf8()
        elif writers_schema.type == 'int':
            if logical_type == avro.constants.DATE:
                return decoder.read_date_from_int()
            if logical_type == avro.constants.TIME_MILLIS:
                return decoder.read_time_millis_from_int()
            return decoder.read_int()
        elif writers_schema.type == 'long':
            if logical_type == avro.constants.TIME_MICROS:
                return decoder.read_time_micros_from_long()
            elif logical_type == avro.constants.TIMESTAMP_MILLIS:
                return decoder.read_timestamp_millis_from_long()
            elif logical_type == avro.constants.TIMESTAMP_MICROS:
                return decoder.read_timestamp_micros_from_long()
            else:
                return decoder.read_long()
        elif writers_schema.type == 'float':
            return decoder.read_float()
        elif writers_schema.type == 'double':
            return decoder.read_double()
        elif writers_schema.type == 'bytes':
            if logical_type == 'decimal':
                return decoder.read_decimal_from_bytes(
                    writers_schema.get_prop('precision'),
                    writers_schema.get_prop('scale')
                )
            else:
                return decoder.read_bytes()
        elif writers_schema.type == 'fixed':
            if logical_type == 'decimal':
                return decoder.read_decimal_from_fixed(
                    writers_schema.get_prop('precision'),
                    writers_schema.get_prop('scale'),
                    writers_schema.size
                )
            return self.read_fixed(writers_schema, readers_schema, decoder)
        elif writers_schema.type == 'enum':
            return self.read_enum(writers_schema, readers_schema, decoder)
        elif writers_schema.type == 'array':
            return self.read_array(writers_schema, readers_schema, decoder)
        elif writers_schema.type == 'map':
            return self.read_map(writers_schema, readers_schema, decoder)
        elif writers_schema.type in ['record', 'error', 'request']:
            return self.read_record(writers_schema, readers_schema, decoder)
        else:
            fail_msg = "Cannot read unknown schema type: %s" % writers_schema.type
            raise avro.errors.AvroException(fail_msg)

    def skip_data(self, writers_schema, decoder):
        if writers_schema.type == 'null':
            return decoder.skip_null()
        elif writers_schema.type == 'boolean':
            return decoder.skip_boolean()
        elif writers_schema.type == 'string':
            return decoder.skip_utf8()
        elif writers_schema.type == 'int':
            return decoder.skip_int()
        elif writers_schema.type == 'long':
            return decoder.skip_long()
        elif writers_schema.type == 'float':
            return decoder.skip_float()
        elif writers_schema.type == 'double':
            return decoder.skip_double()
        elif writers_schema.type == 'bytes':
            return decoder.skip_bytes()
        elif writers_schema.type == 'fixed':
            return self.skip_fixed(writers_schema, decoder)
        elif writers_schema.type == 'enum':
            return self.skip_enum(writers_schema, decoder)
        elif writers_schema.type == 'array':
            return self.skip_array(writers_schema, decoder)
        elif writers_schema.type == 'map':
            return self.skip_map(writers_schema, decoder)
        elif writers_schema.type in ['union', 'error_union']:
            return self.skip_union(writers_schema, decoder)
        elif writers_schema.type in ['record', 'error', 'request']:
            return self.skip_record(writers_schema, decoder)
        else:
            fail_msg = "Unknown schema type: %s" % writers_schema.type
            raise avro.errors.AvroException(fail_msg)

    def read_fixed(self, writers_schema, readers_schema, decoder):
        """
        Fixed instances are encoded using the number of bytes declared
        in the schema.
        """
        return decoder.read(writers_schema.size)

    def skip_fixed(self, writers_schema, decoder):
        return decoder.skip(writers_schema.size)

    def read_enum(self, writers_schema, readers_schema, decoder):
        """
        An enum is encoded by a int, representing the zero-based position
        of the symbol in the schema.
        """
        # read data
        index_of_symbol = decoder.read_int()
        if index_of_symbol >= len(writers_schema.symbols):
            fail_msg = "Can't access enum index %d for enum with %d symbols"\
                       % (index_of_symbol, len(writers_schema.symbols))
            raise avro.errors.SchemaResolutionException(fail_msg, writers_schema, readers_schema)
        read_symbol = writers_schema.symbols[index_of_symbol]

        # schema resolution
        if read_symbol not in readers_schema.symbols:
            fail_msg = "Symbol %s not present in Reader's Schema" % read_symbol
            raise avro.errors.SchemaResolutionException(fail_msg, writers_schema, readers_schema)

        return read_symbol

    def skip_enum(self, writers_schema, decoder):
        return decoder.skip_int()

    def read_array(self, writers_schema, readers_schema, decoder):
        """
        Arrays are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many array items.
        A block with count zero indicates the end of the array.
        Each item is encoded per the array's item schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        read_items = []
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                block_size = decoder.read_long()
            for i in range(block_count):
                read_items.append(self.read_data(writers_schema.items,
                                                 readers_schema.items, decoder))
            block_count = decoder.read_long()
        return read_items

    def skip_array(self, writers_schema, decoder):
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_size = decoder.read_long()
                decoder.skip(block_size)
            else:
                for i in range(block_count):
                    self.skip_data(writers_schema.items, decoder)
            block_count = decoder.read_long()

    def read_map(self, writers_schema, readers_schema, decoder):
        """
        Maps are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many key/value pairs.
        A block with count zero indicates the end of the map.
        Each item is encoded per the map's value schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        read_items = {}
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_count = -block_count
                block_size = decoder.read_long()
            for i in range(block_count):
                key = decoder.read_utf8()
                read_items[key] = self.read_data(writers_schema.values,
                                                 readers_schema.values, decoder)
            block_count = decoder.read_long()
        return read_items

    def skip_map(self, writers_schema, decoder):
        block_count = decoder.read_long()
        while block_count != 0:
            if block_count < 0:
                block_size = decoder.read_long()
                decoder.skip(block_size)
            else:
                for i in range(block_count):
                    decoder.skip_utf8()
                    self.skip_data(writers_schema.values, decoder)
            block_count = decoder.read_long()

    def read_union(self, writers_schema, readers_schema, decoder):
        """
        A union is encoded by first writing an int value indicating
        the zero-based position within the union of the schema of its value.
        The value is then encoded per the indicated schema within the union.
        """
        # schema resolution
        index_of_schema = int(decoder.read_long())
        if index_of_schema >= len(writers_schema.schemas):
            fail_msg = "Can't access branch index %d for union with %d branches"\
                       % (index_of_schema, len(writers_schema.schemas))
            raise avro.errors.SchemaResolutionException(fail_msg, writers_schema, readers_schema)
        selected_writers_schema = writers_schema.schemas[index_of_schema]

        # read data
        return self.read_data(selected_writers_schema, readers_schema, decoder)

    def skip_union(self, writers_schema, decoder):
        index_of_schema = int(decoder.read_long())
        if index_of_schema >= len(writers_schema.schemas):
            fail_msg = "Can't access branch index %d for union with %d branches"\
                       % (index_of_schema, len(writers_schema.schemas))
            raise avro.errors.SchemaResolutionException(fail_msg, writers_schema)
        return self.skip_data(writers_schema.schemas[index_of_schema], decoder)

    def read_record(self, writers_schema, readers_schema, decoder):
        """
        A record is encoded by encoding the values of its fields
        in the order that they are declared. In other words, a record
        is encoded as just the concatenation of the encodings of its fields.
        Field values are encoded per their schema.

        Schema Resolution:
         * the ordering of fields may be different: fields are matched by name.
         * schemas for fields with the same name in both records are resolved
           recursively.
         * if the writer's record contains a field with a name not present in the
           reader's record, the writer's value for that field is ignored.
         * if the reader's record schema has a field that contains a default value,
           and writer's schema does not have a field with the same name, then the
           reader should use the default value from its field.
         * if the reader's record schema has a field with no default value, and
           writer's schema does not have a field with the same name, then the
           field's value is unset.
        """
        # schema resolution
        readers_fields_dict = readers_schema.fields_dict
        read_record = {}
        for field in writers_schema.fields:
            readers_field = readers_fields_dict.get(field.name)
            if readers_field is not None:
                field_val = self.read_data(field.type, readers_field.type, decoder)
                read_record[field.name] = field_val
            else:
                self.skip_data(field.type, decoder)

        # fill in default values
        if len(readers_fields_dict) > len(read_record):
            writers_fields_dict = writers_schema.fields_dict
            for field_name, field in readers_fields_dict.items():
                if field_name not in writers_fields_dict:
                    if field.has_default:
                        field_val = self._read_default_value(field.type, field.default)
                        read_record[field.name] = field_val
                    else:
                        fail_msg = 'No default value for field %s' % field_name
                        raise avro.errors.SchemaResolutionException(fail_msg, writers_schema,
                                                                    readers_schema)
        return read_record

    def skip_record(self, writers_schema, decoder):
        for field in writers_schema.fields:
            self.skip_data(field.type, decoder)

    def _read_default_value(self, field_schema, default_value):
        """
        Basically a JSON Decoder?
        """
        if field_schema.type == 'null':
            return None
        elif field_schema.type == 'boolean':
            return bool(default_value)
        elif field_schema.type == 'int':
            return int(default_value)
        elif field_schema.type == 'long':
            return int(default_value)
        elif field_schema.type in ['float', 'double']:
            return float(default_value)
        elif field_schema.type in ['enum', 'fixed', 'string', 'bytes']:
            return default_value
        elif field_schema.type == 'array':
            read_array = []
            for json_val in default_value:
                item_val = self._read_default_value(field_schema.items, json_val)
                read_array.append(item_val)
            return read_array
        elif field_schema.type == 'map':
            read_map = {}
            for key, json_val in default_value.items():
                map_val = self._read_default_value(field_schema.values, json_val)
                read_map[key] = map_val
            return read_map
        elif field_schema.type in ['union', 'error_union']:
            return self._read_default_value(field_schema.schemas[0], default_value)
        elif field_schema.type == 'record':
            read_record = {}
            for field in field_schema.fields:
                json_val = default_value.get(field.name)
                if json_val is None:
                    json_val = field.default
                field_val = self._read_default_value(field.type, json_val)
                read_record[field.name] = field_val
            return read_record
        else:
            fail_msg = 'Unknown type: %s' % field_schema.type
            raise avro.errors.AvroException(fail_msg)


class DatumWriter:
    """DatumWriter for generic python objects."""

    def __init__(self, writers_schema=None):
        self._writers_schema = writers_schema

    # read/write properties
    def set_writers_schema(self, writers_schema):
        self._writers_schema = writers_schema
    writers_schema = property(lambda self: self._writers_schema,
                              set_writers_schema)

    def write(self, datum, encoder):
        validate(self.writers_schema, datum, raise_on_error=True)
        self.write_data(self.writers_schema, datum, encoder)

    def write_data(self, writers_schema, datum, encoder):
        # function dispatch to write datum
        logical_type = getattr(writers_schema, 'logical_type', None)
        if writers_schema.type == 'null':
            encoder.write_null(datum)
        elif writers_schema.type == 'boolean':
            encoder.write_boolean(datum)
        elif writers_schema.type == 'string':
            encoder.write_utf8(datum)
        elif writers_schema.type == 'int':
            if logical_type == avro.constants.DATE:
                encoder.write_date_int(datum)
            elif logical_type == avro.constants.TIME_MILLIS:
                encoder.write_time_millis_int(datum)
            else:
                encoder.write_int(datum)
        elif writers_schema.type == 'long':
            if logical_type == avro.constants.TIME_MICROS:
                encoder.write_time_micros_long(datum)
            elif logical_type == avro.constants.TIMESTAMP_MILLIS:
                encoder.write_timestamp_millis_long(datum)
            elif logical_type == avro.constants.TIMESTAMP_MICROS:
                encoder.write_timestamp_micros_long(datum)
            else:
                encoder.write_long(datum)
        elif writers_schema.type == 'float':
            encoder.write_float(datum)
        elif writers_schema.type == 'double':
            encoder.write_double(datum)
        elif writers_schema.type == 'bytes':
            if logical_type == 'decimal':
                encoder.write_decimal_bytes(datum, writers_schema.get_prop('scale'))
            else:
                encoder.write_bytes(datum)
        elif writers_schema.type == 'fixed':
            if logical_type == 'decimal':
                encoder.write_decimal_fixed(
                    datum,
                    writers_schema.get_prop('scale'),
                    writers_schema.get_prop('size')
                )
            else:
                self.write_fixed(writers_schema, datum, encoder)
        elif writers_schema.type == 'enum':
            self.write_enum(writers_schema, datum, encoder)
        elif writers_schema.type == 'array':
            self.write_array(writers_schema, datum, encoder)
        elif writers_schema.type == 'map':
            self.write_map(writers_schema, datum, encoder)
        elif writers_schema.type in ['union', 'error_union']:
            self.write_union(writers_schema, datum, encoder)
        elif writers_schema.type in ['record', 'error', 'request']:
            self.write_record(writers_schema, datum, encoder)
        else:
            fail_msg = 'Unknown type: %s' % writers_schema.type
            raise avro.errors.AvroException(fail_msg)

    def write_fixed(self, writers_schema, datum, encoder):
        """
        Fixed instances are encoded using the number of bytes declared
        in the schema.
        """
        encoder.write(datum)

    def write_enum(self, writers_schema, datum, encoder):
        """
        An enum is encoded by a int, representing the zero-based position
        of the symbol in the schema.
        """
        index_of_datum = writers_schema.symbols.index(datum)
        encoder.write_int(index_of_datum)

    def write_array(self, writers_schema, datum, encoder):
        """
        Arrays are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many array items.
        A block with count zero indicates the end of the array.
        Each item is encoded per the array's item schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        if len(datum) > 0:
            encoder.write_long(len(datum))
            for item in datum:
                self.write_data(writers_schema.items, item, encoder)
        encoder.write_long(0)

    def write_map(self, writers_schema, datum, encoder):
        """
        Maps are encoded as a series of blocks.

        Each block consists of a long count value,
        followed by that many key/value pairs.
        A block with count zero indicates the end of the map.
        Each item is encoded per the map's value schema.

        If a block's count is negative,
        then the count is followed immediately by a long block size,
        indicating the number of bytes in the block.
        The actual count in this case
        is the absolute value of the count written.
        """
        if len(datum) > 0:
            encoder.write_long(len(datum))
            for key, val in datum.items():
                encoder.write_utf8(key)
                self.write_data(writers_schema.values, val, encoder)
        encoder.write_long(0)

    def write_union(self, writers_schema, datum, encoder):
        """
        A union is encoded by first writing an int value indicating
        the zero-based position within the union of the schema of its value.
        The value is then encoded per the indicated schema within the union.
        """
        # resolve union
        index_of_schema = -1
        for i, candidate_schema in enumerate(writers_schema.schemas):
            if validate(candidate_schema, datum):
                index_of_schema = i
        if index_of_schema < 0:
            raise avro.errors.AvroTypeException(writers_schema, datum)

        # write data
        encoder.write_long(index_of_schema)
        self.write_data(writers_schema.schemas[index_of_schema], datum, encoder)

    def write_record(self, writers_schema, datum, encoder):
        """
        A record is encoded by encoding the values of its fields
        in the order that they are declared. In other words, a record
        is encoded as just the concatenation of the encodings of its fields.
        Field values are encoded per their schema.
        """
        for field in writers_schema.fields:
            self.write_data(field.type, datum.get(field.name), encoder)
