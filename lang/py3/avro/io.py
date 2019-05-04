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

"""Input/output utilities.

Includes:
 - i/o-specific constants
 - i/o-specific exceptions
 - schema validation
 - leaf value encoding and decoding
 - datum reader/writer stuff (?)

Also includes a generic representation for data, which uses the
following mapping:
 - Schema records are implemented as dict.
 - Schema arrays are implemented as list.
 - Schema maps are implemented as dict.
 - Schema strings are implemented as unicode.
 - Schema bytes are implemented as str.
 - Schema ints are implemented as int.
 - Schema longs are implemented as long.
 - Schema floats are implemented as float.
 - Schema doubles are implemented as float.
 - Schema booleans are implemented as bool.
"""

import binascii
import json
import logging
import struct
import sys
from typing import BinaryIO, Dict, List, Optional, Union

from avro import schema

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Constants

AvroTypes = Union[None, bool, int, float, bytes, str, List, Dict]

INT_MIN_VALUE = -(1 << 31)
INT_MAX_VALUE = (1 << 31) - 1
LONG_MIN_VALUE = -(1 << 63)
LONG_MAX_VALUE = (1 << 63) - 1

STRUCT_INT = struct.Struct('!I')     # big-endian unsigned int
STRUCT_FLOAT = struct.Struct('<f')   # little-endian float
STRUCT_DOUBLE = struct.Struct('<d')  # little-endian double
STRUCT_CRC32 = struct.Struct('>I')   # big-endian unsigned int


# ------------------------------------------------------------------------------
# Exceptions


class AvroTypeException(schema.AvroException):
  """Raised when datum is not an example of schema."""
  def __init__(self, expected_schema: schema.Schema, datum: bytes) -> None:
    pretty_expected = json.dumps(json.loads(str(expected_schema)), indent=2)
    fail_msg = "The datum %s is not an example of the schema %s"\
               % (datum, pretty_expected)
    super().__init__(fail_msg)


class SchemaResolutionException(schema.AvroException):
  def __init__(self, fail_msg: str, writer_schema: Optional[schema.Schema]=None, reader_schema: Optional[schema.Schema]=None) -> None:
    pretty_writers = json.dumps(json.loads(str(writer_schema)), indent=2)
    pretty_readers = json.dumps(json.loads(str(reader_schema)), indent=2)
    if writer_schema: fail_msg += "\nWriter's Schema: %s" % pretty_writers
    if reader_schema: fail_msg += "\nReader's Schema: %s" % pretty_readers
    schema.AvroException.__init__(self, fail_msg)


# ------------------------------------------------------------------------------
# Validate

_valid = {
  'null': lambda s, d: d is None,
  'boolean': lambda s, d: isinstance(d, bool),
  'string': lambda s, d: isinstance(d, str),
  'bytes': lambda s, d: isinstance(d, bytes),
  'int': lambda s, d: isinstance(d, int) and (INT_MIN_VALUE <= d <= INT_MAX_VALUE),
  'long': lambda s, d: isinstance(d, int) and (LONG_MIN_VALUE <= d <= LONG_MAX_VALUE),
  'float': lambda s, d: isinstance(d, (int, float)),
  'fixed': lambda s, d: isinstance(d, bytes) and len(d) == s.size,
  'enum': lambda s, d: d in s.symbols,
  'array': lambda s, d: isinstance(d, list) and all(Validate(s.items, item) for item in d),
  'map': lambda s, d: (isinstance(d, dict) and all(isinstance(key, str) for key in d)
                       and all(Validate(s.values, value) for value in d.values())),
  'union': lambda s, d: any(Validate(branch, d) for branch in s.schemas),
  'record': lambda s, d: (isinstance(d, dict)
                          and all(Validate(f.type, d.get(f.name)) for f in s.fields)
                          and {f.name for f in s.fields}.issuperset(d.keys()))
}
_valid['double'] = _valid['float']
_valid['error_union'] = _valid['union']
_valid['error'] = _valid['request'] = _valid['record']

def Validate(expected_schema: schema.Schema, datum: bytes) -> bool:
  """Determines if a python datum is an instance of a schema.

  Args:
    expected_schema: Schema to validate against.
    datum: Datum to validate.
  Returns:
    True if the datum is an instance of the schema.
  """
  try:
    return _valid[expected_schema.type](expected_schema, datum)
  except KeyError:
    raise AvroTypeException(expected_schema, datum)


# ------------------------------------------------------------------------------
# Decoder/Encoder


class BinaryDecoder(object):
  """Read leaf values."""
  def __init__(self, reader: BinaryIO) -> None:
    """
    reader is a Python object on which we can call read, seek, and tell.
    """
    self._reader = reader

  @property
  def reader(self) -> BinaryIO:
    """Reports the reader used by this decoder."""
    return self._reader

  def read(self, n: int) -> bytes:
    """Read n bytes.

    Args:
      n: Number of bytes to read.
    Returns:
      The next n bytes from the input.
    """
    assert (n >= 0), n
    input_bytes = self.reader.read(n)
    assert (len(input_bytes) == n), input_bytes
    return input_bytes

  def read_null(self) -> None:
    """
    null is written as zero bytes
    """
    return None

  def read_boolean(self) -> bool:
    """
    a boolean is written as a single byte
    whose value is either 0 (false) or 1 (true).
    """
    return ord(self.read(1)) == 1

  def read_int(self) -> int:
    """
    int and long values are written using variable-length, zig-zag coding.
    """
    return self.read_long()

  def read_long(self) -> int:
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

  def read_float(self) -> float:
    """
    A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    return STRUCT_FLOAT.unpack(self.read(4))[0]

  def read_double(self) -> float:
    """
    A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    return STRUCT_DOUBLE.unpack(self.read(8))[0]

  def read_bytes(self) -> bytes:
    """
    Bytes are encoded as a long followed by that many bytes of data.
    """
    nbytes = self.read_long()
    assert (nbytes >= 0), nbytes
    return self.read(nbytes)

  def read_utf8(self) -> str:
    """
    A string is encoded as a long followed by
    that many bytes of UTF-8 encoded character data.
    """
    input_bytes = self.read_bytes()
    try:
      return input_bytes.decode('utf-8')
    except UnicodeDecodeError as exn:
      logger.error('Invalid UTF-8 input bytes: %r', input_bytes)
      raise exn

  def check_crc32(self, bytes: bytes) -> None:
    checksum = STRUCT_CRC32.unpack(self.read(4))[0]
    if binascii.crc32(bytes) & 0xffffffff != checksum:
      raise schema.AvroException("Checksum failure")

  def skip_null(self) -> None:
    pass

  def skip_boolean(self) -> None:
    self.skip(1)

  def skip_int(self) -> None:
    self.skip_long()

  def skip_long(self) -> None:
    b = ord(self.read(1))
    while (b & 0x80) != 0:
      b = ord(self.read(1))

  def skip_float(self) -> None:
    self.skip(4)

  def skip_double(self) -> None:
    self.skip(8)

  def skip_bytes(self) -> None:
    self.skip(self.read_long())

  def skip_utf8(self) -> None:
    self.skip_bytes()

  def skip(self, n: int) -> None:
    self.reader.seek(self.reader.tell() + n)


# ------------------------------------------------------------------------------


class BinaryEncoder(object):
  """Write leaf values."""

  def __init__(self, writer: BinaryIO) -> None:
    """
    writer is a Python object on which we can call write.
    """
    self._writer = writer

  @property
  def writer(self) -> BinaryIO:
    """Reports the writer used by this encoder."""
    return self._writer


  def write(self, datum: bytes) -> None:
    """Write a sequence of bytes.

    Args:
      datum: Byte array, as a Python bytes.
    """
    assert isinstance(datum, bytes), ('Expecting bytes, got %r' % datum)
    self.writer.write(datum)

  def WriteByte(self, byte: int) -> None:
    self.writer.write(bytes((byte,)))

  def write_null(self, datum: bytes) -> None:
    """
    null is written as zero bytes
    """
    pass

  def write_boolean(self, datum: bytes) -> None:
    """
    a boolean is written as a single byte
    whose value is either 0 (false) or 1 (true).
    """
    # Python maps True to 1 and False to 0.
    self.WriteByte(int(bool(datum)))

  def write_int(self, datum: int) -> None:
    """
    int and long values are written using variable-length, zig-zag coding.
    """
    self.write_long(datum)

  def write_long(self, datum: int) -> None:
    """
    int and long values are written using variable-length, zig-zag coding.
    """
    datum = (datum << 1) ^ (datum >> 63)
    while (datum & ~0x7F) != 0:
      self.WriteByte((datum & 0x7f) | 0x80)
      datum >>= 7
    self.WriteByte(datum)

  def write_float(self, datum: float) -> None:
    """
    A float is written as 4 bytes.
    The float is converted into a 32-bit integer using a method equivalent to
    Java's floatToIntBits and then encoded in little-endian format.
    """
    self.write(STRUCT_FLOAT.pack(datum))

  def write_double(self, datum: float) -> None:
    """
    A double is written as 8 bytes.
    The double is converted into a 64-bit integer using a method equivalent to
    Java's doubleToLongBits and then encoded in little-endian format.
    """
    self.write(STRUCT_DOUBLE.pack(datum))

  def write_bytes(self, datum: bytes) -> None:
    """
    Bytes are encoded as a long followed by that many bytes of data.
    """
    self.write_long(len(datum))
    self.write(datum)

  def write_utf8(self, datum: str) -> None:
    """
    A string is encoded as a long followed by
    that many bytes of UTF-8 encoded character data.
    """
    self.write_bytes(datum.encode("utf-8"))

  def write_crc32(self, bytes: bytes) -> None:
    """
    A 4-byte, big-endian CRC32 checksum
    """
    self.write(STRUCT_CRC32.pack(binascii.crc32(bytes) & 0xffffffff))


# ------------------------------------------------------------------------------
# DatumReader/Writer


class DatumReader(object):
  """Deserialize Avro-encoded data into a Python data structure."""
  @staticmethod
  def check_props(
      schema_one: schema.Schema,
      schema_two: schema.Schema,
      prop_list: List[str],
  ) -> bool:
    for prop in prop_list:
      if getattr(schema_one, prop) != getattr(schema_two, prop):
        return False
    return True

  @staticmethod
  def match_schemas(writer_schema: schema.Schema, reader_schema: schema.Schema) -> bool:
    w_type = writer_schema.type
    r_type = reader_schema.type
    if 'union' in [w_type, r_type] or 'error_union' in [w_type, r_type]:
      return True
    elif (w_type in schema.PRIMITIVE_TYPES and r_type in schema.PRIMITIVE_TYPES
          and w_type == r_type):
      return True
    elif (w_type == r_type == 'record' and
          DatumReader.check_props(writer_schema, reader_schema,
                                  ['fullname'])):
      return True
    elif (w_type == r_type == 'error' and
          DatumReader.check_props(writer_schema, reader_schema,
                                  ['fullname'])):
      return True
    elif (w_type == r_type == 'request'):
      return True
    elif (w_type == r_type == 'fixed' and
          DatumReader.check_props(writer_schema, reader_schema,
                                  ['fullname', 'size'])):
      return True
    elif (w_type == r_type == 'enum' and
          DatumReader.check_props(writer_schema, reader_schema,
                                  ['fullname'])):
      return True
    elif (w_type == r_type == 'map' and
          DatumReader.check_props(writer_schema.values,
                                  reader_schema.values, ['type'])):
      return True
    elif (w_type == r_type == 'array' and
          DatumReader.check_props(writer_schema.items,
                                  reader_schema.items, ['type'])):
      return True

    # Handle schema promotion
    if w_type == 'int' and r_type in ['long', 'float', 'double']:
      return True
    elif w_type == 'long' and r_type in ['float', 'double']:
      return True
    elif w_type == 'float' and r_type == 'double':
      return True
    return False

  def __init__(self, writer_schema: Optional[schema.Schema]=None, reader_schema: Optional[schema.Schema]=None) -> None:
    """
    As defined in the Avro specification, we call the schema encoded
    in the data the "writer's schema", and the schema expected by the
    reader the "reader's schema".
    """
    self._writer_schema = writer_schema
    self._reader_schema = reader_schema

  # read/write properties
  def set_writer_schema(self, writer_schema: schema.Schema) -> None:
    self._writer_schema = writer_schema
  writer_schema = property(lambda self: self._writer_schema,
                            set_writer_schema)
  def set_reader_schema(self, reader_schema: schema.Schema) -> None:
    self._reader_schema = reader_schema
  reader_schema = property(lambda self: self._reader_schema,
                            set_reader_schema)

  def read(self, decoder: BinaryDecoder) -> AvroTypes:
    if self.reader_schema is None:
      self.reader_schema = self.writer_schema
    return self.read_data(self.writer_schema, self.reader_schema, decoder)

  def read_data(self, writer_schema: schema.Schema, reader_schema: schema.Schema, decoder: BinaryDecoder) -> AvroTypes:
    # schema matching
    if not DatumReader.match_schemas(writer_schema, reader_schema):
      fail_msg = 'Schemas do not match.'
      raise SchemaResolutionException(fail_msg, writer_schema, reader_schema)

    # schema resolution: reader's schema is a union, writer's schema is not
    if (writer_schema.type not in ['union', 'error_union']
        and reader_schema.type in ['union', 'error_union']):
      for s in reader_schema.schemas:
        if DatumReader.match_schemas(writer_schema, s):
          return self.read_data(writer_schema, s, decoder)
      fail_msg = 'Schemas do not match.'
      raise SchemaResolutionException(fail_msg, writer_schema, reader_schema)

    # function dispatch for reading data based on type of writer's schema
    if writer_schema.type == 'null':
      return decoder.read_null()
    elif writer_schema.type == 'boolean':
      return decoder.read_boolean()
    elif writer_schema.type == 'string':
      return decoder.read_utf8()
    elif writer_schema.type == 'int':
      return decoder.read_int()
    elif writer_schema.type == 'long':
      return decoder.read_long()
    elif writer_schema.type == 'float':
      return decoder.read_float()
    elif writer_schema.type == 'double':
      return decoder.read_double()
    elif writer_schema.type == 'bytes':
      return decoder.read_bytes()
    elif writer_schema.type == 'fixed':
      return self.read_fixed(writer_schema, reader_schema, decoder)
    elif writer_schema.type == 'enum':
      return self.read_enum(writer_schema, reader_schema, decoder)
    elif writer_schema.type == 'array':
      return self.read_array(writer_schema, reader_schema, decoder)
    elif writer_schema.type == 'map':
      return self.read_map(writer_schema, reader_schema, decoder)
    elif writer_schema.type in ['union', 'error_union']:
      return self.read_union(writer_schema, reader_schema, decoder)
    elif writer_schema.type in ['record', 'error', 'request']:
      return self.read_record(writer_schema, reader_schema, decoder)
    else:
      fail_msg = "Cannot read unknown schema type: %s" % writer_schema.type
      raise schema.AvroException(fail_msg)

  def skip_data(self, writer_schema: schema.Schema, decoder: BinaryDecoder) -> None:
    if writer_schema.type == 'null':
      return decoder.skip_null()
    elif writer_schema.type == 'boolean':
      return decoder.skip_boolean()
    elif writer_schema.type == 'string':
      return decoder.skip_utf8()
    elif writer_schema.type == 'int':
      return decoder.skip_int()
    elif writer_schema.type == 'long':
      return decoder.skip_long()
    elif writer_schema.type == 'float':
      return decoder.skip_float()
    elif writer_schema.type == 'double':
      return decoder.skip_double()
    elif writer_schema.type == 'bytes':
      return decoder.skip_bytes()
    elif writer_schema.type == 'fixed':
      return self.skip_fixed(writer_schema, decoder)
    elif writer_schema.type == 'enum':
      return self.skip_enum(writer_schema, decoder)
    elif writer_schema.type == 'array':
      return self.skip_array(writer_schema, decoder)
    elif writer_schema.type == 'map':
      return self.skip_map(writer_schema, decoder)
    elif writer_schema.type in ['union', 'error_union']:
      return self.skip_union(writer_schema, decoder)
    elif writer_schema.type in ['record', 'error', 'request']:
      return self.skip_record(writer_schema, decoder)
    else:
      fail_msg = "Unknown schema type: %s" % writer_schema.type
      raise schema.AvroException(fail_msg)

  def read_fixed(self, writer_schema: schema.Schema, reader_schema: schema.Schema, decoder: BinaryDecoder) -> str:
    """
    Fixed instances are encoded using the number of bytes declared
    in the schema.
    """
    return decoder.read(writer_schema.size)

  def skip_fixed(self, writer_schema: schema.Schema, decoder: BinaryDecoder) -> None:
    return decoder.skip(writer_schema.size)

  def read_enum(self, writer_schema: schema.Schema, reader_schema: schema.Schema, decoder: BinaryDecoder) -> str:
    """
    An enum is encoded by a int, representing the zero-based position
    of the symbol in the schema.
    """
    # read data
    index_of_symbol = decoder.read_int()
    if index_of_symbol >= len(writer_schema.symbols):
      fail_msg = "Can't access enum index %d for enum with %d symbols"\
                 % (index_of_symbol, len(writer_schema.symbols))
      raise SchemaResolutionException(fail_msg, writer_schema, reader_schema)
    read_symbol = writer_schema.symbols[index_of_symbol]

    # schema resolution
    if read_symbol not in reader_schema.symbols:
      fail_msg = "Symbol %s not present in Reader's Schema" % read_symbol
      raise SchemaResolutionException(fail_msg, writer_schema, reader_schema)

    return read_symbol

  def skip_enum(self, writer_schema: schema.Schema, decoder: BinaryDecoder) -> None:
    return decoder.skip_int()

  def read_array(self, writer_schema: schema.Schema, reader_schema: schema.Schema, decoder: BinaryDecoder) -> List:
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
        read_items.append(self.read_data(writer_schema.items,
                                         reader_schema.items, decoder))
      block_count = decoder.read_long()
    return read_items

  def skip_array(self, writer_schema: schema.Schema, decoder: BinaryDecoder) -> None:
    block_count = decoder.read_long()
    while block_count != 0:
      if block_count < 0:
        block_size = decoder.read_long()
        decoder.skip(block_size)
      else:
        for i in range(block_count):
          self.skip_data(writer_schema.items, decoder)
      block_count = decoder.read_long()

  def read_map(self, writer_schema: schema.Schema, reader_schema: schema.Schema, decoder: BinaryDecoder) -> Dict:
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
        read_items[key] = self.read_data(writer_schema.values,
                                         reader_schema.values, decoder)
      block_count = decoder.read_long()
    return read_items

  def skip_map(self, writer_schema: schema.Schema, decoder: BinaryDecoder) -> None:
    block_count = decoder.read_long()
    while block_count != 0:
      if block_count < 0:
        block_size = decoder.read_long()
        decoder.skip(block_size)
      else:
        for i in range(block_count):
          decoder.skip_utf8()
          self.skip_data(writer_schema.values, decoder)
      block_count = decoder.read_long()

  def read_union(self, writer_schema: schema.Schema, reader_schema: schema.Schema, decoder: BinaryDecoder) -> AvroTypes:
    """
    A union is encoded by first writing a long value indicating
    the zero-based position within the union of the schema of its value.
    The value is then encoded per the indicated schema within the union.
    """
    # schema resolution
    index_of_schema = int(decoder.read_long())
    if index_of_schema >= len(writer_schema.schemas):
      fail_msg = "Can't access branch index %d for union with %d branches"\
                 % (index_of_schema, len(writer_schema.schemas))
      raise SchemaResolutionException(fail_msg, writer_schema, reader_schema)
    selected_writer_schema = writer_schema.schemas[index_of_schema]

    # read data
    return self.read_data(selected_writer_schema, reader_schema, decoder)

  def skip_union(self, writer_schema: schema.Schema, decoder: BinaryDecoder) -> None:
    index_of_schema = int(decoder.read_long())
    if index_of_schema >= len(writer_schema.schemas):
      fail_msg = "Can't access branch index %d for union with %d branches"\
                 % (index_of_schema, len(writer_schema.schemas))
      raise SchemaResolutionException(fail_msg, writer_schema)
    return self.skip_data(writer_schema.schemas[index_of_schema], decoder)

  def read_record(self, writer_schema: schema.Schema, reader_schema: schema.Schema, decoder: BinaryDecoder) -> AvroTypes:
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
    readers_fields_dict = reader_schema.field_map
    read_record = {}
    for field in writer_schema.fields:
      readers_field = readers_fields_dict.get(field.name)
      if readers_field is not None:
        field_val = self.read_data(field.type, readers_field.type, decoder)
        read_record[field.name] = field_val
      else:
        self.skip_data(field.type, decoder)

    # fill in default values
    if len(readers_fields_dict) > len(read_record):
      writers_fields_dict = writer_schema.field_map
      for field_name, field in readers_fields_dict.items():
        if field_name not in writers_fields_dict:
          if field.has_default:
            field_val = self._read_default_value(field.type, field.default)
            read_record[field.name] = field_val
          else:
            fail_msg = 'No default value for field %s' % field_name
            raise SchemaResolutionException(fail_msg, writer_schema,
                                            reader_schema)
    return read_record

  def skip_record(self, writer_schema: schema.Schema, decoder: BinaryDecoder) -> None:
    for field in writer_schema.fields:
      self.skip_data(field.type, decoder)

  def _read_default_value(self, field_schema: schema.Schema, default_value: AvroTypes) -> AvroTypes:
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
        if json_val is None: json_val = field.default
        field_val = self._read_default_value(field.type, json_val)
        read_record[field.name] = field_val
      return read_record
    else:
      fail_msg = 'Unknown type: %s' % field_schema.type
      raise schema.AvroException(fail_msg)


# ------------------------------------------------------------------------------


class DatumWriter(object):
  """DatumWriter for generic python objects."""
  def __init__(self, writer_schema: Optional[schema.Schema]=None) -> None:
    self._writer_schema = writer_schema

  # read/write properties
  def set_writer_schema(self, writer_schema: schema.Schema) -> None:
    self._writer_schema = writer_schema
  writer_schema = property(lambda self: self._writer_schema,
                            set_writer_schema)

  def write(self, datum: bytes, encoder: BinaryEncoder) -> None:
    # validate datum
    if not Validate(self.writer_schema, datum):
      raise AvroTypeException(self.writer_schema, datum)

    self.write_data(self.writer_schema, datum, encoder)

  def write_data(self, writer_schema, datum, encoder):
    # function dispatch to write datum
    if writer_schema.type == 'null':
      encoder.write_null(datum)
    elif writer_schema.type == 'boolean':
      encoder.write_boolean(datum)
    elif writer_schema.type == 'string':
      encoder.write_utf8(datum)
    elif writer_schema.type == 'int':
      encoder.write_int(datum)
    elif writer_schema.type == 'long':
      encoder.write_long(datum)
    elif writer_schema.type == 'float':
      encoder.write_float(datum)
    elif writer_schema.type == 'double':
      encoder.write_double(datum)
    elif writer_schema.type == 'bytes':
      encoder.write_bytes(datum)
    elif writer_schema.type == 'fixed':
      self.write_fixed(writer_schema, datum, encoder)
    elif writer_schema.type == 'enum':
      self.write_enum(writer_schema, datum, encoder)
    elif writer_schema.type == 'array':
      self.write_array(writer_schema, datum, encoder)
    elif writer_schema.type == 'map':
      self.write_map(writer_schema, datum, encoder)
    elif writer_schema.type in ['union', 'error_union']:
      self.write_union(writer_schema, datum, encoder)
    elif writer_schema.type in ['record', 'error', 'request']:
      self.write_record(writer_schema, datum, encoder)
    else:
      fail_msg = 'Unknown type: %s' % writer_schema.type
      raise schema.AvroException(fail_msg)

  def write_fixed(self, writer_schema: schema.Schema, datum: bytes, encoder: BinaryEncoder) -> None:
    """
    Fixed instances are encoded using the number of bytes declared
    in the schema.
    """
    encoder.write(datum)

  def write_enum(self, writer_schema: schema.Schema, datum: str, encoder: BinaryEncoder) -> None:
    """
    An enum is encoded by a int, representing the zero-based position
    of the symbol in the schema.
    """
    index_of_datum = writer_schema.symbols.index(datum)
    encoder.write_int(index_of_datum)

  def write_array(self, writer_schema: schema.Schema, datum: List, encoder: BinaryEncoder) -> None:
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
        self.write_data(writer_schema.items, item, encoder)
    encoder.write_long(0)

  def write_map(self, writer_schema: schema.Schema, datum: Dict, encoder: BinaryEncoder) -> None:
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
        self.write_data(writer_schema.values, val, encoder)
    encoder.write_long(0)

  def write_union(self, writer_schema: schema.Schema, datum: AvroTypes, encoder: BinaryEncoder) -> None:
    """
    A union is encoded by first writing a long value indicating
    the zero-based position within the union of the schema of its value.
    The value is then encoded per the indicated schema within the union.
    """
    # resolve union
    index_of_schema = -1
    for i, candidate_schema in enumerate(writer_schema.schemas):
      if Validate(candidate_schema, datum):
        index_of_schema = i
    if index_of_schema < 0: raise AvroTypeException(writer_schema, datum)

    # write data
    encoder.write_long(index_of_schema)
    self.write_data(writer_schema.schemas[index_of_schema], datum, encoder)

  def write_record(self, writer_schema: schema.Schema, datum: Dict, encoder: BinaryEncoder) -> None:
    """
    A record is encoded by encoding the values of its fields
    in the order that they are declared. In other words, a record
    is encoded as just the concatenation of the encodings of its fields.
    Field values are encoded per their schema.
    """
    for field in writer_schema.fields:
      self.write_data(field.type, datum.get(field.name), encoder)


if __name__ == '__main__':
  raise Exception('Not a standalone module')
