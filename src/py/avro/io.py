#Licensed to the Apache Software Foundation (ASF) under one
#or more contributor license agreements.  See the NOTICE file
#distributed with this work for additional information
#regarding copyright ownership.  The ASF licenses this file
#to you under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance
#with the License.  You may obtain a copy of the License at
#
#http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

"""Input/Output utilities."""

import struct
import avro.schema as schema

#Constants
_INT_MIN_VALUE = -(1 << 31)
_INT_MAX_VALUE = (1 << 31) - 1
_LONG_MIN_VALUE = -(1 << 63)
_LONG_MAX_VALUE = (1 << 63) - 1
_STRUCT_INT = struct.Struct('!I')
_STRUCT_LONG = struct.Struct('!Q')
_STRUCT_FLOAT = struct.Struct('!f')
_STRUCT_DOUBLE = struct.Struct('!d')

class AvroTypeException(schema.AvroException):
  """Raised when illegal type is used."""
  def __init__(self, schm=None, datum=None, msg=None):
    if msg is None:
      msg = "Not a "+schema.stringval(schm)+": "+datum.__str__()
    schema.AvroException.__init__(self, msg)

class DatumReaderBase(object):
  """Base class for reading data of a schema."""

  def setschema(self, schema):
    pass

  def read(self, decoder):
    """Read a datum. Traverse the schema, depth-first, reading all leaf values
    in the schema into a datum that is returned"""
    pass

class DatumWriterBase(object):
  """Base class for writing data of a schema."""
  def setschema(self, schema):
    pass

  def write(self, data, encoder):
    """Write a datum. Traverse the schema, depth first, writing each leaf value
    in the schema from the datum to the output."""
    pass


class Decoder(object):
  """Read leaf values."""

  def __init__(self, reader):
    self.__reader = reader

  def readboolean(self):
    return ord(self.__reader.read(1)) == 1

  def readint(self):
    return self.readlong()

  def readlong(self):
    b = ord(self.__reader.read(1))
    n = b & 0x7F
    shift = 7
    while (b & 0x80) != 0:
      b = ord(self.__reader.read(1))
      n |= (b & 0x7F) << shift
      shift += 7
    datum = (n >> 1) ^ -(n & 1)
    return datum

  def readfloat(self):
    bits = (((ord(self.__reader.read(1)) & 0xffL)    ) |
        ((ord(self.__reader.read(1)) & 0xffL) <<  8) |
        ((ord(self.__reader.read(1)) & 0xffL) << 16) |
        ((ord(self.__reader.read(1)) & 0xffL) << 24))
    return _STRUCT_FLOAT.unpack(_STRUCT_INT.pack(bits))[0]

  def readdouble(self):
    bits = (((ord(self.__reader.read(1)) & 0xffL)    ) |
        ((ord(self.__reader.read(1)) & 0xffL) <<  8) |
        ((ord(self.__reader.read(1)) & 0xffL) << 16) |
        ((ord(self.__reader.read(1)) & 0xffL) << 24) |
        ((ord(self.__reader.read(1)) & 0xffL) << 32) |
        ((ord(self.__reader.read(1)) & 0xffL) << 40) |
        ((ord(self.__reader.read(1)) & 0xffL) << 48) |
        ((ord(self.__reader.read(1)) & 0xffL) << 56))
    return _STRUCT_DOUBLE.unpack(_STRUCT_LONG.pack(bits))[0]

  def readbytes(self):
    return self.read(self.readlong())

  def readutf8(self):
    return unicode(self.readbytes(), "utf-8")

  def read(self, len):
    return struct.unpack(len.__str__()+'s', self.__reader.read(len))[0]

  def skipboolean(self):
    self.skip(1)

  def skipint(self):
    self.skip(4)

  def skiplong(self):
    self.skip(8)

  def skipfloat(self):
    self.skip(4)

  def skipdouble(self):
    self.skip(8)

  def skipbytes(self):
    self.skip(self.readlong())

  def skiputf8(self):
    self.skipbytes()

  def skip(self, len):
    self.__reader.seek(self.__reader.tell()+len)

class Encoder(object):
  """Write leaf values."""

  def __init__(self, writer):
    self.__writer = writer
  
  def writeboolean(self, datum):
    if not isinstance(datum, bool):
      raise AvroTypeException(schema.BOOLEAN, datum)
    if datum:
      self.__writer.write(chr(1))
    else:
      self.__writer.write(chr(0))

  def writeint(self, n):
    if n < _INT_MIN_VALUE or n > _INT_MAX_VALUE:
      raise AvroTypeException(schema.INT, n, 
                              "datam too big to fit into avro int")
    self.writelong(n);

  def writelong(self, n):
    if n < _LONG_MIN_VALUE or n > _LONG_MAX_VALUE:
      raise AvroTypeException(schema.LONG, n, 
                              "datam too big to fit into avro long")
    n = (n << 1) ^ (n >> 63)
    while (n & ~0x7F) != 0:
      self.__writer.write(chr((n & 0x7f) | 0x80))
      n >>=7
    self.__writer.write(chr(n))

  def writefloat(self, datum):
    bits = _STRUCT_INT.unpack(_STRUCT_FLOAT.pack(datum))[0]
    self.__writer.write(chr((bits    ) & 0xFF))
    self.__writer.write(chr((bits >>  8) & 0xFF))
    self.__writer.write(chr((bits >>  16) & 0xFF))
    self.__writer.write(chr((bits >>  24) & 0xFF))

  def writedouble(self, datum):
    bits = _STRUCT_LONG.unpack(_STRUCT_DOUBLE.pack(datum))[0]
    self.__writer.write(chr((bits    ) & 0xFF))
    self.__writer.write(chr((bits >>  8) & 0xFF))
    self.__writer.write(chr((bits >>  16) & 0xFF))
    self.__writer.write(chr((bits >>  24) & 0xFF))
    self.__writer.write(chr((bits >>  32) & 0xFF))
    self.__writer.write(chr((bits >>  40) & 0xFF))
    self.__writer.write(chr((bits >>  48) & 0xFF))
    self.__writer.write(chr((bits >>  56) & 0xFF))

  def writebytes(self, datum):
    if not isinstance(datum, str):
      raise AvroTypeException(schema.BYTES, datum, 
                              "avro BYTES should be python str")
    self.writelong(len(datum))
    self.__writer.write(struct.pack(len(datum).__str__()+'s',datum))

  def writeutf8(self, datum):
    if not isinstance(datum, basestring):
      raise AvroTypeException(schema.STRING, datum, 
                              "avro STRING should be python unicode")
    datum = datum.encode("utf-8")
    self.writebytes(datum)

  def write(self, datum):
    self.__writer.write(datum)

