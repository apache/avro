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
Contains Codecs for Python Avro.

Note that the word "codecs" means "compression/decompression algorithms" in the
Avro world (https://avro.apache.org/docs/current/spec.html#Object+Container+Files),
so don't confuse it with the Python's "codecs", which is a package mainly for
converting charsets (https://docs.python.org/3/library/codecs.html).
"""

from __future__ import absolute_import, division, print_function

import io
import struct
import sys
import zlib
from abc import ABCMeta, abstractmethod
from binascii import crc32
from struct import Struct

import avro.io
from avro.schema import AvroException

#
# Constants
#
STRUCT_CRC32 = Struct('>I')  # big-endian unsigned int


try:
    import bz2
    has_bzip2 = True
except ImportError:
    has_bzip2 = False
try:
    import snappy
    has_snappy = True
except ImportError:
    has_snappy = False
try:
    import zstandard as zstd
    has_zstandard = True
except ImportError:
    has_zstandard = False


class Codec:
    """Abstract base class for all Avro codec classes."""
    __metaclass__ = ABCMeta

    @abstractmethod
    def compress(self, data):
        """Compress the passed data.

        :param data: a byte string to be compressed
        :type data: str

        :rtype: tuple
        :return: compressed data and its length
        """
        pass

    @abstractmethod
    def decompress(self, readers_decoder):
        """Read compressed data via the passed BinaryDecoder and decompress it.

        :param readers_decoder: a BinaryDecoder object currently being used for
                                reading an object container file
        :type readers_decoder: avro.io.BinaryDecoder

        :rtype: avro.io.BinaryDecoder
        :return: a newly instantiated BinaryDecoder object that contains the
                 decompressed data which is wrapped by a StringIO
        """
        pass


class NullCodec(Codec):
    def compress(self, data):
        return data, len(data)

    def decompress(self, readers_decoder):
        readers_decoder.skip_long()
        return readers_decoder


class DeflateCodec(Codec):
    def compress(self, data):
        # The first two characters and last character are zlib
        # wrappers around deflate data.
        compressed_data = zlib.compress(data)[2:-1]
        return compressed_data, len(compressed_data)

    def decompress(self, readers_decoder):
        # Compressed data is stored as (length, data), which
        # corresponds to how the "bytes" type is encoded.
        data = readers_decoder.read_bytes()
        # -15 is the log of the window size; negative indicates
        # "raw" (no zlib headers) decompression.  See zlib.h.
        uncompressed = zlib.decompress(data, -15)
        return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


if has_bzip2:
    class BZip2Codec(Codec):
        def compress(self, data):
            compressed_data = bz2.compress(data)
            return compressed_data, len(compressed_data)

        def decompress(self, readers_decoder):
            length = readers_decoder.read_long()
            data = readers_decoder.read(length)
            uncompressed = bz2.decompress(data)
            return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


if has_snappy:
    class SnappyCodec(Codec):
        def compress(self, data):
            compressed_data = snappy.compress(data)
            # A 4-byte, big-endian CRC32 checksum
            compressed_data += STRUCT_CRC32.pack(crc32(data) & 0xffffffff)
            return compressed_data, len(compressed_data)

        def decompress(self, readers_decoder):
            # Compressed data includes a 4-byte CRC32 checksum
            length = readers_decoder.read_long()
            data = readers_decoder.read(length - 4)
            uncompressed = snappy.decompress(data)
            checksum = readers_decoder.read(4)
            self.check_crc32(uncompressed, checksum)
            return avro.io.BinaryDecoder(io.BytesIO(uncompressed))

        def check_crc32(self, bytes, checksum):
            checksum = STRUCT_CRC32.unpack(checksum)[0]
            if crc32(bytes) & 0xffffffff != checksum:
                raise schema.AvroException("Checksum failure")


if has_zstandard:
    class ZstandardCodec(Codec):
        def compress(self, data):
            compressed_data = zstd.ZstdCompressor().compress(data)
            return compressed_data, len(compressed_data)

        def decompress(self, readers_decoder):
            length = readers_decoder.read_long()
            data = readers_decoder.read(length)
            uncompressed = bytearray()
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(io.BytesIO(data)) as reader:
                while True:
                    chunk = reader.read(16384)
                    if not chunk:
                        break
                    uncompressed.extend(chunk)
            return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


class Codecs(object):
    @staticmethod
    def get_codec(codec_name):
        codec_name = codec_name.lower()
        if codec_name == "null":
            return NullCodec()
        elif codec_name == "deflate":
            return DeflateCodec()
        elif codec_name == "bzip2" and has_bzip2:
            return BZip2Codec()
        elif codec_name == "snappy" and has_snappy:
            return SnappyCodec()
        elif codec_name == "zstandard" and has_zstandard:
            return ZstandardCodec()
        else:
            raise ValueError("Unsupported codec: %r" % codec_name)

    @staticmethod
    def supported_codec_names():
        codec_names = ['null', 'deflate']
        if has_bzip2:
            codec_names.append('bzip2')
        if has_snappy:
            codec_names.append('snappy')
        if has_zstandard:
            codec_names.append('zstandard')
        return codec_names
