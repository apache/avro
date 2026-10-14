#!/usr/bin/env python3

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

import abc
import binascii
import io
import os
import sys
import struct
import zlib
from typing import Dict, Tuple, Type

import avro.errors
import avro.io

#
# Constants
#
STRUCT_CRC32 = struct.Struct(">I")  # big-endian unsigned int

# Name of the environment variable used to override the default maximum size of
# a single decompressed data-file block.
MAX_DECOMPRESS_LENGTH_ENV = "AVRO_MAX_DECOMPRESS_LENGTH"

# Default upper bound, in bytes, on the size a single data-file block may
# decompress to. A block with a very high compression ratio (or a malformed
# block) can otherwise expand to far more memory than its compressed size.
# Reading a block that would decompress beyond this limit raises an
# :class:`avro.errors.AvroDecompressionSizeException`. This mirrors the Java
# SDK's ``org.apache.avro.limits.decompress.maxLength`` limit (AVRO-4247). The
# default may be overridden with the ``AVRO_MAX_DECOMPRESS_LENGTH`` environment
# variable.
DEFAULT_MAX_DECOMPRESS_LENGTH = 200 * 1024 * 1024  # 200 MiB


def _max_decompress_length() -> int:
    """Return the maximum decompressed block size, honoring the environment override."""
    value = os.environ.get(MAX_DECOMPRESS_LENGTH_ENV)
    if value is None:
        return DEFAULT_MAX_DECOMPRESS_LENGTH
    try:
        parsed = int(value)
    except ValueError:
        return DEFAULT_MAX_DECOMPRESS_LENGTH
    if parsed <= 0:
        return DEFAULT_MAX_DECOMPRESS_LENGTH
    # Clamp to sys.maxsize so the value stays a valid Py_ssize_t: it is passed as
    # the max_length to zlib/bz2 decompress(), which raise OverflowError for a
    # value that does not fit. sys.maxsize is already far larger than any real
    # block, so clamping only affects absurd overrides.
    return min(parsed, sys.maxsize)


def _raise_decompression_too_large(limit: int) -> None:
    raise avro.errors.AvroDecompressionSizeException(f"Decompressed block size exceeds the maximum allowed of {limit} bytes")


def _decompress_read_ceiling(limit: int) -> int:
    """Return limit + 1 (one byte past the limit, to detect an over-limit block),
    but never exceeding sys.maxsize so the value stays a valid Py_ssize_t.

    zlib/bz2 decompress() raise OverflowError for a max_length above Py_ssize_t.
    When the limit is already sys.maxsize (an oversized env override, clamped by
    _max_decompress_length), no realistic block can exceed it, so returning
    sys.maxsize instead of sys.maxsize + 1 is safe.
    """
    return limit if limit >= sys.maxsize else limit + 1


def _snappy_uncompressed_length(data: bytes) -> "int | None":
    """Return the uncompressed length declared in a raw Snappy block header.

    The Snappy format prefixes the compressed data with the uncompressed length
    encoded as a little-endian base-128 varint. Returns ``None`` if the header
    cannot be parsed.
    """
    result = 0
    shift = 0
    for byte in data:
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result
        shift += 7
        if shift > 63:
            break
    return None


def _check_crc32(bytes_: bytes, checksum: bytes) -> None:
    if binascii.crc32(bytes_) & 0xFFFFFFFF != STRUCT_CRC32.unpack(checksum)[0]:
        raise avro.errors.AvroException("Checksum failure")


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


class Codec(abc.ABC):
    """Abstract base class for all Avro codec classes."""

    @staticmethod
    @abc.abstractmethod
    def compress(data: bytes) -> Tuple[bytes, int]:
        """Compress the passed data.

        :param data: a byte string to be compressed
        :type data: str

        :rtype: tuple
        :return: compressed data and its length
        """

    @staticmethod
    @abc.abstractmethod
    def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
        """Read compressed data via the passed BinaryDecoder and decompress it.

        :param readers_decoder: a BinaryDecoder object currently being used for
                                reading an object container file
        :type readers_decoder: avro.io.BinaryDecoder

        :rtype: avro.io.BinaryDecoder
        :return: a newly instantiated BinaryDecoder object that contains the
                 decompressed data which is wrapped by a StringIO
        """


class NullCodec(Codec):
    @staticmethod
    def compress(data: bytes) -> Tuple[bytes, int]:
        return data, len(data)

    @staticmethod
    def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
        readers_decoder.skip_long()
        return readers_decoder


class DeflateCodec(Codec):
    @staticmethod
    def compress(data: bytes) -> Tuple[bytes, int]:
        # The first two characters and last character are zlib
        # wrappers around deflate data.
        compressed_data = zlib.compress(data)[2:-1]
        return compressed_data, len(compressed_data)

    @staticmethod
    def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
        # Compressed data is stored as (length, data), which
        # corresponds to how the "bytes" type is encoded.
        data = readers_decoder.read_bytes()
        # -15 is the log of the window size; negative indicates
        # "raw" (no zlib headers) decompression.  See zlib.h.
        limit = _max_decompress_length()
        decompressor = zlib.decompressobj(-15)
        # Decompress in bounded steps: request at most (limit + 1 - produced)
        # bytes each call so the accumulated output can never exceed the limit by
        # more than one byte before being rejected. decompress()'s max_length
        # leaves unconsumed input in unconsumed_tail, and flush() would otherwise
        # emit the remainder unbounded, so drain the tail in a loop and bound the
        # final flush too.
        ceiling = _decompress_read_ceiling(limit)
        uncompressed = bytearray()
        pending = data
        while True:
            want = ceiling - len(uncompressed)
            uncompressed += decompressor.decompress(pending, want)
            if len(uncompressed) > limit:
                _raise_decompression_too_large(limit)
            pending = decompressor.unconsumed_tail
            if not pending:
                break
        uncompressed += decompressor.flush(ceiling - len(uncompressed))
        if len(uncompressed) > limit:
            _raise_decompression_too_large(limit)
        if not decompressor.eof:
            # The end-of-stream marker was not reached: the block is truncated or
            # corrupt. zlib.decompress() used to raise for this; preserve that.
            raise avro.errors.InvalidAvroBinaryEncoding("Truncated or corrupt deflate block")
        return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


if has_bzip2:

    class BZip2Codec(Codec):
        @staticmethod
        def compress(data: bytes) -> Tuple[bytes, int]:
            compressed_data = bz2.compress(data)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
            length = readers_decoder.read_long()
            data = readers_decoder.read(length)
            limit = _max_decompress_length()
            ceiling = _decompress_read_ceiling(limit)
            uncompressed = bytearray()
            decompressor = bz2.BZ2Decompressor()
            input_data = data
            while True:
                # Request enough to detect exceeding the limit without allocating
                # the full (potentially huge) output.
                want = ceiling - len(uncompressed)
                if want <= 0:
                    want = 1
                uncompressed += decompressor.decompress(input_data, want)
                input_data = b""  # subsequent calls drain buffered output
                if len(uncompressed) > limit:
                    _raise_decompression_too_large(limit)
                if decompressor.eof:
                    # Handle concatenated bzip2 streams as bz2.decompress does.
                    input_data = decompressor.unused_data
                    if not input_data:
                        break
                    decompressor = bz2.BZ2Decompressor()
                elif decompressor.needs_input:
                    # All input consumed but the stream did not end: truncated or corrupt.
                    raise avro.errors.InvalidAvroBinaryEncoding("Truncated or corrupt bzip2 block")
                # otherwise output was capped for this call; loop to drain more
            return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


if has_snappy:

    class SnappyCodec(Codec):
        @staticmethod
        def compress(data: bytes) -> Tuple[bytes, int]:
            compressed_data = snappy.compress(data)
            # A 4-byte, big-endian CRC32 checksum
            compressed_data += STRUCT_CRC32.pack(binascii.crc32(data) & 0xFFFFFFFF)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
            # Compressed data includes a 4-byte CRC32 checksum
            length = readers_decoder.read_long()
            if length < 4:
                raise avro.errors.InvalidAvroBinaryEncoding(
                    f"Invalid snappy block length {length}: must be at least 4 bytes for the trailing CRC32 checksum"
                )
            data = readers_decoder.read(length - 4)
            limit = _max_decompress_length()
            # The Snappy block header declares the uncompressed length as a
            # varint; reject an over-large block before allocating for it.
            declared = _snappy_uncompressed_length(data)
            if declared is not None and declared > limit:
                _raise_decompression_too_large(limit)
            uncompressed = snappy.decompress(data)
            if len(uncompressed) > limit:
                _raise_decompression_too_large(limit)
            checksum = readers_decoder.read(4)
            _check_crc32(uncompressed, checksum)
            return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


if has_zstandard:

    class ZstandardCodec(Codec):
        @staticmethod
        def compress(data: bytes) -> Tuple[bytes, int]:
            compressed_data = zstd.ZstdCompressor().compress(data)
            return compressed_data, len(compressed_data)

        @staticmethod
        def decompress(readers_decoder: avro.io.BinaryDecoder) -> avro.io.BinaryDecoder:
            length = readers_decoder.read_long()
            data = readers_decoder.read(length)
            limit = _max_decompress_length()
            uncompressed = bytearray()
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(io.BytesIO(data)) as reader:
                while True:
                    chunk = reader.read(16384)
                    if not chunk:
                        break
                    # Check before extending so the buffer never grows past the limit.
                    if len(uncompressed) + len(chunk) > limit:
                        _raise_decompression_too_large(limit)
                    uncompressed.extend(chunk)
            return avro.io.BinaryDecoder(io.BytesIO(uncompressed))


KNOWN_CODECS: Dict[str, Type[Codec]] = {
    name[:-5].lower(): class_
    for name, class_ in globals().items()
    if class_ != Codec and name.endswith("Codec") and isinstance(class_, type) and issubclass(class_, Codec)
}


def get_codec(codec_name: str) -> Type[Codec]:
    try:
        return KNOWN_CODECS[codec_name]
    except KeyError:
        raise avro.errors.UnsupportedCodec(f"Unsupported codec: {codec_name}. (Is it installed?)")
