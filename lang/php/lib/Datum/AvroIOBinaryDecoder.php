<?php

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache\Avro\Datum;

// @todo Implement JSON encoding, as is required by the Avro spec.
use Apache\Avro\Avro;
use Apache\Avro\AvroException;
use Apache\Avro\AvroGMP;
use Apache\Avro\AvroIO;

/**
 * Decodes and reads Avro data from an AvroIO object encoded using
 * Avro binary encoding.
 *
 * @package Avro
 */
class AvroIOBinaryDecoder
{

    /**
     * @var AvroIO
     */
    private $io;

    /**
     * @param AvroIO $io object from which to read.
     */
    public function __construct($io)
    {
        Avro::check_platform();
        $this->io = $io;
    }

    /**
     * @returns null
     */
    public function read_null()
    {
        return null;
    }

    /**
     * @returns boolean
     */
    public function read_boolean()
    {
        return (bool) (1 == ord($this->next_byte()));
    }

    /**
     * @returns string the next byte from $this->io.
     * @throws AvroException if the next byte cannot be read.
     */
    private function next_byte()
    {
        return $this->read(1);
    }

    /**
     * @param int $len count of bytes to read
     * @returns string
     */
    public function read($len)
    {
        return $this->io->read($len);
    }

    /**
     * @returns int
     */
    public function read_int()
    {
        return (int) $this->read_long();
    }

    /**
     * @returns long
     */
    public function read_long()
    {
        $byte = ord($this->next_byte());
        $bytes = array($byte);
        while (0 != ($byte & 0x80)) {
            $byte = ord($this->next_byte());
            $bytes [] = $byte;
        }

        if (Avro::uses_gmp()) {
            return AvroGMP::decode_long_from_array($bytes);
        }

        return self::decode_long_from_array($bytes);
    }

    /**
     * @param int[] array of byte ascii values
     * @returns long decoded value
     * @internal Requires 64-bit platform
     */
    public static function decode_long_from_array($bytes)
    {
        $b = array_shift($bytes);
        $n = $b & 0x7f;
        $shift = 7;
        while (0 != ($b & 0x80)) {
            $b = array_shift($bytes);
            $n |= (($b & 0x7f) << $shift);
            $shift += 7;
        }
        return (($n >> 1) ^ -($n & 1));
    }

    /**
     * @returns float
     */
    public function read_float()
    {
        return self::int_bits_to_float($this->read(4));
    }

    /**
     * Performs decoding of the binary string to a float value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link AvroIOBinaryEncoder::float_to_int_bits()} for details.
     *
     * @param string $bits
     * @returns float
     */
    public static function int_bits_to_float($bits)
    {
        $float = unpack('f', $bits);
        return (float) $float[1];
    }

    /**
     * @returns double
     */
    public function read_double()
    {
        return self::long_bits_to_double($this->read(8));
    }

    /**
     * Performs decoding of the binary string to a double value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link AvroIOBinaryEncoder::float_to_int_bits()} for details.
     *
     * @param string $bits
     * @returns float
     */
    public static function long_bits_to_double($bits)
    {
        $double = unpack('d', $bits);
        return (double) $double[1];
    }

    /**
     * A string is encoded as a long followed by that many bytes
     * of UTF-8 encoded character data.
     * @returns string
     */
    public function read_string()
    {
        return $this->read_bytes();
    }

    /**
     * @returns string
     */
    public function read_bytes()
    {
        return $this->read($this->read_long());
    }

    public function skip_null()
    {
        return null;
    }

    public function skip_boolean()
    {
        return $this->skip(1);
    }

    /**
     * @param int $len count of bytes to skip
     * @uses AvroIO::seek()
     */
    public function skip($len)
    {
        $this->seek($len, AvroIO::SEEK_CUR);
    }

    /**
     * @param int $offset
     * @param int $whence
     * @returns boolean true upon success
     * @uses AvroIO::seek()
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    public function skip_int()
    {
        return $this->skip_long();
    }

    public function skip_long()
    {
        $b = $this->next_byte();
        while (0 != (ord($b) & 0x80)) {
            $b = $this->next_byte();
        }
    }

    public function skip_float()
    {
        return $this->skip(4);
    }

    public function skip_double()
    {
        return $this->skip(8);
    }

    public function skip_string()
    {
        return $this->skip_bytes();
    }

    public function skip_bytes()
    {
        return $this->skip($this->read_long());
    }

    /**
     * @returns int position of pointer in AvroIO instance
     * @uses AvroIO::tell()
     */
    private function tell()
    {
        return $this->io->tell();
    }
}
