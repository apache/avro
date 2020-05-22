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

use Apache\Avro\Avro;
use Apache\Avro\AvroGMP;
use Apache\Avro\AvroIO;

/**
 * Encodes and writes Avro data to an AvroIO object using
 * Avro binary encoding.
 *
 * @package Avro
 */
class AvroIOBinaryEncoder
{
    /**
     * @var AvroIO
     */
    private $io;

    /**
     * @param AvroIO $io object to which data is to be written.
     *
     */
    public function __construct($io)
    {
        Avro::check_platform();
        $this->io = $io;
    }

    /**
     * @param null $datum actual value is ignored
     */
    public function write_null($datum)
    {
        return null;
    }

    /**
     * @param boolean $datum
     */
    public function write_boolean($datum)
    {
        $byte = $datum ? chr(1) : chr(0);
        $this->write($byte);
    }

    /**
     * @param string $datum
     */
    public function write($datum)
    {
        $this->io->write($datum);
    }

    /**
     * @param int $datum
     */
    public function write_int($datum)
    {
        $this->write_long($datum);
    }

    /**
     * @param int $n
     */
    public function write_long($n)
    {
        if (Avro::uses_gmp()) {
            $this->write(AvroGMP::encode_long($n));
        } else {
            $this->write(self::encode_long($n));
        }
    }

    /**
     * @param int|string $n
     * @returns string long $n encoded as bytes
     * @internal This relies on 64-bit PHP.
     */
    public static function encode_long($n)
    {
        $n = (int) $n;
        $n = ($n << 1) ^ ($n >> 63);
        $str = '';
        while (0 != ($n & ~0x7F)) {
            $str .= chr(($n & 0x7F) | 0x80);
            $n >>= 7;
        }
        $str .= chr($n);
        return $str;
    }

    /**
     * @param float $datum
     * @uses self::float_to_int_bits()
     */
    public function write_float($datum)
    {
        $this->write(self::float_to_int_bits($datum));
    }

    /**
     * Performs encoding of the given float value to a binary string
     *
     * XXX: This is <b>not</b> endian-aware! The {@link Avro::check_platform()}
     * called in {@link AvroIOBinaryEncoder::__construct()} should ensure the
     * library is only used on little-endian platforms, which ensure the little-endian
     * encoding required by the Avro spec.
     *
     * @param float $float
     * @returns string bytes
     * @see Avro::check_platform()
     */
    public static function float_to_int_bits($float)
    {
        return pack('f', (float) $float);
    }

    /**
     * @param float $datum
     * @uses self::double_to_long_bits()
     */
    public function write_double($datum)
    {
        $this->write(self::double_to_long_bits($datum));
    }

    /**
     * Performs encoding of the given double value to a binary string
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link AvroIOBinaryEncoder::float_to_int_bits()} for details.
     *
     * @param double $double
     * @returns string bytes
     */
    public static function double_to_long_bits($double)
    {
        return pack('d', (double) $double);
    }

    /**
     * @param string $str
     * @uses self::write_bytes()
     */
    public function write_string($str)
    {
        $this->write_bytes($str);
    }

    /**
     * @param string $bytes
     */
    public function write_bytes($bytes)
    {
        $this->write_long(strlen($bytes));
        $this->write($bytes);
    }
}
