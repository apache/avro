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
use Apache\Avro\Schema\AvroFixedSchema;
use Apache\Avro\Schema\AvroSchema;

/**
 * Decodes and reads Avro data from an AvroIO object encoded using
 * Avro binary encoding.
 *
 * @package Avro
 */
class AvroIOBinaryDecoder
{
    /**
     * @param AvroIO $io object from which to read.
     */
    public function __construct(
        private AvroIO $io
    ) {
        Avro::checkPlatform();
    }

    /**
     * @returns null
     */
    public function readNull()
    {
        return null;
    }

    /**
     * @returns boolean
     */
    public function readBoolean(): bool
    {
        return 1 === ord($this->nextByte());
    }

    /**
     * @returns string the next byte from $this->io.
     * @throws AvroException if the next byte cannot be read.
     */
    private function nextByte(): string
    {
        return $this->read(1);
    }

    /**
     * @param int $len count of bytes to read
     * @returns string
     */
    public function read(int $len): string
    {
        return $this->io->read($len);
    }

    public function readInt(): int
    {
        return (int) $this->readLong();
    }

    /**
     * @returns string|int
     */
    public function readLong(): string|int
    {
        $byte = ord($this->nextByte());
        $bytes = [$byte];
        while (0 != ($byte & 0x80)) {
            $byte = ord($this->nextByte());
            $bytes [] = $byte;
        }

        if (Avro::usesGmp()) {
            return AvroGMP::decodeLongFromArray($bytes);
        }

        return self::decodeLongFromArray($bytes);
    }

    /**
     * @param int[] $bytes array of byte ascii values
     * @return int decoded value
     * @internal Requires 64-bit platform
     */
    public static function decodeLongFromArray(array $bytes): int
    {
        $b = array_shift($bytes);
        $n = $b & 0x7f;
        $shift = 7;
        while (0 != ($b & 0x80)) {
            $b = array_shift($bytes);
            $n |= (($b & 0x7f) << $shift);
            $shift += 7;
        }
        return ($n >> 1) ^ (($n >> 63) << 63) ^ -($n & 1);
    }

    public function readFloat(): float
    {
        return self::intBitsToFloat($this->read(4));
    }

    /**
     * Performs decoding of the binary string to a float value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link AvroIOBinaryEncoder::floatToIntBits()} for details.
     */
    public static function intBitsToFloat(string $bits): float
    {
        $float = unpack('g', $bits);
        return (float) $float[1];
    }

    /**
     * @returns double
     */
    public function readDouble(): float
    {
        return self::longBitsToDouble($this->read(8));
    }

    /**
     * Performs decoding of the binary string to a double value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link AvroIOBinaryEncoder::floatToIntBits()} for details.
     *
     */
    public static function longBitsToDouble(string $bits)
    {
        $double = unpack('e', $bits);
        return (float) $double[1];
    }

    /**
     * A string is encoded as a long followed by that many bytes
     * of UTF-8 encoded character data.
     * @returns string
     */
    public function readString(): string
    {
        return $this->readBytes();
    }

    /**
     * @returns string
     */
    public function readBytes(): string
    {
        return $this->read($this->readLong());
    }

    public function skipNull(): void
    {
        return;
    }

    public function skipBoolean(): void
    {
        $this->skip(1);
    }

    /**
     * @param int $len count of bytes to skip
     * @uses AvroIO::seek()
     */
    public function skip(int $len): void
    {
        $this->seek($len, AvroIO::SEEK_CUR);
    }

    /**
     * @uses AvroIO::seek()
     */
    private function seek(int $offset, int $whence): bool
    {
        return $this->io->seek($offset, $whence);
    }

    public function skipInt(): void
    {
        $this->skipLong();
    }

    public function skipLong(): void
    {
        $b = ord($this->nextByte());
        while (0 != ($b & 0x80)) {
            $b = ord($this->nextByte());
        }
    }

    public function skipFloat(): void
    {
        $this->skip(4);
    }

    public function skipDouble(): void
    {
        $this->skip(8);
    }

    public function skipString(): void
    {
        $this->skipBytes();
    }

    public function skipBytes(): void
    {
        $this->skip($this->readLong());
    }

    public function skipFixed(AvroFixedSchema $writers_schema, AvroIOBinaryDecoder $decoder): void
    {
        $decoder->skip($writers_schema->size());
    }

    public function skipEnum(AvroSchema $writers_schema, AvroIOBinaryDecoder $decoder): void
    {
        $decoder->skipInt();
    }

    public function skipUnion($writers_schema, AvroIOBinaryDecoder $decoder): void
    {
        $index = $decoder->readLong();
        AvroIODatumReader::skipData($writers_schema->schemaByIndex($index), $decoder);
    }

    public function skipRecord($writers_schema, AvroIOBinaryDecoder $decoder): void
    {
        foreach ($writers_schema->fields() as $f) {
            AvroIODatumReader::skipData($f->type(), $decoder);
        }
    }

    public function skipArray($writers_schema, AvroIOBinaryDecoder $decoder): void
    {
        $block_count = $decoder->readLong();
        while (0 !== $block_count) {
            if ($block_count < 0) {
                $decoder->skip($this->readLong());
            }
            for ($i = 0; $i < $block_count; $i++) {
                AvroIODatumReader::skipData($writers_schema->items(), $decoder);
            }
            $block_count = $decoder->readLong();
        }
    }

    public function skipMap($writers_schema, AvroIOBinaryDecoder $decoder): void
    {
        $block_count = $decoder->readLong();
        while (0 !== $block_count) {
            if ($block_count < 0) {
                $decoder->skip($this->readLong());
            }
            for ($i = 0; $i < $block_count; $i++) {
                $decoder->skipString();
                AvroIODatumReader::skipData($writers_schema->values(), $decoder);
            }
            $block_count = $decoder->readLong();
        }
    }

    /**
     * @return int position of pointer in AvroIO instance
     * @uses AvroIO::tell()
     */
    private function tell(): int
    {
        return $this->io->tell();
    }
}
